package workflow

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

const (
	WorkflowAdmissionOpen     = "open"
	WorkflowAdmissionDraining = "draining"

	workflowAdmissionReasonMaxRunes = 500
	adminWorkflowRunListMaxLimit    = 200

	// A cancellation the request path just wrote is still being reconciled by
	// that request; the scan leaves it alone for this long.
	workflowCancellationScanMinAge = 5 * time.Second
)

// errWorkflowAdmissionDraining is returned by every entry that would create a
// workflow run while admission is draining. It is temporary by design: the
// administrator reopens admission once the release is verified.
var errWorkflowAdmissionDraining = httpx.NewError(
	http.StatusServiceUnavailable,
	httpx.ErrorCode("WORKFLOW_DRAINING"),
	"workflow 正在维护排空，暂不接受新的运行，请稍后重试",
)

type WorkflowAdmissionResponse struct {
	State           string     `json:"state"`
	Reason          string     `json:"reason,omitempty"`
	ChangedBy       *string    `json:"changed_by,omitempty"`
	ChangedAt       time.Time  `json:"changed_at"`
	NonterminalRuns int64      `json:"nonterminal_runs"`
	CancellingRuns  int64      `json:"cancelling_runs"`
	CheckedAt       *time.Time `json:"checked_at,omitempty"`
}

type SetWorkflowAdmissionRequest struct {
	State  string `json:"state" validate:"required,oneof=open draining"`
	Reason string `json:"reason" validate:"max=2000"`
}

type AdminWorkflowRunSummary struct {
	ID                string     `json:"id"`
	WorkflowID        string     `json:"workflow_id"`
	OwnerID           string     `json:"owner_id"`
	Status            string     `json:"status"`
	CreatedAt         time.Time  `json:"created_at"`
	ClaimedAt         *time.Time `json:"claimed_at,omitempty"`
	CancellationState string     `json:"cancellation_state,omitempty"`
}

type AdminWorkflowRunListResponse struct {
	State string                    `json:"state"`
	Items []AdminWorkflowRunSummary `json:"items"`
}

type AdminWorkflowCancellationResponse struct {
	WorkflowRunID     string `json:"workflow_run_id"`
	OwnerID           string `json:"owner_id"`
	Status            string `json:"status"`
	CancellationState string `json:"cancellation_state"`
	RequestedBy       string `json:"requested_by"`
	RequestedVia      string `json:"requested_via"`
}

// GetWorkflowAdmission reports the durable admission state and how much work
// is left before a release may migrate.
func (s *Service) GetWorkflowAdmission(ctx context.Context) (*WorkflowAdmissionResponse, error) {
	row, err := s.queries.GetWorkflowAdmission(ctx)
	if err != nil {
		log.Error().Err(err).Msg("workflow.GetWorkflowAdmission")
		return nil, httpx.Internal("读取 workflow 准入状态失败")
	}
	resp := workflowAdmissionToResponse(row)
	if err := s.fillWorkflowDrainCounts(ctx, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// SetWorkflowAdmission switches admission. The update takes the admission row
// lock, which waits for every creation or claim that already read the row FOR
// SHARE; once it commits as draining, nothing created or claimed afterwards
// saw 'open'.
func (s *Service) SetWorkflowAdmission(
	ctx context.Context,
	adminUserID uuid.UUID,
	req *SetWorkflowAdmissionRequest,
) (*WorkflowAdmissionResponse, error) {
	if adminUserID == uuid.Nil || req == nil {
		return nil, httpx.BadRequest("workflow 准入参数无效")
	}
	state := strings.TrimSpace(req.State)
	if state != WorkflowAdmissionOpen && state != WorkflowAdmissionDraining {
		return nil, httpx.BadRequest("state 只能是 open 或 draining")
	}
	reason := strings.TrimSpace(req.Reason)
	if len([]rune(reason)) > workflowAdmissionReasonMaxRunes {
		return nil, httpx.BadRequest("reason 过长")
	}
	var reasonParam *string
	if reason != "" {
		reasonParam = &reason
	}
	changedBy := adminUserID
	row, err := s.queries.SetWorkflowAdmission(ctx, db.SetWorkflowAdmissionParams{
		State: state, Reason: reasonParam, ChangedBy: &changedBy,
	})
	if err != nil {
		log.Error().Err(err).Str("state", state).Msg("workflow.SetWorkflowAdmission")
		return nil, httpx.Internal("更新 workflow 准入状态失败")
	}
	log.Info().Str("state", state).Str("admin_user_id", adminUserID.String()).Msg("workflow: admission changed")
	resp := workflowAdmissionToResponse(row)
	if err := s.fillWorkflowDrainCounts(ctx, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func workflowAdmissionToResponse(row db.WorkflowAdmission) *WorkflowAdmissionResponse {
	resp := &WorkflowAdmissionResponse{State: row.State, ChangedAt: row.ChangedAt}
	if row.Reason != nil {
		resp.Reason = *row.Reason
	}
	if row.ChangedBy != nil {
		id := row.ChangedBy.String()
		resp.ChangedBy = &id
	}
	return resp
}

func (s *Service) fillWorkflowDrainCounts(ctx context.Context, resp *WorkflowAdmissionResponse) error {
	var checkedAt time.Time
	if err := s.pool.QueryRow(ctx, `
		SELECT
		    (SELECT count(*) FROM workflow_runs
		     WHERE status IN ('pending', 'running', 'paused')),
		    (SELECT count(*) FROM workflow_run_cancellations
		     WHERE state IN ('requested', 'stopping')),
		    clock_timestamp()
	`).Scan(&resp.NonterminalRuns, &resp.CancellingRuns, &checkedAt); err != nil {
		log.Error().Err(err).Msg("workflow.fillWorkflowDrainCounts")
		return httpx.Internal("统计 workflow 排空进度失败")
	}
	resp.CheckedAt = &checkedAt
	return nil
}

// ListAdminWorkflowRuns lists every user's runs that still block a drain:
// "nonterminal" runs and runs whose cancellation is still "cancelling"
// (requested or stopping).
func (s *Service) ListAdminWorkflowRuns(ctx context.Context, state string, limit int32) (*AdminWorkflowRunListResponse, error) {
	if limit <= 0 || limit > adminWorkflowRunListMaxLimit {
		limit = 50
	}
	var query string
	switch state {
	case "", "nonterminal":
		state = "nonterminal"
		query = `
			SELECT r.id, r.workflow_id, r.user_id, r.status, r.created_at, r.claimed_at,
			       COALESCE(c.state, '')
			FROM workflow_runs r
			LEFT JOIN workflow_run_cancellations c ON c.workflow_run_id = r.id
			WHERE r.status IN ('pending', 'running', 'paused')
			ORDER BY r.created_at ASC, r.id ASC
			LIMIT $1`
	case "cancelling":
		query = `
			SELECT r.id, r.workflow_id, r.user_id, r.status, r.created_at, r.claimed_at,
			       c.state
			FROM workflow_run_cancellations c
			JOIN workflow_runs r ON r.id = c.workflow_run_id
			WHERE c.state IN ('requested', 'stopping')
			ORDER BY c.requested_at ASC, c.workflow_run_id ASC
			LIMIT $1`
	default:
		return nil, httpx.BadRequest("state 只能是 nonterminal 或 cancelling")
	}
	rows, err := s.pool.Query(ctx, query, limit)
	if err != nil {
		log.Error().Err(err).Str("state", state).Msg("workflow.ListAdminWorkflowRuns")
		return nil, httpx.Internal("查询 workflow_run 失败")
	}
	defer rows.Close()
	items := []AdminWorkflowRunSummary{}
	for rows.Next() {
		var id, workflowID, ownerID uuid.UUID
		var item AdminWorkflowRunSummary
		if err := rows.Scan(&id, &workflowID, &ownerID, &item.Status, &item.CreatedAt, &item.ClaimedAt, &item.CancellationState); err != nil {
			return nil, httpx.Internal("查询 workflow_run 失败")
		}
		item.ID, item.WorkflowID, item.OwnerID = id.String(), workflowID.String(), ownerID.String()
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, httpx.Internal("查询 workflow_run 失败")
	}
	return &AdminWorkflowRunListResponse{State: state, Items: items}, nil
}

// AdminCancelWorkflowRun cancels any user's run for a drain. It reuses the
// owner cancellation transaction: the run owner stays the authorization
// subject for Runtime, and the administrator is recorded as the requester.
// A pending run is not simply flipped to canceled: the transaction writes a
// durable cancellation, invalidates unlaunched children and reconciles every
// child already created (an old stale requeue can leave those behind a
// pending run).
func (s *Service) AdminCancelWorkflowRun(
	ctx context.Context,
	adminUserID, workflowRunID uuid.UUID,
) (*AdminWorkflowCancellationResponse, error) {
	if adminUserID == uuid.Nil || workflowRunID == uuid.Nil {
		return nil, httpx.BadRequest("workflow cancellation identity 无效")
	}
	run, err := s.queries.GetWorkflowRunByID(ctx, workflowRunID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("workflow_run 不存在")
	}
	if err != nil {
		return nil, httpx.Internal("读取 workflow_run 失败")
	}
	ownerID := run.UserID
	evidence, err := s.requestWorkflowCancellation(
		ctx, ownerID, workflowRunID, "OWNER_CANCEL_REQUESTED",
		cancellationRequester{userID: adminUserID, via: cancellationRequestedViaAdmin},
	)
	if err != nil {
		return nil, err
	}
	if evidence.State == "not_applied" {
		return nil, httpx.Conflict("只有 pending / running / paused workflow_run 可以取消")
	}
	if evidence.State == "requested" || evidence.State == "stopping" {
		if err := s.reconcileWorkflowCancellation(ctx, ownerID, workflowRunID); err != nil {
			// The cancellation is durable; the scan keeps reconciling it.
			log.Warn().Err(err).Str("workflow_run_id", workflowRunID.String()).Msg("workflow.AdminCancelWorkflowRun: reconcile")
		}
	}
	return s.adminWorkflowCancellation(ctx, workflowRunID)
}

func (s *Service) adminWorkflowCancellation(ctx context.Context, workflowRunID uuid.UUID) (*AdminWorkflowCancellationResponse, error) {
	var ownerID, requestedBy uuid.UUID
	resp := &AdminWorkflowCancellationResponse{WorkflowRunID: workflowRunID.String()}
	if err := s.pool.QueryRow(ctx, `
		SELECT r.user_id, r.status, c.state, c.requested_by, c.requested_via
		FROM workflow_runs r
		JOIN workflow_run_cancellations c ON c.workflow_run_id = r.id
		WHERE r.id = $1
	`, workflowRunID).Scan(&ownerID, &resp.Status, &resp.CancellationState, &requestedBy, &resp.RequestedVia); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("workflow cancellation 不存在")
		}
		return nil, httpx.Internal("读取 workflow cancellation 失败")
	}
	resp.OwnerID, resp.RequestedBy = ownerID.String(), requestedBy.String()
	return resp, nil
}

// ReconcileWorkflowCancellations advances cancellations left in requested or
// stopping: a crash between the cancellation commit and its first
// reconciliation, a reconciliation error, or children that stop after the
// request returned. It does not filter on the parent run, which is already
// canceled by then. It returns how many records it attempted.
func (s *Service) ReconcileWorkflowCancellations(ctx context.Context, limit int) (int, error) {
	if s == nil || s.pool == nil {
		return 0, nil
	}
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.pool.Query(ctx, `
		SELECT workflow_run_id, actor_user_id
		FROM workflow_run_cancellations
		WHERE state IN ('requested', 'stopping')
		  AND updated_at <= clock_timestamp() - make_interval(secs => $2)
		ORDER BY updated_at ASC, workflow_run_id ASC
		LIMIT $1
	`, limit, workflowCancellationScanMinAge.Seconds())
	if err != nil {
		return 0, err
	}
	type pending struct{ runID, ownerID uuid.UUID }
	items := []pending{}
	for rows.Next() {
		var item pending
		if err := rows.Scan(&item.runID, &item.ownerID); err != nil {
			rows.Close()
			return 0, err
		}
		items = append(items, item)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	for _, item := range items {
		if ctx.Err() != nil {
			return len(items), ctx.Err()
		}
		if err := s.reconcileWorkflowCancellation(ctx, item.ownerID, item.runID); err != nil {
			log.Warn().Err(err).Str("workflow_run_id", item.runID.String()).Msg("workflow: cancellation reconcile failed; will retry")
			continue
		}
		// Record that this pass looked at the cancellation even when its state
		// did not change, so the scan works through the oldest first.
		if _, err := s.pool.Exec(ctx, `
			UPDATE workflow_run_cancellations
			SET updated_at = clock_timestamp()
			WHERE workflow_run_id = $1 AND state IN ('requested', 'stopping')
		`, item.runID); err != nil {
			return len(items), err
		}
	}
	return len(items), nil
}
