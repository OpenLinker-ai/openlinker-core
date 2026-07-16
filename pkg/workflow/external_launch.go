package workflow

import (
	"context"
	"errors"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

type ExternalExecutionLaunchFence struct {
	CallerServiceID   string
	ExternalRequestID uuid.UUID
	ActorUserID       uuid.UUID
	LaunchToken       uuid.UUID
}

var ErrExternalWorkflowLaunchFenceRejected = errors.New("external workflow launch fence rejected")

// StartExternalExecutionWorkflowRunWithFence commits the deterministic
// Workflow Run only while the external launch token is current. The external
// key row is always locked before the external execution row.
func (s *Service) StartExternalExecutionWorkflowRunWithFence(
	ctx context.Context,
	targetOwnerID, actorUserID, workflowID uuid.UUID,
	input map[string]interface{},
	fence ExternalExecutionLaunchFence,
) (*WorkflowRunResponse, error) {
	callerServiceID := strings.TrimSpace(fence.CallerServiceID)
	if s == nil || s.pool == nil || callerServiceID == "" || callerServiceID != fence.CallerServiceID ||
		targetOwnerID == uuid.Nil || actorUserID == uuid.Nil || workflowID == uuid.Nil ||
		fence.ExternalRequestID == uuid.Nil || fence.ActorUserID != actorUserID || fence.LaunchToken == uuid.Nil {
		return nil, httpx.Conflict(ErrExternalWorkflowLaunchFenceRejected.Error())
	}
	input, inputJSON, err := normalizeExternalExecutionWorkflowInput(input)
	if err != nil {
		return nil, httpx.BadRequest("input 不是合法 JSON")
	}
	if replay, found, err := s.lookupExternalExecutionWorkflowRun(
		ctx, callerServiceID, actorUserID, workflowID, fence.ExternalRequestID,
		input, defaultWorkflowRunMaxAttempts,
	); err != nil || found {
		return replay, err
	}
	if s.runtime == nil {
		return nil, httpx.Internal("workflow runtime 未配置")
	}
	w, nodes, err := s.getWorkflowForOwner(ctx, targetOwnerID, workflowID)
	if err != nil {
		return nil, err
	}
	if w.Status != "active" || len(nodes) == 0 {
		return nil, httpx.Conflict("workflow 当前不可执行")
	}
	if _, err := workflowGraphFromDefinition(w, nodes); err != nil {
		return nil, err
	}
	if err := s.validateWorkflowStoredAgentsAvailable(ctx, uuid.Nil, nodes, true); err != nil {
		return nil, err
	}

	runID := externalExecutionWorkflowRunID(callerServiceID, fence.ExternalRequestID)
	var run db.WorkflowRun
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var keyActorID uuid.UUID
		if err := tx.QueryRow(ctx, `
			SELECT actor_user_id
			FROM external_execution_keys
			WHERE caller_service_id = $1 AND external_request_id = $2
			FOR UPDATE
		`, callerServiceID, fence.ExternalRequestID).Scan(&keyActorID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrExternalWorkflowLaunchFenceRejected
			}
			return err
		}
		if keyActorID != actorUserID {
			return ErrExternalWorkflowLaunchFenceRejected
		}
		var state, targetType string
		var token uuid.UUID
		var targetID uuid.UUID
		var leaseActive bool
		if err := tx.QueryRow(ctx, `
			SELECT start_state, start_token, start_lease_until > clock_timestamp(),
			       target_type, target_id
			FROM external_executions
			WHERE caller_service_id = $1 AND external_request_id = $2
			FOR UPDATE
		`, callerServiceID, fence.ExternalRequestID).Scan(
			&state, &token, &leaseActive, &targetType, &targetID,
		); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrExternalWorkflowLaunchFenceRejected
			}
			return err
		}
		if state != "launching" || token != fence.LaunchToken || !leaseActive ||
			targetType != "workflow" || targetID != workflowID {
			return ErrExternalWorkflowLaunchFenceRejected
		}
		created, err := s.queries.WithTx(tx).CreatePendingExternalExecutionWorkflowRun(
			ctx, db.CreatePendingExternalExecutionWorkflowRunParams{
				ID: runID, WorkflowID: workflowID, UserID: actorUserID,
				Input: inputJSON, MaxAttempts: defaultWorkflowRunMaxAttempts,
			},
		)
		if errors.Is(err, pgx.ErrNoRows) {
			existing, getErr := s.queries.WithTx(tx).GetWorkflowRunByID(ctx, runID)
			if getErr != nil {
				return getErr
			}
			if !externalExecutionWorkflowRunMatches(
				existing, workflowID, actorUserID, input, defaultWorkflowRunMaxAttempts,
			) {
				return httpx.Conflict("external_request_id 已用于其他 workflow 执行")
			}
			run = existing
			return nil
		}
		if err != nil {
			return err
		}
		run = created
		return nil
	})
	if errors.Is(err, ErrExternalWorkflowLaunchFenceRejected) {
		return nil, httpx.Conflict(ErrExternalWorkflowLaunchFenceRejected.Error())
	}
	if err != nil {
		return nil, httpx.Internal("创建 external workflow_run 失败")
	}
	resp := workflowRunToResponse(run, nil)
	return &resp, nil
}

// LookupExternalExecutionWorkflowRunByIdentity is the key-only recovery path.
// The deterministic ID plus actor/target checks are sufficient; no Cloud input
// is required and the method never mutates state.
func (s *Service) LookupExternalExecutionWorkflowRunByIdentity(
	ctx context.Context,
	callerServiceID string,
	actorUserID, workflowID, externalRequestID uuid.UUID,
) (*WorkflowRunResponse, bool, error) {
	callerServiceID = strings.TrimSpace(callerServiceID)
	if callerServiceID == "" || actorUserID == uuid.Nil || workflowID == uuid.Nil || externalRequestID == uuid.Nil {
		return nil, false, httpx.BadRequest("external workflow durable identity 无效")
	}
	run, err := s.queries.GetWorkflowRunByID(ctx, externalExecutionWorkflowRunID(callerServiceID, externalRequestID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if run.UserID != actorUserID || run.WorkflowID != workflowID || run.MaxAttempts != defaultWorkflowRunMaxAttempts {
		return nil, false, httpx.Conflict("external workflow durable identity conflict")
	}
	resp := workflowRunToResponse(run, nil)
	return &resp, true, nil
}
