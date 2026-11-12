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
	// ExpectedContractHash is the contract the caller authorized. The launch
	// transaction recomputes the hash of the bound version and refuses on a
	// mismatch.
	ExpectedContractHash string
	// TargetVersionID binds the launch to one definition version; nil binds
	// the version current at launch.
	TargetVersionID *uuid.UUID
}

var ErrExternalWorkflowLaunchFenceRejected = errors.New("external workflow launch fence rejected")

// Deterministic launch rejections, decided inside the launch transaction.
// The transaction writes nothing; the caller records the rejection.
var (
	ErrExternalWorkflowTargetUnavailable     = errors.New("external workflow target unavailable")
	ErrExternalWorkflowTargetContractChanged = errors.New("external workflow target contract changed")
)

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
		// A committed run is replayed without re-checking eligibility.
		if existing, getErr := s.queries.WithTx(tx).GetWorkflowRunByID(ctx, runID); getErr == nil {
			if !externalExecutionWorkflowRunMatches(existing, workflowID, actorUserID, input, defaultWorkflowRunMaxAttempts) {
				return httpx.Conflict("external_request_id 已用于其他 workflow 执行")
			}
			run = existing
			return nil
		} else if !errors.Is(getErr, pgx.ErrNoRows) {
			return getErr
		}
		versionID, err := s.recheckExternalWorkflowLaunch(ctx, tx, targetOwnerID, workflowID, fence)
		if err != nil {
			return err
		}
		created, err := s.queries.WithTx(tx).CreatePendingExternalExecutionWorkflowRun(
			ctx, db.CreatePendingExternalExecutionWorkflowRunParams{
				ID: runID, WorkflowID: workflowID, UserID: actorUserID,
				Input: inputJSON, MaxAttempts: defaultWorkflowRunMaxAttempts,
				WorkflowVersionID: &versionID,
			},
		)
		if errors.Is(err, pgx.ErrNoRows) {
			existing, getErr := s.queries.WithTx(tx).GetWorkflowRunByID(ctx, runID)
			if errors.Is(getErr, pgx.ErrNoRows) {
				// No row and no identity: the insert was refused by admission.
				return errWorkflowAdmissionDraining
			}
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
	if errors.Is(err, errWorkflowAdmissionDraining) {
		return nil, errWorkflowAdmissionDraining
	}
	if errors.Is(err, ErrExternalWorkflowTargetUnavailable) || errors.Is(err, ErrExternalWorkflowTargetContractChanged) {
		return nil, err
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

// recheckExternalWorkflowLaunch runs inside the launch transaction, after the
// external key and execution rows are locked. Lock order: workflow row (FOR
// SHARE, must be active) -> node Agent rows (FOR SHARE, by id). Agent
// visibility, lifecycle and capability writes take the Agent row lock first,
// so eligibility and the recomputed contract hash cannot change until this
// transaction commits. External Execution never applies the owner exception
// for private Agents.
func (s *Service) recheckExternalWorkflowLaunch(
	ctx context.Context,
	tx pgx.Tx,
	targetOwnerID, workflowID uuid.UUID,
	fence ExternalExecutionLaunchFence,
) (uuid.UUID, error) {
	var status string
	var ownerID uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT status, user_id FROM workflows WHERE id = $1 FOR SHARE`, workflowID).Scan(&status, &ownerID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return uuid.Nil, ErrExternalWorkflowTargetUnavailable
		}
		return uuid.Nil, err
	}
	if status != workflowStatusActive || ownerID != targetOwnerID {
		return uuid.Nil, ErrExternalWorkflowTargetUnavailable
	}
	def, err := loadWorkflowVersion(ctx, tx, workflowID, fence.TargetVersionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, ErrExternalWorkflowTargetUnavailable
	}
	if err != nil {
		return uuid.Nil, err
	}
	if def.semantics != workflowSemanticsV1 || len(def.nodes) == 0 {
		return uuid.Nil, ErrExternalWorkflowTargetUnavailable
	}
	if _, err := tx.Exec(ctx, `
		SELECT 1 FROM agents WHERE id IN (
		    SELECT agent_id FROM workflow_version_nodes WHERE workflow_version_id = $1
		) ORDER BY id FOR SHARE
	`, def.id); err != nil {
		return uuid.Nil, err
	}
	if _, err := workflowGraphFromDefinition(def.workflow, def.nodes); err != nil {
		return uuid.Nil, ErrExternalWorkflowTargetUnavailable
	}
	txQueries := db.New(tx)
	if err := s.validateWorkflowStoredAgentsAvailableWith(ctx, txQueries, uuid.Nil, def.nodes, true); err != nil {
		if isWorkflowHTTPError(err) {
			return uuid.Nil, ErrExternalWorkflowTargetUnavailable
		}
		return uuid.Nil, err
	}
	if fence.ExpectedContractHash != "" {
		hash, err := s.externalExecutionWorkflowContractHashWith(ctx, txQueries, def.workflow, def.nodes)
		if err != nil {
			if isWorkflowHTTPError(err) {
				return uuid.Nil, ErrExternalWorkflowTargetUnavailable
			}
			return uuid.Nil, err
		}
		if hash != fence.ExpectedContractHash {
			return uuid.Nil, ErrExternalWorkflowTargetContractChanged
		}
	}
	return def.id, nil
}
