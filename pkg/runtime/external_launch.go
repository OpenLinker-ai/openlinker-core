package runtime

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

var (
	ErrExternalExecutionLaunchFenceRejected = errors.New("external execution launch fence rejected")
	ErrWorkflowChildLaunchFenceRejected     = errors.New("workflow child launch fence rejected")
)

// RunCreationIdentity is the non-sensitive identity needed to recover a Run
// after creation committed but its caller crashed before recording the Run ID.
type RunCreationIdentity struct {
	IdempotencyKeyHash  []byte
	CreationFingerprint []byte
}

type ExternalExecutionLaunchFence struct {
	CallerServiceID   string
	ExternalRequestID uuid.UUID
	ActorUserID       uuid.UUID
	LaunchToken       uuid.UUID
}

type WorkflowChildLaunchFence struct {
	WorkflowRunID  uuid.UUID
	WorkflowNodeID uuid.UUID
	LaunchToken    uuid.UUID
}

func (s *Service) PrepareRunCreationIdentity(req *RunRequest, source string) (RunCreationIdentity, error) {
	if s == nil {
		return RunCreationIdentity{}, httpx.Internal("Runtime 未配置")
	}
	if source == "" {
		source = "web"
	}
	normalized, err := s.normalizeRunCreation(req, source, createRunOptions{})
	if err != nil {
		return RunCreationIdentity{}, err
	}
	return RunCreationIdentity{
		IdempotencyKeyHash:  append([]byte(nil), normalized.idempotencyKeyHash...),
		CreationFingerprint: append([]byte(nil), normalized.idempotencyFingerprint...),
	}, nil
}

// LookupRunByCreationIdentity is strictly read-only and requires the exact
// actor/key/fingerprint tuple persisted before launch. It never needs input.
func (s *Service) LookupRunByCreationIdentity(
	ctx context.Context,
	actorUserID uuid.UUID,
	keyHash, fingerprint []byte,
) (*RunResponse, bool, error) {
	if s == nil || actorUserID == uuid.Nil || len(keyHash) != sha256.Size || len(fingerprint) != sha256.Size {
		return nil, false, httpx.BadRequest("Runtime durable creation identity 无效")
	}
	if s.queries == nil {
		return nil, false, httpx.Internal("Runtime 存储未配置")
	}
	runID, found, err := s.findExistingRunByIdentity(ctx, actorUserID, keyHash, fingerprint)
	if err != nil || !found {
		return nil, found, err
	}
	resp, err := s.idempotencyReplayResponse(ctx, actorUserID, runID)
	return resp, true, err
}

// StartExternalRun creates a Run only while the launch token is current. The
// external key row is locked before its execution row, which is the global
// key -> execution lock order shared with cancellation.
func (s *Service) StartExternalRun(
	ctx context.Context,
	actorUserID uuid.UUID,
	req *RunRequest,
	source string,
	fence ExternalExecutionLaunchFence,
) (*RunResponse, error) {
	if err := validateExternalExecutionLaunchFence(actorUserID, fence); err != nil {
		return nil, err
	}
	identity, err := s.PrepareRunCreationIdentity(req, source)
	if err != nil {
		return nil, err
	}
	return s.startRunWithOptions(ctx, actorUserID, req, source, createRunOptions{
		allowOfflineQueuedRuntime: true,
		beforeCreate: func(txCtx context.Context, tx pgx.Tx) error {
			return verifyExternalExecutionLaunchFence(txCtx, tx, fence, identity)
		},
	})
}

// RunWorkflowChild makes the workflow parent/step launch fence part of the Run
// creation transaction. The created state and child Run ID commit atomically.
func (s *Service) RunWorkflowChild(
	ctx context.Context,
	actorUserID uuid.UUID,
	req *RunRequest,
	source string,
	fence WorkflowChildLaunchFence,
) (*RunResponse, error) {
	if actorUserID == uuid.Nil || fence.WorkflowRunID == uuid.Nil ||
		fence.WorkflowNodeID == uuid.Nil || fence.LaunchToken == uuid.Nil {
		return nil, httpx.Conflict(ErrWorkflowChildLaunchFenceRejected.Error())
	}
	return s.runWithOptions(ctx, actorUserID, req, source, createRunOptions{
		beforeCreate: func(txCtx context.Context, tx pgx.Tx) error {
			return verifyWorkflowChildLaunchFence(txCtx, tx, actorUserID, fence)
		},
		afterCreate: func(txCtx context.Context, tx pgx.Tx, runID uuid.UUID) error {
			commandTag, err := tx.Exec(txCtx, `
				UPDATE workflow_step_launches
				SET state = 'created', run_id = $4, updated_at = clock_timestamp()
				WHERE workflow_run_id = $1
				  AND workflow_node_id = $2
				  AND launch_token = $3
				  AND state = 'claimed'
				  AND run_id IS NULL
			`, fence.WorkflowRunID, fence.WorkflowNodeID, fence.LaunchToken, runID)
			if err != nil {
				return err
			}
			if commandTag.RowsAffected() != 1 {
				return ErrWorkflowChildLaunchFenceRejected
			}
			return nil
		},
	})
}

func (s *Service) runWithOptions(
	ctx context.Context,
	userID uuid.UUID,
	req *RunRequest,
	source string,
	opts createRunOptions,
) (*RunResponse, error) {
	invocation, resp, err := s.createRunningRun(ctx, userID, req, source, opts)
	if err != nil {
		return nil, err
	}
	if invocation == nil {
		return resp, nil
	}
	if s.isQueuedRuntime(invocation) {
		s.recordRunEventBestEffort(ctx, invocation.runID, "run.dispatch.pending", map[string]interface{}{
			"connection_mode": invocation.agent.ConnectionMode,
			"agent_id":        invocation.agent.ID.String(),
		})
		return resp, nil
	}
	return s.executeRun(ctx, invocation), nil
}

func validateExternalExecutionLaunchFence(actorUserID uuid.UUID, fence ExternalExecutionLaunchFence) error {
	if actorUserID == uuid.Nil || strings.TrimSpace(fence.CallerServiceID) == "" ||
		fence.CallerServiceID != strings.TrimSpace(fence.CallerServiceID) ||
		fence.ExternalRequestID == uuid.Nil || fence.ActorUserID != actorUserID ||
		fence.LaunchToken == uuid.Nil {
		return httpx.Conflict(ErrExternalExecutionLaunchFenceRejected.Error())
	}
	return nil
}

func verifyExternalExecutionLaunchFence(
	ctx context.Context,
	tx pgx.Tx,
	fence ExternalExecutionLaunchFence,
	identity RunCreationIdentity,
) error {
	var actorUserID uuid.UUID
	if err := tx.QueryRow(ctx, `
		SELECT actor_user_id
		FROM external_execution_keys
		WHERE caller_service_id = $1 AND external_request_id = $2
		FOR UPDATE
	`, fence.CallerServiceID, fence.ExternalRequestID).Scan(&actorUserID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrExternalExecutionLaunchFenceRejected
		}
		return err
	}
	if actorUserID != fence.ActorUserID {
		return ErrExternalExecutionLaunchFenceRejected
	}
	var storedKeyHash, storedFingerprint []byte
	var token uuid.UUID
	var leaseActive bool
	var state string
	if err := tx.QueryRow(ctx, `
		SELECT start_state, start_token,
		       start_lease_until > clock_timestamp(),
		       downstream_idempotency_key_hash,
		       downstream_creation_fingerprint
		FROM external_executions
		WHERE caller_service_id = $1 AND external_request_id = $2
		FOR UPDATE
	`, fence.CallerServiceID, fence.ExternalRequestID).Scan(
		&state, &token, &leaseActive, &storedKeyHash, &storedFingerprint,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrExternalExecutionLaunchFenceRejected
		}
		return err
	}
	if state != "launching" || token != fence.LaunchToken || !leaseActive {
		return ErrExternalExecutionLaunchFenceRejected
	}
	if storedKeyHash != nil || storedFingerprint != nil {
		if subtle.ConstantTimeCompare(storedKeyHash, identity.IdempotencyKeyHash) != 1 ||
			subtle.ConstantTimeCompare(storedFingerprint, identity.CreationFingerprint) != 1 {
			return ErrExternalExecutionLaunchFenceRejected
		}
		return nil
	}
	commandTag, err := tx.Exec(ctx, `
		UPDATE external_executions
		SET downstream_idempotency_key_hash = $4,
		    downstream_creation_fingerprint = $5,
		    updated_at = clock_timestamp()
		WHERE caller_service_id = $1
		  AND external_request_id = $2
		  AND start_state = 'launching'
		  AND start_token = $3
		  AND downstream_idempotency_key_hash IS NULL
		  AND downstream_creation_fingerprint IS NULL
	`, fence.CallerServiceID, fence.ExternalRequestID, fence.LaunchToken,
		identity.IdempotencyKeyHash, identity.CreationFingerprint)
	if err != nil {
		return err
	}
	if commandTag.RowsAffected() != 1 {
		return ErrExternalExecutionLaunchFenceRejected
	}
	return nil
}

func verifyWorkflowChildLaunchFence(
	ctx context.Context,
	tx pgx.Tx,
	actorUserID uuid.UUID,
	fence WorkflowChildLaunchFence,
) error {
	var parentUserID uuid.UUID
	var parentStatus string
	if err := tx.QueryRow(ctx, `
		SELECT user_id, status
		FROM workflow_runs
		WHERE id = $1
		FOR UPDATE
	`, fence.WorkflowRunID).Scan(&parentUserID, &parentStatus); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrWorkflowChildLaunchFenceRejected
		}
		return err
	}
	if parentUserID != actorUserID || parentStatus != "running" {
		return ErrWorkflowChildLaunchFenceRejected
	}
	var token uuid.UUID
	var state string
	if err := tx.QueryRow(ctx, `
		SELECT launch_token, state
		FROM workflow_step_launches
		WHERE workflow_run_id = $1 AND workflow_node_id = $2
		FOR UPDATE
	`, fence.WorkflowRunID, fence.WorkflowNodeID).Scan(&token, &state); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrWorkflowChildLaunchFenceRejected
		}
		return err
	}
	if token != fence.LaunchToken || state != "claimed" {
		return ErrWorkflowChildLaunchFenceRejected
	}
	return nil
}
