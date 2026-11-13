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

// WorkflowChildLaunchFence binds a workflow child Run creation to one launch
// row (run, node, attempt, token) and to the parent run's execution lease
// (owner, epoch). Runtime re-checks both inside its own creation transaction,
// after locking the parent run, using the database clock for lease expiry.
type WorkflowChildLaunchFence struct {
	WorkflowRunID  uuid.UUID
	WorkflowNodeID uuid.UUID
	Attempt        int32
	LaunchToken    uuid.UUID
	LeaseOwner     uuid.UUID
	LeaseEpoch     int64
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
		singleAcceptedExecution: true,
		beforeCreate: func(txCtx context.Context, tx pgx.Tx) error {
			return verifyWorkflowChildLaunchFence(txCtx, tx, actorUserID, fence)
		},
		afterCreate: func(txCtx context.Context, tx pgx.Tx, runID uuid.UUID) error {
			commandTag, err := tx.Exec(txCtx, `
				UPDATE workflow_step_launches
				SET state = 'created', run_id = $4, updated_at = clock_timestamp()
				WHERE workflow_run_id = $1
				  AND workflow_node_id = $2
				  AND attempt = $5
				  AND launch_token = $3
				  AND state = 'claimed'
				  AND run_id IS NULL
			`, fence.WorkflowRunID, fence.WorkflowNodeID, fence.LaunchToken, runID, fence.Attempt)
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
	if fence.LeaseOwner == uuid.Nil || fence.LeaseEpoch < 1 || fence.Attempt < 1 {
		return ErrWorkflowChildLaunchFenceRejected
	}
	// Lock the parent first, then evaluate the lease with a fresh clock: a
	// single SELECT ... FOR UPDATE computes clock_timestamp() before its lock
	// wait ends and would accept a lease that expired during the wait.
	var locked int
	if err := tx.QueryRow(ctx, `
		SELECT 1 FROM workflow_runs WHERE id = $1 FOR UPDATE
	`, fence.WorkflowRunID).Scan(&locked); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrWorkflowChildLaunchFenceRejected
		}
		return err
	}
	var parentUserID uuid.UUID
	var parentStatus string
	var leaseHeld bool
	if err := tx.QueryRow(ctx, `
		SELECT user_id, status,
		       COALESCE(lease_owner = $2 AND lease_epoch = $3 AND lease_expires_at > clock_timestamp(), false)
		FROM workflow_runs
		WHERE id = $1
	`, fence.WorkflowRunID, fence.LeaseOwner, fence.LeaseEpoch).Scan(&parentUserID, &parentStatus, &leaseHeld); err != nil {
		return err
	}
	if parentUserID != actorUserID || parentStatus != "running" || !leaseHeld {
		return ErrWorkflowChildLaunchFenceRejected
	}
	var token uuid.UUID
	var state string
	var stopRequested bool
	if err := tx.QueryRow(ctx, `
		SELECT l.launch_token, l.state,
		       EXISTS (
		           SELECT 1 FROM workflow_step_stops s
		           WHERE s.workflow_run_id = l.workflow_run_id
		             AND s.workflow_node_id = l.workflow_node_id
		             AND s.attempt = l.attempt
		       )
		FROM workflow_step_launches l
		WHERE l.workflow_run_id = $1 AND l.workflow_node_id = $2 AND l.attempt = $3
		FOR UPDATE OF l
	`, fence.WorkflowRunID, fence.WorkflowNodeID, fence.Attempt).Scan(&token, &state, &stopRequested); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrWorkflowChildLaunchFenceRejected
		}
		return err
	}
	if token != fence.LaunchToken || state != "claimed" || stopRequested {
		return ErrWorkflowChildLaunchFenceRejected
	}
	// The attempt must still be running and inside its deadline, evaluated
	// now that the launch row is locked: a creation that arrives after the
	// attempt timed out (or was canceled) is void.
	var attemptLive bool
	if err := tx.QueryRow(ctx, `
		SELECT status = 'running' AND (deadline_at IS NULL OR clock_timestamp() < deadline_at)
		FROM workflow_run_steps
		WHERE workflow_run_id = $1 AND workflow_node_id = $2 AND attempt = $3
	`, fence.WorkflowRunID, fence.WorkflowNodeID, fence.Attempt).Scan(&attemptLive); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrWorkflowChildLaunchFenceRejected
		}
		return err
	}
	if !attemptLive {
		return ErrWorkflowChildLaunchFenceRejected
	}
	return nil
}

// WorkflowChildStopFence authorizes cancelling one workflow attempt's child
// Run. It is the stop record's own claim, never the parent's execution lease:
// only the stop reconciler holding the claim may cancel the child.
type WorkflowChildStopFence struct {
	WorkflowRunID  uuid.UUID
	WorkflowNodeID uuid.UUID
	Attempt        int32
	ChildRunID     uuid.UUID
	ClaimOwner     uuid.UUID
	ClaimEpoch     int64
}

// ErrWorkflowChildStopFenceRejected means the stop claim is no longer held
// (expired, taken over) or the record no longer asks for this child to stop.
var ErrWorkflowChildStopFenceRejected = errors.New("workflow child stop fence rejected")

// CancelWorkflowChildFenced cancels a workflow child Run in Runtime's own
// cancellation transaction after locking parent -> launch -> stop record and
// checking the stop claim with the database clock. The stop record moves to
// stopping in the same transaction. A child that already ended returns
// ErrRuntimeCancellationRunEnded; the caller then settles from evidence.
func (s *Service) CancelWorkflowChildFenced(ctx context.Context, fence WorkflowChildStopFence) error {
	if s == nil || s.cancellation == nil || s.pool == nil {
		return errors.New("Runtime cancellation is not configured")
	}
	if fence.WorkflowRunID == uuid.Nil || fence.WorkflowNodeID == uuid.Nil || fence.Attempt < 1 ||
		fence.ChildRunID == uuid.Nil || fence.ClaimOwner == uuid.Nil || fence.ClaimEpoch < 1 {
		return ErrWorkflowChildStopFenceRejected
	}
	var ownerID uuid.UUID
	if err := s.pool.QueryRow(ctx, `SELECT user_id FROM workflow_runs WHERE id = $1`, fence.WorkflowRunID).Scan(&ownerID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrWorkflowChildStopFenceRejected
		}
		return err
	}
	result, err := s.cancellation.cancelOwnedRun(ctx, ownerID, fence.ChildRunID, "workflow attempt stopped",
		func(txCtx context.Context, tx pgx.Tx) error {
			return applyWorkflowChildStopFence(txCtx, tx, fence)
		})
	if err != nil {
		return err
	}
	if s.browserObservation != nil && result.Cancellation.TargetAttemptID != nil {
		s.browserObservation.closeAttemptAsync(fence.ChildRunID, *result.Cancellation.TargetAttemptID, "run_browser_closed")
	}
	return nil
}

func applyWorkflowChildStopFence(ctx context.Context, tx pgx.Tx, fence WorkflowChildStopFence) error {
	var locked int
	if err := tx.QueryRow(ctx, `SELECT 1 FROM workflow_runs WHERE id = $1 FOR UPDATE`, fence.WorkflowRunID).Scan(&locked); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrWorkflowChildStopFenceRejected
		}
		return err
	}
	var launchRunID *uuid.UUID
	if err := tx.QueryRow(ctx, `
		SELECT run_id FROM workflow_step_launches
		WHERE workflow_run_id = $1 AND workflow_node_id = $2 AND attempt = $3
		FOR UPDATE
	`, fence.WorkflowRunID, fence.WorkflowNodeID, fence.Attempt).Scan(&launchRunID); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if launchRunID != nil && *launchRunID != fence.ChildRunID {
		return ErrWorkflowChildStopFenceRejected
	}
	if err := tx.QueryRow(ctx, `
		SELECT 1 FROM workflow_step_stops
		WHERE workflow_run_id = $1 AND workflow_node_id = $2 AND attempt = $3
		FOR UPDATE
	`, fence.WorkflowRunID, fence.WorkflowNodeID, fence.Attempt).Scan(&locked); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrWorkflowChildStopFenceRejected
		}
		return err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE workflow_step_stops
		SET state = 'stopping',
		    applied_at = COALESCE(applied_at, clock_timestamp()),
		    updated_at = clock_timestamp()
		WHERE workflow_run_id = $1 AND workflow_node_id = $2 AND attempt = $3
		  AND child_run_id = $4
		  AND claim_owner = $5 AND claim_epoch = $6
		  AND claim_expires_at > clock_timestamp()
		  AND state IN ('requested', 'stopping')
	`, fence.WorkflowRunID, fence.WorkflowNodeID, fence.Attempt, fence.ChildRunID, fence.ClaimOwner, fence.ClaimEpoch)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrWorkflowChildStopFenceRejected
	}
	// Workflow run observers follow the stop record through the run revision.
	if _, err := tx.Exec(ctx, `UPDATE workflow_runs SET revision = revision + 1 WHERE id = $1`, fence.WorkflowRunID); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `SELECT pg_notify('workflow_run.changed', $1)`, fence.WorkflowRunID.String())
	return err
}
