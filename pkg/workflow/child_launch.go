package workflow

import (
	"context"
	"crypto/subtle"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

type workflowLaunchRuntime interface {
	PrepareRunCreationIdentity(*runtime.RunRequest, string) (runtime.RunCreationIdentity, error)
	RunWorkflowChild(context.Context, uuid.UUID, *runtime.RunRequest, string, runtime.WorkflowChildLaunchFence) (*runtime.RunResponse, error)
}

// claimWorkflowChildLaunch claims (or re-claims) the launch of one attempt.
// When the launch already created its child, the child Run ID is returned and
// the caller attaches it instead of creating again.
func (s *Service) claimWorkflowChildLaunch(
	ctx context.Context,
	run dbWorkflowRunIdentity,
	nodeID, stepID uuid.UUID,
	nodeKey string,
	attempt int32,
	identity runtime.RunCreationIdentity,
) (runtime.WorkflowChildLaunchFence, uuid.UUID, error) {
	if s == nil || s.pool == nil || run.ID == uuid.Nil || run.UserID == uuid.Nil ||
		nodeID == uuid.Nil || stepID == uuid.Nil || attempt < 1 || len(identity.IdempotencyKeyHash) != 32 ||
		len(identity.CreationFingerprint) != 32 {
		return runtime.WorkflowChildLaunchFence{}, uuid.Nil, errors.New("workflow child launch identity is invalid")
	}
	var fence runtime.WorkflowChildLaunchFence
	var created uuid.UUID
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var ownerID uuid.UUID
		var status string
		if err := tx.QueryRow(ctx, `
			SELECT user_id, status FROM workflow_runs WHERE id = $1 FOR UPDATE
		`, run.ID).Scan(&ownerID, &status); err != nil {
			return err
		}
		if ownerID != run.UserID || status != workflowRunStatusRunning {
			return runtime.ErrWorkflowChildLaunchFenceRejected
		}
		if err := s.lockWorkflowRunLease(ctx, tx, run.ID); err != nil {
			return err
		}
		lease, _ := workflowRunLeaseFrom(ctx)
		var token uuid.UUID
		var keyHash, fingerprint []byte
		var state string
		var childRunID *uuid.UUID
		err := tx.QueryRow(ctx, `
			SELECT launch_token, idempotency_key_hash, creation_fingerprint, state, run_id
			FROM workflow_step_launches
			WHERE workflow_run_id = $1 AND workflow_node_id = $2 AND attempt = $3
			FOR UPDATE
		`, run.ID, nodeID, attempt).Scan(&token, &keyHash, &fingerprint, &state, &childRunID)
		if err == nil {
			if subtle.ConstantTimeCompare(keyHash, identity.IdempotencyKeyHash) != 1 ||
				subtle.ConstantTimeCompare(fingerprint, identity.CreationFingerprint) != 1 ||
				state == "invalidated" {
				return runtime.ErrWorkflowChildLaunchFenceRejected
			}
			if _, err := tx.Exec(ctx, `
				UPDATE workflow_step_launches
				SET workflow_run_step_id = $3, updated_at = clock_timestamp()
				WHERE workflow_run_id = $1 AND workflow_node_id = $2 AND attempt = $4
			`, run.ID, nodeID, stepID, attempt); err != nil {
				return err
			}
			fence = workflowChildFence(run.ID, nodeID, attempt, token, lease)
			if childRunID != nil && (state == "created" || state == "attached") {
				created = *childRunID
			}
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		token = uuid.New()
		if _, err := tx.Exec(ctx, `
			INSERT INTO workflow_step_launches (
			    workflow_run_id, workflow_node_id, workflow_run_step_id, node_key,
			    launch_token, idempotency_key_hash, creation_fingerprint, state, attempt
			) VALUES ($1, $2, $3, $4, $5, $6, $7, 'claimed', $8)
		`, run.ID, nodeID, stepID, nodeKey, token,
			identity.IdempotencyKeyHash, identity.CreationFingerprint, attempt); err != nil {
			return err
		}
		fence = workflowChildFence(run.ID, nodeID, attempt, token, lease)
		return nil
	})
	return fence, created, err
}

// dbWorkflowRunIdentity avoids coupling the launch helper to generated model
// details beyond the immutable parent identity.
type dbWorkflowRunIdentity struct {
	ID     uuid.UUID
	UserID uuid.UUID
}

func (s *Service) attachWorkflowChildLaunch(
	ctx context.Context,
	fence runtime.WorkflowChildLaunchFence,
	stepID, childRunID uuid.UUID,
) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.lockWorkflowRunLease(ctx, tx, fence.WorkflowRunID); err != nil {
			return err
		}
		var token uuid.UUID
		var state string
		var storedRunID *uuid.UUID
		if err := tx.QueryRow(ctx, `
			SELECT launch_token, state, run_id
			FROM workflow_step_launches
			WHERE workflow_run_id = $1 AND workflow_node_id = $2 AND attempt = $3
			FOR UPDATE
		`, fence.WorkflowRunID, fence.WorkflowNodeID, fence.Attempt).Scan(&token, &state, &storedRunID); err != nil {
			return err
		}
		if token != fence.LaunchToken || storedRunID == nil || *storedRunID != childRunID ||
			(state != "created" && state != "attached") {
			return runtime.ErrWorkflowChildLaunchFenceRejected
		}
		commandTag, err := tx.Exec(ctx, `
			UPDATE workflow_run_steps
			SET run_id = $2, updated_at = clock_timestamp()
			WHERE id = $1 AND workflow_run_id = $3
			  AND status = 'running'
			  AND (run_id IS NULL OR run_id = $2)
		`, stepID, childRunID, fence.WorkflowRunID)
		if err != nil {
			return err
		}
		if commandTag.RowsAffected() != 1 {
			return runtime.ErrWorkflowChildLaunchFenceRejected
		}
		_, err = tx.Exec(ctx, `
			UPDATE workflow_step_launches
			SET state = 'attached', workflow_run_step_id = $3, updated_at = clock_timestamp()
			WHERE workflow_run_id = $1 AND workflow_node_id = $2 AND attempt = $4
		`, fence.WorkflowRunID, fence.WorkflowNodeID, stepID, fence.Attempt)
		return err
	})
}

// workflowChildFence binds a child Run creation to the launch row and to the
// executor's run lease, which Runtime checks again inside its own transaction.
func workflowChildFence(runID, nodeID uuid.UUID, attempt int32, token uuid.UUID, lease workflowRunLease) runtime.WorkflowChildLaunchFence {
	return runtime.WorkflowChildLaunchFence{
		WorkflowRunID:  runID,
		WorkflowNodeID: nodeID,
		Attempt:        attempt,
		LaunchToken:    token,
		LeaseOwner:     lease.owner,
		LeaseEpoch:     lease.epoch,
	}
}
