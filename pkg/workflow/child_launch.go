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

func (s *Service) claimWorkflowChildLaunch(
	ctx context.Context,
	run dbWorkflowRunIdentity,
	nodeID, stepID uuid.UUID,
	nodeKey string,
	identity runtime.RunCreationIdentity,
) (runtime.WorkflowChildLaunchFence, error) {
	if s == nil || s.pool == nil || run.ID == uuid.Nil || run.UserID == uuid.Nil ||
		nodeID == uuid.Nil || stepID == uuid.Nil || len(identity.IdempotencyKeyHash) != 32 ||
		len(identity.CreationFingerprint) != 32 {
		return runtime.WorkflowChildLaunchFence{}, errors.New("workflow child launch identity is invalid")
	}
	var fence runtime.WorkflowChildLaunchFence
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
		var token uuid.UUID
		var keyHash, fingerprint []byte
		var state string
		var childRunID *uuid.UUID
		err := tx.QueryRow(ctx, `
			SELECT launch_token, idempotency_key_hash, creation_fingerprint, state, run_id
			FROM workflow_step_launches
			WHERE workflow_run_id = $1 AND workflow_node_id = $2
			FOR UPDATE
		`, run.ID, nodeID).Scan(&token, &keyHash, &fingerprint, &state, &childRunID)
		if err == nil {
			if subtle.ConstantTimeCompare(keyHash, identity.IdempotencyKeyHash) != 1 ||
				subtle.ConstantTimeCompare(fingerprint, identity.CreationFingerprint) != 1 ||
				state == "invalidated" {
				return runtime.ErrWorkflowChildLaunchFenceRejected
			}
			if _, err := tx.Exec(ctx, `
				UPDATE workflow_step_launches
				SET workflow_run_step_id = $3, updated_at = clock_timestamp()
				WHERE workflow_run_id = $1 AND workflow_node_id = $2
			`, run.ID, nodeID, stepID); err != nil {
				return err
			}
			fence = runtime.WorkflowChildLaunchFence{
				WorkflowRunID: run.ID, WorkflowNodeID: nodeID, LaunchToken: token,
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
			    launch_token, idempotency_key_hash, creation_fingerprint, state
			) VALUES ($1, $2, $3, $4, $5, $6, $7, 'claimed')
		`, run.ID, nodeID, stepID, nodeKey, token,
			identity.IdempotencyKeyHash, identity.CreationFingerprint); err != nil {
			return err
		}
		fence = runtime.WorkflowChildLaunchFence{
			WorkflowRunID: run.ID, WorkflowNodeID: nodeID, LaunchToken: token,
		}
		return nil
	})
	return fence, err
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
		var status string
		if err := tx.QueryRow(ctx, `
			SELECT status FROM workflow_runs WHERE id = $1 FOR UPDATE
		`, fence.WorkflowRunID).Scan(&status); err != nil {
			return err
		}
		var token uuid.UUID
		var state string
		var storedRunID *uuid.UUID
		if err := tx.QueryRow(ctx, `
			SELECT launch_token, state, run_id
			FROM workflow_step_launches
			WHERE workflow_run_id = $1 AND workflow_node_id = $2
			FOR UPDATE
		`, fence.WorkflowRunID, fence.WorkflowNodeID).Scan(&token, &state, &storedRunID); err != nil {
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
			WHERE workflow_run_id = $1 AND workflow_node_id = $2
		`, fence.WorkflowRunID, fence.WorkflowNodeID, stepID)
		return err
	})
}
