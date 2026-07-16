package workflow

import (
	"context"
	"crypto/sha256"
	"errors"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

var workflowCancellationIDNamespace = uuid.MustParse("5845a2c3-f2a8-5a52-af7f-798c1fca258b")

type CancellationEvidence struct {
	CancellationID uuid.UUID
	WorkflowRunID  uuid.UUID
	ActorUserID    uuid.UUID
	ReasonCode     string
	State          string
	ErrorCode      string
	RequestedAt    time.Time
	AppliedAt      *time.Time
	FinishedAt     *time.Time
}

type workflowCancellationRuntime interface {
	CancelRun(context.Context, uuid.UUID, uuid.UUID) (*runtime.RunResponse, error)
	GetRunCancellationEvidence(context.Context, uuid.UUID, uuid.UUID) (runtime.RunCancellationEvidence, error)
}

func deterministicWorkflowCancellationID(workflowRunID uuid.UUID) uuid.UUID {
	return uuid.NewHash(sha256.New(), workflowCancellationIDNamespace, []byte(workflowRunID.String()), 5)
}

// CancelExternalWorkflowRun persists first-writer intent, prevents all new
// child launches, then reconciles physical child stop evidence.
func (s *Service) CancelExternalWorkflowRun(
	ctx context.Context,
	actorUserID, workflowRunID uuid.UUID,
	reasonCode string,
) (*WorkflowRunResponse, CancellationEvidence, error) {
	if reasonCode != "CALLER_REQUESTED" && reasonCode != "DEADLINE_EXCEEDED" {
		return nil, CancellationEvidence{}, httpx.BadRequest("workflow cancellation reason 无效")
	}
	evidence, err := s.requestWorkflowCancellation(ctx, actorUserID, workflowRunID, reasonCode)
	if err != nil {
		return nil, CancellationEvidence{}, err
	}
	if evidence.State == "requested" || evidence.State == "stopping" {
		if err := s.reconcileWorkflowCancellation(ctx, actorUserID, workflowRunID); err != nil {
			return nil, evidence, err
		}
		evidence, err = s.GetWorkflowCancellationEvidence(ctx, actorUserID, workflowRunID)
		if err != nil {
			return nil, CancellationEvidence{}, err
		}
	}
	resp, err := s.GetWorkflowRun(ctx, actorUserID, workflowRunID)
	return resp, evidence, err
}

func (s *Service) GetWorkflowCancellationEvidence(
	ctx context.Context,
	actorUserID, workflowRunID uuid.UUID,
) (CancellationEvidence, error) {
	if s == nil || s.pool == nil || actorUserID == uuid.Nil || workflowRunID == uuid.Nil {
		return CancellationEvidence{}, httpx.BadRequest("workflow cancellation identity 无效")
	}
	var evidence CancellationEvidence
	var errorCode *string
	err := s.pool.QueryRow(ctx, `
		SELECT id, workflow_run_id, actor_user_id, reason_code, state, error_code,
		       requested_at, applied_at, finished_at
		FROM workflow_run_cancellations
		WHERE workflow_run_id = $1
	`, workflowRunID).Scan(
		&evidence.CancellationID, &evidence.WorkflowRunID, &evidence.ActorUserID,
		&evidence.ReasonCode, &evidence.State, &errorCode, &evidence.RequestedAt,
		&evidence.AppliedAt, &evidence.FinishedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && evidence.ActorUserID != actorUserID) {
		return CancellationEvidence{}, httpx.NotFound("workflow cancellation 不存在")
	}
	if err != nil {
		return CancellationEvidence{}, err
	}
	if errorCode != nil {
		evidence.ErrorCode = *errorCode
	}
	return evidence, nil
}

func (s *Service) requestWorkflowCancellation(
	ctx context.Context,
	actorUserID, workflowRunID uuid.UUID,
	reasonCode string,
) (CancellationEvidence, error) {
	if s == nil || s.pool == nil || actorUserID == uuid.Nil || workflowRunID == uuid.Nil {
		return CancellationEvidence{}, httpx.BadRequest("workflow cancellation identity 无效")
	}
	var evidence CancellationEvidence
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var ownerID uuid.UUID
		var status string
		if err := tx.QueryRow(ctx, `
			SELECT user_id, status FROM workflow_runs WHERE id = $1 FOR UPDATE
		`, workflowRunID).Scan(&ownerID, &status); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return httpx.NotFound("workflow_run 不存在")
			}
			return err
		}
		if ownerID != actorUserID {
			return httpx.NotFound("workflow_run 不存在")
		}
		if existing, err := scanWorkflowCancellation(tx.QueryRow(ctx, `
			SELECT id, workflow_run_id, actor_user_id, reason_code, state, error_code,
			       requested_at, applied_at, finished_at
			FROM workflow_run_cancellations
			WHERE workflow_run_id = $1
			FOR UPDATE
		`, workflowRunID)); err == nil {
			evidence = existing
			return nil
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}

		state := "requested"
		if status == workflowRunStatusSuccess || status == workflowRunStatusFailed {
			state = "not_applied"
		} else if status != workflowRunStatusPending && status != workflowRunStatusRunning &&
			status != workflowRunStatusPaused && status != workflowRunStatusCanceled {
			return httpx.Conflict("workflow_run 状态不可取消")
		}
		if state == "requested" {
			if _, err := tx.Exec(ctx, `
				UPDATE workflow_runs
				SET status = 'canceled', claimed_at = NULL, next_retry_at = NULL,
				    error_message = COALESCE(error_message, 'workflow run canceled'),
				    finished_at = COALESCE(finished_at, clock_timestamp()),
				    updated_at = clock_timestamp()
				WHERE id = $1
			`, workflowRunID); err != nil {
				return err
			}
			// Parent row is already locked. Every child creation locks it before
			// its launch row, so invalidation and create are mutually exclusive.
			if _, err := tx.Exec(ctx, `
				UPDATE workflow_step_launches
				SET state = 'invalidated', updated_at = clock_timestamp()
				WHERE workflow_run_id = $1 AND state = 'claimed' AND run_id IS NULL
			`, workflowRunID); err != nil {
				return err
			}
		}
		row := tx.QueryRow(ctx, `
			INSERT INTO workflow_run_cancellations (
			    workflow_run_id, id, actor_user_id, reason_code, state,
			    applied_at, finished_at
			) VALUES (
			    $1, $2, $3, $4, $5,
			    CASE WHEN $5 = 'not_applied' THEN clock_timestamp() ELSE NULL END,
			    CASE WHEN $5 = 'not_applied' THEN clock_timestamp() ELSE NULL END
			)
			RETURNING id, workflow_run_id, actor_user_id, reason_code, state, error_code,
			          requested_at, applied_at, finished_at
		`, workflowRunID, deterministicWorkflowCancellationID(workflowRunID), actorUserID, reasonCode, state)
		var err error
		evidence, err = scanWorkflowCancellation(row)
		return err
	})
	return evidence, err
}

func (s *Service) reconcileWorkflowCancellation(
	ctx context.Context,
	actorUserID, workflowRunID uuid.UUID,
) error {
	if s == nil || s.pool == nil {
		return errors.New("workflow cancellation store is not configured")
	}
	// First restore create-commit-before-attach evidence. created.run_id was
	// written in the child Run transaction and is authoritative.
	if err := s.attachRecoveredWorkflowChildren(ctx, workflowRunID); err != nil {
		return err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT run_id
		FROM (
		    SELECT run_id FROM workflow_step_launches
		    WHERE workflow_run_id = $1 AND run_id IS NOT NULL
		    UNION ALL
		    SELECT run_id FROM workflow_run_steps
		    WHERE workflow_run_id = $1 AND run_id IS NOT NULL
		) children
		ORDER BY run_id
	`, workflowRunID)
	if err != nil {
		return err
	}
	childIDs := make([]uuid.UUID, 0)
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		childIDs = append(childIDs, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if len(childIDs) == 0 {
		return s.advanceWorkflowCancellation(ctx, workflowRunID, "stopped")
	}
	runtimeSvc, ok := s.runtime.(workflowCancellationRuntime)
	if !ok {
		return errors.New("workflow Runtime physical cancellation is not configured")
	}
	sort.Slice(childIDs, func(i, j int) bool { return childIDs[i].String() < childIDs[j].String() })

	anyStopping := false
	anyUnconfirmed := false
	for _, childID := range childIDs {
		run, getErr := s.runtime.GetRun(ctx, actorUserID, childID)
		if getErr != nil {
			return getErr
		}
		switch normalizeRunStatus(run.Status) {
		case runtimeRunStatusSuccess, runtimeRunStatusFailed, runtimeRunStatusTimeout:
			continue
		case runtimeRunStatusCanceled:
			// Existing evidence is inspected below.
		default:
			if _, cancelErr := runtimeSvc.CancelRun(ctx, actorUserID, childID); cancelErr != nil {
				// A terminal transition may have won; re-read before treating it as
				// infrastructure failure.
				run, getErr = s.runtime.GetRun(ctx, actorUserID, childID)
				if getErr != nil {
					return cancelErr
				}
				if status := normalizeRunStatus(run.Status); status == runtimeRunStatusSuccess ||
					status == runtimeRunStatusFailed || status == runtimeRunStatusTimeout {
					continue
				}
			}
		}
		evidence, evidenceErr := runtimeSvc.GetRunCancellationEvidence(ctx, actorUserID, childID)
		if evidenceErr != nil {
			return evidenceErr
		}
		switch evidence.State {
		case "stopped":
		case "unconfirmed":
			anyUnconfirmed = true
		default:
			anyStopping = true
		}
	}
	next := "stopped"
	if anyUnconfirmed {
		next = "unconfirmed"
	} else if anyStopping {
		next = "stopping"
	}
	return s.advanceWorkflowCancellation(ctx, workflowRunID, next)
}

func (s *Service) attachRecoveredWorkflowChildren(ctx context.Context, workflowRunID uuid.UUID) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var status string
		if err := tx.QueryRow(ctx, `
			SELECT status FROM workflow_runs WHERE id = $1 FOR UPDATE
		`, workflowRunID).Scan(&status); err != nil {
			return err
		}
		if status != workflowRunStatusCanceled {
			return errors.New("workflow recovery requires canceled parent")
		}
		rows, err := tx.Query(ctx, `
			SELECT workflow_node_id, workflow_run_step_id, run_id
			FROM workflow_step_launches
			WHERE workflow_run_id = $1 AND state = 'created' AND run_id IS NOT NULL
			ORDER BY workflow_node_id
			FOR UPDATE
		`, workflowRunID)
		if err != nil {
			return err
		}
		type recovery struct {
			nodeID uuid.UUID
			stepID *uuid.UUID
			runID  uuid.UUID
		}
		items := []recovery{}
		for rows.Next() {
			var item recovery
			if err := rows.Scan(&item.nodeID, &item.stepID, &item.runID); err != nil {
				rows.Close()
				return err
			}
			items = append(items, item)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, item := range items {
			if item.stepID != nil {
				if _, err := tx.Exec(ctx, `
					UPDATE workflow_run_steps
					SET run_id = $2, updated_at = clock_timestamp()
					WHERE id = $1 AND workflow_run_id = $3
					  AND (run_id IS NULL OR run_id = $2)
				`, *item.stepID, item.runID, workflowRunID); err != nil {
					return err
				}
			}
			if _, err := tx.Exec(ctx, `
				UPDATE workflow_step_launches
				SET state = 'attached', updated_at = clock_timestamp()
				WHERE workflow_run_id = $1 AND workflow_node_id = $2
				  AND state = 'created' AND run_id = $3
			`, workflowRunID, item.nodeID, item.runID); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Service) advanceWorkflowCancellation(ctx context.Context, workflowRunID uuid.UUID, next string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT 1 FROM workflow_runs WHERE id = $1 FOR UPDATE`, workflowRunID); err != nil {
			return err
		}
		var current string
		if err := tx.QueryRow(ctx, `
			SELECT state FROM workflow_run_cancellations
			WHERE workflow_run_id = $1 FOR UPDATE
		`, workflowRunID).Scan(&current); err != nil {
			return err
		}
		if current == next || current == "stopped" || current == "unconfirmed" || current == "not_applied" {
			return nil
		}
		allowed := current == "requested" && (next == "stopping" || next == "stopped" || next == "unconfirmed") ||
			current == "stopping" && (next == "stopped" || next == "unconfirmed")
		if !allowed {
			return errors.New("invalid workflow cancellation transition")
		}
		_, err := tx.Exec(ctx, `
			UPDATE workflow_run_cancellations
			SET state = $2,
			    applied_at = COALESCE(applied_at, clock_timestamp()),
			    finished_at = CASE WHEN $2 IN ('stopped', 'unconfirmed')
			        THEN COALESCE(finished_at, clock_timestamp()) ELSE NULL END,
			    error_code = CASE WHEN $2 = 'unconfirmed' THEN 'CANCEL_UNCONFIRMED' ELSE NULL END,
			    updated_at = clock_timestamp()
			WHERE workflow_run_id = $1
		`, workflowRunID, next)
		return err
	})
}

func scanWorkflowCancellation(row interface{ Scan(...any) error }) (CancellationEvidence, error) {
	var evidence CancellationEvidence
	var errorCode *string
	err := row.Scan(
		&evidence.CancellationID, &evidence.WorkflowRunID, &evidence.ActorUserID,
		&evidence.ReasonCode, &evidence.State, &errorCode, &evidence.RequestedAt,
		&evidence.AppliedAt, &evidence.FinishedAt,
	)
	if errorCode != nil {
		evidence.ErrorCode = *errorCode
	}
	return evidence, err
}
