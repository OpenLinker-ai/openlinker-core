package workflow

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

// The stop reconciler drives workflow_step_stops to stopped/unconfirmed with
// its own short claim, independent of the parent run's execution lease. It
// keeps working after the parent failed, paused or lost its executor. It may
// only cancel the attempt's child, advance the stop record, and run the two
// lease-free derivations (settleFromRuntime, recomputeNode). It never creates
// or attaches children, schedules nodes or writes a run's final state.
const (
	defaultWorkflowStopClaimTTL = 30 * time.Second
	workflowStopScanBatch       = 20
	workflowDeadlineScanBatch   = 50
)

type workflowStopRuntime interface {
	CancelWorkflowChildFenced(context.Context, runtime.WorkflowChildStopFence) error
}

type workflowStopClaim struct {
	runID, nodeID uuid.UUID
	attempt       int32
	childRunID    *uuid.UUID
	state         string
	epoch         int64
}

// ReconcileWorkflowStepStops claims and advances up to batch stop records.
func (s *Service) ReconcileWorkflowStepStops(ctx context.Context, batch int) (int, error) {
	if s == nil || s.pool == nil {
		return 0, nil
	}
	if batch <= 0 {
		batch = workflowStopScanBatch
	}
	rows, err := s.pool.Query(ctx, `
		SELECT workflow_run_id, workflow_node_id, attempt
		FROM workflow_step_stops
		WHERE state IN ('requested', 'stopping')
		  AND (claim_owner IS NULL OR claim_expires_at < clock_timestamp())
		ORDER BY updated_at, workflow_run_id, workflow_node_id, attempt
		LIMIT $1
	`, batch)
	if err != nil {
		return 0, err
	}
	type key struct {
		runID, nodeID uuid.UUID
		attempt       int32
	}
	keys := []key{}
	for rows.Next() {
		var k key
		if err := rows.Scan(&k.runID, &k.nodeID, &k.attempt); err != nil {
			rows.Close()
			return 0, err
		}
		keys = append(keys, k)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	handled := 0
	for _, k := range keys {
		claim, ok, err := s.claimWorkflowStepStop(ctx, k.runID, k.nodeID, k.attempt)
		if err != nil {
			return handled, err
		}
		if !ok {
			continue
		}
		if err := s.driveWorkflowStepStop(ctx, claim); err != nil {
			log.Warn().Err(err).Str("workflow_run_id", k.runID.String()).Str("workflow_node_id", k.nodeID.String()).
				Int32("attempt", k.attempt).Msg("workflow: stop reconcile")
		}
		handled++
	}
	return handled, nil
}

// reconcileWorkflowRunStops drives the open stop records of one run, best
// effort and bounded. It is the stop reconciler's own logic (stop claim, not
// the execution lease), invoked right after a run recorded new stops.
func (s *Service) reconcileWorkflowRunStops(ctx context.Context, runID uuid.UUID) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	rows, err := s.pool.Query(ctx, `
		SELECT workflow_node_id, attempt FROM workflow_step_stops
		WHERE workflow_run_id = $1 AND state IN ('requested', 'stopping')
		ORDER BY workflow_node_id, attempt
	`, runID)
	if err != nil {
		return
	}
	type key struct {
		nodeID  uuid.UUID
		attempt int32
	}
	keys := []key{}
	for rows.Next() {
		var k key
		if rows.Scan(&k.nodeID, &k.attempt) == nil {
			keys = append(keys, k)
		}
	}
	rows.Close()
	for _, k := range keys {
		claim, ok, err := s.claimWorkflowStepStop(ctx, runID, k.nodeID, k.attempt)
		if err != nil || !ok {
			continue
		}
		if err := s.driveWorkflowStepStop(ctx, claim); err != nil {
			log.Warn().Err(err).Str("workflow_run_id", runID.String()).Msg("workflow: immediate stop reconcile")
		}
	}
}

func (s *Service) claimWorkflowStepStop(ctx context.Context, runID, nodeID uuid.UUID, attempt int32) (workflowStopClaim, bool, error) {
	claim := workflowStopClaim{runID: runID, nodeID: nodeID, attempt: attempt}
	ok := false
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := lockWorkflowRunForTransition(ctx, tx, runID); err != nil {
			return err
		}
		var locked int
		if err := tx.QueryRow(ctx, `
			SELECT 1 FROM workflow_step_stops
			WHERE workflow_run_id = $1 AND workflow_node_id = $2 AND attempt = $3
			FOR UPDATE
		`, runID, nodeID, attempt).Scan(&locked); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil
			}
			return err
		}
		err := tx.QueryRow(ctx, `
			UPDATE workflow_step_stops
			SET claim_owner = $4, claim_epoch = claim_epoch + 1,
			    claim_expires_at = clock_timestamp() + make_interval(secs => $5),
			    updated_at = clock_timestamp()
			WHERE workflow_run_id = $1 AND workflow_node_id = $2 AND attempt = $3
			  AND state IN ('requested', 'stopping')
			  AND (claim_owner IS NULL OR claim_expires_at < clock_timestamp())
			RETURNING child_run_id, state, claim_epoch
		`, runID, nodeID, attempt, s.instanceID, defaultWorkflowStopClaimTTL.Seconds()).Scan(&claim.childRunID, &claim.state, &claim.epoch)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		ok = true
		return nil
	})
	return claim, ok, err
}

func (s *Service) driveWorkflowStepStop(ctx context.Context, claim workflowStopClaim) error {
	if claim.childRunID == nil {
		return s.finishWorkflowStepStop(ctx, claim, "stopped")
	}
	if claim.state == "requested" {
		stopper, ok := s.runtime.(workflowStopRuntime)
		if !ok {
			return errors.New("workflow child stop requires the Runtime service")
		}
		err := stopper.CancelWorkflowChildFenced(ctx, runtime.WorkflowChildStopFence{
			WorkflowRunID:  claim.runID,
			WorkflowNodeID: claim.nodeID,
			Attempt:        claim.attempt,
			ChildRunID:     *claim.childRunID,
			ClaimOwner:     s.instanceID,
			ClaimEpoch:     claim.epoch,
		})
		switch {
		case err == nil, errors.Is(err, runtime.ErrRuntimeCancellationRunEnded):
			// Cancelled now, or it had already ended: settle from evidence.
		case errors.Is(err, runtime.ErrWorkflowChildStopFenceRejected):
			return nil // the claim was lost; its new holder continues
		default:
			s.releaseWorkflowStepStopClaim(ctx, claim)
			return err
		}
	}
	evidence, err := runtime.ClassifyWorkflowChildStop(ctx, s.pool, *claim.childRunID)
	if err != nil {
		s.releaseWorkflowStepStopClaim(ctx, claim)
		return err
	}
	if evidence.State == runtime.WorkflowChildStopInProgress {
		s.releaseWorkflowStepStopClaim(ctx, claim)
		return nil
	}
	next := "stopped"
	if evidence.CancellationUnconfirmed {
		next = "unconfirmed"
	}
	return s.finishWorkflowStepStop(ctx, claim, next)
}

// workflowStepStopFenced locks parent -> stop record and checks the claim
// with a fresh clock before fn runs.
func (s *Service) workflowStepStopFenced(ctx context.Context, claim workflowStopClaim, fn func(pgx.Tx) error) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := lockWorkflowRunForTransition(ctx, tx, claim.runID); err != nil {
			return err
		}
		var locked int
		if err := tx.QueryRow(ctx, `
			SELECT 1 FROM workflow_step_stops
			WHERE workflow_run_id = $1 AND workflow_node_id = $2 AND attempt = $3
			FOR UPDATE
		`, claim.runID, claim.nodeID, claim.attempt).Scan(&locked); err != nil {
			return err
		}
		var held bool
		if err := tx.QueryRow(ctx, `
			SELECT COALESCE(claim_owner = $4 AND claim_epoch = $5 AND claim_expires_at > clock_timestamp(), false)
			FROM workflow_step_stops
			WHERE workflow_run_id = $1 AND workflow_node_id = $2 AND attempt = $3
		`, claim.runID, claim.nodeID, claim.attempt, s.instanceID, claim.epoch).Scan(&held); err != nil {
			return err
		}
		if !held {
			return runtime.ErrWorkflowChildStopFenceRejected
		}
		return fn(tx)
	})
}

func (s *Service) finishWorkflowStepStop(ctx context.Context, claim workflowStopClaim, next string) error {
	err := s.workflowStepStopFenced(ctx, claim, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			UPDATE workflow_step_stops
			SET state = $4,
			    applied_at = COALESCE(applied_at, clock_timestamp()),
			    finished_at = clock_timestamp(),
			    claim_owner = NULL, claim_expires_at = NULL,
			    updated_at = clock_timestamp()
			WHERE workflow_run_id = $1 AND workflow_node_id = $2 AND attempt = $3
			  AND state IN ('requested', 'stopping')
		`, claim.runID, claim.nodeID, claim.attempt, next); err != nil {
			return err
		}
		return s.recomputeNode(ctx, tx, claim.runID, claim.nodeID)
	})
	if errors.Is(err, runtime.ErrWorkflowChildStopFenceRejected) {
		return nil
	}
	return err
}

func (s *Service) releaseWorkflowStepStopClaim(ctx context.Context, claim workflowStopClaim) {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), workflowStepEvidenceWriteTimeout)
	defer cancel()
	err := s.workflowStepStopFenced(releaseCtx, claim, func(tx pgx.Tx) error {
		_, err := tx.Exec(releaseCtx, `
			UPDATE workflow_step_stops
			SET claim_owner = NULL, claim_expires_at = NULL, updated_at = clock_timestamp()
			WHERE workflow_run_id = $1 AND workflow_node_id = $2 AND attempt = $3
		`, claim.runID, claim.nodeID, claim.attempt)
		return err
	})
	if err != nil && !errors.Is(err, runtime.ErrWorkflowChildStopFenceRejected) {
		log.Warn().Err(err).Str("workflow_run_id", claim.runID.String()).Msg("workflow: release stop claim")
	}
}

// SettleExpiredWorkflowAttempts enforces node deadlines independently of any
// executor: every running attempt (and any pending attempt that already has
// a deadline) past deadline_at is settled from Runtime facts, even when its
// run is paused or has no lease. Legacy unfinished rows are never touched.
func (s *Service) SettleExpiredWorkflowAttempts(ctx context.Context, batch int) (int, error) {
	if s == nil || s.pool == nil {
		return 0, nil
	}
	if batch <= 0 {
		batch = workflowDeadlineScanBatch
	}
	rows, err := s.pool.Query(ctx, `
		SELECT workflow_run_id, workflow_node_id, attempt
		FROM workflow_run_steps
		WHERE status IN ('pending', 'running')
		  AND deadline_at IS NOT NULL
		  AND deadline_at <= clock_timestamp()
		  AND NOT legacy_unfinished
		ORDER BY deadline_at
		LIMIT $1
	`, batch)
	if err != nil {
		return 0, err
	}
	type key struct {
		runID, nodeID uuid.UUID
		attempt       int32
	}
	keys := []key{}
	for rows.Next() {
		var k key
		if err := rows.Scan(&k.runID, &k.nodeID, &k.attempt); err != nil {
			rows.Close()
			return 0, err
		}
		keys = append(keys, k)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	settled := 0
	for _, k := range keys {
		status, err := s.settleFromRuntime(ctx, k.runID, k.nodeID, k.attempt)
		if err != nil {
			return settled, err
		}
		if status != attemptRunning && status != attemptPending {
			settled++
		}
	}
	return settled, nil
}
