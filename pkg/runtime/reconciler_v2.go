package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

const maxRuntimeV2ReconcileBatch = 1000

var (
	ErrRuntimeV2ReconcilerNotConfigured = errors.New("runtime v2 deadline reconciler is not configured")
	ErrRuntimeV2ReconcileBatchInvalid   = errors.New("runtime v2 deadline reconcile batch must be between 1 and 1000")
)

// RuntimeV2ReconcileBatchResult reports committed state changes. Scanned is
// bounded by the requested limit; candidates skipped because another worker
// held a SKIP LOCKED row are not counted as reconciled.
type RuntimeV2ReconcileBatchResult struct {
	Scanned      int `json:"scanned"`
	Reconciled   int `json:"reconciled"`
	Requeued     int `json:"requeued"`
	TimedOut     int `json:"timed_out"`
	DeadLettered int `json:"dead_lettered"`
}

// RuntimeV2DeadlineReconciler is a standalone worker boundary. It is not
// wired to coreapi here: callers choose scheduling, cadence, and shutdown.
type RuntimeV2DeadlineReconciler struct {
	pool         *pgxpool.Pool
	retryPlanner ResultRetryPlanner
}

// NewRuntimeV2DeadlineReconciler builds the lease/deadline worker. A nil retry
// planner uses the same bounded retry policy as ResultFinalizer.
func NewRuntimeV2DeadlineReconciler(
	pool *pgxpool.Pool,
	retryPlanner ResultRetryPlanner,
) *RuntimeV2DeadlineReconciler {
	if retryPlanner == nil {
		retryPlanner = fixedResultRetryPlanner{}
	}
	return &RuntimeV2DeadlineReconciler{pool: pool, retryPlanner: retryPlanner}
}

type runtimeV2ReconcileDisposition uint8

const (
	runtimeV2ReconcileSkipped runtimeV2ReconcileDisposition = iota
	runtimeV2ReconcileRequeued
	runtimeV2ReconcileTimedOut
	runtimeV2ReconcileDeadLettered
)

// ReconcileBatch discovers at most limit due Runs with PostgreSQL's clock and
// commits each winner independently. A failed candidate rolls back its entire
// Attempt/capacity/Run/artifact transaction and stops the batch.
func (r *RuntimeV2DeadlineReconciler) ReconcileBatch(
	ctx context.Context,
	limit int,
) (RuntimeV2ReconcileBatchResult, error) {
	if r == nil || r.pool == nil || r.retryPlanner == nil {
		return RuntimeV2ReconcileBatchResult{}, ErrRuntimeV2ReconcilerNotConfigured
	}
	if limit < 1 || limit > maxRuntimeV2ReconcileBatch {
		return RuntimeV2ReconcileBatchResult{}, ErrRuntimeV2ReconcileBatchInvalid
	}

	candidates, err := db.New(r.pool).ListDueRuntimeV2ReconcileCandidates(ctx, int32(limit))
	if err != nil {
		return RuntimeV2ReconcileBatchResult{}, err
	}
	result := RuntimeV2ReconcileBatchResult{Scanned: len(candidates)}
	for _, candidate := range candidates {
		disposition, reconcileErr := r.reconcileCandidate(ctx, candidate)
		if reconcileErr != nil {
			return result, reconcileErr
		}
		switch disposition {
		case runtimeV2ReconcileSkipped:
			continue
		case runtimeV2ReconcileRequeued:
			result.Requeued++
		case runtimeV2ReconcileTimedOut:
			result.TimedOut++
		case runtimeV2ReconcileDeadLettered:
			result.DeadLettered++
		default:
			return result, errors.New("runtime v2 reconciler returned an unknown disposition")
		}
		result.Reconciled++
	}
	return result, nil
}

func (r *RuntimeV2DeadlineReconciler) reconcileCandidate(
	ctx context.Context,
	candidate db.ListDueRuntimeV2ReconcileCandidatesRow,
) (runtimeV2ReconcileDisposition, error) {
	if candidate.RunID == uuid.Nil || candidate.DatabaseNow.IsZero() || candidate.DueAt.IsZero() {
		return runtimeV2ReconcileSkipped, errors.New("runtime v2 reconcile candidate is incomplete")
	}
	if candidate.AttemptID == nil {
		if candidate.ExecutorType != nil || candidate.RuntimeSessionID != nil || candidate.NodeID != nil {
			return runtimeV2ReconcileSkipped, errors.New("runtime v2 deadline-only candidate has Attempt identity")
		}
	} else if candidate.ExecutorType == nil {
		return runtimeV2ReconcileSkipped, errors.New("runtime v2 Attempt candidate has no executor type")
	}

	disposition := runtimeV2ReconcileSkipped
	err := pgx.BeginTxFunc(ctx, r.pool, pgx.TxOptions{
		IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadWrite,
	}, func(tx pgx.Tx) error {
		queries := db.New(tx)
		if candidate.AttemptID == nil {
			locked, lockErr := queries.LockDueRuntimeV2RunWithoutAttempt(ctx, candidate.RunID)
			if errors.Is(lockErr, pgx.ErrNoRows) {
				return nil
			}
			if lockErr != nil {
				return lockErr
			}
			terminal := runtimeV2DeadlineTerminalForRun(locked)
			if terminal == nil {
				return errors.New("locked runtime v2 deadline-only Run is not due")
			}
			if err := finalizeRuntimeV2ReconciledTerminal(ctx, queries, locked, nil, *terminal); err != nil {
				return err
			}
			disposition = runtimeV2ReconcileTimedOut
			return nil
		}

		switch *candidate.ExecutorType {
		case "agent_node":
			if candidate.RuntimeSessionID == nil || candidate.NodeID == nil {
				return errors.New("agent_node reconcile candidate is missing capacity owner")
			}
			lockedSession, lockErr := queries.LockRuntimeSessionForV2Reconcile(ctx, *candidate.RuntimeSessionID)
			if errors.Is(lockErr, pgx.ErrNoRows) {
				return nil
			}
			if lockErr != nil {
				return lockErr
			}
			if lockedSession != *candidate.RuntimeSessionID {
				return errors.New("runtime reconcile Session lock returned a different owner")
			}
			lockedNode, lockErr := queries.LockRuntimeNodeForV2Reconcile(ctx, *candidate.NodeID)
			if errors.Is(lockErr, pgx.ErrNoRows) {
				return nil
			}
			if lockErr != nil {
				return lockErr
			}
			if lockedNode != *candidate.NodeID {
				return errors.New("runtime reconcile Node lock returned a different owner")
			}
		case "core_http", "core_mcp":
			if candidate.RuntimeSessionID != nil || candidate.NodeID != nil {
				return errors.New("Core reconcile candidate unexpectedly owns Agent Node capacity")
			}
		default:
			return fmt.Errorf("runtime reconcile candidate has unknown executor type %q", *candidate.ExecutorType)
		}

		locked, lockErr := queries.LockDueRuntimeV2RunWithAttempt(ctx, db.LockDueRuntimeV2RunWithAttemptParams{
			RunID: candidate.RunID, AttemptID: *candidate.AttemptID,
			ExecutorType:     *candidate.ExecutorType,
			RuntimeSessionID: candidate.RuntimeSessionID, NodeID: candidate.NodeID,
		})
		if errors.Is(lockErr, pgx.ErrNoRows) {
			return nil
		}
		if lockErr != nil {
			return lockErr
		}
		attempt, attemptErr := queries.LockRunAttemptForResult(ctx, db.LockRunAttemptForResultParams{
			RunID: locked.ID, ID: *candidate.AttemptID,
		})
		if attemptErr != nil {
			return attemptErr
		}
		if err := validateRuntimeV2ReconcileAttempt(locked, attempt, candidate); err != nil {
			return err
		}

		switch locked.DispatchState {
		case string(RuntimeDispatchOffered):
			var reconcileErr error
			disposition, reconcileErr = r.reconcileExpiredOffer(ctx, queries, locked, attempt)
			return reconcileErr
		case string(RuntimeDispatchExecuting):
			var reconcileErr error
			disposition, reconcileErr = r.reconcileExpiredExecution(ctx, queries, locked, attempt)
			return reconcileErr
		default:
			return errors.New("runtime v2 Attempt candidate changed to an unsupported dispatch state")
		}
	})
	if err != nil {
		return runtimeV2ReconcileSkipped, err
	}
	return disposition, nil
}

func (r *RuntimeV2DeadlineReconciler) reconcileExpiredOffer(
	ctx context.Context,
	queries *db.Queries,
	locked db.RuntimeV2ReconcileLockedRunRow,
	attempt db.RunAttempt,
) (runtimeV2ReconcileDisposition, error) {
	terminal := runtimeV2OfferTerminalForRun(locked, attempt)
	attemptErrorCode := "RUNTIME_OFFER_EXPIRED"
	if terminal != nil {
		attemptErrorCode = terminal.errorCode
	}
	finished, err := queries.FinishRuntimeV2ReconciledAttempt(ctx, db.FinishRuntimeV2ReconciledAttemptParams{
		Outcome: "offer_expired", ErrorCode: attemptErrorCode,
		RunID: attempt.RunID, AttemptID: attempt.ID,
		LeaseID: attempt.LeaseID, FencingToken: attempt.FencingToken,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return runtimeV2ReconcileSkipped, errors.New("runtime v2 expired offer changed after Run lock")
	}
	if err != nil {
		return runtimeV2ReconcileSkipped, err
	}
	if err = releaseRuntimeAttemptCapacity(ctx, queries, attempt); err != nil {
		return runtimeV2ReconcileSkipped, err
	}
	if terminal != nil {
		if err = finalizeRuntimeV2ReconciledTerminal(ctx, queries, locked, &finished, *terminal); err != nil {
			return runtimeV2ReconcileSkipped, err
		}
		return runtimeV2ReconcileTimedOut, nil
	}

	transitioned, err := queries.ResetRuntimeV2RunAfterReconciledOffer(ctx, db.ResetRuntimeV2RunAfterReconciledOfferParams{
		RunID: attempt.RunID, AttemptID: attempt.ID,
		LeaseID: attempt.LeaseID, FencingToken: attempt.FencingToken,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return reconcileRuntimeV2DeadlineCrossedAfterAttemptFinish(ctx, queries, locked, &finished)
	}
	if err != nil {
		return runtimeV2ReconcileSkipped, err
	}
	if transitioned.DispatchState != string(RuntimeDispatchPending) || transitioned.NextAttemptAt != nil {
		return runtimeV2ReconcileSkipped, errors.New("expired offer did not return its Run to pending")
	}
	if err = createRuntimeV2AvailableSignal(ctx, queries, locked.AgentID, locked.ID, nil); err != nil {
		return runtimeV2ReconcileSkipped, err
	}
	return runtimeV2ReconcileRequeued, nil
}

func (r *RuntimeV2DeadlineReconciler) reconcileExpiredExecution(
	ctx context.Context,
	queries *db.Queries,
	locked db.RuntimeV2ReconcileLockedRunRow,
	attempt db.RunAttempt,
) (runtimeV2ReconcileDisposition, error) {
	terminal := runtimeV2ExecutionTerminalForRun(locked)
	outcome := "lease_expired"
	attemptErrorCode := "LEASE_EXPIRED"
	var retryDelay time.Duration
	if terminal != nil && terminal.errorCode == "RUN_DEADLINE_EXCEEDED" {
		outcome = "timeout"
		attemptErrorCode = terminal.errorCode
	}
	if terminal == nil {
		retryDelay = r.retryPlanner.NextRetryDelay(locked.AttemptCount)
		if retryDelay < time.Millisecond || retryDelay > 60*time.Second {
			return runtimeV2ReconcileSkipped, fmt.Errorf("runtime v2 retry planner returned invalid delay %s", retryDelay)
		}
	}
	finished, err := queries.FinishRuntimeV2ReconciledAttempt(ctx, db.FinishRuntimeV2ReconciledAttemptParams{
		Outcome: outcome, ErrorCode: attemptErrorCode,
		RunID: attempt.RunID, AttemptID: attempt.ID,
		LeaseID: attempt.LeaseID, FencingToken: attempt.FencingToken,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return runtimeV2ReconcileSkipped, errors.New("runtime v2 expired execution changed after Run lock")
	}
	if err != nil {
		return runtimeV2ReconcileSkipped, err
	}
	if err = releaseRuntimeAttemptCapacity(ctx, queries, attempt); err != nil {
		return runtimeV2ReconcileSkipped, err
	}
	if terminal != nil {
		if err = finalizeRuntimeV2ReconciledTerminal(ctx, queries, locked, &finished, *terminal); err != nil {
			return runtimeV2ReconcileSkipped, err
		}
		if terminal.dispatchState == string(RuntimeDispatchDeadLetter) {
			return runtimeV2ReconcileDeadLettered, nil
		}
		return runtimeV2ReconcileTimedOut, nil
	}

	transitioned, err := queries.TransitionRuntimeV2RunAfterExpiredAttempt(ctx, db.TransitionRuntimeV2RunAfterExpiredAttemptParams{
		RetryAfterMs: retryDelay.Milliseconds(), RunID: attempt.RunID,
		AttemptID: attempt.ID, LeaseID: attempt.LeaseID,
		FencingToken: attempt.FencingToken,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return reconcileRuntimeV2DeadlineCrossedAfterAttemptFinish(ctx, queries, locked, &finished)
	}
	if err != nil {
		return runtimeV2ReconcileSkipped, err
	}
	if transitioned.DispatchState != string(RuntimeDispatchRetryWait) || transitioned.NextAttemptAt == nil {
		return runtimeV2ReconcileSkipped, errors.New("expired runtime Attempt did not enter retry_wait")
	}
	if err = createRuntimeV2AvailableSignal(
		ctx, queries, locked.AgentID, locked.ID, transitioned.NextAttemptAt,
	); err != nil {
		return runtimeV2ReconcileSkipped, err
	}
	return runtimeV2ReconcileRequeued, nil
}

func reconcileRuntimeV2DeadlineCrossedAfterAttemptFinish(
	ctx context.Context,
	queries *db.Queries,
	locked db.RuntimeV2ReconcileLockedRunRow,
	finished *db.RunAttempt,
) (runtimeV2ReconcileDisposition, error) {
	databaseNow, err := queries.GetRuntimeV2ReconcileDatabaseClock(ctx)
	if err != nil {
		return runtimeV2ReconcileSkipped, err
	}
	locked.DatabaseNow = databaseNow
	terminal := runtimeV2DeadlineTerminalForRun(locked)
	if terminal == nil {
		return runtimeV2ReconcileSkipped, errors.New("runtime v2 reconcile transition lost its fenced Run")
	}
	if err = finalizeRuntimeV2ReconciledTerminal(ctx, queries, locked, finished, *terminal); err != nil {
		return runtimeV2ReconcileSkipped, err
	}
	return runtimeV2ReconcileTimedOut, nil
}

type runtimeV2ReconcileTerminal struct {
	status         string
	dispatchState  string
	classification RuntimeResultClassification
	errorCode      string
	errorMessage   string
}

func runtimeV2DeadlineTerminalForRun(locked db.RuntimeV2ReconcileLockedRunRow) *runtimeV2ReconcileTerminal {
	if !locked.DatabaseNow.Before(locked.RunDeadlineAt) {
		return &runtimeV2ReconcileTerminal{
			status: string(RuntimeRunTimeout), dispatchState: string(RuntimeDispatchTerminal),
			classification: RuntimeResultClassificationTimeout,
			errorCode:      "RUN_DEADLINE_EXCEEDED", errorMessage: "Run deadline exceeded",
		}
	}
	if !locked.DatabaseNow.Before(locked.DispatchDeadlineAt) {
		return &runtimeV2ReconcileTerminal{
			status: string(RuntimeRunTimeout), dispatchState: string(RuntimeDispatchTerminal),
			classification: RuntimeResultClassificationTimeout,
			errorCode:      "RUNTIME_DISPATCH_TIMEOUT", errorMessage: "Runtime dispatch deadline exceeded",
		}
	}
	return nil
}

func runtimeV2OfferTerminalForRun(
	locked db.RuntimeV2ReconcileLockedRunRow,
	attempt db.RunAttempt,
) *runtimeV2ReconcileTerminal {
	if terminal := runtimeV2DeadlineTerminalForRun(locked); terminal != nil {
		return terminal
	}
	if locked.OfferCount >= locked.MaxOfferCount && !locked.DatabaseNow.Before(attempt.OfferExpiresAt) {
		return &runtimeV2ReconcileTerminal{
			status: string(RuntimeRunTimeout), dispatchState: string(RuntimeDispatchTerminal),
			classification: RuntimeResultClassificationTimeout,
			errorCode:      "RUNTIME_DISPATCH_TIMEOUT",
			errorMessage:   "Runtime offer budget exhausted before dispatch completed",
		}
	}
	return nil
}

func runtimeV2ExecutionTerminalForRun(locked db.RuntimeV2ReconcileLockedRunRow) *runtimeV2ReconcileTerminal {
	if terminal := runtimeV2DeadlineTerminalForRun(locked); terminal != nil {
		return terminal
	}
	if locked.AttemptCount >= locked.MaxAttempts {
		return &runtimeV2ReconcileTerminal{
			status: string(RuntimeRunFailed), dispatchState: string(RuntimeDispatchDeadLetter),
			classification: RuntimeResultClassificationDeadLetter,
			errorCode:      "RUNTIME_RETRY_EXHAUSTED", errorMessage: "Runtime retry budget exhausted",
		}
	}
	return nil
}

func finalizeRuntimeV2ReconciledTerminal(
	ctx context.Context,
	queries *db.Queries,
	locked db.RuntimeV2ReconcileLockedRunRow,
	finishedAttempt *db.RunAttempt,
	terminal runtimeV2ReconcileTerminal,
) error {
	durationMS, err := runtimeCancellationDurationMS(locked.DatabaseNow, locked.StartedAt)
	if err != nil {
		return err
	}
	run := runtimeResultRun{
		id: locked.ID, userID: locked.UserID, agentID: locked.AgentID,
		status: locked.Status, dispatchState: locked.DispatchState,
		connectionMode:      locked.ConnectionModeSnapshot,
		endpointIdempotency: locked.EndpointIdempotencySnapshot,
		attemptCount:        locked.AttemptCount, maxAttempts: locked.MaxAttempts,
		latestAttemptID: locked.LatestAttemptID, activeAttemptID: locked.ActiveAttemptID,
		leaseID: locked.LeaseID, fencingToken: locked.FencingToken,
		runtimeNodeID: locked.RuntimeNodeID, runtimeWorkerID: locked.RuntimeWorkerID,
		runtimeSessionID: locked.RuntimeSessionID, runDeadlineAt: locked.RunDeadlineAt,
		creatorRevenueCents: locked.CreatorRevenueCents, databaseNow: locked.DatabaseNow,
	}
	terminalEventID := deterministicTerminalEventID(locked.ID, terminal.status)
	effectPlan, err := discoverRuntimeResultEffects(ctx, queries, run, terminalEventID, "run.failed")
	if err != nil {
		return err
	}
	payload, err := marshalRuntimeResultJSON(map[string]any{
		"classification": terminal.classification,
		"duration_ms":    durationMS,
		"error_code":     terminal.errorCode,
		"error_message":  terminal.errorMessage,
		"status":         terminal.status,
		"terminal":       true,
	})
	if err != nil {
		return err
	}
	if err = queries.LockRunEventSequence(ctx, locked.ID); err != nil {
		return err
	}
	event, err := queries.InsertTerminalRunEvent(ctx, db.InsertTerminalRunEventParams{
		ID: terminalEventID, RunID: locked.ID, ParentRunID: effectPlan.parentRunID,
		EventType: "run.failed", Payload: payload,
	})
	if err != nil {
		return err
	}
	if terminal.dispatchState == string(RuntimeDispatchDeadLetter) {
		if finishedAttempt == nil || finishedAttempt.AttemptNo == nil {
			return errors.New("dead-lettered runtime v2 Run has no final Attempt number")
		}
		attemptErrorCode := finishedAttempt.ErrorCode
		if err = ensureRuntimeResultDeadLetter(
			ctx, queries, locked.ID, *finishedAttempt.AttemptNo, attemptErrorCode,
		); err != nil {
			return err
		}
	}
	var attemptID *uuid.UUID
	if finishedAttempt != nil {
		id := finishedAttempt.ID
		attemptID = &id
	}
	finalized, err := queries.FinalizeRuntimeV2ReconciledRun(ctx, db.FinalizeRuntimeV2ReconciledRunParams{
		Status: terminal.status, DispatchState: terminal.dispatchState,
		ErrorCode: terminal.errorCode, ErrorMessage: terminal.errorMessage,
		DurationMs: durationMS, TerminalEventID: event.ID,
		RunID: locked.ID, AttemptID: attemptID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return errors.New("runtime v2 terminal reconcile lost its fenced Run")
	}
	if err != nil {
		return err
	}
	if finalized.Status != terminal.status || finalized.DispatchState != terminal.dispatchState ||
		finalized.TerminalEventID == nil || *finalized.TerminalEventID != event.ID {
		return errors.New("runtime v2 terminal reconcile produced inconsistent Run facts")
	}
	if err = insertRuntimeResultLedgerAndStats(ctx, queries, run, event.ID, 0, 0); err != nil {
		return err
	}
	for _, effect := range effectPlan.effects {
		if err = ensureRuntimeResultEffect(ctx, queries, locked.ID, event.ID, effect); err != nil {
			return err
		}
	}
	return nil
}

func createRuntimeV2AvailableSignal(
	ctx context.Context,
	queries *db.Queries,
	agentID, runID uuid.UUID,
	availableAt *time.Time,
) error {
	payload, err := CanonicalizeRFC8785(map[string]any{"run_id": runID.String()})
	if err != nil {
		return err
	}
	_, err = queries.CreateRuntimeSignal(ctx, db.CreateRuntimeSignalParams{
		EventType: "run.available", AgentID: agentID, RunID: &runID,
		Payload: payload, AvailableAt: availableAt,
	})
	return err
}

func validateRuntimeV2ReconcileAttempt(
	locked db.RuntimeV2ReconcileLockedRunRow,
	attempt db.RunAttempt,
	candidate db.ListDueRuntimeV2ReconcileCandidatesRow,
) error {
	if candidate.AttemptID == nil || candidate.ExecutorType == nil ||
		attempt.ID != *candidate.AttemptID || attempt.RunID != locked.ID ||
		attempt.AgentID != locked.AgentID || attempt.ExecutorType != *candidate.ExecutorType ||
		attempt.FinishedAt != nil || attempt.Outcome != nil || attempt.ResultID != nil ||
		locked.LatestAttemptID == nil || *locked.LatestAttemptID != attempt.ID ||
		locked.ActiveAttemptID == nil || *locked.ActiveAttemptID != attempt.ID ||
		locked.LeaseID == nil || *locked.LeaseID != attempt.LeaseID ||
		locked.FencingToken != attempt.FencingToken || locked.CancelRequestID != nil {
		return errors.New("runtime v2 reconcile Attempt no longer matches its Run")
	}
	if attempt.ExecutorType == "agent_node" {
		if attempt.SlotAcquiredAt == nil || attempt.SlotReleasedAt != nil ||
			attempt.ActiveRuntimeSessionID == nil || attempt.NodeID == nil ||
			candidate.RuntimeSessionID == nil || *candidate.RuntimeSessionID != *attempt.ActiveRuntimeSessionID ||
			candidate.NodeID == nil || *candidate.NodeID != *attempt.NodeID {
			return errors.New("runtime v2 reconcile Attempt capacity identity changed")
		}
	} else if attempt.SlotAcquiredAt != nil || attempt.SlotReleasedAt != nil ||
		attempt.ActiveRuntimeSessionID != nil || candidate.RuntimeSessionID != nil || candidate.NodeID != nil {
		return errors.New("Core runtime v2 reconcile Attempt has capacity evidence")
	}
	switch locked.DispatchState {
	case string(RuntimeDispatchOffered):
		if attempt.AcceptedAt != nil || attempt.AttemptNo != nil {
			return errors.New("offered runtime v2 reconcile Attempt is already accepted")
		}
	case string(RuntimeDispatchExecuting):
		if attempt.AcceptedAt == nil || attempt.AttemptNo == nil || *attempt.AttemptNo != locked.AttemptCount {
			return errors.New("executing runtime v2 reconcile Attempt lacks acceptance evidence")
		}
	default:
		return errors.New("runtime v2 reconcile Attempt has invalid dispatch state")
	}
	return nil
}
