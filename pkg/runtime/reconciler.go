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

const maxRuntimeReconcileBatch = 1000

var (
	ErrRuntimeReconcilerNotConfigured = errors.New("Runtime deadline reconciler is not configured")
	ErrRuntimeReconcileBatchInvalid   = errors.New("Runtime deadline reconcile batch must be between 1 and 1000")
)

// RuntimeReconcileBatchResult reports committed state changes. Scanned is
// bounded by the requested limit; candidates skipped because another worker
// held a SKIP LOCKED row are not counted as reconciled.
type RuntimeReconcileBatchResult struct {
	Scanned       int `json:"scanned"`
	Reconciled    int `json:"reconciled"`
	Requeued      int `json:"requeued"`
	TimedOut      int `json:"timed_out"`
	DeadLettered  int `json:"dead_lettered"`
	ResultUnknown int `json:"result_unknown"`
}

// RuntimeDeadlineReconciler is a standalone worker boundary. It is not
// wired to coreapi here: callers choose scheduling, cadence, and shutdown.
type RuntimeDeadlineReconciler struct {
	pool         *pgxpool.Pool
	retryPlanner ResultRetryPlanner
}

// NewRuntimeDeadlineReconciler builds the lease/deadline worker. A nil retry
// planner uses the same bounded retry policy as ResultFinalizer.
func NewRuntimeDeadlineReconciler(
	pool *pgxpool.Pool,
	retryPlanner ResultRetryPlanner,
) *RuntimeDeadlineReconciler {
	if retryPlanner == nil {
		retryPlanner = fixedResultRetryPlanner{}
	}
	return &RuntimeDeadlineReconciler{pool: pool, retryPlanner: retryPlanner}
}

func (r *RuntimeDeadlineReconciler) nextReconcileDue(
	ctx context.Context,
) (*time.Time, time.Time, error) {
	if r == nil || r.pool == nil {
		return nil, time.Time{}, ErrRuntimeReconcilerNotConfigured
	}
	next, err := db.New(r.pool).NextRuntimeReconcileDue(ctx)
	return next.NextDueAt, next.DatabaseNow, err
}

type runtimeReconcileDisposition uint8

const (
	runtimeReconcileSkipped runtimeReconcileDisposition = iota
	runtimeReconcileRequeued
	runtimeReconcileTimedOut
	runtimeReconcileDeadLettered
	runtimeReconcileResultUnknown
)

// ReconcileBatch discovers at most limit due Runs with PostgreSQL's clock and
// commits each winner independently. A failed candidate rolls back its entire
// Attempt/capacity/Run/artifact transaction and stops the batch.
func (r *RuntimeDeadlineReconciler) ReconcileBatch(
	ctx context.Context,
	limit int,
) (RuntimeReconcileBatchResult, error) {
	if r == nil || r.pool == nil || r.retryPlanner == nil {
		return RuntimeReconcileBatchResult{}, ErrRuntimeReconcilerNotConfigured
	}
	if limit < 1 || limit > maxRuntimeReconcileBatch {
		return RuntimeReconcileBatchResult{}, ErrRuntimeReconcileBatchInvalid
	}

	candidates, err := db.New(r.pool).ListDueRuntimeReconcileCandidates(ctx, int32(limit))
	if err != nil {
		return RuntimeReconcileBatchResult{}, err
	}
	result := RuntimeReconcileBatchResult{Scanned: len(candidates)}
	for _, candidate := range candidates {
		disposition, reconcileErr := r.reconcileCandidate(ctx, candidate)
		if reconcileErr != nil {
			return result, reconcileErr
		}
		switch disposition {
		case runtimeReconcileSkipped:
			continue
		case runtimeReconcileRequeued:
			result.Requeued++
		case runtimeReconcileTimedOut:
			result.TimedOut++
		case runtimeReconcileDeadLettered:
			result.DeadLettered++
		case runtimeReconcileResultUnknown:
			result.ResultUnknown++
		default:
			return result, errors.New("Runtime reconciler returned an unknown disposition")
		}
		result.Reconciled++
	}
	return result, nil
}

func (r *RuntimeDeadlineReconciler) reconcileCandidate(
	ctx context.Context,
	candidate db.ListDueRuntimeReconcileCandidatesRow,
) (runtimeReconcileDisposition, error) {
	if candidate.RunID == uuid.Nil || candidate.DatabaseNow.IsZero() || candidate.DueAt.IsZero() {
		return runtimeReconcileSkipped, errors.New("Runtime reconcile candidate is incomplete")
	}
	if candidate.AttemptID == nil {
		if candidate.ExecutorType != nil || candidate.RuntimeSessionID != nil || candidate.NodeID != nil {
			return runtimeReconcileSkipped, errors.New("Runtime deadline-only candidate has Attempt identity")
		}
	} else if candidate.ExecutorType == nil {
		return runtimeReconcileSkipped, errors.New("Runtime Attempt candidate has no executor type")
	}

	disposition := runtimeReconcileSkipped
	err := pgx.BeginTxFunc(ctx, r.pool, pgx.TxOptions{
		IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadWrite,
	}, func(tx pgx.Tx) error {
		queries := db.New(tx)
		if candidate.AttemptID == nil {
			locked, lockErr := queries.LockDueRuntimeRunWithoutAttempt(ctx, candidate.RunID)
			if errors.Is(lockErr, pgx.ErrNoRows) {
				return nil
			}
			if lockErr != nil {
				return lockErr
			}
			terminal := runtimeDeadlineTerminalForRun(locked)
			if terminal == nil {
				return errors.New("locked Runtime deadline-only Run is not due")
			}
			if err := finalizeRuntimeReconciledTerminal(ctx, queries, locked, nil, *terminal); err != nil {
				return err
			}
			disposition = runtimeReconcileTimedOut
			return nil
		}

		switch *candidate.ExecutorType {
		case "runtime":
			if candidate.RuntimeSessionID == nil || candidate.NodeID == nil {
				return errors.New("runtime reconcile candidate is missing capacity owner")
			}
			lockedSession, lockErr := queries.LockRuntimeSessionForReconcile(ctx, *candidate.RuntimeSessionID)
			if errors.Is(lockErr, pgx.ErrNoRows) {
				return nil
			}
			if lockErr != nil {
				return lockErr
			}
			if lockedSession != *candidate.RuntimeSessionID {
				return errors.New("runtime reconcile Session lock returned a different owner")
			}
			lockedNode, lockErr := queries.LockRuntimeNodeForReconcile(ctx, *candidate.NodeID)
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
				return errors.New("Core reconcile candidate unexpectedly owns Runtime Worker capacity")
			}
		default:
			return fmt.Errorf("runtime reconcile candidate has unknown executor type %q", *candidate.ExecutorType)
		}

		locked, lockErr := queries.LockDueRuntimeRunWithAttempt(ctx, db.LockDueRuntimeRunWithAttemptParams{
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
		if err := validateRuntimeReconcileAttempt(locked, attempt, candidate); err != nil {
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
			return errors.New("Runtime Attempt candidate changed to an unsupported dispatch state")
		}
	})
	if err != nil {
		return runtimeReconcileSkipped, err
	}
	return disposition, nil
}

func (r *RuntimeDeadlineReconciler) reconcileExpiredOffer(
	ctx context.Context,
	queries *db.Queries,
	locked db.RuntimeReconcileLockedRunRow,
	attempt db.RunAttempt,
) (runtimeReconcileDisposition, error) {
	terminal := runtimeOfferTerminalForRun(locked, attempt)
	attemptErrorCode := "RUNTIME_OFFER_EXPIRED"
	if terminal != nil {
		attemptErrorCode = terminal.errorCode
	}
	finished, err := queries.FinishRuntimeReconciledAttempt(ctx, db.FinishRuntimeReconciledAttemptParams{
		Outcome: "offer_expired", ErrorCode: attemptErrorCode,
		RunID: attempt.RunID, AttemptID: attempt.ID,
		LeaseID: attempt.LeaseID, FencingToken: attempt.FencingToken,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return runtimeReconcileSkipped, errors.New("Runtime expired offer changed after Run lock")
	}
	if err != nil {
		return runtimeReconcileSkipped, err
	}
	if err = releaseRuntimeAttemptCapacity(ctx, queries, attempt); err != nil {
		return runtimeReconcileSkipped, err
	}
	if terminal != nil {
		if err = finalizeRuntimeReconciledTerminal(ctx, queries, locked, &finished, *terminal); err != nil {
			return runtimeReconcileSkipped, err
		}
		return runtimeReconcileTimedOut, nil
	}

	transitioned, err := queries.ResetRuntimeRunAfterReconciledOffer(ctx, db.ResetRuntimeRunAfterReconciledOfferParams{
		RunID: attempt.RunID, AttemptID: attempt.ID,
		LeaseID: attempt.LeaseID, FencingToken: attempt.FencingToken,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return reconcileRuntimeDeadlineCrossedAfterAttemptFinish(ctx, queries, locked, &finished)
	}
	if err != nil {
		return runtimeReconcileSkipped, err
	}
	if transitioned.DispatchState != string(RuntimeDispatchPending) || transitioned.NextAttemptAt != nil {
		return runtimeReconcileSkipped, errors.New("expired offer did not return its Run to pending")
	}
	if err = createRuntimeAvailableSignal(ctx, queries, locked.AgentID, locked.ID, nil); err != nil {
		return runtimeReconcileSkipped, err
	}
	return runtimeReconcileRequeued, nil
}

func (r *RuntimeDeadlineReconciler) reconcileExpiredExecution(
	ctx context.Context,
	queries *db.Queries,
	locked db.RuntimeReconcileLockedRunRow,
	attempt db.RunAttempt,
) (runtimeReconcileDisposition, error) {
	terminal := runtimeExecutionTerminalForRun(locked)
	outcome := "lease_expired"
	attemptErrorCode := "LEASE_EXPIRED"
	var retryDelay time.Duration
	coreResultUnknown := (attempt.ExecutorType == "core_http" || attempt.ExecutorType == "core_mcp") &&
		(locked.EndpointIdempotencySnapshot == nil || !*locked.EndpointIdempotencySnapshot)
	if coreResultUnknown {
		terminal = &runtimeReconcileTerminal{
			status: string(RuntimeRunFailed), dispatchState: string(RuntimeDispatchTerminal),
			classification: RuntimeResultClassificationNonRetryable,
			errorCode:      "ENDPOINT_RESULT_UNKNOWN",
			errorMessage:   "Endpoint execution outcome could not be confirmed",
		}
		outcome = "result_unknown"
		attemptErrorCode = terminal.errorCode
	}
	if terminal != nil && terminal.errorCode == "RUN_DEADLINE_EXCEEDED" {
		outcome = "timeout"
		attemptErrorCode = terminal.errorCode
	}
	if terminal == nil {
		retryDelay = r.retryPlanner.NextRetryDelay(locked.AttemptCount)
		if retryDelay < time.Millisecond || retryDelay > 60*time.Second {
			return runtimeReconcileSkipped, fmt.Errorf("Runtime retry planner returned invalid delay %s", retryDelay)
		}
	}
	finished, err := queries.FinishRuntimeReconciledAttempt(ctx, db.FinishRuntimeReconciledAttemptParams{
		Outcome: outcome, ErrorCode: attemptErrorCode,
		RunID: attempt.RunID, AttemptID: attempt.ID,
		LeaseID: attempt.LeaseID, FencingToken: attempt.FencingToken,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return runtimeReconcileSkipped, errors.New("Runtime expired execution changed after Run lock")
	}
	if err != nil {
		return runtimeReconcileSkipped, err
	}
	if err = releaseRuntimeAttemptCapacity(ctx, queries, attempt); err != nil {
		return runtimeReconcileSkipped, err
	}
	if terminal != nil {
		if err = finalizeRuntimeReconciledTerminal(ctx, queries, locked, &finished, *terminal); err != nil {
			return runtimeReconcileSkipped, err
		}
		if terminal.dispatchState == string(RuntimeDispatchDeadLetter) {
			return runtimeReconcileDeadLettered, nil
		}
		if coreResultUnknown {
			return runtimeReconcileResultUnknown, nil
		}
		return runtimeReconcileTimedOut, nil
	}

	transitioned, err := queries.TransitionRuntimeRunAfterExpiredAttempt(ctx, db.TransitionRuntimeRunAfterExpiredAttemptParams{
		RetryAfterMs: retryDelay.Milliseconds(), RunID: attempt.RunID,
		AttemptID: attempt.ID, LeaseID: attempt.LeaseID,
		FencingToken: attempt.FencingToken,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return reconcileRuntimeDeadlineCrossedAfterAttemptFinish(ctx, queries, locked, &finished)
	}
	if err != nil {
		return runtimeReconcileSkipped, err
	}
	if transitioned.DispatchState != string(RuntimeDispatchRetryWait) || transitioned.NextAttemptAt == nil {
		return runtimeReconcileSkipped, errors.New("expired runtime Attempt did not enter retry_wait")
	}
	if err = createRuntimeAvailableSignal(
		ctx, queries, locked.AgentID, locked.ID, transitioned.NextAttemptAt,
	); err != nil {
		return runtimeReconcileSkipped, err
	}
	return runtimeReconcileRequeued, nil
}

func reconcileRuntimeDeadlineCrossedAfterAttemptFinish(
	ctx context.Context,
	queries *db.Queries,
	locked db.RuntimeReconcileLockedRunRow,
	finished *db.RunAttempt,
) (runtimeReconcileDisposition, error) {
	databaseNow, err := queries.GetRuntimeReconcileDatabaseClock(ctx)
	if err != nil {
		return runtimeReconcileSkipped, err
	}
	locked.DatabaseNow = databaseNow
	terminal := runtimeDeadlineTerminalForRun(locked)
	if terminal == nil {
		return runtimeReconcileSkipped, errors.New("Runtime reconcile transition lost its fenced Run")
	}
	if err = finalizeRuntimeReconciledTerminal(ctx, queries, locked, finished, *terminal); err != nil {
		return runtimeReconcileSkipped, err
	}
	return runtimeReconcileTimedOut, nil
}

type runtimeReconcileTerminal struct {
	status         string
	dispatchState  string
	classification RuntimeResultClassification
	errorCode      string
	errorMessage   string
}

func runtimeDeadlineTerminalForRun(locked db.RuntimeReconcileLockedRunRow) *runtimeReconcileTerminal {
	if !locked.DatabaseNow.Before(locked.RunDeadlineAt) {
		return &runtimeReconcileTerminal{
			status: string(RuntimeRunTimeout), dispatchState: string(RuntimeDispatchTerminal),
			classification: RuntimeResultClassificationTimeout,
			errorCode:      "RUN_DEADLINE_EXCEEDED", errorMessage: "Run deadline exceeded",
		}
	}
	if !locked.DatabaseNow.Before(locked.DispatchDeadlineAt) {
		return &runtimeReconcileTerminal{
			status: string(RuntimeRunTimeout), dispatchState: string(RuntimeDispatchTerminal),
			classification: RuntimeResultClassificationTimeout,
			errorCode:      "RUNTIME_DISPATCH_TIMEOUT", errorMessage: "Runtime dispatch deadline exceeded",
		}
	}
	return nil
}

func runtimeOfferTerminalForRun(
	locked db.RuntimeReconcileLockedRunRow,
	attempt db.RunAttempt,
) *runtimeReconcileTerminal {
	if terminal := runtimeDeadlineTerminalForRun(locked); terminal != nil {
		return terminal
	}
	if locked.OfferCount >= locked.MaxOfferCount && !locked.DatabaseNow.Before(attempt.OfferExpiresAt) {
		return &runtimeReconcileTerminal{
			status: string(RuntimeRunTimeout), dispatchState: string(RuntimeDispatchTerminal),
			classification: RuntimeResultClassificationTimeout,
			errorCode:      "RUNTIME_DISPATCH_TIMEOUT",
			errorMessage:   "Runtime offer budget exhausted before dispatch completed",
		}
	}
	return nil
}

func runtimeExecutionTerminalForRun(locked db.RuntimeReconcileLockedRunRow) *runtimeReconcileTerminal {
	if terminal := runtimeDeadlineTerminalForRun(locked); terminal != nil {
		return terminal
	}
	if locked.AttemptCount >= locked.MaxAttempts {
		return &runtimeReconcileTerminal{
			status: string(RuntimeRunFailed), dispatchState: string(RuntimeDispatchDeadLetter),
			classification: RuntimeResultClassificationDeadLetter,
			errorCode:      "RUNTIME_RETRY_EXHAUSTED", errorMessage: "Runtime retry budget exhausted",
		}
	}
	return nil
}

func finalizeRuntimeReconciledTerminal(
	ctx context.Context,
	queries *db.Queries,
	locked db.RuntimeReconcileLockedRunRow,
	finishedAttempt *db.RunAttempt,
	terminal runtimeReconcileTerminal,
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
			return errors.New("dead-lettered Runtime Run has no final Attempt number")
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
	finalized, err := queries.FinalizeRuntimeReconciledRun(ctx, db.FinalizeRuntimeReconciledRunParams{
		Status: terminal.status, DispatchState: terminal.dispatchState,
		ErrorCode: terminal.errorCode, ErrorMessage: terminal.errorMessage,
		DurationMs: durationMS, TerminalEventID: event.ID,
		RunID: locked.ID, AttemptID: attemptID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return errors.New("Runtime terminal reconcile lost its fenced Run")
	}
	if err != nil {
		return err
	}
	if finalized.Status != terminal.status || finalized.DispatchState != terminal.dispatchState ||
		finalized.TerminalEventID == nil || *finalized.TerminalEventID != event.ID {
		return errors.New("Runtime terminal reconcile produced inconsistent Run facts")
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

func createRuntimeAvailableSignal(
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

func validateRuntimeReconcileAttempt(
	locked db.RuntimeReconcileLockedRunRow,
	attempt db.RunAttempt,
	candidate db.ListDueRuntimeReconcileCandidatesRow,
) error {
	if candidate.AttemptID == nil || candidate.ExecutorType == nil ||
		attempt.ID != *candidate.AttemptID || attempt.RunID != locked.ID ||
		attempt.AgentID != locked.AgentID || attempt.ExecutorType != *candidate.ExecutorType ||
		attempt.FinishedAt != nil || attempt.Outcome != nil || attempt.ResultID != nil ||
		locked.LatestAttemptID == nil || *locked.LatestAttemptID != attempt.ID ||
		locked.ActiveAttemptID == nil || *locked.ActiveAttemptID != attempt.ID ||
		locked.LeaseID == nil || *locked.LeaseID != attempt.LeaseID ||
		locked.FencingToken != attempt.FencingToken || locked.CancelRequestID != nil {
		return errors.New("Runtime reconcile Attempt no longer matches its Run")
	}
	if attempt.ExecutorType == "runtime" {
		if attempt.SlotAcquiredAt == nil || attempt.SlotReleasedAt != nil ||
			attempt.ActiveRuntimeSessionID == nil || attempt.NodeID == nil ||
			candidate.RuntimeSessionID == nil || *candidate.RuntimeSessionID != *attempt.ActiveRuntimeSessionID ||
			candidate.NodeID == nil || *candidate.NodeID != *attempt.NodeID {
			return errors.New("Runtime reconcile Attempt capacity identity changed")
		}
	} else if attempt.SlotAcquiredAt != nil || attempt.SlotReleasedAt != nil ||
		attempt.ActiveRuntimeSessionID != nil || candidate.RuntimeSessionID != nil || candidate.NodeID != nil {
		return errors.New("Core Runtime reconcile Attempt has capacity evidence")
	}
	switch locked.DispatchState {
	case string(RuntimeDispatchOffered):
		if attempt.AcceptedAt != nil || attempt.AttemptNo != nil {
			return errors.New("offered Runtime reconcile Attempt is already accepted")
		}
	case string(RuntimeDispatchExecuting):
		if attempt.AcceptedAt == nil || attempt.AttemptNo == nil || *attempt.AttemptNo != locked.AttemptCount {
			return errors.New("executing Runtime reconcile Attempt lacks acceptance evidence")
		}
	default:
		return errors.New("Runtime reconcile Attempt has invalid dispatch state")
	}
	return nil
}
