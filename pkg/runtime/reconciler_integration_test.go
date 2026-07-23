package runtime_test

import (
	"context"
	"crypto/sha256"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

func TestRuntimeDeadlineReconcilerConcurrentLeaseExpiryIsExactlyOnce(t *testing.T) {
	pool := setupTestDB(t)
	requireRuntimeDeadlineReconcilerSchema(t, pool)
	fixture := insertEventStoreExecutingAttempt(t, pool, 20*time.Millisecond)
	waitForRuntimeLeaseDue(t, pool, fixture.identity.AttemptID)
	reconciler := runtime.NewRuntimeDeadlineReconciler(pool, nil)
	_, err := reconciler.ReconcileBatch(context.Background(), 0)
	require.ErrorIs(t, err, runtime.ErrRuntimeReconcileBatchInvalid)
	_, err = reconciler.ReconcileBatch(context.Background(), 1001)
	require.ErrorIs(t, err, runtime.ErrRuntimeReconcileBatchInvalid)

	const workers = 48
	results := make(chan runtime.RuntimeReconcileBatchResult, workers)
	errors := make(chan error, workers)
	start := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var wait sync.WaitGroup
	wait.Add(workers)
	for range workers {
		go func() {
			defer wait.Done()
			<-start
			result, err := reconciler.ReconcileBatch(ctx, 1)
			results <- result
			errors <- err
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	close(errors)

	for reconcileErr := range errors {
		require.NoError(t, reconcileErr)
	}
	totalReconciled, totalRequeued := 0, 0
	for result := range results {
		totalReconciled += result.Reconciled
		totalRequeued += result.Requeued
	}
	require.Equal(t, 1, totalReconciled)
	require.Equal(t, 1, totalRequeued)

	var status, dispatchState string
	var nextAttemptAt *time.Time
	var attemptOutcome *string
	var availableSignals int
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT r.status, r.dispatch_state, r.next_attempt_at, a.outcome,
		       (SELECT COUNT(*) FROM runtime_signal_outbox
		        WHERE run_id = r.id AND event_type = 'run.available')
		FROM runs r
		JOIN run_attempts a ON a.id = $2
		WHERE r.id = $1`, fixture.identity.RunID, fixture.identity.AttemptID).Scan(
		&status, &dispatchState, &nextAttemptAt, &attemptOutcome, &availableSignals,
	))
	require.Equal(t, "running", status)
	require.Equal(t, "retry_wait", dispatchState)
	require.NotNil(t, nextAttemptAt)
	require.NotNil(t, attemptOutcome)
	require.Equal(t, "lease_expired", *attemptOutcome)
	require.Equal(t, 1, availableSignals)
	assertFinalizerCapacityReleased(t, pool, fixture)

	again, err := reconciler.ReconcileBatch(context.Background(), 20)
	require.NoError(t, err)
	require.Zero(t, again.Reconciled)
	assertFinalizerTerminalCounts(t, pool, fixture.identity.RunID, 0, 0, 0)
}

func TestRuntimeDeadlineReconcilerUsesSkipLockedCapacityOrder(t *testing.T) {
	pool := setupTestDB(t)
	requireRuntimeDeadlineReconcilerSchema(t, pool)
	fixture := insertEventStoreExecutingAttempt(t, pool, 20*time.Millisecond)
	waitForRuntimeLeaseDue(t, pool, fixture.identity.AttemptID)
	require.NotNil(t, fixture.identity.RuntimeSessionID)

	blocker, err := pool.Begin(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = blocker.Rollback(context.Background()) })
	_, err = blocker.Exec(context.Background(), `
		SELECT runtime_session_id FROM runtime_sessions
		WHERE runtime_session_id = $1 FOR UPDATE`, *fixture.identity.RuntimeSessionID)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	started := time.Now()
	skipped, err := runtime.NewRuntimeDeadlineReconciler(pool, nil).ReconcileBatch(ctx, 1)
	require.NoError(t, err)
	require.Less(t, time.Since(started), 400*time.Millisecond)
	require.Equal(t, 1, skipped.Scanned)
	require.Zero(t, skipped.Reconciled)
	assertFinalizerAttemptStillExecuting(t, pool, fixture.identity.RunID, fixture.identity.AttemptID)

	require.NoError(t, blocker.Rollback(context.Background()))
	won, err := runtime.NewRuntimeDeadlineReconciler(pool, nil).ReconcileBatch(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, 1, won.Reconciled)
	require.Equal(t, 1, won.Requeued)
	assertFinalizerCapacityReleased(t, pool, fixture)
}

func TestRuntimeDeadlineReconcilerReleasesCapacityFromClosedSession(t *testing.T) {
	pool := setupTestDB(t)
	requireRuntimeDeadlineReconcilerSchema(t, pool)
	fixture := insertEventStoreExecutingAttempt(t, pool, 20*time.Millisecond)
	require.NotNil(t, fixture.identity.RuntimeSessionID)

	err := pgx.BeginTxFunc(context.Background(), pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if _, err := tx.Exec(context.Background(), `
			UPDATE runtime_session_attachments
			SET detached_at = clock_timestamp(), disconnect_reason = 'closed Session reconcile test'
			WHERE runtime_session_id = $1 AND detached_at IS NULL`,
			*fixture.identity.RuntimeSessionID,
		); err != nil {
			return err
		}
		_, err := tx.Exec(context.Background(), `
			UPDATE runtime_sessions
			SET status = 'closed', attached_core_instance_id = NULL,
			    disconnected_at = clock_timestamp(), updated_at = clock_timestamp()
			WHERE runtime_session_id = $1 AND status = 'active'`,
			*fixture.identity.RuntimeSessionID,
		)
		return err
	})
	require.NoError(t, err)
	waitForRuntimeLeaseDue(t, pool, fixture.identity.AttemptID)

	result, err := runtime.NewRuntimeDeadlineReconciler(pool, nil).ReconcileBatch(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, 1, result.Requeued)
	assertFinalizerCapacityReleased(t, pool, fixture)

	var sessionStatus string
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT status FROM runtime_sessions WHERE runtime_session_id = $1`,
		*fixture.identity.RuntimeSessionID,
	).Scan(&sessionStatus))
	require.Equal(t, "closed", sessionStatus)
}

func TestRuntimeDeadlineReconcilerTerminalRollbackAndDeadLetterAreAtomic(t *testing.T) {
	pool := setupTestDB(t)
	requireRuntimeDeadlineReconcilerSchema(t, pool)
	fixture := insertEventStoreExecutingAttempt(t, pool, 20*time.Millisecond)
	setFinalizerMaxAttempts(t, pool, fixture.identity.RunID, 1)
	seedFinalizerEffectTargets(t, pool, fixture.identity.RunID, fixture.identity.AgentID, "run.failed")
	installFailingFinalizerEffectTrigger(t, pool)
	waitForRuntimeLeaseDue(t, pool, fixture.identity.AttemptID)
	reconciler := runtime.NewRuntimeDeadlineReconciler(pool, nil)

	_, err := reconciler.ReconcileBatch(context.Background(), 1)
	require.ErrorContains(t, err, "forced finalizer effect failure")
	assertFinalizerAttemptStillExecuting(t, pool, fixture.identity.RunID, fixture.identity.AttemptID)
	assertFinalizerTerminalCounts(t, pool, fixture.identity.RunID, 0, 0, 0)
	assertRuntimeReconcileCapacity(t, pool, fixture, 1, 1, false)

	_, err = pool.Exec(context.Background(), `DROP TRIGGER fail_finalizer_effect_insert ON run_effect_outbox`)
	require.NoError(t, err)
	_, err = pool.Exec(context.Background(), `DROP FUNCTION fail_finalizer_effect_insert()`)
	require.NoError(t, err)

	result, err := reconciler.ReconcileBatch(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, 1, result.Reconciled)
	require.Equal(t, 1, result.DeadLettered)
	assertFinalizerTerminalCounts(t, pool, fixture.identity.RunID, 1, 3, 1)
	assertFinalizerCapacityReleased(t, pool, fixture)

	var status, dispatchState, runErrorCode, attemptOutcome string
	var consecutiveFailures int32
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT r.status, r.dispatch_state, r.error_code, a.outcome,
		       availability.consecutive_failures
		FROM runs r
		JOIN run_attempts a ON a.id = $2
		JOIN agent_availability_snapshots availability ON availability.agent_id = r.agent_id
		WHERE r.id = $1`, fixture.identity.RunID, fixture.identity.AttemptID).Scan(
		&status, &dispatchState, &runErrorCode, &attemptOutcome, &consecutiveFailures,
	))
	require.Equal(t, "failed", status)
	require.Equal(t, "dead_letter", dispatchState)
	require.Equal(t, "RUNTIME_RETRY_EXHAUSTED", runErrorCode)
	require.Equal(t, "lease_expired", attemptOutcome)
	require.Equal(t, int32(1), consecutiveFailures)
}

func TestRuntimeDeadlineReconcilerRunDeadlineWinsForAcceptedAttempt(t *testing.T) {
	pool := setupTestDB(t)
	requireRuntimeDeadlineReconcilerSchema(t, pool)
	fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
	expireFinalizerFixtureAtDatabaseClock(t, pool, fixture)

	result, err := runtime.NewRuntimeDeadlineReconciler(pool, nil).ReconcileBatch(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, 1, result.Reconciled)
	require.Equal(t, 1, result.TimedOut)
	assertFinalizerCapacityReleased(t, pool, fixture)
	assertFinalizerTerminalCounts(t, pool, fixture.identity.RunID, 1, 0, 0)

	var status, dispatchState, runErrorCode, attemptOutcome string
	var resultID *uuid.UUID
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT r.status, r.dispatch_state, r.error_code, r.result_id, a.outcome
		FROM runs r JOIN run_attempts a ON a.id = $2 WHERE r.id = $1`,
		fixture.identity.RunID, fixture.identity.AttemptID,
	).Scan(&status, &dispatchState, &runErrorCode, &resultID, &attemptOutcome))
	require.Equal(t, "timeout", status)
	require.Equal(t, "terminal", dispatchState)
	require.Equal(t, "RUN_DEADLINE_EXCEEDED", runErrorCode)
	require.Nil(t, resultID)
	require.Equal(t, "timeout", attemptOutcome)
}

func TestRuntimeDeadlineReconcilerExpiresOffersToPendingOrTerminal(t *testing.T) {
	t.Run("another offer remains", func(t *testing.T) {
		pool := setupTestDB(t)
		requireRuntimeDeadlineReconcilerSchema(t, pool)
		fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
		convertRuntimeReconcileFixtureToOffered(t, pool, fixture, 2)
		waitForRuntimeOfferDue(t, pool, fixture.identity.AttemptID)

		result, err := runtime.NewRuntimeDeadlineReconciler(pool, nil).ReconcileBatch(context.Background(), 1)
		require.NoError(t, err)
		require.Equal(t, 1, result.Requeued)
		assertFinalizerCapacityReleased(t, pool, fixture)

		var status, dispatchState, outcome string
		var activeAttemptID *uuid.UUID
		var signals int
		require.NoError(t, pool.QueryRow(context.Background(), `
			SELECT r.status, r.dispatch_state, r.active_attempt_id, a.outcome,
			       (SELECT COUNT(*) FROM runtime_signal_outbox
			        WHERE run_id = r.id AND event_type = 'run.available')
			FROM runs r JOIN run_attempts a ON a.id = $2 WHERE r.id = $1`,
			fixture.identity.RunID, fixture.identity.AttemptID,
		).Scan(&status, &dispatchState, &activeAttemptID, &outcome, &signals))
		require.Equal(t, "running", status)
		require.Equal(t, "pending", dispatchState)
		require.Nil(t, activeAttemptID)
		require.Equal(t, "offer_expired", outcome)
		require.Equal(t, 1, signals)
		assertFinalizerTerminalCounts(t, pool, fixture.identity.RunID, 0, 0, 0)
	})

	t.Run("offer budget is terminal", func(t *testing.T) {
		pool := setupTestDB(t)
		requireRuntimeDeadlineReconcilerSchema(t, pool)
		fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
		convertRuntimeReconcileFixtureToOffered(t, pool, fixture, 1)
		waitForRuntimeOfferDue(t, pool, fixture.identity.AttemptID)

		result, err := runtime.NewRuntimeDeadlineReconciler(pool, nil).ReconcileBatch(context.Background(), 1)
		require.NoError(t, err)
		require.Equal(t, 1, result.TimedOut)
		assertFinalizerCapacityReleased(t, pool, fixture)
		assertFinalizerTerminalCounts(t, pool, fixture.identity.RunID, 1, 0, 0)

		var status, dispatchState, errorCode, outcome string
		require.NoError(t, pool.QueryRow(context.Background(), `
			SELECT r.status, r.dispatch_state, r.error_code, a.outcome
			FROM runs r JOIN run_attempts a ON a.id = $2 WHERE r.id = $1`,
			fixture.identity.RunID, fixture.identity.AttemptID,
		).Scan(&status, &dispatchState, &errorCode, &outcome))
		require.Equal(t, "timeout", status)
		require.Equal(t, "terminal", dispatchState)
		require.Equal(t, "RUNTIME_DISPATCH_TIMEOUT", errorCode)
		require.Equal(t, "offer_expired", outcome)
	})
}

func TestRuntimeDeadlineReconcilerTerminatesPendingWithoutAttempt(t *testing.T) {
	pool := setupTestDB(t)
	requireRuntimeDeadlineReconcilerSchema(t, pool)
	runID := insertRuntimePendingDeadlineRun(t, pool, 80*time.Millisecond)
	waitForRuntimeRunDispatchDue(t, pool, runID)

	result, err := runtime.NewRuntimeDeadlineReconciler(pool, nil).ReconcileBatch(context.Background(), 5)
	require.NoError(t, err)
	require.Equal(t, 1, result.Reconciled)
	require.Equal(t, 1, result.TimedOut)
	assertFinalizerTerminalCounts(t, pool, runID, 1, 0, 0)

	var status, dispatchState, errorCode, classification string
	var latestAttemptID *uuid.UUID
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT r.status, r.dispatch_state, r.error_code, r.latest_attempt_id,
		       e.payload->>'classification'
		FROM runs r JOIN run_events e ON e.id = r.terminal_event_id
		WHERE r.id = $1`, runID).Scan(
		&status, &dispatchState, &errorCode, &latestAttemptID, &classification,
	))
	require.Equal(t, "timeout", status)
	require.Equal(t, "terminal", dispatchState)
	require.Equal(t, "RUNTIME_DISPATCH_TIMEOUT", errorCode)
	require.Nil(t, latestAttemptID)
	require.Equal(t, "timeout", classification)
}

func TestRuntimeDeadlineReconcilerLeavesCancellationToCancellationReaper(t *testing.T) {
	pool := setupTestDB(t)
	requireRuntimeDeadlineReconcilerSchema(t, pool)
	fixture := insertEventStoreExecutingAttempt(t, pool, 20*time.Millisecond)
	var ownerID uuid.UUID
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT user_id FROM runs WHERE id = $1`, fixture.identity.RunID).Scan(&ownerID))
	_, err := runtime.NewRuntimeCancellationCoordinator(pool).CancelOwnedRun(
		context.Background(), ownerID, fixture.identity.RunID, "reconciler exclusion",
	)
	require.NoError(t, err)
	waitForRuntimeLeaseDue(t, pool, fixture.identity.AttemptID)

	result, err := runtime.NewRuntimeDeadlineReconciler(pool, nil).ReconcileBatch(context.Background(), 20)
	require.NoError(t, err)
	require.Zero(t, result.Reconciled)
	assertRuntimeReconcileCapacity(t, pool, fixture, 1, 1, false)

	var cancelRequestID *uuid.UUID
	var finishedAt *time.Time
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT r.cancel_request_id, a.finished_at
		FROM runs r JOIN run_attempts a ON a.id = $2 WHERE r.id = $1`,
		fixture.identity.RunID, fixture.identity.AttemptID,
	).Scan(&cancelRequestID, &finishedAt))
	require.NotNil(t, cancelRequestID)
	require.Nil(t, finishedAt)
}

func requireRuntimeDeadlineReconcilerSchema(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	var version int32
	var migration string
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT schema_version, migration_name
		FROM runtime_schema_contracts
		WHERE runtime_contract_id = 'openlinker.runtime.v2' AND is_current`).Scan(
		&version, &migration,
	))
	if version < 66 {
		t.Skipf("Runtime deadline reconciler migration is not installed: version=%d migration=%s", version, migration)
	}
}

func waitForRuntimeLeaseDue(t *testing.T, pool *pgxpool.Pool, attemptID uuid.UUID) {
	t.Helper()
	require.Eventually(t, func() bool {
		var due bool
		err := pool.QueryRow(context.Background(), `
			SELECT clock_timestamp() >= lease_expires_at
			FROM run_attempts WHERE id = $1`, attemptID).Scan(&due)
		return err == nil && due
	}, 2*time.Second, 5*time.Millisecond)
}

func waitForRuntimeOfferDue(t *testing.T, pool *pgxpool.Pool, attemptID uuid.UUID) {
	t.Helper()
	require.Eventually(t, func() bool {
		var due bool
		err := pool.QueryRow(context.Background(), `
			SELECT clock_timestamp() >= offer_expires_at
			FROM run_attempts WHERE id = $1`, attemptID).Scan(&due)
		return err == nil && due
	}, 2*time.Second, 5*time.Millisecond)
}

func waitForRuntimeRunDispatchDue(t *testing.T, pool *pgxpool.Pool, runID uuid.UUID) {
	t.Helper()
	require.Eventually(t, func() bool {
		var due bool
		err := pool.QueryRow(context.Background(), `
			SELECT clock_timestamp() >= dispatch_deadline_at
			FROM runs WHERE id = $1`, runID).Scan(&due)
		return err == nil && due
	}, 2*time.Second, 5*time.Millisecond)
}

func convertRuntimeReconcileFixtureToOffered(
	t *testing.T,
	pool *pgxpool.Pool,
	fixture eventStoreFixture,
	maxOffers int32,
) {
	t.Helper()
	err := pgx.BeginTxFunc(context.Background(), pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if _, err := tx.Exec(context.Background(), `SET LOCAL session_replication_role = replica`); err != nil {
			return err
		}
		var offerExpiresAt time.Time
		if err := tx.QueryRow(context.Background(), `
			SELECT GREATEST(clock_timestamp() + INTERVAL '20 milliseconds', offered_at)
			FROM run_attempts WHERE id = $1`, fixture.identity.AttemptID).Scan(&offerExpiresAt); err != nil {
			return err
		}
		if _, err := tx.Exec(context.Background(), `
			UPDATE run_attempts
			SET attempt_no = NULL,
			    accepted_at = NULL,
			    last_renewed_at = NULL,
			    runtime_attachment_id = NULL,
			    offer_expires_at = $2
			WHERE id = $1`, fixture.identity.AttemptID, offerExpiresAt); err != nil {
			return err
		}
		_, err := tx.Exec(context.Background(), `
			UPDATE runs
			SET dispatch_state = 'offered',
			    attempt_count = 0,
			    max_offer_count = $2,
			    lease_accepted_at = NULL
			WHERE id = $1`, fixture.identity.RunID, maxOffers)
		return err
	})
	if err != nil && strings.Contains(err.Error(), "permission denied") {
		t.Skipf("test database cannot create an unaccepted immutable fixture: %v", err)
	}
	require.NoError(t, err)
}

func insertRuntimePendingDeadlineRun(
	t *testing.T,
	pool *pgxpool.Pool,
	dispatchTTL time.Duration,
) uuid.UUID {
	t.Helper()
	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, creatorID, "https://example.test/reconcile", 0, "approved")
	runID := uuid.New()
	keyHash := sha256.Sum256([]byte("reconcile-key/" + runID.String()))
	fingerprint := sha256.Sum256([]byte("reconcile-fingerprint/" + runID.String()))
	var databaseNow time.Time
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT clock_timestamp()`).Scan(&databaseNow))
	dispatchDeadline := databaseNow.Add(dispatchTTL)
	runDeadline := databaseNow.Add(5 * time.Second)
	_, err := pool.Exec(context.Background(), `
		INSERT INTO runs (
			id, user_id, agent_id, input, status,
			cost_cents, platform_fee_cents, creator_revenue_cents, source,
			runtime_contract_id, idempotency_key_hash, idempotency_fingerprint,
			request_metadata, connection_mode_snapshot,
			dispatch_state, max_offer_count, max_attempts,
			dispatch_deadline_at, run_deadline_at
		) VALUES (
			$1, $2, $3, '{}'::jsonb, 'running',
			0, 0, 0, 'api',
			'openlinker.runtime.v2', $4, $5,
			'{}'::jsonb, 'runtime',
			'pending', 3, 3, $6, $7
		)`, runID, userID, agentID, keyHash[:], fingerprint[:], dispatchDeadline, runDeadline)
	require.NoError(t, err)
	return runID
}

func assertRuntimeReconcileCapacity(
	t *testing.T,
	pool *pgxpool.Pool,
	fixture eventStoreFixture,
	wantSession, wantNode int32,
	wantReleased bool,
) {
	t.Helper()
	var sessionInflight, nodeInflight int32
	var slotReleasedAt *time.Time
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT session.inflight, node.inflight, attempt.slot_released_at
		FROM run_attempts attempt
		JOIN runtime_sessions session ON session.runtime_session_id = attempt.runtime_session_id
		JOIN runtime_nodes node ON node.node_id = attempt.node_id
		WHERE attempt.run_id = $1 AND attempt.id = $2`,
		fixture.identity.RunID, fixture.identity.AttemptID,
	).Scan(&sessionInflight, &nodeInflight, &slotReleasedAt))
	require.Equal(t, wantSession, sessionInflight)
	require.Equal(t, wantNode, nodeInflight)
	require.Equal(t, wantReleased, slotReleasedAt != nil)
}

func TestRuntimeDeadlineReconcilerCoreAttemptDoesNotRequireCapacity(t *testing.T) {
	pool := setupTestDB(t)
	requireRuntimeDeadlineReconcilerSchema(t, pool)
	fixture := insertEventStoreExecutingAttempt(t, pool, 20*time.Millisecond)
	convertRuntimeCancellationFixtureToCoreHTTP(t, pool, fixture)
	waitForRuntimeLeaseDue(t, pool, fixture.identity.AttemptID)

	result, err := runtime.NewRuntimeDeadlineReconciler(pool, nil).ReconcileBatch(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, 1, result.Requeued)
	var dispatchState, outcome string
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT r.dispatch_state, a.outcome
		FROM runs r JOIN run_attempts a ON a.id = $2 WHERE r.id = $1`,
		fixture.identity.RunID, fixture.identity.AttemptID,
	).Scan(&dispatchState, &outcome))
	require.Equal(t, "retry_wait", dispatchState)
	require.Equal(t, "lease_expired", outcome)
}
