package runtime_test

import (
	"context"
	"fmt"
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

func TestResultFinalizerConcurrentSuccessIsExactlyOnce(t *testing.T) {
	pool := setupTestDB(t)
	requireReliableRuntimeV2Schema(t, pool)
	fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
	seedFinalizerEffectTargets(t, pool, fixture.identity.RunID, fixture.identity.AgentID, "run.completed")
	finalizer := runtime.NewResultFinalizer(pool, nil, nil)
	request := successfulRuntimeResult(fixture, map[string]any{
		"answer": "one terminal transaction",
	})

	const workers = 100
	type result struct {
		ack runtime.RuntimeResultAck
		err error
	}
	results := make(chan result, workers)
	start := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var wait sync.WaitGroup
	wait.Add(workers)
	for range workers {
		go func() {
			defer wait.Done()
			<-start
			ack, err := finalizer.Finalize(ctx, fixture.principal, request)
			results <- result{ack: ack, err: err}
		}()
	}
	close(start)
	wait.Wait()
	close(results)

	created, replayed := 0, 0
	var first runtime.RuntimeResultAck
	for result := range results {
		require.NoError(t, result.err)
		if first.ResultID == uuid.Nil {
			first = result.ack
		}
		require.Equal(t, request.ResultID, result.ack.ResultID)
		require.Equal(t, "success", result.ack.RunStatus)
		require.Equal(t, "terminal", result.ack.DispatchState)
		require.Equal(t, runtime.RuntimeResultClassificationSuccess, result.ack.Classification)
		if result.ack.Replayed {
			replayed++
		} else {
			created++
		}
	}
	require.Equal(t, 1, created)
	require.Equal(t, workers-1, replayed)

	assertFinalizerTerminalCounts(t, pool, fixture.identity.RunID, 1, 3, 0)
	assertFinalizerCapacityReleased(t, pool, fixture)
	var terminalPayload []byte
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT payload FROM run_events
		WHERE run_id = $1 AND event_type = 'run.completed'`, fixture.identity.RunID).Scan(&terminalPayload))
	require.Contains(t, string(terminalPayload), `"answer": "one terminal transaction"`)
	var totalCalls int32
	var availabilitySuccesses int
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT a.total_calls,
		       (SELECT COUNT(*) FROM agent_availability_snapshots s
		         WHERE s.agent_id = a.id AND s.consecutive_failures = 0
		           AND s.last_successful_run_at IS NOT NULL)
		FROM agents a WHERE a.id = $1`, fixture.identity.AgentID).Scan(&totalCalls, &availabilitySuccesses))
	require.Equal(t, int32(1), totalCalls)
	require.Equal(t, 1, availabilitySuccesses)

	rows, err := pool.Query(context.Background(), `
		SELECT effect_type, max_attempts, metadata::text
		FROM run_effect_outbox WHERE run_id = $1 ORDER BY effect_type`, fixture.identity.RunID)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var effectType, metadata string
		var maxAttempts int32
		require.NoError(t, rows.Scan(&effectType, &maxAttempts, &metadata))
		require.Equal(t, int32(3), maxAttempts, effectType)
		for _, secret := range []string{"creator-webhook-secret", "default-delivery-secret", "callback-secret", "one terminal transaction"} {
			require.NotContains(t, metadata, secret)
		}
	}
	require.NoError(t, rows.Err())

	conflictingPayload := request
	conflictingPayload.Output = map[string]any{"answer": "different"}
	_, err = finalizer.Finalize(context.Background(), fixture.principal, conflictingPayload)
	require.True(t, runtime.IsRuntimeResultError(err, runtime.RuntimeResultErrorResultIDConflict), "%v", err)
	conflictingID := request
	conflictingID.ResultID = uuid.New()
	_, err = finalizer.Finalize(context.Background(), fixture.principal, conflictingID)
	require.True(t, runtime.IsRuntimeResultError(err, runtime.RuntimeResultErrorResultIDConflict), "%v", err)
	assertFinalizerTerminalCounts(t, pool, fixture.identity.RunID, 1, 3, 0)
}

func TestResultFinalizerNonRetryableFailureAndReplayOwnership(t *testing.T) {
	pool := setupTestDB(t)
	requireReliableRuntimeV2Schema(t, pool)
	fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
	finalizer := runtime.NewResultFinalizer(pool, nil, nil)
	request := runtime.RuntimeResultRequest{
		AttemptIdentity: fixture.identity,
		ResultID:        uuid.New(),
		Status:          "failed",
		Error: &runtime.RuntimeResultFailure{
			ErrorCode: "POLICY_REJECTED",
			Message:   "request violates target policy",
		},
		DurationMS: 11,
	}

	wrongPrincipal := fixture.principal
	wrongPrincipal.AgentID = uuid.New()
	_, err := finalizer.Finalize(context.Background(), wrongPrincipal, request)
	require.True(t, runtime.IsRuntimeResultError(err, runtime.RuntimeResultErrorLeaseIdentityMismatch), "%v", err)
	assertFinalizerAttemptStillExecuting(t, pool, fixture.identity.RunID, fixture.identity.AttemptID)

	first, err := finalizer.Finalize(context.Background(), fixture.principal, request)
	require.NoError(t, err)
	require.Equal(t, runtime.RuntimeResultClassificationNonRetryable, first.Classification)
	require.Equal(t, "failed", first.RunStatus)
	require.Equal(t, "terminal", first.DispatchState)
	second, err := finalizer.Finalize(context.Background(), fixture.principal, request)
	require.NoError(t, err)
	require.True(t, second.Replayed)
	require.Equal(t, runtime.RuntimeResultClassificationNonRetryable, second.Classification)

	wrongSession := fixture.principal
	newSessionID := uuid.New()
	wrongSession.RuntimeSessionID = &newSessionID
	_, err = finalizer.Finalize(context.Background(), wrongSession, request)
	require.True(t, runtime.IsRuntimeResultError(err, runtime.RuntimeResultErrorLeaseIdentityMismatch), "%v", err)
	assertFinalizerTerminalCounts(t, pool, fixture.identity.RunID, 1, 0, 0)

	var errorCode, eventErrorCode, eventErrorMessage string
	var consecutiveFailures int32
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT r.error_code,
		       e.payload->>'error_code', e.payload->>'error_message',
		       s.consecutive_failures
		FROM runs r
		JOIN run_events e ON e.id = r.terminal_event_id
		JOIN agent_availability_snapshots s ON s.agent_id = r.agent_id
		WHERE r.id = $1`, fixture.identity.RunID).Scan(
		&errorCode, &eventErrorCode, &eventErrorMessage, &consecutiveFailures,
	))
	require.Equal(t, "POLICY_REJECTED", errorCode)
	require.Equal(t, errorCode, eventErrorCode)
	require.Equal(t, request.Error.Message, eventErrorMessage)
	require.Equal(t, int32(1), consecutiveFailures)
}

func TestResultFinalizerRequiresCompleteEventsInItsTransaction(t *testing.T) {
	pool := setupTestDB(t)
	requireReliableRuntimeV2Schema(t, pool)
	fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
	store := runtime.NewEventStore(pool)
	for _, sequence := range []int64{1, 3} {
		_, err := store.Append(context.Background(), fixture.principal, fixture.identity, eventStoreRequest(sequence))
		require.NoError(t, err)
	}
	request := successfulRuntimeResult(fixture, map[string]any{"complete": true})
	request.FinalClientEventSeq = 3
	finalizer := runtime.NewResultFinalizer(pool, nil, nil)

	_, err := finalizer.Finalize(context.Background(), fixture.principal, request)
	var resultErr *runtime.RuntimeResultError
	require.ErrorAs(t, err, &resultErr)
	require.Equal(t, runtime.RuntimeResultErrorEventsMissing, resultErr.Code)
	require.Equal(t, []runtime.EventRange{{Start: 2, End: 2}}, resultErr.MissingRanges)
	assertFinalizerAttemptStillExecuting(t, pool, fixture.identity.RunID, fixture.identity.AttemptID)

	_, err = store.Append(context.Background(), fixture.principal, fixture.identity, eventStoreRequest(2))
	require.NoError(t, err)
	ack, err := finalizer.Finalize(context.Background(), fixture.principal, request)
	require.NoError(t, err)
	require.False(t, ack.Replayed)
	require.Equal(t, "success", ack.RunStatus)
	assertFinalizerTerminalCounts(t, pool, fixture.identity.RunID, 1, 0, 0)
}

func TestResultFinalizerRetryScheduleAndRollbackAreAtomic(t *testing.T) {
	t.Run("retry schedule is materialized once", func(t *testing.T) {
		pool := setupTestDB(t)
		requireReliableRuntimeV2Schema(t, pool)
		fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
		finalizer := runtime.NewResultFinalizer(pool, nil, nil)
		request := retryableRuntimeResult(fixture)

		first, err := finalizer.Finalize(context.Background(), fixture.principal, request)
		require.NoError(t, err)
		require.False(t, first.Replayed)
		require.Equal(t, runtime.RuntimeResultClassificationRetryable, first.Classification)
		require.Equal(t, "retry_wait", first.DispatchState)
		require.NotNil(t, first.NextAttemptAt)

		second, err := finalizer.Finalize(context.Background(), fixture.principal, request)
		require.NoError(t, err)
		require.True(t, second.Replayed)
		require.Equal(t, first.NextAttemptAt, second.NextAttemptAt)
		assertFinalizerTerminalCounts(t, pool, fixture.identity.RunID, 0, 0, 0)
		assertFinalizerCapacityReleased(t, pool, fixture)
	})

	t.Run("planner failure rolls back Attempt and Run writes", func(t *testing.T) {
		pool := setupTestDB(t)
		requireReliableRuntimeV2Schema(t, pool)
		fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
		badPlanner := runtime.ResultRetryPlannerFunc(func(int32) time.Duration { return -time.Second })
		finalizer := runtime.NewResultFinalizer(pool, nil, badPlanner)
		_, err := finalizer.Finalize(context.Background(), fixture.principal, retryableRuntimeResult(fixture))
		require.Error(t, err)
		assertFinalizerAttemptStillExecuting(t, pool, fixture.identity.RunID, fixture.identity.AttemptID)
		assertFinalizerTerminalCounts(t, pool, fixture.identity.RunID, 0, 0, 0)
	})

	t.Run("effect insert failure rolls back the entire terminal transaction", func(t *testing.T) {
		pool := setupTestDB(t)
		requireReliableRuntimeV2Schema(t, pool)
		fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
		seedFinalizerEffectTargets(t, pool, fixture.identity.RunID, fixture.identity.AgentID, "run.completed")
		installFailingFinalizerEffectTrigger(t, pool)
		finalizer := runtime.NewResultFinalizer(pool, nil, nil)
		_, err := finalizer.Finalize(
			context.Background(), fixture.principal,
			successfulRuntimeResult(fixture, map[string]any{"rolled_back": true}),
		)
		require.ErrorContains(t, err, "forced finalizer effect failure")
		assertFinalizerAttemptStillExecuting(t, pool, fixture.identity.RunID, fixture.identity.AttemptID)
		assertFinalizerTerminalCounts(t, pool, fixture.identity.RunID, 0, 0, 0)

		var totalCalls int32
		var availabilityRows int
		require.NoError(t, pool.QueryRow(context.Background(), `
			SELECT a.total_calls,
			       (SELECT COUNT(*) FROM agent_availability_snapshots WHERE agent_id = a.id)
			FROM agents a WHERE a.id = $1`, fixture.identity.AgentID).Scan(&totalCalls, &availabilityRows))
		require.Zero(t, totalCalls)
		require.Zero(t, availabilityRows)
	})
}

func TestResultFinalizerRetryExhaustionCreatesOneDLQ(t *testing.T) {
	pool := setupTestDB(t)
	requireReliableRuntimeV2Schema(t, pool)
	fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
	setFinalizerMaxAttempts(t, pool, fixture.identity.RunID, 1)
	finalizer := runtime.NewResultFinalizer(pool, nil, nil)
	request := retryableRuntimeResult(fixture)

	first, err := finalizer.Finalize(context.Background(), fixture.principal, request)
	require.NoError(t, err)
	require.Equal(t, runtime.RuntimeResultClassificationDeadLetter, first.Classification)
	require.Equal(t, "failed", first.RunStatus)
	require.Equal(t, "dead_letter", first.DispatchState)
	second, err := finalizer.Finalize(context.Background(), fixture.principal, request)
	require.NoError(t, err)
	require.True(t, second.Replayed)
	require.Equal(t, runtime.RuntimeResultClassificationDeadLetter, second.Classification)
	assertFinalizerTerminalCounts(t, pool, fixture.identity.RunID, 1, 0, 1)

	var errorCode string
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT error_code FROM runs WHERE id = $1`, fixture.identity.RunID).Scan(&errorCode))
	require.Equal(t, "RUNTIME_RETRY_EXHAUSTED", errorCode)
}

func TestResultFinalizerRunDeadlineWinsAndReplaysTimeout(t *testing.T) {
	pool := setupTestDB(t)
	requireReliableRuntimeV2Schema(t, pool)
	fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
	expireFinalizerFixtureAtDatabaseClock(t, pool, fixture)
	request := successfulRuntimeResult(fixture, map[string]any{"must_not_publish": true})
	request.FinalClientEventSeq = 5 // deadline branch does not require missing spool upload
	finalizer := runtime.NewResultFinalizer(pool, nil, nil)

	first, err := finalizer.Finalize(context.Background(), fixture.principal, request)
	require.NoError(t, err)
	require.Equal(t, runtime.RuntimeResultClassificationTimeout, first.Classification)
	require.Equal(t, "timeout", first.RunStatus)
	require.Equal(t, "terminal", first.DispatchState)
	second, err := finalizer.Finalize(context.Background(), fixture.principal, request)
	require.NoError(t, err)
	require.True(t, second.Replayed)
	require.Equal(t, runtime.RuntimeResultClassificationTimeout, second.Classification)

	var runResultID *uuid.UUID
	var output []byte
	var attemptResultID *uuid.UUID
	var finalSequence *int64
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT r.result_id, r.output, a.result_id, a.final_client_event_seq
		FROM runs r JOIN run_attempts a ON a.id = $2
		WHERE r.id = $1`, fixture.identity.RunID, fixture.identity.AttemptID).Scan(
		&runResultID, &output, &attemptResultID, &finalSequence,
	))
	require.Nil(t, runResultID)
	require.Nil(t, output)
	require.NotNil(t, attemptResultID)
	require.Equal(t, request.ResultID, *attemptResultID)
	require.NotNil(t, finalSequence)
	require.Equal(t, int64(5), *finalSequence)
	assertFinalizerTerminalCounts(t, pool, fixture.identity.RunID, 1, 0, 0)
}

func TestResultFinalizerFailedResultAfterDeadlineKeepsLateErrorPrivate(t *testing.T) {
	pool := setupTestDB(t)
	requireReliableRuntimeV2Schema(t, pool)
	fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
	expireFinalizerFixtureAtDatabaseClock(t, pool, fixture)
	request := retryableRuntimeResult(fixture)
	request.Error.ErrorCode = "LATE_UPSTREAM_SECRET"
	request.Error.Message = "late private detail must not reach the public event"
	finalizer := runtime.NewResultFinalizer(pool, nil, nil)

	ack, err := finalizer.Finalize(context.Background(), fixture.principal, request)
	require.NoError(t, err)
	require.Equal(t, runtime.RuntimeResultClassificationTimeout, ack.Classification)
	require.Equal(t, "timeout", ack.RunStatus)

	var runErrorCode, runErrorMessage string
	var eventPayload []byte
	var attemptErrorCode, attemptErrorDetail *string
	var attemptResultID *uuid.UUID
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT r.error_code, r.error_message, e.payload,
		       a.error_code, a.error_detail_redacted, a.result_id
		FROM runs r
		JOIN run_events e ON e.id = r.terminal_event_id
		JOIN run_attempts a ON a.id = $2
		WHERE r.id = $1`, fixture.identity.RunID, fixture.identity.AttemptID).Scan(
		&runErrorCode, &runErrorMessage, &eventPayload,
		&attemptErrorCode, &attemptErrorDetail, &attemptResultID,
	))
	require.Equal(t, "RUN_DEADLINE_EXCEEDED", runErrorCode)
	require.Equal(t, "Run deadline exceeded", runErrorMessage)
	require.Contains(t, string(eventPayload), `"error_code": "RUN_DEADLINE_EXCEEDED"`)
	require.Contains(t, string(eventPayload), `"error_message": "Run deadline exceeded"`)
	require.NotContains(t, string(eventPayload), request.Error.ErrorCode)
	require.NotContains(t, string(eventPayload), request.Error.Message)
	require.NotNil(t, attemptErrorCode)
	require.Equal(t, request.Error.ErrorCode, *attemptErrorCode)
	require.NotNil(t, attemptErrorDetail)
	require.Equal(t, request.Error.Message, *attemptErrorDetail)
	require.NotNil(t, attemptResultID)
	require.Equal(t, request.ResultID, *attemptResultID)
}

func TestResultFinalizerDelegatedChildPlansOnlyParentAndCallbackEffects(t *testing.T) {
	pool := setupTestDB(t)
	requireReliableRuntimeV2Schema(t, pool)
	parent := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
	child := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
	seedFinalizerEffectTargets(t, pool, child.identity.RunID, child.identity.AgentID, "run.completed")
	_, err := pool.Exec(context.Background(), `
		INSERT INTO run_delegations (child_run_id, parent_run_id, caller_agent_id, reason)
		VALUES ($1, $2, $3, 'finalizer delegated child test')`,
		child.identity.RunID, parent.identity.RunID, parent.identity.AgentID,
	)
	require.NoError(t, err)

	finalizer := runtime.NewResultFinalizer(pool, nil, nil)
	_, err = finalizer.Finalize(
		context.Background(), child.principal,
		successfulRuntimeResult(child, map[string]any{"delegated": true}),
	)
	require.NoError(t, err)
	assertFinalizerTerminalCounts(t, pool, child.identity.RunID, 1, 2, 0)

	rows, err := pool.Query(context.Background(), `
		SELECT effect_type, target_key, max_attempts
		FROM run_effect_outbox WHERE run_id = $1 ORDER BY effect_type`, child.identity.RunID)
	require.NoError(t, err)
	defer rows.Close()
	types := make([]string, 0, 2)
	for rows.Next() {
		var effectType, targetKey string
		var maxAttempts int32
		require.NoError(t, rows.Scan(&effectType, &targetKey, &maxAttempts))
		types = append(types, effectType)
		switch effectType {
		case "run.parent_completion":
			require.Contains(t, targetKey, parent.identity.RunID.String())
			require.Equal(t, int32(12), maxAttempts)
		case "run.task_callback":
			require.Equal(t, int32(3), maxAttempts)
		default:
			t.Fatalf("delegated child planned forbidden effect %q", effectType)
		}
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []string{"run.parent_completion", "run.task_callback"}, types)

	var eventParentID *uuid.UUID
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT parent_run_id FROM run_events
		WHERE run_id = $1 AND payload->>'terminal' = 'true'`, child.identity.RunID).Scan(&eventParentID))
	require.NotNil(t, eventParentID)
	require.Equal(t, parent.identity.RunID, *eventParentID)
}

func successfulRuntimeResult(fixture eventStoreFixture, output map[string]any) runtime.RuntimeResultRequest {
	return runtime.RuntimeResultRequest{
		AttemptIdentity:     fixture.identity,
		ResultID:            uuid.New(),
		Status:              "success",
		Output:              output,
		DurationMS:          25,
		FinalClientEventSeq: 0,
	}
}

func retryableRuntimeResult(fixture eventStoreFixture) runtime.RuntimeResultRequest {
	return runtime.RuntimeResultRequest{
		AttemptIdentity: fixture.identity,
		ResultID:        uuid.New(),
		Status:          "failed",
		Error: &runtime.RuntimeResultFailure{
			ErrorCode:     "TEMPORARY_UNAVAILABLE",
			Message:       "temporary upstream failure",
			RetryableHint: true,
		},
		DurationMS:          30,
		FinalClientEventSeq: 0,
	}
}

func seedFinalizerEffectTargets(
	t *testing.T,
	pool *pgxpool.Pool,
	runID, agentID uuid.UUID,
	eventType string,
) {
	t.Helper()
	var userID uuid.UUID
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT user_id FROM runs WHERE id = $1`, runID).Scan(&userID))
	_, err := pool.Exec(context.Background(), `
		UPDATE agents
		SET webhook_url = 'https://creator.example/finalizer',
		    webhook_secret = 'creator-webhook-secret'
		WHERE id = $1`, agentID)
	require.NoError(t, err)
	_, err = pool.Exec(context.Background(), `
		INSERT INTO delivery_targets (user_id, name, type, config, secret, is_default)
		VALUES ($1, 'Finalizer default', 'webhook',
		        jsonb_build_object('url', 'https://user.example/finalizer', 'event_types', jsonb_build_array($2::text)),
		        'default-delivery-secret', TRUE)`, userID, eventType)
	require.NoError(t, err)
	_, err = pool.Exec(context.Background(), `
		INSERT INTO task_callback_subscriptions (
			run_id, owner_user_id, target_url, secret, event_types, metadata
		) VALUES (
			$1, $2, 'https://caller.example/finalizer', 'callback-secret', ARRAY[$3]::text[], '{}'::jsonb
		)`, runID, userID, eventType)
	require.NoError(t, err)
}

func setFinalizerMaxAttempts(t *testing.T, pool *pgxpool.Pool, runID uuid.UUID, maxAttempts int32) {
	t.Helper()
	err := pgx.BeginTxFunc(context.Background(), pool, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
		if _, err := tx.Exec(context.Background(), `SET LOCAL session_replication_role = replica`); err != nil {
			return err
		}
		_, err := tx.Exec(context.Background(), `UPDATE runs SET max_attempts = $2 WHERE id = $1`, runID, maxAttempts)
		return err
	})
	if err != nil && strings.Contains(err.Error(), "permission denied") {
		t.Skipf("test database cannot adjust immutable retry fixture: %v", err)
	}
	require.NoError(t, err)
}

func installFailingFinalizerEffectTrigger(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
		CREATE OR REPLACE FUNCTION fail_finalizer_effect_insert()
		RETURNS trigger AS $function$
		BEGIN
			RAISE EXCEPTION 'forced finalizer effect failure';
		END
		$function$ LANGUAGE plpgsql`)
	require.NoError(t, err)
	_, err = pool.Exec(context.Background(), `
		CREATE TRIGGER fail_finalizer_effect_insert
		BEFORE INSERT ON run_effect_outbox
		FOR EACH ROW EXECUTE FUNCTION fail_finalizer_effect_insert()`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DROP TRIGGER IF EXISTS fail_finalizer_effect_insert ON run_effect_outbox`)
		_, _ = pool.Exec(context.Background(), `DROP FUNCTION IF EXISTS fail_finalizer_effect_insert()`)
	})
}

func assertFinalizerTerminalCounts(
	t *testing.T,
	pool *pgxpool.Pool,
	runID uuid.UUID,
	terminalEvents, effects, deadLetters int,
) {
	t.Helper()
	var gotEvents, gotLedgers, gotEffects, gotDeadLetters int
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT
			(SELECT COUNT(*) FROM run_events WHERE run_id = $1 AND payload->>'terminal' = 'true'),
			(SELECT COUNT(*) FROM run_accounting_ledger WHERE run_id = $1),
			(SELECT COUNT(*) FROM run_effect_outbox WHERE run_id = $1),
			(SELECT COUNT(*) FROM run_dead_letters WHERE run_id = $1)`, runID).Scan(
		&gotEvents, &gotLedgers, &gotEffects, &gotDeadLetters,
	))
	require.Equal(t, terminalEvents, gotEvents)
	if terminalEvents == 0 {
		require.Zero(t, gotLedgers)
	} else {
		require.Equal(t, 1, gotLedgers)
	}
	require.Equal(t, effects, gotEffects)
	require.Equal(t, deadLetters, gotDeadLetters)
}

func assertFinalizerCapacityReleased(t *testing.T, pool *pgxpool.Pool, fixture eventStoreFixture) {
	t.Helper()
	var slotReleasedAt *time.Time
	var activeSessionID *uuid.UUID
	var sessionInflight, nodeInflight int32
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT a.slot_released_at, a.active_runtime_session_id, s.inflight, n.inflight
		FROM run_attempts a
		JOIN runtime_sessions s ON s.runtime_session_id = a.runtime_session_id
		JOIN runtime_nodes n ON n.node_id = a.node_id
		WHERE a.run_id = $1 AND a.id = $2`, fixture.identity.RunID, fixture.identity.AttemptID).Scan(
		&slotReleasedAt, &activeSessionID, &sessionInflight, &nodeInflight,
	))
	require.NotNil(t, slotReleasedAt)
	require.Nil(t, activeSessionID)
	require.Zero(t, sessionInflight)
	require.Zero(t, nodeInflight)
}

func assertFinalizerAttemptStillExecuting(t *testing.T, pool *pgxpool.Pool, runID, attemptID uuid.UUID) {
	t.Helper()
	var dispatchState string
	var activeAttemptID *uuid.UUID
	var resultID *uuid.UUID
	var finishedAt *time.Time
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT r.dispatch_state, r.active_attempt_id, a.result_id, a.finished_at
		FROM runs r JOIN run_attempts a ON a.id = $2
		WHERE r.id = $1`, runID, attemptID).Scan(&dispatchState, &activeAttemptID, &resultID, &finishedAt))
	require.Equal(t, "executing", dispatchState)
	require.NotNil(t, activeAttemptID)
	require.Equal(t, attemptID, *activeAttemptID)
	require.Nil(t, resultID)
	require.Nil(t, finishedAt)
}

func expireFinalizerFixtureAtDatabaseClock(t *testing.T, pool *pgxpool.Pool, fixture eventStoreFixture) {
	t.Helper()
	err := pgx.BeginTxFunc(context.Background(), pool, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
		if _, err := tx.Exec(context.Background(), `SET LOCAL session_replication_role = replica`); err != nil {
			return fmt.Errorf("disable fixture immutability triggers: %w", err)
		}
		var offeredAt time.Time
		if err := tx.QueryRow(context.Background(), `SELECT offered_at FROM run_attempts WHERE id = $1`, fixture.identity.AttemptID).Scan(&offeredAt); err != nil {
			return err
		}
		expiresAt := offeredAt.Add(20 * time.Millisecond)
		if _, err := tx.Exec(context.Background(), `
			UPDATE run_attempts
			SET lease_expires_at = $2, attempt_deadline_at = $2
			WHERE id = $1`, fixture.identity.AttemptID, expiresAt); err != nil {
			return err
		}
		if _, err := tx.Exec(context.Background(), `
			UPDATE runs
			SET dispatch_deadline_at = $2,
			    run_deadline_at = $3,
			    lease_expires_at = $3,
			    attempt_deadline_at = $3
			WHERE id = $1`, fixture.identity.RunID, offeredAt, expiresAt); err != nil {
			return err
		}
		return nil
	})
	if err != nil && strings.Contains(err.Error(), "permission denied") {
		t.Skipf("test database cannot create an expired immutable fixture: %v", err)
	}
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		var expired bool
		err := pool.QueryRow(context.Background(), `
			SELECT clock_timestamp() >= run_deadline_at FROM runs WHERE id = $1`, fixture.identity.RunID).Scan(&expired)
		return err == nil && expired
	}, time.Second, 5*time.Millisecond)
}
