package runtime_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

func TestRuntimeCancellationV2AgentNodeLifecycle(t *testing.T) {
	pool := setupTestDB(t)
	requireRuntimeCancellationV2Schema(t, pool)
	fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
	seedFinalizerEffectTargets(t, pool, fixture.identity.RunID, fixture.identity.AgentID, "run.canceled")

	var ownerID uuid.UUID
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT user_id FROM runs WHERE id = $1`, fixture.identity.RunID).Scan(&ownerID))

	coordinator := runtime.NewRuntimeCancellationCoordinator(pool)
	created, err := coordinator.CancelOwnedRun(
		context.Background(), ownerID, fixture.identity.RunID, "Owner no longer needs this run",
	)
	require.NoError(t, err)
	require.False(t, created.Replayed)
	require.Equal(t, "canceled", created.Run.Status)
	require.Equal(t, "requested", created.Cancellation.State)
	require.NotNil(t, created.Cancellation.TargetAttemptID)
	require.Equal(t, fixture.identity.AttemptID, *created.Cancellation.TargetAttemptID)

	assertRuntimeCancellationAttemptCapacity(t, pool, fixture, false, 1, 1)
	assertRuntimeCancellationTerminalFacts(t, pool, fixture.identity.RunID, 1, 1, 3, 1)
	var availabilityFailures int
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT COUNT(*) FROM agent_availability_snapshots
		WHERE agent_id = $1 AND consecutive_failures > 0`, fixture.identity.AgentID).Scan(&availabilityFailures))
	require.Zero(t, availabilityFailures, "owner cancellation is not Agent failure evidence")

	replayed, err := coordinator.CancelOwnedRun(
		context.Background(), ownerID, fixture.identity.RunID, "A replay cannot rewrite the reason",
	)
	require.NoError(t, err)
	require.True(t, replayed.Replayed)
	require.Equal(t, created.Cancellation.ID, replayed.Cancellation.ID)
	assertRuntimeCancellationTerminalFacts(t, pool, fixture.identity.RunID, 1, 1, 3, 1)

	principal := runtimeCancellationSessionPrincipal(t, pool, fixture)
	commands, err := coordinator.PollCommands(context.Background(), principal)
	require.NoError(t, err)
	require.Len(t, commands.Commands, 1)
	require.False(t, commands.DatabaseTime.IsZero())
	decoded, err := runtime.DecodePendingCommand(commands.Commands[0])
	require.NoError(t, err)
	require.NotNil(t, decoded.Cancel)
	require.Equal(t, created.Cancellation.ID, decoded.Cancel.CancellationID)
	require.Equal(t, fixture.identity.RunID, decoded.Cancel.AttemptIdentity.RunID)
	require.Equal(t, fixture.identity.FencingToken, decoded.Cancel.AttemptIdentity.FencingToken)

	stopping, err := coordinator.AckCancel(context.Background(), principal, runtime.RunCancelAckPayload{
		CancellationID:  decoded.Cancel.CancellationID,
		AttemptIdentity: decoded.Cancel.AttemptIdentity,
		CancelState:     runtime.RuntimeCancelStopping,
	})
	require.NoError(t, err)
	require.Equal(t, runtime.RuntimeCancelStopping, stopping.CancelState)
	assertRuntimeCancellationAttemptCapacity(t, pool, fixture, false, 1, 1)

	stoppedRequest := runtime.RunCancelAckPayload{
		CancellationID:  decoded.Cancel.CancellationID,
		AttemptIdentity: decoded.Cancel.AttemptIdentity,
		CancelState:     runtime.RuntimeCancelStopped,
	}
	stopped, err := coordinator.AckCancel(context.Background(), principal, stoppedRequest)
	require.NoError(t, err)
	require.Equal(t, runtime.RuntimeCancelStopped, stopped.CancelState)
	assertRuntimeCancellationAttemptCapacity(t, pool, fixture, true, 0, 0)

	replayedStop, err := coordinator.AckCancel(context.Background(), principal, stoppedRequest)
	require.NoError(t, err)
	require.Equal(t, stopped, replayedStop)
	assertRuntimeCancellationAttemptCapacity(t, pool, fixture, true, 0, 0)
	assertRuntimeCancellationTerminalFacts(t, pool, fixture.identity.RunID, 1, 1, 3, 1)
}

func TestRuntimeCancellationV2RollsBackEveryTerminalFactTogether(t *testing.T) {
	pool := setupTestDB(t)
	requireRuntimeCancellationV2Schema(t, pool)
	fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
	seedFinalizerEffectTargets(t, pool, fixture.identity.RunID, fixture.identity.AgentID, "run.canceled")
	installFailingFinalizerEffectTrigger(t, pool)

	var ownerID uuid.UUID
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT user_id FROM runs WHERE id = $1`, fixture.identity.RunID).Scan(&ownerID))

	_, err := runtime.NewRuntimeCancellationCoordinator(pool).CancelOwnedRun(
		context.Background(), ownerID, fixture.identity.RunID, "must roll back",
	)
	require.Error(t, err)

	var status, dispatchState string
	var activeAttemptID *uuid.UUID
	var cancellations, terminalEvents, ledgers, effects, cancelSignals int
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT r.status, r.dispatch_state, r.active_attempt_id,
		       (SELECT COUNT(*) FROM run_cancellations WHERE run_id = r.id),
		       (SELECT COUNT(*) FROM run_events
		         WHERE run_id = r.id AND event_type = 'run.canceled'),
		       (SELECT COUNT(*) FROM run_accounting_ledger WHERE run_id = r.id),
		       (SELECT COUNT(*) FROM run_effect_outbox WHERE run_id = r.id),
		       (SELECT COUNT(*) FROM runtime_signal_outbox
		         WHERE run_id = r.id AND event_type = 'run.cancel')
		FROM runs r WHERE r.id = $1`, fixture.identity.RunID).Scan(
		&status, &dispatchState, &activeAttemptID,
		&cancellations, &terminalEvents, &ledgers, &effects, &cancelSignals,
	))
	require.Equal(t, "running", status)
	require.Equal(t, "executing", dispatchState)
	require.NotNil(t, activeAttemptID)
	require.Equal(t, fixture.identity.AttemptID, *activeAttemptID)
	require.Zero(t, cancellations)
	require.Zero(t, terminalEvents)
	require.Zero(t, ledgers)
	require.Zero(t, effects)
	require.Zero(t, cancelSignals)
	assertRuntimeCancellationAttemptCapacity(t, pool, fixture, false, 1, 1)
}

func TestServiceCancelRunRoutesRuntimeV2ToDurableCoordinator(t *testing.T) {
	pool := setupTestDB(t)
	requireRuntimeCancellationV2Schema(t, pool)
	fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)

	var ownerID uuid.UUID
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT user_id FROM runs WHERE id = $1`, fixture.identity.RunID).Scan(&ownerID))

	response, err := newTestService(t, pool).CancelRun(context.Background(), ownerID, fixture.identity.RunID)
	require.NoError(t, err)
	require.Equal(t, fixture.identity.RunID.String(), response.RunID)
	require.Equal(t, "canceled", response.Status)
	require.Equal(t, "CANCELED", response.ErrorCode)
	assertRuntimeCancellationAttemptCapacity(t, pool, fixture, false, 1, 1)
	assertRuntimeCancellationTerminalFacts(t, pool, fixture.identity.RunID, 1, 1, 0, 1)
}

func TestRuntimeCancellationV2DeadlineReaperStopsDeliveryAndReleasesAtomically(t *testing.T) {
	pool := setupTestDB(t)
	requireRuntimeCancellationV2Schema(t, pool)
	fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
	coordinator := runtime.NewRuntimeCancellationCoordinator(pool)

	var ownerID uuid.UUID
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT user_id FROM runs WHERE id = $1`, fixture.identity.RunID).Scan(&ownerID))
	created, err := coordinator.CancelOwnedRun(
		context.Background(), ownerID, fixture.identity.RunID, "deadline reaper",
	)
	require.NoError(t, err)
	principal := runtimeCancellationSessionPrincipal(t, pool, fixture)
	commands, err := coordinator.PollCommands(context.Background(), principal)
	require.NoError(t, err)
	require.Len(t, commands.Commands, 1)

	expireRuntimeCancellationRequestAtDatabaseClock(t, pool, fixture.identity.RunID, created.Cancellation.ID)
	overdueCommands, err := coordinator.PollCommands(context.Background(), principal)
	require.NoError(t, err)
	require.Empty(t, overdueCommands.Commands, "an expired stop command must not be redelivered")

	reaped, err := coordinator.ReapExpiredCancellation(context.Background())
	require.NoError(t, err)
	require.NotNil(t, reaped)
	require.Equal(t, runtime.RuntimeCancelUnconfirmed, reaped.CancelState)
	require.Equal(t, "CANCEL_UNCONFIRMED", reaped.ErrorCode)
	assertRuntimeCancellationAttemptCapacity(t, pool, fixture, true, 0, 0)

	again, err := coordinator.ReapExpiredCancellation(context.Background())
	require.NoError(t, err)
	require.Nil(t, again)
	assertRuntimeCancellationAttemptCapacity(t, pool, fixture, true, 0, 0)

	var cancellationState, runCancelState string
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT c.state, r.cancel_state
		FROM run_cancellations c
		JOIN runs r ON r.id = c.run_id AND r.cancel_request_id = c.id
		WHERE c.run_id = $1`, fixture.identity.RunID).Scan(&cancellationState, &runCancelState))
	require.Equal(t, "unconfirmed", cancellationState)
	require.Equal(t, cancellationState, runCancelState)
}

func TestRuntimeCancellationV2CoreAttemptEndsOnlyAfterExecutionStops(t *testing.T) {
	pool := setupTestDB(t)
	requireRuntimeCancellationV2Schema(t, pool)
	fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
	convertRuntimeCancellationFixtureToCoreHTTP(t, pool, fixture)

	var ownerID uuid.UUID
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT user_id FROM runs WHERE id = $1`, fixture.identity.RunID).Scan(&ownerID))
	result, err := runtime.NewRuntimeCancellationCoordinator(pool).CancelOwnedRun(
		context.Background(), ownerID, fixture.identity.RunID, "stop Core execution",
	)
	require.NoError(t, err)
	require.Equal(t, "canceled", result.Run.Status)
	require.Equal(t, "requested", result.Cancellation.State)

	var finishedAt *time.Time
	var outcome *string
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT finished_at, outcome FROM run_attempts
		WHERE run_id = $1 AND id = $2`, fixture.identity.RunID, fixture.identity.AttemptID).Scan(
		&finishedAt, &outcome,
	))
	require.Nil(t, finishedAt)
	require.Nil(t, outcome)

	coreIdentity := fixture.identity
	coreIdentity.NodeID = nil
	coreIdentity.WorkerID = nil
	coreIdentity.RuntimeSessionID = nil
	stopped, err := runtime.NewRuntimeCancellationCoordinator(pool).AcknowledgeCoreStopped(
		context.Background(), fixture.coreInstanceID, coreIdentity,
	)
	require.NoError(t, err)
	require.Equal(t, runtime.RuntimeCancelStopped, stopped.CancelState)
	assertRuntimeCancellationTerminalFacts(t, pool, fixture.identity.RunID, 1, 1, 0, 1)
}

func TestRuntimeCancellationV2CoreCrashBecomesUnconfirmedAtDatabaseDeadline(t *testing.T) {
	pool := setupTestDB(t)
	requireRuntimeCancellationV2Schema(t, pool)
	fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
	convertRuntimeCancellationFixtureToCoreHTTP(t, pool, fixture)
	var ownerID uuid.UUID
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT user_id FROM runs WHERE id = $1`, fixture.identity.RunID).Scan(&ownerID))
	coordinator := runtime.NewRuntimeCancellationCoordinator(pool)
	created, err := coordinator.CancelOwnedRun(
		context.Background(), ownerID, fixture.identity.RunID, "Core process disappeared",
	)
	require.NoError(t, err)
	require.Equal(t, "requested", created.Cancellation.State)
	expireRuntimeCancellationRequestAtDatabaseClock(
		t, pool, fixture.identity.RunID, created.Cancellation.ID,
	)

	state, err := coordinator.ReapExpiredCancellation(context.Background())
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, runtime.RuntimeCancelUnconfirmed, state.CancelState)
	var outcome, errorCode string
	var finished bool
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT outcome, error_code, finished_at IS NOT NULL
		FROM run_attempts WHERE run_id = $1 AND id = $2`,
		fixture.identity.RunID, fixture.identity.AttemptID,
	).Scan(&outcome, &errorCode, &finished))
	require.Equal(t, "canceled", outcome)
	require.Equal(t, "CANCEL_UNCONFIRMED", errorCode)
	require.True(t, finished)
	assertRuntimeCancellationTerminalFacts(t, pool, fixture.identity.RunID, 1, 1, 0, 1)
}

func TestRuntimeCancellationV2ConcurrentOwnerRequestsHaveOneWinner(t *testing.T) {
	pool := setupTestDB(t)
	requireRuntimeCancellationV2Schema(t, pool)
	fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
	var ownerID uuid.UUID
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT user_id FROM runs WHERE id = $1`, fixture.identity.RunID).Scan(&ownerID))

	coordinator := runtime.NewRuntimeCancellationCoordinator(pool)
	const workers = 32
	type cancellationResult struct {
		result runtime.RuntimeCancellationResult
		err    error
	}
	results := make(chan cancellationResult, workers)
	start := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var wait sync.WaitGroup
	wait.Add(workers)
	for range workers {
		go func() {
			defer wait.Done()
			<-start
			result, err := coordinator.CancelOwnedRun(ctx, ownerID, fixture.identity.RunID, "concurrent owner cancel")
			results <- cancellationResult{result: result, err: err}
		}()
	}
	close(start)
	wait.Wait()
	close(results)

	winners, replays := 0, 0
	var cancellationID uuid.UUID
	for got := range results {
		require.NoError(t, got.err)
		if cancellationID == uuid.Nil {
			cancellationID = got.result.Cancellation.ID
		}
		require.Equal(t, cancellationID, got.result.Cancellation.ID)
		if got.result.Replayed {
			replays++
		} else {
			winners++
		}
	}
	require.Equal(t, 1, winners)
	require.Equal(t, workers-1, replays)
	assertRuntimeCancellationAttemptCapacity(t, pool, fixture, false, 1, 1)
	assertRuntimeCancellationTerminalFacts(t, pool, fixture.identity.RunID, 1, 1, 0, 1)
}

func TestRuntimeCancellationV2CancelResultRace1000Contenders(t *testing.T) {
	pool := setupTestDB(t)
	requireRuntimeCancellationV2Schema(t, pool)
	fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
	var ownerID uuid.UUID
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT user_id FROM runs WHERE id = $1`, fixture.identity.RunID).Scan(&ownerID))

	coordinator := runtime.NewRuntimeCancellationCoordinator(pool)
	finalizer := runtime.NewResultFinalizer(pool, nil, nil)
	request := successfulRuntimeResult(fixture, map[string]any{"winner": "result"})
	const contenders = 1000
	start := make(chan struct{})
	errorsSeen := make(chan error, contenders)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var wait sync.WaitGroup
	wait.Add(contenders)
	for index := range contenders {
		index := index
		go func() {
			defer wait.Done()
			<-start
			if index%2 == 0 {
				_, err := finalizer.Finalize(ctx, fixture.principal, request)
				if err != nil &&
					!runtime.IsRuntimeResultError(err, runtime.RuntimeResultErrorRunAlreadyTerminal) &&
					!runtime.IsRuntimeResultError(err, runtime.RuntimeResultErrorRunCancelRequested) {
					errorsSeen <- err
				}
				return
			}
			_, err := coordinator.CancelOwnedRun(ctx, ownerID, fixture.identity.RunID, "cancel/result race")
			if err != nil && !errors.Is(err, runtime.ErrRuntimeCancellationRunEnded) {
				errorsSeen <- err
			}
		}()
	}
	close(start)
	wait.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		require.NoError(t, err)
	}

	var status, dispatchState string
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT status, dispatch_state FROM runs WHERE id = $1`, fixture.identity.RunID).Scan(
		&status, &dispatchState,
	))
	require.Contains(t, []string{"success", "canceled"}, status)
	require.Equal(t, "terminal", dispatchState)
	var terminalEvents, ledgerRows int
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT
			(SELECT COUNT(*)::int FROM run_events WHERE run_id = $1 AND event_type IN ('run.completed', 'run.canceled')),
			(SELECT COUNT(*)::int FROM run_accounting_ledger WHERE run_id = $1)`, fixture.identity.RunID).Scan(
		&terminalEvents, &ledgerRows,
	))
	require.Equal(t, 1, terminalEvents)
	require.Equal(t, 1, ledgerRows)
}

func TestRuntimeCancellationV2CancelAckRace1000Contenders(t *testing.T) {
	pool := setupTestDB(t)
	requireRuntimeCancellationV2Schema(t, pool)
	fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
	var ownerID uuid.UUID
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT user_id FROM runs WHERE id = $1`, fixture.identity.RunID).Scan(&ownerID))
	coordinator := runtime.NewRuntimeCancellationCoordinator(pool)
	created, err := coordinator.CancelOwnedRun(
		context.Background(), ownerID, fixture.identity.RunID, "cancel/ack race",
	)
	require.NoError(t, err)
	principal := runtimeCancellationSessionPrincipal(t, pool, fixture)
	commands, err := coordinator.PollCommands(context.Background(), principal)
	require.NoError(t, err)
	require.Len(t, commands.Commands, 1)
	command, err := runtime.DecodePendingCommand(commands.Commands[0])
	require.NoError(t, err)
	require.NotNil(t, command.Cancel)
	ack := runtime.RunCancelAckPayload{
		CancellationID:  command.Cancel.CancellationID,
		AttemptIdentity: command.Cancel.AttemptIdentity,
		CancelState:     runtime.RuntimeCancelStopped,
	}

	const contenders = 1000
	start := make(chan struct{})
	errorsSeen := make(chan error, contenders)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var wait sync.WaitGroup
	wait.Add(contenders)
	for index := range contenders {
		index := index
		go func() {
			defer wait.Done()
			<-start
			if index%2 == 0 {
				_, err := coordinator.CancelOwnedRun(ctx, ownerID, fixture.identity.RunID, "cancel replay")
				if err != nil {
					errorsSeen <- err
				}
				return
			}
			_, err := coordinator.AckCancel(ctx, principal, ack)
			if err != nil {
				errorsSeen <- err
			}
		}()
	}
	close(start)
	wait.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		require.NoError(t, err)
	}

	state, err := coordinator.AckCancel(context.Background(), principal, ack)
	require.NoError(t, err)
	require.Equal(t, runtime.RuntimeCancelStopped, state.CancelState)
	require.Equal(t, created.Cancellation.ID, state.CancellationID)
	assertRuntimeCancellationAttemptCapacity(t, pool, fixture, true, 0, 0)
	assertRuntimeCancellationTerminalFacts(t, pool, fixture.identity.RunID, 1, 1, 0, 1)
}

func requireRuntimeCancellationV2Schema(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	var version int32
	var migration string
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT schema_version, migration_name
		FROM runtime_schema_contracts
		WHERE runtime_contract_id = 'openlinker.runtime.v2' AND is_current`).Scan(&version, &migration))
	if version < 65 {
		t.Skipf("runtime cancellation v2 migration is not installed: version=%d migration=%s", version, migration)
	}
}

func runtimeCancellationSessionPrincipal(
	t *testing.T,
	pool *pgxpool.Pool,
	fixture eventStoreFixture,
) runtime.RuntimeSessionPrincipal {
	t.Helper()
	require.NotNil(t, fixture.identity.RuntimeSessionID)
	require.NotNil(t, fixture.identity.NodeID)
	require.NotNil(t, fixture.identity.WorkerID)
	principal := runtime.RuntimeSessionPrincipal{
		RuntimeSessionID: *fixture.identity.RuntimeSessionID,
		NodeID:           *fixture.identity.NodeID,
		AgentID:          fixture.identity.AgentID,
		CredentialID:     fixture.credentialID,
		WorkerID:         *fixture.identity.WorkerID,
		CoreInstanceID:   fixture.coreInstanceID,
	}
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT s.session_epoch, s.device_certificate_serial,
		       n.device_public_key_thumbprint, attachment.id, s.status, clock_timestamp()
		FROM runtime_sessions s
		JOIN runtime_nodes n ON n.node_id = s.node_id
		JOIN runtime_session_attachments attachment
		  ON attachment.runtime_session_id = s.runtime_session_id
		 AND attachment.core_instance_id = s.attached_core_instance_id
		 AND attachment.detached_at IS NULL
		WHERE s.runtime_session_id = $1`, principal.RuntimeSessionID).Scan(
		&principal.SessionEpoch,
		&principal.DeviceCertificateSerial,
		&principal.DevicePublicKeyThumbprintSHA256,
		&principal.AttachmentID,
		&principal.Status,
		&principal.DatabaseTime,
	))
	return principal
}

func expireRuntimeCancellationRequestAtDatabaseClock(
	t *testing.T,
	pool *pgxpool.Pool,
	runID, cancellationID uuid.UUID,
) {
	t.Helper()
	err := pgx.BeginTxFunc(context.Background(), pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if _, err := tx.Exec(context.Background(), `SET LOCAL session_replication_role = replica`); err != nil {
			return err
		}
		if _, err := tx.Exec(context.Background(), `
			UPDATE run_cancellations
			SET requested_at = clock_timestamp() - INTERVAL '1 minute',
			    updated_at = clock_timestamp() - INTERVAL '1 minute'
			WHERE run_id = $1 AND id = $2`, runID, cancellationID); err != nil {
			return err
		}
		_, err := tx.Exec(context.Background(), `
			UPDATE runs
			SET cancel_requested_at = (
				SELECT requested_at FROM run_cancellations
				WHERE run_id = $1 AND id = $2
			)
			WHERE id = $1 AND cancel_request_id = $2`, runID, cancellationID)
		return err
	})
	if err != nil {
		t.Skipf("test database cannot age immutable cancellation evidence: %v", err)
	}
}

func convertRuntimeCancellationFixtureToCoreHTTP(
	t *testing.T,
	pool *pgxpool.Pool,
	fixture eventStoreFixture,
) {
	t.Helper()
	err := pgx.BeginTxFunc(context.Background(), pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if _, err := tx.Exec(context.Background(), `SET LOCAL session_replication_role = replica`); err != nil {
			return err
		}
		if _, err := tx.Exec(context.Background(), `
			UPDATE run_attempts
			SET executor_type = 'core_http',
			    runtime_token_id = NULL,
			    runtime_worker_id = NULL,
			    runtime_session_id = NULL,
			    node_id = NULL,
			    slot_acquired_at = NULL,
			    slot_released_at = NULL,
			    active_runtime_session_id = NULL
			WHERE run_id = $1 AND id = $2`, fixture.identity.RunID, fixture.identity.AttemptID); err != nil {
			return err
		}
		if _, err := tx.Exec(context.Background(), `
			UPDATE runs
			SET connection_mode_snapshot = 'direct_http',
			    endpoint_idempotency_snapshot = TRUE,
			    executor_type = 'core_http',
			    runtime_node_id = NULL,
			    runtime_worker_id = NULL,
			    runtime_session_id = NULL,
			    lease_token_id = NULL
			WHERE id = $1`, fixture.identity.RunID); err != nil {
			return err
		}
		if _, err := tx.Exec(context.Background(), `
			UPDATE runtime_sessions SET inflight = 0
			WHERE runtime_session_id = $1`, *fixture.identity.RuntimeSessionID); err != nil {
			return err
		}
		_, err := tx.Exec(context.Background(), `
			UPDATE runtime_nodes SET inflight = 0
			WHERE node_id = $1`, *fixture.identity.NodeID)
		return err
	})
	if err != nil {
		t.Skipf("test database cannot convert immutable fixture to Core execution: %v", err)
	}
}

func assertRuntimeCancellationAttemptCapacity(
	t *testing.T,
	pool *pgxpool.Pool,
	fixture eventStoreFixture,
	wantFinished bool,
	wantSessionInflight, wantNodeInflight int32,
) {
	t.Helper()
	var finishedAt, slotReleasedAt *time.Time
	var outcome *string
	var activeSessionID *uuid.UUID
	var sessionInflight, nodeInflight int32
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT a.finished_at, a.outcome, a.slot_released_at,
		       a.active_runtime_session_id, s.inflight, n.inflight
		FROM run_attempts a
		JOIN runtime_sessions s ON s.runtime_session_id = a.runtime_session_id
		JOIN runtime_nodes n ON n.node_id = a.node_id
		WHERE a.run_id = $1 AND a.id = $2`,
		fixture.identity.RunID, fixture.identity.AttemptID,
	).Scan(&finishedAt, &outcome, &slotReleasedAt, &activeSessionID, &sessionInflight, &nodeInflight))
	require.Equal(t, wantSessionInflight, sessionInflight)
	require.Equal(t, wantNodeInflight, nodeInflight)
	if wantFinished {
		require.NotNil(t, finishedAt)
		require.NotNil(t, outcome)
		require.Equal(t, "canceled", *outcome)
		require.NotNil(t, slotReleasedAt)
		require.Nil(t, activeSessionID)
		return
	}
	require.Nil(t, finishedAt)
	require.Nil(t, outcome)
	require.Nil(t, slotReleasedAt)
	require.NotNil(t, activeSessionID)
}

func assertRuntimeCancellationTerminalFacts(
	t *testing.T,
	pool *pgxpool.Pool,
	runID uuid.UUID,
	wantEvents, wantLedgers, wantEffects, wantSignals int,
) {
	t.Helper()
	var events, ledgers, effects, signals int
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT
			(SELECT COUNT(*) FROM run_events
			 WHERE run_id = $1 AND event_type = 'run.canceled' AND payload->>'terminal' = 'true'),
			(SELECT COUNT(*) FROM run_accounting_ledger WHERE run_id = $1),
			(SELECT COUNT(*) FROM run_effect_outbox WHERE run_id = $1),
			(SELECT COUNT(*) FROM runtime_signal_outbox
			 WHERE run_id = $1 AND event_type = 'run.cancel')`, runID).Scan(
		&events, &ledgers, &effects, &signals,
	))
	require.Equal(t, wantEvents, events)
	require.Equal(t, wantLedgers, ledgers)
	require.Equal(t, wantEffects, effects)
	require.Equal(t, wantSignals, signals)
}
