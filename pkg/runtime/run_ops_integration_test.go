package runtime_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

func TestRuntimeDeadLetterReplayIsAppendOnlyAndIdempotent(t *testing.T) {
	pool := setupTestDB(t)
	requireReliableRuntimeSchema(t, pool)
	fixture := insertEventStoreExecutingAttempt(t, pool, 20*time.Millisecond)
	setFinalizerMaxAttempts(t, pool, fixture.identity.RunID, 1)
	seedFinalizerEffectTargets(t, pool, fixture.identity.RunID, fixture.identity.AgentID, "run.failed")
	waitForRuntimeLeaseDue(t, pool, fixture.identity.AttemptID)

	result, err := runtime.NewRuntimeDeadlineReconciler(pool, nil).ReconcileBatch(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, 1, result.DeadLettered)

	var ownerID uuid.UUID
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT user_id FROM runs WHERE id = $1`, fixture.identity.RunID,
	).Scan(&ownerID))
	queries := db.New(pool)
	_, err = queries.UpsertA2AContextMapping(context.Background(), db.UpsertA2AContextMappingParams{
		RunID: fixture.identity.RunID, UserID: ownerID, AgentID: fixture.identity.AgentID,
		ProtocolContextID: "replay-conversation", ProtocolTaskID: "replay-source-task",
		RootContextID: "replay-conversation", TraceID: "replay-trace", Source: "a2a_protocol",
	})
	require.NoError(t, err)
	// Replay uses the current Agent configuration. Keep this fixture on the
	// queued v2 path so the test does not depend on an external endpoint.
	_, err = pool.Exec(context.Background(), `
		UPDATE agents
		SET connection_mode = 'runtime',
		    endpoint_url = 'openlinker-runtime://run-ops-test'
		WHERE id = $1`,
		fixture.identity.AgentID)
	require.NoError(t, err)
	_, err = pool.Exec(context.Background(), `
		UPDATE agent_tokens SET last_used_at = clock_timestamp() WHERE id = $1`,
		fixture.credentialID)
	require.NoError(t, err)

	var sourceStatus, sourceDispatch string
	var sourceAttempts, sourceEvents, sourceDLQ int32
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT r.status, r.dispatch_state,
		       (SELECT COUNT(*)::int FROM run_attempts WHERE run_id = r.id),
		       (SELECT COUNT(*)::int FROM run_events WHERE run_id = r.id),
		       (SELECT COUNT(*)::int FROM run_dead_letters WHERE run_id = r.id)
		FROM runs r WHERE r.id = $1`, fixture.identity.RunID).Scan(
		&sourceStatus, &sourceDispatch, &sourceAttempts, &sourceEvents, &sourceDLQ,
	))
	require.Equal(t, "failed", sourceStatus)
	require.Equal(t, "dead_letter", sourceDispatch)
	require.Equal(t, int32(1), sourceDLQ)

	svc := newTestService(t, pool)
	const workers = 24
	type replayResult struct {
		response *runtime.RunResponse
		err      error
	}
	results := make(chan replayResult, workers)
	start := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(workers)
	for range workers {
		go func() {
			defer wait.Done()
			<-start
			response, replayErr := svc.ReplayRun(
				context.Background(), ownerID, fixture.identity.RunID, "dlq-replay-once", "api",
			)
			results <- replayResult{response: response, err: replayErr}
		}()
	}
	close(start)
	wait.Wait()
	close(results)

	createdRunID := ""
	replayedResponses := 0
	for item := range results {
		require.NoError(t, item.err)
		require.NotNil(t, item.response)
		if createdRunID == "" {
			createdRunID = item.response.RunID
		}
		require.Equal(t, createdRunID, item.response.RunID)
		require.Equal(t, fixture.identity.RunID.String(), item.response.ReplayOfRunID)
		if item.response.Replayed {
			replayedResponses++
		}
	}
	require.Equal(t, workers-1, replayedResponses)
	require.NotEqual(t, fixture.identity.RunID.String(), createdRunID)

	createdID := uuid.MustParse(createdRunID)
	created, err := svc.GetRun(context.Background(), ownerID, createdID)
	require.NoError(t, err)
	require.Equal(t, runtime.RuntimeContractID, created.RuntimeContractID)
	require.Equal(t, "pending", created.DispatchState)
	require.Zero(t, created.AttemptCount)
	require.Equal(t, int32(3), created.MaxAttempts)
	require.Equal(t, fixture.identity.RunID.String(), created.ReplayOfRunID)
	require.Empty(t, created.ActiveAttemptID)
	require.Nil(t, created.DeadLetteredAt)
	replayedContext, err := queries.GetA2AContextMappingByRun(context.Background(), createdID)
	require.NoError(t, err)
	require.Equal(t, "replay-conversation", replayedContext.ProtocolContextID)
	require.Equal(t, "replay-conversation", replayedContext.RootContextID)
	require.Equal(t, "replay-source-task", replayedContext.ProtocolTaskID)

	listedRuns, err := queries.ListRunsByUserWithAgent(context.Background(), db.ListRunsByUserWithAgentParams{
		UserID: ownerID,
		Limit:  10,
		Offset: 0,
	})
	require.NoError(t, err)
	require.NotEmpty(t, listedRuns)
	require.Equal(t, createdID, listedRuns[0].ID)
	require.Equal(t, runtime.RuntimeContractID, listedRuns[0].RuntimeContractID)
	require.Equal(t, "pending", listedRuns[0].DispatchState)

	agentRuns, err := queries.ListRunsByUserAndAgent(context.Background(), db.ListRunsByUserAndAgentParams{
		UserID:         ownerID,
		AgentID:        fixture.identity.AgentID,
		NoCursor:       true,
		NoStatusFilter: true,
		NoSinceFilter:  true,
		Limit:          10,
	})
	require.NoError(t, err)
	require.NotEmpty(t, agentRuns)
	require.Equal(t, runtime.RuntimeContractID, agentRuns[0].RuntimeContractID)

	callRecords, err := queries.ListCallRecordsForUser(context.Background(), db.ListCallRecordsForUserParams{
		UserID: ownerID,
		View:   "made",
		Sort:   "started_desc",
		Limit:  10,
	})
	require.NoError(t, err)
	require.NotEmpty(t, callRecords)
	require.Equal(t, runtime.RuntimeContractID, callRecords[0].RuntimeContractID)
	require.Equal(t, "pending", callRecords[0].DispatchState)

	var replayChildren int32
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT COUNT(*)::int FROM runs WHERE replay_of_run_id = $1`,
		fixture.identity.RunID,
	).Scan(&replayChildren))
	require.Equal(t, int32(1), replayChildren)

	var afterStatus, afterDispatch string
	var afterAttempts, afterEvents, afterDLQ int32
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT r.status, r.dispatch_state,
		       (SELECT COUNT(*)::int FROM run_attempts WHERE run_id = r.id),
		       (SELECT COUNT(*)::int FROM run_events WHERE run_id = r.id),
		       (SELECT COUNT(*)::int FROM run_dead_letters WHERE run_id = r.id)
		FROM runs r WHERE r.id = $1`, fixture.identity.RunID).Scan(
		&afterStatus, &afterDispatch, &afterAttempts, &afterEvents, &afterDLQ,
	))
	require.Equal(t, sourceStatus, afterStatus)
	require.Equal(t, sourceDispatch, afterDispatch)
	require.Equal(t, sourceAttempts, afterAttempts)
	require.Equal(t, sourceEvents, afterEvents)
	require.Equal(t, sourceDLQ, afterDLQ)

	second, err := svc.ReplayRun(
		context.Background(), ownerID, fixture.identity.RunID, "dlq-replay-two", "api",
	)
	require.NoError(t, err)
	require.NotEqual(t, createdRunID, second.RunID)
	require.Equal(t, fixture.identity.RunID.String(), second.ReplayOfRunID)

	deadLetters, err := svc.ListRuntimeDeadLetters(context.Background(), 25, 0)
	require.NoError(t, err)
	require.Equal(t, int32(1), deadLetters.Total)
	require.Len(t, deadLetters.Items, 1)
	require.Equal(t, fixture.identity.RunID.String(), deadLetters.Items[0].RunID)
	require.Equal(t, fixture.identity.AttemptID.String(), deadLetters.Items[0].FinalAttemptID)
	require.Equal(t, int32(1), deadLetters.Items[0].FinalAttemptNo)
	require.Equal(t, "RUNTIME_RETRY_EXHAUSTED", deadLetters.Items[0].ReasonCode)
	require.Equal(t, []string{createdRunID, second.RunID}, deadLetters.Items[0].ReplayedAsRunIDs)

	otherOwner := insertRuntimeUser(t, pool)
	_, err = svc.ReplayRun(context.Background(), otherOwner, fixture.identity.RunID, "not-owner", "api")
	requireRunOpsHTTPStatus(t, err, http.StatusNotFound)
	_, err = svc.ReplayRun(context.Background(), ownerID, createdID, "not-dlq", "api")
	requireRunOpsHTTPStatus(t, err, http.StatusConflict)

	t.Run("retained input cannot create a new replay but committed replay still resolves", func(t *testing.T) {
		updateErr := pgx.BeginFunc(context.Background(), pool, func(tx pgx.Tx) error {
			if _, err := tx.Exec(context.Background(), `SET LOCAL session_replication_role = replica`); err != nil {
				return err
			}
			_, err := tx.Exec(context.Background(), `UPDATE runs SET input = 'null'::jsonb WHERE id = $1`, fixture.identity.RunID)
			return err
		})
		if updateErr != nil && strings.Contains(updateErr.Error(), "permission denied") {
			t.Skipf("test database cannot emulate input retention: %v", updateErr)
		}
		require.NoError(t, updateErr)

		committed, replayErr := svc.ReplayRun(
			context.Background(), ownerID, fixture.identity.RunID, "dlq-replay-once", "api",
		)
		require.NoError(t, replayErr)
		require.True(t, committed.Replayed)
		require.Equal(t, createdRunID, committed.RunID)

		_, replayErr = svc.ReplayRun(
			context.Background(), ownerID, fixture.identity.RunID, "retained-input-new-key", "api",
		)
		var httpErr *httpx.HTTPError
		require.True(t, errors.As(replayErr, &httpErr))
		require.Equal(t, http.StatusConflict, httpErr.Status)
		require.Equal(t, httpx.ErrorCode(runtime.RuntimeErrorReplayInputUnavailable), httpErr.Code)
	})
}

func requireRunOpsHTTPStatus(t *testing.T, err error, want int) {
	t.Helper()
	var httpErr *httpx.HTTPError
	require.True(t, errors.As(err, &httpErr), "expected HTTP error, got %T: %v", err, err)
	require.Equal(t, want, httpErr.Status)
}
