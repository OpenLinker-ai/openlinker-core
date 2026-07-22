package runtime_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

func TestRunCreationPersistsTrustedA2AMetadataBeforeImmutableInsert(t *testing.T) {
	pool := setupTestDB(t)
	requireReliableRuntimeSchema(t, pool)
	svc := newTestService(t, pool)
	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)

	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"answer":"done"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 0, "approved")
	const conversationID = "plugin-conversation-1"

	first, err := svc.Run(context.Background(), userID, &runtime.RunRequest{
		AgentID: agentID.String(),
		Input:   map[string]any{"task": "remember the nonce"},
		Metadata: map[string]any{
			"client":       "native-plugin-test",
			"a2a":          map[string]any{"source": "caller"},
			"conversation": map[string]any{"source": "caller"},
		},
		A2AContext: &runtime.RunA2AContextRequest{
			ProtocolContextID: conversationID,
			ProtocolTaskID:    "plugin-turn-1",
			RootContextID:     conversationID,
		},
		IdempotencyKey: "plugin-conversation-turn-1",
	}, "mcp")
	require.NoError(t, err)
	require.Equal(t, "success", first.Status)

	firstID := uuid.MustParse(first.RunID)
	var firstMetadataJSON []byte
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT request_metadata FROM runs WHERE id = $1`, firstID,
	).Scan(&firstMetadataJSON))
	var firstMetadata map[string]any
	require.NoError(t, json.Unmarshal(firstMetadataJSON, &firstMetadata))
	require.Equal(t, "native-plugin-test", firstMetadata["client"])
	firstA2A := firstMetadata["a2a"].(map[string]any)
	require.Equal(t, conversationID, firstA2A["protocol_context_id"])
	firstConversation := firstMetadata["conversation"].(map[string]any)
	require.Equal(t, conversationID, firstConversation["session_key"])
	require.Equal(t, first.RunID, firstConversation["current_run_id"])
	require.Equal(t, "plugin-turn-1", firstConversation["current_protocol_task_id"])
	require.Equal(t, "core", firstConversation["source"])
	require.NotContains(t, firstConversation, "history_before_current")
	var mappedTargetAgentID uuid.UUID
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT target_agent_id FROM a2a_context_mappings WHERE run_id = $1`, firstID,
	).Scan(&mappedTargetAgentID))
	require.Equal(t, agentID, mappedTargetAgentID)

	_, err = pool.Exec(context.Background(),
		`UPDATE runs SET request_metadata = '{"tampered":true}'::jsonb WHERE id = $1`, firstID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "run creation identity is immutable")

	second, err := svc.Run(context.Background(), userID, &runtime.RunRequest{
		AgentID: agentID.String(),
		Input:   map[string]any{"task": "recall the nonce"},
		A2AContext: &runtime.RunA2AContextRequest{
			ProtocolContextID: conversationID,
			ProtocolTaskID:    "plugin-turn-2",
			RootContextID:     conversationID,
		},
		IdempotencyKey: "plugin-conversation-turn-2",
	}, "mcp")
	require.NoError(t, err)
	require.Equal(t, "success", second.Status)

	var secondMetadataJSON []byte
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT request_metadata FROM runs WHERE id = $1`, uuid.MustParse(second.RunID),
	).Scan(&secondMetadataJSON))
	var secondMetadata map[string]any
	require.NoError(t, json.Unmarshal(secondMetadataJSON, &secondMetadata))
	secondConversation := secondMetadata["conversation"].(map[string]any)
	require.Equal(t, conversationID, secondConversation["session_key"])
	require.Equal(t, second.RunID, secondConversation["current_run_id"])
	require.Equal(t, "plugin-turn-2", secondConversation["current_protocol_task_id"])
	history := secondConversation["history_before_current"].([]any)
	require.Len(t, history, 1)
	require.Equal(t, first.RunID, history[0].(map[string]any)["run_id"])
	require.Equal(t, "user", history[0].(map[string]any)["role"])
}

func TestRunCreationIdempotencyOneWinnerUnder100ConcurrentRequests(t *testing.T) {
	pool := setupTestDB(t)
	requireReliableRuntimeSchema(t, pool)
	svc := newTestService(t, pool)
	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)

	endpoint, endpointCalls := mockEndpointCounting(mockEndpointReturning(http.StatusOK, `{"output":{"answer":"done"}}`))
	agentURL := startMockEndpointForService(t, svc, endpoint)
	agentID := insertAgent(t, pool, creatorID, agentURL, 0, "approved")

	const workers = 100
	const idempotencyKey = "concurrent-run/private-but-never-persisted"
	type result struct {
		response *runtime.RunResponse
		err      error
	}
	results := make(chan result, workers)
	start := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var wait sync.WaitGroup
	wait.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wait.Done()
			<-start
			response, err := svc.StartRun(ctx, userID, &runtime.RunRequest{
				AgentID:          agentID.String(),
				Input:            map[string]any{"query": "same body", "limit": 10},
				Metadata:         map[string]any{"locale": "zh-CN"},
				IdempotencyKey:   idempotencyKey,
				CreationProtocol: "rest",
				CreationMethod:   "runs.create",
			}, "web")
			results <- result{response: response, err: err}
		}()
	}
	close(start)
	wait.Wait()
	close(results)

	var runID string
	created, replayed := 0, 0
	for got := range results {
		if got.err != nil {
			t.Fatalf("concurrent StartRun() error: %v", got.err)
		}
		if got.response == nil || got.response.RunID == "" {
			t.Fatalf("concurrent StartRun() response = %#v", got.response)
		}
		if runID == "" {
			runID = got.response.RunID
		}
		if got.response.RunID != runID {
			t.Fatalf("idempotency created multiple run IDs: %s and %s", runID, got.response.RunID)
		}
		if got.response.Replayed {
			replayed++
		} else {
			created++
		}
	}
	require.Equal(t, 1, created, "exactly one request must win creation")
	require.Equal(t, workers-1, replayed, "every loser must be returned as a replay")
	require.Eventually(t, func() bool { return endpointCalls() == 1 }, 3*time.Second, 10*time.Millisecond)

	var runCount, createdEventCount, availableSignalCount int
	err := pool.QueryRow(context.Background(), `
		SELECT
			(SELECT COUNT(*) FROM runs WHERE user_id = $1),
			(SELECT COUNT(*)
			   FROM run_events e
			   JOIN runs r ON r.id = e.run_id
			  WHERE r.user_id = $1 AND e.event_type = 'run.created'),
			(SELECT COUNT(*)
			   FROM runtime_signal_outbox s
			   JOIN runs r ON r.id = s.run_id
			  WHERE r.user_id = $1 AND s.event_type = 'run.available')`, userID).
		Scan(&runCount, &createdEventCount, &availableSignalCount)
	require.NoError(t, err)
	require.Equal(t, 1, runCount)
	require.Equal(t, 1, createdEventCount)
	require.Equal(t, 1, availableSignalCount)

	wantKeyHash := sha256.Sum256([]byte(idempotencyKey))
	var storedKeyHash []byte
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT idempotency_key_hash FROM runs WHERE id = $1`, uuid.MustParse(runID)).Scan(&storedKeyHash))
	require.Equal(t, wantKeyHash[:], storedKeyHash)

	replay, err := svc.StartRun(context.Background(), userID, &runtime.RunRequest{
		AgentID:          agentID.String(),
		Input:            map[string]any{"limit": 10, "query": "same body"},
		Metadata:         map[string]any{"locale": "zh-CN"},
		IdempotencyKey:   idempotencyKey,
		CreationProtocol: "REST",
		CreationMethod:   "RUNS.CREATE",
	}, "web")
	require.NoError(t, err)
	require.True(t, replay.Replayed)
	require.Equal(t, runID, replay.RunID)
	require.Equal(t, int64(1), endpointCalls(), "a replay must not execute the endpoint again")

	_, err = svc.StartRun(context.Background(), userID, &runtime.RunRequest{
		AgentID:          agentID.String(),
		Input:            map[string]any{"query": "different body", "limit": 10},
		Metadata:         map[string]any{"locale": "zh-CN"},
		IdempotencyKey:   idempotencyKey,
		CreationProtocol: "rest",
		CreationMethod:   "runs.create",
	}, "web")
	var httpErr *httpx.HTTPError
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, http.StatusConflict, httpErr.Status)
	require.Equal(t, httpx.ErrorCode(runtime.IdempotencyErrorKeyReused), httpErr.Code)
	require.NotContains(t, httpErr.Error(), idempotencyKey)
	require.Equal(t, int64(1), endpointCalls(), "a conflicting key must not execute the endpoint")

	// Guard the asynchronous winner against accidentally dispatching twice
	// after the replay/conflict checks have returned.
	require.Never(t, func() bool { return endpointCalls() > 1 }, 150*time.Millisecond, 10*time.Millisecond)
}

func TestRunCreationRollsBackAtEveryRequiredInsertBoundary(t *testing.T) {
	pool := setupTestDB(t)
	requireReliableRuntimeSchema(t, pool)
	svc := newTestService(t, pool)
	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)

	endpoint, endpointCalls := mockEndpointCounting(mockEndpointReturning(http.StatusOK, `{"output":{"answer":"done"}}`))
	agentURL := startMockEndpointForService(t, svc, endpoint)
	agentID := insertAgent(t, pool, creatorID, agentURL, 0, "approved")

	_, err := pool.Exec(context.Background(), `
		CREATE OR REPLACE FUNCTION runtime_test_reject_insert()
		RETURNS trigger
		LANGUAGE plpgsql
		AS $$
		BEGIN
			RAISE EXCEPTION 'runtime test injected insert failure';
		END;
		$$`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DROP FUNCTION IF EXISTS runtime_test_reject_insert() CASCADE`)
	})

	for _, table := range []string{"runs", "run_events", "runtime_signal_outbox"} {
		t.Run(table, func(t *testing.T) {
			triggerName := "runtime_test_reject_" + table
			_, err := pool.Exec(context.Background(), `CREATE TRIGGER `+triggerName+`
				BEFORE INSERT ON `+table+`
				FOR EACH ROW EXECUTE FUNCTION runtime_test_reject_insert()`)
			require.NoError(t, err)

			_, runErr := svc.StartRun(context.Background(), userID, &runtime.RunRequest{
				AgentID:          agentID.String(),
				Input:            map[string]any{"failure_boundary": table},
				IdempotencyKey:   "rollback-boundary/" + table,
				CreationProtocol: "rest",
				CreationMethod:   "runs.create",
			}, "web")
			require.Error(t, runErr)

			_, err = pool.Exec(context.Background(), `DROP TRIGGER `+triggerName+` ON `+table)
			require.NoError(t, err)

			var runs, events, signals int
			require.NoError(t, pool.QueryRow(context.Background(), `
				SELECT
					(SELECT COUNT(*) FROM runs WHERE user_id = $1),
					(SELECT COUNT(*)
					   FROM run_events e
					   JOIN runs r ON r.id = e.run_id
					  WHERE r.user_id = $1),
					(SELECT COUNT(*)
					   FROM runtime_signal_outbox s
					   JOIN runs r ON r.id = s.run_id
					  WHERE r.user_id = $1)`, userID).Scan(&runs, &events, &signals))
			require.Zero(t, runs, "failed creation left a partial Run")
			require.Zero(t, events, "failed creation left a partial event")
			require.Zero(t, signals, "failed creation left a partial signal")
			require.Zero(t, endpointCalls(), "failed creation reached the Agent endpoint")
		})
	}
}

func requireReliableRuntimeSchema(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	var ready bool
	err := pool.QueryRow(context.Background(), `
		SELECT to_regclass('runtime_signal_outbox') IS NOT NULL
		   AND EXISTS (
			SELECT 1
			  FROM information_schema.columns
			 WHERE table_schema = current_schema()
			   AND table_name = 'runs'
			   AND column_name = 'idempotency_fingerprint'
		   )`).Scan(&ready)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			t.Fatalf("checking reliable Runtime schema: %v", err)
		}
		t.Skipf("migration 063 schema check unavailable: %v", err)
	}
	if !ready {
		t.Skip("migration 063 reliable Runtime schema is not applied")
	}
}
