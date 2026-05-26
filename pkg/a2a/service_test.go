package a2a_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kinzhi/openlinker-core/pkg/a2a"
	"github.com/kinzhi/openlinker-core/pkg/config"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
	"github.com/kinzhi/openlinker-core/pkg/runtime"
)

const truncateA2ATables = "TRUNCATE run_delegations, agent_runtime_tokens, agent_call_policies, run_events, runs, agents, users RESTART IDENTITY CASCADE"

func setupService(t *testing.T) (*pgxpool.Pool, *a2a.Service, *runtime.Service) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL 未设置，跳过 A2A 集成测试")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	require.NoError(t, pool.Ping(context.Background()))
	_, err = pool.Exec(context.Background(), truncateA2ATables)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), truncateA2ATables)
		pool.Close()
	})
	runtimeSvc := runtime.NewService(pool, &config.Config{PlatformFeeRate: 0.25, RunTimeoutSeconds: 2})
	return pool, a2a.NewService(pool, runtimeSvc), runtimeSvc
}

func insertCreator(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO users (id, email, password_hash, display_name, is_creator)
		 VALUES ($1, $2, 'x', 'creator', TRUE)`,
		id, id.String()+"@example.com")
	require.NoError(t, err)
	return id
}

func insertAgent(t *testing.T, pool *pgxpool.Pool, creatorID uuid.UUID, endpoint string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO agents (
		  id, creator_id, slug, name, description, endpoint_url, price_per_call_cents, tags,
		  lifecycle_status, visibility, certification_status
		) VALUES ($1, $2, $3, 'A2A Agent', 'test', $4, 25, '{}', 'active', 'public', 'unreviewed')`,
		id, creatorID, "a2a-"+id.String()[:8], endpoint)
	require.NoError(t, err)
	return id
}

func insertParentRun(t *testing.T, pool *pgxpool.Pool, userID, callerAgentID uuid.UUID) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO runs (
		  id, user_id, agent_id, input, status, cost_cents, platform_fee_cents,
		  creator_revenue_cents, source
		) VALUES ($1, $2, $3, '{}'::jsonb, 'running', 0, 0, 0, 'web')`,
		id, userID, callerAgentID)
	require.NoError(t, err)
	return id
}

func TestCallAgent_RecordsFreeDelegationWithoutLeakingUserID(t *testing.T) {
	pool, svc, runtimeSvc := setupService(t)
	callerOwner := insertCreator(t, pool)
	targetOwner := insertCreator(t, pool)

	var receivedHeader string
	var receivedParent string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedHeader = r.Header.Get("X-OpenLinker-User-Id")
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		receivedParent, _ = body["parent_run_id"].(string)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"output":{"text":"child ok"}}`))
	}))
	defer server.Close()
	defaultTransport := http.DefaultTransport
	http.DefaultTransport = server.Client().Transport
	t.Cleanup(func() { http.DefaultTransport = defaultTransport })

	callerID := insertAgent(t, pool, callerOwner, "https://example.com/caller")
	targetID := insertAgent(t, pool, targetOwner, server.URL)
	parentRunID := insertParentRun(t, pool, callerOwner, callerID)

	token, err := svc.CreateRuntimeToken(context.Background(), callerOwner, callerID, &a2a.CreateRuntimeTokenRequest{Name: "worker"})
	require.NoError(t, err)
	require.NotEmpty(t, token.PlaintextToken)

	child, err := svc.CallAgent(context.Background(), token.PlaintextToken, &a2a.CallAgentRequest{
		ParentRunID: parentRunID.String(), TargetAgentID: targetID.String(),
		Reason: "summarize", Input: map[string]any{"q": "hello"},
	})
	require.NoError(t, err)
	assert.Equal(t, "success", child.Status)
	assert.Equal(t, int32(0), child.CostCents)
	assert.Equal(t, "free_delegation", child.BillingMode)
	assert.Equal(t, parentRunID.String(), child.ParentRunID)
	assert.Empty(t, receivedHeader)
	assert.Equal(t, parentRunID.String(), receivedParent)

	reloaded, err := runtimeSvc.GetRun(context.Background(), callerOwner, uuid.MustParse(child.RunID))
	require.NoError(t, err)
	assert.Equal(t, parentRunID.String(), reloaded.ParentRunID)
	assert.Equal(t, callerID.String(), reloaded.CallerAgentID)
	assert.Equal(t, "free_delegation", reloaded.BillingMode)

	children, err := svc.ListChildren(context.Background(), callerOwner, parentRunID)
	require.NoError(t, err)
	require.Len(t, children, 1)
	assert.Equal(t, child.RunID, children[0].ChildRunID)
	assert.Equal(t, targetID.String(), children[0].TargetAgentID)

	var parentEvents int
	err = pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM run_events
		 WHERE run_id=$1 AND event_type IN ('run.child.created', 'run.child.completed')`, parentRunID).
		Scan(&parentEvents)
	require.NoError(t, err)
	assert.Equal(t, 2, parentEvents)
}

func TestRun_EndToEndDelegationCompletesParentAndChild(t *testing.T) {
	pool, svc, runtimeSvc := setupService(t)
	owner := insertCreator(t, pool)

	var callerID uuid.UUID
	var targetID uuid.UUID
	var runtimeToken string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Input map[string]any `json:"input"`
			RunID string         `json:"run_id"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		w.Header().Set("Content-Type", "application/json")

		if r.URL.Path == "/worker" {
			_, _ = w.Write([]byte(`{"output":{"answer":"worker completed"}}`))
			return
		}

		child, err := svc.CallAgent(r.Context(), runtimeToken, &a2a.CallAgentRequest{
			ParentRunID:   request.RunID,
			TargetAgentID: targetID.String(),
			Reason:        "delegate a verifiable subtask",
			Input:         map[string]any{"question": "finish child"},
		})
		require.NoError(t, err)
		require.Equal(t, "success", child.Status)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"output": map[string]any{
				"answer":       "parent completed",
				"child_run_id": child.RunID,
			},
		})
	}))
	defer server.Close()

	defaultTransport := http.DefaultTransport
	http.DefaultTransport = server.Client().Transport
	t.Cleanup(func() { http.DefaultTransport = defaultTransport })

	callerID = insertAgent(t, pool, owner, server.URL+"/caller")
	targetID = insertAgent(t, pool, owner, server.URL+"/worker")
	token, err := svc.CreateRuntimeToken(context.Background(), owner, callerID, &a2a.CreateRuntimeTokenRequest{Name: "orchestrator"})
	require.NoError(t, err)
	runtimeToken = token.PlaintextToken

	parent, err := runtimeSvc.Run(context.Background(), owner, &runtime.RunRequest{
		AgentID: callerID.String(),
		Input:   map[string]any{"task": "complete through delegation"},
	}, "web")
	require.NoError(t, err)
	assert.Equal(t, "success", parent.Status)
	assert.Equal(t, "parent completed", parent.Output["answer"])

	children, err := svc.ListChildren(context.Background(), owner, uuid.MustParse(parent.RunID))
	require.NoError(t, err)
	require.Len(t, children, 1)
	assert.Equal(t, "success", children[0].Status)
	assert.Equal(t, targetID.String(), children[0].TargetAgentID)

	events, err := runtimeSvc.ListRunEvents(context.Background(), owner, uuid.MustParse(parent.RunID), 0, 20)
	require.NoError(t, err)
	var eventTypes []string
	for _, event := range events {
		eventTypes = append(eventTypes, event.EventType)
	}
	assert.Contains(t, eventTypes, "run.child.created")
	assert.Contains(t, eventTypes, "run.child.completed")
	assert.Contains(t, eventTypes, "run.completed")
}

func TestCallAgent_RespectsPrivateTargetPolicy(t *testing.T) {
	pool, svc, _ := setupService(t)
	callerOwner := insertCreator(t, pool)
	targetOwner := insertCreator(t, pool)
	callerID := insertAgent(t, pool, callerOwner, "https://example.com/caller")
	targetID := insertAgent(t, pool, targetOwner, "https://example.com/target")
	parentRunID := insertParentRun(t, pool, callerOwner, callerID)

	token, err := svc.CreateRuntimeToken(context.Background(), callerOwner, callerID, &a2a.CreateRuntimeTokenRequest{Name: "worker"})
	require.NoError(t, err)
	_, err = svc.UpdateCallPolicy(context.Background(), targetOwner, targetID, &a2a.UpdateCallPolicyRequest{CallableBy: "private"})
	require.NoError(t, err)

	_, err = svc.CallAgent(context.Background(), token.PlaintextToken, &a2a.CallAgentRequest{
		ParentRunID: parentRunID.String(), TargetAgentID: targetID.String(), Input: map[string]any{"q": "hello"},
	})
	require.Error(t, err)
	httpErr, ok := err.(*httpx.HTTPError)
	require.True(t, ok)
	assert.Equal(t, http.StatusForbidden, httpErr.Status)
}
