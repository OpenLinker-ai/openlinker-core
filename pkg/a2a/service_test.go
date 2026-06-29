package a2a_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/a2a"
	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/OpenLinker-ai/openlinker-core/pkg/webhook"
)

const truncateA2ATables = "TRUNCATE task_callback_deliveries, task_callback_subscriptions, run_artifact_chunks, run_artifacts, run_messages, run_delegations, agent_runtime_tokens, agent_call_policies, run_events, runs, agents, users RESTART IDENTITY CASCADE"

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
	runtimeSvc := runtime.NewService(pool, &config.Config{
		PlatformFeeRate:         0.25,
		RunTimeoutSeconds:       15,
		AllowLocalHTTPEndpoints: true,
	})
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

func insertAgent(t *testing.T, pool *pgxpool.Pool, creatorID uuid.UUID, endpoint string, tags ...string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	agentTags := tags
	if agentTags == nil {
		agentTags = []string{}
	}
	_, err := pool.Exec(context.Background(),
		`INSERT INTO agents (
		  id, creator_id, slug, name, description, endpoint_url, price_per_call_cents, tags,
		  lifecycle_status, visibility, certification_status
		) VALUES ($1, $2, $3, 'A2A Agent', 'test', $4, 25, $5, 'active', 'public', 'unreviewed')`,
		id, creatorID, "a2a-"+id.String()[:8], endpoint, agentTags)
	require.NoError(t, err)
	return id
}

func attachSkill(t *testing.T, pool *pgxpool.Pool, agentID uuid.UUID, skillID, skillName string) {
	t.Helper()
	category := strings.Split(skillID, "/")[0]
	_, err := pool.Exec(context.Background(),
		`INSERT INTO skills (id, category, name, description, sort_order)
		 VALUES ($1, $2, $3, 'test skill', 0)
		 ON CONFLICT (id) DO NOTHING`,
		skillID, category, skillName)
	require.NoError(t, err)
	_, err = pool.Exec(context.Background(),
		`INSERT INTO agent_skills (agent_id, skill_id)
		 VALUES ($1, $2)
		 ON CONFLICT DO NOTHING`,
		agentID, skillID)
	require.NoError(t, err)
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

func insertDelegatedRun(t *testing.T, pool *pgxpool.Pool, userID, childAgentID, parentRunID, callerAgentID uuid.UUID) uuid.UUID {
	t.Helper()
	childRunID := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO runs (
		  id, user_id, agent_id, input, status, cost_cents, platform_fee_cents,
		  creator_revenue_cents, source
		) VALUES ($1, $2, $3, '{}'::jsonb, 'running', 0, 0, 0, 'api')`,
		childRunID, userID, childAgentID)
	require.NoError(t, err)
	_, err = pool.Exec(context.Background(),
		`INSERT INTO run_delegations (child_run_id, parent_run_id, caller_agent_id, reason)
		 VALUES ($1, $2, $3, 'test chain')`,
		childRunID, parentRunID, callerAgentID)
	require.NoError(t, err)
	return childRunID
}

func makeRuntimePullAgent(t *testing.T, pool *pgxpool.Pool, agentID uuid.UUID) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`UPDATE agents
		    SET connection_mode='runtime_pull',
		        endpoint_url='openlinker-runtime-pull://' || slug
		  WHERE id=$1`,
		agentID)
	require.NoError(t, err)
}

func TestCreateRuntimeToken_RuntimePullAgentCanClaimPendingRun(t *testing.T) {
	pool, svc, runtimeSvc := setupService(t)
	ownerID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, ownerID, "https://example.com/runtime")
	makeRuntimePullAgent(t, pool, agentID)
	runID := insertParentRun(t, pool, ownerID, agentID)

	token, err := svc.CreateRuntimeToken(context.Background(), ownerID, agentID, &a2a.CreateRuntimeTokenRequest{Name: "runtime-worker"})
	require.NoError(t, err)
	require.NotEmpty(t, token.PlaintextToken)
	assert.Contains(t, token.Scopes, "agent:call")
	assert.Contains(t, token.Scopes, "agent:pull")

	claimed, err := runtimeSvc.ClaimRuntimePullRun(context.Background(), token.PlaintextToken)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, runID.String(), claimed.RunID)
}

func TestRuntimeWorkbenchShowsPendingRuntimePullDiagnostics(t *testing.T) {
	pool, svc, _ := setupService(t)
	ownerID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, ownerID, "https://example.com/runtime")
	makeRuntimePullAgent(t, pool, agentID)
	runID := insertParentRun(t, pool, ownerID, agentID)

	token, err := svc.CreateRuntimeToken(context.Background(), ownerID, agentID, &a2a.CreateRuntimeTokenRequest{Name: "runtime-worker"})
	require.NoError(t, err)
	require.NotEmpty(t, token.PlaintextToken)

	workbench, err := svc.GetRuntimeWorkbench(context.Background(), ownerID, agentID)
	require.NoError(t, err)
	assert.Equal(t, agentID.String(), workbench.Agent.ID)
	assert.Equal(t, "runtime_pull", workbench.Agent.ConnectionMode)
	assert.Equal(t, int32(1), workbench.Runtime.ActiveTokenCount)
	assert.Equal(t, int32(1), workbench.Runtime.PendingRunCount)
	assert.True(t, workbench.Runtime.ClaimNow)
	require.Len(t, workbench.Tokens, 1)
	assert.Equal(t, []string{"agent:call", "agent:pull"}, workbench.Tokens[0].Scopes)
	require.NotEmpty(t, workbench.RecentRuns)
	assert.Equal(t, runID.String(), workbench.RecentRuns[0].RunID)
	assert.Equal(t, "running", workbench.RecentRuns[0].Status)

	var codes []string
	for _, item := range workbench.Diagnostics {
		codes = append(codes, item.Code)
	}
	assert.Contains(t, codes, "no_recent_runtime_activity")
	assert.Contains(t, codes, "pending_claimable_runs")
}

func TestRuntimeWorkbenchFlagsRuntimePullTokenScopeMissing(t *testing.T) {
	pool, svc, _ := setupService(t)
	ownerID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, ownerID, "https://example.com/runtime")
	makeRuntimePullAgent(t, pool, agentID)

	_, err := pool.Exec(context.Background(),
		`INSERT INTO agent_runtime_tokens (
			agent_id, created_by_user_id, name, prefix, token_hash, scopes
		) VALUES ($1, $2, 'call-only', $3, 'hash', $4)`,
		agentID,
		ownerID,
		"ol_live_abcd",
		[]string{"agent:call"},
	)
	require.NoError(t, err)

	workbench, err := svc.GetRuntimeWorkbench(context.Background(), ownerID, agentID)
	require.NoError(t, err)

	var codes []string
	for _, item := range workbench.Diagnostics {
		codes = append(codes, item.Code)
	}
	assert.Contains(t, codes, "scope_missing")
}

func TestRuntimeTokenRevokeAndCallPolicyReadback(t *testing.T) {
	pool, svc, _ := setupService(t)
	ownerID := insertCreator(t, pool)
	otherUserID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, ownerID, "https://example.com/runtime")

	token, err := svc.CreateRuntimeToken(context.Background(), ownerID, agentID, &a2a.CreateRuntimeTokenRequest{Name: "worker"})
	require.NoError(t, err)
	require.NotEmpty(t, token.PlaintextToken)

	policy, err := svc.GetCallPolicy(context.Background(), ownerID, agentID)
	require.NoError(t, err)
	assert.Equal(t, "public", policy.CallableBy)

	updatedPolicy, err := svc.UpdateCallPolicy(context.Background(), ownerID, agentID, &a2a.UpdateCallPolicyRequest{CallableBy: "same_creator"})
	require.NoError(t, err)
	assert.Equal(t, "same_creator", updatedPolicy.CallableBy)
	policy, err = svc.GetCallPolicy(context.Background(), ownerID, agentID)
	require.NoError(t, err)
	assert.Equal(t, "same_creator", policy.CallableBy)

	err = svc.RevokeRuntimeToken(context.Background(), ownerID, uuid.MustParse(token.ID))
	require.NoError(t, err)
	tokens, err := svc.ListRuntimeTokens(context.Background(), ownerID, agentID)
	require.NoError(t, err)
	require.Len(t, tokens, 1)
	require.NotNil(t, tokens[0].RevokedAt)

	err = svc.RevokeRuntimeToken(context.Background(), ownerID, uuid.MustParse(token.ID))
	requireA2AServiceHTTPStatus(t, err, http.StatusNotFound)
	_, err = svc.GetCallPolicy(context.Background(), otherUserID, agentID)
	requireA2AServiceHTTPStatus(t, err, http.StatusNotFound)
}

func TestCallAgent_RecordsFreeDelegationWithoutLeakingUserID(t *testing.T) {
	pool, svc, runtimeSvc := setupService(t)
	callerOwner := insertCreator(t, pool)
	targetOwner := insertCreator(t, pool)
	pushSvc := webhook.NewService(pool, &config.Config{AllowLocalHTTPEndpoints: true})
	svc.SetTaskCallbackManager(pushSvc)

	var receivedHeader string
	var receivedParent string
	var receivedA2A struct {
		CurrentRunID      string   `json:"current_run_id"`
		ParentRunID       string   `json:"parent_run_id"`
		CallerAgentID     string   `json:"caller_agent_id"`
		CallAgentEndpoint string   `json:"call_agent_endpoint"`
		RuntimeScopes     []string `json:"runtime_scopes"`
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedHeader = r.Header.Get("X-OpenLinker-User-Id")
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		receivedParent, _ = body["parent_run_id"].(string)
		a2aRaw, _ := json.Marshal(body["a2a"])
		_ = json.Unmarshal(a2aRaw, &receivedA2A)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"output":{"text":"child ok"}}`))
	}))
	defer server.Close()
	pushServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	defer pushServer.Close()
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
		TaskCallback: &a2a.A2APushNotificationConfig{
			URL:    pushServer.URL + "/a2a/events",
			Secret: "caller-a2a-secret",
			EventTypesAlias: []string{
				"run.completed",
				"run.failed",
				"run.canceled",
			},
			Metadata: map[string]any{"client": "caller-agent"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "success", child.Status)
	assert.Equal(t, int32(0), child.CostCents)
	assert.Equal(t, "free_delegation", child.BillingMode)
	assert.Equal(t, parentRunID.String(), child.ParentRunID)
	require.NotNil(t, child.TaskCallback)
	assert.Equal(t, pushServer.URL+"/a2a/events", child.TaskCallback.TargetURL)
	assert.Equal(t, []string{"run.completed", "run.failed", "run.canceled"}, child.TaskCallback.EventTypes)
	assert.Equal(t, "caller-a2a-secret", child.TaskCallback.Secret)
	assert.Empty(t, receivedHeader)
	assert.Equal(t, parentRunID.String(), receivedParent)
	assert.Equal(t, child.RunID, receivedA2A.CurrentRunID)
	assert.Equal(t, parentRunID.String(), receivedA2A.ParentRunID)
	assert.Equal(t, callerID.String(), receivedA2A.CallerAgentID)
	assert.Equal(t, "http://localhost:8080/api/v1/agent-runtime/call-agent", receivedA2A.CallAgentEndpoint)
	assert.Contains(t, receivedA2A.RuntimeScopes, "agent:call")

	reloaded, err := runtimeSvc.GetRun(context.Background(), callerOwner, uuid.MustParse(child.RunID))
	require.NoError(t, err)
	assert.Equal(t, parentRunID.String(), reloaded.ParentRunID)
	assert.Equal(t, callerID.String(), reloaded.CallerAgentID)
	assert.Equal(t, "free_delegation", reloaded.BillingMode)
	var subscriptionCount int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM task_callback_subscriptions WHERE run_id=$1 AND owner_user_id=$2 AND target_url=$3`,
		child.RunID, callerOwner, pushServer.URL+"/a2a/events",
	).Scan(&subscriptionCount))
	assert.Equal(t, 1, subscriptionCount)

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

func TestCallAgent_InvalidTaskCallbackDoesNotCreateChildRun(t *testing.T) {
	pool, svc, _ := setupService(t)
	callerOwner := insertCreator(t, pool)
	targetOwner := insertCreator(t, pool)
	svc.SetTaskCallbackManager(webhook.NewService(pool, &config.Config{AllowLocalHTTPEndpoints: true}))

	var targetHits int
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetHits++
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

	_, err = svc.CallAgent(context.Background(), token.PlaintextToken, &a2a.CallAgentRequest{
		ParentRunID:   parentRunID.String(),
		TargetAgentID: targetID.String(),
		Reason:        "summarize",
		Input:         map[string]any{"q": "hello"},
		TaskCallback: &a2a.A2APushNotificationConfig{
			EventTypesAlias: []string{"run.completed"},
		},
	})
	requireA2AServiceHTTPStatus(t, err, http.StatusBadRequest)
	assert.Equal(t, 0, targetHits)

	var delegationCount int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM run_delegations WHERE parent_run_id=$1`,
		parentRunID,
	).Scan(&delegationCount))
	assert.Equal(t, 0, delegationCount)

	var runCount int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM runs WHERE id<>$1`,
		parentRunID,
	).Scan(&runCount))
	assert.Equal(t, 0, runCount)
}

func TestCallAgentRejectsDelegationCycle(t *testing.T) {
	pool, svc, _ := setupService(t)
	owner := insertCreator(t, pool)
	agentA := insertAgent(t, pool, owner, "https://example.com/a")
	agentB := insertAgent(t, pool, owner, "https://example.com/b")
	rootRun := insertParentRun(t, pool, owner, agentA)
	childRun := insertDelegatedRun(t, pool, owner, agentB, rootRun, agentA)

	tokenB, err := svc.CreateRuntimeToken(context.Background(), owner, agentB, &a2a.CreateRuntimeTokenRequest{Name: "b-worker"})
	require.NoError(t, err)

	_, err = svc.CallAgent(context.Background(), tokenB.PlaintextToken, &a2a.CallAgentRequest{
		CurrentRunID:  childRun.String(),
		TargetAgentID: agentA.String(),
		Input:         map[string]any{"q": "loop back"},
	})
	requireA2AServiceHTTPStatus(t, err, http.StatusUnprocessableEntity)
}

func TestCallAgentRejectsWhenRunningDelegationLimitReached(t *testing.T) {
	pool, svc, _ := setupService(t)
	svc.SetDelegationLimits(8, 1)
	owner := insertCreator(t, pool)
	agentA := insertAgent(t, pool, owner, "https://example.com/a")
	agentB := insertAgent(t, pool, owner, "https://example.com/b")
	agentC := insertAgent(t, pool, owner, "https://example.com/c")
	rootRun := insertParentRun(t, pool, owner, agentA)
	_ = insertDelegatedRun(t, pool, owner, agentB, rootRun, agentA)

	tokenA, err := svc.CreateRuntimeToken(context.Background(), owner, agentA, &a2a.CreateRuntimeTokenRequest{Name: "a-worker"})
	require.NoError(t, err)

	_, err = svc.CallAgent(context.Background(), tokenA.PlaintextToken, &a2a.CallAgentRequest{
		CurrentRunID:  rootRun.String(),
		TargetAgentID: agentC.String(),
		Input:         map[string]any{"q": "over limit"},
	})
	requireA2AServiceHTTPStatus(t, err, http.StatusTooManyRequests)
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
			A2A   struct {
				CurrentRunID      string `json:"current_run_id"`
				CallAgentEndpoint string `json:"call_agent_endpoint"`
			} `json:"a2a"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		w.Header().Set("Content-Type", "application/json")

		if r.URL.Path == "/worker" {
			_, _ = w.Write([]byte(`{"output":{"answer":"worker completed"}}`))
			return
		}

		child, err := svc.CallAgent(r.Context(), runtimeToken, &a2a.CallAgentRequest{
			CurrentRunID:  request.A2A.CurrentRunID,
			TargetAgentID: targetID.String(),
			Reason:        "delegate a verifiable subtask",
			Input:         map[string]any{"question": "finish child"},
		})
		require.NoError(t, err)
		require.Equal(t, request.RunID, request.A2A.CurrentRunID)
		require.Equal(t, "http://localhost:8080/api/v1/agent-runtime/call-agent", request.A2A.CallAgentEndpoint)
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

	callerID = insertAgent(t, pool, owner, server.URL+"/caller", "orchestration")
	targetID = insertAgent(t, pool, owner, server.URL+"/worker", "worker")
	attachSkill(t, pool, callerID, "ai/agent-orchestration", "Agent 编排")
	attachSkill(t, pool, targetID, "content/summarization", "摘要")
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
	assert.Equal(t, []string{"orchestration"}, children[0].CallerAgentTags)
	assert.Equal(t, []string{"worker"}, children[0].TargetAgentTags)
	require.Len(t, children[0].CallerSkills, 1)
	assert.Equal(t, "Agent 编排", children[0].CallerSkills[0].Name)
	require.Len(t, children[0].TargetSkills, 1)
	assert.Equal(t, "content/summarization", children[0].TargetSkills[0].ID)

	parents, err := svc.ListParentRuns(context.Background(), owner, 1, 10, "")
	require.NoError(t, err)
	require.Len(t, parents.Items, 1)
	assert.Equal(t, int32(1), parents.Total)
	assert.Equal(t, parent.RunID, parents.Items[0].ParentRunID)
	assert.Equal(t, callerID.String(), parents.Items[0].CallerAgentID)
	assert.Equal(t, "A2A Agent", parents.Items[0].CallerAgentName)
	assert.Equal(t, []string{"orchestration"}, parents.Items[0].CallerAgentTags)
	require.Len(t, parents.Items[0].CallerSkills, 1)
	assert.Equal(t, "ai/agent-orchestration", parents.Items[0].CallerSkills[0].ID)
	assert.Equal(t, "web", parents.Items[0].Source)
	assert.Equal(t, int32(1), parents.Items[0].ActiveRuntimeTokenCount)
	require.NotNil(t, parents.Items[0].LastRuntimeTokenUsedAt)
	assert.Equal(t, int32(1), parents.Items[0].ChildCount)
	assert.Equal(t, int32(1), parents.Items[0].SuccessfulChildCount)

	callerFiltered, err := svc.ListParentRuns(context.Background(), owner, 1, 10, "orchestration")
	require.NoError(t, err)
	require.Len(t, callerFiltered.Items, 1)
	assert.Equal(t, parent.RunID, callerFiltered.Items[0].ParentRunID)

	targetFiltered, err := svc.ListParentRuns(context.Background(), owner, 1, 10, "worker")
	require.NoError(t, err)
	require.Len(t, targetFiltered.Items, 1)
	assert.Equal(t, parent.RunID, targetFiltered.Items[0].ParentRunID)

	missingFiltered, err := svc.ListParentRuns(context.Background(), owner, 1, 10, "agent-that-does-not-exist")
	require.NoError(t, err)
	require.Empty(t, missingFiltered.Items)
	assert.Equal(t, int32(0), missingFiltered.Total)

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

func TestProtocolMessageSendAndGetTask(t *testing.T) {
	pool, svc, _ := setupService(t)
	owner := insertCreator(t, pool)

	var receivedInput map[string]any
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Input map[string]any `json:"input"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		receivedInput = request.Input
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"output": map[string]any{
				"summary": "A2A protocol adapter completed",
				"answer":  "done",
				"artifacts": []map[string]any{{
					"title":           "Result CSV",
					"artifact_type":   "file",
					"file_uri":        "https://files.example/a2a-result.csv",
					"file_name":       "a2a-result.csv",
					"mime_type":       "text/csv",
					"file_sha256":     "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
					"file_size_bytes": 256,
					"content":         map[string]any{"rows": 2},
				}},
			},
			"events": []map[string]any{{
				"event_type": "run.message.delta",
				"payload": map[string]any{
					"text": "adapter evidence message",
				},
			}},
		})
	}))
	defer server.Close()
	defaultTransport := http.DefaultTransport
	http.DefaultTransport = server.Client().Transport
	t.Cleanup(func() { http.DefaultTransport = defaultTransport })

	agentID := insertAgent(t, pool, owner, server.URL)
	slug := "a2a-" + agentID.String()[:8]

	task, err := svc.SendProtocolMessage(context.Background(), owner, slug, &a2a.A2AMessageSendParams{
		Message: a2a.A2AMessage{
			Kind:      "message",
			MessageID: "msg-1",
			ContextID: "ctx-1",
			Role:      "user",
			Parts: []map[string]any{{
				"kind": "text",
				"text": "请完成一次标准 A2A 调用",
			}},
		},
		Metadata: map[string]any{"trace_id": "a2a-protocol-test"},
	})
	require.NoError(t, err)
	assert.Equal(t, "task", task.Kind)
	assert.Equal(t, "completed", task.Status.State)
	assert.Equal(t, "ctx-1", task.ContextID)
	require.NotNil(t, task.Status.Message)
	require.NotEmpty(t, task.Artifacts)
	assert.Equal(t, "请完成一次标准 A2A 调用", receivedInput["message"])

	historyLength := 10
	reloaded, err := svc.GetProtocolTask(context.Background(), owner, slug, task.ID, &historyLength)
	require.NoError(t, err)
	assert.Equal(t, task.ID, reloaded.ID)
	assert.Equal(t, "completed", reloaded.Status.State)
	require.NotEmpty(t, reloaded.History)
	require.NotEmpty(t, reloaded.Artifacts)
	var fileArtifact *a2a.A2AArtifact
	for i := range reloaded.Artifacts {
		if reloaded.Artifacts[i].Name == "Result CSV" {
			fileArtifact = &reloaded.Artifacts[i]
			break
		}
	}
	require.NotNil(t, fileArtifact)
	require.Len(t, fileArtifact.Parts, 1)
	assert.Equal(t, "file", fileArtifact.Parts[0]["kind"])
	filePart, ok := fileArtifact.Parts[0]["file"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "https://files.example/a2a-result.csv", filePart["uri"])
	assert.Equal(t, "text/csv", filePart["mimeType"])
}

func TestProtocolMessageAcceptsCurrentPartShapes(t *testing.T) {
	pool, svc, _ := setupService(t)
	owner := insertCreator(t, pool)

	var receivedInput map[string]any
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Input map[string]any `json:"input"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		receivedInput = request.Input
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"output":{"summary":"current parts ok"}}`))
	}))
	defer server.Close()
	defaultTransport := http.DefaultTransport
	http.DefaultTransport = server.Client().Transport
	t.Cleanup(func() { http.DefaultTransport = defaultTransport })

	agentID := insertAgent(t, pool, owner, server.URL)
	slug := "a2a-" + agentID.String()[:8]

	task, err := svc.SendProtocolMessage(context.Background(), owner, slug, &a2a.A2AMessageSendParams{
		Message: a2a.A2AMessage{
			MessageID: "msg-current",
			ContextID: "ctx-current",
			Role:      "user",
			Parts: []map[string]any{
				{"text": "请读取附件并输出摘要"},
				{"data": map[string]any{"rows": 3}, "mediaType": "application/json"},
				{
					"url":       "https://files.example/input.csv",
					"filename":  "input.csv",
					"mediaType": "text/csv",
				},
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "completed", task.Status.State)
	assert.Equal(t, "请读取附件并输出摘要", receivedInput["message"])
	dataParts, ok := receivedInput["data_parts"].([]any)
	require.True(t, ok)
	require.Len(t, dataParts, 1)
	assert.Equal(t, map[string]any{"rows": float64(3)}, dataParts[0])
	files, ok := receivedInput["files"].([]any)
	require.True(t, ok)
	require.Len(t, files, 1)
	file, ok := files[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "https://files.example/input.csv", file["uri"])
	assert.Equal(t, "input.csv", file["name"])
	assert.Equal(t, "text/csv", file["mimeType"])
}

func TestProtocolServiceValidationAndOwnershipEdges(t *testing.T) {
	pool, svc, _ := setupService(t)
	owner := insertCreator(t, pool)
	other := insertCreator(t, pool)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"output":{"summary":"edge task done"}}`))
	}))
	defer server.Close()

	agentID := insertAgent(t, pool, owner, server.URL)
	otherAgentID := insertAgent(t, pool, owner, server.URL)
	slug := "a2a-" + agentID.String()[:8]
	otherSlug := "a2a-" + otherAgentID.String()[:8]
	params := &a2a.A2AMessageSendParams{Message: a2a.A2AMessage{
		Kind:  "message",
		Role:  "user",
		Parts: []map[string]any{{"kind": "text", "text": "validate service edges"}},
	}}

	_, err := svc.SendProtocolMessage(context.Background(), owner, " ", params)
	requireA2AServiceHTTPStatus(t, err, http.StatusBadRequest)
	_, err = svc.SendProtocolMessage(context.Background(), owner, slug, nil)
	requireA2AServiceHTTPStatus(t, err, http.StatusBadRequest)
	_, err = svc.SendProtocolMessage(context.Background(), owner, "missing-agent", params)
	requireA2AServiceHTTPStatus(t, err, http.StatusNotFound)
	_, err = svc.SendProtocolMessage(context.Background(), owner, slug, &a2a.A2AMessageSendParams{Message: a2a.A2AMessage{Role: "user"}})
	requireA2AServiceHTTPStatus(t, err, http.StatusUnprocessableEntity)

	_, err = svc.StartProtocolMessage(context.Background(), owner, " ", params)
	requireA2AServiceHTTPStatus(t, err, http.StatusBadRequest)
	_, err = svc.StartProtocolMessage(context.Background(), owner, slug, nil)
	requireA2AServiceHTTPStatus(t, err, http.StatusBadRequest)
	_, err = svc.StartProtocolMessage(context.Background(), owner, "missing-agent", params)
	requireA2AServiceHTTPStatus(t, err, http.StatusNotFound)

	task, err := svc.SendProtocolMessage(context.Background(), owner, slug, params)
	require.NoError(t, err)

	_, err = svc.GetProtocolTask(context.Background(), owner, "", task.ID, nil)
	requireA2AServiceHTTPStatus(t, err, http.StatusBadRequest)
	_, err = svc.GetProtocolTask(context.Background(), owner, slug, "not-a-uuid", nil)
	requireA2AServiceHTTPStatus(t, err, http.StatusBadRequest)
	_, err = svc.GetProtocolTask(context.Background(), other, slug, task.ID, nil)
	requireA2AServiceHTTPStatus(t, err, http.StatusNotFound)
	_, err = svc.GetProtocolTask(context.Background(), owner, otherSlug, task.ID, nil)
	requireA2AServiceHTTPStatus(t, err, http.StatusNotFound)

	_, err = svc.ListProtocolTasks(context.Background(), owner, slug, &a2a.A2ATaskListParams{PageToken: "bad-token"})
	requireA2AServiceHTTPStatus(t, err, http.StatusBadRequest)
	_, err = svc.ListProtocolTasks(context.Background(), owner, slug, &a2a.A2ATaskListParams{StatusTimestampAfter: "not-time"})
	requireA2AServiceHTTPStatus(t, err, http.StatusBadRequest)

	pushSvc := webhook.NewService(pool, &config.Config{AllowLocalHTTPEndpoints: true})
	svc.SetTaskCallbackManager(pushSvc)
	pushParams := a2a.A2ATaskPushConfigParams{
		ID:                     "not-a-uuid",
		PushNotificationConfig: a2a.A2APushNotificationConfig{URL: server.URL + "/push"},
	}
	_, err = svc.SetPushNotificationConfig(context.Background(), owner, slug, &pushParams)
	requireA2AServiceHTTPStatus(t, err, http.StatusBadRequest)

	missingRunID := uuid.NewString()
	_, err = svc.ListPushNotificationConfigs(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{ID: missingRunID})
	requireA2AServiceHTTPStatus(t, err, http.StatusNotFound)
	_, err = svc.GetPushNotificationConfig(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{
		ID:                       missingRunID,
		PushNotificationConfigID: uuid.NewString(),
	})
	requireA2AServiceHTTPStatus(t, err, http.StatusNotFound)
	err = svc.DeletePushNotificationConfig(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{
		ID:                       missingRunID,
		PushNotificationConfigID: uuid.NewString(),
	})
	requireA2AServiceHTTPStatus(t, err, http.StatusNotFound)
}

func TestProtocolStreamEventsExposeStatusAndArtifactUpdates(t *testing.T) {
	pool, svc, _ := setupService(t)
	owner := insertCreator(t, pool)

	release := make(chan struct{})
	called := make(chan struct{}, 1)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case called <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"output": map[string]any{"summary": "stream completed"},
			"events": []map[string]any{
				{"event_type": "run.message.delta", "payload": map[string]any{"text": "working"}},
				{"event_type": "run.artifact.delta", "payload": map[string]any{
					"artifact_id": "stream-report",
					"title":       "Stream Report",
					"data":        map[string]any{"ok": true},
					"last_chunk":  true,
				}},
			},
		})
	}))
	defer server.Close()
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	defaultTransport := http.DefaultTransport
	http.DefaultTransport = server.Client().Transport
	t.Cleanup(func() { http.DefaultTransport = defaultTransport })

	agentID := insertAgent(t, pool, owner, server.URL)
	slug := "a2a-" + agentID.String()[:8]

	task, err := svc.StartProtocolMessage(context.Background(), owner, slug, &a2a.A2AMessageSendParams{
		Message: a2a.A2AMessage{
			Kind:      "message",
			MessageID: "msg-stream",
			ContextID: "ctx-stream",
			Role:      "user",
			Parts:     []map[string]any{{"kind": "text", "text": "stream it"}},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "working", task.Status.State)

	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("async endpoint was not called")
	}

	items, terminal, nextSequence, err := svc.ListProtocolTaskEvents(context.Background(), owner, slug, task.ID, 0)
	require.NoError(t, err)
	assert.False(t, terminal)
	assert.GreaterOrEqual(t, len(items), 2)
	assert.Greater(t, nextSequence, int32(0))

	close(release)
	var finalItems []interface{}
	require.Eventually(t, func() bool {
		var err error
		finalItems, terminal, _, err = svc.ListProtocolTaskEvents(context.Background(), owner, slug, task.ID, 0)
		return err == nil && terminal
	}, time.Second, 20*time.Millisecond)

	var sawFinal bool
	var sawArtifact bool
	for _, item := range finalItems {
		switch event := item.(type) {
		case *a2a.A2ATaskStatusUpdateEvent:
			if event.Final && event.Status.State == "completed" {
				sawFinal = true
			}
		case *a2a.A2ATaskArtifactUpdateEvent:
			if event.Artifact.ArtifactID == "stream-report" {
				sawArtifact = true
			}
		}
	}
	assert.True(t, sawFinal, "expected final completed status-update")
	assert.True(t, sawArtifact, "expected artifact-update from run.artifact.delta")
}

func TestProtocolMessageReturnImmediatelyStartsAsyncTask(t *testing.T) {
	pool, svc, _ := setupService(t)
	owner := insertCreator(t, pool)

	called := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-called:
		default:
			close(called)
		}
		<-release
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"output":{"summary":"async done"}}`))
	}))
	defer server.Close()
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	defaultTransport := http.DefaultTransport
	http.DefaultTransport = server.Client().Transport
	t.Cleanup(func() { http.DefaultTransport = defaultTransport })

	agentID := insertAgent(t, pool, owner, server.URL)
	slug := "a2a-" + agentID.String()[:8]
	returnImmediately := true
	done := make(chan struct {
		task *a2a.A2ATask
		err  error
	}, 1)
	go func() {
		task, err := svc.SendProtocolMessage(context.Background(), owner, slug, &a2a.A2AMessageSendParams{
			Message: a2a.A2AMessage{
				MessageID: "msg-return-immediately",
				ContextID: "ctx-return-immediately",
				Role:      "ROLE_USER",
				Parts:     []map[string]any{{"text": "return now"}},
			},
			Configuration: &a2a.A2ASendConfiguration{ReturnImmediately: &returnImmediately},
		})
		done <- struct {
			task *a2a.A2ATask
			err  error
		}{task: task, err: err}
	}()

	var result struct {
		task *a2a.A2ATask
		err  error
	}
	select {
	case result = <-done:
	case <-time.After(500 * time.Millisecond):
		close(release)
		t.Fatal("SendProtocolMessage blocked despite returnImmediately=true")
	}
	require.NoError(t, result.err)
	require.NotNil(t, result.task)
	assert.Equal(t, "working", result.task.Status.State)
	assert.Equal(t, "ctx-return-immediately", result.task.ContextID)

	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("async endpoint was not called")
	}
	close(release)
}

func TestProtocolListTasksSupportsPagingFiltersAndArtifacts(t *testing.T) {
	pool, svc, _ := setupService(t)
	owner := insertCreator(t, pool)

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Input map[string]any `json:"input"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"output": map[string]any{
				"summary": "listed " + request.Input["message"].(string),
				"artifacts": []map[string]any{{
					"title":         "List Evidence",
					"artifact_type": "json",
					"content":       map[string]any{"message": request.Input["message"]},
				}},
			},
			"events": []map[string]any{{
				"event_type": "run.message.delta",
				"payload": map[string]any{
					"text": "history for " + request.Input["message"].(string),
				},
			}},
		})
	}))
	defer server.Close()
	defaultTransport := http.DefaultTransport
	http.DefaultTransport = server.Client().Transport
	t.Cleanup(func() { http.DefaultTransport = defaultTransport })

	agentID := insertAgent(t, pool, owner, server.URL)
	slug := "a2a-" + agentID.String()[:8]
	first, err := svc.SendProtocolMessage(context.Background(), owner, slug, &a2a.A2AMessageSendParams{
		Message: a2a.A2AMessage{
			MessageID: "msg-list-1",
			ContextID: "ctx-list-1",
			Role:      "user",
			Parts:     []map[string]any{{"kind": "text", "text": "first"}},
		},
	})
	require.NoError(t, err)
	second, err := svc.SendProtocolMessage(context.Background(), owner, slug, &a2a.A2AMessageSendParams{
		Message: a2a.A2AMessage{
			MessageID: "msg-list-2",
			ContextID: "ctx-list-2",
			Role:      "user",
			Parts:     []map[string]any{{"kind": "text", "text": "second"}},
		},
	})
	require.NoError(t, err)

	pageSize := 1
	historyLength := 1
	includeArtifacts := true
	page, err := svc.ListProtocolTasks(context.Background(), owner, slug, &a2a.A2ATaskListParams{
		Status:           "completed",
		PageSize:         &pageSize,
		HistoryLength:    &historyLength,
		IncludeArtifacts: &includeArtifacts,
	})
	require.NoError(t, err)
	require.Len(t, page.Tasks, 1)
	assert.Equal(t, int32(1), page.PageSize)
	assert.Equal(t, int32(2), page.TotalSize)
	assert.NotEmpty(t, page.NextPageToken)
	assert.NotEmpty(t, page.Tasks[0].Artifacts)
	assert.NotEmpty(t, page.Tasks[0].History)

	nextPage, err := svc.ListProtocolTasks(context.Background(), owner, slug, &a2a.A2ATaskListParams{
		Status:    "completed",
		PageSize:  &pageSize,
		PageToken: page.NextPageToken,
	})
	require.NoError(t, err)
	require.Len(t, nextPage.Tasks, 1)
	assert.Empty(t, nextPage.NextPageToken)

	listedIDs := map[string]bool{
		page.Tasks[0].ID:     true,
		nextPage.Tasks[0].ID: true,
	}
	assert.True(t, listedIDs[first.ID])
	assert.True(t, listedIDs[second.ID])

	byContext, err := svc.ListProtocolTasks(context.Background(), owner, slug, &a2a.A2ATaskListParams{ContextID: "ctx-list-1"})
	require.NoError(t, err)
	require.Len(t, byContext.Tasks, 1)
	assert.Equal(t, first.ID, byContext.Tasks[0].ID)
	assert.Equal(t, int32(1), byContext.TotalSize)

	_, err = svc.ListProtocolTasks(context.Background(), owner, "", nil)
	requireA2AServiceHTTPStatus(t, err, http.StatusBadRequest)
	_, err = svc.ListProtocolTasks(context.Background(), owner, "missing", nil)
	requireA2AServiceHTTPStatus(t, err, http.StatusNotFound)
	_, err = svc.ListProtocolTasks(context.Background(), owner, slug, &a2a.A2ATaskListParams{Status: "strange"})
	requireA2AServiceHTTPStatus(t, err, http.StatusBadRequest)
}

func TestProtocolCancelTaskMapsRunCancellation(t *testing.T) {
	pool, svc, _ := setupService(t)
	owner := insertCreator(t, pool)

	release := make(chan struct{})
	called := make(chan struct{}, 1)
	releaseServer := func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case called <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"output":{"summary":"too late"}}`))
	}))
	defer server.Close()
	t.Cleanup(releaseServer)
	defaultTransport := http.DefaultTransport
	http.DefaultTransport = server.Client().Transport
	t.Cleanup(func() { http.DefaultTransport = defaultTransport })

	agentID := insertAgent(t, pool, owner, server.URL)
	slug := "a2a-" + agentID.String()[:8]
	task, err := svc.StartProtocolMessage(context.Background(), owner, slug, &a2a.A2AMessageSendParams{
		Message: a2a.A2AMessage{
			MessageID: "msg-cancel",
			ContextID: "ctx-cancel",
			Role:      "user",
			Parts:     []map[string]any{{"kind": "text", "text": "cancel me"}},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "working", task.Status.State)

	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("async endpoint was not called")
	}

	canceled, err := svc.CancelProtocolTask(context.Background(), owner, slug, task.ID)
	require.NoError(t, err)
	assert.Equal(t, task.ID, canceled.ID)
	assert.Equal(t, "ctx-cancel", canceled.ContextID)
	assert.Equal(t, "canceled", canceled.Status.State)
	require.NotNil(t, canceled.Status.Message)
	assert.Contains(t, canceled.Status.Message.Parts[0]["text"], "canceled")
	releaseServer()

	_, err = svc.CancelProtocolTask(context.Background(), owner, slug, "not-a-uuid")
	requireA2AServiceHTTPStatus(t, err, http.StatusBadRequest)
	_, err = svc.CancelProtocolTask(context.Background(), owner, slug, uuid.NewString())
	requireA2AServiceHTTPStatus(t, err, http.StatusNotFound)
}

func TestProtocolMessageCreatesInlinePushNotificationConfig(t *testing.T) {
	pool, svc, _ := setupService(t)
	owner := insertCreator(t, pool)
	pushSvc := webhook.NewService(pool, &config.Config{AllowLocalHTTPEndpoints: true})
	svc.SetTaskCallbackManager(pushSvc)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"output":{"summary":"inline push configured"}}`))
	}))
	defer server.Close()

	agentID := insertAgent(t, pool, owner, server.URL)
	slug := "a2a-" + agentID.String()[:8]
	task, err := svc.SendProtocolMessage(context.Background(), owner, slug, &a2a.A2AMessageSendParams{
		Message: a2a.A2AMessage{
			MessageID: "msg-inline-push",
			ContextID: "ctx-inline-push",
			Role:      "user",
			Parts:     []map[string]any{{"kind": "text", "text": "configure inline push"}},
		},
		Configuration: &a2a.A2ASendConfiguration{
			PushNotificationConfig: &a2a.A2APushNotificationConfig{
				URL:   server.URL + "/inline-push",
				Token: "inline-token",
				Metadata: map[string]any{
					"client": "inline-a2a",
				},
			},
		},
	})
	require.NoError(t, err)

	list, err := svc.ListPushNotificationConfigs(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{ID: task.ID})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, task.ID, list.Items[0].TaskID)
	assert.Equal(t, server.URL+"/inline-push", list.Items[0].PushNotificationConfig.URL)
	assert.Equal(t, "Bearer", list.Items[0].PushNotificationConfig.Authentication.Scheme)
	assert.Equal(t, "inline-a2a", list.Items[0].PushNotificationConfig.Metadata["client"])
}

func TestPushNotificationConfigMapsToTaskCallback(t *testing.T) {
	pool, svc, _ := setupService(t)
	owner := insertCreator(t, pool)
	pushSvc := webhook.NewService(pool, &config.Config{AllowLocalHTTPEndpoints: true})
	svc.SetTaskCallbackManager(pushSvc)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"output":{"summary":"push config task done"}}`))
	}))
	defer server.Close()
	defaultTransport := http.DefaultTransport
	http.DefaultTransport = server.Client().Transport
	t.Cleanup(func() { http.DefaultTransport = defaultTransport })

	agentID := insertAgent(t, pool, owner, server.URL)
	slug := "a2a-" + agentID.String()[:8]
	task, err := svc.SendProtocolMessage(context.Background(), owner, slug, &a2a.A2AMessageSendParams{
		Message: a2a.A2AMessage{
			Kind:  "message",
			Role:  "user",
			Parts: []map[string]any{{"kind": "text", "text": "configure push"}},
		},
	})
	require.NoError(t, err)

	cfg, err := svc.SetPushNotificationConfig(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{
		ID: task.ID,
		PushNotificationConfig: a2a.A2APushNotificationConfig{
			URL:   server.URL + "/push",
			Token: "test-token",
			Metadata: map[string]any{
				"client": "a2a-test",
			},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, cfg.PushNotificationConfig.ID)
	assert.Equal(t, task.ID, cfg.TaskID)
	assert.Equal(t, "Bearer", cfg.PushNotificationConfig.Authentication.Scheme)

	list, err := svc.ListPushNotificationConfigs(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{ID: task.ID})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, cfg.PushNotificationConfig.ID, list.Items[0].PushNotificationConfig.ID)
	assert.Equal(t, "Bearer", list.Items[0].PushNotificationConfig.Authentication.Scheme)
	assert.Equal(t, "a2a-test", list.Items[0].PushNotificationConfig.Metadata["client"])

	got, err := svc.GetPushNotificationConfig(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{
		ID:                       task.ID,
		PushNotificationConfigID: cfg.PushNotificationConfig.ID,
	})
	require.NoError(t, err)
	assert.Equal(t, cfg.PushNotificationConfig.ID, got.PushNotificationConfig.ID)

	err = svc.DeletePushNotificationConfig(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{
		ID:                       task.ID,
		PushNotificationConfigID: cfg.PushNotificationConfig.ID,
	})
	require.NoError(t, err)
	list, err = svc.ListPushNotificationConfigs(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{ID: task.ID})
	require.NoError(t, err)
	assert.Empty(t, list.Items)
}

func TestPushNotificationConfigLookupAndValidationEdges(t *testing.T) {
	pool, svc, _ := setupService(t)
	owner := insertCreator(t, pool)
	pushSvc := webhook.NewService(pool, &config.Config{AllowLocalHTTPEndpoints: true})
	svc.SetTaskCallbackManager(pushSvc)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"output":{"summary":"push edge task done"}}`))
	}))
	defer server.Close()

	agentID := insertAgent(t, pool, owner, server.URL)
	slug := "a2a-" + agentID.String()[:8]
	task, err := svc.SendProtocolMessage(context.Background(), owner, slug, &a2a.A2AMessageSendParams{
		Message: a2a.A2AMessage{
			Kind:  "message",
			Role:  "user",
			Parts: []map[string]any{{"kind": "text", "text": "exercise push lookup edges"}},
		},
	})
	require.NoError(t, err)

	_, err = svc.SetPushNotificationConfig(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{
		ID:                     task.ID,
		PushNotificationConfig: a2a.A2APushNotificationConfig{URL: " "},
	})
	requireA2AServiceHTTPStatus(t, err, http.StatusBadRequest)

	_, err = svc.GetPushNotificationConfig(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{ID: task.ID})
	requireA2AServiceHTTPStatus(t, err, http.StatusBadRequest)

	cfg1, err := svc.SetPushNotificationConfig(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{
		ID: task.ID,
		PushNotificationConfig: a2a.A2APushNotificationConfig{
			URL:        server.URL + "/push-1",
			EventTypes: []string{"run.completed"},
			Authentication: &a2a.A2APushAuthenticationInfo{
				Scheme:      "HMAC",
				Credentials: "secret-1",
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "HMAC", cfg1.PushNotificationConfig.Authentication.Scheme)
	assert.Empty(t, cfg1.PushNotificationConfig.Authentication.Credentials)

	auto, err := svc.GetPushNotificationConfig(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{ID: task.ID})
	require.NoError(t, err)
	assert.Equal(t, cfg1.PushNotificationConfig.ID, auto.PushNotificationConfig.ID)

	byEmbeddedID, err := svc.GetPushNotificationConfig(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{
		ID: task.ID,
		PushNotificationConfig: a2a.A2APushNotificationConfig{
			ID: cfg1.PushNotificationConfig.ID,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, cfg1.PushNotificationConfig.ID, byEmbeddedID.PushNotificationConfig.ID)

	cfg2, err := svc.SetPushNotificationConfig(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{
		ID: task.ID,
		PushNotificationConfig: a2a.A2APushNotificationConfig{
			URL:   server.URL + "/push-2",
			Token: "token-2",
		},
	})
	require.NoError(t, err)

	_, err = svc.GetPushNotificationConfig(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{ID: task.ID})
	requireA2AServiceHTTPStatus(t, err, http.StatusBadRequest)

	_, err = svc.GetPushNotificationConfig(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{
		ID:                       task.ID,
		PushNotificationConfigID: uuid.NewString(),
	})
	requireA2AServiceHTTPStatus(t, err, http.StatusNotFound)

	err = svc.DeletePushNotificationConfig(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{
		ID:                       task.ID,
		PushNotificationConfigID: "bad",
	})
	requireA2AServiceHTTPStatus(t, err, http.StatusBadRequest)

	err = svc.DeletePushNotificationConfig(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{
		ID: task.ID,
		PushNotificationConfig: a2a.A2APushNotificationConfig{
			ID: cfg2.PushNotificationConfig.ID,
		},
	})
	require.NoError(t, err)

	list, err := svc.ListPushNotificationConfigs(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{ID: task.ID})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, cfg1.PushNotificationConfig.ID, list.Items[0].PushNotificationConfig.ID)
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

func requireA2AServiceHTTPStatus(t *testing.T, err error, want int) {
	t.Helper()
	require.Error(t, err)
	httpErr, ok := err.(*httpx.HTTPError)
	require.True(t, ok, "expected *httpx.HTTPError, got %T (%v)", err, err)
	assert.Equal(t, want, httpErr.Status)
}
