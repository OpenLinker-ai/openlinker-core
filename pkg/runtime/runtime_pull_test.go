package runtime_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"

	"github.com/kinzhi/openlinker-core/pkg/httpx"
	"github.com/kinzhi/openlinker-core/pkg/runtime"
)

func setRuntimePullMode(t *testing.T, pool *pgxpool.Pool, agentID uuid.UUID) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`UPDATE agents
		 SET connection_mode='runtime_pull',
		     endpoint_url=$2
		 WHERE id=$1`,
		agentID,
		"openlinker-runtime-pull://"+agentID.String(),
	)
	require.NoError(t, err)
}

func setRuntimeWSMode(t *testing.T, pool *pgxpool.Pool, agentID uuid.UUID) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`UPDATE agents
		 SET connection_mode='runtime_ws',
		     endpoint_url=$2
		 WHERE id=$1`,
		agentID,
		"openlinker-runtime-ws://"+agentID.String(),
	)
	require.NoError(t, err)
}

func insertRuntimeToken(t *testing.T, pool *pgxpool.Pool, agentID, creatorID uuid.UUID, scopes []string) string {
	t.Helper()
	raw := make([]byte, 32)
	_, err := rand.Read(raw)
	require.NoError(t, err)
	plaintext := "rt_live_" + hex.EncodeToString(raw)
	hash, err := bcrypt.GenerateFromPassword([]byte(plaintext), bcrypt.DefaultCost)
	require.NoError(t, err)
	_, err = pool.Exec(context.Background(),
		`INSERT INTO agent_runtime_tokens (
			agent_id, created_by_user_id, name, prefix, token_hash, scopes
		) VALUES ($1, $2, 'test-runtime', $3, $4, $5)`,
		agentID,
		creatorID,
		plaintext[:12],
		string(hash),
		scopes,
	)
	require.NoError(t, err)
	return plaintext
}

func readRuntimeTokenLastUsed(t *testing.T, pool *pgxpool.Pool, plaintext string) *time.Time {
	t.Helper()
	var lastUsed sql.NullTime
	err := pool.QueryRow(context.Background(),
		`SELECT last_used_at FROM agent_runtime_tokens WHERE prefix=$1`,
		plaintext[:12],
	).Scan(&lastUsed)
	require.NoError(t, err)
	if !lastUsed.Valid {
		return nil
	}
	return &lastUsed.Time
}

func markRuntimePullAvailable(t *testing.T, svc *runtime.Service, token string) {
	t.Helper()
	_, err := svc.HeartbeatAgent(context.Background(), token)
	require.NoError(t, err)
}

func TestRuntimePull_ClaimAndCompleteSuccess(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, creatorID, "https://example.com/not-used", 10, "approved")
	setRuntimePullMode(t, pool, agentID)
	token := insertRuntimeToken(t, pool, agentID, creatorID, []string{"agent:call", "agent:pull"})
	markRuntimePullAvailable(t, svc, token)

	started, err := svc.Run(ctx, userID, makeRunReq(agentID, map[string]any{"q": "from user"}), "")
	require.NoError(t, err)
	require.Equal(t, "running", started.Status)
	runID := mustParseUUID(t, started.RunID)

	claimed, err := svc.ClaimRuntimePullRun(ctx, token)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, started.RunID, claimed.RunID)
	assert.Equal(t, agentID.String(), claimed.AgentID)
	assert.Equal(t, "from user", claimed.Input["q"])
	assert.Equal(t, "web", claimed.Source)
	assert.Equal(t, "/api/v1/agent-runtime/runs/"+started.RunID+"/result", claimed.ResultEndpoint)
	assert.Equal(t, "POST", claimed.ResultMethod)
	assert.True(t, claimed.ResultRequired)
	assert.Equal(t, 300, claimed.Metadata["claim_ttl_seconds"])
	assert.Equal(t, 0, claimed.Metadata["recommended_next_claim_after_seconds"])
	assert.Equal(t, 60, claimed.Metadata["recommended_heartbeat_after_seconds"])
	assert.Equal(t, 30, claimed.Metadata["max_long_poll_wait_seconds"])
	assert.Equal(t, true, claimed.Metadata["result_required"])
	assert.Equal(t, 900, claimed.Metadata["result_timeout_seconds"])
	require.NotNil(t, claimed.A2A)
	assert.Equal(t, started.RunID, claimed.A2A.CurrentRunID)
	assert.Equal(t, "http://localhost:8080/api/v1/agent-runtime/call-agent", claimed.A2A.CallAgentEndpoint)
	assert.Contains(t, claimed.A2A.RuntimeScopes, "agent:call")

	completed, err := svc.CompleteRuntimePullRun(ctx, token, runID, &runtime.RuntimePullResultRequest{
		Status: "success",
		Output: map[string]interface{}{
			"answer": "done by local agent",
		},
		Events: []runtime.AgentEvent{{
			EventType: "run.message.delta",
			Payload:   map[string]interface{}{"text": "local step"},
		}},
	})
	require.NoError(t, err)
	require.Equal(t, "success", completed.Status)
	assert.Equal(t, "done by local agent", completed.Output["answer"])

	reloaded, err := svc.GetRun(ctx, userID, runID)
	require.NoError(t, err)
	assert.Equal(t, "success", reloaded.Status)
	assert.Equal(t, "done by local agent", reloaded.Output["answer"])

	again, err := svc.ClaimRuntimePullRun(ctx, token)
	require.NoError(t, err)
	assert.Nil(t, again)

	events := readRunEvents(t, pool, runID)
	var eventTypes []string
	for _, event := range events {
		eventTypes = append(eventTypes, event.EventType)
	}
	assert.Contains(t, eventTypes, "run.dispatch.pending")
	assert.Contains(t, eventTypes, "run.dispatch.claimed")
	assert.Contains(t, eventTypes, "run.message.delta")
	assert.Contains(t, eventTypes, "run.completed")
}

func TestRuntimePull_RunDelegatedQueuesFreeChildAndCompletesParent(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000)
	callerCreatorID := insertCreator(t, pool)
	targetCreatorID := insertCreator(t, pool)
	callerAgentID := insertAgent(t, pool, callerCreatorID, "https://example.com/caller", 10, "approved")
	targetAgentID := insertAgent(t, pool, targetCreatorID, "https://example.com/not-used", 25, "approved")
	setRuntimePullMode(t, pool, targetAgentID)
	token := insertRuntimeToken(t, pool, targetAgentID, targetCreatorID, []string{"agent:call", "agent:pull"})
	markRuntimePullAvailable(t, svc, token)
	parentRunID := insertRunningRun(t, pool, userID, callerAgentID)

	started, err := svc.RunDelegated(ctx, userID, runtime.Delegation{
		ParentRunID:   parentRunID,
		CallerAgentID: callerAgentID,
		Reason:        "delegate queued runtime",
	}, makeRunReq(targetAgentID, map[string]any{"q": "queued child"}))
	require.NoError(t, err)
	require.Equal(t, "running", started.Status)
	assert.Equal(t, int32(0), started.CostCents)
	assert.Equal(t, parentRunID.String(), started.ParentRunID)
	assert.Equal(t, callerAgentID.String(), started.CallerAgentID)
	assert.Equal(t, "free_delegation", started.BillingMode)
	childRunID := mustParseUUID(t, started.RunID)

	child := readRun(t, pool, childRunID)
	assert.Equal(t, "running", child.Status)
	assert.Equal(t, int32(0), child.CostCents)
	assert.Equal(t, int32(0), child.PlatformFeeCents)
	assert.Equal(t, int32(0), child.CreatorRevenueCents)

	claimed, err := svc.ClaimRuntimePullRun(ctx, token)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, started.RunID, claimed.RunID)
	assert.Equal(t, "api", claimed.Source)
	require.NotNil(t, claimed.A2A)
	assert.Equal(t, started.RunID, claimed.A2A.CurrentRunID)
	assert.Equal(t, parentRunID.String(), claimed.A2A.ParentRunID)
	assert.Equal(t, callerAgentID.String(), claimed.A2A.CallerAgentID)
	assert.Equal(t, "http://localhost:8080/api/v1/agent-runtime/call-agent", claimed.A2A.CallAgentEndpoint)
	assert.Contains(t, claimed.A2A.RuntimeScopes, "agent:call")

	completed, err := svc.CompleteRuntimePullRun(ctx, token, childRunID, &runtime.RuntimePullResultRequest{
		Status: "success",
		Output: map[string]interface{}{"answer": "delegated runtime ok"},
		Events: []runtime.AgentEvent{{
			EventType: "run.message.delta",
			Payload:   map[string]interface{}{"text": "queued child progress"},
		}},
	})
	require.NoError(t, err)
	require.Equal(t, "success", completed.Status)
	assert.Equal(t, int32(0), completed.CostCents)
	assert.Equal(t, parentRunID.String(), completed.ParentRunID)
	assert.Equal(t, callerAgentID.String(), completed.CallerAgentID)
	assert.Equal(t, "free_delegation", completed.BillingMode)
	assert.Equal(t, "delegated runtime ok", completed.Output["answer"])

	reloaded, err := svc.GetRun(ctx, userID, childRunID)
	require.NoError(t, err)
	assert.Equal(t, "success", reloaded.Status)
	assert.Equal(t, parentRunID.String(), reloaded.ParentRunID)
	assert.Equal(t, callerAgentID.String(), reloaded.CallerAgentID)
	assert.Equal(t, "free_delegation", reloaded.BillingMode)

	parentEvents := readRunEvents(t, pool, parentRunID)
	var parentEventTypes []string
	for _, event := range parentEvents {
		parentEventTypes = append(parentEventTypes, event.EventType)
	}
	assert.Contains(t, parentEventTypes, "run.child.created")
	assert.Contains(t, parentEventTypes, "run.child.completed")
	assert.Equal(t, started.RunID, parentEvents[0].Payload["child_run_id"])
	assert.Equal(t, "success", parentEvents[len(parentEvents)-1].Payload["status"])

	childEvents := readRunEvents(t, pool, childRunID)
	var childEventTypes []string
	for _, event := range childEvents {
		childEventTypes = append(childEventTypes, event.EventType)
	}
	assert.Contains(t, childEventTypes, "run.dispatch.pending")
	assert.Contains(t, childEventTypes, "run.dispatch.claimed")
	assert.Contains(t, childEventTypes, "run.message.delta")
	assert.Contains(t, childEventTypes, "run.completed")
	require.NotNil(t, childEvents[0].ParentRunID)
	assert.Equal(t, parentRunID, *childEvents[0].ParentRunID)
	assert.Equal(t, "free_delegation", childEvents[0].Payload["billing_mode"])
}

func TestRuntimeWS_AssignsRunAndAcceptsResultOverOpenConnection(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, creatorID, "https://example.com/not-used", 10, "approved")
	setRuntimeWSMode(t, pool, agentID)
	token := insertRuntimeToken(t, pool, agentID, creatorID, []string{"agent:call", "agent:pull"})

	e := echo.New()
	e.HTTPErrorHandler = func(err error, c echo.Context) {
		if c.Response().Committed {
			return
		}
		_ = httpx.SendError(c, err)
	}
	runtime.NewHandler(svc).RegisterAgentRuntime(e.Group("/api/v1"))
	server := httptest.NewServer(e)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/api/v1/agent-runtime/ws"
	header := http.Header{}
	header.Set("Authorization", "Bearer "+token)
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, header)
	require.NoError(t, err)
	if resp != nil {
		defer resp.Body.Close()
	}
	defer conn.Close()

	var ready runtime.RuntimeWSServerMessage
	require.NoError(t, conn.ReadJSON(&ready))
	require.Equal(t, "runtime.ready", ready.Type)
	require.Equal(t, agentID.String(), ready.AgentID)
	require.NotNil(t, ready.Heartbeat)
	require.Equal(t, "healthy", ready.Heartbeat.AvailabilityStatus)

	started, err := svc.Run(ctx, userID, makeRunReq(agentID, map[string]any{"q": "via ws"}), "")
	require.NoError(t, err)
	require.Equal(t, "running", started.Status)

	var assigned runtime.RuntimeWSServerMessage
	require.NoError(t, conn.ReadJSON(&assigned))
	require.Equal(t, "run.assigned", assigned.Type)
	require.Equal(t, started.RunID, assigned.RunID)
	require.Equal(t, agentID.String(), assigned.AgentID)
	require.Equal(t, "via ws", assigned.Input["q"])
	require.NotNil(t, assigned.A2A)
	require.Equal(t, started.RunID, assigned.A2A.CurrentRunID)

	require.NoError(t, conn.WriteJSON(runtime.RuntimeWSClientMessage{
		Type:      "run.event",
		ID:        "event-1",
		RunID:     assigned.RunID,
		EventType: "run.message.delta",
		Payload:   map[string]interface{}{"text": "ws progress"},
	}))

	var eventAck runtime.RuntimeWSServerMessage
	require.NoError(t, conn.ReadJSON(&eventAck))
	require.Equal(t, "run.event.accepted", eventAck.Type)
	require.Equal(t, "event-1", eventAck.ID)
	require.Equal(t, assigned.RunID, eventAck.RunID)
	require.NotNil(t, eventAck.Event)
	require.Equal(t, "run.message.delta", eventAck.Event.EventType)

	require.NoError(t, conn.WriteJSON(runtime.RuntimeWSClientMessage{
		Type:   "run.result",
		ID:     "result-1",
		RunID:  assigned.RunID,
		Status: "success",
		Output: map[string]interface{}{"answer": "done over websocket"},
		Events: []runtime.AgentEvent{{
			EventType: "run.message.delta",
			Payload:   map[string]interface{}{"text": "ws step"},
		}},
	}))

	var ack runtime.RuntimeWSServerMessage
	require.NoError(t, conn.ReadJSON(&ack))
	require.Equal(t, "run.result.accepted", ack.Type)
	require.Equal(t, "result-1", ack.ID)
	require.Equal(t, assigned.RunID, ack.RunID)
	require.NotNil(t, ack.Result)
	require.Equal(t, "success", ack.Result.Status)

	reloaded, err := svc.GetRun(ctx, userID, mustParseUUID(t, started.RunID))
	require.NoError(t, err)
	require.Equal(t, "success", reloaded.Status)
	require.Equal(t, "done over websocket", reloaded.Output["answer"])

	events := readRunEvents(t, pool, mustParseUUID(t, started.RunID))
	var eventTypes []string
	for _, event := range events {
		eventTypes = append(eventTypes, event.EventType)
	}
	assert.Contains(t, eventTypes, "run.dispatch.claimed")
	assert.Contains(t, eventTypes, "run.message.delta")
	assert.Contains(t, eventTypes, "run.completed")
}

func TestRuntimeWS_HeartbeatClaimAndProtocolMessages(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)

	creatorID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, creatorID, "https://example.com/not-used", 10, "approved")
	setRuntimeWSMode(t, pool, agentID)
	token := insertRuntimeToken(t, pool, agentID, creatorID, []string{"agent:call", "agent:pull"})

	e := echo.New()
	e.HTTPErrorHandler = func(err error, c echo.Context) {
		if c.Response().Committed {
			return
		}
		_ = httpx.SendError(c, err)
	}
	runtime.NewHandler(svc).RegisterAgentRuntime(e.Group("/api/v1"))
	server := httptest.NewServer(e)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/api/v1/agent-runtime/ws"
	header := http.Header{}
	header.Set("Authorization", "Bearer "+token)
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, header)
	require.NoError(t, err)
	if resp != nil {
		defer resp.Body.Close()
	}
	defer conn.Close()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(3*time.Second)))

	var ready runtime.RuntimeWSServerMessage
	require.NoError(t, conn.ReadJSON(&ready))
	require.Equal(t, "runtime.ready", ready.Type)
	require.Equal(t, agentID.String(), ready.AgentID)
	require.NotNil(t, ready.Heartbeat)

	require.NoError(t, conn.WriteJSON(runtime.RuntimeWSClientMessage{
		Type: "heartbeat",
		ID:   "heartbeat-1",
	}))
	var heartbeat runtime.RuntimeWSServerMessage
	require.NoError(t, conn.ReadJSON(&heartbeat))
	require.Equal(t, "runtime.heartbeat", heartbeat.Type)
	require.Equal(t, "heartbeat-1", heartbeat.ID)
	require.Equal(t, agentID.String(), heartbeat.AgentID)
	require.NotNil(t, heartbeat.Heartbeat)
	require.Equal(t, int32(0), heartbeat.Heartbeat.PendingRunCount)

	require.NoError(t, conn.WriteJSON(runtime.RuntimeWSClientMessage{
		Type: "runtime.claim",
		ID:   "claim-empty",
	}))
	var empty runtime.RuntimeWSServerMessage
	require.NoError(t, conn.ReadJSON(&empty))
	require.Equal(t, "run.empty", empty.Type)
	require.Equal(t, "claim-empty", empty.ID)
	require.Equal(t, agentID.String(), empty.AgentID)
	require.Greater(t, empty.RetryAfterSeconds, int32(0))

	require.NoError(t, conn.WriteJSON(runtime.RuntimeWSClientMessage{
		Type: "not-supported",
		ID:   "unknown-1",
	}))
	var unknown runtime.RuntimeWSServerMessage
	require.NoError(t, conn.ReadJSON(&unknown))
	require.Equal(t, "error", unknown.Type)
	require.Equal(t, "unknown-1", unknown.ID)
	require.NotNil(t, unknown.Error)
	require.Equal(t, "UNKNOWN_WS_MESSAGE", unknown.Error.Code)
}

func TestRuntimePull_ReportRuntimeTokenRunEventValidationAndArtifactDelta(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, creatorID, "https://example.com/not-used", 10, "approved")
	setRuntimePullMode(t, pool, agentID)
	claimingToken := insertRuntimeToken(t, pool, agentID, creatorID, []string{"agent:call", "agent:pull"})
	otherToken := insertRuntimeToken(t, pool, agentID, creatorID, []string{"agent:call", "agent:pull"})
	markRuntimePullAvailable(t, svc, claimingToken)

	started, err := svc.Run(ctx, userID, makeRunReq(agentID, map[string]any{"q": "stream events"}), "")
	require.NoError(t, err)
	runID := mustParseUUID(t, started.RunID)
	claimed, err := svc.ClaimRuntimePullRun(ctx, claimingToken)
	require.NoError(t, err)
	require.NotNil(t, claimed)

	_, err = svc.ReportRuntimeTokenRunEvent(ctx, claimingToken, runID, nil)
	assertHTTPStatus(t, err, http.StatusBadRequest)

	_, err = svc.ReportRuntimeTokenRunEvent(ctx, claimingToken, runID, &runtime.ReportRunEventRequest{
		EventType: "run.unsupported",
	})
	assertHTTPStatus(t, err, http.StatusUnprocessableEntity)

	_, err = svc.ReportRuntimeTokenRunEvent(ctx, otherToken, runID, &runtime.ReportRunEventRequest{
		EventType: "run.message.delta",
		Payload:   map[string]interface{}{"text": "wrong token"},
	})
	assertHTTPStatus(t, err, http.StatusConflict)

	_, err = svc.ReportRuntimeTokenRunEvent(ctx, claimingToken, runID, &runtime.ReportRunEventRequest{
		EventType: "run.message.delta",
		Payload:   map[string]interface{}{"bad": func() {}},
	})
	assertHTTPStatus(t, err, http.StatusBadRequest)

	event, err := svc.ReportRuntimeTokenRunEvent(ctx, claimingToken, runID, &runtime.ReportRunEventRequest{
		EventType: "run.artifact.delta",
		Payload: map[string]interface{}{
			"artifact_id":   "artifact-stream",
			"artifact_type": "text",
			"title":         "Streaming Artifact",
			"text":          "partial artifact content",
			"last_chunk":    true,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, event)
	assert.Equal(t, "run.artifact.delta", event.EventType)

	events := readRunEvents(t, pool, runID)
	var eventTypes []string
	for _, event := range events {
		eventTypes = append(eventTypes, event.EventType)
	}
	assert.Contains(t, eventTypes, "run.dispatch.claimed")
	assert.Contains(t, eventTypes, "run.artifact.delta")
}

func TestRuntimePull_OnlyClaimingTokenCanComplete(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, creatorID, "https://example.com/not-used", 10, "approved")
	setRuntimePullMode(t, pool, agentID)
	claimingToken := insertRuntimeToken(t, pool, agentID, creatorID, []string{"agent:call", "agent:pull"})
	otherToken := insertRuntimeToken(t, pool, agentID, creatorID, []string{"agent:call", "agent:pull"})
	markRuntimePullAvailable(t, svc, claimingToken)

	started, err := svc.Run(ctx, userID, makeRunReq(agentID, map[string]any{"q": "claim"}), "")
	require.NoError(t, err)
	runID := mustParseUUID(t, started.RunID)

	claimed, err := svc.ClaimRuntimePullRun(ctx, claimingToken)
	require.NoError(t, err)
	require.NotNil(t, claimed)

	_, err = svc.CompleteRuntimePullRun(ctx, otherToken, runID, &runtime.RuntimePullResultRequest{
		Status: "success",
		Output: map[string]interface{}{"answer": "wrong token"},
	})
	assertHTTPStatus(t, err, 409)

	completed, err := svc.CompleteRuntimePullRun(ctx, claimingToken, runID, &runtime.RuntimePullResultRequest{
		Status: "failed",
		Error:  &runtime.AgentError{Code: "LOCAL_ERROR", Message: "local worker failed"},
	})
	require.NoError(t, err)
	assert.Equal(t, "failed", completed.Status)
	assert.Equal(t, "LOCAL_ERROR", completed.ErrorCode)

	reloaded, err := svc.GetRun(ctx, userID, runID)
	require.NoError(t, err)
	assert.Equal(t, "failed", reloaded.Status)
	assert.Equal(t, "LOCAL_ERROR", reloaded.ErrorCode)
}

func TestRuntimePull_RequiresPullScope(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, creatorID, "https://example.com/not-used", 10, "approved")
	setRuntimePullMode(t, pool, agentID)
	callOnlyToken := insertRuntimeToken(t, pool, agentID, creatorID, []string{"agent:call"})
	pullToken := insertRuntimeToken(t, pool, agentID, creatorID, []string{"agent:pull"})
	markRuntimePullAvailable(t, svc, pullToken)

	started, err := svc.Run(ctx, userID, makeRunReq(agentID, map[string]any{"q": "scope"}), "")
	require.NoError(t, err)
	require.Equal(t, "running", started.Status)

	claimed, err := svc.ClaimRuntimePullRun(ctx, callOnlyToken)
	require.Nil(t, claimed)
	assertHTTPStatus(t, err, 401)
}

func TestRuntimePull_RunRequiresRecentPullHeartbeat(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, creatorID, "https://example.com/not-used", 10, "approved")
	setRuntimePullMode(t, pool, agentID)
	token := insertRuntimeToken(t, pool, agentID, creatorID, []string{"agent:pull"})

	_, err := svc.Run(ctx, userID, makeRunReq(agentID, map[string]any{"q": "offline"}), "")
	assertHTTPStatus(t, err, 409)
	assert.Equal(t, 0, countRunsForUser(t, pool, userID))

	markRuntimePullAvailable(t, svc, token)
	started, err := svc.Run(ctx, userID, makeRunReq(agentID, map[string]any{"q": "online"}), "")
	require.NoError(t, err)
	assert.Equal(t, "running", started.Status)
}

func TestRuntimePull_StartRunQueuesWhenWorkerOffline(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, creatorID, "https://example.com/not-used", 10, "approved")
	setRuntimePullMode(t, pool, agentID)
	token := insertRuntimeToken(t, pool, agentID, creatorID, []string{"agent:pull"})

	started, err := svc.StartRun(ctx, userID, makeRunReq(agentID, map[string]any{"q": "queue while offline"}), "api")
	require.NoError(t, err)
	assert.Equal(t, "running", started.Status)
	require.NotNil(t, started.NextAction)
	assert.Equal(t, "start_runtime_worker", started.NextAction.Type)
	runID := mustParseUUID(t, started.RunID)

	events := readRunEvents(t, pool, runID)
	var dispatchEvent *runEventRow
	var eventTypes []string
	for i := range events {
		eventTypes = append(eventTypes, events[i].EventType)
		if events[i].EventType == "run.dispatch.waiting_runtime" {
			dispatchEvent = &events[i]
		}
	}
	assert.Contains(t, eventTypes, "run.created")
	assert.Contains(t, eventTypes, "run.started")
	require.NotNil(t, dispatchEvent)
	assert.Equal(t, "runtime_pull", dispatchEvent.Payload["connection_mode"])
	assert.Equal(t, "runtime_offline", dispatchEvent.Payload["reason"])

	claimed, err := svc.ClaimRuntimePullRun(ctx, token)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, started.RunID, claimed.RunID)
	assert.Equal(t, "queue while offline", claimed.Input["q"])
}

func TestRuntimePull_EmptyClaimDoesNotRefreshToken(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	creatorID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, creatorID, "https://example.com/not-used", 10, "approved")
	setRuntimePullMode(t, pool, agentID)
	token := insertRuntimeToken(t, pool, agentID, creatorID, []string{"agent:pull"})
	baseline := time.Now().Add(-2 * time.Minute).UTC().Truncate(time.Microsecond)
	_, err := pool.Exec(ctx,
		`UPDATE agent_runtime_tokens SET last_used_at=$2 WHERE prefix=$1`,
		token[:12],
		baseline,
	)
	require.NoError(t, err)

	claimed, err := svc.ClaimRuntimePullRun(ctx, token)
	require.NoError(t, err)
	assert.Nil(t, claimed)

	lastUsed := readRuntimeTokenLastUsed(t, pool, token)
	require.NotNil(t, lastUsed)
	assert.WithinDuration(t, baseline, *lastUsed, time.Millisecond)
}

func TestRuntimePull_HeartbeatReturnsClaimHints(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, creatorID, "https://example.com/not-used", 10, "approved")
	setRuntimePullMode(t, pool, agentID)
	token := insertRuntimeToken(t, pool, agentID, creatorID, []string{"agent:pull"})

	empty, err := svc.HeartbeatAgent(ctx, token)
	require.NoError(t, err)
	assert.Equal(t, int32(0), empty.PendingRunCount)
	assert.False(t, empty.ClaimNow)
	assert.Equal(t, int32(30), empty.NextClaimAfterSeconds)
	assert.Equal(t, int32(60), empty.RecommendedHeartbeatAfterSeconds)
	assert.Equal(t, int32(30), empty.MaxClaimWaitSeconds)

	started, err := svc.Run(ctx, userID, makeRunReq(agentID, map[string]any{"q": "hint"}), "")
	require.NoError(t, err)
	require.Equal(t, "running", started.Status)

	pending, err := svc.HeartbeatAgent(ctx, token)
	require.NoError(t, err)
	assert.Equal(t, int32(1), pending.PendingRunCount)
	assert.True(t, pending.ClaimNow)
	assert.Equal(t, int32(0), pending.NextClaimAfterSeconds)
}

func TestRuntimePull_LongPollClaimWaitsForRun(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, creatorID, "https://example.com/not-used", 10, "approved")
	setRuntimePullMode(t, pool, agentID)
	token := insertRuntimeToken(t, pool, agentID, creatorID, []string{"agent:pull"})
	markRuntimePullAvailable(t, svc, token)

	type claimResult struct {
		resp *runtime.RuntimePullRunResponse
		err  error
	}
	resultC := make(chan claimResult, 1)
	go func() {
		resp, err := svc.ClaimRuntimePullRun(ctx, token, runtime.RuntimePullClaimOptions{Wait: 2 * time.Second})
		resultC <- claimResult{resp: resp, err: err}
	}()

	time.Sleep(100 * time.Millisecond)
	started, err := svc.Run(ctx, userID, makeRunReq(agentID, map[string]any{"q": "long poll"}), "")
	require.NoError(t, err)

	select {
	case result := <-resultC:
		require.NoError(t, result.err)
		require.NotNil(t, result.resp)
		assert.Equal(t, started.RunID, result.resp.RunID)
		assert.Equal(t, "long poll", result.resp.Input["q"])
	case <-time.After(3 * time.Second):
		t.Fatal("long-poll claim did not return the run before timeout")
	}
}

func TestAgentHeartbeat_MarksAgentHealthy(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	creatorID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, creatorID, "https://example.com/heartbeat", 10, "approved")
	token := insertRuntimeToken(t, pool, agentID, creatorID, []string{"agent:call"})

	_, err := pool.Exec(ctx,
		`INSERT INTO agent_availability_snapshots (
			agent_id, availability_status, last_failed_run_at, last_checked_at, consecutive_failures
		) VALUES ($1, 'degraded', NOW(), NOW(), 2)`,
		agentID,
	)
	require.NoError(t, err)

	resp, err := svc.HeartbeatAgent(ctx, token)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, agentID.String(), resp.AgentID)
	assert.Equal(t, "healthy", resp.AvailabilityStatus)
	assert.Equal(t, int32(0), resp.ConsecutiveFailures)
	require.NotNil(t, resp.LastCheckedAt)

	availability := readAgentAvailability(t, pool, agentID)
	assert.Equal(t, "healthy", availability.Status)
	assert.Equal(t, int32(0), availability.ConsecutiveFailures)
	require.NotNil(t, availability.LastCheckedAt)
}

func TestRuntimePull_ReclaimsStaleClaim(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, creatorID, "https://example.com/not-used", 10, "approved")
	setRuntimePullMode(t, pool, agentID)
	oldToken := insertRuntimeToken(t, pool, agentID, creatorID, []string{"agent:call", "agent:pull"})
	newToken := insertRuntimeToken(t, pool, agentID, creatorID, []string{"agent:call", "agent:pull"})
	markRuntimePullAvailable(t, svc, oldToken)

	started, err := svc.Run(ctx, userID, makeRunReq(agentID, map[string]any{"q": "stale"}), "")
	require.NoError(t, err)
	runID := mustParseUUID(t, started.RunID)

	claimed, err := svc.ClaimRuntimePullRun(ctx, oldToken)
	require.NoError(t, err)
	require.NotNil(t, claimed)

	_, err = pool.Exec(ctx,
		`UPDATE runs SET claimed_at=$2 WHERE id=$1`,
		runID,
		time.Now().Add(-6*time.Minute),
	)
	require.NoError(t, err)

	reclaimed, err := svc.ClaimRuntimePullRun(ctx, newToken)
	require.NoError(t, err)
	require.NotNil(t, reclaimed)
	assert.Equal(t, started.RunID, reclaimed.RunID)

	_, err = svc.CompleteRuntimePullRun(ctx, oldToken, runID, &runtime.RuntimePullResultRequest{
		Status: "success",
		Output: map[string]interface{}{"answer": "old token"},
	})
	assertHTTPStatus(t, err, 409)
}

func TestRuntimePull_TimeoutsUnclaimedRun(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, creatorID, "https://example.com/not-used", 10, "approved")
	setRuntimePullMode(t, pool, agentID)
	token := insertRuntimeToken(t, pool, agentID, creatorID, []string{"agent:pull"})
	markRuntimePullAvailable(t, svc, token)

	started, err := svc.Run(ctx, userID, makeRunReq(agentID, map[string]any{"q": "nobody claims"}), "")
	require.NoError(t, err)
	runID := mustParseUUID(t, started.RunID)
	_, err = pool.Exec(ctx,
		`UPDATE runs SET started_at=$2 WHERE id=$1`,
		runID,
		time.Now().Add(-3*time.Minute),
	)
	require.NoError(t, err)

	timedOut, err := svc.TimeoutStaleRuntimePullRuns(ctx, runtime.RuntimePullRunTimeoutConfig{
		DispatchTimeout: time.Minute,
		ResultTimeout:   10 * time.Minute,
		BatchSize:       10,
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), timedOut)

	reloaded, err := svc.GetRun(ctx, userID, runID)
	require.NoError(t, err)
	assert.Equal(t, "timeout", reloaded.Status)
	assert.Equal(t, "RUNTIME_PULL_NOT_CLAIMED", reloaded.ErrorCode)
	assert.Contains(t, reloaded.ErrorMsg, "未被 Agent runtime 领取")

	claimed, err := svc.ClaimRuntimePullRun(ctx, token)
	require.NoError(t, err)
	assert.Nil(t, claimed)

	availability := readAgentAvailability(t, pool, agentID)
	assert.Equal(t, "degraded", availability.Status)

	events := readRunEvents(t, pool, runID)
	var eventTypes []string
	for _, event := range events {
		eventTypes = append(eventTypes, event.EventType)
	}
	assert.Contains(t, eventTypes, "run.failed")
}

func TestRuntimePullWorker_TimeoutsStaleUnclaimedRun(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, creatorID, "https://example.com/not-used", 10, "approved")
	setRuntimePullMode(t, pool, agentID)
	token := insertRuntimeToken(t, pool, agentID, creatorID, []string{"agent:pull"})
	markRuntimePullAvailable(t, svc, token)

	started, err := svc.Run(ctx, userID, makeRunReq(agentID, map[string]any{"q": "worker timeout"}), "")
	require.NoError(t, err)
	runID := mustParseUUID(t, started.RunID)
	_, err = pool.Exec(ctx,
		`UPDATE runs SET started_at=$2 WHERE id=$1`,
		runID,
		time.Now().Add(-3*time.Minute),
	)
	require.NoError(t, err)

	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		runtime.StartRuntimePullRunWorker(workerCtx, svc, runtime.RuntimePullRunWorkerConfig{})
	}()

	require.Eventually(t, func() bool {
		reloaded, err := svc.GetRun(context.Background(), userID, runID)
		return err == nil && reloaded.Status == "timeout" && reloaded.ErrorCode == "RUNTIME_PULL_NOT_CLAIMED"
	}, 2*time.Second, 20*time.Millisecond)

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runtime pull worker did not stop after cancellation")
	}

	claimed, err := svc.ClaimRuntimePullRun(ctx, token)
	require.NoError(t, err)
	assert.Nil(t, claimed)

	availability := readAgentAvailability(t, pool, agentID)
	assert.Equal(t, "degraded", availability.Status)
}

func TestRuntimePull_TimeoutsClaimedRunWithoutResult(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, creatorID, "https://example.com/not-used", 10, "approved")
	setRuntimePullMode(t, pool, agentID)
	token := insertRuntimeToken(t, pool, agentID, creatorID, []string{"agent:pull"})
	markRuntimePullAvailable(t, svc, token)

	started, err := svc.Run(ctx, userID, makeRunReq(agentID, map[string]any{"q": "claimed but no result"}), "")
	require.NoError(t, err)
	runID := mustParseUUID(t, started.RunID)
	claimed, err := svc.ClaimRuntimePullRun(ctx, token)
	require.NoError(t, err)
	require.NotNil(t, claimed)

	_, err = pool.Exec(ctx,
		`UPDATE runs SET started_at=$2, claimed_at=$3 WHERE id=$1`,
		runID,
		time.Now().Add(-12*time.Minute),
		time.Now().Add(-11*time.Minute),
	)
	require.NoError(t, err)

	timedOut, err := svc.TimeoutStaleRuntimePullRuns(ctx, runtime.RuntimePullRunTimeoutConfig{
		DispatchTimeout: time.Minute,
		ResultTimeout:   10 * time.Minute,
		BatchSize:       10,
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), timedOut)

	reloaded, err := svc.GetRun(ctx, userID, runID)
	require.NoError(t, err)
	assert.Equal(t, "timeout", reloaded.Status)
	assert.Equal(t, "RUNTIME_PULL_RESULT_TIMEOUT", reloaded.ErrorCode)
	assert.Contains(t, reloaded.ErrorMsg, "未在超时时间内回传结果")

	_, err = svc.CompleteRuntimePullRun(ctx, token, runID, &runtime.RuntimePullResultRequest{
		Status: "success",
		Output: map[string]interface{}{"answer": "late"},
	})
	assertHTTPStatus(t, err, 409)
}

func TestRuntimePull_TimeoutsClaimedRunWithoutResultDespiteRepeatedClaim(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, creatorID, "https://example.com/not-used", 10, "approved")
	setRuntimePullMode(t, pool, agentID)
	token := insertRuntimeToken(t, pool, agentID, creatorID, []string{"agent:pull"})
	markRuntimePullAvailable(t, svc, token)

	started, err := svc.Run(ctx, userID, makeRunReq(agentID, map[string]any{"q": "claimed repeatedly"}), "")
	require.NoError(t, err)
	runID := mustParseUUID(t, started.RunID)
	claimed, err := svc.ClaimRuntimePullRun(ctx, token)
	require.NoError(t, err)
	require.NotNil(t, claimed)

	_, err = pool.Exec(ctx,
		`UPDATE runs SET started_at=$2, claimed_at=$3 WHERE id=$1`,
		runID,
		time.Now().Add(-12*time.Minute),
		time.Now(),
	)
	require.NoError(t, err)

	timedOut, err := svc.TimeoutStaleRuntimePullRuns(ctx, runtime.RuntimePullRunTimeoutConfig{
		DispatchTimeout: time.Minute,
		ResultTimeout:   10 * time.Minute,
		BatchSize:       10,
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), timedOut)

	reloaded, err := svc.GetRun(ctx, userID, runID)
	require.NoError(t, err)
	assert.Equal(t, "timeout", reloaded.Status)
	assert.Equal(t, "RUNTIME_PULL_RESULT_TIMEOUT", reloaded.ErrorCode)
	assert.Contains(t, reloaded.ErrorMsg, "未在超时时间内回传结果")
}
