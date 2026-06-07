package runtime_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"

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
	assert.Equal(t, 300, claimed.Metadata["claim_ttl_seconds"])
	assert.Equal(t, 0, claimed.Metadata["recommended_next_claim_after_seconds"])
	assert.Equal(t, 60, claimed.Metadata["recommended_heartbeat_after_seconds"])
	assert.Equal(t, 30, claimed.Metadata["max_long_poll_wait_seconds"])
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
