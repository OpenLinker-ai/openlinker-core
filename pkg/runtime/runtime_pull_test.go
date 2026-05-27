package runtime_test

import (
	"context"
	"crypto/rand"
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

func TestRuntimePull_ClaimAndCompleteSuccess(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, creatorID, "https://example.com/not-used", 10, "approved")
	setRuntimePullMode(t, pool, agentID)
	token := insertRuntimeToken(t, pool, agentID, creatorID, []string{"agent:call", "agent:pull"})

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
