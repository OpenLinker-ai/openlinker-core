package runtime_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

func setMCPServerMode(t *testing.T, pool *pgxpool.Pool, agentID uuid.UUID) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`UPDATE agents
		 SET connection_mode='mcp_server',
		     mcp_tool_name='demo.tool'
		 WHERE id=$1`,
		agentID,
	)
	require.NoError(t, err)
}

func TestEndpointRunTimeoutsStaleDirectAndMCPRunsOnly(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	directAgentID := insertAgent(t, pool, creatorID, "https://example.com/direct", 10, "approved")
	mcpAgentID := insertAgent(t, pool, creatorID, "https://example.com/mcp", 10, "approved")
	setMCPServerMode(t, pool, mcpAgentID)
	queuedAgentID := insertAgent(t, pool, creatorID, "https://example.com/not-used", 10, "approved")
	setRuntimePullMode(t, pool, queuedAgentID)

	directRunID := insertRunningRun(t, pool, userID, directAgentID)
	mcpRunID := insertRunningRun(t, pool, userID, mcpAgentID)
	queuedRunID := insertRunningRun(t, pool, userID, queuedAgentID)
	_, err := pool.Exec(ctx,
		`UPDATE runs SET started_at=$2 WHERE id = ANY($1::uuid[])`,
		[]uuid.UUID{directRunID, mcpRunID, queuedRunID},
		time.Now().Add(-10*time.Minute),
	)
	require.NoError(t, err)

	timedOut, err := svc.TimeoutStaleEndpointRuns(ctx, runtime.EndpointRunTimeoutConfig{
		StaleAfter: time.Minute,
		BatchSize:  10,
	})
	require.NoError(t, err)
	assert.Equal(t, int32(2), timedOut)

	directRun, err := svc.GetRun(ctx, userID, directRunID)
	require.NoError(t, err)
	assert.Equal(t, "timeout", directRun.Status)
	assert.Equal(t, "ENDPOINT_RUN_TIMEOUT", directRun.ErrorCode)
	assert.Contains(t, directRun.ErrorMsg, "endpoint")

	mcpRun, err := svc.GetRun(ctx, userID, mcpRunID)
	require.NoError(t, err)
	assert.Equal(t, "timeout", mcpRun.Status)
	assert.Equal(t, "ENDPOINT_RUN_TIMEOUT", mcpRun.ErrorCode)
	assert.Contains(t, mcpRun.ErrorMsg, "MCP")

	queuedRun, err := svc.GetRun(ctx, userID, queuedRunID)
	require.NoError(t, err)
	assert.Equal(t, "running", queuedRun.Status)

	assert.Equal(t, "degraded", readAgentAvailability(t, pool, directAgentID).Status)
	assert.Equal(t, "degraded", readAgentAvailability(t, pool, mcpAgentID).Status)

	directEvents := readRunEvents(t, pool, directRunID)
	require.NotEmpty(t, directEvents)
	assert.Equal(t, "run.failed", directEvents[len(directEvents)-1].EventType)
	assert.Equal(t, "direct_http", directEvents[len(directEvents)-1].Payload["connection_mode"])

	mcpEvents := readRunEvents(t, pool, mcpRunID)
	require.NotEmpty(t, mcpEvents)
	assert.Equal(t, "mcp_server", mcpEvents[len(mcpEvents)-1].Payload["connection_mode"])
}
