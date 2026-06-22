package agent_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/kinzhi/openlinker-core/pkg/agent"
)

// 覆盖 Phase 2 缺口 2：metric snapshots（docs/29 §3.4）。

func TestMetricService_AggregateOnce_NoRuns(t *testing.T) {
	pool := setupTestDB(t)
	creator := insertCreatorUser(t, pool, "Metric Creator")
	agentID := createApprovedAgent(t, pool, creator, "metric-empty")
	ctx := context.Background()

	svc := agent.NewMetricService(pool)
	require.NoError(t, svc.AggregateOnce(ctx))

	resp, err := svc.GetSnapshots(ctx, agentID)
	require.NoError(t, err)
	require.Equal(t, agentID.String(), resp.AgentID)
	require.Len(t, resp.Items, 3, "应当为 24h/7d/30d 各写 1 行")
	for _, it := range resp.Items {
		require.Equal(t, int32(0), it.CallCount)
		require.Equal(t, int32(0), it.SuccessRateBps)
	}
}

func TestMetricService_AggregateOnce_WithRuns(t *testing.T) {
	pool := setupTestDB(t)
	creator := insertCreatorUser(t, pool, "Metric Creator")
	agentID := createApprovedAgent(t, pool, creator, "metric-with-runs")
	ctx := context.Background()

	// 插入 4 个 runs：3 success / 1 failed，全在 24h 内。
	insertRun(t, pool, creator, agentID, "success", 1500, time.Now().Add(-1*time.Hour))
	insertRun(t, pool, creator, agentID, "success", 2500, time.Now().Add(-2*time.Hour))
	insertRun(t, pool, creator, agentID, "success", 3500, time.Now().Add(-3*time.Hour))
	insertRun(t, pool, creator, agentID, "failed", 500, time.Now().Add(-4*time.Hour))

	svc := agent.NewMetricService(pool)
	require.NoError(t, svc.AggregateOnce(ctx))

	resp, err := svc.GetSnapshots(ctx, agentID)
	require.NoError(t, err)
	require.Len(t, resp.Items, 3)

	for _, it := range resp.Items {
		require.Equal(t, int32(4), it.CallCount, "window=%s", it.TimeWindow)
		require.Equal(t, int32(3), it.SuccessCount)
		require.Equal(t, int32(1), it.FailureCount)
		require.Equal(t, int32(7500), it.SuccessRateBps, "75% = 7500 bps")
		require.NotNil(t, it.MedianLatencyMs)
		require.NotNil(t, it.P95LatencyMs)
	}
}

func TestMetricService_AggregateOnce_SkipsDisabled(t *testing.T) {
	pool := setupTestDB(t)
	creator := insertCreatorUser(t, pool, "Metric Creator")
	active := createApprovedAgent(t, pool, creator, "metric-active")
	disabled := createApprovedAgent(t, pool, creator, "metric-disabled", WithStatus("disabled"))
	ctx := context.Background()

	svc := agent.NewMetricService(pool)
	require.NoError(t, svc.AggregateOnce(ctx))

	activeResp, err := svc.GetSnapshots(ctx, active)
	require.NoError(t, err)
	require.Len(t, activeResp.Items, 3)

	disabledResp, err := svc.GetSnapshots(ctx, disabled)
	require.NoError(t, err)
	require.Empty(t, disabledResp.Items, "disabled Agent 不应被聚合")
}

func TestMetricService_AggregateOnce_Idempotent(t *testing.T) {
	pool := setupTestDB(t)
	creator := insertCreatorUser(t, pool, "Metric Creator")
	agentID := createApprovedAgent(t, pool, creator, "metric-idem")
	ctx := context.Background()

	insertRun(t, pool, creator, agentID, "success", 1000, time.Now().Add(-30*time.Minute))

	svc := agent.NewMetricService(pool)
	require.NoError(t, svc.AggregateOnce(ctx))
	require.NoError(t, svc.AggregateOnce(ctx))

	resp, err := svc.GetSnapshots(ctx, agentID)
	require.NoError(t, err)
	require.Len(t, resp.Items, 3, "二次聚合也只会 upsert，不会复制行")
}

func TestStartMetricWorkerAggregatesAndSweepsExpiredApprovals(t *testing.T) {
	pool := setupTestDB(t)
	creator := insertCreatorUser(t, pool, "Metric Worker Creator")
	agentID := createApprovedAgent(t, pool, creator, "metric-worker")
	ctx := context.Background()

	insertRun(t, pool, creator, agentID, "success", 1000, time.Now().Add(-15*time.Minute))

	approvals := agent.NewApprovalService(pool, nil)
	approval, err := approvals.CreateApproval(ctx, creator, &agent.CreateApprovalRequest{
		AgentID:          agentID.String(),
		Action:           "x",
		ExpiresInMinutes: 5,
	})
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`UPDATE agent_action_approval_requests SET expires_at = NOW() - INTERVAL '1 minute' WHERE id = $1`,
		uuid.MustParse(approval.ID))
	require.NoError(t, err)

	metrics := agent.NewMetricService(pool)
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	agent.StartMetricWorker(workerCtx, metrics, approvals)

	require.Eventually(t, func() bool {
		snapshots, err := metrics.GetSnapshots(ctx, agentID)
		if err != nil || len(snapshots.Items) != 3 {
			return false
		}
		for _, item := range snapshots.Items {
			if item.CallCount != 1 || item.SuccessCount != 1 {
				return false
			}
		}
		got, err := approvals.GetApproval(ctx, creator, uuid.MustParse(approval.ID))
		return err == nil && got.Status == "expired"
	}, 2*time.Second, 20*time.Millisecond)

	cancel()
	time.Sleep(20 * time.Millisecond)
}

// insertRun 直接 SQL 插一条 run，给 metric worker 测试用。
func insertRun(t *testing.T, pool *pgxpool.Pool, userID, agentID uuid.UUID, status string, durationMs int32, startedAt time.Time) {
	t.Helper()
	finishedAt := startedAt.Add(time.Duration(durationMs) * time.Millisecond)
	_, err := pool.Exec(context.Background(),
		`INSERT INTO runs (id, user_id, agent_id, input, output, status,
		                   cost_cents, platform_fee_cents, creator_revenue_cents,
		                   duration_ms, started_at, finished_at)
		 VALUES ($1, $2, $3, '{}'::jsonb, '{}'::jsonb, $4, 0, 0, 0, $5, $6, $7)`,
		uuid.New(), userID, agentID, status, durationMs, startedAt, finishedAt)
	require.NoError(t, err, "insert run")
}
