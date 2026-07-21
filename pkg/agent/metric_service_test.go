package agent_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	coreruntime "github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

// 覆盖 Phase 2 缺口 2：metric snapshots（docs/29 §3.4）。

func TestMetricService_AggregateOnce_NoRuns(t *testing.T) {
	pool := setupTestDB(t)
	creator := insertCreatorUser(t, pool, "Metric Creator")
	agentID := createApprovedAgent(t, pool, creator, "metric-empty")
	ctx := context.Background()

	svc := agent.NewMetricService(pool)
	var observations []coreruntime.WorkerObservation
	svc.SetWorkerObserver(coreruntime.WorkerObserverFunc(func(observation coreruntime.WorkerObservation) {
		observations = append(observations, observation)
	}))
	require.NoError(t, svc.AggregateOnce(ctx))
	require.Equal(t, []coreruntime.WorkerObservation{
		{Category: "agent.metric.aggregate_query", Reason: "24h"},
		{Category: "agent.metric.upsert_rows", Reason: "24h", BatchSize: 1},
		{Category: "agent.metric.aggregate_query", Reason: "7d"},
		{Category: "agent.metric.upsert_rows", Reason: "7d", BatchSize: 1},
		{Category: "agent.metric.aggregate_query", Reason: "30d"},
		{Category: "agent.metric.upsert_rows", Reason: "30d", BatchSize: 1},
	}, observations)

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
	first, err := svc.GetSnapshots(ctx, agentID)
	require.NoError(t, err)
	var secondObservations []coreruntime.WorkerObservation
	svc.SetWorkerObserver(coreruntime.WorkerObserverFunc(func(observation coreruntime.WorkerObservation) {
		secondObservations = append(secondObservations, observation)
	}))
	require.NoError(t, svc.AggregateOnce(ctx))

	resp, err := svc.GetSnapshots(ctx, agentID)
	require.NoError(t, err)
	require.Len(t, resp.Items, 3, "二次聚合也只会 upsert，不会复制行")
	require.Equal(t, first.Items, resp.Items, "数值未变化时不应只为刷新时间戳重写快照")
	require.Equal(t, []coreruntime.WorkerObservation{
		{Category: "agent.metric.aggregate_query", Reason: "24h"},
		{Category: "agent.metric.upsert_rows", Reason: "24h", BatchSize: 0},
		{Category: "agent.metric.aggregate_query", Reason: "7d"},
		{Category: "agent.metric.upsert_rows", Reason: "7d", BatchSize: 0},
		{Category: "agent.metric.aggregate_query", Reason: "30d"},
		{Category: "agent.metric.upsert_rows", Reason: "30d", BatchSize: 0},
	}, secondObservations)
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

func TestStartMetricWorkerWithDirtyRefreshesClaimAndKeepsLegacyResult(t *testing.T) {
	pool := setupTestDB(t)
	creator := insertCreatorUser(t, pool, "Metric Dirty Creator")
	agentID := createApprovedAgent(t, pool, creator, "metric-dirty-worker")
	insertRun(t, pool, creator, agentID, "success", 900, time.Now().Add(-time.Minute))

	claim := agent.AgentMetricDirtyClaim{AgentID: agentID, Version: 1, Owner: uuid.New()}
	var initialRefreshDone atomic.Bool
	dirty := &metricDirtyStoreFake{
		claim: claim, acked: make(chan struct{}), initialRefreshDone: initialRefreshDone.Load,
	}
	metrics := agent.NewMetricService(pool)
	dirtyRefreshObserved := make(chan struct{}, 1)
	metrics.SetWorkerObserver(coreruntime.WorkerObserverFunc(func(observation coreruntime.WorkerObservation) {
		if observation.Category == "agent.metric.upsert_rows" && observation.Reason == "30d" {
			initialRefreshDone.Store(true)
		}
		if observation.Category == "agent.metric.dirty_refresh_rows" {
			select {
			case dirtyRefreshObserved <- struct{}{}:
			default:
			}
		}
	}))
	workerCtx, cancel := context.WithCancel(context.Background())
	agent.StartMetricWorkerWithDirty(workerCtx, metrics, nil, dirty, nil)
	defer cancel()

	select {
	case <-dirty.acked:
	case <-time.After(2 * time.Second):
		t.Fatal("Redis dirty metric claim was not acknowledged")
	}
	require.False(t, dirty.wasClaimedBeforeInitialRefresh(), "dirty refresh must not overlap startup aggregation")
	select {
	case <-dirtyRefreshObserved:
	case <-time.After(time.Second):
		t.Fatal("dirty Agent batch did not execute the selected refresh query")
	}
	snapshots, err := metrics.GetSnapshots(context.Background(), agentID)
	require.NoError(t, err)
	require.Len(t, snapshots.Items, 3)
	for _, snapshot := range snapshots.Items {
		require.Equal(t, int32(1), snapshot.CallCount)
		require.Equal(t, int32(1), snapshot.SuccessCount)
	}
}

type metricDirtyStoreFake struct {
	mu      sync.Mutex
	claim   agent.AgentMetricDirtyClaim
	claimed bool
	acked   chan struct{}
	ackOnce sync.Once

	initialRefreshDone func() bool
	claimedBeforeReady bool
}

func (f *metricDirtyStoreFake) Mark(context.Context, []uuid.UUID) error { return nil }

func (f *metricDirtyStoreFake) Claim(
	context.Context, uuid.UUID, time.Duration, int,
) ([]agent.AgentMetricDirtyClaim, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.initialRefreshDone != nil && !f.initialRefreshDone() {
		f.claimedBeforeReady = true
	}
	if f.claimed {
		return nil, nil
	}
	f.claimed = true
	return []agent.AgentMetricDirtyClaim{f.claim}, nil
}

func (f *metricDirtyStoreFake) wasClaimedBeforeInitialRefresh() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.claimedBeforeReady
}

func (f *metricDirtyStoreFake) Ack(
	_ context.Context, claim agent.AgentMetricDirtyClaim,
) (bool, error) {
	if claim != f.claim {
		return false, nil
	}
	f.ackOnce.Do(func() { close(f.acked) })
	return true, nil
}

func (f *metricDirtyStoreFake) Nack(context.Context, agent.AgentMetricDirtyClaim) (bool, error) {
	return true, nil
}

func (f *metricDirtyStoreFake) Cursor(context.Context) (agent.AgentMetricCursor, bool, error) {
	return agent.AgentMetricCursor{Time: time.Now().Add(time.Hour), ID: uuid.Nil}, true, nil
}

func (f *metricDirtyStoreFake) AdvanceCursor(context.Context, agent.AgentMetricCursor) (bool, error) {
	return true, nil
}

// insertRun 直接 SQL 插一条 run，给 metric worker 测试用。
func insertRun(t *testing.T, pool *pgxpool.Pool, userID, agentID uuid.UUID, status string, durationMs int32, startedAt time.Time) {
	t.Helper()
	insertLegacyTerminalRun(t, pool, userID, agentID, status, durationMs, 0, 0, 0, startedAt)
}
