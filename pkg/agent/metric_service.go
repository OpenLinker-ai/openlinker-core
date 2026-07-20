package agent

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/eventwake"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	coreruntime "github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

// MetricService Agent 指标快照读 + worker 共享层（docs/29 §3.4）。
type MetricService struct {
	queries  *db.Queries
	pool     *pgxpool.Pool
	observer coreruntime.WorkerObserver
}

func NewMetricService(pool *pgxpool.Pool) *MetricService {
	return &MetricService{queries: db.New(pool), pool: pool}
}

// SetWorkerObserver installs payload-free test instrumentation only.
func (s *MetricService) SetWorkerObserver(observer coreruntime.WorkerObserver) {
	if s != nil {
		s.observer = observer
	}
}

// MetricSnapshot 单个窗口的指标。
type MetricSnapshot struct {
	TimeWindow      string `json:"time_window"`
	CallCount       int32  `json:"call_count"`
	SuccessCount    int32  `json:"success_count"`
	FailureCount    int32  `json:"failure_count"`
	SuccessRateBps  int32  `json:"success_rate_bps"`
	MedianLatencyMs *int32 `json:"median_latency_ms,omitempty"`
	P95LatencyMs    *int32 `json:"p95_latency_ms,omitempty"`
	SnapshottedAt   string `json:"snapshotted_at"`
}

// MetricSnapshotsResponse 公开 GET /agents/:id/metrics 响应。
type MetricSnapshotsResponse struct {
	AgentID string           `json:"agent_id"`
	Items   []MetricSnapshot `json:"items"`
}

// GetSnapshots 返回某 Agent 全部窗口的快照（24h/7d/30d）。
func (s *MetricService) GetSnapshots(ctx context.Context, agentID uuid.UUID) (*MetricSnapshotsResponse, error) {
	rows, err := s.queries.ListAgentMetricSnapshotsByAgent(ctx, agentID)
	if err != nil {
		return nil, httpx.Internal("查询指标快照失败")
	}
	items := make([]MetricSnapshot, 0, len(rows))
	for _, r := range rows {
		items = append(items, MetricSnapshot{
			TimeWindow:      r.TimeWindow,
			CallCount:       r.CallCount,
			SuccessCount:    r.SuccessCount,
			FailureCount:    r.FailureCount,
			SuccessRateBps:  r.SuccessRateBps,
			MedianLatencyMs: r.MedianLatencyMs,
			P95LatencyMs:    r.P95LatencyMs,
			SnapshottedAt:   r.SnapshottedAt.UTC().Format(time.RFC3339),
		})
	}
	return &MetricSnapshotsResponse{AgentID: agentID.String(), Items: items}, nil
}

// metricWindow 与 schema CHECK 同步。
type metricWindow struct {
	label    string
	interval string
}

var metricWindows = []metricWindow{
	{"24h", "24 hours"},
	{"7d", "7 days"},
	{"30d", "30 days"},
}

const (
	metricChangeScanBatch      = int32(512)
	metricChangeScanMaxBatches = 32
	metricDirtyClaimBatch      = 128
	metricDirtyMaxClaimBatches = 4
	metricDirtyClaimLease      = 30 * time.Second
	metricDirtyEventCoalesce   = time.Second
	metricDirtyReconcile       = time.Minute
	metricDirtyErrorRetry      = 5 * time.Second
	metricRunChangedWakeTopic  = "run.changed"
)

// AggregateOnce 跑一次完整聚合（worker 与测试都用这个入口）。
func (s *MetricService) AggregateOnce(ctx context.Context) error {
	for _, w := range metricWindows {
		if s.observer != nil {
			s.observer.ObserveWorker(coreruntime.WorkerObservation{
				Category: "agent.metric.aggregate_query", Reason: w.label,
			})
		}
		refreshed, err := s.queries.RefreshAgentMetricSnapshotsForWindow(
			ctx,
			db.RefreshAgentMetricSnapshotsForWindowParams{
				TimeWindow: w.label,
				Interval:   w.interval,
			},
		)
		if err != nil {
			return err
		}
		if s.observer != nil {
			s.observer.ObserveWorker(coreruntime.WorkerObservation{
				Category: "agent.metric.upsert_rows", Reason: w.label, BatchSize: int(refreshed),
			})
		}
	}
	return nil
}

func (s *MetricService) refreshAgents(ctx context.Context, agentIDs []uuid.UUID) error {
	if s == nil || s.queries == nil || len(agentIDs) == 0 {
		return nil
	}
	for _, window := range metricWindows {
		refreshed, err := s.queries.RefreshAgentMetricSnapshotsForAgentsAndWindow(
			ctx,
			db.RefreshAgentMetricSnapshotsForAgentsAndWindowParams{
				AgentIDs: agentIDs, TimeWindow: window.label, Interval: window.interval,
			},
		)
		if err != nil {
			return err
		}
		if s.observer != nil {
			s.observer.ObserveWorker(coreruntime.WorkerObservation{
				Category: "agent.metric.dirty_refresh_rows",
				Reason:   window.label, BatchSize: int(refreshed),
			})
		}
	}
	return nil
}

func (s *MetricService) scanMetricChanges(
	ctx context.Context,
	dirty AgentMetricDirtyStore,
) (bool, error) {
	if s == nil || s.queries == nil || dirty == nil {
		return false, errors.New("agent metric change scanner is not configured")
	}
	cursor, ok, err := dirty.Cursor(ctx)
	if err != nil {
		return false, err
	}
	if !ok {
		now, clockErr := s.queries.AgentMetricDatabaseNow(ctx)
		if clockErr != nil {
			return false, clockErr
		}
		cursor = AgentMetricCursor{Time: now.Add(-30 * 24 * time.Hour), ID: uuid.Nil}
		if _, err = dirty.AdvanceCursor(ctx, cursor); err != nil {
			return false, err
		}
	}
	for batch := 0; batch < metricChangeScanMaxBatches; batch++ {
		rows, queryErr := s.queries.ListAgentMetricChangesAfter(ctx, db.ListAgentMetricChangesAfterParams{
			CursorTime: cursor.Time, CursorID: cursor.ID, BatchLimit: metricChangeScanBatch,
		})
		if queryErr != nil {
			return false, queryErr
		}
		if len(rows) == 0 {
			return false, nil
		}
		agentIDs := make([]uuid.UUID, 0, len(rows))
		seen := make(map[uuid.UUID]struct{}, len(rows))
		for _, row := range rows {
			if row.AgentID == uuid.Nil {
				return false, errors.New("agent metric change row has no Agent ID")
			}
			if _, duplicate := seen[row.AgentID]; !duplicate {
				seen[row.AgentID] = struct{}{}
				agentIDs = append(agentIDs, row.AgentID)
			}
		}
		if err = dirty.Mark(ctx, agentIDs); err != nil {
			return false, err
		}
		last := rows[len(rows)-1]
		cursor = AgentMetricCursor{Time: last.CursorTime, ID: last.CursorID}
		if _, err = dirty.AdvanceCursor(ctx, cursor); err != nil {
			return false, err
		}
		if len(rows) < int(metricChangeScanBatch) {
			return false, nil
		}
	}
	return true, nil
}

func (s *MetricService) processMetricDirty(
	ctx context.Context,
	dirty AgentMetricDirtyStore,
) (bool, error) {
	for batch := 0; batch < metricDirtyMaxClaimBatches; batch++ {
		owner := uuid.New()
		claims, err := dirty.Claim(ctx, owner, metricDirtyClaimLease, metricDirtyClaimBatch)
		if err != nil {
			return false, err
		}
		if len(claims) == 0 {
			return false, nil
		}
		agentIDs := make([]uuid.UUID, 0, len(claims))
		for _, claim := range claims {
			agentIDs = append(agentIDs, claim.AgentID)
		}
		if err = s.refreshAgents(ctx, agentIDs); err != nil {
			for _, claim := range claims {
				_, _ = dirty.Nack(ctx, claim)
			}
			return false, err
		}
		for _, claim := range claims {
			if _, err = dirty.Ack(ctx, claim); err != nil {
				return false, err
			}
		}
		if len(claims) < metricDirtyClaimBatch {
			return false, nil
		}
	}
	return true, nil
}

func (s *MetricService) runMetricDirtyPass(
	ctx context.Context,
	dirty AgentMetricDirtyStore,
) (bool, error) {
	scanMore, err := s.scanMetricChanges(ctx, dirty)
	if err != nil {
		return false, err
	}
	dirtyMore, err := s.processMetricDirty(ctx, dirty)
	return scanMore || dirtyMore, err
}

// StartMetricWorker 启动 5 分钟 tick 的后台聚合 + approval 过期清扫。
// 关闭 ctx 即结束 goroutine。
func StartMetricWorker(ctx context.Context, metric *MetricService, approvals *ApprovalService) {
	startMetricWorker(ctx, metric, approvals, nil)
}

func startMetricWorker(
	ctx context.Context,
	metric *MetricService,
	approvals *ApprovalService,
	initialRefreshDone chan<- struct{},
) {
	if ctx.Err() != nil {
		return
	}
	go func() {
		// 启动立即跑一次，避免冷启动后第一次要等 5 分钟。
		runMetricTick(ctx, metric, approvals)
		if initialRefreshDone != nil {
			close(initialRefreshDone)
		}
		if ctx.Err() != nil {
			return
		}
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				runMetricTick(ctx, metric, approvals)
			}
		}
	}()
}

// StartMetricWorkerWithDirty adds a Redis-backed, event-triggered incremental
// refresh path while retaining StartMetricWorker's five-minute set-based
// refresh and approval sweep as the compatibility/recovery boundary.
func StartMetricWorkerWithDirty(
	ctx context.Context,
	metric *MetricService,
	approvals *ApprovalService,
	dirty AgentMetricDirtyStore,
	wake eventwake.TopicSource,
) {
	if ctx.Err() != nil {
		return
	}
	if metric == nil || dirty == nil {
		StartMetricWorker(ctx, metric, approvals)
		return
	}
	initialRefreshDone := make(chan struct{})
	startMetricWorker(ctx, metric, approvals, initialRefreshDone)
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-initialRefreshDone:
			startMetricDirtyWorker(ctx, metric, dirty, wake)
		}
	}()
}

func startMetricDirtyWorker(
	ctx context.Context,
	metric *MetricService,
	dirty AgentMetricDirtyStore,
	wake eventwake.TopicSource,
) {
	run := func(reason string) (eventwake.SchedulerResult, error) {
		if metric.observer != nil {
			metric.observer.ObserveWorker(coreruntime.WorkerObservation{
				Category: "agent.metric.dirty_pass", Reason: reason,
			})
		}
		hasMore, err := metric.runMetricDirtyPass(ctx, dirty)
		if hasMore {
			return eventwake.SchedulerResult{HasNext: true}, err
		}
		return eventwake.SchedulerResult{}, err
	}
	for ctx.Err() == nil {
		if wake == nil || !wake.Health().Connected {
			if _, err := run("degraded_reconcile"); err != nil && ctx.Err() == nil {
				log.Warn().Err(err).Msg("agent.metric_worker: dirty reconciliation")
			}
			timer := time.NewTimer(metricDirtyReconcile)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return
			case <-timer.C:
			}
			continue
		}
		subscription, err := wake.SubscribeTopic(metricRunChangedWakeTopic)
		if err != nil {
			timer := time.NewTimer(time.Second)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return
			case <-timer.C:
			}
			continue
		}
		err = eventwake.RunScheduler(ctx, subscription, eventwake.SchedulerConfig{
			ReconcileInterval: metricDirtyReconcile,
			ErrorRetry:        metricDirtyErrorRetry,
			HealthCheck:       time.Second,
			Healthy:           func() bool { return wake.Health().Connected },
		}, func(_ context.Context, reason string) (eventwake.SchedulerResult, error) {
			// A Run can emit several transactional notifications and a busy Core
			// can receive hundreds per second. Keep notifications as wake hints,
			// but coalesce them before reading the durable cursor so event volume
			// cannot become an equivalent PostgreSQL query rate.
			if reason == "event" {
				timer := time.NewTimer(metricDirtyEventCoalesce)
				select {
				case <-ctx.Done():
					if !timer.Stop() {
						<-timer.C
					}
					return eventwake.SchedulerResult{}, nil
				case <-timer.C:
				}
			}
			return run(reason)
		})
		subscription.Close()
		if ctx.Err() != nil {
			return
		}
		if err != nil && !errors.Is(err, eventwake.ErrWakeSourceDegraded) {
			log.Warn().Err(err).Msg("agent.metric_worker: event scheduler")
		}
	}
}

func runMetricTick(ctx context.Context, metric *MetricService, approvals *ApprovalService) {
	if metric != nil {
		if err := metric.AggregateOnce(ctx); err != nil {
			log.Warn().Err(err).Msg("agent.metric_worker: AggregateOnce")
		}
	}
	if approvals != nil {
		if _, err := approvals.SweepExpiredApprovals(ctx); err != nil {
			log.Warn().Err(err).Msg("agent.metric_worker: SweepExpiredApprovals")
		}
	}
}
