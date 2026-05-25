package agent

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	db "github.com/kinzhi/openlinker-core/pkg/db/generated"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

// MetricService Agent 指标快照读 + worker 共享层（docs/29 §3.4）。
type MetricService struct {
	queries *db.Queries
	pool    *pgxpool.Pool
}

func NewMetricService(pool *pgxpool.Pool) *MetricService {
	return &MetricService{queries: db.New(pool), pool: pool}
}

// MetricSnapshot 单个窗口的指标。
type MetricSnapshot struct {
	TimeWindow      string  `json:"time_window"`
	CallCount       int32   `json:"call_count"`
	SuccessCount    int32   `json:"success_count"`
	FailureCount    int32   `json:"failure_count"`
	SuccessRateBps  int32   `json:"success_rate_bps"`
	MedianLatencyMs *int32  `json:"median_latency_ms,omitempty"`
	P95LatencyMs    *int32  `json:"p95_latency_ms,omitempty"`
	SnapshottedAt   string  `json:"snapshotted_at"`
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

// AggregateOnce 跑一次完整聚合（worker 与测试都用这个入口）。
func (s *MetricService) AggregateOnce(ctx context.Context) error {
	for _, w := range metricWindows {
		rows, err := s.queries.AggregateAgentRunsForWindow(ctx, w.interval)
		if err != nil {
			return err
		}
		for _, r := range rows {
			rate := int32(0)
			if r.CallCount > 0 {
				rate = int32(float64(r.SuccessCount) / float64(r.CallCount) * 10000)
			}
			if err := s.queries.UpsertAgentMetricSnapshot(ctx, db.UpsertAgentMetricSnapshotParams{
				AgentID:         r.AgentID,
				TimeWindow:      w.label,
				CallCount:       r.CallCount,
				SuccessCount:    r.SuccessCount,
				FailureCount:    r.FailureCount,
				SuccessRateBps:  rate,
				MedianLatencyMs: r.MedianLatencyMs,
				P95LatencyMs:    r.P95LatencyMs,
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

// StartMetricWorker 启动 5 分钟 tick 的后台聚合 + approval 过期清扫。
// 关闭 ctx 即结束 goroutine。
func StartMetricWorker(ctx context.Context, metric *MetricService, approvals *ApprovalService) {
	go func() {
		// 启动立即跑一次，避免冷启动后第一次要等 5 分钟。
		runMetricTick(ctx, metric, approvals)
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
