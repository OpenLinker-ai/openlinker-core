// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/agent_metrics.sql）。

package db

import (
	"context"

	"github.com/google/uuid"
)

const upsertAgentMetricSnapshot = `-- name: UpsertAgentMetricSnapshot :exec
INSERT INTO agent_metric_snapshots (
    agent_id, time_window, call_count, success_count, failure_count,
    success_rate_bps, median_latency_ms, p95_latency_ms, snapshotted_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, NOW()
)
ON CONFLICT (agent_id, time_window) DO UPDATE
SET call_count = EXCLUDED.call_count,
    success_count = EXCLUDED.success_count,
    failure_count = EXCLUDED.failure_count,
    success_rate_bps = EXCLUDED.success_rate_bps,
    median_latency_ms = EXCLUDED.median_latency_ms,
    p95_latency_ms = EXCLUDED.p95_latency_ms,
    snapshotted_at = NOW()`

type UpsertAgentMetricSnapshotParams struct {
	AgentID         uuid.UUID `db:"agent_id" json:"agent_id"`
	TimeWindow      string    `db:"time_window" json:"time_window"`
	CallCount       int32     `db:"call_count" json:"call_count"`
	SuccessCount    int32     `db:"success_count" json:"success_count"`
	FailureCount    int32     `db:"failure_count" json:"failure_count"`
	SuccessRateBps  int32     `db:"success_rate_bps" json:"success_rate_bps"`
	MedianLatencyMs *int32    `db:"median_latency_ms" json:"median_latency_ms"`
	P95LatencyMs    *int32    `db:"p95_latency_ms" json:"p95_latency_ms"`
}

func (q *Queries) UpsertAgentMetricSnapshot(ctx context.Context, arg UpsertAgentMetricSnapshotParams) error {
	_, err := q.db.Exec(ctx, upsertAgentMetricSnapshot,
		arg.AgentID, arg.TimeWindow, arg.CallCount, arg.SuccessCount, arg.FailureCount,
		arg.SuccessRateBps, arg.MedianLatencyMs, arg.P95LatencyMs)
	return err
}

const listAgentMetricSnapshotsByAgent = `-- name: ListAgentMetricSnapshotsByAgent :many
SELECT agent_id, time_window, call_count, success_count, failure_count,
       success_rate_bps, median_latency_ms, p95_latency_ms, snapshotted_at
FROM agent_metric_snapshots
WHERE agent_id = $1
ORDER BY time_window`

func (q *Queries) ListAgentMetricSnapshotsByAgent(ctx context.Context, agentID uuid.UUID) ([]AgentMetricSnapshot, error) {
	rows, err := q.db.Query(ctx, listAgentMetricSnapshotsByAgent, agentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []AgentMetricSnapshot
	for rows.Next() {
		var s AgentMetricSnapshot
		if err := rows.Scan(
			&s.AgentID, &s.TimeWindow, &s.CallCount, &s.SuccessCount, &s.FailureCount,
			&s.SuccessRateBps, &s.MedianLatencyMs, &s.P95LatencyMs, &s.SnapshottedAt,
		); err != nil {
			return nil, err
		}
		items = append(items, s)
	}
	return items, rows.Err()
}

const aggregateAgentRunsForWindow = `-- name: AggregateAgentRunsForWindow :many
SELECT a.id AS agent_id,
       COUNT(r.id)::int AS call_count,
       COUNT(*) FILTER (WHERE r.status = 'success')::int AS success_count,
       COUNT(*) FILTER (WHERE r.status IN ('failed', 'timeout'))::int AS failure_count,
       (percentile_cont(0.5) WITHIN GROUP (ORDER BY r.duration_ms))::int AS median_latency_ms,
       (percentile_cont(0.95) WITHIN GROUP (ORDER BY r.duration_ms))::int AS p95_latency_ms
FROM agents a
LEFT JOIN runs r
       ON r.agent_id = a.id
      AND r.started_at >= NOW() - $1::interval
WHERE a.lifecycle_status = 'active'
GROUP BY a.id`

// AggregateAgentRunsForWindowRow worker 聚合返回行。
type AggregateAgentRunsForWindowRow struct {
	AgentID         uuid.UUID `db:"agent_id" json:"agent_id"`
	CallCount       int32     `db:"call_count" json:"call_count"`
	SuccessCount    int32     `db:"success_count" json:"success_count"`
	FailureCount    int32     `db:"failure_count" json:"failure_count"`
	MedianLatencyMs *int32    `db:"median_latency_ms" json:"median_latency_ms"`
	P95LatencyMs    *int32    `db:"p95_latency_ms" json:"p95_latency_ms"`
}

func (q *Queries) AggregateAgentRunsForWindow(ctx context.Context, interval string) ([]AggregateAgentRunsForWindowRow, error) {
	rows, err := q.db.Query(ctx, aggregateAgentRunsForWindow, interval)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []AggregateAgentRunsForWindowRow
	for rows.Next() {
		var r AggregateAgentRunsForWindowRow
		if err := rows.Scan(
			&r.AgentID, &r.CallCount, &r.SuccessCount, &r.FailureCount,
			&r.MedianLatencyMs, &r.P95LatencyMs,
		); err != nil {
			return nil, err
		}
		items = append(items, r)
	}
	return items, rows.Err()
}
