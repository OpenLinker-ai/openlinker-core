// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/agent_metrics.sql）。

package db

import (
	"context"
	"time"

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

const refreshAgentMetricSnapshotsForWindow = `-- name: RefreshAgentMetricSnapshotsForWindow :one
WITH database_clock AS (
    SELECT clock_timestamp() AS now
), aggregated AS (
    SELECT a.id AS agent_id,
           COUNT(r.id)::int AS call_count,
           COUNT(*) FILTER (WHERE r.status = 'success')::int AS success_count,
           COUNT(*) FILTER (WHERE r.status IN ('failed', 'timeout'))::int AS failure_count,
           (percentile_cont(0.5) WITHIN GROUP (ORDER BY r.duration_ms))::int AS median_latency_ms,
           (percentile_cont(0.95) WITHIN GROUP (ORDER BY r.duration_ms))::int AS p95_latency_ms,
           database_clock.now AS snapshotted_at
    FROM agents a
    CROSS JOIN database_clock
    LEFT JOIN runs r
           ON r.agent_id = a.id
          AND r.started_at >= database_clock.now - $2::interval
    WHERE a.lifecycle_status = 'active'
    GROUP BY a.id, database_clock.now
), refreshed AS (
    INSERT INTO agent_metric_snapshots (
        agent_id, time_window, call_count, success_count, failure_count,
        success_rate_bps, median_latency_ms, p95_latency_ms, snapshotted_at
    )
    SELECT agent_id, $1, call_count, success_count, failure_count,
           CASE WHEN call_count > 0
                THEN (success_count::bigint * 10000 / call_count)::int
                ELSE 0
           END,
           median_latency_ms, p95_latency_ms, snapshotted_at
    FROM aggregated
    ON CONFLICT (agent_id, time_window) DO UPDATE
    SET call_count = EXCLUDED.call_count,
        success_count = EXCLUDED.success_count,
        failure_count = EXCLUDED.failure_count,
        success_rate_bps = EXCLUDED.success_rate_bps,
        median_latency_ms = EXCLUDED.median_latency_ms,
        p95_latency_ms = EXCLUDED.p95_latency_ms,
        snapshotted_at = EXCLUDED.snapshotted_at
    WHERE (
        agent_metric_snapshots.call_count,
        agent_metric_snapshots.success_count,
        agent_metric_snapshots.failure_count,
        agent_metric_snapshots.success_rate_bps,
        agent_metric_snapshots.median_latency_ms,
        agent_metric_snapshots.p95_latency_ms
    ) IS DISTINCT FROM (
        EXCLUDED.call_count,
        EXCLUDED.success_count,
        EXCLUDED.failure_count,
        EXCLUDED.success_rate_bps,
        EXCLUDED.median_latency_ms,
        EXCLUDED.p95_latency_ms
    )
    RETURNING 1
)
SELECT COUNT(*)::int AS refreshed_count
FROM refreshed`

type RefreshAgentMetricSnapshotsForWindowParams struct {
	TimeWindow string `db:"time_window" json:"time_window"`
	Interval   string `db:"interval" json:"interval"`
}

func (q *Queries) RefreshAgentMetricSnapshotsForWindow(
	ctx context.Context,
	arg RefreshAgentMetricSnapshotsForWindowParams,
) (int32, error) {
	var count int32
	err := q.db.QueryRow(ctx, refreshAgentMetricSnapshotsForWindow, arg.TimeWindow, arg.Interval).Scan(&count)
	return count, err
}

const refreshAgentMetricSnapshotsForAgentsAndWindow = `-- name: RefreshAgentMetricSnapshotsForAgentsAndWindow :one
WITH database_clock AS (
    SELECT clock_timestamp() AS now
), aggregated AS (
    SELECT a.id AS agent_id,
           COUNT(r.id)::int AS call_count,
           COUNT(*) FILTER (WHERE r.status = 'success')::int AS success_count,
           COUNT(*) FILTER (WHERE r.status IN ('failed', 'timeout'))::int AS failure_count,
           (percentile_cont(0.5) WITHIN GROUP (ORDER BY r.duration_ms))::int AS median_latency_ms,
           (percentile_cont(0.95) WITHIN GROUP (ORDER BY r.duration_ms))::int AS p95_latency_ms,
           database_clock.now AS snapshotted_at
    FROM agents a
    CROSS JOIN database_clock
    LEFT JOIN runs r
           ON r.agent_id = a.id
          AND r.started_at >= database_clock.now - $3::interval
    WHERE a.lifecycle_status = 'active'
      AND a.id = ANY($1::uuid[])
    GROUP BY a.id, database_clock.now
), refreshed AS (
    INSERT INTO agent_metric_snapshots (
        agent_id, time_window, call_count, success_count, failure_count,
        success_rate_bps, median_latency_ms, p95_latency_ms, snapshotted_at
    )
    SELECT agent_id, $2, call_count, success_count, failure_count,
           CASE WHEN call_count > 0
                THEN (success_count::bigint * 10000 / call_count)::int
                ELSE 0
           END,
           median_latency_ms, p95_latency_ms, snapshotted_at
    FROM aggregated
    ON CONFLICT (agent_id, time_window) DO UPDATE
    SET call_count = EXCLUDED.call_count,
        success_count = EXCLUDED.success_count,
        failure_count = EXCLUDED.failure_count,
        success_rate_bps = EXCLUDED.success_rate_bps,
        median_latency_ms = EXCLUDED.median_latency_ms,
        p95_latency_ms = EXCLUDED.p95_latency_ms,
        snapshotted_at = EXCLUDED.snapshotted_at
    WHERE (
        agent_metric_snapshots.call_count,
        agent_metric_snapshots.success_count,
        agent_metric_snapshots.failure_count,
        agent_metric_snapshots.success_rate_bps,
        agent_metric_snapshots.median_latency_ms,
        agent_metric_snapshots.p95_latency_ms
    ) IS DISTINCT FROM (
        EXCLUDED.call_count,
        EXCLUDED.success_count,
        EXCLUDED.failure_count,
        EXCLUDED.success_rate_bps,
        EXCLUDED.median_latency_ms,
        EXCLUDED.p95_latency_ms
    )
    RETURNING 1
)
SELECT COUNT(*)::int AS refreshed_count
FROM refreshed`

type RefreshAgentMetricSnapshotsForAgentsAndWindowParams struct {
	AgentIDs   []uuid.UUID `db:"agent_ids" json:"agent_ids"`
	TimeWindow string      `db:"time_window" json:"time_window"`
	Interval   string      `db:"interval" json:"interval"`
}

func (q *Queries) RefreshAgentMetricSnapshotsForAgentsAndWindow(
	ctx context.Context,
	arg RefreshAgentMetricSnapshotsForAgentsAndWindowParams,
) (int32, error) {
	var count int32
	err := q.db.QueryRow(
		ctx, refreshAgentMetricSnapshotsForAgentsAndWindow,
		arg.AgentIDs, arg.TimeWindow, arg.Interval,
	).Scan(&count)
	return count, err
}

const agentMetricDatabaseNow = `-- name: AgentMetricDatabaseNow :one
SELECT clock_timestamp() AS database_now`

func (q *Queries) AgentMetricDatabaseNow(ctx context.Context) (time.Time, error) {
	var now time.Time
	err := q.db.QueryRow(ctx, agentMetricDatabaseNow).Scan(&now)
	return now, err
}

const listAgentMetricChangesAfter = `-- name: ListAgentMetricChangesAfter :many
SELECT e.created_at AS cursor_time,
       e.id AS cursor_id,
       r.agent_id
FROM run_events e
JOIN runs r ON r.id = e.run_id
WHERE (e.created_at, e.id) > ($1::timestamptz, $2::uuid)
ORDER BY e.created_at, e.id
LIMIT $3`

type ListAgentMetricChangesAfterParams struct {
	CursorTime time.Time `db:"cursor_time" json:"cursor_time"`
	CursorID   uuid.UUID `db:"cursor_id" json:"cursor_id"`
	BatchLimit int32     `db:"batch_limit" json:"batch_limit"`
}

type ListAgentMetricChangesAfterRow struct {
	CursorTime time.Time `db:"cursor_time" json:"cursor_time"`
	CursorID   uuid.UUID `db:"cursor_id" json:"cursor_id"`
	AgentID    uuid.UUID `db:"agent_id" json:"agent_id"`
}

func (q *Queries) ListAgentMetricChangesAfter(
	ctx context.Context,
	arg ListAgentMetricChangesAfterParams,
) ([]ListAgentMetricChangesAfterRow, error) {
	rows, err := q.db.Query(ctx, listAgentMetricChangesAfter, arg.CursorTime, arg.CursorID, arg.BatchLimit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]ListAgentMetricChangesAfterRow, 0)
	for rows.Next() {
		var item ListAgentMetricChangesAfterRow
		if err := rows.Scan(&item.CursorTime, &item.CursorID, &item.AgentID); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
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
