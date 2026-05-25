-- agent_metrics.sql
-- Phase 2 缺口 2：Agent 指标快照（docs/29 §3.4）。
-- 后台 worker 每 5 分钟 upsert；前端按 agent_id 读。

-- name: UpsertAgentMetricSnapshot :exec
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
    snapshotted_at = NOW();

-- name: ListAgentMetricSnapshotsByAgent :many
SELECT agent_id, time_window, call_count, success_count, failure_count,
       success_rate_bps, median_latency_ms, p95_latency_ms, snapshotted_at
FROM agent_metric_snapshots
WHERE agent_id = $1
ORDER BY time_window;

-- name: AggregateAgentRunsForWindow :many
-- 按 lifecycle_status=active 的 agents 聚合最近 :interval 时间窗的 runs。
-- 返回 (agent_id, total, succeeded, failed, median, p95)。
-- $1 = INTERVAL 字符串（'24 hours' / '7 days' / '30 days'）。
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
GROUP BY a.id;
