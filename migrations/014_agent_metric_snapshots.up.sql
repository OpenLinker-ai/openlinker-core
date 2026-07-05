-- 014_agent_metric_snapshots.up.sql
-- Phase 2 缺口 2：Agent 调用指标快照（docs/29 §3.4）。
-- 由后台 worker 每 5 分钟为每个 active Agent × {24h, 7d, 30d} 写一行；
-- 创作者与外部 API 都不能直接写。

BEGIN;

CREATE TABLE agent_metric_snapshots (
    agent_id UUID NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    time_window TEXT NOT NULL,
    call_count INTEGER NOT NULL DEFAULT 0,
    success_count INTEGER NOT NULL DEFAULT 0,
    failure_count INTEGER NOT NULL DEFAULT 0,
    success_rate_bps INTEGER NOT NULL DEFAULT 0,
    median_latency_ms INTEGER,
    p95_latency_ms INTEGER,
    snapshotted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (agent_id, time_window),
    CONSTRAINT agent_metric_snapshots_time_window_valid
        CHECK (time_window IN ('24h', '7d', '30d')),
    CONSTRAINT agent_metric_snapshots_counts_nonneg
        CHECK (call_count >= 0 AND success_count >= 0 AND failure_count >= 0),
    CONSTRAINT agent_metric_snapshots_rate_range
        CHECK (success_rate_bps BETWEEN 0 AND 10000)
);

CREATE INDEX idx_agent_metric_snapshots_freshness
    ON agent_metric_snapshots (snapshotted_at);

COMMIT;
