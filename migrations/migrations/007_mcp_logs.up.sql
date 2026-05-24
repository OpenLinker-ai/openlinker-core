-- 007_mcp_logs.up.sql
--
-- 模块 C：MCP 外部入口。
--
-- 单一变更：在 runs 表新增 source 列，把"来源渠道"作为一等公民。
-- 取值 'web' / 'mcp' / 'api'。/usage 页据此显示徽章；后续也方便分桶统计。
-- 不另开 mcp_request_logs 表 —— runs + api_keys 已能完整描述一次 MCP 调用。

ALTER TABLE runs
    ADD COLUMN source TEXT NOT NULL DEFAULT 'web'
        CHECK (source IN ('web', 'mcp', 'api'));

-- 按来源 + 时间查询用（/usage 按来源筛选；后续 admin 看板按渠道统计）
CREATE INDEX idx_runs_source_time ON runs (source, started_at DESC);
