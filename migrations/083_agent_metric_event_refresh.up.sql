-- 083_agent_metric_event_refresh.up.sql
-- Supports a durable RunEvent cursor used only to rebuild Redis metric dirty
-- hints. The existing five-minute set-based refresh remains authoritative.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_run_events_metric_cursor
    ON run_events (created_at, id)
    INCLUDE (run_id);
