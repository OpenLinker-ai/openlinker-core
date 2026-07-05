-- 007_mcp_logs.down.sql
DROP INDEX IF EXISTS idx_runs_source_time;
ALTER TABLE runs DROP COLUMN IF EXISTS source;
