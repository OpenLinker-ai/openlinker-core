BEGIN;

DROP INDEX IF EXISTS idx_agent_availability_status;
DROP TABLE IF EXISTS agent_availability_snapshots;

COMMIT;
