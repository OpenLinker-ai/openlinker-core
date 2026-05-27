BEGIN;

DROP INDEX IF EXISTS idx_agent_availability_alerts_unread;
DROP INDEX IF EXISTS idx_agent_availability_alerts_creator;
DROP INDEX IF EXISTS idx_agent_availability_alerts_open;
DROP TABLE IF EXISTS agent_availability_alerts;

COMMIT;
