-- 005_agent_capabilities.down.sql

BEGIN;

DROP TABLE IF EXISTS agent_onboarding_status;
DROP TABLE IF EXISTS agent_examples;
DROP TABLE IF EXISTS agent_capabilities;

COMMIT;
