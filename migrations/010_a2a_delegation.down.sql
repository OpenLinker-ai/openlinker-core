-- 010_a2a_delegation.down.sql

BEGIN;

DROP TABLE IF EXISTS run_delegations;
DROP TRIGGER IF EXISTS agent_call_policies_set_updated_at ON agent_call_policies;
DROP TABLE IF EXISTS agent_call_policies;
DROP TABLE IF EXISTS agent_runtime_tokens;

COMMIT;
