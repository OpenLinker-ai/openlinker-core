BEGIN;

ALTER TABLE agent_runtime_tokens
    DROP CONSTRAINT IF EXISTS agent_runtime_tokens_scopes_known,
    DROP CONSTRAINT IF EXISTS agent_runtime_tokens_scopes_nonempty,
    ADD CONSTRAINT agent_runtime_tokens_call_scope CHECK ('agent:call' = ANY(scopes));

DROP INDEX IF EXISTS idx_task_queries_claimed_agent;
DROP INDEX IF EXISTS idx_task_queries_claimed_by_user;

ALTER TABLE task_queries
    DROP CONSTRAINT IF EXISTS task_queries_completion_consistency,
    DROP CONSTRAINT IF EXISTS task_queries_claim_consistency,
    DROP CONSTRAINT IF EXISTS task_queries_completion_summary_len,
    DROP COLUMN IF EXISTS completion_run_id,
    DROP COLUMN IF EXISTS completion_summary,
    DROP COLUMN IF EXISTS completed_at,
    DROP COLUMN IF EXISTS claim_run_id,
    DROP COLUMN IF EXISTS claimed_at,
    DROP COLUMN IF EXISTS claimed_by_user_id,
    DROP COLUMN IF EXISTS claimed_agent_id;

COMMIT;
