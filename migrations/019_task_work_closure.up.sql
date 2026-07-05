BEGIN;

ALTER TABLE task_queries
    ADD COLUMN claimed_agent_id UUID REFERENCES agents(id) ON DELETE SET NULL,
    ADD COLUMN claimed_by_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    ADD COLUMN claimed_at TIMESTAMPTZ,
    ADD COLUMN claim_run_id UUID REFERENCES runs(id) ON DELETE SET NULL,
    ADD COLUMN completed_at TIMESTAMPTZ,
    ADD COLUMN completion_summary TEXT,
    ADD COLUMN completion_run_id UUID REFERENCES runs(id) ON DELETE SET NULL,
    ADD CONSTRAINT task_queries_completion_summary_len CHECK (
        completion_summary IS NULL OR char_length(completion_summary) <= 2000
    ),
    ADD CONSTRAINT task_queries_claim_consistency CHECK (
        (claimed_agent_id IS NULL AND claimed_by_user_id IS NULL AND claimed_at IS NULL)
        OR
        (claimed_agent_id IS NOT NULL AND claimed_by_user_id IS NOT NULL AND claimed_at IS NOT NULL)
    ),
    ADD CONSTRAINT task_queries_completion_consistency CHECK (
        completed_at IS NULL OR claimed_agent_id IS NOT NULL
    );

CREATE INDEX idx_task_queries_claimed_by_user
    ON task_queries (claimed_by_user_id, claimed_at DESC)
    WHERE claimed_by_user_id IS NOT NULL;

CREATE INDEX idx_task_queries_claimed_agent
    ON task_queries (claimed_agent_id, claimed_at DESC)
    WHERE claimed_agent_id IS NOT NULL;

ALTER TABLE agent_runtime_tokens
    DROP CONSTRAINT IF EXISTS agent_runtime_tokens_call_scope,
    ADD CONSTRAINT agent_runtime_tokens_scopes_nonempty CHECK (cardinality(scopes) > 0),
    ADD CONSTRAINT agent_runtime_tokens_scopes_known CHECK (
        scopes <@ ARRAY['agent:call', 'agent:pull']::text[]
    );

COMMIT;
