CREATE INDEX IF NOT EXISTS idx_runs_runtime_claimed_token
    ON runs (agent_id, claimed_by_runtime_token_id, started_at)
    WHERE status = 'running' AND claimed_by_runtime_token_id IS NOT NULL;
