CREATE INDEX IF NOT EXISTS idx_runs_runtime_claim_stale
    ON runs (agent_id, claimed_at, started_at)
    WHERE status = 'running' AND claimed_at IS NOT NULL;
