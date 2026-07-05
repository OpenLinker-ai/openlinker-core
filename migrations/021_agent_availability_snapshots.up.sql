BEGIN;

CREATE TABLE agent_availability_snapshots (
    agent_id UUID PRIMARY KEY REFERENCES agents(id) ON DELETE CASCADE,
    availability_status TEXT NOT NULL DEFAULT 'unknown',
    last_successful_run_at TIMESTAMPTZ,
    last_failed_run_at TIMESTAMPTZ,
    last_checked_at TIMESTAMPTZ,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT agent_availability_status_valid
        CHECK (availability_status IN ('unknown', 'healthy', 'degraded', 'unreachable')),
    CONSTRAINT agent_availability_failures_nonneg
        CHECK (consecutive_failures >= 0)
);

INSERT INTO agent_availability_snapshots (agent_id)
SELECT id
FROM agents
ON CONFLICT (agent_id) DO NOTHING;

CREATE INDEX idx_agent_availability_status
    ON agent_availability_snapshots (availability_status, updated_at DESC);

COMMIT;
