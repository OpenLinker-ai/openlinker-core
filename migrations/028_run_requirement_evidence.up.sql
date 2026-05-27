BEGIN;

CREATE TABLE run_requirement_evidence (
    run_id UUID PRIMARY KEY REFERENCES runs(id) ON DELETE CASCADE,
    task_id UUID NOT NULL REFERENCES task_queries(id) ON DELETE CASCADE,
    agent_id UUID NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    required_skill_ids TEXT[] NOT NULL DEFAULT '{}',
    required_mcp_tools TEXT[] NOT NULL DEFAULT '{}',
    agent_skill_ids TEXT[] NOT NULL DEFAULT '{}',
    matched_skill_ids TEXT[] NOT NULL DEFAULT '{}',
    missing_skill_ids TEXT[] NOT NULL DEFAULT '{}',
    used_mcp_tools TEXT[] NOT NULL DEFAULT '{}',
    missing_mcp_tools TEXT[] NOT NULL DEFAULT '{}',
    coverage_status TEXT NOT NULL DEFAULT 'no_requirements'
        CHECK (coverage_status IN ('covered', 'partial', 'missing_requirements', 'no_requirements')),
    evidence_source TEXT NOT NULL DEFAULT 'web'
        CHECK (evidence_source IN ('web', 'mcp', 'api')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_run_requirement_evidence_task
    ON run_requirement_evidence (task_id, created_at DESC);

CREATE INDEX idx_run_requirement_evidence_agent
    ON run_requirement_evidence (agent_id, created_at DESC);

COMMIT;
