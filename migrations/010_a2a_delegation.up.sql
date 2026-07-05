-- 010_a2a_delegation.up.sql
-- Platform-mediated Agent-to-Agent delegation foundation.

BEGIN;

CREATE TABLE agent_runtime_tokens (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id UUID NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    created_by_user_id UUID NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    name TEXT NOT NULL,
    prefix TEXT NOT NULL UNIQUE,
    token_hash TEXT NOT NULL,
    scopes TEXT[] NOT NULL DEFAULT ARRAY['agent:call']::text[],
    last_used_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT agent_runtime_tokens_name_len CHECK (char_length(name) BETWEEN 1 AND 80),
    CONSTRAINT agent_runtime_tokens_prefix_format CHECK (prefix ~ '^ol_agent_[a-f0-9]+$'),
    CONSTRAINT agent_runtime_tokens_call_scope CHECK ('agent:call' = ANY(scopes))
);

CREATE INDEX idx_agent_runtime_tokens_agent
    ON agent_runtime_tokens (agent_id, created_at DESC);
CREATE INDEX idx_agent_runtime_tokens_prefix_active
    ON agent_runtime_tokens (prefix)
    WHERE revoked_at IS NULL;

CREATE TABLE agent_call_policies (
    agent_id UUID PRIMARY KEY REFERENCES agents(id) ON DELETE CASCADE,
    callable_by TEXT NOT NULL DEFAULT 'public',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT agent_call_policies_callable_by_valid
        CHECK (callable_by IN ('public', 'same_creator', 'private'))
);

INSERT INTO agent_call_policies (agent_id)
SELECT id FROM agents
ON CONFLICT (agent_id) DO NOTHING;

CREATE TRIGGER agent_call_policies_set_updated_at
    BEFORE UPDATE ON agent_call_policies
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

CREATE TABLE run_delegations (
    child_run_id UUID PRIMARY KEY REFERENCES runs(id) ON DELETE CASCADE,
    parent_run_id UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    caller_agent_id UUID NOT NULL REFERENCES agents(id) ON DELETE RESTRICT,
    reason TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT run_delegations_distinct_runs CHECK (child_run_id <> parent_run_id),
    CONSTRAINT run_delegations_reason_len CHECK (char_length(reason) <= 500)
);

CREATE INDEX idx_run_delegations_parent
    ON run_delegations (parent_run_id, created_at ASC);
CREATE INDEX idx_run_delegations_caller
    ON run_delegations (caller_agent_id, created_at DESC);

COMMIT;
