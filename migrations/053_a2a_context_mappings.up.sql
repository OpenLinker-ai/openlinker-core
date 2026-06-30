-- 053_a2a_context_mappings.up.sql
-- Platform-level A2A context mapping. This keeps protocol context/task ids
-- separate from OpenLinker run/delegation lineage so multi-Agent calls remain
-- auditable without overloading the A2A protocol fields.

BEGIN;

CREATE TABLE a2a_context_mappings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL UNIQUE REFERENCES runs(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    agent_id UUID NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    protocol_context_id TEXT NOT NULL,
    protocol_task_id TEXT NOT NULL,
    root_context_id TEXT NOT NULL,
    parent_context_id TEXT NOT NULL DEFAULT '',
    parent_task_id TEXT NOT NULL DEFAULT '',
    parent_run_id UUID REFERENCES runs(id) ON DELETE SET NULL,
    caller_agent_id UUID REFERENCES agents(id) ON DELETE SET NULL,
    target_agent_id UUID REFERENCES agents(id) ON DELETE SET NULL,
    trace_id TEXT NOT NULL DEFAULT '',
    reference_task_ids TEXT[] NOT NULL DEFAULT ARRAY[]::text[],
    source TEXT NOT NULL DEFAULT 'a2a_protocol',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT a2a_context_mappings_protocol_context_len
        CHECK (char_length(protocol_context_id) BETWEEN 1 AND 200),
    CONSTRAINT a2a_context_mappings_protocol_task_len
        CHECK (char_length(protocol_task_id) BETWEEN 1 AND 200),
    CONSTRAINT a2a_context_mappings_root_context_len
        CHECK (char_length(root_context_id) BETWEEN 1 AND 200),
    CONSTRAINT a2a_context_mappings_parent_context_len
        CHECK (char_length(parent_context_id) <= 200),
    CONSTRAINT a2a_context_mappings_parent_task_len
        CHECK (char_length(parent_task_id) <= 200),
    CONSTRAINT a2a_context_mappings_trace_len
        CHECK (char_length(trace_id) <= 200),
    CONSTRAINT a2a_context_mappings_source_valid
        CHECK (source IN ('a2a_protocol', 'agent_delegation'))
);

CREATE INDEX idx_a2a_context_mappings_user_root
    ON a2a_context_mappings (user_id, root_context_id, created_at ASC);

CREATE INDEX idx_a2a_context_mappings_user_protocol_context
    ON a2a_context_mappings (user_id, agent_id, protocol_context_id, created_at DESC);

CREATE INDEX idx_a2a_context_mappings_parent_run
    ON a2a_context_mappings (parent_run_id, created_at ASC)
    WHERE parent_run_id IS NOT NULL;

CREATE INDEX idx_a2a_context_mappings_trace
    ON a2a_context_mappings (trace_id, created_at ASC)
    WHERE trace_id <> '';

CREATE TRIGGER a2a_context_mappings_set_updated_at
    BEFORE UPDATE ON a2a_context_mappings
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

COMMIT;
