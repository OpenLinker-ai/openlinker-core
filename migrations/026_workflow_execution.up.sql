BEGIN;

CREATE TABLE workflows (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'active',
    edges JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT workflows_status_valid CHECK (status IN ('active', 'archived')),
    CONSTRAINT workflows_name_len CHECK (char_length(name) BETWEEN 1 AND 120),
    CONSTRAINT workflows_edges_array CHECK (jsonb_typeof(edges) = 'array')
);

CREATE INDEX idx_workflows_user ON workflows (user_id, created_at DESC);

CREATE TRIGGER workflows_set_updated_at
    BEFORE UPDATE ON workflows
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

CREATE TABLE workflow_nodes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    node_key TEXT NOT NULL,
    node_type TEXT NOT NULL DEFAULT 'agent',
    agent_id UUID NOT NULL REFERENCES agents(id) ON DELETE RESTRICT,
    title TEXT NOT NULL,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    position INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT workflow_nodes_type_valid CHECK (node_type IN ('agent')),
    CONSTRAINT workflow_nodes_key_len CHECK (char_length(node_key) BETWEEN 1 AND 80),
    CONSTRAINT workflow_nodes_title_len CHECK (char_length(title) BETWEEN 1 AND 160),
    CONSTRAINT workflow_nodes_position_nonnegative CHECK (position >= 0)
);

CREATE UNIQUE INDEX idx_workflow_nodes_key
    ON workflow_nodes (workflow_id, node_key);

CREATE INDEX idx_workflow_nodes_order
    ON workflow_nodes (workflow_id, position ASC, created_at ASC);

CREATE TABLE workflow_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'running',
    input JSONB NOT NULL DEFAULT '{}'::jsonb,
    output JSONB,
    error_message TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT workflow_runs_status_valid CHECK (status IN ('running', 'success', 'failed'))
);

CREATE INDEX idx_workflow_runs_workflow ON workflow_runs (workflow_id, created_at DESC);
CREATE INDEX idx_workflow_runs_user ON workflow_runs (user_id, created_at DESC);

CREATE TRIGGER workflow_runs_set_updated_at
    BEFORE UPDATE ON workflow_runs
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

CREATE TABLE workflow_run_steps (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_run_id UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    workflow_node_id UUID NOT NULL REFERENCES workflow_nodes(id) ON DELETE RESTRICT,
    node_key TEXT NOT NULL,
    agent_id UUID NOT NULL REFERENCES agents(id) ON DELETE RESTRICT,
    run_id UUID REFERENCES runs(id) ON DELETE SET NULL,
    status TEXT NOT NULL DEFAULT 'running',
    input JSONB NOT NULL DEFAULT '{}'::jsonb,
    output JSONB,
    error_message TEXT,
    sequence INTEGER NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT workflow_run_steps_status_valid CHECK (status IN ('running', 'success', 'failed')),
    CONSTRAINT workflow_run_steps_sequence_nonnegative CHECK (sequence >= 0)
);

CREATE INDEX idx_workflow_run_steps_run ON workflow_run_steps (workflow_run_id, sequence ASC);

CREATE TRIGGER workflow_run_steps_set_updated_at
    BEFORE UPDATE ON workflow_run_steps
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

COMMIT;
