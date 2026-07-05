BEGIN;

ALTER TABLE agents
    ADD COLUMN connection_mode TEXT NOT NULL DEFAULT 'direct_http',
    ADD COLUMN mcp_tool_name TEXT;

ALTER TABLE agents
    ADD CONSTRAINT agents_connection_mode_valid
        CHECK (connection_mode IN ('direct_http', 'mcp_server', 'runtime_pull')),
    ADD CONSTRAINT agents_mcp_tool_required
        CHECK (
            connection_mode <> 'mcp_server'
            OR (
                mcp_tool_name IS NOT NULL
                AND char_length(trim(mcp_tool_name)) BETWEEN 1 AND 120
            )
        ),
    ADD CONSTRAINT agents_runtime_pull_endpoint
        CHECK (
            connection_mode <> 'runtime_pull'
            OR endpoint_url LIKE 'openlinker-runtime-pull://%'
        );

ALTER TABLE agents
    DROP CONSTRAINT IF EXISTS agents_endpoint_https,
    ADD CONSTRAINT agents_endpoint_https CHECK (
        endpoint_url LIKE 'https://%' OR
        endpoint_url = 'http://localhost' OR
        endpoint_url LIKE 'http://localhost:%' OR
        endpoint_url LIKE 'http://localhost/%' OR
        endpoint_url = 'http://127.0.0.1' OR
        endpoint_url LIKE 'http://127.0.0.1:%' OR
        endpoint_url LIKE 'http://127.0.0.1/%' OR
        endpoint_url = 'http://[::1]' OR
        endpoint_url LIKE 'http://[::1]:%' OR
        endpoint_url LIKE 'http://[::1]/%' OR
        endpoint_url LIKE 'openlinker-runtime-pull://%'
    );

ALTER TABLE runs
    ADD COLUMN claimed_by_runtime_token_id UUID REFERENCES agent_runtime_tokens(id) ON DELETE SET NULL,
    ADD COLUMN claimed_at TIMESTAMPTZ;

CREATE INDEX idx_runs_runtime_pull_claim
    ON runs (agent_id, started_at ASC)
    WHERE status = 'running' AND claimed_at IS NULL;

COMMIT;
