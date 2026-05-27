BEGIN;

DROP INDEX IF EXISTS idx_runs_runtime_pull_claim;

ALTER TABLE runs
    DROP COLUMN IF EXISTS claimed_at,
    DROP COLUMN IF EXISTS claimed_by_runtime_token_id;

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
        endpoint_url LIKE 'http://[::1]/%'
    );

ALTER TABLE agents
    DROP CONSTRAINT IF EXISTS agents_runtime_pull_endpoint,
    DROP CONSTRAINT IF EXISTS agents_mcp_tool_required,
    DROP CONSTRAINT IF EXISTS agents_connection_mode_valid,
    DROP COLUMN IF EXISTS mcp_tool_name,
    DROP COLUMN IF EXISTS connection_mode;

COMMIT;
