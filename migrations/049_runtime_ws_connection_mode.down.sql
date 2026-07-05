BEGIN;

UPDATE agents
SET connection_mode = 'runtime_pull',
    endpoint_url = regexp_replace(endpoint_url, '^openlinker-runtime-ws://', 'openlinker-runtime-pull://')
WHERE connection_mode = 'runtime_ws';

ALTER TABLE agents
    DROP CONSTRAINT IF EXISTS agents_connection_mode_valid,
    ADD CONSTRAINT agents_connection_mode_valid
        CHECK (connection_mode IN ('direct_http', 'mcp_server', 'runtime_pull'));

ALTER TABLE agents
    DROP CONSTRAINT IF EXISTS agents_runtime_queue_endpoint,
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

COMMIT;
