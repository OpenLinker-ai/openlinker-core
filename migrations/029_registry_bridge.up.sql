BEGIN;

CREATE TABLE registry_nodes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    node_name TEXT NOT NULL,
    node_type TEXT NOT NULL DEFAULT 'bridge_proxy'
        CHECK (node_type IN ('self_hosted', 'bridge_proxy')),
    base_url TEXT,
    secret_prefix TEXT NOT NULL,
    secret_hash TEXT NOT NULL,
    scopes TEXT[] NOT NULL DEFAULT ARRAY['heartbeat', 'listing:sync']::text[],
    heartbeat_status TEXT NOT NULL DEFAULT 'unknown'
        CHECK (heartbeat_status IN ('unknown', 'healthy', 'stale', 'revoked')),
    last_heartbeat_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT registry_nodes_name_len CHECK (char_length(node_name) BETWEEN 2 AND 120),
    CONSTRAINT registry_nodes_secret_prefix_format CHECK (secret_prefix ~ '^rn_live_[a-f0-9]+$'),
    CONSTRAINT registry_nodes_base_url_len CHECK (base_url IS NULL OR char_length(base_url) <= 500),
    CONSTRAINT registry_nodes_scopes_known CHECK (
        scopes <@ ARRAY['heartbeat', 'listing:sync', 'proxy:pull', 'proxy:result']::text[]
    )
);

CREATE INDEX idx_registry_nodes_owner
    ON registry_nodes (owner_user_id, created_at DESC);

CREATE INDEX idx_registry_nodes_secret_prefix
    ON registry_nodes (secret_prefix)
    WHERE revoked_at IS NULL;

CREATE TRIGGER registry_nodes_set_updated_at BEFORE UPDATE ON registry_nodes
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

CREATE TABLE cloud_listing_links (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cloud_listing_id UUID NOT NULL DEFAULT gen_random_uuid(),
    registry_node_id UUID NOT NULL REFERENCES registry_nodes(id) ON DELETE CASCADE,
    local_agent_id UUID NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    routing_mode TEXT NOT NULL DEFAULT 'pull_proxy'
        CHECK (routing_mode IN ('direct_endpoint', 'pull_proxy')),
    payload_policy TEXT NOT NULL DEFAULT 'metadata_only'
        CHECK (payload_policy IN ('metadata_only', 'store_run_summary', 'store_full_payload')),
    sync_status TEXT NOT NULL DEFAULT 'linked'
        CHECK (sync_status IN ('linked', 'paused', 'error')),
    last_sync_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT cloud_listing_links_node_agent_unique UNIQUE (registry_node_id, local_agent_id)
);

CREATE INDEX idx_cloud_listing_links_node
    ON cloud_listing_links (registry_node_id, created_at DESC);

CREATE INDEX idx_cloud_listing_links_agent
    ON cloud_listing_links (local_agent_id, created_at DESC);

CREATE INDEX idx_cloud_listing_links_cloud_listing
    ON cloud_listing_links (cloud_listing_id);

CREATE TRIGGER cloud_listing_links_set_updated_at BEFORE UPDATE ON cloud_listing_links
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

COMMIT;
