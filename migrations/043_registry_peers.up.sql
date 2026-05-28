BEGIN;

CREATE TABLE registry_peers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    api_base_url TEXT NOT NULL,
    bearer_token TEXT NOT NULL,
    credential_hint TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'paused')),
    last_used_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT registry_peers_name_len CHECK (char_length(name) BETWEEN 2 AND 120),
    CONSTRAINT registry_peers_api_base_url_len CHECK (char_length(api_base_url) BETWEEN 8 AND 500),
    CONSTRAINT registry_peers_bearer_token_len CHECK (char_length(bearer_token) BETWEEN 8 AND 4096)
);

CREATE INDEX idx_registry_peers_owner
    ON registry_peers (owner_user_id, created_at DESC);

CREATE UNIQUE INDEX idx_registry_peers_owner_name
    ON registry_peers (owner_user_id, lower(name));

CREATE TRIGGER registry_peers_set_updated_at BEFORE UPDATE ON registry_peers
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

COMMIT;
