BEGIN;

CREATE TABLE registry_federation_invites (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    api_base_url TEXT NOT NULL,
    bearer_token TEXT NOT NULL,
    token_prefix TEXT NOT NULL,
    token_hash TEXT NOT NULL,
    credential_hint TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'consumed', 'expired', 'revoked')),
    expires_at TIMESTAMPTZ NOT NULL,
    consumed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT registry_federation_invites_name_len CHECK (char_length(name) BETWEEN 2 AND 120),
    CONSTRAINT registry_federation_invites_api_base_url_len CHECK (char_length(api_base_url) BETWEEN 8 AND 500),
    CONSTRAINT registry_federation_invites_bearer_token_len CHECK (char_length(bearer_token) BETWEEN 8 AND 4096)
);

CREATE INDEX idx_registry_federation_invites_owner
    ON registry_federation_invites (owner_user_id, created_at DESC);

CREATE INDEX idx_registry_federation_invites_token_prefix
    ON registry_federation_invites (token_prefix, status);

CREATE TRIGGER registry_federation_invites_set_updated_at BEFORE UPDATE ON registry_federation_invites
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

COMMIT;
