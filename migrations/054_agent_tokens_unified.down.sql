BEGIN;

CREATE TABLE IF NOT EXISTS agent_registration_tokens (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    creator_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    label TEXT NOT NULL,
    prefix TEXT NOT NULL,
    token_hash TEXT NOT NULL,
    max_agents INTEGER NOT NULL DEFAULT 1,
    used_count INTEGER NOT NULL DEFAULT 0,
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ,
    last_used_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT agent_registration_tokens_label_len
        CHECK (char_length(label) BETWEEN 1 AND 80),
    CONSTRAINT agent_registration_tokens_prefix_format
        CHECK (prefix ~ '^ol_live_[a-f0-9]+$'),
    CONSTRAINT agent_registration_tokens_max_agents_range
        CHECK (max_agents BETWEEN 1 AND 10),
    CONSTRAINT agent_registration_tokens_used_nonneg
        CHECK (used_count >= 0)
);

INSERT INTO agent_registration_tokens (
    id, creator_user_id, label, prefix, token_hash, max_agents, used_count,
    expires_at, revoked_at, last_used_at, created_at
)
SELECT
    id,
    creator_user_id,
    name,
    prefix,
    token_hash,
    1,
    CASE WHEN redeemed_at IS NULL THEN 0 ELSE 1 END,
    COALESCE(expires_at, created_at),
    revoked_at,
    last_used_at,
    created_at
FROM agent_tokens
WHERE status = 'pending_registration';

ALTER TABLE agent_tokens
    DROP CONSTRAINT IF EXISTS agent_tokens_name_len,
    DROP CONSTRAINT IF EXISTS agent_tokens_prefix_format,
    DROP CONSTRAINT IF EXISTS agent_tokens_scopes_nonempty,
    DROP CONSTRAINT IF EXISTS agent_tokens_scopes_known,
    DROP CONSTRAINT IF EXISTS agent_tokens_status_valid,
    DROP CONSTRAINT IF EXISTS agent_tokens_pending_shape,
    DROP CONSTRAINT IF EXISTS agent_tokens_runtime_shape;

DELETE FROM agent_tokens WHERE agent_id IS NULL;

ALTER TABLE agent_tokens
    ALTER COLUMN agent_id SET NOT NULL;

ALTER TABLE agent_tokens
    DROP COLUMN IF EXISTS status,
    DROP COLUMN IF EXISTS expires_at,
    DROP COLUMN IF EXISTS redeemed_at;

ALTER TABLE agent_tokens
    RENAME COLUMN creator_user_id TO created_by_user_id;

ALTER TABLE agent_tokens RENAME TO agent_runtime_tokens;

ALTER TABLE agent_runtime_tokens
    ADD CONSTRAINT agent_runtime_tokens_name_len
        CHECK (char_length(name) BETWEEN 1 AND 80),
    ADD CONSTRAINT agent_runtime_tokens_prefix_format
        CHECK (prefix ~ '^ol_live_[a-f0-9]+$'),
    ADD CONSTRAINT agent_runtime_tokens_scopes_nonempty
        CHECK (cardinality(scopes) > 0),
    ADD CONSTRAINT agent_runtime_tokens_scopes_known
        CHECK (scopes <@ ARRAY['agent:call', 'agent:pull']::text[]);

DROP INDEX IF EXISTS idx_agent_tokens_creator;
DROP INDEX IF EXISTS idx_agent_tokens_agent;
DROP INDEX IF EXISTS idx_agent_tokens_prefix_active;
DROP INDEX IF EXISTS idx_agent_tokens_pending_expiry;

CREATE INDEX IF NOT EXISTS idx_agent_runtime_tokens_agent
    ON agent_runtime_tokens (agent_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_agent_runtime_tokens_prefix_active
    ON agent_runtime_tokens (prefix)
    WHERE revoked_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_agent_registration_tokens_creator
    ON agent_registration_tokens (creator_user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_agent_registration_tokens_prefix_active
    ON agent_registration_tokens (prefix)
    WHERE revoked_at IS NULL;

COMMIT;
