BEGIN;

ALTER TABLE IF EXISTS agent_runtime_tokens RENAME TO agent_tokens;

ALTER TABLE agent_tokens
    RENAME COLUMN created_by_user_id TO creator_user_id;

ALTER TABLE agent_tokens
    ALTER COLUMN agent_id DROP NOT NULL;

ALTER TABLE agent_tokens
    DROP CONSTRAINT IF EXISTS agent_runtime_tokens_prefix_format,
    DROP CONSTRAINT IF EXISTS agent_runtime_tokens_name_len,
    DROP CONSTRAINT IF EXISTS agent_runtime_tokens_scopes_nonempty,
    DROP CONSTRAINT IF EXISTS agent_runtime_tokens_scopes_known,
    DROP CONSTRAINT IF EXISTS agent_runtime_tokens_call_scope;

ALTER TABLE agent_tokens
    ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active_runtime',
    ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS redeemed_at TIMESTAMPTZ;

UPDATE agent_tokens
SET status = CASE WHEN revoked_at IS NULL THEN 'active_runtime' ELSE 'revoked' END,
    redeemed_at = COALESCE(redeemed_at, created_at);

INSERT INTO agent_tokens (
    id, agent_id, creator_user_id, name, prefix, token_hash, scopes,
    last_used_at, revoked_at, created_at, status, expires_at, redeemed_at
)
SELECT
    id,
    NULL,
    creator_user_id,
    label,
    prefix,
    token_hash,
    ARRAY['agent:call', 'agent:pull']::text[],
    last_used_at,
    COALESCE(revoked_at, NOW()),
    created_at,
    'revoked',
    expires_at,
    NULL
FROM agent_registration_tokens
ON CONFLICT (id) DO NOTHING;

DROP TABLE IF EXISTS agent_registration_tokens;

UPDATE agent_tokens
SET prefix = 'ol_agent_' || substr(replace(id::text, '-', ''), 1, 4),
    revoked_at = COALESCE(revoked_at, NOW()),
    status = 'revoked'
WHERE prefix !~ '^ol_agent_[a-f0-9]+$';

ALTER TABLE agent_tokens
    ADD CONSTRAINT agent_tokens_name_len
        CHECK (char_length(name) BETWEEN 1 AND 80),
    ADD CONSTRAINT agent_tokens_prefix_format
        CHECK (prefix ~ '^ol_agent_[a-f0-9]+$'),
    ADD CONSTRAINT agent_tokens_scopes_nonempty
        CHECK (cardinality(scopes) > 0),
    ADD CONSTRAINT agent_tokens_scopes_known
        CHECK (scopes <@ ARRAY['agent:call', 'agent:pull']::text[]),
    ADD CONSTRAINT agent_tokens_status_valid
        CHECK (status IN ('pending_registration', 'active_runtime', 'revoked')),
    ADD CONSTRAINT agent_tokens_pending_shape
        CHECK (
            (status = 'pending_registration' AND agent_id IS NULL AND redeemed_at IS NULL)
            OR status <> 'pending_registration'
        ),
    ADD CONSTRAINT agent_tokens_runtime_shape
        CHECK (
            (status = 'active_runtime' AND agent_id IS NOT NULL AND redeemed_at IS NOT NULL)
            OR status <> 'active_runtime'
        );

DROP INDEX IF EXISTS idx_agent_runtime_tokens_agent;
DROP INDEX IF EXISTS idx_agent_runtime_tokens_prefix_active;
DROP INDEX IF EXISTS idx_agent_registration_tokens_creator;
DROP INDEX IF EXISTS idx_agent_registration_tokens_prefix_active;

CREATE INDEX IF NOT EXISTS idx_agent_tokens_creator
    ON agent_tokens (creator_user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_agent_tokens_agent
    ON agent_tokens (agent_id, created_at DESC)
    WHERE agent_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_agent_tokens_prefix_active
    ON agent_tokens (prefix)
    WHERE revoked_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_agent_tokens_pending_expiry
    ON agent_tokens (expires_at)
    WHERE status = 'pending_registration' AND revoked_at IS NULL;

COMMIT;
