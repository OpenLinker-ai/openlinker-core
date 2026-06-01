-- 047_allow_agent_token_prefix_collisions.up.sql
-- Token prefix is a short lookup hint, not an identifier. Verification already
-- handles multiple candidates with the same prefix by bcrypt-comparing the full
-- plaintext token, so uniqueness here causes random production insert failures
-- once enough ol_live_* tokens have been minted.

BEGIN;

ALTER TABLE agent_registration_tokens
    DROP CONSTRAINT IF EXISTS agent_registration_tokens_prefix_key;

ALTER TABLE agent_runtime_tokens
    DROP CONSTRAINT IF EXISTS agent_runtime_tokens_prefix_key;

CREATE INDEX IF NOT EXISTS idx_agent_registration_tokens_prefix_active
    ON agent_registration_tokens (prefix)
    WHERE revoked_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_agent_runtime_tokens_prefix_active
    ON agent_runtime_tokens (prefix)
    WHERE revoked_at IS NULL;

COMMIT;
