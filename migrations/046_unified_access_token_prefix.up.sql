-- 046_unified_access_token_prefix.up.sql
-- Keep self-registration and Agent-bound credentials on the canonical
-- ol_agent_* Agent Token prefix.

BEGIN;

ALTER TABLE agent_runtime_tokens
    DROP CONSTRAINT IF EXISTS agent_runtime_tokens_prefix_format,
    ADD CONSTRAINT agent_runtime_tokens_prefix_format
        CHECK (prefix ~ '^ol_agent_[a-f0-9]+$');

ALTER TABLE agent_registration_tokens
    DROP CONSTRAINT IF EXISTS agent_registration_tokens_prefix_format,
    ADD CONSTRAINT agent_registration_tokens_prefix_format
        CHECK (prefix ~ '^ol_agent_[a-f0-9]+$');

COMMIT;
