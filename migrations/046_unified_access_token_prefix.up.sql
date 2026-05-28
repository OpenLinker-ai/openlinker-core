-- 046_unified_access_token_prefix.up.sql
-- Allow the unified ol_live_* access-token prefix for self-registration and Agent-bound tokens.
-- Legacy br_live_* / rt_live_* tokens remain valid for existing local/dev data.

BEGIN;

ALTER TABLE agent_runtime_tokens
    DROP CONSTRAINT IF EXISTS agent_runtime_tokens_prefix_format,
    ADD CONSTRAINT agent_runtime_tokens_prefix_format
        CHECK (prefix ~ '^(rt_live_|ol_live_)[a-f0-9]+$');

ALTER TABLE agent_registration_tokens
    DROP CONSTRAINT IF EXISTS agent_registration_tokens_prefix_format,
    ADD CONSTRAINT agent_registration_tokens_prefix_format
        CHECK (prefix ~ '^(br_live_|ol_live_)[a-f0-9]+$');

COMMIT;
