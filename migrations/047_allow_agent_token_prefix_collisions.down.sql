-- 047_allow_agent_token_prefix_collisions.down.sql
-- Restoring uniqueness can fail if duplicate prefixes were minted while this
-- migration was active. Prefer leaving prefix as a lookup hint in production.

BEGIN;

ALTER TABLE agent_registration_tokens
    ADD CONSTRAINT agent_registration_tokens_prefix_key UNIQUE (prefix);

ALTER TABLE agent_runtime_tokens
    ADD CONSTRAINT agent_runtime_tokens_prefix_key UNIQUE (prefix);

COMMIT;
