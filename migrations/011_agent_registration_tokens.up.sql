-- 011_agent_registration_tokens.up.sql
-- Phase 2 缺口 1：Agent 自注册 Bootstrap Token。
-- docs/29 §2，docs/22 §2.1。
--
-- Bootstrap Token 与 Runtime Token 解耦：
--   bootstrap (br_live_xxx) 短期、与创作者绑定，只能换 (1..max_agents) 次注册；
--   runtime   (rt_live_xxx) 长期、与单个 Agent 绑定，由 a2a / runtime 调用流复用。
--
-- 用尽 (used_count >= max_agents) 或过期 (expires_at <= NOW()) 或撤销 (revoked_at IS NOT NULL)
-- 任一条件成立即视为失效。

BEGIN;

CREATE TABLE agent_registration_tokens (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    creator_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    label TEXT NOT NULL,
    prefix TEXT NOT NULL UNIQUE,
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
        CHECK (prefix ~ '^br_live_[a-f0-9]+$'),
    CONSTRAINT agent_registration_tokens_max_agents_range
        CHECK (max_agents BETWEEN 1 AND 10),
    CONSTRAINT agent_registration_tokens_used_nonneg
        CHECK (used_count >= 0 AND used_count <= max_agents)
);

CREATE INDEX idx_agent_registration_tokens_creator
    ON agent_registration_tokens (creator_user_id, created_at DESC);

CREATE INDEX idx_agent_registration_tokens_prefix_active
    ON agent_registration_tokens (prefix)
    WHERE revoked_at IS NULL;

COMMIT;
