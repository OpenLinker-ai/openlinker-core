-- 011_agent_registration_tokens.up.sql
-- Phase 2 缺口 1：Agent 自注册访问令牌。
-- docs/29 §2，docs/22 §2.1。
--
-- 注册用途访问令牌与 Agent 绑定用途访问令牌已经统一为同一种
-- ol_agent_* Agent Token；状态和绑定关系决定它处于待注册还是运行态。
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
        CHECK (prefix ~ '^ol_agent_[a-f0-9]+$'),
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
