-- 013_agent_action_approvals.up.sql
-- Phase 2 缺口 2：高风险动作审批通道（docs/29 §3.4）。
--
-- 当前阶段只落数据模型 + JWT 路径上的 CRUD；
-- Agent 绑定访问令牌自动写 approval 的"202 + url"路径需要 scope 系统，后置。
--
-- requested_by_token_id 允许 NULL：人类（创作者本人或运营）从 /hub 发起的也走这条记录。

BEGIN;

CREATE TABLE agent_action_approval_requests (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id UUID NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    requested_by_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    requested_by_token_id UUID REFERENCES agent_runtime_tokens(id) ON DELETE SET NULL,
    action TEXT NOT NULL,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'pending',
    approval_url_slug TEXT NOT NULL UNIQUE,
    expires_at TIMESTAMPTZ NOT NULL,
    decided_at TIMESTAMPTZ,
    decided_by_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    decision_note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT agent_action_approval_action_len
        CHECK (char_length(action) BETWEEN 1 AND 80),
    CONSTRAINT agent_action_approval_status_valid
        CHECK (status IN ('pending', 'confirmed', 'rejected', 'expired')),
    CONSTRAINT agent_action_approval_slug_format
        CHECK (approval_url_slug ~ '^[a-zA-Z0-9_-]{16,64}$'),
    CONSTRAINT agent_action_approval_decision_consistent
        CHECK (
            (status = 'pending' AND decided_at IS NULL AND decided_by_user_id IS NULL)
            OR (status IN ('confirmed', 'rejected') AND decided_at IS NOT NULL)
            OR (status = 'expired')
        )
);

CREATE INDEX idx_agent_approvals_agent
    ON agent_action_approval_requests (agent_id, created_at DESC);
CREATE INDEX idx_agent_approvals_creator_pending
    ON agent_action_approval_requests (requested_by_user_id, created_at DESC)
    WHERE status = 'pending';
CREATE INDEX idx_agent_approvals_expiry
    ON agent_action_approval_requests (expires_at)
    WHERE status = 'pending';

COMMIT;
