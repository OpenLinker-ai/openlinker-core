-- 002_api_keys_and_webhooks.up.sql
-- 子轮 2.1：API Key 管理 + Webhook
-- 关联 docs/13-phase1-prd.md（Phase 2 扩展）

BEGIN;

-- ──────────────────────────────────────────────────────
-- 7. API Keys（用户/开发者用 cURL 调 API 的密钥）
-- ──────────────────────────────────────────────────────
CREATE TABLE api_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name TEXT NOT NULL,                     -- 用户自取（如 "production" / "ci"）
    prefix TEXT NOT NULL,                   -- 明文前缀（"sk_live_abcd" 头 12 位），UI 展示
    key_hash TEXT NOT NULL,                 -- bcrypt(完整明文)，验证时 bcrypt.Compare
    last_used_at TIMESTAMPTZ,               -- 最近一次成功调用
    revoked_at TIMESTAMPTZ,                 -- 撤销时间（NULL 表示有效）
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT api_keys_prefix_format CHECK (prefix ~ '^sk_live_[a-f0-9]+$'),
    CONSTRAINT api_keys_name_len CHECK (char_length(name) BETWEEN 1 AND 80)
);

-- 一个用户的有效 key 数量限制（10 个）通过应用层校验
CREATE INDEX idx_api_keys_user ON api_keys (user_id, created_at DESC) WHERE revoked_at IS NULL;
CREATE INDEX idx_api_keys_prefix ON api_keys (prefix);

-- ──────────────────────────────────────────────────────
-- 8. agents 表加 webhook_url + webhook_secret
-- ──────────────────────────────────────────────────────
ALTER TABLE agents
    ADD COLUMN webhook_url TEXT,
    ADD COLUMN webhook_secret TEXT;

ALTER TABLE agents
    ADD CONSTRAINT agents_webhook_https CHECK (
        webhook_url IS NULL OR webhook_url LIKE 'https://%'
    );

-- ──────────────────────────────────────────────────────
-- 9. webhook 投递日志
-- ──────────────────────────────────────────────────────
CREATE TABLE webhook_deliveries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id UUID NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    run_id UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    url TEXT NOT NULL,                      -- 投递时的 webhook_url（快照）
    payload JSONB NOT NULL,                 -- 投递的 body
    status TEXT NOT NULL DEFAULT 'pending', -- pending / success / failed
    response_status INTEGER,                -- HTTP status code
    response_body TEXT,                     -- 截断到 1KB
    error_message TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMPTZ,              -- 下次重试时间（NULL 表示完成或放弃）
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT webhook_deliveries_status_valid CHECK (status IN ('pending', 'success', 'failed'))
);

CREATE INDEX idx_webhook_deliveries_agent ON webhook_deliveries (agent_id, created_at DESC);
CREATE INDEX idx_webhook_deliveries_run ON webhook_deliveries (run_id);
CREATE INDEX idx_webhook_deliveries_pending
    ON webhook_deliveries (next_retry_at)
    WHERE status = 'pending' AND next_retry_at IS NOT NULL;

CREATE TRIGGER webhook_deliveries_set_updated_at
    BEFORE UPDATE ON webhook_deliveries
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

COMMIT;
