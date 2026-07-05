-- 002_webhooks.up.sql
-- 子轮 2.1: agents.webhook_url/secret + webhook_deliveries
-- api_keys 表已迁至 openlinker-cloud/migrations/001_cloud_init.up.sql (商业化能力归 cloud)

BEGIN;

-- ──────────────────────────────────────────────────────
-- agents 加 webhook_url + webhook_secret(创作者侧 webhook,归 core)
-- ──────────────────────────────────────────────────────
ALTER TABLE agents
    ADD COLUMN webhook_url TEXT,
    ADD COLUMN webhook_secret TEXT;

ALTER TABLE agents
    ADD CONSTRAINT agents_webhook_https CHECK (
        webhook_url IS NULL OR webhook_url LIKE 'https://%'
    );

-- ──────────────────────────────────────────────────────
-- webhook 投递日志
-- ──────────────────────────────────────────────────────
CREATE TABLE webhook_deliveries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id UUID NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    run_id UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    response_status INTEGER,
    response_body TEXT,
    error_message TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMPTZ,
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
