-- 009_delivery_targets.up.sql
-- Phase 2 §7：用户侧 Output Delivery 抽象。
--
-- 设计：
--   - delivery_targets：用户拥有的投递目标（Phase 2 支持 webhook / slack）
--   - run_deliveries：每次投递的记录，跑 webhook 同款 1min/5min/30min 重试
--
-- 与 webhook_deliveries 共存而非替换：webhook_deliveries 是旧创作者侧 webhook
-- 队列表；run_deliveries 服务用户侧自配 target。

BEGIN;

CREATE TABLE delivery_targets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    secret TEXT NOT NULL,
    is_default BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT delivery_targets_type_valid CHECK (type IN ('webhook', 'slack')),
    CONSTRAINT delivery_targets_name_len CHECK (char_length(name) BETWEEN 1 AND 80)
);

CREATE INDEX idx_delivery_targets_user ON delivery_targets (user_id, created_at DESC);

-- 每个用户最多一个 is_default=true 的 target（部分唯一索引）
CREATE UNIQUE INDEX idx_delivery_targets_default_per_user
    ON delivery_targets (user_id)
    WHERE is_default = TRUE;

CREATE TRIGGER delivery_targets_set_updated_at
    BEFORE UPDATE ON delivery_targets
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

CREATE TABLE run_deliveries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    target_id UUID NOT NULL REFERENCES delivery_targets(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    -- target_type / target_url 是投递时的快照，target 被删后仍能看到历史
    target_type TEXT NOT NULL,
    target_url TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    response_status INTEGER,
    response_body TEXT,
    error_message TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT run_deliveries_status_valid CHECK (status IN ('pending', 'success', 'failed'))
);

CREATE INDEX idx_run_deliveries_run ON run_deliveries (run_id, created_at DESC);
CREATE INDEX idx_run_deliveries_user ON run_deliveries (user_id, created_at DESC);
CREATE INDEX idx_run_deliveries_pending
    ON run_deliveries (next_retry_at)
    WHERE status = 'pending' AND next_retry_at IS NOT NULL;

CREATE TRIGGER run_deliveries_set_updated_at
    BEFORE UPDATE ON run_deliveries
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

COMMIT;
