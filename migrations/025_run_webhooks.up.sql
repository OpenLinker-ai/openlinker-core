BEGIN;

CREATE TABLE run_webhook_subscriptions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    owner_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    caller_agent_id UUID REFERENCES agents(id) ON DELETE SET NULL,
    target_url TEXT NOT NULL,
    secret TEXT NOT NULL,
    event_types TEXT[] NOT NULL DEFAULT ARRAY['run.completed', 'run.failed']::TEXT[],
    status TEXT NOT NULL DEFAULT 'active',
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ,
    CONSTRAINT run_webhook_subscriptions_status_valid
        CHECK (status IN ('active', 'paused', 'failed', 'deleted')),
    CONSTRAINT run_webhook_subscriptions_failures_nonneg
        CHECK (consecutive_failures >= 0),
    CONSTRAINT run_webhook_subscriptions_event_types_nonempty
        CHECK (cardinality(event_types) > 0),
    CONSTRAINT run_webhook_subscriptions_url_len
        CHECK (char_length(target_url) BETWEEN 1 AND 500)
);

CREATE INDEX idx_run_webhook_subscriptions_run
    ON run_webhook_subscriptions (run_id, created_at DESC)
    WHERE status <> 'deleted';

CREATE INDEX idx_run_webhook_subscriptions_active
    ON run_webhook_subscriptions (run_id)
    WHERE status = 'active';

CREATE TRIGGER run_webhook_subscriptions_set_updated_at
    BEFORE UPDATE ON run_webhook_subscriptions
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

CREATE TABLE run_webhook_deliveries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subscription_id UUID NOT NULL REFERENCES run_webhook_subscriptions(id) ON DELETE CASCADE,
    run_event_id UUID NOT NULL REFERENCES run_events(id) ON DELETE CASCADE,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    response_status INTEGER,
    response_body TEXT,
    error_message TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT run_webhook_deliveries_status_valid
        CHECK (status IN ('pending', 'success', 'failed'))
);

CREATE UNIQUE INDEX idx_run_webhook_deliveries_subscription_event
    ON run_webhook_deliveries (subscription_id, run_event_id);

CREATE INDEX idx_run_webhook_deliveries_pending
    ON run_webhook_deliveries (next_retry_at)
    WHERE status = 'pending' AND next_retry_at IS NOT NULL;

CREATE INDEX idx_run_webhook_deliveries_subscription
    ON run_webhook_deliveries (subscription_id, created_at DESC);

CREATE TRIGGER run_webhook_deliveries_set_updated_at
    BEFORE UPDATE ON run_webhook_deliveries
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

COMMIT;
