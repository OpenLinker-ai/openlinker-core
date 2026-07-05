BEGIN;

CREATE TABLE agent_availability_alerts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id UUID NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    creator_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    alert_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    availability_status TEXT NOT NULL,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    title TEXT NOT NULL,
    message TEXT NOT NULL,
    last_error TEXT,
    repair_hints TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    read_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT agent_availability_alert_type_valid
        CHECK (alert_type IN ('availability_failed', 'availability_recovered')),
    CONSTRAINT agent_availability_alert_severity_valid
        CHECK (severity IN ('info', 'warning', 'critical')),
    CONSTRAINT agent_availability_alert_status_valid
        CHECK (availability_status IN ('unknown', 'healthy', 'degraded', 'unreachable')),
    CONSTRAINT agent_availability_alert_failures_nonneg
        CHECK (consecutive_failures >= 0),
    CONSTRAINT agent_availability_alert_title_len
        CHECK (char_length(title) BETWEEN 1 AND 160),
    CONSTRAINT agent_availability_alert_message_len
        CHECK (char_length(message) BETWEEN 1 AND 1000)
);

CREATE UNIQUE INDEX idx_agent_availability_alerts_open
    ON agent_availability_alerts (agent_id, alert_type)
    WHERE read_at IS NULL;

CREATE INDEX idx_agent_availability_alerts_creator
    ON agent_availability_alerts (creator_id, created_at DESC);

CREATE INDEX idx_agent_availability_alerts_unread
    ON agent_availability_alerts (creator_id, created_at DESC)
    WHERE read_at IS NULL;

CREATE TRIGGER agent_availability_alerts_set_updated_at
    BEFORE UPDATE ON agent_availability_alerts
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

COMMIT;
