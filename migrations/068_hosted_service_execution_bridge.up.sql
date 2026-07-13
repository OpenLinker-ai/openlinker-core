BEGIN;

CREATE TABLE hosted_service_executions (
    external_order_id UUID PRIMARY KEY,
    buyer_user_id UUID NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    seller_user_id UUID NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    target_type TEXT NOT NULL CHECK (target_type IN ('agent', 'workflow')),
    target_id UUID NOT NULL,
    input_fingerprint BYTEA NOT NULL CHECK (octet_length(input_fingerprint) = 32),
    trace_id TEXT NOT NULL CHECK (length(trace_id) BETWEEN 1 AND 200),
    execution_kind TEXT NULL CHECK (execution_kind IN ('run', 'workflow_run')),
    execution_id UUID NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK ((execution_kind IS NULL) = (execution_id IS NULL))
);

CREATE INDEX idx_hosted_service_executions_buyer
    ON hosted_service_executions (buyer_user_id, created_at DESC);

CREATE INDEX idx_hosted_service_executions_seller
    ON hosted_service_executions (seller_user_id, created_at DESC);

CREATE UNIQUE INDEX idx_hosted_service_executions_execution
    ON hosted_service_executions (execution_kind, execution_id)
    WHERE execution_id IS NOT NULL;

CREATE TRIGGER hosted_service_executions_set_updated_at
    BEFORE UPDATE ON hosted_service_executions
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

COMMIT;
