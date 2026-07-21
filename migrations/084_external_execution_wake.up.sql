-- 084_external_execution_wake.up.sql
-- Advisory wake hints for Core-owned external cancellation reconciliation.
-- PostgreSQL rows remain authoritative; payloads contain no execution data.

BEGIN;

CREATE FUNCTION emit_external_execution_wake_notification()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    resource_id TEXT;
    payload TEXT;
BEGIN
    CASE TG_TABLE_NAME
        WHEN 'external_execution_cancellations' THEN
            resource_id := NEW.external_request_id::TEXT;
        WHEN 'external_executions' THEN
            resource_id := NEW.external_request_id::TEXT;
        WHEN 'run_cancellations' THEN
            resource_id := NEW.run_id::TEXT;
        WHEN 'workflow_run_cancellations' THEN
            resource_id := NEW.workflow_run_id::TEXT;
        ELSE
            RAISE EXCEPTION 'external execution wake table is not allowlisted';
    END CASE;

    IF resource_id IS NULL OR resource_id = '' OR octet_length(resource_id) > 200 THEN
        RAISE EXCEPTION 'external execution wake resource identifier is invalid';
    END IF;
    payload := jsonb_build_object(
        'version', 1,
        'topic', 'external_execution.changed',
        'resource_id', resource_id,
        'generation', floor(extract(epoch FROM clock_timestamp()) * 1000000)::BIGINT,
        'produced_at', to_char(
            clock_timestamp() AT TIME ZONE 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
        )
    )::TEXT;
    IF octet_length(payload) > 1024 THEN
        RAISE EXCEPTION 'external execution wake payload is too large';
    END IF;
    PERFORM pg_notify('openlinker_external_v1', payload);
    RETURN NEW;
END
$$;

CREATE TRIGGER external_execution_cancellations_wake_insert
AFTER INSERT ON external_execution_cancellations
FOR EACH ROW EXECUTE FUNCTION emit_external_execution_wake_notification();

CREATE TRIGGER external_execution_cancellations_wake_update
AFTER UPDATE ON external_execution_cancellations
FOR EACH ROW
WHEN (
    OLD.state IS DISTINCT FROM NEW.state
    OR OLD.execution_kind_snapshot IS DISTINCT FROM NEW.execution_kind_snapshot
    OR OLD.execution_id_snapshot IS DISTINCT FROM NEW.execution_id_snapshot
)
EXECUTE FUNCTION emit_external_execution_wake_notification();

CREATE TRIGGER external_executions_cancellation_wake_update
AFTER UPDATE ON external_executions
FOR EACH ROW
WHEN (
    OLD.start_state IS DISTINCT FROM NEW.start_state
    OR OLD.execution_kind IS DISTINCT FROM NEW.execution_kind
    OR OLD.execution_id IS DISTINCT FROM NEW.execution_id
    OR OLD.downstream_idempotency_key_hash IS DISTINCT FROM NEW.downstream_idempotency_key_hash
    OR OLD.downstream_creation_fingerprint IS DISTINCT FROM NEW.downstream_creation_fingerprint
)
EXECUTE FUNCTION emit_external_execution_wake_notification();

CREATE TRIGGER run_cancellations_external_execution_wake_insert
AFTER INSERT ON run_cancellations
FOR EACH ROW EXECUTE FUNCTION emit_external_execution_wake_notification();

CREATE TRIGGER run_cancellations_external_execution_wake_update
AFTER UPDATE ON run_cancellations
FOR EACH ROW
WHEN (OLD.state IS DISTINCT FROM NEW.state)
EXECUTE FUNCTION emit_external_execution_wake_notification();

CREATE TRIGGER workflow_run_cancellations_external_execution_wake_insert
AFTER INSERT ON workflow_run_cancellations
FOR EACH ROW EXECUTE FUNCTION emit_external_execution_wake_notification();

CREATE TRIGGER workflow_run_cancellations_external_execution_wake_update
AFTER UPDATE ON workflow_run_cancellations
FOR EACH ROW
WHEN (OLD.state IS DISTINCT FROM NEW.state)
EXECUTE FUNCTION emit_external_execution_wake_notification();

COMMIT;
