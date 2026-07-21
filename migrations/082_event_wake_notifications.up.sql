-- 082_event_wake_notifications.up.sql
--
-- Transactional, advisory wake hints for Core-owned Run and durable-work
-- tables. PostgreSQL remains authoritative: consumers must re-read state and
-- retain the existing claim, lease, fencing, retry, and terminal-state rules.

BEGIN;

CREATE FUNCTION emit_event_wake_notification()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    channel_name TEXT;
    topic_name TEXT;
    resource_id TEXT;
    wake_generation BIGINT;
    payload TEXT;
BEGIN
    IF TG_NARGS <> 2 THEN
        RAISE EXCEPTION 'event wake trigger arguments are invalid';
    END IF;

    channel_name := TG_ARGV[0];
    topic_name := TG_ARGV[1];

    -- Keep the producer table/channel/topic mapping closed. Direct field
    -- access intentionally avoids converting a potentially large Run or
    -- outbox row (including input/output/payload) to JSON just to read its ID.
    IF TG_TABLE_NAME = 'run_events'
       AND channel_name = 'openlinker_run_v1'
       AND topic_name = 'run.changed' THEN
        resource_id := NEW.run_id::TEXT;
        wake_generation := NEW.sequence::BIGINT;
    ELSIF TG_TABLE_NAME = 'runs'
       AND channel_name = 'openlinker_run_v1'
       AND topic_name = 'run.changed' THEN
        resource_id := NEW.id::TEXT;
        wake_generation := floor(
            extract(epoch FROM clock_timestamp()) * 1000000
        )::BIGINT;
    ELSIF TG_TABLE_NAME = 'runtime_signal_outbox'
       AND channel_name = 'openlinker_work_v1'
       AND topic_name = 'work.runtime_signal.available' THEN
        resource_id := NEW.id::TEXT;
        wake_generation := NEW.attempt_count::BIGINT;
    ELSIF TG_TABLE_NAME = 'run_effect_outbox'
       AND channel_name = 'openlinker_work_v1'
       AND topic_name = 'work.run_effect.available' THEN
        resource_id := NEW.id::TEXT;
        wake_generation := NEW.attempt_count::BIGINT;
    ELSE
        RAISE EXCEPTION 'event wake channel/topic is not allowlisted';
    END IF;

    IF resource_id IS NULL OR resource_id = '' OR octet_length(resource_id) > 200 THEN
        RAISE EXCEPTION 'event wake resource identifier is invalid';
    END IF;
    IF wake_generation < 0 THEN
        RAISE EXCEPTION 'event wake generation is invalid';
    END IF;

    payload := jsonb_build_object(
        'version', 1,
        'topic', topic_name,
        'resource_id', resource_id,
        'generation', wake_generation,
        'produced_at', to_char(
            clock_timestamp() AT TIME ZONE 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
        )
    )::TEXT;
    IF octet_length(payload) > 1024 THEN
        RAISE EXCEPTION 'event wake payload is too large';
    END IF;

    PERFORM pg_notify(channel_name, payload);
    RETURN NEW;
END
$$;

CREATE TRIGGER run_events_event_wake_insert
AFTER INSERT ON run_events
FOR EACH ROW
EXECUTE FUNCTION emit_event_wake_notification(
    'openlinker_run_v1', 'run.changed'
);

CREATE TRIGGER runs_event_wake_insert
AFTER INSERT ON runs
FOR EACH ROW
EXECUTE FUNCTION emit_event_wake_notification(
    'openlinker_run_v1', 'run.changed'
);

CREATE TRIGGER runs_event_wake_state_update
AFTER UPDATE ON runs
FOR EACH ROW
WHEN (
    OLD.status IS DISTINCT FROM NEW.status
    OR OLD.dispatch_state IS DISTINCT FROM NEW.dispatch_state
    OR OLD.active_attempt_id IS DISTINCT FROM NEW.active_attempt_id
    OR OLD.attempt_count IS DISTINCT FROM NEW.attempt_count
    OR OLD.cancel_state IS DISTINCT FROM NEW.cancel_state
    OR OLD.result_id IS DISTINCT FROM NEW.result_id
    OR OLD.terminal_event_id IS DISTINCT FROM NEW.terminal_event_id
)
EXECUTE FUNCTION emit_event_wake_notification(
    'openlinker_run_v1', 'run.changed'
);

CREATE TRIGGER runtime_signal_outbox_event_wake_insert
AFTER INSERT ON runtime_signal_outbox
FOR EACH ROW
EXECUTE FUNCTION emit_event_wake_notification(
    'openlinker_work_v1', 'work.runtime_signal.available'
);

CREATE TRIGGER runtime_signal_outbox_event_wake_due_update
AFTER UPDATE ON runtime_signal_outbox
FOR EACH ROW
WHEN (
    (NEW.status = 'pending' AND OLD.status IS DISTINCT FROM NEW.status)
    OR (
        NEW.status = 'pending'
        AND NEW.available_at < OLD.available_at
    )
    OR (
        NEW.status = 'processing'
        AND OLD.status = 'processing'
        AND NEW.lease_expires_at IS NOT NULL
        AND (
            OLD.lease_expires_at IS NULL
            OR NEW.lease_expires_at < OLD.lease_expires_at
        )
    )
)
EXECUTE FUNCTION emit_event_wake_notification(
    'openlinker_work_v1', 'work.runtime_signal.available'
);

CREATE TRIGGER run_effect_outbox_event_wake_insert
AFTER INSERT ON run_effect_outbox
FOR EACH ROW
EXECUTE FUNCTION emit_event_wake_notification(
    'openlinker_work_v1', 'work.run_effect.available'
);

CREATE TRIGGER run_effect_outbox_event_wake_due_update
AFTER UPDATE ON run_effect_outbox
FOR EACH ROW
WHEN (
    (NEW.status = 'pending' AND OLD.status IS DISTINCT FROM NEW.status)
    OR (
        NEW.status = 'pending'
        AND NEW.available_at < OLD.available_at
    )
    OR (
        NEW.status = 'processing'
        AND OLD.status = 'processing'
        AND NEW.lease_expires_at IS NOT NULL
        AND (
            OLD.lease_expires_at IS NULL
            OR NEW.lease_expires_at < OLD.lease_expires_at
        )
    )
)
EXECUTE FUNCTION emit_event_wake_notification(
    'openlinker_work_v1', 'work.run_effect.available'
);

COMMIT;
