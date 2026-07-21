-- 082_event_wake_notifications.down.sql
-- Removes advisory notification objects only. No business data is changed.

BEGIN;

DROP TRIGGER IF EXISTS run_effect_outbox_event_wake_due_update ON run_effect_outbox;
DROP TRIGGER IF EXISTS run_effect_outbox_event_wake_insert ON run_effect_outbox;
DROP TRIGGER IF EXISTS runtime_signal_outbox_event_wake_due_update ON runtime_signal_outbox;
DROP TRIGGER IF EXISTS runtime_signal_outbox_event_wake_insert ON runtime_signal_outbox;
DROP TRIGGER IF EXISTS runs_event_wake_state_update ON runs;
DROP TRIGGER IF EXISTS runs_event_wake_insert ON runs;
DROP TRIGGER IF EXISTS run_events_event_wake_insert ON run_events;

DROP FUNCTION IF EXISTS emit_event_wake_notification();

COMMIT;
