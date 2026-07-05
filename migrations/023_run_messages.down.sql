BEGIN;

DROP INDEX IF EXISTS idx_run_messages_event_sequence;
DROP INDEX IF EXISTS idx_run_messages_run;
DROP TABLE IF EXISTS run_messages;

COMMIT;
