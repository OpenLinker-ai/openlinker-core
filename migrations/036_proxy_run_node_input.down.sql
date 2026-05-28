BEGIN;

ALTER TABLE proxy_runs
    DROP COLUMN IF EXISTS node_input;

COMMIT;
