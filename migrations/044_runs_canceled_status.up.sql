ALTER TABLE runs
    DROP CONSTRAINT IF EXISTS runs_status_valid;

ALTER TABLE runs
    ADD CONSTRAINT runs_status_valid
        CHECK (status IN ('running', 'success', 'failed', 'timeout', 'canceled'));
