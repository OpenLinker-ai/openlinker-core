BEGIN;

ALTER TABLE workflow_runs
    DROP CONSTRAINT workflow_runs_status_valid;

ALTER TABLE workflow_runs
    ADD CONSTRAINT workflow_runs_status_valid
        CHECK (status IN ('pending', 'running', 'paused', 'canceled', 'success', 'failed'));

COMMIT;
