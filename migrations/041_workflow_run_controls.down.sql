BEGIN;

UPDATE workflow_runs
SET status = 'pending',
    claimed_at = NULL,
    next_retry_at = NOW(),
    updated_at = NOW()
WHERE status = 'paused';

UPDATE workflow_runs
SET status = 'failed',
    error_message = COALESCE(error_message, 'workflow run was canceled'),
    finished_at = COALESCE(finished_at, NOW()),
    claimed_at = NULL,
    next_retry_at = NULL,
    updated_at = NOW()
WHERE status = 'canceled';

ALTER TABLE workflow_runs
    DROP CONSTRAINT workflow_runs_status_valid;

ALTER TABLE workflow_runs
    ADD CONSTRAINT workflow_runs_status_valid
        CHECK (status IN ('pending', 'running', 'success', 'failed'));

COMMIT;
