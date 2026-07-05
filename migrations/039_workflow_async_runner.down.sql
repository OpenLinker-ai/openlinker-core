BEGIN;

UPDATE workflow_runs
SET status = 'failed',
    error_message = COALESCE(error_message, 'workflow async run cannot be downgraded'),
    finished_at = COALESCE(finished_at, NOW())
WHERE status = 'pending';

DROP INDEX IF EXISTS idx_workflow_runs_pending;

ALTER TABLE workflow_runs
    DROP CONSTRAINT IF EXISTS workflow_runs_max_attempts_valid,
    DROP CONSTRAINT IF EXISTS workflow_runs_attempt_count_nonnegative,
    DROP CONSTRAINT workflow_runs_status_valid,
    DROP COLUMN IF EXISTS last_worker_error,
    DROP COLUMN IF EXISTS claimed_at,
    DROP COLUMN IF EXISTS next_retry_at,
    DROP COLUMN IF EXISTS max_attempts,
    DROP COLUMN IF EXISTS attempt_count,
    ADD CONSTRAINT workflow_runs_status_valid
        CHECK (status IN ('running', 'success', 'failed'));

COMMIT;
