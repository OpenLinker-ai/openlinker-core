UPDATE runs
SET status = 'failed',
    error_code = COALESCE(error_code, 'CANCELED'),
    error_message = COALESCE(error_message, 'run was canceled before migration rollback'),
    finished_at = COALESCE(finished_at, NOW())
WHERE status = 'canceled';

ALTER TABLE runs
    DROP CONSTRAINT IF EXISTS runs_status_valid;

ALTER TABLE runs
    ADD CONSTRAINT runs_status_valid
        CHECK (status IN ('running', 'success', 'failed', 'timeout'));
