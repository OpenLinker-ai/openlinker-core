BEGIN;

ALTER TABLE workflow_runs
    DROP CONSTRAINT workflow_runs_status_valid;

ALTER TABLE workflow_runs
    ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 3,
    ADD COLUMN next_retry_at TIMESTAMPTZ,
    ADD COLUMN claimed_at TIMESTAMPTZ,
    ADD COLUMN last_worker_error TEXT,
    ADD CONSTRAINT workflow_runs_status_valid
        CHECK (status IN ('pending', 'running', 'success', 'failed')),
    ADD CONSTRAINT workflow_runs_attempt_count_nonnegative
        CHECK (attempt_count >= 0),
    ADD CONSTRAINT workflow_runs_max_attempts_valid
        CHECK (max_attempts BETWEEN 1 AND 10);

CREATE INDEX idx_workflow_runs_pending
    ON workflow_runs (status, next_retry_at, created_at)
    WHERE status IN ('pending', 'running');

COMMIT;
