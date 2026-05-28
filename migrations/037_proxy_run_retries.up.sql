BEGIN;

ALTER TABLE proxy_runs
    ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 3,
    ADD COLUMN next_retry_at TIMESTAMPTZ;

ALTER TABLE proxy_runs
    ADD CONSTRAINT proxy_runs_attempt_count_nonnegative CHECK (attempt_count >= 0),
    ADD CONSTRAINT proxy_runs_max_attempts_range CHECK (max_attempts BETWEEN 1 AND 10);

CREATE INDEX idx_proxy_runs_node_retry
    ON proxy_runs (registry_node_id, status, next_retry_at, created_at)
    WHERE status = 'pending';

COMMIT;
