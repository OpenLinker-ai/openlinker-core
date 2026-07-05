BEGIN;

DROP INDEX IF EXISTS idx_proxy_runs_node_retry;

ALTER TABLE proxy_runs
    DROP CONSTRAINT IF EXISTS proxy_runs_max_attempts_range,
    DROP CONSTRAINT IF EXISTS proxy_runs_attempt_count_nonnegative;

ALTER TABLE proxy_runs
    DROP COLUMN IF EXISTS next_retry_at,
    DROP COLUMN IF EXISTS max_attempts,
    DROP COLUMN IF EXISTS attempt_count;

COMMIT;
