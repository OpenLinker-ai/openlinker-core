-- Transactional signal outbox. Redis/local buses are delivery accelerators only.

-- name: CreateRuntimeSignal :one
INSERT INTO runtime_signal_outbox (
    event_type, agent_id, run_id, payload, available_at
) VALUES ($1, $2, $3, $4, COALESCE($5, clock_timestamp()))
RETURNING *;

-- name: GetRuntimeSignalByID :one
SELECT * FROM runtime_signal_outbox WHERE id = $1;

-- name: ClaimRuntimeSignals :many
WITH candidates AS (
    SELECT id
    FROM runtime_signal_outbox
    WHERE (
        status = 'pending'
        AND available_at <= clock_timestamp()
    ) OR (
        status = 'processing'
        AND lease_expires_at <= clock_timestamp()
    )
    ORDER BY COALESCE(lease_expires_at, available_at), created_at, id
    LIMIT $3
    FOR UPDATE SKIP LOCKED
)
UPDATE runtime_signal_outbox s
SET status = 'processing',
    lease_owner = $1,
    lease_expires_at = clock_timestamp() + ($2::bigint * INTERVAL '1 millisecond'),
    attempt_count = attempt_count + 1
FROM candidates
WHERE s.id = candidates.id
RETURNING s.*;

-- name: MarkRuntimeSignalPublished :one
UPDATE runtime_signal_outbox
SET status = 'published',
    lease_owner = NULL,
    lease_expires_at = NULL,
    published_at = clock_timestamp(),
    last_error = NULL
WHERE id = $1
  AND status = 'processing'
  AND lease_owner = $2
RETURNING *;

-- name: RetryRuntimeSignal :one
UPDATE runtime_signal_outbox
SET status = 'pending',
    lease_owner = NULL,
    lease_expires_at = NULL,
    available_at = clock_timestamp() + ($3::bigint * INTERVAL '1 millisecond'),
    last_error = $4
WHERE id = $1
  AND status = 'processing'
  AND lease_owner = $2
RETURNING *;

-- name: CountPendingRuntimeSignals :one
SELECT COUNT(*)::int
FROM runtime_signal_outbox
WHERE status IN ('pending', 'processing');

-- name: NextRuntimeSignalDue :one
SELECT MIN(candidate.next_due_at)::timestamptz AS next_due_at,
    clock_timestamp() AS database_now
FROM (
    (SELECT available_at AS next_due_at
     FROM runtime_signal_outbox
     WHERE status = 'pending'
     ORDER BY available_at
     LIMIT 1)
    UNION ALL
    (SELECT lease_expires_at AS next_due_at
     FROM runtime_signal_outbox
     WHERE status = 'processing'
     ORDER BY lease_expires_at
     LIMIT 1)
) AS candidate;
