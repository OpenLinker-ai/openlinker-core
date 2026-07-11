-- Reliable runtime v2 cancellation evidence.
--
-- Writes must update the runs cancellation summary in the same transaction.
-- The initial cancellation request belongs to the canceled terminal transaction
-- (terminal Event + ledger). Later ACK states only advance stop evidence and the
-- Run cancellation summary; they never rewrite the public terminal facts.

-- name: CreateRunCancellation :one
INSERT INTO run_cancellations (
    id, run_id, target_attempt_id, requested_by_type, requested_by_id, reason
) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (run_id) DO UPDATE
SET updated_at = run_cancellations.updated_at
RETURNING *;

-- name: GetRunCancellationByID :one
SELECT * FROM run_cancellations WHERE id = $1;

-- name: GetRunCancellationByRun :one
SELECT * FROM run_cancellations WHERE run_id = $1;

-- name: AdvanceRunCancellation :one
UPDATE run_cancellations
SET state = $3,
    delivered_at = CASE
        WHEN target_attempt_id IS NOT NULL
             AND $3 IN ('delivered', 'stopping', 'stopped', 'unsupported')
            THEN COALESCE(delivered_at, clock_timestamp())
        ELSE delivered_at
    END,
    stopping_at = CASE
        WHEN target_attempt_id IS NOT NULL AND $3 IN ('stopping', 'stopped')
            THEN COALESCE(stopping_at, clock_timestamp())
        ELSE stopping_at
    END,
    stopped_at = CASE
        WHEN $3 = 'stopped' THEN COALESCE(stopped_at, clock_timestamp())
        ELSE stopped_at
    END,
    acknowledged_at = CASE
        WHEN target_attempt_id IS NOT NULL
             AND $3 IN ('stopping', 'stopped', 'unsupported')
            THEN COALESCE(acknowledged_at, clock_timestamp())
        ELSE acknowledged_at
    END,
    error_code = $4,
    updated_at = clock_timestamp()
WHERE id = $1
  AND run_id = $2
RETURNING *;

-- name: ListUnsettledRunCancellations :many
SELECT *
FROM run_cancellations
WHERE state IN ('requested', 'delivered', 'stopping', 'unconfirmed')
ORDER BY updated_at ASC, id ASC
LIMIT $1;

-- name: LockUnsettledRunCancellations :many
SELECT *
FROM run_cancellations
WHERE state IN ('requested', 'delivered', 'stopping', 'unconfirmed')
ORDER BY updated_at ASC, id ASC
LIMIT $1
FOR UPDATE SKIP LOCKED;
