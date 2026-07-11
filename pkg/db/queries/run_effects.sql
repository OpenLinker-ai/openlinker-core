-- Terminal side-effect outbox and accounting ledger.

-- name: CreateRunEffect :one
INSERT INTO run_effect_outbox (
    id, run_id, terminal_event_id, effect_type, target_key, metadata,
    available_at, max_attempts
) VALUES (
    $1, $2, $3, $4, $5, COALESCE($6, '{}'::jsonb),
    COALESCE($7, clock_timestamp()), $8
)
ON CONFLICT (run_id, effect_type, target_key) DO UPDATE
SET run_id = run_effect_outbox.run_id
RETURNING *;

-- name: GetRunEffectByID :one
SELECT * FROM run_effect_outbox WHERE id = $1;

-- name: ListRunEffectsByRun :many
SELECT *
FROM run_effect_outbox
WHERE run_id = $1
ORDER BY created_at ASC, id ASC;

-- name: ClaimRunEffects :many
WITH candidates AS (
    SELECT id
    FROM run_effect_outbox
    WHERE attempt_count < max_attempts
      AND (
          (status = 'pending' AND available_at <= clock_timestamp())
          OR (status = 'processing' AND lease_expires_at <= clock_timestamp())
      )
    ORDER BY COALESCE(lease_expires_at, available_at), created_at, id
    LIMIT $3
    FOR UPDATE SKIP LOCKED
)
UPDATE run_effect_outbox e
SET status = 'processing',
    lease_owner = $1,
    lease_expires_at = clock_timestamp() + ($2::bigint * INTERVAL '1 millisecond'),
    attempt_count = attempt_count + 1
FROM candidates
WHERE e.id = candidates.id
RETURNING e.*;

-- name: DeadLetterExpiredRunEffectsAtLimit :many
UPDATE run_effect_outbox
SET status = 'dead_letter',
    lease_owner = NULL,
    lease_expires_at = NULL,
    last_error = COALESCE(last_error, 'processing lease expired at retry limit')
WHERE status = 'processing'
  AND lease_expires_at <= clock_timestamp()
  AND attempt_count >= max_attempts
RETURNING *;

-- name: MarkRunEffectSucceeded :one
UPDATE run_effect_outbox
SET status = 'succeeded',
    lease_owner = NULL,
    lease_expires_at = NULL,
    completed_at = clock_timestamp(),
    last_error = NULL
WHERE id = $1
  AND status = 'processing'
  AND lease_owner = $2
RETURNING *;

-- name: RetryOrDeadLetterRunEffect :one
UPDATE run_effect_outbox
SET status = CASE
        WHEN attempt_count >= max_attempts THEN 'dead_letter'
        ELSE 'pending'
    END,
    lease_owner = NULL,
    lease_expires_at = NULL,
    available_at = $3,
    last_error = $4
WHERE id = $1
  AND status = 'processing'
  AND lease_owner = $2
RETURNING *;

-- name: ReplayRunEffect :one
UPDATE run_effect_outbox
SET status = 'pending',
    available_at = clock_timestamp(),
    attempt_count = 0,
    completed_at = NULL,
    last_error = NULL
WHERE id = $1
  AND status = 'dead_letter'
RETURNING *;

-- name: InsertRunAccountingLedger :one
INSERT INTO run_accounting_ledger (
    run_id, terminal_event_id, agent_id, success_delta, revenue_delta_cents
) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (run_id) DO NOTHING
RETURNING *;

-- name: GetRunAccountingLedger :one
SELECT * FROM run_accounting_ledger WHERE run_id = $1;
