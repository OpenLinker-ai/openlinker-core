-- Terminal side-effect outbox and accounting ledger.

-- name: CreateRunEffect :one
INSERT INTO run_effect_outbox (
    id, run_id, terminal_event_id, effect_type, target_key, metadata,
    available_at, max_attempts
) VALUES (
    $1, $2, $3, $4, $5, COALESCE($6, '{}'::jsonb),
    COALESCE($7, clock_timestamp()), $8
)
ON CONFLICT (run_id, effect_type, target_key) DO NOTHING
RETURNING *;

-- name: GetRunEffectByID :one
SELECT * FROM run_effect_outbox WHERE id = $1;

-- name: GetRunEffectByBusinessKey :one
SELECT *
FROM run_effect_outbox
WHERE run_id = $1
  AND effect_type = $2
  AND target_key = $3;

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
-- Cleanup path: the lease is already expired, so no worker remains entitled to
-- write it. This batch prevents at-limit processing rows from becoming stuck.
UPDATE run_effect_outbox
SET status = 'dead_letter',
    lease_owner = NULL,
    lease_expires_at = NULL,
    completed_at = NULL,
    dead_lettered_at = clock_timestamp(),
    last_error = COALESCE(last_error, 'processing lease expired at retry limit')
WHERE status = 'processing'
  AND lease_expires_at <= clock_timestamp()
  AND attempt_count >= max_attempts
RETURNING *;

-- name: NextRunEffectDue :one
SELECT MIN(candidate.next_due_at)::timestamptz AS next_due_at,
    clock_timestamp() AS database_now
FROM (
    (SELECT available_at AS next_due_at
     FROM run_effect_outbox
     WHERE status = 'pending' AND attempt_count < max_attempts
     ORDER BY available_at
     LIMIT 1)
    UNION ALL
    (SELECT lease_expires_at AS next_due_at
     FROM run_effect_outbox
     WHERE status = 'processing'
     ORDER BY lease_expires_at
     LIMIT 1)
) AS candidate;

-- name: DeadLetterRunEffect :one
-- In-lease permanent failure path. The claim generation is fenced by both
-- owner and attempt_count so a stale worker cannot dead-letter a newer claim.
UPDATE run_effect_outbox
SET status = 'dead_letter',
    lease_owner = NULL,
    lease_expires_at = NULL,
    completed_at = NULL,
    dead_lettered_at = clock_timestamp(),
    last_error = $4
WHERE id = $1
  AND status = 'processing'
  AND lease_owner = $2
  AND attempt_count = $3
RETURNING *;

-- name: MarkRunEffectSucceeded :one
UPDATE run_effect_outbox
SET status = 'succeeded',
    lease_owner = NULL,
    lease_expires_at = NULL,
    completed_at = clock_timestamp(),
    dead_lettered_at = NULL,
    last_error = NULL
WHERE id = $1
  AND status = 'processing'
  AND lease_owner = $2
  AND attempt_count = $3
RETURNING *;

-- name: RetryOrDeadLetterRunEffect :one
UPDATE run_effect_outbox
SET status = CASE
        WHEN attempt_count >= max_attempts THEN 'dead_letter'
        ELSE 'pending'
    END,
    lease_owner = NULL,
    lease_expires_at = NULL,
    available_at = clock_timestamp() + ($4::bigint * INTERVAL '1 millisecond'),
    completed_at = NULL,
    dead_lettered_at = CASE
        WHEN attempt_count >= max_attempts THEN clock_timestamp()
        ELSE NULL
    END,
    last_error = $5
WHERE id = $1
  AND status = 'processing'
  AND lease_owner = $2
  AND attempt_count = $3
  AND $4::bigint >= 0
RETURNING *;

-- name: ReplayRunEffect :one
WITH target AS (
    SELECT id
    FROM run_effect_outbox
    WHERE id = $1
      AND status = 'dead_letter'
    FOR UPDATE
), replay_audit AS (
    INSERT INTO run_effect_replays (
        effect_outbox_id, actor_type, actor_id, reason
    )
    SELECT target.id, $2, $3, $4
    FROM target
    RETURNING effect_outbox_id
)
UPDATE run_effect_outbox effect
SET status = 'pending',
    available_at = clock_timestamp(),
    lease_owner = NULL,
    lease_expires_at = NULL,
    attempt_count = 0,
    completed_at = NULL,
    dead_lettered_at = NULL,
    last_error = NULL
FROM replay_audit
WHERE effect.id = replay_audit.effect_outbox_id
RETURNING effect.*;

-- name: ListRunEffectReplaysByEffect :many
SELECT *
FROM run_effect_replays
WHERE effect_outbox_id = $1
ORDER BY replayed_at DESC, id DESC;

-- name: InsertRunAccountingLedger :one
INSERT INTO run_accounting_ledger (
    run_id, terminal_event_id, agent_id, success_delta, revenue_delta_cents
) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (run_id) DO NOTHING
RETURNING *;

-- name: GetRunAccountingLedger :one
SELECT * FROM run_accounting_ledger WHERE run_id = $1;
