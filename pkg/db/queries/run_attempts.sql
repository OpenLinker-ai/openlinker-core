-- Reliable Runtime Attempt lifecycle.
--
-- Every write in this file is a transaction primitive, not a standalone
-- operation. Callers must update the matching runs summary in the same
-- transaction; event sequence updates must also insert the run_event before
-- commit. The deferred v2 invariants intentionally reject partial commits.

-- name: LockNextPendingRuntimeRun :one
SELECT id
FROM runs
WHERE status = 'running'
  AND dispatch_state = 'pending'
ORDER BY started_at ASC, id ASC
LIMIT 1
FOR UPDATE SKIP LOCKED;

-- name: LockNextDueRetryRuntimeRun :one
SELECT id
FROM runs
WHERE status = 'running'
  AND dispatch_state = 'retry_wait'
  AND next_attempt_at <= clock_timestamp()
ORDER BY next_attempt_at ASC, started_at ASC, id ASC
LIMIT 1
FOR UPDATE SKIP LOCKED;

-- name: CreateRunAttempt :one
INSERT INTO run_attempts (
    id, run_id, agent_id, offer_no, executor_type, lease_id, fencing_token,
    runtime_token_id, runtime_worker_id, runtime_session_id, node_id,
    offered_by_core_instance_id, attached_core_instance_id,
    offer_expires_at, lease_expires_at, attempt_deadline_at,
    slot_acquired_at, active_runtime_session_id
) VALUES (
    $1, $2, $3, $4, $5, $6, $7,
    $8, $9, $10, $11,
    $12, $13, $14, $15, $16,
    CASE WHEN $5::text = 'runtime' THEN clock_timestamp() ELSE NULL END,
    CASE WHEN $5::text = 'runtime' THEN $10::uuid ELSE NULL END
)
RETURNING *;

-- name: GetRunAttemptByID :one
SELECT * FROM run_attempts WHERE id = $1;

-- name: LockRunAttemptForResult :one
-- Result 事务已先锁 Run；这里锁定指定 Attempt，禁止反向取锁。
SELECT *
FROM run_attempts
WHERE run_id = $1
  AND id = $2
FOR UPDATE;

-- name: GetRunAttemptByLeaseID :one
SELECT * FROM run_attempts WHERE lease_id = $1;

-- name: GetRunAttemptByResultID :one
SELECT *
FROM run_attempts
WHERE run_id = $1
  AND result_id = $2;

-- name: GetRunAttemptByIdentity :one
SELECT *
FROM run_attempts
WHERE run_id = $1
  AND id = $2
  AND lease_id = $3
  AND fencing_token = $4;

-- name: GetActiveRunAttemptByRun :one
SELECT a.*
FROM run_attempts a
JOIN runs r ON r.id = a.run_id AND r.active_attempt_id = a.id
WHERE a.run_id = $1;

-- name: ListRunAttemptsByRun :many
SELECT *
FROM run_attempts
WHERE run_id = $1
ORDER BY offer_no ASC, id ASC;

-- name: GetRunAttemptTransportEvidence :one
SELECT attachment.transport,
       attachment.transport_reason,
       attachment.transport_changed_at
FROM runs run
JOIN run_attempts attempt
  ON attempt.run_id = run.id
 AND attempt.id = COALESCE(run.active_attempt_id, run.latest_attempt_id)
JOIN runtime_session_attachments attachment
  ON attachment.id = attempt.runtime_attachment_id
 AND attachment.runtime_session_id = attempt.runtime_session_id
WHERE run.id = $1
  AND attempt.executor_type = 'runtime'
  AND attempt.accepted_at IS NOT NULL
  AND attachment.transport IN ('websocket', 'long_poll');

-- name: AcceptRunAttempt :one
UPDATE run_attempts
SET attempt_no = $5,
    accepted_at = clock_timestamp(),
    last_renewed_at = clock_timestamp(),
    lease_expires_at = $6,
    attached_core_instance_id = $7
WHERE run_id = $1
  AND id = $2
  AND lease_id = $3
  AND fencing_token = $4
  AND accepted_at IS NULL
  AND finished_at IS NULL
  AND offer_expires_at >= clock_timestamp()
RETURNING *;

-- name: RenewRunAttempt :one
UPDATE run_attempts
SET last_renewed_at = clock_timestamp(),
    lease_expires_at = $5,
    attached_core_instance_id = $6
WHERE run_id = $1
  AND id = $2
  AND lease_id = $3
  AND fencing_token = $4
  AND accepted_at IS NOT NULL
  AND finished_at IS NULL
  AND lease_expires_at >= clock_timestamp()
RETURNING *;

-- name: AdvanceRunAttemptEventSequence :one
UPDATE run_attempts
SET last_client_event_seq = GREATEST(last_client_event_seq, $5)
WHERE run_id = $1
  AND id = $2
  AND lease_id = $3
  AND fencing_token = $4
  AND accepted_at IS NOT NULL
  AND finished_at IS NULL
  AND result_id IS NULL
RETURNING *;

-- name: FinishRunAttempt :one
UPDATE run_attempts
SET finished_at = clock_timestamp(),
    outcome = $5,
    result_id = $6,
    result_fingerprint = $7,
    result_classification = $8,
    final_client_event_seq = $9,
    error_code = $10,
    error_detail_redacted = $11,
    result_acknowledged_at = clock_timestamp()
WHERE run_id = $1
  AND id = $2
  AND lease_id = $3
  AND fencing_token = $4
  AND accepted_at IS NOT NULL
  AND finished_at IS NULL
  AND result_id IS NULL
  AND $6::uuid IS NOT NULL
  AND $7::bytea IS NOT NULL
  AND $8::text IS NOT NULL
  AND $9::bigint IS NOT NULL
RETURNING *;

-- name: AcknowledgeRunAttemptResult :one
UPDATE run_attempts
SET result_acknowledged_at = COALESCE(result_acknowledged_at, clock_timestamp())
WHERE run_id = $1
  AND id = $2
  AND result_id = $3
RETURNING *;
