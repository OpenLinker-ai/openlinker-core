-- Core-owned Runtime HTTP/MCP Attempt lifecycle.
--
-- The caller must execute these statements in one READ COMMITTED transaction
-- in this order: LockCoreRunForExecution -> CreateRunAttempt ->
-- AcceptRunAttempt -> MirrorCoreRunAcceptedAttempt. The Run row is the sole
-- per-Run linearization lock; external I/O begins only after commit.

-- name: LockCoreRunForExecution :one
WITH database_clock AS MATERIALIZED (
    SELECT clock_timestamp() AS database_now
)
SELECT r.id, r.user_id, r.agent_id, r.status, r.dispatch_state,
       r.connection_mode_snapshot, r.endpoint_idempotency_snapshot,
       r.offer_count, r.max_offer_count, r.attempt_count, r.max_attempts,
       r.fencing_token, r.dispatch_deadline_at, r.run_deadline_at,
       r.cancel_request_id, c.database_now
FROM runs r
CROSS JOIN database_clock c
WHERE r.id = sqlc.arg(run_id)
  AND r.agent_id = sqlc.arg(agent_id)
  AND r.runtime_contract_id = 'openlinker.runtime.v2'
  AND r.status = 'running'
  AND r.dispatch_state IN ('pending', 'retry_wait')
  AND (r.dispatch_state <> 'retry_wait' OR r.next_attempt_at <= c.database_now)
  AND r.active_attempt_id IS NULL
  AND r.cancel_request_id IS NULL
  AND r.connection_mode_snapshot IN ('direct_http', 'mcp_server')
  AND r.offer_count < r.max_offer_count
  AND r.attempt_count < r.max_attempts
  AND r.dispatch_deadline_at > c.database_now
  AND r.run_deadline_at > c.database_now
FOR UPDATE OF r;

-- name: MirrorCoreRunAcceptedAttempt :one
UPDATE runs r
SET dispatch_state = 'executing',
    next_attempt_at = NULL,
    offer_count = a.offer_no,
    attempt_count = a.attempt_no,
    latest_attempt_id = a.id,
    active_attempt_id = a.id,
    lease_id = a.lease_id,
    fencing_token = a.fencing_token,
    executor_type = a.executor_type,
    active_core_instance_id = a.attached_core_instance_id,
    runtime_node_id = NULL,
    runtime_worker_id = NULL,
    runtime_session_id = NULL,
    lease_token_id = NULL,
    lease_offered_at = a.offered_at,
    lease_accepted_at = a.accepted_at,
    lease_expires_at = a.lease_expires_at,
    attempt_deadline_at = a.attempt_deadline_at,
    error_code = NULL,
    error_message = NULL
FROM run_attempts a
WHERE r.id = sqlc.arg(run_id)
  AND a.run_id = r.id
  AND a.id = sqlc.arg(attempt_id)
  AND a.lease_id = sqlc.arg(lease_id)
  AND a.fencing_token = sqlc.arg(fencing_token)
  AND a.executor_type = sqlc.arg(executor_type)
  AND a.executor_type IN ('core_http', 'core_mcp')
  AND a.runtime_token_id IS NULL
  AND a.runtime_worker_id IS NULL
  AND a.runtime_session_id IS NULL
  AND a.node_id IS NULL
  AND a.accepted_at IS NOT NULL
  AND a.attempt_no IS NOT NULL
  AND a.finished_at IS NULL
  AND a.outcome IS NULL
  AND a.result_id IS NULL
  AND r.runtime_contract_id = 'openlinker.runtime.v2'
  AND r.status = 'running'
  AND r.dispatch_state IN ('pending', 'retry_wait')
  AND r.active_attempt_id IS NULL
  AND r.cancel_request_id IS NULL
  AND r.connection_mode_snapshot = CASE a.executor_type
      WHEN 'core_http' THEN 'direct_http'
      WHEN 'core_mcp' THEN 'mcp_server'
  END
  AND a.offer_no = r.offer_count + 1
  AND a.attempt_no = r.attempt_count + 1
  AND a.fencing_token = r.fencing_token + 1
RETURNING r.id, r.status, r.dispatch_state, r.offer_count, r.attempt_count,
          r.latest_attempt_id, r.active_attempt_id, r.lease_id,
          r.fencing_token, r.executor_type, r.active_core_instance_id,
          r.lease_offered_at, r.lease_accepted_at, r.lease_expires_at,
          r.attempt_deadline_at;

-- name: CoreAttemptCancellationRequested :one
SELECT EXISTS (
    SELECT 1
    FROM runs r
    JOIN run_cancellations c
      ON c.run_id = r.id
     AND c.id = r.cancel_request_id
    JOIN run_attempts a
      ON a.run_id = r.id
     AND a.id = c.target_attempt_id
    WHERE r.id = sqlc.arg(run_id)
      AND r.runtime_contract_id = 'openlinker.runtime.v2'
      AND r.status = 'canceled'
      AND r.dispatch_state = 'terminal'
      AND c.target_attempt_id = sqlc.arg(attempt_id)
      AND c.state IN ('requested', 'delivered', 'stopping')
      AND a.lease_id = sqlc.arg(lease_id)
      AND a.fencing_token = sqlc.arg(fencing_token)
      AND a.executor_type IN ('core_http', 'core_mcp')
      AND a.finished_at IS NULL
      AND a.outcome IS NULL
      AND a.result_id IS NULL
);

-- name: ListRequestedCoreAttemptCancellations :many
-- One Core-scoped fallback query replaces one query per active HTTP/MCP
-- Attempt. The returned immutable identity is rechecked against the local
-- registry before cancellation, preserving the lease/fencing boundary.
SELECT r.id AS run_id,
       a.id AS attempt_id,
       a.lease_id,
       a.fencing_token
FROM runs r
JOIN run_cancellations c
  ON c.run_id = r.id
 AND c.id = r.cancel_request_id
JOIN run_attempts a
  ON a.run_id = r.id
 AND a.id = c.target_attempt_id
WHERE r.id = ANY($1::uuid[])
  AND r.runtime_contract_id = 'openlinker.runtime.v2'
  AND r.status = 'canceled'
  AND r.dispatch_state = 'terminal'
  AND c.state IN ('requested', 'delivered', 'stopping')
  AND a.executor_type IN ('core_http', 'core_mcp')
  AND a.finished_at IS NULL
  AND a.outcome IS NULL
  AND a.result_id IS NULL;
