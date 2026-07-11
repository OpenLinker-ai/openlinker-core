-- Reliable runtime v2 cancellation coordinator primitives.
--
-- Every mutation transaction locks capacity owner Session -> Node when it may
-- release a slot, then Run -> Attempt -> Cancellation. These queries never
-- acquire those locks in reverse order.

-- name: LockNextRuntimeCancellationCommandRun :one
SELECT r.id AS run_id,
       r.agent_id,
       c.id AS cancellation_id,
       c.target_attempt_id,
       clock_timestamp() AS database_now
FROM runs r
JOIN run_cancellations c
  ON c.run_id = r.id
 AND c.id = r.cancel_request_id
JOIN run_attempts a
  ON a.run_id = r.id
 AND a.id = c.target_attempt_id
WHERE r.runtime_contract_id = 'openlinker.runtime.v2'
  AND r.status = 'canceled'
  AND r.dispatch_state = 'terminal'
  AND r.cancel_state = c.state
  AND c.state IN ('requested', 'delivered', 'stopping')
  AND a.executor_type = 'agent_node'
  AND a.finished_at IS NULL
  AND a.run_id = r.id
  AND a.agent_id = sqlc.arg(agent_id)
  AND a.node_id = sqlc.arg(node_id)
  AND a.runtime_token_id = sqlc.arg(credential_id)
  AND a.runtime_worker_id = sqlc.arg(worker_id)
  AND a.runtime_session_id = sqlc.arg(runtime_session_id)
  AND c.requested_at
      + (sqlc.arg(command_deadline_ms)::bigint * INTERVAL '1 millisecond')
      > clock_timestamp()
ORDER BY c.updated_at ASC, c.id ASC
LIMIT 1
FOR UPDATE OF r SKIP LOCKED;

-- name: FindNextDueRuntimeV2Cancellation :one
SELECT r.id AS run_id,
       r.agent_id,
       c.id AS cancellation_id,
       c.target_attempt_id,
       a.active_runtime_session_id AS runtime_session_id,
       a.node_id
FROM runs r
JOIN run_cancellations c
  ON c.run_id = r.id
 AND c.id = r.cancel_request_id
JOIN run_attempts a
  ON a.run_id = r.id
 AND a.id = c.target_attempt_id
WHERE r.runtime_contract_id = 'openlinker.runtime.v2'
  AND r.status = 'canceled'
  AND r.dispatch_state = 'terminal'
  AND r.cancel_state = c.state
  AND c.state IN ('requested', 'delivered', 'stopping', 'unsupported', 'failed')
  AND c.requested_at
      + (sqlc.arg(command_deadline_ms)::bigint * INTERVAL '1 millisecond')
      <= clock_timestamp()
  AND a.executor_type = 'agent_node'
  AND a.finished_at IS NULL
  AND a.outcome IS NULL
  AND a.slot_acquired_at IS NOT NULL
  AND a.slot_released_at IS NULL
  AND a.active_runtime_session_id IS NOT NULL
  AND a.node_id IS NOT NULL
ORDER BY c.requested_at ASC, c.id ASC
LIMIT 1;

-- name: LockRuntimeSessionForCancellationReap :one
SELECT runtime_session_id
FROM runtime_sessions
WHERE runtime_session_id = sqlc.arg(runtime_session_id)
FOR UPDATE;

-- name: LockRuntimeNodeForCancellationReap :one
SELECT node_id
FROM runtime_nodes
WHERE node_id = sqlc.arg(node_id)
FOR UPDATE;

-- name: LockDueRuntimeV2CancellationRun :one
SELECT r.id AS run_id,
       r.agent_id,
       c.id AS cancellation_id,
       c.target_attempt_id,
       clock_timestamp() AS database_now
FROM runs r
JOIN run_cancellations c
  ON c.run_id = r.id
 AND c.id = r.cancel_request_id
JOIN run_attempts a
  ON a.run_id = r.id
 AND a.id = c.target_attempt_id
WHERE r.id = sqlc.arg(run_id)
  AND c.id = sqlc.arg(cancellation_id)
  AND a.id = sqlc.arg(target_attempt_id)
  AND a.active_runtime_session_id = sqlc.arg(runtime_session_id)
  AND a.node_id = sqlc.arg(node_id)
  AND r.runtime_contract_id = 'openlinker.runtime.v2'
  AND r.status = 'canceled'
  AND r.dispatch_state = 'terminal'
  AND r.cancel_state = c.state
  AND c.state IN ('requested', 'delivered', 'stopping', 'unsupported', 'failed')
  AND c.requested_at
      + (sqlc.arg(command_deadline_ms)::bigint * INTERVAL '1 millisecond')
      <= clock_timestamp()
  AND a.executor_type = 'agent_node'
  AND a.finished_at IS NULL
  AND a.outcome IS NULL
  AND a.slot_acquired_at IS NOT NULL
  AND a.slot_released_at IS NULL
ORDER BY c.requested_at ASC, c.id ASC
LIMIT 1
FOR UPDATE OF r;

-- name: LockRunCancellationForMutation :one
SELECT *
FROM run_cancellations
WHERE run_id = sqlc.arg(run_id)
  AND id = sqlc.arg(cancellation_id)
FOR UPDATE;

-- name: AdvanceRuntimeV2RunCancellation :one
UPDATE run_cancellations
SET state = sqlc.arg(next_state),
    delivered_at = CASE
        WHEN target_attempt_id IS NOT NULL
             AND sqlc.arg(next_state) IN ('delivered', 'stopping', 'stopped', 'unsupported', 'failed')
            THEN COALESCE(delivered_at, clock_timestamp())
        ELSE delivered_at
    END,
    stopping_at = CASE
        WHEN target_attempt_id IS NOT NULL
             AND sqlc.arg(next_state) IN ('stopping', 'stopped')
            THEN COALESCE(stopping_at, clock_timestamp())
        ELSE stopping_at
    END,
    stopped_at = CASE
        WHEN sqlc.arg(next_state) = 'stopped'
            THEN COALESCE(stopped_at, clock_timestamp())
        ELSE stopped_at
    END,
    acknowledged_at = CASE
        WHEN target_attempt_id IS NOT NULL
             AND sqlc.arg(next_state) IN ('stopping', 'stopped', 'unsupported', 'failed')
            THEN COALESCE(acknowledged_at, clock_timestamp())
        ELSE acknowledged_at
    END,
    error_code = sqlc.narg(error_code),
    updated_at = clock_timestamp()
WHERE run_id = sqlc.arg(run_id)
  AND id = sqlc.arg(cancellation_id)
  AND state = sqlc.arg(expected_state)
  AND sqlc.arg(next_state) IN ('delivered', 'stopping', 'stopped', 'unsupported', 'failed', 'unconfirmed')
RETURNING *;

-- name: FinalizeRuntimeV2RunCancellation :one
UPDATE runs r
SET status = 'canceled',
    dispatch_state = 'terminal',
    output = NULL,
    error_code = 'CANCELED',
    error_message = COALESCE(c.reason, 'Run canceled'),
    duration_ms = sqlc.arg(duration_ms),
    finished_at = clock_timestamp(),
    next_attempt_at = NULL,
    active_attempt_id = NULL,
    lease_id = NULL,
    executor_type = NULL,
    active_core_instance_id = NULL,
    runtime_node_id = NULL,
    runtime_worker_id = NULL,
    runtime_session_id = NULL,
    lease_token_id = NULL,
    lease_offered_at = NULL,
    lease_accepted_at = NULL,
    lease_expires_at = NULL,
    attempt_deadline_at = NULL,
    result_id = NULL,
    result_fingerprint = NULL,
    terminal_event_id = sqlc.arg(terminal_event_id),
    dead_lettered_at = NULL,
    cancel_request_id = c.id,
    cancel_state = c.state,
    cancel_requested_at = c.requested_at,
    cancel_acknowledged_at = c.acknowledged_at,
    cancel_reason = c.reason
FROM run_cancellations c
WHERE r.id = sqlc.arg(run_id)
  AND r.runtime_contract_id = 'openlinker.runtime.v2'
  AND r.status = 'running'
  AND r.dispatch_state IN ('pending', 'offered', 'executing', 'retry_wait')
  AND c.run_id = r.id
  AND c.id = sqlc.arg(cancellation_id)
  AND sqlc.arg(duration_ms)::int >= 0
  AND (
      (
          c.target_attempt_id IS NULL
          AND c.state = 'stopped'
          AND r.active_attempt_id IS NULL
          AND r.dispatch_state IN ('pending', 'retry_wait')
      )
      OR
      (
          c.target_attempt_id IS NOT NULL
          AND c.state = 'requested'
          AND r.active_attempt_id = c.target_attempt_id
          AND r.dispatch_state IN ('offered', 'executing')
          AND EXISTS (
              SELECT 1
              FROM run_attempts a
              WHERE a.run_id = r.id
                AND a.id = c.target_attempt_id
                AND a.finished_at IS NULL
                AND a.outcome IS NULL
          )
      )
      OR
      (
          c.target_attempt_id IS NOT NULL
          AND c.state = 'stopped'
          AND r.active_attempt_id = c.target_attempt_id
          AND r.dispatch_state = 'executing'
          AND EXISTS (
              SELECT 1
              FROM run_attempts a
              WHERE a.run_id = r.id
                AND a.id = c.target_attempt_id
                AND a.executor_type IN ('core_http', 'core_mcp')
                AND a.finished_at IS NOT NULL
                AND a.outcome = 'canceled'
                AND a.result_id IS NULL
          )
      )
  )
RETURNING r.id, r.status, r.dispatch_state, r.error_code, r.error_message,
          r.duration_ms, r.finished_at, r.terminal_event_id,
          r.cancel_request_id, r.cancel_state, r.cancel_requested_at,
          r.cancel_acknowledged_at, r.cancel_reason,
          clock_timestamp() AS database_now;

-- name: MirrorRuntimeV2RunCancellationState :one
UPDATE runs r
SET cancel_state = c.state,
    cancel_acknowledged_at = c.acknowledged_at,
    cancel_reason = c.reason
FROM run_cancellations c
WHERE r.id = sqlc.arg(run_id)
  AND r.runtime_contract_id = 'openlinker.runtime.v2'
  AND r.status = 'canceled'
  AND r.dispatch_state = 'terminal'
  AND r.cancel_request_id = sqlc.arg(cancellation_id)
  AND c.run_id = r.id
  AND c.id = r.cancel_request_id
RETURNING r.id, r.cancel_request_id, r.cancel_state,
          r.cancel_acknowledged_at, r.cancel_reason,
          clock_timestamp() AS database_now;

-- name: FinishRuntimeV2CanceledAttempt :one
UPDATE run_attempts a
SET finished_at = clock_timestamp(),
    outcome = 'canceled',
    error_code = sqlc.narg(error_code),
    error_detail_redacted = NULL
WHERE a.run_id = sqlc.arg(run_id)
  AND a.id = sqlc.arg(attempt_id)
  AND a.lease_id = sqlc.arg(lease_id)
  AND a.fencing_token = sqlc.arg(fencing_token)
  AND a.executor_type = 'agent_node'
  AND a.finished_at IS NULL
  AND a.outcome IS NULL
  AND a.result_id IS NULL
  AND EXISTS (
      SELECT 1
      FROM runs r
      JOIN run_cancellations c
        ON c.run_id = r.id
       AND c.id = r.cancel_request_id
      WHERE r.id = a.run_id
        AND r.runtime_contract_id = 'openlinker.runtime.v2'
        AND r.status = 'canceled'
        AND r.dispatch_state = 'terminal'
        AND c.target_attempt_id = a.id
        AND c.state IN ('stopped', 'unconfirmed')
  )
RETURNING *;

-- name: FinishRuntimeV2CoreCanceledAttempt :one
UPDATE run_attempts a
SET finished_at = clock_timestamp(),
    outcome = 'canceled',
    error_code = 'CANCELED',
    error_detail_redacted = NULL
WHERE a.run_id = sqlc.arg(run_id)
  AND a.id = sqlc.arg(attempt_id)
  AND a.lease_id = sqlc.arg(lease_id)
  AND a.fencing_token = sqlc.arg(fencing_token)
  AND a.executor_type IN ('core_http', 'core_mcp')
  AND a.finished_at IS NULL
  AND a.outcome IS NULL
  AND a.result_id IS NULL
  AND EXISTS (
      SELECT 1
      FROM runs r
      JOIN run_cancellations c ON c.run_id = r.id
      WHERE r.id = a.run_id
        AND r.runtime_contract_id = 'openlinker.runtime.v2'
        AND r.status = 'running'
        AND r.dispatch_state = 'executing'
        AND r.active_attempt_id = a.id
        AND c.target_attempt_id = a.id
        AND c.state = 'stopped'
  )
RETURNING *;
