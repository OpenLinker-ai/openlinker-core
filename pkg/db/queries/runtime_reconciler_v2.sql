-- Reliable runtime v2 lease/deadline reconciliation primitives.
--
-- Candidate discovery is deliberately lock-free. An Agent Node Attempt owns a
-- Session/Node capacity slot, so the worker must lock Session -> Node before it
-- may lock Run -> Attempt. The exact lock queries revalidate every candidate
-- with PostgreSQL's clock and use SKIP LOCKED to keep concurrent workers from
-- waiting on one another.

-- name: ListDueRuntimeV2ReconcileCandidates :many
WITH database_clock AS MATERIALIZED (
    SELECT clock_timestamp() AS database_now
), candidates AS (
    SELECT r.id AS run_id,
           r.active_attempt_id AS attempt_id,
           a.executor_type,
           a.active_runtime_session_id AS runtime_session_id,
           a.node_id,
           CASE r.dispatch_state
               WHEN 'offered' THEN LEAST(
                   a.offer_expires_at,
                   r.dispatch_deadline_at,
                   r.run_deadline_at
               )
               WHEN 'executing' THEN LEAST(
                   a.lease_expires_at,
                   a.attempt_deadline_at,
                   r.run_deadline_at
               )
               ELSE LEAST(r.dispatch_deadline_at, r.run_deadline_at)
           END AS due_at,
           c.database_now
    FROM runs r
    LEFT JOIN run_attempts a
      ON a.run_id = r.id
     AND a.id = r.active_attempt_id
    CROSS JOIN database_clock c
    WHERE r.runtime_contract_id = 'openlinker.runtime.v2'
      AND r.status = 'running'
      AND r.cancel_request_id IS NULL
      AND (
          (
              r.dispatch_state = 'offered'
              AND r.active_attempt_id IS NOT NULL
              AND a.id = r.active_attempt_id
              AND a.finished_at IS NULL
              AND a.outcome IS NULL
              AND a.result_id IS NULL
              AND a.accepted_at IS NULL
              AND a.attempt_no IS NULL
              AND LEAST(
                  a.offer_expires_at,
                  r.dispatch_deadline_at,
                  r.run_deadline_at
              ) <= c.database_now
          )
          OR (
              r.dispatch_state = 'executing'
              AND r.active_attempt_id IS NOT NULL
              AND a.id = r.active_attempt_id
              AND a.finished_at IS NULL
              AND a.outcome IS NULL
              AND a.result_id IS NULL
              AND a.accepted_at IS NOT NULL
              AND a.attempt_no IS NOT NULL
              AND LEAST(
                  a.lease_expires_at,
                  a.attempt_deadline_at,
                  r.run_deadline_at
              ) <= c.database_now
          )
          OR (
              r.dispatch_state IN ('pending', 'retry_wait')
              AND r.active_attempt_id IS NULL
              AND LEAST(r.dispatch_deadline_at, r.run_deadline_at)
                  <= c.database_now
          )
      )
)
SELECT run_id, attempt_id, executor_type, runtime_session_id, node_id,
       due_at, database_now
FROM candidates
ORDER BY due_at ASC, run_id ASC
LIMIT sqlc.arg(batch_limit);

-- name: LockRuntimeSessionForV2Reconcile :one
SELECT runtime_session_id
FROM runtime_sessions
WHERE runtime_session_id = sqlc.arg(runtime_session_id)
FOR UPDATE SKIP LOCKED;

-- name: LockRuntimeNodeForV2Reconcile :one
SELECT node_id
FROM runtime_nodes
WHERE node_id = sqlc.arg(node_id)
FOR UPDATE SKIP LOCKED;

-- name: LockDueRuntimeV2RunWithAttempt :one
WITH database_clock AS MATERIALIZED (
    SELECT clock_timestamp() AS database_now
)
SELECT r.id, r.user_id, r.agent_id, r.status, r.dispatch_state,
       r.connection_mode_snapshot, r.endpoint_idempotency_snapshot,
       r.offer_count, r.max_offer_count, r.attempt_count, r.max_attempts,
       r.latest_attempt_id, r.active_attempt_id, r.lease_id,
       r.fencing_token, r.executor_type, r.runtime_node_id,
       r.runtime_worker_id, r.runtime_session_id,
       r.dispatch_deadline_at, r.run_deadline_at, r.cancel_request_id,
       r.creator_revenue_cents, r.started_at, c.database_now
FROM runs r
JOIN run_attempts a
  ON a.run_id = r.id
 AND a.id = r.active_attempt_id
CROSS JOIN database_clock c
WHERE r.id = sqlc.arg(run_id)
  AND a.id = sqlc.arg(attempt_id)
  AND a.executor_type = sqlc.arg(executor_type)
  AND a.active_runtime_session_id IS NOT DISTINCT FROM sqlc.narg(runtime_session_id)
  AND a.node_id IS NOT DISTINCT FROM sqlc.narg(node_id)
  AND r.runtime_contract_id = 'openlinker.runtime.v2'
  AND r.status = 'running'
  AND r.cancel_request_id IS NULL
  AND r.latest_attempt_id = a.id
  AND r.active_attempt_id = a.id
  AND r.lease_id = a.lease_id
  AND r.fencing_token = a.fencing_token
  AND r.executor_type = a.executor_type
  AND a.finished_at IS NULL
  AND a.outcome IS NULL
  AND a.result_id IS NULL
  AND (
      (
          r.dispatch_state = 'offered'
          AND a.accepted_at IS NULL
          AND a.attempt_no IS NULL
          AND LEAST(
              a.offer_expires_at,
              r.dispatch_deadline_at,
              r.run_deadline_at
          ) <= c.database_now
      )
      OR (
          r.dispatch_state = 'executing'
          AND a.accepted_at IS NOT NULL
          AND a.attempt_no IS NOT NULL
          AND LEAST(
              a.lease_expires_at,
              a.attempt_deadline_at,
              r.run_deadline_at
          ) <= c.database_now
      )
  )
FOR UPDATE OF r SKIP LOCKED;

-- name: LockDueRuntimeV2RunWithoutAttempt :one
WITH database_clock AS MATERIALIZED (
    SELECT clock_timestamp() AS database_now
)
SELECT r.id, r.user_id, r.agent_id, r.status, r.dispatch_state,
       r.connection_mode_snapshot, r.endpoint_idempotency_snapshot,
       r.offer_count, r.max_offer_count, r.attempt_count, r.max_attempts,
       r.latest_attempt_id, r.active_attempt_id, r.lease_id,
       r.fencing_token, r.executor_type, r.runtime_node_id,
       r.runtime_worker_id, r.runtime_session_id,
       r.dispatch_deadline_at, r.run_deadline_at, r.cancel_request_id,
       r.creator_revenue_cents, r.started_at, c.database_now
FROM runs r
CROSS JOIN database_clock c
WHERE r.id = sqlc.arg(run_id)
  AND r.runtime_contract_id = 'openlinker.runtime.v2'
  AND r.status = 'running'
  AND r.dispatch_state IN ('pending', 'retry_wait')
  AND r.active_attempt_id IS NULL
  AND r.cancel_request_id IS NULL
  AND LEAST(r.dispatch_deadline_at, r.run_deadline_at) <= c.database_now
FOR UPDATE OF r SKIP LOCKED;

-- name: FinishRuntimeV2ReconciledAttempt :one
UPDATE run_attempts a
SET finished_at = clock_timestamp(),
    outcome = sqlc.arg(outcome),
    error_code = sqlc.arg(error_code),
    error_detail_redacted = NULL
FROM runs r
WHERE a.run_id = sqlc.arg(run_id)
  AND a.id = sqlc.arg(attempt_id)
  AND a.lease_id = sqlc.arg(lease_id)
  AND a.fencing_token = sqlc.arg(fencing_token)
  AND a.finished_at IS NULL
  AND a.outcome IS NULL
  AND a.result_id IS NULL
  AND r.id = a.run_id
  AND r.runtime_contract_id = 'openlinker.runtime.v2'
  AND r.status = 'running'
  AND r.cancel_request_id IS NULL
  AND r.latest_attempt_id = a.id
  AND r.active_attempt_id = a.id
  AND r.lease_id = a.lease_id
  AND r.fencing_token = a.fencing_token
  AND (
      (
          sqlc.arg(outcome) = 'offer_expired'
          AND r.dispatch_state = 'offered'
          AND a.accepted_at IS NULL
          AND a.attempt_no IS NULL
      )
      OR (
          sqlc.arg(outcome) IN ('lease_expired', 'timeout')
          AND r.dispatch_state = 'executing'
          AND a.accepted_at IS NOT NULL
          AND a.attempt_no IS NOT NULL
      )
  )
RETURNING a.*;

-- name: ResetRuntimeV2RunAfterReconciledOffer :one
WITH database_clock AS MATERIALIZED (
    SELECT clock_timestamp() AS database_now
)
UPDATE runs r
SET dispatch_state = 'pending',
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
    error_code = NULL,
    error_message = NULL
FROM run_attempts a, database_clock c
WHERE r.id = sqlc.arg(run_id)
  AND a.run_id = r.id
  AND a.id = sqlc.arg(attempt_id)
  AND a.lease_id = sqlc.arg(lease_id)
  AND a.fencing_token = sqlc.arg(fencing_token)
  AND a.finished_at IS NOT NULL
  AND a.outcome = 'offer_expired'
  AND a.accepted_at IS NULL
  AND a.attempt_no IS NULL
  AND r.runtime_contract_id = 'openlinker.runtime.v2'
  AND r.status = 'running'
  AND r.dispatch_state = 'offered'
  AND r.cancel_request_id IS NULL
  AND r.active_attempt_id = a.id
  AND r.latest_attempt_id = a.id
  AND r.lease_id = a.lease_id
  AND r.fencing_token = a.fencing_token
  AND r.offer_count < r.max_offer_count
  AND r.dispatch_deadline_at > c.database_now
  AND r.run_deadline_at > c.database_now
RETURNING r.id, r.status, r.dispatch_state, r.next_attempt_at,
          c.database_now;

-- name: TransitionRuntimeV2RunAfterExpiredAttempt :one
WITH database_clock AS MATERIALIZED (
    SELECT clock_timestamp() AS database_now
)
UPDATE runs r
SET dispatch_state = 'retry_wait',
    next_attempt_at = LEAST(
        c.database_now
            + (sqlc.arg(retry_after_ms)::bigint * INTERVAL '1 millisecond'),
        r.dispatch_deadline_at,
        r.run_deadline_at
    ),
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
    error_code = NULL,
    error_message = NULL
FROM run_attempts a, database_clock c
WHERE r.id = sqlc.arg(run_id)
  AND a.run_id = r.id
  AND a.id = sqlc.arg(attempt_id)
  AND a.lease_id = sqlc.arg(lease_id)
  AND a.fencing_token = sqlc.arg(fencing_token)
  AND a.finished_at IS NOT NULL
  AND a.outcome = 'lease_expired'
  AND a.accepted_at IS NOT NULL
  AND a.attempt_no IS NOT NULL
  AND r.runtime_contract_id = 'openlinker.runtime.v2'
  AND r.status = 'running'
  AND r.dispatch_state = 'executing'
  AND r.cancel_request_id IS NULL
  AND r.active_attempt_id = a.id
  AND r.latest_attempt_id = a.id
  AND r.lease_id = a.lease_id
  AND r.fencing_token = a.fencing_token
  AND r.attempt_count < r.max_attempts
  AND r.dispatch_deadline_at > c.database_now
  AND r.run_deadline_at > c.database_now
  AND sqlc.arg(retry_after_ms)::bigint BETWEEN 1 AND 60000
RETURNING r.id, r.status, r.dispatch_state, r.next_attempt_at,
          c.database_now;

-- name: FinalizeRuntimeV2ReconciledRun :one
UPDATE runs r
SET status = sqlc.arg(status),
    dispatch_state = sqlc.arg(dispatch_state),
    output = NULL,
    error_code = sqlc.arg(error_code),
    error_message = sqlc.arg(error_message),
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
    dead_lettered_at = CASE
        WHEN sqlc.arg(dispatch_state) = 'dead_letter' THEN clock_timestamp()
        ELSE NULL
    END
WHERE r.id = sqlc.arg(run_id)
  AND r.runtime_contract_id = 'openlinker.runtime.v2'
  AND r.status = 'running'
  AND r.dispatch_state IN ('pending', 'offered', 'executing', 'retry_wait')
  AND r.cancel_request_id IS NULL
  AND r.terminal_event_id IS NULL
  AND sqlc.arg(duration_ms)::int >= 0
  AND (
      (
          sqlc.narg(attempt_id)::uuid IS NULL
          AND r.active_attempt_id IS NULL
          AND r.dispatch_state IN ('pending', 'retry_wait')
      )
      OR (
          sqlc.narg(attempt_id)::uuid IS NOT NULL
          AND r.active_attempt_id = sqlc.narg(attempt_id)
          AND r.latest_attempt_id = sqlc.narg(attempt_id)
          AND EXISTS (
              SELECT 1
              FROM run_attempts a
              WHERE a.run_id = r.id
                AND a.id = sqlc.narg(attempt_id)
                AND a.finished_at IS NOT NULL
                AND a.outcome IN (
                    'offer_expired', 'lease_expired', 'timeout',
                    'result_unknown', 'retryable_failure'
                )
          )
      )
  )
  AND (
      (
          sqlc.arg(status) = 'timeout'
          AND sqlc.arg(dispatch_state) = 'terminal'
          AND sqlc.arg(error_code) IN (
              'RUNTIME_DISPATCH_TIMEOUT', 'RUN_DEADLINE_EXCEEDED'
          )
      )
      OR (
          sqlc.arg(status) = 'failed'
          AND sqlc.arg(dispatch_state) = 'dead_letter'
          AND sqlc.arg(error_code) = 'RUNTIME_RETRY_EXHAUSTED'
          AND sqlc.narg(attempt_id)::uuid IS NOT NULL
          AND r.attempt_count >= r.max_attempts
          AND EXISTS (
              SELECT 1
              FROM run_attempts a
              WHERE a.run_id = r.id
                AND a.id = sqlc.narg(attempt_id)
                AND a.attempt_no = r.attempt_count
                AND a.finished_at IS NOT NULL
                AND a.outcome IN ('lease_expired', 'result_unknown')
          )
      )
  )
RETURNING r.id, r.status, r.dispatch_state, r.error_code,
          r.error_message, r.duration_ms, r.finished_at,
          r.terminal_event_id, r.dead_lettered_at;

-- name: GetRuntimeV2ReconcileDatabaseClock :one
SELECT clock_timestamp() AS database_now;
