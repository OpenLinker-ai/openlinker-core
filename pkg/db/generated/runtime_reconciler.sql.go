// Code generated from pkg/db/queries/runtime_reconciler.sql. DO NOT EDIT.

package db

import (
	"context"
	"time"

	"github.com/google/uuid"
)

const nextRuntimeReconcileDue = `-- name: NextRuntimeReconcileDue :one
WITH database_clock AS MATERIALIZED (
    SELECT clock_timestamp() AS database_now
), eligible AS (
    SELECT CASE r.dispatch_state
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
           END AS due_at
    FROM runs r
    LEFT JOIN run_attempts a
      ON a.run_id = r.id
     AND a.id = r.active_attempt_id
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
          )
          OR (
              r.dispatch_state IN ('pending', 'retry_wait')
              AND r.active_attempt_id IS NULL
          )
      )
)
SELECT (SELECT MIN(due_at) FROM eligible) AS next_due_at,
       database_clock.database_now
FROM database_clock`

type NextRuntimeReconcileDueRow struct {
	NextDueAt   *time.Time `db:"next_due_at" json:"next_due_at"`
	DatabaseNow time.Time  `db:"database_now" json:"database_now"`
}

func (q *Queries) NextRuntimeReconcileDue(ctx context.Context) (NextRuntimeReconcileDueRow, error) {
	row := q.db.QueryRow(ctx, nextRuntimeReconcileDue)
	var i NextRuntimeReconcileDueRow
	err := row.Scan(&i.NextDueAt, &i.DatabaseNow)
	return i, err
}

const listDueRuntimeReconcileCandidates = `-- name: ListDueRuntimeReconcileCandidates :many
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
LIMIT $1`

type ListDueRuntimeReconcileCandidatesRow struct {
	RunID            uuid.UUID  `db:"run_id" json:"run_id"`
	AttemptID        *uuid.UUID `db:"attempt_id" json:"attempt_id"`
	ExecutorType     *string    `db:"executor_type" json:"executor_type"`
	RuntimeSessionID *uuid.UUID `db:"runtime_session_id" json:"runtime_session_id"`
	NodeID           *uuid.UUID `db:"node_id" json:"node_id"`
	DueAt            time.Time  `db:"due_at" json:"due_at"`
	DatabaseNow      time.Time  `db:"database_now" json:"database_now"`
}

func (q *Queries) ListDueRuntimeReconcileCandidates(ctx context.Context, batchLimit int32) ([]ListDueRuntimeReconcileCandidatesRow, error) {
	rows, err := q.db.Query(ctx, listDueRuntimeReconcileCandidates, batchLimit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]ListDueRuntimeReconcileCandidatesRow, 0)
	for rows.Next() {
		var item ListDueRuntimeReconcileCandidatesRow
		if err := rows.Scan(
			&item.RunID, &item.AttemptID, &item.ExecutorType,
			&item.RuntimeSessionID, &item.NodeID, &item.DueAt,
			&item.DatabaseNow,
		); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const lockRuntimeSessionForReconcile = `-- name: LockRuntimeSessionForReconcile :one
SELECT runtime_session_id
FROM runtime_sessions
WHERE runtime_session_id = $1
FOR UPDATE SKIP LOCKED`

func (q *Queries) LockRuntimeSessionForReconcile(ctx context.Context, runtimeSessionID uuid.UUID) (uuid.UUID, error) {
	var lockedID uuid.UUID
	err := q.db.QueryRow(ctx, lockRuntimeSessionForReconcile, runtimeSessionID).Scan(&lockedID)
	return lockedID, err
}

const lockRuntimeNodeForReconcile = `-- name: LockRuntimeNodeForReconcile :one
SELECT node_id
FROM runtime_nodes
WHERE node_id = $1
FOR UPDATE SKIP LOCKED`

func (q *Queries) LockRuntimeNodeForReconcile(ctx context.Context, nodeID uuid.UUID) (uuid.UUID, error) {
	var lockedID uuid.UUID
	err := q.db.QueryRow(ctx, lockRuntimeNodeForReconcile, nodeID).Scan(&lockedID)
	return lockedID, err
}

const lockDueRuntimeRunWithAttempt = `-- name: LockDueRuntimeRunWithAttempt :one
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
WHERE r.id = $1
  AND a.id = $2
  AND a.executor_type = $3
  AND a.active_runtime_session_id IS NOT DISTINCT FROM $4
  AND a.node_id IS NOT DISTINCT FROM $5
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
FOR UPDATE OF r SKIP LOCKED`

type LockDueRuntimeRunWithAttemptParams struct {
	RunID            uuid.UUID  `db:"run_id" json:"run_id"`
	AttemptID        uuid.UUID  `db:"attempt_id" json:"attempt_id"`
	ExecutorType     string     `db:"executor_type" json:"executor_type"`
	RuntimeSessionID *uuid.UUID `db:"runtime_session_id" json:"runtime_session_id"`
	NodeID           *uuid.UUID `db:"node_id" json:"node_id"`
}

type RuntimeReconcileLockedRunRow struct {
	ID                          uuid.UUID  `db:"id" json:"id"`
	UserID                      uuid.UUID  `db:"user_id" json:"user_id"`
	AgentID                     uuid.UUID  `db:"agent_id" json:"agent_id"`
	Status                      string     `db:"status" json:"status"`
	DispatchState               string     `db:"dispatch_state" json:"dispatch_state"`
	ConnectionModeSnapshot      *string    `db:"connection_mode_snapshot" json:"connection_mode_snapshot"`
	EndpointIdempotencySnapshot *bool      `db:"endpoint_idempotency_snapshot" json:"endpoint_idempotency_snapshot"`
	OfferCount                  int32      `db:"offer_count" json:"offer_count"`
	MaxOfferCount               int32      `db:"max_offer_count" json:"max_offer_count"`
	AttemptCount                int32      `db:"attempt_count" json:"attempt_count"`
	MaxAttempts                 int32      `db:"max_attempts" json:"max_attempts"`
	LatestAttemptID             *uuid.UUID `db:"latest_attempt_id" json:"latest_attempt_id"`
	ActiveAttemptID             *uuid.UUID `db:"active_attempt_id" json:"active_attempt_id"`
	LeaseID                     *uuid.UUID `db:"lease_id" json:"lease_id"`
	FencingToken                int64      `db:"fencing_token" json:"fencing_token"`
	ExecutorType                *string    `db:"executor_type" json:"executor_type"`
	RuntimeNodeID               *uuid.UUID `db:"runtime_node_id" json:"runtime_node_id"`
	RuntimeWorkerID             *string    `db:"runtime_worker_id" json:"runtime_worker_id"`
	RuntimeSessionID            *uuid.UUID `db:"runtime_session_id" json:"runtime_session_id"`
	DispatchDeadlineAt          time.Time  `db:"dispatch_deadline_at" json:"dispatch_deadline_at"`
	RunDeadlineAt               time.Time  `db:"run_deadline_at" json:"run_deadline_at"`
	CancelRequestID             *uuid.UUID `db:"cancel_request_id" json:"cancel_request_id"`
	CreatorRevenueCents         int32      `db:"creator_revenue_cents" json:"creator_revenue_cents"`
	StartedAt                   time.Time  `db:"started_at" json:"started_at"`
	DatabaseNow                 time.Time  `db:"database_now" json:"database_now"`
}

type LockDueRuntimeRunWithAttemptRow = RuntimeReconcileLockedRunRow

func scanRuntimeReconcileLockedRun(row interface{ Scan(...any) error }, run *RuntimeReconcileLockedRunRow) error {
	return row.Scan(
		&run.ID, &run.UserID, &run.AgentID, &run.Status,
		&run.DispatchState, &run.ConnectionModeSnapshot,
		&run.EndpointIdempotencySnapshot, &run.OfferCount,
		&run.MaxOfferCount, &run.AttemptCount, &run.MaxAttempts,
		&run.LatestAttemptID, &run.ActiveAttemptID, &run.LeaseID,
		&run.FencingToken, &run.ExecutorType, &run.RuntimeNodeID,
		&run.RuntimeWorkerID, &run.RuntimeSessionID,
		&run.DispatchDeadlineAt, &run.RunDeadlineAt, &run.CancelRequestID,
		&run.CreatorRevenueCents, &run.StartedAt, &run.DatabaseNow,
	)
}

func (q *Queries) LockDueRuntimeRunWithAttempt(ctx context.Context, arg LockDueRuntimeRunWithAttemptParams) (LockDueRuntimeRunWithAttemptRow, error) {
	var run LockDueRuntimeRunWithAttemptRow
	err := scanRuntimeReconcileLockedRun(q.db.QueryRow(
		ctx, lockDueRuntimeRunWithAttempt,
		arg.RunID, arg.AttemptID, arg.ExecutorType,
		arg.RuntimeSessionID, arg.NodeID,
	), &run)
	return run, err
}

const lockDueRuntimeRunWithoutAttempt = `-- name: LockDueRuntimeRunWithoutAttempt :one
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
WHERE r.id = $1
  AND r.runtime_contract_id = 'openlinker.runtime.v2'
  AND r.status = 'running'
  AND r.dispatch_state IN ('pending', 'retry_wait')
  AND r.active_attempt_id IS NULL
  AND r.cancel_request_id IS NULL
  AND LEAST(r.dispatch_deadline_at, r.run_deadline_at) <= c.database_now
FOR UPDATE OF r SKIP LOCKED`

type LockDueRuntimeRunWithoutAttemptRow = RuntimeReconcileLockedRunRow

func (q *Queries) LockDueRuntimeRunWithoutAttempt(ctx context.Context, runID uuid.UUID) (LockDueRuntimeRunWithoutAttemptRow, error) {
	var run LockDueRuntimeRunWithoutAttemptRow
	err := scanRuntimeReconcileLockedRun(
		q.db.QueryRow(ctx, lockDueRuntimeRunWithoutAttempt, runID), &run,
	)
	return run, err
}

const finishRuntimeReconciledAttempt = `-- name: FinishRuntimeReconciledAttempt :one
UPDATE run_attempts a
SET finished_at = clock_timestamp(),
    outcome = $1,
    error_code = $2,
    error_detail_redacted = NULL
FROM runs r
WHERE a.run_id = $3
  AND a.id = $4
  AND a.lease_id = $5
  AND a.fencing_token = $6
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
          $1 = 'offer_expired'
          AND r.dispatch_state = 'offered'
          AND a.accepted_at IS NULL
          AND a.attempt_no IS NULL
      )
      OR (
          $1 IN ('lease_expired', 'timeout', 'result_unknown')
          AND r.dispatch_state = 'executing'
          AND a.accepted_at IS NOT NULL
          AND a.attempt_no IS NOT NULL
      )
  )
RETURNING a.id, a.run_id, a.agent_id, a.offer_no, a.attempt_no,
          a.executor_type, a.lease_id, a.fencing_token, a.runtime_token_id,
          a.runtime_worker_id, a.runtime_session_id, a.node_id,
          a.offered_by_core_instance_id, a.attached_core_instance_id,
          a.offered_at, a.offer_expires_at, a.accepted_at, a.last_renewed_at,
          a.lease_expires_at, a.attempt_deadline_at, a.finished_at, a.outcome,
          a.result_id, a.result_fingerprint, a.result_classification,
          a.result_acknowledged_at, a.last_client_event_seq,
          a.final_client_event_seq, a.error_code, a.error_detail_redacted,
          a.created_at, a.slot_acquired_at, a.slot_released_at,
          a.active_runtime_session_id, a.runtime_attachment_id`

type FinishRuntimeReconciledAttemptParams struct {
	Outcome      string    `db:"outcome" json:"outcome"`
	ErrorCode    string    `db:"error_code" json:"error_code"`
	RunID        uuid.UUID `db:"run_id" json:"run_id"`
	AttemptID    uuid.UUID `db:"attempt_id" json:"attempt_id"`
	LeaseID      uuid.UUID `db:"lease_id" json:"lease_id"`
	FencingToken int64     `db:"fencing_token" json:"fencing_token"`
}

func (q *Queries) FinishRuntimeReconciledAttempt(ctx context.Context, arg FinishRuntimeReconciledAttemptParams) (RunAttempt, error) {
	var attempt RunAttempt
	err := scanRunAttempt(q.db.QueryRow(
		ctx, finishRuntimeReconciledAttempt,
		arg.Outcome, arg.ErrorCode, arg.RunID, arg.AttemptID,
		arg.LeaseID, arg.FencingToken,
	), &attempt)
	return attempt, err
}

const resetRuntimeRunAfterReconciledOffer = `-- name: ResetRuntimeRunAfterReconciledOffer :one
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
WHERE r.id = $1
  AND a.run_id = r.id
  AND a.id = $2
  AND a.lease_id = $3
  AND a.fencing_token = $4
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
          c.database_now`

type ResetRuntimeRunAfterReconciledOfferParams struct {
	RunID        uuid.UUID `db:"run_id" json:"run_id"`
	AttemptID    uuid.UUID `db:"attempt_id" json:"attempt_id"`
	LeaseID      uuid.UUID `db:"lease_id" json:"lease_id"`
	FencingToken int64     `db:"fencing_token" json:"fencing_token"`
}

type RuntimeReconcileTransitionRow struct {
	ID            uuid.UUID  `db:"id" json:"id"`
	Status        string     `db:"status" json:"status"`
	DispatchState string     `db:"dispatch_state" json:"dispatch_state"`
	NextAttemptAt *time.Time `db:"next_attempt_at" json:"next_attempt_at"`
	DatabaseNow   time.Time  `db:"database_now" json:"database_now"`
}

type ResetRuntimeRunAfterReconciledOfferRow = RuntimeReconcileTransitionRow

func (q *Queries) ResetRuntimeRunAfterReconciledOffer(ctx context.Context, arg ResetRuntimeRunAfterReconciledOfferParams) (ResetRuntimeRunAfterReconciledOfferRow, error) {
	var run ResetRuntimeRunAfterReconciledOfferRow
	err := q.db.QueryRow(
		ctx, resetRuntimeRunAfterReconciledOffer,
		arg.RunID, arg.AttemptID, arg.LeaseID, arg.FencingToken,
	).Scan(
		&run.ID, &run.Status, &run.DispatchState,
		&run.NextAttemptAt, &run.DatabaseNow,
	)
	return run, err
}

const transitionRuntimeRunAfterExpiredAttempt = `-- name: TransitionRuntimeRunAfterExpiredAttempt :one
WITH database_clock AS MATERIALIZED (
    SELECT clock_timestamp() AS database_now
)
UPDATE runs r
SET dispatch_state = 'retry_wait',
    next_attempt_at = LEAST(
        c.database_now + ($1::bigint * INTERVAL '1 millisecond'),
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
WHERE r.id = $2
  AND a.run_id = r.id
  AND a.id = $3
  AND a.lease_id = $4
  AND a.fencing_token = $5
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
  AND $1::bigint BETWEEN 1 AND 60000
RETURNING r.id, r.status, r.dispatch_state, r.next_attempt_at,
          c.database_now`

type TransitionRuntimeRunAfterExpiredAttemptParams struct {
	RetryAfterMs int64     `db:"retry_after_ms" json:"retry_after_ms"`
	RunID        uuid.UUID `db:"run_id" json:"run_id"`
	AttemptID    uuid.UUID `db:"attempt_id" json:"attempt_id"`
	LeaseID      uuid.UUID `db:"lease_id" json:"lease_id"`
	FencingToken int64     `db:"fencing_token" json:"fencing_token"`
}

type TransitionRuntimeRunAfterExpiredAttemptRow = RuntimeReconcileTransitionRow

func (q *Queries) TransitionRuntimeRunAfterExpiredAttempt(ctx context.Context, arg TransitionRuntimeRunAfterExpiredAttemptParams) (TransitionRuntimeRunAfterExpiredAttemptRow, error) {
	var run TransitionRuntimeRunAfterExpiredAttemptRow
	err := q.db.QueryRow(
		ctx, transitionRuntimeRunAfterExpiredAttempt,
		arg.RetryAfterMs, arg.RunID, arg.AttemptID,
		arg.LeaseID, arg.FencingToken,
	).Scan(
		&run.ID, &run.Status, &run.DispatchState,
		&run.NextAttemptAt, &run.DatabaseNow,
	)
	return run, err
}

const finalizeRuntimeReconciledRun = `-- name: FinalizeRuntimeReconciledRun :one
UPDATE runs r
SET status = $1,
    dispatch_state = $2,
    output = NULL,
    error_code = $3,
    error_message = $4,
    duration_ms = $5,
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
    terminal_event_id = $6,
    dead_lettered_at = CASE
        WHEN $2 = 'dead_letter' THEN clock_timestamp()
        ELSE NULL
    END
WHERE r.id = $7
  AND r.runtime_contract_id = 'openlinker.runtime.v2'
  AND r.status = 'running'
  AND r.dispatch_state IN ('pending', 'offered', 'executing', 'retry_wait')
  AND r.cancel_request_id IS NULL
  AND r.terminal_event_id IS NULL
  AND $5::int >= 0
  AND (
      (
          $8::uuid IS NULL
          AND r.active_attempt_id IS NULL
          AND r.dispatch_state IN ('pending', 'retry_wait')
      )
      OR (
          $8::uuid IS NOT NULL
          AND r.active_attempt_id = $8
          AND r.latest_attempt_id = $8
          AND EXISTS (
              SELECT 1
              FROM run_attempts a
              WHERE a.run_id = r.id
                AND a.id = $8
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
          $1 = 'timeout'
          AND $2 = 'terminal'
          AND $3 IN ('RUNTIME_DISPATCH_TIMEOUT', 'RUN_DEADLINE_EXCEEDED')
      )
      OR (
          $1 = 'failed'
          AND $2 = 'dead_letter'
          AND $3 = 'RUNTIME_RETRY_EXHAUSTED'
          AND $8::uuid IS NOT NULL
          AND r.attempt_count >= r.max_attempts
          AND EXISTS (
              SELECT 1
              FROM run_attempts a
              WHERE a.run_id = r.id
                AND a.id = $8
                AND a.attempt_no = r.attempt_count
                AND a.finished_at IS NOT NULL
                AND a.outcome IN ('lease_expired', 'result_unknown')
          )
      )
      OR (
          $1 = 'failed'
          AND $2 = 'terminal'
          AND $3 = 'ENDPOINT_RESULT_UNKNOWN'
          AND $8::uuid IS NOT NULL
          AND r.endpoint_idempotency_snapshot = FALSE
          AND EXISTS (
              SELECT 1
              FROM run_attempts a
              WHERE a.run_id = r.id
                AND a.id = $8
                AND a.executor_type IN ('core_http', 'core_mcp')
                AND a.finished_at IS NOT NULL
                AND a.outcome = 'result_unknown'
                AND a.result_id IS NULL
          )
      )
  )
RETURNING r.id, r.status, r.dispatch_state, r.error_code,
          r.error_message, r.duration_ms, r.finished_at,
          r.terminal_event_id, r.dead_lettered_at`

type FinalizeRuntimeReconciledRunParams struct {
	Status          string     `db:"status" json:"status"`
	DispatchState   string     `db:"dispatch_state" json:"dispatch_state"`
	ErrorCode       string     `db:"error_code" json:"error_code"`
	ErrorMessage    string     `db:"error_message" json:"error_message"`
	DurationMs      int32      `db:"duration_ms" json:"duration_ms"`
	TerminalEventID uuid.UUID  `db:"terminal_event_id" json:"terminal_event_id"`
	RunID           uuid.UUID  `db:"run_id" json:"run_id"`
	AttemptID       *uuid.UUID `db:"attempt_id" json:"attempt_id"`
}

type FinalizeRuntimeReconciledRunRow struct {
	ID              uuid.UUID  `db:"id" json:"id"`
	Status          string     `db:"status" json:"status"`
	DispatchState   string     `db:"dispatch_state" json:"dispatch_state"`
	ErrorCode       *string    `db:"error_code" json:"error_code"`
	ErrorMessage    *string    `db:"error_message" json:"error_message"`
	DurationMs      *int32     `db:"duration_ms" json:"duration_ms"`
	FinishedAt      *time.Time `db:"finished_at" json:"finished_at"`
	TerminalEventID *uuid.UUID `db:"terminal_event_id" json:"terminal_event_id"`
	DeadLetteredAt  *time.Time `db:"dead_lettered_at" json:"dead_lettered_at"`
}

func (q *Queries) FinalizeRuntimeReconciledRun(ctx context.Context, arg FinalizeRuntimeReconciledRunParams) (FinalizeRuntimeReconciledRunRow, error) {
	var run FinalizeRuntimeReconciledRunRow
	err := q.db.QueryRow(
		ctx, finalizeRuntimeReconciledRun,
		arg.Status, arg.DispatchState, arg.ErrorCode, arg.ErrorMessage,
		arg.DurationMs, arg.TerminalEventID, arg.RunID, arg.AttemptID,
	).Scan(
		&run.ID, &run.Status, &run.DispatchState, &run.ErrorCode,
		&run.ErrorMessage, &run.DurationMs, &run.FinishedAt,
		&run.TerminalEventID, &run.DeadLetteredAt,
	)
	return run, err
}

const getRuntimeReconcileDatabaseClock = `-- name: GetRuntimeReconcileDatabaseClock :one
SELECT clock_timestamp() AS database_now`

func (q *Queries) GetRuntimeReconcileDatabaseClock(ctx context.Context) (time.Time, error) {
	var databaseNow time.Time
	err := q.db.QueryRow(ctx, getRuntimeReconcileDatabaseClock).Scan(&databaseNow)
	return databaseNow, err
}
