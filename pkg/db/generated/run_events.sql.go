// Hand-maintained SQL query implementation.
// sqlc comparison output is isolated under .sqlc/generated; review it manually before any migration of this runtime package.

package db

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func scanRunEvent(row interface {
	Scan(dest ...any) error
}, e *RunEvent) error {
	return row.Scan(
		&e.ID,
		&e.RunID,
		&e.ParentRunID,
		&e.Sequence,
		&e.EventType,
		&e.Payload,
		&e.CreatedAt,
		&e.ClientEventID,
		&e.ClientEventSeq,
		&e.PayloadFingerprint,
		&e.AttemptID,
		&e.AttemptNo,
		&e.FencingToken,
	)
}

const lockRunEventSequence = `-- name: LockRunEventSequence :exec
SELECT pg_advisory_xact_lock(hashtextextended($1::uuid::text, 0))`

// LockRunEventSequence serializes global Event sequence allocation for a Run.
// Callers must first lock the Run row and hold both locks in the Event INSERT
// transaction.
func (q *Queries) LockRunEventSequence(ctx context.Context, runID uuid.UUID) error {
	_, err := q.db.Exec(ctx, lockRunEventSequence, runID)
	return err
}

const lockRunForSystemEventAppend = `-- name: LockRunForSystemEventAppend :one
SELECT r.id
FROM runs r
WHERE r.id = $1
FOR UPDATE`

const lockRunForEventAppend = `-- name: LockRunForEventAppend :one
SELECT r.id, r.agent_id, r.status, r.runtime_contract_id, r.dispatch_state,
       r.active_attempt_id, r.lease_id, r.fencing_token, r.executor_type,
       r.runtime_node_id, r.runtime_worker_id, r.runtime_session_id,
       r.lease_token_id, r.lease_accepted_at, r.lease_expires_at,
       r.attempt_deadline_at, r.run_deadline_at,
       clock_timestamp() AS database_now
FROM runs r
WHERE r.id = $1
  AND r.runtime_contract_id = 'openlinker.runtime.v2'
FOR UPDATE`

type LockRunForEventAppendRow struct {
	ID                uuid.UUID  `db:"id" json:"id"`
	AgentID           uuid.UUID  `db:"agent_id" json:"agent_id"`
	Status            string     `db:"status" json:"status"`
	RuntimeContractID string     `db:"runtime_contract_id" json:"runtime_contract_id"`
	DispatchState     string     `db:"dispatch_state" json:"dispatch_state"`
	ActiveAttemptID   *uuid.UUID `db:"active_attempt_id" json:"active_attempt_id"`
	LeaseID           *uuid.UUID `db:"lease_id" json:"lease_id"`
	FencingToken      int64      `db:"fencing_token" json:"fencing_token"`
	ExecutorType      *string    `db:"executor_type" json:"executor_type"`
	RuntimeNodeID     *uuid.UUID `db:"runtime_node_id" json:"runtime_node_id"`
	RuntimeWorkerID   *string    `db:"runtime_worker_id" json:"runtime_worker_id"`
	RuntimeSessionID  *uuid.UUID `db:"runtime_session_id" json:"runtime_session_id"`
	LeaseTokenID      *uuid.UUID `db:"lease_token_id" json:"lease_token_id"`
	LeaseAcceptedAt   *time.Time `db:"lease_accepted_at" json:"lease_accepted_at"`
	LeaseExpiresAt    *time.Time `db:"lease_expires_at" json:"lease_expires_at"`
	AttemptDeadlineAt *time.Time `db:"attempt_deadline_at" json:"attempt_deadline_at"`
	RunDeadlineAt     *time.Time `db:"run_deadline_at" json:"run_deadline_at"`
	DatabaseNow       time.Time  `db:"database_now" json:"database_now"`
}

// LockRunForEventAppend first locks and returns the v2 Run fields required to
// validate an Event's active Attempt, lease, fence, worker, session and all
// deadlines against one database-clock observation.
func (q *Queries) LockRunForEventAppend(ctx context.Context, runID uuid.UUID) (LockRunForEventAppendRow, error) {
	var r LockRunForEventAppendRow
	err := q.db.QueryRow(ctx, lockRunForEventAppend, runID).Scan(
		&r.ID,
		&r.AgentID,
		&r.Status,
		&r.RuntimeContractID,
		&r.DispatchState,
		&r.ActiveAttemptID,
		&r.LeaseID,
		&r.FencingToken,
		&r.ExecutorType,
		&r.RuntimeNodeID,
		&r.RuntimeWorkerID,
		&r.RuntimeSessionID,
		&r.LeaseTokenID,
		&r.LeaseAcceptedAt,
		&r.LeaseExpiresAt,
		&r.AttemptDeadlineAt,
		&r.RunDeadlineAt,
		&r.DatabaseNow,
	)
	return r, err
}

const createRunEvent = `-- name: CreateRunEvent :one
WITH target_run AS (
    SELECT r.id
    FROM runs r
    WHERE r.id = $1::uuid
),
next_sequence AS (
    SELECT COALESCE(MAX(e.sequence), 0)::int + 1 AS sequence
    FROM run_events e
    JOIN target_run r ON r.id = e.run_id
)
INSERT INTO run_events (
    run_id, parent_run_id, sequence, event_type, payload
)
SELECT
    target_run.id, $2, next_sequence.sequence, $3, $4
FROM target_run, next_sequence
RETURNING run_events.id, run_events.run_id, run_events.parent_run_id,
          run_events.sequence, run_events.event_type, run_events.payload,
          run_events.created_at, run_events.client_event_id,
          run_events.client_event_seq, run_events.payload_fingerprint,
          run_events.attempt_id, run_events.attempt_no,
          run_events.fencing_token`

// CreateRunEventParams contains a system-generated Event. ParentRunID is
// normally nil for a single Agent Run.
type CreateRunEventParams struct {
	RunID       uuid.UUID  `db:"run_id" json:"run_id"`
	ParentRunID *uuid.UUID `db:"parent_run_id" json:"parent_run_id"`
	EventType   string     `db:"event_type" json:"event_type"`
	Payload     []byte     `db:"payload" json:"payload"`
}

// CreateRunEvent appends a system Event and allocates its global sequence.
func (q *Queries) CreateRunEvent(ctx context.Context, arg CreateRunEventParams) (RunEvent, error) {
	if tx, ok := q.db.(pgx.Tx); ok {
		return createRunEventInTx(ctx, tx, arg)
	}
	if beginner, ok := q.db.(interface {
		Begin(context.Context) (pgx.Tx, error)
	}); ok {
		tx, err := beginner.Begin(ctx)
		if err != nil {
			return RunEvent{}, err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		event, err := createRunEventInTx(ctx, tx, arg)
		if err != nil {
			return RunEvent{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return RunEvent{}, err
		}
		return event, nil
	}

	row := q.db.QueryRow(ctx, createRunEvent,
		arg.RunID,
		arg.ParentRunID,
		arg.EventType,
		arg.Payload,
	)
	var e RunEvent
	err := scanRunEvent(row, &e)
	return e, err
}

func createRunEventInTx(ctx context.Context, tx pgx.Tx, arg CreateRunEventParams) (RunEvent, error) {
	var lockedRunID uuid.UUID
	if err := tx.QueryRow(ctx, lockRunForSystemEventAppend, arg.RunID).Scan(&lockedRunID); err != nil {
		return RunEvent{}, err
	}
	if _, err := tx.Exec(ctx, lockRunEventSequence, arg.RunID); err != nil {
		return RunEvent{}, err
	}
	row := tx.QueryRow(ctx, createRunEvent,
		arg.RunID,
		arg.ParentRunID,
		arg.EventType,
		arg.Payload,
	)
	var e RunEvent
	err := scanRunEvent(row, &e)
	return e, err
}

const createRunEffectParentEvent = `-- name: CreateRunEffectParentEvent :one
WITH target_run AS (
    SELECT r.id
    FROM runs r
    WHERE r.id = $2::uuid
),
next_sequence AS (
    SELECT COALESCE(MAX(e.sequence), 0)::int + 1 AS sequence
    FROM run_events e
    JOIN target_run r ON r.id = e.run_id
)
INSERT INTO run_events (
    id, run_id, parent_run_id, sequence, event_type, payload
)
SELECT
    $1, target_run.id, NULL, next_sequence.sequence, 'run.child.completed', $3
FROM target_run, next_sequence
ON CONFLICT (id) DO NOTHING
RETURNING run_events.id, run_events.run_id, run_events.parent_run_id,
          run_events.sequence, run_events.event_type, run_events.payload,
          run_events.created_at, run_events.client_event_id,
          run_events.client_event_seq, run_events.payload_fingerprint,
          run_events.attempt_id, run_events.attempt_no,
          run_events.fencing_token`

const getMatchingRunEffectParentEvent = `-- name: GetMatchingRunEffectParentEvent :one
SELECT id, run_id, parent_run_id, sequence, event_type, payload, created_at,
       client_event_id, client_event_seq, payload_fingerprint,
       attempt_id, attempt_no, fencing_token
FROM run_events
WHERE id = $1
  AND run_id = $2
  AND parent_run_id IS NULL
  AND event_type = 'run.child.completed'
  AND payload = $3`

type CreateRunEffectParentEventParams struct {
	ID          uuid.UUID `db:"id" json:"id"`
	ParentRunID uuid.UUID `db:"parent_run_id" json:"parent_run_id"`
	Payload     []byte    `db:"payload" json:"payload"`
}

func (q *Queries) CreateRunEffectParentEvent(ctx context.Context, arg CreateRunEffectParentEventParams) (RunEvent, error) {
	if tx, ok := q.db.(pgx.Tx); ok {
		return createRunEffectParentEventInTx(ctx, tx, arg)
	}
	if beginner, ok := q.db.(interface {
		Begin(context.Context) (pgx.Tx, error)
	}); ok {
		tx, err := beginner.Begin(ctx)
		if err != nil {
			return RunEvent{}, err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		event, err := createRunEffectParentEventInTx(ctx, tx, arg)
		if err != nil {
			return RunEvent{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return RunEvent{}, err
		}
		return event, nil
	}
	return RunEvent{}, pgx.ErrTxClosed
}

func createRunEffectParentEventInTx(
	ctx context.Context,
	tx pgx.Tx,
	arg CreateRunEffectParentEventParams,
) (RunEvent, error) {
	var lockedRunID uuid.UUID
	if err := tx.QueryRow(ctx, lockRunForSystemEventAppend, arg.ParentRunID).Scan(&lockedRunID); err != nil {
		return RunEvent{}, err
	}
	if _, err := tx.Exec(ctx, lockRunEventSequence, arg.ParentRunID); err != nil {
		return RunEvent{}, err
	}
	var event RunEvent
	err := scanRunEvent(tx.QueryRow(
		ctx, createRunEffectParentEvent, arg.ID, arg.ParentRunID, arg.Payload,
	), &event)
	if errors.Is(err, pgx.ErrNoRows) {
		err = scanRunEvent(tx.QueryRow(
			ctx, getMatchingRunEffectParentEvent, arg.ID, arg.ParentRunID, arg.Payload,
		), &event)
	}
	return event, err
}

const getRunEventByClientID = `-- name: GetRunEventByClientID :one
SELECT e.id, e.run_id, e.parent_run_id, e.sequence, e.event_type, e.payload,
       e.created_at, e.client_event_id, e.client_event_seq,
       e.payload_fingerprint, e.attempt_id, e.attempt_no, e.fencing_token
FROM run_events e
WHERE e.run_id = $1
  AND e.client_event_id = $2`

type GetRunEventByClientIDParams struct {
	RunID         uuid.UUID `db:"run_id" json:"run_id"`
	ClientEventID uuid.UUID `db:"client_event_id" json:"client_event_id"`
}

func (q *Queries) GetRunEventByClientID(ctx context.Context, arg GetRunEventByClientIDParams) (RunEvent, error) {
	var e RunEvent
	err := scanRunEvent(q.db.QueryRow(ctx, getRunEventByClientID, arg.RunID, arg.ClientEventID), &e)
	return e, err
}

const getRunEventByAttemptSequence = `-- name: GetRunEventByAttemptSequence :one
SELECT e.id, e.run_id, e.parent_run_id, e.sequence, e.event_type, e.payload,
       e.created_at, e.client_event_id, e.client_event_seq,
       e.payload_fingerprint, e.attempt_id, e.attempt_no, e.fencing_token
FROM run_events e
WHERE e.run_id = $1
  AND e.attempt_id = $2
  AND e.attempt_no = $3
  AND e.client_event_seq = $4`

type GetRunEventByAttemptSequenceParams struct {
	RunID          uuid.UUID `db:"run_id" json:"run_id"`
	AttemptID      uuid.UUID `db:"attempt_id" json:"attempt_id"`
	AttemptNo      int32     `db:"attempt_no" json:"attempt_no"`
	ClientEventSeq int64     `db:"client_event_seq" json:"client_event_seq"`
}

func (q *Queries) GetRunEventByAttemptSequence(ctx context.Context, arg GetRunEventByAttemptSequenceParams) (RunEvent, error) {
	var e RunEvent
	err := scanRunEvent(q.db.QueryRow(ctx, getRunEventByAttemptSequence,
		arg.RunID, arg.AttemptID, arg.AttemptNo, arg.ClientEventSeq,
	), &e)
	return e, err
}

const createRuntimeRunEvent = `-- name: CreateRuntimeRunEvent :one
WITH target_run AS (
    SELECT r.id
    FROM runs r
    WHERE r.id = $1::uuid
      AND r.runtime_contract_id = 'openlinker.runtime.v2'
),
next_sequence AS (
    SELECT COALESCE(MAX(e.sequence), 0)::int + 1 AS sequence
    FROM run_events e
    JOIN target_run r ON r.id = e.run_id
)
INSERT INTO run_events (
    run_id, parent_run_id, sequence, event_type, payload,
    client_event_id, client_event_seq, payload_fingerprint,
    attempt_id, attempt_no, fencing_token
)
SELECT target_run.id, $2, next_sequence.sequence, $3, $4,
       $5, $6, $7, $8, $9, $10
FROM target_run, next_sequence
RETURNING run_events.id, run_events.run_id, run_events.parent_run_id,
          run_events.sequence, run_events.event_type, run_events.payload,
          run_events.created_at, run_events.client_event_id,
          run_events.client_event_seq, run_events.payload_fingerprint,
          run_events.attempt_id, run_events.attempt_no,
          run_events.fencing_token`

type CreateRuntimeRunEventParams struct {
	RunID              uuid.UUID  `db:"run_id" json:"run_id"`
	ParentRunID        *uuid.UUID `db:"parent_run_id" json:"parent_run_id"`
	EventType          string     `db:"event_type" json:"event_type"`
	Payload            []byte     `db:"payload" json:"payload"`
	ClientEventID      uuid.UUID  `db:"client_event_id" json:"client_event_id"`
	ClientEventSeq     int64      `db:"client_event_seq" json:"client_event_seq"`
	PayloadFingerprint []byte     `db:"payload_fingerprint" json:"-"`
	AttemptID          uuid.UUID  `db:"attempt_id" json:"attempt_id"`
	AttemptNo          int32      `db:"attempt_no" json:"attempt_no"`
	FencingToken       int64      `db:"fencing_token" json:"fencing_token"`
}

// CreateRuntimeRunEvent inserts a client Event. The caller must already hold
// LockRunForEventAppend and then LockRunEventSequence in the same transaction.
func (q *Queries) CreateRuntimeRunEvent(ctx context.Context, arg CreateRuntimeRunEventParams) (RunEvent, error) {
	var e RunEvent
	err := scanRunEvent(q.db.QueryRow(ctx, createRuntimeRunEvent,
		arg.RunID,
		arg.ParentRunID,
		arg.EventType,
		arg.Payload,
		arg.ClientEventID,
		arg.ClientEventSeq,
		arg.PayloadFingerprint,
		arg.AttemptID,
		arg.AttemptNo,
		arg.FencingToken,
	), &e)
	return e, err
}

const getRunEventRetentionWatermark = `-- name: GetRunEventRetentionWatermark :one
SELECT requested.run_id,
       COALESCE(w.retained_through_sequence, 0)::int AS retained_through_sequence,
       w.updated_at
FROM (VALUES ($1::uuid)) AS requested(run_id)
LEFT JOIN run_event_retention_watermarks w ON w.run_id = requested.run_id`

type GetRunEventRetentionWatermarkRow struct {
	RunID                   uuid.UUID  `db:"run_id" json:"run_id"`
	RetainedThroughSequence int32      `db:"retained_through_sequence" json:"retained_through_sequence"`
	UpdatedAt               *time.Time `db:"updated_at" json:"updated_at"`
}

// GetRunEventRetentionWatermark returns a synthetic zero row if no retention
// evidence has been written for the Run.
func (q *Queries) GetRunEventRetentionWatermark(ctx context.Context, runID uuid.UUID) (GetRunEventRetentionWatermarkRow, error) {
	var w GetRunEventRetentionWatermarkRow
	err := q.db.QueryRow(ctx, getRunEventRetentionWatermark, runID).Scan(
		&w.RunID,
		&w.RetainedThroughSequence,
		&w.UpdatedAt,
	)
	return w, err
}

const upsertRetentionWatermark = `-- name: UpsertRetentionWatermark :one
WITH target_run AS MATERIALIZED (
    SELECT r.id
    FROM runs r
    WHERE r.id = $1
    FOR UPDATE
),
event_lock AS MATERIALIZED (
    SELECT pg_advisory_xact_lock(hashtextextended(target_run.id::text, 0))
    FROM target_run
)
INSERT INTO run_event_retention_watermarks (
    run_id, retained_through_sequence
)
SELECT $1, $2
FROM event_lock
ON CONFLICT (run_id) DO UPDATE
SET retained_through_sequence = GREATEST(
        run_event_retention_watermarks.retained_through_sequence,
        EXCLUDED.retained_through_sequence
    )
RETURNING run_id, retained_through_sequence, updated_at`

type UpsertRetentionWatermarkParams struct {
	RunID                   uuid.UUID `db:"run_id" json:"run_id"`
	RetainedThroughSequence int32     `db:"retained_through_sequence" json:"retained_through_sequence"`
}

func (q *Queries) UpsertRetentionWatermark(ctx context.Context, arg UpsertRetentionWatermarkParams) (RunEventRetentionWatermark, error) {
	var w RunEventRetentionWatermark
	err := q.db.QueryRow(ctx, upsertRetentionWatermark,
		arg.RunID, arg.RetainedThroughSequence,
	).Scan(&w.RunID, &w.RetainedThroughSequence, &w.UpdatedAt)
	return w, err
}

const listRunEvents = `-- name: ListRunEvents :many
SELECT e.id, e.run_id, e.parent_run_id, e.sequence, e.event_type, e.payload,
       e.created_at, e.client_event_id, e.client_event_seq,
       e.payload_fingerprint, e.attempt_id, e.attempt_no, e.fencing_token
FROM run_events e
LEFT JOIN run_event_retention_watermarks w ON w.run_id = e.run_id
WHERE e.run_id = $1
  AND e.sequence > GREATEST($2, COALESCE(w.retained_through_sequence, 0))
ORDER BY e.sequence ASC
LIMIT $3`

type ListRunEventsParams struct {
	RunID         uuid.UUID `db:"run_id" json:"run_id"`
	AfterSequence int32     `db:"after_sequence" json:"after_sequence"`
	Limit         int32     `db:"limit" json:"limit"`
}

func (q *Queries) ListRunEvents(ctx context.Context, arg ListRunEventsParams) ([]RunEvent, error) {
	return q.listRunEvents(ctx, listRunEvents, arg.RunID, arg.AfterSequence, arg.Limit)
}

const listRunEventsByRun = `-- name: ListRunEventsByRun :many
SELECT e.id, e.run_id, e.parent_run_id, e.sequence, e.event_type, e.payload,
       e.created_at, e.client_event_id, e.client_event_seq,
       e.payload_fingerprint, e.attempt_id, e.attempt_no, e.fencing_token
FROM run_events e
LEFT JOIN run_event_retention_watermarks w ON w.run_id = e.run_id
WHERE e.run_id = $1
  AND e.sequence > GREATEST($2, COALESCE(w.retained_through_sequence, 0))
ORDER BY e.sequence ASC
LIMIT $3`

type ListRunEventsByRunParams struct {
	RunID         uuid.UUID `db:"run_id" json:"run_id"`
	AfterSequence int32     `db:"after_sequence" json:"after_sequence"`
	Limit         int32     `db:"limit" json:"limit"`
}

func (q *Queries) ListRunEventsByRun(ctx context.Context, arg ListRunEventsByRunParams) ([]RunEvent, error) {
	return q.listRunEvents(ctx, listRunEventsByRun, arg.RunID, arg.AfterSequence, arg.Limit)
}

func (q *Queries) listRunEvents(ctx context.Context, query string, runID uuid.UUID, afterSequence, limit int32) ([]RunEvent, error) {
	rows, err := q.db.Query(ctx, query, runID, afterSequence, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []RunEvent
	for rows.Next() {
		var e RunEvent
		if err := scanRunEvent(rows, &e); err != nil {
			return nil, err
		}
		items = append(items, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getRunEventBounds = `-- name: GetRunEventBounds :one
SELECT COALESCE(w.retained_through_sequence, 0)::int AS retained_through_sequence,
       MIN(e.sequence) FILTER (
           WHERE e.sequence > COALESCE(w.retained_through_sequence, 0)
       )::int AS first_available_sequence,
       COALESCE(MAX(e.sequence), 0)::int AS last_sequence
FROM (VALUES ($1::uuid)) AS requested(run_id)
LEFT JOIN run_event_retention_watermarks w ON w.run_id = requested.run_id
LEFT JOIN run_events e ON e.run_id = requested.run_id
GROUP BY requested.run_id, w.retained_through_sequence`

type GetRunEventBoundsRow struct {
	RetainedThroughSequence int32  `db:"retained_through_sequence" json:"retained_through_sequence"`
	FirstAvailableSequence  *int32 `db:"first_available_sequence" json:"first_available_sequence"`
	LastSequence            int32  `db:"last_sequence" json:"last_sequence"`
}

func (q *Queries) GetRunEventBounds(ctx context.Context, runID uuid.UUID) (GetRunEventBoundsRow, error) {
	var bounds GetRunEventBoundsRow
	err := q.db.QueryRow(ctx, getRunEventBounds, runID).Scan(
		&bounds.RetainedThroughSequence,
		&bounds.FirstAvailableSequence,
		&bounds.LastSequence,
	)
	return bounds, err
}

const listClientEventSequencesThrough = `-- name: ListClientEventSequencesThrough :many
SELECT e.client_event_seq
FROM run_events e
WHERE e.run_id = $1
  AND e.attempt_id = $2
  AND e.attempt_no = $3
  AND e.client_event_seq BETWEEN 1 AND $4
ORDER BY e.client_event_seq ASC`

type ListClientEventSequencesThroughParams struct {
	RunID           uuid.UUID `db:"run_id" json:"run_id"`
	AttemptID       uuid.UUID `db:"attempt_id" json:"attempt_id"`
	AttemptNo       int32     `db:"attempt_no" json:"attempt_no"`
	ThroughSequence int64     `db:"through_sequence" json:"through_sequence"`
}

func (q *Queries) ListClientEventSequencesThrough(ctx context.Context, arg ListClientEventSequencesThroughParams) ([]int64, error) {
	rows, err := q.db.Query(ctx, listClientEventSequencesThrough,
		arg.RunID, arg.AttemptID, arg.AttemptNo, arg.ThroughSequence,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sequences []int64
	for rows.Next() {
		var sequence int64
		if err := rows.Scan(&sequence); err != nil {
			return nil, err
		}
		sequences = append(sequences, sequence)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return sequences, nil
}
