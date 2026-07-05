// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/runs.sql 的 run_events queries）。

package db

import (
	"context"

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
	)
}

const lockRunEventSequence = `-- name: LockRunEventSequence :exec
SELECT pg_advisory_xact_lock(hashtextextended($1::uuid::text, 0))`

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
RETURNING id, run_id, parent_run_id, sequence, event_type, payload, created_at`

// CreateRunEventParams 入参。
//
// ParentRunID 预留给后续 workflow / A2A child run；当前单 Agent run 一般为空。
type CreateRunEventParams struct {
	RunID       uuid.UUID  `db:"run_id" json:"run_id"`
	ParentRunID *uuid.UUID `db:"parent_run_id" json:"parent_run_id"`
	EventType   string     `db:"event_type" json:"event_type"`
	Payload     []byte     `db:"payload" json:"payload"`
}

// CreateRunEvent 追加 run event，并在单个 run 内分配递增 sequence。
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

const listRunEventsByRun = `-- name: ListRunEventsByRun :many
SELECT id, run_id, parent_run_id, sequence, event_type, payload, created_at
FROM run_events
WHERE run_id = $1 AND sequence > $2
ORDER BY sequence ASC
LIMIT $3`

// ListRunEventsByRunParams 入参。
type ListRunEventsByRunParams struct {
	RunID         uuid.UUID `db:"run_id" json:"run_id"`
	AfterSequence int32     `db:"after_sequence" json:"after_sequence"`
	Limit         int32     `db:"limit" json:"limit"`
}

// ListRunEventsByRun 按 run 内 sequence 正序读取事件。
func (q *Queries) ListRunEventsByRun(ctx context.Context, arg ListRunEventsByRunParams) ([]RunEvent, error) {
	rows, err := q.db.Query(ctx, listRunEventsByRun, arg.RunID, arg.AfterSequence, arg.Limit)
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
