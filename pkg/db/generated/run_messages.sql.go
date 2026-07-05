// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/run_messages.sql）。

package db

import (
	"context"

	"github.com/google/uuid"
)

func scanRunMessage(row interface {
	Scan(dest ...any) error
}, m *RunMessage) error {
	return row.Scan(
		&m.ID,
		&m.RunID,
		&m.EventSequence,
		&m.Role,
		&m.Content,
		&m.Payload,
		&m.CreatedAt,
	)
}

const createRunMessage = `-- name: CreateRunMessage :one
INSERT INTO run_messages (
    run_id, event_sequence, role, content, payload
) VALUES (
    $1, $2, $3, $4, $5
)
RETURNING id, run_id, event_sequence, role, content, payload, created_at`

type CreateRunMessageParams struct {
	RunID         uuid.UUID `db:"run_id" json:"run_id"`
	EventSequence *int32    `db:"event_sequence" json:"event_sequence"`
	Role          string    `db:"role" json:"role"`
	Content       string    `db:"content" json:"content"`
	Payload       []byte    `db:"payload" json:"payload"`
}

func (q *Queries) CreateRunMessage(ctx context.Context, arg CreateRunMessageParams) (RunMessage, error) {
	row := q.db.QueryRow(ctx, createRunMessage,
		arg.RunID,
		arg.EventSequence,
		arg.Role,
		arg.Content,
		arg.Payload,
	)
	var m RunMessage
	err := scanRunMessage(row, &m)
	return m, err
}

const listRunMessagesByRun = `-- name: ListRunMessagesByRun :many
SELECT id, run_id, event_sequence, role, content, payload, created_at
FROM run_messages
WHERE run_id = $1
ORDER BY created_at ASC, id ASC`

func (q *Queries) ListRunMessagesByRun(ctx context.Context, runID uuid.UUID) ([]RunMessage, error) {
	rows, err := q.db.Query(ctx, listRunMessagesByRun, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []RunMessage
	for rows.Next() {
		var m RunMessage
		if err := scanRunMessage(rows, &m); err != nil {
			return nil, err
		}
		items = append(items, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const listRunMessagesByRuns = `-- name: ListRunMessagesByRuns :many
SELECT id, run_id, event_sequence, role, content, payload, created_at
FROM run_messages
WHERE run_id = ANY($1::uuid[])
ORDER BY run_id ASC, created_at ASC, id ASC`

func (q *Queries) ListRunMessagesByRuns(ctx context.Context, runIDs []uuid.UUID) ([]RunMessage, error) {
	rows, err := q.db.Query(ctx, listRunMessagesByRuns, runIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []RunMessage
	for rows.Next() {
		var m RunMessage
		if err := scanRunMessage(rows, &m); err != nil {
			return nil, err
		}
		items = append(items, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
