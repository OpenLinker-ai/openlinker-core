// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/run_dead_letters.sql）。

package db

import (
	"context"

	"github.com/google/uuid"
)

func scanRunDeadLetter(row interface{ Scan(dest ...any) error }, d *RunDeadLetter) error {
	return row.Scan(&d.ID, &d.RunID, &d.FinalAttemptNo, &d.ReasonCode, &d.ReasonRedacted, &d.CreatedAt)
}

const createRunDeadLetter = `-- name: CreateRunDeadLetter :one
INSERT INTO run_dead_letters (run_id, final_attempt_no, reason_code, reason_redacted)
VALUES ($1, $2, $3, $4)
ON CONFLICT (run_id) DO UPDATE SET run_id = run_dead_letters.run_id
RETURNING *`

type CreateRunDeadLetterParams struct {
	RunID          uuid.UUID `db:"run_id" json:"run_id"`
	FinalAttemptNo int32     `db:"final_attempt_no" json:"final_attempt_no"`
	ReasonCode     string    `db:"reason_code" json:"reason_code"`
	ReasonRedacted *string   `db:"reason_redacted" json:"reason_redacted"`
}

func (q *Queries) CreateRunDeadLetter(ctx context.Context, arg CreateRunDeadLetterParams) (RunDeadLetter, error) {
	var deadLetter RunDeadLetter
	err := scanRunDeadLetter(q.db.QueryRow(ctx, createRunDeadLetter,
		arg.RunID, arg.FinalAttemptNo, arg.ReasonCode, arg.ReasonRedacted,
	), &deadLetter)
	return deadLetter, err
}

const getRunDeadLetterByRun = `-- name: GetRunDeadLetterByRun :one
SELECT * FROM run_dead_letters WHERE run_id = $1`

func (q *Queries) GetRunDeadLetterByRun(ctx context.Context, runID uuid.UUID) (RunDeadLetter, error) {
	var deadLetter RunDeadLetter
	err := scanRunDeadLetter(q.db.QueryRow(ctx, getRunDeadLetterByRun, runID), &deadLetter)
	return deadLetter, err
}

const listRunDeadLetters = `-- name: ListRunDeadLetters :many
SELECT * FROM run_dead_letters
ORDER BY created_at DESC, id DESC LIMIT $1 OFFSET $2`

type ListRunDeadLettersParams struct {
	Limit  int32 `db:"limit" json:"limit"`
	Offset int32 `db:"offset" json:"offset"`
}

func (q *Queries) ListRunDeadLetters(ctx context.Context, arg ListRunDeadLettersParams) ([]RunDeadLetter, error) {
	rows, err := q.db.Query(ctx, listRunDeadLetters, arg.Limit, arg.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []RunDeadLetter
	for rows.Next() {
		var deadLetter RunDeadLetter
		if err := scanRunDeadLetter(rows, &deadLetter); err != nil {
			return nil, err
		}
		items = append(items, deadLetter)
	}
	return items, rows.Err()
}

const countRunDeadLetters = `-- name: CountRunDeadLetters :one
SELECT COUNT(*)::int FROM run_dead_letters`

func (q *Queries) CountRunDeadLetters(ctx context.Context) (int32, error) {
	var count int32
	err := q.db.QueryRow(ctx, countRunDeadLetters).Scan(&count)
	return count, err
}
