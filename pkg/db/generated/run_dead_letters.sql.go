// Hand-maintained SQL query implementation.
// Do not run sqlc generate against pkg/db/generated; use make sqlc CONFIRM_OVERWRITE=1 only for an intentional migration.

package db

import (
	"context"
	"time"

	"github.com/google/uuid"
)

func scanRunDeadLetter(row interface{ Scan(dest ...any) error }, d *RunDeadLetter) error {
	return row.Scan(&d.ID, &d.RunID, &d.FinalAttemptNo, &d.ReasonCode, &d.ReasonRedacted, &d.CreatedAt)
}

const createRunDeadLetter = `-- name: CreateRunDeadLetter :one
INSERT INTO run_dead_letters (run_id, final_attempt_no, reason_code, reason_redacted)
VALUES ($1, $2, $3, $4)
ON CONFLICT (run_id) DO NOTHING
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
SELECT dlq.id AS dead_letter_id,
       dlq.run_id,
       r.agent_id,
       a.slug AS agent_slug,
       a.name AS agent_name,
       r.status,
       r.dispatch_state,
       r.attempt_count,
       r.max_attempts,
       r.latest_attempt_id AS final_attempt_id,
       dlq.final_attempt_no,
       r.error_code,
       r.error_message,
       final_attempt.error_detail_redacted,
       dlq.reason_code,
       dlq.reason_redacted,
       r.dead_lettered_at,
       dlq.created_at,
       r.replay_of_run_id,
       ARRAY(
           SELECT replay.id
           FROM runs replay
           WHERE replay.replay_of_run_id = r.id
           ORDER BY replay.started_at ASC, replay.id ASC
       )::uuid[] AS replayed_as_run_ids
FROM run_dead_letters dlq
JOIN runs r ON r.id = dlq.run_id
JOIN agents a ON a.id = r.agent_id
LEFT JOIN run_attempts final_attempt ON final_attempt.id = r.latest_attempt_id
ORDER BY dlq.created_at DESC, dlq.id DESC
LIMIT $1 OFFSET $2`

type ListRunDeadLettersParams struct {
	Limit  int32 `db:"limit" json:"limit"`
	Offset int32 `db:"offset" json:"offset"`
}

type ListRunDeadLettersRow struct {
	DeadLetterID        uuid.UUID   `db:"dead_letter_id" json:"dead_letter_id"`
	RunID               uuid.UUID   `db:"run_id" json:"run_id"`
	AgentID             uuid.UUID   `db:"agent_id" json:"agent_id"`
	AgentSlug           string      `db:"agent_slug" json:"agent_slug"`
	AgentName           string      `db:"agent_name" json:"agent_name"`
	Status              string      `db:"status" json:"status"`
	DispatchState       string      `db:"dispatch_state" json:"dispatch_state"`
	AttemptCount        int32       `db:"attempt_count" json:"attempt_count"`
	MaxAttempts         int32       `db:"max_attempts" json:"max_attempts"`
	FinalAttemptID      *uuid.UUID  `db:"final_attempt_id" json:"final_attempt_id"`
	FinalAttemptNo      int32       `db:"final_attempt_no" json:"final_attempt_no"`
	ErrorCode           *string     `db:"error_code" json:"error_code"`
	ErrorMessage        *string     `db:"error_message" json:"error_message"`
	ErrorDetailRedacted *string     `db:"error_detail_redacted" json:"error_detail_redacted"`
	ReasonCode          string      `db:"reason_code" json:"reason_code"`
	ReasonRedacted      *string     `db:"reason_redacted" json:"reason_redacted"`
	DeadLetteredAt      *time.Time  `db:"dead_lettered_at" json:"dead_lettered_at"`
	CreatedAt           time.Time   `db:"created_at" json:"created_at"`
	ReplayOfRunID       *uuid.UUID  `db:"replay_of_run_id" json:"replay_of_run_id"`
	ReplayedAsRunIDs    []uuid.UUID `db:"replayed_as_run_ids" json:"replayed_as_run_ids"`
}

func (q *Queries) ListRunDeadLetters(ctx context.Context, arg ListRunDeadLettersParams) ([]ListRunDeadLettersRow, error) {
	rows, err := q.db.Query(ctx, listRunDeadLetters, arg.Limit, arg.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []ListRunDeadLettersRow
	for rows.Next() {
		var item ListRunDeadLettersRow
		if err := rows.Scan(
			&item.DeadLetterID,
			&item.RunID,
			&item.AgentID,
			&item.AgentSlug,
			&item.AgentName,
			&item.Status,
			&item.DispatchState,
			&item.AttemptCount,
			&item.MaxAttempts,
			&item.FinalAttemptID,
			&item.FinalAttemptNo,
			&item.ErrorCode,
			&item.ErrorMessage,
			&item.ErrorDetailRedacted,
			&item.ReasonCode,
			&item.ReasonRedacted,
			&item.DeadLetteredAt,
			&item.CreatedAt,
			&item.ReplayOfRunID,
			&item.ReplayedAsRunIDs,
		); err != nil {
			return nil, err
		}
		items = append(items, item)
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
