// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/run_cancellations.sql）。

package db

import (
	"context"

	"github.com/google/uuid"
)

func scanRunCancellation(row interface{ Scan(dest ...any) error }, c *RunCancellation) error {
	return row.Scan(
		&c.ID, &c.RunID, &c.TargetAttemptID, &c.State, &c.RequestedByType,
		&c.RequestedByID, &c.Reason, &c.RequestedAt, &c.DeliveredAt,
		&c.StoppingAt, &c.StoppedAt, &c.AcknowledgedAt, &c.ErrorCode, &c.UpdatedAt,
	)
}

const createRunCancellation = `-- name: CreateRunCancellation :one
INSERT INTO run_cancellations (
    id, run_id, target_attempt_id, requested_by_type, requested_by_id, reason
) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (run_id) DO UPDATE SET updated_at = run_cancellations.updated_at
RETURNING *`

type CreateRunCancellationParams struct {
	ID              uuid.UUID  `db:"id" json:"id"`
	RunID           uuid.UUID  `db:"run_id" json:"run_id"`
	TargetAttemptID *uuid.UUID `db:"target_attempt_id" json:"target_attempt_id"`
	RequestedByType string     `db:"requested_by_type" json:"requested_by_type"`
	RequestedByID   uuid.UUID  `db:"requested_by_id" json:"requested_by_id"`
	Reason          *string    `db:"reason" json:"reason"`
}

func (q *Queries) CreateRunCancellation(ctx context.Context, arg CreateRunCancellationParams) (RunCancellation, error) {
	var cancellation RunCancellation
	err := scanRunCancellation(q.db.QueryRow(ctx, createRunCancellation,
		arg.ID, arg.RunID, arg.TargetAttemptID, arg.RequestedByType,
		arg.RequestedByID, arg.Reason,
	), &cancellation)
	return cancellation, err
}

const getRunCancellationByID = `-- name: GetRunCancellationByID :one
SELECT * FROM run_cancellations WHERE id = $1`

func (q *Queries) GetRunCancellationByID(ctx context.Context, id uuid.UUID) (RunCancellation, error) {
	var cancellation RunCancellation
	err := scanRunCancellation(q.db.QueryRow(ctx, getRunCancellationByID, id), &cancellation)
	return cancellation, err
}

const getRunCancellationByRun = `-- name: GetRunCancellationByRun :one
SELECT * FROM run_cancellations WHERE run_id = $1`

func (q *Queries) GetRunCancellationByRun(ctx context.Context, runID uuid.UUID) (RunCancellation, error) {
	var cancellation RunCancellation
	err := scanRunCancellation(q.db.QueryRow(ctx, getRunCancellationByRun, runID), &cancellation)
	return cancellation, err
}

const advanceRunCancellation = `-- name: AdvanceRunCancellation :one
UPDATE run_cancellations
SET state = $3,
    delivered_at = CASE
        WHEN target_attempt_id IS NOT NULL
             AND $3 IN ('delivered', 'stopping', 'stopped', 'unsupported')
            THEN COALESCE(delivered_at, clock_timestamp())
        ELSE delivered_at END,
    stopping_at = CASE
        WHEN target_attempt_id IS NOT NULL AND $3 IN ('stopping', 'stopped')
            THEN COALESCE(stopping_at, clock_timestamp())
        ELSE stopping_at END,
    stopped_at = CASE
        WHEN $3 = 'stopped' THEN COALESCE(stopped_at, clock_timestamp())
        ELSE stopped_at END,
    acknowledged_at = CASE
        WHEN target_attempt_id IS NOT NULL
             AND $3 IN ('stopping', 'stopped', 'unsupported')
            THEN COALESCE(acknowledged_at, clock_timestamp())
        ELSE acknowledged_at END,
    error_code = $4,
    updated_at = clock_timestamp()
WHERE id = $1 AND run_id = $2
RETURNING *`

type AdvanceRunCancellationParams struct {
	ID        uuid.UUID `db:"id" json:"id"`
	RunID     uuid.UUID `db:"run_id" json:"run_id"`
	State     string    `db:"state" json:"state"`
	ErrorCode *string   `db:"error_code" json:"error_code"`
}

func (q *Queries) AdvanceRunCancellation(ctx context.Context, arg AdvanceRunCancellationParams) (RunCancellation, error) {
	var cancellation RunCancellation
	err := scanRunCancellation(q.db.QueryRow(ctx, advanceRunCancellation,
		arg.ID, arg.RunID, arg.State, arg.ErrorCode,
	), &cancellation)
	return cancellation, err
}

const listUnsettledRunCancellations = `-- name: ListUnsettledRunCancellations :many
SELECT * FROM run_cancellations
WHERE state IN ('requested', 'delivered', 'stopping', 'unconfirmed')
ORDER BY updated_at ASC, id ASC LIMIT $1`

const lockUnsettledRunCancellations = `-- name: LockUnsettledRunCancellations :many
SELECT * FROM run_cancellations
WHERE state IN ('requested', 'delivered', 'stopping', 'unconfirmed')
ORDER BY updated_at ASC, id ASC LIMIT $1
FOR UPDATE SKIP LOCKED`

func (q *Queries) ListUnsettledRunCancellations(ctx context.Context, limit int32) ([]RunCancellation, error) {
	return q.listRunCancellations(ctx, listUnsettledRunCancellations, limit)
}

func (q *Queries) LockUnsettledRunCancellations(ctx context.Context, limit int32) ([]RunCancellation, error) {
	return q.listRunCancellations(ctx, lockUnsettledRunCancellations, limit)
}

func (q *Queries) listRunCancellations(ctx context.Context, query string, args ...any) ([]RunCancellation, error) {
	rows, err := q.db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []RunCancellation
	for rows.Next() {
		var cancellation RunCancellation
		if err := scanRunCancellation(rows, &cancellation); err != nil {
			return nil, err
		}
		items = append(items, cancellation)
	}
	return items, rows.Err()
}
