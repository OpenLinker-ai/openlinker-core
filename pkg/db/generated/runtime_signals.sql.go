// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/runtime_signals.sql）。

package db

import (
	"context"
	"time"

	"github.com/google/uuid"
)

func scanRuntimeSignal(row interface{ Scan(dest ...any) error }, s *RuntimeSignalOutbox) error {
	return row.Scan(
		&s.ID, &s.EventType, &s.AgentID, &s.RunID, &s.Payload, &s.CreatedAt,
		&s.AvailableAt, &s.Status, &s.LeaseOwner, &s.LeaseExpiresAt,
		&s.PublishedAt, &s.AttemptCount, &s.LastError,
	)
}

const createRuntimeSignal = `-- name: CreateRuntimeSignal :one
INSERT INTO runtime_signal_outbox (event_type, agent_id, run_id, payload, available_at)
VALUES ($1, $2, $3, $4, COALESCE($5, clock_timestamp()))
RETURNING *`

type CreateRuntimeSignalParams struct {
	EventType   string     `db:"event_type" json:"event_type"`
	AgentID     uuid.UUID  `db:"agent_id" json:"agent_id"`
	RunID       *uuid.UUID `db:"run_id" json:"run_id"`
	Payload     []byte     `db:"payload" json:"payload"`
	AvailableAt *time.Time `db:"available_at" json:"available_at"`
}

func (q *Queries) CreateRuntimeSignal(ctx context.Context, arg CreateRuntimeSignalParams) (RuntimeSignalOutbox, error) {
	var signal RuntimeSignalOutbox
	err := scanRuntimeSignal(q.db.QueryRow(ctx, createRuntimeSignal,
		arg.EventType, arg.AgentID, arg.RunID, arg.Payload, arg.AvailableAt,
	), &signal)
	return signal, err
}

const getRuntimeSignalByID = `-- name: GetRuntimeSignalByID :one
SELECT * FROM runtime_signal_outbox WHERE id = $1`

func (q *Queries) GetRuntimeSignalByID(ctx context.Context, id uuid.UUID) (RuntimeSignalOutbox, error) {
	var signal RuntimeSignalOutbox
	err := scanRuntimeSignal(q.db.QueryRow(ctx, getRuntimeSignalByID, id), &signal)
	return signal, err
}

const claimRuntimeSignals = `-- name: ClaimRuntimeSignals :many
WITH candidates AS (
    SELECT id FROM runtime_signal_outbox
    WHERE (status = 'pending' AND available_at <= clock_timestamp())
       OR (status = 'processing' AND lease_expires_at <= clock_timestamp())
    ORDER BY COALESCE(lease_expires_at, available_at), created_at, id
    LIMIT $3 FOR UPDATE SKIP LOCKED
)
UPDATE runtime_signal_outbox s
SET status = 'processing', lease_owner = $1,
    lease_expires_at = clock_timestamp() + ($2::bigint * INTERVAL '1 millisecond'),
    attempt_count = attempt_count + 1
FROM candidates WHERE s.id = candidates.id
RETURNING s.*`

type ClaimRuntimeSignalsParams struct {
	LeaseOwner      uuid.UUID `db:"lease_owner" json:"lease_owner"`
	LeaseDurationMs int64     `db:"lease_duration_ms" json:"lease_duration_ms"`
	Limit           int32     `db:"limit" json:"limit"`
}

func (q *Queries) ClaimRuntimeSignals(ctx context.Context, arg ClaimRuntimeSignalsParams) ([]RuntimeSignalOutbox, error) {
	rows, err := q.db.Query(ctx, claimRuntimeSignals, arg.LeaseOwner, arg.LeaseDurationMs, arg.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []RuntimeSignalOutbox
	for rows.Next() {
		var signal RuntimeSignalOutbox
		if err := scanRuntimeSignal(rows, &signal); err != nil {
			return nil, err
		}
		items = append(items, signal)
	}
	return items, rows.Err()
}

const markRuntimeSignalPublished = `-- name: MarkRuntimeSignalPublished :one
UPDATE runtime_signal_outbox
SET status = 'published', lease_owner = NULL, lease_expires_at = NULL,
    published_at = clock_timestamp(), last_error = NULL
WHERE id = $1 AND status = 'processing' AND lease_owner = $2
RETURNING *`

type MarkRuntimeSignalPublishedParams struct {
	ID         uuid.UUID `db:"id" json:"id"`
	LeaseOwner uuid.UUID `db:"lease_owner" json:"lease_owner"`
}

func (q *Queries) MarkRuntimeSignalPublished(ctx context.Context, arg MarkRuntimeSignalPublishedParams) (RuntimeSignalOutbox, error) {
	var signal RuntimeSignalOutbox
	err := scanRuntimeSignal(q.db.QueryRow(ctx, markRuntimeSignalPublished, arg.ID, arg.LeaseOwner), &signal)
	return signal, err
}

const retryRuntimeSignal = `-- name: RetryRuntimeSignal :one
UPDATE runtime_signal_outbox
SET status = 'pending', lease_owner = NULL, lease_expires_at = NULL,
    available_at = $3, last_error = $4
WHERE id = $1 AND status = 'processing' AND lease_owner = $2
RETURNING *`

type RetryRuntimeSignalParams struct {
	ID          uuid.UUID `db:"id" json:"id"`
	LeaseOwner  uuid.UUID `db:"lease_owner" json:"lease_owner"`
	AvailableAt time.Time `db:"available_at" json:"available_at"`
	LastError   string    `db:"last_error" json:"last_error"`
}

func (q *Queries) RetryRuntimeSignal(ctx context.Context, arg RetryRuntimeSignalParams) (RuntimeSignalOutbox, error) {
	var signal RuntimeSignalOutbox
	err := scanRuntimeSignal(q.db.QueryRow(ctx, retryRuntimeSignal,
		arg.ID, arg.LeaseOwner, arg.AvailableAt, arg.LastError,
	), &signal)
	return signal, err
}

const countPendingRuntimeSignals = `-- name: CountPendingRuntimeSignals :one
SELECT COUNT(*)::int FROM runtime_signal_outbox WHERE status IN ('pending', 'processing')`

func (q *Queries) CountPendingRuntimeSignals(ctx context.Context) (int32, error) {
	var count int32
	err := q.db.QueryRow(ctx, countPendingRuntimeSignals).Scan(&count)
	return count, err
}
