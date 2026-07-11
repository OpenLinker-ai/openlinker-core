// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/run_effects.sql）。

package db

import (
	"context"
	"time"

	"github.com/google/uuid"
)

func scanRunEffect(row interface{ Scan(dest ...any) error }, e *RunEffectOutbox) error {
	return row.Scan(
		&e.ID, &e.RunID, &e.TerminalEventID, &e.EffectType, &e.TargetKey,
		&e.Metadata, &e.Status, &e.AvailableAt, &e.LeaseOwner, &e.LeaseExpiresAt,
		&e.AttemptCount, &e.MaxAttempts, &e.CompletedAt, &e.LastError, &e.CreatedAt,
	)
}

func scanRunAccountingLedger(row interface{ Scan(dest ...any) error }, l *RunAccountingLedger) error {
	return row.Scan(
		&l.RunID, &l.TerminalEventID, &l.AgentID, &l.SuccessDelta,
		&l.RevenueDeltaCents, &l.CreatedAt,
	)
}

const createRunEffect = `-- name: CreateRunEffect :one
INSERT INTO run_effect_outbox (
    id, run_id, terminal_event_id, effect_type, target_key, metadata,
    available_at, max_attempts
) VALUES ($1, $2, $3, $4, $5, COALESCE($6, '{}'::jsonb),
          COALESCE($7, clock_timestamp()), $8)
ON CONFLICT (run_id, effect_type, target_key) DO UPDATE
SET run_id = run_effect_outbox.run_id
RETURNING *`

type CreateRunEffectParams struct {
	ID              uuid.UUID  `db:"id" json:"id"`
	RunID           uuid.UUID  `db:"run_id" json:"run_id"`
	TerminalEventID uuid.UUID  `db:"terminal_event_id" json:"terminal_event_id"`
	EffectType      string     `db:"effect_type" json:"effect_type"`
	TargetKey       string     `db:"target_key" json:"target_key"`
	Metadata        []byte     `db:"metadata" json:"metadata"`
	AvailableAt     *time.Time `db:"available_at" json:"available_at"`
	MaxAttempts     int32      `db:"max_attempts" json:"max_attempts"`
}

func (q *Queries) CreateRunEffect(ctx context.Context, arg CreateRunEffectParams) (RunEffectOutbox, error) {
	var effect RunEffectOutbox
	err := scanRunEffect(q.db.QueryRow(ctx, createRunEffect,
		arg.ID, arg.RunID, arg.TerminalEventID, arg.EffectType, arg.TargetKey,
		arg.Metadata, arg.AvailableAt, arg.MaxAttempts,
	), &effect)
	return effect, err
}

const getRunEffectByID = `-- name: GetRunEffectByID :one
SELECT * FROM run_effect_outbox WHERE id = $1`

func (q *Queries) GetRunEffectByID(ctx context.Context, id uuid.UUID) (RunEffectOutbox, error) {
	var effect RunEffectOutbox
	err := scanRunEffect(q.db.QueryRow(ctx, getRunEffectByID, id), &effect)
	return effect, err
}

const listRunEffectsByRun = `-- name: ListRunEffectsByRun :many
SELECT * FROM run_effect_outbox WHERE run_id = $1 ORDER BY created_at ASC, id ASC`

func (q *Queries) ListRunEffectsByRun(ctx context.Context, runID uuid.UUID) ([]RunEffectOutbox, error) {
	rows, err := q.db.Query(ctx, listRunEffectsByRun, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []RunEffectOutbox
	for rows.Next() {
		var effect RunEffectOutbox
		if err := scanRunEffect(rows, &effect); err != nil {
			return nil, err
		}
		items = append(items, effect)
	}
	return items, rows.Err()
}

const claimRunEffects = `-- name: ClaimRunEffects :many
WITH candidates AS (
    SELECT id FROM run_effect_outbox
    WHERE attempt_count < max_attempts
      AND ((status = 'pending' AND available_at <= clock_timestamp())
        OR (status = 'processing' AND lease_expires_at <= clock_timestamp()))
    ORDER BY COALESCE(lease_expires_at, available_at), created_at, id
    LIMIT $3 FOR UPDATE SKIP LOCKED
)
UPDATE run_effect_outbox e
SET status = 'processing', lease_owner = $1,
    lease_expires_at = clock_timestamp() + ($2::bigint * INTERVAL '1 millisecond'),
    attempt_count = attempt_count + 1
FROM candidates WHERE e.id = candidates.id
RETURNING e.*`

type ClaimRunEffectsParams struct {
	LeaseOwner      uuid.UUID `db:"lease_owner" json:"lease_owner"`
	LeaseDurationMs int64     `db:"lease_duration_ms" json:"lease_duration_ms"`
	Limit           int32     `db:"limit" json:"limit"`
}

func (q *Queries) ClaimRunEffects(ctx context.Context, arg ClaimRunEffectsParams) ([]RunEffectOutbox, error) {
	rows, err := q.db.Query(ctx, claimRunEffects, arg.LeaseOwner, arg.LeaseDurationMs, arg.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []RunEffectOutbox
	for rows.Next() {
		var effect RunEffectOutbox
		if err := scanRunEffect(rows, &effect); err != nil {
			return nil, err
		}
		items = append(items, effect)
	}
	return items, rows.Err()
}

const deadLetterExpiredRunEffectsAtLimit = `-- name: DeadLetterExpiredRunEffectsAtLimit :many
UPDATE run_effect_outbox
SET status = 'dead_letter', lease_owner = NULL, lease_expires_at = NULL,
    last_error = COALESCE(last_error, 'processing lease expired at retry limit')
WHERE status = 'processing' AND lease_expires_at <= clock_timestamp()
  AND attempt_count >= max_attempts
RETURNING *`

func (q *Queries) DeadLetterExpiredRunEffectsAtLimit(ctx context.Context) ([]RunEffectOutbox, error) {
	rows, err := q.db.Query(ctx, deadLetterExpiredRunEffectsAtLimit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []RunEffectOutbox
	for rows.Next() {
		var effect RunEffectOutbox
		if err := scanRunEffect(rows, &effect); err != nil {
			return nil, err
		}
		items = append(items, effect)
	}
	return items, rows.Err()
}

const markRunEffectSucceeded = `-- name: MarkRunEffectSucceeded :one
UPDATE run_effect_outbox
SET status = 'succeeded', lease_owner = NULL, lease_expires_at = NULL,
    completed_at = clock_timestamp(), last_error = NULL
WHERE id = $1 AND status = 'processing' AND lease_owner = $2
RETURNING *`

type MarkRunEffectSucceededParams struct {
	ID         uuid.UUID `db:"id" json:"id"`
	LeaseOwner uuid.UUID `db:"lease_owner" json:"lease_owner"`
}

func (q *Queries) MarkRunEffectSucceeded(ctx context.Context, arg MarkRunEffectSucceededParams) (RunEffectOutbox, error) {
	var effect RunEffectOutbox
	err := scanRunEffect(q.db.QueryRow(ctx, markRunEffectSucceeded, arg.ID, arg.LeaseOwner), &effect)
	return effect, err
}

const retryOrDeadLetterRunEffect = `-- name: RetryOrDeadLetterRunEffect :one
UPDATE run_effect_outbox
SET status = CASE WHEN attempt_count >= max_attempts THEN 'dead_letter' ELSE 'pending' END,
    lease_owner = NULL, lease_expires_at = NULL, available_at = $3, last_error = $4
WHERE id = $1 AND status = 'processing' AND lease_owner = $2
RETURNING *`

type RetryOrDeadLetterRunEffectParams struct {
	ID          uuid.UUID `db:"id" json:"id"`
	LeaseOwner  uuid.UUID `db:"lease_owner" json:"lease_owner"`
	AvailableAt time.Time `db:"available_at" json:"available_at"`
	LastError   string    `db:"last_error" json:"last_error"`
}

func (q *Queries) RetryOrDeadLetterRunEffect(ctx context.Context, arg RetryOrDeadLetterRunEffectParams) (RunEffectOutbox, error) {
	var effect RunEffectOutbox
	err := scanRunEffect(q.db.QueryRow(ctx, retryOrDeadLetterRunEffect,
		arg.ID, arg.LeaseOwner, arg.AvailableAt, arg.LastError,
	), &effect)
	return effect, err
}

const replayRunEffect = `-- name: ReplayRunEffect :one
UPDATE run_effect_outbox
SET status = 'pending', available_at = clock_timestamp(), attempt_count = 0,
    completed_at = NULL, last_error = NULL
WHERE id = $1 AND status = 'dead_letter'
RETURNING *`

func (q *Queries) ReplayRunEffect(ctx context.Context, id uuid.UUID) (RunEffectOutbox, error) {
	var effect RunEffectOutbox
	err := scanRunEffect(q.db.QueryRow(ctx, replayRunEffect, id), &effect)
	return effect, err
}

const insertRunAccountingLedger = `-- name: InsertRunAccountingLedger :one
INSERT INTO run_accounting_ledger (
    run_id, terminal_event_id, agent_id, success_delta, revenue_delta_cents
) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (run_id) DO NOTHING
RETURNING *`

type InsertRunAccountingLedgerParams struct {
	RunID             uuid.UUID `db:"run_id" json:"run_id"`
	TerminalEventID   uuid.UUID `db:"terminal_event_id" json:"terminal_event_id"`
	AgentID           uuid.UUID `db:"agent_id" json:"agent_id"`
	SuccessDelta      int32     `db:"success_delta" json:"success_delta"`
	RevenueDeltaCents int64     `db:"revenue_delta_cents" json:"revenue_delta_cents"`
}

func (q *Queries) InsertRunAccountingLedger(ctx context.Context, arg InsertRunAccountingLedgerParams) (RunAccountingLedger, error) {
	var ledger RunAccountingLedger
	err := scanRunAccountingLedger(q.db.QueryRow(ctx, insertRunAccountingLedger,
		arg.RunID, arg.TerminalEventID, arg.AgentID, arg.SuccessDelta,
		arg.RevenueDeltaCents,
	), &ledger)
	return ledger, err
}

const getRunAccountingLedger = `-- name: GetRunAccountingLedger :one
SELECT * FROM run_accounting_ledger WHERE run_id = $1`

func (q *Queries) GetRunAccountingLedger(ctx context.Context, runID uuid.UUID) (RunAccountingLedger, error) {
	var ledger RunAccountingLedger
	err := scanRunAccountingLedger(q.db.QueryRow(ctx, getRunAccountingLedger, runID), &ledger)
	return ledger, err
}
