// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/run_attempts.sql）。

package db

import (
	"context"
	"time"

	"github.com/google/uuid"
)

func scanRunAttempt(row interface{ Scan(dest ...any) error }, a *RunAttempt) error {
	return row.Scan(
		&a.ID, &a.RunID, &a.AgentID, &a.OfferNo, &a.AttemptNo,
		&a.ExecutorType, &a.LeaseID, &a.FencingToken, &a.RuntimeTokenID,
		&a.RuntimeWorkerID, &a.RuntimeSessionID, &a.NodeID,
		&a.OfferedByCoreInstanceID, &a.AttachedCoreInstanceID, &a.OfferedAt,
		&a.OfferExpiresAt, &a.AcceptedAt, &a.LastRenewedAt, &a.LeaseExpiresAt,
		&a.AttemptDeadlineAt, &a.FinishedAt, &a.Outcome, &a.ResultID,
		&a.ResultFingerprint, &a.ResultClassification, &a.ResultAcknowledgedAt,
		&a.LastClientEventSeq, &a.FinalClientEventSeq, &a.ErrorCode,
		&a.ErrorDetailRedacted, &a.CreatedAt,
	)
}

const lockNextPendingRuntimeRun = `-- name: LockNextPendingRuntimeRun :one
SELECT id
FROM runs
WHERE status = 'running'
  AND dispatch_state = 'pending'
ORDER BY started_at ASC, id ASC
LIMIT 1
FOR UPDATE SKIP LOCKED`

func (q *Queries) LockNextPendingRuntimeRun(ctx context.Context) (uuid.UUID, error) {
	row := q.db.QueryRow(ctx, lockNextPendingRuntimeRun)
	var id uuid.UUID
	err := row.Scan(&id)
	return id, err
}

const lockNextDueRetryRuntimeRun = `-- name: LockNextDueRetryRuntimeRun :one
SELECT id
FROM runs
WHERE status = 'running'
  AND dispatch_state = 'retry_wait'
  AND next_attempt_at <= clock_timestamp()
ORDER BY next_attempt_at ASC, started_at ASC, id ASC
LIMIT 1
FOR UPDATE SKIP LOCKED`

func (q *Queries) LockNextDueRetryRuntimeRun(ctx context.Context) (uuid.UUID, error) {
	row := q.db.QueryRow(ctx, lockNextDueRetryRuntimeRun)
	var id uuid.UUID
	err := row.Scan(&id)
	return id, err
}

const createRunAttempt = `-- name: CreateRunAttempt :one
INSERT INTO run_attempts (
    id, run_id, agent_id, offer_no, executor_type, lease_id, fencing_token,
    runtime_token_id, runtime_worker_id, runtime_session_id, node_id,
    offered_by_core_instance_id, attached_core_instance_id,
    offer_expires_at, lease_expires_at, attempt_deadline_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7,
    $8, $9, $10, $11, $12, $13, $14, $15, $16
)
RETURNING *`

type CreateRunAttemptParams struct {
	ID                      uuid.UUID  `db:"id" json:"id"`
	RunID                   uuid.UUID  `db:"run_id" json:"run_id"`
	AgentID                 uuid.UUID  `db:"agent_id" json:"agent_id"`
	OfferNo                 int32      `db:"offer_no" json:"offer_no"`
	ExecutorType            string     `db:"executor_type" json:"executor_type"`
	LeaseID                 uuid.UUID  `db:"lease_id" json:"lease_id"`
	FencingToken            int64      `db:"fencing_token" json:"fencing_token"`
	RuntimeTokenID          *uuid.UUID `db:"runtime_token_id" json:"runtime_token_id"`
	RuntimeWorkerID         *string    `db:"runtime_worker_id" json:"runtime_worker_id"`
	RuntimeSessionID        *uuid.UUID `db:"runtime_session_id" json:"runtime_session_id"`
	NodeID                  *uuid.UUID `db:"node_id" json:"node_id"`
	OfferedByCoreInstanceID uuid.UUID  `db:"offered_by_core_instance_id" json:"offered_by_core_instance_id"`
	AttachedCoreInstanceID  uuid.UUID  `db:"attached_core_instance_id" json:"attached_core_instance_id"`
	OfferExpiresAt          time.Time  `db:"offer_expires_at" json:"offer_expires_at"`
	LeaseExpiresAt          time.Time  `db:"lease_expires_at" json:"lease_expires_at"`
	AttemptDeadlineAt       time.Time  `db:"attempt_deadline_at" json:"attempt_deadline_at"`
}

func (q *Queries) CreateRunAttempt(ctx context.Context, arg CreateRunAttemptParams) (RunAttempt, error) {
	row := q.db.QueryRow(ctx, createRunAttempt,
		arg.ID, arg.RunID, arg.AgentID, arg.OfferNo, arg.ExecutorType,
		arg.LeaseID, arg.FencingToken, arg.RuntimeTokenID, arg.RuntimeWorkerID,
		arg.RuntimeSessionID, arg.NodeID, arg.OfferedByCoreInstanceID,
		arg.AttachedCoreInstanceID, arg.OfferExpiresAt, arg.LeaseExpiresAt,
		arg.AttemptDeadlineAt,
	)
	var attempt RunAttempt
	err := scanRunAttempt(row, &attempt)
	return attempt, err
}

const getRunAttemptByID = `-- name: GetRunAttemptByID :one
SELECT * FROM run_attempts WHERE id = $1`

func (q *Queries) GetRunAttemptByID(ctx context.Context, id uuid.UUID) (RunAttempt, error) {
	var attempt RunAttempt
	err := scanRunAttempt(q.db.QueryRow(ctx, getRunAttemptByID, id), &attempt)
	return attempt, err
}

const lockRunAttemptForResult = `-- name: LockRunAttemptForResult :one
SELECT *
FROM run_attempts
WHERE run_id = $1 AND id = $2
FOR UPDATE`

type LockRunAttemptForResultParams struct {
	RunID uuid.UUID `db:"run_id" json:"run_id"`
	ID    uuid.UUID `db:"id" json:"id"`
}

func (q *Queries) LockRunAttemptForResult(ctx context.Context, arg LockRunAttemptForResultParams) (RunAttempt, error) {
	var attempt RunAttempt
	err := scanRunAttempt(q.db.QueryRow(ctx, lockRunAttemptForResult, arg.RunID, arg.ID), &attempt)
	return attempt, err
}

const getRunAttemptByLeaseID = `-- name: GetRunAttemptByLeaseID :one
SELECT * FROM run_attempts WHERE lease_id = $1`

func (q *Queries) GetRunAttemptByLeaseID(ctx context.Context, leaseID uuid.UUID) (RunAttempt, error) {
	var attempt RunAttempt
	err := scanRunAttempt(q.db.QueryRow(ctx, getRunAttemptByLeaseID, leaseID), &attempt)
	return attempt, err
}

const getRunAttemptByResultID = `-- name: GetRunAttemptByResultID :one
SELECT *
FROM run_attempts
WHERE run_id = $1 AND result_id = $2`

type GetRunAttemptByResultIDParams struct {
	RunID    uuid.UUID `db:"run_id" json:"run_id"`
	ResultID uuid.UUID `db:"result_id" json:"result_id"`
}

func (q *Queries) GetRunAttemptByResultID(ctx context.Context, arg GetRunAttemptByResultIDParams) (RunAttempt, error) {
	var attempt RunAttempt
	err := scanRunAttempt(q.db.QueryRow(ctx, getRunAttemptByResultID, arg.RunID, arg.ResultID), &attempt)
	return attempt, err
}

const getRunAttemptByIdentity = `-- name: GetRunAttemptByIdentity :one
SELECT * FROM run_attempts
WHERE run_id = $1 AND id = $2 AND lease_id = $3 AND fencing_token = $4`

type GetRunAttemptByIdentityParams struct {
	RunID        uuid.UUID `db:"run_id" json:"run_id"`
	ID           uuid.UUID `db:"id" json:"id"`
	LeaseID      uuid.UUID `db:"lease_id" json:"lease_id"`
	FencingToken int64     `db:"fencing_token" json:"fencing_token"`
}

func (q *Queries) GetRunAttemptByIdentity(ctx context.Context, arg GetRunAttemptByIdentityParams) (RunAttempt, error) {
	var attempt RunAttempt
	err := scanRunAttempt(q.db.QueryRow(ctx, getRunAttemptByIdentity, arg.RunID, arg.ID, arg.LeaseID, arg.FencingToken), &attempt)
	return attempt, err
}

const getActiveRunAttemptByRun = `-- name: GetActiveRunAttemptByRun :one
SELECT a.* FROM run_attempts a
JOIN runs r ON r.id = a.run_id AND r.active_attempt_id = a.id
WHERE a.run_id = $1`

func (q *Queries) GetActiveRunAttemptByRun(ctx context.Context, runID uuid.UUID) (RunAttempt, error) {
	var attempt RunAttempt
	err := scanRunAttempt(q.db.QueryRow(ctx, getActiveRunAttemptByRun, runID), &attempt)
	return attempt, err
}

const listRunAttemptsByRun = `-- name: ListRunAttemptsByRun :many
SELECT * FROM run_attempts WHERE run_id = $1 ORDER BY offer_no ASC, id ASC`

func (q *Queries) ListRunAttemptsByRun(ctx context.Context, runID uuid.UUID) ([]RunAttempt, error) {
	rows, err := q.db.Query(ctx, listRunAttemptsByRun, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []RunAttempt
	for rows.Next() {
		var attempt RunAttempt
		if err := scanRunAttempt(rows, &attempt); err != nil {
			return nil, err
		}
		items = append(items, attempt)
	}
	return items, rows.Err()
}

const acceptRunAttempt = `-- name: AcceptRunAttempt :one
UPDATE run_attempts
SET attempt_no = $5, accepted_at = clock_timestamp(),
    last_renewed_at = clock_timestamp(), lease_expires_at = $6,
    attached_core_instance_id = $7
WHERE run_id = $1 AND id = $2 AND lease_id = $3 AND fencing_token = $4
  AND accepted_at IS NULL AND finished_at IS NULL
  AND offer_expires_at >= clock_timestamp()
RETURNING *`

type AcceptRunAttemptParams struct {
	RunID                  uuid.UUID `db:"run_id" json:"run_id"`
	ID                     uuid.UUID `db:"id" json:"id"`
	LeaseID                uuid.UUID `db:"lease_id" json:"lease_id"`
	FencingToken           int64     `db:"fencing_token" json:"fencing_token"`
	AttemptNo              int32     `db:"attempt_no" json:"attempt_no"`
	LeaseExpiresAt         time.Time `db:"lease_expires_at" json:"lease_expires_at"`
	AttachedCoreInstanceID uuid.UUID `db:"attached_core_instance_id" json:"attached_core_instance_id"`
}

func (q *Queries) AcceptRunAttempt(ctx context.Context, arg AcceptRunAttemptParams) (RunAttempt, error) {
	var attempt RunAttempt
	err := scanRunAttempt(q.db.QueryRow(ctx, acceptRunAttempt,
		arg.RunID, arg.ID, arg.LeaseID, arg.FencingToken, arg.AttemptNo,
		arg.LeaseExpiresAt, arg.AttachedCoreInstanceID,
	), &attempt)
	return attempt, err
}

const renewRunAttempt = `-- name: RenewRunAttempt :one
UPDATE run_attempts
SET last_renewed_at = clock_timestamp(), lease_expires_at = $5,
    attached_core_instance_id = $6
WHERE run_id = $1 AND id = $2 AND lease_id = $3 AND fencing_token = $4
  AND accepted_at IS NOT NULL AND finished_at IS NULL
  AND lease_expires_at >= clock_timestamp()
RETURNING *`

type RenewRunAttemptParams struct {
	RunID                  uuid.UUID `db:"run_id" json:"run_id"`
	ID                     uuid.UUID `db:"id" json:"id"`
	LeaseID                uuid.UUID `db:"lease_id" json:"lease_id"`
	FencingToken           int64     `db:"fencing_token" json:"fencing_token"`
	LeaseExpiresAt         time.Time `db:"lease_expires_at" json:"lease_expires_at"`
	AttachedCoreInstanceID uuid.UUID `db:"attached_core_instance_id" json:"attached_core_instance_id"`
}

func (q *Queries) RenewRunAttempt(ctx context.Context, arg RenewRunAttemptParams) (RunAttempt, error) {
	var attempt RunAttempt
	err := scanRunAttempt(q.db.QueryRow(ctx, renewRunAttempt,
		arg.RunID, arg.ID, arg.LeaseID, arg.FencingToken, arg.LeaseExpiresAt,
		arg.AttachedCoreInstanceID,
	), &attempt)
	return attempt, err
}

const advanceRunAttemptEventSequence = `-- name: AdvanceRunAttemptEventSequence :one
UPDATE run_attempts
SET last_client_event_seq = GREATEST(last_client_event_seq, $5)
WHERE run_id = $1 AND id = $2 AND lease_id = $3 AND fencing_token = $4
  AND accepted_at IS NOT NULL AND finished_at IS NULL AND result_id IS NULL
RETURNING *`

type AdvanceRunAttemptEventSequenceParams struct {
	RunID          uuid.UUID `db:"run_id" json:"run_id"`
	ID             uuid.UUID `db:"id" json:"id"`
	LeaseID        uuid.UUID `db:"lease_id" json:"lease_id"`
	FencingToken   int64     `db:"fencing_token" json:"fencing_token"`
	ClientEventSeq int64     `db:"client_event_seq" json:"client_event_seq"`
}

func (q *Queries) AdvanceRunAttemptEventSequence(ctx context.Context, arg AdvanceRunAttemptEventSequenceParams) (RunAttempt, error) {
	var attempt RunAttempt
	err := scanRunAttempt(q.db.QueryRow(ctx, advanceRunAttemptEventSequence,
		arg.RunID, arg.ID, arg.LeaseID, arg.FencingToken, arg.ClientEventSeq,
	), &attempt)
	return attempt, err
}

const finishRunAttempt = `-- name: FinishRunAttempt :one
UPDATE run_attempts
SET finished_at = clock_timestamp(), outcome = $5, result_id = $6,
    result_fingerprint = $7, result_classification = $8,
    final_client_event_seq = $9, error_code = $10, error_detail_redacted = $11,
    result_acknowledged_at = clock_timestamp()
WHERE run_id = $1 AND id = $2 AND lease_id = $3 AND fencing_token = $4
  AND accepted_at IS NOT NULL AND finished_at IS NULL AND result_id IS NULL
  AND $6::uuid IS NOT NULL AND $7::bytea IS NOT NULL
  AND $8::text IS NOT NULL AND $9::bigint IS NOT NULL
RETURNING *`

type FinishRunAttemptParams struct {
	RunID                uuid.UUID `db:"run_id" json:"run_id"`
	ID                   uuid.UUID `db:"id" json:"id"`
	LeaseID              uuid.UUID `db:"lease_id" json:"lease_id"`
	FencingToken         int64     `db:"fencing_token" json:"fencing_token"`
	Outcome              string    `db:"outcome" json:"outcome"`
	ResultID             uuid.UUID `db:"result_id" json:"result_id"`
	ResultFingerprint    []byte    `db:"result_fingerprint" json:"-"`
	ResultClassification string    `db:"result_classification" json:"result_classification"`
	FinalClientEventSeq  int64     `db:"final_client_event_seq" json:"final_client_event_seq"`
	ErrorCode            *string   `db:"error_code" json:"error_code"`
	ErrorDetailRedacted  *string   `db:"error_detail_redacted" json:"error_detail_redacted"`
}

func (q *Queries) FinishRunAttempt(ctx context.Context, arg FinishRunAttemptParams) (RunAttempt, error) {
	var attempt RunAttempt
	err := scanRunAttempt(q.db.QueryRow(ctx, finishRunAttempt,
		arg.RunID, arg.ID, arg.LeaseID, arg.FencingToken, arg.Outcome,
		arg.ResultID, arg.ResultFingerprint, arg.ResultClassification,
		arg.FinalClientEventSeq, arg.ErrorCode, arg.ErrorDetailRedacted,
	), &attempt)
	return attempt, err
}

const acknowledgeRunAttemptResult = `-- name: AcknowledgeRunAttemptResult :one
UPDATE run_attempts
SET result_acknowledged_at = COALESCE(result_acknowledged_at, clock_timestamp())
WHERE run_id = $1 AND id = $2 AND result_id = $3
RETURNING *`

type AcknowledgeRunAttemptResultParams struct {
	RunID    uuid.UUID `db:"run_id" json:"run_id"`
	ID       uuid.UUID `db:"id" json:"id"`
	ResultID uuid.UUID `db:"result_id" json:"result_id"`
}

func (q *Queries) AcknowledgeRunAttemptResult(ctx context.Context, arg AcknowledgeRunAttemptResultParams) (RunAttempt, error) {
	var attempt RunAttempt
	err := scanRunAttempt(q.db.QueryRow(ctx, acknowledgeRunAttemptResult, arg.RunID, arg.ID, arg.ResultID), &attempt)
	return attempt, err
}
