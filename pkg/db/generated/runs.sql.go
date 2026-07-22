// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/runs.sql）。
//
// 模块 4 / 6 共享此文件初始内容。
//   - 模块 4 (runtime) 写入到本文件
//   - 模块 6 (dashboard) 写入独立文件 runs_dashboard.sql.go 避免冲突

package db

import (
	"context"
	"time"

	"github.com/google/uuid"
)

const runsCount = `-- name: RunsCount :one
SELECT COUNT(*)::int AS total FROM runs`

// RunsCount 全局调用次数（占位用，避免空文件）。
func (q *Queries) RunsCount(ctx context.Context) (int32, error) {
	row := q.db.QueryRow(ctx, runsCount)
	var total int32
	err := row.Scan(&total)
	return total, err
}

// scanRun 在 runs_dashboard.sql.go 中定义（包内共享）。
// CreateRun / GetRunByID 共用同一列顺序。

const createRun = `-- name: CreateRun :one
INSERT INTO runs (
    id, user_id, agent_id, input, status,
    cost_cents, platform_fee_cents, creator_revenue_cents, source,
    idempotency_key_hash, idempotency_fingerprint, request_metadata,
    connection_mode_snapshot, endpoint_idempotency_snapshot,
    max_offer_count, max_attempts, dispatch_deadline_at, run_deadline_at,
    replay_of_run_id
) VALUES (
    $1, $2, $3, $4, 'running', $5, $6, $7, $8,
    $9, $10, $11, $12, $13, $14, $15,
    clock_timestamp() + ($16::bigint * INTERVAL '1 millisecond'),
    clock_timestamp() + ($17::bigint * INTERVAL '1 millisecond'),
    $18
)
ON CONFLICT (user_id, idempotency_key_hash)
    WHERE idempotency_key_hash IS NOT NULL
    DO NOTHING
RETURNING runs.id, runs.user_id, runs.agent_id, runs.input, runs.output,
          runs.status, runs.error_code, runs.error_message, runs.cost_cents,
          runs.platform_fee_cents, runs.creator_revenue_cents, runs.duration_ms,
          runs.started_at, runs.finished_at, runs.source,
          runs.runtime_contract_id, runs.dispatch_state, runs.attempt_count,
          runs.max_attempts, runs.next_attempt_at, runs.latest_attempt_id,
          runs.active_attempt_id, runs.cancel_state, runs.cancel_requested_at,
          runs.cancel_acknowledged_at, runs.cancel_reason,
          runs.dead_lettered_at, runs.replay_of_run_id`

// CreateRunParams 入参。
//
// Input 是 JSONB 原始字节，调用方需先 json.Marshal。
// CostCents = PlatformFeeCents + CreatorRevenueCents（service 层计算）。
// Source 取值 'web' / 'mcp' / 'api'，由 handler 从 auth_method 派生。
type CreateRunParams struct {
	ID                          uuid.UUID  `db:"id" json:"id"`
	UserID                      uuid.UUID  `db:"user_id" json:"user_id"`
	AgentID                     uuid.UUID  `db:"agent_id" json:"agent_id"`
	Input                       []byte     `db:"input" json:"input"`
	CostCents                   int32      `db:"cost_cents" json:"cost_cents"`
	PlatformFeeCents            int32      `db:"platform_fee_cents" json:"platform_fee_cents"`
	CreatorRevenueCents         int32      `db:"creator_revenue_cents" json:"creator_revenue_cents"`
	Source                      string     `db:"source" json:"source"`
	IdempotencyKeyHash          []byte     `db:"idempotency_key_hash" json:"-"`
	IdempotencyFingerprint      []byte     `db:"idempotency_fingerprint" json:"-"`
	RequestMetadata             []byte     `db:"request_metadata" json:"request_metadata"`
	ConnectionModeSnapshot      string     `db:"connection_mode_snapshot" json:"connection_mode_snapshot"`
	EndpointIdempotencySnapshot *bool      `db:"endpoint_idempotency_snapshot" json:"endpoint_idempotency_snapshot"`
	MaxOfferCount               int32      `db:"max_offer_count" json:"max_offer_count"`
	MaxAttempts                 int32      `db:"max_attempts" json:"max_attempts"`
	DispatchDeadlineAfterMs     int64      `db:"dispatch_deadline_after_ms" json:"dispatch_deadline_after_ms"`
	RunDeadlineAfterMs          int64      `db:"run_deadline_after_ms" json:"run_deadline_after_ms"`
	ReplayOfRunID               *uuid.UUID `db:"replay_of_run_id" json:"replay_of_run_id"`
}

// CreateRun 在事务内创建调用记录，初始 status='running'。
func (q *Queries) CreateRun(ctx context.Context, arg CreateRunParams) (Run, error) {
	row := q.db.QueryRow(ctx, createRun,
		arg.ID,
		arg.UserID,
		arg.AgentID,
		arg.Input,
		arg.CostCents,
		arg.PlatformFeeCents,
		arg.CreatorRevenueCents,
		arg.Source,
		arg.IdempotencyKeyHash,
		arg.IdempotencyFingerprint,
		arg.RequestMetadata,
		arg.ConnectionModeSnapshot,
		arg.EndpointIdempotencySnapshot,
		arg.MaxOfferCount,
		arg.MaxAttempts,
		arg.DispatchDeadlineAfterMs,
		arg.RunDeadlineAfterMs,
		arg.ReplayOfRunID,
	)
	var r Run
	err := scanRun(row, &r)
	return r, err
}

const getRunIdempotencyRecord = `-- name: GetRunIdempotencyRecord :one
SELECT id, idempotency_fingerprint
FROM runs
WHERE user_id = $1
  AND idempotency_key_hash = $2
  AND runtime_contract_id = 'openlinker.runtime.v2'`

type GetRunIdempotencyRecordParams struct {
	UserID             uuid.UUID `db:"user_id" json:"user_id"`
	IdempotencyKeyHash []byte    `db:"idempotency_key_hash" json:"-"`
}

type GetRunIdempotencyRecordRow struct {
	ID                     uuid.UUID `db:"id" json:"id"`
	IdempotencyFingerprint []byte    `db:"idempotency_fingerprint" json:"-"`
}

func (q *Queries) GetRunIdempotencyRecord(ctx context.Context, arg GetRunIdempotencyRecordParams) (GetRunIdempotencyRecordRow, error) {
	row := q.db.QueryRow(ctx, getRunIdempotencyRecord, arg.UserID, arg.IdempotencyKeyHash)
	var record GetRunIdempotencyRecordRow
	err := row.Scan(&record.ID, &record.IdempotencyFingerprint)
	return record, err
}

const lockReplaySourceForCreate = `-- name: LockReplaySourceForCreate :one
SELECT r.id,
       (jsonb_typeof(r.input) = 'object')::bool AS input_available
FROM runs r
JOIN run_dead_letters dlq ON dlq.run_id = r.id
WHERE r.id = $1
  AND r.user_id = $2
  AND r.runtime_contract_id = 'openlinker.runtime.v2'
  AND r.status = 'failed'
  AND r.dispatch_state = 'dead_letter'
FOR UPDATE OF r`

type LockReplaySourceForCreateParams struct {
	ID     uuid.UUID `db:"id" json:"id"`
	UserID uuid.UUID `db:"user_id" json:"user_id"`
}

type LockReplaySourceForCreateRow struct {
	ID             uuid.UUID `db:"id" json:"id"`
	InputAvailable bool      `db:"input_available" json:"input_available"`
}

func (q *Queries) LockReplaySourceForCreate(ctx context.Context, arg LockReplaySourceForCreateParams) (LockReplaySourceForCreateRow, error) {
	row := q.db.QueryRow(ctx, lockReplaySourceForCreate, arg.ID, arg.UserID)
	var result LockReplaySourceForCreateRow
	err := row.Scan(&result.ID, &result.InputAvailable)
	return result, err
}

const getRunByID = `-- name: GetRunByID :one
SELECT id, user_id, agent_id, input, output, status, error_code, error_message,
       cost_cents, platform_fee_cents, creator_revenue_cents, duration_ms,
       started_at, finished_at, source, runtime_contract_id, dispatch_state,
       attempt_count, max_attempts, next_attempt_at, latest_attempt_id,
       active_attempt_id, cancel_state, cancel_requested_at,
       cancel_acknowledged_at, cancel_reason, dead_lettered_at,
       replay_of_run_id
FROM runs
WHERE id = $1`

// GetRunByID 按 id 查单条调用记录（详情页用，service 层做 owner 校验）。
func (q *Queries) GetRunByID(ctx context.Context, id uuid.UUID) (Run, error) {
	row := q.db.QueryRow(ctx, getRunByID, id)
	var r Run
	err := scanRun(row, &r)
	return r, err
}

const getRunStatusForViewer = `-- name: GetRunStatusForViewer :one
SELECT r.status
FROM runs r
LEFT JOIN agents a ON a.id = r.agent_id
WHERE r.id = $1
  AND (r.user_id = $2 OR a.creator_id = $2)`

type GetRunStatusForViewerParams struct {
	RunID    uuid.UUID `db:"run_id" json:"run_id"`
	ViewerID uuid.UUID `db:"viewer_id" json:"viewer_id"`
}

func (q *Queries) GetRunStatusForViewer(ctx context.Context, arg GetRunStatusForViewerParams) (string, error) {
	row := q.db.QueryRow(ctx, getRunStatusForViewer, arg.RunID, arg.ViewerID)
	var status string
	err := row.Scan(&status)
	return status, err
}

const getRunReplayPayload = `-- name: GetRunReplayPayload :one
SELECT input, request_metadata
FROM runs
WHERE id = $1`

type GetRunReplayPayloadRow struct {
	Input           []byte `db:"input" json:"input"`
	RequestMetadata []byte `db:"request_metadata" json:"request_metadata"`
}

func (q *Queries) GetRunReplayPayload(ctx context.Context, id uuid.UUID) (GetRunReplayPayloadRow, error) {
	row := q.db.QueryRow(ctx, getRunReplayPayload, id)
	var payload GetRunReplayPayloadRow
	err := row.Scan(&payload.Input, &payload.RequestMetadata)
	return payload, err
}

const updateRunRequestMetadata = `-- name: UpdateRunRequestMetadata :exec
UPDATE runs
SET request_metadata = $2
WHERE id = $1`

type UpdateRunRequestMetadataParams struct {
	ID              uuid.UUID `db:"id" json:"id"`
	RequestMetadata []byte    `db:"request_metadata" json:"request_metadata"`
}

func (q *Queries) UpdateRunRequestMetadata(ctx context.Context, arg UpdateRunRequestMetadataParams) error {
	_, err := q.db.Exec(ctx, updateRunRequestMetadata, arg.ID, arg.RequestMetadata)
	return err
}

const lockRunForResultFinalization = `-- name: LockRunForResultFinalization :one
SELECT r.id, r.user_id, r.agent_id, r.status, r.dispatch_state,
       r.runtime_contract_id, r.connection_mode_snapshot,
       r.endpoint_idempotency_snapshot, r.output, r.error_code,
       r.error_message, r.attempt_count, r.max_attempts,
       r.latest_attempt_id, r.active_attempt_id, r.lease_id,
       r.fencing_token, r.runtime_node_id, r.runtime_worker_id,
       r.runtime_session_id, r.lease_token_id, r.run_deadline_at,
       r.next_attempt_at, r.result_id, r.result_fingerprint,
       r.terminal_event_id, r.dead_lettered_at, r.cancel_request_id,
       r.cancel_state, r.creator_revenue_cents, r.started_at,
       r.finished_at, clock_timestamp() AS database_now
FROM runs r
WHERE r.id = $1
  AND r.runtime_contract_id = 'openlinker.runtime.v2'
FOR UPDATE`

type LockRunForResultFinalizationRow struct {
	ID                          uuid.UUID  `db:"id" json:"id"`
	UserID                      uuid.UUID  `db:"user_id" json:"user_id"`
	AgentID                     uuid.UUID  `db:"agent_id" json:"agent_id"`
	Status                      string     `db:"status" json:"status"`
	DispatchState               string     `db:"dispatch_state" json:"dispatch_state"`
	RuntimeContractID           string     `db:"runtime_contract_id" json:"runtime_contract_id"`
	ConnectionModeSnapshot      *string    `db:"connection_mode_snapshot" json:"connection_mode_snapshot"`
	EndpointIdempotencySnapshot *bool      `db:"endpoint_idempotency_snapshot" json:"endpoint_idempotency_snapshot"`
	Output                      []byte     `db:"output" json:"output"`
	ErrorCode                   *string    `db:"error_code" json:"error_code"`
	ErrorMessage                *string    `db:"error_message" json:"error_message"`
	AttemptCount                int32      `db:"attempt_count" json:"attempt_count"`
	MaxAttempts                 int32      `db:"max_attempts" json:"max_attempts"`
	LatestAttemptID             *uuid.UUID `db:"latest_attempt_id" json:"latest_attempt_id"`
	ActiveAttemptID             *uuid.UUID `db:"active_attempt_id" json:"active_attempt_id"`
	LeaseID                     *uuid.UUID `db:"lease_id" json:"lease_id"`
	FencingToken                int64      `db:"fencing_token" json:"fencing_token"`
	RuntimeNodeID               *uuid.UUID `db:"runtime_node_id" json:"runtime_node_id"`
	RuntimeWorkerID             *string    `db:"runtime_worker_id" json:"runtime_worker_id"`
	RuntimeSessionID            *uuid.UUID `db:"runtime_session_id" json:"runtime_session_id"`
	LeaseTokenID                *uuid.UUID `db:"lease_token_id" json:"lease_token_id"`
	RunDeadlineAt               *time.Time `db:"run_deadline_at" json:"run_deadline_at"`
	NextAttemptAt               *time.Time `db:"next_attempt_at" json:"next_attempt_at"`
	ResultID                    *uuid.UUID `db:"result_id" json:"result_id"`
	ResultFingerprint           []byte     `db:"result_fingerprint" json:"-"`
	TerminalEventID             *uuid.UUID `db:"terminal_event_id" json:"terminal_event_id"`
	DeadLetteredAt              *time.Time `db:"dead_lettered_at" json:"dead_lettered_at"`
	CancelRequestID             *uuid.UUID `db:"cancel_request_id" json:"cancel_request_id"`
	CancelState                 *string    `db:"cancel_state" json:"cancel_state"`
	CreatorRevenueCents         int32      `db:"creator_revenue_cents" json:"creator_revenue_cents"`
	StartedAt                   time.Time  `db:"started_at" json:"started_at"`
	FinishedAt                  *time.Time `db:"finished_at" json:"finished_at"`
	DatabaseNow                 time.Time  `db:"database_now" json:"database_now"`
}

func (q *Queries) LockRunForResultFinalization(ctx context.Context, id uuid.UUID) (LockRunForResultFinalizationRow, error) {
	var r LockRunForResultFinalizationRow
	err := q.db.QueryRow(ctx, lockRunForResultFinalization, id).Scan(
		&r.ID, &r.UserID, &r.AgentID, &r.Status, &r.DispatchState,
		&r.RuntimeContractID, &r.ConnectionModeSnapshot,
		&r.EndpointIdempotencySnapshot, &r.Output, &r.ErrorCode,
		&r.ErrorMessage, &r.AttemptCount, &r.MaxAttempts,
		&r.LatestAttemptID, &r.ActiveAttemptID, &r.LeaseID,
		&r.FencingToken, &r.RuntimeNodeID, &r.RuntimeWorkerID,
		&r.RuntimeSessionID, &r.LeaseTokenID, &r.RunDeadlineAt,
		&r.NextAttemptAt, &r.ResultID, &r.ResultFingerprint,
		&r.TerminalEventID, &r.DeadLetteredAt, &r.CancelRequestID,
		&r.CancelState, &r.CreatorRevenueCents, &r.StartedAt,
		&r.FinishedAt, &r.DatabaseNow,
	)
	return r, err
}

const transitionRunToRetryWait = `-- name: TransitionRunToRetryWait :one
UPDATE runs r
SET status = 'running', dispatch_state = 'retry_wait',
    next_attempt_at = clock_timestamp() + ($3::bigint * INTERVAL '1 millisecond'),
    active_attempt_id = NULL, lease_id = NULL, executor_type = NULL,
    active_core_instance_id = NULL, runtime_node_id = NULL,
    runtime_worker_id = NULL, runtime_session_id = NULL, lease_token_id = NULL,
    lease_offered_at = NULL, lease_accepted_at = NULL, lease_expires_at = NULL,
    attempt_deadline_at = NULL, error_code = NULL, error_message = NULL
FROM run_attempts a
WHERE r.id = $1 AND r.status = 'running' AND r.dispatch_state = 'executing'
  AND r.active_attempt_id = $2 AND a.run_id = r.id AND a.id = $2
  AND a.finished_at IS NOT NULL AND a.outcome = 'retryable_failure'
  AND a.result_id IS NOT NULL AND a.result_acknowledged_at IS NOT NULL
  AND r.attempt_count < r.max_attempts AND $3::bigint >= 0
RETURNING r.id, r.status, r.dispatch_state, r.next_attempt_at,
          r.active_attempt_id, r.result_id, r.terminal_event_id, r.finished_at`

type TransitionRunToRetryWaitParams struct {
	RunID        uuid.UUID `db:"run_id" json:"run_id"`
	AttemptID    uuid.UUID `db:"attempt_id" json:"attempt_id"`
	RetryAfterMs int64     `db:"retry_after_ms" json:"retry_after_ms"`
}

type TransitionRunToRetryWaitRow struct {
	ID              uuid.UUID  `db:"id" json:"id"`
	Status          string     `db:"status" json:"status"`
	DispatchState   string     `db:"dispatch_state" json:"dispatch_state"`
	NextAttemptAt   *time.Time `db:"next_attempt_at" json:"next_attempt_at"`
	ActiveAttemptID *uuid.UUID `db:"active_attempt_id" json:"active_attempt_id"`
	ResultID        *uuid.UUID `db:"result_id" json:"result_id"`
	TerminalEventID *uuid.UUID `db:"terminal_event_id" json:"terminal_event_id"`
	FinishedAt      *time.Time `db:"finished_at" json:"finished_at"`
}

func (q *Queries) TransitionRunToRetryWait(ctx context.Context, arg TransitionRunToRetryWaitParams) (TransitionRunToRetryWaitRow, error) {
	var r TransitionRunToRetryWaitRow
	err := q.db.QueryRow(ctx, transitionRunToRetryWait,
		arg.RunID, arg.AttemptID, arg.RetryAfterMs,
	).Scan(
		&r.ID, &r.Status, &r.DispatchState, &r.NextAttemptAt,
		&r.ActiveAttemptID, &r.ResultID, &r.TerminalEventID, &r.FinishedAt,
	)
	return r, err
}

const finalizeRunFromResult = `-- name: FinalizeRunFromResult :one
UPDATE runs r
SET status = $3, dispatch_state = $4, output = $5::jsonb,
    error_code = $6, error_message = $7, duration_ms = $10,
    finished_at = clock_timestamp(), next_attempt_at = NULL,
    active_attempt_id = NULL, lease_id = NULL, executor_type = NULL,
    active_core_instance_id = NULL, runtime_node_id = NULL,
    runtime_worker_id = NULL, runtime_session_id = NULL, lease_token_id = NULL,
    lease_offered_at = NULL, lease_accepted_at = NULL, lease_expires_at = NULL,
    attempt_deadline_at = NULL, result_id = $8, result_fingerprint = $9,
    terminal_event_id = $11,
    dead_lettered_at = CASE WHEN $4::text = 'dead_letter' THEN clock_timestamp() ELSE NULL END
FROM run_attempts a
WHERE r.id = $1 AND r.runtime_contract_id = 'openlinker.runtime.v2'
  AND r.status = 'running' AND r.dispatch_state = 'executing'
  AND r.active_attempt_id = $2 AND a.run_id = r.id AND a.id = $2
  AND a.finished_at IS NOT NULL AND a.result_id IS NOT NULL
  AND a.result_acknowledged_at IS NOT NULL AND $10::int >= 0
  AND (
      ($3::text = 'success' AND $4::text = 'terminal'
       AND a.outcome = 'success' AND $5::jsonb IS NOT NULL
       AND $6::text IS NULL AND $8::uuid IS NOT DISTINCT FROM a.result_id
       AND $9::bytea IS NOT DISTINCT FROM a.result_fingerprint)
      OR
      ($3::text = 'failed' AND $4::text = 'terminal'
       AND a.outcome = 'non_retryable_failure' AND $5::jsonb IS NULL
       AND $6::text IS NOT NULL AND $8::uuid IS NOT DISTINCT FROM a.result_id
       AND $9::bytea IS NOT DISTINCT FROM a.result_fingerprint)
      OR
      ($3::text = 'failed' AND $4::text = 'dead_letter'
       AND a.outcome = 'retryable_failure' AND $5::jsonb IS NULL
       AND $6::text = 'RUNTIME_RETRY_EXHAUSTED'
       AND $8::uuid IS NOT DISTINCT FROM a.result_id
       AND $9::bytea IS NOT DISTINCT FROM a.result_fingerprint)
      OR
      ($3::text = 'timeout' AND $4::text = 'terminal'
       AND a.outcome = 'timeout' AND $5::jsonb IS NULL
       AND $6::text IS NOT NULL AND $8::uuid IS NULL AND $9::bytea IS NULL)
  )
RETURNING r.id, r.status, r.dispatch_state, r.output, r.error_code,
          r.error_message, r.duration_ms, r.finished_at, r.next_attempt_at,
          r.result_id, r.result_fingerprint, r.terminal_event_id,
          r.dead_lettered_at`

type FinalizeRunFromResultParams struct {
	RunID             uuid.UUID  `db:"run_id" json:"run_id"`
	AttemptID         uuid.UUID  `db:"attempt_id" json:"attempt_id"`
	Status            string     `db:"status" json:"status"`
	DispatchState     string     `db:"dispatch_state" json:"dispatch_state"`
	Output            []byte     `db:"output" json:"output"`
	ErrorCode         *string    `db:"error_code" json:"error_code"`
	ErrorMessage      *string    `db:"error_message" json:"error_message"`
	ResultID          *uuid.UUID `db:"result_id" json:"result_id"`
	ResultFingerprint []byte     `db:"result_fingerprint" json:"-"`
	DurationMs        int32      `db:"duration_ms" json:"duration_ms"`
	TerminalEventID   uuid.UUID  `db:"terminal_event_id" json:"terminal_event_id"`
}

type FinalizeRunFromResultRow struct {
	ID                uuid.UUID  `db:"id" json:"id"`
	Status            string     `db:"status" json:"status"`
	DispatchState     string     `db:"dispatch_state" json:"dispatch_state"`
	Output            []byte     `db:"output" json:"output"`
	ErrorCode         *string    `db:"error_code" json:"error_code"`
	ErrorMessage      *string    `db:"error_message" json:"error_message"`
	DurationMs        *int32     `db:"duration_ms" json:"duration_ms"`
	FinishedAt        *time.Time `db:"finished_at" json:"finished_at"`
	NextAttemptAt     *time.Time `db:"next_attempt_at" json:"next_attempt_at"`
	ResultID          *uuid.UUID `db:"result_id" json:"result_id"`
	ResultFingerprint []byte     `db:"result_fingerprint" json:"-"`
	TerminalEventID   *uuid.UUID `db:"terminal_event_id" json:"terminal_event_id"`
	DeadLetteredAt    *time.Time `db:"dead_lettered_at" json:"dead_lettered_at"`
}

func (q *Queries) FinalizeRunFromResult(ctx context.Context, arg FinalizeRunFromResultParams) (FinalizeRunFromResultRow, error) {
	var r FinalizeRunFromResultRow
	err := q.db.QueryRow(ctx, finalizeRunFromResult,
		arg.RunID, arg.AttemptID, arg.Status, arg.DispatchState, arg.Output,
		arg.ErrorCode, arg.ErrorMessage, arg.ResultID, arg.ResultFingerprint,
		arg.DurationMs, arg.TerminalEventID,
	).Scan(
		&r.ID, &r.Status, &r.DispatchState, &r.Output, &r.ErrorCode,
		&r.ErrorMessage, &r.DurationMs, &r.FinishedAt, &r.NextAttemptAt,
		&r.ResultID, &r.ResultFingerprint, &r.TerminalEventID,
		&r.DeadLetteredAt,
	)
	return r, err
}

const insertTerminalRunEvent = `-- name: InsertTerminalRunEvent :one
WITH target_run AS (
    SELECT r.id FROM runs r
    WHERE r.id = $2::uuid AND r.runtime_contract_id = 'openlinker.runtime.v2'
), next_sequence AS (
    SELECT COALESCE(MAX(e.sequence), 0)::int + 1 AS sequence
    FROM run_events e JOIN target_run r ON r.id = e.run_id
)
INSERT INTO run_events (id, run_id, parent_run_id, sequence, event_type, payload)
SELECT $1, target_run.id, $3, next_sequence.sequence, $4, $5
FROM target_run, next_sequence
RETURNING run_events.id, run_events.run_id, run_events.parent_run_id,
          run_events.sequence, run_events.event_type, run_events.payload,
          run_events.created_at, run_events.client_event_id,
          run_events.client_event_seq, run_events.payload_fingerprint,
          run_events.attempt_id, run_events.attempt_no,
          run_events.fencing_token`

type InsertTerminalRunEventParams struct {
	ID          uuid.UUID  `db:"id" json:"id"`
	RunID       uuid.UUID  `db:"run_id" json:"run_id"`
	ParentRunID *uuid.UUID `db:"parent_run_id" json:"parent_run_id"`
	EventType   string     `db:"event_type" json:"event_type"`
	Payload     []byte     `db:"payload" json:"payload"`
}

// InsertTerminalRunEvent requires the caller to hold the Run row lock and the
// per-Run advisory Event lock. ID must be deterministically derived by Core.
func (q *Queries) InsertTerminalRunEvent(ctx context.Context, arg InsertTerminalRunEventParams) (RunEvent, error) {
	var event RunEvent
	err := scanRunEvent(q.db.QueryRow(ctx, insertTerminalRunEvent,
		arg.ID, arg.RunID, arg.ParentRunID, arg.EventType, arg.Payload,
	), &event)
	return event, err
}
