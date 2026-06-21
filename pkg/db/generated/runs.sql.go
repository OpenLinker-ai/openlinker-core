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
	"github.com/jackc/pgx/v5"
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
    user_id, agent_id, input, status,
    cost_cents, platform_fee_cents, creator_revenue_cents, source
) VALUES (
    $1, $2, $3, 'running', $4, $5, $6, $7
)
RETURNING id, user_id, agent_id, input, output, status, error_code, error_message,
          cost_cents, platform_fee_cents, creator_revenue_cents, duration_ms,
          started_at, finished_at, source`

// CreateRunParams 入参。
//
// Input 是 JSONB 原始字节，调用方需先 json.Marshal。
// CostCents = PlatformFeeCents + CreatorRevenueCents（service 层计算）。
// Source 取值 'web' / 'mcp' / 'api'，由 handler 从 auth_method 派生。
type CreateRunParams struct {
	UserID              uuid.UUID `db:"user_id" json:"user_id"`
	AgentID             uuid.UUID `db:"agent_id" json:"agent_id"`
	Input               []byte    `db:"input" json:"input"`
	CostCents           int32     `db:"cost_cents" json:"cost_cents"`
	PlatformFeeCents    int32     `db:"platform_fee_cents" json:"platform_fee_cents"`
	CreatorRevenueCents int32     `db:"creator_revenue_cents" json:"creator_revenue_cents"`
	Source              string    `db:"source" json:"source"`
}

// CreateRun 在事务内创建调用记录，初始 status='running'。
func (q *Queries) CreateRun(ctx context.Context, arg CreateRunParams) (Run, error) {
	row := q.db.QueryRow(ctx, createRun,
		arg.UserID,
		arg.AgentID,
		arg.Input,
		arg.CostCents,
		arg.PlatformFeeCents,
		arg.CreatorRevenueCents,
		arg.Source,
	)
	var r Run
	err := scanRun(row, &r)
	return r, err
}

const markRunSuccess = `-- name: MarkRunSuccess :exec
UPDATE runs
SET status = 'success',
    output = $2,
    duration_ms = $3,
    finished_at = NOW()
WHERE id = $1 AND status = 'running'`

// MarkRunSuccessParams 入参。
type MarkRunSuccessParams struct {
	ID         uuid.UUID `db:"id" json:"id"`
	Output     []byte    `db:"output" json:"output"`
	DurationMs int32     `db:"duration_ms" json:"duration_ms"`
}

// MarkRunSuccess 调用成功：写 output 与耗时。
// status='running' 守卫保证幂等（重放无副作用）。
func (q *Queries) MarkRunSuccess(ctx context.Context, arg MarkRunSuccessParams) error {
	tag, err := q.db.Exec(ctx, markRunSuccess, arg.ID, arg.Output, arg.DurationMs)
	if err == nil && tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return err
}

const markRunFailed = `-- name: MarkRunFailed :exec
UPDATE runs
SET status = $2,
    error_code = $3,
    error_message = $4,
    duration_ms = $5,
    finished_at = NOW()
WHERE id = $1 AND status = 'running'`

// MarkRunFailedParams 入参。Status 取值 'failed'、'timeout' 或 'canceled'。
type MarkRunFailedParams struct {
	ID           uuid.UUID `db:"id" json:"id"`
	Status       string    `db:"status" json:"status"`
	ErrorCode    *string   `db:"error_code" json:"error_code"`
	ErrorMessage *string   `db:"error_message" json:"error_message"`
	DurationMs   int32     `db:"duration_ms" json:"duration_ms"`
}

// MarkRunFailed 调用失败：写错误信息与耗时。
func (q *Queries) MarkRunFailed(ctx context.Context, arg MarkRunFailedParams) error {
	tag, err := q.db.Exec(ctx, markRunFailed,
		arg.ID,
		arg.Status,
		arg.ErrorCode,
		arg.ErrorMessage,
		arg.DurationMs,
	)
	if err == nil && tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return err
}

const cancelRun = `-- name: CancelRun :one
UPDATE runs
SET status = 'canceled',
    error_code = 'CANCELED',
    error_message = $3,
    duration_ms = GREATEST(0, FLOOR(EXTRACT(EPOCH FROM (NOW() - started_at)) * 1000))::int,
    finished_at = NOW()
WHERE id = $1 AND user_id = $2 AND status = 'running'
RETURNING id, user_id, agent_id, input, output, status, error_code, error_message,
          cost_cents, platform_fee_cents, creator_revenue_cents, duration_ms,
          started_at, finished_at, source`

type CancelRunParams struct {
	ID           uuid.UUID `db:"id" json:"id"`
	UserID       uuid.UUID `db:"user_id" json:"user_id"`
	ErrorMessage string    `db:"error_message" json:"error_message"`
}

// CancelRun marks an owner-readable running run as canceled and returns the final row.
func (q *Queries) CancelRun(ctx context.Context, arg CancelRunParams) (Run, error) {
	row := q.db.QueryRow(ctx, cancelRun, arg.ID, arg.UserID, arg.ErrorMessage)
	var r Run
	err := scanRun(row, &r)
	return r, err
}

const getRunByID = `-- name: GetRunByID :one
SELECT id, user_id, agent_id, input, output, status, error_code, error_message,
       cost_cents, platform_fee_cents, creator_revenue_cents, duration_ms,
       started_at, finished_at, source
FROM runs
WHERE id = $1`

// GetRunByID 按 id 查单条调用记录（详情页用，service 层做 owner 校验）。
func (q *Queries) GetRunByID(ctx context.Context, id uuid.UUID) (Run, error) {
	row := q.db.QueryRow(ctx, getRunByID, id)
	var r Run
	err := scanRun(row, &r)
	return r, err
}

const claimRuntimePullRun = `-- name: ClaimRuntimePullRun :one
WITH candidate AS (
    SELECT r.id
    FROM runs r
    JOIN agents a ON a.id = r.agent_id
    WHERE r.agent_id = $1
      AND r.status = 'running'
      AND a.connection_mode IN ('runtime_pull', 'runtime_ws')
      AND (r.claimed_at IS NULL OR r.claimed_at < NOW() - INTERVAL '5 minutes')
    ORDER BY r.started_at ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
UPDATE runs r
SET claimed_by_runtime_token_id = $2,
    claimed_at = NOW()
FROM candidate
WHERE r.id = candidate.id
RETURNING r.id, r.user_id, r.agent_id, r.input, r.output, r.status, r.error_code, r.error_message,
          r.cost_cents, r.platform_fee_cents, r.creator_revenue_cents, r.duration_ms,
          r.started_at, r.finished_at, r.source`

type ClaimRuntimePullRunParams struct {
	AgentID        uuid.UUID `db:"agent_id" json:"agent_id"`
	RuntimeTokenID uuid.UUID `db:"runtime_token_id" json:"runtime_token_id"`
}

// ClaimRuntimePullRun atomically assigns the oldest pending queued runtime run to a Runtime Token.
func (q *Queries) ClaimRuntimePullRun(ctx context.Context, arg ClaimRuntimePullRunParams) (Run, error) {
	row := q.db.QueryRow(ctx, claimRuntimePullRun, arg.AgentID, arg.RuntimeTokenID)
	var r Run
	err := scanRun(row, &r)
	return r, err
}

const countClaimableRuntimePullRuns = `-- name: CountClaimableRuntimePullRuns :one
SELECT COUNT(*)::int AS total
FROM runs r
JOIN agents a ON a.id = r.agent_id
WHERE r.agent_id = $1
  AND r.status = 'running'
  AND a.connection_mode IN ('runtime_pull', 'runtime_ws')
  AND (r.claimed_at IS NULL OR r.claimed_at < NOW() - INTERVAL '5 minutes')`

func (q *Queries) CountClaimableRuntimePullRuns(ctx context.Context, agentID uuid.UUID) (int32, error) {
	row := q.db.QueryRow(ctx, countClaimableRuntimePullRuns, agentID)
	var total int32
	err := row.Scan(&total)
	return total, err
}

const getRuntimePullRunState = `-- name: GetRuntimePullRunState :one
SELECT id, user_id, agent_id, status, cost_cents, creator_revenue_cents,
       started_at, claimed_by_runtime_token_id
FROM runs
WHERE id = $1`

type RuntimePullRunState struct {
	ID                      uuid.UUID  `db:"id" json:"id"`
	UserID                  uuid.UUID  `db:"user_id" json:"user_id"`
	AgentID                 uuid.UUID  `db:"agent_id" json:"agent_id"`
	Status                  string     `db:"status" json:"status"`
	CostCents               int32      `db:"cost_cents" json:"cost_cents"`
	CreatorRevenueCents     int32      `db:"creator_revenue_cents" json:"creator_revenue_cents"`
	StartedAt               time.Time  `db:"started_at" json:"started_at"`
	ClaimedByRuntimeTokenID *uuid.UUID `db:"claimed_by_runtime_token_id" json:"claimed_by_runtime_token_id"`
}

func (q *Queries) GetRuntimePullRunState(ctx context.Context, id uuid.UUID) (RuntimePullRunState, error) {
	row := q.db.QueryRow(ctx, getRuntimePullRunState, id)
	var r RuntimePullRunState
	err := row.Scan(
		&r.ID,
		&r.UserID,
		&r.AgentID,
		&r.Status,
		&r.CostCents,
		&r.CreatorRevenueCents,
		&r.StartedAt,
		&r.ClaimedByRuntimeTokenID,
	)
	return r, err
}

const listStaleRuntimePullRuns = `-- name: ListStaleRuntimePullRuns :many
-- runtime_pull 任务如果长时间未被领取或已领取但未回传终态，需要自动收敛为 timeout，
-- 避免用户侧永久看到 running。
-- 已领取任务使用 started_at 的绝对超时窗口，避免坏客户端反复 claim 刷新 claimed_at
-- 导致同一条 run 被无限续命。
SELECT r.id, r.user_id, r.agent_id, r.cost_cents, r.started_at,
       CASE
           WHEN r.claimed_at IS NULL THEN 'RUNTIME_PULL_NOT_CLAIMED'
           ELSE 'RUNTIME_PULL_RESULT_TIMEOUT'
       END::text AS error_code,
       CASE
           WHEN r.claimed_at IS NULL THEN '任务未被 Agent runtime 领取，请确认本地进程正在心跳并使用 GET /agent-runtime/runs/claim?wait=25 拉取任务。'
           ELSE 'Agent runtime 已领取任务，但未在超时时间内回传结果。'
       END::text AS error_message
FROM runs r
JOIN agents a ON a.id = r.agent_id
WHERE r.status = 'running'
  AND a.connection_mode IN ('runtime_pull', 'runtime_ws')
  AND (
    (r.claimed_at IS NULL AND r.started_at < $1)
    OR (r.claimed_at IS NOT NULL AND r.started_at < $2)
  )
ORDER BY r.started_at ASC
LIMIT $3
FOR UPDATE SKIP LOCKED`

type ListStaleRuntimePullRunsParams struct {
	DispatchStaleBefore time.Time `db:"dispatch_stale_before" json:"dispatch_stale_before"`
	ResultStaleBefore   time.Time `db:"result_stale_before" json:"result_stale_before"`
	Limit               int32     `db:"limit" json:"limit"`
}

type ListStaleRuntimePullRunsRow struct {
	ID           uuid.UUID `db:"id" json:"id"`
	UserID       uuid.UUID `db:"user_id" json:"user_id"`
	AgentID      uuid.UUID `db:"agent_id" json:"agent_id"`
	CostCents    int32     `db:"cost_cents" json:"cost_cents"`
	StartedAt    time.Time `db:"started_at" json:"started_at"`
	ErrorCode    string    `db:"error_code" json:"error_code"`
	ErrorMessage string    `db:"error_message" json:"error_message"`
}

func (q *Queries) ListStaleRuntimePullRuns(ctx context.Context, arg ListStaleRuntimePullRunsParams) ([]ListStaleRuntimePullRunsRow, error) {
	rows, err := q.db.Query(ctx, listStaleRuntimePullRuns, arg.DispatchStaleBefore, arg.ResultStaleBefore, arg.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []ListStaleRuntimePullRunsRow
	for rows.Next() {
		var item ListStaleRuntimePullRunsRow
		if err := rows.Scan(
			&item.ID,
			&item.UserID,
			&item.AgentID,
			&item.CostCents,
			&item.StartedAt,
			&item.ErrorCode,
			&item.ErrorMessage,
		); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
