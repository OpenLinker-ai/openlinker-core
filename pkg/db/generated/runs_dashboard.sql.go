// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/runs.sql 中 "## 模块 6" 区块）。
//
// 模块 6 (dashboard，读) 由 subagent-6a 维护，独立文件避免与 subagent-4a 编辑的
// runs.sql.go (模块 4 写) 冲突。

package db

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// scanRun 把一行扫描成 Run 结构（按 SELECT 列顺序）。
//
// 由 subagent-6a 维护、模块 4 / 6 共用：
//   - 模块 4：CreateRun / GetRunByID 在 runs.sql.go 中复用
//   - 模块 6：ListRunsByUser 在本文件复用
//
// 列顺序变更需同步两边的 SELECT / RETURNING 子句。
func scanRun(row interface {
	Scan(dest ...any) error
}, r *Run) error {
	return row.Scan(
		&r.ID,
		&r.UserID,
		&r.AgentID,
		&r.Input,
		&r.Output,
		&r.Status,
		&r.ErrorCode,
		&r.ErrorMessage,
		&r.CostCents,
		&r.PlatformFeeCents,
		&r.CreatorRevenueCents,
		&r.DurationMs,
		&r.StartedAt,
		&r.FinishedAt,
		&r.Source,
	)
}

const listRunsByUser = `-- name: ListRunsByUser :many
SELECT id, user_id, agent_id, input, output, status, error_code, error_message,
       cost_cents, platform_fee_cents, creator_revenue_cents, duration_ms,
       started_at, finished_at, source
FROM runs
WHERE user_id = $1
ORDER BY started_at DESC
LIMIT $2 OFFSET $3`

// ListRunsByUserParams 入参。
type ListRunsByUserParams struct {
	UserID uuid.UUID `db:"user_id" json:"user_id"`
	Limit  int32     `db:"limit" json:"limit"`
	Offset int32     `db:"offset" json:"offset"`
}

// ListRunsByUser 我的调用历史（不带 join，预留给只需要 run 数据的场景）。
func (q *Queries) ListRunsByUser(ctx context.Context, arg ListRunsByUserParams) ([]Run, error) {
	rows, err := q.db.Query(ctx, listRunsByUser, arg.UserID, arg.Limit, arg.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []Run
	for rows.Next() {
		var r Run
		if err := scanRun(rows, &r); err != nil {
			return nil, err
		}
		items = append(items, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const listRunsByUserWithAgent = `-- name: ListRunsByUserWithAgent :many
SELECT r.id, r.user_id, r.agent_id, r.input, r.output, r.status,
       r.error_code, r.error_message, r.cost_cents, r.platform_fee_cents,
       r.creator_revenue_cents, r.duration_ms, r.started_at, r.finished_at,
       r.source,
       a.slug AS agent_slug, a.name AS agent_name
FROM runs r
JOIN agents a ON a.id = r.agent_id
WHERE r.user_id = $1
ORDER BY r.started_at DESC
LIMIT $2 OFFSET $3`

// ListRunsByUserWithAgentParams 入参。
type ListRunsByUserWithAgentParams struct {
	UserID uuid.UUID `db:"user_id" json:"user_id"`
	Limit  int32     `db:"limit" json:"limit"`
	Offset int32     `db:"offset" json:"offset"`
}

// ListRunsByUserWithAgentRow 返回行：嵌入 Run + agent 展示字段。
type ListRunsByUserWithAgentRow struct {
	Run
	AgentSlug string `db:"agent_slug" json:"agent_slug"`
	AgentName string `db:"agent_name" json:"agent_name"`
}

// ListRunsByUserWithAgent 我的调用历史（带 agent 名称展示）。
func (q *Queries) ListRunsByUserWithAgent(ctx context.Context, arg ListRunsByUserWithAgentParams) ([]ListRunsByUserWithAgentRow, error) {
	rows, err := q.db.Query(ctx, listRunsByUserWithAgent, arg.UserID, arg.Limit, arg.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []ListRunsByUserWithAgentRow
	for rows.Next() {
		var r ListRunsByUserWithAgentRow
		if err := rows.Scan(
			&r.ID,
			&r.UserID,
			&r.AgentID,
			&r.Input,
			&r.Output,
			&r.Status,
			&r.ErrorCode,
			&r.ErrorMessage,
			&r.CostCents,
			&r.PlatformFeeCents,
			&r.CreatorRevenueCents,
			&r.DurationMs,
			&r.StartedAt,
			&r.FinishedAt,
			&r.Source,
			&r.AgentSlug,
			&r.AgentName,
		); err != nil {
			return nil, err
		}
		items = append(items, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const listRunsByUserAndAgent = `-- name: ListRunsByUserAndAgent :many
SELECT r.id, r.user_id, r.agent_id, r.input, r.output, r.status, r.error_code, r.error_message,
       r.cost_cents, r.platform_fee_cents, r.creator_revenue_cents, r.duration_ms,
       r.started_at, r.finished_at, r.source
FROM runs r
LEFT JOIN a2a_context_mappings ctx ON ctx.run_id = r.id
WHERE r.user_id = $1
  AND r.agent_id = $2
  AND ($3::bool OR (r.started_at, r.id) < ($4::timestamptz, $5::uuid))
  AND ($6::bool OR r.status = ANY($7::text[]))
  AND ($8::bool OR COALESCE(r.finished_at, r.started_at) >= $9::timestamptz)
  AND (
      $10::text = ''
      OR ctx.protocol_context_id = $10
      OR ctx.root_context_id = $10
      OR r.input->>'a2a_context_id' = $10
      OR r.id::text = $10
  )
ORDER BY r.started_at DESC, r.id DESC
LIMIT $11`

// ListRunsByUserAndAgentParams 入参。
type ListRunsByUserAndAgentParams struct {
	UserID          uuid.UUID `db:"user_id" json:"user_id"`
	AgentID         uuid.UUID `db:"agent_id" json:"agent_id"`
	NoCursor        bool      `db:"no_cursor" json:"no_cursor"`
	CursorStartedAt time.Time `db:"cursor_started_at" json:"cursor_started_at"`
	CursorID        uuid.UUID `db:"cursor_id" json:"cursor_id"`
	NoStatusFilter  bool      `db:"no_status_filter" json:"no_status_filter"`
	Statuses        []string  `db:"statuses" json:"statuses"`
	NoSinceFilter   bool      `db:"no_since_filter" json:"no_since_filter"`
	Since           time.Time `db:"since" json:"since"`
	ContextID       string    `db:"context_id" json:"context_id"`
	Limit           int32     `db:"limit" json:"limit"`
}

// ListRunsByUserAndAgent returns one user's A2A-visible tasks for one Agent.
func (q *Queries) ListRunsByUserAndAgent(ctx context.Context, arg ListRunsByUserAndAgentParams) ([]Run, error) {
	rows, err := q.db.Query(ctx, listRunsByUserAndAgent, arg.UserID, arg.AgentID, arg.NoCursor, arg.CursorStartedAt, arg.CursorID, arg.NoStatusFilter, arg.Statuses, arg.NoSinceFilter, arg.Since, arg.ContextID, arg.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []Run
	for rows.Next() {
		var r Run
		if err := scanRun(rows, &r); err != nil {
			return nil, err
		}
		items = append(items, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const countRunsByUserAndAgent = `-- name: CountRunsByUserAndAgent :one
SELECT COUNT(*)::int AS total
FROM runs r
LEFT JOIN a2a_context_mappings ctx ON ctx.run_id = r.id
WHERE r.user_id = $1
  AND r.agent_id = $2
  AND ($3::bool OR r.status = ANY($4::text[]))
  AND ($5::bool OR COALESCE(r.finished_at, r.started_at) >= $6::timestamptz)
  AND (
      $7::text = ''
      OR ctx.protocol_context_id = $7
      OR ctx.root_context_id = $7
      OR r.input->>'a2a_context_id' = $7
      OR r.id::text = $7
  )`

type CountRunsByUserAndAgentParams struct {
	UserID         uuid.UUID `db:"user_id" json:"user_id"`
	AgentID        uuid.UUID `db:"agent_id" json:"agent_id"`
	NoStatusFilter bool      `db:"no_status_filter" json:"no_status_filter"`
	Statuses       []string  `db:"statuses" json:"statuses"`
	NoSinceFilter  bool      `db:"no_since_filter" json:"no_since_filter"`
	Since          time.Time `db:"since" json:"since"`
	ContextID      string    `db:"context_id" json:"context_id"`
}

func (q *Queries) CountRunsByUserAndAgent(ctx context.Context, arg CountRunsByUserAndAgentParams) (int32, error) {
	row := q.db.QueryRow(ctx, countRunsByUserAndAgent, arg.UserID, arg.AgentID, arg.NoStatusFilter, arg.Statuses, arg.NoSinceFilter, arg.Since, arg.ContextID)
	var total int32
	err := row.Scan(&total)
	return total, err
}

const listRunsByCreatorAgentWithAgent = `-- name: ListRunsByCreatorAgentWithAgent :many
SELECT r.id, r.user_id, r.agent_id, r.input, r.output, r.status,
       r.error_code, r.error_message, r.cost_cents, r.platform_fee_cents,
       r.creator_revenue_cents, r.duration_ms, r.started_at, r.finished_at,
       r.source, r.claimed_by_runtime_token_id, r.claimed_at,
       a.slug AS agent_slug, a.name AS agent_name
FROM runs r
JOIN agents a ON a.id = r.agent_id
WHERE a.creator_id = $1 AND r.agent_id = $2
ORDER BY r.started_at DESC
LIMIT $3 OFFSET $4`

// ListRunsByCreatorAgentWithAgentParams 入参。
type ListRunsByCreatorAgentWithAgentParams struct {
	CreatorID uuid.UUID `db:"creator_id" json:"creator_id"`
	AgentID   uuid.UUID `db:"agent_id" json:"agent_id"`
	Limit     int32     `db:"limit" json:"limit"`
	Offset    int32     `db:"offset" json:"offset"`
}

// ListRunsByCreatorAgentWithAgentRow 返回行：嵌入 Run + agent 展示字段。
type ListRunsByCreatorAgentWithAgentRow struct {
	Run
	AgentSlug string `db:"agent_slug" json:"agent_slug"`
	AgentName string `db:"agent_name" json:"agent_name"`
}

// ListRunsByCreatorAgentWithAgent 创作者查看某个自己 Agent 的被调用历史。
func (q *Queries) ListRunsByCreatorAgentWithAgent(ctx context.Context, arg ListRunsByCreatorAgentWithAgentParams) ([]ListRunsByCreatorAgentWithAgentRow, error) {
	rows, err := q.db.Query(ctx, listRunsByCreatorAgentWithAgent, arg.CreatorID, arg.AgentID, arg.Limit, arg.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []ListRunsByCreatorAgentWithAgentRow
	for rows.Next() {
		var r ListRunsByCreatorAgentWithAgentRow
		if err := rows.Scan(
			&r.ID,
			&r.UserID,
			&r.AgentID,
			&r.Input,
			&r.Output,
			&r.Status,
			&r.ErrorCode,
			&r.ErrorMessage,
			&r.CostCents,
			&r.PlatformFeeCents,
			&r.CreatorRevenueCents,
			&r.DurationMs,
			&r.StartedAt,
			&r.FinishedAt,
			&r.Source,
			&r.ClaimedByRuntimeTokenID,
			&r.ClaimedAt,
			&r.AgentSlug,
			&r.AgentName,
		); err != nil {
			return nil, err
		}
		items = append(items, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const countRunsByUser = `-- name: CountRunsByUser :one
SELECT COUNT(*)::int AS total FROM runs WHERE user_id = $1`

// CountRunsByUser 用户全部调用次数。
func (q *Queries) CountRunsByUser(ctx context.Context, userID uuid.UUID) (int32, error) {
	row := q.db.QueryRow(ctx, countRunsByUser, userID)
	var total int32
	err := row.Scan(&total)
	return total, err
}

const countRunsByCreatorAgent = `-- name: CountRunsByCreatorAgent :one
SELECT COUNT(*)::int AS total
FROM runs r
JOIN agents a ON a.id = r.agent_id
WHERE a.creator_id = $1 AND r.agent_id = $2`

// CountRunsByCreatorAgent 创作者某个 Agent 的全部被调用次数。
func (q *Queries) CountRunsByCreatorAgent(ctx context.Context, arg CountRunsByCreatorAgentParams) (int32, error) {
	row := q.db.QueryRow(ctx, countRunsByCreatorAgent, arg.CreatorID, arg.AgentID)
	var total int32
	err := row.Scan(&total)
	return total, err
}

// CountRunsByCreatorAgentParams 入参。
type CountRunsByCreatorAgentParams struct {
	CreatorID uuid.UUID `db:"creator_id" json:"creator_id"`
	AgentID   uuid.UUID `db:"agent_id" json:"agent_id"`
}

const countRunsByUserThisMonth = `-- name: CountRunsByUserThisMonth :one
SELECT COUNT(*)::int AS total
FROM runs
WHERE user_id = $1 AND started_at >= date_trunc('month', NOW())`

// CountRunsByUserThisMonth 用户本月调用次数（按 UTC，date_trunc('month', NOW())）。
func (q *Queries) CountRunsByUserThisMonth(ctx context.Context, userID uuid.UUID) (int32, error) {
	row := q.db.QueryRow(ctx, countRunsByUserThisMonth, userID)
	var total int32
	err := row.Scan(&total)
	return total, err
}

const sumSpentByUserThisMonth = `-- name: SumSpentByUserThisMonth :one
SELECT COALESCE(SUM(cost_cents), 0)::bigint AS total_spent
FROM runs
WHERE user_id = $1 AND status = 'success' AND started_at >= date_trunc('month', NOW())`

// SumSpentByUserThisMonth 用户本月成功调用总花费（cents）。
func (q *Queries) SumSpentByUserThisMonth(ctx context.Context, userID uuid.UUID) (int64, error) {
	row := q.db.QueryRow(ctx, sumSpentByUserThisMonth, userID)
	var total int64
	err := row.Scan(&total)
	return total, err
}

const sumEarningsByCreatorThisMonth = `-- name: SumEarningsByCreatorThisMonth :one
SELECT COALESCE(SUM(r.creator_revenue_cents), 0)::bigint AS total_earned
FROM runs r
JOIN agents a ON a.id = r.agent_id
WHERE a.creator_id = $1 AND r.status = 'success' AND r.started_at >= date_trunc('month', NOW())`

// SumEarningsByCreatorThisMonth 创作者本月收入（cents），通过 agent.creator_id 关联。
func (q *Queries) SumEarningsByCreatorThisMonth(ctx context.Context, creatorID uuid.UUID) (int64, error) {
	row := q.db.QueryRow(ctx, sumEarningsByCreatorThisMonth, creatorID)
	var total int64
	err := row.Scan(&total)
	return total, err
}

const countRunsForCreatorThisMonth = `-- name: CountRunsForCreatorThisMonth :one
SELECT COUNT(*)::int AS total
FROM runs r
JOIN agents a ON a.id = r.agent_id
WHERE a.creator_id = $1 AND r.status = 'success' AND r.started_at >= date_trunc('month', NOW())`

// CountRunsForCreatorThisMonth 创作者本月被成功调用次数。
func (q *Queries) CountRunsForCreatorThisMonth(ctx context.Context, creatorID uuid.UUID) (int32, error) {
	row := q.db.QueryRow(ctx, countRunsForCreatorThisMonth, creatorID)
	var total int32
	err := row.Scan(&total)
	return total, err
}

const listAgentStatsForCreator = `-- name: ListAgentStatsForCreator :many
SELECT a.id, a.slug, a.name,
       (CASE
            WHEN a.lifecycle_status = 'disabled' THEN 'disabled'
            WHEN a.certification_status = 'pending' THEN 'pending'
            WHEN a.certification_status = 'rejected' THEN 'rejected'
            ELSE 'approved'
       END)::text AS status,
       a.price_per_call_cents,
       a.total_calls AS lifetime_calls, a.total_revenue_cents AS lifetime_revenue,
       COALESCE(monthly.calls_this_month, 0) AS calls_this_month,
       COALESCE(monthly.revenue_this_month, 0) AS revenue_this_month
FROM agents a
LEFT JOIN (
    SELECT agent_id,
           COUNT(*) AS calls_this_month,
           SUM(creator_revenue_cents) AS revenue_this_month
    FROM runs
    WHERE status = 'success' AND started_at >= date_trunc('month', NOW())
    GROUP BY agent_id
) monthly ON monthly.agent_id = a.id
WHERE a.creator_id = $1
ORDER BY a.created_at DESC`

// ListAgentStatsForCreatorRow 创作者 dashboard 的每个 Agent 行。
//
// 注：lifetime_calls 来自 agents.total_calls (int32)；
// lifetime_revenue 来自 agents.total_revenue_cents (int64)；
// monthly 子查询 COUNT(*) 在 PostgreSQL 中返回 bigint，故 calls_this_month 用 int64；
// SUM(int32) 同理为 bigint。
type ListAgentStatsForCreatorRow struct {
	ID                uuid.UUID `db:"id" json:"id"`
	Slug              string    `db:"slug" json:"slug"`
	Name              string    `db:"name" json:"name"`
	Status            string    `db:"status" json:"status"`
	PricePerCallCents int32     `db:"price_per_call_cents" json:"price_per_call_cents"`
	LifetimeCalls     int32     `db:"lifetime_calls" json:"lifetime_calls"`
	LifetimeRevenue   int64     `db:"lifetime_revenue" json:"lifetime_revenue"`
	CallsThisMonth    int64     `db:"calls_this_month" json:"calls_this_month"`
	RevenueThisMonth  int64     `db:"revenue_this_month" json:"revenue_this_month"`
}

// ListAgentStatsForCreator 创作者每个 Agent 的本月调用 + 收入。
func (q *Queries) ListAgentStatsForCreator(ctx context.Context, creatorID uuid.UUID) ([]ListAgentStatsForCreatorRow, error) {
	rows, err := q.db.Query(ctx, listAgentStatsForCreator, creatorID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []ListAgentStatsForCreatorRow
	for rows.Next() {
		var r ListAgentStatsForCreatorRow
		if err := rows.Scan(
			&r.ID,
			&r.Slug,
			&r.Name,
			&r.Status,
			&r.PricePerCallCents,
			&r.LifetimeCalls,
			&r.LifetimeRevenue,
			&r.CallsThisMonth,
			&r.RevenueThisMonth,
		); err != nil {
			return nil, err
		}
		items = append(items, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const countAgentsByCreator = `-- name: CountAgentsByCreator :one
SELECT COUNT(*)::int AS total
FROM agents
WHERE creator_id = $1
  AND lifecycle_status = 'active'`

// CountAgentsByCreator 创作者当前 Agent 数（不包含已下架 disabled）。
func (q *Queries) CountAgentsByCreator(ctx context.Context, creatorID uuid.UUID) (int32, error) {
	row := q.db.QueryRow(ctx, countAgentsByCreator, creatorID)
	var total int32
	err := row.Scan(&total)
	return total, err
}

const countPublicAgentsByCreator = `-- name: CountPublicAgentsByCreator :one
SELECT COUNT(*)::int AS total
FROM agents
WHERE creator_id = $1
  AND lifecycle_status = 'active'
  AND visibility = 'public'`

// CountPublicAgentsByCreator 创作者当前公开 Agent 数。
func (q *Queries) CountPublicAgentsByCreator(ctx context.Context, creatorID uuid.UUID) (int32, error) {
	row := q.db.QueryRow(ctx, countPublicAgentsByCreator, creatorID)
	var total int32
	err := row.Scan(&total)
	return total, err
}

const countPendingAgentsByCreator = `-- name: CountPendingAgentsByCreator :one
SELECT COUNT(*)::int AS total
FROM agents
WHERE creator_id = $1
  AND lifecycle_status = 'active'
  AND certification_status = 'pending'`

// CountPendingAgentsByCreator 创作者人工处理队列 Agent 数。
func (q *Queries) CountPendingAgentsByCreator(ctx context.Context, creatorID uuid.UUID) (int32, error) {
	row := q.db.QueryRow(ctx, countPendingAgentsByCreator, creatorID)
	var total int32
	err := row.Scan(&total)
	return total, err
}
