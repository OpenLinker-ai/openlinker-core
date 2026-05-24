// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/agents.sql）。
//
// 模块 2 / 3 共享此文件初始内容。各 subagent 在对应区块下追加 query。
//
// 文件分工避免冲突：
//   - 模块 2 (Agent 注册) 把 query 加到本文件
//   - 模块 3 (市场查询)   把 query 加到 agents_market.sql.go（独立文件）
//
// 这样两边各自维护一个 .go 文件，避免 git merge 冲突。

package db

import (
	"context"

	"github.com/google/uuid"
)

const agentsCount = `-- name: AgentsCount :one
SELECT COUNT(*)::int AS total FROM agents WHERE status = 'approved'`

// AgentsCount 已公开 Agent 总数（市场页用）。
func (q *Queries) AgentsCount(ctx context.Context) (int32, error) {
	row := q.db.QueryRow(ctx, agentsCount)
	var total int32
	err := row.Scan(&total)
	return total, err
}

// scanAgent 把一行扫描成 Agent 结构（按声明列顺序，给 RETURNING / SELECT 共用）。
//
// 内部 helper：保持 query const 与 scan 列顺序一致，
// 添加新字段时只需在此处和 SQL 同步即可。
func scanAgent(row interface {
	Scan(dest ...any) error
}, a *Agent) error {
	return row.Scan(
		&a.ID,
		&a.CreatorID,
		&a.Slug,
		&a.Name,
		&a.Description,
		&a.EndpointURL,
		&a.EndpointAuthHeader,
		&a.PricePerCallCents,
		&a.Tags,
		&a.Status,
		&a.RejectionReason,
		&a.ApprovedAt,
		&a.TotalCalls,
		&a.TotalRevenueCents,
		&a.WebhookURL,
		&a.CreatedAt,
		&a.UpdatedAt,
	)
}

const createAgent = `-- name: CreateAgent :one
INSERT INTO agents (
    creator_id, slug, name, description, endpoint_url,
    endpoint_auth_header, price_per_call_cents, tags, status, approved_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, 'approved', NOW()
)
RETURNING id, creator_id, slug, name, description, endpoint_url,
          endpoint_auth_header, price_per_call_cents, tags,
          status, rejection_reason, approved_at,
          total_calls, total_revenue_cents,
          webhook_url, created_at, updated_at`

// CreateAgentParams 入参。
type CreateAgentParams struct {
	CreatorID          uuid.UUID `db:"creator_id" json:"creator_id"`
	Slug               string    `db:"slug" json:"slug"`
	Name               string    `db:"name" json:"name"`
	Description        string    `db:"description" json:"description"`
	EndpointURL        string    `db:"endpoint_url" json:"endpoint_url"`
	EndpointAuthHeader *string   `db:"endpoint_auth_header" json:"endpoint_auth_header"`
	PricePerCallCents  int32     `db:"price_per_call_cents" json:"price_per_call_cents"`
	Tags               []string  `db:"tags" json:"tags"`
}

// CreateAgent 创作者新建 Agent（默认立即公开）。
func (q *Queries) CreateAgent(ctx context.Context, arg CreateAgentParams) (Agent, error) {
	row := q.db.QueryRow(ctx, createAgent,
		arg.CreatorID,
		arg.Slug,
		arg.Name,
		arg.Description,
		arg.EndpointURL,
		arg.EndpointAuthHeader,
		arg.PricePerCallCents,
		arg.Tags,
	)
	var a Agent
	err := scanAgent(row, &a)
	return a, err
}

const updateAgentDraft = `-- name: UpdateAgentDraft :one
UPDATE agents
SET name = $2,
    description = $3,
    endpoint_url = $4,
    endpoint_auth_header = $5,
    price_per_call_cents = $6,
    tags = $7,
    status = 'approved',
    approved_at = COALESCE(approved_at, NOW()),
    rejection_reason = NULL,
    updated_at = NOW()
WHERE id = $1 AND creator_id = $8 AND status IN ('approved', 'pending', 'rejected')
RETURNING id, creator_id, slug, name, description, endpoint_url,
          endpoint_auth_header, price_per_call_cents, tags,
          status, rejection_reason, approved_at,
          total_calls, total_revenue_cents,
          webhook_url, created_at, updated_at`

// UpdateAgentDraftParams 入参（disabled 外均可改，编辑后保持公开）。
type UpdateAgentDraftParams struct {
	ID                 uuid.UUID `db:"id" json:"id"`
	Name               string    `db:"name" json:"name"`
	Description        string    `db:"description" json:"description"`
	EndpointURL        string    `db:"endpoint_url" json:"endpoint_url"`
	EndpointAuthHeader *string   `db:"endpoint_auth_header" json:"endpoint_auth_header"`
	PricePerCallCents  int32     `db:"price_per_call_cents" json:"price_per_call_cents"`
	Tags               []string  `db:"tags" json:"tags"`
	CreatorID          uuid.UUID `db:"creator_id" json:"creator_id"`
}

// UpdateAgentDraft 创作者编辑草稿；命中 0 行表示不存在或状态不允许编辑（service 据此区分错误）。
func (q *Queries) UpdateAgentDraft(ctx context.Context, arg UpdateAgentDraftParams) (Agent, error) {
	row := q.db.QueryRow(ctx, updateAgentDraft,
		arg.ID,
		arg.Name,
		arg.Description,
		arg.EndpointURL,
		arg.EndpointAuthHeader,
		arg.PricePerCallCents,
		arg.Tags,
		arg.CreatorID,
	)
	var a Agent
	err := scanAgent(row, &a)
	return a, err
}

const getAgentByIDForOwner = `-- name: GetAgentByIDForOwner :one
SELECT id, creator_id, slug, name, description, endpoint_url,
       endpoint_auth_header, price_per_call_cents, tags,
       status, rejection_reason, approved_at,
       total_calls, total_revenue_cents,
       webhook_url, created_at, updated_at
FROM agents
WHERE id = $1 AND creator_id = $2`

// GetAgentByIDForOwnerParams 入参。
type GetAgentByIDForOwnerParams struct {
	ID        uuid.UUID `db:"id" json:"id"`
	CreatorID uuid.UUID `db:"creator_id" json:"creator_id"`
}

// GetAgentByIDForOwner 创作者按 id 查自己的 Agent（含人工处理队列 / 已下架）。
func (q *Queries) GetAgentByIDForOwner(ctx context.Context, arg GetAgentByIDForOwnerParams) (Agent, error) {
	row := q.db.QueryRow(ctx, getAgentByIDForOwner, arg.ID, arg.CreatorID)
	var a Agent
	err := scanAgent(row, &a)
	return a, err
}

const getAgentByID = `-- name: GetAgentByID :one
SELECT id, creator_id, slug, name, description, endpoint_url,
       endpoint_auth_header, price_per_call_cents, tags,
       status, rejection_reason, approved_at,
       total_calls, total_revenue_cents,
       webhook_url, created_at, updated_at
FROM agents
WHERE id = $1`

// GetAgentByID 通用按 id 查（不限制 creator）。
func (q *Queries) GetAgentByID(ctx context.Context, id uuid.UUID) (Agent, error) {
	row := q.db.QueryRow(ctx, getAgentByID, id)
	var a Agent
	err := scanAgent(row, &a)
	return a, err
}

const listAgentsByCreator = `-- name: ListAgentsByCreator :many
SELECT id, creator_id, slug, name, description, endpoint_url,
       endpoint_auth_header, price_per_call_cents, tags,
       status, rejection_reason, approved_at,
       total_calls, total_revenue_cents,
       webhook_url, created_at, updated_at
FROM agents
WHERE creator_id = $1
ORDER BY created_at DESC`

// ListAgentsByCreator 创作者中心列表（含所有状态，倒序）。
func (q *Queries) ListAgentsByCreator(ctx context.Context, creatorID uuid.UUID) ([]Agent, error) {
	rows, err := q.db.Query(ctx, listAgentsByCreator, creatorID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []Agent
	for rows.Next() {
		var a Agent
		if err := scanAgent(rows, &a); err != nil {
			return nil, err
		}
		items = append(items, a)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const disableAgent = `-- name: DisableAgent :exec
UPDATE agents
SET status = 'disabled',
    updated_at = NOW()
WHERE id = $1 AND creator_id = $2`

// DisableAgentParams 入参。
type DisableAgentParams struct {
	ID        uuid.UUID `db:"id" json:"id"`
	CreatorID uuid.UUID `db:"creator_id" json:"creator_id"`
}

// DisableAgent 创作者主动下架。
// 返回受影响行数：0 表示 Agent 不属于该 creator 或不存在。
func (q *Queries) DisableAgent(ctx context.Context, arg DisableAgentParams) (int64, error) {
	tag, err := q.db.Exec(ctx, disableAgent, arg.ID, arg.CreatorID)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

const approveAgent = `-- name: ApproveAgent :exec
UPDATE agents
SET status = 'approved',
    approved_at = NOW(),
    rejection_reason = NULL,
    updated_at = NOW()
WHERE id = $1 AND status IN ('pending', 'rejected')`

// ApproveAgent admin/运营手动放行。
// 返回受影响行数：0 表示 Agent 不存在或状态不允许（如 disabled）。
func (q *Queries) ApproveAgent(ctx context.Context, id uuid.UUID) (int64, error) {
	tag, err := q.db.Exec(ctx, approveAgent, id)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

const rejectAgent = `-- name: RejectAgent :exec
UPDATE agents
SET status = 'rejected',
    rejection_reason = $2,
    updated_at = NOW()
WHERE id = $1 AND status = 'pending'`

// RejectAgentParams 入参。
type RejectAgentParams struct {
	ID              uuid.UUID `db:"id" json:"id"`
	RejectionReason string    `db:"rejection_reason" json:"rejection_reason"`
}

// RejectAgent admin/运营手动拒绝。
// 返回受影响行数：0 表示 Agent 不存在或不在 pending（不能拒已 approved/rejected/disabled 的）。
func (q *Queries) RejectAgent(ctx context.Context, arg RejectAgentParams) (int64, error) {
	tag, err := q.db.Exec(ctx, rejectAgent, arg.ID, arg.RejectionReason)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

const checkSlugAvailable = `-- name: CheckSlugAvailable :one
SELECT NOT EXISTS(SELECT 1 FROM agents WHERE slug = $1) AS available`

// CheckSlugAvailable 返回 slug 是否未被占用。
func (q *Queries) CheckSlugAvailable(ctx context.Context, slug string) (bool, error) {
	row := q.db.QueryRow(ctx, checkSlugAvailable, slug)
	var available bool
	err := row.Scan(&available)
	return available, err
}

const listPendingAgents = `-- name: ListPendingAgents :many
SELECT a.id, a.creator_id, a.slug, a.name, a.description, a.endpoint_url,
       a.endpoint_auth_header, a.price_per_call_cents, a.tags,
       a.status, a.rejection_reason, a.approved_at,
       a.total_calls, a.total_revenue_cents,
       a.webhook_url, a.created_at, a.updated_at,
       u.email AS creator_email,
       u.display_name AS creator_name
FROM agents a
JOIN users u ON u.id = a.creator_id
WHERE a.status = 'pending'
ORDER BY a.created_at ASC`

// ListPendingAgentsRow JOIN 结果行：Agent + 创作者邮箱 / 名字。
type ListPendingAgentsRow struct {
	Agent
	CreatorEmail string `db:"creator_email" json:"creator_email"`
	CreatorName  string `db:"creator_name" json:"creator_name"`
}

// ListPendingAgents admin/运营人工处理队列（默认发布不进入该队列）。
func (q *Queries) ListPendingAgents(ctx context.Context) ([]ListPendingAgentsRow, error) {
	rows, err := q.db.Query(ctx, listPendingAgents)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []ListPendingAgentsRow
	for rows.Next() {
		var r ListPendingAgentsRow
		if err := rows.Scan(
			&r.ID,
			&r.CreatorID,
			&r.Slug,
			&r.Name,
			&r.Description,
			&r.EndpointURL,
			&r.EndpointAuthHeader,
			&r.PricePerCallCents,
			&r.Tags,
			&r.Status,
			&r.RejectionReason,
			&r.ApprovedAt,
			&r.TotalCalls,
			&r.TotalRevenueCents,
			&r.WebhookURL,
			&r.CreatedAt,
			&r.UpdatedAt,
			&r.CreatorEmail,
			&r.CreatorName,
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

const incrementAgentStats = `-- name: IncrementAgentStats :exec
UPDATE agents
SET total_calls = total_calls + 1,
    total_revenue_cents = total_revenue_cents + $2,
    updated_at = NOW()
WHERE id = $1`

// IncrementAgentStatsParams 入参。
//
// RevenueCents 是本次调用归属创作者的金额（cost - platform_fee），
// 用 int64 与 agents.total_revenue_cents (BIGINT) 一致，避免长期累计溢出。
type IncrementAgentStatsParams struct {
	ID           uuid.UUID `db:"id" json:"id"`
	RevenueCents int64     `db:"total_revenue_cents" json:"total_revenue_cents"`
}

// IncrementAgentStats 调用成功后累加 agent.total_calls + total_revenue_cents（模块 4 计费用）。
func (q *Queries) IncrementAgentStats(ctx context.Context, arg IncrementAgentStatsParams) error {
	_, err := q.db.Exec(ctx, incrementAgentStats, arg.ID, arg.RevenueCents)
	return err
}
