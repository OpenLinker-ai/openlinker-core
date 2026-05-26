// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/agents.sql）。
//
// 模块 2 / 3 共享此文件。模块 3 的市场查询在 agents_market.sql.go 独立维护。

package db

import (
	"context"

	"github.com/google/uuid"
)

const agentsCount = `-- name: AgentsCount :one
SELECT COUNT(*)::int AS total
FROM agents
WHERE visibility = 'public' AND lifecycle_status = 'active'`

// AgentsCount 公开 Agent 总数（市场页用）。
func (q *Queries) AgentsCount(ctx context.Context) (int32, error) {
	row := q.db.QueryRow(ctx, agentsCount)
	var total int32
	err := row.Scan(&total)
	return total, err
}

// scanAgent 把一行扫描成 Agent 结构（按声明列顺序，给 RETURNING / SELECT 共用）。
//
// Phase 2 缺口 2 后列顺序：
//
//	id, creator_id, slug, name, description, endpoint_url,
//	endpoint_auth_header, price_per_call_cents, tags,
//	lifecycle_status, visibility, certification_status,
//	rejection_reason, certified_at,
//	total_calls, total_revenue_cents,
//	webhook_url, created_at, updated_at
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
		&a.LifecycleStatus,
		&a.Visibility,
		&a.CertificationStatus,
		&a.RejectionReason,
		&a.CertifiedAt,
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
    endpoint_auth_header, price_per_call_cents, tags, visibility
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9
)
RETURNING id, creator_id, slug, name, description, endpoint_url,
          endpoint_auth_header, price_per_call_cents, tags,
          lifecycle_status, visibility, certification_status,
          rejection_reason, certified_at,
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
	Visibility         string    `db:"visibility" json:"visibility"`
}

// CreateAgent 创作者新建 Agent。默认 lifecycle=active, visibility=public, certification=unreviewed。
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
		arg.Visibility,
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
    visibility = $9,
    updated_at = NOW()
WHERE id = $1 AND creator_id = $8 AND lifecycle_status = 'active'
RETURNING id, creator_id, slug, name, description, endpoint_url,
          endpoint_auth_header, price_per_call_cents, tags,
          lifecycle_status, visibility, certification_status,
          rejection_reason, certified_at,
          total_calls, total_revenue_cents,
          webhook_url, created_at, updated_at`

// UpdateAgentDraftParams 入参。Visibility 可由创作者直接改 (public/unlisted/private)。
type UpdateAgentDraftParams struct {
	ID                 uuid.UUID `db:"id" json:"id"`
	Name               string    `db:"name" json:"name"`
	Description        string    `db:"description" json:"description"`
	EndpointURL        string    `db:"endpoint_url" json:"endpoint_url"`
	EndpointAuthHeader *string   `db:"endpoint_auth_header" json:"endpoint_auth_header"`
	PricePerCallCents  int32     `db:"price_per_call_cents" json:"price_per_call_cents"`
	Tags               []string  `db:"tags" json:"tags"`
	CreatorID          uuid.UUID `db:"creator_id" json:"creator_id"`
	Visibility         string    `db:"visibility" json:"visibility"`
}

// UpdateAgentDraft 创作者编辑；命中 0 行表示 Agent 不存在 / 不属于该 creator / 已下架。
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
		arg.Visibility,
	)
	var a Agent
	err := scanAgent(row, &a)
	return a, err
}

const setAgentVisibilityForOwner = `-- name: SetAgentVisibilityForOwner :exec
UPDATE agents
SET visibility = $3,
    updated_at = NOW()
WHERE id = $1 AND creator_id = $2
  AND lifecycle_status = 'active'`

type SetAgentVisibilityForOwnerParams struct {
	ID         uuid.UUID `db:"id" json:"id"`
	CreatorID  uuid.UUID `db:"creator_id" json:"creator_id"`
	Visibility string    `db:"visibility" json:"visibility"`
}

func (q *Queries) SetAgentVisibilityForOwner(ctx context.Context, arg SetAgentVisibilityForOwnerParams) error {
	_, err := q.db.Exec(ctx, setAgentVisibilityForOwner, arg.ID, arg.CreatorID, arg.Visibility)
	return err
}

const getAgentByIDForOwner = `-- name: GetAgentByIDForOwner :one
SELECT id, creator_id, slug, name, description, endpoint_url,
       endpoint_auth_header, price_per_call_cents, tags,
       lifecycle_status, visibility, certification_status,
       rejection_reason, certified_at,
       total_calls, total_revenue_cents,
       webhook_url, created_at, updated_at
FROM agents
WHERE id = $1 AND creator_id = $2`

type GetAgentByIDForOwnerParams struct {
	ID        uuid.UUID `db:"id" json:"id"`
	CreatorID uuid.UUID `db:"creator_id" json:"creator_id"`
}

func (q *Queries) GetAgentByIDForOwner(ctx context.Context, arg GetAgentByIDForOwnerParams) (Agent, error) {
	row := q.db.QueryRow(ctx, getAgentByIDForOwner, arg.ID, arg.CreatorID)
	var a Agent
	err := scanAgent(row, &a)
	return a, err
}

const getAgentByID = `-- name: GetAgentByID :one
SELECT id, creator_id, slug, name, description, endpoint_url,
       endpoint_auth_header, price_per_call_cents, tags,
       lifecycle_status, visibility, certification_status,
       rejection_reason, certified_at,
       total_calls, total_revenue_cents,
       webhook_url, created_at, updated_at
FROM agents
WHERE id = $1`

func (q *Queries) GetAgentByID(ctx context.Context, id uuid.UUID) (Agent, error) {
	row := q.db.QueryRow(ctx, getAgentByID, id)
	var a Agent
	err := scanAgent(row, &a)
	return a, err
}

const listAgentsByCreator = `-- name: ListAgentsByCreator :many
SELECT id, creator_id, slug, name, description, endpoint_url,
       endpoint_auth_header, price_per_call_cents, tags,
       lifecycle_status, visibility, certification_status,
       rejection_reason, certified_at,
       total_calls, total_revenue_cents,
       webhook_url, created_at, updated_at
FROM agents
WHERE creator_id = $1
ORDER BY created_at DESC`

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
SET lifecycle_status = 'disabled',
    updated_at = NOW()
WHERE id = $1 AND creator_id = $2`

type DisableAgentParams struct {
	ID        uuid.UUID `db:"id" json:"id"`
	CreatorID uuid.UUID `db:"creator_id" json:"creator_id"`
}

// DisableAgent 创作者主动下架 (lifecycle_status='disabled')。
// 返回受影响行数：0 表示 Agent 不属于该 creator 或不存在。
func (q *Queries) DisableAgent(ctx context.Context, arg DisableAgentParams) (int64, error) {
	tag, err := q.db.Exec(ctx, disableAgent, arg.ID, arg.CreatorID)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

const requestCertification = `-- name: RequestCertification :exec
UPDATE agents
SET certification_status = 'pending',
    rejection_reason = NULL,
    updated_at = NOW()
WHERE id = $1 AND creator_id = $2
  AND certification_status IN ('unreviewed', 'rejected')`

type RequestCertificationParams struct {
	ID        uuid.UUID `db:"id" json:"id"`
	CreatorID uuid.UUID `db:"creator_id" json:"creator_id"`
}

// RequestCertification 创作者发起认证申请。
// 返回受影响行数：0 表示 Agent 不属于该 creator / 状态已 pending 或 certified（不能重复触发）。
func (q *Queries) RequestCertification(ctx context.Context, arg RequestCertificationParams) (int64, error) {
	tag, err := q.db.Exec(ctx, requestCertification, arg.ID, arg.CreatorID)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

const certifyAgent = `-- name: CertifyAgent :exec
UPDATE agents
SET certification_status = 'certified',
    certified_at = NOW(),
    rejection_reason = NULL,
    updated_at = NOW()
WHERE id = $1 AND certification_status = 'pending'`

// CertifyAgent 运营授予认证。
// 返回受影响行数：0 表示 Agent 不在 pending（不能 certify 未申请 / 已认证 / 已拒绝的）。
func (q *Queries) CertifyAgent(ctx context.Context, id uuid.UUID) (int64, error) {
	tag, err := q.db.Exec(ctx, certifyAgent, id)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

const rejectCertification = `-- name: RejectCertification :exec
UPDATE agents
SET certification_status = 'rejected',
    rejection_reason = $2,
    updated_at = NOW()
WHERE id = $1 AND certification_status = 'pending'`

type RejectCertificationParams struct {
	ID              uuid.UUID `db:"id" json:"id"`
	RejectionReason string    `db:"rejection_reason" json:"rejection_reason"`
}

// RejectCertification 运营拒绝认证（写原因）。
// 返回受影响行数：0 表示 Agent 不在 pending。
func (q *Queries) RejectCertification(ctx context.Context, arg RejectCertificationParams) (int64, error) {
	tag, err := q.db.Exec(ctx, rejectCertification, arg.ID, arg.RejectionReason)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

const checkSlugAvailable = `-- name: CheckSlugAvailable :one
SELECT NOT EXISTS(SELECT 1 FROM agents WHERE slug = $1) AS available`

func (q *Queries) CheckSlugAvailable(ctx context.Context, slug string) (bool, error) {
	row := q.db.QueryRow(ctx, checkSlugAvailable, slug)
	var available bool
	err := row.Scan(&available)
	return available, err
}

const listPendingAgents = `-- name: ListPendingAgents :many
SELECT a.id, a.creator_id, a.slug, a.name, a.description, a.endpoint_url,
       a.endpoint_auth_header, a.price_per_call_cents, a.tags,
       a.lifecycle_status, a.visibility, a.certification_status,
       a.rejection_reason, a.certified_at,
       a.total_calls, a.total_revenue_cents,
       a.webhook_url, a.created_at, a.updated_at,
       u.email AS creator_email,
       u.display_name AS creator_name
FROM agents a
JOIN users u ON u.id = a.creator_id
WHERE a.certification_status = 'pending'
  AND a.lifecycle_status = 'active'
ORDER BY a.created_at ASC`

// ListPendingAgentsRow Agent + 创作者邮箱 / 名字。
type ListPendingAgentsRow struct {
	Agent
	CreatorEmail string `db:"creator_email" json:"creator_email"`
	CreatorName  string `db:"creator_name" json:"creator_name"`
}

// ListPendingAgents 运营人工处理认证申请队列。
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
			&r.LifecycleStatus,
			&r.Visibility,
			&r.CertificationStatus,
			&r.RejectionReason,
			&r.CertifiedAt,
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

type IncrementAgentStatsParams struct {
	ID           uuid.UUID `db:"id" json:"id"`
	RevenueCents int64     `db:"total_revenue_cents" json:"total_revenue_cents"`
}

func (q *Queries) IncrementAgentStats(ctx context.Context, arg IncrementAgentStatsParams) error {
	_, err := q.db.Exec(ctx, incrementAgentStats, arg.ID, arg.RevenueCents)
	return err
}
