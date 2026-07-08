// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/agents.sql）。
//
// 模块 2 / 3 共享此文件。模块 3 的市场查询在 agents_market.sql.go 独立维护。

package db

import (
	"context"
	"time"

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
//	webhook_url, connection_mode, mcp_tool_name, created_at, updated_at
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
		&a.ConnectionMode,
		&a.MCPToolName,
		&a.CreatedAt,
		&a.UpdatedAt,
	)
}

const createAgent = `-- name: CreateAgent :one
INSERT INTO agents (
    creator_id, slug, name, description, endpoint_url,
    endpoint_auth_header, price_per_call_cents, tags, visibility,
    connection_mode, mcp_tool_name
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
)
RETURNING id, creator_id, slug, name, description, endpoint_url,
          endpoint_auth_header, price_per_call_cents, tags,
          lifecycle_status, visibility, certification_status,
          rejection_reason, certified_at,
          total_calls, total_revenue_cents,
          webhook_url, connection_mode, mcp_tool_name, created_at, updated_at`

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
	ConnectionMode     string    `db:"connection_mode" json:"connection_mode"`
	MCPToolName        *string   `db:"mcp_tool_name" json:"mcp_tool_name"`
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
		arg.ConnectionMode,
		arg.MCPToolName,
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
    connection_mode = $10,
    mcp_tool_name = $11,
    updated_at = NOW()
WHERE id = $1 AND creator_id = $8 AND lifecycle_status = 'active'
RETURNING id, creator_id, slug, name, description, endpoint_url,
          endpoint_auth_header, price_per_call_cents, tags,
          lifecycle_status, visibility, certification_status,
          rejection_reason, certified_at,
          total_calls, total_revenue_cents,
          webhook_url, connection_mode, mcp_tool_name, created_at, updated_at`

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
	ConnectionMode     string    `db:"connection_mode" json:"connection_mode"`
	MCPToolName        *string   `db:"mcp_tool_name" json:"mcp_tool_name"`
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
		arg.ConnectionMode,
		arg.MCPToolName,
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
       webhook_url, connection_mode, mcp_tool_name, created_at, updated_at
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
       webhook_url, connection_mode, mcp_tool_name, created_at, updated_at
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
       webhook_url, connection_mode, mcp_tool_name, created_at, updated_at
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

const listAgentsByCreatorPage = `-- name: ListAgentsByCreatorPage :many
SELECT a.id, a.creator_id, a.slug, a.name, a.description, a.endpoint_url,
       a.endpoint_auth_header, a.price_per_call_cents, a.tags,
       a.lifecycle_status, a.visibility, a.certification_status,
       a.rejection_reason, a.certified_at,
       a.total_calls, a.total_revenue_cents,
       a.webhook_url, a.connection_mode, a.mcp_tool_name, a.created_at, a.updated_at,
       COALESCE(av.availability_status, 'unknown') AS availability_status,
       av.last_successful_run_at AS availability_last_successful_run_at,
       av.last_failed_run_at AS availability_last_failed_run_at,
       av.last_checked_at AS availability_last_checked_at,
       COALESCE(av.consecutive_failures, 0)::int AS availability_consecutive_failures,
       rt.last_runtime_token_used_at,
       COALESCE(monthly.calls_this_month, 0)::bigint AS calls_this_month,
       COALESCE(monthly.revenue_this_month, 0)::bigint AS revenue_this_month
FROM agents a
LEFT JOIN agent_availability_snapshots av ON av.agent_id = a.id
LEFT JOIN LATERAL (
    SELECT MAX(last_used_at) AS last_runtime_token_used_at
    FROM agent_tokens
    WHERE agent_id = a.id
      AND revoked_at IS NULL
      AND status = 'active_runtime'
      AND 'agent:pull' = ANY(scopes)
) rt ON TRUE
LEFT JOIN LATERAL (
    SELECT
        COUNT(*)::bigint AS calls_this_month,
        COALESCE(SUM(creator_revenue_cents), 0)::bigint AS revenue_this_month
    FROM runs r
    WHERE r.agent_id = a.id
      AND r.status = 'success'
      AND r.started_at >= date_trunc('month', NOW())
) monthly ON TRUE
WHERE a.creator_id = $1
  AND (
    $2::text = ''
    OR a.slug ILIKE '%' || $2 || '%'
    OR a.name ILIKE '%' || $2 || '%'
    OR a.description ILIKE '%' || $2 || '%'
    OR a.endpoint_url ILIKE '%' || $2 || '%'
    OR array_to_string(a.tags, ' ') ILIKE '%' || $2 || '%'
  )
  AND (
    $3::text = ''
    OR ($3 = 'active' AND a.lifecycle_status = 'active')
    OR ($3 = 'online' AND a.lifecycle_status = 'active' AND (
      COALESCE(av.availability_status, 'unknown') = 'healthy'
      OR (
        av.last_successful_run_at IS NOT NULL
        AND COALESCE(av.availability_status, 'unknown') <> 'unreachable'
      )
    ) AND NOT (
      a.connection_mode IN ('runtime_pull', 'runtime_ws')
      AND COALESCE(rt.last_runtime_token_used_at < NOW() - INTERVAL '5 minutes', TRUE)
    ))
    OR ($3 = 'offline' AND a.lifecycle_status = 'active' AND COALESCE(av.availability_status, 'unknown') <> 'degraded' AND NOT (
      (
        COALESCE(av.availability_status, 'unknown') = 'healthy'
        OR (
          av.last_successful_run_at IS NOT NULL
          AND COALESCE(av.availability_status, 'unknown') <> 'unreachable'
        )
      )
      AND NOT (
        a.connection_mode IN ('runtime_pull', 'runtime_ws')
        AND COALESCE(rt.last_runtime_token_used_at < NOW() - INTERVAL '5 minutes', TRUE)
      )
    ))
    OR ($3 = 'degraded' AND COALESCE(av.availability_status, 'unknown') = 'degraded')
    OR ($3 = 'disabled' AND a.lifecycle_status = 'disabled')
    OR ($3 = 'review' AND a.certification_status = 'pending')
	  )
	  AND ($4::text = '' OR a.visibility = $4)
	  AND ($5::text = '' OR a.certification_status = $5)
	  AND (
	    cardinality($9::text[]) = 0
	    OR EXISTS (
	      SELECT 1
	      FROM agent_skills askill
	      WHERE askill.agent_id = a.id
	        AND askill.skill_id = ANY($9::text[])
	    )
	  )
	ORDER BY
  CASE WHEN $6 = 'name' THEN lower(a.name) END ASC,
  CASE WHEN $6 = 'created_at' THEN a.created_at END DESC,
  CASE WHEN $6 = 'lifetime_calls' THEN a.total_calls END DESC,
  CASE WHEN $6 = 'calls_this_month' THEN COALESCE(monthly.calls_this_month, 0) END DESC,
  a.created_at DESC
LIMIT $7 OFFSET $8`

type ListAgentsByCreatorPageParams struct {
	CreatorID           uuid.UUID `db:"creator_id" json:"creator_id"`
	Query               string    `db:"query" json:"query"`
	Status              string    `db:"status" json:"status"`
	Visibility          string    `db:"visibility" json:"visibility"`
	CertificationStatus string    `db:"certification_status" json:"certification_status"`
	SortBy              string    `db:"sort_by" json:"sort_by"`
	Limit               int32     `db:"limit" json:"limit"`
	Offset              int32     `db:"offset" json:"offset"`
	SkillIds            []string  `db:"skill_ids" json:"skill_ids"`
}

type ListAgentsByCreatorPageRow struct {
	Agent
	AvailabilityStatus              string     `db:"availability_status" json:"availability_status"`
	AvailabilityLastSuccessfulRunAt *time.Time `db:"availability_last_successful_run_at" json:"availability_last_successful_run_at"`
	AvailabilityLastFailedRunAt     *time.Time `db:"availability_last_failed_run_at" json:"availability_last_failed_run_at"`
	AvailabilityLastCheckedAt       *time.Time `db:"availability_last_checked_at" json:"availability_last_checked_at"`
	AvailabilityConsecutiveFailures int32      `db:"availability_consecutive_failures" json:"availability_consecutive_failures"`
	LastRuntimeTokenUsedAt          *time.Time `db:"last_runtime_token_used_at" json:"last_runtime_token_used_at"`
	CallsThisMonth                  int64      `db:"calls_this_month" json:"calls_this_month"`
	RevenueThisMonth                int64      `db:"revenue_this_month" json:"revenue_this_month"`
}

func (q *Queries) ListAgentsByCreatorPage(ctx context.Context, arg ListAgentsByCreatorPageParams) ([]ListAgentsByCreatorPageRow, error) {
	rows, err := q.db.Query(ctx, listAgentsByCreatorPage, arg.CreatorID, arg.Query, arg.Status, arg.Visibility, arg.CertificationStatus, arg.SortBy, arg.Limit, arg.Offset, arg.SkillIds)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []ListAgentsByCreatorPageRow
	for rows.Next() {
		var r ListAgentsByCreatorPageRow
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
			&r.ConnectionMode,
			&r.MCPToolName,
			&r.CreatedAt,
			&r.UpdatedAt,
			&r.AvailabilityStatus,
			&r.AvailabilityLastSuccessfulRunAt,
			&r.AvailabilityLastFailedRunAt,
			&r.AvailabilityLastCheckedAt,
			&r.AvailabilityConsecutiveFailures,
			&r.LastRuntimeTokenUsedAt,
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

const countAgentsByCreatorFiltered = `-- name: CountAgentsByCreatorFiltered :one
SELECT COUNT(*)::int AS total
FROM agents a
LEFT JOIN agent_availability_snapshots av ON av.agent_id = a.id
LEFT JOIN LATERAL (
    SELECT MAX(last_used_at) AS last_runtime_token_used_at
    FROM agent_tokens
    WHERE agent_id = a.id
      AND revoked_at IS NULL
      AND status = 'active_runtime'
      AND 'agent:pull' = ANY(scopes)
) rt ON TRUE
WHERE a.creator_id = $1
  AND (
    $2::text = ''
    OR a.slug ILIKE '%' || $2 || '%'
    OR a.name ILIKE '%' || $2 || '%'
    OR a.description ILIKE '%' || $2 || '%'
    OR a.endpoint_url ILIKE '%' || $2 || '%'
    OR array_to_string(a.tags, ' ') ILIKE '%' || $2 || '%'
  )
  AND (
    $3::text = ''
    OR ($3 = 'active' AND a.lifecycle_status = 'active')
    OR ($3 = 'online' AND a.lifecycle_status = 'active' AND (
      COALESCE(av.availability_status, 'unknown') = 'healthy'
      OR (
        av.last_successful_run_at IS NOT NULL
        AND COALESCE(av.availability_status, 'unknown') <> 'unreachable'
      )
    ) AND NOT (
      a.connection_mode IN ('runtime_pull', 'runtime_ws')
      AND COALESCE(rt.last_runtime_token_used_at < NOW() - INTERVAL '5 minutes', TRUE)
    ))
    OR ($3 = 'offline' AND a.lifecycle_status = 'active' AND COALESCE(av.availability_status, 'unknown') <> 'degraded' AND NOT (
      (
        COALESCE(av.availability_status, 'unknown') = 'healthy'
        OR (
          av.last_successful_run_at IS NOT NULL
          AND COALESCE(av.availability_status, 'unknown') <> 'unreachable'
        )
      )
      AND NOT (
        a.connection_mode IN ('runtime_pull', 'runtime_ws')
        AND COALESCE(rt.last_runtime_token_used_at < NOW() - INTERVAL '5 minutes', TRUE)
      )
    ))
    OR ($3 = 'degraded' AND COALESCE(av.availability_status, 'unknown') = 'degraded')
    OR ($3 = 'disabled' AND a.lifecycle_status = 'disabled')
    OR ($3 = 'review' AND a.certification_status = 'pending')
	  )
	  AND ($4::text = '' OR a.visibility = $4)
	  AND ($5::text = '' OR a.certification_status = $5)
	  AND (
	    cardinality($6::text[]) = 0
	    OR EXISTS (
	      SELECT 1
	      FROM agent_skills askill
	      WHERE askill.agent_id = a.id
	        AND askill.skill_id = ANY($6::text[])
	    )
	  )`

type CountAgentsByCreatorFilteredParams struct {
	CreatorID           uuid.UUID `db:"creator_id" json:"creator_id"`
	Query               string    `db:"query" json:"query"`
	Status              string    `db:"status" json:"status"`
	Visibility          string    `db:"visibility" json:"visibility"`
	CertificationStatus string    `db:"certification_status" json:"certification_status"`
	SkillIds            []string  `db:"skill_ids" json:"skill_ids"`
}

func (q *Queries) CountAgentsByCreatorFiltered(ctx context.Context, arg CountAgentsByCreatorFilteredParams) (int32, error) {
	row := q.db.QueryRow(ctx, countAgentsByCreatorFiltered, arg.CreatorID, arg.Query, arg.Status, arg.Visibility, arg.CertificationStatus, arg.SkillIds)
	var total int32
	err := row.Scan(&total)
	return total, err
}

const countAgentBucketsByCreator = `-- name: CountAgentBucketsByCreator :one
SELECT
  COUNT(*) FILTER (WHERE a.lifecycle_status = 'active')::int AS total,
  COUNT(*) FILTER (WHERE a.lifecycle_status = 'active' AND (
    COALESCE(av.availability_status, 'unknown') = 'healthy'
    OR (
      av.last_successful_run_at IS NOT NULL
      AND COALESCE(av.availability_status, 'unknown') <> 'unreachable'
    )
  ) AND NOT (
    a.connection_mode IN ('runtime_pull', 'runtime_ws')
    AND COALESCE(rt.last_runtime_token_used_at < NOW() - INTERVAL '5 minutes', TRUE)
  ))::int AS online,
  COUNT(*) FILTER (WHERE a.lifecycle_status = 'active' AND a.visibility = 'public')::int AS public,
  COUNT(*) FILTER (WHERE a.lifecycle_status = 'active' AND a.visibility = 'unlisted')::int AS unlisted,
  COUNT(*) FILTER (WHERE a.lifecycle_status = 'active' AND a.visibility = 'private')::int AS private,
  COUNT(*) FILTER (WHERE a.lifecycle_status = 'active' AND a.certification_status = 'pending')::int AS pending
FROM agents a
LEFT JOIN agent_availability_snapshots av ON av.agent_id = a.id
LEFT JOIN LATERAL (
    SELECT MAX(last_used_at) AS last_runtime_token_used_at
    FROM agent_tokens
    WHERE agent_id = a.id
      AND revoked_at IS NULL
      AND status = 'active_runtime'
      AND 'agent:pull' = ANY(scopes)
) rt ON TRUE
WHERE a.creator_id = $1`

type CountAgentBucketsByCreatorRow struct {
	Total    int32 `db:"total" json:"total"`
	Online   int32 `db:"online" json:"online"`
	Public   int32 `db:"public" json:"public"`
	Unlisted int32 `db:"unlisted" json:"unlisted"`
	Private  int32 `db:"private" json:"private"`
	Pending  int32 `db:"pending" json:"pending"`
}

func (q *Queries) CountAgentBucketsByCreator(ctx context.Context, creatorID uuid.UUID) (CountAgentBucketsByCreatorRow, error) {
	row := q.db.QueryRow(ctx, countAgentBucketsByCreator, creatorID)
	var r CountAgentBucketsByCreatorRow
	err := row.Scan(&r.Total, &r.Online, &r.Public, &r.Unlisted, &r.Private, &r.Pending)
	return r, err
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
       a.webhook_url, a.connection_mode, a.mcp_tool_name, a.created_at, a.updated_at,
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
			&r.ConnectionMode,
			&r.MCPToolName,
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

const listAdminAgents = `-- name: ListAdminAgents :many
SELECT a.id, a.creator_id, a.slug, a.name, a.description, a.endpoint_url,
       a.endpoint_auth_header, a.price_per_call_cents, a.tags,
       a.lifecycle_status, a.visibility, a.certification_status,
       a.rejection_reason, a.certified_at,
       a.total_calls, a.total_revenue_cents,
       a.webhook_url, a.connection_mode, a.mcp_tool_name, a.created_at, a.updated_at,
       u.email AS creator_email,
       u.display_name AS creator_name,
       COALESCE(task_stats.recommended_task_count, 0)::int AS recommended_task_count,
       COALESCE(task_stats.chosen_task_count, 0)::int AS chosen_task_count,
       COALESCE(task_stats.claimed_task_count, 0)::int AS claimed_task_count,
       COALESCE(task_stats.completed_task_count, 0)::int AS completed_task_count,
       run_stats.last_run_at
FROM agents a
JOIN users u ON u.id = a.creator_id
LEFT JOIN LATERAL (
    SELECT
        COUNT(*) FILTER (WHERE a.id = ANY(t.recommended_agent_ids))::int AS recommended_task_count,
        COUNT(*) FILTER (WHERE t.chosen_agent_id = a.id)::int AS chosen_task_count,
        COUNT(*) FILTER (WHERE t.claimed_agent_id = a.id)::int AS claimed_task_count,
        COUNT(*) FILTER (WHERE t.claimed_agent_id = a.id AND t.completed_at IS NOT NULL)::int AS completed_task_count
    FROM task_queries t
    WHERE a.id = ANY(t.recommended_agent_ids)
       OR t.chosen_agent_id = a.id
       OR t.claimed_agent_id = a.id
) task_stats ON TRUE
LEFT JOIN LATERAL (
    SELECT MAX(started_at) AS last_run_at
    FROM runs
    WHERE agent_id = a.id
) run_stats ON TRUE
WHERE (
    $1::text = ''
    OR a.slug ILIKE '%' || $1 || '%'
    OR a.name ILIKE '%' || $1 || '%'
    OR a.description ILIKE '%' || $1 || '%'
    OR u.email ILIKE '%' || $1 || '%'
    OR u.display_name ILIKE '%' || $1 || '%'
  )
  AND ($2::text = '' OR a.lifecycle_status = $2)
  AND ($3::text = '' OR a.visibility = $3)
  AND ($4::text = '' OR a.certification_status = $4)
ORDER BY a.updated_at DESC, a.created_at DESC
LIMIT $5 OFFSET $6`

type ListAdminAgentsParams struct {
	Query               string `db:"query" json:"query"`
	LifecycleStatus     string `db:"lifecycle_status" json:"lifecycle_status"`
	Visibility          string `db:"visibility" json:"visibility"`
	CertificationStatus string `db:"certification_status" json:"certification_status"`
	Limit               int32  `db:"limit" json:"limit"`
	Offset              int32  `db:"offset" json:"offset"`
}

type ListAdminAgentsRow struct {
	Agent
	CreatorEmail         string     `db:"creator_email" json:"creator_email"`
	CreatorName          string     `db:"creator_name" json:"creator_name"`
	RecommendedTaskCount int32      `db:"recommended_task_count" json:"recommended_task_count"`
	ChosenTaskCount      int32      `db:"chosen_task_count" json:"chosen_task_count"`
	ClaimedTaskCount     int32      `db:"claimed_task_count" json:"claimed_task_count"`
	CompletedTaskCount   int32      `db:"completed_task_count" json:"completed_task_count"`
	LastRunAt            *time.Time `db:"last_run_at" json:"last_run_at"`
}

// ListAdminAgents 管理台 Agent 列表。
func (q *Queries) ListAdminAgents(ctx context.Context, arg ListAdminAgentsParams) ([]ListAdminAgentsRow, error) {
	rows, err := q.db.Query(ctx, listAdminAgents, arg.Query, arg.LifecycleStatus, arg.Visibility, arg.CertificationStatus, arg.Limit, arg.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []ListAdminAgentsRow
	for rows.Next() {
		var r ListAdminAgentsRow
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
			&r.ConnectionMode,
			&r.MCPToolName,
			&r.CreatedAt,
			&r.UpdatedAt,
			&r.CreatorEmail,
			&r.CreatorName,
			&r.RecommendedTaskCount,
			&r.ChosenTaskCount,
			&r.ClaimedTaskCount,
			&r.CompletedTaskCount,
			&r.LastRunAt,
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

const countAdminAgents = `-- name: CountAdminAgents :one
SELECT COUNT(*)::int AS total
FROM agents a
JOIN users u ON u.id = a.creator_id
WHERE (
    $1::text = ''
    OR a.slug ILIKE '%' || $1 || '%'
    OR a.name ILIKE '%' || $1 || '%'
    OR a.description ILIKE '%' || $1 || '%'
    OR u.email ILIKE '%' || $1 || '%'
    OR u.display_name ILIKE '%' || $1 || '%'
  )
  AND ($2::text = '' OR a.lifecycle_status = $2)
  AND ($3::text = '' OR a.visibility = $3)
  AND ($4::text = '' OR a.certification_status = $4)`

type CountAdminAgentsParams struct {
	Query               string `db:"query" json:"query"`
	LifecycleStatus     string `db:"lifecycle_status" json:"lifecycle_status"`
	Visibility          string `db:"visibility" json:"visibility"`
	CertificationStatus string `db:"certification_status" json:"certification_status"`
}

func (q *Queries) CountAdminAgents(ctx context.Context, arg CountAdminAgentsParams) (int32, error) {
	row := q.db.QueryRow(ctx, countAdminAgents, arg.Query, arg.LifecycleStatus, arg.Visibility, arg.CertificationStatus)
	var total int32
	err := row.Scan(&total)
	return total, err
}

const updateAdminAgentModeration = `-- name: UpdateAdminAgentModeration :one
UPDATE agents
SET lifecycle_status = $2,
    visibility = $3,
    certification_status = $4,
    rejection_reason = CASE
        WHEN $4 = 'rejected' THEN NULLIF($5, '')
        WHEN $4 IN ('unreviewed', 'pending', 'certified') THEN NULL
        ELSE rejection_reason
    END,
    certified_at = CASE
        WHEN $4 = 'certified' THEN COALESCE(certified_at, NOW())
        WHEN $4 IN ('unreviewed', 'pending', 'rejected') THEN NULL
        ELSE certified_at
    END,
    updated_at = NOW()
WHERE id = $1
RETURNING id, creator_id, slug, name, description, endpoint_url,
          endpoint_auth_header, price_per_call_cents, tags,
          lifecycle_status, visibility, certification_status,
          rejection_reason, certified_at,
          total_calls, total_revenue_cents,
          webhook_url, connection_mode, mcp_tool_name, created_at, updated_at`

type UpdateAdminAgentModerationParams struct {
	ID                  uuid.UUID `db:"id" json:"id"`
	LifecycleStatus     string    `db:"lifecycle_status" json:"lifecycle_status"`
	Visibility          string    `db:"visibility" json:"visibility"`
	CertificationStatus string    `db:"certification_status" json:"certification_status"`
	RejectionReason     string    `db:"rejection_reason" json:"rejection_reason"`
}

// UpdateAdminAgentModeration 管理台调整 Agent 三维状态。
func (q *Queries) UpdateAdminAgentModeration(ctx context.Context, arg UpdateAdminAgentModerationParams) (Agent, error) {
	row := q.db.QueryRow(ctx, updateAdminAgentModeration, arg.ID, arg.LifecycleStatus, arg.Visibility, arg.CertificationStatus, arg.RejectionReason)
	var a Agent
	err := scanAgent(row, &a)
	return a, err
}

const getAdminSummary = `-- name: GetAdminSummary :one
SELECT
  (SELECT COUNT(*)::int FROM users WHERE deleted_at IS NULL) AS total_users,
  (SELECT COUNT(*)::int FROM users WHERE deleted_at IS NULL AND is_admin) AS admin_users,
  (SELECT COUNT(*)::int FROM users WHERE deleted_at IS NULL AND is_creator) AS creator_users,
  (SELECT COUNT(*)::int FROM users WHERE deleted_at IS NULL AND creator_verified) AS verified_creators,
  (SELECT COUNT(*)::int FROM agents) AS total_agents,
  (SELECT COUNT(*)::int FROM agents WHERE lifecycle_status = 'active') AS active_agents,
  (SELECT COUNT(*)::int FROM agents WHERE lifecycle_status = 'disabled') AS disabled_agents,
  (SELECT COUNT(*)::int FROM agents WHERE certification_status = 'pending') AS pending_agents,
  (SELECT COUNT(*)::int FROM agents WHERE certification_status = 'certified') AS certified_agents,
  (SELECT COUNT(*)::int FROM task_queries) AS total_tasks,
  (SELECT COUNT(*)::int FROM task_queries WHERE visibility = 'public') AS public_tasks,
  (SELECT COUNT(*)::int FROM task_queries WHERE visibility = 'private') AS private_tasks,
  (SELECT COUNT(*)::int FROM task_queries WHERE completed_at IS NULL AND claimed_agent_id IS NULL AND chosen_agent_id IS NULL AND cardinality(recommended_agent_ids) > 0) AS open_tasks,
  (SELECT COUNT(*)::int FROM task_queries WHERE completed_at IS NULL AND claimed_agent_id IS NOT NULL) AS claimed_tasks,
  (SELECT COUNT(*)::int FROM task_queries WHERE completed_at IS NOT NULL) AS completed_tasks,
  (SELECT COUNT(*)::int FROM task_queries WHERE delivery_status = 'accepted') AS accepted_tasks,
  (SELECT COUNT(*)::int FROM task_queries WHERE delivery_status = 'revision_requested') AS revision_requested_tasks`

type GetAdminSummaryRow struct {
	TotalUsers             int32 `db:"total_users" json:"total_users"`
	AdminUsers             int32 `db:"admin_users" json:"admin_users"`
	CreatorUsers           int32 `db:"creator_users" json:"creator_users"`
	VerifiedCreators       int32 `db:"verified_creators" json:"verified_creators"`
	TotalAgents            int32 `db:"total_agents" json:"total_agents"`
	ActiveAgents           int32 `db:"active_agents" json:"active_agents"`
	DisabledAgents         int32 `db:"disabled_agents" json:"disabled_agents"`
	PendingAgents          int32 `db:"pending_agents" json:"pending_agents"`
	CertifiedAgents        int32 `db:"certified_agents" json:"certified_agents"`
	TotalTasks             int32 `db:"total_tasks" json:"total_tasks"`
	PublicTasks            int32 `db:"public_tasks" json:"public_tasks"`
	PrivateTasks           int32 `db:"private_tasks" json:"private_tasks"`
	OpenTasks              int32 `db:"open_tasks" json:"open_tasks"`
	ClaimedTasks           int32 `db:"claimed_tasks" json:"claimed_tasks"`
	CompletedTasks         int32 `db:"completed_tasks" json:"completed_tasks"`
	AcceptedTasks          int32 `db:"accepted_tasks" json:"accepted_tasks"`
	RevisionRequestedTasks int32 `db:"revision_requested_tasks" json:"revision_requested_tasks"`
}

func (q *Queries) GetAdminSummary(ctx context.Context) (GetAdminSummaryRow, error) {
	row := q.db.QueryRow(ctx, getAdminSummary)
	var r GetAdminSummaryRow
	err := row.Scan(
		&r.TotalUsers,
		&r.AdminUsers,
		&r.CreatorUsers,
		&r.VerifiedCreators,
		&r.TotalAgents,
		&r.ActiveAgents,
		&r.DisabledAgents,
		&r.PendingAgents,
		&r.CertifiedAgents,
		&r.TotalTasks,
		&r.PublicTasks,
		&r.PrivateTasks,
		&r.OpenTasks,
		&r.ClaimedTasks,
		&r.CompletedTasks,
		&r.AcceptedTasks,
		&r.RevisionRequestedTasks,
	)
	return r, err
}
