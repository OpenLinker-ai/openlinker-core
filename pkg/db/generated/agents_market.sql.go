// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/agents.sql 中模块 3 区块的 query）。
//
// 模块 3 (市场查询，只读) 独立文件，避免与模块 2 (Agent 注册写入) 同时
// 修改 agents.sql.go 时产生 git merge 冲突。本文件只新增市场查询
// 相关 query 方法（List / Count / GetBySlug），不修改已有的 AgentsCount。

package db

import (
	"context"
)

const listApprovedAgents = `-- name: ListApprovedAgents :many
SELECT a.id, a.creator_id, a.slug, a.name, a.description, a.endpoint_url,
       a.endpoint_auth_header, a.price_per_call_cents, a.tags, a.status,
       a.rejection_reason, a.approved_at, a.total_calls, a.total_revenue_cents,
       a.created_at, a.updated_at, u.display_name AS creator_name
FROM agents a
JOIN users u ON u.id = a.creator_id
WHERE a.status = 'approved'
  AND (cardinality($1::text[]) = 0 OR a.tags && $1::text[])
  AND ($2::text = '' OR a.name ILIKE '%' || $2 || '%' OR a.description ILIKE '%' || $2 || '%')
ORDER BY a.created_at DESC
LIMIT $3 OFFSET $4`

// ListApprovedAgentsParams 入参。
//
// Tags 空数组（len==0）表示不按 tag 过滤；非空时使用 Postgres 数组重叠运算
// 符 `&&`，任意 tag 命中即返回。
// Keyword 空串表示不按关键词过滤；非空时对 name/description 做 ILIKE。
type ListApprovedAgentsParams struct {
	Tags    []string `db:"tags" json:"tags"`
	Keyword string   `db:"keyword" json:"keyword"`
	Limit   int32    `db:"limit" json:"limit"`
	Offset  int32    `db:"offset" json:"offset"`
}

// ListApprovedAgentsRow 行类型：Agent 全字段 + 关联 creator 显示名。
type ListApprovedAgentsRow struct {
	Agent
	CreatorName string `db:"creator_name" json:"creator_name"`
}

// ListApprovedAgents 市场列表（带 tag 筛选 + 关键词搜索 + 分页）。
func (q *Queries) ListApprovedAgents(ctx context.Context, arg ListApprovedAgentsParams) ([]ListApprovedAgentsRow, error) {
	// pgx/v5 把 []string 直接序列化为 Postgres text[]
	rows, err := q.db.Query(ctx, listApprovedAgents,
		arg.Tags,
		arg.Keyword,
		arg.Limit,
		arg.Offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []ListApprovedAgentsRow
	for rows.Next() {
		var r ListApprovedAgentsRow
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
			&r.CreatedAt,
			&r.UpdatedAt,
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

const countApprovedAgents = `-- name: CountApprovedAgents :one
SELECT COUNT(*)::int AS total
FROM agents a
WHERE a.status = 'approved'
  AND (cardinality($1::text[]) = 0 OR a.tags && $1::text[])
  AND ($2::text = '' OR a.name ILIKE '%' || $2 || '%' OR a.description ILIKE '%' || $2 || '%')`

// CountApprovedAgentsParams 入参（与 ListApprovedAgents 的过滤条件保持一致）。
type CountApprovedAgentsParams struct {
	Tags    []string `db:"tags" json:"tags"`
	Keyword string   `db:"keyword" json:"keyword"`
}

// CountApprovedAgents 满足过滤条件的已公开 Agent 数量（用于分页 total）。
func (q *Queries) CountApprovedAgents(ctx context.Context, arg CountApprovedAgentsParams) (int32, error) {
	row := q.db.QueryRow(ctx, countApprovedAgents, arg.Tags, arg.Keyword)
	var total int32
	err := row.Scan(&total)
	return total, err
}

const getAgentBySlug = `-- name: GetAgentBySlug :one
SELECT a.id, a.creator_id, a.slug, a.name, a.description, a.endpoint_url,
       a.endpoint_auth_header, a.price_per_call_cents, a.tags, a.status,
       a.rejection_reason, a.approved_at, a.total_calls, a.total_revenue_cents,
       a.created_at, a.updated_at, u.display_name AS creator_name
FROM agents a
JOIN users u ON u.id = a.creator_id
WHERE a.slug = $1 AND a.status = 'approved'`

// GetAgentBySlugRow 详情行类型：Agent 全字段 + creator 显示名。
type GetAgentBySlugRow struct {
	Agent
	CreatorName string `db:"creator_name" json:"creator_name"`
}

// GetAgentBySlug 按 slug 查询已公开 Agent 详情。
// 未找到（不存在 / 未公开 / 被拒绝 / 已禁用）时返回 pgx.ErrNoRows。
func (q *Queries) GetAgentBySlug(ctx context.Context, slug string) (GetAgentBySlugRow, error) {
	row := q.db.QueryRow(ctx, getAgentBySlug, slug)
	var r GetAgentBySlugRow
	err := row.Scan(
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
		&r.CreatedAt,
		&r.UpdatedAt,
		&r.CreatorName,
	)
	return r, err
}
