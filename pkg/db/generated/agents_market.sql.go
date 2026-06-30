// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/agents.sql 中模块 3 区块的 query）。
//
// 模块 3（市场查询）独立文件。Phase 2 缺口 2 之后市场过滤口径：
//   visibility = 'public' AND lifecycle_status = 'active'
// （详情页另允许 unlisted）。

package db

import (
	"context"
	"time"

	"github.com/google/uuid"
)

const listPublicAgents = `-- name: ListPublicAgents :many
SELECT a.id, a.creator_id, a.slug, a.name, a.description, a.endpoint_url,
       a.endpoint_auth_header, a.price_per_call_cents, a.tags,
       a.lifecycle_status, a.visibility, a.certification_status,
       a.rejection_reason, a.certified_at, a.total_calls, a.total_revenue_cents,
       a.webhook_url, a.connection_mode, a.mcp_tool_name, a.created_at, a.updated_at,
       u.display_name AS creator_name,
       COALESCE(av.availability_status, 'unknown') AS availability_status,
       av.last_successful_run_at AS availability_last_successful_run_at,
       av.last_failed_run_at AS availability_last_failed_run_at,
       av.last_checked_at AS availability_last_checked_at,
       COALESCE(av.consecutive_failures, 0)::int AS availability_consecutive_failures,
       rt.last_runtime_token_used_at,
       COALESCE(skill_stats.verified_count, 0)::int AS verified_skill_count,
       skill_stats.latest_batch_id AS latest_benchmark_id
FROM agents a
JOIN users u ON u.id = a.creator_id
LEFT JOIN agent_availability_snapshots av ON av.agent_id = a.id
LEFT JOIN LATERAL (
    SELECT MAX(last_used_at) AS last_runtime_token_used_at
    FROM agent_runtime_tokens
    WHERE agent_id = a.id
      AND revoked_at IS NULL
      AND 'agent:pull' = ANY(scopes)
) rt ON TRUE
LEFT JOIN LATERAL (
    SELECT
        COUNT(*) FILTER (WHERE s.status = 'verified')::int AS verified_count,
        (
            SELECT latest.last_batch_id
            FROM agent_skill_scores latest
            WHERE latest.agent_id = a.id
              AND latest.last_batch_id IS NOT NULL
            ORDER BY latest.updated_at DESC
            LIMIT 1
        ) AS latest_batch_id
    FROM agent_skill_scores s
    WHERE s.agent_id = a.id
) skill_stats ON TRUE
WHERE a.visibility = 'public'
  AND a.lifecycle_status = 'active'
  AND NOT EXISTS (
    SELECT 1
    FROM unnest(a.tags) AS tag
    WHERE lower(tag) IN ('internal', 'test', 'validation')
       OR tag IN ('内部', '测试', '验收')
  )
  AND (cardinality($1::text[]) = 0 OR a.tags && $1::text[])
  AND ($2::text = '' OR a.name ILIKE '%' || $2 || '%' OR a.description ILIKE '%' || $2 || '%')
  AND (
    NOT $5::bool
    OR (
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
    )
  )
ORDER BY CASE
    WHEN (
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
    ) THEN 0
    ELSE 1
END ASC,
CASE
    WHEN a.connection_mode IN ('runtime_pull', 'runtime_ws')
      AND COALESCE(rt.last_runtime_token_used_at < NOW() - INTERVAL '5 minutes', TRUE)
      THEN 3
    ELSE CASE COALESCE(av.availability_status, 'unknown')
    WHEN 'healthy' THEN 0
    WHEN 'unknown' THEN 1
    WHEN 'degraded' THEN 2
    ELSE 3
    END
END ASC,
    a.created_at DESC
LIMIT $3 OFFSET $4`

type ListPublicAgentsParams struct {
	Tags         []string `db:"tags" json:"tags"`
	Keyword      string   `db:"keyword" json:"keyword"`
	Limit        int32    `db:"limit" json:"limit"`
	Offset       int32    `db:"offset" json:"offset"`
	CallableOnly bool     `db:"callable_only" json:"callable_only"`
}

type ListPublicAgentsRow struct {
	Agent
	CreatorName                     string     `db:"creator_name" json:"creator_name"`
	AvailabilityStatus              string     `db:"availability_status" json:"availability_status"`
	AvailabilityLastSuccessfulRunAt *time.Time `db:"availability_last_successful_run_at" json:"availability_last_successful_run_at"`
	AvailabilityLastFailedRunAt     *time.Time `db:"availability_last_failed_run_at" json:"availability_last_failed_run_at"`
	AvailabilityLastCheckedAt       *time.Time `db:"availability_last_checked_at" json:"availability_last_checked_at"`
	AvailabilityConsecutiveFailures int32      `db:"availability_consecutive_failures" json:"availability_consecutive_failures"`
	LastRuntimeTokenUsedAt          *time.Time `db:"last_runtime_token_used_at" json:"last_runtime_token_used_at"`
	VerifiedSkillCount              int32      `db:"verified_skill_count" json:"verified_skill_count"`
	LatestBenchmarkID               *uuid.UUID `db:"latest_benchmark_id" json:"latest_benchmark_id"`
}

// ListPublicAgents 市场列表（visibility=public + lifecycle=active；tag/keyword 过滤；分页）。
func (q *Queries) ListPublicAgents(ctx context.Context, arg ListPublicAgentsParams) ([]ListPublicAgentsRow, error) {
	rows, err := q.db.Query(ctx, listPublicAgents,
		arg.Tags,
		arg.Keyword,
		arg.Limit,
		arg.Offset,
		arg.CallableOnly,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []ListPublicAgentsRow
	for rows.Next() {
		var r ListPublicAgentsRow
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
			&r.CreatorName,
			&r.AvailabilityStatus,
			&r.AvailabilityLastSuccessfulRunAt,
			&r.AvailabilityLastFailedRunAt,
			&r.AvailabilityLastCheckedAt,
			&r.AvailabilityConsecutiveFailures,
			&r.LastRuntimeTokenUsedAt,
			&r.VerifiedSkillCount,
			&r.LatestBenchmarkID,
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

const countPublicAgents = `-- name: CountPublicAgents :one
SELECT COUNT(*)::int AS total
FROM agents a
LEFT JOIN agent_availability_snapshots av ON av.agent_id = a.id
LEFT JOIN LATERAL (
    SELECT MAX(last_used_at) AS last_runtime_token_used_at
    FROM agent_runtime_tokens
    WHERE agent_id = a.id
      AND revoked_at IS NULL
      AND 'agent:pull' = ANY(scopes)
) rt ON TRUE
WHERE a.visibility = 'public'
  AND a.lifecycle_status = 'active'
  AND NOT EXISTS (
    SELECT 1
    FROM unnest(a.tags) AS tag
    WHERE lower(tag) IN ('internal', 'test', 'validation')
       OR tag IN ('内部', '测试', '验收')
  )
  AND (cardinality($1::text[]) = 0 OR a.tags && $1::text[])
  AND ($2::text = '' OR a.name ILIKE '%' || $2 || '%' OR a.description ILIKE '%' || $2 || '%')
  AND (
    NOT $3::bool
    OR (
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
    )
  )`

type CountPublicAgentsParams struct {
	Tags         []string `db:"tags" json:"tags"`
	Keyword      string   `db:"keyword" json:"keyword"`
	CallableOnly bool     `db:"callable_only" json:"callable_only"`
}

func (q *Queries) CountPublicAgents(ctx context.Context, arg CountPublicAgentsParams) (int32, error) {
	row := q.db.QueryRow(ctx, countPublicAgents, arg.Tags, arg.Keyword, arg.CallableOnly)
	var total int32
	err := row.Scan(&total)
	return total, err
}

const getAgentBySlug = `-- name: GetAgentBySlug :one
SELECT a.id, a.creator_id, a.slug, a.name, a.description, a.endpoint_url,
       a.endpoint_auth_header, a.price_per_call_cents, a.tags,
       a.lifecycle_status, a.visibility, a.certification_status,
       a.rejection_reason, a.certified_at, a.total_calls, a.total_revenue_cents,
       a.webhook_url, a.connection_mode, a.mcp_tool_name, a.created_at, a.updated_at,
       u.display_name AS creator_name
FROM agents a
JOIN users u ON u.id = a.creator_id
WHERE a.slug = $1
  AND a.visibility IN ('public', 'unlisted')
  AND a.lifecycle_status = 'active'`

type GetAgentBySlugRow struct {
	Agent
	CreatorName string `db:"creator_name" json:"creator_name"`
}

// GetAgentBySlug 详情页。unlisted 可凭直链访问；private 与 disabled 返回 pgx.ErrNoRows。
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
		&r.CreatorName,
	)
	return r, err
}

const getAgentBySlugForOwner = `-- name: GetAgentBySlugForOwner :one
SELECT a.id, a.creator_id, a.slug, a.name, a.description, a.endpoint_url,
       a.endpoint_auth_header, a.price_per_call_cents, a.tags,
       a.lifecycle_status, a.visibility, a.certification_status,
       a.rejection_reason, a.certified_at, a.total_calls, a.total_revenue_cents,
       a.webhook_url, a.connection_mode, a.mcp_tool_name, a.created_at, a.updated_at,
       u.display_name AS creator_name
FROM agents a
JOIN users u ON u.id = a.creator_id
WHERE a.slug = $1
  AND a.creator_id = $2
  AND a.lifecycle_status = 'active'`

type GetAgentBySlugForOwnerParams struct {
	Slug      string    `db:"slug" json:"slug"`
	CreatorID uuid.UUID `db:"creator_id" json:"creator_id"`
}

type GetAgentBySlugForOwnerRow struct {
	Agent
	CreatorName string `db:"creator_name" json:"creator_name"`
}

// GetAgentBySlugForOwner 创作者自测详情。owner 可按 slug 访问自己的 private/unlisted/public active Agent。
func (q *Queries) GetAgentBySlugForOwner(ctx context.Context, arg GetAgentBySlugForOwnerParams) (GetAgentBySlugForOwnerRow, error) {
	row := q.db.QueryRow(ctx, getAgentBySlugForOwner, arg.Slug, arg.CreatorID)
	var r GetAgentBySlugForOwnerRow
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
		&r.CreatorName,
	)
	return r, err
}
