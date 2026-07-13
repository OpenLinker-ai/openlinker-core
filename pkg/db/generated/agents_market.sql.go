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
WITH active_runtime_agents AS (
    SELECT DISTINCT s.agent_id
    FROM runtime_sessions s
    JOIN runtime_nodes n
      ON n.node_id = s.node_id
    JOIN agent_tokens t
      ON t.id = s.credential_id
     AND t.agent_id = s.agent_id
    JOIN runtime_schema_contracts contract
      ON contract.runtime_contract_id = s.runtime_contract_id
     AND contract.runtime_contract_digest = s.runtime_contract_digest
     AND contract.is_current
    WHERE s.status IN ('active', 'draining')
      AND s.attached_core_instance_id IS NOT NULL
      AND s.disconnected_at IS NULL
      AND s.heartbeat_at >= clock_timestamp() - INTERVAL '45 seconds'
      AND s.protocol_version = 2
      AND s.runtime_contract_id = 'openlinker.runtime.v2'
      AND s.runtime_contract_digest = '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9'
      AND s.features @> ARRAY[
          'lease_fence',
          'assignment_confirm',
          'renew',
          'resume',
          'event_ack',
          'result_ack',
          'cancel',
          'persistent_spool'
      ]::text[]
      AND n.status IN ('active', 'draining')
      AND n.revoked_at IS NULL
      AND n.protocol_version = s.protocol_version
      AND n.runtime_contract_id = s.runtime_contract_id
      AND n.runtime_contract_digest = s.runtime_contract_digest
      AND n.device_certificate_serial = s.device_certificate_serial
      AND n.node_version = s.node_version
      AND n.features @> s.features
      AND s.features @> n.features
      AND n.last_seen_at IS NOT NULL
      AND n.last_seen_at >= clock_timestamp() - INTERVAL '45 seconds'
      AND t.status = 'active_runtime'
      AND t.revoked_at IS NULL
      AND t.scopes @> ARRAY['agent:pull']::text[]
      AND (t.expires_at IS NULL OR t.expires_at > clock_timestamp())
      AND EXISTS (
          SELECT 1
          FROM runtime_session_attachments attachment
          WHERE attachment.runtime_session_id = s.runtime_session_id
            AND attachment.core_instance_id = s.attached_core_instance_id
            AND attachment.detached_at IS NULL
      )
)
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
       skill_stats.latest_batch_id AS latest_benchmark_id,
       COALESCE(declared_skills.skill_ids, ARRAY[]::text[]) AS skill_ids,
       COALESCE(declared_skills.skill_categories, ARRAY[]::text[]) AS skill_categories,
       COALESCE(declared_skills.skill_names, ARRAY[]::text[]) AS skill_names,
       COALESCE(declared_skills.skill_descriptions, ARRAY[]::text[]) AS skill_descriptions
FROM agents a
JOIN users u ON u.id = a.creator_id
LEFT JOIN agent_availability_snapshots av ON av.agent_id = a.id
LEFT JOIN active_runtime_agents runtime_truth ON runtime_truth.agent_id = a.id
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
LEFT JOIN LATERAL (
    SELECT
        ARRAY_AGG(s.id ORDER BY s.category, s.sort_order, s.id)::text[] AS skill_ids,
        ARRAY_AGG(s.category ORDER BY s.category, s.sort_order, s.id)::text[] AS skill_categories,
        ARRAY_AGG(s.name ORDER BY s.category, s.sort_order, s.id)::text[] AS skill_names,
        ARRAY_AGG(s.description ORDER BY s.category, s.sort_order, s.id)::text[] AS skill_descriptions
    FROM agent_skills ag
    JOIN skills s ON s.id = ag.skill_id
    WHERE ag.agent_id = a.id
) declared_skills ON TRUE
WHERE a.visibility = 'public'
  AND a.lifecycle_status = 'active'
  AND NOT EXISTS (
    SELECT 1
    FROM unnest(a.tags) AS tag
    WHERE lower(tag) IN ('internal', 'test', 'testing', 'validation')
       OR tag IN ('内部', '测试', '验收')
  )
  AND (cardinality($1::text[]) = 0 OR a.tags && $1::text[])
  AND (
    $2::text = ''
    OR a.name ILIKE '%' || $2 || '%'
    OR a.description ILIKE '%' || $2 || '%'
    OR EXISTS (
        SELECT 1
        FROM agent_skills ag_search
        JOIN skills s_search ON s_search.id = ag_search.skill_id
        WHERE ag_search.agent_id = a.id
          AND (
            s_search.id ILIKE '%' || $2 || '%'
            OR s_search.name ILIKE '%' || $2 || '%'
            OR s_search.description ILIKE '%' || $2 || '%'
          )
    )
  )
  AND (
    cardinality($6::text[]) = 0
    OR EXISTS (
        SELECT 1
        FROM agent_skills ag_filter
        WHERE ag_filter.agent_id = a.id
          AND ag_filter.skill_id = ANY($6::text[])
    )
  )
  AND (
    NOT $5::bool
    OR (
      a.connection_mode = 'runtime'
      AND runtime_truth.agent_id IS NOT NULL
    )
    OR (
      a.connection_mode <> 'runtime'
      AND (
        COALESCE(av.availability_status, 'unknown') = 'healthy'
        OR (
          av.last_successful_run_at IS NOT NULL
          AND COALESCE(av.availability_status, 'unknown') <> 'unreachable'
        )
      )
    )
  )
ORDER BY CASE
    WHEN (
      (
        a.connection_mode = 'runtime'
        AND runtime_truth.agent_id IS NOT NULL
      )
      OR (
        a.connection_mode <> 'runtime'
        AND (
          COALESCE(av.availability_status, 'unknown') = 'healthy'
          OR (
            av.last_successful_run_at IS NOT NULL
            AND COALESCE(av.availability_status, 'unknown') <> 'unreachable'
          )
        )
      )
    ) THEN 0
    ELSE 1
END ASC,
CASE
    WHEN a.connection_mode = 'runtime' THEN
      CASE WHEN runtime_truth.agent_id IS NOT NULL THEN 0 ELSE 3 END
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
	SkillIDs     []string `db:"skill_ids" json:"skill_ids"`
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
	SkillIDs                        []string   `db:"skill_ids" json:"skill_ids"`
	SkillCategories                 []string   `db:"skill_categories" json:"skill_categories"`
	SkillNames                      []string   `db:"skill_names" json:"skill_names"`
	SkillDescriptions               []string   `db:"skill_descriptions" json:"skill_descriptions"`
}

// ListPublicAgents 市场列表（visibility=public + lifecycle=active；tag/keyword 过滤；分页）。
func (q *Queries) ListPublicAgents(ctx context.Context, arg ListPublicAgentsParams) ([]ListPublicAgentsRow, error) {
	rows, err := q.db.Query(ctx, listPublicAgents,
		arg.Tags,
		arg.Keyword,
		arg.Limit,
		arg.Offset,
		arg.CallableOnly,
		arg.SkillIDs,
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
			&r.SkillIDs,
			&r.SkillCategories,
			&r.SkillNames,
			&r.SkillDescriptions,
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
WITH active_runtime_agents AS (
    SELECT DISTINCT s.agent_id
    FROM runtime_sessions s
    JOIN runtime_nodes n
      ON n.node_id = s.node_id
    JOIN agent_tokens t
      ON t.id = s.credential_id
     AND t.agent_id = s.agent_id
    JOIN runtime_schema_contracts contract
      ON contract.runtime_contract_id = s.runtime_contract_id
     AND contract.runtime_contract_digest = s.runtime_contract_digest
     AND contract.is_current
    WHERE s.status IN ('active', 'draining')
      AND s.attached_core_instance_id IS NOT NULL
      AND s.disconnected_at IS NULL
      AND s.heartbeat_at >= clock_timestamp() - INTERVAL '45 seconds'
      AND s.protocol_version = 2
      AND s.runtime_contract_id = 'openlinker.runtime.v2'
      AND s.runtime_contract_digest = '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9'
      AND s.features @> ARRAY[
          'lease_fence',
          'assignment_confirm',
          'renew',
          'resume',
          'event_ack',
          'result_ack',
          'cancel',
          'persistent_spool'
      ]::text[]
      AND n.status IN ('active', 'draining')
      AND n.revoked_at IS NULL
      AND n.protocol_version = s.protocol_version
      AND n.runtime_contract_id = s.runtime_contract_id
      AND n.runtime_contract_digest = s.runtime_contract_digest
      AND n.device_certificate_serial = s.device_certificate_serial
      AND n.node_version = s.node_version
      AND n.features @> s.features
      AND s.features @> n.features
      AND n.last_seen_at IS NOT NULL
      AND n.last_seen_at >= clock_timestamp() - INTERVAL '45 seconds'
      AND t.status = 'active_runtime'
      AND t.revoked_at IS NULL
      AND t.scopes @> ARRAY['agent:pull']::text[]
      AND (t.expires_at IS NULL OR t.expires_at > clock_timestamp())
      AND EXISTS (
          SELECT 1
          FROM runtime_session_attachments attachment
          WHERE attachment.runtime_session_id = s.runtime_session_id
            AND attachment.core_instance_id = s.attached_core_instance_id
            AND attachment.detached_at IS NULL
      )
)
SELECT COUNT(*)::int AS total
FROM agents a
LEFT JOIN agent_availability_snapshots av ON av.agent_id = a.id
LEFT JOIN active_runtime_agents runtime_truth ON runtime_truth.agent_id = a.id
WHERE a.visibility = 'public'
  AND a.lifecycle_status = 'active'
  AND NOT EXISTS (
    SELECT 1
    FROM unnest(a.tags) AS tag
    WHERE lower(tag) IN ('internal', 'test', 'testing', 'validation')
       OR tag IN ('内部', '测试', '验收')
  )
  AND (cardinality($1::text[]) = 0 OR a.tags && $1::text[])
  AND (
    $2::text = ''
    OR a.name ILIKE '%' || $2 || '%'
    OR a.description ILIKE '%' || $2 || '%'
    OR EXISTS (
        SELECT 1
        FROM agent_skills ag_search
        JOIN skills s_search ON s_search.id = ag_search.skill_id
        WHERE ag_search.agent_id = a.id
          AND (
            s_search.id ILIKE '%' || $2 || '%'
            OR s_search.name ILIKE '%' || $2 || '%'
            OR s_search.description ILIKE '%' || $2 || '%'
          )
    )
  )
  AND (
    cardinality($4::text[]) = 0
    OR EXISTS (
        SELECT 1
        FROM agent_skills ag_filter
        WHERE ag_filter.agent_id = a.id
          AND ag_filter.skill_id = ANY($4::text[])
    )
  )
  AND (
    NOT $3::bool
    OR (
      a.connection_mode = 'runtime'
      AND runtime_truth.agent_id IS NOT NULL
    )
    OR (
      a.connection_mode <> 'runtime'
      AND (
        COALESCE(av.availability_status, 'unknown') = 'healthy'
        OR (
          av.last_successful_run_at IS NOT NULL
          AND COALESCE(av.availability_status, 'unknown') <> 'unreachable'
        )
      )
    )
  )`

type CountPublicAgentsParams struct {
	Tags         []string `db:"tags" json:"tags"`
	Keyword      string   `db:"keyword" json:"keyword"`
	CallableOnly bool     `db:"callable_only" json:"callable_only"`
	SkillIDs     []string `db:"skill_ids" json:"skill_ids"`
}

func (q *Queries) CountPublicAgents(ctx context.Context, arg CountPublicAgentsParams) (int32, error) {
	row := q.db.QueryRow(ctx, countPublicAgents, arg.Tags, arg.Keyword, arg.CallableOnly, arg.SkillIDs)
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
