-- agents.sql
--
-- 模块 2 / 3 共用的 agents 表查询。Phase 2 缺口 2（docs/29 §三）之后：
--   status         (已移除)  → 拆为 lifecycle_status / visibility / certification_status
--   approved_at    (已移除)  → 重命名为 certified_at
--   rejection_reason          → 现在专指 certification rejection 原因
--
-- 文件分工约定：
--   模块 2（Agent 注册，写）：CreateAgent / UpdateAgent / DisableAgent / ListAgentsByCreator
--     CountAgentsByCreator / GetAgentByIDForOwner / CheckSlugAvailable
--   模块 2 认证：RequestCertification / CertifyAgent / RejectCertification
--   模块 3（市场查询，读）：ListPublicAgents / GetAgentBySlug / CountPublicAgents
--   公共：IncrementAgentStats
--
-- 为避免 git merge 冲突，模块 2 把 query 集中加在 -- ## 模块 2 标记之后；
-- 模块 3 加在 -- ## 模块 3 标记之后。

-- ## 模块 2（Agent 注册 + 公开状态 + 创作者列表）

-- name: CreateAgent :one
-- 创作者新建 Agent：默认 lifecycle=active, visibility=public, certification=unreviewed。
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
          webhook_url, connection_mode, mcp_tool_name, created_at, updated_at;

-- name: UpdateAgentDraft :one
-- 创作者编辑：可以同时改 visibility（public / unlisted / private）。
-- 只允许在未下架时编辑。
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
          webhook_url, connection_mode, mcp_tool_name, created_at, updated_at;

-- name: GetAgentByIDForOwner :one
SELECT id, creator_id, slug, name, description, endpoint_url,
       endpoint_auth_header, price_per_call_cents, tags,
       lifecycle_status, visibility, certification_status,
       rejection_reason, certified_at,
       total_calls, total_revenue_cents,
       webhook_url, connection_mode, mcp_tool_name, created_at, updated_at
FROM agents
WHERE id = $1 AND creator_id = $2;

-- name: GetAgentByID :one
SELECT id, creator_id, slug, name, description, endpoint_url,
       endpoint_auth_header, price_per_call_cents, tags,
       lifecycle_status, visibility, certification_status,
       rejection_reason, certified_at,
       total_calls, total_revenue_cents,
       webhook_url, connection_mode, mcp_tool_name, created_at, updated_at
FROM agents
WHERE id = $1;

-- name: ListAgentsByCreator :many
SELECT id, creator_id, slug, name, description, endpoint_url,
       endpoint_auth_header, price_per_call_cents, tags,
       lifecycle_status, visibility, certification_status,
       rejection_reason, certified_at,
       total_calls, total_revenue_cents,
       webhook_url, connection_mode, mcp_tool_name, created_at, updated_at
FROM agents
WHERE creator_id = $1
ORDER BY created_at DESC;

-- name: ListAgentsByCreatorPage :many
-- 创作者中心 Agent 分页列表：搜索、状态筛选和排序都在数据库侧完成。
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
LIMIT $7 OFFSET $8;

-- name: CountAgentsByCreatorFiltered :one
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
	  );

-- name: CountAgentBucketsByCreator :one
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
WHERE a.creator_id = $1;

-- name: DisableAgent :exec
-- 创作者主动下架：lifecycle_status='disabled'。
UPDATE agents
SET lifecycle_status = 'disabled',
    updated_at = NOW()
WHERE id = $1 AND creator_id = $2;

-- name: RequestCertification :exec
-- 创作者发起认证申请：unreviewed/rejected → pending。
-- 已 certified / 已 pending 不重复触发。
UPDATE agents
SET certification_status = 'pending',
    rejection_reason = NULL,
    updated_at = NOW()
WHERE id = $1 AND creator_id = $2
  AND certification_status IN ('unreviewed', 'rejected');

-- name: SetAgentVisibilityForOwner :exec
UPDATE agents
SET visibility = $3,
    updated_at = NOW()
WHERE id = $1 AND creator_id = $2
  AND lifecycle_status = 'active';

-- name: CertifyAgent :exec
-- 运营授予认证：pending → certified。
UPDATE agents
SET certification_status = 'certified',
    certified_at = NOW(),
    rejection_reason = NULL,
    updated_at = NOW()
WHERE id = $1 AND certification_status = 'pending';

-- name: RejectCertification :exec
-- 运营拒绝认证：pending → rejected（写原因）。
UPDATE agents
SET certification_status = 'rejected',
    rejection_reason = $2,
    updated_at = NOW()
WHERE id = $1 AND certification_status = 'pending';

-- name: CheckSlugAvailable :one
SELECT NOT EXISTS(SELECT 1 FROM agents WHERE slug = $1) AS available;

-- name: ListPendingAgents :many
-- 运营人工处理认证申请队列。
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
ORDER BY a.created_at ASC;

-- name: IncrementAgentStats :exec
UPDATE agents
SET total_calls = total_calls + 1,
    total_revenue_cents = total_revenue_cents + $2,
    updated_at = NOW()
WHERE id = $1;

-- name: ListAdminAgents :many
-- 管理台 Agent 列表：可搜索 Agent / 创作者，可按三维状态筛选。
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
LIMIT $5 OFFSET $6;

-- name: CountAdminAgents :one
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
  AND ($4::text = '' OR a.certification_status = $4);

-- name: UpdateAdminAgentModeration :one
-- 管理台调整 Agent 生命周期、可见性、认证状态。
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
          webhook_url, connection_mode, mcp_tool_name, created_at, updated_at;

-- name: GetAdminSummary :one
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
  (SELECT COUNT(*)::int FROM task_queries WHERE delivery_status = 'revision_requested') AS revision_requested_tasks;

-- ## 模块 3（市场查询 + 详情）

-- name: ListPublicAgents :many
-- 市场列表：visibility=public AND lifecycle_status=active。
-- $1 tags TEXT[]（空数组表示不筛选）；$2 keyword TEXT（空串表示不搜索）；$3 limit；$4 offset；$5 callable_only；$6 skill_ids TEXT[]。
SELECT a.*, u.display_name AS creator_name,
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
LIMIT $3 OFFSET $4;

-- name: CountPublicAgents :one
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
  );

-- name: GetAgentBySlug :one
-- 详情页：unlisted 也能按链接访问；private 拒绝；disabled 拒绝。
SELECT a.*, u.display_name AS creator_name
FROM agents a
JOIN users u ON u.id = a.creator_id
WHERE a.slug = $1
  AND a.visibility IN ('public', 'unlisted')
  AND a.lifecycle_status = 'active';

-- name: GetAgentBySlugForOwner :one
-- 创作者自测详情：owner 可按 slug 访问自己的 private/unlisted/public active Agent。
SELECT a.*, u.display_name AS creator_name
FROM agents a
JOIN users u ON u.id = a.creator_id
WHERE a.slug = $1
  AND a.creator_id = $2
  AND a.lifecycle_status = 'active';

-- ## 共用 - 占位
-- name: AgentsCount :one
SELECT COUNT(*)::int AS total
FROM agents
WHERE visibility = 'public' AND lifecycle_status = 'active';
