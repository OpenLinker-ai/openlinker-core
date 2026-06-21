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

-- ## 模块 3（市场查询 + 详情）

-- name: ListPublicAgents :many
-- 市场列表：visibility=public AND lifecycle_status=active。
-- $1 tags TEXT[]（空数组表示不筛选）；$2 keyword TEXT（空串表示不搜索）；$3 limit；$4 offset；$5 callable_only。
SELECT a.*, u.display_name AS creator_name
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
LIMIT $3 OFFSET $4;

-- name: CountPublicAgents :one
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
