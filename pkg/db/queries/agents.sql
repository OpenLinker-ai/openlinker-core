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
    endpoint_auth_header, price_per_call_cents, tags
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8
)
RETURNING id, creator_id, slug, name, description, endpoint_url,
          endpoint_auth_header, price_per_call_cents, tags,
          lifecycle_status, visibility, certification_status,
          rejection_reason, certified_at,
          total_calls, total_revenue_cents,
          webhook_url, created_at, updated_at;

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
    updated_at = NOW()
WHERE id = $1 AND creator_id = $8 AND lifecycle_status = 'active'
RETURNING id, creator_id, slug, name, description, endpoint_url,
          endpoint_auth_header, price_per_call_cents, tags,
          lifecycle_status, visibility, certification_status,
          rejection_reason, certified_at,
          total_calls, total_revenue_cents,
          webhook_url, created_at, updated_at;

-- name: GetAgentByIDForOwner :one
SELECT id, creator_id, slug, name, description, endpoint_url,
       endpoint_auth_header, price_per_call_cents, tags,
       lifecycle_status, visibility, certification_status,
       rejection_reason, certified_at,
       total_calls, total_revenue_cents,
       webhook_url, created_at, updated_at
FROM agents
WHERE id = $1 AND creator_id = $2;

-- name: GetAgentByID :one
SELECT id, creator_id, slug, name, description, endpoint_url,
       endpoint_auth_header, price_per_call_cents, tags,
       lifecycle_status, visibility, certification_status,
       rejection_reason, certified_at,
       total_calls, total_revenue_cents,
       webhook_url, created_at, updated_at
FROM agents
WHERE id = $1;

-- name: ListAgentsByCreator :many
SELECT id, creator_id, slug, name, description, endpoint_url,
       endpoint_auth_header, price_per_call_cents, tags,
       lifecycle_status, visibility, certification_status,
       rejection_reason, certified_at,
       total_calls, total_revenue_cents,
       webhook_url, created_at, updated_at
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
       a.webhook_url, a.created_at, a.updated_at,
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
-- $1 tags TEXT[]（空数组表示不筛选）；$2 keyword TEXT（空串表示不搜索）；$3 limit；$4 offset。
SELECT a.*, u.display_name AS creator_name
FROM agents a
JOIN users u ON u.id = a.creator_id
WHERE a.visibility = 'public'
  AND a.lifecycle_status = 'active'
  AND (cardinality($1::text[]) = 0 OR a.tags && $1::text[])
  AND ($2::text = '' OR a.name ILIKE '%' || $2 || '%' OR a.description ILIKE '%' || $2 || '%')
ORDER BY a.created_at DESC
LIMIT $3 OFFSET $4;

-- name: CountPublicAgents :one
SELECT COUNT(*)::int AS total
FROM agents a
WHERE a.visibility = 'public'
  AND a.lifecycle_status = 'active'
  AND (cardinality($1::text[]) = 0 OR a.tags && $1::text[])
  AND ($2::text = '' OR a.name ILIKE '%' || $2 || '%' OR a.description ILIKE '%' || $2 || '%');

-- name: GetAgentBySlug :one
-- 详情页：unlisted 也能按链接访问；private 拒绝；disabled 拒绝。
SELECT a.*, u.display_name AS creator_name
FROM agents a
JOIN users u ON u.id = a.creator_id
WHERE a.slug = $1
  AND a.visibility IN ('public', 'unlisted')
  AND a.lifecycle_status = 'active';

-- ## 共用 - 占位
-- name: AgentsCount :one
SELECT COUNT(*)::int AS total
FROM agents
WHERE visibility = 'public' AND lifecycle_status = 'active';
