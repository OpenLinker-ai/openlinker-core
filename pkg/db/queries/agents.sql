-- agents.sql
--
-- 模块 2 / 3 共用的 agents 表查询。
-- 文件分工约定：
--   模块 2（Agent 注册，写）由 subagent-2a 维护，写入这些 query：
--     CreateAgent / UpdateAgent / DisableAgent / ListAgentsByCreator
--     CountAgentsByCreator / GetAgentByIDForOwner
--     ApproveAgent / RejectAgent / IncrementAgentStats
--     CheckSlugAvailable
--   模块 3（市场查询，读）由 subagent-3a 维护，写入这些 query：
--     ListApprovedAgents / GetAgentBySlug / CountApprovedAgents
--     ListApprovedAgentsByTags
--
-- 为避免 git merge 冲突，subagent-2a 把 query 集中加在 -- ## 模块 2 标记之后；
-- subagent-3a 加在 -- ## 模块 3 标记之后。

-- ## 模块 2（Agent 注册 + 公开状态 + 创作者列表）
-- subagent-2a 在此区块下追加 query

-- name: CreateAgent :one
-- 创作者新建 Agent 后立即公开；推荐 / 认证另走 benchmark 或运营队列。
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
          webhook_url, created_at, updated_at;

-- name: UpdateAgentDraft :one
-- 创作者编辑：公开 Agent 可直接更新；pending/rejected 兼容旧数据，编辑后回到公开状态。
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
          webhook_url, created_at, updated_at;

-- name: GetAgentByIDForOwner :one
-- 创作者自己看自己的 agent（含人工处理队列 / 已下架），用于编辑前查询
SELECT id, creator_id, slug, name, description, endpoint_url,
       endpoint_auth_header, price_per_call_cents, tags,
       status, rejection_reason, approved_at,
       total_calls, total_revenue_cents,
       webhook_url, created_at, updated_at
FROM agents
WHERE id = $1 AND creator_id = $2;

-- name: GetAgentByID :one
-- 通用查询（不限制 creator）
SELECT id, creator_id, slug, name, description, endpoint_url,
       endpoint_auth_header, price_per_call_cents, tags,
       status, rejection_reason, approved_at,
       total_calls, total_revenue_cents,
       webhook_url, created_at, updated_at
FROM agents
WHERE id = $1;

-- name: ListAgentsByCreator :many
-- 创作者中心列表（含所有状态）
SELECT id, creator_id, slug, name, description, endpoint_url,
       endpoint_auth_header, price_per_call_cents, tags,
       status, rejection_reason, approved_at,
       total_calls, total_revenue_cents,
       webhook_url, created_at, updated_at
FROM agents
WHERE creator_id = $1
ORDER BY created_at DESC;

-- name: DisableAgent :exec
-- 创作者主动下架；限定 creator_id 防越权
UPDATE agents
SET status = 'disabled',
    updated_at = NOW()
WHERE id = $1 AND creator_id = $2;

-- name: ApproveAgent :exec
-- admin/运营手动放行：approved_at = NOW()，rejection_reason 清空；只允许 pending/rejected → approved
UPDATE agents
SET status = 'approved',
    approved_at = NOW(),
    rejection_reason = NULL,
    updated_at = NOW()
WHERE id = $1 AND status IN ('pending', 'rejected');

-- name: RejectAgent :exec
-- admin/运营手动拒绝：写入 rejection_reason；只允许 pending → rejected
UPDATE agents
SET status = 'rejected',
    rejection_reason = $2,
    updated_at = NOW()
WHERE id = $1 AND status = 'pending';

-- name: CheckSlugAvailable :one
-- 返回 slug 是否可用（true=可用，即未被占用）
SELECT NOT EXISTS(SELECT 1 FROM agents WHERE slug = $1) AS available;

-- name: ListPendingAgents :many
-- admin/运营人工处理队列（默认发布不进入该队列）；带 creator email/name
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
ORDER BY a.created_at ASC;

-- name: IncrementAgentStats :exec
-- 调用成功后累加 agent.total_calls + total_revenue_cents（模块 4 计费用）
UPDATE agents
SET total_calls = total_calls + 1,
    total_revenue_cents = total_revenue_cents + $2,
    updated_at = NOW()
WHERE id = $1;

-- ## 模块 3（市场查询 + 详情）
-- subagent-3a 在此区块下追加 query

-- name: ListApprovedAgents :many
-- 市场列表：按 tag 筛选 + 关键词搜索 + 分页
-- $1: tags TEXT[]（空数组表示不筛选）
-- $2: keyword TEXT（空串表示不搜索）
-- $3: limit  $4: offset
SELECT a.*, u.display_name AS creator_name
FROM agents a
JOIN users u ON u.id = a.creator_id
WHERE a.status = 'approved'
  AND (cardinality($1::text[]) = 0 OR a.tags && $1::text[])
  AND ($2::text = '' OR a.name ILIKE '%' || $2 || '%' OR a.description ILIKE '%' || $2 || '%')
ORDER BY a.created_at DESC
LIMIT $3 OFFSET $4;

-- name: CountApprovedAgents :one
-- 同样的过滤条件下计数
SELECT COUNT(*)::int AS total
FROM agents a
WHERE a.status = 'approved'
  AND (cardinality($1::text[]) = 0 OR a.tags && $1::text[])
  AND ($2::text = '' OR a.name ILIKE '%' || $2 || '%' OR a.description ILIKE '%' || $2 || '%');

-- name: GetAgentBySlug :one
-- 详情页：按 slug 查（仅 approved）
SELECT a.*, u.display_name AS creator_name
FROM agents a
JOIN users u ON u.id = a.creator_id
WHERE a.slug = $1 AND a.status = 'approved';

-- ## 共用 - 占位（避免空文件）
-- name: AgentsCount :one
SELECT COUNT(*)::int AS total FROM agents WHERE status = 'approved';
