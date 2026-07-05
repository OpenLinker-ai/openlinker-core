-- tasks.sql
--
-- 子轮 2.4 任务驱动 A 形态。task_queries 表保存"用户自然语言 → 解析 skill →
-- 推荐 Agent → 用户最终选择"的全过程，便于离线分析推荐质量。

-- name: CreateTaskQuery :one
-- 写入一条任务查询：原始 query + Skill/MCP 引用 + 推荐 agent_id 顺序。
INSERT INTO task_queries (user_id, query, parsed_skills, mcp_tools, recommended_agent_ids)
VALUES ($1, $2, $3, $4, $5)
	RETURNING id, user_id, query, parsed_skills, mcp_tools, recommended_agent_ids,
	          chosen_agent_id, chosen_at,
	          claimed_agent_id, claimed_by_user_id, claimed_at, claim_run_id,
	          completed_at, completion_summary, completion_run_id,
	          delivery_status, delivery_visibility, delivery_artifact,
	          accepted_at, revision_requested_at, revision_note,
	          visibility, public_summary, published_at,
	          created_at;

-- name: GetTaskQuery :one
-- 按 id 查单条；调用方需自行校验 user_id 归属。
	SELECT id, user_id, query, parsed_skills, mcp_tools, recommended_agent_ids,
	       chosen_agent_id, chosen_at,
	       claimed_agent_id, claimed_by_user_id, claimed_at, claim_run_id,
	       completed_at, completion_summary, completion_run_id,
	       delivery_status, delivery_visibility, delivery_artifact,
	       accepted_at, revision_requested_at, revision_note,
	       visibility, public_summary, published_at,
	       created_at
	FROM task_queries
	WHERE id = $1;

-- name: MarkTaskQueryChosen :one
-- 用户选定推荐里某个 agent：写入 chosen_agent_id + chosen_at。
-- 限定 user_id 防越权；命中 0 行表示不存在 / 不属于该 user。
UPDATE task_queries
SET chosen_agent_id = $3,
    chosen_at = NOW()
WHERE id = $1 AND user_id = $2
	RETURNING id, user_id, query, parsed_skills, mcp_tools, recommended_agent_ids,
	          chosen_agent_id, chosen_at,
	          claimed_agent_id, claimed_by_user_id, claimed_at, claim_run_id,
	          completed_at, completion_summary, completion_run_id,
	          delivery_status, delivery_visibility, delivery_artifact,
	          accepted_at, revision_requested_at, revision_note,
	          visibility, public_summary, published_at,
	          created_at;

-- name: PublishTaskQuery :one
-- 用户显式把推荐草稿发布到任务广场；公开列表只展示 public_summary，不暴露完整 query。
UPDATE task_queries
SET visibility = 'public',
    public_summary = $3,
    published_at = COALESCE(published_at, NOW())
WHERE id = $1
  AND user_id = $2
  AND visibility = 'private'
  AND completed_at IS NULL
RETURNING id, user_id, query, parsed_skills, mcp_tools, recommended_agent_ids,
          chosen_agent_id, chosen_at,
          claimed_agent_id, claimed_by_user_id, claimed_at, claim_run_id,
          completed_at, completion_summary, completion_run_id,
          delivery_status, delivery_visibility, delivery_artifact,
          accepted_at, revision_requested_at, revision_note,
          visibility, public_summary, published_at,
          created_at;

-- name: ClaimTaskQuery :one
-- 创作者用自己的 Agent 接入公开任务。已被用户选择 / 已被接入 / 已完成的任务不可重复接入。
UPDATE task_queries
SET claimed_agent_id = $3,
    claimed_by_user_id = $2,
    claimed_at = NOW()
WHERE id = $1
  AND visibility = 'public'
  AND claimed_agent_id IS NULL
  AND completed_at IS NULL
  AND chosen_agent_id IS NULL
RETURNING id, user_id, query, parsed_skills, mcp_tools, recommended_agent_ids,
          chosen_agent_id, chosen_at,
          claimed_agent_id, claimed_by_user_id, claimed_at, claim_run_id,
	          completed_at, completion_summary, completion_run_id,
	          delivery_status, delivery_visibility, delivery_artifact,
	          accepted_at, revision_requested_at, revision_note,
	          visibility, public_summary, published_at,
	          created_at;

-- name: CompleteTaskQuery :one
-- 接单方或任务发布者把成功 run 写回任务详情，形成"任务 -> run -> 结果"闭环。
UPDATE task_queries
SET claimed_agent_id = COALESCE(claimed_agent_id, $3),
    claimed_by_user_id = COALESCE(claimed_by_user_id, $2),
    claimed_at = COALESCE(claimed_at, NOW()),
    claim_run_id = COALESCE(claim_run_id, $4),
    completed_at = NOW(),
    completion_summary = $5,
    completion_run_id = $4,
    delivery_status = 'submitted',
    delivery_artifact = $6,
    delivery_visibility = $7,
    accepted_at = NULL,
    revision_requested_at = NULL,
    revision_note = NULL
WHERE id = $1
  AND (completed_at IS NULL OR delivery_status = 'revision_requested')
  AND (user_id = $2 OR claimed_by_user_id = $2)
  AND (
      claimed_agent_id = $3
      OR (claimed_agent_id IS NULL AND chosen_agent_id = $3)
  )
RETURNING id, user_id, query, parsed_skills, mcp_tools, recommended_agent_ids,
          chosen_agent_id, chosen_at,
          claimed_agent_id, claimed_by_user_id, claimed_at, claim_run_id,
	          completed_at, completion_summary, completion_run_id,
	          delivery_status, delivery_visibility, delivery_artifact,
	          accepted_at, revision_requested_at, revision_note,
	          visibility, public_summary, published_at,
	          created_at;

-- name: AcceptTaskDelivery :one
-- 任务发布者验收提交的结果。
UPDATE task_queries
SET delivery_status = 'accepted',
    accepted_at = NOW()
WHERE id = $1
  AND user_id = $2
  AND delivery_status = 'submitted'
RETURNING id, user_id, query, parsed_skills, mcp_tools, recommended_agent_ids,
          chosen_agent_id, chosen_at,
          claimed_agent_id, claimed_by_user_id, claimed_at, claim_run_id,
	          completed_at, completion_summary, completion_run_id,
	          delivery_status, delivery_visibility, delivery_artifact,
	          accepted_at, revision_requested_at, revision_note,
	          visibility, public_summary, published_at,
	          created_at;

-- name: RequestTaskRevision :one
-- 任务发布者要求修订，接单方可以再次 Complete 提交。
UPDATE task_queries
SET delivery_status = 'revision_requested',
    revision_requested_at = NOW(),
    revision_note = $3,
    accepted_at = NULL
WHERE id = $1
  AND user_id = $2
  AND delivery_status = 'submitted'
RETURNING id, user_id, query, parsed_skills, mcp_tools, recommended_agent_ids,
          chosen_agent_id, chosen_at,
          claimed_agent_id, claimed_by_user_id, claimed_at, claim_run_id,
	          completed_at, completion_summary, completion_run_id,
	          delivery_status, delivery_visibility, delivery_artifact,
	          accepted_at, revision_requested_at, revision_note,
	          visibility, public_summary, published_at,
	          created_at;

-- name: ListTaskQueriesByUser :many
-- "我的任务历史"：按 created_at 倒序最多 20 条。
	SELECT id, user_id, query, parsed_skills, mcp_tools, recommended_agent_ids,
	       chosen_agent_id, chosen_at,
	       claimed_agent_id, claimed_by_user_id, claimed_at, claim_run_id,
	       completed_at, completion_summary, completion_run_id,
	       delivery_status, delivery_visibility, delivery_artifact,
	       accepted_at, revision_requested_at, revision_note,
	       visibility, public_summary, published_at,
	       created_at
	FROM task_queries
WHERE user_id = $1
ORDER BY created_at DESC
LIMIT $2;

-- name: ListPublicTaskQueries :many
-- 最近公开任务流（任务广场用）。只返回显式发布的 public 任务；不返回用户邮箱/姓名。
SELECT id, user_id, query, parsed_skills, mcp_tools, recommended_agent_ids,
       chosen_agent_id, chosen_at,
       claimed_agent_id, claimed_by_user_id, claimed_at, claim_run_id,
       completed_at, completion_summary, completion_run_id,
       delivery_status, delivery_visibility, delivery_artifact,
       accepted_at, revision_requested_at, revision_note,
       visibility, public_summary, published_at,
       created_at
FROM task_queries
WHERE visibility = 'public'
ORDER BY published_at DESC, created_at DESC
LIMIT $1;

-- name: GetAgentsByIDs :many
-- 任务推荐回填：按一组 agent_id 批量取详情（含 creator 显示名）。
-- 只回当前仍公开运行的 Agent；已下架 / private / unlisted 的历史推荐不再展示。
SELECT a.id, a.creator_id, a.slug, a.name, a.description, a.endpoint_url,
       a.endpoint_auth_header, a.price_per_call_cents, a.tags,
       a.lifecycle_status, a.visibility, a.certification_status,
       a.rejection_reason, a.certified_at, a.total_calls, a.total_revenue_cents,
       a.created_at, a.updated_at, u.display_name AS creator_name
FROM agents a
JOIN users u ON u.id = a.creator_id
WHERE a.id = ANY($1::uuid[])
  AND a.visibility = 'public'
  AND a.lifecycle_status = 'active';

-- name: ListAdminTasks :many
-- 管理台任务列表：可搜索任务内容/发布者/接单 Agent，可按可见性、交付状态、派生任务状态筛选。
SELECT t.id, t.user_id, t.query, t.parsed_skills, t.mcp_tools, t.recommended_agent_ids,
       t.chosen_agent_id, t.chosen_at,
       t.claimed_agent_id, t.claimed_by_user_id, t.claimed_at, t.claim_run_id,
       t.completed_at, t.completion_summary, t.completion_run_id,
       t.delivery_status, t.delivery_visibility, t.delivery_artifact,
       t.accepted_at, t.revision_requested_at, t.revision_note,
       t.visibility, t.public_summary, t.published_at,
       t.created_at,
       u.email AS user_email,
       u.display_name AS user_display_name,
       chosen.slug AS chosen_agent_slug,
       chosen.name AS chosen_agent_name,
       claimed.slug AS claimed_agent_slug,
       claimed.name AS claimed_agent_name,
       claimed_user.email AS claimed_by_email,
       claimed_user.display_name AS claimed_by_display_name
FROM task_queries t
JOIN users u ON u.id = t.user_id
LEFT JOIN agents chosen ON chosen.id = t.chosen_agent_id
LEFT JOIN agents claimed ON claimed.id = t.claimed_agent_id
LEFT JOIN users claimed_user ON claimed_user.id = t.claimed_by_user_id
WHERE (
    $1::text = ''
    OR t.query ILIKE '%' || $1 || '%'
    OR COALESCE(t.public_summary, '') ILIKE '%' || $1 || '%'
    OR u.email ILIKE '%' || $1 || '%'
    OR u.display_name ILIKE '%' || $1 || '%'
    OR chosen.slug ILIKE '%' || $1 || '%'
    OR chosen.name ILIKE '%' || $1 || '%'
    OR claimed.slug ILIKE '%' || $1 || '%'
    OR claimed.name ILIKE '%' || $1 || '%'
  )
  AND ($2::text = '' OR t.visibility = $2)
  AND ($3::text = '' OR t.delivery_status = $3)
  AND (
    $4::text = ''
    OR ($4 = 'accepted' AND t.delivery_status = 'accepted')
    OR ($4 = 'revision_requested' AND t.delivery_status = 'revision_requested')
    OR ($4 = 'completed' AND t.delivery_status NOT IN ('accepted', 'revision_requested') AND t.completed_at IS NOT NULL)
    OR ($4 = 'in_progress' AND t.delivery_status NOT IN ('accepted', 'revision_requested') AND t.completed_at IS NULL AND t.claimed_agent_id IS NOT NULL)
    OR ($4 = 'matched' AND t.delivery_status NOT IN ('accepted', 'revision_requested') AND t.completed_at IS NULL AND t.claimed_agent_id IS NULL AND t.chosen_agent_id IS NOT NULL)
    OR ($4 = 'needs_agent' AND t.delivery_status NOT IN ('accepted', 'revision_requested') AND t.completed_at IS NULL AND t.claimed_agent_id IS NULL AND t.chosen_agent_id IS NULL AND cardinality(t.recommended_agent_ids) = 0)
    OR ($4 = 'open' AND t.delivery_status NOT IN ('accepted', 'revision_requested') AND t.completed_at IS NULL AND t.claimed_agent_id IS NULL AND t.chosen_agent_id IS NULL AND cardinality(t.recommended_agent_ids) > 0)
  )
ORDER BY t.created_at DESC
LIMIT $5 OFFSET $6;

-- name: CountAdminTasks :one
SELECT COUNT(*)::int AS total
FROM task_queries t
JOIN users u ON u.id = t.user_id
LEFT JOIN agents chosen ON chosen.id = t.chosen_agent_id
LEFT JOIN agents claimed ON claimed.id = t.claimed_agent_id
WHERE (
    $1::text = ''
    OR t.query ILIKE '%' || $1 || '%'
    OR COALESCE(t.public_summary, '') ILIKE '%' || $1 || '%'
    OR u.email ILIKE '%' || $1 || '%'
    OR u.display_name ILIKE '%' || $1 || '%'
    OR chosen.slug ILIKE '%' || $1 || '%'
    OR chosen.name ILIKE '%' || $1 || '%'
    OR claimed.slug ILIKE '%' || $1 || '%'
    OR claimed.name ILIKE '%' || $1 || '%'
  )
  AND ($2::text = '' OR t.visibility = $2)
  AND ($3::text = '' OR t.delivery_status = $3)
  AND (
    $4::text = ''
    OR ($4 = 'accepted' AND t.delivery_status = 'accepted')
    OR ($4 = 'revision_requested' AND t.delivery_status = 'revision_requested')
    OR ($4 = 'completed' AND t.delivery_status NOT IN ('accepted', 'revision_requested') AND t.completed_at IS NOT NULL)
    OR ($4 = 'in_progress' AND t.delivery_status NOT IN ('accepted', 'revision_requested') AND t.completed_at IS NULL AND t.claimed_agent_id IS NOT NULL)
    OR ($4 = 'matched' AND t.delivery_status NOT IN ('accepted', 'revision_requested') AND t.completed_at IS NULL AND t.claimed_agent_id IS NULL AND t.chosen_agent_id IS NOT NULL)
    OR ($4 = 'needs_agent' AND t.delivery_status NOT IN ('accepted', 'revision_requested') AND t.completed_at IS NULL AND t.claimed_agent_id IS NULL AND t.chosen_agent_id IS NULL AND cardinality(t.recommended_agent_ids) = 0)
    OR ($4 = 'open' AND t.delivery_status NOT IN ('accepted', 'revision_requested') AND t.completed_at IS NULL AND t.claimed_agent_id IS NULL AND t.chosen_agent_id IS NULL AND cardinality(t.recommended_agent_ids) > 0)
  );
