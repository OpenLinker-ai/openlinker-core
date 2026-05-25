-- tasks.sql
--
-- 子轮 2.4 任务驱动 A 形态。task_queries 表保存"用户自然语言 → 解析 skill →
-- 推荐 Agent → 用户最终选择"的全过程，便于离线分析推荐质量。

-- name: CreateTaskQuery :one
-- 写入一条任务查询：原始 query + LLM/规则解析出的 skill_id 列表 + 推荐 agent_id 顺序。
INSERT INTO task_queries (user_id, query, parsed_skills, recommended_agent_ids)
VALUES ($1, $2, $3, $4)
RETURNING id, user_id, query, parsed_skills, recommended_agent_ids,
          chosen_agent_id, chosen_at, created_at;

-- name: GetTaskQuery :one
-- 按 id 查单条；调用方需自行校验 user_id 归属。
SELECT id, user_id, query, parsed_skills, recommended_agent_ids,
       chosen_agent_id, chosen_at, created_at
FROM task_queries
WHERE id = $1;

-- name: MarkTaskQueryChosen :one
-- 用户选定推荐里某个 agent：写入 chosen_agent_id + chosen_at。
-- 限定 user_id 防越权；命中 0 行表示不存在 / 不属于该 user。
UPDATE task_queries
SET chosen_agent_id = $3,
    chosen_at = NOW()
WHERE id = $1 AND user_id = $2
RETURNING id, user_id, query, parsed_skills, recommended_agent_ids,
          chosen_agent_id, chosen_at, created_at;

-- name: ListTaskQueriesByUser :many
-- "我的任务历史"：按 created_at 倒序最多 20 条。
SELECT id, user_id, query, parsed_skills, recommended_agent_ids,
       chosen_agent_id, chosen_at, created_at
FROM task_queries
WHERE user_id = $1
ORDER BY created_at DESC
LIMIT $2;

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
