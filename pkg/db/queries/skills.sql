-- name: ListSkills :many
-- 列出全部内置 skill（按 category + sort_order 升序），用于 /publish 表单与发现页。
SELECT id, category, name, description, sort_order, created_at
FROM skills
ORDER BY category, sort_order;

-- name: GetSkill :one
-- 按 id 取单条 skill；用于 Agent 声明 skill 时的存在性校验。
SELECT id, category, name, description, sort_order, created_at
FROM skills
WHERE id = $1;

-- name: ListAgentSkills :many
-- 列出某个 Agent 已声明的 skill（join 出完整 skill 行，按 category + sort_order 排序）。
SELECT s.id, s.category, s.name, s.description, s.sort_order, s.created_at
FROM agent_skills ag
JOIN skills s ON s.id = ag.skill_id
WHERE ag.agent_id = $1
ORDER BY s.category, s.sort_order;

-- name: ListAgentsBySkills :many
-- 任务驱动推荐：传入 skill_id 数组，返回每个 agent_id 命中了多少个输入 skill。
-- 仅返回 status='approved' 的已公开 Agent；按命中数 desc + 累计调用 desc 排序。
SELECT a.id AS agent_id,
       COUNT(*)::int AS match_count,
       a.total_calls
FROM agent_skills ag
JOIN agents a ON a.id = ag.agent_id
WHERE ag.skill_id = ANY($1::text[])
  AND a.status = 'approved'
GROUP BY a.id, a.total_calls
ORDER BY match_count DESC, a.total_calls DESC, a.id;
