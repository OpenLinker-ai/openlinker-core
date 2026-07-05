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

-- name: CreateSkillProposal :one
-- 用户提交缺失 Skill；同一用户同一 proposed_skill_id 重复提交时覆盖最新说明。
INSERT INTO skill_proposals (
  owner_user_id,
  agent_id,
  proposed_skill_id,
  category,
  name,
  description,
  source,
  status,
  matched_skill_id
) VALUES (
  $1, $2, $3, $4, $5, $6, $7, $8, $9
)
ON CONFLICT (owner_user_id, proposed_skill_id) DO UPDATE SET
  agent_id = EXCLUDED.agent_id,
  category = EXCLUDED.category,
  name = EXCLUDED.name,
  description = EXCLUDED.description,
  source = EXCLUDED.source,
  status = EXCLUDED.status,
  matched_skill_id = EXCLUDED.matched_skill_id,
  updated_at = NOW()
RETURNING id, owner_user_id, agent_id, proposed_skill_id, category, name, description, source, status, matched_skill_id, created_at, updated_at;

-- name: ListSkillProposalsByOwner :many
-- 创作者侧查看自己提交或导入生成的 Skill Proposal。
SELECT id, owner_user_id, agent_id, proposed_skill_id, category, name, description, source, status, matched_skill_id, created_at, updated_at
FROM skill_proposals
WHERE owner_user_id = $1
ORDER BY updated_at DESC, created_at DESC
LIMIT 100;

-- name: ListAgentsBySkills :many
-- 任务驱动推荐：传入 skill_id 数组，返回每个 agent_id 命中了多少个输入 skill。
-- 仅返回 status='approved' 的已公开 Agent；按命中数 desc + 累计调用 desc 排序。
SELECT a.id AS agent_id,
       COUNT(*)::int AS match_count,
       a.total_calls
FROM agent_skills ag
JOIN agents a ON a.id = ag.agent_id
WHERE ag.skill_id = ANY($1::text[])
  AND a.visibility = 'public' AND a.lifecycle_status = 'active'
GROUP BY a.id, a.total_calls
ORDER BY match_count DESC, a.total_calls DESC, a.id;
