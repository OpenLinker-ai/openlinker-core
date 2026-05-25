-- agent_registration.sql
-- Phase 2 缺口 1：Agent 自注册 Bootstrap Token 查询。
-- docs/29 §2。

-- name: CreateAgentRegistrationToken :one
INSERT INTO agent_registration_tokens (
    creator_user_id, label, prefix, token_hash, max_agents, expires_at
) VALUES (
    $1, $2, $3, $4, $5, $6
)
RETURNING id, creator_user_id, label, prefix, token_hash, max_agents, used_count,
          expires_at, revoked_at, last_used_at, created_at;

-- name: ListAgentRegistrationTokensByCreator :many
SELECT id, creator_user_id, label, prefix, token_hash, max_agents, used_count,
       expires_at, revoked_at, last_used_at, created_at
FROM agent_registration_tokens
WHERE creator_user_id = $1
ORDER BY created_at DESC;

-- name: ListActiveAgentRegistrationTokensByPrefix :many
SELECT id, creator_user_id, label, prefix, token_hash, max_agents, used_count,
       expires_at, revoked_at, last_used_at, created_at
FROM agent_registration_tokens
WHERE prefix = $1 AND revoked_at IS NULL;

-- name: RevokeAgentRegistrationTokenForCreator :execrows
UPDATE agent_registration_tokens
SET revoked_at = NOW()
WHERE id = $1
  AND creator_user_id = $2
  AND revoked_at IS NULL;

-- name: ConsumeAgentRegistrationToken :one
-- 原子消费：仅当未撤销 / 未过期 / 未用尽时 +1，并返回最新行。
-- 任一条件不满足 → 0 行 → service 层翻译成 401。
UPDATE agent_registration_tokens
SET used_count = used_count + 1,
    last_used_at = NOW()
WHERE id = $1
  AND revoked_at IS NULL
  AND expires_at > NOW()
  AND used_count < max_agents
RETURNING id, creator_user_id, label, prefix, token_hash, max_agents, used_count,
          expires_at, revoked_at, last_used_at, created_at;
