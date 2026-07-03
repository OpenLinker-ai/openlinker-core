-- name: CreateAgentToken :one
INSERT INTO agent_tokens (
    agent_id, creator_user_id, name, prefix, token_hash, scopes, status, expires_at, redeemed_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9
)
RETURNING id, agent_id, creator_user_id, name, prefix, token_hash, scopes, status,
          expires_at, redeemed_at, last_used_at, revoked_at, created_at;

-- name: ListAgentTokensByCreator :many
SELECT id, agent_id, creator_user_id, name, prefix, token_hash, scopes, status,
       expires_at, redeemed_at, last_used_at, revoked_at, created_at
FROM agent_tokens
WHERE creator_user_id = $1
ORDER BY created_at DESC;

-- name: ListAgentTokensByCreatorAndAgent :many
SELECT id, agent_id, creator_user_id, name, prefix, token_hash, scopes, status,
       expires_at, redeemed_at, last_used_at, revoked_at, created_at
FROM agent_tokens
WHERE creator_user_id = $1 AND agent_id = $2
ORDER BY created_at DESC;

-- name: ListActiveAgentTokensByPrefix :many
SELECT id, agent_id, creator_user_id, name, prefix, token_hash, scopes, status,
       expires_at, redeemed_at, last_used_at, revoked_at, created_at
FROM agent_tokens
WHERE prefix = $1 AND revoked_at IS NULL;

-- name: RedeemPendingAgentToken :one
UPDATE agent_tokens
SET agent_id = $2,
    scopes = $3,
    status = 'active_runtime',
    redeemed_at = NOW(),
    last_used_at = NOW(),
    expires_at = NULL
WHERE id = $1
  AND creator_user_id = $4
  AND agent_id IS NULL
  AND status = 'pending_registration'
  AND revoked_at IS NULL
  AND (expires_at IS NULL OR expires_at > NOW())
RETURNING id, agent_id, creator_user_id, name, prefix, token_hash, scopes, status,
          expires_at, redeemed_at, last_used_at, revoked_at, created_at;

-- name: RevokeAgentTokenForCreator :execrows
UPDATE agent_tokens
SET revoked_at = NOW(),
    status = 'revoked'
WHERE id = $1
  AND creator_user_id = $2
  AND revoked_at IS NULL;

-- name: CountActiveAgentTokensByAgent :one
SELECT COUNT(*)::int AS total
FROM agent_tokens
WHERE agent_id = $1
  AND status = 'active_runtime'
  AND revoked_at IS NULL;
