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
ORDER BY
  CASE WHEN $4 = 'name' AND $5 = 'asc' THEN name END ASC NULLS LAST,
  CASE WHEN $4 = 'name' AND $5 = 'desc' THEN name END DESC NULLS LAST,
  CASE WHEN $4 = 'status' AND $5 = 'asc' THEN status END ASC NULLS LAST,
  CASE WHEN $4 = 'status' AND $5 = 'desc' THEN status END DESC NULLS LAST,
  CASE WHEN $4 = 'expires_at' AND $5 = 'asc' THEN expires_at END ASC NULLS LAST,
  CASE WHEN $4 = 'expires_at' AND $5 = 'desc' THEN expires_at END DESC NULLS LAST,
  CASE WHEN $4 = 'last_used_at' AND $5 = 'asc' THEN last_used_at END ASC NULLS LAST,
  CASE WHEN $4 = 'last_used_at' AND $5 = 'desc' THEN last_used_at END DESC NULLS LAST,
  CASE WHEN $4 = 'created_at' AND $5 = 'asc' THEN created_at END ASC,
  CASE WHEN $4 = 'created_at' AND $5 = 'desc' THEN created_at END DESC,
  created_at DESC,
  id DESC
LIMIT $2 OFFSET $3;

-- name: CountAgentTokensByCreator :one
SELECT COUNT(*)::int AS total
FROM agent_tokens
WHERE creator_user_id = $1;

-- name: ListAgentTokensByCreatorAndAgent :many
SELECT id, agent_id, creator_user_id, name, prefix, token_hash, scopes, status,
       expires_at, redeemed_at, last_used_at, revoked_at, created_at
FROM agent_tokens
WHERE creator_user_id = $1 AND agent_id = $2
ORDER BY
  CASE WHEN $5 = 'name' AND $6 = 'asc' THEN name END ASC NULLS LAST,
  CASE WHEN $5 = 'name' AND $6 = 'desc' THEN name END DESC NULLS LAST,
  CASE WHEN $5 = 'status' AND $6 = 'asc' THEN status END ASC NULLS LAST,
  CASE WHEN $5 = 'status' AND $6 = 'desc' THEN status END DESC NULLS LAST,
  CASE WHEN $5 = 'expires_at' AND $6 = 'asc' THEN expires_at END ASC NULLS LAST,
  CASE WHEN $5 = 'expires_at' AND $6 = 'desc' THEN expires_at END DESC NULLS LAST,
  CASE WHEN $5 = 'last_used_at' AND $6 = 'asc' THEN last_used_at END ASC NULLS LAST,
  CASE WHEN $5 = 'last_used_at' AND $6 = 'desc' THEN last_used_at END DESC NULLS LAST,
  CASE WHEN $5 = 'created_at' AND $6 = 'asc' THEN created_at END ASC,
  CASE WHEN $5 = 'created_at' AND $6 = 'desc' THEN created_at END DESC,
  created_at DESC,
  id DESC
LIMIT $3 OFFSET $4;

-- name: CountAgentTokensByCreatorAndAgent :one
SELECT COUNT(*)::int AS total
FROM agent_tokens
WHERE creator_user_id = $1 AND agent_id = $2;

-- name: ListAgentTokensByCreatorFiltered :many
SELECT id, agent_id, creator_user_id, name, prefix, token_hash, scopes, status,
       expires_at, redeemed_at, last_used_at, revoked_at, created_at
FROM agent_tokens
WHERE creator_user_id = sqlc.arg(creator_user_id)
  AND (
    sqlc.arg(filter_status)::text = 'all'
    OR (sqlc.arg(filter_status)::text = 'active' AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at > NOW()))
    OR (sqlc.arg(filter_status)::text = 'revoked' AND revoked_at IS NOT NULL)
    OR (sqlc.arg(filter_status)::text = 'expired' AND revoked_at IS NULL AND expires_at IS NOT NULL AND expires_at <= NOW())
  )
  AND (
    sqlc.arg(search)::text = ''
    OR POSITION(LOWER(sqlc.arg(search)::text) IN LOWER(name)) > 0
    OR POSITION(LOWER(sqlc.arg(search)::text) IN LOWER(prefix)) > 0
  )
ORDER BY
  CASE WHEN sqlc.arg(sort_by)::text = 'name' AND sqlc.arg(sort_dir)::text = 'asc' THEN name END ASC NULLS LAST,
  CASE WHEN sqlc.arg(sort_by)::text = 'name' AND sqlc.arg(sort_dir)::text = 'desc' THEN name END DESC NULLS LAST,
  CASE WHEN sqlc.arg(sort_by)::text = 'status' AND sqlc.arg(sort_dir)::text = 'asc' THEN status END ASC NULLS LAST,
  CASE WHEN sqlc.arg(sort_by)::text = 'status' AND sqlc.arg(sort_dir)::text = 'desc' THEN status END DESC NULLS LAST,
  CASE WHEN sqlc.arg(sort_by)::text = 'expires_at' AND sqlc.arg(sort_dir)::text = 'asc' THEN expires_at END ASC NULLS LAST,
  CASE WHEN sqlc.arg(sort_by)::text = 'expires_at' AND sqlc.arg(sort_dir)::text = 'desc' THEN expires_at END DESC NULLS LAST,
  CASE WHEN sqlc.arg(sort_by)::text = 'last_used_at' AND sqlc.arg(sort_dir)::text = 'asc' THEN last_used_at END ASC NULLS LAST,
  CASE WHEN sqlc.arg(sort_by)::text = 'last_used_at' AND sqlc.arg(sort_dir)::text = 'desc' THEN last_used_at END DESC NULLS LAST,
  CASE WHEN sqlc.arg(sort_by)::text = 'created_at' AND sqlc.arg(sort_dir)::text = 'asc' THEN created_at END ASC,
  CASE WHEN sqlc.arg(sort_by)::text = 'created_at' AND sqlc.arg(sort_dir)::text = 'desc' THEN created_at END DESC,
  created_at DESC,
  id DESC
LIMIT sqlc.arg(limit) OFFSET sqlc.arg(offset);

-- name: CountAgentTokensByCreatorFiltered :one
SELECT COUNT(*)::int AS total
FROM agent_tokens
WHERE creator_user_id = sqlc.arg(creator_user_id)
  AND (
    sqlc.arg(filter_status)::text = 'all'
    OR (sqlc.arg(filter_status)::text = 'active' AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at > NOW()))
    OR (sqlc.arg(filter_status)::text = 'revoked' AND revoked_at IS NOT NULL)
    OR (sqlc.arg(filter_status)::text = 'expired' AND revoked_at IS NULL AND expires_at IS NOT NULL AND expires_at <= NOW())
  )
  AND (
    sqlc.arg(search)::text = ''
    OR POSITION(LOWER(sqlc.arg(search)::text) IN LOWER(name)) > 0
    OR POSITION(LOWER(sqlc.arg(search)::text) IN LOWER(prefix)) > 0
  );

-- name: ListAgentTokensByCreatorAndAgentFiltered :many
SELECT id, agent_id, creator_user_id, name, prefix, token_hash, scopes, status,
       expires_at, redeemed_at, last_used_at, revoked_at, created_at
FROM agent_tokens
WHERE creator_user_id = sqlc.arg(creator_user_id)
  AND agent_id = sqlc.arg(agent_id)
  AND (
    sqlc.arg(filter_status)::text = 'all'
    OR (sqlc.arg(filter_status)::text = 'active' AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at > NOW()))
    OR (sqlc.arg(filter_status)::text = 'revoked' AND revoked_at IS NOT NULL)
    OR (sqlc.arg(filter_status)::text = 'expired' AND revoked_at IS NULL AND expires_at IS NOT NULL AND expires_at <= NOW())
  )
  AND (
    sqlc.arg(search)::text = ''
    OR POSITION(LOWER(sqlc.arg(search)::text) IN LOWER(name)) > 0
    OR POSITION(LOWER(sqlc.arg(search)::text) IN LOWER(prefix)) > 0
  )
ORDER BY
  CASE WHEN sqlc.arg(sort_by)::text = 'name' AND sqlc.arg(sort_dir)::text = 'asc' THEN name END ASC NULLS LAST,
  CASE WHEN sqlc.arg(sort_by)::text = 'name' AND sqlc.arg(sort_dir)::text = 'desc' THEN name END DESC NULLS LAST,
  CASE WHEN sqlc.arg(sort_by)::text = 'status' AND sqlc.arg(sort_dir)::text = 'asc' THEN status END ASC NULLS LAST,
  CASE WHEN sqlc.arg(sort_by)::text = 'status' AND sqlc.arg(sort_dir)::text = 'desc' THEN status END DESC NULLS LAST,
  CASE WHEN sqlc.arg(sort_by)::text = 'expires_at' AND sqlc.arg(sort_dir)::text = 'asc' THEN expires_at END ASC NULLS LAST,
  CASE WHEN sqlc.arg(sort_by)::text = 'expires_at' AND sqlc.arg(sort_dir)::text = 'desc' THEN expires_at END DESC NULLS LAST,
  CASE WHEN sqlc.arg(sort_by)::text = 'last_used_at' AND sqlc.arg(sort_dir)::text = 'asc' THEN last_used_at END ASC NULLS LAST,
  CASE WHEN sqlc.arg(sort_by)::text = 'last_used_at' AND sqlc.arg(sort_dir)::text = 'desc' THEN last_used_at END DESC NULLS LAST,
  CASE WHEN sqlc.arg(sort_by)::text = 'created_at' AND sqlc.arg(sort_dir)::text = 'asc' THEN created_at END ASC,
  CASE WHEN sqlc.arg(sort_by)::text = 'created_at' AND sqlc.arg(sort_dir)::text = 'desc' THEN created_at END DESC,
  created_at DESC,
  id DESC
LIMIT sqlc.arg(limit) OFFSET sqlc.arg(offset);

-- name: CountAgentTokensByCreatorAndAgentFiltered :one
SELECT COUNT(*)::int AS total
FROM agent_tokens
WHERE creator_user_id = sqlc.arg(creator_user_id)
  AND agent_id = sqlc.arg(agent_id)
  AND (
    sqlc.arg(filter_status)::text = 'all'
    OR (sqlc.arg(filter_status)::text = 'active' AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at > NOW()))
    OR (sqlc.arg(filter_status)::text = 'revoked' AND revoked_at IS NOT NULL)
    OR (sqlc.arg(filter_status)::text = 'expired' AND revoked_at IS NULL AND expires_at IS NOT NULL AND expires_at <= NOW())
  )
  AND (
    sqlc.arg(search)::text = ''
    OR POSITION(LOWER(sqlc.arg(search)::text) IN LOWER(name)) > 0
    OR POSITION(LOWER(sqlc.arg(search)::text) IN LOWER(prefix)) > 0
  );

-- name: GetAgentTokenByIDForCreator :one
SELECT id, agent_id, creator_user_id, name, prefix, token_hash, scopes, status,
       expires_at, redeemed_at, last_used_at, revoked_at, created_at
FROM agent_tokens
WHERE id = $1 AND creator_user_id = $2;

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
    expires_at = NULL,
    token_hash = $5
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
    status = 'revoked',
    revocation_kind = 'manual'
WHERE id = $1
  AND creator_user_id = $2
  AND revoked_at IS NULL;

-- name: CountActiveAgentTokensByAgent :one
SELECT COUNT(*)::int AS total
FROM agent_tokens
WHERE agent_id = $1
  AND status = 'active_runtime'
  AND revoked_at IS NULL;
