-- name: CreateUserToken :one
INSERT INTO user_tokens (user_id, name, prefix, token_hash, scopes, expires_at)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING id, user_id, name, prefix, token_hash, scopes, expires_at,
          last_used_at, revoked_at, created_at, updated_at;

-- name: CountActiveUserTokensByUser :one
SELECT COUNT(*)::int
FROM user_tokens
WHERE user_id = $1
  AND revoked_at IS NULL
  AND (expires_at IS NULL OR expires_at > NOW());

-- name: ListUserTokensByUser :many
SELECT id, user_id, name, prefix, token_hash, scopes, expires_at,
       last_used_at, revoked_at, created_at, updated_at
FROM user_tokens
WHERE user_id = $1
ORDER BY
  CASE WHEN $4 = 'name' AND $5 = 'asc' THEN name END ASC NULLS LAST,
  CASE WHEN $4 = 'name' AND $5 = 'desc' THEN name END DESC NULLS LAST,
  CASE WHEN $4 = 'expires_at' AND $5 = 'asc' THEN expires_at END ASC NULLS LAST,
  CASE WHEN $4 = 'expires_at' AND $5 = 'desc' THEN expires_at END DESC NULLS LAST,
  CASE WHEN $4 = 'last_used_at' AND $5 = 'asc' THEN last_used_at END ASC NULLS LAST,
  CASE WHEN $4 = 'last_used_at' AND $5 = 'desc' THEN last_used_at END DESC NULLS LAST,
  CASE WHEN $4 = 'created_at' AND $5 = 'asc' THEN created_at END ASC,
  CASE WHEN $4 = 'created_at' AND $5 = 'desc' THEN created_at END DESC,
  created_at DESC,
  id DESC
LIMIT $2 OFFSET $3;

-- name: CountUserTokensByUser :one
SELECT COUNT(*)::int FROM user_tokens WHERE user_id = $1;

-- name: GetUserTokenByIDForUser :one
SELECT id, user_id, name, prefix, token_hash, scopes, expires_at,
       last_used_at, revoked_at, created_at, updated_at
FROM user_tokens
WHERE id = $1 AND user_id = $2;

-- name: ListActiveUserTokensByPrefix :many
SELECT id, user_id, name, prefix, token_hash, scopes, expires_at,
       last_used_at, revoked_at, created_at, updated_at
FROM user_tokens
WHERE prefix = $1
  AND revoked_at IS NULL
  AND (expires_at IS NULL OR expires_at > NOW());

-- name: UpdateUserTokenMetadata :one
UPDATE user_tokens
SET name = $3,
    scopes = $4,
    expires_at = $5
WHERE id = $1 AND user_id = $2 AND revoked_at IS NULL
RETURNING id, user_id, name, prefix, token_hash, scopes, expires_at,
          last_used_at, revoked_at, created_at, updated_at;

-- name: RevokeUserTokenForUser :execrows
UPDATE user_tokens
SET revoked_at = NOW()
WHERE id = $1 AND user_id = $2 AND revoked_at IS NULL;

-- name: TouchUserToken :exec
UPDATE user_tokens
SET last_used_at = clock_timestamp()
WHERE id = $1
  AND (
    last_used_at IS NULL
    OR last_used_at < clock_timestamp() - INTERVAL '5 minutes'
  );

-- name: ListUserTokenCoreGrants :many
SELECT id, token_id, permission, resource_type, resource_id, constraints, created_at
FROM user_token_core_grants
WHERE token_id = $1
ORDER BY permission, resource_type, resource_id NULLS FIRST;

-- name: DeleteUserTokenCoreGrants :exec
DELETE FROM user_token_core_grants WHERE token_id = $1;

-- name: CreateUserTokenCoreGrant :one
INSERT INTO user_token_core_grants (
    token_id, permission, resource_type, resource_id, constraints
) VALUES ($1, $2, $3, $4, $5)
RETURNING id, token_id, permission, resource_type, resource_id, constraints, created_at;

-- name: GetCoreIssuerInstanceID :one
SELECT issuer_instance_id FROM core_instance_identity WHERE singleton = TRUE;
