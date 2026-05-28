-- name: CreateApiKey :one
-- 创建访问令牌行。key_hash 是 bcrypt(完整明文)。
INSERT INTO api_keys (user_id, name, prefix, key_hash, scopes)
VALUES ($1, $2, $3, $4, $5)
RETURNING id, user_id, name, prefix, key_hash, scopes, last_used_at, revoked_at, created_at;

-- name: ListApiKeysByUser :many
-- 列出用户全部访问令牌（含已撤销），按创建时间倒序。
SELECT id, user_id, name, prefix, key_hash, scopes, last_used_at, revoked_at, created_at
FROM api_keys
WHERE user_id = $1
ORDER BY created_at DESC;

-- name: CountActiveApiKeysByUser :one
-- 统计用户当前有效（未撤销）访问令牌数量，用于上限校验。
SELECT COUNT(*)::int AS total
FROM api_keys
WHERE user_id = $1 AND revoked_at IS NULL;

-- name: GetApiKeyByID :one
-- 按 id 取单条（包括已撤销的）。
SELECT id, user_id, name, prefix, key_hash, scopes, last_used_at, revoked_at, created_at
FROM api_keys
WHERE id = $1;

-- name: ListApiKeysByPrefix :many
-- 鉴权用：按 prefix 找候选（一般 1-2 条），由调用方 bcrypt.Compare 完整明文。
-- 仅返回未撤销的 key。
SELECT id, user_id, name, prefix, key_hash, scopes, last_used_at, revoked_at, created_at
FROM api_keys
WHERE prefix = $1 AND revoked_at IS NULL;

-- name: RevokeApiKey :exec
-- 撤销：仅在 owner 匹配且尚未撤销时生效。
UPDATE api_keys
SET revoked_at = NOW()
WHERE id = $1 AND user_id = $2 AND revoked_at IS NULL;

-- name: TouchApiKey :exec
-- 鉴权命中后更新 last_used_at（fire-and-forget）。
UPDATE api_keys
SET last_used_at = NOW()
WHERE id = $1;
