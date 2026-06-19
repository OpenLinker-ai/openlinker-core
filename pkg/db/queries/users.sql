-- name: CreateUser :one
-- 创建邮箱密码注册用户
INSERT INTO users (
    email,
    password_hash,
    oauth_provider,
    oauth_id,
    display_name,
    avatar_url
) VALUES (
    $1, $2, $3, $4, $5, $6
)
RETURNING id, email, password_hash, oauth_provider, oauth_id, display_name,
          avatar_url, is_creator, creator_verified, is_admin,
          created_at, updated_at, deleted_at;

-- name: GetUserByEmail :one
-- 按 email 查活跃用户
SELECT id, email, password_hash, oauth_provider, oauth_id, display_name,
       avatar_url, is_creator, creator_verified, is_admin,
       created_at, updated_at, deleted_at
FROM users
WHERE email = $1 AND deleted_at IS NULL;

-- name: GetUserByOAuth :one
-- 按 OAuth provider + oauth_id 查活跃用户
SELECT id, email, password_hash, oauth_provider, oauth_id, display_name,
       avatar_url, is_creator, creator_verified, is_admin,
       created_at, updated_at, deleted_at
FROM users
WHERE oauth_provider = $1 AND oauth_id = $2 AND deleted_at IS NULL;

-- name: GetUserByID :one
-- 按 id 查活跃用户
SELECT id, email, password_hash, oauth_provider, oauth_id, display_name,
       avatar_url, is_creator, creator_verified, is_admin,
       created_at, updated_at, deleted_at
FROM users
WHERE id = $1 AND deleted_at IS NULL;

-- name: UpdateUserOAuth :exec
-- 把已有 email 用户绑定到 OAuth（Phase 1 暂不使用，预留）
UPDATE users
SET oauth_provider = $2,
    oauth_id = $3,
    avatar_url = COALESCE(NULLIF($4, ''), avatar_url)
WHERE id = $1 AND deleted_at IS NULL;

-- name: UpdateUserBecomeCreator :exec
-- Phase 1：一键成为创作者（无审核），后续 creator_verified 由人工设置
UPDATE users
SET is_creator = TRUE,
    updated_at = NOW()
WHERE id = $1 AND deleted_at IS NULL;
