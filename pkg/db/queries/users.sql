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

-- name: ListAdminUsers :many
-- 管理台用户列表：按邮箱 / 昵称搜索，可筛角色。
SELECT id, email, password_hash, oauth_provider, oauth_id, display_name,
       avatar_url, is_creator, creator_verified, is_admin,
       created_at, updated_at, deleted_at
FROM users
WHERE deleted_at IS NULL
  AND (
    $1::text = ''
    OR email ILIKE '%' || $1 || '%'
    OR display_name ILIKE '%' || $1 || '%'
  )
  AND (
    $2::text = ''
    OR ($2 = 'admin' AND is_admin)
    OR ($2 = 'creator' AND is_creator)
    OR ($2 = 'creator_verified' AND creator_verified)
    OR ($2 = 'regular' AND NOT is_admin AND NOT is_creator)
  )
ORDER BY created_at DESC
LIMIT $3 OFFSET $4;

-- name: CountAdminUsers :one
SELECT COUNT(*)::int AS total
FROM users
WHERE deleted_at IS NULL
  AND (
    $1::text = ''
    OR email ILIKE '%' || $1 || '%'
    OR display_name ILIKE '%' || $1 || '%'
  )
  AND (
    $2::text = ''
    OR ($2 = 'admin' AND is_admin)
    OR ($2 = 'creator' AND is_creator)
    OR ($2 = 'creator_verified' AND creator_verified)
    OR ($2 = 'regular' AND NOT is_admin AND NOT is_creator)
  );

-- name: UpdateAdminUserFlags :one
-- 管理台调整用户身份标志。
UPDATE users
SET is_admin = $2,
    is_creator = $3,
    creator_verified = $4,
    updated_at = NOW()
WHERE id = $1 AND deleted_at IS NULL
RETURNING id, email, password_hash, oauth_provider, oauth_id, display_name,
          avatar_url, is_creator, creator_verified, is_admin,
          created_at, updated_at, deleted_at;
