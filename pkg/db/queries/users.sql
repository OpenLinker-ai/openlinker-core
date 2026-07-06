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
          avatar_url, is_creator, creator_verified, is_admin, disabled_at,
          created_at, updated_at, deleted_at;

-- name: CreateAdminUser :one
-- 管理台创建邮箱密码用户，可同时设置管理/创作者标志。
INSERT INTO users (
    email,
    password_hash,
    display_name,
    is_admin,
    is_creator,
    creator_verified
) VALUES (
    $1, $2, $3, $4, $5, $6
)
RETURNING id, email, password_hash, oauth_provider, oauth_id, display_name,
          avatar_url, is_creator, creator_verified, is_admin, disabled_at,
          created_at, updated_at, deleted_at;

-- name: GetUserByEmail :one
-- 按 email 查活跃用户
SELECT id, email, password_hash, oauth_provider, oauth_id, display_name,
       avatar_url, is_creator, creator_verified, is_admin, disabled_at,
       created_at, updated_at, deleted_at
FROM users
WHERE email = $1 AND deleted_at IS NULL;

-- name: GetUserByOAuth :one
-- 按 OAuth provider + oauth_id 查活跃用户
SELECT id, email, password_hash, oauth_provider, oauth_id, display_name,
       avatar_url, is_creator, creator_verified, is_admin, disabled_at,
       created_at, updated_at, deleted_at
FROM users
WHERE oauth_provider = $1 AND oauth_id = $2 AND deleted_at IS NULL;

-- name: GetUserByID :one
-- 按 id 查活跃用户
SELECT id, email, password_hash, oauth_provider, oauth_id, display_name,
       avatar_url, is_creator, creator_verified, is_admin, disabled_at,
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
WHERE id = $1 AND deleted_at IS NULL AND disabled_at IS NULL;

-- name: ListAdminUsers :many
-- 管理台用户列表：按邮箱 / 昵称搜索，可筛角色。
SELECT u.id, u.email, u.password_hash, u.oauth_provider, u.oauth_id, u.display_name,
       u.avatar_url, u.is_creator, u.creator_verified, u.is_admin, u.disabled_at,
       u.created_at, u.updated_at, u.deleted_at,
       COALESCE(agent_stats.agent_count, 0)::int AS agent_count,
       COALESCE(agent_stats.active_agent_count, 0)::int AS active_agent_count,
       COALESCE(task_stats.task_count, 0)::int AS task_count,
       COALESCE(task_stats.public_task_count, 0)::int AS public_task_count,
       COALESCE(run_stats.run_count, 0)::int AS run_count,
       task_stats.last_task_at,
       run_stats.last_run_at
FROM users u
LEFT JOIN LATERAL (
    SELECT
        COUNT(*)::int AS agent_count,
        COUNT(*) FILTER (WHERE lifecycle_status = 'active')::int AS active_agent_count
    FROM agents
    WHERE creator_id = u.id
) agent_stats ON TRUE
LEFT JOIN LATERAL (
    SELECT
        COUNT(*)::int AS task_count,
        COUNT(*) FILTER (WHERE visibility = 'public')::int AS public_task_count,
        MAX(created_at) AS last_task_at
    FROM task_queries
    WHERE user_id = u.id
) task_stats ON TRUE
LEFT JOIN LATERAL (
    SELECT
        COUNT(*)::int AS run_count,
        MAX(started_at) AS last_run_at
    FROM runs
    WHERE user_id = u.id
) run_stats ON TRUE
WHERE u.deleted_at IS NULL
  AND (
    $1::text = ''
    OR u.email ILIKE '%' || $1 || '%'
    OR u.display_name ILIKE '%' || $1 || '%'
  )
  AND (
    $2::text = ''
    OR ($2 = 'admin' AND u.is_admin)
    OR ($2 = 'creator' AND u.is_creator)
    OR ($2 = 'creator_verified' AND u.creator_verified)
    OR ($2 = 'regular' AND NOT u.is_admin AND NOT u.is_creator)
    OR ($2 = 'disabled' AND u.disabled_at IS NOT NULL)
  )
ORDER BY u.created_at DESC
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
    OR ($2 = 'disabled' AND disabled_at IS NOT NULL)
  );

-- name: UpdateAdminUserFlags :one
-- 管理台调整用户身份标志。
UPDATE users
SET is_admin = $2,
    is_creator = $3,
    creator_verified = $4,
    disabled_at = CASE WHEN $5 THEN COALESCE(disabled_at, NOW()) ELSE NULL END,
    updated_at = NOW()
WHERE id = $1 AND deleted_at IS NULL
RETURNING id, email, password_hash, oauth_provider, oauth_id, display_name,
          avatar_url, is_creator, creator_verified, is_admin, disabled_at,
          created_at, updated_at, deleted_at;
