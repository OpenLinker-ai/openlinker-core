// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/users.sql）。

package db

import (
	"context"

	"github.com/google/uuid"
)

const createUser = `-- name: CreateUser :one
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
          created_at, updated_at, deleted_at`

// CreateUserParams 入参（password_hash / oauth_* / avatar_url 可空）。
type CreateUserParams struct {
	Email         string  `db:"email" json:"email"`
	PasswordHash  *string `db:"password_hash" json:"password_hash"`
	OauthProvider *string `db:"oauth_provider" json:"oauth_provider"`
	OauthID       *string `db:"oauth_id" json:"oauth_id"`
	DisplayName   string  `db:"display_name" json:"display_name"`
	AvatarURL     *string `db:"avatar_url" json:"avatar_url"`
}

// CreateUser 创建用户。
func (q *Queries) CreateUser(ctx context.Context, arg CreateUserParams) (User, error) {
	row := q.db.QueryRow(ctx, createUser,
		arg.Email,
		arg.PasswordHash,
		arg.OauthProvider,
		arg.OauthID,
		arg.DisplayName,
		arg.AvatarURL,
	)
	var u User
	err := row.Scan(
		&u.ID,
		&u.Email,
		&u.PasswordHash,
		&u.OauthProvider,
		&u.OauthID,
		&u.DisplayName,
		&u.AvatarURL,
		&u.IsCreator,
		&u.CreatorVerified,
		&u.IsAdmin,
		&u.CreatedAt,
		&u.UpdatedAt,
		&u.DeletedAt,
	)
	return u, err
}

const getUserByEmail = `-- name: GetUserByEmail :one
SELECT id, email, password_hash, oauth_provider, oauth_id, display_name,
       avatar_url, is_creator, creator_verified, is_admin,
       created_at, updated_at, deleted_at
FROM users
WHERE email = $1 AND deleted_at IS NULL`

// GetUserByEmail 按 email 查活跃用户。
func (q *Queries) GetUserByEmail(ctx context.Context, email string) (User, error) {
	row := q.db.QueryRow(ctx, getUserByEmail, email)
	var u User
	err := row.Scan(
		&u.ID,
		&u.Email,
		&u.PasswordHash,
		&u.OauthProvider,
		&u.OauthID,
		&u.DisplayName,
		&u.AvatarURL,
		&u.IsCreator,
		&u.CreatorVerified,
		&u.IsAdmin,
		&u.CreatedAt,
		&u.UpdatedAt,
		&u.DeletedAt,
	)
	return u, err
}

const getUserByOAuth = `-- name: GetUserByOAuth :one
SELECT id, email, password_hash, oauth_provider, oauth_id, display_name,
       avatar_url, is_creator, creator_verified, is_admin,
       created_at, updated_at, deleted_at
FROM users
WHERE oauth_provider = $1 AND oauth_id = $2 AND deleted_at IS NULL`

// GetUserByOAuthParams 入参。
type GetUserByOAuthParams struct {
	OauthProvider *string `db:"oauth_provider" json:"oauth_provider"`
	OauthID       *string `db:"oauth_id" json:"oauth_id"`
}

// GetUserByOAuth 按 OAuth provider + id 查活跃用户。
func (q *Queries) GetUserByOAuth(ctx context.Context, arg GetUserByOAuthParams) (User, error) {
	row := q.db.QueryRow(ctx, getUserByOAuth, arg.OauthProvider, arg.OauthID)
	var u User
	err := row.Scan(
		&u.ID,
		&u.Email,
		&u.PasswordHash,
		&u.OauthProvider,
		&u.OauthID,
		&u.DisplayName,
		&u.AvatarURL,
		&u.IsCreator,
		&u.CreatorVerified,
		&u.IsAdmin,
		&u.CreatedAt,
		&u.UpdatedAt,
		&u.DeletedAt,
	)
	return u, err
}

const getUserByID = `-- name: GetUserByID :one
SELECT id, email, password_hash, oauth_provider, oauth_id, display_name,
       avatar_url, is_creator, creator_verified, is_admin,
       created_at, updated_at, deleted_at
FROM users
WHERE id = $1 AND deleted_at IS NULL`

// GetUserByID 按 id 查活跃用户。
func (q *Queries) GetUserByID(ctx context.Context, id uuid.UUID) (User, error) {
	row := q.db.QueryRow(ctx, getUserByID, id)
	var u User
	err := row.Scan(
		&u.ID,
		&u.Email,
		&u.PasswordHash,
		&u.OauthProvider,
		&u.OauthID,
		&u.DisplayName,
		&u.AvatarURL,
		&u.IsCreator,
		&u.CreatorVerified,
		&u.IsAdmin,
		&u.CreatedAt,
		&u.UpdatedAt,
		&u.DeletedAt,
	)
	return u, err
}

const updateUserOAuth = `-- name: UpdateUserOAuth :exec
UPDATE users
SET oauth_provider = $2,
    oauth_id = $3,
    avatar_url = COALESCE(NULLIF($4, ''), avatar_url)
WHERE id = $1 AND deleted_at IS NULL`

// UpdateUserOAuthParams 入参。
type UpdateUserOAuthParams struct {
	ID            uuid.UUID `db:"id" json:"id"`
	OauthProvider *string   `db:"oauth_provider" json:"oauth_provider"`
	OauthID       *string   `db:"oauth_id" json:"oauth_id"`
	AvatarURL     string    `db:"avatar_url" json:"avatar_url"`
}

// UpdateUserOAuth 已有 email 用户绑定 OAuth（预留，Phase 1 不使用）。
func (q *Queries) UpdateUserOAuth(ctx context.Context, arg UpdateUserOAuthParams) error {
	_, err := q.db.Exec(ctx, updateUserOAuth,
		arg.ID,
		arg.OauthProvider,
		arg.OauthID,
		arg.AvatarURL,
	)
	return err
}

const updateUserBecomeCreator = `-- name: UpdateUserBecomeCreator :exec
UPDATE users
SET is_creator = TRUE,
    updated_at = NOW()
WHERE id = $1 AND deleted_at IS NULL`

// UpdateUserBecomeCreator Phase 1 一键成为创作者（无审核）。
// 返回受影响行数：0 表示用户不存在或已被软删。
func (q *Queries) UpdateUserBecomeCreator(ctx context.Context, id uuid.UUID) (int64, error) {
	tag, err := q.db.Exec(ctx, updateUserBecomeCreator, id)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
