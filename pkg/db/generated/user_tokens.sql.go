// Code generated manually from pkg/db/queries/user_tokens.sql.

package db

import (
	"context"
	"time"

	"github.com/google/uuid"
)

func scanUserToken(row interface{ Scan(...any) error }, token *UserToken) error {
	return row.Scan(
		&token.ID,
		&token.UserID,
		&token.Name,
		&token.Prefix,
		&token.TokenHash,
		&token.Scopes,
		&token.ExpiresAt,
		&token.LastUsedAt,
		&token.RevokedAt,
		&token.CreatedAt,
		&token.UpdatedAt,
	)
}

func scanUserTokenCoreGrant(row interface{ Scan(...any) error }, grant *UserTokenCoreGrant) error {
	return row.Scan(
		&grant.ID,
		&grant.TokenID,
		&grant.Permission,
		&grant.ResourceType,
		&grant.ResourceID,
		&grant.Constraints,
		&grant.CreatedAt,
	)
}

const createUserToken = `-- name: CreateUserToken :one
INSERT INTO user_tokens (user_id, name, prefix, token_hash, scopes, expires_at)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING id, user_id, name, prefix, token_hash, scopes, expires_at,
          last_used_at, revoked_at, created_at, updated_at`

type CreateUserTokenParams struct {
	UserID    uuid.UUID  `db:"user_id" json:"user_id"`
	Name      string     `db:"name" json:"name"`
	Prefix    string     `db:"prefix" json:"prefix"`
	TokenHash string     `db:"token_hash" json:"-"`
	Scopes    []string   `db:"scopes" json:"scopes"`
	ExpiresAt *time.Time `db:"expires_at" json:"expires_at"`
}

func (q *Queries) CreateUserToken(ctx context.Context, arg CreateUserTokenParams) (UserToken, error) {
	row := q.db.QueryRow(ctx, createUserToken, arg.UserID, arg.Name, arg.Prefix, arg.TokenHash, arg.Scopes, arg.ExpiresAt)
	var token UserToken
	err := scanUserToken(row, &token)
	return token, err
}

const countActiveUserTokensByUser = `-- name: CountActiveUserTokensByUser :one
SELECT COUNT(*)::int
FROM user_tokens
WHERE user_id = $1
  AND revoked_at IS NULL
  AND (expires_at IS NULL OR expires_at > NOW())`

func (q *Queries) CountActiveUserTokensByUser(ctx context.Context, userID uuid.UUID) (int32, error) {
	var total int32
	err := q.db.QueryRow(ctx, countActiveUserTokensByUser, userID).Scan(&total)
	return total, err
}

const listUserTokensByUser = `-- name: ListUserTokensByUser :many
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
LIMIT $2 OFFSET $3`

type ListUserTokensByUserParams struct {
	UserID  uuid.UUID `db:"user_id" json:"user_id"`
	Limit   int32     `db:"limit" json:"limit"`
	Offset  int32     `db:"offset" json:"offset"`
	SortBy  string    `db:"sort_by" json:"sort_by"`
	SortDir string    `db:"sort_dir" json:"sort_dir"`
}

func (q *Queries) ListUserTokensByUser(ctx context.Context, arg ListUserTokensByUserParams) ([]UserToken, error) {
	rows, err := q.db.Query(ctx, listUserTokensByUser, arg.UserID, arg.Limit, arg.Offset, arg.SortBy, arg.SortDir)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]UserToken, 0)
	for rows.Next() {
		var token UserToken
		if err := scanUserToken(rows, &token); err != nil {
			return nil, err
		}
		items = append(items, token)
	}
	return items, rows.Err()
}

const countUserTokensByUser = `-- name: CountUserTokensByUser :one
SELECT COUNT(*)::int FROM user_tokens WHERE user_id = $1`

func (q *Queries) CountUserTokensByUser(ctx context.Context, userID uuid.UUID) (int32, error) {
	var total int32
	err := q.db.QueryRow(ctx, countUserTokensByUser, userID).Scan(&total)
	return total, err
}

const getUserTokenByIDForUser = `-- name: GetUserTokenByIDForUser :one
SELECT id, user_id, name, prefix, token_hash, scopes, expires_at,
       last_used_at, revoked_at, created_at, updated_at
FROM user_tokens
WHERE id = $1 AND user_id = $2`

type GetUserTokenByIDForUserParams struct {
	ID     uuid.UUID `db:"id" json:"id"`
	UserID uuid.UUID `db:"user_id" json:"user_id"`
}

func (q *Queries) GetUserTokenByIDForUser(ctx context.Context, arg GetUserTokenByIDForUserParams) (UserToken, error) {
	row := q.db.QueryRow(ctx, getUserTokenByIDForUser, arg.ID, arg.UserID)
	var token UserToken
	err := scanUserToken(row, &token)
	return token, err
}

const listActiveUserTokensByPrefix = `-- name: ListActiveUserTokensByPrefix :many
SELECT id, user_id, name, prefix, token_hash, scopes, expires_at,
       last_used_at, revoked_at, created_at, updated_at
FROM user_tokens
WHERE prefix = $1
  AND revoked_at IS NULL
  AND (expires_at IS NULL OR expires_at > NOW())`

func (q *Queries) ListActiveUserTokensByPrefix(ctx context.Context, prefix string) ([]UserToken, error) {
	rows, err := q.db.Query(ctx, listActiveUserTokensByPrefix, prefix)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]UserToken, 0)
	for rows.Next() {
		var token UserToken
		if err := scanUserToken(rows, &token); err != nil {
			return nil, err
		}
		items = append(items, token)
	}
	return items, rows.Err()
}

const updateUserTokenMetadata = `-- name: UpdateUserTokenMetadata :one
UPDATE user_tokens
SET name = $3,
    scopes = $4,
    expires_at = $5
WHERE id = $1 AND user_id = $2 AND revoked_at IS NULL
RETURNING id, user_id, name, prefix, token_hash, scopes, expires_at,
          last_used_at, revoked_at, created_at, updated_at`

type UpdateUserTokenMetadataParams struct {
	ID        uuid.UUID  `db:"id" json:"id"`
	UserID    uuid.UUID  `db:"user_id" json:"user_id"`
	Name      string     `db:"name" json:"name"`
	Scopes    []string   `db:"scopes" json:"scopes"`
	ExpiresAt *time.Time `db:"expires_at" json:"expires_at"`
}

func (q *Queries) UpdateUserTokenMetadata(ctx context.Context, arg UpdateUserTokenMetadataParams) (UserToken, error) {
	row := q.db.QueryRow(ctx, updateUserTokenMetadata, arg.ID, arg.UserID, arg.Name, arg.Scopes, arg.ExpiresAt)
	var token UserToken
	err := scanUserToken(row, &token)
	return token, err
}

const revokeUserTokenForUser = `-- name: RevokeUserTokenForUser :execrows
UPDATE user_tokens
SET revoked_at = NOW()
WHERE id = $1 AND user_id = $2 AND revoked_at IS NULL`

type RevokeUserTokenForUserParams struct {
	ID     uuid.UUID `db:"id" json:"id"`
	UserID uuid.UUID `db:"user_id" json:"user_id"`
}

func (q *Queries) RevokeUserTokenForUser(ctx context.Context, arg RevokeUserTokenForUserParams) (int64, error) {
	tag, err := q.db.Exec(ctx, revokeUserTokenForUser, arg.ID, arg.UserID)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

const touchUserToken = `-- name: TouchUserToken :exec
UPDATE user_tokens
SET last_used_at = clock_timestamp()
WHERE id = $1
  AND (
    last_used_at IS NULL
    OR last_used_at < clock_timestamp() - INTERVAL '5 minutes'
  )`

func (q *Queries) TouchUserToken(ctx context.Context, id uuid.UUID) error {
	_, err := q.db.Exec(ctx, touchUserToken, id)
	return err
}

const listUserTokenCoreGrants = `-- name: ListUserTokenCoreGrants :many
SELECT id, token_id, permission, resource_type, resource_id, constraints, created_at
FROM user_token_core_grants
WHERE token_id = $1
ORDER BY permission, resource_type, resource_id NULLS FIRST`

func (q *Queries) ListUserTokenCoreGrants(ctx context.Context, tokenID uuid.UUID) ([]UserTokenCoreGrant, error) {
	rows, err := q.db.Query(ctx, listUserTokenCoreGrants, tokenID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]UserTokenCoreGrant, 0)
	for rows.Next() {
		var grant UserTokenCoreGrant
		if err := scanUserTokenCoreGrant(rows, &grant); err != nil {
			return nil, err
		}
		items = append(items, grant)
	}
	return items, rows.Err()
}

const deleteUserTokenCoreGrants = `-- name: DeleteUserTokenCoreGrants :exec
DELETE FROM user_token_core_grants WHERE token_id = $1`

func (q *Queries) DeleteUserTokenCoreGrants(ctx context.Context, tokenID uuid.UUID) error {
	_, err := q.db.Exec(ctx, deleteUserTokenCoreGrants, tokenID)
	return err
}

const createUserTokenCoreGrant = `-- name: CreateUserTokenCoreGrant :one
INSERT INTO user_token_core_grants (
    token_id, permission, resource_type, resource_id, constraints
) VALUES ($1, $2, $3, $4, $5)
RETURNING id, token_id, permission, resource_type, resource_id, constraints, created_at`

type CreateUserTokenCoreGrantParams struct {
	TokenID      uuid.UUID  `db:"token_id" json:"token_id"`
	Permission   string     `db:"permission" json:"permission"`
	ResourceType string     `db:"resource_type" json:"resource_type"`
	ResourceID   *uuid.UUID `db:"resource_id" json:"resource_id"`
	Constraints  []byte     `db:"constraints" json:"constraints"`
}

func (q *Queries) CreateUserTokenCoreGrant(ctx context.Context, arg CreateUserTokenCoreGrantParams) (UserTokenCoreGrant, error) {
	row := q.db.QueryRow(ctx, createUserTokenCoreGrant, arg.TokenID, arg.Permission, arg.ResourceType, arg.ResourceID, arg.Constraints)
	var grant UserTokenCoreGrant
	err := scanUserTokenCoreGrant(row, &grant)
	return grant, err
}

const getCoreIssuerInstanceID = `-- name: GetCoreIssuerInstanceID :one
SELECT issuer_instance_id FROM core_instance_identity WHERE singleton = TRUE`

func (q *Queries) GetCoreIssuerInstanceID(ctx context.Context) (string, error) {
	var issuer string
	err := q.db.QueryRow(ctx, getCoreIssuerInstanceID).Scan(&issuer)
	return issuer, err
}
