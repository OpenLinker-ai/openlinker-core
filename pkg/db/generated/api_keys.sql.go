// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/api_keys.sql）。
//
// 子轮 2.1：API Key 管理（CRUD + 鉴权）。

package db

import (
	"context"

	"github.com/google/uuid"
)

// scanApiKey 把一行扫描成 ApiKey 结构（按声明列顺序，给所有 SELECT/RETURNING 共用）。
func scanApiKey(row interface {
	Scan(dest ...any) error
}, k *ApiKey) error {
	return row.Scan(
		&k.ID,
		&k.UserID,
		&k.Name,
		&k.Prefix,
		&k.KeyHash,
		&k.LastUsedAt,
		&k.RevokedAt,
		&k.CreatedAt,
	)
}

const createApiKey = `-- name: CreateApiKey :one
INSERT INTO api_keys (user_id, name, prefix, key_hash)
VALUES ($1, $2, $3, $4)
RETURNING id, user_id, name, prefix, key_hash, last_used_at, revoked_at, created_at`

// CreateApiKeyParams 入参。
type CreateApiKeyParams struct {
	UserID  uuid.UUID `db:"user_id" json:"user_id"`
	Name    string    `db:"name" json:"name"`
	Prefix  string    `db:"prefix" json:"prefix"`
	KeyHash string    `db:"key_hash" json:"key_hash"`
}

// CreateApiKey 创建 API Key 行（key_hash 应为 bcrypt 完整明文后的值）。
func (q *Queries) CreateApiKey(ctx context.Context, arg CreateApiKeyParams) (ApiKey, error) {
	row := q.db.QueryRow(ctx, createApiKey,
		arg.UserID,
		arg.Name,
		arg.Prefix,
		arg.KeyHash,
	)
	var k ApiKey
	err := scanApiKey(row, &k)
	return k, err
}

const listApiKeysByUser = `-- name: ListApiKeysByUser :many
SELECT id, user_id, name, prefix, key_hash, last_used_at, revoked_at, created_at
FROM api_keys
WHERE user_id = $1
ORDER BY created_at DESC`

// ListApiKeysByUser 列出用户全部 API Key（含已撤销）。
func (q *Queries) ListApiKeysByUser(ctx context.Context, userID uuid.UUID) ([]ApiKey, error) {
	rows, err := q.db.Query(ctx, listApiKeysByUser, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []ApiKey
	for rows.Next() {
		var k ApiKey
		if err := scanApiKey(rows, &k); err != nil {
			return nil, err
		}
		items = append(items, k)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const countActiveApiKeysByUser = `-- name: CountActiveApiKeysByUser :one
SELECT COUNT(*)::int AS total
FROM api_keys
WHERE user_id = $1 AND revoked_at IS NULL`

// CountActiveApiKeysByUser 用户当前有效（未撤销）数量。
func (q *Queries) CountActiveApiKeysByUser(ctx context.Context, userID uuid.UUID) (int32, error) {
	row := q.db.QueryRow(ctx, countActiveApiKeysByUser, userID)
	var total int32
	err := row.Scan(&total)
	return total, err
}

const getApiKeyByID = `-- name: GetApiKeyByID :one
SELECT id, user_id, name, prefix, key_hash, last_used_at, revoked_at, created_at
FROM api_keys
WHERE id = $1`

// GetApiKeyByID 按 id 查（含已撤销）。
func (q *Queries) GetApiKeyByID(ctx context.Context, id uuid.UUID) (ApiKey, error) {
	row := q.db.QueryRow(ctx, getApiKeyByID, id)
	var k ApiKey
	err := scanApiKey(row, &k)
	return k, err
}

const listApiKeysByPrefix = `-- name: ListApiKeysByPrefix :many
SELECT id, user_id, name, prefix, key_hash, last_used_at, revoked_at, created_at
FROM api_keys
WHERE prefix = $1 AND revoked_at IS NULL`

// ListApiKeysByPrefix 鉴权用：按 prefix 取候选（仅未撤销），由调用方 bcrypt.Compare 验证。
func (q *Queries) ListApiKeysByPrefix(ctx context.Context, prefix string) ([]ApiKey, error) {
	rows, err := q.db.Query(ctx, listApiKeysByPrefix, prefix)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []ApiKey
	for rows.Next() {
		var k ApiKey
		if err := scanApiKey(rows, &k); err != nil {
			return nil, err
		}
		items = append(items, k)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const revokeApiKey = `-- name: RevokeApiKey :exec
UPDATE api_keys
SET revoked_at = NOW()
WHERE id = $1 AND user_id = $2 AND revoked_at IS NULL`

// RevokeApiKeyParams 入参。
type RevokeApiKeyParams struct {
	ID     uuid.UUID `db:"id" json:"id"`
	UserID uuid.UUID `db:"user_id" json:"user_id"`
}

// RevokeApiKey 撤销 API Key（仅 owner 匹配 + 未撤销时生效）。
// 返回受影响行数：0 表示不存在 / 不是该 user / 已撤销。
func (q *Queries) RevokeApiKey(ctx context.Context, arg RevokeApiKeyParams) (int64, error) {
	tag, err := q.db.Exec(ctx, revokeApiKey, arg.ID, arg.UserID)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

const touchApiKey = `-- name: TouchApiKey :exec
UPDATE api_keys
SET last_used_at = NOW()
WHERE id = $1`

// TouchApiKey 更新 last_used_at（鉴权命中后 fire-and-forget 调用）。
func (q *Queries) TouchApiKey(ctx context.Context, id uuid.UUID) error {
	_, err := q.db.Exec(ctx, touchApiKey, id)
	return err
}
