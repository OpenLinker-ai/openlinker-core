// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/agent_tokens.sql）。

package db

import (
	"context"
	"time"

	"github.com/google/uuid"
)

func scanAgentToken(row interface {
	Scan(dest ...any) error
}, token *AgentToken) error {
	return row.Scan(
		&token.ID,
		&token.AgentID,
		&token.CreatorUserID,
		&token.Name,
		&token.Prefix,
		&token.TokenHash,
		&token.Scopes,
		&token.Status,
		&token.ExpiresAt,
		&token.RedeemedAt,
		&token.LastUsedAt,
		&token.RevokedAt,
		&token.CreatedAt,
	)
}

const createAgentToken = `-- name: CreateAgentToken :one
INSERT INTO agent_tokens (
    agent_id, creator_user_id, name, prefix, token_hash, scopes, status, expires_at, redeemed_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9
)
RETURNING id, agent_id, creator_user_id, name, prefix, token_hash, scopes, status,
          expires_at, redeemed_at, last_used_at, revoked_at, created_at`

type CreateAgentTokenParams struct {
	AgentID       *uuid.UUID `db:"agent_id" json:"agent_id"`
	CreatorUserID uuid.UUID  `db:"creator_user_id" json:"creator_user_id"`
	Name          string     `db:"name" json:"name"`
	Prefix        string     `db:"prefix" json:"prefix"`
	TokenHash     string     `db:"token_hash" json:"-"`
	Scopes        []string   `db:"scopes" json:"scopes"`
	Status        string     `db:"status" json:"status"`
	ExpiresAt     *time.Time `db:"expires_at" json:"expires_at"`
	RedeemedAt    *time.Time `db:"redeemed_at" json:"redeemed_at"`
}

func (q *Queries) CreateAgentToken(ctx context.Context, arg CreateAgentTokenParams) (AgentToken, error) {
	row := q.db.QueryRow(ctx, createAgentToken,
		arg.AgentID,
		arg.CreatorUserID,
		arg.Name,
		arg.Prefix,
		arg.TokenHash,
		arg.Scopes,
		arg.Status,
		arg.ExpiresAt,
		arg.RedeemedAt,
	)
	var token AgentToken
	err := scanAgentToken(row, &token)
	return token, err
}

const listAgentTokensByCreator = `-- name: ListAgentTokensByCreator :many
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
LIMIT $2 OFFSET $3`

type ListAgentTokensByCreatorParams struct {
	CreatorUserID uuid.UUID `db:"creator_user_id" json:"creator_user_id"`
	Limit         int32     `db:"limit" json:"limit"`
	Offset        int32     `db:"offset" json:"offset"`
	SortBy        string    `db:"sort_by" json:"sort_by"`
	SortDir       string    `db:"sort_dir" json:"sort_dir"`
}

func (q *Queries) ListAgentTokensByCreator(ctx context.Context, arg ListAgentTokensByCreatorParams) ([]AgentToken, error) {
	rows, err := q.db.Query(ctx, listAgentTokensByCreator, arg.CreatorUserID, arg.Limit, arg.Offset, arg.SortBy, arg.SortDir)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []AgentToken
	for rows.Next() {
		var token AgentToken
		if err := scanAgentToken(rows, &token); err != nil {
			return nil, err
		}
		items = append(items, token)
	}
	return items, rows.Err()
}

const countAgentTokensByCreator = `-- name: CountAgentTokensByCreator :one
SELECT COUNT(*)::int AS total
FROM agent_tokens
WHERE creator_user_id = $1`

func (q *Queries) CountAgentTokensByCreator(ctx context.Context, creatorUserID uuid.UUID) (int32, error) {
	var total int32
	err := q.db.QueryRow(ctx, countAgentTokensByCreator, creatorUserID).Scan(&total)
	return total, err
}

const listAgentTokensByCreatorAndAgent = `-- name: ListAgentTokensByCreatorAndAgent :many
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
LIMIT $3 OFFSET $4`

type ListAgentTokensByCreatorAndAgentParams struct {
	CreatorUserID uuid.UUID `db:"creator_user_id" json:"creator_user_id"`
	AgentID       uuid.UUID `db:"agent_id" json:"agent_id"`
	Limit         int32     `db:"limit" json:"limit"`
	Offset        int32     `db:"offset" json:"offset"`
	SortBy        string    `db:"sort_by" json:"sort_by"`
	SortDir       string    `db:"sort_dir" json:"sort_dir"`
}

func (q *Queries) ListAgentTokensByCreatorAndAgent(ctx context.Context, arg ListAgentTokensByCreatorAndAgentParams) ([]AgentToken, error) {
	rows, err := q.db.Query(ctx, listAgentTokensByCreatorAndAgent, arg.CreatorUserID, arg.AgentID, arg.Limit, arg.Offset, arg.SortBy, arg.SortDir)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []AgentToken
	for rows.Next() {
		var token AgentToken
		if err := scanAgentToken(rows, &token); err != nil {
			return nil, err
		}
		items = append(items, token)
	}
	return items, rows.Err()
}

const countAgentTokensByCreatorAndAgent = `-- name: CountAgentTokensByCreatorAndAgent :one
SELECT COUNT(*)::int AS total
FROM agent_tokens
WHERE creator_user_id = $1 AND agent_id = $2`

type CountAgentTokensByCreatorAndAgentParams struct {
	CreatorUserID uuid.UUID `db:"creator_user_id" json:"creator_user_id"`
	AgentID       uuid.UUID `db:"agent_id" json:"agent_id"`
}

func (q *Queries) CountAgentTokensByCreatorAndAgent(ctx context.Context, arg CountAgentTokensByCreatorAndAgentParams) (int32, error) {
	var total int32
	err := q.db.QueryRow(ctx, countAgentTokensByCreatorAndAgent, arg.CreatorUserID, arg.AgentID).Scan(&total)
	return total, err
}

const getAgentTokenByIDForCreator = `-- name: GetAgentTokenByIDForCreator :one
SELECT id, agent_id, creator_user_id, name, prefix, token_hash, scopes, status,
       expires_at, redeemed_at, last_used_at, revoked_at, created_at
FROM agent_tokens
WHERE id = $1 AND creator_user_id = $2`

type GetAgentTokenByIDForCreatorParams struct {
	ID            uuid.UUID `db:"id" json:"id"`
	CreatorUserID uuid.UUID `db:"creator_user_id" json:"creator_user_id"`
}

func (q *Queries) GetAgentTokenByIDForCreator(ctx context.Context, arg GetAgentTokenByIDForCreatorParams) (AgentToken, error) {
	row := q.db.QueryRow(ctx, getAgentTokenByIDForCreator, arg.ID, arg.CreatorUserID)
	var token AgentToken
	err := scanAgentToken(row, &token)
	return token, err
}

const listActiveAgentTokensByPrefix = `-- name: ListActiveAgentTokensByPrefix :many
SELECT id, agent_id, creator_user_id, name, prefix, token_hash, scopes, status,
       expires_at, redeemed_at, last_used_at, revoked_at, created_at
FROM agent_tokens
WHERE prefix = $1 AND revoked_at IS NULL`

func (q *Queries) ListActiveAgentTokensByPrefix(ctx context.Context, prefix string) ([]AgentToken, error) {
	rows, err := q.db.Query(ctx, listActiveAgentTokensByPrefix, prefix)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []AgentToken
	for rows.Next() {
		var token AgentToken
		if err := scanAgentToken(rows, &token); err != nil {
			return nil, err
		}
		items = append(items, token)
	}
	return items, rows.Err()
}

const redeemPendingAgentToken = `-- name: RedeemPendingAgentToken :one
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
          expires_at, redeemed_at, last_used_at, revoked_at, created_at`

type RedeemPendingAgentTokenParams struct {
	ID            uuid.UUID `db:"id" json:"id"`
	AgentID       uuid.UUID `db:"agent_id" json:"agent_id"`
	Scopes        []string  `db:"scopes" json:"scopes"`
	CreatorUserID uuid.UUID `db:"creator_user_id" json:"creator_user_id"`
	TokenHash     string    `db:"token_hash" json:"-"`
}

func (q *Queries) RedeemPendingAgentToken(ctx context.Context, arg RedeemPendingAgentTokenParams) (AgentToken, error) {
	row := q.db.QueryRow(ctx, redeemPendingAgentToken, arg.ID, arg.AgentID, arg.Scopes, arg.CreatorUserID, arg.TokenHash)
	var token AgentToken
	err := scanAgentToken(row, &token)
	return token, err
}

const revokeAgentTokenForCreator = `-- name: RevokeAgentTokenForCreator :execrows
UPDATE agent_tokens
SET revoked_at = NOW(),
    status = 'revoked',
    revocation_kind = 'manual'
WHERE id = $1
  AND creator_user_id = $2
  AND revoked_at IS NULL`

type RevokeAgentTokenForCreatorParams struct {
	ID            uuid.UUID `db:"id" json:"id"`
	CreatorUserID uuid.UUID `db:"creator_user_id" json:"creator_user_id"`
}

func (q *Queries) RevokeAgentTokenForCreator(ctx context.Context, arg RevokeAgentTokenForCreatorParams) (int64, error) {
	tag, err := q.db.Exec(ctx, revokeAgentTokenForCreator, arg.ID, arg.CreatorUserID)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

const countActiveAgentTokensByAgent = `-- name: CountActiveAgentTokensByAgent :one
SELECT COUNT(*)::int AS total
FROM agent_tokens
WHERE agent_id = $1
  AND status = 'active_runtime'
  AND revoked_at IS NULL`

func (q *Queries) CountActiveAgentTokensByAgent(ctx context.Context, agentID uuid.UUID) (int32, error) {
	var total int32
	err := q.db.QueryRow(ctx, countActiveAgentTokensByAgent, agentID).Scan(&total)
	return total, err
}
