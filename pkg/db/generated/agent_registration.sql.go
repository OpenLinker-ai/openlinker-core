// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/agent_registration.sql）。

package db

import (
	"context"
	"time"

	"github.com/google/uuid"
)

const createAgentRegistrationToken = `-- name: CreateAgentRegistrationToken :one
INSERT INTO agent_registration_tokens (
    creator_user_id, label, prefix, token_hash, max_agents, expires_at
) VALUES (
    $1, $2, $3, $4, $5, $6
)
RETURNING id, creator_user_id, label, prefix, token_hash, max_agents, used_count,
          expires_at, revoked_at, last_used_at, created_at`

type CreateAgentRegistrationTokenParams struct {
	CreatorUserID uuid.UUID `db:"creator_user_id" json:"creator_user_id"`
	Label         string    `db:"label" json:"label"`
	Prefix        string    `db:"prefix" json:"prefix"`
	TokenHash     string    `db:"token_hash" json:"-"`
	MaxAgents     int32     `db:"max_agents" json:"max_agents"`
	ExpiresAt     time.Time `db:"expires_at" json:"expires_at"`
}

func (q *Queries) CreateAgentRegistrationToken(ctx context.Context, arg CreateAgentRegistrationTokenParams) (AgentRegistrationToken, error) {
	row := q.db.QueryRow(ctx, createAgentRegistrationToken,
		arg.CreatorUserID, arg.Label, arg.Prefix, arg.TokenHash, arg.MaxAgents, arg.ExpiresAt)
	var token AgentRegistrationToken
	err := row.Scan(
		&token.ID, &token.CreatorUserID, &token.Label, &token.Prefix, &token.TokenHash,
		&token.MaxAgents, &token.UsedCount, &token.ExpiresAt, &token.RevokedAt,
		&token.LastUsedAt, &token.CreatedAt,
	)
	return token, err
}

const listAgentRegistrationTokensByCreator = `-- name: ListAgentRegistrationTokensByCreator :many
SELECT id, creator_user_id, label, prefix, token_hash, max_agents, used_count,
       expires_at, revoked_at, last_used_at, created_at
FROM agent_registration_tokens
WHERE creator_user_id = $1
ORDER BY created_at DESC`

func (q *Queries) ListAgentRegistrationTokensByCreator(ctx context.Context, creatorUserID uuid.UUID) ([]AgentRegistrationToken, error) {
	rows, err := q.db.Query(ctx, listAgentRegistrationTokensByCreator, creatorUserID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []AgentRegistrationToken
	for rows.Next() {
		var token AgentRegistrationToken
		if err := rows.Scan(
			&token.ID, &token.CreatorUserID, &token.Label, &token.Prefix, &token.TokenHash,
			&token.MaxAgents, &token.UsedCount, &token.ExpiresAt, &token.RevokedAt,
			&token.LastUsedAt, &token.CreatedAt,
		); err != nil {
			return nil, err
		}
		items = append(items, token)
	}
	return items, rows.Err()
}

const listActiveAgentRegistrationTokensByPrefix = `-- name: ListActiveAgentRegistrationTokensByPrefix :many
SELECT id, creator_user_id, label, prefix, token_hash, max_agents, used_count,
       expires_at, revoked_at, last_used_at, created_at
FROM agent_registration_tokens
WHERE prefix = $1 AND revoked_at IS NULL`

func (q *Queries) ListActiveAgentRegistrationTokensByPrefix(ctx context.Context, prefix string) ([]AgentRegistrationToken, error) {
	rows, err := q.db.Query(ctx, listActiveAgentRegistrationTokensByPrefix, prefix)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []AgentRegistrationToken
	for rows.Next() {
		var token AgentRegistrationToken
		if err := rows.Scan(
			&token.ID, &token.CreatorUserID, &token.Label, &token.Prefix, &token.TokenHash,
			&token.MaxAgents, &token.UsedCount, &token.ExpiresAt, &token.RevokedAt,
			&token.LastUsedAt, &token.CreatedAt,
		); err != nil {
			return nil, err
		}
		items = append(items, token)
	}
	return items, rows.Err()
}

const revokeAgentRegistrationTokenForCreator = `-- name: RevokeAgentRegistrationTokenForCreator :execrows
UPDATE agent_registration_tokens
SET revoked_at = NOW()
WHERE id = $1
  AND creator_user_id = $2
  AND revoked_at IS NULL`

type RevokeAgentRegistrationTokenForCreatorParams struct {
	ID            uuid.UUID `db:"id" json:"id"`
	CreatorUserID uuid.UUID `db:"creator_user_id" json:"creator_user_id"`
}

func (q *Queries) RevokeAgentRegistrationTokenForCreator(ctx context.Context, arg RevokeAgentRegistrationTokenForCreatorParams) (int64, error) {
	tag, err := q.db.Exec(ctx, revokeAgentRegistrationTokenForCreator, arg.ID, arg.CreatorUserID)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

const consumeAgentRegistrationToken = `-- name: ConsumeAgentRegistrationToken :one
UPDATE agent_registration_tokens
SET used_count = used_count + 1,
    last_used_at = NOW()
WHERE id = $1
  AND revoked_at IS NULL
  AND expires_at > NOW()
  AND used_count < max_agents
RETURNING id, creator_user_id, label, prefix, token_hash, max_agents, used_count,
          expires_at, revoked_at, last_used_at, created_at`

func (q *Queries) ConsumeAgentRegistrationToken(ctx context.Context, id uuid.UUID) (AgentRegistrationToken, error) {
	row := q.db.QueryRow(ctx, consumeAgentRegistrationToken, id)
	var token AgentRegistrationToken
	err := row.Scan(
		&token.ID, &token.CreatorUserID, &token.Label, &token.Prefix, &token.TokenHash,
		&token.MaxAgents, &token.UsedCount, &token.ExpiresAt, &token.RevokedAt,
		&token.LastUsedAt, &token.CreatedAt,
	)
	return token, err
}
