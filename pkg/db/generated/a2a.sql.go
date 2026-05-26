// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/a2a.sql）。

package db

import (
	"context"
	"time"

	"github.com/google/uuid"
)

const createAgentRuntimeToken = `-- name: CreateAgentRuntimeToken :one
INSERT INTO agent_runtime_tokens (
    agent_id, created_by_user_id, name, prefix, token_hash, scopes
) VALUES (
    $1, $2, $3, $4, $5, $6
)
RETURNING id, agent_id, created_by_user_id, name, prefix, token_hash, scopes,
          last_used_at, revoked_at, created_at`

type CreateAgentRuntimeTokenParams struct {
	AgentID         uuid.UUID `db:"agent_id" json:"agent_id"`
	CreatedByUserID uuid.UUID `db:"created_by_user_id" json:"created_by_user_id"`
	Name            string    `db:"name" json:"name"`
	Prefix          string    `db:"prefix" json:"prefix"`
	TokenHash       string    `db:"token_hash" json:"-"`
	Scopes          []string  `db:"scopes" json:"scopes"`
}

func (q *Queries) CreateAgentRuntimeToken(ctx context.Context, arg CreateAgentRuntimeTokenParams) (AgentRuntimeToken, error) {
	row := q.db.QueryRow(ctx, createAgentRuntimeToken,
		arg.AgentID, arg.CreatedByUserID, arg.Name, arg.Prefix, arg.TokenHash, arg.Scopes)
	var token AgentRuntimeToken
	err := row.Scan(
		&token.ID, &token.AgentID, &token.CreatedByUserID, &token.Name, &token.Prefix,
		&token.TokenHash, &token.Scopes, &token.LastUsedAt, &token.RevokedAt, &token.CreatedAt,
	)
	return token, err
}

const countActiveAgentRuntimeTokens = `-- name: CountActiveAgentRuntimeTokens :one
SELECT COUNT(*)::int AS total
FROM agent_runtime_tokens
WHERE agent_id = $1 AND revoked_at IS NULL`

func (q *Queries) CountActiveAgentRuntimeTokens(ctx context.Context, agentID uuid.UUID) (int32, error) {
	var total int32
	err := q.db.QueryRow(ctx, countActiveAgentRuntimeTokens, agentID).Scan(&total)
	return total, err
}

const listAgentRuntimeTokensForOwner = `-- name: ListAgentRuntimeTokensForOwner :many
SELECT t.id, t.agent_id, t.created_by_user_id, t.name, t.prefix, t.token_hash, t.scopes,
       t.last_used_at, t.revoked_at, t.created_at
FROM agent_runtime_tokens t
JOIN agents a ON a.id = t.agent_id
WHERE t.agent_id = $1 AND a.creator_id = $2
ORDER BY t.created_at DESC`

type ListAgentRuntimeTokensForOwnerParams struct {
	AgentID uuid.UUID `db:"agent_id" json:"agent_id"`
	UserID  uuid.UUID `db:"user_id" json:"user_id"`
}

func (q *Queries) ListAgentRuntimeTokensForOwner(ctx context.Context, arg ListAgentRuntimeTokensForOwnerParams) ([]AgentRuntimeToken, error) {
	rows, err := q.db.Query(ctx, listAgentRuntimeTokensForOwner, arg.AgentID, arg.UserID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []AgentRuntimeToken
	for rows.Next() {
		var token AgentRuntimeToken
		if err := rows.Scan(
			&token.ID, &token.AgentID, &token.CreatedByUserID, &token.Name, &token.Prefix,
			&token.TokenHash, &token.Scopes, &token.LastUsedAt, &token.RevokedAt, &token.CreatedAt,
		); err != nil {
			return nil, err
		}
		items = append(items, token)
	}
	return items, rows.Err()
}

const listActiveAgentRuntimeTokensByPrefix = `-- name: ListActiveAgentRuntimeTokensByPrefix :many
SELECT id, agent_id, created_by_user_id, name, prefix, token_hash, scopes,
       last_used_at, revoked_at, created_at
FROM agent_runtime_tokens
WHERE prefix = $1 AND revoked_at IS NULL`

func (q *Queries) ListActiveAgentRuntimeTokensByPrefix(ctx context.Context, prefix string) ([]AgentRuntimeToken, error) {
	rows, err := q.db.Query(ctx, listActiveAgentRuntimeTokensByPrefix, prefix)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []AgentRuntimeToken
	for rows.Next() {
		var token AgentRuntimeToken
		if err := rows.Scan(
			&token.ID, &token.AgentID, &token.CreatedByUserID, &token.Name, &token.Prefix,
			&token.TokenHash, &token.Scopes, &token.LastUsedAt, &token.RevokedAt, &token.CreatedAt,
		); err != nil {
			return nil, err
		}
		items = append(items, token)
	}
	return items, rows.Err()
}

const touchAgentRuntimeToken = `-- name: TouchAgentRuntimeToken :exec
UPDATE agent_runtime_tokens SET last_used_at = NOW()
WHERE id = $1 AND revoked_at IS NULL`

func (q *Queries) TouchAgentRuntimeToken(ctx context.Context, id uuid.UUID) error {
	_, err := q.db.Exec(ctx, touchAgentRuntimeToken, id)
	return err
}

const revokeAgentRuntimeTokenForOwner = `-- name: RevokeAgentRuntimeTokenForOwner :execrows
UPDATE agent_runtime_tokens t
SET revoked_at = NOW()
FROM agents a
WHERE t.id = $1
  AND t.agent_id = a.id
  AND a.creator_id = $2
  AND t.revoked_at IS NULL`

type RevokeAgentRuntimeTokenForOwnerParams struct {
	ID     uuid.UUID `db:"id" json:"id"`
	UserID uuid.UUID `db:"user_id" json:"user_id"`
}

func (q *Queries) RevokeAgentRuntimeTokenForOwner(ctx context.Context, arg RevokeAgentRuntimeTokenForOwnerParams) (int64, error) {
	tag, err := q.db.Exec(ctx, revokeAgentRuntimeTokenForOwner, arg.ID, arg.UserID)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

const getAgentCallPolicy = `-- name: GetAgentCallPolicy :one
SELECT COALESCE(
    (SELECT callable_by FROM agent_call_policies WHERE agent_id = $1),
    'public'
)::text AS callable_by`

func (q *Queries) GetAgentCallPolicy(ctx context.Context, agentID uuid.UUID) (string, error) {
	var policy string
	err := q.db.QueryRow(ctx, getAgentCallPolicy, agentID).Scan(&policy)
	return policy, err
}

const upsertAgentCallPolicyForOwner = `-- name: UpsertAgentCallPolicyForOwner :one
INSERT INTO agent_call_policies (agent_id, callable_by)
SELECT a.id, $3
FROM agents a
WHERE a.id = $1 AND a.creator_id = $2
ON CONFLICT (agent_id) DO UPDATE
SET callable_by = EXCLUDED.callable_by,
    updated_at = NOW()
RETURNING agent_id, callable_by, updated_at`

type UpsertAgentCallPolicyForOwnerParams struct {
	AgentID    uuid.UUID `db:"agent_id" json:"agent_id"`
	UserID     uuid.UUID `db:"user_id" json:"user_id"`
	CallableBy string    `db:"callable_by" json:"callable_by"`
}

func (q *Queries) UpsertAgentCallPolicyForOwner(ctx context.Context, arg UpsertAgentCallPolicyForOwnerParams) (AgentCallPolicy, error) {
	var policy AgentCallPolicy
	err := q.db.QueryRow(ctx, upsertAgentCallPolicyForOwner, arg.AgentID, arg.UserID, arg.CallableBy).
		Scan(&policy.AgentID, &policy.CallableBy, &policy.UpdatedAt)
	return policy, err
}

const createRunDelegation = `-- name: CreateRunDelegation :one
INSERT INTO run_delegations (child_run_id, parent_run_id, caller_agent_id, reason)
VALUES ($1, $2, $3, $4)
RETURNING child_run_id, parent_run_id, caller_agent_id, reason, created_at`

type CreateRunDelegationParams struct {
	ChildRunID    uuid.UUID `db:"child_run_id" json:"child_run_id"`
	ParentRunID   uuid.UUID `db:"parent_run_id" json:"parent_run_id"`
	CallerAgentID uuid.UUID `db:"caller_agent_id" json:"caller_agent_id"`
	Reason        string    `db:"reason" json:"reason"`
}

func (q *Queries) CreateRunDelegation(ctx context.Context, arg CreateRunDelegationParams) (RunDelegation, error) {
	var delegation RunDelegation
	err := q.db.QueryRow(ctx, createRunDelegation,
		arg.ChildRunID, arg.ParentRunID, arg.CallerAgentID, arg.Reason).
		Scan(&delegation.ChildRunID, &delegation.ParentRunID, &delegation.CallerAgentID,
			&delegation.Reason, &delegation.CreatedAt)
	return delegation, err
}

const getRunDelegationByChild = `-- name: GetRunDelegationByChild :one
SELECT child_run_id, parent_run_id, caller_agent_id, reason, created_at
FROM run_delegations
WHERE child_run_id = $1`

func (q *Queries) GetRunDelegationByChild(ctx context.Context, childRunID uuid.UUID) (RunDelegation, error) {
	var delegation RunDelegation
	err := q.db.QueryRow(ctx, getRunDelegationByChild, childRunID).
		Scan(&delegation.ChildRunID, &delegation.ParentRunID, &delegation.CallerAgentID,
			&delegation.Reason, &delegation.CreatedAt)
	return delegation, err
}

const listChildRunsByParentAndUser = `-- name: ListChildRunsByParentAndUser :many
SELECT c.id AS child_run_id, d.parent_run_id, d.caller_agent_id, d.reason,
       c.status, c.cost_cents, c.duration_ms, c.started_at, c.finished_at, c.source,
       a.id AS target_agent_id, a.slug AS target_agent_slug, a.name AS target_agent_name
FROM run_delegations d
JOIN runs p ON p.id = d.parent_run_id
JOIN runs c ON c.id = d.child_run_id
JOIN agents a ON a.id = c.agent_id
WHERE d.parent_run_id = $1 AND p.user_id = $2
ORDER BY d.created_at ASC`

type ListChildRunsByParentAndUserParams struct {
	ParentRunID uuid.UUID `db:"parent_run_id" json:"parent_run_id"`
	UserID      uuid.UUID `db:"user_id" json:"user_id"`
}

type ListChildRunsByParentAndUserRow struct {
	ChildRunID      uuid.UUID  `json:"child_run_id"`
	ParentRunID     uuid.UUID  `json:"parent_run_id"`
	CallerAgentID   uuid.UUID  `json:"caller_agent_id"`
	Reason          string     `json:"reason"`
	Status          string     `json:"status"`
	CostCents       int32      `json:"cost_cents"`
	DurationMs      *int32     `json:"duration_ms"`
	StartedAt       time.Time  `json:"started_at"`
	FinishedAt      *time.Time `json:"finished_at"`
	Source          string     `json:"source"`
	TargetAgentID   uuid.UUID  `json:"target_agent_id"`
	TargetAgentSlug string     `json:"target_agent_slug"`
	TargetAgentName string     `json:"target_agent_name"`
}

func (q *Queries) ListChildRunsByParentAndUser(ctx context.Context, arg ListChildRunsByParentAndUserParams) ([]ListChildRunsByParentAndUserRow, error) {
	rows, err := q.db.Query(ctx, listChildRunsByParentAndUser, arg.ParentRunID, arg.UserID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []ListChildRunsByParentAndUserRow
	for rows.Next() {
		var item ListChildRunsByParentAndUserRow
		if err := rows.Scan(
			&item.ChildRunID, &item.ParentRunID, &item.CallerAgentID, &item.Reason,
			&item.Status, &item.CostCents, &item.DurationMs, &item.StartedAt, &item.FinishedAt,
			&item.Source, &item.TargetAgentID, &item.TargetAgentSlug, &item.TargetAgentName,
		); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

const listParentRunsWithDelegationsByUser = `-- name: ListParentRunsWithDelegationsByUser :many
SELECT p.id AS parent_run_id, a.id AS caller_agent_id, a.slug AS caller_agent_slug,
       a.name AS caller_agent_name, p.status, p.duration_ms, p.started_at, p.finished_at,
       COUNT(d.child_run_id)::int AS child_count,
       (COUNT(d.child_run_id) FILTER (WHERE c.status = 'success'))::int AS successful_child_count,
       (COUNT(d.child_run_id) FILTER (WHERE c.status = 'running'))::int AS running_child_count
FROM runs p
JOIN run_delegations d ON d.parent_run_id = p.id
JOIN runs c ON c.id = d.child_run_id
JOIN agents a ON a.id = p.agent_id
WHERE p.user_id = $1
GROUP BY p.id, a.id, a.slug, a.name, p.status, p.duration_ms, p.started_at, p.finished_at
ORDER BY p.started_at DESC
LIMIT $2 OFFSET $3`

type ListParentRunsWithDelegationsByUserParams struct {
	UserID uuid.UUID `db:"user_id" json:"user_id"`
	Limit  int32     `db:"limit" json:"limit"`
	Offset int32     `db:"offset" json:"offset"`
}

type ListParentRunsWithDelegationsByUserRow struct {
	ParentRunID          uuid.UUID  `json:"parent_run_id"`
	CallerAgentID        uuid.UUID  `json:"caller_agent_id"`
	CallerAgentSlug      string     `json:"caller_agent_slug"`
	CallerAgentName      string     `json:"caller_agent_name"`
	Status               string     `json:"status"`
	DurationMs           *int32     `json:"duration_ms"`
	StartedAt            time.Time  `json:"started_at"`
	FinishedAt           *time.Time `json:"finished_at"`
	ChildCount           int32      `json:"child_count"`
	SuccessfulChildCount int32      `json:"successful_child_count"`
	RunningChildCount    int32      `json:"running_child_count"`
}

func (q *Queries) ListParentRunsWithDelegationsByUser(ctx context.Context, arg ListParentRunsWithDelegationsByUserParams) ([]ListParentRunsWithDelegationsByUserRow, error) {
	rows, err := q.db.Query(ctx, listParentRunsWithDelegationsByUser, arg.UserID, arg.Limit, arg.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []ListParentRunsWithDelegationsByUserRow
	for rows.Next() {
		var item ListParentRunsWithDelegationsByUserRow
		if err := rows.Scan(
			&item.ParentRunID, &item.CallerAgentID, &item.CallerAgentSlug, &item.CallerAgentName,
			&item.Status, &item.DurationMs, &item.StartedAt, &item.FinishedAt, &item.ChildCount,
			&item.SuccessfulChildCount, &item.RunningChildCount,
		); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

const countParentRunsWithDelegationsByUser = `-- name: CountParentRunsWithDelegationsByUser :one
SELECT COUNT(DISTINCT d.parent_run_id)::int AS total
FROM run_delegations d
JOIN runs p ON p.id = d.parent_run_id
WHERE p.user_id = $1`

func (q *Queries) CountParentRunsWithDelegationsByUser(ctx context.Context, userID uuid.UUID) (int32, error) {
	var total int32
	err := q.db.QueryRow(ctx, countParentRunsWithDelegationsByUser, userID).Scan(&total)
	return total, err
}
