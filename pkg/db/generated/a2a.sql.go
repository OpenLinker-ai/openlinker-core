// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/a2a.sql）。

package db

import (
	"context"
	"time"

	"github.com/google/uuid"
)

const createAgentRuntimeToken = `-- name: CreateAgentRuntimeToken :one
INSERT INTO agent_tokens (
    agent_id, creator_user_id, name, prefix, token_hash, scopes, status, redeemed_at
) VALUES (
    $1, $2, $3, $4, $5, $6, 'active_runtime', NOW()
)
RETURNING id, agent_id, creator_user_id, name, prefix, token_hash, scopes,
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
FROM agent_tokens
WHERE agent_id = $1 AND status = 'active_runtime' AND revoked_at IS NULL`

func (q *Queries) CountActiveAgentRuntimeTokens(ctx context.Context, agentID uuid.UUID) (int32, error) {
	var total int32
	err := q.db.QueryRow(ctx, countActiveAgentRuntimeTokens, agentID).Scan(&total)
	return total, err
}

const listAgentRuntimeTokensForOwner = `-- name: ListAgentRuntimeTokensForOwner :many
SELECT t.id, t.agent_id, t.creator_user_id, t.name, t.prefix, t.token_hash, t.scopes,
       t.last_used_at, t.revoked_at, t.created_at
FROM agent_tokens t
JOIN agents a ON a.id = t.agent_id
WHERE t.agent_id = $1 AND a.creator_id = $2 AND t.status = 'active_runtime'
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
SELECT t.id, t.agent_id, t.creator_user_id, t.name, t.prefix, t.token_hash, t.scopes,
       t.last_used_at, t.revoked_at, t.created_at, a.connection_mode
FROM agent_tokens t
JOIN agents a ON a.id = t.agent_id
WHERE t.prefix = $1 AND t.revoked_at IS NULL AND t.status = 'active_runtime' AND t.agent_id IS NOT NULL`

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
			&token.ConnectionMode,
		); err != nil {
			return nil, err
		}
		items = append(items, token)
	}
	return items, rows.Err()
}

const touchAgentRuntimeToken = `-- name: TouchAgentRuntimeToken :exec
UPDATE agent_tokens SET last_used_at = NOW()
WHERE id = $1 AND revoked_at IS NULL`

func (q *Queries) TouchAgentRuntimeToken(ctx context.Context, id uuid.UUID) error {
	_, err := q.db.Exec(ctx, touchAgentRuntimeToken, id)
	return err
}

const hasRecentRuntimePullToken = `-- name: HasRecentRuntimePullToken :one
SELECT EXISTS(
    SELECT 1
    FROM agent_tokens
    WHERE agent_id = $1
      AND revoked_at IS NULL
      AND status = 'active_runtime'
      AND 'agent:pull' = ANY(scopes)
      AND last_used_at >= NOW() - INTERVAL '5 minutes'
)::bool AS has_recent_runtime_pull_token`

func (q *Queries) HasRecentRuntimePullToken(ctx context.Context, agentID uuid.UUID) (bool, error) {
	var ok bool
	err := q.db.QueryRow(ctx, hasRecentRuntimePullToken, agentID).Scan(&ok)
	return ok, err
}

const revokeAgentRuntimeTokenForOwner = `-- name: RevokeAgentRuntimeTokenForOwner :execrows
UPDATE agent_tokens t
SET revoked_at = NOW(),
    status = 'revoked',
    revocation_kind = 'manual'
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

const upsertA2AContextMapping = `-- name: UpsertA2AContextMapping :one
INSERT INTO a2a_context_mappings (
    run_id, user_id, agent_id, protocol_context_id, protocol_task_id,
    root_context_id, parent_context_id, parent_task_id, parent_run_id,
    caller_agent_id, target_agent_id, trace_id, reference_task_ids, source
) VALUES (
    $1, $2, $3, $4, $5,
    $6, $7, $8, $9,
    $10, $11, $12, $13, $14
)
ON CONFLICT (run_id) DO UPDATE
SET protocol_context_id = EXCLUDED.protocol_context_id,
    protocol_task_id = EXCLUDED.protocol_task_id,
    root_context_id = EXCLUDED.root_context_id,
    parent_context_id = EXCLUDED.parent_context_id,
    parent_task_id = EXCLUDED.parent_task_id,
    parent_run_id = EXCLUDED.parent_run_id,
    caller_agent_id = EXCLUDED.caller_agent_id,
    target_agent_id = EXCLUDED.target_agent_id,
    trace_id = EXCLUDED.trace_id,
    reference_task_ids = EXCLUDED.reference_task_ids,
    source = EXCLUDED.source,
    updated_at = NOW()
RETURNING id, run_id, user_id, agent_id, protocol_context_id, protocol_task_id,
          root_context_id, parent_context_id, parent_task_id, parent_run_id,
          caller_agent_id, target_agent_id, trace_id, reference_task_ids,
          source, created_at, updated_at`

type UpsertA2AContextMappingParams struct {
	RunID             uuid.UUID  `db:"run_id" json:"run_id"`
	UserID            uuid.UUID  `db:"user_id" json:"user_id"`
	AgentID           uuid.UUID  `db:"agent_id" json:"agent_id"`
	ProtocolContextID string     `db:"protocol_context_id" json:"protocol_context_id"`
	ProtocolTaskID    string     `db:"protocol_task_id" json:"protocol_task_id"`
	RootContextID     string     `db:"root_context_id" json:"root_context_id"`
	ParentContextID   string     `db:"parent_context_id" json:"parent_context_id"`
	ParentTaskID      string     `db:"parent_task_id" json:"parent_task_id"`
	ParentRunID       *uuid.UUID `db:"parent_run_id" json:"parent_run_id"`
	CallerAgentID     *uuid.UUID `db:"caller_agent_id" json:"caller_agent_id"`
	TargetAgentID     *uuid.UUID `db:"target_agent_id" json:"target_agent_id"`
	TraceID           string     `db:"trace_id" json:"trace_id"`
	ReferenceTaskIDs  []string   `db:"reference_task_ids" json:"reference_task_ids"`
	Source            string     `db:"source" json:"source"`
}

func (q *Queries) UpsertA2AContextMapping(ctx context.Context, arg UpsertA2AContextMappingParams) (A2AContextMapping, error) {
	var mapping A2AContextMapping
	err := q.db.QueryRow(ctx, upsertA2AContextMapping,
		arg.RunID, arg.UserID, arg.AgentID, arg.ProtocolContextID, arg.ProtocolTaskID,
		arg.RootContextID, arg.ParentContextID, arg.ParentTaskID, arg.ParentRunID,
		arg.CallerAgentID, arg.TargetAgentID, arg.TraceID, arg.ReferenceTaskIDs, arg.Source,
	).Scan(
		&mapping.ID, &mapping.RunID, &mapping.UserID, &mapping.AgentID,
		&mapping.ProtocolContextID, &mapping.ProtocolTaskID, &mapping.RootContextID,
		&mapping.ParentContextID, &mapping.ParentTaskID, &mapping.ParentRunID,
		&mapping.CallerAgentID, &mapping.TargetAgentID, &mapping.TraceID,
		&mapping.ReferenceTaskIDs, &mapping.Source, &mapping.CreatedAt, &mapping.UpdatedAt,
	)
	return mapping, err
}

const getA2AContextMappingByRun = `-- name: GetA2AContextMappingByRun :one
SELECT id, run_id, user_id, agent_id, protocol_context_id, protocol_task_id,
       root_context_id, parent_context_id, parent_task_id, parent_run_id,
       caller_agent_id, target_agent_id, trace_id, reference_task_ids,
       source, created_at, updated_at
FROM a2a_context_mappings
WHERE run_id = $1`

func (q *Queries) GetA2AContextMappingByRun(ctx context.Context, runID uuid.UUID) (A2AContextMapping, error) {
	var mapping A2AContextMapping
	err := q.db.QueryRow(ctx, getA2AContextMappingByRun, runID).Scan(
		&mapping.ID, &mapping.RunID, &mapping.UserID, &mapping.AgentID,
		&mapping.ProtocolContextID, &mapping.ProtocolTaskID, &mapping.RootContextID,
		&mapping.ParentContextID, &mapping.ParentTaskID, &mapping.ParentRunID,
		&mapping.CallerAgentID, &mapping.TargetAgentID, &mapping.TraceID,
		&mapping.ReferenceTaskIDs, &mapping.Source, &mapping.CreatedAt, &mapping.UpdatedAt,
	)
	return mapping, err
}

const listA2AContextMappingsByRoot = `-- name: ListA2AContextMappingsByRoot :many
SELECT id, run_id, user_id, agent_id, protocol_context_id, protocol_task_id,
       root_context_id, parent_context_id, parent_task_id, parent_run_id,
       caller_agent_id, target_agent_id, trace_id, reference_task_ids,
       source, created_at, updated_at
FROM a2a_context_mappings
WHERE user_id = $1 AND root_context_id = $2
ORDER BY created_at ASC, run_id ASC
LIMIT $3`

type ListA2AContextMappingsByRootParams struct {
	UserID        uuid.UUID `db:"user_id" json:"user_id"`
	RootContextID string    `db:"root_context_id" json:"root_context_id"`
	Limit         int32     `db:"limit" json:"limit"`
}

func (q *Queries) ListA2AContextMappingsByRoot(ctx context.Context, arg ListA2AContextMappingsByRootParams) ([]A2AContextMapping, error) {
	rows, err := q.db.Query(ctx, listA2AContextMappingsByRoot, arg.UserID, arg.RootContextID, arg.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []A2AContextMapping
	for rows.Next() {
		var mapping A2AContextMapping
		if err := rows.Scan(
			&mapping.ID, &mapping.RunID, &mapping.UserID, &mapping.AgentID,
			&mapping.ProtocolContextID, &mapping.ProtocolTaskID, &mapping.RootContextID,
			&mapping.ParentContextID, &mapping.ParentTaskID, &mapping.ParentRunID,
			&mapping.CallerAgentID, &mapping.TargetAgentID, &mapping.TraceID,
			&mapping.ReferenceTaskIDs, &mapping.Source, &mapping.CreatedAt, &mapping.UpdatedAt,
		); err != nil {
			return nil, err
		}
		items = append(items, mapping)
	}
	return items, rows.Err()
}

const listA2AContextMappingsBeforeRunByRoot = `-- name: ListA2AContextMappingsBeforeRunByRoot :many
SELECT id, run_id, user_id, agent_id, protocol_context_id, protocol_task_id,
       root_context_id, parent_context_id, parent_task_id, parent_run_id,
       caller_agent_id, target_agent_id, trace_id, reference_task_ids,
       source, created_at, updated_at
FROM (
    SELECT id, run_id, user_id, agent_id, protocol_context_id, protocol_task_id,
           root_context_id, parent_context_id, parent_task_id, parent_run_id,
           caller_agent_id, target_agent_id, trace_id, reference_task_ids,
           source, created_at, updated_at
    FROM a2a_context_mappings
    WHERE user_id = $1
      AND root_context_id = $2
      AND (created_at, run_id) < ($3, $4)
    ORDER BY created_at DESC, run_id DESC
    LIMIT $5
) history
ORDER BY created_at ASC, run_id ASC`

type ListA2AContextMappingsBeforeRunByRootParams struct {
	UserID        uuid.UUID `db:"user_id" json:"user_id"`
	RootContextID string    `db:"root_context_id" json:"root_context_id"`
	CreatedAt     time.Time `db:"created_at" json:"created_at"`
	RunID         uuid.UUID `db:"run_id" json:"run_id"`
	Limit         int32     `db:"limit" json:"limit"`
}

func (q *Queries) ListA2AContextMappingsBeforeRunByRoot(ctx context.Context, arg ListA2AContextMappingsBeforeRunByRootParams) ([]A2AContextMapping, error) {
	rows, err := q.db.Query(ctx, listA2AContextMappingsBeforeRunByRoot, arg.UserID, arg.RootContextID, arg.CreatedAt, arg.RunID, arg.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []A2AContextMapping
	for rows.Next() {
		var mapping A2AContextMapping
		if err := rows.Scan(
			&mapping.ID, &mapping.RunID, &mapping.UserID, &mapping.AgentID,
			&mapping.ProtocolContextID, &mapping.ProtocolTaskID, &mapping.RootContextID,
			&mapping.ParentContextID, &mapping.ParentTaskID, &mapping.ParentRunID,
			&mapping.CallerAgentID, &mapping.TargetAgentID, &mapping.TraceID,
			&mapping.ReferenceTaskIDs, &mapping.Source, &mapping.CreatedAt, &mapping.UpdatedAt,
		); err != nil {
			return nil, err
		}
		items = append(items, mapping)
	}
	return items, rows.Err()
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

const listDelegationLineage = `-- name: ListDelegationLineage :many
WITH RECURSIVE lineage AS (
    SELECT r.id, r.agent_id, 0 AS depth
    FROM runs r
    WHERE r.id = $1
  UNION ALL
    SELECT p.id, p.agent_id, lineage.depth + 1
    FROM lineage
    JOIN run_delegations d ON d.child_run_id = lineage.id
    JOIN runs p ON p.id = d.parent_run_id
    WHERE lineage.depth < $2
)
SELECT agent_id
FROM lineage
ORDER BY depth ASC`

type ListDelegationLineageParams struct {
	RunID    uuid.UUID `db:"run_id" json:"run_id"`
	MaxDepth int32     `db:"max_depth" json:"max_depth"`
}

func (q *Queries) ListDelegationLineage(ctx context.Context, arg ListDelegationLineageParams) ([]uuid.UUID, error) {
	rows, err := q.db.Query(ctx, listDelegationLineage, arg.RunID, arg.MaxDepth)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []uuid.UUID
	for rows.Next() {
		var agentID uuid.UUID
		if err := rows.Scan(&agentID); err != nil {
			return nil, err
		}
		items = append(items, agentID)
	}
	return items, rows.Err()
}

const countRunningDelegations = `-- name: CountRunningDelegations :one
SELECT COUNT(*)::int AS total
FROM run_delegations d
JOIN runs r ON r.id = d.child_run_id
WHERE r.status = 'running'`

func (q *Queries) CountRunningDelegations(ctx context.Context) (int32, error) {
	var total int32
	err := q.db.QueryRow(ctx, countRunningDelegations).Scan(&total)
	return total, err
}

const listChildRunsByParentAndUser = `-- name: ListChildRunsByParentAndUser :many
SELECT c.id AS child_run_id, d.parent_run_id, d.caller_agent_id, d.reason,
       c.status, c.cost_cents, c.duration_ms, c.started_at, c.finished_at, c.source,
       caller.slug AS caller_agent_slug, caller.name AS caller_agent_name,
       caller.tags AS caller_agent_tags,
       COALESCE(caller_skills.skill_ids, ARRAY[]::text[]) AS caller_skill_ids,
       COALESCE(caller_skills.skill_names, ARRAY[]::text[]) AS caller_skill_names,
       target.id AS target_agent_id, target.slug AS target_agent_slug,
       target.name AS target_agent_name, target.tags AS target_agent_tags,
       COALESCE(target_skills.skill_ids, ARRAY[]::text[]) AS target_skill_ids,
       COALESCE(target_skills.skill_names, ARRAY[]::text[]) AS target_skill_names,
       COALESCE(child_ctx.protocol_context_id, '')::text AS protocol_context_id,
       COALESCE(child_ctx.protocol_task_id, '')::text AS protocol_task_id,
       COALESCE(child_ctx.root_context_id, '')::text AS root_context_id,
       COALESCE(child_ctx.parent_context_id, '')::text AS parent_context_id,
       COALESCE(child_ctx.parent_task_id, '')::text AS parent_task_id,
       COALESCE(child_ctx.trace_id, '')::text AS trace_id,
       COALESCE(child_ctx.reference_task_ids, ARRAY[]::text[]) AS reference_task_ids,
       COALESCE(child_ctx.source, '')::text AS context_source
FROM run_delegations d
JOIN runs p ON p.id = d.parent_run_id
JOIN runs c ON c.id = d.child_run_id
JOIN agents caller ON caller.id = d.caller_agent_id
JOIN agents target ON target.id = c.agent_id
LEFT JOIN a2a_context_mappings child_ctx ON child_ctx.run_id = c.id
LEFT JOIN LATERAL (
    SELECT ARRAY_AGG(s.id ORDER BY s.category, s.sort_order, s.id)::text[] AS skill_ids,
           ARRAY_AGG(s.name ORDER BY s.category, s.sort_order, s.id)::text[] AS skill_names
    FROM agent_skills ag
    JOIN skills s ON s.id = ag.skill_id
    WHERE ag.agent_id = caller.id
) caller_skills ON TRUE
LEFT JOIN LATERAL (
    SELECT ARRAY_AGG(s.id ORDER BY s.category, s.sort_order, s.id)::text[] AS skill_ids,
           ARRAY_AGG(s.name ORDER BY s.category, s.sort_order, s.id)::text[] AS skill_names
    FROM agent_skills ag
    JOIN skills s ON s.id = ag.skill_id
    WHERE ag.agent_id = target.id
) target_skills ON TRUE
WHERE d.parent_run_id = $1 AND p.user_id = $2
ORDER BY d.created_at ASC`

type ListChildRunsByParentAndUserParams struct {
	ParentRunID uuid.UUID `db:"parent_run_id" json:"parent_run_id"`
	UserID      uuid.UUID `db:"user_id" json:"user_id"`
}

type ListChildRunsByParentAndUserRow struct {
	ChildRunID        uuid.UUID  `json:"child_run_id"`
	ParentRunID       uuid.UUID  `json:"parent_run_id"`
	CallerAgentID     uuid.UUID  `json:"caller_agent_id"`
	Reason            string     `json:"reason"`
	Status            string     `json:"status"`
	CostCents         int32      `json:"cost_cents"`
	DurationMs        *int32     `json:"duration_ms"`
	StartedAt         time.Time  `json:"started_at"`
	FinishedAt        *time.Time `json:"finished_at"`
	Source            string     `json:"source"`
	CallerAgentSlug   string     `json:"caller_agent_slug"`
	CallerAgentName   string     `json:"caller_agent_name"`
	CallerAgentTags   []string   `json:"caller_agent_tags"`
	CallerSkillIDs    []string   `json:"caller_skill_ids"`
	CallerSkillNames  []string   `json:"caller_skill_names"`
	TargetAgentID     uuid.UUID  `json:"target_agent_id"`
	TargetAgentSlug   string     `json:"target_agent_slug"`
	TargetAgentName   string     `json:"target_agent_name"`
	TargetAgentTags   []string   `json:"target_agent_tags"`
	TargetSkillIDs    []string   `json:"target_skill_ids"`
	TargetSkillNames  []string   `json:"target_skill_names"`
	ProtocolContextID string     `json:"protocol_context_id"`
	ProtocolTaskID    string     `json:"protocol_task_id"`
	RootContextID     string     `json:"root_context_id"`
	ParentContextID   string     `json:"parent_context_id"`
	ParentTaskID      string     `json:"parent_task_id"`
	TraceID           string     `json:"trace_id"`
	ReferenceTaskIDs  []string   `json:"reference_task_ids"`
	ContextSource     string     `json:"context_source"`
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
			&item.Source, &item.CallerAgentSlug, &item.CallerAgentName, &item.CallerAgentTags,
			&item.CallerSkillIDs, &item.CallerSkillNames, &item.TargetAgentID, &item.TargetAgentSlug,
			&item.TargetAgentName, &item.TargetAgentTags, &item.TargetSkillIDs, &item.TargetSkillNames,
			&item.ProtocolContextID, &item.ProtocolTaskID, &item.RootContextID, &item.ParentContextID,
			&item.ParentTaskID, &item.TraceID, &item.ReferenceTaskIDs, &item.ContextSource,
		); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

const listParentRunsWithDelegationsByUser = `-- name: ListParentRunsWithDelegationsByUser :many
WITH RECURSIVE root_parents AS (
    SELECT p.id AS parent_run_id, a.id AS caller_agent_id, a.slug AS caller_agent_slug,
           a.name AS caller_agent_name, a.tags AS caller_agent_tags,
           COALESCE(caller_skills.skill_ids, ARRAY[]::text[]) AS caller_skill_ids,
           COALESCE(caller_skills.skill_names, ARRAY[]::text[]) AS caller_skill_names,
           p.source AS parent_source, p.status, p.duration_ms, p.started_at, p.finished_at,
           COALESCE(token_stats.active_runtime_token_count, 0)::int AS active_runtime_token_count,
           token_stats.last_runtime_token_used_at,
           COALESCE(parent_ctx.protocol_context_id, '')::text AS protocol_context_id,
           COALESCE(parent_ctx.protocol_task_id, '')::text AS protocol_task_id,
           COALESCE(NULLIF(child_root.root_context_id, ''), NULLIF(parent_ctx.root_context_id, ''), p.id::text)::text AS root_context_id,
           COALESCE(NULLIF(child_root.trace_id, ''), NULLIF(parent_ctx.trace_id, ''), '')::text AS trace_id
    FROM runs p
    JOIN run_delegations d ON d.parent_run_id = p.id
    JOIN agents a ON a.id = p.agent_id
    LEFT JOIN run_delegations incoming ON incoming.child_run_id = p.id
    LEFT JOIN a2a_context_mappings parent_ctx ON parent_ctx.run_id = p.id
    LEFT JOIN LATERAL (
        SELECT root_context_id, trace_id
        FROM a2a_context_mappings
        WHERE parent_run_id = p.id
        ORDER BY created_at ASC
        LIMIT 1
    ) child_root ON TRUE
    LEFT JOIN LATERAL (
        SELECT ARRAY_AGG(s.id ORDER BY s.category, s.sort_order, s.id)::text[] AS skill_ids,
               ARRAY_AGG(s.name ORDER BY s.category, s.sort_order, s.id)::text[] AS skill_names
        FROM agent_skills ag
        JOIN skills s ON s.id = ag.skill_id
        WHERE ag.agent_id = a.id
    ) caller_skills ON TRUE
    LEFT JOIN LATERAL (
        SELECT COUNT(*)::int AS active_runtime_token_count,
               MAX(last_used_at) AS last_runtime_token_used_at
        FROM agent_tokens
        WHERE agent_id = a.id AND revoked_at IS NULL AND status = 'active_runtime'
    ) token_stats ON TRUE
    WHERE p.user_id = $1
      AND incoming.child_run_id IS NULL
    GROUP BY p.id, a.id, a.slug, a.name, a.tags, caller_skills.skill_ids,
             caller_skills.skill_names, token_stats.active_runtime_token_count,
             token_stats.last_runtime_token_used_at, p.source, p.status, p.duration_ms,
             p.started_at, p.finished_at, parent_ctx.protocol_context_id,
             parent_ctx.protocol_task_id, parent_ctx.root_context_id, parent_ctx.trace_id,
             child_root.root_context_id, child_root.trace_id
), delegation_tree AS (
    SELECT rp.parent_run_id AS root_parent_run_id, d.parent_run_id,
           d.child_run_id, c.agent_id AS target_agent_id, c.status, 1 AS depth
    FROM root_parents rp
    JOIN run_delegations d ON d.parent_run_id = rp.parent_run_id
    JOIN runs c ON c.id = d.child_run_id
  UNION ALL
    SELECT tree.root_parent_run_id, d.parent_run_id,
           d.child_run_id, c.agent_id AS target_agent_id, c.status, tree.depth + 1
    FROM delegation_tree tree
    JOIN run_delegations d ON d.parent_run_id = tree.child_run_id
    JOIN runs c ON c.id = d.child_run_id
    WHERE tree.depth < 16
)
SELECT rp.parent_run_id, rp.caller_agent_id, rp.caller_agent_slug,
       rp.caller_agent_name, rp.caller_agent_tags,
       rp.caller_skill_ids, rp.caller_skill_names,
       rp.parent_source, rp.status, rp.duration_ms, rp.started_at, rp.finished_at,
       COUNT(DISTINCT tree.child_run_id)::int AS child_count,
       (COUNT(DISTINCT tree.child_run_id) FILTER (WHERE tree.status = 'success'))::int AS successful_child_count,
       (COUNT(DISTINCT tree.child_run_id) FILTER (WHERE tree.status = 'running'))::int AS running_child_count,
       rp.active_runtime_token_count,
       rp.last_runtime_token_used_at,
       rp.protocol_context_id,
       rp.protocol_task_id,
       rp.root_context_id,
       rp.trace_id
FROM root_parents rp
LEFT JOIN delegation_tree tree ON tree.root_parent_run_id = rp.parent_run_id
WHERE (
      $2 = ''
      OR rp.parent_run_id::text ILIKE '%' || $2 || '%'
      OR rp.root_context_id ILIKE '%' || $2 || '%'
      OR rp.trace_id ILIKE '%' || $2 || '%'
      OR rp.caller_agent_slug ILIKE '%' || $2 || '%'
      OR rp.caller_agent_name ILIKE '%' || $2 || '%'
      OR EXISTS (
          SELECT 1
          FROM unnest(rp.caller_agent_tags) AS tag
          WHERE tag ILIKE '%' || $2 || '%'
      )
      OR EXISTS (
          SELECT 1
          FROM unnest(rp.caller_skill_ids || rp.caller_skill_names) AS skill
          WHERE skill ILIKE '%' || $2 || '%'
      )
      OR EXISTS (
          SELECT 1
          FROM delegation_tree t2
          JOIN agents target ON target.id = t2.target_agent_id
          WHERE t2.root_parent_run_id = rp.parent_run_id
            AND (
                t2.child_run_id::text ILIKE '%' || $2 || '%'
                OR target.slug ILIKE '%' || $2 || '%'
                OR target.name ILIKE '%' || $2 || '%'
                OR EXISTS (
                    SELECT 1
                    FROM unnest(target.tags) AS tag
                    WHERE tag ILIKE '%' || $2 || '%'
                )
                OR EXISTS (
                    SELECT 1
                    FROM agent_skills ag
                    JOIN skills s ON s.id = ag.skill_id
                    WHERE ag.agent_id = target.id
                      AND (s.id ILIKE '%' || $2 || '%' OR s.name ILIKE '%' || $2 || '%')
                )
            )
      )
  )
GROUP BY rp.parent_run_id, rp.caller_agent_id, rp.caller_agent_slug,
         rp.caller_agent_name, rp.caller_agent_tags,
         rp.caller_skill_ids, rp.caller_skill_names,
         rp.parent_source, rp.status, rp.duration_ms, rp.started_at, rp.finished_at,
         rp.active_runtime_token_count, rp.last_runtime_token_used_at,
         rp.protocol_context_id, rp.protocol_task_id, rp.root_context_id, rp.trace_id
ORDER BY rp.started_at DESC
LIMIT $3 OFFSET $4`

type ListParentRunsWithDelegationsByUserParams struct {
	UserID uuid.UUID `db:"user_id" json:"user_id"`
	Search string    `db:"search" json:"search"`
	Limit  int32     `db:"limit" json:"limit"`
	Offset int32     `db:"offset" json:"offset"`
}

type ListParentRunsWithDelegationsByUserRow struct {
	ParentRunID             uuid.UUID  `json:"parent_run_id"`
	CallerAgentID           uuid.UUID  `json:"caller_agent_id"`
	CallerAgentSlug         string     `json:"caller_agent_slug"`
	CallerAgentName         string     `json:"caller_agent_name"`
	CallerAgentTags         []string   `json:"caller_agent_tags"`
	CallerSkillIDs          []string   `json:"caller_skill_ids"`
	CallerSkillNames        []string   `json:"caller_skill_names"`
	ParentSource            string     `json:"parent_source"`
	Status                  string     `json:"status"`
	DurationMs              *int32     `json:"duration_ms"`
	StartedAt               time.Time  `json:"started_at"`
	FinishedAt              *time.Time `json:"finished_at"`
	ChildCount              int32      `json:"child_count"`
	SuccessfulChildCount    int32      `json:"successful_child_count"`
	RunningChildCount       int32      `json:"running_child_count"`
	ActiveRuntimeTokenCount int32      `json:"active_runtime_token_count"`
	LastRuntimeTokenUsedAt  *time.Time `json:"last_runtime_token_used_at"`
	ProtocolContextID       string     `json:"protocol_context_id"`
	ProtocolTaskID          string     `json:"protocol_task_id"`
	RootContextID           string     `json:"root_context_id"`
	TraceID                 string     `json:"trace_id"`
}

func (q *Queries) ListParentRunsWithDelegationsByUser(ctx context.Context, arg ListParentRunsWithDelegationsByUserParams) ([]ListParentRunsWithDelegationsByUserRow, error) {
	rows, err := q.db.Query(ctx, listParentRunsWithDelegationsByUser, arg.UserID, arg.Search, arg.Limit, arg.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []ListParentRunsWithDelegationsByUserRow
	for rows.Next() {
		var item ListParentRunsWithDelegationsByUserRow
		if err := rows.Scan(
			&item.ParentRunID, &item.CallerAgentID, &item.CallerAgentSlug, &item.CallerAgentName,
			&item.CallerAgentTags, &item.CallerSkillIDs, &item.CallerSkillNames, &item.ParentSource,
			&item.Status, &item.DurationMs, &item.StartedAt, &item.FinishedAt, &item.ChildCount,
			&item.SuccessfulChildCount, &item.RunningChildCount, &item.ActiveRuntimeTokenCount,
			&item.LastRuntimeTokenUsedAt, &item.ProtocolContextID, &item.ProtocolTaskID,
			&item.RootContextID, &item.TraceID,
		); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

const countParentRunsWithDelegationsByUser = `-- name: CountParentRunsWithDelegationsByUser :one
WITH RECURSIVE root_parents AS (
    SELECT p.id AS parent_run_id, a.slug AS caller_agent_slug,
           a.name AS caller_agent_name, a.tags AS caller_agent_tags,
           COALESCE(caller_skills.skill_ids, ARRAY[]::text[]) AS caller_skill_ids,
           COALESCE(caller_skills.skill_names, ARRAY[]::text[]) AS caller_skill_names,
           COALESCE(NULLIF(child_root.root_context_id, ''), NULLIF(parent_ctx.root_context_id, ''), p.id::text)::text AS root_context_id,
           COALESCE(NULLIF(child_root.trace_id, ''), NULLIF(parent_ctx.trace_id, ''), '')::text AS trace_id
    FROM runs p
    JOIN run_delegations d ON d.parent_run_id = p.id
    JOIN agents a ON a.id = p.agent_id
    LEFT JOIN run_delegations incoming ON incoming.child_run_id = p.id
    LEFT JOIN a2a_context_mappings parent_ctx ON parent_ctx.run_id = p.id
    LEFT JOIN LATERAL (
        SELECT root_context_id, trace_id
        FROM a2a_context_mappings
        WHERE parent_run_id = p.id
        ORDER BY created_at ASC
        LIMIT 1
    ) child_root ON TRUE
    LEFT JOIN LATERAL (
        SELECT ARRAY_AGG(s.id ORDER BY s.category, s.sort_order, s.id)::text[] AS skill_ids,
               ARRAY_AGG(s.name ORDER BY s.category, s.sort_order, s.id)::text[] AS skill_names
        FROM agent_skills ag
        JOIN skills s ON s.id = ag.skill_id
        WHERE ag.agent_id = a.id
    ) caller_skills ON TRUE
    WHERE p.user_id = $1
      AND incoming.child_run_id IS NULL
    GROUP BY p.id, a.id, a.slug, a.name, a.tags, caller_skills.skill_ids,
             caller_skills.skill_names, parent_ctx.root_context_id, parent_ctx.trace_id,
             child_root.root_context_id, child_root.trace_id
), delegation_tree AS (
    SELECT rp.parent_run_id AS root_parent_run_id, d.parent_run_id,
           d.child_run_id, c.agent_id AS target_agent_id, 1 AS depth
    FROM root_parents rp
    JOIN run_delegations d ON d.parent_run_id = rp.parent_run_id
    JOIN runs c ON c.id = d.child_run_id
  UNION ALL
    SELECT tree.root_parent_run_id, d.parent_run_id,
           d.child_run_id, c.agent_id AS target_agent_id, tree.depth + 1
    FROM delegation_tree tree
    JOIN run_delegations d ON d.parent_run_id = tree.child_run_id
    JOIN runs c ON c.id = d.child_run_id
    WHERE tree.depth < 16
)
SELECT COUNT(*)::int AS total
FROM root_parents rp
WHERE (
      $2 = ''
      OR rp.parent_run_id::text ILIKE '%' || $2 || '%'
      OR rp.root_context_id ILIKE '%' || $2 || '%'
      OR rp.trace_id ILIKE '%' || $2 || '%'
      OR rp.caller_agent_slug ILIKE '%' || $2 || '%'
      OR rp.caller_agent_name ILIKE '%' || $2 || '%'
      OR EXISTS (
          SELECT 1
          FROM unnest(rp.caller_agent_tags) AS tag
          WHERE tag ILIKE '%' || $2 || '%'
      )
      OR EXISTS (
          SELECT 1
          FROM unnest(rp.caller_skill_ids || rp.caller_skill_names) AS skill
          WHERE skill ILIKE '%' || $2 || '%'
      )
      OR EXISTS (
          SELECT 1
          FROM delegation_tree t2
          JOIN agents target ON target.id = t2.target_agent_id
          WHERE t2.root_parent_run_id = rp.parent_run_id
            AND (
                t2.child_run_id::text ILIKE '%' || $2 || '%'
                OR target.slug ILIKE '%' || $2 || '%'
                OR target.name ILIKE '%' || $2 || '%'
                OR EXISTS (
                    SELECT 1
                    FROM unnest(target.tags) AS tag
                    WHERE tag ILIKE '%' || $2 || '%'
                )
                OR EXISTS (
                    SELECT 1
                    FROM agent_skills ag
                    JOIN skills s ON s.id = ag.skill_id
                    WHERE ag.agent_id = target.id
                      AND (s.id ILIKE '%' || $2 || '%' OR s.name ILIKE '%' || $2 || '%')
                )
            )
      )
  )`

type CountParentRunsWithDelegationsByUserParams struct {
	UserID uuid.UUID `db:"user_id" json:"user_id"`
	Search string    `db:"search" json:"search"`
}

func (q *Queries) CountParentRunsWithDelegationsByUser(ctx context.Context, arg CountParentRunsWithDelegationsByUserParams) (int32, error) {
	var total int32
	err := q.db.QueryRow(ctx, countParentRunsWithDelegationsByUser, arg.UserID, arg.Search).Scan(&total)
	return total, err
}
