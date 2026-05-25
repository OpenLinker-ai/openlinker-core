-- a2a.sql
-- Platform-mediated Agent-to-Agent tokens, policy and run delegation queries.

-- name: CreateAgentRuntimeToken :one
INSERT INTO agent_runtime_tokens (
    agent_id, created_by_user_id, name, prefix, token_hash, scopes
) VALUES (
    $1, $2, $3, $4, $5, $6
)
RETURNING id, agent_id, created_by_user_id, name, prefix, token_hash, scopes,
          last_used_at, revoked_at, created_at;

-- name: CountActiveAgentRuntimeTokens :one
SELECT COUNT(*)::int AS total
FROM agent_runtime_tokens
WHERE agent_id = $1 AND revoked_at IS NULL;

-- name: ListAgentRuntimeTokensForOwner :many
SELECT t.id, t.agent_id, t.created_by_user_id, t.name, t.prefix, t.token_hash, t.scopes,
       t.last_used_at, t.revoked_at, t.created_at
FROM agent_runtime_tokens t
JOIN agents a ON a.id = t.agent_id
WHERE t.agent_id = $1 AND a.creator_id = $2
ORDER BY t.created_at DESC;

-- name: ListActiveAgentRuntimeTokensByPrefix :many
SELECT id, agent_id, created_by_user_id, name, prefix, token_hash, scopes,
       last_used_at, revoked_at, created_at
FROM agent_runtime_tokens
WHERE prefix = $1 AND revoked_at IS NULL;

-- name: TouchAgentRuntimeToken :exec
UPDATE agent_runtime_tokens SET last_used_at = NOW()
WHERE id = $1 AND revoked_at IS NULL;

-- name: RevokeAgentRuntimeTokenForOwner :execrows
UPDATE agent_runtime_tokens t
SET revoked_at = NOW()
FROM agents a
WHERE t.id = $1
  AND t.agent_id = a.id
  AND a.creator_id = $2
  AND t.revoked_at IS NULL;

-- name: GetAgentCallPolicy :one
SELECT COALESCE(
    (SELECT callable_by FROM agent_call_policies WHERE agent_id = $1),
    'public'
)::text AS callable_by;

-- name: UpsertAgentCallPolicyForOwner :one
INSERT INTO agent_call_policies (agent_id, callable_by)
SELECT a.id, $3
FROM agents a
WHERE a.id = $1 AND a.creator_id = $2
ON CONFLICT (agent_id) DO UPDATE
SET callable_by = EXCLUDED.callable_by,
    updated_at = NOW()
RETURNING agent_id, callable_by, updated_at;

-- name: CreateRunDelegation :one
INSERT INTO run_delegations (child_run_id, parent_run_id, caller_agent_id, reason)
VALUES ($1, $2, $3, $4)
RETURNING child_run_id, parent_run_id, caller_agent_id, reason, created_at;

-- name: GetRunDelegationByChild :one
SELECT child_run_id, parent_run_id, caller_agent_id, reason, created_at
FROM run_delegations
WHERE child_run_id = $1;

-- name: ListChildRunsByParentAndUser :many
SELECT c.id AS child_run_id, d.parent_run_id, d.caller_agent_id, d.reason,
       c.status, c.cost_cents, c.duration_ms, c.started_at, c.finished_at, c.source,
       a.id AS target_agent_id, a.slug AS target_agent_slug, a.name AS target_agent_name
FROM run_delegations d
JOIN runs p ON p.id = d.parent_run_id
JOIN runs c ON c.id = d.child_run_id
JOIN agents a ON a.id = c.agent_id
WHERE d.parent_run_id = $1 AND p.user_id = $2
ORDER BY d.created_at ASC;
