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

-- name: HasRecentRuntimePullToken :one
SELECT EXISTS(
    SELECT 1
    FROM agent_runtime_tokens
    WHERE agent_id = $1
      AND revoked_at IS NULL
      AND 'agent:pull' = ANY(scopes)
      AND last_used_at >= NOW() - INTERVAL '5 minutes'
)::bool AS has_recent_runtime_pull_token;

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
       caller.slug AS caller_agent_slug, caller.name AS caller_agent_name,
       caller.tags AS caller_agent_tags,
       COALESCE(caller_skills.skill_ids, ARRAY[]::text[]) AS caller_skill_ids,
       COALESCE(caller_skills.skill_names, ARRAY[]::text[]) AS caller_skill_names,
       target.id AS target_agent_id, target.slug AS target_agent_slug,
       target.name AS target_agent_name, target.tags AS target_agent_tags,
       COALESCE(target_skills.skill_ids, ARRAY[]::text[]) AS target_skill_ids,
       COALESCE(target_skills.skill_names, ARRAY[]::text[]) AS target_skill_names
FROM run_delegations d
JOIN runs p ON p.id = d.parent_run_id
JOIN runs c ON c.id = d.child_run_id
JOIN agents caller ON caller.id = d.caller_agent_id
JOIN agents target ON target.id = c.agent_id
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
ORDER BY d.created_at ASC;

-- name: ListParentRunsWithDelegationsByUser :many
SELECT p.id AS parent_run_id, a.id AS caller_agent_id, a.slug AS caller_agent_slug,
       a.name AS caller_agent_name, a.tags AS caller_agent_tags,
       COALESCE(caller_skills.skill_ids, ARRAY[]::text[]) AS caller_skill_ids,
       COALESCE(caller_skills.skill_names, ARRAY[]::text[]) AS caller_skill_names,
       p.source AS parent_source, p.status, p.duration_ms, p.started_at, p.finished_at,
       COUNT(d.child_run_id)::int AS child_count,
       (COUNT(d.child_run_id) FILTER (WHERE c.status = 'success'))::int AS successful_child_count,
       (COUNT(d.child_run_id) FILTER (WHERE c.status = 'running'))::int AS running_child_count,
       COALESCE(token_stats.active_runtime_token_count, 0)::int AS active_runtime_token_count,
       token_stats.last_runtime_token_used_at
FROM runs p
JOIN run_delegations d ON d.parent_run_id = p.id
JOIN runs c ON c.id = d.child_run_id
JOIN agents a ON a.id = p.agent_id
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
    FROM agent_runtime_tokens
    WHERE agent_id = a.id AND revoked_at IS NULL
) token_stats ON TRUE
WHERE p.user_id = $1
  AND (
      $2 = ''
      OR p.id::text ILIKE '%' || $2 || '%'
      OR a.slug ILIKE '%' || $2 || '%'
      OR a.name ILIKE '%' || $2 || '%'
      OR EXISTS (
          SELECT 1
          FROM unnest(a.tags) AS tag
          WHERE tag ILIKE '%' || $2 || '%'
      )
      OR EXISTS (
          SELECT 1
          FROM agent_skills ag
          JOIN skills s ON s.id = ag.skill_id
          WHERE ag.agent_id = a.id
            AND (s.id ILIKE '%' || $2 || '%' OR s.name ILIKE '%' || $2 || '%')
      )
      OR EXISTS (
          SELECT 1
          FROM run_delegations d2
          JOIN runs c2 ON c2.id = d2.child_run_id
          JOIN agents target ON target.id = c2.agent_id
          WHERE d2.parent_run_id = p.id
            AND (
                target.slug ILIKE '%' || $2 || '%'
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
GROUP BY p.id, a.id, a.slug, a.name, a.tags, caller_skills.skill_ids,
         caller_skills.skill_names, token_stats.active_runtime_token_count,
         token_stats.last_runtime_token_used_at, p.source, p.status, p.duration_ms,
         p.started_at, p.finished_at
ORDER BY p.started_at DESC
LIMIT $3 OFFSET $4;

-- name: CountParentRunsWithDelegationsByUser :one
SELECT COUNT(DISTINCT d.parent_run_id)::int AS total
FROM run_delegations d
JOIN runs p ON p.id = d.parent_run_id
JOIN agents a ON a.id = p.agent_id
WHERE p.user_id = $1
  AND (
      $2 = ''
      OR p.id::text ILIKE '%' || $2 || '%'
      OR a.slug ILIKE '%' || $2 || '%'
      OR a.name ILIKE '%' || $2 || '%'
      OR EXISTS (
          SELECT 1
          FROM unnest(a.tags) AS tag
          WHERE tag ILIKE '%' || $2 || '%'
      )
      OR EXISTS (
          SELECT 1
          FROM agent_skills ag
          JOIN skills s ON s.id = ag.skill_id
          WHERE ag.agent_id = a.id
            AND (s.id ILIKE '%' || $2 || '%' OR s.name ILIKE '%' || $2 || '%')
      )
      OR EXISTS (
          SELECT 1
          FROM run_delegations d2
          JOIN runs c2 ON c2.id = d2.child_run_id
          JOIN agents target ON target.id = c2.agent_id
          WHERE d2.parent_run_id = p.id
            AND (
                target.slug ILIKE '%' || $2 || '%'
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
  );
