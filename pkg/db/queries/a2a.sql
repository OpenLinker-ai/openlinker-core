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

-- name: UpsertA2AContextMapping :one
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
          source, created_at, updated_at;

-- name: GetA2AContextMappingByRun :one
SELECT id, run_id, user_id, agent_id, protocol_context_id, protocol_task_id,
       root_context_id, parent_context_id, parent_task_id, parent_run_id,
       caller_agent_id, target_agent_id, trace_id, reference_task_ids,
       source, created_at, updated_at
FROM a2a_context_mappings
WHERE run_id = $1;

-- name: ListA2AContextMappingsByRoot :many
SELECT id, run_id, user_id, agent_id, protocol_context_id, protocol_task_id,
       root_context_id, parent_context_id, parent_task_id, parent_run_id,
       caller_agent_id, target_agent_id, trace_id, reference_task_ids,
       source, created_at, updated_at
FROM a2a_context_mappings
WHERE user_id = $1 AND root_context_id = $2
ORDER BY created_at ASC, run_id ASC
LIMIT $3;

-- name: GetRunDelegationByChild :one
SELECT child_run_id, parent_run_id, caller_agent_id, reason, created_at
FROM run_delegations
WHERE child_run_id = $1;

-- name: ListDelegationLineage :many
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
ORDER BY depth ASC;

-- name: CountRunningDelegations :one
SELECT COUNT(*)::int AS total
FROM run_delegations d
JOIN runs r ON r.id = d.child_run_id
WHERE r.status = 'running';

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
       token_stats.last_runtime_token_used_at,
       COALESCE(parent_ctx.protocol_context_id, '')::text AS protocol_context_id,
       COALESCE(parent_ctx.protocol_task_id, '')::text AS protocol_task_id,
       COALESCE(child_root.root_context_id, parent_ctx.root_context_id, '')::text AS root_context_id,
       COALESCE(child_root.trace_id, parent_ctx.trace_id, '')::text AS trace_id
FROM runs p
JOIN run_delegations d ON d.parent_run_id = p.id
JOIN runs c ON c.id = d.child_run_id
JOIN agents a ON a.id = p.agent_id
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
         p.started_at, p.finished_at, parent_ctx.protocol_context_id,
         parent_ctx.protocol_task_id, parent_ctx.root_context_id, parent_ctx.trace_id,
         child_root.root_context_id, child_root.trace_id
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
