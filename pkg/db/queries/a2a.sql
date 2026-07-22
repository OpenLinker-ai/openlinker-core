-- a2a.sql
-- Platform-mediated Agent-to-Agent tokens, policy and run delegation queries.

-- name: ListActiveAgentRuntimeTokensByPrefix :many
SELECT t.id, t.agent_id, t.creator_user_id, t.name, t.prefix, t.token_hash, t.scopes,
       t.last_used_at, t.revoked_at, t.created_at, a.connection_mode
FROM agent_tokens t
JOIN agents a ON a.id = t.agent_id
WHERE t.prefix = $1
  AND t.revoked_at IS NULL
  AND t.status = 'active_runtime'
  AND t.agent_id IS NOT NULL
  AND (t.expires_at IS NULL OR t.expires_at > clock_timestamp());

-- name: TouchAgentRuntimeToken :exec
UPDATE agent_tokens SET last_used_at = clock_timestamp()
WHERE id = $1
  AND status = 'active_runtime'
  AND revoked_at IS NULL
  AND (expires_at IS NULL OR expires_at > clock_timestamp());

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

-- name: ListRecentA2AContextMappingsByRoot :many
-- Run creation calls this before inserting the current Run so its immutable
-- request metadata can include only previously committed conversation history.
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
    ORDER BY created_at DESC, run_id DESC
    LIMIT $3
) history
ORDER BY created_at ASC, run_id ASC;

-- name: ListA2AContextMappingsBeforeRunByRoot :many
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
ORDER BY created_at ASC, run_id ASC;

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
LIMIT $3 OFFSET $4;

-- name: CountParentRunsWithDelegationsByUser :one
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
  );
