-- runs.sql
--
-- 模块 4（调用执行）+ 模块 6（双面板）共用的 runs 表查询。
-- 文件分工约定：
--   模块 4（runtime，写）由 subagent-4a 维护：
--     CreateRun / MarkRunSuccess / MarkRunFailed / GetRunByID
--   模块 6（dashboard，读）由 subagent-6a 维护：
--     ListRunsByUser / CountRunsByUserThisMonth
--     SumSpentByUserThisMonth / SumEarningsByCreatorThisMonth
--     ListRecentRunsForCreator / GetUserUsageStats / GetCreatorStats

-- ## 模块 4（调用执行 + 计费）
-- subagent-4a 在此区块下追加 query

-- name: CreateRun :one
-- 创建 run 记录（事务内，状态 running）
INSERT INTO runs (
    user_id, agent_id, input, status,
    cost_cents, platform_fee_cents, creator_revenue_cents, source
) VALUES (
    $1, $2, $3, 'running', $4, $5, $6, $7
)
RETURNING runs.id, runs.user_id, runs.agent_id, runs.input, runs.output,
          runs.status, runs.error_code, runs.error_message, runs.cost_cents,
          runs.platform_fee_cents, runs.creator_revenue_cents, runs.duration_ms,
          runs.started_at, runs.finished_at, runs.source;

-- name: MarkRunSuccess :exec
-- 调用成功：写 output, status=success, duration_ms, finished_at
UPDATE runs
SET status = 'success',
    output = $2,
    duration_ms = $3,
    finished_at = NOW()
WHERE runs.id = $1 AND runs.status = 'running';

-- name: MarkRunFailed :exec
-- 调用失败：写 status, error_code, error_message, duration_ms
UPDATE runs
SET status = $2,
    error_code = $3,
    error_message = $4,
    duration_ms = $5,
    finished_at = NOW()
WHERE runs.id = $1 AND runs.status = 'running';

-- name: CancelRun :one
-- 用户取消 running run。仅 owner 可取消，终态 run 不被覆盖。
UPDATE runs
SET status = 'canceled',
    error_code = 'CANCELED',
    error_message = $3,
    duration_ms = GREATEST(0, FLOOR(EXTRACT(EPOCH FROM (NOW() - started_at)) * 1000))::int,
    finished_at = NOW()
WHERE runs.id = $1 AND runs.user_id = $2 AND runs.status = 'running'
RETURNING runs.id, runs.user_id, runs.agent_id, runs.input, runs.output,
          runs.status, runs.error_code, runs.error_message, runs.cost_cents,
          runs.platform_fee_cents, runs.creator_revenue_cents, runs.duration_ms,
          runs.started_at, runs.finished_at, runs.source;

-- name: GetRunByID :one
SELECT r.id, r.user_id, r.agent_id, r.input, r.output, r.status,
       r.error_code, r.error_message, r.cost_cents, r.platform_fee_cents,
       r.creator_revenue_cents, r.duration_ms, r.started_at, r.finished_at,
       r.source
FROM runs r
WHERE r.id = $1;

-- name: ClaimRuntimePullRun :one
-- Agent 通过绑定自身的访问令牌主动拉取自己名下队列型 runtime 模式的 pending run。
-- 热路径只领取未 claimed 的 run，确保可使用 idx_runs_runtime_pull_claim。
WITH candidate AS (
    SELECT r.id
    FROM runs r
    WHERE r.agent_id = $1
      AND r.status = 'running'
      AND r.claimed_at IS NULL
    ORDER BY r.started_at ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
UPDATE runs r
SET claimed_by_runtime_token_id = $2,
    claimed_at = NOW()
FROM candidate
WHERE r.id = candidate.id
RETURNING r.id, r.user_id, r.agent_id, r.input, r.output, r.status, r.error_code, r.error_message,
          r.cost_cents, r.platform_fee_cents, r.creator_revenue_cents, r.duration_ms,
          r.started_at, r.finished_at, r.source;

-- name: GetClaimedRuntimePullRunByToken :one
-- 同一个 runtime token 已领取但响应丢失时，重试 claim 应返回原 run，而不是 204。
SELECT r.id, r.user_id, r.agent_id, r.input, r.output, r.status, r.error_code, r.error_message,
       r.cost_cents, r.platform_fee_cents, r.creator_revenue_cents, r.duration_ms,
       r.started_at, r.finished_at, r.source
FROM runs r
WHERE r.agent_id = $1
  AND r.status = 'running'
  AND r.claimed_by_runtime_token_id = $2
ORDER BY r.started_at ASC
LIMIT 1;

-- name: ClaimStaleRuntimePullRun :one
-- 兜底领取超过 claim TTL 的 run，避免 Agent 崩溃后任务永久卡住。
WITH candidate AS (
    SELECT r.id
    FROM runs r
    WHERE r.agent_id = $1
      AND r.status = 'running'
      AND r.claimed_at IS NOT NULL
      AND r.claimed_at < $3
    ORDER BY r.started_at ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
UPDATE runs r
SET claimed_by_runtime_token_id = $2,
    claimed_at = NOW()
FROM candidate
WHERE r.id = candidate.id
RETURNING r.id, r.user_id, r.agent_id, r.input, r.output, r.status, r.error_code, r.error_message,
          r.cost_cents, r.platform_fee_cents, r.creator_revenue_cents, r.duration_ms,
          r.started_at, r.finished_at, r.source;

-- name: ReleaseRuntimePullRunClaim :exec
-- WebSocket assignment send can fail after the DB claim succeeds. Release that
-- claim immediately so another live runtime connection can pick it up instead
-- of waiting for the stale-claim TTL.
UPDATE runs
SET claimed_by_runtime_token_id = NULL,
    claimed_at = NULL
WHERE id = $1
  AND claimed_by_runtime_token_id = $2
  AND status = 'running';

-- name: CountClaimableRuntimePullRuns :one
-- 心跳只读统计：让队列型 runtime worker 先看 pending hint，再决定是否进入 claim。
SELECT COUNT(*)::int AS total
FROM runs r
JOIN agents a ON a.id = r.agent_id
WHERE r.agent_id = $1
  AND r.status = 'running'
  AND a.connection_mode IN ('runtime_pull', 'runtime_ws')
  AND (r.claimed_at IS NULL OR r.claimed_at < NOW() - INTERVAL '5 minutes');

-- name: GetRuntimePullRunState :one
SELECT r.id, r.user_id, r.agent_id, r.status, r.cost_cents,
       r.creator_revenue_cents, r.started_at, r.claimed_by_runtime_token_id
FROM runs r
WHERE r.id = $1;

-- name: ListStaleRuntimePullRuns :many
-- 队列型 runtime 任务如果长时间未被领取或已领取但未回传终态，需要自动收敛为 timeout，
-- 避免用户侧永久看到 running。
-- 已领取任务使用 started_at 的绝对超时窗口，避免坏客户端反复 claim 刷新 claimed_at
-- 导致同一条 run 被无限续命。
SELECT r.id, r.user_id, r.agent_id, r.cost_cents, r.started_at,
       CASE
           WHEN r.claimed_at IS NULL THEN 'RUNTIME_PULL_NOT_CLAIMED'
           ELSE 'RUNTIME_PULL_RESULT_TIMEOUT'
       END::text AS error_code,
       CASE
           WHEN r.claimed_at IS NULL THEN '任务未被 Agent runtime 领取，请确认本地进程正在心跳并使用 GET /agent-runtime/runs/claim?wait=25 拉取任务。'
           ELSE 'Agent runtime 已领取任务，但未在超时时间内回传结果。'
       END::text AS error_message
FROM runs r
JOIN agents a ON a.id = r.agent_id
WHERE r.status = 'running'
  AND a.connection_mode IN ('runtime_pull', 'runtime_ws')
  AND (
    (r.claimed_at IS NULL AND r.started_at < $1)
    OR (r.claimed_at IS NOT NULL AND r.started_at < $2)
  )
ORDER BY r.started_at ASC
LIMIT $3
FOR UPDATE SKIP LOCKED;

-- name: ListStaleEndpointRuns :many
-- direct_http / mcp_server 由 core API 进程主动调用 endpoint。若进程在创建
-- running run 后崩溃、重启或 DB 暂时不可写，普通执行协程可能来不及把 run
-- 收敛到终态；该查询只扫描非队列型 endpoint run，队列型 runtime_pull/runtime_ws
-- 仍由 ListStaleRuntimePullRuns 处理。
SELECT r.id, r.user_id, r.agent_id, r.cost_cents, r.started_at,
       COALESCE(NULLIF(a.connection_mode, ''), 'direct_http')::text AS connection_mode,
       'ENDPOINT_RUN_TIMEOUT'::text AS error_code,
       CASE COALESCE(NULLIF(a.connection_mode, ''), 'direct_http')
           WHEN 'mcp_server' THEN 'Agent MCP server 调用超过平台兜底时间，已自动标记 timeout。请确认 MCP endpoint/tool 响应时间，长任务建议改用 runtime_ws/runtime_pull。'
           ELSE 'Agent endpoint 调用超过平台兜底时间，已自动标记 timeout。请确认 endpoint 响应时间或网络连通性，长任务建议改用 runtime_ws/runtime_pull。'
       END::text AS error_message
FROM runs r
JOIN agents a ON a.id = r.agent_id
WHERE r.status = 'running'
  AND COALESCE(NULLIF(a.connection_mode, ''), 'direct_http') IN ('direct_http', 'mcp_server')
  AND r.started_at < $1
ORDER BY r.started_at ASC
LIMIT $2
FOR UPDATE SKIP LOCKED;

-- name: LockRunEventSequence :exec
-- 在事务内按 run_id 串行化事件序列分配。调用方必须先执行本查询，再用新语句执行
-- CreateRunEvent；否则高并发语句会沿用旧 MVCC 快照，仍可能读到相同 MAX(sequence)。
SELECT pg_advisory_xact_lock(hashtextextended($1::uuid::text, 0));

-- name: CreateRunEvent :one
-- 追加 run event；调用方需先在同一事务中执行 LockRunEventSequence，保证同一个
-- run 内 sequence 单调递增。advisory lock 已提供同 run 串行化，这里只验证 run
-- 存在，避免和 claim/result 对 runs 行争用 FOR UPDATE 锁。
WITH target_run AS (
    SELECT r.id
    FROM runs r
    WHERE r.id = $1::uuid
),
next_sequence AS (
    SELECT COALESCE(MAX(e.sequence), 0)::int + 1 AS sequence
    FROM run_events e
    JOIN target_run r ON r.id = e.run_id
)
INSERT INTO run_events (
    run_id, parent_run_id, sequence, event_type, payload
)
SELECT
    target_run.id, $2, next_sequence.sequence, $3, $4
FROM target_run, next_sequence
RETURNING run_events.id, run_events.run_id, run_events.parent_run_id,
          run_events.sequence, run_events.event_type, run_events.payload,
          run_events.created_at;

-- name: ListRunEventsByRun :many
SELECT e.id, e.run_id, e.parent_run_id, e.sequence, e.event_type, e.payload, e.created_at
FROM run_events e
WHERE e.run_id = $1 AND e.sequence > $2
ORDER BY e.sequence ASC
LIMIT $3;

-- ## 模块 6（双面板数据查询）
-- subagent-6a 在此区块下追加 query

-- name: ListRunsByUser :many
SELECT r.id, r.user_id, r.agent_id, r.input, r.output, r.status, r.error_code, r.error_message,
       r.cost_cents, r.platform_fee_cents, r.creator_revenue_cents, r.duration_ms,
       r.started_at, r.finished_at, r.source
FROM runs r
WHERE r.user_id = $1
ORDER BY r.started_at DESC
LIMIT $2 OFFSET $3;

-- name: ListRunsByUserWithAgent :many
-- 列表里要展示 agent name，join 上去
SELECT r.id, r.user_id, r.agent_id, r.input, r.output, r.status,
       r.error_code, r.error_message, r.cost_cents, r.platform_fee_cents,
       r.creator_revenue_cents, r.duration_ms, r.started_at, r.finished_at,
       r.source,
       a.slug AS agent_slug, a.name AS agent_name
FROM runs r
JOIN agents a ON a.id = r.agent_id
WHERE r.user_id = $1
ORDER BY r.started_at DESC
LIMIT $2 OFFSET $3;

-- name: ListRunsByUserAndAgent :many
-- A2A ListTasks: owner-scoped keyset page for one public Agent.
SELECT r.id, r.user_id, r.agent_id, r.input, r.output, r.status, r.error_code, r.error_message,
       r.cost_cents, r.platform_fee_cents, r.creator_revenue_cents, r.duration_ms,
       r.started_at, r.finished_at, r.source
FROM runs r
LEFT JOIN a2a_context_mappings ctx ON ctx.run_id = r.id
WHERE r.user_id = $1
  AND r.agent_id = $2
  AND ($3::bool OR (r.started_at, r.id) < ($4::timestamptz, $5::uuid))
  AND ($6::bool OR r.status = ANY($7::text[]))
  AND ($8::bool OR COALESCE(r.finished_at, r.started_at) >= $9::timestamptz)
  AND (
      $10::text = ''
      OR ctx.protocol_context_id = $10
      OR ctx.root_context_id = $10
      OR r.input->>'a2a_context_id' = $10
      OR r.id::text = $10
  )
ORDER BY r.started_at DESC, r.id DESC
LIMIT $11;

-- name: CountRunsByUserAndAgent :one
SELECT COUNT(*)::int AS total
FROM runs r
LEFT JOIN a2a_context_mappings ctx ON ctx.run_id = r.id
WHERE r.user_id = $1
  AND r.agent_id = $2
  AND ($3::bool OR r.status = ANY($4::text[]))
  AND ($5::bool OR COALESCE(r.finished_at, r.started_at) >= $6::timestamptz)
  AND (
      $7::text = ''
      OR ctx.protocol_context_id = $7
      OR ctx.root_context_id = $7
      OR r.input->>'a2a_context_id' = $7
      OR r.id::text = $7
  );

-- name: ListRunsByCreatorAgentWithAgent :many
-- 创作者查看某个自己 Agent 的被调用历史。
SELECT r.id, r.user_id, r.agent_id, r.input, r.output, r.status,
       r.error_code, r.error_message, r.cost_cents, r.platform_fee_cents,
       r.creator_revenue_cents, r.duration_ms, r.started_at, r.finished_at,
       r.source, r.claimed_by_runtime_token_id, r.claimed_at,
       a.slug AS agent_slug, a.name AS agent_name
FROM runs r
JOIN agents a ON a.id = r.agent_id
WHERE a.creator_id = $1 AND r.agent_id = $2
ORDER BY r.started_at DESC
LIMIT $3 OFFSET $4;

-- name: CountRunsByUser :one
SELECT COUNT(*)::int AS total FROM runs WHERE user_id = $1;

-- name: ListCallRecordsForUser :many
-- 用户视角调用记录：同一个页面展示“我调用的”和“我的 Agent 被调用的”。
-- direction:
--   made     = 当前用户作为调用方
--   received = 当前用户拥有的 Agent 被调用
--   both     = 当前用户调用了自己拥有的 Agent
SELECT r.id,
       r.user_id,
       r.agent_id,
       r.status,
       CASE WHEN r.user_id = $1 THEN r.cost_cents ELSE 0 END::int AS cost_cents,
       CASE WHEN a.creator_id = $1 THEN r.creator_revenue_cents ELSE 0 END::int AS creator_revenue_cents,
       r.duration_ms,
       r.started_at,
       r.finished_at,
       r.source,
       a.slug AS agent_slug,
       a.name AS agent_name,
       CASE
           WHEN r.user_id = $1 AND a.creator_id = $1 THEN 'both'
           WHEN r.user_id = $1 THEN 'made'
           ELSE 'received'
       END::text AS direction,
       COALESCE(d.parent_run_id::text, '')::text AS parent_run_id,
       COALESCE(d.caller_agent_id::text, '')::text AS caller_agent_id,
       COALESCE(caller.slug, '')::text AS caller_agent_slug,
       COALESCE(caller.name, '')::text AS caller_agent_name,
       COALESCE(ctx.protocol_context_id, '')::text AS protocol_context_id,
       COALESCE(ctx.protocol_task_id, '')::text AS protocol_task_id,
       COALESCE(ctx.root_context_id, '')::text AS root_context_id,
       COALESCE(ctx.parent_context_id, '')::text AS parent_context_id,
       COALESCE(ctx.parent_task_id, '')::text AS parent_task_id,
       COALESCE(ctx.trace_id, '')::text AS trace_id,
       COALESCE(ctx.reference_task_ids, ARRAY[]::text[]) AS reference_task_ids,
       COALESCE(ctx.source, '')::text AS context_source,
       COALESCE(NULLIF(ctx.protocol_task_id, ''), r.id::text)::text AS call_id,
       COALESCE(children.child_count, 0)::int AS child_count
FROM runs r
JOIN agents a ON a.id = r.agent_id
LEFT JOIN run_delegations d ON d.child_run_id = r.id
LEFT JOIN agents caller ON caller.id = d.caller_agent_id
LEFT JOIN a2a_context_mappings ctx ON ctx.run_id = r.id
LEFT JOIN LATERAL (
    SELECT COUNT(*)::int AS child_count
    FROM run_delegations cd
    WHERE cd.parent_run_id = r.id
) children ON TRUE
WHERE (
    ($2 = 'made' AND r.user_id = $1)
    OR ($2 = 'received' AND a.creator_id = $1)
    OR ($2 = 'all' AND (r.user_id = $1 OR a.creator_id = $1))
)
AND (
    $3 = ''
    OR r.id::text ILIKE '%' || $3 || '%'
    OR r.agent_id::text ILIKE '%' || $3 || '%'
    OR r.status ILIKE '%' || $3 || '%'
    OR r.source ILIKE '%' || $3 || '%'
    OR a.slug ILIKE '%' || $3 || '%'
    OR a.name ILIKE '%' || $3 || '%'
    OR COALESCE(d.parent_run_id::text, '') ILIKE '%' || $3 || '%'
    OR COALESCE(d.caller_agent_id::text, '') ILIKE '%' || $3 || '%'
    OR COALESCE(caller.slug, '') ILIKE '%' || $3 || '%'
    OR COALESCE(caller.name, '') ILIKE '%' || $3 || '%'
    OR COALESCE(ctx.protocol_context_id, '') ILIKE '%' || $3 || '%'
    OR COALESCE(ctx.protocol_task_id, '') ILIKE '%' || $3 || '%'
    OR COALESCE(ctx.root_context_id, '') ILIKE '%' || $3 || '%'
    OR COALESCE(ctx.parent_context_id, '') ILIKE '%' || $3 || '%'
    OR COALESCE(ctx.parent_task_id, '') ILIKE '%' || $3 || '%'
    OR COALESCE(ctx.trace_id, '') ILIKE '%' || $3 || '%'
    OR COALESCE(ctx.source, '') ILIKE '%' || $3 || '%'
    OR COALESCE(NULLIF(ctx.protocol_task_id, ''), r.id::text) ILIKE '%' || $3 || '%'
    OR array_to_string(COALESCE(ctx.reference_task_ids, ARRAY[]::text[]), ' ') ILIKE '%' || $3 || '%'
)
AND ($4 = '' OR r.status = $4)
AND ($5 = '' OR r.source = $5)
AND (
    $6 = ''
    OR ($6 = 'direct' AND d.parent_run_id IS NULL AND COALESCE(children.child_count, 0) = 0)
    OR ($6 = 'a2a_child' AND d.parent_run_id IS NOT NULL)
    OR ($6 = 'a2a_parent' AND d.parent_run_id IS NULL AND COALESCE(children.child_count, 0) > 0)
)
ORDER BY
    CASE WHEN $7 = 'started_asc' THEN r.started_at END ASC,
    CASE WHEN $7 = 'started_desc' THEN r.started_at END DESC,
    CASE WHEN $7 = 'amount_asc' THEN
        CASE WHEN a.creator_id = $1 AND r.user_id <> $1 THEN r.creator_revenue_cents ELSE r.cost_cents END
    END ASC,
    CASE WHEN $7 = 'amount_desc' THEN
        CASE WHEN a.creator_id = $1 AND r.user_id <> $1 THEN r.creator_revenue_cents ELSE r.cost_cents END
    END DESC,
    CASE WHEN $7 = 'duration_asc' THEN COALESCE(r.duration_ms, 2147483647) END ASC,
    CASE WHEN $7 = 'duration_desc' THEN COALESCE(r.duration_ms, -1) END DESC,
    r.started_at DESC,
    r.id DESC
LIMIT $8 OFFSET $9;

-- name: CountCallRecordsForUser :one
SELECT COUNT(*)::int AS total
FROM runs r
JOIN agents a ON a.id = r.agent_id
LEFT JOIN run_delegations d ON d.child_run_id = r.id
LEFT JOIN LATERAL (
    SELECT COUNT(*)::int AS child_count
    FROM run_delegations cd
    WHERE cd.parent_run_id = r.id
) children ON TRUE
WHERE (
    ($2 = 'made' AND r.user_id = $1)
    OR ($2 = 'received' AND a.creator_id = $1)
    OR ($2 = 'all' AND (r.user_id = $1 OR a.creator_id = $1))
)
AND (
    $3 = ''
    OR r.id::text ILIKE '%' || $3 || '%'
    OR r.agent_id::text ILIKE '%' || $3 || '%'
    OR r.status ILIKE '%' || $3 || '%'
    OR r.source ILIKE '%' || $3 || '%'
    OR a.slug ILIKE '%' || $3 || '%'
    OR a.name ILIKE '%' || $3 || '%'
    OR EXISTS (
        SELECT 1
        FROM run_delegations d
        LEFT JOIN agents caller ON caller.id = d.caller_agent_id
        WHERE d.child_run_id = r.id
          AND (
              d.parent_run_id::text ILIKE '%' || $3 || '%'
              OR d.caller_agent_id::text ILIKE '%' || $3 || '%'
              OR COALESCE(caller.slug, '') ILIKE '%' || $3 || '%'
              OR COALESCE(caller.name, '') ILIKE '%' || $3 || '%'
          )
    )
    OR EXISTS (
        SELECT 1
        FROM a2a_context_mappings ctx
        WHERE ctx.run_id = r.id
          AND (
              ctx.protocol_context_id ILIKE '%' || $3 || '%'
              OR ctx.protocol_task_id ILIKE '%' || $3 || '%'
              OR ctx.root_context_id ILIKE '%' || $3 || '%'
              OR ctx.parent_context_id ILIKE '%' || $3 || '%'
              OR ctx.parent_task_id ILIKE '%' || $3 || '%'
              OR ctx.trace_id ILIKE '%' || $3 || '%'
              OR ctx.source ILIKE '%' || $3 || '%'
              OR COALESCE(NULLIF(ctx.protocol_task_id, ''), r.id::text) ILIKE '%' || $3 || '%'
              OR array_to_string(ctx.reference_task_ids, ' ') ILIKE '%' || $3 || '%'
          )
    )
)
AND ($4 = '' OR r.status = $4)
AND ($5 = '' OR r.source = $5)
AND (
    $6 = ''
    OR ($6 = 'direct' AND d.parent_run_id IS NULL AND COALESCE(children.child_count, 0) = 0)
    OR ($6 = 'a2a_child' AND d.parent_run_id IS NOT NULL)
    OR ($6 = 'a2a_parent' AND d.parent_run_id IS NULL AND COALESCE(children.child_count, 0) > 0)
);

-- name: CountRunsByCreatorAgent :one
SELECT COUNT(*)::int AS total
FROM runs r
JOIN agents a ON a.id = r.agent_id
WHERE a.creator_id = $1 AND r.agent_id = $2;

-- name: CountRunsByUserThisMonth :one
SELECT COUNT(*)::int AS total
FROM runs
WHERE user_id = $1 AND started_at >= date_trunc('month', NOW());

-- name: SumSpentByUserThisMonth :one
SELECT COALESCE(SUM(cost_cents), 0)::bigint AS total_spent
FROM runs
WHERE user_id = $1 AND status = 'success' AND started_at >= date_trunc('month', NOW());

-- name: SumEarningsByCreatorThisMonth :one
-- 通过 agent.creator_id 关联
SELECT COALESCE(SUM(r.creator_revenue_cents), 0)::bigint AS total_earned
FROM runs r
JOIN agents a ON a.id = r.agent_id
WHERE a.creator_id = $1 AND r.status = 'success' AND r.started_at >= date_trunc('month', NOW());

-- name: CountRunsForCreatorThisMonth :one
SELECT COUNT(*)::int AS total
FROM runs r
JOIN agents a ON a.id = r.agent_id
WHERE a.creator_id = $1 AND r.status = 'success' AND r.started_at >= date_trunc('month', NOW());

-- name: ListAgentStatsForCreator :many
-- 创作者每个 Agent 的本月调用 + 收入（用于 creator dashboard）
SELECT a.id, a.slug, a.name,
       (CASE
            WHEN a.lifecycle_status = 'disabled' THEN 'disabled'
            WHEN a.certification_status = 'pending' THEN 'pending'
            WHEN a.certification_status = 'rejected' THEN 'rejected'
            ELSE 'approved'
       END)::text AS status,
       a.price_per_call_cents,
       a.total_calls AS lifetime_calls, a.total_revenue_cents AS lifetime_revenue,
       COALESCE(monthly.calls_this_month, 0) AS calls_this_month,
       COALESCE(monthly.revenue_this_month, 0) AS revenue_this_month
FROM agents a
LEFT JOIN (
    SELECT agent_id,
           COUNT(*) AS calls_this_month,
           SUM(creator_revenue_cents) AS revenue_this_month
    FROM runs
    WHERE status = 'success' AND started_at >= date_trunc('month', NOW())
    GROUP BY agent_id
) monthly ON monthly.agent_id = a.id
WHERE a.creator_id = $1
ORDER BY a.created_at DESC;

-- name: CountAgentsByCreator :one
-- 创作者当前 Agent 数（不包含已下架 disabled）
SELECT COUNT(*)::int AS total
FROM agents
WHERE creator_id = $1
  AND lifecycle_status = 'active';

-- name: CountPublicAgentsByCreator :one
-- 创作者当前公开 Agent 数（active + public）
SELECT COUNT(*)::int AS total
FROM agents
WHERE creator_id = $1
  AND lifecycle_status = 'active'
  AND visibility = 'public';

-- name: CountPendingAgentsByCreator :one
-- 创作者人工处理队列 Agent 数
SELECT COUNT(*)::int AS total
FROM agents
WHERE creator_id = $1
  AND lifecycle_status = 'active'
  AND certification_status = 'pending';

-- ## 共用 - 占位
-- name: RunsCount :one
SELECT COUNT(*)::int AS total FROM runs;
