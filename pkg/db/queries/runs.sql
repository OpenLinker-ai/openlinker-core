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
RETURNING id, user_id, agent_id, input, output, status, error_code, error_message,
          cost_cents, platform_fee_cents, creator_revenue_cents, duration_ms,
          started_at, finished_at, source;

-- name: MarkRunSuccess :exec
-- 调用成功：写 output, status=success, duration_ms, finished_at
UPDATE runs
SET status = 'success',
    output = $2,
    duration_ms = $3,
    finished_at = NOW()
WHERE id = $1 AND status = 'running';

-- name: MarkRunFailed :exec
-- 调用失败：写 status, error_code, error_message, duration_ms
UPDATE runs
SET status = $2,
    error_code = $3,
    error_message = $4,
    duration_ms = $5,
    finished_at = NOW()
WHERE id = $1 AND status = 'running';

-- name: CancelRun :one
-- 用户取消 running run。仅 owner 可取消，终态 run 不被覆盖。
UPDATE runs
SET status = 'canceled',
    error_code = 'CANCELED',
    error_message = $3,
    duration_ms = GREATEST(0, FLOOR(EXTRACT(EPOCH FROM (NOW() - started_at)) * 1000))::int,
    finished_at = NOW()
WHERE id = $1 AND user_id = $2 AND status = 'running'
RETURNING id, user_id, agent_id, input, output, status, error_code, error_message,
          cost_cents, platform_fee_cents, creator_revenue_cents, duration_ms,
          started_at, finished_at, source;

-- name: GetRunByID :one
SELECT id, user_id, agent_id, input, output, status, error_code, error_message,
       cost_cents, platform_fee_cents, creator_revenue_cents, duration_ms,
       started_at, finished_at, source
FROM runs
WHERE id = $1;

-- name: ClaimRuntimePullRun :one
-- Agent 通过绑定自身的访问令牌主动拉取自己名下 runtime_pull 模式的 pending run。
-- claimed_at 超过 5 分钟视为可重领，避免 Agent 崩溃后任务永久卡住。
WITH candidate AS (
    SELECT r.id
    FROM runs r
    JOIN agents a ON a.id = r.agent_id
    WHERE r.agent_id = $1
      AND r.status = 'running'
      AND a.connection_mode = 'runtime_pull'
      AND (r.claimed_at IS NULL OR r.claimed_at < NOW() - INTERVAL '5 minutes')
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

-- name: CountClaimableRuntimePullRuns :one
-- 心跳只读统计：让 runtime_pull worker 先看 pending hint，再决定是否进入 claim。
SELECT COUNT(*)::int AS total
FROM runs r
JOIN agents a ON a.id = r.agent_id
WHERE r.agent_id = $1
  AND r.status = 'running'
  AND a.connection_mode = 'runtime_pull'
  AND (r.claimed_at IS NULL OR r.claimed_at < NOW() - INTERVAL '5 minutes');

-- name: GetRuntimePullRunState :one
SELECT id, user_id, agent_id, status, cost_cents, creator_revenue_cents,
       started_at, claimed_by_runtime_token_id
FROM runs
WHERE id = $1;

-- name: ListStaleRuntimePullRuns :many
-- runtime_pull 任务如果长时间未被领取或已领取但未回传终态，需要自动收敛为 timeout，
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
  AND a.connection_mode = 'runtime_pull'
  AND (
    (r.claimed_at IS NULL AND r.started_at < $1)
    OR (r.claimed_at IS NOT NULL AND r.started_at < $2)
  )
ORDER BY r.started_at ASC
LIMIT $3
FOR UPDATE SKIP LOCKED;

-- name: CreateRunEvent :one
-- 追加 run event；锁 run 行来保证同一个 run 内 sequence 单调递增。
WITH locked_run AS (
    SELECT id FROM runs WHERE id = $1 FOR UPDATE
),
next_sequence AS (
    SELECT COALESCE(MAX(e.sequence), 0)::int + 1 AS sequence
    FROM run_events e
    JOIN locked_run r ON r.id = e.run_id
)
INSERT INTO run_events (
    run_id, parent_run_id, sequence, event_type, payload
)
SELECT
    locked_run.id, $2, next_sequence.sequence, $3, $4
FROM locked_run, next_sequence
RETURNING id, run_id, parent_run_id, sequence, event_type, payload, created_at;

-- name: ListRunEventsByRun :many
SELECT id, run_id, parent_run_id, sequence, event_type, payload, created_at
FROM run_events
WHERE run_id = $1 AND sequence > $2
ORDER BY sequence ASC
LIMIT $3;

-- ## 模块 6（双面板数据查询）
-- subagent-6a 在此区块下追加 query

-- name: ListRunsByUser :many
SELECT id, user_id, agent_id, input, output, status, error_code, error_message,
       cost_cents, platform_fee_cents, creator_revenue_cents, duration_ms,
       started_at, finished_at, source
FROM runs
WHERE user_id = $1
ORDER BY started_at DESC
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
SELECT id, user_id, agent_id, input, output, status, error_code, error_message,
       cost_cents, platform_fee_cents, creator_revenue_cents, duration_ms,
       started_at, finished_at, source
FROM runs
WHERE user_id = $1
  AND agent_id = $2
  AND ($3::bool OR (started_at, id) < ($4::timestamptz, $5::uuid))
  AND ($6::bool OR status = ANY($7::text[]))
  AND ($8::bool OR COALESCE(finished_at, started_at) >= $9::timestamptz)
  AND ($10::text = '' OR input->>'a2a_context_id' = $10 OR id::text = $10)
ORDER BY started_at DESC, id DESC
LIMIT $11;

-- name: CountRunsByUserAndAgent :one
SELECT COUNT(*)::int AS total
FROM runs
WHERE user_id = $1
  AND agent_id = $2
  AND ($3::bool OR status = ANY($4::text[]))
  AND ($5::bool OR COALESCE(finished_at, started_at) >= $6::timestamptz)
  AND ($7::text = '' OR input->>'a2a_context_id' = $7 OR id::text = $7);

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
-- 创作者总 Agent 数
SELECT COUNT(*)::int AS total FROM agents WHERE creator_id = $1;

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
