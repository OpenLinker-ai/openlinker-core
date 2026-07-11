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
    id, user_id, agent_id, input, status,
    cost_cents, platform_fee_cents, creator_revenue_cents, source,
    idempotency_key_hash, idempotency_fingerprint, request_metadata,
    connection_mode_snapshot, endpoint_idempotency_snapshot,
    max_offer_count, max_attempts, dispatch_deadline_at, run_deadline_at
) VALUES (
    $1, $2, $3, $4, 'running', $5, $6, $7, $8,
    $9, $10, $11, $12, $13, $14, $15,
    clock_timestamp() + ($16::bigint * INTERVAL '1 millisecond'),
    clock_timestamp() + ($17::bigint * INTERVAL '1 millisecond')
)
ON CONFLICT (user_id, idempotency_key_hash)
    WHERE idempotency_key_hash IS NOT NULL
    DO NOTHING
RETURNING runs.id, runs.user_id, runs.agent_id, runs.input, runs.output,
          runs.status, runs.error_code, runs.error_message, runs.cost_cents,
          runs.platform_fee_cents, runs.creator_revenue_cents, runs.duration_ms,
          runs.started_at, runs.finished_at, runs.source;

-- name: GetRunIdempotencyRecord :one
SELECT id, idempotency_fingerprint
FROM runs
WHERE user_id = $1
  AND idempotency_key_hash = $2
  AND runtime_contract_id = 'openlinker.runtime.v2';

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

-- name: GetRunRuntimeContractID :one
SELECT runtime_contract_id
FROM runs
WHERE id = $1;

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
  AND r.runtime_contract_id <> 'openlinker.runtime.v2'
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

-- name: LockRunForResultFinalization :one
-- Result 事务的首把锁。所有 deadline 判断使用同一行返回的数据库时钟，
-- 后续固定按 Run -> Attempt -> Event advisory lock 顺序取锁。
SELECT r.id, r.user_id, r.agent_id, r.status, r.dispatch_state,
       r.runtime_contract_id, r.connection_mode_snapshot,
       r.endpoint_idempotency_snapshot, r.output, r.error_code,
       r.error_message, r.attempt_count, r.max_attempts,
       r.latest_attempt_id, r.active_attempt_id, r.lease_id,
       r.fencing_token, r.runtime_node_id, r.runtime_worker_id,
       r.runtime_session_id, r.lease_token_id, r.run_deadline_at,
       r.next_attempt_at, r.result_id, r.result_fingerprint,
       r.terminal_event_id, r.dead_lettered_at, r.cancel_request_id,
       r.cancel_state, r.creator_revenue_cents, r.started_at,
       r.finished_at, clock_timestamp() AS database_now
FROM runs r
WHERE r.id = $1
  AND r.runtime_contract_id = 'openlinker.runtime.v2'
FOR UPDATE;

-- name: TransitionRunToRetryWait :one
-- Attempt 必须已在同一事务中保存 retryable Result。重试时间只接受延迟，
-- 由数据库时钟一次性物化，Result 重放不得重新计算。
UPDATE runs r
SET status = 'running',
    dispatch_state = 'retry_wait',
    next_attempt_at = clock_timestamp() + ($3::bigint * INTERVAL '1 millisecond'),
    active_attempt_id = NULL,
    lease_id = NULL,
    executor_type = NULL,
    active_core_instance_id = NULL,
    runtime_node_id = NULL,
    runtime_worker_id = NULL,
    runtime_session_id = NULL,
    lease_token_id = NULL,
    lease_offered_at = NULL,
    lease_accepted_at = NULL,
    lease_expires_at = NULL,
    attempt_deadline_at = NULL,
    error_code = NULL,
    error_message = NULL
FROM run_attempts a
WHERE r.id = $1
  AND r.status = 'running'
  AND r.dispatch_state = 'executing'
  AND r.active_attempt_id = $2
  AND a.run_id = r.id
  AND a.id = $2
  AND a.finished_at IS NOT NULL
  AND a.outcome = 'retryable_failure'
  AND a.result_id IS NOT NULL
  AND a.result_acknowledged_at IS NOT NULL
  AND r.attempt_count < r.max_attempts
  AND $3::bigint >= 0
RETURNING r.id, r.status, r.dispatch_state, r.next_attempt_at,
          r.active_attempt_id, r.result_id, r.terminal_event_id,
          r.finished_at;

-- name: FinalizeRunFromResult :one
-- Attempt、terminal Event、ledger、DLQ/effects 必须由调用方在同一事务内写入。
-- timeout 分支仅在 Attempt 保存 Result tuple，Run 不公开 result/output。
UPDATE runs r
SET status = $3,
    dispatch_state = $4,
    output = $5::jsonb,
    error_code = $6,
    error_message = $7,
    duration_ms = $10,
    finished_at = clock_timestamp(),
    next_attempt_at = NULL,
    active_attempt_id = NULL,
    lease_id = NULL,
    executor_type = NULL,
    active_core_instance_id = NULL,
    runtime_node_id = NULL,
    runtime_worker_id = NULL,
    runtime_session_id = NULL,
    lease_token_id = NULL,
    lease_offered_at = NULL,
    lease_accepted_at = NULL,
    lease_expires_at = NULL,
    attempt_deadline_at = NULL,
    result_id = $8,
    result_fingerprint = $9,
    terminal_event_id = $11,
    dead_lettered_at = CASE
        WHEN $4::text = 'dead_letter' THEN clock_timestamp()
        ELSE NULL
    END
FROM run_attempts a
WHERE r.id = $1
  AND r.runtime_contract_id = 'openlinker.runtime.v2'
  AND r.status = 'running'
  AND r.dispatch_state = 'executing'
  AND r.active_attempt_id = $2
  AND a.run_id = r.id
  AND a.id = $2
  AND a.finished_at IS NOT NULL
  AND a.result_id IS NOT NULL
  AND a.result_acknowledged_at IS NOT NULL
  AND $10::int >= 0
  AND (
      (
          $3::text = 'success'
          AND $4::text = 'terminal'
          AND a.outcome = 'success'
          AND $5::jsonb IS NOT NULL
          AND $6::text IS NULL
          AND $8::uuid IS NOT DISTINCT FROM a.result_id
          AND $9::bytea IS NOT DISTINCT FROM a.result_fingerprint
      )
      OR (
          $3::text = 'failed'
          AND $4::text = 'terminal'
          AND a.outcome = 'non_retryable_failure'
          AND $5::jsonb IS NULL
          AND $6::text IS NOT NULL
          AND $8::uuid IS NOT DISTINCT FROM a.result_id
          AND $9::bytea IS NOT DISTINCT FROM a.result_fingerprint
      )
      OR (
          $3::text = 'failed'
          AND $4::text = 'dead_letter'
          AND a.outcome = 'retryable_failure'
          AND $5::jsonb IS NULL
          AND $6::text = 'RUNTIME_RETRY_EXHAUSTED'
          AND $8::uuid IS NOT DISTINCT FROM a.result_id
          AND $9::bytea IS NOT DISTINCT FROM a.result_fingerprint
      )
      OR (
          $3::text = 'timeout'
          AND $4::text = 'terminal'
          AND a.outcome = 'timeout'
          AND $5::jsonb IS NULL
          AND $6::text IS NOT NULL
          AND $8::uuid IS NULL
          AND $9::bytea IS NULL
      )
  )
RETURNING r.id, r.status, r.dispatch_state, r.output, r.error_code,
          r.error_message, r.duration_ms, r.finished_at, r.next_attempt_at,
          r.result_id, r.result_fingerprint, r.terminal_event_id,
          r.dead_lettered_at;

-- name: LockRunEventSequence :exec
-- 在事务内按 run_id 串行化事件序列分配。调用方必须先锁定 Run 行，再执行本查询；
-- 否则会与 Event INSERT 的 Run FK 或其他 Event writer 形成逆序锁依赖。
SELECT pg_advisory_xact_lock(hashtextextended($1::uuid::text, 0));

-- name: InsertTerminalRunEvent :one
-- 调用方必须已经按 Run row -> advisory lock 顺序持锁。Event ID 由 Core
-- 根据 Run ID 和公开终态确定性派生，不得由数据库随机生成。
WITH target_run AS (
    SELECT r.id
    FROM runs r
    WHERE r.id = $2::uuid
      AND r.runtime_contract_id = 'openlinker.runtime.v2'
),
next_sequence AS (
    SELECT COALESCE(MAX(e.sequence), 0)::int + 1 AS sequence
    FROM run_events e
    JOIN target_run r ON r.id = e.run_id
)
INSERT INTO run_events (
    id, run_id, parent_run_id, sequence, event_type, payload
)
SELECT $1, target_run.id, $3, next_sequence.sequence, $4, $5
FROM target_run, next_sequence
RETURNING run_events.id, run_events.run_id, run_events.parent_run_id,
          run_events.sequence, run_events.event_type, run_events.payload,
          run_events.created_at, run_events.client_event_id,
          run_events.client_event_seq, run_events.payload_fingerprint,
          run_events.attempt_id, run_events.attempt_no,
          run_events.fencing_token;

-- name: LockRunForSystemEventAppend :one
-- system Event 不需要 runtime Attempt 摘要，但必须与所有写入路径统一采用
-- Run row -> advisory lock 的顺序。
SELECT r.id
FROM runs r
WHERE r.id = $1
FOR UPDATE;

-- name: LockRunForEventAppend :one
-- 追加客户端 Event 前先锁定 v2 Run 摘要，完成 replay/new 校验后，再执行
-- LockRunEventSequence。所有 Event writer 固定采用 Run row -> advisory lock 顺序。
SELECT r.id, r.agent_id, r.status, r.runtime_contract_id, r.dispatch_state,
       r.active_attempt_id, r.lease_id, r.fencing_token, r.executor_type,
       r.runtime_node_id, r.runtime_worker_id, r.runtime_session_id,
       r.lease_token_id, r.lease_accepted_at, r.lease_expires_at,
       r.attempt_deadline_at, r.run_deadline_at,
       clock_timestamp() AS database_now
FROM runs r
WHERE r.id = $1
  AND r.runtime_contract_id = 'openlinker.runtime.v2'
FOR UPDATE;

-- name: CreateRunEvent :one
-- 追加 system Event；generated wrapper 会在同一事务中先执行
-- LockRunForSystemEventAppend，再执行 LockRunEventSequence，保证所有 Event writer
-- 统一采用 Run row -> advisory lock 顺序并单调分配全局 sequence。
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
          run_events.created_at, run_events.client_event_id,
          run_events.client_event_seq, run_events.payload_fingerprint,
          run_events.attempt_id, run_events.attempt_no,
          run_events.fencing_token;

-- name: CreateRunEffectParentEvent :one
-- Caller holds the parent Run row lock and Event advisory lock. Effect ID is
-- the durable business identity; a retry may return the original Event only
-- when every immutable field still matches.
WITH target_run AS (
    SELECT r.id
    FROM runs r
    WHERE r.id = $2::uuid
),
next_sequence AS (
    SELECT COALESCE(MAX(e.sequence), 0)::int + 1 AS sequence
    FROM run_events e
    JOIN target_run r ON r.id = e.run_id
)
INSERT INTO run_events (
    id, run_id, parent_run_id, sequence, event_type, payload
)
SELECT
    $1, target_run.id, NULL, next_sequence.sequence, 'run.child.completed', $3
FROM target_run, next_sequence
ON CONFLICT (id) DO NOTHING
RETURNING run_events.id, run_events.run_id, run_events.parent_run_id,
          run_events.sequence, run_events.event_type, run_events.payload,
          run_events.created_at, run_events.client_event_id,
          run_events.client_event_seq, run_events.payload_fingerprint,
          run_events.attempt_id, run_events.attempt_no,
          run_events.fencing_token;

-- name: GetMatchingRunEffectParentEvent :one
SELECT id, run_id, parent_run_id, sequence, event_type, payload, created_at,
       client_event_id, client_event_seq, payload_fingerprint,
       attempt_id, attempt_no, fencing_token
FROM run_events
WHERE id = $1
  AND run_id = $2
  AND parent_run_id IS NULL
  AND event_type = 'run.child.completed'
  AND payload = $3;

-- name: GetRunEventByClientID :one
SELECT e.id, e.run_id, e.parent_run_id, e.sequence, e.event_type, e.payload,
       e.created_at, e.client_event_id, e.client_event_seq,
       e.payload_fingerprint, e.attempt_id, e.attempt_no, e.fencing_token
FROM run_events e
WHERE e.run_id = $1
  AND e.client_event_id = $2;

-- name: GetRunEventByAttemptSequence :one
SELECT e.id, e.run_id, e.parent_run_id, e.sequence, e.event_type, e.payload,
       e.created_at, e.client_event_id, e.client_event_seq,
       e.payload_fingerprint, e.attempt_id, e.attempt_no, e.fencing_token
FROM run_events e
WHERE e.run_id = $1
  AND e.attempt_id = $2
  AND e.attempt_no = $3
  AND e.client_event_seq = $4;

-- name: CreateRuntimeRunEvent :one
-- 调用方必须已在同一事务中先执行 LockRunForEventAppend，再执行
-- LockRunEventSequence。该语句保存完整客户端/Attempt identity，并只分配
-- Core 全局 sequence；客户端 sequence 始终由 runtime 从 1 开始稳定提供。
WITH target_run AS (
    SELECT r.id
    FROM runs r
    WHERE r.id = $1::uuid
      AND r.runtime_contract_id = 'openlinker.runtime.v2'
),
next_sequence AS (
    SELECT COALESCE(MAX(e.sequence), 0)::int + 1 AS sequence
    FROM run_events e
    JOIN target_run r ON r.id = e.run_id
)
INSERT INTO run_events (
    run_id, parent_run_id, sequence, event_type, payload,
    client_event_id, client_event_seq, payload_fingerprint,
    attempt_id, attempt_no, fencing_token
)
SELECT target_run.id, $2, next_sequence.sequence, $3, $4,
       $5, $6, $7, $8, $9, $10
FROM target_run, next_sequence
RETURNING run_events.id, run_events.run_id, run_events.parent_run_id,
          run_events.sequence, run_events.event_type, run_events.payload,
          run_events.created_at, run_events.client_event_id,
          run_events.client_event_seq, run_events.payload_fingerprint,
          run_events.attempt_id, run_events.attempt_no,
          run_events.fencing_token;

-- name: GetRunEventRetentionWatermark :one
-- 没有 retention 行时返回逻辑水位 0；updated_at 为 NULL，调用方无需把
-- “尚未 retention”误判为真实 evidence row。
SELECT requested.run_id,
       COALESCE(w.retained_through_sequence, 0)::int AS retained_through_sequence,
       w.updated_at
FROM (VALUES ($1::uuid)) AS requested(run_id)
LEFT JOIN run_event_retention_watermarks w ON w.run_id = requested.run_id;

-- name: UpsertRetentionWatermark :one
-- 水位只前进；migration trigger 还会校验它不超过当前 MAX(sequence)，并强制
-- updated_at 使用数据库时钟。advisory lock 与 Event append 共享同一锁域。
WITH target_run AS MATERIALIZED (
    SELECT r.id
    FROM runs r
    WHERE r.id = $1
    FOR UPDATE
),
event_lock AS MATERIALIZED (
    SELECT pg_advisory_xact_lock(hashtextextended(target_run.id::text, 0))
    FROM target_run
)
INSERT INTO run_event_retention_watermarks (
    run_id, retained_through_sequence
)
SELECT $1, $2
FROM event_lock
ON CONFLICT (run_id) DO UPDATE
SET retained_through_sequence = GREATEST(
        run_event_retention_watermarks.retained_through_sequence,
        EXCLUDED.retained_through_sequence
    )
RETURNING run_id, retained_through_sequence, updated_at;

-- name: ListRunEvents :many
SELECT e.id, e.run_id, e.parent_run_id, e.sequence, e.event_type, e.payload,
       e.created_at, e.client_event_id, e.client_event_seq,
       e.payload_fingerprint, e.attempt_id, e.attempt_no, e.fencing_token
FROM run_events e
LEFT JOIN run_event_retention_watermarks w ON w.run_id = e.run_id
WHERE e.run_id = $1
  AND e.sequence > GREATEST($2, COALESCE(w.retained_through_sequence, 0))
ORDER BY e.sequence ASC
LIMIT $3;

-- name: ListRunEventsByRun :many
SELECT e.id, e.run_id, e.parent_run_id, e.sequence, e.event_type, e.payload,
       e.created_at, e.client_event_id, e.client_event_seq,
       e.payload_fingerprint, e.attempt_id, e.attempt_no, e.fencing_token
FROM run_events e
LEFT JOIN run_event_retention_watermarks w ON w.run_id = e.run_id
WHERE e.run_id = $1
  AND e.sequence > GREATEST($2, COALESCE(w.retained_through_sequence, 0))
ORDER BY e.sequence ASC
LIMIT $3;

-- name: GetRunEventBounds :one
SELECT COALESCE(w.retained_through_sequence, 0)::int AS retained_through_sequence,
       MIN(e.sequence) FILTER (
           WHERE e.sequence > COALESCE(w.retained_through_sequence, 0)
       )::int AS first_available_sequence,
       COALESCE(MAX(e.sequence), 0)::int AS last_sequence
FROM (VALUES ($1::uuid)) AS requested(run_id)
LEFT JOIN run_event_retention_watermarks w ON w.run_id = requested.run_id
LEFT JOIN run_events e ON e.run_id = requested.run_id
GROUP BY requested.run_id, w.retained_through_sequence;

-- name: ListClientEventSequencesThrough :many
SELECT e.client_event_seq
FROM run_events e
WHERE e.run_id = $1
  AND e.attempt_id = $2
  AND e.attempt_no = $3
  AND e.client_event_seq BETWEEN 1 AND $4
ORDER BY e.client_event_seq ASC;

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

-- name: GetUserDashboardUsage :one
-- 用户概览用量聚合：单次扫描当前用户 runs，避免 dashboard 串行统计放大延迟。
SELECT
  COUNT(*) FILTER (WHERE started_at >= date_trunc('month', NOW()))::int AS this_month_calls,
  COALESCE(SUM(cost_cents) FILTER (WHERE status = 'success' AND started_at >= date_trunc('month', NOW())), 0)::bigint AS this_month_spent,
  COUNT(*)::int AS total_calls
FROM runs
WHERE user_id = $1;

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

-- name: GetCreatorDashboardSummary :one
-- 创作者概览聚合：以 creator 的 agent 集合为起点，避免在大测试数据中重复扫描全量 runs。
WITH creator_agents AS (
  SELECT id, lifecycle_status, visibility, certification_status
  FROM agents
  WHERE creator_id = $1
)
SELECT
  COALESCE(SUM(monthly.calls_this_month), 0)::int AS this_month_calls,
  COALESCE(SUM(monthly.revenue_this_month), 0)::bigint AS this_month_revenue,
  COUNT(*) FILTER (WHERE ca.lifecycle_status = 'active')::int AS total_agents,
  COUNT(*) FILTER (WHERE ca.lifecycle_status = 'active' AND ca.visibility = 'public')::int AS public_agents,
  COUNT(*) FILTER (WHERE ca.lifecycle_status = 'active' AND ca.certification_status = 'pending')::int AS pending_agents
FROM creator_agents ca
LEFT JOIN LATERAL (
  SELECT COUNT(*)::int AS calls_this_month,
         COALESCE(SUM(r.creator_revenue_cents), 0)::bigint AS revenue_this_month
  FROM runs r
  WHERE r.agent_id = ca.id
    AND r.status = 'success'
    AND r.started_at >= date_trunc('month', NOW())
) monthly ON TRUE;

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
