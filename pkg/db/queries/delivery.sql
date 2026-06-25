-- delivery.sql
--
-- Phase 2 §7：用户侧 Output Delivery。
-- 表结构见 migrations/009_delivery_targets.up.sql：
--   - delivery_targets：用户拥有的投递目标
--   - run_deliveries：每次投递记录 + 重试状态
--   - delivery_targets.config：{url, event_types}；旧 {url} 自动按终态事件处理
--
-- 设计要点：
--   - 所有写操作必须带 user_id 防越权（同 webhook_deliveries 用 creator_id）
--   - MarkDelivery* 三件套与 webhook 同款：pending/success/failed + next_retry_at
--   - ListPendingRunDeliveries 给 worker 用，按 next_retry_at ASC 处理

-- name: CreateDeliveryTarget :one
INSERT INTO delivery_targets (user_id, name, type, config, secret, is_default)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING id, user_id, name, type, config, secret, is_default, created_at, updated_at;

-- name: ListDeliveryTargetsByUser :many
SELECT id, user_id, name, type, config, secret, is_default, created_at, updated_at
FROM delivery_targets
WHERE user_id = $1
ORDER BY is_default DESC, created_at DESC;

-- name: GetDeliveryTargetByID :one
SELECT id, user_id, name, type, config, secret, is_default, created_at, updated_at
FROM delivery_targets
WHERE id = $1;

-- name: GetDefaultDeliveryTarget :one
SELECT id, user_id, name, type, config, secret, is_default, created_at, updated_at
FROM delivery_targets
WHERE user_id = $1 AND is_default = TRUE
LIMIT 1;

-- name: DeleteDeliveryTarget :execrows
-- 限定 user_id 防越权；返回行数 0 表示越权或不存在。
DELETE FROM delivery_targets WHERE id = $1 AND user_id = $2;

-- name: ClearDefaultDeliveryTarget :exec
-- 设新 default 前先清旧 default（防部分唯一索引冲突）。
UPDATE delivery_targets
SET is_default = FALSE, updated_at = NOW()
WHERE user_id = $1 AND is_default = TRUE;

-- name: SetDeliveryTargetDefault :execrows
UPDATE delivery_targets
SET is_default = TRUE, updated_at = NOW()
WHERE id = $1 AND user_id = $2;

-- name: UpdateDeliveryTargetConfig :one
UPDATE delivery_targets
SET config = $3,
    updated_at = NOW()
WHERE id = $1 AND user_id = $2
RETURNING id, user_id, name, type, config, secret, is_default, created_at, updated_at;

-- name: CreateRunDelivery :one
INSERT INTO run_deliveries (
    run_id, target_id, user_id, target_type, target_url, payload,
    status, attempt_count, next_retry_at
) VALUES (
    $1, $2, $3, $4, $5, $6, 'pending', 0, NOW()
)
RETURNING id, run_id, target_id, user_id, target_type, target_url, payload,
          status, response_status, response_body, error_message,
          attempt_count, next_retry_at, created_at, updated_at;

-- name: GetRunDeliveryByID :one
-- worker 重试时按 id 取记录，附带 target.secret + 当前 config。
-- target 被删除时 LEFT JOIN：secret/config 为 NULL，业务层标 final failed。
SELECT d.id, d.run_id, d.target_id, d.user_id, d.target_type, d.target_url, d.payload,
       d.status, d.response_status, d.response_body, d.error_message,
       d.attempt_count, d.next_retry_at, d.created_at, d.updated_at,
       t.secret AS target_secret,
       t.config AS target_config
FROM run_deliveries d
LEFT JOIN delivery_targets t ON t.id = d.target_id
WHERE d.id = $1;

-- name: MarkRunDeliverySuccess :exec
UPDATE run_deliveries
SET status = 'success',
    response_status = $2,
    response_body = $3,
    error_message = NULL,
    attempt_count = attempt_count + 1,
    next_retry_at = NULL,
    updated_at = NOW()
WHERE id = $1;

-- name: MarkRunDeliveryFailedRetry :exec
UPDATE run_deliveries
SET status = 'pending',
    response_status = $2,
    response_body = $3,
    error_message = $4,
    attempt_count = attempt_count + 1,
    next_retry_at = $5,
    updated_at = NOW()
WHERE id = $1;

-- name: MarkRunDeliveryFailedFinal :exec
UPDATE run_deliveries
SET status = 'failed',
    response_status = $2,
    response_body = $3,
    error_message = $4,
    attempt_count = attempt_count + 1,
    next_retry_at = NULL,
    updated_at = NOW()
WHERE id = $1;

-- name: ListPendingRunDeliveries :many
SELECT id, run_id, target_id, user_id, target_type, target_url, payload,
       status, response_status, response_body, error_message,
       attempt_count, next_retry_at, created_at, updated_at
FROM run_deliveries
WHERE status = 'pending' AND next_retry_at IS NOT NULL AND next_retry_at <= NOW()
ORDER BY next_retry_at ASC
LIMIT 50;

-- name: ListRunDeliveriesByRun :many
-- 按 run 查投递历史（用户侧 /run/:id 详情用）。
-- 业务层应先校验 run.user_id == 当前用户。
SELECT id, run_id, target_id, user_id, target_type, target_url, payload,
       status, response_status, response_body, error_message,
       attempt_count, next_retry_at, created_at, updated_at
FROM run_deliveries
WHERE run_id = $1
ORDER BY created_at DESC;

-- name: ListRunDeliveriesByUser :many
-- 用户侧外部投递历史列表；仅返回当前用户自己的 run_deliveries。
-- 可选按 agent/run/status 过滤，给独立历史页使用。
SELECT d.id, d.run_id, d.target_id, d.user_id, d.target_type, d.target_url, d.payload,
       d.status, d.response_status, d.response_body, d.error_message,
       d.attempt_count, d.next_retry_at, d.created_at, d.updated_at
FROM run_deliveries d
JOIN runs r ON r.id = d.run_id
WHERE d.user_id = $1
  AND ($2 = FALSE OR r.agent_id = $3)
  AND ($4 = FALSE OR d.run_id = $5)
  AND ($6 = '' OR d.status = $6)
ORDER BY d.created_at DESC
LIMIT $7;

-- name: ResetRunDeliveryForRetry :execrows
-- 手动重试 failed 投递：清重试状态，立即可投递。
UPDATE run_deliveries
SET status = 'pending',
    next_retry_at = NOW(),
    updated_at = NOW()
WHERE id = $1 AND user_id = $2 AND status = 'failed';
