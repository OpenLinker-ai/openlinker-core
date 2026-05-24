-- webhooks.sql
--
-- 子轮 2.1（Phase 2）：Agent 调用结束后向创作者 webhook 推送结果。
-- 表结构见 migrations/002_api_keys_and_webhooks.up.sql：
--   - agents.webhook_url / webhook_secret
--   - webhook_deliveries（投递日志 + 重试状态）
--
-- 设计要点：
--   - SetAgentWebhook / ClearAgentWebhook 限定 creator_id，防越权
--   - GetAgentWebhookConfig 仅返回 webhook 相关字段（runtime 触发投递时用）
--   - MarkDeliveryFailedRetry / MarkDeliveryFailedFinal 分两条：
--     业务层根据 attempt_count 判断是否还有重试机会
--   - ListPendingDeliveries 给后台 worker 用，按 next_retry_at ASC 处理

-- name: SetAgentWebhook :execrows
-- 创作者设置 webhook（同时刷新 secret）。返回受影响行数：0 表示越权或 agent 不存在。
UPDATE agents
SET webhook_url = $2,
    webhook_secret = $3,
    updated_at = NOW()
WHERE id = $1 AND creator_id = $4;

-- name: ClearAgentWebhook :execrows
-- 创作者清除 webhook 配置。返回受影响行数：0 表示越权或 agent 不存在。
UPDATE agents
SET webhook_url = NULL,
    webhook_secret = NULL,
    updated_at = NOW()
WHERE id = $1 AND creator_id = $2;

-- name: GetAgentWebhookConfig :one
-- 仅取 webhook 相关字段（runtime 触发投递时用）。
SELECT id, creator_id, slug, webhook_url, webhook_secret
FROM agents
WHERE id = $1;

-- name: CreateWebhookDelivery :one
-- 创建投递记录，初始 status='pending'，next_retry_at=NOW()（立即可投递）。
INSERT INTO webhook_deliveries (
    agent_id, run_id, url, payload, status, attempt_count, next_retry_at
) VALUES (
    $1, $2, $3, $4, 'pending', 0, NOW()
)
RETURNING id, agent_id, run_id, url, payload, status,
          response_status, response_body, error_message,
          attempt_count, next_retry_at, created_at, updated_at;

-- name: GetWebhookDeliveryByID :one
-- worker 重试时按 id 取记录，附带 agent.webhook_secret（投递签名用）。
SELECT d.id, d.agent_id, d.run_id, d.url, d.payload, d.status,
       d.response_status, d.response_body, d.error_message,
       d.attempt_count, d.next_retry_at, d.created_at, d.updated_at,
       a.webhook_secret AS webhook_secret
FROM webhook_deliveries d
JOIN agents a ON a.id = d.agent_id
WHERE d.id = $1;

-- name: MarkDeliverySuccess :exec
-- 投递成功：status=success，next_retry_at=NULL，stop。
UPDATE webhook_deliveries
SET status = 'success',
    response_status = $2,
    response_body = $3,
    error_message = NULL,
    attempt_count = attempt_count + 1,
    next_retry_at = NULL,
    updated_at = NOW()
WHERE id = $1;

-- name: MarkDeliveryFailedRetry :exec
-- 失败但还有重试机会：保持 status=pending，写入下次重试时间。
UPDATE webhook_deliveries
SET status = 'pending',
    response_status = $2,
    response_body = $3,
    error_message = $4,
    attempt_count = attempt_count + 1,
    next_retry_at = $5,
    updated_at = NOW()
WHERE id = $1;

-- name: MarkDeliveryFailedFinal :exec
-- 失败且放弃重试：status=failed，next_retry_at=NULL。
UPDATE webhook_deliveries
SET status = 'failed',
    response_status = $2,
    response_body = $3,
    error_message = $4,
    attempt_count = attempt_count + 1,
    next_retry_at = NULL,
    updated_at = NOW()
WHERE id = $1;

-- name: ListPendingDeliveries :many
-- worker 拉取所有应当重试的投递（按下次时间排序，限制 50 条防一次扫太多）。
SELECT id, agent_id, run_id, url, payload, status,
       response_status, response_body, error_message,
       attempt_count, next_retry_at, created_at, updated_at
FROM webhook_deliveries
WHERE status = 'pending' AND next_retry_at IS NOT NULL AND next_retry_at <= NOW()
ORDER BY next_retry_at ASC
LIMIT 50;

-- name: ListDeliveriesByAgent :many
-- 创作者查看某个 agent 的投递历史（按时间倒序，分页）。
-- 业务层应先校验 agent.creator_id == 当前用户。
SELECT id, agent_id, run_id, url, payload, status,
       response_status, response_body, error_message,
       attempt_count, next_retry_at, created_at, updated_at
FROM webhook_deliveries
WHERE agent_id = $1
ORDER BY created_at DESC
LIMIT $2;
