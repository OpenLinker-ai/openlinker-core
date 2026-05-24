// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/webhooks.sql）。
//
// 子轮 2.1（Phase 2）：webhook 投递相关 query。
// 风格参考 wallets.sql.go：const + 方法，逐字段 Scan，避免与其它 subagent 冲突。

package db

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// scanWebhookDelivery 把一行扫描成 WebhookDelivery（按 SELECT 列顺序）。
func scanWebhookDelivery(row interface {
	Scan(dest ...any) error
}, d *WebhookDelivery) error {
	return row.Scan(
		&d.ID,
		&d.AgentID,
		&d.RunID,
		&d.URL,
		&d.Payload,
		&d.Status,
		&d.ResponseStatus,
		&d.ResponseBody,
		&d.ErrorMessage,
		&d.AttemptCount,
		&d.NextRetryAt,
		&d.CreatedAt,
		&d.UpdatedAt,
	)
}

const setAgentWebhook = `-- name: SetAgentWebhook :execrows
UPDATE agents
SET webhook_url = $2,
    webhook_secret = $3,
    updated_at = NOW()
WHERE id = $1 AND creator_id = $4`

// SetAgentWebhookParams 入参。
type SetAgentWebhookParams struct {
	ID            uuid.UUID `db:"id" json:"id"`
	WebhookURL    *string   `db:"webhook_url" json:"webhook_url"`
	WebhookSecret *string   `db:"webhook_secret" json:"webhook_secret"`
	CreatorID     uuid.UUID `db:"creator_id" json:"creator_id"`
}

// SetAgentWebhook 创作者设置 webhook（同时刷新 secret）。
// 返回受影响行数：0 表示越权或 agent 不存在。
func (q *Queries) SetAgentWebhook(ctx context.Context, arg SetAgentWebhookParams) (int64, error) {
	tag, err := q.db.Exec(ctx, setAgentWebhook,
		arg.ID,
		arg.WebhookURL,
		arg.WebhookSecret,
		arg.CreatorID,
	)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

const clearAgentWebhook = `-- name: ClearAgentWebhook :execrows
UPDATE agents
SET webhook_url = NULL,
    webhook_secret = NULL,
    updated_at = NOW()
WHERE id = $1 AND creator_id = $2`

// ClearAgentWebhookParams 入参。
type ClearAgentWebhookParams struct {
	ID        uuid.UUID `db:"id" json:"id"`
	CreatorID uuid.UUID `db:"creator_id" json:"creator_id"`
}

// ClearAgentWebhook 创作者清除 webhook 配置。
// 返回受影响行数：0 表示越权或 agent 不存在。
func (q *Queries) ClearAgentWebhook(ctx context.Context, arg ClearAgentWebhookParams) (int64, error) {
	tag, err := q.db.Exec(ctx, clearAgentWebhook, arg.ID, arg.CreatorID)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

const getAgentWebhookConfig = `-- name: GetAgentWebhookConfig :one
SELECT id, creator_id, slug, webhook_url, webhook_secret
FROM agents
WHERE id = $1`

// GetAgentWebhookConfigRow 仅 webhook 触发用最小字段集。
type GetAgentWebhookConfigRow struct {
	ID            uuid.UUID `db:"id" json:"id"`
	CreatorID     uuid.UUID `db:"creator_id" json:"creator_id"`
	Slug          string    `db:"slug" json:"slug"`
	WebhookURL    *string   `db:"webhook_url" json:"webhook_url"`
	WebhookSecret *string   `db:"webhook_secret" json:"-"`
}

// GetAgentWebhookConfig 仅取 webhook 字段（runtime 触发投递时用）。
func (q *Queries) GetAgentWebhookConfig(ctx context.Context, id uuid.UUID) (GetAgentWebhookConfigRow, error) {
	row := q.db.QueryRow(ctx, getAgentWebhookConfig, id)
	var r GetAgentWebhookConfigRow
	err := row.Scan(&r.ID, &r.CreatorID, &r.Slug, &r.WebhookURL, &r.WebhookSecret)
	return r, err
}

const createWebhookDelivery = `-- name: CreateWebhookDelivery :one
INSERT INTO webhook_deliveries (
    agent_id, run_id, url, payload, status, attempt_count, next_retry_at
) VALUES (
    $1, $2, $3, $4, 'pending', 0, NOW()
)
RETURNING id, agent_id, run_id, url, payload, status,
          response_status, response_body, error_message,
          attempt_count, next_retry_at, created_at, updated_at`

// CreateWebhookDeliveryParams 入参。
type CreateWebhookDeliveryParams struct {
	AgentID uuid.UUID `db:"agent_id" json:"agent_id"`
	RunID   uuid.UUID `db:"run_id" json:"run_id"`
	URL     string    `db:"url" json:"url"`
	Payload []byte    `db:"payload" json:"payload"`
}

// CreateWebhookDelivery 创建投递记录（初始 pending、立即可投递）。
func (q *Queries) CreateWebhookDelivery(ctx context.Context, arg CreateWebhookDeliveryParams) (WebhookDelivery, error) {
	row := q.db.QueryRow(ctx, createWebhookDelivery,
		arg.AgentID,
		arg.RunID,
		arg.URL,
		arg.Payload,
	)
	var d WebhookDelivery
	err := scanWebhookDelivery(row, &d)
	return d, err
}

const getWebhookDeliveryByID = `-- name: GetWebhookDeliveryByID :one
SELECT d.id, d.agent_id, d.run_id, d.url, d.payload, d.status,
       d.response_status, d.response_body, d.error_message,
       d.attempt_count, d.next_retry_at, d.created_at, d.updated_at,
       a.webhook_secret AS webhook_secret
FROM webhook_deliveries d
JOIN agents a ON a.id = d.agent_id
WHERE d.id = $1`

// GetWebhookDeliveryRow JOIN 结果：投递记录 + 当前 agent 的 secret。
//
// 投递时若 agent 已 ClearWebhook（secret = NULL），worker 会判定 final fail。
type GetWebhookDeliveryRow struct {
	WebhookDelivery
	WebhookSecret *string `db:"webhook_secret" json:"-"`
}

// GetWebhookDeliveryByID worker 重试时按 id 取记录（含 secret）。
func (q *Queries) GetWebhookDeliveryByID(ctx context.Context, id uuid.UUID) (GetWebhookDeliveryRow, error) {
	row := q.db.QueryRow(ctx, getWebhookDeliveryByID, id)
	var r GetWebhookDeliveryRow
	err := row.Scan(
		&r.ID,
		&r.AgentID,
		&r.RunID,
		&r.URL,
		&r.Payload,
		&r.Status,
		&r.ResponseStatus,
		&r.ResponseBody,
		&r.ErrorMessage,
		&r.AttemptCount,
		&r.NextRetryAt,
		&r.CreatedAt,
		&r.UpdatedAt,
		&r.WebhookSecret,
	)
	return r, err
}

const markDeliverySuccess = `-- name: MarkDeliverySuccess :exec
UPDATE webhook_deliveries
SET status = 'success',
    response_status = $2,
    response_body = $3,
    error_message = NULL,
    attempt_count = attempt_count + 1,
    next_retry_at = NULL,
    updated_at = NOW()
WHERE id = $1`

// MarkDeliverySuccessParams 入参。
type MarkDeliverySuccessParams struct {
	ID             uuid.UUID `db:"id" json:"id"`
	ResponseStatus *int32    `db:"response_status" json:"response_status"`
	ResponseBody   *string   `db:"response_body" json:"response_body"`
}

// MarkDeliverySuccess 投递成功：终态。
func (q *Queries) MarkDeliverySuccess(ctx context.Context, arg MarkDeliverySuccessParams) error {
	_, err := q.db.Exec(ctx, markDeliverySuccess, arg.ID, arg.ResponseStatus, arg.ResponseBody)
	return err
}

const markDeliveryFailedRetry = `-- name: MarkDeliveryFailedRetry :exec
UPDATE webhook_deliveries
SET status = 'pending',
    response_status = $2,
    response_body = $3,
    error_message = $4,
    attempt_count = attempt_count + 1,
    next_retry_at = $5,
    updated_at = NOW()
WHERE id = $1`

// MarkDeliveryFailedRetryParams 入参。
type MarkDeliveryFailedRetryParams struct {
	ID             uuid.UUID `db:"id" json:"id"`
	ResponseStatus *int32    `db:"response_status" json:"response_status"`
	ResponseBody   *string   `db:"response_body" json:"response_body"`
	ErrorMessage   *string   `db:"error_message" json:"error_message"`
	NextRetryAt    time.Time `db:"next_retry_at" json:"next_retry_at"`
}

// MarkDeliveryFailedRetry 失败但还有重试机会，写入下次重试时间。
func (q *Queries) MarkDeliveryFailedRetry(ctx context.Context, arg MarkDeliveryFailedRetryParams) error {
	_, err := q.db.Exec(ctx, markDeliveryFailedRetry,
		arg.ID,
		arg.ResponseStatus,
		arg.ResponseBody,
		arg.ErrorMessage,
		arg.NextRetryAt,
	)
	return err
}

const markDeliveryFailedFinal = `-- name: MarkDeliveryFailedFinal :exec
UPDATE webhook_deliveries
SET status = 'failed',
    response_status = $2,
    response_body = $3,
    error_message = $4,
    attempt_count = attempt_count + 1,
    next_retry_at = NULL,
    updated_at = NOW()
WHERE id = $1`

// MarkDeliveryFailedFinalParams 入参。
type MarkDeliveryFailedFinalParams struct {
	ID             uuid.UUID `db:"id" json:"id"`
	ResponseStatus *int32    `db:"response_status" json:"response_status"`
	ResponseBody   *string   `db:"response_body" json:"response_body"`
	ErrorMessage   *string   `db:"error_message" json:"error_message"`
}

// MarkDeliveryFailedFinal 失败且放弃重试：终态 failed。
func (q *Queries) MarkDeliveryFailedFinal(ctx context.Context, arg MarkDeliveryFailedFinalParams) error {
	_, err := q.db.Exec(ctx, markDeliveryFailedFinal,
		arg.ID,
		arg.ResponseStatus,
		arg.ResponseBody,
		arg.ErrorMessage,
	)
	return err
}

const listPendingDeliveries = `-- name: ListPendingDeliveries :many
SELECT id, agent_id, run_id, url, payload, status,
       response_status, response_body, error_message,
       attempt_count, next_retry_at, created_at, updated_at
FROM webhook_deliveries
WHERE status = 'pending' AND next_retry_at IS NOT NULL AND next_retry_at <= NOW()
ORDER BY next_retry_at ASC
LIMIT 50`

// ListPendingDeliveries worker 取出所有应重试的投递。
func (q *Queries) ListPendingDeliveries(ctx context.Context) ([]WebhookDelivery, error) {
	rows, err := q.db.Query(ctx, listPendingDeliveries)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []WebhookDelivery
	for rows.Next() {
		var d WebhookDelivery
		if err := scanWebhookDelivery(rows, &d); err != nil {
			return nil, err
		}
		items = append(items, d)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const listDeliveriesByAgent = `-- name: ListDeliveriesByAgent :many
SELECT id, agent_id, run_id, url, payload, status,
       response_status, response_body, error_message,
       attempt_count, next_retry_at, created_at, updated_at
FROM webhook_deliveries
WHERE agent_id = $1
ORDER BY created_at DESC
LIMIT $2`

// ListDeliveriesByAgentParams 入参。
type ListDeliveriesByAgentParams struct {
	AgentID uuid.UUID `db:"agent_id" json:"agent_id"`
	Limit   int32     `db:"limit" json:"limit"`
}

// ListDeliveriesByAgent 创作者查看 agent 的投递历史。
// 业务层应先校验 agent.creator_id == 当前用户。
func (q *Queries) ListDeliveriesByAgent(ctx context.Context, arg ListDeliveriesByAgentParams) ([]WebhookDelivery, error) {
	rows, err := q.db.Query(ctx, listDeliveriesByAgent, arg.AgentID, arg.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []WebhookDelivery
	for rows.Next() {
		var d WebhookDelivery
		if err := scanWebhookDelivery(rows, &d); err != nil {
			return nil, err
		}
		items = append(items, d)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
