package webhook

// SetWebhookRequest POST /api/v1/creator/agents/:id/webhook 请求体。
//
// URL 必须 https，由 schema CHECK 兜底（agents_webhook_https），同时这里前置校验。
type SetWebhookRequest struct {
	URL string `json:"webhook_url" validate:"required,url,startswith=https://,max=500"`
}

// SetWebhookResponse 创建 / 重置 webhook 响应。
//
// Secret 仅在创建 / rotate 时返回一次，后续无法再取（DB 仅存原值，但接口不再返回）。
type SetWebhookResponse struct {
	URL    string `json:"webhook_url"`
	Secret string `json:"webhook_secret"`
}

// DeliveryListItem GET /api/v1/creator/agents/:id/webhook/deliveries 返回项。
//
// payload / response_body 不返回（避免巨大响应；如需 debug 可再加详情接口）。
type DeliveryListItem struct {
	ID             string  `json:"id"`
	RunID          string  `json:"run_id"`
	URL            string  `json:"url"`
	Status         string  `json:"status"`
	ResponseStatus *int32  `json:"response_status,omitempty"`
	ErrorMessage   *string `json:"error_message,omitempty"`
	AttemptCount   int32   `json:"attempt_count"`
	NextRetryAt    *string `json:"next_retry_at,omitempty"`
	CreatedAt      string  `json:"created_at"`
	UpdatedAt      string  `json:"updated_at"`
}

// CreateTaskCallbackRequest POST /api/v1/runs/:id/task-callbacks.
type CreateTaskCallbackRequest struct {
	URL             string                 `json:"target_url" validate:"required,url,max=500"`
	EventTypes      []string               `json:"event_types,omitempty" validate:"omitempty,dive,oneof=run.created run.started run.dispatch.pending run.dispatch.claimed run.requirements.snapshotted run.message.delta run.artifact.delta run.status.changed run.child.created run.child.completed run.completed run.failed run.canceled"`
	AuthScheme      string                 `json:"auth_scheme,omitempty" validate:"omitempty,max=80"`
	AuthCredentials string                 `json:"auth_credentials,omitempty" validate:"omitempty,max=1000"`
	Metadata        map[string]interface{} `json:"metadata,omitempty"`
}

// TaskCallbackSubscriptionResponse is returned to the run owner.
type TaskCallbackSubscriptionResponse struct {
	ID                  string   `json:"id"`
	RunID               string   `json:"run_id"`
	TargetURL           string   `json:"target_url"`
	EventTypes          []string `json:"event_types"`
	AuthScheme          string   `json:"auth_scheme,omitempty"`
	Status              string   `json:"status"`
	ConsecutiveFailures int32    `json:"consecutive_failures"`
	Secret              string   `json:"secret,omitempty"`
	CreatedAt           string   `json:"created_at"`
	UpdatedAt           string   `json:"updated_at"`
}

type BatchTaskCallbackSubscriptionsRequest struct {
	SubscriptionIDs []string `json:"subscription_ids" validate:"required,min=1,max=50,dive,uuid"`
	Action          string   `json:"action" validate:"required,oneof=pause resume delete"`
}

type TaskCallbackSubscriptionListResponse struct {
	Items []TaskCallbackSubscriptionResponse `json:"items"`
}

type BatchTaskCallbackSubscriptionsResponse struct {
	Action       string                             `json:"action"`
	UpdatedCount int                                `json:"updated_count"`
	Items        []TaskCallbackSubscriptionResponse `json:"items"`
}

// TaskCallbackPayload is the A2A-style push body derived from run_events.
type TaskCallbackPayload struct {
	EventID        string                 `json:"event_id"`
	RunID          string                 `json:"run_id"`
	ParentRunID    string                 `json:"parent_run_id,omitempty"`
	EventType      string                 `json:"event_type"`
	Sequence       int32                  `json:"sequence"`
	Payload        map[string]interface{} `json:"payload"`
	SubscriptionID string                 `json:"subscription_id"`
	CreatedAt      string                 `json:"created_at"`
}

// WebhookPayload 投递给创作者 webhook_url 的 body。
//
// 字段含义：
//   - Event 固定 "run.completed"（成功 / 失败 / 超时 都是该事件，由 status 区分）
//   - Status: 'success' / 'failed' / 'timeout'
//   - Output 仅 success 时非空
//   - ErrorCode / ErrorMessage 仅 failed / timeout 时非空
//   - CostCents：成功时 = agent 价格；失败 / 超时 = 0（已退款）
type WebhookPayload struct {
	Event        string                 `json:"event"`
	RunID        string                 `json:"run_id"`
	AgentID      string                 `json:"agent_id"`
	AgentSlug    string                 `json:"agent_slug"`
	UserID       string                 `json:"user_id"`
	Status       string                 `json:"status"`
	Input        map[string]interface{} `json:"input"`
	Output       map[string]interface{} `json:"output,omitempty"`
	ErrorCode    string                 `json:"error_code,omitempty"`
	ErrorMessage string                 `json:"error_message,omitempty"`
	CostCents    int32                  `json:"cost_cents"`
	DurationMs   int32                  `json:"duration_ms"`
	StartedAt    string                 `json:"started_at"`
	FinishedAt   string                 `json:"finished_at"`
}
