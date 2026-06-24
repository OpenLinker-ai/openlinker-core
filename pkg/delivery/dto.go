package delivery

import (
	"encoding/json"
	"time"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

// CreateTargetRequest POST /api/v1/delivery-targets。
//
// 仅支持 webhook / slack 两种 type。URL 默认必须 HTTPS；
// 本地开发可由服务端配置允许 loopback HTTP。
type CreateTargetRequest struct {
	Name      string `json:"name" validate:"required,min=1,max=80"`
	Type      string `json:"type" validate:"required,oneof=webhook slack"`
	URL       string `json:"url" validate:"required,url,max=500"`
	IsDefault bool   `json:"is_default"`
}

// TargetResponse 投递目标返回体。Secret 仅 Create / Rotate 时返回一次。
type TargetResponse struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Type      string          `json:"type"`
	URL       string          `json:"url"`
	Secret    string          `json:"secret,omitempty"`
	IsDefault bool            `json:"is_default"`
	Config    json.RawMessage `json:"config,omitempty"`
	CreatedAt string          `json:"created_at"`
}

// DeliverRequest POST /api/v1/runs/:id/deliver。
//
// TargetID 为空时使用用户的 is_default target；都没有则 400。
type DeliverRequest struct {
	TargetID string `json:"target_id,omitempty"`
}

// DeliveryItem 投递历史项（不含 payload / response_body，避免巨大响应）。
type DeliveryItem struct {
	ID             string  `json:"id"`
	RunID          string  `json:"run_id"`
	TargetID       string  `json:"target_id"`
	TargetType     string  `json:"target_type"`
	TargetURL      string  `json:"target_url"`
	Status         string  `json:"status"`
	ResponseStatus *int32  `json:"response_status,omitempty"`
	ErrorMessage   *string `json:"error_message,omitempty"`
	AttemptCount   int32   `json:"attempt_count"`
	NextRetryAt    *string `json:"next_retry_at,omitempty"`
	CreatedAt      string  `json:"created_at"`
	UpdatedAt      string  `json:"updated_at"`
}

// DeliveryListFilter GET /api/v1/deliveries 查询条件。
//
// 只列出当前用户自己的外部投递记录；AgentID / RunID / Status 都是可选过滤。
type DeliveryListFilter struct {
	AgentID *string
	RunID   *string
	Status  string
	Limit   int32
}

// DeliveryPayload Run 完成后投递给用户 target 的 body。
//
// webhook target 直接收到该 JSON；slack target 由 service 层重新打包成 text。
type DeliveryPayload struct {
	Event      string                 `json:"event"`
	RunID      string                 `json:"run_id"`
	AgentID    string                 `json:"agent_id"`
	AgentSlug  string                 `json:"agent_slug"`
	AgentName  string                 `json:"agent_name"`
	Status     string                 `json:"status"`
	Input      map[string]interface{} `json:"input"`
	Output     map[string]interface{} `json:"output,omitempty"`
	CostCents  int32                  `json:"cost_cents"`
	DurationMs int32                  `json:"duration_ms"`
	StartedAt  string                 `json:"started_at"`
	FinishedAt string                 `json:"finished_at"`
}

func toTargetResponse(t db.DeliveryTarget, includeSecret bool) TargetResponse {
	resp := TargetResponse{
		ID:        t.ID.String(),
		Name:      t.Name,
		Type:      t.Type,
		IsDefault: t.IsDefault,
		Config:    json.RawMessage(t.Config),
		CreatedAt: t.CreatedAt.UTC().Format(time.RFC3339),
	}
	// 解析 config 取 url 字段方便前端展示
	var cfg map[string]string
	if err := json.Unmarshal(t.Config, &cfg); err == nil {
		resp.URL = cfg["url"]
	}
	if includeSecret {
		resp.Secret = t.Secret
	}
	return resp
}

func toDeliveryItem(d db.RunDelivery) DeliveryItem {
	item := DeliveryItem{
		ID:             d.ID.String(),
		RunID:          d.RunID.String(),
		TargetID:       d.TargetID.String(),
		TargetType:     d.TargetType,
		TargetURL:      d.TargetURL,
		Status:         d.Status,
		ResponseStatus: d.ResponseStatus,
		ErrorMessage:   d.ErrorMessage,
		AttemptCount:   d.AttemptCount,
		CreatedAt:      d.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:      d.UpdatedAt.UTC().Format(time.RFC3339),
	}
	if d.NextRetryAt != nil {
		t := d.NextRetryAt.UTC().Format(time.RFC3339)
		item.NextRetryAt = &t
	}
	return item
}
