package runtime

import "time"

// RunRequest POST /api/v1/run 请求体。
//
// AgentID 在 service 层 uuid.Parse 校验。
// Input 必填，为创作者 endpoint 接收的入参（透传）。
// Metadata 可选，平台原样转发给 endpoint，常用于 trace_id / 客户端版本等。
type RunRequest struct {
	AgentID  string                 `json:"agent_id" validate:"required,uuid"`
	Input    map[string]interface{} `json:"input" validate:"required"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// RunResponse POST /api/v1/run 同步响应体，或 POST /api/v1/runs 异步启动响应体。
//
// Status: 'running' / 'success' / 'failed' / 'timeout'。
// 失败 / 超时 时 Output 为空、ErrorCode + ErrorMsg 必填，CostCents=0（已退款）。
// Source: 'web' / 'mcp' / 'api'，由 handler 从鉴权方式推导。
type RunResponse struct {
	RunID         string                 `json:"run_id"`
	Status        string                 `json:"status"`
	Output        map[string]interface{} `json:"output,omitempty"`
	ErrorCode     string                 `json:"error_code,omitempty"`
	ErrorMsg      string                 `json:"error_message,omitempty"`
	CostCents     int32                  `json:"cost_cents"`
	DurationMs    int32                  `json:"duration_ms"`
	Source        string                 `json:"source,omitempty"`
	ParentRunID   string                 `json:"parent_run_id,omitempty"`
	CallerAgentID string                 `json:"caller_agent_id,omitempty"`
	BillingMode   string                 `json:"billing_mode,omitempty"`
}

// RunEventResponse GET /api/v1/runs/:id/events 响应项。
//
// sequence 在单个 run 内单调递增，后续 SSE 可用作 Last-Event-ID。
type RunEventResponse struct {
	EventID     string                 `json:"event_id"`
	RunID       string                 `json:"run_id"`
	ParentRunID string                 `json:"parent_run_id,omitempty"`
	Sequence    int32                  `json:"sequence"`
	EventType   string                 `json:"event_type"`
	Payload     map[string]interface{} `json:"payload"`
	CreatedAt   time.Time              `json:"created_at"`
}

// AgentRequest 平台 → 创作者 endpoint 的请求体。
//
// RunID 让创作者侧可对账 / 排查；Metadata 原样转发。
type AgentRequest struct {
	Input         map[string]interface{} `json:"input"`
	Metadata      map[string]interface{} `json:"metadata,omitempty"`
	RunID         string                 `json:"run_id"`
	ParentRunID   string                 `json:"parent_run_id,omitempty"`
	CallerAgentID string                 `json:"caller_agent_id,omitempty"`
}

// AgentResponse 创作者 endpoint → 平台的响应体。
//
// Output 业务结果（成功时必填）；Error 业务错误（失败时必填）；
// CostUSD 创作者透明告知本次调用真实成本（可选，目前平台不做核对，仅记录用）。
type AgentResponse struct {
	Output   map[string]interface{} `json:"output"`
	Events   []AgentEvent           `json:"events,omitempty"`
	CostUSD  *float64               `json:"cost_usd,omitempty"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
	Error    *AgentError            `json:"error,omitempty"`
}

// AgentEvent 是 Agent endpoint 可选返回的运行中事件。
//
// Phase 2 先允许少量 OpenLinker-native 事件；后续可映射到 A2A Message / Part / Artifact。
type AgentEvent struct {
	EventType string                 `json:"event_type"`
	Payload   map[string]interface{} `json:"payload"`
}

// ReportRunEventRequest Agent endpoint -> OpenLinker 的运行中事件上报请求体。
type ReportRunEventRequest struct {
	EventType string                 `json:"event_type"`
	Payload   map[string]interface{} `json:"payload,omitempty"`
}

// AgentError 创作者侧错误（业务级 4xx 等场景）。
type AgentError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
