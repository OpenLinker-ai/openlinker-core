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
	RunID               string                          `json:"run_id"`
	Status              string                          `json:"status"`
	Output              map[string]interface{}          `json:"output,omitempty"`
	ErrorCode           string                          `json:"error_code,omitempty"`
	ErrorMsg            string                          `json:"error_message,omitempty"`
	CostCents           int32                           `json:"cost_cents"`
	DurationMs          int32                           `json:"duration_ms"`
	Source              string                          `json:"source,omitempty"`
	ParentRunID         string                          `json:"parent_run_id,omitempty"`
	CallerAgentID       string                          `json:"caller_agent_id,omitempty"`
	BillingMode         string                          `json:"billing_mode,omitempty"`
	RequirementEvidence *RunRequirementEvidenceResponse `json:"requirement_evidence,omitempty"`
	NextAction          *RunNextAction                  `json:"next_action,omitempty"`
}

// RunRequirementEvidenceResponse proves which task Skill/MCP requirements were
// snapshotted onto a run and how the selected Agent covered them.
type RunRequirementEvidenceResponse struct {
	RunID            string    `json:"run_id"`
	TaskID           string    `json:"task_id"`
	AgentID          string    `json:"agent_id"`
	RequiredSkillIDs []string  `json:"required_skill_ids"`
	RequiredMCPTools []string  `json:"required_mcp_tools"`
	AgentSkillIDs    []string  `json:"agent_skill_ids"`
	MatchedSkillIDs  []string  `json:"matched_skill_ids"`
	MissingSkillIDs  []string  `json:"missing_skill_ids"`
	UsedMCPTools     []string  `json:"used_mcp_tools"`
	MissingMCPTools  []string  `json:"missing_mcp_tools"`
	CoverageStatus   string    `json:"coverage_status"`
	EvidenceSource   string    `json:"evidence_source"`
	CreatedAt        time.Time `json:"created_at"`
}

// RunNextAction is a machine-readable hint for the UI or external clients.
//
// Type is stable enough for clients to branch on. Hint is human-facing.
type RunNextAction struct {
	Type            string                 `json:"type"`
	Label           string                 `json:"label"`
	Hint            string                 `json:"hint"`
	Href            string                 `json:"href,omitempty"`
	Method          string                 `json:"method,omitempty"`
	RequiresHuman   bool                   `json:"requires_human,omitempty"`
	ResourceType    string                 `json:"resource_type,omitempty"`
	ResourceID      string                 `json:"resource_id,omitempty"`
	Source          string                 `json:"source,omitempty"`
	AdditionalProps map[string]interface{} `json:"additional_props,omitempty"`
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

// RunArtifactResponse is a persisted, owner-readable artifact produced by a run.
type RunArtifactResponse struct {
	ID               string                 `json:"id"`
	RunID            string                 `json:"run_id"`
	ArtifactType     string                 `json:"artifact_type"`
	Title            string                 `json:"title"`
	Content          map[string]interface{} `json:"content"`
	Visibility       string                 `json:"visibility"`
	SourceArtifactID string                 `json:"source_artifact_id,omitempty"`
	MimeType         string                 `json:"mime_type,omitempty"`
	FileURI          string                 `json:"file_uri,omitempty"`
	FileName         string                 `json:"file_name,omitempty"`
	FileSHA256       string                 `json:"file_sha256,omitempty"`
	FileSizeBytes    *int64                 `json:"file_size_bytes,omitempty"`
	CreatedAt        time.Time              `json:"created_at"`
}

// RunMessageResponse is a stable replay record derived from user input and agent message events.
type RunMessageResponse struct {
	ID            string                 `json:"id"`
	RunID         string                 `json:"run_id"`
	EventSequence *int32                 `json:"event_sequence,omitempty"`
	Role          string                 `json:"role"`
	Content       string                 `json:"content"`
	Payload       map[string]interface{} `json:"payload"`
	CreatedAt     time.Time              `json:"created_at"`
}

// AgentHeartbeatResponse confirms that an Agent-bound access token owner is alive.
type AgentHeartbeatResponse struct {
	AgentID             string     `json:"agent_id"`
	AvailabilityStatus  string     `json:"availability_status"`
	LastCheckedAt       *time.Time `json:"last_checked_at,omitempty"`
	ConsecutiveFailures int32      `json:"consecutive_failures"`
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
	A2A           *AgentA2AContext       `json:"a2a,omitempty"`
}

// AgentA2AContext tells an Agent how to delegate from its current run without
// making a human copy/paste a parent run id from the UI.
type AgentA2AContext struct {
	CurrentRunID      string   `json:"current_run_id"`
	ParentRunID       string   `json:"parent_run_id,omitempty"`
	CallerAgentID     string   `json:"caller_agent_id,omitempty"`
	CallAgentEndpoint string   `json:"call_agent_endpoint"`
	CallAgentMethod   string   `json:"call_agent_method"`
	RuntimeTokenType  string   `json:"runtime_token_type"`
	RuntimeScopes     []string `json:"runtime_scopes"`
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

// RuntimePullRunResponse 是内网 / IPv4 / NAT Agent 主动拉任务时拿到的 payload。
type RuntimePullRunResponse struct {
	RunID    string                 `json:"run_id"`
	AgentID  string                 `json:"agent_id"`
	Input    map[string]interface{} `json:"input"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
	Source   string                 `json:"source"`
	A2A      *AgentA2AContext       `json:"a2a,omitempty"`
}

// RuntimePullResultRequest 是 runtime_pull Agent 执行完任务后回传的结果。
//
// Status 支持 success / failed / timeout；success 必须带 output，失败建议带 error。
type RuntimePullResultRequest struct {
	Status     string                 `json:"status" validate:"required,oneof=success failed timeout"`
	Output     map[string]interface{} `json:"output,omitempty"`
	Events     []AgentEvent           `json:"events,omitempty"`
	Error      *AgentError            `json:"error,omitempty"`
	DurationMs int32                  `json:"duration_ms,omitempty"`
}
