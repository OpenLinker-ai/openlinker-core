package runtime

import "time"

// RunRequest POST /api/v1/run 请求体。
//
// AgentID 在 service 层 uuid.Parse 校验。
// Input 必填，为创作者 endpoint 接收的入参（透传）。
// Metadata 可选，平台原样转发给 endpoint，常用于 trace_id / 客户端版本等。
type RunRequest struct {
	AgentID                string                 `json:"agent_id" validate:"required,uuid"`
	Input                  map[string]interface{} `json:"input" validate:"required"`
	Metadata               map[string]interface{} `json:"metadata,omitempty"`
	A2AContext             *RunA2AContextRequest  `json:"a2a_context,omitempty"`
	TaskCallback           *TaskCallbackConfig    `json:"task_callback,omitempty"`
	PushNotification       *TaskCallbackConfig    `json:"push_notification,omitempty"`
	PushNotificationAlias  *TaskCallbackConfig    `json:"pushNotification,omitempty"`
	PushNotificationConfig *TaskCallbackConfig    `json:"pushNotificationConfig,omitempty"`
}

type TaskCallbackAuthentication struct {
	Scheme      string `json:"scheme,omitempty"`
	Credentials string `json:"credentials,omitempty"`
}

type TaskCallbackConfig struct {
	URL             string                      `json:"url,omitempty"`
	Token           string                      `json:"token,omitempty"`
	Secret          string                      `json:"secret,omitempty"`
	Authentication  *TaskCallbackAuthentication `json:"authentication,omitempty"`
	Metadata        map[string]interface{}      `json:"metadata,omitempty"`
	EventTypes      []string                    `json:"eventTypes,omitempty"`
	EventTypesAlias []string                    `json:"event_types,omitempty"`
}

type RunA2AContextRequest struct {
	ProtocolContextID string   `json:"protocol_context_id,omitempty"`
	ProtocolTaskID    string   `json:"protocol_task_id,omitempty"`
	RootContextID     string   `json:"root_context_id,omitempty"`
	ParentContextID   string   `json:"parent_context_id,omitempty"`
	ParentTaskID      string   `json:"parent_task_id,omitempty"`
	ParentRunID       string   `json:"parent_run_id,omitempty"`
	CallerAgentID     string   `json:"caller_agent_id,omitempty"`
	TargetAgentID     string   `json:"target_agent_id,omitempty"`
	TraceID           string   `json:"trace_id,omitempty"`
	ReferenceTaskIDs  []string `json:"reference_task_ids,omitempty"`
	Source            string   `json:"source,omitempty"`
}

type RunA2AContextResponse struct {
	ProtocolContextID string   `json:"protocol_context_id"`
	ProtocolTaskID    string   `json:"protocol_task_id"`
	RootContextID     string   `json:"root_context_id"`
	ParentContextID   string   `json:"parent_context_id,omitempty"`
	ParentTaskID      string   `json:"parent_task_id,omitempty"`
	ParentRunID       string   `json:"parent_run_id,omitempty"`
	CallerAgentID     string   `json:"caller_agent_id,omitempty"`
	TargetAgentID     string   `json:"target_agent_id,omitempty"`
	TraceID           string   `json:"trace_id,omitempty"`
	ReferenceTaskIDs  []string `json:"reference_task_ids,omitempty"`
	Source            string   `json:"source,omitempty"`
}

// RunResponse POST /api/v1/run 同步响应体，或 POST /api/v1/runs 异步启动响应体。
//
// Status: 'running' / 'success' / 'failed' / 'timeout'。
// 失败 / 超时 时 Output 为空、ErrorCode + ErrorMsg 必填，CostCents=0（已退款）。
// Source: 'web' / 'mcp' / 'api'，由 handler 从鉴权方式推导。
type RunResponse struct {
	RunID               string                          `json:"run_id"`
	AgentID             string                          `json:"agent_id,omitempty"`
	AgentSlug           string                          `json:"agent_slug,omitempty"`
	AgentName           string                          `json:"agent_name,omitempty"`
	AgentConnectionMode string                          `json:"agent_connection_mode,omitempty"`
	Status              string                          `json:"status"`
	Input               map[string]interface{}          `json:"input,omitempty"`
	Output              map[string]interface{}          `json:"output,omitempty"`
	ErrorCode           string                          `json:"error_code,omitempty"`
	ErrorMsg            string                          `json:"error_message,omitempty"`
	CostCents           int32                           `json:"cost_cents"`
	DurationMs          int32                           `json:"duration_ms"`
	Source              string                          `json:"source,omitempty"`
	ParentRunID         string                          `json:"parent_run_id,omitempty"`
	CallerAgentID       string                          `json:"caller_agent_id,omitempty"`
	BillingMode         string                          `json:"billing_mode,omitempty"`
	A2AContext          *RunA2AContextResponse          `json:"a2a_context,omitempty"`
	TaskCallback        *RunTaskCallbackResponse        `json:"task_callback,omitempty"`
	RequirementEvidence *RunRequirementEvidenceResponse `json:"requirement_evidence,omitempty"`
	EvidenceSummary     *RunEvidenceSummary             `json:"evidence_summary,omitempty"`
	NextAction          *RunNextAction                  `json:"next_action,omitempty"`
}

// RunTaskCallbackResponse describes a caller-owned task callback created while
// starting or delegating a run. Secret is only populated on creation.
type RunTaskCallbackResponse struct {
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

// RunEvidenceSummary gives UI and external clients a compact view of why a run
// is trustworthy without forcing them to stitch together multiple endpoints.
type RunEvidenceSummary struct {
	Status            string  `json:"status"`
	CoverageStatus    string  `json:"coverage_status"`
	MatchedSkillCount int     `json:"matched_skill_count"`
	MissingSkillCount int     `json:"missing_skill_count"`
	UsedMCPToolCount  int     `json:"used_mcp_tool_count"`
	ArtifactCount     int     `json:"artifact_count"`
	MessageCount      int     `json:"message_count"`
	DeliveryStatus    *string `json:"delivery_status,omitempty"`
	PublicSafe        bool    `json:"public_safe"`
	EvidenceURL       string  `json:"evidence_url"`
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
	AgentID                          string     `json:"agent_id"`
	AvailabilityStatus               string     `json:"availability_status"`
	LastCheckedAt                    *time.Time `json:"last_checked_at,omitempty"`
	ConsecutiveFailures              int32      `json:"consecutive_failures"`
	PendingRunCount                  int32      `json:"pending_run_count"`
	ClaimNow                         bool       `json:"claim_now"`
	NextClaimAfterSeconds            int32      `json:"next_claim_after_seconds"`
	RecommendedHeartbeatAfterSeconds int32      `json:"recommended_heartbeat_after_seconds"`
	MaxClaimWaitSeconds              int32      `json:"max_claim_wait_seconds"`
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
	ProtocolContextID string   `json:"protocol_context_id,omitempty"`
	ProtocolTaskID    string   `json:"protocol_task_id,omitempty"`
	RootContextID     string   `json:"root_context_id,omitempty"`
	ParentContextID   string   `json:"parent_context_id,omitempty"`
	ParentTaskID      string   `json:"parent_task_id,omitempty"`
	TraceID           string   `json:"trace_id,omitempty"`
	ReferenceTaskIDs  []string `json:"reference_task_ids,omitempty"`
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
	RunID          string                 `json:"run_id"`
	AgentID        string                 `json:"agent_id"`
	Input          map[string]interface{} `json:"input"`
	Metadata       map[string]interface{} `json:"metadata,omitempty"`
	Source         string                 `json:"source"`
	ResultEndpoint string                 `json:"result_endpoint"`
	ResultMethod   string                 `json:"result_method"`
	ResultRequired bool                   `json:"result_required"`
	A2A            *AgentA2AContext       `json:"a2a,omitempty"`
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

// RuntimeWSClientMessage is sent by a NAT/private Agent over
// /api/v1/agent-runtime/ws. The same runtime token and run state checks used by
// runtime_pull are reused for result and event writes.
type RuntimeWSClientMessage struct {
	Type       string                 `json:"type"`
	ID         string                 `json:"id,omitempty"`
	RunID      string                 `json:"run_id,omitempty"`
	EventType  string                 `json:"event_type,omitempty"`
	Payload    map[string]interface{} `json:"payload,omitempty"`
	Status     string                 `json:"status,omitempty"`
	Output     map[string]interface{} `json:"output,omitempty"`
	Events     []AgentEvent           `json:"events,omitempty"`
	Error      *AgentError            `json:"error,omitempty"`
	DurationMs int32                  `json:"duration_ms,omitempty"`
}

// RuntimeWSServerMessage is sent by OpenLinker over the Agent WebSocket.
// run.assigned is intentionally flattened so simple JS workers can consume it
// without understanding nested protocol objects.
type RuntimeWSServerMessage struct {
	Type              string                  `json:"type"`
	ID                string                  `json:"id,omitempty"`
	RunID             string                  `json:"run_id,omitempty"`
	AgentID           string                  `json:"agent_id,omitempty"`
	Input             map[string]interface{}  `json:"input,omitempty"`
	Metadata          map[string]interface{}  `json:"metadata,omitempty"`
	Source            string                  `json:"source,omitempty"`
	ResultEndpoint    string                  `json:"result_endpoint,omitempty"`
	ResultMethod      string                  `json:"result_method,omitempty"`
	ResultRequired    bool                    `json:"result_required,omitempty"`
	A2A               *AgentA2AContext        `json:"a2a,omitempty"`
	Status            string                  `json:"status,omitempty"`
	Result            *RunResponse            `json:"result,omitempty"`
	Event             *RunEventResponse       `json:"event,omitempty"`
	Heartbeat         *AgentHeartbeatResponse `json:"heartbeat,omitempty"`
	Error             *AgentError             `json:"error,omitempty"`
	RetryAfterSeconds int32                   `json:"retry_after_seconds,omitempty"`
}
