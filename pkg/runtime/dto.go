package runtime

import "time"

// RunRequest POST /api/v1/run 请求体。
//
// AgentID 在 service 层 uuid.Parse 校验。
// Input 必填，为创作者 endpoint 接收的入参（透传）。
// Metadata 可选，平台原样转发给 endpoint，常用于 trace_id / 客户端版本等。
type RunRequest struct {
	AgentID        string                 `json:"agent_id" validate:"required,uuid"`
	Input          map[string]interface{} `json:"input" validate:"required"`
	Metadata       map[string]interface{} `json:"metadata,omitempty"`
	A2AContext     *RunA2AContextRequest  `json:"a2a_context,omitempty"`
	IdempotencyKey string                 `json:"-"`

	// CreationProtocol and CreationMethod are normalized by each entrypoint.
	// They are execution semantics, not user-controlled JSON fields, and are
	// included in the idempotency fingerprint.
	CreationProtocol string         `json:"-"`
	CreationMethod   string         `json:"-"`
	CreationOptions  map[string]any `json:"-"`
	// TaskCallback is OpenLinker's canonical callback field. PushNotification,
	// PushNotificationAlias, and PushNotificationConfig are A2A compatibility
	// aliases; taskCallbackConfigFromRunRequest chooses the first non-empty
	// value in this declaration order.
	TaskCallback           *TaskCallbackConfig `json:"task_callback,omitempty"`
	PushNotification       *TaskCallbackConfig `json:"push_notification,omitempty"`
	PushNotificationAlias  *TaskCallbackConfig `json:"pushNotification,omitempty"`
	PushNotificationConfig *TaskCallbackConfig `json:"pushNotificationConfig,omitempty"`
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
	MessageID           string                 `json:"message_id,omitempty"`
	ProtocolContextID   string                 `json:"protocol_context_id,omitempty"`
	ProtocolTaskID      string                 `json:"protocol_task_id,omitempty"`
	RootContextID       string                 `json:"root_context_id,omitempty"`
	ParentContextID     string                 `json:"parent_context_id,omitempty"`
	ParentTaskID        string                 `json:"parent_task_id,omitempty"`
	ParentRunID         string                 `json:"parent_run_id,omitempty"`
	CallerAgentID       string                 `json:"caller_agent_id,omitempty"`
	TargetAgentID       string                 `json:"target_agent_id,omitempty"`
	TraceID             string                 `json:"trace_id,omitempty"`
	ReferenceTaskIDs    []string               `json:"reference_task_ids,omitempty"`
	Source              string                 `json:"source,omitempty"`
	AcceptedOutputModes []string               `json:"accepted_output_modes,omitempty"`
	Extensions          []string               `json:"extensions,omitempty"`
	Visibility          string                 `json:"visibility,omitempty"`
	Options             map[string]interface{} `json:"options,omitempty"`
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

// ConversationContext is the Core-owned multi-run message context for a
// protocol/root context. Callers send the current message; Core packages prior
// persisted messages so runtimes do not trust client-supplied history.
type ConversationContext struct {
	ID                   string                `json:"id"`
	SessionKey           string                `json:"session_key"`
	ProtocolContextID    string                `json:"protocol_context_id,omitempty"`
	RootContextID        string                `json:"root_context_id,omitempty"`
	CurrentRunID         string                `json:"current_run_id"`
	CurrentProtocolTask  string                `json:"current_protocol_task_id,omitempty"`
	HistoryBeforeCurrent []ConversationMessage `json:"history_before_current,omitempty"`
	Truncated            bool                  `json:"truncated"`
	Source               string                `json:"source"`
}

type ConversationMessage struct {
	RunID         string                 `json:"run_id"`
	EventSequence *int32                 `json:"event_sequence,omitempty"`
	Role          string                 `json:"role"`
	Content       string                 `json:"content"`
	Payload       map[string]interface{} `json:"payload,omitempty"`
	CreatedAt     string                 `json:"created_at,omitempty"`
}

// RunResponse POST /api/v1/run 同步响应体，或 POST /api/v1/runs 异步启动响应体。
//
// Status: 'running' / 'success' / 'failed' / 'timeout'。
// 失败 / 超时 时 Output 为空、ErrorCode + ErrorMsg 必填，CostCents=0（已退款）。
// Source: 'web' / 'mcp' / 'api'，由 handler 从鉴权方式推导。
type RunResponse struct {
	RunID               string                 `json:"run_id"`
	AgentID             string                 `json:"agent_id,omitempty"`
	AgentSlug           string                 `json:"agent_slug,omitempty"`
	AgentName           string                 `json:"agent_name,omitempty"`
	AgentConnectionMode string                 `json:"agent_connection_mode,omitempty"`
	Status              string                 `json:"status"`
	Input               map[string]interface{} `json:"input,omitempty"`
	Output              map[string]interface{} `json:"output,omitempty"`
	ErrorCode           string                 `json:"error_code,omitempty"`
	// ErrorMsg keeps the historical Go field name while preserving the public
	// JSON contract as error_message.
	ErrorMsg                  string                          `json:"error_message,omitempty"`
	CostCents                 int32                           `json:"cost_cents"`
	DurationMs                int32                           `json:"duration_ms"`
	StartedAt                 time.Time                       `json:"started_at"`
	FinishedAt                *time.Time                      `json:"finished_at,omitempty"`
	Source                    string                          `json:"source,omitempty"`
	RuntimeContractID         string                          `json:"runtime_contract_id"`
	RuntimeTransport          string                          `json:"runtime_transport,omitempty"`
	RuntimeTransportReason    string                          `json:"runtime_transport_reason,omitempty"`
	RuntimeTransportChangedAt *time.Time                      `json:"runtime_transport_changed_at,omitempty"`
	DispatchState             string                          `json:"dispatch_state"`
	AttemptCount              int32                           `json:"attempt_count"`
	MaxAttempts               int32                           `json:"max_attempts"`
	NextAttemptAt             *time.Time                      `json:"next_attempt_at,omitempty"`
	LatestAttemptID           string                          `json:"latest_attempt_id,omitempty"`
	ActiveAttemptID           string                          `json:"active_attempt_id,omitempty"`
	CancelState               string                          `json:"cancel_state,omitempty"`
	CancelRequestedAt         *time.Time                      `json:"cancel_requested_at,omitempty"`
	CancelAcknowledgedAt      *time.Time                      `json:"cancel_acknowledged_at,omitempty"`
	CancelReason              string                          `json:"cancel_reason,omitempty"`
	DeadLetteredAt            *time.Time                      `json:"dead_lettered_at,omitempty"`
	ReplayOfRunID             string                          `json:"replay_of_run_id,omitempty"`
	ParentRunID               string                          `json:"parent_run_id,omitempty"`
	CallerAgentID             string                          `json:"caller_agent_id,omitempty"`
	BillingMode               string                          `json:"billing_mode,omitempty"`
	A2AContext                *RunA2AContextResponse          `json:"a2a_context,omitempty"`
	TaskCallback              *RunTaskCallbackResponse        `json:"task_callback,omitempty"`
	RequirementEvidence       *RunRequirementEvidenceResponse `json:"requirement_evidence,omitempty"`
	EvidenceSummary           *RunEvidenceSummary             `json:"evidence_summary,omitempty"`
	NextAction                *RunNextAction                  `json:"next_action,omitempty"`
	Replayed                  bool                            `json:"replayed"`
}

// RuntimeDeadLetterListResponse is the admin-only, input-free DLQ inventory.
// It intentionally exposes only redacted execution evidence and replay lineage.
type RuntimeDeadLetterListResponse struct {
	Items  []RuntimeDeadLetterListItem `json:"items"`
	Total  int32                       `json:"total"`
	Limit  int32                       `json:"limit"`
	Offset int32                       `json:"offset"`
}

type RuntimeDeadLetterListItem struct {
	DeadLetterID     string     `json:"dead_letter_id"`
	RunID            string     `json:"run_id"`
	AgentID          string     `json:"agent_id"`
	AgentSlug        string     `json:"agent_slug"`
	AgentName        string     `json:"agent_name"`
	Status           string     `json:"status"`
	DispatchState    string     `json:"dispatch_state"`
	AttemptCount     int32      `json:"attempt_count"`
	MaxAttempts      int32      `json:"max_attempts"`
	FinalAttemptID   string     `json:"final_attempt_id,omitempty"`
	FinalAttemptNo   int32      `json:"final_attempt_no"`
	ErrorCode        string     `json:"error_code,omitempty"`
	ErrorMessage     string     `json:"error_message,omitempty"`
	ErrorDetail      string     `json:"error_detail_redacted,omitempty"`
	ReasonCode       string     `json:"reason_code"`
	Reason           string     `json:"reason_redacted,omitempty"`
	DeadLetteredAt   *time.Time `json:"dead_lettered_at,omitempty"`
	CreatedAt        time.Time  `json:"created_at"`
	ReplayOfRunID    string     `json:"replay_of_run_id,omitempty"`
	ReplayedAsRunIDs []string   `json:"replayed_as_run_ids"`
}

// RuntimeNodeListResponse is the admin-only Runtime Node inventory. The
// database timestamp and current contract make freshness/compatibility
// decisions reproducible without trusting the API host clock.
type RuntimeNodeListResponse struct {
	Items                 []RuntimeNodeListItem `json:"items"`
	Total                 int32                 `json:"total"`
	Limit                 int32                 `json:"limit"`
	Offset                int32                 `json:"offset"`
	CurrentContractID     string                `json:"current_contract_id"`
	CurrentContractDigest string                `json:"current_contract_digest"`
	DatabaseTime          time.Time             `json:"database_time"`
}

type RuntimeNodeListItem struct {
	NodeID                string     `json:"node_id"`
	DisplayName           string     `json:"display_name"`
	NodeVersion           string     `json:"node_version"`
	ProtocolVersion       int32      `json:"protocol_version"`
	RuntimeContractID     string     `json:"runtime_contract_id"`
	RuntimeContractDigest string     `json:"runtime_contract_digest"`
	ContractMatch         bool       `json:"contract_match"`
	Features              []string   `json:"features"`
	Capacity              int32      `json:"capacity"`
	Inflight              int32      `json:"inflight"`
	Status                string     `json:"status"`
	LastSeenAt            *time.Time `json:"last_seen_at,omitempty"`
	DrainingAt            *time.Time `json:"draining_at,omitempty"`
	RevokedAt             *time.Time `json:"revoked_at,omitempty"`
	RevokeReason          *string    `json:"revoke_reason,omitempty"`
	CreatedAt             time.Time  `json:"created_at"`
	UpdatedAt             time.Time  `json:"updated_at"`
	ActiveSessionCount    int32      `json:"active_session_count"`
	ActiveAgentCount      int32      `json:"active_agent_count"`
}

type RevokeRuntimeNodeRequest struct {
	Reason string `json:"reason" validate:"required,min=1,max=500"`
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

// RunEventPageResponse is an owner-readable event page plus the durable
// retention boundary used to interpret that page.
type RunEventPageResponse struct {
	Items []RunEventResponse `json:"items"`
	Meta  RunEventPageMeta   `json:"meta"`
}

// RunEventPageMeta makes an incomplete event history explicit. Sequence zero
// is the cursor before the first event; unavailable bounds are encoded as null.
type RunEventPageMeta struct {
	RequestedAfterSequence    int32  `json:"requested_after_sequence"`
	EffectiveAfterSequence    int32  `json:"effective_after_sequence"`
	RetainedThroughSequence   int32  `json:"retained_through_sequence"`
	EarliestAvailableSequence *int32 `json:"earliest_available_sequence"`
	LatestAvailableSequence   *int32 `json:"latest_available_sequence"`
	RetentionGap              bool   `json:"retention_gap"`
	Terminal                  bool   `json:"terminal"`
	StreamComplete            bool   `json:"stream_complete"`
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
	Conversation  *ConversationContext   `json:"conversation,omitempty"`
}

// AgentA2AContext tells an Agent how to delegate from its current run without
// making a human copy/paste a parent run id from the UI.
type AgentA2AContext struct {
	CurrentRunID        string   `json:"current_run_id"`
	MessageID           string   `json:"message_id,omitempty"`
	Protocol            string   `json:"protocol,omitempty"`
	Method              string   `json:"method,omitempty"`
	ParentRunID         string   `json:"parent_run_id,omitempty"`
	CallerAgentID       string   `json:"caller_agent_id,omitempty"`
	ProtocolContextID   string   `json:"protocol_context_id,omitempty"`
	ProtocolTaskID      string   `json:"protocol_task_id,omitempty"`
	RootContextID       string   `json:"root_context_id,omitempty"`
	ParentContextID     string   `json:"parent_context_id,omitempty"`
	ParentTaskID        string   `json:"parent_task_id,omitempty"`
	TraceID             string   `json:"trace_id,omitempty"`
	ReferenceTaskIDs    []string `json:"reference_task_ids,omitempty"`
	AcceptedOutputModes []string `json:"accepted_output_modes,omitempty"`
	Extensions          []string `json:"extensions,omitempty"`
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

// AgentError 创作者侧错误（业务级 4xx 等场景）。
type AgentError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
