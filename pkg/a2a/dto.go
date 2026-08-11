package a2a

// UpdateCallPolicyRequest configures which Agents may call the target through OpenLinker.
type UpdateCallPolicyRequest struct {
	CallableBy string `json:"callable_by" validate:"required,oneof=public same_creator private"`
}

type CallPolicyResponse struct {
	AgentID    string `json:"agent_id"`
	CallableBy string `json:"callable_by"`
	UpdatedAt  string `json:"updated_at,omitempty"`
}

type RuntimeWorkbenchResponse struct {
	Agent       RuntimeWorkbenchAgent        `json:"agent"`
	Runtime     RuntimeWorkbenchRuntime      `json:"runtime"`
	RecentRuns  []RuntimeWorkbenchRun        `json:"recent_runs"`
	Diagnostics []RuntimeWorkbenchDiagnostic `json:"diagnostics"`
}

type RuntimeWorkbenchAgent struct {
	ID                  string `json:"id"`
	Slug                string `json:"slug"`
	Name                string `json:"name"`
	ConnectionMode      string `json:"connection_mode"`
	LifecycleStatus     string `json:"lifecycle_status"`
	Visibility          string `json:"visibility"`
	CertificationStatus string `json:"certification_status"`
	ReadinessCallable   bool   `json:"readiness_callable"`
	AvailabilityStatus  string `json:"availability_status"`
}

type RuntimeWorkbenchRuntime struct {
	RuntimeContractID     string           `json:"runtime_contract_id"`
	RuntimeContractDigest string           `json:"runtime_contract_digest"`
	TransportPolicy       string           `json:"transport_policy"`
	PrimaryTransport      string           `json:"primary_transport"`
	FallbackTransport     string           `json:"fallback_transport"`
	CurrentTransport      string           `json:"current_transport"`
	TransportCounts       map[string]int32 `json:"transport_counts"`
	TransportChangedAt    *string          `json:"transport_changed_at,omitempty"`
	FallbackReason        *string          `json:"fallback_reason,omitempty"`
	ConnectionStatus      string           `json:"connection_status"`
	ActiveNodeCount       int32            `json:"active_node_count"`
	ActiveSessionCount    int32            `json:"active_session_count"`
	ReadySessionCount     int32            `json:"ready_session_count"`
	WorkerFeatures        []string         `json:"worker_features"`
	DrainingSessionCount  int32            `json:"draining_session_count"`
	TotalCapacity         int32            `json:"total_capacity"`
	TotalInflight         int32            `json:"total_inflight"`
	PendingRunCount       int32            `json:"pending_run_count"`
	RetryWaitRunCount     int32            `json:"retry_wait_run_count"`
	OfferedRunCount       int32            `json:"offered_run_count"`
	ExecutingRunCount     int32            `json:"executing_run_count"`
	LastSessionActivityAt *string          `json:"last_session_activity_at,omitempty"`
	LastAssignmentAt      *string          `json:"last_assignment_at,omitempty"`
	LastResultAt          *string          `json:"last_result_at,omitempty"`
}

type RuntimeWorkbenchRun struct {
	RunID                string  `json:"run_id"`
	Status               string  `json:"status"`
	DispatchState        string  `json:"dispatch_state"`
	AttemptCount         int32   `json:"attempt_count"`
	MaxAttempts          int32   `json:"max_attempts"`
	NextAttemptAt        *string `json:"next_attempt_at,omitempty"`
	Source               string  `json:"source"`
	StartedAt            string  `json:"started_at"`
	FinishedAt           *string `json:"finished_at,omitempty"`
	LatestAttemptID      *string `json:"latest_attempt_id,omitempty"`
	LatestAttemptState   *string `json:"latest_attempt_state,omitempty"`
	LastAssignmentAt     *string `json:"last_assignment_at,omitempty"`
	LatestAttemptEndedAt *string `json:"latest_attempt_ended_at,omitempty"`
	ErrorCode            *string `json:"error_code,omitempty"`
	ErrorMessage         *string `json:"error_message,omitempty"`
	DetailURL            string  `json:"detail_url"`
}

type RuntimeWorkbenchDiagnostic struct {
	Code            string `json:"code"`
	Severity        string `json:"severity"`
	Summary         string `json:"summary"`
	TechnicalDetail string `json:"technical_detail"`
	NextAction      string `json:"next_action"`
}

// SkillRef is the small capability badge shown in A2A call-chain views.
type SkillRef struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type A2AContextRef struct {
	ProtocolContextID string   `json:"protocol_context_id,omitempty"`
	ProtocolTaskID    string   `json:"protocol_task_id,omitempty"`
	RootContextID     string   `json:"root_context_id,omitempty"`
	ParentContextID   string   `json:"parent_context_id,omitempty"`
	ParentTaskID      string   `json:"parent_task_id,omitempty"`
	TraceID           string   `json:"trace_id,omitempty"`
	ReferenceTaskIDs  []string `json:"reference_task_ids,omitempty"`
	Source            string   `json:"source,omitempty"`
}

type ChildRunResponse struct {
	ChildRunID      string             `json:"child_run_id"`
	ParentRunID     string             `json:"parent_run_id"`
	CallerAgentID   string             `json:"caller_agent_id"`
	CallerAgentSlug string             `json:"caller_agent_slug"`
	CallerAgentName string             `json:"caller_agent_name"`
	CallerAgentTags []string           `json:"caller_agent_tags"`
	CallerSkills    []SkillRef         `json:"caller_skills"`
	TargetAgentID   string             `json:"target_agent_id"`
	TargetAgentSlug string             `json:"target_agent_slug"`
	TargetAgentName string             `json:"target_agent_name"`
	TargetAgentTags []string           `json:"target_agent_tags"`
	TargetSkills    []SkillRef         `json:"target_skills"`
	Reason          string             `json:"reason"`
	Status          string             `json:"status"`
	CostCents       int32              `json:"cost_cents"`
	DurationMs      *int32             `json:"duration_ms,omitempty"`
	StartedAt       string             `json:"started_at"`
	FinishedAt      *string            `json:"finished_at,omitempty"`
	Source          string             `json:"source"`
	BillingMode     string             `json:"billing_mode"`
	A2AContext      *A2AContextRef     `json:"a2a_context,omitempty"`
	Children        []ChildRunResponse `json:"children,omitempty"`
}

// ParentRunSummary identifies one user-owned run that delegated work to child Agents.
type ParentRunSummary struct {
	ParentRunID           string         `json:"parent_run_id"`
	CallerAgentID         string         `json:"caller_agent_id"`
	CallerAgentSlug       string         `json:"caller_agent_slug"`
	CallerAgentName       string         `json:"caller_agent_name"`
	CallerAgentTags       []string       `json:"caller_agent_tags"`
	CallerSkills          []SkillRef     `json:"caller_skills"`
	Source                string         `json:"source"`
	Status                string         `json:"status"`
	DurationMs            *int32         `json:"duration_ms,omitempty"`
	StartedAt             string         `json:"started_at"`
	FinishedAt            *string        `json:"finished_at,omitempty"`
	ChildCount            int32          `json:"child_count"`
	SuccessfulChildCount  int32          `json:"successful_child_count"`
	RunningChildCount     int32          `json:"running_child_count"`
	ActiveAgentTokenCount int32          `json:"active_agent_token_count"`
	LastAgentTokenUsedAt  *string        `json:"last_agent_token_used_at,omitempty"`
	A2AContext            *A2AContextRef `json:"a2a_context,omitempty"`
}

// ParentRunListResponse is the user's A2A entry directory.
type ParentRunListResponse struct {
	Items []ParentRunSummary `json:"items"`
	Total int32              `json:"total"`
	Page  int32              `json:"page"`
	Size  int32              `json:"size"`
}
