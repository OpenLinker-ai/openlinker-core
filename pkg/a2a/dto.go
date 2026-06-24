package a2a

// CreateRuntimeTokenRequest creates a credential bound to one calling Agent.
type CreateRuntimeTokenRequest struct {
	Name string `json:"name" validate:"required,min=1,max=80"`
}

// RuntimeTokenResponse returns token metadata. PlaintextToken is only populated once on creation.
type RuntimeTokenResponse struct {
	ID             string   `json:"id"`
	AgentID        string   `json:"agent_id"`
	Name           string   `json:"name"`
	Prefix         string   `json:"prefix"`
	Scopes         []string `json:"scopes"`
	PlaintextToken string   `json:"plaintext_token,omitempty"`
	LastUsedAt     *string  `json:"last_used_at,omitempty"`
	RevokedAt      *string  `json:"revoked_at,omitempty"`
	CreatedAt      string   `json:"created_at"`
}

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
	Tokens      []RuntimeTokenResponse       `json:"tokens"`
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
	ActiveTokenCount                 int32   `json:"active_token_count"`
	PendingRunCount                  int32   `json:"pending_run_count"`
	ClaimNow                         bool    `json:"claim_now"`
	LastRuntimeActivityAt            *string `json:"last_runtime_activity_at,omitempty"`
	LastClaimedAt                    *string `json:"last_claimed_at,omitempty"`
	LastResultAt                     *string `json:"last_result_at,omitempty"`
	RecommendedHeartbeatAfterSeconds int32   `json:"recommended_heartbeat_after_seconds"`
	MaxClaimWaitSeconds              int32   `json:"max_claim_wait_seconds"`
}

type RuntimeWorkbenchRun struct {
	RunID        string  `json:"run_id"`
	Status       string  `json:"status"`
	Source       string  `json:"source"`
	StartedAt    string  `json:"started_at"`
	ClaimedAt    *string `json:"claimed_at,omitempty"`
	FinishedAt   *string `json:"finished_at,omitempty"`
	ErrorCode    *string `json:"error_code,omitempty"`
	ErrorMessage *string `json:"error_message,omitempty"`
	DetailURL    string  `json:"detail_url"`
}

type RuntimeWorkbenchDiagnostic struct {
	Code       string `json:"code"`
	Severity   string `json:"severity"`
	Message    string `json:"message"`
	NextAction string `json:"next_action"`
}

// CallAgentRequest is sent by Agent A using its runtime token.
type CallAgentRequest struct {
	ParentRunID            string                     `json:"parent_run_id,omitempty" validate:"omitempty,uuid"`
	CurrentRunID           string                     `json:"current_run_id,omitempty" validate:"omitempty,uuid"`
	TargetAgentID          string                     `json:"target_agent_id" validate:"required,uuid"`
	Reason                 string                     `json:"reason" validate:"max=500"`
	Input                  map[string]interface{}     `json:"input" validate:"required"`
	Metadata               map[string]interface{}     `json:"metadata,omitempty"`
	TaskCallback           *A2APushNotificationConfig `json:"task_callback,omitempty"`
	PushNotification       *A2APushNotificationConfig `json:"push_notification,omitempty"`
	PushNotificationConfig *A2APushNotificationConfig `json:"pushNotificationConfig,omitempty"`
}

// SkillRef is the small capability badge shown in A2A call-chain views.
type SkillRef struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type ChildRunResponse struct {
	ChildRunID      string     `json:"child_run_id"`
	ParentRunID     string     `json:"parent_run_id"`
	CallerAgentID   string     `json:"caller_agent_id"`
	CallerAgentSlug string     `json:"caller_agent_slug"`
	CallerAgentName string     `json:"caller_agent_name"`
	CallerAgentTags []string   `json:"caller_agent_tags"`
	CallerSkills    []SkillRef `json:"caller_skills"`
	TargetAgentID   string     `json:"target_agent_id"`
	TargetAgentSlug string     `json:"target_agent_slug"`
	TargetAgentName string     `json:"target_agent_name"`
	TargetAgentTags []string   `json:"target_agent_tags"`
	TargetSkills    []SkillRef `json:"target_skills"`
	Reason          string     `json:"reason"`
	Status          string     `json:"status"`
	CostCents       int32      `json:"cost_cents"`
	DurationMs      *int32     `json:"duration_ms,omitempty"`
	StartedAt       string     `json:"started_at"`
	FinishedAt      *string    `json:"finished_at,omitempty"`
	Source          string     `json:"source"`
	BillingMode     string     `json:"billing_mode"`
}

// ParentRunSummary identifies one user-owned run that delegated work to child Agents.
type ParentRunSummary struct {
	ParentRunID             string     `json:"parent_run_id"`
	CallerAgentID           string     `json:"caller_agent_id"`
	CallerAgentSlug         string     `json:"caller_agent_slug"`
	CallerAgentName         string     `json:"caller_agent_name"`
	CallerAgentTags         []string   `json:"caller_agent_tags"`
	CallerSkills            []SkillRef `json:"caller_skills"`
	Source                  string     `json:"source"`
	Status                  string     `json:"status"`
	DurationMs              *int32     `json:"duration_ms,omitempty"`
	StartedAt               string     `json:"started_at"`
	FinishedAt              *string    `json:"finished_at,omitempty"`
	ChildCount              int32      `json:"child_count"`
	SuccessfulChildCount    int32      `json:"successful_child_count"`
	RunningChildCount       int32      `json:"running_child_count"`
	ActiveRuntimeTokenCount int32      `json:"active_runtime_token_count"`
	LastRuntimeTokenUsedAt  *string    `json:"last_runtime_token_used_at,omitempty"`
}

// ParentRunListResponse is the user's A2A entry directory.
type ParentRunListResponse struct {
	Items []ParentRunSummary `json:"items"`
	Total int32              `json:"total"`
	Page  int32              `json:"page"`
	Size  int32              `json:"size"`
}
