package apitypes

import "time"

type MeResponse struct {
	UserID        string `json:"user_id"`
	Email         string `json:"email"`
	DisplayName   string `json:"display_name"`
	AvatarURL     string `json:"avatar_url,omitempty"`
	IsCreator     bool   `json:"is_creator"`
	IsAdmin       bool   `json:"is_admin"`
	HasPassword   bool   `json:"has_password"`
	IsOAuthUser   bool   `json:"is_oauth_user"`
	OAuthProvider string `json:"oauth_provider,omitempty"`
	AuthMethod    string `json:"auth_method"`
}

type RunListItem struct {
	ID                   string     `json:"id"`
	AgentID              string     `json:"agent_id"`
	AgentSlug            string     `json:"agent_slug"`
	AgentName            string     `json:"agent_name"`
	Status               string     `json:"status"`
	CostCents            int32      `json:"cost_cents"`
	DurationMs           *int32     `json:"duration_ms,omitempty"`
	StartedAt            string     `json:"started_at"`
	Source               string     `json:"source,omitempty"`
	RuntimeContractID    string     `json:"runtime_contract_id"`
	DispatchState        string     `json:"dispatch_state"`
	AttemptCount         int32      `json:"attempt_count"`
	MaxAttempts          int32      `json:"max_attempts"`
	NextAttemptAt        *time.Time `json:"next_attempt_at,omitempty"`
	LatestAttemptID      string     `json:"latest_attempt_id,omitempty"`
	ActiveAttemptID      string     `json:"active_attempt_id,omitempty"`
	CancelState          string     `json:"cancel_state,omitempty"`
	CancelRequestedAt    *time.Time `json:"cancel_requested_at,omitempty"`
	CancelAcknowledgedAt *time.Time `json:"cancel_acknowledged_at,omitempty"`
	CancelReason         string     `json:"cancel_reason,omitempty"`
	DeadLetteredAt       *time.Time `json:"dead_lettered_at,omitempty"`
	ReplayOfRunID        string     `json:"replay_of_run_id,omitempty"`
}

type RunListResponse struct {
	Items []RunListItem `json:"items"`
	Total int32         `json:"total"`
	Page  int32         `json:"page"`
	Size  int32         `json:"size"`
}

type CallRecordAgentRef struct {
	ID   string `json:"id"`
	Slug string `json:"slug"`
	Name string `json:"name"`
}

type CallRecordA2AContext struct {
	SessionID         string   `json:"session_id,omitempty"`
	CallID            string   `json:"call_id,omitempty"`
	ProtocolContextID string   `json:"protocol_context_id,omitempty"`
	ProtocolTaskID    string   `json:"protocol_task_id,omitempty"`
	RootContextID     string   `json:"root_context_id,omitempty"`
	ParentContextID   string   `json:"parent_context_id,omitempty"`
	ParentTaskID      string   `json:"parent_task_id,omitempty"`
	TraceID           string   `json:"trace_id,omitempty"`
	ReferenceTaskIDs  []string `json:"reference_task_ids,omitempty"`
	Source            string   `json:"source,omitempty"`
}

type CallRecordItem struct {
	ID                        string                `json:"id"`
	RunID                     string                `json:"run_id"`
	Direction                 string                `json:"direction"`
	Relation                  string                `json:"relation"`
	AgentID                   string                `json:"agent_id"`
	AgentSlug                 string                `json:"agent_slug"`
	AgentName                 string                `json:"agent_name"`
	TargetAgent               CallRecordAgentRef    `json:"target_agent"`
	CallerAgent               *CallRecordAgentRef   `json:"caller_agent,omitempty"`
	Status                    string                `json:"status"`
	CostCents                 int32                 `json:"cost_cents"`
	CreatorRevenueCents       int32                 `json:"creator_revenue_cents"`
	DurationMs                *int32                `json:"duration_ms,omitempty"`
	StartedAt                 string                `json:"started_at"`
	FinishedAt                string                `json:"finished_at,omitempty"`
	Source                    string                `json:"source,omitempty"`
	RuntimeContractID         string                `json:"runtime_contract_id"`
	AgentConnectionMode       string                `json:"agent_connection_mode,omitempty"`
	RuntimeTransport          string                `json:"runtime_transport,omitempty"`
	RuntimeTransportReason    string                `json:"runtime_transport_reason,omitempty"`
	RuntimeTransportChangedAt *time.Time            `json:"runtime_transport_changed_at,omitempty"`
	DispatchState             string                `json:"dispatch_state"`
	AttemptCount              int32                 `json:"attempt_count"`
	MaxAttempts               int32                 `json:"max_attempts"`
	NextAttemptAt             *time.Time            `json:"next_attempt_at,omitempty"`
	LatestAttemptID           string                `json:"latest_attempt_id,omitempty"`
	ActiveAttemptID           string                `json:"active_attempt_id,omitempty"`
	CancelState               string                `json:"cancel_state,omitempty"`
	CancelRequestedAt         *time.Time            `json:"cancel_requested_at,omitempty"`
	CancelAcknowledgedAt      *time.Time            `json:"cancel_acknowledged_at,omitempty"`
	CancelReason              string                `json:"cancel_reason,omitempty"`
	DeadLetteredAt            *time.Time            `json:"dead_lettered_at,omitempty"`
	ReplayOfRunID             string                `json:"replay_of_run_id,omitempty"`
	ParentRunID               string                `json:"parent_run_id,omitempty"`
	ChildCount                int32                 `json:"child_count"`
	CallID                    string                `json:"call_id"`
	A2AContext                *CallRecordA2AContext `json:"a2a_context,omitempty"`
}

type CallRecordListResponse struct {
	Items          []CallRecordItem `json:"items"`
	Total          int32            `json:"total"`
	Page           int32            `json:"page"`
	Size           int32            `json:"size"`
	View           string           `json:"view"`
	Query          string           `json:"query,omitempty"`
	Sort           string           `json:"sort"`
	StatusFilter   string           `json:"status_filter,omitempty"`
	SourceFilter   string           `json:"source_filter,omitempty"`
	RelationFilter string           `json:"relation_filter,omitempty"`
}

type UsageStats struct {
	ThisMonthCalls int32 `json:"this_month_calls"`
	ThisMonthSpent int64 `json:"this_month_spent_cents"`
	TotalCalls     int32 `json:"total_calls"`
}

type CreatorSummary struct {
	ThisMonthCalls   int32 `json:"this_month_calls_received"`
	ThisMonthRevenue int64 `json:"this_month_revenue_cents"`
	TotalAgents      int32 `json:"total_agents"`
	PublicAgents     int32 `json:"public_agents"`
	PendingAgents    int32 `json:"pending_agents"`
}

type UserDashboardResponse struct {
	IsCreator  bool            `json:"is_creator"`
	Usage      UsageStats      `json:"usage"`
	Creator    *CreatorSummary `json:"creator,omitempty"`
	RecentRuns []RunListItem   `json:"recent_runs"`
}

type AgentStatsItem struct {
	ID               string `json:"id"`
	Slug             string `json:"slug"`
	Name             string `json:"name"`
	Status           string `json:"status"`
	PriceCents       int32  `json:"price_per_call_cents"`
	LifetimeCalls    int32  `json:"lifetime_calls"`
	LifetimeRevenue  int64  `json:"lifetime_revenue_cents"`
	CallsThisMonth   int64  `json:"calls_this_month"`
	RevenueThisMonth int64  `json:"revenue_this_month_cents"`
}

type CreatorDashboardResponse struct {
	Summary CreatorSummary   `json:"summary"`
	Agents  []AgentStatsItem `json:"agents"`
}
