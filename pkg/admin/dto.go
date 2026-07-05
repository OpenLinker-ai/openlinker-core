// Package admin exposes core-owned platform administration endpoints.
package admin

// SummaryResponse is the admin console aggregate snapshot.
type SummaryResponse struct {
	TotalUsers             int32 `json:"total_users"`
	AdminUsers             int32 `json:"admin_users"`
	CreatorUsers           int32 `json:"creator_users"`
	VerifiedCreators       int32 `json:"verified_creators"`
	TotalAgents            int32 `json:"total_agents"`
	ActiveAgents           int32 `json:"active_agents"`
	DisabledAgents         int32 `json:"disabled_agents"`
	PendingAgents          int32 `json:"pending_agents"`
	CertifiedAgents        int32 `json:"certified_agents"`
	TotalTasks             int32 `json:"total_tasks"`
	PublicTasks            int32 `json:"public_tasks"`
	PrivateTasks           int32 `json:"private_tasks"`
	OpenTasks              int32 `json:"open_tasks"`
	ClaimedTasks           int32 `json:"claimed_tasks"`
	CompletedTasks         int32 `json:"completed_tasks"`
	AcceptedTasks          int32 `json:"accepted_tasks"`
	RevisionRequestedTasks int32 `json:"revision_requested_tasks"`
}

type UserItem struct {
	ID               string  `json:"id"`
	Email            string  `json:"email"`
	DisplayName      string  `json:"display_name"`
	AvatarURL        string  `json:"avatar_url,omitempty"`
	HasPassword      bool    `json:"has_password"`
	IsOAuthUser      bool    `json:"is_oauth_user"`
	OAuthProvider    string  `json:"oauth_provider,omitempty"`
	AuthMethod       string  `json:"auth_method"`
	IsCreator        bool    `json:"is_creator"`
	CreatorVerified  bool    `json:"creator_verified"`
	IsAdmin          bool    `json:"is_admin"`
	CreatedAt        string  `json:"created_at"`
	UpdatedAt        string  `json:"updated_at"`
	AgentCount       int32   `json:"agent_count"`
	ActiveAgentCount int32   `json:"active_agent_count"`
	TaskCount        int32   `json:"task_count"`
	PublicTaskCount  int32   `json:"public_task_count"`
	RunCount         int32   `json:"run_count"`
	LastTaskAt       *string `json:"last_task_at,omitempty"`
	LastRunAt        *string `json:"last_run_at,omitempty"`
}

type UserListResponse struct {
	Items  []UserItem `json:"items"`
	Total  int32      `json:"total"`
	Limit  int32      `json:"limit"`
	Offset int32      `json:"offset"`
}

type CreateUserRequest struct {
	Email           string `json:"email"`
	DisplayName     string `json:"display_name"`
	Password        string `json:"password"`
	IsAdmin         bool   `json:"is_admin"`
	IsCreator       bool   `json:"is_creator"`
	CreatorVerified bool   `json:"creator_verified"`
}

type UpdateUserFlagsRequest struct {
	IsAdmin         *bool `json:"is_admin"`
	IsCreator       *bool `json:"is_creator"`
	CreatorVerified *bool `json:"creator_verified"`
}

type AgentCreator struct {
	ID          string `json:"id"`
	Email       string `json:"email"`
	DisplayName string `json:"display_name"`
}

type AgentItem struct {
	ID                   string        `json:"id"`
	CreatorID            string        `json:"creator_id"`
	Slug                 string        `json:"slug"`
	Name                 string        `json:"name"`
	Description          string        `json:"description"`
	EndpointURL          string        `json:"endpoint_url"`
	PricePerCallCents    int32         `json:"price_per_call_cents"`
	Tags                 []string      `json:"tags"`
	LifecycleStatus      string        `json:"lifecycle_status"`
	Visibility           string        `json:"visibility"`
	CertificationStatus  string        `json:"certification_status"`
	RejectionReason      *string       `json:"rejection_reason,omitempty"`
	TotalCalls           int32         `json:"total_calls"`
	TotalRevenueCents    int64         `json:"total_revenue_cents"`
	ConnectionMode       string        `json:"connection_mode"`
	MCPToolName          *string       `json:"mcp_tool_name,omitempty"`
	RecommendedTaskCount int32         `json:"recommended_task_count"`
	ChosenTaskCount      int32         `json:"chosen_task_count"`
	ClaimedTaskCount     int32         `json:"claimed_task_count"`
	CompletedTaskCount   int32         `json:"completed_task_count"`
	LastRunAt            *string       `json:"last_run_at,omitempty"`
	CreatedAt            string        `json:"created_at"`
	UpdatedAt            string        `json:"updated_at"`
	CertifiedAt          *string       `json:"certified_at,omitempty"`
	Creator              *AgentCreator `json:"creator,omitempty"`
}

type AgentListResponse struct {
	Items  []AgentItem `json:"items"`
	Total  int32       `json:"total"`
	Limit  int32       `json:"limit"`
	Offset int32       `json:"offset"`
}

type UpdateAgentModerationRequest struct {
	LifecycleStatus     string `json:"lifecycle_status"`
	Visibility          string `json:"visibility"`
	CertificationStatus string `json:"certification_status"`
	RejectionReason     string `json:"rejection_reason"`
}

type TaskUser struct {
	ID          string `json:"id"`
	Email       string `json:"email"`
	DisplayName string `json:"display_name"`
}

type TaskAgent struct {
	ID   string `json:"id"`
	Slug string `json:"slug"`
	Name string `json:"name"`
}

type TaskItem struct {
	ID                    string     `json:"id"`
	UserID                string     `json:"user_id"`
	Query                 string     `json:"query"`
	Visibility            string     `json:"visibility"`
	PublicSummary         *string    `json:"public_summary,omitempty"`
	ParsedSkills          []string   `json:"parsed_skills"`
	MCPTools              []string   `json:"mcp_tools"`
	RecommendedAgentCount int        `json:"recommended_agent_count"`
	Status                string     `json:"status"`
	ChosenAgentID         *string    `json:"chosen_agent_id,omitempty"`
	ChosenAt              *string    `json:"chosen_at,omitempty"`
	ClaimedAgentID        *string    `json:"claimed_agent_id,omitempty"`
	ClaimedByUserID       *string    `json:"claimed_by_user_id,omitempty"`
	ClaimedAt             *string    `json:"claimed_at,omitempty"`
	ClaimRunID            *string    `json:"claim_run_id,omitempty"`
	CompletedAt           *string    `json:"completed_at,omitempty"`
	CompletionSummary     *string    `json:"completion_summary,omitempty"`
	CompletionRunID       *string    `json:"completion_run_id,omitempty"`
	DeliveryStatus        string     `json:"delivery_status"`
	DeliveryVisibility    string     `json:"delivery_visibility"`
	AcceptedAt            *string    `json:"accepted_at,omitempty"`
	RevisionRequestedAt   *string    `json:"revision_requested_at,omitempty"`
	RevisionNote          *string    `json:"revision_note,omitempty"`
	PublishedAt           *string    `json:"published_at,omitempty"`
	CreatedAt             string     `json:"created_at"`
	User                  *TaskUser  `json:"user,omitempty"`
	ChosenAgent           *TaskAgent `json:"chosen_agent,omitempty"`
	ClaimedAgent          *TaskAgent `json:"claimed_agent,omitempty"`
	ClaimedBy             *TaskUser  `json:"claimed_by,omitempty"`
}

type TaskListResponse struct {
	Items  []TaskItem `json:"items"`
	Total  int32      `json:"total"`
	Limit  int32      `json:"limit"`
	Offset int32      `json:"offset"`
}
