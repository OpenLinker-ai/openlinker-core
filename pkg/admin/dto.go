// Package admin exposes core-owned platform administration endpoints.
package admin

// SummaryResponse is the admin console aggregate snapshot.
type SummaryResponse struct {
	TotalUsers       int32 `json:"total_users"`
	AdminUsers       int32 `json:"admin_users"`
	CreatorUsers     int32 `json:"creator_users"`
	VerifiedCreators int32 `json:"verified_creators"`
	TotalAgents      int32 `json:"total_agents"`
	ActiveAgents     int32 `json:"active_agents"`
	DisabledAgents   int32 `json:"disabled_agents"`
	PendingAgents    int32 `json:"pending_agents"`
	CertifiedAgents  int32 `json:"certified_agents"`
}

type UserItem struct {
	ID              string `json:"id"`
	Email           string `json:"email"`
	DisplayName     string `json:"display_name"`
	AvatarURL       string `json:"avatar_url,omitempty"`
	IsCreator       bool   `json:"is_creator"`
	CreatorVerified bool   `json:"creator_verified"`
	IsAdmin         bool   `json:"is_admin"`
	CreatedAt       string `json:"created_at"`
	UpdatedAt       string `json:"updated_at"`
}

type UserListResponse struct {
	Items  []UserItem `json:"items"`
	Total  int32      `json:"total"`
	Limit  int32      `json:"limit"`
	Offset int32      `json:"offset"`
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
	ID                  string        `json:"id"`
	CreatorID           string        `json:"creator_id"`
	Slug                string        `json:"slug"`
	Name                string        `json:"name"`
	Description         string        `json:"description"`
	EndpointURL         string        `json:"endpoint_url"`
	PricePerCallCents   int32         `json:"price_per_call_cents"`
	Tags                []string      `json:"tags"`
	LifecycleStatus     string        `json:"lifecycle_status"`
	Visibility          string        `json:"visibility"`
	CertificationStatus string        `json:"certification_status"`
	RejectionReason     *string       `json:"rejection_reason,omitempty"`
	TotalCalls          int32         `json:"total_calls"`
	TotalRevenueCents   int64         `json:"total_revenue_cents"`
	ConnectionMode      string        `json:"connection_mode"`
	MCPToolName         *string       `json:"mcp_tool_name,omitempty"`
	CreatedAt           string        `json:"created_at"`
	UpdatedAt           string        `json:"updated_at"`
	CertifiedAt         *string       `json:"certified_at,omitempty"`
	Creator             *AgentCreator `json:"creator,omitempty"`
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
