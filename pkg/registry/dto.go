package registry

// CreateNodeRequest creates a cloud-side identity for a self-hosted Registry /
// Bridge Node. The returned node_secret is shown once.
type CreateNodeRequest struct {
	NodeName string   `json:"node_name" validate:"required,min=2,max=120"`
	NodeType string   `json:"node_type,omitempty" validate:"omitempty,oneof=self_hosted bridge_proxy"`
	BaseURL  string   `json:"base_url,omitempty" validate:"omitempty,url,max=500"`
	Scopes   []string `json:"scopes,omitempty" validate:"omitempty,max=4,dive,oneof=heartbeat listing:sync proxy:pull proxy:result"`
}

type RegistryNodeResponse struct {
	ID              string   `json:"id"`
	NodeName        string   `json:"node_name"`
	NodeType        string   `json:"node_type"`
	BaseURL         string   `json:"base_url,omitempty"`
	SecretPrefix    string   `json:"secret_prefix"`
	NodeSecret      string   `json:"node_secret,omitempty"`
	Scopes          []string `json:"scopes"`
	HeartbeatStatus string   `json:"heartbeat_status"`
	LastHeartbeatAt string   `json:"last_heartbeat_at,omitempty"`
	RevokedAt       string   `json:"revoked_at,omitempty"`
	CreatedAt       string   `json:"created_at"`
	UpdatedAt       string   `json:"updated_at"`
}

type RegistryNodeListResponse struct {
	Items []RegistryNodeResponse `json:"items"`
}

type RotateNodeSecretResponse struct {
	RegistryNodeResponse
}

type HeartbeatResponse struct {
	NodeID             string `json:"node_id"`
	HeartbeatStatus    string `json:"heartbeat_status"`
	LastHeartbeatAt    string `json:"last_heartbeat_at"`
	LinkedListingCount int32  `json:"linked_listing_count"`
	PendingRunCount    int32  `json:"pending_run_count"`
}

type CreateCloudListingRequest struct {
	RegistryNodeID string `json:"registry_node_id" validate:"required,uuid"`
	AgentID        string `json:"agent_id" validate:"required,uuid"`
	RoutingMode    string `json:"routing_mode,omitempty" validate:"omitempty,oneof=direct_endpoint pull_proxy"`
	PayloadPolicy  string `json:"payload_policy,omitempty" validate:"omitempty,oneof=metadata_only store_run_summary store_full_payload"`
}

type CloudListingLinkResponse struct {
	ID             string `json:"id"`
	CloudListingID string `json:"cloud_listing_id"`
	RegistryNodeID string `json:"registry_node_id"`
	NodeName       string `json:"node_name"`
	AgentID        string `json:"agent_id"`
	AgentSlug      string `json:"agent_slug"`
	AgentName      string `json:"agent_name"`
	RoutingMode    string `json:"routing_mode"`
	PayloadPolicy  string `json:"payload_policy"`
	SyncStatus     string `json:"sync_status"`
	LastSyncAt     string `json:"last_sync_at"`
	CreatedAt      string `json:"created_at"`
	UpdatedAt      string `json:"updated_at"`
}

type CloudListingListResponse struct {
	Items []CloudListingLinkResponse `json:"items"`
}

type UpdateCloudListingStatusRequest struct {
	SyncStatus string `json:"sync_status" validate:"required,oneof=linked paused"`
}

type CreateProxyRunRequest struct {
	CloudListingID string         `json:"cloud_listing_id" validate:"required,uuid"`
	IdempotencyKey string         `json:"idempotency_key,omitempty" validate:"omitempty,min=8,max=160"`
	Input          map[string]any `json:"input,omitempty"`
	InputSummary   string         `json:"input_summary,omitempty" validate:"omitempty,max=500"`
}

type CompleteProxyRunRequest struct {
	Status        string         `json:"status" validate:"required,oneof=success failed timeout"`
	Output        map[string]any `json:"output,omitempty"`
	OutputSummary string         `json:"output_summary,omitempty" validate:"omitempty,max=1000"`
	ErrorCode     string         `json:"error_code,omitempty" validate:"omitempty,max=80"`
	ErrorMessage  string         `json:"error_message,omitempty" validate:"omitempty,max=1000"`
}

type ProxyRunResponse struct {
	ID                 string         `json:"id"`
	CloudRunID         string         `json:"cloud_run_id"`
	CloudListingLinkID string         `json:"cloud_listing_link_id"`
	CloudListingID     string         `json:"cloud_listing_id"`
	RegistryNodeID     string         `json:"registry_node_id"`
	LocalAgentID       string         `json:"local_agent_id"`
	RequestingUserID   string         `json:"requesting_user_id"`
	IdempotencyKey     string         `json:"idempotency_key"`
	Status             string         `json:"status"`
	PayloadPolicy      string         `json:"payload_policy"`
	Input              map[string]any `json:"input,omitempty"`
	InputSummary       string         `json:"input_summary,omitempty"`
	Output             map[string]any `json:"output,omitempty"`
	OutputSummary      string         `json:"output_summary,omitempty"`
	ErrorCode          string         `json:"error_code,omitempty"`
	ErrorMessage       string         `json:"error_message,omitempty"`
	ClaimedAt          string         `json:"claimed_at,omitempty"`
	FinishedAt         string         `json:"finished_at,omitempty"`
	CreatedAt          string         `json:"created_at"`
	UpdatedAt          string         `json:"updated_at"`
}
