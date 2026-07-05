package registry

// CreateNodeRequest creates a registry-side identity for a self-hosted Registry /
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

type HeartbeatResponse struct {
	NodeID             string `json:"node_id"`
	HeartbeatStatus    string `json:"heartbeat_status"`
	LastHeartbeatAt    string `json:"last_heartbeat_at"`
	LinkedListingCount int32  `json:"linked_listing_count"`
	PendingRunCount    int32  `json:"pending_run_count"`
}

type NodeMetadataSyncResponse struct {
	RegistryNodeID     string `json:"registry_node_id"`
	SyncedListingCount int32  `json:"synced_listing_count"`
	SyncedAt           string `json:"synced_at"`
}

type CreateRegistryPeerRequest struct {
	Name          string `json:"name" validate:"required,min=2,max=120"`
	APIBaseURL    string `json:"api_base_url" validate:"required,url,max=500"`
	BearerToken   string `json:"bearer_token" validate:"required,min=8,max=4096"`
	InitialStatus string `json:"initial_status,omitempty" validate:"omitempty,oneof=active paused"`
}

type RegistryPeerResponse struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	APIBaseURL     string `json:"api_base_url"`
	CredentialHint string `json:"credential_hint"`
	Status         string `json:"status"`
	LastUsedAt     string `json:"last_used_at,omitempty"`
	CreatedAt      string `json:"created_at"`
	UpdatedAt      string `json:"updated_at"`
}

type RegistryPeerListResponse struct {
	Items []RegistryPeerResponse `json:"items"`
}

type CreateRegistryFederationInviteRequest struct {
	Name             string `json:"name" validate:"required,min=2,max=120"`
	APIBaseURL       string `json:"api_base_url" validate:"required,url,max=500"`
	BearerToken      string `json:"bearer_token,omitempty" validate:"omitempty,min=8,max=4096"`
	ExpiresInSeconds int32  `json:"expires_in_seconds,omitempty" validate:"omitempty,min=60,max=604800"`
}

type RegistryFederationInviteResponse struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	APIBaseURL      string `json:"api_base_url"`
	CredentialHint  string `json:"credential_hint"`
	Status          string `json:"status"`
	TokenPrefix     string `json:"token_prefix,omitempty"`
	FederationToken string `json:"federation_token,omitempty"`
	ExchangeURL     string `json:"exchange_url"`
	ExpiresAt       string `json:"expires_at"`
	ConsumedAt      string `json:"consumed_at,omitempty"`
	CreatedAt       string `json:"created_at"`
	UpdatedAt       string `json:"updated_at"`
}

type ConsumeRegistryFederationInviteRequest struct {
	FederationToken string `json:"federation_token" validate:"required,min=8,max=4096"`
}

type RegistryFederationExchangeMaterial struct {
	Name           string `json:"name"`
	APIBaseURL     string `json:"api_base_url"`
	BearerToken    string `json:"bearer_token"`
	CredentialHint string `json:"credential_hint"`
	ExpiresAt      string `json:"expires_at"`
}

type ExchangeRegistryFederationInviteRequest struct {
	ExchangeURL     string `json:"exchange_url" validate:"required,url,max=600"`
	FederationToken string `json:"federation_token" validate:"required,min=8,max=4096"`
	Name            string `json:"name,omitempty" validate:"omitempty,min=2,max=120"`
	InitialStatus   string `json:"initial_status,omitempty" validate:"omitempty,oneof=active paused"`
}

type RegistryFederationExchangeResponse struct {
	Peer                 RegistryPeerResponse `json:"peer"`
	ExchangeURL          string               `json:"exchange_url"`
	RemoteCredentialHint string               `json:"remote_credential_hint"`
}

type CreateRegistryListingRequest struct {
	RegistryListingID    string   `json:"registry_listing_id,omitempty" validate:"omitempty,uuid"`
	RegistryNodeID       string   `json:"registry_node_id" validate:"required,uuid"`
	AgentID              string   `json:"agent_id" validate:"required,uuid"`
	RoutingMode          string   `json:"routing_mode,omitempty" validate:"omitempty,oneof=direct_endpoint pull_proxy"`
	PayloadPolicy        string   `json:"payload_policy,omitempty" validate:"omitempty,oneof=metadata_only store_run_summary store_full_payload"`
	PayloadRedactionKeys []string `json:"payload_redaction_keys,omitempty" validate:"omitempty,max=20,dive,min=1,max=80"`
}

type RegistryListingLinkResponse struct {
	ID                   string   `json:"id"`
	RegistryListingID    string   `json:"registry_listing_id"`
	RegistryNodeID       string   `json:"registry_node_id"`
	NodeName             string   `json:"node_name"`
	AgentID              string   `json:"agent_id"`
	AgentSlug            string   `json:"agent_slug"`
	AgentName            string   `json:"agent_name"`
	AgentDescription     string   `json:"agent_description,omitempty"`
	AgentTags            []string `json:"agent_tags,omitempty"`
	AvailabilityStatus   string   `json:"availability_status"`
	MetadataSyncedAt     string   `json:"metadata_synced_at,omitempty"`
	MetadataSyncError    string   `json:"metadata_sync_error,omitempty"`
	RoutingMode          string   `json:"routing_mode"`
	PayloadPolicy        string   `json:"payload_policy"`
	PayloadRedactionKeys []string `json:"payload_redaction_keys,omitempty"`
	SyncStatus           string   `json:"sync_status"`
	LastSyncAt           string   `json:"last_sync_at"`
	CreatedAt            string   `json:"created_at"`
	UpdatedAt            string   `json:"updated_at"`
}

type RegistryListingListResponse struct {
	Items []RegistryListingLinkResponse `json:"items"`
}

type UpdateRegistryListingStatusRequest struct {
	SyncStatus string `json:"sync_status" validate:"required,oneof=linked paused"`
}

type CreateProxyRunRequest struct {
	RegistryListingID string         `json:"registry_listing_id" validate:"required,uuid"`
	IdempotencyKey    string         `json:"idempotency_key,omitempty" validate:"omitempty,min=8,max=160"`
	Input             map[string]any `json:"input,omitempty"`
	InputSummary      string         `json:"input_summary,omitempty" validate:"omitempty,max=500"`
}

type CreateRemoteProxyRunRequest struct {
	RegistryPeerID          string         `json:"registry_peer_id,omitempty" validate:"omitempty,uuid"`
	RemoteAPIBaseURL        string         `json:"remote_api_base_url,omitempty" validate:"omitempty,url,max=500"`
	RemoteBearerToken       string         `json:"remote_bearer_token,omitempty" validate:"omitempty,min=8,max=4096"`
	RemoteRegistryListingID string         `json:"remote_registry_listing_id" validate:"required,uuid"`
	IdempotencyKey          string         `json:"idempotency_key,omitempty" validate:"omitempty,min=8,max=160"`
	Input                   map[string]any `json:"input,omitempty"`
	InputSummary            string         `json:"input_summary,omitempty" validate:"omitempty,max=500"`
}

type CompleteProxyRunRequest struct {
	Status        string         `json:"status" validate:"required,oneof=success failed timeout"`
	Output        map[string]any `json:"output,omitempty"`
	OutputSummary string         `json:"output_summary,omitempty" validate:"omitempty,max=1000"`
	ErrorCode     string         `json:"error_code,omitempty" validate:"omitempty,max=80"`
	ErrorMessage  string         `json:"error_message,omitempty" validate:"omitempty,max=1000"`
	Retryable     bool           `json:"retryable,omitempty"`
	RetryAfterSec int32          `json:"retry_after_seconds,omitempty" validate:"omitempty,min=0,max=3600"`
}

type ProxyRunResponse struct {
	ID                    string         `json:"id"`
	RegistryRunID         string         `json:"registry_run_id"`
	RegistryListingLinkID string         `json:"registry_listing_link_id"`
	RegistryListingID     string         `json:"registry_listing_id"`
	RegistryNodeID        string         `json:"registry_node_id"`
	LocalAgentID          string         `json:"local_agent_id"`
	RequestingUserID      string         `json:"requesting_user_id"`
	IdempotencyKey        string         `json:"idempotency_key"`
	Status                string         `json:"status"`
	PayloadPolicy         string         `json:"payload_policy"`
	Input                 map[string]any `json:"input,omitempty"`
	InputSummary          string         `json:"input_summary,omitempty"`
	Output                map[string]any `json:"output,omitempty"`
	OutputSummary         string         `json:"output_summary,omitempty"`
	ErrorCode             string         `json:"error_code,omitempty"`
	ErrorMessage          string         `json:"error_message,omitempty"`
	AttemptCount          int32          `json:"attempt_count"`
	MaxAttempts           int32          `json:"max_attempts"`
	NextRetryAt           string         `json:"next_retry_at,omitempty"`
	ClaimedAt             string         `json:"claimed_at,omitempty"`
	FinishedAt            string         `json:"finished_at,omitempty"`
	CreatedAt             string         `json:"created_at"`
	UpdatedAt             string         `json:"updated_at"`
}

type RemoteProxyRunResponse struct {
	RemoteAPIBaseURL string           `json:"remote_api_base_url"`
	RegistryPeerID   string           `json:"registry_peer_id,omitempty"`
	RouteMode        string           `json:"route_mode"`
	RemoteRun        ProxyRunResponse `json:"remote_run"`
}

type ProxyRunArtifactResponse struct {
	ID               string         `json:"id"`
	ProxyRunID       string         `json:"proxy_run_id"`
	RegistryRunID    string         `json:"registry_run_id"`
	SourceArtifactID string         `json:"source_artifact_id"`
	ArtifactType     string         `json:"artifact_type"`
	Title            string         `json:"title"`
	Content          map[string]any `json:"content,omitempty"`
	MimeType         string         `json:"mime_type,omitempty"`
	FileURI          string         `json:"file_uri,omitempty"`
	FileName         string         `json:"file_name,omitempty"`
	FileSHA256       string         `json:"file_sha256,omitempty"`
	FileSizeBytes    *int64         `json:"file_size_bytes,omitempty"`
	CreatedAt        string         `json:"created_at"`
}

type ProxyRunArtifactListResponse struct {
	ProxyRunID string                     `json:"proxy_run_id"`
	Items      []ProxyRunArtifactResponse `json:"items"`
}

type ProxyRunArtifactDownload struct {
	ArtifactID  string
	FileName    string
	ContentType string
	SizeBytes   int64
	SHA256      string
	Body        []byte
}
