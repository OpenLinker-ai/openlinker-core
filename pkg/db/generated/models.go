// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成。

package db

import (
	"time"

	"github.com/google/uuid"
)

// User 对应 users 表。
// password_hash / oauth_provider / oauth_id / avatar_url / deleted_at 可空。
type User struct {
	ID              uuid.UUID  `db:"id" json:"id"`
	Email           string     `db:"email" json:"email"`
	PasswordHash    *string    `db:"password_hash" json:"password_hash"`
	OauthProvider   *string    `db:"oauth_provider" json:"oauth_provider"`
	OauthID         *string    `db:"oauth_id" json:"oauth_id"`
	DisplayName     string     `db:"display_name" json:"display_name"`
	AvatarURL       *string    `db:"avatar_url" json:"avatar_url"`
	IsCreator       bool       `db:"is_creator" json:"is_creator"`
	CreatorVerified bool       `db:"creator_verified" json:"creator_verified"`
	IsAdmin         bool       `db:"is_admin" json:"is_admin"`
	CreatedAt       time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt       time.Time  `db:"updated_at" json:"updated_at"`
	DeletedAt       *time.Time `db:"deleted_at" json:"deleted_at"`
}

// Agent 对应 agents 表（Phase 2 缺口 2 后的三维状态机模型）。
//
// lifecycle_status     active | disabled                              （docs/29 §三）
// visibility           public | unlisted | private
// certification_status unreviewed | pending | certified | rejected
// 金额：price_per_call_cents 用 int32（INTEGER）；累计 total_revenue_cents 用 int64（BIGINT）。
// tags 用 []string（Postgres TEXT[]）。
// webhook_url / webhook_secret 子轮 2.1 引入，可为 NULL（未配置）。
type Agent struct {
	ID                  uuid.UUID  `db:"id" json:"id"`
	CreatorID           uuid.UUID  `db:"creator_id" json:"creator_id"`
	Slug                string     `db:"slug" json:"slug"`
	Name                string     `db:"name" json:"name"`
	Description         string     `db:"description" json:"description"`
	EndpointURL         string     `db:"endpoint_url" json:"endpoint_url"`
	EndpointAuthHeader  *string    `db:"endpoint_auth_header" json:"endpoint_auth_header"`
	PricePerCallCents   int32      `db:"price_per_call_cents" json:"price_per_call_cents"`
	Tags                []string   `db:"tags" json:"tags"`
	LifecycleStatus     string     `db:"lifecycle_status" json:"lifecycle_status"`
	Visibility          string     `db:"visibility" json:"visibility"`
	CertificationStatus string     `db:"certification_status" json:"certification_status"`
	RejectionReason     *string    `db:"rejection_reason" json:"rejection_reason"`
	CertifiedAt         *time.Time `db:"certified_at" json:"certified_at"`
	TotalCalls          int32      `db:"total_calls" json:"total_calls"`
	TotalRevenueCents   int64      `db:"total_revenue_cents" json:"total_revenue_cents"`
	WebhookURL          *string    `db:"webhook_url" json:"webhook_url"`
	WebhookSecret       *string    `db:"webhook_secret" json:"-"` // 不暴露给前端
	ConnectionMode      string     `db:"connection_mode" json:"connection_mode"`
	MCPToolName         *string    `db:"mcp_tool_name" json:"mcp_tool_name"`
	CreatedAt           time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt           time.Time  `db:"updated_at" json:"updated_at"`
}

// Run 对应 runs 表（每次调用一行）。
//
// status: 'running' | 'success' | 'failed' | 'timeout' | 'canceled'
// 金额单位 cents（int32）：cost_cents = platform_fee_cents + creator_revenue_cents
// input/output 是 JSONB（[]byte，原样透传）
type Run struct {
	ID                      uuid.UUID  `db:"id" json:"id"`
	UserID                  uuid.UUID  `db:"user_id" json:"user_id"`
	AgentID                 uuid.UUID  `db:"agent_id" json:"agent_id"`
	Input                   []byte     `db:"input" json:"input"`
	Output                  []byte     `db:"output" json:"output"`
	Status                  string     `db:"status" json:"status"`
	ErrorCode               *string    `db:"error_code" json:"error_code"`
	ErrorMessage            *string    `db:"error_message" json:"error_message"`
	CostCents               int32      `db:"cost_cents" json:"cost_cents"`
	PlatformFeeCents        int32      `db:"platform_fee_cents" json:"platform_fee_cents"`
	CreatorRevenueCents     int32      `db:"creator_revenue_cents" json:"creator_revenue_cents"`
	DurationMs              *int32     `db:"duration_ms" json:"duration_ms"`
	StartedAt               time.Time  `db:"started_at" json:"started_at"`
	FinishedAt              *time.Time `db:"finished_at" json:"finished_at"`
	Source                  string     `db:"source" json:"source"`
	ClaimedByRuntimeTokenID *uuid.UUID `db:"claimed_by_runtime_token_id" json:"claimed_by_runtime_token_id"`
	ClaimedAt               *time.Time `db:"claimed_at" json:"claimed_at"`
}

// RunEvent 对应 run_events 表。
//
// sequence 在单个 run 内递增，用于 SSE Last-Event-ID / 断线续传。
// payload 是 JSONB 原始字节，调用方按 event_type 解析。
type RunEvent struct {
	ID          uuid.UUID  `db:"id" json:"id"`
	RunID       uuid.UUID  `db:"run_id" json:"run_id"`
	ParentRunID *uuid.UUID `db:"parent_run_id" json:"parent_run_id"`
	Sequence    int32      `db:"sequence" json:"sequence"`
	EventType   string     `db:"event_type" json:"event_type"`
	Payload     []byte     `db:"payload" json:"payload"`
	CreatedAt   time.Time  `db:"created_at" json:"created_at"`
}

// RunArtifact 对应 run_artifacts 表。
//
// artifact_type: json | text | file | data
// visibility: private | shared | public_example
type RunArtifact struct {
	ID               uuid.UUID `db:"id" json:"id"`
	RunID            uuid.UUID `db:"run_id" json:"run_id"`
	ArtifactType     string    `db:"artifact_type" json:"artifact_type"`
	Title            string    `db:"title" json:"title"`
	Content          []byte    `db:"content" json:"content"`
	Visibility       string    `db:"visibility" json:"visibility"`
	SourceArtifactID *string   `db:"source_artifact_id" json:"source_artifact_id"`
	MimeType         *string   `db:"mime_type" json:"mime_type"`
	FileUri          *string   `db:"file_uri" json:"file_uri"`
	FileName         *string   `db:"file_name" json:"file_name"`
	FileSha256       *string   `db:"file_sha256" json:"file_sha256"`
	FileSizeBytes    *int64    `db:"file_size_bytes" json:"file_size_bytes"`
	CreatedAt        time.Time `db:"created_at" json:"created_at"`
}

// RunArtifactChunk 对应 run_artifact_chunks 表。
type RunArtifactChunk struct {
	ID               uuid.UUID `db:"id" json:"id"`
	RunID            uuid.UUID `db:"run_id" json:"run_id"`
	RunArtifactID    uuid.UUID `db:"run_artifact_id" json:"run_artifact_id"`
	SourceArtifactID string    `db:"source_artifact_id" json:"source_artifact_id"`
	EventSequence    *int32    `db:"event_sequence" json:"event_sequence"`
	ChunkIndex       int32     `db:"chunk_index" json:"chunk_index"`
	Append           bool      `db:"append" json:"append"`
	LastChunk        bool      `db:"last_chunk" json:"last_chunk"`
	Parts            []byte    `db:"parts" json:"parts"`
	Payload          []byte    `db:"payload" json:"payload"`
	PartsSha256      *string   `db:"parts_sha256" json:"parts_sha256"`
	PayloadSha256    *string   `db:"payload_sha256" json:"payload_sha256"`
	DeclaredSha256   *string   `db:"declared_sha256" json:"declared_sha256"`
	ChecksumStatus   string    `db:"checksum_status" json:"checksum_status"`
	CreatedAt        time.Time `db:"created_at" json:"created_at"`
}

// RunMessage 对应 run_messages 表。
//
// role: user | agent | tool | platform
type RunMessage struct {
	ID            uuid.UUID `db:"id" json:"id"`
	RunID         uuid.UUID `db:"run_id" json:"run_id"`
	EventSequence *int32    `db:"event_sequence" json:"event_sequence"`
	Role          string    `db:"role" json:"role"`
	Content       string    `db:"content" json:"content"`
	Payload       []byte    `db:"payload" json:"payload"`
	CreatedAt     time.Time `db:"created_at" json:"created_at"`
}

// RunRequirementEvidence 对应 run_requirement_evidence 表。
//
// 它把任务发布时声明的 Skill/MCP 要求快照到一次实际 run，避免运行完成后
// 只能从临时 metadata 猜测这次调用是否覆盖任务要求。
type RunRequirementEvidence struct {
	RunID            uuid.UUID `db:"run_id" json:"run_id"`
	TaskID           uuid.UUID `db:"task_id" json:"task_id"`
	AgentID          uuid.UUID `db:"agent_id" json:"agent_id"`
	UserID           uuid.UUID `db:"user_id" json:"user_id"`
	RequiredSkillIDs []string  `db:"required_skill_ids" json:"required_skill_ids"`
	RequiredMCPTools []string  `db:"required_mcp_tools" json:"required_mcp_tools"`
	AgentSkillIDs    []string  `db:"agent_skill_ids" json:"agent_skill_ids"`
	MatchedSkillIDs  []string  `db:"matched_skill_ids" json:"matched_skill_ids"`
	MissingSkillIDs  []string  `db:"missing_skill_ids" json:"missing_skill_ids"`
	UsedMCPTools     []string  `db:"used_mcp_tools" json:"used_mcp_tools"`
	MissingMCPTools  []string  `db:"missing_mcp_tools" json:"missing_mcp_tools"`
	CoverageStatus   string    `db:"coverage_status" json:"coverage_status"`
	EvidenceSource   string    `db:"evidence_source" json:"evidence_source"`
	CreatedAt        time.Time `db:"created_at" json:"created_at"`
}

// AgentActionApprovalRequest Phase 2 缺口 2：高风险动作审批记录（docs/29 §3.4）。
//
// status: 'pending' | 'confirmed' | 'rejected' | 'expired'
// PayloadJSON 是 JSONB 原始字节，调用方按 action 解析。
// RequestedByTokenID 允许 NULL：人类直接发起的审批不绑 Runtime Token。
type AgentActionApprovalRequest struct {
	ID                 uuid.UUID  `db:"id" json:"id"`
	AgentID            uuid.UUID  `db:"agent_id" json:"agent_id"`
	RequestedByUserID  *uuid.UUID `db:"requested_by_user_id" json:"requested_by_user_id"`
	RequestedByTokenID *uuid.UUID `db:"requested_by_token_id" json:"requested_by_token_id"`
	Action             string     `db:"action" json:"action"`
	PayloadJSON        []byte     `db:"payload_json" json:"payload_json"`
	Status             string     `db:"status" json:"status"`
	ApprovalURLSlug    string     `db:"approval_url_slug" json:"approval_url_slug"`
	ExpiresAt          time.Time  `db:"expires_at" json:"expires_at"`
	DecidedAt          *time.Time `db:"decided_at" json:"decided_at"`
	DecidedByUserID    *uuid.UUID `db:"decided_by_user_id" json:"decided_by_user_id"`
	DecisionNote       *string    `db:"decision_note" json:"decision_note"`
	CreatedAt          time.Time  `db:"created_at" json:"created_at"`
}

// AgentMetricSnapshot Phase 2 缺口 2：Agent 指标快照（docs/29 §3.4）。
// 由 metric worker 每 5 分钟为每个 active Agent × {24h, 7d, 30d} upsert 一行。
type AgentMetricSnapshot struct {
	AgentID         uuid.UUID `db:"agent_id" json:"agent_id"`
	TimeWindow      string    `db:"time_window" json:"time_window"`
	CallCount       int32     `db:"call_count" json:"call_count"`
	SuccessCount    int32     `db:"success_count" json:"success_count"`
	FailureCount    int32     `db:"failure_count" json:"failure_count"`
	SuccessRateBps  int32     `db:"success_rate_bps" json:"success_rate_bps"`
	MedianLatencyMs *int32    `db:"median_latency_ms" json:"median_latency_ms"`
	P95LatencyMs    *int32    `db:"p95_latency_ms" json:"p95_latency_ms"`
	SnapshottedAt   time.Time `db:"snapshotted_at" json:"snapshotted_at"`
}

// AgentAvailabilitySnapshot 对应 agent_availability_snapshots 表。
//
// availability_status: unknown | healthy | degraded | unreachable
type AgentAvailabilitySnapshot struct {
	AgentID             uuid.UUID  `db:"agent_id" json:"agent_id"`
	AvailabilityStatus  string     `db:"availability_status" json:"availability_status"`
	LastSuccessfulRunAt *time.Time `db:"last_successful_run_at" json:"last_successful_run_at"`
	LastFailedRunAt     *time.Time `db:"last_failed_run_at" json:"last_failed_run_at"`
	LastCheckedAt       *time.Time `db:"last_checked_at" json:"last_checked_at"`
	ConsecutiveFailures int32      `db:"consecutive_failures" json:"consecutive_failures"`
	UpdatedAt           time.Time  `db:"updated_at" json:"updated_at"`
}

// AgentAvailabilityAlert 是创作者侧站内可用性告警。
type AgentAvailabilityAlert struct {
	ID                  uuid.UUID  `db:"id" json:"id"`
	AgentID             uuid.UUID  `db:"agent_id" json:"agent_id"`
	CreatorID           uuid.UUID  `db:"creator_id" json:"creator_id"`
	AlertType           string     `db:"alert_type" json:"alert_type"`
	Severity            string     `db:"severity" json:"severity"`
	AvailabilityStatus  string     `db:"availability_status" json:"availability_status"`
	ConsecutiveFailures int32      `db:"consecutive_failures" json:"consecutive_failures"`
	Title               string     `db:"title" json:"title"`
	Message             string     `db:"message" json:"message"`
	LastError           *string    `db:"last_error" json:"last_error"`
	RepairHints         []string   `db:"repair_hints" json:"repair_hints"`
	ReadAt              *time.Time `db:"read_at" json:"read_at"`
	CreatedAt           time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt           time.Time  `db:"updated_at" json:"updated_at"`
}

// RegistryNode 对应 registry_nodes 表。
//
// 这是 Bridge / Proxy Node 的代理身份；明文 secret 只在创建时返回，数据库只保存 hash。
type RegistryNode struct {
	ID              uuid.UUID  `db:"id" json:"id"`
	OwnerUserID     uuid.UUID  `db:"owner_user_id" json:"owner_user_id"`
	NodeName        string     `db:"node_name" json:"node_name"`
	NodeType        string     `db:"node_type" json:"node_type"`
	BaseURL         *string    `db:"base_url" json:"base_url"`
	SecretPrefix    string     `db:"secret_prefix" json:"secret_prefix"`
	SecretHash      string     `db:"secret_hash" json:"-"`
	Scopes          []string   `db:"scopes" json:"scopes"`
	HeartbeatStatus string     `db:"heartbeat_status" json:"heartbeat_status"`
	LastHeartbeatAt *time.Time `db:"last_heartbeat_at" json:"last_heartbeat_at"`
	RevokedAt       *time.Time `db:"revoked_at" json:"revoked_at"`
	CreatedAt       time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt       time.Time  `db:"updated_at" json:"updated_at"`
}

// RegistryPeer stores trusted remote registry credentials for federated proxy
// routing. BearerToken is intentionally omitted from JSON output.
type RegistryPeer struct {
	ID             uuid.UUID  `db:"id" json:"id"`
	OwnerUserID    uuid.UUID  `db:"owner_user_id" json:"owner_user_id"`
	Name           string     `db:"name" json:"name"`
	APIBaseURL     string     `db:"api_base_url" json:"api_base_url"`
	BearerToken    string     `db:"bearer_token" json:"-"`
	CredentialHint string     `db:"credential_hint" json:"credential_hint"`
	Status         string     `db:"status" json:"status"`
	LastUsedAt     *time.Time `db:"last_used_at" json:"last_used_at"`
	CreatedAt      time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt      time.Time  `db:"updated_at" json:"updated_at"`
}

// RegistryFederationInvite is a one-time token exchange record used to create
// a RegistryPeer without copying the remote bearer token by hand.
type RegistryFederationInvite struct {
	ID             uuid.UUID  `db:"id" json:"id"`
	OwnerUserID    uuid.UUID  `db:"owner_user_id" json:"owner_user_id"`
	Name           string     `db:"name" json:"name"`
	APIBaseURL     string     `db:"api_base_url" json:"api_base_url"`
	BearerToken    string     `db:"bearer_token" json:"-"`
	TokenPrefix    string     `db:"token_prefix" json:"token_prefix"`
	TokenHash      string     `db:"token_hash" json:"-"`
	CredentialHint string     `db:"credential_hint" json:"credential_hint"`
	Status         string     `db:"status" json:"status"`
	ExpiresAt      time.Time  `db:"expires_at" json:"expires_at"`
	ConsumedAt     *time.Time `db:"consumed_at" json:"consumed_at"`
	CreatedAt      time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt      time.Time  `db:"updated_at" json:"updated_at"`
}

// RegistryListingLink 对应 registry_listing_links 表。
//
// 它表达“用户显式把某个本地 Agent 暴露成 Registry Listing”的关系。
type RegistryListingLink struct {
	ID                       uuid.UUID  `db:"id" json:"id"`
	RegistryListingID        uuid.UUID  `db:"registry_listing_id" json:"registry_listing_id"`
	RegistryNodeID           uuid.UUID  `db:"registry_node_id" json:"registry_node_id"`
	LocalAgentID             uuid.UUID  `db:"local_agent_id" json:"local_agent_id"`
	RoutingMode              string     `db:"routing_mode" json:"routing_mode"`
	PayloadPolicy            string     `db:"payload_policy" json:"payload_policy"`
	PayloadRedactionKeys     []string   `db:"payload_redaction_keys" json:"payload_redaction_keys"`
	SyncStatus               string     `db:"sync_status" json:"sync_status"`
	SyncedAgentSlug          string     `db:"synced_agent_slug" json:"synced_agent_slug"`
	SyncedAgentName          string     `db:"synced_agent_name" json:"synced_agent_name"`
	SyncedAgentDescription   string     `db:"synced_agent_description" json:"synced_agent_description"`
	SyncedAgentTags          []string   `db:"synced_agent_tags" json:"synced_agent_tags"`
	SyncedAvailabilityStatus string     `db:"synced_availability_status" json:"synced_availability_status"`
	MetadataSyncedAt         *time.Time `db:"metadata_synced_at" json:"metadata_synced_at"`
	MetadataSyncError        *string    `db:"metadata_sync_error" json:"metadata_sync_error"`
	LastSyncAt               time.Time  `db:"last_sync_at" json:"last_sync_at"`
	CreatedAt                time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt                time.Time  `db:"updated_at" json:"updated_at"`
}

// ProxyRun 对应 proxy_runs 表。
//
// 它是 Registry Listing 经 Registry Node 拉取执行的代理任务状态机。
type ProxyRun struct {
	ID                    uuid.UUID  `db:"id" json:"id"`
	RegistryRunID         uuid.UUID  `db:"registry_run_id" json:"registry_run_id"`
	RegistryListingLinkID uuid.UUID  `db:"registry_listing_link_id" json:"registry_listing_link_id"`
	RegistryListingID     uuid.UUID  `db:"registry_listing_id" json:"registry_listing_id"`
	RegistryNodeID        uuid.UUID  `db:"registry_node_id" json:"registry_node_id"`
	LocalAgentID          uuid.UUID  `db:"local_agent_id" json:"local_agent_id"`
	RequestingUserID      uuid.UUID  `db:"requesting_user_id" json:"requesting_user_id"`
	IdempotencyKey        string     `db:"idempotency_key" json:"idempotency_key"`
	Status                string     `db:"status" json:"status"`
	PayloadPolicy         string     `db:"payload_policy" json:"payload_policy"`
	PayloadRedactionKeys  []string   `db:"payload_redaction_keys" json:"payload_redaction_keys"`
	Input                 []byte     `db:"input" json:"input"`
	InputSummary          *string    `db:"input_summary" json:"input_summary"`
	Output                []byte     `db:"output" json:"output"`
	OutputSummary         *string    `db:"output_summary" json:"output_summary"`
	ErrorCode             *string    `db:"error_code" json:"error_code"`
	ErrorMessage          *string    `db:"error_message" json:"error_message"`
	AttemptCount          int32      `db:"attempt_count" json:"attempt_count"`
	MaxAttempts           int32      `db:"max_attempts" json:"max_attempts"`
	NextRetryAt           *time.Time `db:"next_retry_at" json:"next_retry_at"`
	ClaimedAt             *time.Time `db:"claimed_at" json:"claimed_at"`
	FinishedAt            *time.Time `db:"finished_at" json:"finished_at"`
	CreatedAt             time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt             time.Time  `db:"updated_at" json:"updated_at"`
}

// ProxyRunArtifact stores artifact references returned by a Registry Node for a
// proxy run. It deliberately keeps file metadata separate from proxy_runs.output
// so metadata_only listings can still expose downloadable result references.
type ProxyRunArtifact struct {
	ID               uuid.UUID `db:"id" json:"id"`
	ProxyRunID       uuid.UUID `db:"proxy_run_id" json:"proxy_run_id"`
	RegistryRunID    uuid.UUID `db:"registry_run_id" json:"registry_run_id"`
	SourceArtifactID string    `db:"source_artifact_id" json:"source_artifact_id"`
	ArtifactType     string    `db:"artifact_type" json:"artifact_type"`
	Title            string    `db:"title" json:"title"`
	Content          []byte    `db:"content" json:"content"`
	MimeType         *string   `db:"mime_type" json:"mime_type"`
	FileURI          *string   `db:"file_uri" json:"file_uri"`
	FileName         *string   `db:"file_name" json:"file_name"`
	FileSHA256       *string   `db:"file_sha256" json:"file_sha256"`
	FileSizeBytes    *int64    `db:"file_size_bytes" json:"file_size_bytes"`
	CreatedAt        time.Time `db:"created_at" json:"created_at"`
}

// AgentToken 是统一 Agent 接入凭证。pending_registration 阶段 agent_id 为空；
// 注册成功后同一 token 转为 active_runtime 并绑定单个 Agent。
type AgentToken struct {
	ID            uuid.UUID  `db:"id" json:"id"`
	AgentID       *uuid.UUID `db:"agent_id" json:"agent_id"`
	CreatorUserID uuid.UUID  `db:"creator_user_id" json:"creator_user_id"`
	Name          string     `db:"name" json:"name"`
	Prefix        string     `db:"prefix" json:"prefix"`
	TokenHash     string     `db:"token_hash" json:"-"`
	Scopes        []string   `db:"scopes" json:"scopes"`
	Status        string     `db:"status" json:"status"`
	ExpiresAt     *time.Time `db:"expires_at" json:"expires_at"`
	RedeemedAt    *time.Time `db:"redeemed_at" json:"redeemed_at"`
	LastUsedAt    *time.Time `db:"last_used_at" json:"last_used_at"`
	RevokedAt     *time.Time `db:"revoked_at" json:"revoked_at"`
	CreatedAt     time.Time  `db:"created_at" json:"created_at"`
}

// AgentRuntimeToken 是绑定单个 Agent 的自动化调用凭证。
// 明文 token 仅创建时返回，数据库只保存 hash 与显示用 prefix。
type AgentRuntimeToken struct {
	ID              uuid.UUID  `db:"id" json:"id"`
	AgentID         uuid.UUID  `db:"agent_id" json:"agent_id"`
	CreatedByUserID uuid.UUID  `db:"created_by_user_id" json:"created_by_user_id"`
	Name            string     `db:"name" json:"name"`
	Prefix          string     `db:"prefix" json:"prefix"`
	TokenHash       string     `db:"token_hash" json:"-"`
	Scopes          []string   `db:"scopes" json:"scopes"`
	LastUsedAt      *time.Time `db:"last_used_at" json:"last_used_at"`
	RevokedAt       *time.Time `db:"revoked_at" json:"revoked_at"`
	CreatedAt       time.Time  `db:"created_at" json:"created_at"`
}

// AgentCallPolicy 控制目标 Agent 是否接受来自其他 Agent 的平台代理调用。
type AgentCallPolicy struct {
	AgentID    uuid.UUID `db:"agent_id" json:"agent_id"`
	CallableBy string    `db:"callable_by" json:"callable_by"`
	UpdatedAt  time.Time `db:"updated_at" json:"updated_at"`
}

// RunDelegation 将被调用的 child run 关联到发起委派的 parent run。
type RunDelegation struct {
	ChildRunID    uuid.UUID `db:"child_run_id" json:"child_run_id"`
	ParentRunID   uuid.UUID `db:"parent_run_id" json:"parent_run_id"`
	CallerAgentID uuid.UUID `db:"caller_agent_id" json:"caller_agent_id"`
	Reason        string    `db:"reason" json:"reason"`
	CreatedAt     time.Time `db:"created_at" json:"created_at"`
}

// A2AContextMapping links protocol context/task ids to OpenLinker run lineage.
type A2AContextMapping struct {
	ID                uuid.UUID  `db:"id" json:"id"`
	RunID             uuid.UUID  `db:"run_id" json:"run_id"`
	UserID            uuid.UUID  `db:"user_id" json:"user_id"`
	AgentID           uuid.UUID  `db:"agent_id" json:"agent_id"`
	ProtocolContextID string     `db:"protocol_context_id" json:"protocol_context_id"`
	ProtocolTaskID    string     `db:"protocol_task_id" json:"protocol_task_id"`
	RootContextID     string     `db:"root_context_id" json:"root_context_id"`
	ParentContextID   string     `db:"parent_context_id" json:"parent_context_id"`
	ParentTaskID      string     `db:"parent_task_id" json:"parent_task_id"`
	ParentRunID       *uuid.UUID `db:"parent_run_id" json:"parent_run_id"`
	CallerAgentID     *uuid.UUID `db:"caller_agent_id" json:"caller_agent_id"`
	TargetAgentID     *uuid.UUID `db:"target_agent_id" json:"target_agent_id"`
	TraceID           string     `db:"trace_id" json:"trace_id"`
	ReferenceTaskIDs  []string   `db:"reference_task_ids" json:"reference_task_ids"`
	Source            string     `db:"source" json:"source"`
	CreatedAt         time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt         time.Time  `db:"updated_at" json:"updated_at"`
}

// Skill 对应 skills 表（平台维护的 30 个核心 skill）。
//
// id 形如 "content/translation" / "dev/code-review"；category 是 id 前的目录名。
// description 用于 LLM 解析任务描述时做语义匹配。
type Skill struct {
	ID          string    `db:"id" json:"id"`
	Category    string    `db:"category" json:"category"`
	Name        string    `db:"name" json:"name"`
	Description string    `db:"description" json:"description"`
	SortOrder   int32     `db:"sort_order" json:"sort_order"`
	CreatedAt   time.Time `db:"created_at" json:"created_at"`
}

// SkillProposal 对应 skill_proposals 表。
//
// 用户提交缺失 Skill 或导入 Agent 声明时写入，平台后续可合并到 skills。
// 当 proposed_skill_id 已存在时 status=merged，并通过 matched_skill_id 指向内置 Skill。
type SkillProposal struct {
	ID              uuid.UUID  `db:"id" json:"id"`
	OwnerUserID     uuid.UUID  `db:"owner_user_id" json:"owner_user_id"`
	AgentID         *uuid.UUID `db:"agent_id" json:"agent_id"`
	ProposedSkillID string     `db:"proposed_skill_id" json:"proposed_skill_id"`
	Category        string     `db:"category" json:"category"`
	Name            string     `db:"name" json:"name"`
	Description     string     `db:"description" json:"description"`
	Source          string     `db:"source" json:"source"`
	Status          string     `db:"status" json:"status"`
	MatchedSkillID  *string    `db:"matched_skill_id" json:"matched_skill_id"`
	CreatedAt       time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt       time.Time  `db:"updated_at" json:"updated_at"`
}

// AgentSkill 对应 agent_skills 关联表。
type AgentSkill struct {
	AgentID   uuid.UUID `db:"agent_id" json:"agent_id"`
	SkillID   string    `db:"skill_id" json:"skill_id"`
	CreatedAt time.Time `db:"created_at" json:"created_at"`
}

// TaskQuery 对应 task_queries 表（任务驱动 A 形态）。
//
// query 是用户自然语言描述；parsed_skills 是关联/解析出的 skill_id 列表；mcp_tools 是任务关联的 MCP 工具名。
// recommended_agent_ids 按推荐顺序保存（top 3）；chosen_agent_id 记录用户最终选择（可空）。
// claimed_* / completion_* 让任务广场形成"接单 -> 运行 -> 结果回填"的最小闭环。
// delivery_* / accepted_at / revision_* 记录任务交付、验收和修订状态。
// visibility / public_summary / published_at 区分私有推荐草稿和显式公开任务。
type TaskQuery struct {
	ID                  uuid.UUID   `db:"id" json:"id"`
	UserID              uuid.UUID   `db:"user_id" json:"user_id"`
	Query               string      `db:"query" json:"query"`
	ParsedSkills        []string    `db:"parsed_skills" json:"parsed_skills"`
	MCPTools            []string    `db:"mcp_tools" json:"mcp_tools"`
	RecommendedAgentIDs []uuid.UUID `db:"recommended_agent_ids" json:"recommended_agent_ids"`
	ChosenAgentID       *uuid.UUID  `db:"chosen_agent_id" json:"chosen_agent_id"`
	ChosenAt            *time.Time  `db:"chosen_at" json:"chosen_at"`
	ClaimedAgentID      *uuid.UUID  `db:"claimed_agent_id" json:"claimed_agent_id"`
	ClaimedByUserID     *uuid.UUID  `db:"claimed_by_user_id" json:"claimed_by_user_id"`
	ClaimedAt           *time.Time  `db:"claimed_at" json:"claimed_at"`
	ClaimRunID          *uuid.UUID  `db:"claim_run_id" json:"claim_run_id"`
	CompletedAt         *time.Time  `db:"completed_at" json:"completed_at"`
	CompletionSummary   *string     `db:"completion_summary" json:"completion_summary"`
	CompletionRunID     *uuid.UUID  `db:"completion_run_id" json:"completion_run_id"`
	DeliveryStatus      string      `db:"delivery_status" json:"delivery_status"`
	DeliveryVisibility  string      `db:"delivery_visibility" json:"delivery_visibility"`
	DeliveryArtifact    []byte      `db:"delivery_artifact" json:"delivery_artifact"`
	AcceptedAt          *time.Time  `db:"accepted_at" json:"accepted_at"`
	RevisionRequestedAt *time.Time  `db:"revision_requested_at" json:"revision_requested_at"`
	RevisionNote        *string     `db:"revision_note" json:"revision_note"`
	Visibility          string      `db:"visibility" json:"visibility"`
	PublicSummary       *string     `db:"public_summary" json:"public_summary"`
	PublishedAt         *time.Time  `db:"published_at" json:"published_at"`
	CreatedAt           time.Time   `db:"created_at" json:"created_at"`
}

// AgentCapability 对应 agent_capabilities 表。
//
// 模块 A（Phase 2 §4）：当前版本的能力声明（JSON Schema input/output）。
// input_schema / output_schema 是 JSONB（[]byte，原样透传，业务层负责解码 + 校验 draft-07）。
// version 在 upsert 时自增；published_at 反映最近一次发布时间。
type AgentCapability struct {
	ID           uuid.UUID `db:"id" json:"id"`
	AgentID      uuid.UUID `db:"agent_id" json:"agent_id"`
	InputSchema  []byte    `db:"input_schema" json:"input_schema"`
	OutputSchema []byte    `db:"output_schema" json:"output_schema"`
	Summary      string    `db:"summary" json:"summary"`
	Version      int32     `db:"version" json:"version"`
	PublishedAt  time.Time `db:"published_at" json:"published_at"`
	UpdatedAt    time.Time `db:"updated_at" json:"updated_at"`
}

// AgentExample 对应 agent_examples 表。
//
// input_json 必填；expected_output_json 可空（dry-run 仅校验结构，benchmark 才比对）。
// sort_order 决定展示与 dry-run 取首条的顺序。
type AgentExample struct {
	ID                 uuid.UUID `db:"id" json:"id"`
	AgentID            uuid.UUID `db:"agent_id" json:"agent_id"`
	Title              string    `db:"title" json:"title"`
	InputJSON          []byte    `db:"input_json" json:"input_json"`
	ExpectedOutputJSON []byte    `db:"expected_output_json" json:"expected_output_json"`
	SortOrder          int32     `db:"sort_order" json:"sort_order"`
	CreatedAt          time.Time `db:"created_at" json:"created_at"`
	UpdatedAt          time.Time `db:"updated_at" json:"updated_at"`
}

// AgentOnboardingStatus 对应 agent_onboarding_status 表。
//
// 与 agents 1:1。endpoint_set 在 CreateAgent 后由 EnsureOnboardingStatus 写入 TRUE。
// dry_run_last_result: 'pending' | 'pass' | 'fail'。
type AgentOnboardingStatus struct {
	AgentID          uuid.UUID  `db:"agent_id" json:"agent_id"`
	EndpointSet      bool       `db:"endpoint_set" json:"endpoint_set"`
	CapabilitiesSet  bool       `db:"capabilities_set" json:"capabilities_set"`
	ExamplesSet      bool       `db:"examples_set" json:"examples_set"`
	DryRunPassed     bool       `db:"dry_run_passed" json:"dry_run_passed"`
	DryRunLastResult string     `db:"dry_run_last_result" json:"dry_run_last_result"`
	DryRunError      *string    `db:"dry_run_error" json:"dry_run_error"`
	DryRunAt         *time.Time `db:"dry_run_at" json:"dry_run_at"`
	UpdatedAt        time.Time  `db:"updated_at" json:"updated_at"`
}

// SkillTestCase 对应 skill_test_cases 表。
//
// 模块 B（Phase 2 §5）：Skill Benchmark 测试用例（平台维护）。
// input_json 喂给 Agent endpoint；judge_prompt 是 LLM-as-judge 评分模板，
// service 层会用真实 endpoint 输出替换 prompt 中的 {output} 占位。
type SkillTestCase struct {
	ID          uuid.UUID `db:"id" json:"id"`
	SkillID     string    `db:"skill_id" json:"skill_id"`
	Title       string    `db:"title" json:"title"`
	InputJSON   []byte    `db:"input_json" json:"input_json"`
	JudgePrompt string    `db:"judge_prompt" json:"judge_prompt"`
	SortOrder   int32     `db:"sort_order" json:"sort_order"`
	CreatedAt   time.Time `db:"created_at" json:"created_at"`
}

// AgentSkillBenchmarkRun 对应 agent_skill_benchmark_runs 表。
//
// 单条 case 的执行记录。status: 'pending' | 'success' | 'failed'。
// score 仅 success 非空（0-100）；raw_output / judge_reasoning 仅 success 写入。
// 同一次"跑某 skill"的多条 case 共享 batch_id。
type AgentSkillBenchmarkRun struct {
	ID             uuid.UUID  `db:"id" json:"id"`
	BatchID        uuid.UUID  `db:"batch_id" json:"batch_id"`
	AgentID        uuid.UUID  `db:"agent_id" json:"agent_id"`
	SkillID        string     `db:"skill_id" json:"skill_id"`
	TestCaseID     uuid.UUID  `db:"test_case_id" json:"test_case_id"`
	Status         string     `db:"status" json:"status"`
	Score          *int32     `db:"score" json:"score"`
	RawOutput      []byte     `db:"raw_output" json:"raw_output"`
	JudgeReasoning *string    `db:"judge_reasoning" json:"judge_reasoning"`
	ErrorMessage   *string    `db:"error_message" json:"error_message"`
	StartedAt      time.Time  `db:"started_at" json:"started_at"`
	FinishedAt     *time.Time `db:"finished_at" json:"finished_at"`
}

// AgentSkillScore 对应 agent_skill_scores 表（聚合）。
//
// status: 'pending' | 'verified' | 'failed'
// verified = 平均分 >= 阈值（service 中配置，默认 75）。
type AgentSkillScore struct {
	AgentID      uuid.UUID  `db:"agent_id" json:"agent_id"`
	SkillID      string     `db:"skill_id" json:"skill_id"`
	Status       string     `db:"status" json:"status"`
	AverageScore *int32     `db:"average_score" json:"average_score"`
	PassCount    int32      `db:"pass_count" json:"pass_count"`
	TotalCount   int32      `db:"total_count" json:"total_count"`
	LastBatchID  *uuid.UUID `db:"last_batch_id" json:"last_batch_id"`
	VerifiedAt   *time.Time `db:"verified_at" json:"verified_at"`
	UpdatedAt    time.Time  `db:"updated_at" json:"updated_at"`
}

// DeliveryTarget 对应 delivery_targets 表（用户拥有的投递目标）。
//
// type: 'webhook' | 'slack'。Config 是 JSONB（{url, event_types}）。
// Secret 给 webhook 投递做 HMAC 签名；slack 也存一份以备未来扩展。
type DeliveryTarget struct {
	ID        uuid.UUID `db:"id" json:"id"`
	UserID    uuid.UUID `db:"user_id" json:"user_id"`
	Name      string    `db:"name" json:"name"`
	Type      string    `db:"type" json:"type"`
	Config    []byte    `db:"config" json:"config"`
	Secret    string    `db:"secret" json:"-"`
	IsDefault bool      `db:"is_default" json:"is_default"`
	CreatedAt time.Time `db:"created_at" json:"created_at"`
	UpdatedAt time.Time `db:"updated_at" json:"updated_at"`
}

// RunDelivery 对应 run_deliveries 表（用户侧投递记录）。
//
// status: 'pending' | 'success' | 'failed'
// target_type / target_url 是投递时快照，即便 target 被删也能展示历史。
type RunDelivery struct {
	ID             uuid.UUID  `db:"id" json:"id"`
	RunID          uuid.UUID  `db:"run_id" json:"run_id"`
	TargetID       uuid.UUID  `db:"target_id" json:"target_id"`
	UserID         uuid.UUID  `db:"user_id" json:"user_id"`
	TargetType     string     `db:"target_type" json:"target_type"`
	TargetURL      string     `db:"target_url" json:"target_url"`
	Payload        []byte     `db:"payload" json:"payload"`
	Status         string     `db:"status" json:"status"`
	ResponseStatus *int32     `db:"response_status" json:"response_status"`
	ResponseBody   *string    `db:"response_body" json:"response_body"`
	ErrorMessage   *string    `db:"error_message" json:"error_message"`
	AttemptCount   int32      `db:"attempt_count" json:"attempt_count"`
	NextRetryAt    *time.Time `db:"next_retry_at" json:"next_retry_at"`
	CreatedAt      time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt      time.Time  `db:"updated_at" json:"updated_at"`
}

// WebhookDelivery 对应 webhook_deliveries 表（投递日志）。
//
// status: 'pending' | 'success' | 'failed'
// next_retry_at NULL 表示完成或放弃；非 NULL 时由 worker 重试
type WebhookDelivery struct {
	ID             uuid.UUID  `db:"id" json:"id"`
	AgentID        uuid.UUID  `db:"agent_id" json:"agent_id"`
	RunID          uuid.UUID  `db:"run_id" json:"run_id"`
	URL            string     `db:"url" json:"url"`
	Payload        []byte     `db:"payload" json:"payload"`
	Status         string     `db:"status" json:"status"`
	ResponseStatus *int32     `db:"response_status" json:"response_status"`
	ResponseBody   *string    `db:"response_body" json:"response_body"`
	ErrorMessage   *string    `db:"error_message" json:"error_message"`
	AttemptCount   int32      `db:"attempt_count" json:"attempt_count"`
	NextRetryAt    *time.Time `db:"next_retry_at" json:"next_retry_at"`
	CreatedAt      time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt      time.Time  `db:"updated_at" json:"updated_at"`
}

// TaskCallbackSubscription 对应 task_callback_subscriptions 表。
//
// status: 'active' | 'paused' | 'failed' | 'deleted'
type TaskCallbackSubscription struct {
	ID                  uuid.UUID  `db:"id" json:"id"`
	RunID               uuid.UUID  `db:"run_id" json:"run_id"`
	OwnerUserID         uuid.UUID  `db:"owner_user_id" json:"owner_user_id"`
	CallerAgentID       *uuid.UUID `db:"caller_agent_id" json:"caller_agent_id"`
	TargetURL           string     `db:"target_url" json:"target_url"`
	Secret              string     `db:"secret" json:"-"`
	EventTypes          []string   `db:"event_types" json:"event_types"`
	AuthScheme          *string    `db:"auth_scheme" json:"auth_scheme,omitempty"`
	AuthCredentials     *string    `db:"auth_credentials" json:"-"`
	Metadata            []byte     `db:"metadata" json:"metadata"`
	Status              string     `db:"status" json:"status"`
	ConsecutiveFailures int32      `db:"consecutive_failures" json:"consecutive_failures"`
	CreatedAt           time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt           time.Time  `db:"updated_at" json:"updated_at"`
	DeletedAt           *time.Time `db:"deleted_at" json:"deleted_at"`
}

// TaskCallbackDelivery 对应 task_callback_deliveries 表。
type TaskCallbackDelivery struct {
	ID             uuid.UUID  `db:"id" json:"id"`
	SubscriptionID uuid.UUID  `db:"subscription_id" json:"subscription_id"`
	RunEventID     uuid.UUID  `db:"run_event_id" json:"run_event_id"`
	Payload        []byte     `db:"payload" json:"payload"`
	Status         string     `db:"status" json:"status"`
	ResponseStatus *int32     `db:"response_status" json:"response_status"`
	ResponseBody   *string    `db:"response_body" json:"response_body"`
	ErrorMessage   *string    `db:"error_message" json:"error_message"`
	AttemptCount   int32      `db:"attempt_count" json:"attempt_count"`
	NextRetryAt    *time.Time `db:"next_retry_at" json:"next_retry_at"`
	DeliveredAt    *time.Time `db:"delivered_at" json:"delivered_at"`
	CreatedAt      time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt      time.Time  `db:"updated_at" json:"updated_at"`
}

// Workflow 对应 workflows 表。
type Workflow struct {
	ID          uuid.UUID `db:"id" json:"id"`
	UserID      uuid.UUID `db:"user_id" json:"user_id"`
	Name        string    `db:"name" json:"name"`
	Description string    `db:"description" json:"description"`
	Status      string    `db:"status" json:"status"`
	Edges       []byte    `db:"edges" json:"edges"`
	CreatedAt   time.Time `db:"created_at" json:"created_at"`
	UpdatedAt   time.Time `db:"updated_at" json:"updated_at"`
}

// WorkflowNode 对应 workflow_nodes 表。
type WorkflowNode struct {
	ID         uuid.UUID `db:"id" json:"id"`
	WorkflowID uuid.UUID `db:"workflow_id" json:"workflow_id"`
	NodeKey    string    `db:"node_key" json:"node_key"`
	NodeType   string    `db:"node_type" json:"node_type"`
	AgentID    uuid.UUID `db:"agent_id" json:"agent_id"`
	Title      string    `db:"title" json:"title"`
	Config     []byte    `db:"config" json:"config"`
	Position   int32     `db:"position" json:"position"`
	CreatedAt  time.Time `db:"created_at" json:"created_at"`
}

// WorkflowRun 对应 workflow_runs 表。
type WorkflowRun struct {
	ID              uuid.UUID  `db:"id" json:"id"`
	WorkflowID      uuid.UUID  `db:"workflow_id" json:"workflow_id"`
	UserID          uuid.UUID  `db:"user_id" json:"user_id"`
	Status          string     `db:"status" json:"status"`
	Input           []byte     `db:"input" json:"input"`
	Output          []byte     `db:"output" json:"output"`
	ErrorMessage    *string    `db:"error_message" json:"error_message"`
	StartedAt       time.Time  `db:"started_at" json:"started_at"`
	FinishedAt      *time.Time `db:"finished_at" json:"finished_at"`
	CreatedAt       time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt       time.Time  `db:"updated_at" json:"updated_at"`
	AttemptCount    int32      `db:"attempt_count" json:"attempt_count"`
	MaxAttempts     int32      `db:"max_attempts" json:"max_attempts"`
	NextRetryAt     *time.Time `db:"next_retry_at" json:"next_retry_at"`
	ClaimedAt       *time.Time `db:"claimed_at" json:"claimed_at"`
	LastWorkerError *string    `db:"last_worker_error" json:"last_worker_error"`
}

// WorkflowRunStep 对应 workflow_run_steps 表。
type WorkflowRunStep struct {
	ID             uuid.UUID  `db:"id" json:"id"`
	WorkflowRunID  uuid.UUID  `db:"workflow_run_id" json:"workflow_run_id"`
	WorkflowNodeID uuid.UUID  `db:"workflow_node_id" json:"workflow_node_id"`
	NodeKey        string     `db:"node_key" json:"node_key"`
	AgentID        uuid.UUID  `db:"agent_id" json:"agent_id"`
	RunID          *uuid.UUID `db:"run_id" json:"run_id"`
	Status         string     `db:"status" json:"status"`
	Input          []byte     `db:"input" json:"input"`
	Output         []byte     `db:"output" json:"output"`
	ErrorMessage   *string    `db:"error_message" json:"error_message"`
	Sequence       int32      `db:"sequence" json:"sequence"`
	StartedAt      time.Time  `db:"started_at" json:"started_at"`
	FinishedAt     *time.Time `db:"finished_at" json:"finished_at"`
	CreatedAt      time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt      time.Time  `db:"updated_at" json:"updated_at"`
}
