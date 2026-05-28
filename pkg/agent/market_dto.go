// Package agent —— 市场（用户侧只读）DTO 定义。
//
// 注意：dto.go（创作者侧 / 写入侧）由 subagent-2a 维护，
// 本文件只放与市场查询相关的响应类型，避免冲突。

package agent

// MarketListItem 市场列表中的单个 Agent 摘要。
//
// 仅暴露公开字段；endpoint_url / endpoint_auth_header 不出现在列表里。
type MarketListItem struct {
	ID                string       `json:"id"`
	Slug              string       `json:"slug"`
	Name              string       `json:"name"`
	Description       string       `json:"description"`
	PricePerCallCents int32        `json:"price_per_call_cents"`
	Tags              []string     `json:"tags"`
	TotalCalls        int32        `json:"total_calls"`
	Creator           CreatorMini  `json:"creator"`
	ConnectionMode    string       `json:"connection_mode"`
	MCPToolName       *string      `json:"mcp_tool_name,omitempty"`
	Availability      Availability `json:"availability"`
}

// CreatorMini 列表 / 详情里嵌入的创作者轻量信息。
//
// Phase 1 只暴露 display_name，未来可加 avatar / verified 等字段。
type CreatorMini struct {
	DisplayName string `json:"display_name"`
}

// SkillMini 详情页公开展示的 Agent skill。
type SkillMini struct {
	ID          string `json:"id"`
	Category    string `json:"category"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

// MarketListResponse GET /agents 响应。
//
// page 从 1 开始；total 是符合过滤条件的总数。
type MarketListResponse struct {
	Items []MarketListItem `json:"items"`
	Total int32            `json:"total"`
	Page  int32            `json:"page"`
	Size  int32            `json:"size"`
}

// Availability is the public availability signal derived from real run results.
//
// It deliberately separates "registered/listed" from "recently reachable".
type Availability struct {
	Status              string  `json:"status"`
	Label               string  `json:"label"`
	Hint                string  `json:"hint"`
	LastSuccessfulRunAt *string `json:"last_successful_run_at,omitempty"`
	LastFailedRunAt     *string `json:"last_failed_run_at,omitempty"`
	LastCheckedAt       *string `json:"last_checked_at,omitempty"`
	ConsecutiveFailures int32   `json:"consecutive_failures"`
}

// AgentDetailResponse GET /agents/:slug 响应。
//
// 详情页比列表多 endpoint_url / created_at / certified_at 等字段，
// 但 endpoint_auth_header 始终不暴露（仅 runtime 调用时服务端使用）。
type AgentDetailResponse struct {
	ID                  string              `json:"id"`
	Slug                string              `json:"slug"`
	Name                string              `json:"name"`
	Description         string              `json:"description"`
	EndpointURL         string              `json:"endpoint_url"`
	PricePerCallCents   int32               `json:"price_per_call_cents"`
	Tags                []string            `json:"tags"`
	TotalCalls          int32               `json:"total_calls"`
	Creator             CreatorMini         `json:"creator"`
	CreatedAt           string              `json:"created_at"`
	CertifiedAt         *string             `json:"certified_at,omitempty"`
	LifecycleStatus     string              `json:"lifecycle_status"`
	Visibility          string              `json:"visibility"`
	CertificationStatus string              `json:"certification_status"`
	ConnectionMode      string              `json:"connection_mode"`
	MCPToolName         *string             `json:"mcp_tool_name,omitempty"`
	Availability        Availability        `json:"availability"`
	VerifiedSkillCount  int32               `json:"verified_skill_count"`
	LatestBenchmarkID   *string             `json:"latest_benchmark_batch_id,omitempty"`
	Skills              []SkillMini         `json:"skills"`
	Capability          *CapabilityResponse `json:"capability,omitempty"`
	Examples            []ExampleResponse   `json:"examples"`
}

// AgentCardResponse is a public, machine-readable card for Agent discovery.
//
// It intentionally points clients at OpenLinker platform invocation endpoints
// instead of exposing private endpoint secrets.
type AgentCardResponse struct {
	Name                      string                 `json:"name"`
	Description               string                 `json:"description"`
	URL                       string                 `json:"url"`
	Version                   string                 `json:"version"`
	ProtocolVersion           string                 `json:"protocolVersion,omitempty"`
	ProtocolVersions          []string               `json:"protocolVersions,omitempty"`
	SupportedInterfaces       []AgentCardInterface   `json:"supportedInterfaces,omitempty"`
	Provider                  AgentCardProvider      `json:"provider"`
	Capabilities              AgentCardCapabilities  `json:"capabilities"`
	DefaultInputModes         []string               `json:"default_input_modes"`
	DefaultOutputModes        []string               `json:"default_output_modes"`
	DefaultInputModesCurrent  []string               `json:"defaultInputModes,omitempty"`
	DefaultOutputModesCurrent []string               `json:"defaultOutputModes,omitempty"`
	Skills                    []AgentCardSkill       `json:"skills"`
	Authentication            AgentCardAuth          `json:"authentication"`
	OpenLinker                AgentCardOpenLinkerExt `json:"openlinker"`
	Capability                *CapabilityResponse    `json:"capability,omitempty"`
	Examples                  []ExampleResponse      `json:"examples,omitempty"`
	Signature                 *AgentCardSignature    `json:"signature,omitempty"`
}

type AgentCardProvider struct {
	Organization string `json:"organization"`
}

type AgentCardCapabilities struct {
	Streaming               bool `json:"streaming"`
	PushNotifications       bool `json:"pushNotifications"`
	PushNotificationsLegacy bool `json:"push_notifications"`
	Delegation              bool `json:"delegation"`
	ExtendedAgentCard       bool `json:"extendedAgentCard,omitempty"`
}

type AgentCardInterface struct {
	URL             string `json:"url"`
	ProtocolBinding string `json:"protocolBinding"`
	ProtocolVersion string `json:"protocolVersion"`
}

type AgentCardSkill struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Tags        []string `json:"tags,omitempty"`
}

type AgentCardAuth struct {
	Schemes []string `json:"schemes"`
	Scopes  []string `json:"scopes"`
}

type AgentCardOpenLinkerExt struct {
	AgentID                string   `json:"agent_id"`
	Slug                   string   `json:"slug"`
	CardVariant            string   `json:"card_variant"`
	ExtendedCardEndpoint   string   `json:"extended_card_endpoint"`
	ConnectionMode         string   `json:"connection_mode"`
	MCPToolName            *string  `json:"mcp_tool_name,omitempty"`
	AvailabilityStatus     string   `json:"availability_status"`
	CertificationStatus    string   `json:"certification_status"`
	VerifiedSkillCount     int32    `json:"verified_skill_count"`
	LatestBenchmarkBatchID *string  `json:"latest_benchmark_batch_id,omitempty"`
	CapabilityDeclared     bool     `json:"capability_declared"`
	ExampleCount           int32    `json:"example_count"`
	InvocationEndpoint     string   `json:"invocation_endpoint"`
	StreamEndpoint         string   `json:"stream_endpoint"`
	RunLookupEndpoint      string   `json:"run_lookup_endpoint"`
	TaskLookupEndpoint     string   `json:"task_lookup_endpoint"`
	TaskSubscribeEndpoint  string   `json:"task_subscribe_endpoint"`
	SkillIDs               []string `json:"skill_ids"`
}

type AgentCardSignature struct {
	Algorithm     string `json:"algorithm"`
	KeyID         string `json:"key_id"`
	PublicKey     string `json:"public_key"`
	PayloadDigest string `json:"payload_digest"`
	Signature     string `json:"signature"`
}
