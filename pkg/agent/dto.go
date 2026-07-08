package agent

// CreateAgentRequest 创建 Agent 请求体。
//
// slug 格式（^[a-z0-9][a-z0-9-]*[a-z0-9]$, 3..80）由 service 层用 regex 校验，
// 避免给 validator 注册自定义 tag 提高维护成本。
type CreateAgentRequest struct {
	Slug               string   `json:"slug" validate:"required,min=3,max=80"`
	Name               string   `json:"name" validate:"required,min=3,max=80"`
	Description        string   `json:"description" validate:"max=500"`
	EndpointURL        string   `json:"endpoint_url" validate:"max=500"`
	EndpointAuthHeader string   `json:"endpoint_auth_header" validate:"max=500"`
	PricePerCallCents  int32    `json:"price_per_call_cents" validate:"min=0,max=1000000"`
	Tags               []string `json:"tags" validate:"required,min=1,max=5,dive,min=2,max=30"`
	SkillIDs           []string `json:"skill_ids,omitempty" validate:"omitempty,max=5,dive,min=1,max=120"`
	Visibility         string   `json:"visibility" validate:"omitempty,oneof=public unlisted private"`
	ConnectionMode     string   `json:"connection_mode" validate:"omitempty,oneof=direct_http mcp_server runtime_pull runtime_ws"`
	MCPToolName        string   `json:"mcp_tool_name" validate:"omitempty,min=1,max=120"`
}

// UpdateAgentRequest 编辑 Agent 请求体。slug 不可改。
// Visibility 可空字符串视为不改，否则只接受 public / unlisted / private。
type UpdateAgentRequest struct {
	Name               string   `json:"name" validate:"required,min=3,max=80"`
	Description        string   `json:"description" validate:"max=500"`
	EndpointURL        string   `json:"endpoint_url" validate:"max=500"`
	EndpointAuthHeader string   `json:"endpoint_auth_header" validate:"max=500"`
	ClearEndpointAuth  bool     `json:"clear_endpoint_auth_header"`
	PricePerCallCents  int32    `json:"price_per_call_cents" validate:"min=0,max=1000000"`
	Tags               []string `json:"tags" validate:"required,min=1,max=5,dive,min=2,max=30"`
	Visibility         string   `json:"visibility" validate:"omitempty,oneof=public unlisted private"`
	ConnectionMode     string   `json:"connection_mode" validate:"omitempty,oneof=direct_http mcp_server runtime_pull runtime_ws"`
	MCPToolName        string   `json:"mcp_tool_name" validate:"omitempty,min=1,max=120"`
}

// UpdateVisibilityRequest 仅切换市场可见性，不要求重传 endpoint 鉴权等敏感配置。
type UpdateVisibilityRequest struct {
	Visibility string `json:"visibility" validate:"required,oneof=public unlisted private"`
}

// AgentResponse 单个 Agent 的统一返回 DTO。
//
// Phase 2 缺口 2：Status 是从 LifecycleStatus / CertificationStatus 派生的字段，
// 仅供尚未切换的老前端读取；新前端应直接读 LifecycleStatus / Visibility / CertificationStatus。
//
// Creator 字段仅在 admin 人工处理队列等接口填充，普通创作者列表为空。
// EndpointAuthHeader 不返回（避免泄露），仅 owner GET 需要时才单独返回。
type AgentResponse struct {
	ID                  string        `json:"id"`
	Slug                string        `json:"slug"`
	Name                string        `json:"name"`
	Description         string        `json:"description"`
	EndpointURL         string        `json:"endpoint_url"`
	PricePerCallCents   int32         `json:"price_per_call_cents"`
	Tags                []string      `json:"tags"`
	SkillIDs            []string      `json:"skill_ids,omitempty"`
	Status              string        `json:"status"` // 派生，老前端兼容
	LifecycleStatus     string        `json:"lifecycle_status"`
	Visibility          string        `json:"visibility"`
	CertificationStatus string        `json:"certification_status"`
	RejectionReason     *string       `json:"rejection_reason,omitempty"`
	TotalCalls          int32         `json:"total_calls"`
	TotalRevenueCents   int64         `json:"total_revenue_cents"`
	CallsThisMonth      int64         `json:"calls_this_month,omitempty"`
	RevenueThisMonth    int64         `json:"revenue_this_month_cents,omitempty"`
	ConnectionMode      string        `json:"connection_mode"`
	MCPToolName         *string       `json:"mcp_tool_name,omitempty"`
	Availability        *Availability `json:"availability,omitempty"`
	Readiness           *Readiness    `json:"readiness,omitempty"`
	CreatedAt           string        `json:"created_at"`
	CertifiedAt         *string       `json:"certified_at,omitempty"`
	Creator             *Creator      `json:"creator,omitempty"`
}

type AgentCounts struct {
	Total    int32 `json:"total"`
	Online   int32 `json:"online"`
	Public   int32 `json:"public"`
	Unlisted int32 `json:"unlisted"`
	Private  int32 `json:"private"`
	Pending  int32 `json:"pending"`
}

type AgentListOptions struct {
	Query               string
	Status              string
	Visibility          string
	CertificationStatus string
	SortBy              string
	SkillIDs            []string
	Limit               int32
	Offset              int32
}

type AgentListResponse struct {
	Items  []AgentResponse `json:"items"`
	Total  int32           `json:"total"`
	Limit  int32           `json:"limit"`
	Offset int32           `json:"offset"`
	Counts AgentCounts     `json:"counts"`
}

// Creator admin 视图嵌入的创作者信息。
type Creator struct {
	ID          string `json:"id"`
	Email       string `json:"email"`
	DisplayName string `json:"display_name"`
}

// SlugCheckResponse GET /agents/check-slug 响应。
type SlugCheckResponse struct {
	Slug      string `json:"slug"`
	Available bool   `json:"available"`
}

// RejectRequest admin 拒绝 Agent 请求体。
type RejectRequest struct {
	Reason string `json:"reason" validate:"required,min=5,max=500"`
}

// UpsertCapabilityRequest 保存 Agent 能力声明。
type UpsertCapabilityRequest struct {
	InputSchema  map[string]interface{} `json:"input_schema" validate:"required"`
	OutputSchema map[string]interface{} `json:"output_schema" validate:"required"`
	Summary      string                 `json:"summary" validate:"max=1000"`
}

// CapabilityResponse Agent 能力声明响应。
type CapabilityResponse struct {
	ID           string                 `json:"id"`
	AgentID      string                 `json:"agent_id"`
	InputSchema  map[string]interface{} `json:"input_schema"`
	OutputSchema map[string]interface{} `json:"output_schema"`
	Summary      string                 `json:"summary"`
	Version      int32                  `json:"version"`
	PublishedAt  string                 `json:"published_at"`
	UpdatedAt    string                 `json:"updated_at"`
}

// CreateExampleRequest 新增 Agent 示例。
type CreateExampleRequest struct {
	Title              string                 `json:"title" validate:"required,min=1,max=120"`
	InputJSON          map[string]interface{} `json:"input_json" validate:"required"`
	ExpectedOutputJSON map[string]interface{} `json:"expected_output_json,omitempty"`
	SortOrder          int32                  `json:"sort_order"`
}

// ExampleResponse Agent 示例响应。
type ExampleResponse struct {
	ID                 string                 `json:"id"`
	AgentID            string                 `json:"agent_id"`
	Title              string                 `json:"title"`
	InputJSON          map[string]interface{} `json:"input_json"`
	ExpectedOutputJSON map[string]interface{} `json:"expected_output_json,omitempty"`
	SortOrder          int32                  `json:"sort_order"`
	CreatedAt          string                 `json:"created_at"`
	UpdatedAt          string                 `json:"updated_at"`
}

// OnboardingStatusResponse Agent 接入完成度。
type OnboardingStatusResponse struct {
	AgentID          string  `json:"agent_id"`
	EndpointSet      bool    `json:"endpoint_set"`
	CapabilitiesSet  bool    `json:"capabilities_set"`
	ExamplesSet      bool    `json:"examples_set"`
	DryRunPassed     bool    `json:"dry_run_passed"`
	DryRunLastResult string  `json:"dry_run_last_result"`
	DryRunError      *string `json:"dry_run_error,omitempty"`
	DryRunAt         *string `json:"dry_run_at,omitempty"`
	UpdatedAt        string  `json:"updated_at"`
}

// OnboardingResponse 聚合创作者接入页所需状态。
type OnboardingResponse struct {
	Status     OnboardingStatusResponse `json:"status"`
	Capability *CapabilityResponse      `json:"capability,omitempty"`
	Examples   []ExampleResponse        `json:"examples"`
	// Availability 是创作者侧修复体验使用的实时可用性快照。
	Availability Availability `json:"availability"`
}

// DryRunResponse POST /creator/agents/:id/dry-run 响应。
//
// result = "pass" / "fail"；fail 时 error 非空，pass 时 error 为 nil。
// output 是创作者 endpoint 返回的原始 JSON object（pass 时存在；fail 时可能为 nil）。
type DryRunResponse struct {
	Result       string                 `json:"result"`
	Error        *string                `json:"error,omitempty"`
	Output       map[string]interface{} `json:"output,omitempty"`
	Availability Availability           `json:"availability"`
	RepairHints  []string               `json:"repair_hints,omitempty"`
}

// AvailabilityAlertResponse 是创作者侧站内可用性告警。
type AvailabilityAlertResponse struct {
	ID                  string   `json:"id"`
	AgentID             string   `json:"agent_id"`
	AgentSlug           string   `json:"agent_slug,omitempty"`
	AgentName           string   `json:"agent_name,omitempty"`
	Type                string   `json:"type"`
	Severity            string   `json:"severity"`
	AvailabilityStatus  string   `json:"availability_status"`
	ConsecutiveFailures int32    `json:"consecutive_failures"`
	Title               string   `json:"title"`
	Message             string   `json:"message"`
	LastError           *string  `json:"last_error,omitempty"`
	RepairHints         []string `json:"repair_hints,omitempty"`
	ReadAt              *string  `json:"read_at,omitempty"`
	CreatedAt           string   `json:"created_at"`
	UpdatedAt           string   `json:"updated_at"`
}

// AvailabilityAlertListResponse GET /creator/availability-alerts 响应。
type AvailabilityAlertListResponse struct {
	Items  []AvailabilityAlertResponse `json:"items"`
	Total  int32                       `json:"total"`
	Unread int32                       `json:"unread"`
}

// AvailabilityCheckBatchResponse 平台巡检批次结果。
type AvailabilityCheckBatchResponse struct {
	Checked int32                       `json:"checked"`
	Passed  int32                       `json:"passed"`
	Failed  int32                       `json:"failed"`
	Alerts  []AvailabilityAlertResponse `json:"alerts"`
}
