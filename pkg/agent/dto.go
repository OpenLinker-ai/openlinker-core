package agent

// CreateAgentRequest 创建 Agent 请求体。
//
// slug 格式（^[a-z0-9][a-z0-9-]*[a-z0-9]$, 3..80）由 service 层用 regex 校验，
// 避免给 validator 注册自定义 tag 提高维护成本。
type CreateAgentRequest struct {
	Slug               string   `json:"slug" validate:"required,min=3,max=80"`
	Name               string   `json:"name" validate:"required,min=3,max=80"`
	Description        string   `json:"description" validate:"required,max=500"`
	EndpointURL        string   `json:"endpoint_url" validate:"required,url,startswith=https://,max=500"`
	EndpointAuthHeader string   `json:"endpoint_auth_header" validate:"max=500"`
	PricePerCallCents  int32    `json:"price_per_call_cents" validate:"required,min=1,max=1000000"`
	Tags               []string `json:"tags" validate:"required,min=1,max=5,dive,min=2,max=30"`
}

// UpdateAgentRequest 编辑 Agent 请求体。slug 不可改。
// Visibility 可空字符串视为不改，否则只接受 public / unlisted / private。
type UpdateAgentRequest struct {
	Name               string   `json:"name" validate:"required,min=3,max=80"`
	Description        string   `json:"description" validate:"required,max=500"`
	EndpointURL        string   `json:"endpoint_url" validate:"required,url,startswith=https://,max=500"`
	EndpointAuthHeader string   `json:"endpoint_auth_header" validate:"max=500"`
	PricePerCallCents  int32    `json:"price_per_call_cents" validate:"required,min=1,max=1000000"`
	Tags               []string `json:"tags" validate:"required,min=1,max=5,dive,min=2,max=30"`
	Visibility         string   `json:"visibility" validate:"omitempty,oneof=public unlisted private"`
}

// AgentResponse 单个 Agent 的统一返回 DTO。
//
// Phase 2 缺口 2：Status 是从 LifecycleStatus / CertificationStatus 派生的字段，
// 仅供尚未切换的老前端读取；新前端应直接读 LifecycleStatus / Visibility / CertificationStatus。
//
// Creator 字段仅在 admin 人工处理队列等接口填充，普通创作者列表为空。
// EndpointAuthHeader 不返回（避免泄露），仅 owner GET 需要时才单独返回。
type AgentResponse struct {
	ID                  string   `json:"id"`
	Slug                string   `json:"slug"`
	Name                string   `json:"name"`
	Description         string   `json:"description"`
	EndpointURL         string   `json:"endpoint_url"`
	PricePerCallCents   int32    `json:"price_per_call_cents"`
	Tags                []string `json:"tags"`
	Status              string   `json:"status"` // 派生，老前端兼容
	LifecycleStatus     string   `json:"lifecycle_status"`
	Visibility          string   `json:"visibility"`
	CertificationStatus string   `json:"certification_status"`
	RejectionReason     *string  `json:"rejection_reason,omitempty"`
	TotalCalls          int32    `json:"total_calls"`
	TotalRevenueCents   int64    `json:"total_revenue_cents"`
	WebhookURL          *string  `json:"webhook_url,omitempty"`
	CreatedAt           string   `json:"created_at"`
	CertifiedAt         *string  `json:"certified_at,omitempty"`
	Creator             *Creator `json:"creator,omitempty"`
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
}

// DryRunResponse POST /creator/agents/:id/dry-run 响应。
//
// result = "pass" / "fail"；fail 时 error 非空，pass 时 error 为 nil。
// output 是创作者 endpoint 返回的原始 JSON object（pass 时存在；fail 时可能为 nil）。
type DryRunResponse struct {
	Result string                 `json:"result"`
	Error  *string                `json:"error,omitempty"`
	Output map[string]interface{} `json:"output,omitempty"`
}
