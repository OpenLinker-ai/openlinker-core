package agent

// CreateBootstrapTokenRequest 创作者侧铸造 Bootstrap Token 请求体。
//
// expires_in_minutes 默认 30；max_agents 默认 1。label 用于人类侧识别用途。
type CreateBootstrapTokenRequest struct {
	Label            string `json:"label" validate:"required,min=1,max=80"`
	ExpiresInMinutes int32  `json:"expires_in_minutes" validate:"omitempty,min=5,max=1440"`
	MaxAgents        int32  `json:"max_agents" validate:"omitempty,min=1,max=10"`
}

// BootstrapTokenResponse 列表 / 创建共用响应。
// PlaintextToken 仅创建时一次性返回，之后调用 List 仅看到 prefix。
type BootstrapTokenResponse struct {
	ID             string  `json:"id"`
	Label          string  `json:"label"`
	Prefix         string  `json:"prefix"`
	MaxAgents      int32   `json:"max_agents"`
	UsedCount      int32   `json:"used_count"`
	ExpiresAt      string  `json:"expires_at"`
	RevokedAt      *string `json:"revoked_at,omitempty"`
	LastUsedAt     *string `json:"last_used_at,omitempty"`
	CreatedAt      string  `json:"created_at"`
	PlaintextToken string  `json:"plaintext_token,omitempty"`
}

// RegisterAgentViaBootstrapRequest Agent 侧自注册请求体。
//
// bootstrap_token 必填；slug 可选（未填时由 name 派生）。
// 其余字段与 CreateAgentRequest 一致，但不要求 JWT。
type RegisterAgentViaBootstrapRequest struct {
	BootstrapToken     string   `json:"bootstrap_token" validate:"required,min=24,max=128"`
	Slug               string   `json:"slug" validate:"omitempty,min=3,max=80"`
	Name               string   `json:"name" validate:"required,min=3,max=80"`
	Description        string   `json:"description" validate:"max=500"`
	EndpointURL        string   `json:"endpoint_url" validate:"required,url,startswith=https://,max=500"`
	EndpointAuthHeader string   `json:"endpoint_auth_header" validate:"max=500"`
	PricePerCallCents  int32    `json:"price_per_call_cents" validate:"max=1000000"`
	Tags               []string `json:"tags" validate:"required,min=1,max=5,dive,min=2,max=30"`
	AbilityTags        []string `json:"ability_tags" validate:"omitempty,max=5,dive,min=2,max=30"`
	Visibility         string   `json:"visibility" validate:"omitempty,oneof=public unlisted private"`
	RuntimeTokenName   string   `json:"runtime_token_name" validate:"omitempty,min=1,max=80"`
}

// RegisterAgentViaBootstrapResponse 自注册成功响应。
// AgentID + Slug 给 Agent 后续 self-identify；RuntimeToken 是平台调用 Agent 时的凭证。
type RegisterAgentViaBootstrapResponse struct {
	Agent        AgentResponse         `json:"agent"`
	RuntimeToken BootstrapRuntimeToken `json:"runtime_token"`
	UsedCount    int32                 `json:"bootstrap_used_count"`
	MaxAgents    int32                 `json:"bootstrap_max_agents"`
}

// BootstrapRuntimeToken Runtime Token 一次性明文返回（仅在自注册响应内）。
type BootstrapRuntimeToken struct {
	ID             string `json:"id"`
	Prefix         string `json:"prefix"`
	PlaintextToken string `json:"plaintext_token"`
	CreatedAt      string `json:"created_at"`
}
