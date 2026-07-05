package agent

// CreateAgentTokenRequest 创作者侧创建 Agent 接入凭证。
//
// agent_id 为空时创建 pending_registration token，用于新 Agent 自注册；
// agent_id 不为空时创建 active_runtime token，用于已有 Agent 轮换接入凭证。
type CreateAgentTokenRequest struct {
	Name             string   `json:"name" validate:"required,min=1,max=80"`
	AgentID          string   `json:"agent_id" validate:"omitempty,uuid"`
	Scopes           []string `json:"scopes" validate:"omitempty,max=2,dive,oneof=agent:call agent:pull"`
	ExpiresInMinutes int32    `json:"expires_in_minutes" validate:"omitempty,min=5,max=1440"`
}

// AgentTokenResponse 列表 / 创建共用响应。
// PlaintextToken 仅创建时一次性返回，之后调用 List 仅看到 prefix。
type AgentTokenResponse struct {
	ID             string   `json:"id"`
	AgentID        *string  `json:"agent_id,omitempty"`
	Name           string   `json:"name"`
	Prefix         string   `json:"prefix"`
	Status         string   `json:"status"`
	Scopes         []string `json:"scopes"`
	ExpiresAt      *string  `json:"expires_at,omitempty"`
	RedeemedAt     *string  `json:"redeemed_at,omitempty"`
	RevokedAt      *string  `json:"revoked_at,omitempty"`
	LastUsedAt     *string  `json:"last_used_at,omitempty"`
	CreatedAt      string   `json:"created_at"`
	PlaintextToken string   `json:"plaintext_token,omitempty"`
}

// RegisterAgentViaTokenRequest Agent 侧自注册请求体。
//
// agent_token 必填；slug 可选（未填时由 name 派生）。
// 其余字段与 CreateAgentRequest 一致，但不要求 JWT。
type RegisterAgentViaTokenRequest struct {
	AgentToken         string   `json:"agent_token" validate:"required,min=24,max=128"`
	Slug               string   `json:"slug" validate:"omitempty,min=3,max=80"`
	Name               string   `json:"name" validate:"required,min=3,max=80"`
	Description        string   `json:"description" validate:"max=500"`
	EndpointURL        string   `json:"endpoint_url" validate:"max=500"`
	EndpointAuthHeader string   `json:"endpoint_auth_header" validate:"max=500"`
	PricePerCallCents  int32    `json:"price_per_call_cents" validate:"min=0,max=1000000"`
	Tags               []string `json:"tags" validate:"required,min=1,max=5,dive,min=2,max=30"`
	AbilityTags        []string `json:"ability_tags" validate:"omitempty,max=5,dive,min=2,max=30"`
	SkillIDs           []string `json:"skill_ids" validate:"omitempty,max=5,dive,min=3,max=80"`
	Visibility         string   `json:"visibility" validate:"omitempty,oneof=public unlisted private"`
	ConnectionMode     string   `json:"connection_mode" validate:"omitempty,oneof=direct_http mcp_server runtime_pull runtime_ws"`
	MCPToolName        string   `json:"mcp_tool_name" validate:"omitempty,min=1,max=120"`
}

// RegisterAgentViaTokenResponse 自注册成功响应。
// AgentToken 返回元数据；明文仍是请求里使用的同一枚 OPENLINKER_AGENT_TOKEN。
type RegisterAgentViaTokenResponse struct {
	Agent      AgentResponse      `json:"agent"`
	AgentToken AgentTokenResponse `json:"agent_token"`
}
