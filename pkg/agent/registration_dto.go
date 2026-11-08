package agent

import (
	coreruntime "github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

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

// ListAgentTokensOptions 控制 Agent Token 列表分页和排序。
type ListAgentTokensOptions struct {
	Limit   int32
	Offset  int32
	SortBy  string
	SortDir string
	Status  string
	Query   string
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

// AgentTokenListResponse Agent Token 列表响应。
type AgentTokenListResponse struct {
	Items   []AgentTokenResponse `json:"items"`
	Total   int32                `json:"total"`
	Limit   int32                `json:"limit"`
	Offset  int32                `json:"offset"`
	SortBy  string               `json:"sort_by"`
	SortDir string               `json:"sort_dir"`
	HasMore bool                 `json:"has_more"`
}

// RegisterAgentViaTokenRequest Agent 侧自注册请求体。
//
// agent_token 必填；slug 可选（未填时由 name 派生）。
// 其余字段与 CreateAgentRequest 一致，但不要求 JWT。visibility 省略时为 private。
//
// capability / examples 可选，形状与校验和创作者端
// PUT /creator/agents/:id/capabilities、POST /creator/agents/:id/examples 相同，
// 与 Agent 创建在同一事务内写入。注册重放（同一枚已兑换 token + 同一 slug）
// 不写库，也不比较这两个字段；响应按当前已保存状态给出 onboarding 与 next_actions。
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
	ConnectionMode     string   `json:"connection_mode" validate:"omitempty,oneof=direct_http mcp_server runtime"`
	MCPToolName        string   `json:"mcp_tool_name" validate:"omitempty,min=1,max=120"`
	// Capability declares input/output JSON Schemas at registration time.
	Capability *UpsertCapabilityRequest `json:"capability,omitempty" validate:"omitempty"`
	// Examples adds up to 20 input/output examples at registration time.
	Examples []CreateExampleRequest `json:"examples,omitempty" validate:"omitempty,max=20,dive"`
}

// RegisterAgentViaTokenResponse 自注册成功响应。
// AgentToken 返回元数据；明文仍是请求里使用的同一枚 OPENLINKER_AGENT_TOKEN。
//
// Agent 带 availability / readiness。Onboarding 是当前接入完成度；NextActions 是
// 让 Agent 变为可调用 / 公开仍需完成的有序步骤（最多 5 项）。两者都是注册提交后
// 读取的当前状态，读取失败时省略（不代表无需后续步骤）；全部完成时 NextActions 为 []。
type RegisterAgentViaTokenResponse struct {
	Agent       AgentResponse                `json:"agent"`
	AgentToken  AgentTokenResponse           `json:"agent_token"`
	Onboarding  *OnboardingStatusResponse    `json:"onboarding,omitempty"`
	NextActions *[]coreruntime.RunNextAction `json:"next_actions,omitempty"`
}
