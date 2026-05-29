package mcp

// 模块 C（MCP 外部入口）：把 OpenLinker 当 Claude / Cursor / Codex 的外部工具暴露。
// 路径：
//   - POST /api/v1/mcp      MCP Streamable HTTP JSON-RPC 入口（JSON response mode）
//   - /api/v1/mcp/*         兼容旧脚本的 REST JSON 工具入口
//
// 鉴权：仅访问令牌（ol_live_xxx；兼容历史 sk_live_xxx）。
//
// 不引入 mark3labs/mcp-go SDK —— 5 个工具直接转发到既有 service，
// 鉴权直接复用 HybridAuthMiddleware + apikey.Service。

// ToolDescriptor 暴露给 MCP 客户端的工具元信息。
//
// 客户端用 InputSchema 决定参数表单 / 校验；OutputSchema 留作 hint，不强制校验。
type ToolDescriptor struct {
	Name         string                 `json:"name"`
	Description  string                 `json:"description"`
	InputSchema  map[string]interface{} `json:"input_schema"`
	OutputSchema map[string]interface{} `json:"output_schema,omitempty"`
}

// ToolsResponse GET /api/v1/mcp/tools 响应。
type ToolsResponse struct {
	Tools []ToolDescriptor `json:"tools"`
}

// SearchAgentsRequest POST /api/v1/mcp/search_agents 请求体。
//
// 全部可选；不传任何字段时返回默认市场首页。
type SearchAgentsRequest struct {
	Query string   `json:"query,omitempty"`
	Tags  []string `json:"tags,omitempty"`
	Limit int32    `json:"limit,omitempty"`
}

// GetAgentRequest POST /api/v1/mcp/get_agent 请求体。
//
// slug 是市场 URL 中的人类可读 id（如 "translator-zh-en"）。
type GetAgentRequest struct {
	Slug string `json:"slug" validate:"required"`
}

// RunAgentRequest POST /api/v1/mcp/run_agent 请求体。
//
// agent_id 是 UUID；input 透传给创作者 endpoint。
type RunAgentRequest struct {
	AgentID  string                 `json:"agent_id" validate:"required,uuid"`
	Input    map[string]interface{} `json:"input" validate:"required"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// GetRunRequest POST /api/v1/mcp/get_run 请求体。
type GetRunRequest struct {
	RunID string `json:"run_id" validate:"required,uuid"`
}

// CreateTaskRequest POST /api/v1/mcp/create_task 请求体。
//
// query 长度与 task.RecommendRequest 保持一致（4-500 字符）。
type CreateTaskRequest struct {
	Query    string   `json:"query" validate:"required,min=4,max=500"`
	SkillIDs []string `json:"skill_ids,omitempty" validate:"omitempty,max=5,dive,min=1,max=80"`
	MCPTools []string `json:"mcp_tools,omitempty" validate:"omitempty,max=5,dive,min=1,max=80"`
}
