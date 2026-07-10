package mcp

import (
	"context"

	"github.com/google/uuid"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/OpenLinker-ai/openlinker-core/pkg/task"
)

// 默认 search 分页：MCP 客户端不希望一次拿太多，给个保守上限。
const (
	defaultMCPSearchLimit int32 = 10
	maxMCPSearchLimit     int32 = 50
)

// Service 极薄 facade，把 MCP 5 个工具直接转到既有 service。
// 不重新初始化任何依赖：main.go 把现成的 *MarketService / *runtime.Service /
// *task.Service 直接注入。
type Service struct {
	market  *agent.MarketService
	runtime *runtime.Service
	task    *task.Service
}

// NewService 构造 MCP service。
func NewService(market *agent.MarketService, runtimeSvc *runtime.Service, taskSvc *task.Service) *Service {
	return &Service{market: market, runtime: runtimeSvc, task: taskSvc}
}

// SearchAgents 转 market.ListMarketWithSkills，按 query/tags/skill_ids 过滤。
func (s *Service) SearchAgents(ctx context.Context, req *SearchAgentsRequest) (*agent.MarketListResponse, error) {
	limit := req.Limit
	if limit <= 0 {
		limit = defaultMCPSearchLimit
	}
	if limit > maxMCPSearchLimit {
		limit = maxMCPSearchLimit
	}
	tags := req.Tags
	if tags == nil {
		tags = []string{}
	}
	return s.market.ListMarketWithSkills(ctx, tags, req.Query, req.SkillIDs, 1, limit)
}

// GetAgent 转 market.GetBySlug；返回结构里已经包含 capability / examples。
func (s *Service) GetAgent(ctx context.Context, req *GetAgentRequest) (*agent.AgentDetailResponse, error) {
	return s.market.GetBySlug(ctx, req.Slug)
}

// RunAgent 转 runtime.Run，并标记 source='mcp'，让 /usage 能识别来源。
func (s *Service) RunAgent(ctx context.Context, userID uuid.UUID, req *RunAgentRequest) (*runtime.RunResponse, error) {
	metadata := map[string]interface{}{}
	for k, v := range req.Metadata {
		metadata[k] = v
	}
	metadata["used_mcp_tools"] = appendStringList(metadata["used_mcp_tools"], "run_agent")
	return s.runtime.Run(ctx, userID, &runtime.RunRequest{
		AgentID:  req.AgentID,
		Input:    req.Input,
		Metadata: metadata,
	}, "mcp")
}

// GetRun 转 runtime.GetRun（owner 校验在 service 内做）。
func (s *Service) GetRun(ctx context.Context, userID, runID uuid.UUID) (*runtime.RunResponse, error) {
	return s.runtime.GetRun(ctx, userID, runID)
}

// CreateTask 把自然语言任务交给 task.Recommend，返回解析出的 skill + 推荐 Agent。
// 客户端拿到推荐后通常用 run_agent 实际调用。
func (s *Service) CreateTask(ctx context.Context, userID uuid.UUID, req *CreateTaskRequest) (*task.RecommendResponse, error) {
	return s.task.Recommend(ctx, userID, &task.RecommendRequest{
		Query:    req.Query,
		SkillIDs: req.SkillIDs,
		MCPTools: req.MCPTools,
	})
}

// Tools 返回工具描述，硬编码常量；MCP 客户端用 InputSchema 决定参数表单。
func (s *Service) Tools() []ToolDescriptor {
	return mcpTools
}

var mcpTools = []ToolDescriptor{
	{
		Name:        "search_agents",
		Description: "Search public Agent listings by text, tags, or declared Skill IDs.",
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"query": map[string]interface{}{"type": "string", "description": "Optional text matched against Agent names and descriptions."},
				"tags":  map[string]interface{}{"type": "array", "items": map[string]interface{}{"type": "string"}, "description": "Optional tags; an Agent matches when any tag is present."},
				"skill_ids": map[string]interface{}{
					"type":        "array",
					"maxItems":    5,
					"items":       map[string]interface{}{"type": "string"},
					"description": "Optional declared OpenLinker Skill IDs; an Agent matches when any ID is present.",
				},
				"limit": map[string]interface{}{"type": "integer", "minimum": 1, "maximum": 50, "default": 10},
			},
		},
	},
	{
		Name:        "get_agent",
		Description: "Get an Agent listing by slug, including its capability schemas and examples.",
		InputSchema: map[string]interface{}{
			"type":     "object",
			"required": []string{"slug"},
			"properties": map[string]interface{}{
				"slug": map[string]interface{}{"type": "string", "description": "Agent slug."},
			},
		},
	},
	{
		Name:        "run_agent",
		Description: "Invoke an Agent by ID and wait for the result.",
		InputSchema: map[string]interface{}{
			"type":     "object",
			"required": []string{"agent_id", "input"},
			"properties": map[string]interface{}{
				"agent_id": map[string]interface{}{"type": "string", "format": "uuid"},
				"input":    map[string]interface{}{"type": "object", "description": "Input object sent to the selected Agent."},
				"metadata": map[string]interface{}{"type": "object", "description": "Optional run metadata. Include task_id to associate the run with a task."},
			},
		},
	},
	{
		Name:        "get_run",
		Description: "Get a run by ID when the authenticated user is allowed to view it.",
		InputSchema: map[string]interface{}{
			"type":     "object",
			"required": []string{"run_id"},
			"properties": map[string]interface{}{
				"run_id": map[string]interface{}{"type": "string", "format": "uuid"},
			},
		},
	},
	{
		Name:        "create_task",
		Description: "Create a private task from a natural-language request and return Skill and MCP references, Agent recommendations, and an optional next action.",
		InputSchema: map[string]interface{}{
			"type":     "object",
			"required": []string{"query"},
			"properties": map[string]interface{}{
				"query":     map[string]interface{}{"type": "string", "minLength": 4, "maxLength": 500, "description": "Task description, 4 to 500 characters."},
				"skill_ids": map[string]interface{}{"type": "array", "maxItems": 5, "items": map[string]interface{}{"type": "string"}, "description": "Optional Skill IDs to include when matching Agents."},
				"mcp_tools": map[string]interface{}{"type": "array", "maxItems": 5, "items": map[string]interface{}{"type": "string", "enum": []string{"create_task", "search_agents", "get_agent", "run_agent", "get_run"}}, "description": "Optional MCP tool names associated with the task."},
			},
		},
		OutputSchema: map[string]interface{}{
			"type":     "object",
			"required": []string{"task_id", "visibility", "parsed_skills", "parsed_skill_refs", "mcp_tools", "mcp_tool_refs", "recommendations"},
			"properties": map[string]interface{}{
				"task_id":           map[string]interface{}{"type": "string", "format": "uuid"},
				"visibility":        map[string]interface{}{"type": "string", "enum": []string{"private", "public"}, "description": "Task visibility. create_task returns a private task unless a safe summary is published separately."},
				"public_summary":    map[string]interface{}{"type": "string", "description": "Public task summary, when available."},
				"parsed_skills":     map[string]interface{}{"type": "array", "items": map[string]interface{}{"type": "string"}, "description": "Skill IDs associated with the task, ordered by relevance."},
				"parsed_skill_refs": skillRefsSchema(),
				"mcp_tools":         map[string]interface{}{"type": "array", "items": map[string]interface{}{"type": "string"}},
				"mcp_tool_refs":     mcpToolRefsSchema(),
				"recommendations": map[string]interface{}{
					"type": "array",
					"items": map[string]interface{}{
						"type":     "object",
						"required": []string{"agent", "match_score", "why", "matched_skills"},
						"properties": map[string]interface{}{
							"agent": map[string]interface{}{
								"type":     "object",
								"required": []string{"id", "slug", "name", "description", "price_per_call_cents", "total_calls", "creator_name", "tags"},
								"properties": map[string]interface{}{
									"id":                   map[string]interface{}{"type": "string", "format": "uuid"},
									"slug":                 map[string]interface{}{"type": "string"},
									"name":                 map[string]interface{}{"type": "string"},
									"description":          map[string]interface{}{"type": "string"},
									"price_per_call_cents": map[string]interface{}{"type": "integer"},
									"total_calls":          map[string]interface{}{"type": "integer"},
									"avg_rating":           map[string]interface{}{"type": "number"},
									"creator_name":         map[string]interface{}{"type": "string"},
									"tags":                 map[string]interface{}{"type": "array", "items": map[string]interface{}{"type": "string"}},
								},
							},
							"match_score":    map[string]interface{}{"type": "number", "minimum": 0, "maximum": 1},
							"why":            map[string]interface{}{"type": "string"},
							"matched_skills": skillRefsSchema(),
						},
					},
				},
				"next_action": taskNextActionSchema(),
			},
		},
	},
}

func taskNextActionSchema() map[string]interface{} {
	return map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"type":   map[string]interface{}{"type": "string"},
			"label":  map[string]interface{}{"type": "string"},
			"hint":   map[string]interface{}{"type": "string"},
			"href":   map[string]interface{}{"type": "string"},
			"reason": map[string]interface{}{"type": "string"},
		},
	}
}

func mcpToolRefsSchema() map[string]interface{} {
	return map[string]interface{}{
		"type": "array",
		"items": map[string]interface{}{
			"type":     "object",
			"required": []string{"name", "description"},
			"properties": map[string]interface{}{
				"name":        map[string]interface{}{"type": "string"},
				"description": map[string]interface{}{"type": "string"},
			},
		},
	}
}

func skillRefsSchema() map[string]interface{} {
	return map[string]interface{}{
		"type": "array",
		"items": map[string]interface{}{
			"type":     "object",
			"required": []string{"id", "category", "name"},
			"properties": map[string]interface{}{
				"id":          map[string]interface{}{"type": "string"},
				"category":    map[string]interface{}{"type": "string"},
				"name":        map[string]interface{}{"type": "string"},
				"description": map[string]interface{}{"type": "string"},
			},
		},
	}
}

func appendStringList(raw interface{}, item string) []string {
	out := []string{}
	switch v := raw.(type) {
	case []string:
		out = append(out, v...)
	case []interface{}:
		for _, entry := range v {
			if s, ok := entry.(string); ok {
				out = append(out, s)
			}
		}
	case string:
		if v != "" {
			out = append(out, v)
		}
	}
	for _, existing := range out {
		if existing == item {
			return out
		}
	}
	return append(out, item)
}
