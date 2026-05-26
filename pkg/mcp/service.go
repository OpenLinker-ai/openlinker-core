package mcp

import (
	"context"

	"github.com/google/uuid"

	"github.com/kinzhi/openlinker-core/pkg/agent"
	"github.com/kinzhi/openlinker-core/pkg/runtime"
	"github.com/kinzhi/openlinker-core/pkg/task"
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

// SearchAgents 转 market.ListMarket，按 query/tags 过滤。
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
	return s.market.ListMarket(ctx, tags, req.Query, 1, limit)
}

// GetAgent 转 market.GetBySlug；返回结构里已经包含 capability / examples。
func (s *Service) GetAgent(ctx context.Context, req *GetAgentRequest) (*agent.AgentDetailResponse, error) {
	return s.market.GetBySlug(ctx, req.Slug)
}

// RunAgent 转 runtime.Run，并标记 source='mcp'，让 /usage 能识别来源。
func (s *Service) RunAgent(ctx context.Context, userID uuid.UUID, req *RunAgentRequest) (*runtime.RunResponse, error) {
	return s.runtime.Run(ctx, userID, &runtime.RunRequest{
		AgentID:  req.AgentID,
		Input:    req.Input,
		Metadata: req.Metadata,
	}, "mcp")
}

// GetRun 转 runtime.GetRun（owner 校验在 service 内做）。
func (s *Service) GetRun(ctx context.Context, userID, runID uuid.UUID) (*runtime.RunResponse, error) {
	return s.runtime.GetRun(ctx, userID, runID)
}

// CreateTask 把自然语言任务交给 task.Recommend，返回解析出的 skill + 推荐 Agent。
// 客户端拿到推荐后通常用 run_agent 实际调用。
func (s *Service) CreateTask(ctx context.Context, userID uuid.UUID, req *CreateTaskRequest) (*task.RecommendResponse, error) {
	return s.task.Recommend(ctx, userID, req.Query)
}

// Tools 返回工具描述，硬编码常量；MCP 客户端用 InputSchema 决定参数表单。
func (s *Service) Tools() []ToolDescriptor {
	return mcpTools
}

var mcpTools = []ToolDescriptor{
	{
		Name:        "search_agents",
		Description: "在 OpenLinker 市场搜索 Agent。可按关键词 / tag 过滤；返回名称、价格、调用次数等列表项。",
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"query": map[string]interface{}{"type": "string", "description": "可选关键词，匹配名称和描述"},
				"tags":  map[string]interface{}{"type": "array", "items": map[string]interface{}{"type": "string"}, "description": "tag 过滤（任意命中）"},
				"limit": map[string]interface{}{"type": "integer", "minimum": 1, "maximum": 50, "default": 10},
			},
		},
	},
	{
		Name:        "get_agent",
		Description: "按 slug 获取单个 Agent 的详情，包括能力声明（input/output JSON Schema）和示例。",
		InputSchema: map[string]interface{}{
			"type":     "object",
			"required": []string{"slug"},
			"properties": map[string]interface{}{
				"slug": map[string]interface{}{"type": "string", "description": "市场 URL 中的 slug"},
			},
		},
	},
	{
		Name:        "run_agent",
		Description: "调用一个 Agent。可直接传 search_agents/get_agent 的 agent_id，也可先用 create_task 让 OpenLinker 推荐后再选择 agent_id；同步等待结果，当前阶段免费运行，不扣除余额。",
		InputSchema: map[string]interface{}{
			"type":     "object",
			"required": []string{"agent_id", "input"},
			"properties": map[string]interface{}{
				"agent_id": map[string]interface{}{"type": "string", "format": "uuid"},
				"input":    map[string]interface{}{"type": "object", "description": "透传给创作者 endpoint 的输入"},
				"metadata": map[string]interface{}{"type": "object", "description": "可选 metadata，原样透传"},
			},
		},
	},
	{
		Name:        "get_run",
		Description: "按 run_id 查询调用结果。仅 owner 可见。",
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
		Description: "把一段自然语言任务交给 OpenLinker 的发布任务流解析。返回任务 ID、解析出的 skill 引用、Top 3 推荐 Agent 以及每个 Agent 命中的 matched_skills；调用方通常再用 run_agent 触发选定的 Agent。",
		InputSchema: map[string]interface{}{
			"type":     "object",
			"required": []string{"query"},
			"properties": map[string]interface{}{
				"query": map[string]interface{}{"type": "string", "minLength": 4, "maxLength": 500, "description": "自然语言任务描述，4-500 字符"},
			},
		},
		OutputSchema: map[string]interface{}{
			"type":     "object",
			"required": []string{"task_id", "parsed_skills", "parsed_skill_refs", "recommendations"},
			"properties": map[string]interface{}{
				"task_id":           map[string]interface{}{"type": "string", "format": "uuid"},
				"parsed_skills":     map[string]interface{}{"type": "array", "items": map[string]interface{}{"type": "string"}, "description": "解析出的 skill_id 列表，按任务相关性排序"},
				"parsed_skill_refs": skillRefsSchema(),
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
			},
		},
	},
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
