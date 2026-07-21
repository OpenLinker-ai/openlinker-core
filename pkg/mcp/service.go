package mcp

import (
	"context"
	"net/http"

	"github.com/google/uuid"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/OpenLinker-ai/openlinker-core/pkg/task"
)

// 默认 search 分页：MCP 客户端不希望一次拿太多，给个保守上限。
const (
	defaultMCPSearchLimit int32 = 10
	maxMCPSearchLimit     int32 = 50
)

// Service 极薄 facade，把 MCP 9 个工具直接转到既有 service。
// 不重新初始化任何依赖：main.go 把现成的 *MarketService / *runtime.Service /
// *task.Service 直接注入。
type Service struct {
	market  *agent.MarketService
	runtime runtimeRunner
	task    *task.Service
}

type runtimeRunner interface {
	Run(ctx context.Context, userID uuid.UUID, req *runtime.RunRequest, source string) (*runtime.RunResponse, error)
	StartRun(ctx context.Context, userID uuid.UUID, req *runtime.RunRequest, source string) (*runtime.RunResponse, error)
	GetRun(ctx context.Context, userID, runID uuid.UUID) (*runtime.RunResponse, error)
	ListRunEventsPage(ctx context.Context, userID, runID uuid.UUID, afterSequence, limit int32) (*runtime.RunEventPageResponse, error)
	ListRunArtifacts(ctx context.Context, userID, runID uuid.UUID) ([]runtime.RunArtifactResponse, error)
	CancelRun(ctx context.Context, userID, runID uuid.UUID) (*runtime.RunResponse, error)
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
	return s.runAgent(ctx, userID, req, false)
}

// StartAgentRun 转 runtime.StartRun，立即返回可轮询的 run。
func (s *Service) StartAgentRun(ctx context.Context, userID uuid.UUID, req *RunAgentRequest) (*runtime.RunResponse, error) {
	return s.runAgent(ctx, userID, req, true)
}

func (s *Service) runAgent(ctx context.Context, userID uuid.UUID, req *RunAgentRequest, async bool) (*runtime.RunResponse, error) {
	if req == nil {
		return nil, httpx.Unprocessable("请求体不能为空")
	}
	if _, err := runtime.HashIdempotencyKey(req.IdempotencyKey); err != nil {
		class, _ := runtime.IdempotencyErrorClassOf(err)
		return nil, httpx.NewError(http.StatusUnprocessableEntity, httpx.ErrorCode(class), err.Error())
	}
	metadata := map[string]interface{}{}
	for k, v := range req.Metadata {
		metadata[k] = v
	}
	method := "run_agent"
	if async {
		method = "start_agent_run"
	}
	metadata["used_mcp_tools"] = appendStringList(metadata["used_mcp_tools"], method)
	runtimeReq := &runtime.RunRequest{
		AgentID:          req.AgentID,
		Input:            req.Input,
		Metadata:         metadata,
		IdempotencyKey:   req.IdempotencyKey,
		CreationProtocol: "mcp",
		CreationMethod:   method,
	}
	if async {
		return s.runtime.StartRun(ctx, userID, runtimeReq, "mcp")
	}
	return s.runtime.Run(ctx, userID, runtimeReq, "mcp")
}

// GetRun 转 runtime.GetRun（owner 校验在 service 内做）。
func (s *Service) GetRun(ctx context.Context, userID, runID uuid.UUID) (*runtime.RunResponse, error) {
	return s.runtime.GetRun(ctx, userID, runID)
}

// ListRunEvents 返回带保留边界的事件页，避免客户端把被清理的历史误判为空历史。
func (s *Service) ListRunEvents(ctx context.Context, userID, runID uuid.UUID, afterSequence, limit int32) (*runtime.RunEventPageResponse, error) {
	return s.runtime.ListRunEventsPage(ctx, userID, runID, afterSequence, limit)
}

// ListRunArtifacts 返回 run 的持久化产物。
func (s *Service) ListRunArtifacts(ctx context.Context, userID, runID uuid.UUID) ([]runtime.RunArtifactResponse, error) {
	return s.runtime.ListRunArtifacts(ctx, userID, runID)
}

// CancelRun 取消仍在执行的 owned run。
func (s *Service) CancelRun(ctx context.Context, userID, runID uuid.UUID) (*runtime.RunResponse, error) {
	return s.runtime.CancelRun(ctx, userID, runID)
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
		Annotations: toolAnnotations(true, false, true, true),
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
		Annotations: toolAnnotations(true, false, true, true),
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
		Description: "Invoke an Agent by ID and wait for the result. Repeating an identical request with the same idempotency key returns the original run.",
		Annotations: toolAnnotations(false, true, true, true),
		InputSchema: map[string]interface{}{
			"type":     "object",
			"required": []string{"agent_id", "input", "idempotency_key"},
			"properties": map[string]interface{}{
				"agent_id":        map[string]interface{}{"type": "string", "format": "uuid"},
				"input":           map[string]interface{}{"type": "object", "description": "Input object sent to the selected Agent."},
				"metadata":        map[string]interface{}{"type": "object", "description": "Optional run metadata. Include task_id to associate the run with a task."},
				"idempotency_key": map[string]interface{}{"type": "string", "minLength": 1, "maxLength": 255, "pattern": "^[ -~]{1,255}$", "description": "Caller-generated printable ASCII key. Reuse it only when retrying the same request."},
			},
		},
	},
	{
		Name:        "start_agent_run",
		Description: "Start an Agent invocation and return immediately with a run that can be inspected while it executes. Repeating an identical request with the same idempotency key returns the original run.",
		Annotations: toolAnnotations(false, true, true, true),
		InputSchema: map[string]interface{}{
			"type":     "object",
			"required": []string{"agent_id", "input", "idempotency_key"},
			"properties": map[string]interface{}{
				"agent_id":        map[string]interface{}{"type": "string", "format": "uuid"},
				"input":           map[string]interface{}{"type": "object", "description": "Input object sent to the selected Agent."},
				"metadata":        map[string]interface{}{"type": "object", "description": "Optional run metadata. Include task_id to associate the run with a task."},
				"idempotency_key": map[string]interface{}{"type": "string", "minLength": 1, "maxLength": 255, "pattern": "^[ -~]{1,255}$", "description": "Caller-generated printable ASCII key. Reuse it only when retrying the same request."},
			},
		},
	},
	{
		Name:        "get_run",
		Description: "Get a run by ID when the authenticated user is allowed to view it.",
		Annotations: toolAnnotations(true, false, true, false),
		InputSchema: map[string]interface{}{
			"type":     "object",
			"required": []string{"run_id"},
			"properties": map[string]interface{}{
				"run_id": map[string]interface{}{"type": "string", "format": "uuid"},
			},
		},
	},
	{
		Name:        "list_run_events",
		Description: "List an owned run's durable event page, including retention boundaries and completion state.",
		Annotations: toolAnnotations(true, false, true, false),
		InputSchema: map[string]interface{}{
			"type":     "object",
			"required": []string{"run_id"},
			"properties": map[string]interface{}{
				"run_id":         map[string]interface{}{"type": "string", "format": "uuid"},
				"after_sequence": map[string]interface{}{"type": "integer", "minimum": 0, "default": 0, "description": "Return events after this durable sequence cursor."},
				"limit":          map[string]interface{}{"type": "integer", "minimum": 1, "maximum": 500, "description": "Maximum events returned in this page."},
			},
		},
	},
	{
		Name:        "list_run_artifacts",
		Description: "List persisted artifacts produced by an owned run.",
		Annotations: toolAnnotations(true, false, true, false),
		InputSchema: map[string]interface{}{
			"type":     "object",
			"required": []string{"run_id"},
			"properties": map[string]interface{}{
				"run_id": map[string]interface{}{"type": "string", "format": "uuid"},
			},
		},
	},
	{
		Name:        "cancel_run",
		Description: "Request cancellation of an owned run that has not reached a terminal state.",
		Annotations: toolAnnotations(false, true, false, false),
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
		Annotations: toolAnnotations(false, false, false, false),
		InputSchema: map[string]interface{}{
			"type":     "object",
			"required": []string{"query"},
			"properties": map[string]interface{}{
				"query":     map[string]interface{}{"type": "string", "minLength": 4, "maxLength": 500, "description": "Task description, 4 to 500 characters."},
				"skill_ids": map[string]interface{}{"type": "array", "maxItems": 5, "items": map[string]interface{}{"type": "string"}, "description": "Optional Skill IDs to include when matching Agents."},
				"mcp_tools": map[string]interface{}{"type": "array", "maxItems": 5, "items": map[string]interface{}{"type": "string", "enum": []string{"create_task", "search_agents", "get_agent", "run_agent", "start_agent_run", "get_run", "list_run_events", "list_run_artifacts", "cancel_run"}}, "description": "Optional MCP tool names associated with the task."},
			},
		},
		OutputSchema: map[string]interface{}{
			"type":     "object",
			"required": []string{"task_id", "visibility", "parsed_skills", "parsed_skill_refs", "mcp_tools", "mcp_tool_refs", "recommendations"},
			"properties": map[string]interface{}{
				"task_id":           map[string]interface{}{"type": "string", "format": "uuid"},
				"visibility":        map[string]interface{}{"type": "string", "enum": []string{"private"}, "description": "Tasks are always private demand context owned by the caller."},
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

func toolAnnotations(readOnly, destructive, idempotent, openWorld bool) map[string]interface{} {
	return map[string]interface{}{
		"readOnlyHint":    readOnly,
		"destructiveHint": destructive,
		"idempotentHint":  idempotent,
		"openWorldHint":   openWorld,
	}
}

func taskNextActionSchema() map[string]interface{} {
	return map[string]interface{}{
		"type":     "object",
		"required": []string{"type", "label", "hint", "href", "reason"},
		"properties": map[string]interface{}{
			"type":        map[string]interface{}{"type": "string", "enum": []string{"connect_agent"}},
			"label":       map[string]interface{}{"type": "string"},
			"hint":        map[string]interface{}{"type": "string"},
			"href":        map[string]interface{}{"type": "string", "description": "Internal /publish link carrying the available query and Skill context."},
			"reason_code": map[string]interface{}{"type": "string"},
			"reason":      map[string]interface{}{"type": "string"},
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
