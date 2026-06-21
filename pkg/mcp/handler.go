package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/kinzhi/openlinker-core/pkg/agent"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
	"github.com/kinzhi/openlinker-core/pkg/runtime"
	"github.com/kinzhi/openlinker-core/pkg/task"
)

const mcpProtocolVersion = "2025-06-18"

// Handler /api/v1/mcp/* 路由。
//
// 路由组应挂 HybridAuthMiddleware：JWT 与访问令牌都能进，
// 但 handler 内强制只接受 apikey（assertAPIKeyAuth），避免浏览器 cookie 误调。
type Handler struct {
	svc       service
	validator *validator.Validate
}

type service interface {
	SearchAgents(ctx context.Context, req *SearchAgentsRequest) (*agent.MarketListResponse, error)
	GetAgent(ctx context.Context, req *GetAgentRequest) (*agent.AgentDetailResponse, error)
	RunAgent(ctx context.Context, userID uuid.UUID, req *RunAgentRequest) (*runtime.RunResponse, error)
	GetRun(ctx context.Context, userID, runID uuid.UUID) (*runtime.RunResponse, error)
	CreateTask(ctx context.Context, userID uuid.UUID, req *CreateTaskRequest) (*task.RecommendResponse, error)
	Tools() []ToolDescriptor
}

// NewHandler 构造 MCP handler。
func NewHandler(svc service) *Handler {
	return &Handler{
		svc:       svc,
		validator: validator.New(validator.WithRequiredStructEnabled()),
	}
}

// Register 挂载所有 MCP 路由。
//
//	GET  /mcp                 入口说明；若客户端请求 SSE stream，返回 405
//	POST /mcp                 MCP Streamable HTTP JSON-RPC（JSON response mode）
//	GET  /mcp/tools           工具元信息
//	POST /mcp/search_agents   市场搜索
//	POST /mcp/get_agent       Agent 详情
//	POST /mcp/run_agent       调用 Agent（写 runs.source='mcp'）
//	POST /mcp/get_run         查询调用结果
//	POST /mcp/create_task     自然语言 → 推荐 Agent
func (h *Handler) Register(api *echo.Group, mw echo.MiddlewareFunc) {
	g := api.Group("/mcp", mw)
	g.GET("", h.GetEndpointInfo)
	g.POST("", h.PostRPC)
	g.GET("/tools", h.GetTools)
	g.POST("/search_agents", h.PostSearchAgents)
	g.POST("/get_agent", h.PostGetAgent)
	g.POST("/run_agent", h.PostRunAgent)
	g.POST("/get_run", h.PostGetRun)
	g.POST("/create_task", h.PostCreateTask)
}

// GetEndpointInfo 让浏览器打开 /api/v1/mcp 时能看到真实用法。
// MCP 的独立 GET SSE stream 当前不实现；符合 Streamable HTTP 的 405 降级语义。
func (h *Handler) GetEndpointInfo(c echo.Context) error {
	if acceptsEventStream(c.Request()) {
		return c.NoContent(http.StatusMethodNotAllowed)
	}
	return c.JSON(http.StatusOK, map[string]interface{}{
		"name":             "openlinker-mcp",
		"transport":        "streamable_http_json_response",
		"protocol_version": mcpProtocolVersion,
		"endpoint":         "/api/v1/mcp",
		"auth":             "Authorization: Bearer ol_live_...",
		"methods":          []string{"initialize", "tools/list", "tools/call"},
		"tools":            toMCPTools(h.tools()),
		"rest_fallback":    "/api/v1/mcp/tools, /api/v1/mcp/search_agents, /api/v1/mcp/run_agent, /api/v1/mcp/get_run, /api/v1/mcp/create_task",
	})
}

// PostRPC exposes OpenLinker as an MCP server endpoint.
//
// It implements the core JSON-RPC methods MCP clients need for tool discovery
// and invocation. Responses use application/json rather than SSE streaming.
func (h *Handler) PostRPC(c echo.Context) error {
	raw, err := readJSONRPCBody(c)
	if err != nil {
		return writeRPCError(c, nil, http.StatusBadRequest, -32700, "Parse error")
	}
	if bytes.HasPrefix(bytes.TrimSpace(raw), []byte("[")) {
		return writeRPCError(c, nil, http.StatusOK, -32600, "Batch JSON-RPC requests are not supported")
	}

	var req rpcRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return writeRPCError(c, nil, http.StatusOK, -32600, "Invalid Request")
	}
	if req.Method == "" || len(req.ID) == 0 {
		return c.NoContent(http.StatusAccepted)
	}
	if req.JSONRPC != "2.0" {
		return writeRPCError(c, req.ID, http.StatusOK, -32600, "Invalid Request")
	}
	if err := assertAPIKeyAuth(c); err != nil {
		return writeRPCHTTPError(c, req.ID, err)
	}

	switch req.Method {
	case "initialize":
		return writeRPCResult(c, req.ID, map[string]interface{}{
			"protocolVersion": mcpProtocolVersion,
			"capabilities": map[string]interface{}{
				"tools": map[string]interface{}{
					"listChanged": false,
				},
			},
			"serverInfo": map[string]interface{}{
				"name":    "openlinker",
				"version": "0.1.0",
			},
		})
	case "tools/list":
		return writeRPCResult(c, req.ID, map[string]interface{}{
			"tools": toMCPTools(h.tools()),
		})
	case "tools/call":
		return h.postRPCToolCall(c, req.ID, req.Params)
	default:
		return writeRPCError(c, req.ID, http.StatusOK, -32601, "Method not found: "+req.Method)
	}
}

// GetTools 列出工具描述。所有 5 个工具都列出来；客户端按 name 选择调用。
func (h *Handler) GetTools(c echo.Context) error {
	if err := assertAPIKeyAuth(c); err != nil {
		return err
	}
	return c.JSON(http.StatusOK, ToolsResponse{Tools: h.tools()})
}

func (h *Handler) postRPCToolCall(c echo.Context, id json.RawMessage, params json.RawMessage) error {
	var req rpcToolCallParams
	if len(bytes.TrimSpace(params)) == 0 {
		return writeRPCError(c, id, http.StatusOK, -32602, "tools/call requires params")
	}
	if err := json.Unmarshal(params, &req); err != nil {
		return writeRPCError(c, id, http.StatusOK, -32602, "Invalid tools/call params")
	}
	if req.Name == "" {
		return writeRPCError(c, id, http.StatusOK, -32602, "tools/call params.name is required")
	}

	result, rpcErr := h.callTool(c, req.Name, req.Arguments)
	if rpcErr != nil {
		return writeRPCError(c, id, http.StatusOK, rpcErr.Code, rpcErr.Message)
	}
	return writeRPCResult(c, id, result)
}

func (h *Handler) callTool(c echo.Context, name string, args json.RawMessage) (mcpToolResult, *rpcError) {
	switch name {
	case "search_agents":
		if err := requireAPIKeyScope(c, "agents:read"); err != nil {
			return toolError(err), nil
		}
		var req SearchAgentsRequest
		if err := decodeToolArguments(args, &req); err != nil {
			return mcpToolResult{}, &rpcError{Code: -32602, Message: "Invalid arguments: " + err.Error()}
		}
		resp, err := h.svc.SearchAgents(c.Request().Context(), &req)
		return toolResult(resp, err), nil
	case "get_agent":
		if err := requireAPIKeyScope(c, "agents:read"); err != nil {
			return toolError(err), nil
		}
		var req GetAgentRequest
		if err := decodeToolArguments(args, &req); err != nil {
			return mcpToolResult{}, &rpcError{Code: -32602, Message: "Invalid arguments: " + err.Error()}
		}
		if err := h.validator.Struct(&req); err != nil {
			return mcpToolResult{}, &rpcError{Code: -32602, Message: "Invalid arguments: " + err.Error()}
		}
		resp, err := h.svc.GetAgent(c.Request().Context(), &req)
		return toolResult(resp, err), nil
	case "run_agent":
		if err := requireAPIKeyScope(c, "agents:run"); err != nil {
			return toolError(err), nil
		}
		uid, err := userIDFromCtx(c)
		if err != nil {
			return toolError(err), nil
		}
		var req RunAgentRequest
		if err := decodeToolArguments(args, &req); err != nil {
			return mcpToolResult{}, &rpcError{Code: -32602, Message: "Invalid arguments: " + err.Error()}
		}
		if err := h.validator.Struct(&req); err != nil {
			return mcpToolResult{}, &rpcError{Code: -32602, Message: "Invalid arguments: " + err.Error()}
		}
		resp, err := h.svc.RunAgent(c.Request().Context(), uid, &req)
		return toolResult(resp, err), nil
	case "get_run":
		if err := requireAPIKeyScope(c, "runs:read"); err != nil {
			return toolError(err), nil
		}
		uid, err := userIDFromCtx(c)
		if err != nil {
			return toolError(err), nil
		}
		var req GetRunRequest
		if err := decodeToolArguments(args, &req); err != nil {
			return mcpToolResult{}, &rpcError{Code: -32602, Message: "Invalid arguments: " + err.Error()}
		}
		if err := h.validator.Struct(&req); err != nil {
			return mcpToolResult{}, &rpcError{Code: -32602, Message: "Invalid arguments: " + err.Error()}
		}
		runID, err := uuid.Parse(req.RunID)
		if err != nil {
			return mcpToolResult{}, &rpcError{Code: -32602, Message: "Invalid arguments: run_id is not a uuid"}
		}
		resp, err := h.svc.GetRun(c.Request().Context(), uid, runID)
		return toolResult(resp, err), nil
	case "create_task":
		if err := requireAPIKeyScope(c, "tasks:write"); err != nil {
			return toolError(err), nil
		}
		uid, err := userIDFromCtx(c)
		if err != nil {
			return toolError(err), nil
		}
		var req CreateTaskRequest
		if err := decodeToolArguments(args, &req); err != nil {
			return mcpToolResult{}, &rpcError{Code: -32602, Message: "Invalid arguments: " + err.Error()}
		}
		if err := h.validator.Struct(&req); err != nil {
			return mcpToolResult{}, &rpcError{Code: -32602, Message: "Invalid arguments: " + err.Error()}
		}
		resp, err := h.svc.CreateTask(c.Request().Context(), uid, &req)
		return toolResult(resp, err), nil
	default:
		return mcpToolResult{}, &rpcError{Code: -32602, Message: "Unknown tool: " + name}
	}
}

// PostSearchAgents 市场搜索。
func (h *Handler) PostSearchAgents(c echo.Context) error {
	if err := requireAPIKeyScope(c, "agents:read"); err != nil {
		return err
	}
	var req SearchAgentsRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	resp, err := h.svc.SearchAgents(c.Request().Context(), &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// PostGetAgent 详情。
func (h *Handler) PostGetAgent(c echo.Context) error {
	if err := requireAPIKeyScope(c, "agents:read"); err != nil {
		return err
	}
	var req GetAgentRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.GetAgent(c.Request().Context(), &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// PostRunAgent 同步调用。把 source 设为 'mcp'。
func (h *Handler) PostRunAgent(c echo.Context) error {
	if err := requireAPIKeyScope(c, "agents:run"); err != nil {
		return err
	}
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	var req RunAgentRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.RunAgent(c.Request().Context(), uid, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// PostGetRun 查 run 详情。
func (h *Handler) PostGetRun(c echo.Context) error {
	if err := requireAPIKeyScope(c, "runs:read"); err != nil {
		return err
	}
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	var req GetRunRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	runID, err := uuid.Parse(req.RunID)
	if err != nil {
		return httpx.BadRequest("run_id 不是合法 uuid")
	}
	resp, err := h.svc.GetRun(c.Request().Context(), uid, runID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// PostCreateTask 把自然语言任务转 task.Recommend，返回 Top 3 Agent 推荐。
func (h *Handler) PostCreateTask(c echo.Context) error {
	if err := requireAPIKeyScope(c, "tasks:write"); err != nil {
		return err
	}
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	var req CreateTaskRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.CreateTask(c.Request().Context(), uid, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// assertAPIKeyAuth MCP 端点不接受网页登录会话；网页用户走 /run 即可。
func assertAPIKeyAuth(c echo.Context) error {
	if httpx.AuthMethodFrom(c) != "apikey" {
		return &httpx.HTTPError{
			Status:  http.StatusForbidden,
			Code:    httpx.CodeForbidden,
			Message: "MCP 端点仅接受访问令牌（ol_live_...）",
			Details: map[string]interface{}{
				"required_auth": "access_token",
				"next_action": map[string]string{
					"type":   "create_access_token",
					"label":  "创建访问令牌",
					"hint":   "在创作者中心的访问令牌页面创建 ol_live_... 令牌后，用 Authorization: Bearer 传给 MCP 客户端。",
					"href":   "/settings/api-keys",
					"reason": "MCP 是给外部客户端和 Agent 使用的服务端入口，不能使用浏览器 JWT 会话调用。",
				},
			},
		}
	}
	return nil
}

func requireAPIKeyScope(c echo.Context, scope string) error {
	if err := assertAPIKeyAuth(c); err != nil {
		return err
	}
	if !httpx.HasScope(c, scope) {
		return &httpx.HTTPError{
			Status:  http.StatusForbidden,
			Code:    httpx.CodeForbidden,
			Message: "访问令牌缺少 scope: " + scope,
			Details: map[string]interface{}{
				"required_scopes": []string{scope},
				"next_action": map[string]string{
					"type":   "create_access_token",
					"label":  "创建包含所需 scope 的访问令牌",
					"hint":   "在创作者中心的访问令牌页面选择 Agent / MCP 任务推荐模板，或手动勾选 " + scope + "。",
					"href":   "/settings/api-keys",
					"reason": "当前访问令牌权限不足，不能执行这个 MCP 工具调用。",
				},
			},
		}
	}
	return nil
}

func userIDFromCtx(c echo.Context) (uuid.UUID, error) {
	idStr := httpx.UserIDFrom(c)
	if idStr == "" {
		return uuid.Nil, httpx.Unauthorized("")
	}
	uid, err := uuid.Parse(idStr)
	if err != nil {
		return uuid.Nil, httpx.Unauthorized("token 无效")
	}
	return uid, nil
}

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  interface{}     `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type rpcToolCallParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
}

type mcpToolDefinition struct {
	Name         string                 `json:"name"`
	Description  string                 `json:"description"`
	InputSchema  map[string]interface{} `json:"inputSchema"`
	OutputSchema map[string]interface{} `json:"outputSchema,omitempty"`
}

type mcpTextContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type mcpToolResult struct {
	Content           []mcpTextContent `json:"content"`
	StructuredContent interface{}      `json:"structuredContent,omitempty"`
	IsError           bool             `json:"isError"`
}

func readJSONRPCBody(c echo.Context) (json.RawMessage, error) {
	defer c.Request().Body.Close()
	var raw json.RawMessage
	dec := json.NewDecoder(c.Request().Body)
	if err := dec.Decode(&raw); err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, errors.New("empty body")
	}
	return raw, nil
}

func writeRPCResult(c echo.Context, id json.RawMessage, result interface{}) error {
	return c.JSON(http.StatusOK, rpcResponse{
		JSONRPC: "2.0",
		ID:      id,
		Result:  result,
	})
}

func writeRPCError(c echo.Context, id json.RawMessage, status int, code int, message string) error {
	return c.JSON(status, rpcResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error: &rpcError{
			Code:    code,
			Message: message,
		},
	})
}

func writeRPCHTTPError(c echo.Context, id json.RawMessage, err error) error {
	var he *httpx.HTTPError
	if errors.As(err, &he) {
		return writeRPCError(c, id, he.Status, -32000, he.Message)
	}
	return writeRPCError(c, id, http.StatusInternalServerError, -32000, "internal error")
}

func decodeToolArguments(raw json.RawMessage, out interface{}) error {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		raw = []byte("{}")
	}
	return json.Unmarshal(raw, out)
}

func toolResult(payload interface{}, err error) mcpToolResult {
	if err != nil {
		return toolError(err)
	}
	pretty, marshalErr := json.MarshalIndent(payload, "", "  ")
	if marshalErr != nil {
		return toolError(marshalErr)
	}
	return mcpToolResult{
		Content: []mcpTextContent{{
			Type: "text",
			Text: string(pretty),
		}},
		StructuredContent: payload,
		IsError:           false,
	}
}

func toolError(err error) mcpToolResult {
	message := "tool execution failed"
	if err != nil && err.Error() != "" {
		message = err.Error()
	}
	var he *httpx.HTTPError
	if errors.As(err, &he) {
		return mcpToolResult{
			Content: []mcpTextContent{{
				Type: "text",
				Text: he.Message,
			}},
			StructuredContent: map[string]interface{}{
				"error": map[string]interface{}{
					"code":    he.Code,
					"message": he.Message,
					"details": he.Details,
				},
			},
			IsError: true,
		}
	}
	return mcpToolResult{
		Content: []mcpTextContent{{
			Type: "text",
			Text: message,
		}},
		IsError: true,
	}
}

func (h *Handler) tools() []ToolDescriptor {
	if h.svc == nil {
		return mcpTools
	}
	return h.svc.Tools()
}

func toMCPTools(tools []ToolDescriptor) []mcpToolDefinition {
	out := make([]mcpToolDefinition, 0, len(tools))
	for _, tool := range tools {
		inputSchema := tool.InputSchema
		if inputSchema == nil {
			inputSchema = map[string]interface{}{"type": "object"}
		}
		out = append(out, mcpToolDefinition{
			Name:         tool.Name,
			Description:  tool.Description,
			InputSchema:  inputSchema,
			OutputSchema: tool.OutputSchema,
		})
	}
	return out
}

func acceptsEventStream(r *http.Request) bool {
	return bytes.Contains([]byte(r.Header.Get("Accept")), []byte("text/event-stream"))
}
