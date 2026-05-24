package mcp

import (
	"net/http"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

// Handler /api/v1/mcp/* 路由。
//
// 路由组应挂 HybridAuthMiddleware：JWT 与 API Key 都能进，
// 但 handler 内强制只接受 apikey（assertAPIKeyAuth），避免浏览器 cookie 误调。
type Handler struct {
	svc       *Service
	validator *validator.Validate
}

// NewHandler 构造 MCP handler。
func NewHandler(svc *Service) *Handler {
	return &Handler{
		svc:       svc,
		validator: validator.New(validator.WithRequiredStructEnabled()),
	}
}

// Register 挂载所有 MCP 路由。
//
//	GET  /mcp/tools           工具元信息
//	POST /mcp/search_agents   市场搜索
//	POST /mcp/get_agent       Agent 详情
//	POST /mcp/run_agent       调用 Agent（写 runs.source='mcp'）
//	POST /mcp/get_run         查询调用结果
//	POST /mcp/create_task     自然语言 → 推荐 Agent
func (h *Handler) Register(api *echo.Group, mw echo.MiddlewareFunc) {
	g := api.Group("/mcp", mw)
	g.GET("/tools", h.GetTools)
	g.POST("/search_agents", h.PostSearchAgents)
	g.POST("/get_agent", h.PostGetAgent)
	g.POST("/run_agent", h.PostRunAgent)
	g.POST("/get_run", h.PostGetRun)
	g.POST("/create_task", h.PostCreateTask)
}

// GetTools 列出工具描述。所有 5 个工具都列出来；客户端按 name 选择调用。
func (h *Handler) GetTools(c echo.Context) error {
	if err := assertAPIKeyAuth(c); err != nil {
		return err
	}
	return c.JSON(http.StatusOK, ToolsResponse{Tools: h.svc.Tools()})
}

// PostSearchAgents 市场搜索。
func (h *Handler) PostSearchAgents(c echo.Context) error {
	if err := assertAPIKeyAuth(c); err != nil {
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
	if err := assertAPIKeyAuth(c); err != nil {
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
	if err := assertAPIKeyAuth(c); err != nil {
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
	if err := assertAPIKeyAuth(c); err != nil {
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
	if err := assertAPIKeyAuth(c); err != nil {
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

// assertAPIKeyAuth MCP 端点不接受浏览器 JWT；JWT 用户走 /run 即可。
func assertAPIKeyAuth(c echo.Context) error {
	if httpx.AuthMethodFrom(c) != "apikey" {
		return httpx.Forbidden("MCP 端点仅接受 API Key（sk_live_...）")
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
