package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/kinzhi/openlinker-core/pkg/config"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

const sseHeartbeatInterval = 15 * time.Second
const ssePollInterval = time.Second

// Handler 调用执行 HTTP 入口。
type Handler struct {
	svc       *Service
	validator *validator.Validate
	cfg       *config.Config
}

// NewHandler 构造 Handler。cfg 可选（测试可省略）。
func NewHandler(svc *Service, cfg ...*config.Config) *Handler {
	h := &Handler{
		svc:       svc,
		validator: validator.New(validator.WithRequiredStructEnabled()),
	}
	if len(cfg) > 0 {
		h.cfg = cfg[0]
	}
	return h
}

// RegisterProtected 注册需要鉴权的端点，分别接收 /run 与 /runs/:id 的 middleware。
//
//	POST /run            同步调用 Agent   —— runMw（JWT + API Key 混合）
//	POST /runs           异步启动调用     —— runMw（JWT + API Key 混合）
//	GET  /runs/:id       单条调用详情     —— queryMw（可按部署选择 JWT-only 或 hybrid）
//	GET  /runs/:id/events 调用事件流      —— queryMw（轮询）
//	GET  /runs/:id/artifacts 运行产物      —— queryMw
//	GET  /runs/:id/messages 运行消息回放    —— queryMw
//	GET  /runs/:id/stream 调用事件 SSE    —— queryMw
//	POST /runs/:id/events Agent 上报事件  —— X-OpenLinker-Token（不使用用户 JWT）
//
// GET /runs 列表由 dashboard 模块（subagent-6a）提供，本模块不挂。
//
// 调用方若两条路由想共用同一个 middleware，传入相同实例即可。
func (h *Handler) RegisterProtected(api *echo.Group, runMw, queryMw echo.MiddlewareFunc) {
	api.POST("/run", h.PostRun, runMw)
	api.POST("/runs", h.PostRunAsync, runMw)
	api.GET("/runs/:id", h.GetRun, queryMw)
	api.GET("/runs/:id/events", h.GetRunEvents, queryMw)
	api.GET("/runs/:id/artifacts", h.GetRunArtifacts, queryMw)
	api.GET("/runs/:id/messages", h.GetRunMessages, queryMw)
	api.GET("/runs/:id/stream", h.StreamRunEvents, queryMw)
	api.POST("/runs/:id/events", h.PostRunEvent)
}

// RegisterAgentRuntime mounts Runtime Token based endpoints used by runtime_pull Agents.
//
//	POST /agent-runtime/heartbeat        Agent 主动上报存活
//	GET  /agent-runtime/runs/claim       Agent 拉取一个 pending run
//	POST /agent-runtime/runs/:id/result  Agent 回传终态结果
func (h *Handler) RegisterAgentRuntime(api *echo.Group) {
	api.POST("/agent-runtime/heartbeat", h.PostAgentHeartbeat)
	api.GET("/agent-runtime/runs/claim", h.ClaimRuntimePullRun)
	api.POST("/agent-runtime/runs/:id/result", h.PostRuntimePullResult)
}

// PostRun 调用 Agent。
//
// 同步等待创作者 endpoint 返回（Phase 1 不做流式 / 异步队列）。
// 失败 / 超时 → status='failed' or 'timeout'，已退款。
func (h *Handler) PostRun(c echo.Context) error {
	if err := requireAPIKeyScope(c, "agents:run"); err != nil {
		return err
	}
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	var req RunRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.Run(c.Request().Context(), uid, &req, sourceFromCtx(c))
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// PostRunAsync 启动异步调用，立即返回 run_id，调用结果通过 GET /runs/:id 或 SSE 查询。
func (h *Handler) PostRunAsync(c echo.Context) error {
	if err := requireAPIKeyScope(c, "agents:run"); err != nil {
		return err
	}
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	var req RunRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.StartRun(c.Request().Context(), uid, &req, sourceFromCtx(c))
	if err != nil {
		return err
	}
	return c.JSON(http.StatusAccepted, resp)
}

// GetRun 查询单条调用详情（仅 owner）。
func (h *Handler) GetRun(c echo.Context) error {
	if err := requireAPIKeyScope(c, "runs:read"); err != nil {
		return err
	}
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	idStr := c.Param("id")
	runID, err := uuid.Parse(idStr)
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	resp, err := h.svc.GetRun(c.Request().Context(), uid, runID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// GetRunEvents 查询 run 事件流。SSE 接口后续会复用同一 service 方法。
func (h *Handler) GetRunEvents(c echo.Context) error {
	if err := requireAPIKeyScope(c, "runs:read"); err != nil {
		return err
	}
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	runID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}

	afterSequence, err := parseOptionalInt32(c.QueryParam("after_sequence"))
	if err != nil {
		return httpx.BadRequest("after_sequence 不是合法整数")
	}
	limit, err := parseOptionalInt32(c.QueryParam("limit"))
	if err != nil {
		return httpx.BadRequest("limit 不是合法整数")
	}

	events, err := h.svc.ListRunEvents(c.Request().Context(), uid, runID, afterSequence, limit)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]interface{}{"events": events})
}

// GetRunArtifacts 查询 run 持久化产物。只返回给 run owner。
func (h *Handler) GetRunArtifacts(c echo.Context) error {
	if err := requireAPIKeyScope(c, "runs:read"); err != nil {
		return err
	}
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	runID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	artifacts, err := h.svc.ListRunArtifacts(c.Request().Context(), uid, runID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]interface{}{"items": artifacts})
}

// GetRunMessages 查询 run 的稳定消息回放。只返回给 run owner。
func (h *Handler) GetRunMessages(c echo.Context) error {
	if err := requireAPIKeyScope(c, "runs:read"); err != nil {
		return err
	}
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	runID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	messages, err := h.svc.ListRunMessages(c.Request().Context(), uid, runID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]interface{}{"items": messages})
}

// StreamRunEvents 以 SSE 输出 run events。
//
// 已结束的 run 会回放事件后关闭；运行中的 run 会轮询等待新事件直到终态或客户端断开。
func (h *Handler) StreamRunEvents(c echo.Context) error {
	if err := requireAPIKeyScope(c, "runs:read"); err != nil {
		return err
	}
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	runID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	afterSequence, err := afterSequenceFromSSE(c)
	if err != nil {
		return httpx.BadRequest("after_sequence / Last-Event-ID 不是合法整数")
	}

	events, err := h.svc.ListRunEvents(c.Request().Context(), uid, runID, afterSequence, defaultRunEventsLimit)
	if err != nil {
		return err
	}

	res := c.Response()
	flusher, ok := res.Writer.(http.Flusher)
	if !ok {
		return httpx.Internal("当前响应不支持 streaming")
	}
	res.Header().Set(echo.HeaderContentType, "text/event-stream")
	res.Header().Set(echo.HeaderCacheControl, "no-cache")
	res.Header().Set(echo.HeaderConnection, "keep-alive")
	res.WriteHeader(http.StatusOK)

	ctx := c.Request().Context()
	pollTicker := time.NewTicker(ssePollInterval)
	defer pollTicker.Stop()
	heartbeatTicker := time.NewTicker(sseHeartbeatInterval)
	defer heartbeatTicker.Stop()

	for {
		terminal, nextSequence, err := writeSSEEvents(res.Writer, events, afterSequence)
		if err != nil {
			return nil
		}
		afterSequence = nextSequence
		flusher.Flush()
		if terminal {
			return nil
		}

		select {
		case <-ctx.Done():
			return nil
		case <-heartbeatTicker.C:
			if err := writeSSEHeartbeat(res.Writer); err != nil {
				return nil
			}
			flusher.Flush()
			events = nil
		case <-pollTicker.C:
			events, err = h.svc.ListRunEvents(ctx, uid, runID, afterSequence, defaultRunEventsLimit)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return nil
				}
				_ = writeSSEStreamError(res.Writer, err)
				flusher.Flush()
				return nil
			}
		}
	}
}

// PostRunEvent 允许 Agent endpoint 在执行中上报 run event。
//
// 鉴权使用 Agent 注册时的 endpoint_auth_header；平台调用 Agent 时也会把同一 secret
// 放进 X-OpenLinker-Token，Agent 可用它回调本接口。
func (h *Handler) PostRunEvent(c echo.Context) error {
	runID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	var req ReportRunEventRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	event, err := h.svc.ReportRunEvent(
		c.Request().Context(),
		runID,
		c.Request().Header.Get("X-OpenLinker-Token"),
		&req,
	)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusCreated, event)
}

func (h *Handler) ClaimRuntimePullRun(c echo.Context) error {
	token, err := runtimeBearerToken(c.Request().Header.Get(echo.HeaderAuthorization))
	if err != nil {
		return err
	}
	resp, err := h.svc.ClaimRuntimePullRun(c.Request().Context(), token)
	if err != nil {
		return err
	}
	if resp == nil {
		return c.NoContent(http.StatusNoContent)
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) PostAgentHeartbeat(c echo.Context) error {
	token, err := runtimeBearerToken(c.Request().Header.Get(echo.HeaderAuthorization))
	if err != nil {
		return err
	}
	resp, err := h.svc.HeartbeatAgent(c.Request().Context(), token)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) PostRuntimePullResult(c echo.Context) error {
	token, err := runtimeBearerToken(c.Request().Header.Get(echo.HeaderAuthorization))
	if err != nil {
		return err
	}
	runID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	var req RuntimePullResultRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.CompleteRuntimePullRun(c.Request().Context(), token, runID, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// userIDFromCtx 从 echo.Context 取出当前登录用户 uuid。
// JWT 中间件已写入 c.Get(httpx.CtxKeyUserID)。
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

// sourceFromCtx 把鉴权方式映射到 runs.source。
// jwt → 'web'（浏览器 / 仪表盘）；apikey → 'api'（cURL / SDK）；
// MCP 路径的 handler 会显式传 "mcp"，绕过本函数。
func sourceFromCtx(c echo.Context) string {
	switch httpx.AuthMethodFrom(c) {
	case "apikey":
		return "api"
	case "jwt":
		return "web"
	default:
		return "web"
	}
}

func requireAPIKeyScope(c echo.Context, scope string) error {
	if httpx.AuthMethodFrom(c) == "apikey" && !httpx.HasScope(c, scope) {
		return httpx.Forbidden("API Key 缺少 scope: " + scope)
	}
	return nil
}

func runtimeBearerToken(header string) (string, error) {
	parts := strings.SplitN(strings.TrimSpace(header), " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || strings.TrimSpace(parts[1]) == "" {
		return "", httpx.Unauthorized("缺少 Runtime Token")
	}
	return strings.TrimSpace(parts[1]), nil
}

func parseOptionalInt32(raw string) (int32, error) {
	if raw == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0, err
	}
	return int32(n), nil
}

func afterSequenceFromSSE(c echo.Context) (int32, error) {
	raw := c.QueryParam("after_sequence")
	if raw == "" {
		raw = c.Request().Header.Get("Last-Event-ID")
	}
	return parseOptionalInt32(raw)
}

func writeSSEEvents(w http.ResponseWriter, events []RunEventResponse, afterSequence int32) (bool, int32, error) {
	terminal := false
	nextSequence := afterSequence
	for _, event := range events {
		payload, err := json.Marshal(event)
		if err != nil {
			return terminal, nextSequence, err
		}
		if _, err := fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", event.Sequence, event.EventType, payload); err != nil {
			return terminal, nextSequence, err
		}
		nextSequence = event.Sequence
		if isTerminalRunEvent(event.EventType) {
			terminal = true
		}
	}
	return terminal, nextSequence, nil
}

func writeSSEHeartbeat(w http.ResponseWriter) error {
	_, err := fmt.Fprint(w, ": heartbeat\n\n")
	return err
}

func writeSSEStreamError(w http.ResponseWriter, err error) error {
	payload, marshalErr := json.Marshal(map[string]string{
		"error": err.Error(),
	})
	if marshalErr != nil {
		return marshalErr
	}
	_, writeErr := fmt.Fprintf(w, "event: run.stream.error\ndata: %s\n\n", payload)
	return writeErr
}

func isTerminalRunEvent(eventType string) bool {
	switch eventType {
	case "run.completed", "run.failed", "run.canceled":
		return true
	default:
		return false
	}
}
