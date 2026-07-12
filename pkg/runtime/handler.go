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

	"github.com/OpenLinker-ai/openlinker-core/pkg/auth"
	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

const sseHeartbeatInterval = 15 * time.Second
const ssePollInterval = time.Second

// Handler 调用执行 HTTP 入口。
type Handler struct {
	svc       runtimeService
	validator *validator.Validate
	cfg       *config.Config
	runtimeV2 *RuntimeV2HTTPController
}

type runtimeService interface {
	Run(context.Context, uuid.UUID, *RunRequest, string) (*RunResponse, error)
	StartRun(context.Context, uuid.UUID, *RunRequest, string) (*RunResponse, error)
	GetRun(context.Context, uuid.UUID, uuid.UUID) (*RunResponse, error)
	ListRunEvents(context.Context, uuid.UUID, uuid.UUID, int32, int32) ([]RunEventResponse, error)
	ListRunEventsPage(context.Context, uuid.UUID, uuid.UUID, int32, int32) (*RunEventPageResponse, error)
	ListRunArtifacts(context.Context, uuid.UUID, uuid.UUID) ([]RunArtifactResponse, error)
	ListRunMessages(context.Context, uuid.UUID, uuid.UUID) ([]RunMessageResponse, error)
	ReportRunEvent(context.Context, uuid.UUID, string, *ReportRunEventRequest) (*RunEventResponse, error)
	ValidateRuntimeToken(context.Context, string, ...string) (db.AgentRuntimeToken, error)
}

// NewHandler 构造 Handler。cfg 可选（测试可省略）。
func NewHandler(svc runtimeService, cfg ...*config.Config) *Handler {
	h := &Handler{
		svc:       svc,
		validator: validator.New(validator.WithRequiredStructEnabled()),
		runtimeV2: newRuntimeV2HTTPControllerForService(svc),
	}
	if len(cfg) > 0 {
		h.cfg = cfg[0]
	}
	return h
}

// RegisterProtected 注册需要鉴权的端点，分别接收 /run 与 /runs/:id 的 middleware。
//
//	POST /run            同步调用 Agent   —— runMw（JWT + 访问令牌混合）
//	POST /runs           异步启动调用     —— runMw（JWT + 访问令牌混合）
//	GET  /runs/:id       单条调用详情     —— queryMw（可按部署选择 JWT-only 或 hybrid）
//	GET  /runs/:id/events 调用事件流      —— queryMw（轮询）
//	GET  /runs/:id/artifacts 运行产物      —— queryMw
//	GET  /runs/:id/messages 运行消息回放    —— queryMw
//	GET  /runs/:id/stream 调用事件 SSE    —— queryMw
//	POST /runs/:id/cancel 取消运行         —— queryMw
//	POST /runs/:id/replay 回放死信运行      —— runMw
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
	api.POST("/runs/:id/cancel", h.CancelRun, queryMw)
	api.POST("/runs/:id/replay", h.ReplayRun, runMw)
	api.POST("/runs/:id/events", h.PostRunEvent)
}

// RegisterAdmin mounts read-only runtime operational inventory. Core API owns
// the concrete JWT/admin middleware wiring so this package stays independent
// from admin policy.
func (h *Handler) RegisterAdmin(api *echo.Group, jwtMw, adminMw echo.MiddlewareFunc) {
	api.GET("/admin/runtime/dead-letters", h.ListRuntimeDeadLetters, jwtMw, adminMw)
	api.GET("/admin/runtime/nodes", h.ListRuntimeNodes, jwtMw, adminMw)
	api.POST("/admin/runtime/nodes/:id/drain", h.DrainRuntimeNode, jwtMw, adminMw)
	api.POST("/admin/runtime/nodes/:id/revoke", h.RevokeRuntimeNode, jwtMw, adminMw)
}

// CancelRun cancels an owned, cancellable run. The concrete Service already
// implements this method; the narrow assertion keeps existing handler fakes
// source-compatible.
func (h *Handler) CancelRun(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	runID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	if err := requireAPIKeyScope(c, "runs:cancel", &runID); err != nil {
		return err
	}
	canceler, ok := h.svc.(interface {
		CancelRun(context.Context, uuid.UUID, uuid.UUID) (*RunResponse, error)
	})
	if !ok {
		return httpx.ServiceUnavailable("Run 取消能力不可用")
	}
	resp, err := canceler.CancelRun(c.Request().Context(), uid, runID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// ReplayRun creates a new Run from one owned dead-letter Run. Agent policy and
// availability are re-evaluated by the normal creation path.
func (h *Handler) ReplayRun(c echo.Context) error {
	if err := auth.RequireAnyPermission(c, "agents:run", "agent"); err != nil {
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
	key := c.Request().Header.Get("Idempotency-Key")
	if _, err := HashIdempotencyKey(key); err != nil {
		return idempotencyHTTPError(err)
	}
	preview, err := h.svc.GetRun(c.Request().Context(), uid, runID)
	if err != nil {
		return err
	}
	if preview == nil {
		return httpx.Internal("查询调用记录失败")
	}
	agentID, err := uuid.Parse(preview.AgentID)
	if err != nil {
		return httpx.Internal("调用记录缺少 Agent 标识")
	}
	if err := requireAPIKeyScope(c, "agents:run", &agentID); err != nil {
		return err
	}
	replayer, ok := h.svc.(interface {
		ReplayRun(context.Context, uuid.UUID, uuid.UUID, string, string) (*RunResponse, error)
	})
	if !ok {
		return httpx.ServiceUnavailable("Run 回放能力不可用")
	}
	resp, err := replayer.ReplayRun(c.Request().Context(), uid, runID, key, sourceFromCtx(c))
	if err != nil {
		return err
	}
	return h.sendRunCreationResponse(c, uid, resp)
}

func (h *Handler) ListRuntimeDeadLetters(c echo.Context) error {
	limit, err := parseOptionalInt32(c.QueryParam("limit"))
	if err != nil || limit < 0 {
		return httpx.BadRequest("limit 不是合法非负整数")
	}
	offset, err := parseOptionalInt32(c.QueryParam("offset"))
	if err != nil || offset < 0 {
		return httpx.BadRequest("offset 不是合法非负整数")
	}
	lister, ok := h.svc.(interface {
		ListRuntimeDeadLetters(context.Context, int32, int32) (*RuntimeDeadLetterListResponse, error)
	})
	if !ok {
		return httpx.ServiceUnavailable("Runtime 死信查询能力不可用")
	}
	resp, err := lister.ListRuntimeDeadLetters(c.Request().Context(), limit, offset)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) ListRuntimeNodes(c echo.Context) error {
	limit, err := parseOptionalInt32(c.QueryParam("limit"))
	if err != nil || limit < 0 {
		return httpx.BadRequest("limit 不是合法非负整数")
	}
	offset, err := parseOptionalInt32(c.QueryParam("offset"))
	if err != nil || offset < 0 {
		return httpx.BadRequest("offset 不是合法非负整数")
	}
	lister, ok := h.svc.(interface {
		ListRuntimeNodes(context.Context, int32, int32) (*RuntimeNodeListResponse, error)
	})
	if !ok {
		return httpx.ServiceUnavailable("Runtime Node 管理能力不可用")
	}
	resp, err := lister.ListRuntimeNodes(c.Request().Context(), limit, offset)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) DrainRuntimeNode(c echo.Context) error {
	nodeID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	drainer, ok := h.svc.(interface {
		DrainRuntimeNode(context.Context, uuid.UUID) (*RuntimeNodeListItem, error)
	})
	if !ok {
		return httpx.ServiceUnavailable("Runtime Node 管理能力不可用")
	}
	resp, err := drainer.DrainRuntimeNode(c.Request().Context(), nodeID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) RevokeRuntimeNode(c echo.Context) error {
	nodeID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	var req RevokeRuntimeNodeRequest
	if err = c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	req.Reason = strings.TrimSpace(req.Reason)
	if err = h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	revoker, ok := h.svc.(interface {
		RevokeRuntimeNode(context.Context, uuid.UUID, string) (*RuntimeNodeListItem, error)
	})
	if !ok {
		return httpx.ServiceUnavailable("Runtime Node 管理能力不可用")
	}
	resp, err := revoker.RevokeRuntimeNode(c.Request().Context(), nodeID, req.Reason)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// RegisterAgentRuntime mounts only the runtime v2 transport. Pre-v2 paths are
// deliberately absent so they resolve as ordinary 404 responses.
func (h *Handler) RegisterAgentRuntime(api *echo.Group) {
	h.runtimeV2.Register(api)
}

// PostRun 调用 Agent。
//
// Endpoint 连接模式会同步等待 Agent 返回；其他运行模式由各自的调度路径处理。
// 失败 / 超时 / 取消 → status='failed' or 'timeout' or 'canceled'，已退款。
func (h *Handler) PostRun(c echo.Context) error {
	if err := auth.RequireAnyPermission(c, "agents:run", "agent"); err != nil {
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
	agentID, _ := uuid.Parse(req.AgentID)
	if err := requireAPIKeyScope(c, "agents:run", &agentID); err != nil {
		return err
	}
	if err := bindRESTRunIdempotency(c, &req); err != nil {
		return err
	}
	resp, err := h.svc.Run(c.Request().Context(), uid, &req, sourceFromCtx(c))
	if err != nil {
		return err
	}
	return h.sendRunCreationResponse(c, uid, resp)
}

// PostRunAsync 启动异步调用，立即返回 run_id，调用结果通过 GET /runs/:id 或 SSE 查询。
func (h *Handler) PostRunAsync(c echo.Context) error {
	if err := auth.RequireAnyPermission(c, "agents:run", "agent"); err != nil {
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
	agentID, _ := uuid.Parse(req.AgentID)
	if err := requireAPIKeyScope(c, "agents:run", &agentID); err != nil {
		return err
	}
	if err := bindRESTRunIdempotency(c, &req); err != nil {
		return err
	}
	resp, err := h.svc.StartRun(c.Request().Context(), uid, &req, sourceFromCtx(c))
	if err != nil {
		return err
	}
	return h.sendRunCreationResponse(c, uid, resp)
}

func bindRESTRunIdempotency(c echo.Context, req *RunRequest) error {
	if req == nil {
		return httpx.Unprocessable("请求体不能为空")
	}
	key := c.Request().Header.Get("Idempotency-Key")
	if _, err := HashIdempotencyKey(key); err != nil {
		return idempotencyHTTPError(err)
	}
	req.IdempotencyKey = key
	req.CreationProtocol = "rest"
	req.CreationMethod = "runs.create"
	return nil
}

func (h *Handler) sendRunCreationResponse(c echo.Context, userID uuid.UUID, resp *RunResponse) error {
	if resp == nil || strings.TrimSpace(resp.RunID) == "" {
		return httpx.Internal("创建调用记录失败")
	}
	wait, preferWait, err := parseRunPreferWait(c.Request().Header.Get("Prefer"))
	if err != nil {
		return err
	}
	wasReplayed := resp.Replayed
	if resp.Status == "running" && wait > 0 {
		runID, parseErr := uuid.Parse(resp.RunID)
		if parseErr != nil {
			return httpx.Internal("创建调用记录失败")
		}
		deadline := time.NewTimer(wait)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer deadline.Stop()
		defer ticker.Stop()
	waitLoop:
		for resp.Status == "running" {
			select {
			case <-c.Request().Context().Done():
				return c.Request().Context().Err()
			case <-deadline.C:
				break waitLoop
			case <-ticker.C:
				current, getErr := h.svc.GetRun(c.Request().Context(), userID, runID)
				if getErr != nil {
					return getErr
				}
				resp = current
				resp.Replayed = wasReplayed
			}
		}
	}
	location := "/api/v1/runs/" + resp.RunID
	c.Response().Header().Set("Location", location)
	status := http.StatusCreated
	if resp.Replayed {
		c.Response().Header().Set("Idempotency-Replayed", "true")
		status = http.StatusOK
		if resp.Status == "running" {
			status = http.StatusAccepted
		}
	}
	if preferWait {
		c.Response().Header().Set("Preference-Applied", "wait="+strconv.Itoa(int(wait/time.Second)))
		if resp.Status == "running" {
			status = http.StatusAccepted
		}
	}
	return c.JSON(status, resp)
}

func parseRunPreferWait(raw string) (time.Duration, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false, nil
	}
	found := false
	waitSeconds := 0
	for _, preference := range strings.Split(raw, ",") {
		preference = strings.TrimSpace(preference)
		if !strings.HasPrefix(strings.ToLower(preference), "wait=") {
			continue
		}
		if found {
			return 0, false, httpx.BadRequest("Prefer 只能包含一个 wait 参数")
		}
		found = true
		value := strings.TrimSpace(preference[len("wait="):])
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 0 || parsed > 30 {
			return 0, false, httpx.BadRequest("Prefer wait 必须是 0 到 30 秒的整数")
		}
		waitSeconds = parsed
	}
	if !found {
		return 0, false, nil
	}
	return time.Duration(waitSeconds) * time.Second, true, nil
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
	if afterSequence < 0 {
		return httpx.BadRequest("after_sequence 不能小于 0")
	}
	limit, err := parseOptionalInt32(c.QueryParam("limit"))
	if err != nil {
		return httpx.BadRequest("limit 不是合法整数")
	}

	page, err := h.svc.ListRunEventsPage(c.Request().Context(), uid, runID, afterSequence, limit)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, page)
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

	page, err := h.svc.ListRunEventsPage(c.Request().Context(), uid, runID, afterSequence, defaultRunEventsLimit)
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
		if page.Meta.RetentionGap {
			if err := writeSSERetentionGap(res.Writer, page.Meta); err != nil {
				return nil
			}
		}
		if page.Meta.EffectiveAfterSequence > afterSequence {
			afterSequence = page.Meta.EffectiveAfterSequence
		}
		terminal, nextSequence, err := writeSSEEvents(res.Writer, page.Items, afterSequence)
		if err != nil {
			return nil
		}
		streamComplete := page.Meta.StreamComplete
		afterSequence = nextSequence
		page = &RunEventPageResponse{Meta: RunEventPageMeta{
			RequestedAfterSequence: afterSequence,
			EffectiveAfterSequence: afterSequence,
		}}
		flusher.Flush()
		if terminal || streamComplete {
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
		case <-pollTicker.C:
			page, err = h.svc.ListRunEventsPage(ctx, uid, runID, afterSequence, defaultRunEventsLimit)
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
// jwt → 'web'（浏览器 / 仪表盘）；user_token → 'api'（访问令牌 / SDK）；
// MCP 路径的 handler 会显式传 "mcp"，绕过本函数。
func sourceFromCtx(c echo.Context) string {
	switch httpx.AuthMethodFrom(c) {
	case "user_token":
		return "api"
	case "jwt":
		return "web"
	default:
		return "web"
	}
}

func requireAPIKeyScope(c echo.Context, permission string, resourceIDs ...*uuid.UUID) error {
	resourceType := "run"
	if strings.HasPrefix(permission, "agents:") {
		resourceType = "agent"
	}
	var resourceID *uuid.UUID
	if len(resourceIDs) > 0 {
		resourceID = resourceIDs[0]
	}
	return auth.RequirePermission(c, permission, resourceType, resourceID)
}

func runtimeBearerToken(header string) (string, error) {
	parts := strings.SplitN(strings.TrimSpace(header), " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || strings.TrimSpace(parts[1]) == "" {
		return "", httpx.Unauthorized("缺少访问令牌")
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
	return checkedInt64ToInt32(n)
}

func afterSequenceFromSSE(c echo.Context) (int32, error) {
	if values, present := c.QueryParams()["after_sequence"]; present {
		if len(values) != 1 {
			return 0, errors.New("after_sequence must appear once")
		}
		return parseSSESequence(values[0])
	}
	raw := c.Request().Header.Get("Last-Event-ID")
	if raw == "" {
		return 0, nil
	}
	return parseSSESequence(raw)
}

func parseSSESequence(raw string) (int32, error) {
	if raw == "" {
		return 0, errors.New("sequence is empty")
	}
	for _, digit := range raw {
		if digit < '0' || digit > '9' {
			return 0, errors.New("sequence must be a non-negative decimal integer")
		}
	}
	n, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0, err
	}
	return int32(n), nil
}

func writeSSEEvents(w http.ResponseWriter, events []RunEventResponse, afterSequence int32) (bool, int32, error) {
	terminal := false
	nextSequence := afterSequence
	for _, event := range events {
		if event.Sequence <= nextSequence {
			continue
		}
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

func writeSSERetentionGap(w http.ResponseWriter, meta RunEventPageMeta) error {
	payload, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "event: run.stream.gap\ndata: %s\n\n", payload)
	return err
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
