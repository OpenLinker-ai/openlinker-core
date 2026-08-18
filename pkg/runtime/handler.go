package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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
	svc                runtimeService
	validator          *validator.Validate
	cfg                *config.Config
	runtime            *RuntimeHTTPController
	observer           WorkerObserver
	runUpdates         RunUpdateSource
	browserControl     *BrowserHumanControl
	browserObservation *BrowserObservation
}

type runtimeService interface {
	Run(context.Context, uuid.UUID, *RunRequest, string) (*RunResponse, error)
	StartRun(context.Context, uuid.UUID, *RunRequest, string) (*RunResponse, error)
	GetRun(context.Context, uuid.UUID, uuid.UUID) (*RunResponse, error)
	ListRunEvents(context.Context, uuid.UUID, uuid.UUID, int32, int32) ([]RunEventResponse, error)
	ListRunEventsPage(context.Context, uuid.UUID, uuid.UUID, int32, int32) (*RunEventPageResponse, error)
	ListRunArtifacts(context.Context, uuid.UUID, uuid.UUID) ([]RunArtifactResponse, error)
	ListRunMessages(context.Context, uuid.UUID, uuid.UUID) ([]RunMessageResponse, error)
	ValidateRuntimeToken(context.Context, string, ...string) (db.AgentRuntimeToken, error)
}

type runWaitStatusService interface {
	GetRunWaitStatus(context.Context, uuid.UUID, uuid.UUID) (string, error)
}

// NewHandler 构造 Handler。cfg 可选（测试可省略）。
func NewHandler(svc runtimeService, cfg ...*config.Config) *Handler {
	h := &Handler{
		svc:       svc,
		validator: validator.New(validator.WithRequiredStructEnabled()),
		runtime:   newRuntimeHTTPControllerForService(svc),
	}
	if provider, ok := svc.(interface {
		BrowserHumanControl() *BrowserHumanControl
	}); ok {
		h.browserControl = provider.BrowserHumanControl()
	}
	if provider, ok := svc.(interface {
		BrowserObservation() *BrowserObservation
	}); ok {
		h.browserObservation = provider.BrowserObservation()
	}
	if len(cfg) > 0 {
		h.cfg = cfg[0]
	}
	return h
}

// SetWorkerObserver installs payload-free test instrumentation. It does not
// change response, retry, timeout, or query behavior.
func (h *Handler) SetWorkerObserver(observer WorkerObserver) {
	if h != nil {
		h.observer = observer
	}
}

func (h *Handler) SetRunUpdateSource(source RunUpdateSource) {
	if h != nil {
		h.runUpdates = source
	}
}

// RegisterProtected 注册需要鉴权的端点，分别接收 /run 与 /runs/:id 的 middleware。
//
//	POST /run            同步调用 Agent   —— runMw（JWT + User Token 混合）
//	POST /runs           异步启动调用     —— runMw（JWT + User Token 混合）
//	GET  /runs/:id       单条调用详情     —— queryMw（可按部署选择 JWT-only 或 hybrid）
//	GET  /runs/:id/events 调用事件流      —— queryMw（轮询）
//	GET  /runs/:id/artifacts 运行产物      —— queryMw
//	GET  /runs/:id/messages 运行消息回放    —— queryMw
//	GET  /runs/:id/stream 调用事件 SSE    —— queryMw
//	POST /runs/:id/cancel 取消运行         —— queryMw
//	POST /runs/:id/replay 回放死信运行      —— runMw
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
	api.GET("/runs/:id/browser-control", h.GetBrowserControl, queryMw)
	api.POST("/runs/:id/browser-control/claim", h.ClaimBrowserControl, queryMw)
	api.POST("/runs/:id/browser-control/release", h.ReleaseBrowserControl, queryMw)
	api.POST("/runs/:id/browser-control/resume", h.ResumeBrowserControl, queryMw)
	api.POST("/runs/:id/browser-control/input", h.SendBrowserControlInput, queryMw)
	api.GET("/runs/:id/browser-control/frame", h.GetBrowserControlFrame, queryMw)
}

func (h *Handler) GetBrowserControl(c echo.Context) error {
	userID, runID, err := h.browserControlIdentity(c)
	if err != nil {
		return err
	}
	state, err := h.browserControl.State(c.Request().Context(), userID, runID)
	if err != nil {
		return browserControlHTTPError(err)
	}
	return c.JSON(http.StatusOK, state)
}

func (h *Handler) ClaimBrowserControl(c echo.Context) error {
	return h.browserControlTransition(c, h.browserControl.Claim)
}

func (h *Handler) ReleaseBrowserControl(c echo.Context) error {
	return h.browserControlTransition(c, h.browserControl.Release)
}

func (h *Handler) ResumeBrowserControl(c echo.Context) error {
	return h.browserControlTransition(c, h.browserControl.Resume)
}

func (h *Handler) browserControlTransition(
	c echo.Context,
	transition func(
		context.Context,
		uuid.UUID,
		uuid.UUID,
	) (BrowserHumanControlState, error),
) error {
	userID, runID, err := h.browserControlIdentity(c)
	if err != nil {
		return err
	}
	state, err := transition(c.Request().Context(), userID, runID)
	if err != nil {
		return browserControlHTTPError(err)
	}
	return c.JSON(http.StatusOK, state)
}

func (h *Handler) SendBrowserControlInput(c echo.Context) error {
	userID, runID, err := h.browserControlIdentity(c)
	if err != nil {
		return err
	}
	var input BrowserViewerInputPayload
	decoder := json.NewDecoder(http.MaxBytesReader(
		c.Response().Writer,
		c.Request().Body,
		64<<10,
	))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		return httpx.BadRequest("浏览器人控输入无效")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return httpx.BadRequest("浏览器人控输入包含多余数据")
	}
	if err := h.browserControl.Input(
		c.Request().Context(),
		userID,
		runID,
		input,
	); err != nil {
		return browserControlHTTPError(err)
	}
	return c.NoContent(http.StatusAccepted)
}

func (h *Handler) GetBrowserControlFrame(c echo.Context) error {
	userID, runID, err := h.browserControlIdentity(c)
	if err != nil {
		return err
	}
	var after uint64
	if raw := strings.TrimSpace(c.QueryParam("after")); raw != "" {
		after, err = strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return httpx.BadRequest("after 必须是非负整数")
		}
	}
	wait := 25 * time.Second
	if raw := strings.TrimSpace(c.QueryParam("wait")); raw != "" {
		seconds, parseErr := strconv.Atoi(raw)
		if parseErr != nil || seconds < 0 || seconds > 25 {
			return httpx.BadRequest("wait 必须是 0 到 25 秒")
		}
		wait = time.Duration(seconds) * time.Second
	}
	ctx, cancel := context.WithTimeout(c.Request().Context(), wait)
	defer cancel()
	frame, err := h.browserControl.WaitFrame(ctx, userID, runID, after)
	if errors.Is(err, context.DeadlineExceeded) {
		return c.NoContent(http.StatusNoContent)
	}
	if err != nil {
		return browserControlHTTPError(err)
	}
	return c.JSON(http.StatusOK, frame)
}

func (h *Handler) browserControlIdentity(
	c echo.Context,
) (uuid.UUID, uuid.UUID, error) {
	if h == nil || h.browserControl == nil {
		return uuid.Nil, uuid.Nil, httpx.ServiceUnavailable("浏览器人控能力不可用")
	}
	userID, err := userIDFromCtx(c)
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	runID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return uuid.Nil, uuid.Nil, httpx.BadRequest("id 不是合法 uuid")
	}
	return userID, runID, nil
}

func browserControlHTTPError(err error) error {
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return httpx.NotFound("浏览器人控会话不存在")
	case strings.Contains(err.Error(), "unavailable"):
		return httpx.ServiceUnavailable("浏览器人控通道不可用")
	case strings.Contains(err.Error(), "not allowed"),
		strings.Contains(err.Error(), "expired"),
		strings.Contains(err.Error(), "not active"),
		strings.Contains(err.Error(), "changed"),
		strings.Contains(err.Error(), "stale"):
		return echo.NewHTTPError(http.StatusConflict, err.Error())
	default:
		return httpx.Internal("浏览器人控操作失败")
	}
}

// RegisterAdmin mounts read-only runtime operational inventory. Core API owns
// the concrete JWT/admin middleware wiring so this package stays independent
// from admin policy.
// RegisterObservation mounts read-only observation on JWT only. It is not folded
// into RegisterProtected because that group runs hybrid middleware: observation
// is a person watching a live screen, so it must bind to a short-lived session
// rather than a long-lived token that can be scripted.
func (h *Handler) RegisterObservation(api *echo.Group, jwtMw echo.MiddlewareFunc) {
	api.GET("/runs/:id/observation", h.GetBrowserObservation, jwtMw)
	api.POST("/runs/:id/observation/start", h.StartBrowserObservation, jwtMw)
	api.POST("/runs/:id/observation/stop", h.StopBrowserObservation, jwtMw)
	api.GET("/runs/:id/observation/frame", h.GetBrowserObservationFrame, jwtMw)
}

func (h *Handler) RegisterAdmin(api *echo.Group, jwtMw, adminMw echo.MiddlewareFunc) {
	// Cross-user observation is a separate route with its own permission, so an
	// owner-scoped grant can never widen into watching someone else's Run.
	api.POST(
		"/admin/runs/:id/observation/start",
		h.StartAdminBrowserObservation,
		jwtMw,
		adminMw,
	)
	api.GET("/admin/runtime/dead-letters", h.ListRuntimeDeadLetters, jwtMw, adminMw)
	api.GET("/admin/runtime/nodes", h.ListRuntimeNodes, jwtMw, adminMw)
	api.POST("/admin/runtime/nodes/:id/drain", h.DrainRuntimeNode, jwtMw, adminMw)
	api.POST("/admin/runtime/nodes/:id/activate", h.ActivateRuntimeNode, jwtMw, adminMw)
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

func (h *Handler) ActivateRuntimeNode(c echo.Context) error {
	nodeID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	activator, ok := h.svc.(interface {
		ActivateRuntimeNode(context.Context, uuid.UUID) (*RuntimeNodeListItem, error)
	})
	if !ok {
		return httpx.ServiceUnavailable("Runtime Node 管理能力不可用")
	}
	resp, err := activator.ActivateRuntimeNode(c.Request().Context(), nodeID)
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

// RegisterAgentRuntime mounts the canonical Agent Runtime transport. The
// public API listener blocks this entire route family; only the dedicated mTLS
// listener dispatches it. Version compatibility is negotiated in the runtime
// handshake instead of being exposed in the URL.
func (h *Handler) RegisterAgentRuntime(api *echo.Group) {
	h.runtime.Register(api)
}

// RegisterAgentRuntimeAttachOnly mounts the cutover-only Session lifecycle.
// Normal execution routes remain owned by RegisterAgentRuntime.
func (h *Handler) RegisterAgentRuntimeAttachOnly(api *echo.Group) {
	h.runtime.RegisterAttachOnly(api)
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
		if h.runUpdates != nil && h.runUpdates.Healthy() {
			subscription, subscribeErr := h.runUpdates.SubscribeRun(runID)
			if subscribeErr == nil {
				defer subscription.Close()
				resp, err = h.waitForRunCreationUpdate(
					c.Request().Context(), userID, runID, resp, wasReplayed, wait, subscription,
				)
				if err != nil {
					return err
				}
			} else {
				resp, err = h.pollRunCreationUpdate(c.Request().Context(), userID, runID, resp, wasReplayed, time.Now().Add(wait))
			}
		} else {
			resp, err = h.pollRunCreationUpdate(c.Request().Context(), userID, runID, resp, wasReplayed, time.Now().Add(wait))
		}
		if err != nil {
			return err
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

func (h *Handler) waitForRunCreationUpdate(
	ctx context.Context,
	userID, runID uuid.UUID,
	current *RunResponse,
	wasReplayed bool,
	wait time.Duration,
	subscription RunUpdateSubscription,
) (*RunResponse, error) {
	deadline := time.Now().Add(wait)
	reason := "event_initial"
	for current.Status == "running" {
		observeWorker(h.observer, "runtime.prefer_wait.run_query", reason, 1)
		updated, err := h.svc.GetRun(ctx, userID, runID)
		if err != nil {
			return nil, err
		}
		current = updated
		current.Replayed = wasReplayed
		if current.Status != "running" {
			break
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		healthCheck := 2 * time.Second
		if remaining < healthCheck {
			healthCheck = remaining
		}
		waitCtx, cancel := context.WithTimeout(ctx, healthCheck)
		waitErr := subscription.Wait(waitCtx)
		cancel()
		if waitErr == nil {
			reason = "event_wake"
			continue
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if time.Until(deadline) <= 0 {
			reason = "deadline_final"
			continue
		}
		if !h.runUpdates.Healthy() {
			return h.pollRunCreationUpdate(ctx, userID, runID, current, wasReplayed, deadline)
		}
		if !errors.Is(waitErr, context.DeadlineExceeded) {
			// The wake channel is advisory. Its failure must never become an
			// HTTP failure when the authoritative polling path is available.
			return h.pollRunCreationUpdate(ctx, userID, runID, current, wasReplayed, deadline)
		}
	}
	return current, nil
}

func (h *Handler) pollRunCreationUpdate(
	ctx context.Context,
	userID, runID uuid.UUID,
	current *RunResponse,
	wasReplayed bool,
	deadline time.Time,
) (*RunResponse, error) {
	for current.Status == "running" {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		wait := 100 * time.Millisecond
		if remaining < wait {
			wait = remaining
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
		observeWorker(h.observer, "runtime.prefer_wait.run_query", "ticker", 1)
		updated, err := h.svc.GetRun(ctx, userID, runID)
		if err != nil {
			return nil, err
		}
		current = updated
		current.Replayed = wasReplayed
	}
	return current, nil
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
	wait, preferWait, err := parseRunPreferWait(c.Request().Header.Get("Prefer"))
	if err != nil {
		return err
	}
	if preferWait && wait > 0 {
		var subscription RunUpdateSubscription
		if h.runUpdates != nil && h.runUpdates.Healthy() {
			subscription, _ = h.runUpdates.SubscribeRun(runID)
			if subscription != nil {
				defer subscription.Close()
			}
		}
		if subscription != nil {
			err = h.waitForRunStatus(c.Request().Context(), uid, runID, wait, subscription)
		} else {
			err = h.pollRunWaitStatus(c.Request().Context(), uid, runID, time.Now().Add(wait), "initial")
		}
		if err != nil {
			return err
		}
	}
	resp, err := h.svc.GetRun(c.Request().Context(), uid, runID)
	if err != nil {
		return err
	}
	if preferWait {
		c.Response().Header().Set("Preference-Applied", "wait="+strconv.Itoa(int(wait/time.Second)))
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) waitForRunStatus(
	ctx context.Context,
	userID, runID uuid.UUID,
	wait time.Duration,
	subscription RunUpdateSubscription,
) error {
	deadline := time.Now().Add(wait)
	status, err := h.getRunWaitStatus(ctx, userID, runID, "event_initial")
	if err != nil {
		return err
	}
	for status == "running" {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil
		}
		healthCheck := 2 * time.Second
		if remaining < healthCheck {
			healthCheck = remaining
		}
		waitCtx, cancel := context.WithTimeout(ctx, healthCheck)
		waitErr := subscription.Wait(waitCtx)
		cancel()
		if waitErr == nil {
			status, err = h.getRunWaitStatus(ctx, userID, runID, "event_wake")
			if err != nil {
				return err
			}
			continue
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Until(deadline) <= 0 {
			return nil
		}
		if h.runUpdates == nil || !h.runUpdates.Healthy() || !errors.Is(waitErr, context.DeadlineExceeded) {
			return h.pollRunWaitStatus(ctx, userID, runID, deadline, "fallback")
		}
	}
	return nil
}

func (h *Handler) pollRunWaitStatus(
	ctx context.Context,
	userID, runID uuid.UUID,
	deadline time.Time,
	initialReason string,
) error {
	status, err := h.getRunWaitStatus(ctx, userID, runID, initialReason)
	if err != nil {
		return err
	}
	for status == "running" {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil
		}
		wait := 100 * time.Millisecond
		if remaining < wait {
			wait = remaining
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
		status, err = h.getRunWaitStatus(ctx, userID, runID, "ticker")
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *Handler) getRunWaitStatus(ctx context.Context, userID, runID uuid.UUID, reason string) (string, error) {
	observeWorker(h.observer, "runtime.prefer_wait.run_status_query", reason, 1)
	if service, ok := h.svc.(runWaitStatusService); ok {
		return service.GetRunWaitStatus(ctx, userID, runID)
	}
	resp, err := h.svc.GetRun(ctx, userID, runID)
	if err != nil {
		return "", err
	}
	if resp == nil {
		return "", httpx.Internal("查询调用记录失败")
	}
	return resp.Status, nil
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
	var runSubscription RunUpdateSubscription
	if h.runUpdates != nil && h.runUpdates.Healthy() {
		runSubscription, _ = h.runUpdates.SubscribeRun(runID)
		if runSubscription != nil {
			defer runSubscription.Close()
		}
	}

	observeWorker(h.observer, "runtime.sse.run_events_query", "initial", 1)
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
	var pollTicker, heartbeatTicker *time.Ticker
	startPolling := func() {
		if pollTicker == nil {
			pollTicker = time.NewTicker(ssePollInterval)
			heartbeatTicker = time.NewTicker(sseHeartbeatInterval)
		}
	}
	defer func() {
		if pollTicker != nil {
			pollTicker.Stop()
			heartbeatTicker.Stop()
		}
	}()
	if runSubscription == nil {
		startPolling()
	}
	nextHeartbeat := time.Now().Add(sseHeartbeatInterval)

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

		if runSubscription == nil {
			select {
			case <-ctx.Done():
				return nil
			case <-heartbeatTicker.C:
				if err := writeSSEHeartbeat(res.Writer); err != nil {
					return nil
				}
				flusher.Flush()
			case <-pollTicker.C:
				observeWorker(h.observer, "runtime.sse.run_events_query", "ticker", 1)
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
			continue
		}

		for runSubscription != nil {
			wait := 2 * time.Second
			if untilHeartbeat := time.Until(nextHeartbeat); untilHeartbeat < wait {
				wait = untilHeartbeat
			}
			if wait <= 0 {
				if err := writeSSEHeartbeat(res.Writer); err != nil {
					return nil
				}
				flusher.Flush()
				nextHeartbeat = time.Now().Add(sseHeartbeatInterval)
				continue
			}
			waitCtx, cancel := context.WithTimeout(ctx, wait)
			waitErr := runSubscription.Wait(waitCtx)
			cancel()
			if waitErr == nil {
				observeWorker(h.observer, "runtime.sse.run_events_query", "event_wake", 1)
				page, err = h.svc.ListRunEventsPage(ctx, uid, runID, afterSequence, defaultRunEventsLimit)
				break
			}
			if ctx.Err() != nil {
				return nil
			}
			if time.Now().After(nextHeartbeat) || time.Now().Equal(nextHeartbeat) {
				if err := writeSSEHeartbeat(res.Writer); err != nil {
					return nil
				}
				flusher.Flush()
				nextHeartbeat = time.Now().Add(sseHeartbeatInterval)
			}
			if !h.runUpdates.Healthy() || !errors.Is(waitErr, context.DeadlineExceeded) {
				runSubscription.Close()
				runSubscription = nil
				startPolling()
				observeWorker(h.observer, "runtime.sse.run_events_query", "degraded_reconcile", 1)
				page, err = h.svc.ListRunEventsPage(ctx, uid, runID, afterSequence, defaultRunEventsLimit)
				break
			}
		}
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
// jwt → 'web'（浏览器 / 仪表盘）；user_token → 'api'（User Token / SDK）；
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
		return "", httpx.Unauthorized("缺少 Agent Token")
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

type browserObservationStartRequest struct {
	Reason string `json:"reason"`
}

func (h *Handler) GetBrowserObservation(c echo.Context) error {
	_, runID, err := h.browserObservationIdentity(c)
	if err != nil {
		return err
	}
	state, err := h.browserObservation.State(c.Request().Context(), runID)
	if err != nil {
		return browserObservationHTTPError(err)
	}
	return c.JSON(http.StatusOK, state)
}

func (h *Handler) StartBrowserObservation(c echo.Context) error {
	return h.startBrowserObservation(c, false)
}

func (h *Handler) StartAdminBrowserObservation(c echo.Context) error {
	return h.startBrowserObservation(c, true)
}

func (h *Handler) startBrowserObservation(c echo.Context, isAdmin bool) error {
	userID, runID, err := h.browserObservationIdentity(c)
	if err != nil {
		return err
	}
	var request browserObservationStartRequest
	if isAdmin {
		if bindErr := c.Bind(&request); bindErr != nil {
			return httpx.BadRequest("请求体不是合法 JSON")
		}
		if strings.TrimSpace(request.Reason) == "" {
			return httpx.BadRequest("跨用户观察必须提供 reason")
		}
	}
	identity, err := h.browserObservation.ResolveIdentity(
		c.Request().Context(),
		runID,
		userID,
		isAdmin,
	)
	if err != nil {
		return browserObservationHTTPError(err)
	}
	state, err := h.browserObservation.Start(
		c.Request().Context(),
		runID,
		userID,
		isAdmin,
		strings.TrimSpace(request.Reason),
		identity,
	)
	if err != nil {
		return browserObservationHTTPError(err)
	}
	return c.JSON(http.StatusOK, state)
}

func (h *Handler) StopBrowserObservation(c echo.Context) error {
	_, runID, err := h.browserObservationIdentity(c)
	if err != nil {
		return err
	}
	if err := h.browserObservation.Stop(
		c.Request().Context(),
		runID,
		"observer_stopped",
	); err != nil {
		return browserObservationHTTPError(err)
	}
	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) browserObservationIdentity(
	c echo.Context,
) (uuid.UUID, uuid.UUID, error) {
	if h == nil || h.browserObservation == nil {
		return uuid.Nil, uuid.Nil, httpx.ServiceUnavailable("浏览器只读观察能力不可用")
	}
	userID, err := userIDFromCtx(c)
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	runID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return uuid.Nil, uuid.Nil, httpx.BadRequest("id 不是合法 uuid")
	}
	return userID, runID, nil
}

func browserObservationHTTPError(err error) error {
	switch {
	case errors.Is(err, ErrObservationChannelUnavailable):
		// The Worker is attached to another Core instance. Saying so plainly
		// keeps a multi-instance deployment from looking like a Runtime that
		// simply never sends frames.
		return httpx.ServiceUnavailable("该 Run 的观察通道不在当前 Core 实例上")
	case errors.Is(err, ErrObservationAlreadyActive):
		return httpx.Conflict("该 Run 已有活动的观察")
	case errors.Is(err, ErrObservationUnsupported):
		return echo.NewHTTPError(
			http.StatusNotImplemented,
			"该 Runtime 不支持只读观察",
		)
	case errors.Is(err, ErrObservationForbidden):
		return httpx.Forbidden("无权观察该 Run")
	}
	return httpx.Internal("观察请求失败")
}

func (h *Handler) GetBrowserObservationFrame(c echo.Context) error {
	_, runID, err := h.browserObservationIdentity(c)
	if err != nil {
		return err
	}
	after, err := strconv.ParseInt(c.QueryParam("after"), 10, 64)
	if err != nil || after < 0 {
		after = 0
	}
	frame, err := h.browserObservation.WaitFrame(c.Request().Context(), runID, after)
	if err != nil {
		return browserObservationHTTPError(err)
	}
	// Frames are live page content: never cached, never stored by an
	// intermediary, and never shared.
	c.Response().Header().Set("Cache-Control", "private, no-store")
	if frame == nil {
		return c.NoContent(http.StatusNoContent)
	}
	return c.JSON(http.StatusOK, frame)
}
