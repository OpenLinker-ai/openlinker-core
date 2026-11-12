package workflow

import (
	"context"
	"net/http"
	"strconv"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/auth"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

type Handler struct {
	svc       workflowService
	validator *validator.Validate
}

type workflowService interface {
	CreateWorkflow(context.Context, uuid.UUID, *CreateWorkflowRequest) (*WorkflowResponse, error)
	ListWorkflows(context.Context, uuid.UUID, int32) (*WorkflowListResponse, error)
	ListWorkflowsPage(context.Context, uuid.UUID, string, string, string, int32, int32) (*WorkflowListResponse, error)
	GetWorkflow(context.Context, uuid.UUID, uuid.UUID) (*WorkflowResponse, error)
	RunWorkflow(context.Context, uuid.UUID, uuid.UUID, *RunWorkflowRequest) (*WorkflowRunResponse, error)
	StartWorkflowRun(context.Context, uuid.UUID, uuid.UUID, *RunWorkflowRequest) (*WorkflowRunResponse, error)
	ListWorkflowRuns(context.Context, uuid.UUID, uuid.UUID, int32) (*WorkflowRunListResponse, error)
	ListWorkflowRunsPage(context.Context, uuid.UUID, uuid.UUID, string, string, string, int32, int32) (*WorkflowRunListResponse, error)
	GetWorkflowRun(context.Context, uuid.UUID, uuid.UUID) (*WorkflowRunResponse, error)
	RetryWorkflowRun(context.Context, uuid.UUID, uuid.UUID, *RetryWorkflowRunRequest) (*WorkflowRunResponse, error)
	RerunWorkflowStep(context.Context, uuid.UUID, uuid.UUID, *RerunWorkflowStepRequest) (*WorkflowStepRerunResponse, error)
	CompareWorkflowRuns(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) (*WorkflowRunComparisonResponse, error)
	PauseWorkflowRun(context.Context, uuid.UUID, uuid.UUID) (*WorkflowRunResponse, error)
	ResumeWorkflowRun(context.Context, uuid.UUID, uuid.UUID) (*WorkflowRunResponse, error)
	CancelWorkflowRun(context.Context, uuid.UUID, uuid.UUID) (*WorkflowRunResponse, error)
}

func NewHandler(svc workflowService) *Handler {
	return &Handler{
		svc:       svc,
		validator: validator.New(validator.WithRequiredStructEnabled()),
	}
}

// RegisterProtected mounts workflow APIs behind user auth.
//
//	POST /workflows              创建 workflow
//	GET  /workflows              查询自己的 workflow 列表
//	GET  /workflows/:id          查询 workflow 定义
//	POST /workflows/:id/run      同步执行 workflow
//	POST /workflows/:id/runs     异步启动 workflow run
//	GET  /workflows/:id/runs     查询 workflow run 历史
//	GET  /workflow-runs/:id      查询 workflow run
//	POST /workflow-runs/:id/retry 复制失败 run 输入并重新入队
//	POST /workflow-runs/:id/steps/rerun 基于既有 run 重跑某个 step 及其下游
//	GET  /workflow-runs/:id/compare/:other_id 对比两个 workflow run
//	POST /workflow-runs/:id/pause 暂停 pending/running run
//	POST /workflow-runs/:id/resume 恢复 paused run
//	POST /workflow-runs/:id/cancel 取消 pending/running/paused run
func (h *Handler) RegisterProtected(api *echo.Group, jwtMiddleware echo.MiddlewareFunc) {
	g := api.Group("/workflows", jwtMiddleware)
	g.POST("", h.Create)
	g.GET("", h.List)
	g.GET("/:id", h.Get)
	g.POST("/:id/run", h.Run)
	g.POST("/:id/runs", h.StartRun)
	g.GET("/:id/runs", h.ListRuns)
	h.registerVersionRoutes(g)
	api.GET("/workflow-runs/:id", h.GetRun, jwtMiddleware)
	api.POST("/workflow-runs/:id/retry", h.RetryRun, jwtMiddleware)
	api.POST("/workflow-runs/:id/steps/rerun", h.RerunStep, jwtMiddleware)
	api.GET("/workflow-runs/:id/compare/:other_id", h.CompareRuns, jwtMiddleware)
	api.POST("/workflow-runs/:id/pause", h.PauseRun, jwtMiddleware)
	api.POST("/workflow-runs/:id/resume", h.ResumeRun, jwtMiddleware)
	api.POST("/workflow-runs/:id/cancel", h.CancelRun, jwtMiddleware)
}

func (h *Handler) Create(c echo.Context) error {
	if err := auth.RequirePermission(c, "workflows:manage", "workflow", nil); err != nil {
		return err
	}
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	var req CreateWorkflowRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.CreateWorkflow(c.Request().Context(), uid, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusCreated, resp)
}

func (h *Handler) List(c echo.Context) error {
	if err := auth.RequirePermission(c, "workflows:read", "workflow", nil); err != nil {
		return err
	}
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	page := int32(1)
	if v := c.QueryParam("page"); v != "" {
		if n, perr := strconv.ParseInt(v, 10, 32); perr == nil && n > 0 {
			page = int32(n) // #nosec G115 -- ParseInt bitSize=32 guarantees range.
		}
	}
	size := int32(20)
	if v := firstNonEmpty(c.QueryParam("size"), c.QueryParam("limit")); v != "" {
		if n, perr := strconv.ParseInt(v, 10, 32); perr == nil && n > 0 {
			if n > 50 {
				n = 50
			}
			size = int32(n) // #nosec G115 -- ParseInt bitSize=32 guarantees range, then size is capped.
		}
	}
	resp, err := h.svc.ListWorkflowsPage(
		c.Request().Context(),
		uid,
		c.QueryParam("q"),
		c.QueryParam("status"),
		c.QueryParam("sort"),
		page,
		size,
	)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func (h *Handler) Get(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathUUID(c)
	if err != nil {
		return err
	}
	if err := auth.RequirePermission(c, "workflows:read", "workflow", &id); err != nil {
		return err
	}
	resp, err := h.svc.GetWorkflow(c.Request().Context(), uid, id)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) Run(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathUUID(c)
	if err != nil {
		return err
	}
	if err := auth.RequirePermission(c, "workflows:run", "workflow", &id); err != nil {
		return err
	}
	var req RunWorkflowRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.RunWorkflow(c.Request().Context(), uid, id, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) StartRun(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathUUID(c)
	if err != nil {
		return err
	}
	if err := auth.RequirePermission(c, "workflows:run", "workflow", &id); err != nil {
		return err
	}
	var req RunWorkflowRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.StartWorkflowRun(c.Request().Context(), uid, id, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusAccepted, resp)
}

func (h *Handler) ListRuns(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathUUID(c)
	if err != nil {
		return err
	}
	if err := auth.RequirePermission(c, "workflows:read", "workflow", &id); err != nil {
		return err
	}
	page := int32(1)
	if v := c.QueryParam("page"); v != "" {
		if n, perr := strconv.ParseInt(v, 10, 32); perr == nil && n > 0 {
			page = int32(n) // #nosec G115 -- ParseInt bitSize=32 guarantees range.
		}
	}
	size := int32(20)
	if v := firstNonEmpty(c.QueryParam("size"), c.QueryParam("limit")); v != "" {
		if n, perr := strconv.ParseInt(v, 10, 32); perr == nil && n > 0 {
			if n > 50 {
				n = 50
			}
			size = int32(n) // #nosec G115 -- ParseInt bitSize=32 guarantees range, then size is capped.
		}
	}
	resp, err := h.svc.ListWorkflowRunsPage(
		c.Request().Context(),
		uid,
		id,
		c.QueryParam("q"),
		c.QueryParam("status"),
		c.QueryParam("sort"),
		page,
		size,
	)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) GetRun(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathUUID(c)
	if err != nil {
		return err
	}
	resp, err := h.requireWorkflowRunPermission(c, uid, id, "workflows:read")
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) RetryRun(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathUUID(c)
	if err != nil {
		return err
	}
	var req RetryWorkflowRunRequest
	if c.Request().ContentLength != 0 {
		if err := c.Bind(&req); err != nil {
			return httpx.BadRequest("请求体格式错误")
		}
	}
	if err := h.requireWorkflowRunActionPermission(c, uid, id, "workflows:run"); err != nil {
		return err
	}
	resp, err := h.svc.RetryWorkflowRun(c.Request().Context(), uid, id, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusAccepted, resp)
}

func (h *Handler) RerunStep(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathUUID(c)
	if err != nil {
		return err
	}
	var req RerunWorkflowStepRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	if err := h.requireWorkflowRunActionPermission(c, uid, id, "workflows:run"); err != nil {
		return err
	}
	resp, err := h.svc.RerunWorkflowStep(c.Request().Context(), uid, id, &req)
	if err != nil {
		return err
	}
	// The rerun run is queued and executed by the worker.
	return c.JSON(http.StatusAccepted, resp)
}

func (h *Handler) CompareRuns(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathUUID(c)
	if err != nil {
		return err
	}
	otherID, err := uuid.Parse(c.Param("other_id"))
	if err != nil {
		return httpx.BadRequest("other_id 不是合法 uuid")
	}
	if err := h.requireWorkflowRunActionPermission(c, uid, id, "workflows:read"); err != nil {
		return err
	}
	if err := h.requireWorkflowRunActionPermission(c, uid, otherID, "workflows:read"); err != nil {
		return err
	}
	resp, err := h.svc.CompareWorkflowRuns(c.Request().Context(), uid, id, otherID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) PauseRun(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathUUID(c)
	if err != nil {
		return err
	}
	if err := h.requireWorkflowRunActionPermission(c, uid, id, "workflows:run"); err != nil {
		return err
	}
	resp, err := h.svc.PauseWorkflowRun(c.Request().Context(), uid, id)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) ResumeRun(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathUUID(c)
	if err != nil {
		return err
	}
	if err := h.requireWorkflowRunActionPermission(c, uid, id, "workflows:run"); err != nil {
		return err
	}
	resp, err := h.svc.ResumeWorkflowRun(c.Request().Context(), uid, id)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusAccepted, resp)
}

func (h *Handler) CancelRun(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathUUID(c)
	if err != nil {
		return err
	}
	if err := h.requireWorkflowRunActionPermission(c, uid, id, "workflows:run"); err != nil {
		return err
	}
	resp, err := h.svc.CancelWorkflowRun(c.Request().Context(), uid, id)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) requireWorkflowRunPermission(c echo.Context, userID, runID uuid.UUID, permission string) (*WorkflowRunResponse, error) {
	run, err := h.svc.GetWorkflowRun(c.Request().Context(), userID, runID)
	if err != nil {
		return nil, err
	}
	if run == nil {
		return nil, httpx.NotFound("Workflow Run 不存在")
	}
	workflowID, err := uuid.Parse(run.WorkflowID)
	if err != nil {
		return nil, httpx.Internal("Workflow Run 资源标识无效")
	}
	if err := auth.RequirePermission(c, permission, "workflow", &workflowID); err != nil {
		return nil, err
	}
	return run, nil
}

func (h *Handler) requireWorkflowRunActionPermission(c echo.Context, userID, runID uuid.UUID, permission string) error {
	principal := auth.PrincipalFrom(c)
	if principal == nil {
		return auth.RequirePermission(c, permission, "workflow", nil)
	}
	if principal.Allows(permission, "workflow", nil) {
		return nil
	}
	if !principal.HasPermission(permission, "workflow") {
		return auth.RequirePermission(c, permission, "workflow", nil)
	}
	_, err := h.requireWorkflowRunPermission(c, userID, runID, permission)
	return err
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

func pathUUID(c echo.Context) (uuid.UUID, error) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return uuid.Nil, httpx.BadRequest("id 不是合法 uuid")
	}
	return id, nil
}
