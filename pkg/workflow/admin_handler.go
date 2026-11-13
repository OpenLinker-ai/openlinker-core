package workflow

import (
	"context"
	"net/http"
	"strconv"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

type workflowAdminService interface {
	GetWorkflowAdmission(context.Context) (*WorkflowAdmissionResponse, error)
	SetWorkflowAdmission(context.Context, uuid.UUID, *SetWorkflowAdmissionRequest) (*WorkflowAdmissionResponse, error)
	ListAdminWorkflowRuns(context.Context, string, int32) (*AdminWorkflowRunListResponse, error)
	AdminCancelWorkflowRun(context.Context, uuid.UUID, uuid.UUID) (*AdminWorkflowCancellationResponse, error)
}

// AdminHandler exposes the drain controls used before a Core release.
type AdminHandler struct {
	svc       workflowAdminService
	validator *validator.Validate
}

func NewAdminHandler(svc workflowAdminService) *AdminHandler {
	return &AdminHandler{svc: svc, validator: validator.New(validator.WithRequiredStructEnabled())}
}

// Register mounts the platform-administrator workflow drain APIs.
//
//	GET  /admin/workflow-admission                 admission state and drain progress
//	POST /admin/workflow-admission                 switch to open or draining
//	GET  /admin/workflow-runs?state=nonterminal|cancelling
//	POST /admin/workflow-runs/:id/cancel           cancel any user's run
func (h *AdminHandler) Register(api *echo.Group, jwtMiddleware, adminMiddleware echo.MiddlewareFunc) {
	g := api.Group("/admin", jwtMiddleware, adminMiddleware)
	g.GET("/workflow-admission", h.GetAdmission)
	g.POST("/workflow-admission", h.SetAdmission)
	g.GET("/workflow-runs", h.ListRuns)
	g.POST("/workflow-runs/:id/cancel", h.CancelRun)
}

func (h *AdminHandler) GetAdmission(c echo.Context) error {
	resp, err := h.svc.GetWorkflowAdmission(c.Request().Context())
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *AdminHandler) SetAdmission(c echo.Context) error {
	adminID, err := adminUserID(c)
	if err != nil {
		return err
	}
	var req SetWorkflowAdmissionRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.BadRequest("state 只能是 open 或 draining")
	}
	resp, err := h.svc.SetWorkflowAdmission(c.Request().Context(), adminID, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *AdminHandler) ListRuns(c echo.Context) error {
	var limit int32
	if raw := c.QueryParam("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			return httpx.BadRequest("limit 无效")
		}
		limit = int32(min(parsed, adminWorkflowRunListMaxLimit))
	}
	resp, err := h.svc.ListAdminWorkflowRuns(c.Request().Context(), c.QueryParam("state"), limit)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *AdminHandler) CancelRun(c echo.Context) error {
	adminID, err := adminUserID(c)
	if err != nil {
		return err
	}
	runID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return httpx.BadRequest("workflow_run id 无效")
	}
	resp, err := h.svc.AdminCancelWorkflowRun(c.Request().Context(), adminID, runID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func adminUserID(c echo.Context) (uuid.UUID, error) {
	id, err := uuid.Parse(httpx.UserIDFrom(c))
	if err != nil {
		return uuid.Nil, httpx.Unauthorized("token 无效")
	}
	return id, nil
}
