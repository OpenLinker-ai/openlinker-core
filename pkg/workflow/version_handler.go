package workflow

import (
	"context"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/auth"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

// workflowVersionService is the versioned-definition surface. It is separate
// from workflowService so services that predate versions still satisfy the
// handler.
type workflowVersionService interface {
	UpdateWorkflow(ctx context.Context, userID, workflowID uuid.UUID, expectedVersion int32, req *UpdateWorkflowRequest) (*WorkflowResponse, error)
	DuplicateWorkflow(ctx context.Context, userID, workflowID uuid.UUID, req *DuplicateWorkflowRequest) (*WorkflowResponse, error)
	SetWorkflowArchived(ctx context.Context, userID, workflowID uuid.UUID, archived bool) (*WorkflowResponse, error)
	ListWorkflowVersions(ctx context.Context, userID, workflowID uuid.UUID) (*WorkflowVersionListResponse, error)
	GetWorkflowVersion(ctx context.Context, userID, workflowID uuid.UUID, version int32) (*WorkflowResponse, error)
}

func (h *Handler) versions() (workflowVersionService, error) {
	svc, ok := h.svc.(workflowVersionService)
	if !ok {
		return nil, httpx.NewError(http.StatusNotImplemented, "NOT_IMPLEMENTED", "workflow versions are not available")
	}
	return svc, nil
}

func (h *Handler) registerVersionRoutes(g *echo.Group) {
	g.PUT("/:id", h.Update)
	g.GET("/:id/versions", h.ListVersions)
	g.GET("/:id/versions/:version", h.GetVersion)
	g.POST("/:id/duplicate", h.Duplicate)
	g.POST("/:id/archive", h.Archive)
	g.POST("/:id/unarchive", h.Unarchive)
}

// parseIfMatchVersion accepts `If-Match: 3` or `If-Match: "3"`.
func parseIfMatchVersion(raw string) int32 {
	raw = strings.Trim(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(raw), "W/")), `"`)
	n, err := strconv.ParseInt(raw, 10, 32)
	if err != nil || n < 1 {
		return 0
	}
	return int32(n)
}

func (h *Handler) Update(c echo.Context) error {
	svc, err := h.versions()
	if err != nil {
		return err
	}
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathUUID(c)
	if err != nil {
		return err
	}
	if err := auth.RequirePermission(c, "workflows:manage", "workflow", &id); err != nil {
		return err
	}
	var req UpdateWorkflowRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := svc.UpdateWorkflow(c.Request().Context(), uid, id, parseIfMatchVersion(c.Request().Header.Get("If-Match")), &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) Duplicate(c echo.Context) error {
	svc, err := h.versions()
	if err != nil {
		return err
	}
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathUUID(c)
	if err != nil {
		return err
	}
	if err := auth.RequirePermission(c, "workflows:manage", "workflow", &id); err != nil {
		return err
	}
	var req DuplicateWorkflowRequest
	if c.Request().ContentLength != 0 {
		if err := c.Bind(&req); err != nil {
			return httpx.BadRequest("请求体格式错误")
		}
		if err := h.validator.Struct(&req); err != nil {
			return httpx.Unprocessable(err.Error())
		}
	}
	resp, err := svc.DuplicateWorkflow(c.Request().Context(), uid, id, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusCreated, resp)
}

func (h *Handler) Archive(c echo.Context) error   { return h.setArchived(c, true) }
func (h *Handler) Unarchive(c echo.Context) error { return h.setArchived(c, false) }

func (h *Handler) setArchived(c echo.Context, archived bool) error {
	svc, err := h.versions()
	if err != nil {
		return err
	}
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathUUID(c)
	if err != nil {
		return err
	}
	if err := auth.RequirePermission(c, "workflows:manage", "workflow", &id); err != nil {
		return err
	}
	resp, err := svc.SetWorkflowArchived(c.Request().Context(), uid, id, archived)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) ListVersions(c echo.Context) error {
	svc, err := h.versions()
	if err != nil {
		return err
	}
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
	resp, err := svc.ListWorkflowVersions(c.Request().Context(), uid, id)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) GetVersion(c echo.Context) error {
	svc, err := h.versions()
	if err != nil {
		return err
	}
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
	version, convErr := strconv.ParseInt(c.Param("version"), 10, 32)
	if convErr != nil || version < 1 {
		return httpx.BadRequest("version 无效")
	}
	resp, err := svc.GetWorkflowVersion(c.Request().Context(), uid, id, int32(version))
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}
