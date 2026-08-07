package userdash

import (
	"context"
	"net/http"
	"strconv"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/auth"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

// Handler serves user dashboard and run history endpoints.
// All routes require a valid JWT.
type Handler struct {
	svc dashboardService
}

type dashboardService interface {
	ListUserRuns(context.Context, uuid.UUID, int32, int32) (*RunListResponse, error)
	ListCallRecords(context.Context, uuid.UUID, string, string, string, string, string, string, int32, int32) (*CallRecordListResponse, error)
	ListCreatorAgentRuns(context.Context, uuid.UUID, uuid.UUID, int32, int32) (*RunListResponse, error)
	GetUserDashboard(context.Context, uuid.UUID) (*UserDashboardResponse, error)
	GetCreatorDashboard(context.Context, uuid.UUID) (*CreatorDashboardResponse, error)
}

func NewHandler(svc dashboardService) *Handler {
	return &Handler{svc: svc}
}

func (h *Handler) RegisterCoreAPI(api *echo.Group, jwtMiddleware echo.MiddlewareFunc) {
	// Do not create an authenticated Group with an empty prefix here. Echo
	// implements group middleware by registering prefix-level 404 catch-alls;
	// an empty prefix would authenticate every unknown /api/v1 path before the
	// router can return its stable NOT_FOUND response.
	api.GET("/runs", h.ListRuns, jwtMiddleware)
	api.GET("/call-records", h.ListCallRecords, jwtMiddleware)
	api.GET("/dashboard", h.GetDashboard, jwtMiddleware)
	api.GET("/creator/dashboard", h.GetCreatorDashboard, jwtMiddleware)
	api.GET("/creator/agents/:id/runs", h.ListCreatorAgentRuns, jwtMiddleware)
}

func (h *Handler) ListRuns(c echo.Context) error {
	if err := auth.RequirePermission(c, "runs:read", "run", nil); err != nil {
		return err
	}
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	page := parseInt32Query(c, "page", defaultPage)
	size := parseInt32Query(c, "size", defaultSize)
	resp, err := h.svc.ListUserRuns(c.Request().Context(), uid, page, size)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) ListCallRecords(c echo.Context) error {
	if err := auth.RequirePermission(c, "runs:read", "run", nil); err != nil {
		return err
	}
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	page := parseInt32Query(c, "page", defaultPage)
	size := parseInt32Query(c, "size", defaultSize)
	resp, err := h.svc.ListCallRecords(
		c.Request().Context(),
		uid,
		c.QueryParam("view"),
		c.QueryParam("q"),
		c.QueryParam("sort"),
		c.QueryParam("status"),
		c.QueryParam("source"),
		c.QueryParam("relation"),
		page,
		size,
	)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) ListCreatorAgentRuns(c echo.Context) error {
	if err := auth.RequirePermission(c, "agents:read", "agent", nil); err != nil {
		return err
	}
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	agentID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	page := parseInt32Query(c, "page", defaultPage)
	size := parseInt32Query(c, "size", defaultSize)
	resp, err := h.svc.ListCreatorAgentRuns(c.Request().Context(), uid, agentID, page, size)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) GetDashboard(c echo.Context) error {
	if err := auth.RequirePermission(c, "runs:read", "run", nil); err != nil {
		return err
	}
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	resp, err := h.svc.GetUserDashboard(c.Request().Context(), uid)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) GetCreatorDashboard(c echo.Context) error {
	if err := auth.RequirePermission(c, "agents:read", "agent", nil); err != nil {
		return err
	}
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	resp, err := h.svc.GetCreatorDashboard(c.Request().Context(), uid)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
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

func parseInt32Query(c echo.Context, key string, fallback int32) int32 {
	raw := c.QueryParam(key)
	if raw == "" {
		return fallback
	}
	n, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return fallback
	}
	// #nosec G115 -- strconv.ParseInt with bitSize=32 guarantees int32 range.
	return int32(n)
}
