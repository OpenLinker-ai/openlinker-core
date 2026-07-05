package userdash

import (
	"context"
	"net/http"
	"strconv"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

// Handler serves user dashboard and run history endpoints.
// All routes require a valid JWT.
type Handler struct {
	svc dashboardService
}

type dashboardService interface {
	ListUserRuns(context.Context, uuid.UUID, int32, int32) (*RunListResponse, error)
	ListCreatorAgentRuns(context.Context, uuid.UUID, uuid.UUID, int32, int32) (*RunListResponse, error)
	GetUserDashboard(context.Context, uuid.UUID) (*UserDashboardResponse, error)
	GetCreatorDashboard(context.Context, uuid.UUID) (*CreatorDashboardResponse, error)
}

func NewHandler(svc dashboardService) *Handler {
	return &Handler{svc: svc}
}

func (h *Handler) RegisterCoreAPI(api *echo.Group, jwtMiddleware echo.MiddlewareFunc) {
	h.registerRoutes(api.Group("", jwtMiddleware))
}

func (h *Handler) registerRoutes(g *echo.Group) {
	g.GET("/runs", h.ListRuns)
	g.GET("/dashboard", h.GetDashboard)
	g.GET("/creator/dashboard", h.GetCreatorDashboard)
	g.GET("/creator/agents/:id/runs", h.ListCreatorAgentRuns)
}

func (h *Handler) ListRuns(c echo.Context) error {
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

func (h *Handler) ListCreatorAgentRuns(c echo.Context) error {
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
