package admin

import (
	"net/http"
	"strconv"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

type Handler struct {
	svc *Service
}

func NewHandler(svc *Service) *Handler {
	return &Handler{svc: svc}
}

func (h *Handler) Register(api *echo.Group, jwtMiddleware, adminMiddleware echo.MiddlewareFunc) {
	g := api.Group("/admin", jwtMiddleware, adminMiddleware)
	g.GET("/summary", h.Summary)
	g.GET("/users", h.ListUsers)
	g.PATCH("/users/:id/flags", h.UpdateUserFlags)
	g.GET("/agents", h.ListAgents)
	g.PATCH("/agents/:id/moderation", h.UpdateAgentModeration)
}

func (h *Handler) Summary(c echo.Context) error {
	resp, err := h.svc.Summary(c.Request().Context())
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) ListUsers(c echo.Context) error {
	resp, err := h.svc.ListUsers(
		c.Request().Context(),
		c.QueryParam("q"),
		c.QueryParam("role"),
		queryInt(c, "limit", defaultLimit),
		queryInt(c, "offset", 0),
	)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) UpdateUserFlags(c echo.Context) error {
	actorID, err := currentUserID(c)
	if err != nil {
		return err
	}
	targetID, err := pathID(c)
	if err != nil {
		return err
	}
	var req UpdateUserFlagsRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	resp, err := h.svc.UpdateUserFlags(c.Request().Context(), actorID, targetID, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) ListAgents(c echo.Context) error {
	resp, err := h.svc.ListAgents(
		c.Request().Context(),
		c.QueryParam("q"),
		c.QueryParam("lifecycle_status"),
		c.QueryParam("visibility"),
		c.QueryParam("certification_status"),
		queryInt(c, "limit", defaultLimit),
		queryInt(c, "offset", 0),
	)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) UpdateAgentModeration(c echo.Context) error {
	id, err := pathID(c)
	if err != nil {
		return err
	}
	var req UpdateAgentModerationRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	resp, err := h.svc.UpdateAgentModeration(c.Request().Context(), id, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func currentUserID(c echo.Context) (uuid.UUID, error) {
	raw := httpx.UserIDFrom(c)
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, httpx.Unauthorized("token 无效")
	}
	return id, nil
}

func pathID(c echo.Context) (uuid.UUID, error) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return uuid.Nil, httpx.BadRequest("id 不是合法 uuid")
	}
	return id, nil
}

func queryInt(c echo.Context, key string, fallback int32) int32 {
	raw := c.QueryParam(key)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return int32(value)
}
