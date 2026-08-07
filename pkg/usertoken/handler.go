package usertoken

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

type tokenService interface {
	Create(context.Context, uuid.UUID, *CreateRequest) (*TokenResponse, error)
	List(context.Context, uuid.UUID, ListOptions) (*ListResponse, error)
	Get(context.Context, uuid.UUID, uuid.UUID) (*TokenResponse, error)
	Update(context.Context, uuid.UUID, uuid.UUID, *UpdateRequest) (*TokenResponse, error)
	Revoke(context.Context, uuid.UUID, uuid.UUID) error
}

type Handler struct {
	svc tokenService
}

func NewHandler(svc tokenService) *Handler { return &Handler{svc: svc} }

// Register deliberately uses the JWT middleware, never HybridAuthMiddleware.
// Handler methods repeat the method check as defense in depth.
func (h *Handler) Register(api *echo.Group, jwtMiddleware echo.MiddlewareFunc) {
	g := api.Group("/user-tokens", jwtMiddleware)
	g.POST("", h.Create)
	g.GET("", h.List)
	g.GET("/:id", h.Get)
	g.PATCH("/:id", h.Update)
	g.DELETE("/:id", h.Revoke)
}

func (h *Handler) Create(c echo.Context) error {
	userID, err := jwtUserID(c)
	if err != nil {
		return err
	}
	var req CreateRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	resp, err := h.svc.Create(c.Request().Context(), userID, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusCreated, resp)
}

func (h *Handler) List(c echo.Context) error {
	userID, err := jwtUserID(c)
	if err != nil {
		return err
	}
	opts, err := listOptions(c)
	if err != nil {
		return err
	}
	resp, err := h.svc.List(c.Request().Context(), userID, opts)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) Get(c echo.Context) error {
	userID, err := jwtUserID(c)
	if err != nil {
		return err
	}
	tokenID, err := pathTokenID(c)
	if err != nil {
		return err
	}
	resp, err := h.svc.Get(c.Request().Context(), userID, tokenID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) Update(c echo.Context) error {
	userID, err := jwtUserID(c)
	if err != nil {
		return err
	}
	tokenID, err := pathTokenID(c)
	if err != nil {
		return err
	}
	var req UpdateRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	resp, err := h.svc.Update(c.Request().Context(), userID, tokenID, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) Revoke(c echo.Context) error {
	userID, err := jwtUserID(c)
	if err != nil {
		return err
	}
	tokenID, err := pathTokenID(c)
	if err != nil {
		return err
	}
	if err := h.svc.Revoke(c.Request().Context(), userID, tokenID); err != nil {
		return err
	}
	return c.NoContent(http.StatusNoContent)
}

func jwtUserID(c echo.Context) (uuid.UUID, error) {
	if httpx.AuthMethodFrom(c) != auth.AuthMethodJWT {
		return uuid.Nil, httpx.PermissionDenied("jwt_session")
	}
	userID, err := uuid.Parse(httpx.UserIDFrom(c))
	if err != nil {
		return uuid.Nil, httpx.Unauthorized("token 无效")
	}
	return userID, nil
}

func pathTokenID(c echo.Context) (uuid.UUID, error) {
	tokenID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return uuid.Nil, httpx.BadRequest("id 不是合法 uuid")
	}
	return tokenID, nil
}

func listOptions(c echo.Context) (ListOptions, error) {
	limit, err := parseInt32(c.QueryParam("limit"), 10, 0, 50, "limit")
	if err != nil {
		return ListOptions{}, err
	}
	offset, err := parseInt32(c.QueryParam("offset"), 0, 0, 100000, "offset")
	if err != nil {
		return ListOptions{}, err
	}
	status, err := tokenListStatus(c.QueryParam("status"))
	if err != nil {
		return ListOptions{}, err
	}
	query := strings.TrimSpace(c.QueryParam("q"))
	if len([]rune(query)) > 120 {
		return ListOptions{}, httpx.BadRequest("q 最多 120 个字符")
	}
	return ListOptions{
		Limit: limit, Offset: offset,
		SortBy:  strings.TrimSpace(c.QueryParam("sort_by")),
		SortDir: strings.ToLower(strings.TrimSpace(c.QueryParam("sort_dir"))),
		Status:  status,
		Query:   query,
	}, nil
}

func tokenListStatus(raw string) (string, error) {
	status := strings.ToLower(strings.TrimSpace(raw))
	if status == "" {
		return "all", nil
	}
	switch status {
	case "active", "revoked", "expired", "all":
		return status, nil
	default:
		return "", httpx.BadRequest("status 必须是 active、revoked、expired 或 all")
	}
}

func parseInt32(raw string, fallback, min, max int32, field string) (int32, error) {
	if strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	value, err := strconv.ParseInt(raw, 10, 32)
	if err != nil || value < int64(min) {
		return 0, httpx.BadRequest(field + " 不是合法整数")
	}
	if value > int64(max) {
		value = int64(max)
	}
	return int32(value), nil
}
