package agent

import (
	"context"
	"net/http"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

// RegistrationHandler Agent 自注册访问令牌 HTTP 入口。
type RegistrationHandler struct {
	svc       registrationService
	validator *validator.Validate
}

type registrationService interface {
	MintBootstrapToken(context.Context, uuid.UUID, *CreateBootstrapTokenRequest) (*BootstrapTokenResponse, error)
	ListBootstrapTokens(context.Context, uuid.UUID) ([]BootstrapTokenResponse, error)
	RevokeBootstrapToken(context.Context, uuid.UUID, uuid.UUID) error
	RegisterAgentViaBootstrap(context.Context, *RegisterAgentViaBootstrapRequest) (*RegisterAgentViaBootstrapResponse, error)
}

func NewRegistrationHandler(svc registrationService) *RegistrationHandler {
	return &RegistrationHandler{
		svc:       svc,
		validator: validator.New(validator.WithRequiredStructEnabled()),
	}
}

// RegisterProtected 创作者侧（需 JWT）。
//
//	POST   /api/v1/creator/agent-registration-tokens
//	GET    /api/v1/creator/agent-registration-tokens
//	DELETE /api/v1/creator/agent-registration-tokens/:id
func (h *RegistrationHandler) RegisterProtected(api *echo.Group, jwtMiddleware echo.MiddlewareFunc) {
	g := api.Group("/creator/agent-registration-tokens", jwtMiddleware)
	g.POST("", h.MintBootstrapToken)
	g.GET("", h.ListBootstrapTokens)
	g.DELETE("/:id", h.RevokeBootstrapToken)
}

// RegisterPublic Agent 侧（无 JWT，凭 bootstrap token）。
//
//	POST /api/v1/agent-registration/agents
//	GET  /skill/publish-agent  -> 静态接入说明（HTML/Markdown）
func (h *RegistrationHandler) RegisterPublic(api *echo.Group) {
	api.POST("/agent-registration/agents", h.RegisterAgentViaBootstrap)
}

// MintBootstrapToken POST /api/v1/creator/agent-registration-tokens
func (h *RegistrationHandler) MintBootstrapToken(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	var req CreateBootstrapTokenRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.MintBootstrapToken(c.Request().Context(), uid, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusCreated, resp)
}

// ListBootstrapTokens GET /api/v1/creator/agent-registration-tokens
func (h *RegistrationHandler) ListBootstrapTokens(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	items, err := h.svc.ListBootstrapTokens(c.Request().Context(), uid)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]any{"items": items})
}

// RevokeBootstrapToken DELETE /api/v1/creator/agent-registration-tokens/:id
func (h *RegistrationHandler) RevokeBootstrapToken(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	tokenID, err := pathID(c)
	if err != nil {
		return err
	}
	if err := h.svc.RevokeBootstrapToken(c.Request().Context(), uid, tokenID); err != nil {
		return err
	}
	return c.NoContent(http.StatusNoContent)
}

// RegisterAgentViaBootstrap POST /api/v1/agent-registration/agents
func (h *RegistrationHandler) RegisterAgentViaBootstrap(c echo.Context) error {
	var req RegisterAgentViaBootstrapRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if req.BootstrapToken == "" {
		parts := strings.SplitN(c.Request().Header.Get(echo.HeaderAuthorization), " ", 2)
		if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
			req.BootstrapToken = strings.TrimSpace(parts[1])
		}
	}
	if len(req.Tags) == 0 {
		req.Tags = req.AbilityTags
	}
	if req.Visibility == "" {
		req.Visibility = "public"
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.RegisterAgentViaBootstrap(c.Request().Context(), &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusCreated, resp)
}
