package agent

import (
	"context"
	"net/http"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

// RegistrationHandler Agent 自注册访问令牌 HTTP 入口。
type RegistrationHandler struct {
	svc       registrationService
	validator *validator.Validate
}

type registrationService interface {
	CreateAgentToken(context.Context, uuid.UUID, *CreateAgentTokenRequest) (*AgentTokenResponse, error)
	ListAgentTokens(context.Context, uuid.UUID, *uuid.UUID) ([]AgentTokenResponse, error)
	RevokeAgentToken(context.Context, uuid.UUID, uuid.UUID) error
	RegisterAgentViaToken(context.Context, *RegisterAgentViaTokenRequest) (*RegisterAgentViaTokenResponse, error)
}

func NewRegistrationHandler(svc registrationService) *RegistrationHandler {
	return &RegistrationHandler{
		svc:       svc,
		validator: validator.New(validator.WithRequiredStructEnabled()),
	}
}

// RegisterProtected 创作者侧（需 JWT）。
//
//	POST   /api/v1/creator/agent-tokens
//	GET    /api/v1/creator/agent-tokens
//	DELETE /api/v1/creator/agent-tokens/:id
func (h *RegistrationHandler) RegisterProtected(api *echo.Group, jwtMiddleware echo.MiddlewareFunc) {
	g := api.Group("/creator/agent-tokens", jwtMiddleware)
	g.POST("", h.CreateAgentToken)
	g.GET("", h.ListAgentTokens)
	g.DELETE("/:id", h.RevokeAgentToken)
}

// RegisterPublic Agent 侧（无 JWT，凭 agent token）。
//
//	POST /api/v1/agent-registration/agents
//	GET  /skill/publish-agent  -> 静态接入说明（HTML/Markdown）
func (h *RegistrationHandler) RegisterPublic(api *echo.Group) {
	api.POST("/agent-registration/agents", h.RegisterAgentViaToken)
}

// CreateAgentToken POST /api/v1/creator/agent-tokens
func (h *RegistrationHandler) CreateAgentToken(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	var req CreateAgentTokenRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.CreateAgentToken(c.Request().Context(), uid, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusCreated, resp)
}

// ListAgentTokens GET /api/v1/creator/agent-tokens
func (h *RegistrationHandler) ListAgentTokens(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	var agentID *uuid.UUID
	if raw := strings.TrimSpace(c.QueryParam("agent_id")); raw != "" {
		parsed, err := uuid.Parse(raw)
		if err != nil {
			return httpx.BadRequest("agent_id 不是合法 uuid")
		}
		agentID = &parsed
	}
	items, err := h.svc.ListAgentTokens(c.Request().Context(), uid, agentID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]any{"items": items})
}

// RevokeAgentToken DELETE /api/v1/creator/agent-tokens/:id
func (h *RegistrationHandler) RevokeAgentToken(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	tokenID, err := pathID(c)
	if err != nil {
		return err
	}
	if err := h.svc.RevokeAgentToken(c.Request().Context(), uid, tokenID); err != nil {
		return err
	}
	return c.NoContent(http.StatusNoContent)
}

// RegisterAgentViaToken POST /api/v1/agent-registration/agents
func (h *RegistrationHandler) RegisterAgentViaToken(c echo.Context) error {
	var req RegisterAgentViaTokenRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if req.AgentToken == "" {
		parts := strings.SplitN(c.Request().Header.Get(echo.HeaderAuthorization), " ", 2)
		if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
			req.AgentToken = strings.TrimSpace(parts[1])
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
	resp, err := h.svc.RegisterAgentViaToken(c.Request().Context(), &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusCreated, resp)
}
