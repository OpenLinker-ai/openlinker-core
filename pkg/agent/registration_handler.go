package agent

import (
	"context"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/auth"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

// RegistrationHandler Agent 自注册访问令牌 HTTP 入口。
type RegistrationHandler struct {
	svc       registrationService
	validator *validator.Validate
}

type registrationService interface {
	CreateAgentToken(context.Context, uuid.UUID, *CreateAgentTokenRequest) (*AgentTokenResponse, error)
	ListAgentTokens(context.Context, uuid.UUID, *uuid.UUID, ListAgentTokensOptions) (*AgentTokenListResponse, error)
	RevokeAgentToken(context.Context, uuid.UUID, uuid.UUID) error
	AgentTokenResource(context.Context, uuid.UUID, uuid.UUID) (*uuid.UUID, error)
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
func (h *RegistrationHandler) RegisterProtected(api *echo.Group, authMiddleware echo.MiddlewareFunc) {
	g := api.Group("/creator/agent-tokens", authMiddleware)
	g.POST("", h.CreateAgentToken)
	g.GET("", h.ListAgentTokens)
	g.DELETE("/:id", h.RevokeAgentToken)
}

// RegisterRuntimeAttachReadOnly mounts token metadata lookup without exposing
// token issuance, revocation, or Agent registration during a release cutover.
func (h *RegistrationHandler) RegisterRuntimeAttachReadOnly(api *echo.Group, authMiddleware echo.MiddlewareFunc) {
	api.GET("/creator/agent-tokens", h.ListAgentTokens, authMiddleware)
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
	var agentID *uuid.UUID
	if strings.TrimSpace(req.AgentID) != "" {
		parsed, _ := uuid.Parse(strings.TrimSpace(req.AgentID))
		agentID = &parsed
	}
	if err := auth.RequirePermission(c, "agent-tokens:issue", "agent", agentID); err != nil {
		return err
	}
	if agentID == nil {
		if err := auth.RequirePermission(c, "agents:create", "agent", nil); err != nil {
			return err
		}
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
	if err := auth.RequirePermission(c, "agent-tokens:read", "agent", agentID); err != nil {
		return err
	}
	opts, err := parseAgentTokenListOptions(c)
	if err != nil {
		return err
	}
	resp, err := h.svc.ListAgentTokens(c.Request().Context(), uid, agentID, opts)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
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
	agentID, err := h.svc.AgentTokenResource(c.Request().Context(), uid, tokenID)
	if err != nil {
		return err
	}
	if err := auth.RequirePermission(c, "agent-tokens:revoke", "agent", agentID); err != nil {
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

func parseAgentTokenListOptions(c echo.Context) (ListAgentTokensOptions, error) {
	limit, err := parseInt32ListQuery(c.QueryParam("limit"), 10, 1, 50, "limit")
	if err != nil {
		return ListAgentTokensOptions{}, err
	}
	offset, err := parseInt32ListQuery(c.QueryParam("offset"), 0, 0, 100000, "offset")
	if err != nil {
		return ListAgentTokensOptions{}, err
	}
	status, err := parseAgentTokenListStatus(c.QueryParam("status"))
	if err != nil {
		return ListAgentTokensOptions{}, err
	}
	query := strings.TrimSpace(c.QueryParam("q"))
	if len([]rune(query)) > 120 {
		return ListAgentTokensOptions{}, httpx.BadRequest("q 最多 120 个字符")
	}
	return normalizeAgentTokenListOptions(ListAgentTokensOptions{
		Limit:   limit,
		Offset:  offset,
		SortBy:  strings.TrimSpace(c.QueryParam("sort_by")),
		SortDir: strings.ToLower(strings.TrimSpace(c.QueryParam("sort_dir"))),
		Status:  status,
		Query:   query,
	}), nil
}

func parseAgentTokenListStatus(raw string) (string, error) {
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

func parseInt32ListQuery(raw string, fallback, minValue, maxValue int32, name string) (int32, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback, nil
	}
	n, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0, httpx.BadRequest(name + " 不是合法整数")
	}
	value := int32(n)
	if value < minValue {
		return 0, httpx.BadRequest(name + " 过小")
	}
	if value > maxValue {
		return maxValue, nil
	}
	return value, nil
}
