package a2a

import (
	"context"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

type Handler struct {
	svc                   service
	cardProvider          AgentCardProvider
	validator             *validator.Validate
	requiredA2AExtensions []string
	runUpdates            runtime.RunUpdateSource
}

type service interface {
	GetRuntimeWorkbench(ctx context.Context, userID, agentID uuid.UUID) (*RuntimeWorkbenchResponse, error)
	GetCallPolicy(ctx context.Context, userID, agentID uuid.UUID) (*CallPolicyResponse, error)
	UpdateCallPolicy(ctx context.Context, userID, agentID uuid.UUID, req *UpdateCallPolicyRequest) (*CallPolicyResponse, error)
	ListChildren(ctx context.Context, userID, parentRunID uuid.UUID) ([]ChildRunResponse, error)
	ListParentRuns(ctx context.Context, userID uuid.UUID, page, size int32, search string) (*ParentRunListResponse, error)
	SendProtocolMessage(ctx context.Context, userID uuid.UUID, slug string, params *A2AMessageSendParams) (*A2ATask, error)
	StartProtocolMessage(ctx context.Context, userID uuid.UUID, slug string, params *A2AMessageSendParams) (*A2ATask, error)
	GetProtocolTask(ctx context.Context, userID uuid.UUID, slug, taskID string, historyLength *int) (*A2ATask, error)
	ListProtocolTasks(ctx context.Context, userID uuid.UUID, slug string, params *A2ATaskListParams) (*A2ATaskListResponse, error)
	CancelProtocolTask(ctx context.Context, userID uuid.UUID, slug, taskID string) (*A2ATask, error)
	ListProtocolTaskEvents(ctx context.Context, userID uuid.UUID, slug, taskID string, afterSequence int32) ([]interface{}, bool, int32, error)
	SetPushNotificationConfig(ctx context.Context, userID uuid.UUID, slug string, params *A2ATaskPushConfigParams) (*A2ATaskPushNotificationConfig, error)
	GetPushNotificationConfig(ctx context.Context, userID uuid.UUID, slug string, params *A2ATaskPushConfigParams) (*A2ATaskPushNotificationConfig, error)
	ListPushNotificationConfigs(ctx context.Context, userID uuid.UUID, slug string, params *A2ATaskPushConfigParams) (*A2ATaskPushConfigList, error)
	DeletePushNotificationConfig(ctx context.Context, userID uuid.UUID, slug string, params *A2ATaskPushConfigParams) error
}

func NewHandler(svc service) *Handler {
	return &Handler{svc: svc, validator: validator.New(validator.WithRequiredStructEnabled())}
}

type AgentCardProvider interface {
	GetAgentCardBySlug(ctx context.Context, slug string) (*agent.AgentCardResponse, error)
	GetExtendedAgentCardBySlug(ctx context.Context, slug string) (*agent.AgentCardResponse, error)
}

func (h *Handler) SetAgentCardProvider(provider AgentCardProvider) {
	h.cardProvider = provider
}

func (h *Handler) SetRunUpdateSource(source runtime.RunUpdateSource) {
	if h != nil {
		h.runUpdates = source
	}
}

// Register mounts creator controls and user-visible A2A trace lookup. Runtime
// credentials are issued and rotated by the Agent registration API, not by a
// second A2A-owned token surface.
func (h *Handler) Register(api *echo.Group, jwtMiddleware, queryMiddleware echo.MiddlewareFunc) {
	creator := api.Group("/creator/agents/:id", jwtMiddleware)
	creator.GET("/runtime-workbench", h.GetRuntimeWorkbench)
	creator.GET("/a2a-policy", h.GetCallPolicy)
	creator.PUT("/a2a-policy", h.UpdateCallPolicy)

	api.GET("/a2a/parents", h.ListParentRuns, queryMiddleware)
	publicProtocol := api.Group("/a2a/agents/:slug")
	publicProtocol.GET("/.well-known/agent-card.json", h.GetPublicAgentCardHTTP)
	protocol := api.Group("/a2a/agents/:slug", queryMiddleware, h.resolveA2ATargetAgent, a2aHTTPErrorMiddleware)
	h.registerProtocolRoutes(protocol)
	api.GET("/runs/:id/children", h.ListChildren, queryMiddleware)
}

// RegisterAgentRuntimeProxy mounts the legacy AgentNode A2A surface on the
// dedicated Runtime mTLS listener. The supplied middleware must derive the
// creator and target Agent from the authenticated Runtime principal.
func (h *Handler) RegisterAgentRuntimeProxy(api *echo.Group, runtimeIdentity echo.MiddlewareFunc) {
	if api == nil || runtimeIdentity == nil {
		return
	}
	protocol := api.Group(
		"/agent-runtime/a2a-proxy/agents/:slug",
		runtimeIdentity,
		h.resolveA2ATargetAgent,
		a2aHTTPErrorMiddleware,
	)
	h.registerProtocolRoutes(protocol)
}

func (h *Handler) registerProtocolRoutes(protocol *echo.Group) {
	protocol.POST("", h.JSONRPC)
	protocol.GET("/extendedAgentCard", h.GetExtendedAgentCardHTTP)
	protocol.POST("/message:action", h.MessageHTTP)
	protocol.GET("/tasks", h.ListTasksHTTP)
	protocol.GET("/tasks/:taskID", h.GetTaskHTTP)
	protocol.POST("/tasks/:taskID/cancel", h.CancelTaskHTTP)
	protocol.POST("/tasks/:taskID/pushNotificationConfig", h.SetTaskPushNotificationHTTP)
	protocol.GET("/tasks/:taskID/pushNotificationConfig", h.ListTaskPushNotificationsHTTP)
	protocol.GET("/tasks/:taskID/pushNotificationConfig/:configID", h.GetTaskPushNotificationHTTP)
	protocol.DELETE("/tasks/:taskID/pushNotificationConfig/:configID", h.DeleteTaskPushNotificationHTTP)
	protocol.POST("/tasks/:taskID/pushNotificationConfigs", h.SetTaskPushNotificationHTTP)
	protocol.GET("/tasks/:taskID/pushNotificationConfigs", h.ListTaskPushNotificationsHTTP)
	protocol.GET("/tasks/:taskID/pushNotificationConfigs/:configID", h.GetTaskPushNotificationHTTP)
	protocol.DELETE("/tasks/:taskID/pushNotificationConfigs/:configID", h.DeleteTaskPushNotificationHTTP)
	protocol.GET("/tasks/:taskID/subscribe", h.SubscribeTaskHTTP)
	protocol.POST("/tasks/:taskID/subscribe", h.SubscribeTaskHTTP)
	protocol.GET("/tasks/*", h.TaskActionHTTP)
	protocol.POST("/tasks/*", h.TaskActionHTTP)
}

func (h *Handler) resolveA2ATargetAgent(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		if h.cardProvider != nil {
			card, err := h.cardProvider.GetAgentCardBySlug(c.Request().Context(), c.Param("slug"))
			if err == nil && card != nil {
				if agentID, parseErr := uuid.Parse(card.OpenLinker.AgentID); parseErr == nil {
					c.Set(a2aTargetAgentIDContextKey, agentID)
				}
			}
		}
		return next(c)
	}
}

func (h *Handler) GetRuntimeWorkbench(c echo.Context) error {
	userID, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	agentID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	resp, err := h.svc.GetRuntimeWorkbench(c.Request().Context(), userID, agentID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) GetCallPolicy(c echo.Context) error {
	userID, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	agentID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	resp, err := h.svc.GetCallPolicy(c.Request().Context(), userID, agentID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) UpdateCallPolicy(c echo.Context) error {
	userID, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	agentID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	var req UpdateCallPolicyRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.UpdateCallPolicy(c.Request().Context(), userID, agentID, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) ListChildren(c echo.Context) error {
	userID, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	parentRunID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	items, err := h.svc.ListChildren(c.Request().Context(), userID, parentRunID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]any{"parent_run_id": parentRunID.String(), "items": items})
}

func (h *Handler) ListParentRuns(c echo.Context) error {
	userID, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	resp, err := h.svc.ListParentRuns(
		c.Request().Context(),
		userID,
		parseInt32Query(c.QueryParam("page"), 1),
		parseInt32Query(c.QueryParam("size"), 10),
		parseSearchQuery(c),
	)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func parseInt32Query(raw string, fallback int32) int32 {
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return fallback
	}
	// #nosec G115 -- strconv.ParseInt with bitSize=32 guarantees int32 range.
	return int32(value)
}

func parseSearchQuery(c echo.Context) string {
	if value := strings.TrimSpace(c.QueryParam("q")); value != "" {
		return value
	}
	return strings.TrimSpace(c.QueryParam("search"))
}

func userIDFromCtx(c echo.Context) (uuid.UUID, error) {
	id := httpx.UserIDFrom(c)
	if id == "" {
		return uuid.Nil, httpx.Unauthorized("")
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		return uuid.Nil, httpx.Unauthorized("token 无效")
	}
	return parsed, nil
}
