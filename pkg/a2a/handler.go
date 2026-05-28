package a2a

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

type Handler struct {
	svc       *Service
	validator *validator.Validate
}

func NewHandler(svc *Service) *Handler {
	return &Handler{svc: svc, validator: validator.New(validator.WithRequiredStructEnabled())}
}

// Register mounts creator controls, runtime-token invocation and user-visible trace lookup.
func (h *Handler) Register(api *echo.Group, jwtMiddleware, queryMiddleware echo.MiddlewareFunc) {
	creator := api.Group("/creator/agents/:id", jwtMiddleware)
	creator.POST("/runtime-tokens", h.CreateRuntimeToken)
	creator.GET("/runtime-tokens", h.ListRuntimeTokens)
	creator.GET("/a2a-policy", h.GetCallPolicy)
	creator.PUT("/a2a-policy", h.UpdateCallPolicy)

	api.DELETE("/creator/runtime-tokens/:tokenID", h.RevokeRuntimeToken, jwtMiddleware)
	api.POST("/agent-runtime/call-agent", h.CallAgent)
	api.GET("/a2a/parents", h.ListParentRuns, queryMiddleware)
	protocol := api.Group("/a2a/agents/:slug", queryMiddleware)
	protocol.POST("", h.JSONRPC)
	protocol.POST("/message:action", h.MessageHTTP)
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
	api.GET("/runs/:id/children", h.ListChildren, queryMiddleware)
}

func (h *Handler) CreateRuntimeToken(c echo.Context) error {
	userID, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	agentID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	var req CreateRuntimeTokenRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.CreateRuntimeToken(c.Request().Context(), userID, agentID, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusCreated, resp)
}

func (h *Handler) ListRuntimeTokens(c echo.Context) error {
	userID, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	agentID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	items, err := h.svc.ListRuntimeTokens(c.Request().Context(), userID, agentID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]any{"items": items})
}

func (h *Handler) RevokeRuntimeToken(c echo.Context) error {
	userID, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	tokenID, err := uuid.Parse(c.Param("tokenID"))
	if err != nil {
		return httpx.BadRequest("tokenID 不是合法 uuid")
	}
	if err := h.svc.RevokeRuntimeToken(c.Request().Context(), userID, tokenID); err != nil {
		return err
	}
	return c.NoContent(http.StatusNoContent)
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

func (h *Handler) CallAgent(c echo.Context) error {
	token, err := bearerToken(c.Request().Header.Get(echo.HeaderAuthorization))
	if err != nil {
		return err
	}
	var req CallAgentRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if req.ParentRunID == "" && req.CurrentRunID == "" {
		req.CurrentRunID = strings.TrimSpace(c.Request().Header.Get("X-OpenLinker-Run-Id"))
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	if req.ParentRunID == "" && req.CurrentRunID == "" {
		return httpx.Unprocessable("current_run_id 或 parent_run_id 必填")
	}
	resp, err := h.svc.CallAgent(c.Request().Context(), token, &req)
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
	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return int32(value)
}

func bearerToken(header string) (string, error) {
	parts := strings.SplitN(strings.TrimSpace(header), " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] == "" {
		return "", httpx.Unauthorized("缺少 Runtime Token")
	}
	return parts[1], nil
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
