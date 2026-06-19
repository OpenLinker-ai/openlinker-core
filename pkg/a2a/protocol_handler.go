package a2a

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

const (
	jsonRPCParseError     = -32700
	jsonRPCInvalidRequest = -32600
	jsonRPCMethodNotFound = -32601
	jsonRPCInvalidParams  = -32602
	jsonRPCInternalError  = -32603
	a2aSSEPollInterval    = time.Second
	a2aSSEHeartbeat       = 15 * time.Second
)

// JSONRPC handles the A2A JSON-RPC binding for one public Agent slug.
func (h *Handler) JSONRPC(c echo.Context) error {
	var req JSONRPCRequest
	if err := json.NewDecoder(c.Request().Body).Decode(&req); err != nil {
		return c.JSON(http.StatusBadRequest, jsonRPCError(nil, jsonRPCParseError, "JSON-RPC 请求体格式错误", nil))
	}
	if req.JSONRPC != "2.0" || strings.TrimSpace(req.Method) == "" {
		return c.JSON(http.StatusOK, jsonRPCError(req.ID, jsonRPCInvalidRequest, "JSON-RPC 请求必须包含 jsonrpc=2.0 和 method", nil))
	}
	protocolVersion, err := a2aVersionFromRequest(c)
	if err != nil {
		return c.JSON(http.StatusOK, a2aUnsupportedVersionJSONRPCError(req.ID, err))
	}
	setA2AVersionHeader(c, protocolVersion)

	userID, err := userIDFromCtx(c)
	if err != nil {
		return c.JSON(http.StatusOK, jsonRPCErrorFrom(req.ID, err))
	}

	switch normalizeA2AJSONRPCMethod(req.Method) {
	case "message/send":
		if err := requireScope(c, "agents:run"); err != nil {
			return c.JSON(http.StatusOK, jsonRPCErrorFrom(req.ID, err))
		}
		var params A2AMessageSendParams
		if err := decodeJSONRPCParams(req.Params, &params); err != nil {
			return c.JSON(http.StatusOK, jsonRPCError(req.ID, jsonRPCInvalidParams, err.Error(), nil))
		}
		attachProtocolVersionMetadata(&params, protocolVersion)
		task, err := h.svc.SendProtocolMessage(c.Request().Context(), userID, c.Param("slug"), &params)
		if err != nil {
			return c.JSON(http.StatusOK, jsonRPCErrorFrom(req.ID, err))
		}
		return c.JSON(http.StatusOK, jsonRPCResultWithVersion(req.ID, task, protocolVersion))
	case "message/stream":
		if err := requireScope(c, "agents:run"); err != nil {
			return c.JSON(http.StatusOK, jsonRPCErrorFrom(req.ID, err))
		}
		var params A2AMessageSendParams
		if err := decodeJSONRPCParams(req.Params, &params); err != nil {
			return c.JSON(http.StatusOK, jsonRPCError(req.ID, jsonRPCInvalidParams, err.Error(), nil))
		}
		attachProtocolVersionMetadata(&params, protocolVersion)
		task, err := h.svc.StartProtocolMessage(c.Request().Context(), userID, c.Param("slug"), &params)
		if err != nil {
			return c.JSON(http.StatusOK, jsonRPCErrorFrom(req.ID, err))
		}
		return h.streamProtocolTask(c, userID, c.Param("slug"), task.ID, req.ID, true, task, protocolVersion)
	case "tasks/get":
		if err := requireScope(c, "runs:read"); err != nil {
			return c.JSON(http.StatusOK, jsonRPCErrorFrom(req.ID, err))
		}
		var params A2ATaskQueryParams
		if err := decodeJSONRPCParams(req.Params, &params); err != nil {
			return c.JSON(http.StatusOK, jsonRPCError(req.ID, jsonRPCInvalidParams, err.Error(), nil))
		}
		task, err := h.svc.GetProtocolTask(c.Request().Context(), userID, c.Param("slug"), params.ID, params.HistoryLength)
		if err != nil {
			return c.JSON(http.StatusOK, jsonRPCErrorFrom(req.ID, err))
		}
		return c.JSON(http.StatusOK, jsonRPCResultWithVersion(req.ID, task, protocolVersion))
	case "tasks/list":
		if err := requireScope(c, "runs:read"); err != nil {
			return c.JSON(http.StatusOK, jsonRPCErrorFrom(req.ID, err))
		}
		var params A2ATaskListParams
		if len(req.Params) > 0 && string(req.Params) != "null" {
			if err := decodeJSONRPCParams(req.Params, &params); err != nil {
				return c.JSON(http.StatusOK, jsonRPCError(req.ID, jsonRPCInvalidParams, err.Error(), nil))
			}
		}
		resp, err := h.svc.ListProtocolTasks(c.Request().Context(), userID, c.Param("slug"), &params)
		if err != nil {
			return c.JSON(http.StatusOK, jsonRPCErrorFrom(req.ID, err))
		}
		return c.JSON(http.StatusOK, jsonRPCResultWithVersion(req.ID, resp, protocolVersion))
	case "tasks/cancel":
		if err := requireScope(c, "agents:run"); err != nil {
			return c.JSON(http.StatusOK, jsonRPCErrorFrom(req.ID, err))
		}
		var params A2ATaskQueryParams
		if err := decodeJSONRPCParams(req.Params, &params); err != nil {
			return c.JSON(http.StatusOK, jsonRPCError(req.ID, jsonRPCInvalidParams, err.Error(), nil))
		}
		task, err := h.svc.CancelProtocolTask(c.Request().Context(), userID, c.Param("slug"), params.ID)
		if err != nil {
			return c.JSON(http.StatusOK, jsonRPCErrorFrom(req.ID, err))
		}
		return c.JSON(http.StatusOK, jsonRPCResultWithVersion(req.ID, task, protocolVersion))
	case "tasks/resubscribe":
		if err := requireScope(c, "runs:read"); err != nil {
			return c.JSON(http.StatusOK, jsonRPCErrorFrom(req.ID, err))
		}
		var params A2ATaskQueryParams
		if err := decodeJSONRPCParams(req.Params, &params); err != nil {
			return c.JSON(http.StatusOK, jsonRPCError(req.ID, jsonRPCInvalidParams, err.Error(), nil))
		}
		task, err := h.svc.GetProtocolTask(c.Request().Context(), userID, c.Param("slug"), params.ID, params.HistoryLength)
		if err != nil {
			return c.JSON(http.StatusOK, jsonRPCErrorFrom(req.ID, err))
		}
		return h.streamProtocolTask(c, userID, c.Param("slug"), params.ID, req.ID, true, task, protocolVersion)
	case "tasks/pushNotificationConfig/set":
		if err := requireScope(c, "runs:read"); err != nil {
			return c.JSON(http.StatusOK, jsonRPCErrorFrom(req.ID, err))
		}
		var params A2ATaskPushConfigParams
		if err := decodeJSONRPCParams(req.Params, &params); err != nil {
			return c.JSON(http.StatusOK, jsonRPCError(req.ID, jsonRPCInvalidParams, err.Error(), nil))
		}
		resp, err := h.svc.SetPushNotificationConfig(c.Request().Context(), userID, c.Param("slug"), &params)
		if err != nil {
			return c.JSON(http.StatusOK, jsonRPCErrorFrom(req.ID, err))
		}
		return c.JSON(http.StatusOK, jsonRPCResultWithVersion(req.ID, resp, protocolVersion))
	case "tasks/pushNotificationConfig/get":
		if err := requireScope(c, "runs:read"); err != nil {
			return c.JSON(http.StatusOK, jsonRPCErrorFrom(req.ID, err))
		}
		var params A2ATaskPushConfigParams
		if err := decodeJSONRPCParams(req.Params, &params); err != nil {
			return c.JSON(http.StatusOK, jsonRPCError(req.ID, jsonRPCInvalidParams, err.Error(), nil))
		}
		resp, err := h.svc.GetPushNotificationConfig(c.Request().Context(), userID, c.Param("slug"), &params)
		if err != nil {
			return c.JSON(http.StatusOK, jsonRPCErrorFrom(req.ID, err))
		}
		return c.JSON(http.StatusOK, jsonRPCResultWithVersion(req.ID, resp, protocolVersion))
	case "tasks/pushNotificationConfig/list":
		if err := requireScope(c, "runs:read"); err != nil {
			return c.JSON(http.StatusOK, jsonRPCErrorFrom(req.ID, err))
		}
		var params A2ATaskPushConfigParams
		if err := decodeJSONRPCParams(req.Params, &params); err != nil {
			return c.JSON(http.StatusOK, jsonRPCError(req.ID, jsonRPCInvalidParams, err.Error(), nil))
		}
		resp, err := h.svc.ListPushNotificationConfigs(c.Request().Context(), userID, c.Param("slug"), &params)
		if err != nil {
			return c.JSON(http.StatusOK, jsonRPCErrorFrom(req.ID, err))
		}
		return c.JSON(http.StatusOK, jsonRPCResultWithVersion(req.ID, resp, protocolVersion))
	case "tasks/pushNotificationConfig/delete":
		if err := requireScope(c, "runs:read"); err != nil {
			return c.JSON(http.StatusOK, jsonRPCErrorFrom(req.ID, err))
		}
		var params A2ATaskPushConfigParams
		if err := decodeJSONRPCParams(req.Params, &params); err != nil {
			return c.JSON(http.StatusOK, jsonRPCError(req.ID, jsonRPCInvalidParams, err.Error(), nil))
		}
		if err := h.svc.DeletePushNotificationConfig(c.Request().Context(), userID, c.Param("slug"), &params); err != nil {
			return c.JSON(http.StatusOK, jsonRPCErrorFrom(req.ID, err))
		}
		return c.JSON(http.StatusOK, jsonRPCNullResult(req.ID))
	case "agent/getExtendedCard":
		if err := requireScope(c, "runs:read"); err != nil {
			return c.JSON(http.StatusOK, jsonRPCErrorFrom(req.ID, err))
		}
		card, err := h.extendedAgentCard(c)
		if err != nil {
			return c.JSON(http.StatusOK, jsonRPCErrorFrom(req.ID, err))
		}
		return c.JSON(http.StatusOK, jsonRPCResultWithVersion(req.ID, card, protocolVersion))
	default:
		return c.JSON(http.StatusOK, jsonRPCError(req.ID, jsonRPCMethodNotFound, "不支持的 A2A 方法: "+req.Method, nil))
	}
}

// GetPublicAgentCardHTTP exposes the A2A well-known Agent Card beneath the
// agent's protocol base URL while keeping the existing marketplace card URL.
func (h *Handler) GetPublicAgentCardHTTP(c echo.Context) error {
	if h.cardProvider == nil {
		return httpx.ServiceUnavailable("A2A Agent Card 服务未启用")
	}
	card, err := h.cardProvider.GetAgentCardBySlug(c.Request().Context(), c.Param("slug"))
	if err != nil {
		return err
	}
	c.Response().Header().Set(echo.HeaderCacheControl, "public, max-age=300")
	return c.JSON(http.StatusOK, card)
}

func (h *Handler) GetExtendedAgentCardHTTP(c echo.Context) error {
	protocolVersion, err := a2aVersionFromRequest(c)
	if err != nil {
		return err
	}
	setA2AVersionHeader(c, protocolVersion)
	if err := requireScope(c, "runs:read"); err != nil {
		return err
	}
	card, err := h.extendedAgentCard(c)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, normalizeA2AResultForVersion(card, protocolVersion))
}

func (h *Handler) extendedAgentCard(c echo.Context) (interface{}, error) {
	if h.cardProvider == nil {
		return nil, httpx.ServiceUnavailable("A2A Extended Agent Card 服务未启用")
	}
	return h.cardProvider.GetExtendedAgentCardBySlug(c.Request().Context(), c.Param("slug"))
}

// SendMessageHTTP handles the A2A HTTP+JSON alias POST /message:send.
func (h *Handler) SendMessageHTTP(c echo.Context) error {
	protocolVersion, err := a2aVersionFromRequest(c)
	if err != nil {
		return err
	}
	setA2AVersionHeader(c, protocolVersion)
	if err := requireScope(c, "agents:run"); err != nil {
		return err
	}
	userID, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	var params A2AMessageSendParams
	if err := c.Bind(&params); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	attachProtocolVersionMetadata(&params, protocolVersion)
	task, err := h.svc.SendProtocolMessage(c.Request().Context(), userID, c.Param("slug"), &params)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, normalizeA2AResultForVersion(task, protocolVersion))
}

func (h *Handler) ListTasksHTTP(c echo.Context) error {
	protocolVersion, err := a2aVersionFromRequest(c)
	if err != nil {
		return err
	}
	setA2AVersionHeader(c, protocolVersion)
	if err := requireScope(c, "runs:read"); err != nil {
		return err
	}
	userID, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	params, err := a2aTaskListParamsFromQuery(c)
	if err != nil {
		return err
	}
	resp, err := h.svc.ListProtocolTasks(c.Request().Context(), userID, c.Param("slug"), params)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, normalizeA2AResultForVersion(resp, protocolVersion))
}

// MessageHTTP dispatches literal A2A colon actions such as /message:send and /message:stream.
func (h *Handler) MessageHTTP(c echo.Context) error {
	action := strings.TrimPrefix(c.Param("action"), ":")
	if action == "" {
		path := c.Request().URL.Path
		if idx := strings.LastIndex(path, "/message:"); idx >= 0 {
			action = path[idx+len("/message:"):]
		}
	}
	switch action {
	case "send":
		return h.SendMessageHTTP(c)
	case "stream":
		return h.StreamMessageHTTP(c)
	default:
		return httpx.NotFound("A2A message action 不存在")
	}
}

// StreamMessageHTTP handles the A2A HTTP+JSON alias POST /message:stream.
func (h *Handler) StreamMessageHTTP(c echo.Context) error {
	protocolVersion, err := a2aVersionFromRequest(c)
	if err != nil {
		return err
	}
	setA2AVersionHeader(c, protocolVersion)
	if err := requireScope(c, "agents:run"); err != nil {
		return err
	}
	userID, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	var params A2AMessageSendParams
	if err := c.Bind(&params); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	attachProtocolVersionMetadata(&params, protocolVersion)
	task, err := h.svc.StartProtocolMessage(c.Request().Context(), userID, c.Param("slug"), &params)
	if err != nil {
		return err
	}
	return h.streamProtocolTask(c, userID, c.Param("slug"), task.ID, nil, false, task, protocolVersion)
}

// GetTaskHTTP handles the A2A HTTP+JSON alias GET /tasks/:taskID.
func (h *Handler) GetTaskHTTP(c echo.Context) error {
	protocolVersion, err := a2aVersionFromRequest(c)
	if err != nil {
		return err
	}
	setA2AVersionHeader(c, protocolVersion)
	if err := requireScope(c, "runs:read"); err != nil {
		return err
	}
	userID, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	historyLength, err := optionalIntQuery(c.QueryParam("historyLength"))
	if err != nil {
		return err
	}
	task, err := h.svc.GetProtocolTask(c.Request().Context(), userID, c.Param("slug"), c.Param("taskID"), historyLength)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, normalizeA2AResultForVersion(task, protocolVersion))
}

// SubscribeTaskHTTP handles the A2A HTTP+JSON task subscription alias.
func (h *Handler) SubscribeTaskHTTP(c echo.Context) error {
	protocolVersion, err := a2aVersionFromRequest(c)
	if err != nil {
		return err
	}
	setA2AVersionHeader(c, protocolVersion)
	if err := requireScope(c, "runs:read"); err != nil {
		return err
	}
	userID, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	taskID, err := taskIDFromSubscribeRequest(c)
	if err != nil {
		return err
	}
	task, err := h.svc.GetProtocolTask(c.Request().Context(), userID, c.Param("slug"), taskID, nil)
	if err != nil {
		return err
	}
	return h.streamProtocolTask(c, userID, c.Param("slug"), taskID, nil, false, task, protocolVersion)
}

// TaskActionHTTP dispatches literal A2A task colon actions such as /tasks/{id}:subscribe and /tasks/{id}:cancel.
func (h *Handler) TaskActionHTTP(c echo.Context) error {
	raw := strings.TrimSpace(c.Param("*"))
	switch {
	case strings.HasSuffix(raw, ":subscribe") || strings.HasSuffix(raw, "/subscribe"):
		return h.SubscribeTaskHTTP(c)
	case strings.HasSuffix(raw, ":cancel") || strings.HasSuffix(raw, "/cancel"):
		return h.CancelTaskHTTP(c)
	default:
		return httpx.NotFound("A2A task action 不存在")
	}
}

// CancelTaskHTTP handles the A2A HTTP+JSON alias POST /tasks/:taskID:cancel.
func (h *Handler) CancelTaskHTTP(c echo.Context) error {
	protocolVersion, err := a2aVersionFromRequest(c)
	if err != nil {
		return err
	}
	setA2AVersionHeader(c, protocolVersion)
	if err := requireScope(c, "agents:run"); err != nil {
		return err
	}
	userID, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	taskID, err := taskIDFromActionRequest(c, "cancel")
	if err != nil {
		return err
	}
	task, err := h.svc.CancelProtocolTask(c.Request().Context(), userID, c.Param("slug"), taskID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, normalizeA2AResultForVersion(task, protocolVersion))
}

func (h *Handler) SetTaskPushNotificationHTTP(c echo.Context) error {
	protocolVersion, err := a2aVersionFromRequest(c)
	if err != nil {
		return err
	}
	setA2AVersionHeader(c, protocolVersion)
	if err := requireScope(c, "runs:read"); err != nil {
		return err
	}
	userID, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	var params A2ATaskPushConfigParams
	if err := c.Bind(&params); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	params.ID = c.Param("taskID")
	resp, err := h.svc.SetPushNotificationConfig(c.Request().Context(), userID, c.Param("slug"), &params)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusCreated, normalizeA2AResultForVersion(resp, protocolVersion))
}

func (h *Handler) ListTaskPushNotificationsHTTP(c echo.Context) error {
	protocolVersion, err := a2aVersionFromRequest(c)
	if err != nil {
		return err
	}
	setA2AVersionHeader(c, protocolVersion)
	if err := requireScope(c, "runs:read"); err != nil {
		return err
	}
	userID, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	resp, err := h.svc.ListPushNotificationConfigs(c.Request().Context(), userID, c.Param("slug"), &A2ATaskPushConfigParams{ID: c.Param("taskID")})
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, normalizeA2AResultForVersion(resp, protocolVersion))
}

func (h *Handler) GetTaskPushNotificationHTTP(c echo.Context) error {
	protocolVersion, err := a2aVersionFromRequest(c)
	if err != nil {
		return err
	}
	setA2AVersionHeader(c, protocolVersion)
	if err := requireScope(c, "runs:read"); err != nil {
		return err
	}
	userID, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	resp, err := h.svc.GetPushNotificationConfig(c.Request().Context(), userID, c.Param("slug"), &A2ATaskPushConfigParams{
		ID:                       c.Param("taskID"),
		PushNotificationConfigID: c.Param("configID"),
	})
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, normalizeA2AResultForVersion(resp, protocolVersion))
}

func (h *Handler) DeleteTaskPushNotificationHTTP(c echo.Context) error {
	protocolVersion, err := a2aVersionFromRequest(c)
	if err != nil {
		return err
	}
	setA2AVersionHeader(c, protocolVersion)
	if err := requireScope(c, "runs:read"); err != nil {
		return err
	}
	userID, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	if err := h.svc.DeletePushNotificationConfig(c.Request().Context(), userID, c.Param("slug"), &A2ATaskPushConfigParams{
		ID:                       c.Param("taskID"),
		PushNotificationConfigID: c.Param("configID"),
	}); err != nil {
		return err
	}
	return c.NoContent(http.StatusNoContent)
}

func normalizeA2AJSONRPCMethod(method string) string {
	switch strings.TrimSpace(method) {
	case "message/send", "message:send", "SendMessage":
		return "message/send"
	case "message/stream", "message:stream", "SendStreamingMessage":
		return "message/stream"
	case "tasks/get", "GetTask":
		return "tasks/get"
	case "tasks/list", "ListTasks":
		return "tasks/list"
	case "tasks/cancel", "CancelTask":
		return "tasks/cancel"
	case "tasks/resubscribe", "SubscribeToTask":
		return "tasks/resubscribe"
	case "tasks/pushNotificationConfig/set", "SetTaskPushNotificationConfig", "CreateTaskPushNotificationConfig":
		return "tasks/pushNotificationConfig/set"
	case "tasks/pushNotificationConfig/get", "GetTaskPushNotificationConfig":
		return "tasks/pushNotificationConfig/get"
	case "tasks/pushNotificationConfig/list", "ListTaskPushNotificationConfigs", "ListTaskPushNotificationConfig":
		return "tasks/pushNotificationConfig/list"
	case "tasks/pushNotificationConfig/delete", "DeleteTaskPushNotificationConfig":
		return "tasks/pushNotificationConfig/delete"
	case "agent/getExtendedCard", "GetExtendedAgentCard":
		return "agent/getExtendedCard"
	default:
		return strings.TrimSpace(method)
	}
}

func attachProtocolVersionMetadata(params *A2AMessageSendParams, version string) {
	if params == nil || version == "" {
		return
	}
	if params.Metadata == nil {
		params.Metadata = map[string]interface{}{}
	}
	params.Metadata["a2a_protocol_version"] = version
}

func decodeJSONRPCParams(raw json.RawMessage, target interface{}) error {
	if len(raw) == 0 || string(raw) == "null" {
		return errors.New("params 不能为空")
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return errors.New("params 格式错误")
	}
	return nil
}

func jsonRPCResult(id json.RawMessage, result interface{}) JSONRPCResponse {
	return JSONRPCResponse{JSONRPC: "2.0", ID: normalizeJSONRPCID(id), Result: result}
}

func jsonRPCNullResult(id json.RawMessage) JSONRPCResponse {
	return JSONRPCResponse{JSONRPC: "2.0", ID: normalizeJSONRPCID(id), Result: json.RawMessage("null")}
}

func jsonRPCError(id json.RawMessage, code int, message string, data interface{}) JSONRPCResponse {
	return JSONRPCResponse{
		JSONRPC: "2.0",
		ID:      normalizeJSONRPCID(id),
		Error:   &JSONRPCError{Code: code, Message: message, Data: data},
	}
}

func jsonRPCErrorFrom(id json.RawMessage, err error) JSONRPCResponse {
	var he *httpx.HTTPError
	if errors.As(err, &he) {
		code := jsonRPCInternalError
		switch he.Status {
		case http.StatusBadRequest, http.StatusUnprocessableEntity:
			code = jsonRPCInvalidParams
		case http.StatusUnauthorized:
			code = -32001
		case http.StatusForbidden:
			code = -32003
		case http.StatusNotFound:
			code = -32004
		case http.StatusConflict:
			code = -32009
		}
		return jsonRPCError(id, code, he.Message, map[string]interface{}{
			"http_status": he.Status,
			"code":        he.Code,
		})
	}
	return jsonRPCError(id, jsonRPCInternalError, "internal error", nil)
}

func normalizeJSONRPCID(id json.RawMessage) json.RawMessage {
	if len(id) == 0 {
		return json.RawMessage("null")
	}
	return id
}

func requireScope(c echo.Context, scope string) error {
	if httpx.AuthMethodFrom(c) == "apikey" && !httpx.HasScope(c, scope) {
		return httpx.Forbidden("访问令牌缺少 scope: " + scope)
	}
	return nil
}

func optionalIntQuery(raw string) (*int, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return nil, httpx.BadRequest("historyLength 必须是非负整数")
	}
	return &value, nil
}

func a2aTaskListParamsFromQuery(c echo.Context) (*A2ATaskListParams, error) {
	pageSize, err := optionalIntQuery(firstQueryParam(c, "pageSize", "page_size"))
	if err != nil {
		return nil, err
	}
	historyLength, err := optionalIntQuery(firstQueryParam(c, "historyLength", "history_length"))
	if err != nil {
		return nil, err
	}
	includeArtifacts, err := optionalBoolQuery(firstQueryParam(c, "includeArtifacts", "include_artifacts"))
	if err != nil {
		return nil, err
	}
	return &A2ATaskListParams{
		ContextID:            firstQueryParam(c, "contextId", "context_id"),
		Status:               firstQueryParam(c, "status"),
		PageSize:             pageSize,
		PageToken:            firstQueryParam(c, "pageToken", "page_token"),
		HistoryLength:        historyLength,
		StatusTimestampAfter: firstQueryParam(c, "statusTimestampAfter", "status_timestamp_after"),
		IncludeArtifacts:     includeArtifacts,
	}, nil
}

func firstQueryParam(c echo.Context, names ...string) string {
	for _, name := range names {
		if value := strings.TrimSpace(c.QueryParam(name)); value != "" {
			return value
		}
	}
	return ""
}

func optionalBoolQuery(raw string) (*bool, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "y", "on":
		value := true
		return &value, nil
	case "0", "false", "no", "n", "off":
		value := false
		return &value, nil
	default:
		return nil, httpx.BadRequest("布尔查询参数必须是 true 或 false")
	}
}

func (h *Handler) streamProtocolTask(c echo.Context, userID uuid.UUID, slug, taskID string, requestID json.RawMessage, jsonRPC bool, initialTask *A2ATask, protocolVersion string) error {
	afterSequence, err := afterSequenceFromA2ASSE(c)
	if err != nil {
		return httpx.BadRequest("after_sequence / Last-Event-ID 不是合法整数")
	}
	res := c.Response()
	flusher, ok := res.Writer.(http.Flusher)
	if !ok {
		return httpx.Internal("当前响应不支持 streaming")
	}
	res.Header().Set(echo.HeaderContentType, "text/event-stream")
	res.Header().Set(echo.HeaderCacheControl, "no-cache")
	res.Header().Set(echo.HeaderConnection, "keep-alive")
	res.WriteHeader(http.StatusOK)

	if initialTask != nil {
		if err := writeA2ASSEPayload(res.Writer, 0, requestID, initialTask, jsonRPC, protocolVersion); err != nil {
			return nil
		}
		flusher.Flush()
	}

	ctx := c.Request().Context()
	pollTicker := time.NewTicker(a2aSSEPollInterval)
	defer pollTicker.Stop()
	heartbeatTicker := time.NewTicker(a2aSSEHeartbeat)
	defer heartbeatTicker.Stop()

	for {
		items, terminal, nextSequence, err := h.svc.ListProtocolTaskEvents(ctx, userID, slug, taskID, afterSequence)
		if err != nil {
			_ = writeA2ASSEError(res.Writer, requestID, err, jsonRPC)
			flusher.Flush()
			return nil
		}
		for _, item := range items {
			seq := sequenceFromStreamItem(item)
			if err := writeA2ASSEPayload(res.Writer, seq, requestID, item, jsonRPC, protocolVersion); err != nil {
				return nil
			}
			if seq > 0 {
				afterSequence = seq
			}
		}
		if nextSequence > afterSequence {
			afterSequence = nextSequence
		}
		flusher.Flush()
		if terminal {
			return nil
		}

		select {
		case <-ctx.Done():
			return nil
		case <-heartbeatTicker.C:
			if _, err := fmt.Fprint(res.Writer, ": heartbeat\n\n"); err != nil {
				return nil
			}
			flusher.Flush()
		case <-pollTicker.C:
		}
	}
}

func writeA2ASSEPayload(w http.ResponseWriter, id int32, requestID json.RawMessage, result interface{}, jsonRPC bool, protocolVersion string) error {
	payload := interface{}(streamResponseForResult(result))
	if jsonRPC {
		rpcResult := result
		if protocolVersion == a2aProtocolVersionCurrent {
			rpcResult = streamResponseForResult(result)
		}
		payload = jsonRPCResultWithVersion(requestID, rpcResult, protocolVersion)
	} else {
		payload = normalizeA2AResultForVersion(payload, protocolVersion)
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	eventName := a2aSSEEventName(result)
	if id > 0 {
		_, err = fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", id, eventName, raw)
		return err
	}
	_, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", eventName, raw)
	return err
}

func writeA2ASSEError(w http.ResponseWriter, requestID json.RawMessage, err error, jsonRPC bool) error {
	payload := interface{}(map[string]interface{}{"error": err.Error()})
	if jsonRPC {
		rpcErr := jsonRPCErrorFrom(requestID, err)
		payload = rpcErr
	}
	raw, marshalErr := json.Marshal(payload)
	if marshalErr != nil {
		return marshalErr
	}
	_, writeErr := fmt.Fprintf(w, "event: task.stream.error\ndata: %s\n\n", raw)
	return writeErr
}

func streamResponseForResult(result interface{}) A2AStreamResponse {
	switch v := result.(type) {
	case *A2ATask:
		return A2AStreamResponse{Task: v}
	case A2ATask:
		return A2AStreamResponse{Task: &v}
	case *A2AMessage:
		return A2AStreamResponse{Message: v}
	case A2AMessage:
		return A2AStreamResponse{Message: &v}
	case *A2ATaskStatusUpdateEvent:
		return A2AStreamResponse{StatusUpdate: v}
	case A2ATaskStatusUpdateEvent:
		return A2AStreamResponse{StatusUpdate: &v}
	case *A2ATaskArtifactUpdateEvent:
		return A2AStreamResponse{ArtifactUpdate: v}
	case A2ATaskArtifactUpdateEvent:
		return A2AStreamResponse{ArtifactUpdate: &v}
	default:
		return A2AStreamResponse{Message: &A2AMessage{
			Kind: "message",
			Role: "agent",
			Parts: []map[string]interface{}{{
				"kind": "data",
				"data": result,
			}},
		}}
	}
}

func a2aSSEEventName(result interface{}) string {
	switch result.(type) {
	case *A2ATask, A2ATask:
		return "task"
	case *A2ATaskArtifactUpdateEvent, A2ATaskArtifactUpdateEvent:
		return "artifact-update"
	case *A2ATaskStatusUpdateEvent, A2ATaskStatusUpdateEvent:
		return "status-update"
	case *A2AMessage, A2AMessage:
		return "message"
	default:
		return "message"
	}
}

func sequenceFromStreamItem(item interface{}) int32 {
	var metadata map[string]interface{}
	switch v := item.(type) {
	case *A2ATaskStatusUpdateEvent:
		metadata = v.Metadata
	case A2ATaskStatusUpdateEvent:
		metadata = v.Metadata
	case *A2ATaskArtifactUpdateEvent:
		metadata = v.Metadata
	case A2ATaskArtifactUpdateEvent:
		metadata = v.Metadata
	default:
		return 0
	}
	raw, ok := metadata["openlinker_sequence"]
	if !ok {
		return 0
	}
	switch value := raw.(type) {
	case int32:
		return value
	case int:
		return int32(value)
	case float64:
		return int32(value)
	default:
		return 0
	}
}

func afterSequenceFromA2ASSE(c echo.Context) (int32, error) {
	raw := c.QueryParam("after_sequence")
	if raw == "" {
		raw = c.Request().Header.Get("Last-Event-ID")
	}
	if raw == "" {
		return 0, nil
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil || parsed < 0 {
		return 0, errors.New("invalid after sequence")
	}
	return int32(parsed), nil
}

func taskIDFromSubscribeRequest(c echo.Context) (string, error) {
	return taskIDFromActionRequest(c, "subscribe")
}

func taskIDFromActionRequest(c echo.Context, action string) (string, error) {
	raw := c.Param("taskID")
	if raw == "" {
		raw = c.Param("*")
	}
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "tasks/")
	if action != "" {
		raw = strings.TrimSuffix(raw, ":"+action)
		raw = strings.TrimSuffix(raw, "/"+action)
	}
	if raw == "" {
		var body struct {
			Name string `json:"name"`
			ID   string `json:"id"`
		}
		_ = c.Bind(&body)
		raw = body.ID
		if raw == "" {
			raw = strings.TrimPrefix(body.Name, "tasks/")
		}
	}
	if raw == "" {
		return "", httpx.BadRequest("缺少 task id")
	}
	return raw, nil
}
