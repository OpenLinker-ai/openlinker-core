package externalexecution

import (
	"bytes"
	"context"
	"io"
	"mime"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

const maximumExternalExecutionRequestBodyBytes = 8 << 20

type executionService interface {
	ValidateTarget(context.Context, *Principal, *TargetValidationRequest) (*TargetValidationResponse, error)
	StartExecution(context.Context, *Principal, *ExecutionRequest) (*ExecutionStartResponse, error)
	GetExecution(context.Context, *Principal, string) (*ExecutionStatusResponse, error)
	CancelExecution(context.Context, *Principal, string, *ExecutionCancelRequest) (*ExecutionStatusResponse, error)
}

type Handler struct {
	svc        executionService
	authorizer *Authorizer
}

func NewHandler(svc executionService, authorizer *Authorizer) *Handler {
	return &Handler{svc: svc, authorizer: authorizer}
}

func (h *Handler) Register(e *echo.Echo) {
	e.POST("/internal/external-execution-targets/validate", h.ValidateTarget)
	e.POST("/internal/external-executions", h.StartExecution)
	e.GET("/internal/external-executions/:external_request_id", h.GetExecution)
	e.POST("/internal/external-executions/:external_request_id/cancel", h.CancelExecution)
}

func (h *Handler) CancelExecution(c echo.Context) error {
	principal, err := h.authorizeRequest(c, ScopeCancelExecution)
	if err != nil {
		return err
	}
	var req ExecutionCancelRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	resp, err := h.svc.CancelExecution(
		c.Request().Context(), principal, c.Param("external_request_id"), &req,
	)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) ValidateTarget(c echo.Context) error {
	principal, err := h.authorizeRequest(c, ScopeValidateTarget)
	if err != nil {
		return err
	}
	var req TargetValidationRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	resp, err := h.svc.ValidateTarget(c.Request().Context(), principal, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) StartExecution(c echo.Context) error {
	principal, err := h.authorizeRequest(c, ScopeStartExecution)
	if err != nil {
		return err
	}
	var req ExecutionRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	resp, err := h.svc.StartExecution(c.Request().Context(), principal, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) GetExecution(c echo.Context) error {
	principal, err := h.authorizeRequest(c, ScopeReadExecution)
	if err != nil {
		return err
	}
	resp, err := h.svc.GetExecution(c.Request().Context(), principal, c.Param("external_request_id"))
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) authorizeRequest(c echo.Context, scope string) (*Principal, error) {
	verified, err := h.authenticate(c, scope)
	if err != nil {
		return nil, err
	}
	body, err := readAndRestoreExternalExecutionBody(c.Request())
	if err != nil {
		return nil, err
	}
	if err := h.authorizer.validateRequestBinding(verified, c.Request(), body); err != nil {
		return nil, err
	}
	if err := validateExternalExecutionHTTPEnvelope(c.Request(), body); err != nil {
		return nil, err
	}
	return h.authorizer.consume(c.Request().Context(), verified)
}

func (h *Handler) authenticate(c echo.Context, scope string) (*verifiedServiceToken, error) {
	if h == nil || h.authorizer == nil {
		return nil, httpx.ServiceUnavailable("外部执行认证未配置")
	}
	authorization := strings.TrimSpace(c.Request().Header.Get(echo.HeaderAuthorization))
	scheme, rawToken, ok := strings.Cut(authorization, " ")
	if !ok || !strings.EqualFold(strings.TrimSpace(scheme), "Bearer") || strings.TrimSpace(rawToken) == "" {
		return nil, httpx.Unauthorized("缺少外部执行服务凭据")
	}
	return h.authorizer.verify(strings.TrimSpace(rawToken), scope)
}

func readAndRestoreExternalExecutionBody(request *http.Request) ([]byte, error) {
	if request == nil {
		return nil, httpx.BadRequest("请求体格式错误")
	}
	if request.Body == nil {
		request.Body = http.NoBody
		return []byte{}, nil
	}
	original := request.Body
	body, err := io.ReadAll(io.LimitReader(original, maximumExternalExecutionRequestBodyBytes+1))
	_ = original.Close()
	if err != nil {
		return nil, httpx.BadRequest("请求体读取失败")
	}
	if len(body) > maximumExternalExecutionRequestBodyBytes {
		return nil, httpx.NewError(http.StatusRequestEntityTooLarge, httpx.CodeBadRequest, "请求体过大")
	}
	request.Body = io.NopCloser(bytes.NewReader(body))
	request.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(body)), nil
	}
	return body, nil
}

func validateExternalExecutionHTTPEnvelope(request *http.Request, body []byte) error {
	if request == nil || request.URL == nil {
		return httpx.BadRequest("请求格式错误")
	}
	if request.URL.RawQuery != "" {
		return httpx.BadRequest("External Execution 请求不支持 query 参数")
	}
	switch strings.ToUpper(request.Method) {
	case http.MethodGet:
		if len(body) != 0 {
			return httpx.BadRequest("GET 请求体必须为空")
		}
	case http.MethodPost:
		mediaType, _, err := mime.ParseMediaType(request.Header.Get(echo.HeaderContentType))
		if err != nil || !strings.EqualFold(mediaType, echo.MIMEApplicationJSON) {
			return httpx.NewError(http.StatusUnsupportedMediaType, httpx.CodeBadRequest, "Content-Type 必须是 application/json")
		}
	default:
		return httpx.NewError(http.StatusMethodNotAllowed, httpx.CodeBadRequest, "请求方法不支持")
	}
	return nil
}
