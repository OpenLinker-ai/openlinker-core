package servicebridge

import (
	"context"
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/usertoken"
)

type bridgeService interface {
	ValidateTarget(context.Context, *TargetValidationRequest) (*TargetValidationResponse, error)
	StartExecution(context.Context, *ExecutionRequest) (*ExecutionStartResponse, error)
	GetExecution(context.Context, string) (*ExecutionStatusResponse, error)
}

type Handler struct {
	svc            bridgeService
	internalSecret string
}

func NewHandler(svc bridgeService, internalSecret string) *Handler {
	return &Handler{svc: svc, internalSecret: strings.TrimSpace(internalSecret)}
}

func (h *Handler) Register(e *echo.Echo) {
	e.POST("/internal/hosted/service-targets/validate", h.ValidateTarget)
	e.POST("/internal/hosted/service-executions", h.StartExecution)
	e.GET("/internal/hosted/service-executions/:external_order_id", h.GetExecution)
}

func (h *Handler) ValidateTarget(c echo.Context) error {
	if err := h.authorize(c); err != nil {
		return err
	}
	var req TargetValidationRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	resp, err := h.svc.ValidateTarget(c.Request().Context(), &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) StartExecution(c echo.Context) error {
	if err := h.authorize(c); err != nil {
		return err
	}
	var req ExecutionRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	resp, err := h.svc.StartExecution(c.Request().Context(), &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) GetExecution(c echo.Context) error {
	if err := h.authorize(c); err != nil {
		return err
	}
	resp, err := h.svc.GetExecution(c.Request().Context(), c.Param("external_order_id"))
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) authorize(c echo.Context) error {
	provided := strings.TrimSpace(c.Request().Header.Get(usertoken.InternalTokenHeader))
	if h.internalSecret == "" || len(provided) != len(h.internalSecret) ||
		subtle.ConstantTimeCompare([]byte(provided), []byte(h.internalSecret)) != 1 {
		return httpx.Unauthorized("内部服务凭据无效")
	}
	return nil
}
