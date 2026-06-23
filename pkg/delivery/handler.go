package delivery

import (
	"context"
	"net/http"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

// Handler Output Delivery HTTP 路由（用户侧）。
type Handler struct {
	svc       deliveryService
	validator *validator.Validate
}

type deliveryService interface {
	CreateTarget(context.Context, uuid.UUID, *CreateTargetRequest) (*TargetResponse, error)
	ListTargets(context.Context, uuid.UUID) ([]TargetResponse, error)
	DeleteTarget(context.Context, uuid.UUID, uuid.UUID) error
	SetDefault(context.Context, uuid.UUID, uuid.UUID) error
	DeliverRun(context.Context, uuid.UUID, uuid.UUID, *uuid.UUID) (*DeliveryItem, error)
	ListByRun(context.Context, uuid.UUID, uuid.UUID) ([]DeliveryItem, error)
	RetryDelivery(context.Context, uuid.UUID, uuid.UUID) error
}

func NewHandler(svc deliveryService) *Handler {
	return &Handler{
		svc:       svc,
		validator: validator.New(validator.WithRequiredStructEnabled()),
	}
}

// RegisterProtected 注册需要 JWT 的用户路由。
//
//	POST   /delivery-targets                       创建
//	GET    /delivery-targets                       列表
//	DELETE /delivery-targets/:id                   删除
//	POST   /delivery-targets/:id/default           设为默认
//	POST   /runs/:id/deliver                       触发投递
//	GET    /runs/:id/deliveries                    投递历史
//	POST   /deliveries/:id/retry                   重试 failed
func (h *Handler) RegisterProtected(api *echo.Group, jwtMiddleware echo.MiddlewareFunc) {
	g := api.Group("", jwtMiddleware)
	g.POST("/delivery-targets", h.CreateTarget)
	g.GET("/delivery-targets", h.ListTargets)
	g.DELETE("/delivery-targets/:id", h.DeleteTarget)
	g.POST("/delivery-targets/:id/default", h.SetDefault)
	g.POST("/runs/:id/deliver", h.DeliverRun)
	g.GET("/runs/:id/deliveries", h.ListDeliveries)
	g.POST("/deliveries/:id/retry", h.RetryDelivery)
}

func (h *Handler) CreateTarget(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	var req CreateTargetRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.CreateTarget(c.Request().Context(), uid, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusCreated, resp)
}

func (h *Handler) ListTargets(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	items, err := h.svc.ListTargets(c.Request().Context(), uid)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]any{"items": items})
}

func (h *Handler) DeleteTarget(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathID(c)
	if err != nil {
		return err
	}
	if err := h.svc.DeleteTarget(c.Request().Context(), id, uid); err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]string{"status": "deleted"})
}

func (h *Handler) SetDefault(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathID(c)
	if err != nil {
		return err
	}
	if err := h.svc.SetDefault(c.Request().Context(), id, uid); err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]string{"status": "default_set"})
}

func (h *Handler) DeliverRun(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	runID, err := pathID(c)
	if err != nil {
		return err
	}
	var req DeliverRequest
	_ = c.Bind(&req) // body 可选
	var targetIDPtr *uuid.UUID
	if req.TargetID != "" {
		tid, err := uuid.Parse(req.TargetID)
		if err != nil {
			return httpx.BadRequest("target_id 不是合法 uuid")
		}
		targetIDPtr = &tid
	}
	item, err := h.svc.DeliverRun(c.Request().Context(), uid, runID, targetIDPtr)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusAccepted, item)
}

func (h *Handler) ListDeliveries(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	runID, err := pathID(c)
	if err != nil {
		return err
	}
	items, err := h.svc.ListByRun(c.Request().Context(), runID, uid)
	if err != nil {
		return err
	}
	if items == nil {
		items = []DeliveryItem{}
	}
	return c.JSON(http.StatusOK, map[string]any{"items": items})
}

func (h *Handler) RetryDelivery(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathID(c)
	if err != nil {
		return err
	}
	if err := h.svc.RetryDelivery(c.Request().Context(), id, uid); err != nil {
		return err
	}
	return c.JSON(http.StatusAccepted, map[string]string{"status": "retry_enqueued"})
}

func userIDFromCtx(c echo.Context) (uuid.UUID, error) {
	idStr := httpx.UserIDFrom(c)
	if idStr == "" {
		return uuid.Nil, httpx.Unauthorized("")
	}
	uid, err := uuid.Parse(idStr)
	if err != nil {
		return uuid.Nil, httpx.Unauthorized("token 无效")
	}
	return uid, nil
}

func pathID(c echo.Context) (uuid.UUID, error) {
	raw := c.Param("id")
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, httpx.BadRequest("id 不是合法 uuid")
	}
	return id, nil
}
