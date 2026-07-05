package delivery

import (
	"context"
	"net/http"
	"strconv"
	"strings"

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
	UpdateTarget(context.Context, uuid.UUID, uuid.UUID, *UpdateTargetRequest) (*TargetResponse, error)
	DeleteTarget(context.Context, uuid.UUID, uuid.UUID) error
	SetDefault(context.Context, uuid.UUID, uuid.UUID) error
	DeliverRun(context.Context, uuid.UUID, uuid.UUID, *uuid.UUID) (*DeliveryItem, error)
	ListByRun(context.Context, uuid.UUID, uuid.UUID) ([]DeliveryItem, error)
	List(context.Context, uuid.UUID, DeliveryListFilter) ([]DeliveryItem, error)
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
//	PATCH  /delivery-targets/:id                   更新通知类型
//	DELETE /delivery-targets/:id                   删除
//	POST   /delivery-targets/:id/default           设为默认
//	POST   /runs/:id/deliver                       触发投递
//	GET    /runs/:id/deliveries                    投递历史
//	GET    /deliveries                             外部投递历史列表
//	POST   /deliveries/:id/retry                   重试 failed
func (h *Handler) RegisterProtected(api *echo.Group, jwtMiddleware echo.MiddlewareFunc) {
	g := api.Group("", jwtMiddleware)
	g.POST("/delivery-targets", h.CreateTarget)
	g.GET("/delivery-targets", h.ListTargets)
	g.PATCH("/delivery-targets/:id", h.UpdateTarget)
	g.DELETE("/delivery-targets/:id", h.DeleteTarget)
	g.POST("/delivery-targets/:id/default", h.SetDefault)
	g.POST("/runs/:id/deliver", h.DeliverRun)
	g.GET("/runs/:id/deliveries", h.ListDeliveries)
	g.GET("/deliveries", h.ListAllDeliveries)
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

func (h *Handler) UpdateTarget(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathID(c)
	if err != nil {
		return err
	}
	var req UpdateTargetRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.UpdateTarget(c.Request().Context(), id, uid, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
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

func (h *Handler) ListAllDeliveries(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	filter, err := deliveryListFilterFromQuery(c)
	if err != nil {
		return err
	}
	items, err := h.svc.List(c.Request().Context(), uid, filter)
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

func deliveryListFilterFromQuery(c echo.Context) (DeliveryListFilter, error) {
	limit := int32(defaultDeliveryHistoryLimit)
	if raw := strings.TrimSpace(c.QueryParam("limit")); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 32)
		if err != nil || n < 1 {
			return DeliveryListFilter{}, httpx.BadRequest("limit 必须是正整数")
		}
		if n > maxDeliveryHistoryLimit {
			n = maxDeliveryHistoryLimit
		}
		// #nosec G115 -- strconv.ParseInt with bitSize=32 guarantees int32 range, then limit is capped.
		limit = int32(n)
	}

	var agentID *string
	if raw := strings.TrimSpace(c.QueryParam("agent_id")); raw != "" {
		if _, err := uuid.Parse(raw); err != nil {
			return DeliveryListFilter{}, httpx.BadRequest("agent_id 不是合法 uuid")
		}
		agentID = &raw
	}

	var runID *string
	if raw := strings.TrimSpace(c.QueryParam("run_id")); raw != "" {
		if _, err := uuid.Parse(raw); err != nil {
			return DeliveryListFilter{}, httpx.BadRequest("run_id 不是合法 uuid")
		}
		runID = &raw
	}

	status := strings.TrimSpace(c.QueryParam("status"))
	if status != "" && status != "pending" && status != "success" && status != "failed" {
		return DeliveryListFilter{}, httpx.BadRequest("status 只能是 pending、success 或 failed")
	}

	return DeliveryListFilter{
		AgentID: agentID,
		RunID:   runID,
		Status:  status,
		Limit:   limit,
	}, nil
}
