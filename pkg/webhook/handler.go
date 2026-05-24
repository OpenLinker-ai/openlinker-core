package webhook

import (
	"net/http"
	"strconv"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/kinzhi/openlinker-core/pkg/config"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

// Handler webhook HTTP 入口（创作者侧）。
type Handler struct {
	svc       *Service
	validator *validator.Validate
	cfg       *config.Config
}

// NewHandler 构造 Handler。cfg 可选（保持与其它模块一致）。
func NewHandler(svc *Service, cfg ...*config.Config) *Handler {
	h := &Handler{
		svc:       svc,
		validator: validator.New(validator.WithRequiredStructEnabled()),
	}
	if len(cfg) > 0 {
		h.cfg = cfg[0]
	}
	return h
}

// RegisterProtected 注册创作者侧端点（需 JWT）。
//
//	POST   /creator/agents/:id/webhook            设置（生成新 secret）
//	DELETE /creator/agents/:id/webhook            清除
//	POST   /creator/agents/:id/webhook/rotate     重新生成 secret
//	GET    /creator/agents/:id/webhook/deliveries 投递历史
func (h *Handler) RegisterProtected(api *echo.Group, jwtMiddleware echo.MiddlewareFunc) {
	g := api.Group("/creator/agents/:id/webhook", jwtMiddleware)
	g.POST("", h.Set)
	g.DELETE("", h.Clear)
	g.POST("/rotate", h.Rotate)
	g.GET("/deliveries", h.ListDeliveries)
}

// Set 设置 webhook（生成新 secret，仅本次返回）。
func (h *Handler) Set(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathID(c)
	if err != nil {
		return err
	}
	var req SetWebhookRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.SetWebhook(c.Request().Context(), id, uid, req.URL)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// Clear 清除 webhook。
func (h *Handler) Clear(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathID(c)
	if err != nil {
		return err
	}
	if err := h.svc.ClearWebhook(c.Request().Context(), id, uid); err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]string{"status": "cleared"})
}

// Rotate 重新生成 secret（保留 url）。
func (h *Handler) Rotate(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathID(c)
	if err != nil {
		return err
	}
	resp, err := h.svc.RotateSecret(c.Request().Context(), id, uid)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// ListDeliveries 查询投递历史。
//
// query: ?limit=20（默认 20，最大 100）
func (h *Handler) ListDeliveries(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathID(c)
	if err != nil {
		return err
	}
	limit := 0
	if v := c.QueryParam("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return httpx.BadRequest("limit 必须是整数")
		}
		limit = n
	}
	items, err := h.svc.ListDeliveries(c.Request().Context(), id, uid, limit)
	if err != nil {
		return err
	}
	if items == nil {
		items = []DeliveryListItem{}
	}
	return c.JSON(http.StatusOK, map[string]any{"items": items})
}

// userIDFromCtx 从 echo.Context 取出当前登录用户 uuid。
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

// pathID 解析 :id 路径参数。
func pathID(c echo.Context) (uuid.UUID, error) {
	raw := c.Param("id")
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, httpx.BadRequest("id 不是合法 uuid")
	}
	return id, nil
}
