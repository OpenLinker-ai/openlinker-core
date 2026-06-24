package webhook

import (
	"context"
	"net/http"
	"strconv"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

// Handler webhook HTTP 入口（创作者侧）。
type Handler struct {
	svc       webhookService
	validator *validator.Validate
	cfg       *config.Config
}

type webhookService interface {
	CreateTaskCallbackSubscription(context.Context, uuid.UUID, uuid.UUID, *CreateTaskCallbackRequest) (*TaskCallbackSubscriptionResponse, error)
	ListTaskCallbackSubscriptions(context.Context, uuid.UUID, uuid.UUID) ([]TaskCallbackSubscriptionResponse, error)
	ListTaskCallbackSubscriptionsForOwner(context.Context, uuid.UUID, string, int) ([]TaskCallbackSubscriptionResponse, error)
	BatchManageTaskCallbackSubscriptions(context.Context, uuid.UUID, *BatchTaskCallbackSubscriptionsRequest) (*BatchTaskCallbackSubscriptionsResponse, error)
	DeleteTaskCallbackSubscription(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) error
	UpdateTaskCallbackSubscriptionStatus(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string) (*TaskCallbackSubscriptionResponse, error)
}

// NewHandler 构造 Handler。cfg 可选（保持与其它模块一致）。
func NewHandler(svc webhookService, cfg ...*config.Config) *Handler {
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
//	POST   /runs/:id/task-callbacks                      为单个 run 注册任务回调
//	GET    /runs/:id/task-callbacks                      查看 run 任务回调
//	POST   /runs/:id/task-callbacks/:callbackID/pause     暂停 run 任务回调
//	POST   /runs/:id/task-callbacks/:callbackID/resume    恢复 run 任务回调
//	DELETE /runs/:id/task-callbacks/:callbackID           删除 run 任务回调
//	GET    /task-callbacks                                汇总当前用户的任务回调
//	POST   /task-callbacks/batch                          批量 pause / resume / delete
func (h *Handler) RegisterProtected(api *echo.Group, jwtMiddleware echo.MiddlewareFunc) {
	taskCallbacks := api.Group("/runs/:id/task-callbacks", jwtMiddleware)
	taskCallbacks.POST("", h.CreateTaskCallback)
	taskCallbacks.GET("", h.ListTaskCallbacks)
	taskCallbacks.POST("/:callbackID/pause", h.PauseTaskCallback)
	taskCallbacks.POST("/:callbackID/resume", h.ResumeTaskCallback)
	taskCallbacks.DELETE("/:callbackID", h.DeleteTaskCallback)

	callbackManager := api.Group("/task-callbacks", jwtMiddleware)
	callbackManager.GET("", h.ListManagedTaskCallbacks)
	callbackManager.POST("/batch", h.BatchManageTaskCallbacks)
}

// CreateTaskCallback 为单个 run 注册调用方任务回调。secret 仅本次返回。
func (h *Handler) CreateTaskCallback(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	runID, err := pathID(c)
	if err != nil {
		return err
	}
	var req CreateTaskCallbackRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.CreateTaskCallbackSubscription(c.Request().Context(), runID, uid, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusCreated, resp)
}

func (h *Handler) ListTaskCallbacks(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	runID, err := pathID(c)
	if err != nil {
		return err
	}
	items, err := h.svc.ListTaskCallbackSubscriptions(c.Request().Context(), runID, uid)
	if err != nil {
		return err
	}
	if items == nil {
		items = []TaskCallbackSubscriptionResponse{}
	}
	return c.JSON(http.StatusOK, map[string]any{"items": items})
}

func (h *Handler) ListManagedTaskCallbacks(c echo.Context) error {
	uid, err := userIDFromCtx(c)
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
	items, err := h.svc.ListTaskCallbackSubscriptionsForOwner(c.Request().Context(), uid, c.QueryParam("status"), limit)
	if err != nil {
		return err
	}
	if items == nil {
		items = []TaskCallbackSubscriptionResponse{}
	}
	return c.JSON(http.StatusOK, TaskCallbackSubscriptionListResponse{Items: items})
}

func (h *Handler) BatchManageTaskCallbacks(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	var req BatchTaskCallbackSubscriptionsRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.BatchManageTaskCallbackSubscriptions(c.Request().Context(), uid, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) DeleteTaskCallback(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	runID, err := pathID(c)
	if err != nil {
		return err
	}
	callbackID, err := taskCallbackIDFromParam(c)
	if err != nil {
		return err
	}
	if err := h.svc.DeleteTaskCallbackSubscription(c.Request().Context(), runID, callbackID, uid); err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]string{"status": "deleted"})
}

func (h *Handler) PauseTaskCallback(c echo.Context) error {
	return h.setTaskCallbackStatus(c, "paused")
}

func (h *Handler) ResumeTaskCallback(c echo.Context) error {
	return h.setTaskCallbackStatus(c, "active")
}

func (h *Handler) setTaskCallbackStatus(c echo.Context, status string) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	runID, err := pathID(c)
	if err != nil {
		return err
	}
	callbackID, err := taskCallbackIDFromParam(c)
	if err != nil {
		return err
	}
	resp, err := h.svc.UpdateTaskCallbackSubscriptionStatus(c.Request().Context(), runID, callbackID, uid, status)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
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

func taskCallbackIDFromParam(c echo.Context) (uuid.UUID, error) {
	raw := c.Param("callbackID")
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, httpx.BadRequest("callbackID 不是合法 uuid")
	}
	return id, nil
}
