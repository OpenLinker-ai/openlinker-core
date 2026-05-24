package task

import (
	"net/http"
	"strconv"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

// Handler 任务驱动 A 形态 HTTP 入口。
type Handler struct {
	svc       *Service
	validator *validator.Validate
}

// NewHandler 构造 Handler。
func NewHandler(svc *Service) *Handler {
	return &Handler{
		svc:       svc,
		validator: validator.New(validator.WithRequiredStructEnabled()),
	}
}

// RegisterProtected 注册需要 JWT 的端点。
//
//	POST /tasks/recommend       自然语言 → 推荐 Top3 Agent
//	POST /tasks/:id/choose      用户选定推荐里某个 Agent
//	GET  /tasks/me              我的任务历史（最多 20 条）
//	GET  /tasks/:id             单个任务详情（含推荐卡回填）
func (h *Handler) RegisterProtected(api *echo.Group, jwtMiddleware echo.MiddlewareFunc) {
	g := api.Group("/tasks", jwtMiddleware)
	g.POST("/recommend", h.Recommend)
	g.POST("/:id/choose", h.Choose)
	g.GET("/me", h.ListMine)
	g.GET("/:id", h.GetByID)
}

// Recommend POST /tasks/recommend
func (h *Handler) Recommend(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	var req RecommendRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.Recommend(c.Request().Context(), uid, req.Query)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// Choose POST /tasks/:id/choose
func (h *Handler) Choose(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	taskID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	var req ChooseRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	if err := h.svc.Choose(c.Request().Context(), taskID, uid, req.AgentID); err != nil {
		return err
	}
	return c.NoContent(http.StatusNoContent)
}

// ListMine GET /tasks/me?limit=20
func (h *Handler) ListMine(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	limit := int32(20)
	if v := c.QueryParam("limit"); v != "" {
		if n, perr := strconv.Atoi(v); perr == nil && n > 0 {
			if n > 20 {
				n = 20
			}
			limit = int32(n)
		}
	}
	items, err := h.svc.ListMine(c.Request().Context(), uid, limit)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]any{"items": items})
}

// GetByID GET /tasks/:id
func (h *Handler) GetByID(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	taskID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	resp, err := h.svc.GetByID(c.Request().Context(), taskID, uid)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// userIDFromCtx 从 echo.Context 取当前登录 user uuid（JWT 中间件已写入）。
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
