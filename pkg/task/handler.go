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
//	POST /tasks/:id/publish     显式把私有推荐草稿发布到任务广场
//	POST /tasks/:id/claim       创作者用自己的 Agent 接入任务广场任务
//	POST /tasks/:id/run         从任务直接启动一次 Agent 运行
//	POST /tasks/:id/complete    把成功 run 写回任务结果
//	POST /tasks/:id/accept      任务发布者验收结果
//	POST /tasks/:id/revision    任务发布者要求修订
//	GET  /tasks/board           任务广场公开列表
//	GET  /tasks/me              我的任务历史（最多 20 条）
//	GET  /tasks/:id             单个任务详情（含推荐卡回填）
func (h *Handler) RegisterProtected(api *echo.Group, jwtMiddleware echo.MiddlewareFunc) {
	api.GET("/tasks/board", h.ListBoard)

	g := api.Group("/tasks", jwtMiddleware)
	g.POST("/recommend", h.Recommend)
	g.POST("/:id/choose", h.Choose)
	g.POST("/:id/publish", h.Publish)
	g.POST("/:id/claim", h.Claim)
	g.POST("/:id/run", h.Run)
	g.POST("/:id/complete", h.Complete)
	g.POST("/:id/accept", h.Accept)
	g.POST("/:id/revision", h.RequestRevision)
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
	resp, err := h.svc.Recommend(c.Request().Context(), uid, &req)
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

// Publish POST /tasks/:id/publish
func (h *Handler) Publish(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	taskID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	var req PublishRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.Publish(c.Request().Context(), taskID, uid, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// Claim POST /tasks/:id/claim
func (h *Handler) Claim(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	taskID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	var req ClaimRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.Claim(c.Request().Context(), taskID, uid, req.AgentID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// Complete POST /tasks/:id/complete
func (h *Handler) Complete(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	taskID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	var req CompleteRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.Complete(c.Request().Context(), taskID, uid, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// Run POST /tasks/:id/run
func (h *Handler) Run(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	taskID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	var req RunTaskRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.RunTask(c.Request().Context(), taskID, uid, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusAccepted, resp)
}

// Accept POST /tasks/:id/accept
func (h *Handler) Accept(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	taskID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	resp, err := h.svc.AcceptDelivery(c.Request().Context(), taskID, uid)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// RequestRevision POST /tasks/:id/revision
func (h *Handler) RequestRevision(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	taskID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	var req RevisionRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.RequestRevision(c.Request().Context(), taskID, uid, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
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

// ListBoard GET /tasks/board?limit=20
func (h *Handler) ListBoard(c echo.Context) error {
	limit := int32(20)
	if v := c.QueryParam("limit"); v != "" {
		if n, perr := strconv.Atoi(v); perr == nil && n > 0 {
			if n > 50 {
				n = 50
			}
			limit = int32(n)
		}
	}
	items, err := h.svc.ListBoard(c.Request().Context(), limit)
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
