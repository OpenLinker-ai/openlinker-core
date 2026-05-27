package workflow

import (
	"net/http"

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
	return &Handler{
		svc:       svc,
		validator: validator.New(validator.WithRequiredStructEnabled()),
	}
}

// RegisterProtected mounts workflow APIs behind user auth.
//
//	POST /workflows              创建 workflow
//	GET  /workflows              查询自己的 workflow 列表
//	GET  /workflows/:id          查询 workflow 定义
//	POST /workflows/:id/run      同步执行 workflow
//	GET  /workflow-runs/:id      查询 workflow run
func (h *Handler) RegisterProtected(api *echo.Group, jwtMiddleware echo.MiddlewareFunc) {
	g := api.Group("/workflows", jwtMiddleware)
	g.POST("", h.Create)
	g.GET("", h.List)
	g.GET("/:id", h.Get)
	g.POST("/:id/run", h.Run)
	api.GET("/workflow-runs/:id", h.GetRun, jwtMiddleware)
}

func (h *Handler) Create(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	var req CreateWorkflowRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.CreateWorkflow(c.Request().Context(), uid, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusCreated, resp)
}

func (h *Handler) List(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	resp, err := h.svc.ListWorkflows(c.Request().Context(), uid, 20)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) Get(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathUUID(c)
	if err != nil {
		return err
	}
	resp, err := h.svc.GetWorkflow(c.Request().Context(), uid, id)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) Run(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathUUID(c)
	if err != nil {
		return err
	}
	var req RunWorkflowRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	resp, err := h.svc.RunWorkflow(c.Request().Context(), uid, id, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) GetRun(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathUUID(c)
	if err != nil {
		return err
	}
	resp, err := h.svc.GetWorkflowRun(c.Request().Context(), uid, id)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
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

func pathUUID(c echo.Context) (uuid.UUID, error) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return uuid.Nil, httpx.BadRequest("id 不是合法 uuid")
	}
	return id, nil
}
