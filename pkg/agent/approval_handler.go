package agent

import (
	"net/http"

	"github.com/go-playground/validator/v10"
	"github.com/labstack/echo/v4"

	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

// ApprovalHandler 高风险动作审批 HTTP 入口。
type ApprovalHandler struct {
	svc       *ApprovalService
	validator *validator.Validate
}

func NewApprovalHandler(svc *ApprovalService) *ApprovalHandler {
	return &ApprovalHandler{
		svc:       svc,
		validator: validator.New(validator.WithRequiredStructEnabled()),
	}
}

// RegisterProtected creator 路径（需 JWT）。
//
//	POST   /api/v1/creator/approvals
//	GET    /api/v1/creator/approvals
//	GET    /api/v1/creator/approvals/:id
//	POST   /api/v1/creator/approvals/:id/confirm
//	POST   /api/v1/creator/approvals/:id/reject
func (h *ApprovalHandler) RegisterProtected(api *echo.Group, jwtMiddleware echo.MiddlewareFunc) {
	g := api.Group("/creator/approvals", jwtMiddleware)
	g.POST("", h.CreateApproval)
	g.GET("", h.ListApprovals)
	g.GET("/:id", h.GetApproval)
	g.POST("/:id/confirm", h.ConfirmApproval)
	g.POST("/:id/reject", h.RejectApproval)
}

func (h *ApprovalHandler) CreateApproval(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	var req CreateApprovalRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.CreateApproval(c.Request().Context(), uid, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusCreated, resp)
}

func (h *ApprovalHandler) ListApprovals(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	items, err := h.svc.ListApprovals(c.Request().Context(), uid)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]any{"items": items})
}

func (h *ApprovalHandler) GetApproval(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathID(c)
	if err != nil {
		return err
	}
	resp, err := h.svc.GetApproval(c.Request().Context(), uid, id)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *ApprovalHandler) ConfirmApproval(c echo.Context) error {
	return h.decide(c, true)
}

func (h *ApprovalHandler) RejectApproval(c echo.Context) error {
	return h.decide(c, false)
}

func (h *ApprovalHandler) decide(c echo.Context, confirm bool) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathID(c)
	if err != nil {
		return err
	}
	var req ApprovalDecisionRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	ctx := c.Request().Context()
	if confirm {
		if err := h.svc.ConfirmApproval(ctx, uid, id, req.Note); err != nil {
			return err
		}
		return c.JSON(http.StatusOK, map[string]string{"status": "confirmed"})
	}
	if err := h.svc.RejectApproval(ctx, uid, id, req.Note); err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]string{"status": "rejected"})
}
