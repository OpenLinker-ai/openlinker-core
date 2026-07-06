package agent

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

// Handler Agent 注册 / 公开状态 HTTP 入口。
type Handler struct {
	svc       agentService
	validator *validator.Validate
	cfg       *config.Config
}

type agentService interface {
	CheckSlug(context.Context, string) (*SlugCheckResponse, error)
	BecomeCreator(context.Context, uuid.UUID) error
	CreateAgent(context.Context, uuid.UUID, *CreateAgentRequest) (*AgentResponse, error)
	ListMyAgents(context.Context, uuid.UUID) ([]AgentResponse, error)
	ListMyAgentsPage(context.Context, uuid.UUID, AgentListOptions) (*AgentListResponse, error)
	GetMyAgent(context.Context, uuid.UUID, uuid.UUID) (*AgentResponse, error)
	UpdateAgent(context.Context, uuid.UUID, uuid.UUID, *UpdateAgentRequest) (*AgentResponse, error)
	SetVisibility(context.Context, uuid.UUID, uuid.UUID, string) (*AgentResponse, error)
	DisableAgent(context.Context, uuid.UUID, uuid.UUID) error
	GetAgentOnboarding(context.Context, uuid.UUID, uuid.UUID) (*OnboardingResponse, error)
	UpsertCapability(context.Context, uuid.UUID, uuid.UUID, *UpsertCapabilityRequest) (*CapabilityResponse, error)
	CreateExample(context.Context, uuid.UUID, uuid.UUID, *CreateExampleRequest) (*ExampleResponse, error)
	DeleteExample(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) error
	RunDryRun(context.Context, uuid.UUID, uuid.UUID) (*DryRunResponse, error)
	ListAvailabilityAlerts(context.Context, uuid.UUID, int32) (*AvailabilityAlertListResponse, error)
	MarkAvailabilityAlertRead(context.Context, uuid.UUID, uuid.UUID) (*AvailabilityAlertResponse, error)
	ListPendingForAdmin(context.Context) ([]AgentResponse, error)
	RequestCertification(context.Context, uuid.UUID, uuid.UUID) error
	CertifyAgent(context.Context, uuid.UUID) error
	RejectCertification(context.Context, uuid.UUID, string) error
}

// NewHandler 构造 Handler。cfg 可选（测试可省略）。
func NewHandler(svc agentService, cfg ...*config.Config) *Handler {
	h := &Handler{
		svc:       svc,
		validator: validator.New(validator.WithRequiredStructEnabled()),
	}
	if len(cfg) > 0 {
		h.cfg = cfg[0]
	}
	return h
}

// Register 公开端点（无需 JWT）。
//
//	GET /agents/check-slug?slug=xxx   实时校验
func (h *Handler) Register(api *echo.Group) {
	api.GET("/agents/check-slug", h.CheckSlug)
}

// RegisterProtected 创作者侧端点（需 JWT）。
//
//	POST   /me/become-creator
//	POST   /creator/agents
//	GET    /creator/agents
//	GET    /creator/agents/:id
//	PATCH  /creator/agents/:id
//	PATCH  /creator/agents/:id/visibility
//	DELETE /creator/agents/:id
//	GET    /creator/agents/:id/onboarding
//	PUT    /creator/agents/:id/capabilities
//	POST   /creator/agents/:id/examples
//	DELETE /creator/agents/:id/examples/:exampleID
//	POST   /creator/agents/:id/health-check
//	GET    /creator/availability-alerts
//	POST   /creator/availability-alerts/:alertID/read
func (h *Handler) RegisterProtected(api *echo.Group, jwtMiddleware echo.MiddlewareFunc) {
	me := api.Group("/me", jwtMiddleware)
	me.POST("/become-creator", h.BecomeCreator)

	creator := api.Group("/creator", jwtMiddleware)
	creator.POST("/agents", h.CreateAgent)
	creator.GET("/agents", h.ListMyAgents)
	creator.GET("/agents/:id", h.GetMyAgent)
	creator.PATCH("/agents/:id", h.UpdateAgent)
	creator.PATCH("/agents/:id/visibility", h.UpdateVisibility)
	creator.DELETE("/agents/:id", h.DisableAgent)

	creator.GET("/agents/:id/onboarding", h.GetAgentOnboarding)
	creator.PUT("/agents/:id/capabilities", h.UpsertCapability)
	creator.POST("/agents/:id/examples", h.CreateExample)
	creator.DELETE("/agents/:id/examples/:exampleID", h.DeleteExample)
	creator.POST("/agents/:id/dry-run", h.RunDryRun)
	creator.POST("/agents/:id/health-check", h.RunHealthCheck)
	creator.POST("/agents/:id/request-certification", h.RequestCertification)
	creator.GET("/availability-alerts", h.ListAvailabilityAlerts)
	creator.POST("/availability-alerts/:alertID/read", h.MarkAvailabilityAlertRead)
}

// RegisterAdmin 管理员/运营人工处理端点（需 JWT + admin 双重中间件）。
//
//	GET  /admin/agents/pending          # 待审认证申请队列
//	POST /admin/agents/:id/certify
//	POST /admin/agents/:id/reject-certification
func (h *Handler) RegisterAdmin(api *echo.Group, jwtMiddleware, adminMiddleware echo.MiddlewareFunc) {
	g := api.Group("/admin", jwtMiddleware, adminMiddleware)
	g.GET("/agents/pending", h.ListPendingAgents)
	g.POST("/agents/:id/certify", h.CertifyAgent)
	g.POST("/agents/:id/reject-certification", h.RejectCertification)
}

// CheckSlug 实时校验 slug 可用性（公开）。
func (h *Handler) CheckSlug(c echo.Context) error {
	slug := c.QueryParam("slug")
	if slug == "" {
		return httpx.BadRequest("slug 参数不能为空")
	}
	resp, err := h.svc.CheckSlug(c.Request().Context(), slug)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// BecomeCreator 当前用户成为创作者（一键）。
func (h *Handler) BecomeCreator(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	if err := h.svc.BecomeCreator(c.Request().Context(), uid); err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]any{"is_creator": true})
}

// CreateAgent 创作者新建 Agent。
func (h *Handler) CreateAgent(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	var req CreateAgentRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.CreateAgent(c.Request().Context(), uid, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusCreated, resp)
}

// ListMyAgents 创作者中心列表。
func (h *Handler) ListMyAgents(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	if hasAgentListQuery(c) {
		resp, err := h.svc.ListMyAgentsPage(c.Request().Context(), uid, AgentListOptions{
			Query:               c.QueryParam("q"),
			Status:              c.QueryParam("status"),
			Visibility:          c.QueryParam("visibility"),
			CertificationStatus: c.QueryParam("certification_status"),
			SortBy:              c.QueryParam("sort_by"),
			Limit:               parseInt32Query(c, "limit", 25),
			Offset:              parseInt32Query(c, "offset", 0),
		})
		if err != nil {
			return err
		}
		return c.JSON(http.StatusOK, resp)
	}
	items, err := h.svc.ListMyAgents(c.Request().Context(), uid)
	if err != nil {
		return err
	}
	if items == nil {
		items = []AgentResponse{}
	}
	return c.JSON(http.StatusOK, map[string]any{"items": items})
}

func hasAgentListQuery(c echo.Context) bool {
	for _, key := range []string{"limit", "offset", "q", "status", "visibility", "certification_status", "sort_by"} {
		if c.QueryParam(key) != "" {
			return true
		}
	}
	return false
}

func parseInt32Query(c echo.Context, key string, fallback int32) int32 {
	raw := c.QueryParam(key)
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return fallback
	}
	// #nosec G115 -- strconv.ParseInt with bitSize=32 guarantees int32 range.
	return int32(value)
}

// GetMyAgent 创作者按 id 查自己的 Agent。
func (h *Handler) GetMyAgent(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathID(c)
	if err != nil {
		return err
	}
	resp, err := h.svc.GetMyAgent(c.Request().Context(), id, uid)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// UpdateAgent 创作者编辑 Agent 基础信息。
func (h *Handler) UpdateAgent(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathID(c)
	if err != nil {
		return err
	}
	var req UpdateAgentRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.UpdateAgent(c.Request().Context(), id, uid, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// UpdateVisibility 创作者仅切换 Agent 市场可见性。
func (h *Handler) UpdateVisibility(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathID(c)
	if err != nil {
		return err
	}
	var req UpdateVisibilityRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.SetVisibility(c.Request().Context(), id, uid, req.Visibility)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// DisableAgent 创作者主动下架。
func (h *Handler) DisableAgent(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathID(c)
	if err != nil {
		return err
	}
	if err := h.svc.DisableAgent(c.Request().Context(), id, uid); err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]string{"status": "disabled"})
}

// GetAgentOnboarding 查询创作者侧接入状态。
func (h *Handler) GetAgentOnboarding(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathID(c)
	if err != nil {
		return err
	}
	resp, err := h.svc.GetAgentOnboarding(c.Request().Context(), id, uid)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// UpsertCapability 保存 Agent 能力声明。
func (h *Handler) UpsertCapability(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathID(c)
	if err != nil {
		return err
	}
	var req UpsertCapabilityRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.UpsertCapability(c.Request().Context(), id, uid, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// CreateExample 新增 Agent 示例。
func (h *Handler) CreateExample(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathID(c)
	if err != nil {
		return err
	}
	var req CreateExampleRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.CreateExample(c.Request().Context(), id, uid, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusCreated, resp)
}

// DeleteExample 删除 Agent 示例。
func (h *Handler) DeleteExample(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathID(c)
	if err != nil {
		return err
	}
	exampleID, err := pathUUID(c, "exampleID")
	if err != nil {
		return err
	}
	if err := h.svc.DeleteExample(c.Request().Context(), id, exampleID, uid); err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]string{"status": "deleted"})
}

// RunDryRun 用首条 example 调一次 endpoint 验证接入。
func (h *Handler) RunDryRun(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathID(c)
	if err != nil {
		return err
	}
	resp, err := h.svc.RunDryRun(c.Request().Context(), id, uid)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// RunHealthCheck 是 dry-run 的产品化入口：同时刷新 Agent availability。
func (h *Handler) RunHealthCheck(c echo.Context) error {
	return h.RunDryRun(c)
}

// ListAvailabilityAlerts lists creator-visible Agent availability alerts.
func (h *Handler) ListAvailabilityAlerts(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	limit := int32(50)
	if raw := c.QueryParam("limit"); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 32)
		if err != nil || n < 1 {
			return httpx.BadRequest("limit 不是合法数字")
		}
		// #nosec G115 -- strconv.ParseInt with bitSize=32 guarantees int32 range.
		limit = int32(n)
	}
	resp, err := h.svc.ListAvailabilityAlerts(c.Request().Context(), uid, limit)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// MarkAvailabilityAlertRead marks a creator availability alert as read.
func (h *Handler) MarkAvailabilityAlertRead(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	alertID, err := pathUUID(c, "alertID")
	if err != nil {
		return err
	}
	resp, err := h.svc.MarkAvailabilityAlertRead(c.Request().Context(), uid, alertID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// ListPendingAgents admin/运营人工处理队列。
func (h *Handler) ListPendingAgents(c echo.Context) error {
	items, err := h.svc.ListPendingForAdmin(c.Request().Context())
	if err != nil {
		return err
	}
	if items == nil {
		items = []AgentResponse{}
	}
	return c.JSON(http.StatusOK, map[string]any{"items": items})
}

// RequestCertification 创作者发起认证申请。
func (h *Handler) RequestCertification(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathID(c)
	if err != nil {
		return err
	}
	if err := h.svc.RequestCertification(c.Request().Context(), id, uid); err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]string{"certification_status": "pending"})
}

// CertifyAgent admin/运营授予认证。pending → certified。
func (h *Handler) CertifyAgent(c echo.Context) error {
	id, err := pathID(c)
	if err != nil {
		return err
	}
	if err := h.svc.CertifyAgent(c.Request().Context(), id); err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]string{"certification_status": "certified"})
}

// RejectCertification admin/运营拒绝认证。pending → rejected。
func (h *Handler) RejectCertification(c echo.Context) error {
	id, err := pathID(c)
	if err != nil {
		return err
	}
	var req RejectRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	if err := h.svc.RejectCertification(c.Request().Context(), id, req.Reason); err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]string{"certification_status": "rejected"})
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
	return pathUUID(c, "id")
}

func pathUUID(c echo.Context, name string) (uuid.UUID, error) {
	raw := c.Param(name)
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, httpx.BadRequest(name + " 不是合法 uuid")
	}
	return id, nil
}
