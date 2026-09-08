package skill

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

// Handler Skill HTTP 入口。
type Handler struct {
	svc       skillService
	q         skillAgentReader // 读写 Agent Skill 前校验 owner
	validator *validator.Validate
}

type skillService interface {
	ListAll(context.Context) ([]db.Skill, error)
	ListPage(context.Context, string, string, string, string, int32, int32) (*SkillListResponse, error)
	SetAgentSkills(context.Context, uuid.UUID, []string) error
	ListForAgent(context.Context, uuid.UUID) ([]db.Skill, error)
	CreateProposal(context.Context, uuid.UUID, *CreateSkillProposalRequest) (*SkillProposalItem, error)
	ListProposals(context.Context, uuid.UUID) ([]SkillProposalItem, error)
	ListProposalsPage(context.Context, uuid.UUID, string, string, string, int32, int32) (*SkillProposalListResponse, error)
}

type skillAgentReader interface {
	GetAgentByID(context.Context, uuid.UUID) (db.Agent, error)
}

// NewHandler 构造 Handler。
func NewHandler(svc skillService, pool *pgxpool.Pool) *Handler {
	return &Handler{
		svc:       svc,
		q:         db.New(pool),
		validator: validator.New(validator.WithRequiredStructEnabled()),
	}
}

// Register 公开端点（无需 JWT）。
//
//	GET /skills    列出全部内置 skill（/publish 表单与发现页用）
func (h *Handler) Register(api *echo.Group) {
	api.GET("/skills", h.ListAll)
}

// RegisterProtected 创作者侧端点（需 JWT）。
//
//	GET /creator/agents/:id/skills      读取所有者已声明的 skill，包括私有 Agent
//	PATCH /creator/agents/:id/skills    覆盖某 Agent 的 skill 列表（最多 5 个）
//	POST /skills/proposals              提交缺失 Skill / 导入声明提案
//	GET /creator/skill-proposals        查看当前用户提案
func (h *Handler) RegisterProtected(api *echo.Group, jwtMiddleware echo.MiddlewareFunc) {
	api.POST("/skills/proposals", h.CreateProposal, jwtMiddleware)

	g := api.Group("/creator", jwtMiddleware)
	g.GET("/agents/:id/skills", h.ListAgentSkills)
	g.PATCH("/agents/:id/skills", h.SetAgentSkills)
	g.GET("/skill-proposals", h.ListProposals)
}

// ListAgentSkills reads declarations through the owner boundary, independently
// of whether the Agent is visible in the public marketplace.
func (h *Handler) ListAgentSkills(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	agentID, err := pathID(c)
	if err != nil {
		return err
	}
	ctx := c.Request().Context()
	a, err := h.q.GetAgentByID(ctx, agentID)
	if errors.Is(err, pgx.ErrNoRows) {
		return httpx.NotFound("Agent 不存在")
	}
	if err != nil {
		log.Error().Err(err).Msg("skill.ListAgentSkills: GetAgentByID")
		return httpx.Internal("查询 Agent 失败")
	}
	if a.CreatorID != uid {
		return httpx.NotFound("Agent 不存在")
	}
	rows, err := h.svc.ListForAgent(ctx, agentID)
	if err != nil {
		return err
	}
	items := make([]SkillItem, 0, len(rows))
	for i := range rows {
		items = append(items, toSkillItem(&rows[i]))
	}
	return c.JSON(http.StatusOK, SetSkillsResponse{AgentID: agentID.String(), Items: items})
}

// ListAll GET /skills?q=&category=&sort=&page=&size=。
func (h *Handler) ListAll(c echo.Context) error {
	resp, err := h.svc.ListPage(
		c.Request().Context(),
		c.QueryParam("q"),
		c.QueryParam("category"),
		c.QueryParam("sort"),
		c.QueryParam("locale"),
		queryInt32(c, "page", 1),
		queryInt32(c, "size", 50),
	)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// SetAgentSkills PATCH /creator/agents/:id/skills。
//
// 鉴权：JWT 解出当前 user → 拉 Agent → 比对 creator_id；不匹配返回 403。
func (h *Handler) SetAgentSkills(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	agentID, err := pathID(c)
	if err != nil {
		return err
	}

	var req SetSkillsRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}

	// 鉴权：必须是该 Agent 的 creator
	ctx := c.Request().Context()
	a, err := h.q.GetAgentByID(ctx, agentID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return httpx.NotFound("Agent 不存在")
		}
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("skill.SetAgentSkills: GetAgentByID")
		return httpx.Internal("查询 Agent 失败")
	}
	if a.CreatorID != uid {
		return httpx.Forbidden("无权修改该 Agent")
	}

	if err := h.svc.SetAgentSkills(ctx, agentID, req.SkillIDs); err != nil {
		return err
	}

	// 回写最新列表，便于前端立即刷新
	rows, err := h.svc.ListForAgent(ctx, agentID)
	if err != nil {
		return err
	}
	items := make([]SkillItem, 0, len(rows))
	for i := range rows {
		items = append(items, toSkillItem(&rows[i]))
	}
	return c.JSON(http.StatusOK, SetSkillsResponse{
		AgentID: agentID.String(),
		Items:   items,
	})
}

// CreateProposal POST /skills/proposals。
func (h *Handler) CreateProposal(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	var req CreateSkillProposalRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	item, err := h.svc.CreateProposal(c.Request().Context(), uid, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusCreated, item)
}

// ListProposals GET /creator/skill-proposals?q=&status=&sort=&page=&size=。
func (h *Handler) ListProposals(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	resp, err := h.svc.ListProposalsPage(
		c.Request().Context(),
		uid,
		c.QueryParam("q"),
		c.QueryParam("status"),
		c.QueryParam("sort"),
		queryInt32(c, "page", 1),
		queryInt32(c, "size", 10),
	)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// toSkillItem db.Skill → API DTO。
func toSkillItem(s *db.Skill) SkillItem {
	return SkillItem{
		ID:           s.ID,
		Category:     s.Category,
		Name:         s.Name,
		Description:  s.Description,
		SortOrder:    s.SortOrder,
		Translations: translationsForSkill(s.ID),
	}
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

func queryInt32(c echo.Context, name string, fallback int32) int32 {
	raw := c.QueryParam(name)
	if raw == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return fallback
	}
	return int32(parsed)
}
