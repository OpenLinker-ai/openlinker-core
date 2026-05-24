package skill

import (
	"errors"
	"net/http"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"

	db "github.com/kinzhi/openlinker-core/pkg/db/generated"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

// Handler Skill HTTP 入口。
type Handler struct {
	svc       *Service
	q         *db.Queries // 仅用于 PATCH 时查 Agent 校验 owner
	validator *validator.Validate
}

// NewHandler 构造 Handler。
func NewHandler(svc *Service, pool *pgxpool.Pool) *Handler {
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
//	PATCH /creator/agents/:id/skills    覆盖某 Agent 的 skill 列表（最多 5 个）
func (h *Handler) RegisterProtected(api *echo.Group, jwtMiddleware echo.MiddlewareFunc) {
	g := api.Group("/creator", jwtMiddleware)
	g.PATCH("/agents/:id/skills", h.SetAgentSkills)
}

// ListAll GET /skills。
func (h *Handler) ListAll(c echo.Context) error {
	rows, err := h.svc.ListAll(c.Request().Context())
	if err != nil {
		return err
	}
	items := make([]SkillItem, 0, len(rows))
	for i := range rows {
		items = append(items, toSkillItem(&rows[i]))
	}
	return c.JSON(http.StatusOK, map[string]any{"items": items})
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

// toSkillItem db.Skill → API DTO。
func toSkillItem(s *db.Skill) SkillItem {
	return SkillItem{
		ID:          s.ID,
		Category:    s.Category,
		Name:        s.Name,
		Description: s.Description,
		SortOrder:   s.SortOrder,
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
