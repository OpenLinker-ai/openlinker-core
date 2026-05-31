package skill

import (
	"net/http"
	"net/url"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

// BenchmarkHandler HTTP 入口。Service 是 nil 时不挂路由（main.go 兜底）。
type BenchmarkHandler struct {
	svc       *BenchmarkService
	validator *validator.Validate
}

// NewBenchmarkHandler 构造。
func NewBenchmarkHandler(svc *BenchmarkService) *BenchmarkHandler {
	return &BenchmarkHandler{
		svc:       svc,
		validator: validator.New(validator.WithRequiredStructEnabled()),
	}
}

// Register 公开端点（无需 JWT）。
//
//	GET /skills/:id/top-agents                      某 skill 下 top-N verified Agent
//	GET /agents/:slug/skill-scores                  按 slug 列出 agent 的 skill 评分
//	GET /agents/:id/benchmarks                      Phase 2 缺口 3：公开 batch 概览
//	GET /agents/:id/benchmarks/:batchID             Phase 2 缺口 3：公开 batch 详情（脱敏）
//	GET /agents/:id/benchmark-results               docs/25 §5.3 别名 → 转发 skill-scores by id
//	GET /benchmark/status                           主动测评运行能力状态（不泄露密钥）
func (h *BenchmarkHandler) Register(api *echo.Group) {
	api.GET("/benchmark/status", h.GetRuntimeStatus)
	api.GET("/skills/:id/top-agents", h.ListTopAgents)
	api.GET("/skills/:category/:name/top-agents", h.ListTopAgents)
	api.GET("/agents/:slug/skill-scores", h.ListScoresBySlug)
	api.GET("/agents/:id/benchmarks", h.ListBatchSummariesPublic)
	api.GET("/agents/:id/benchmarks/:batchID", h.GetBatchPublic)
	api.GET("/agents/:id/benchmark-results", h.ListBenchmarkResults)
}

// RegisterProtected 创作者侧端点（需 JWT）。
//
//	POST /creator/agents/:id/benchmarks                     触发 benchmark
//	GET  /creator/agents/:id/skill-scores                   汇总评分
//	GET  /creator/agents/:id/benchmarks/:batchID            单次 batch 详情
func (h *BenchmarkHandler) RegisterProtected(api *echo.Group, jwtMiddleware echo.MiddlewareFunc) {
	g := api.Group("/creator", jwtMiddleware)
	g.POST("/agents/:id/benchmarks", h.RunBenchmark)
	g.GET("/agents/:id/skill-scores", h.ListMyScores)
	g.GET("/agents/:id/benchmarks/:batchID", h.GetBatch)
}

// RunBenchmark POST /creator/agents/:id/benchmarks。
func (h *BenchmarkHandler) RunBenchmark(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	agentID, err := pathID(c)
	if err != nil {
		return err
	}
	var req RunBenchmarkRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.RunBenchmark(c.Request().Context(), agentID, uid, req.SkillID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusAccepted, resp)
}

// GetRuntimeStatus GET /benchmark/status。
func (h *BenchmarkHandler) GetRuntimeStatus(c echo.Context) error {
	return c.JSON(http.StatusOK, h.svc.RuntimeStatus())
}

// ListMyScores GET /creator/agents/:id/skill-scores。
func (h *BenchmarkHandler) ListMyScores(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	agentID, err := pathID(c)
	if err != nil {
		return err
	}
	// 校验归属
	if err := h.svc.assertOwner(c.Request().Context(), agentID, uid); err != nil {
		return err
	}
	items, err := h.svc.ListAgentScores(c.Request().Context(), agentID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]any{"items": items})
}

// GetBatch GET /creator/agents/:id/benchmarks/:batchID。
func (h *BenchmarkHandler) GetBatch(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	agentID, err := pathID(c)
	if err != nil {
		return err
	}
	batchID, err := pathUUIDParam(c, "batchID")
	if err != nil {
		return err
	}
	detail, err := h.svc.GetBatchDetail(c.Request().Context(), agentID, uid, batchID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, detail)
}

// ListScoresBySlug GET /agents/:slug/skill-scores。
func (h *BenchmarkHandler) ListScoresBySlug(c echo.Context) error {
	slug := c.Param("slug")
	if slug == "" {
		return httpx.BadRequest("slug 不能为空")
	}
	items, err := h.svc.ListAgentScoresBySlug(c.Request().Context(), slug)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]any{"items": items})
}

// ListTopAgents GET /skills/:id/top-agents?limit=3。
func (h *BenchmarkHandler) ListTopAgents(c echo.Context) error {
	skillID := skillIDFromPath(c)
	if skillID == "" {
		return httpx.BadRequest("skill id 不能为空")
	}
	limit := 3
	if raw := c.QueryParam("limit"); raw != "" {
		// 容错：非法 limit 直接落回默认值
		if n := parseLimit(raw); n > 0 {
			limit = n
		}
	}
	items, err := h.svc.ListTopAgents(c.Request().Context(), skillID, limit)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]any{"items": items})
}

func skillIDFromPath(c echo.Context) string {
	skillID := c.Param("id")
	if skillID == "" && c.Param("category") != "" && c.Param("name") != "" {
		skillID = c.Param("category") + "/" + c.Param("name")
	}
	if decoded, err := url.PathUnescape(skillID); err == nil {
		skillID = decoded
	}
	return skillID
}

// ListBatchSummariesPublic GET /agents/:id/benchmarks
func (h *BenchmarkHandler) ListBatchSummariesPublic(c echo.Context) error {
	agentID, err := pathID(c)
	if err != nil {
		return err
	}
	limit := 10
	if raw := c.QueryParam("limit"); raw != "" {
		if n := parseLimit(raw); n > 0 {
			limit = n
		}
	}
	items, err := h.svc.ListBatchSummariesPublic(c.Request().Context(), agentID, limit)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]any{"items": items})
}

// GetBatchPublic GET /agents/:id/benchmarks/:batchID
func (h *BenchmarkHandler) GetBatchPublic(c echo.Context) error {
	agentID, err := pathID(c)
	if err != nil {
		return err
	}
	batchID, err := pathUUIDParam(c, "batchID")
	if err != nil {
		return err
	}
	detail, err := h.svc.GetBatchDetailPublic(c.Request().Context(), agentID, batchID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, detail)
}

// ListBenchmarkResults GET /agents/:id/benchmark-results — docs/25 §5.3 别名。
// 行为与 ListAgentScoresBySlug 对齐，但用 id 查；公开可见性已在 service 层强制。
func (h *BenchmarkHandler) ListBenchmarkResults(c echo.Context) error {
	agentID, err := pathID(c)
	if err != nil {
		return err
	}
	if err := h.svc.assertPublicVisible(c.Request().Context(), agentID); err != nil {
		return err
	}
	items, err := h.svc.ListAgentScores(c.Request().Context(), agentID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]any{"items": items})
}

// pathUUIDParam 解析自定义路径参数为 uuid。
func pathUUIDParam(c echo.Context, name string) (uuid.UUID, error) {
	raw := c.Param(name)
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, httpx.BadRequest(name + " 不是合法 uuid")
	}
	return id, nil
}

// parseLimit 容忍解析；非法返回 0。
func parseLimit(s string) int {
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0
		}
		n = n*10 + int(r-'0')
		if n > 100 {
			return 0
		}
	}
	return n
}
