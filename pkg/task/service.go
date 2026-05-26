package task

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	db "github.com/kinzhi/openlinker-core/pkg/db/generated"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
	"github.com/kinzhi/openlinker-core/pkg/llm"
)

// 默认占位值。评分系统未上线前所有 Agent 显示统一评分。
const placeholderAvgRating float32 = 4.8

// AgentMatch skill 模块返回给推荐器的最小信息。
//
// 本模块独立定义同名结构是为避免 internal/task 与 internal/skill 的潜在循环依赖，
// 调用方（main.go）需把 internal/skill 的实现适配到 SkillRecommender 接口。
type AgentMatch struct {
	AgentID    uuid.UUID
	MatchCount int32
}

// SkillRecommender skill 模块对外暴露给本模块的能力。由 internal/skill.Service 实现。
//
//	ListAll                     — 启动时取全部 skill catalog（用于 LLM prompt + 规则匹配）
//	RecommendAgentsBySkills     — 给定 skill_id 列表，返回命中数量最多的 Agent
type SkillRecommender interface {
	ListAll(ctx context.Context) ([]db.Skill, error)
	RecommendAgentsBySkills(ctx context.Context, skillIDs []string, limit int) ([]AgentMatch, error)
}

// Service 任务驱动 A 形态业务逻辑。
//
// allSkills 在 NewService 时一次性预热，后续仅读不写；30 个固定值，内存可忽略。
type Service struct {
	pool      *pgxpool.Pool
	queries   *db.Queries
	llm       llm.Client
	skillSvc  SkillRecommender
	allSkills []db.Skill
}

// NewService 构造 Service，并立即预热 skill catalog。
//
// 任何启动期失败（skillSvc.ListAll 出错）记 warn 但仍返回 Service —— 后续 Recommend
// 调用会重新尝试加载，避免启动顺序耦合。
func NewService(pool *pgxpool.Pool, llmClient llm.Client, skillSvc SkillRecommender) *Service {
	s := &Service{
		pool:     pool,
		queries:  db.New(pool),
		llm:      llmClient,
		skillSvc: skillSvc,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	skills, err := skillSvc.ListAll(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("task.NewService: warmup ListAll failed")
		return s
	}
	s.allSkills = skills
	log.Info().Int("skill_count", len(skills)).Bool("llm_enabled", llmClient != nil).
		Msg("task service ready")
	return s
}

// Recommend 主流程：解析 → 推荐 → 回填 → 持久化。
func (s *Service) Recommend(ctx context.Context, userID uuid.UUID, query string) (*RecommendResponse, error) {
	query = strings.TrimSpace(query)
	if n := len(query); n < 4 || n > 500 {
		return nil, httpx.Unprocessable("query 长度需在 4-500 字符之间")
	}

	// 0. 兜底刷新 catalog（启动时 warmup 失败的场景）
	skills := s.allSkills
	if len(skills) == 0 {
		var err error
		skills, err = s.skillSvc.ListAll(ctx)
		if err != nil {
			log.Error().Err(err).Msg("task.Recommend: ListAll")
			return nil, httpx.Internal("加载 skill 列表失败")
		}
		s.allSkills = skills
	}

	// 1. 解析 skill：优先 LLM；失败/未配置 → 规则
	var parsed []string
	if s.llm != nil {
		out, err := llmParse(ctx, s.llm, query, skills)
		if err != nil {
			log.Warn().Err(err).Str("query", query).Msg("task.Recommend: llmParse fallback to rule")
		} else {
			parsed = out
		}
	}
	if len(parsed) == 0 {
		parsed = ruleParse(query, skills)
	}
	skillByID := skillCatalogByID(skills)

	// 2. 解析空 → 写入 task_query 但 recommendations 为空，便于离线追踪 miss
	resp := &RecommendResponse{
		ParsedSkills:    parsed,
		ParsedSkillRefs: skillRefsForIDs(parsed, skillByID),
		Recommendations: []Recommendation{},
	}

	if len(parsed) == 0 {
		taskID, err := s.persist(ctx, userID, query, parsed, nil)
		if err != nil {
			return nil, err
		}
		resp.TaskID = taskID
		return resp, nil
	}

	// 3. 调 skill 推荐器
	matches, err := s.skillSvc.RecommendAgentsBySkills(ctx, parsed, 3)
	if err != nil {
		log.Error().Err(err).Strs("skills", parsed).Msg("task.Recommend: RecommendAgentsBySkills")
		return nil, httpx.Internal("推荐 Agent 失败")
	}

	if len(matches) == 0 {
		taskID, err := s.persist(ctx, userID, query, parsed, nil)
		if err != nil {
			return nil, err
		}
		resp.TaskID = taskID
		return resp, nil
	}

	// 4. 回填 Agent 详情
	agentIDs := make([]uuid.UUID, len(matches))
	for i := range matches {
		agentIDs[i] = matches[i].AgentID
	}
	rows, err := s.queries.GetAgentsByIDs(ctx, agentIDs)
	if err != nil {
		log.Error().Err(err).Msg("task.Recommend: GetAgentsByIDs")
		return nil, httpx.Internal("加载 Agent 详情失败")
	}
	byID := make(map[uuid.UUID]*db.GetAgentsByIDsRow, len(rows))
	for i := range rows {
		byID[rows[i].ID] = &rows[i]
	}

	// skill_id → 中文名（用于 Why 文案）
	nameByID := skillNameByID(skills)
	parsedCount := float32(len(parsed))

	recs := make([]Recommendation, 0, len(matches))
	for i := range matches {
		row, ok := byID[matches[i].AgentID]
		if !ok {
			// 推荐了但回填不到（可能被下架）→ 跳过
			continue
		}
		score := float32(matches[i].MatchCount) / parsedCount
		if score > 1 {
			score = 1
		}
		matchedSkills, err := s.matchedSkillRefs(ctx, matches[i].AgentID, parsed)
		if err != nil {
			log.Error().Err(err).Str("agent_id", matches[i].AgentID.String()).Msg("task.Recommend: ListAgentSkills")
			return nil, httpx.Internal("加载 Agent Skill 详情失败")
		}
		recs = append(recs, Recommendation{
			Agent:         toAgentSummary(row),
			MatchScore:    score,
			Why:           buildWhy(parsed, nameByID),
			MatchedSkills: matchedSkills,
		})
	}

	// 5. 持久化（用回填后实际有效的 agent_id 顺序）
	storedIDs := make([]uuid.UUID, 0, len(recs))
	for i := range recs {
		id, _ := uuid.Parse(recs[i].Agent.ID)
		storedIDs = append(storedIDs, id)
	}
	taskID, err := s.persist(ctx, userID, query, parsed, storedIDs)
	if err != nil {
		return nil, err
	}
	resp.TaskID = taskID
	resp.Recommendations = recs
	return resp, nil
}

// Choose 用户在推荐里选定一个 Agent。校验：
//
//   - task_id 必须属于该 user（否则 404，不暴露存在性）
//   - agent_id 必须出现在 recommended_agent_ids 里（否则 400）
func (s *Service) Choose(ctx context.Context, taskID, userID, agentID uuid.UUID) error {
	t, err := s.queries.GetTaskQuery(ctx, taskID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return httpx.NotFound("任务不存在")
		}
		log.Error().Err(err).Str("task_id", taskID.String()).Msg("task.Choose: GetTaskQuery")
		return httpx.Internal("查询任务失败")
	}
	if t.UserID != userID {
		return httpx.NotFound("任务不存在")
	}

	in := false
	for _, id := range t.RecommendedAgentIDs {
		if id == agentID {
			in = true
			break
		}
	}
	if !in {
		return httpx.BadRequest("agent_id 不在推荐列表里")
	}

	if _, err := s.queries.MarkTaskQueryChosen(ctx, db.MarkTaskQueryChosenParams{
		ID:            taskID,
		UserID:        userID,
		ChosenAgentID: agentID,
	}); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return httpx.NotFound("任务不存在")
		}
		log.Error().Err(err).Str("task_id", taskID.String()).Msg("task.Choose: MarkTaskQueryChosen")
		return httpx.Internal("更新任务失败")
	}
	return nil
}

// ListMine 用户最近 limit 条任务历史（默认上限 20）。
func (s *Service) ListMine(ctx context.Context, userID uuid.UUID, limit int32) ([]HistoryItem, error) {
	if limit <= 0 || limit > 20 {
		limit = 20
	}
	rows, err := s.queries.ListTaskQueriesByUser(ctx, db.ListTaskQueriesByUserParams{
		UserID: userID,
		Limit:  limit,
	})
	if err != nil {
		log.Error().Err(err).Str("user_id", userID.String()).Msg("task.ListMine: query")
		return nil, httpx.Internal("查询任务历史失败")
	}
	out := make([]HistoryItem, 0, len(rows))
	for i := range rows {
		out = append(out, toHistoryItem(&rows[i]))
	}
	return out, nil
}

// ListBoard 返回任务广场最近公开任务。列表不暴露发布者身份。
func (s *Service) ListBoard(ctx context.Context, limit int32) ([]PublicTaskItem, error) {
	if limit <= 0 || limit > 50 {
		limit = 20
	}
	rows, err := s.queries.ListPublicTaskQueries(ctx, limit)
	if err != nil {
		log.Error().Err(err).Msg("task.ListBoard: query")
		return nil, httpx.Internal("查询任务广场失败")
	}
	skills := s.allSkills
	if len(skills) == 0 {
		if loaded, err := s.skillSvc.ListAll(ctx); err == nil {
			skills = loaded
			s.allSkills = loaded
		} else {
			log.Warn().Err(err).Msg("task.ListBoard: ListAll")
		}
	}
	skillByID := skillCatalogByID(skills)
	out := make([]PublicTaskItem, 0, len(rows))
	for i := range rows {
		out = append(out, toPublicTaskItem(&rows[i], skillByID))
	}
	return out, nil
}

// GetByID 取单个任务 + 回填推荐卡。用于冷链接（sessionStorage 缓存丢失）。
//
// 权限：task 必须属于该 user，否则 404。
// recommendations 按 recommended_agent_ids 顺序回填，跳过已下架/找不到的 agent。
// parsed_skills 用于生成 Why 文案，保持与 Recommend 一致。
func (s *Service) GetByID(ctx context.Context, taskID, userID uuid.UUID) (*DetailResponse, error) {
	t, err := s.queries.GetTaskQuery(ctx, taskID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("任务不存在")
		}
		log.Error().Err(err).Str("task_id", taskID.String()).Msg("task.GetByID: GetTaskQuery")
		return nil, httpx.Internal("查询任务失败")
	}
	if t.UserID != userID {
		return nil, httpx.NotFound("任务不存在")
	}

	resp := &DetailResponse{
		ID:           t.ID.String(),
		Query:        t.Query,
		ParsedSkills: append([]string{}, t.ParsedSkills...),
		CreatedAt:    t.CreatedAt.UTC().Format(time.RFC3339),

		Recommendations: []Recommendation{},
	}
	if t.ChosenAgentID != nil {
		s := t.ChosenAgentID.String()
		resp.ChosenAgentID = &s
	}
	if t.ChosenAt != nil {
		ts := t.ChosenAt.UTC().Format(time.RFC3339)
		resp.ChosenAt = &ts
	}

	// 用 catalog 回填 Why 文案（与 Recommend 一致）
	skills := s.allSkills
	if len(skills) == 0 {
		if loaded, err := s.skillSvc.ListAll(ctx); err == nil {
			skills = loaded
			s.allSkills = loaded
		}
	}
	skillByID := skillCatalogByID(skills)
	resp.ParsedSkillRefs = skillRefsForIDs(t.ParsedSkills, skillByID)

	if len(t.RecommendedAgentIDs) == 0 {
		return resp, nil
	}

	rows, err := s.queries.GetAgentsByIDs(ctx, t.RecommendedAgentIDs)
	if err != nil {
		log.Error().Err(err).Msg("task.GetByID: GetAgentsByIDs")
		return nil, httpx.Internal("加载 Agent 详情失败")
	}
	byID := make(map[uuid.UUID]*db.GetAgentsByIDsRow, len(rows))
	for i := range rows {
		byID[rows[i].ID] = &rows[i]
	}
	nameByID := skillNameByID(skills)

	parsedCount := float32(len(t.ParsedSkills))
	if parsedCount == 0 {
		parsedCount = 1
	}
	for _, aid := range t.RecommendedAgentIDs {
		row, ok := byID[aid]
		if !ok {
			continue
		}
		matchedSkills, err := s.matchedSkillRefs(ctx, aid, t.ParsedSkills)
		if err != nil {
			log.Error().Err(err).Str("agent_id", aid.String()).Msg("task.GetByID: ListAgentSkills")
			return nil, httpx.Internal("加载 Agent Skill 详情失败")
		}
		score := float32(len(matchedSkills)) / parsedCount
		if score > 1 {
			score = 1
		}
		resp.Recommendations = append(resp.Recommendations, Recommendation{
			Agent:         toAgentSummary(row),
			MatchScore:    score,
			Why:           buildWhy(t.ParsedSkills, nameByID),
			MatchedSkills: matchedSkills,
		})
	}
	return resp, nil
}

// persist 写入 task_queries 并返回 id；recommended 为 nil 时存空数组。
func (s *Service) persist(ctx context.Context, userID uuid.UUID, query string, parsed []string, recommended []uuid.UUID) (uuid.UUID, error) {
	if recommended == nil {
		recommended = []uuid.UUID{}
	}
	if parsed == nil {
		parsed = []string{}
	}
	t, err := s.queries.CreateTaskQuery(ctx, db.CreateTaskQueryParams{
		UserID:              userID,
		Query:               query,
		ParsedSkills:        parsed,
		RecommendedAgentIDs: recommended,
	})
	if err != nil {
		log.Error().Err(err).Str("user_id", userID.String()).Msg("task.persist: CreateTaskQuery")
		return uuid.Nil, httpx.Internal("保存任务记录失败")
	}
	return t.ID, nil
}

// buildWhy 按解析顺序生成中文文案，如 "匹配 SQL 查询 + 数据分析"。
func buildWhy(parsed []string, nameByID map[string]string) string {
	parts := make([]string, 0, len(parsed))
	for _, id := range parsed {
		if name, ok := nameByID[id]; ok {
			parts = append(parts, name)
		} else {
			parts = append(parts, id)
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return "匹配 " + strings.Join(parts, " + ")
}

func skillNameByID(skills []db.Skill) map[string]string {
	out := make(map[string]string, len(skills))
	for i := range skills {
		out[skills[i].ID] = skills[i].Name
	}
	return out
}

func skillCatalogByID(skills []db.Skill) map[string]db.Skill {
	out := make(map[string]db.Skill, len(skills))
	for i := range skills {
		out[skills[i].ID] = skills[i]
	}
	return out
}

func skillRefsForIDs(ids []string, byID map[string]db.Skill) []SkillRef {
	out := make([]SkillRef, 0, len(ids))
	for _, id := range ids {
		if s, ok := byID[id]; ok {
			out = append(out, skillRefFromSkill(s))
			continue
		}
		out = append(out, SkillRef{ID: id, Name: id})
	}
	return out
}

func skillRefFromSkill(s db.Skill) SkillRef {
	return SkillRef{
		ID:          s.ID,
		Category:    s.Category,
		Name:        s.Name,
		Description: s.Description,
	}
}

func (s *Service) matchedSkillRefs(ctx context.Context, agentID uuid.UUID, parsed []string) ([]SkillRef, error) {
	if len(parsed) == 0 {
		return []SkillRef{}, nil
	}
	declared, err := s.queries.ListAgentSkills(ctx, agentID)
	if err != nil {
		return nil, err
	}
	declaredByID := make(map[string]db.Skill, len(declared))
	for i := range declared {
		declaredByID[declared[i].ID] = declared[i]
	}
	out := make([]SkillRef, 0, len(parsed))
	for _, id := range parsed {
		if s, ok := declaredByID[id]; ok {
			out = append(out, skillRefFromSkill(s))
		}
	}
	return out, nil
}

// toAgentSummary 把 GetAgentsByIDsRow 转成响应 DTO。
func toAgentSummary(r *db.GetAgentsByIDsRow) AgentSummary {
	return AgentSummary{
		ID:                r.ID.String(),
		Slug:              r.Slug,
		Name:              r.Name,
		Description:       r.Description,
		PricePerCallCents: r.PricePerCallCents,
		TotalCalls:        r.TotalCalls,
		AvgRating:         placeholderAvgRating,
		CreatorName:       r.CreatorName,
		Tags:              append([]string{}, r.Tags...),
	}
}

// toHistoryItem db.TaskQuery → HistoryItem。
func toHistoryItem(t *db.TaskQuery) HistoryItem {
	item := HistoryItem{
		ID:                  t.ID.String(),
		Query:               t.Query,
		ParsedSkills:        append([]string{}, t.ParsedSkills...),
		RecommendedAgentIDs: make([]string, 0, len(t.RecommendedAgentIDs)),
		CreatedAt:           t.CreatedAt.UTC().Format(time.RFC3339),
	}
	for _, id := range t.RecommendedAgentIDs {
		item.RecommendedAgentIDs = append(item.RecommendedAgentIDs, id.String())
	}
	if t.ChosenAgentID != nil {
		s := t.ChosenAgentID.String()
		item.ChosenAgentID = &s
	}
	if t.ChosenAt != nil {
		ts := t.ChosenAt.UTC().Format(time.RFC3339)
		item.ChosenAt = &ts
	}
	return item
}

func toPublicTaskItem(t *db.TaskQuery, skillByID map[string]db.Skill) PublicTaskItem {
	status := "open"
	if t.ChosenAgentID != nil {
		status = "matched"
	} else if len(t.RecommendedAgentIDs) == 0 {
		status = "needs_agent"
	}
	return PublicTaskItem{
		ID:                    t.ID.String(),
		Query:                 t.Query,
		ParsedSkills:          append([]string{}, t.ParsedSkills...),
		ParsedSkillRefs:       skillRefsForIDs(t.ParsedSkills, skillByID),
		RecommendedAgentCount: len(t.RecommendedAgentIDs),
		Status:                status,
		CreatedAt:             t.CreatedAt.UTC().Format(time.RFC3339),
	}
}
