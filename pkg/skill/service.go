package skill

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

// MaxSkillsPerAgent 单个 Agent 最多可声明的 skill 数量（PRD：5 个上限）。
const MaxSkillsPerAgent = 5

var (
	proposedSkillIDPattern  = regexp.MustCompile(`^[a-z][a-z0-9]*(?:[/_-][a-z0-9]+)*$`)
	proposalCategoryPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{1,79}$`)
)

// Service Skill 业务逻辑层。
type Service struct {
	pool *pgxpool.Pool
	q    *db.Queries
}

// NewService 构造 Service。
func NewService(pool *pgxpool.Pool) *Service {
	return &Service{
		pool: pool,
		q:    db.New(pool),
	}
}

// ListAll 返回平台内置 skill（公开，给 /publish 表单与发现页用）。
func (s *Service) ListAll(ctx context.Context) ([]db.Skill, error) {
	items, err := s.q.ListSkills(ctx, db.ListSkillsParams{
		Sort:  "order",
		Limit: 200,
	})
	if err != nil {
		log.Error().Err(err).Msg("skill.ListAll: ListSkills")
		return nil, httpx.Internal("查询 skill 列表失败")
	}
	return items, nil
}

// ListPage 返回公开 Skill 目录分页结果。
func (s *Service) ListPage(ctx context.Context, query, category, listSort, locale string, page, size int32) (*SkillListResponse, error) {
	page, size = normalizeSkillPage(page, size, 50, 200)
	query = normalizeSkillListQuery(query)
	category = normalizeSkillCategoryFilter(category)
	listSort = normalizeSkillListSort(listSort)
	if normalizeSkillLocale(locale) == "en" {
		return s.listEnglishPage(ctx, query, category, listSort, page, size)
	}
	offset := (page - 1) * size
	rows, err := s.q.ListSkills(ctx, db.ListSkillsParams{
		Query:    query,
		Category: category,
		Sort:     listSort,
		Limit:    size,
		Offset:   offset,
	})
	if err != nil {
		log.Error().Err(err).Msg("skill.ListPage: ListSkills")
		return nil, httpx.Internal("查询 skill 列表失败")
	}
	total, err := s.q.CountSkills(ctx, db.CountSkillsParams{
		Query:    query,
		Category: category,
	})
	if err != nil {
		log.Error().Err(err).Msg("skill.ListPage: CountSkills")
		return nil, httpx.Internal("查询 skill 数量失败")
	}
	items := make([]SkillItem, 0, len(rows))
	for i := range rows {
		items = append(items, toSkillItem(&rows[i]))
	}
	return &SkillListResponse{
		Items:          items,
		Total:          total,
		Page:           page,
		Size:           size,
		Query:          query,
		CategoryFilter: category,
		Sort:           listSort,
	}, nil
}

// listEnglishPage keeps the canonical Chinese catalog in PostgreSQL while
// applying public English copy at the API boundary. The curated Skill catalog
// is deliberately small, so filtering before pagination keeps search, total,
// and name ordering consistent with what an English client displays.
func (s *Service) listEnglishPage(ctx context.Context, query, category, listSort string, page, size int32) (*SkillListResponse, error) {
	dbSort := listSort
	if listSort == "name_asc" || listSort == "name_desc" {
		dbSort = "order"
	}
	rows, err := s.q.ListSkills(ctx, db.ListSkillsParams{
		Category: category,
		Sort:     dbSort,
		Limit:    200,
	})
	if err != nil {
		log.Error().Err(err).Msg("skill.ListPage: ListSkills English catalog")
		return nil, httpx.Internal("查询 skill 列表失败")
	}

	rows = filterEnglishSkillRows(rows, query)
	if listSort == "name_asc" || listSort == "name_desc" {
		sortEnglishSkillRows(rows, listSort == "name_desc")
	}
	total := int64(len(rows))
	rows = paginateSkillRows(rows, page, size)
	items := make([]SkillItem, 0, len(rows))
	for i := range rows {
		items = append(items, toSkillItem(&rows[i]))
	}
	return &SkillListResponse{
		Items:          items,
		Total:          total,
		Page:           page,
		Size:           size,
		Query:          query,
		CategoryFilter: category,
		Sort:           listSort,
	}, nil
}

func filterEnglishSkillRows(rows []db.Skill, query string) []db.Skill {
	needle := strings.ToLower(strings.TrimSpace(query))
	if needle == "" {
		return rows
	}
	filtered := make([]db.Skill, 0, len(rows))
	for i := range rows {
		translation, ok := englishSkillTranslations[rows[i].ID]
		if strings.Contains(strings.ToLower(rows[i].ID), needle) ||
			(ok && (strings.Contains(strings.ToLower(translation.Name), needle) ||
				strings.Contains(strings.ToLower(translation.Description), needle))) {
			filtered = append(filtered, rows[i])
		}
	}
	return filtered
}

func sortEnglishSkillRows(rows []db.Skill, descending bool) {
	sort.SliceStable(rows, func(i, j int) bool {
		left := englishSkillName(rows[i].ID)
		right := englishSkillName(rows[j].ID)
		if left == right {
			return rows[i].ID < rows[j].ID
		}
		if descending {
			return left > right
		}
		return left < right
	})
}

func englishSkillName(skillID string) string {
	if translation, ok := englishSkillTranslations[skillID]; ok && strings.TrimSpace(translation.Name) != "" {
		return strings.ToLower(strings.TrimSpace(translation.Name))
	}
	return strings.ToLower(strings.TrimSpace(skillID))
}

func paginateSkillRows(rows []db.Skill, page, size int32) []db.Skill {
	start := int64(page-1) * int64(size)
	if start >= int64(len(rows)) {
		return []db.Skill{}
	}
	end := start + int64(size)
	if end > int64(len(rows)) {
		end = int64(len(rows))
	}
	return rows[int(start):int(end)]
}

// ListForAgent 返回某 Agent 已声明的 skill 详情。
func (s *Service) ListForAgent(ctx context.Context, agentID uuid.UUID) ([]db.Skill, error) {
	items, err := s.q.ListAgentSkills(ctx, agentID)
	if err != nil {
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("skill.ListForAgent: ListAgentSkills")
		return nil, httpx.Internal("查询 Agent skill 失败")
	}
	return items, nil
}

// CreateProposal 创建或更新当前用户的 Skill Proposal。
func (s *Service) CreateProposal(ctx context.Context, ownerID uuid.UUID, req *CreateSkillProposalRequest) (*SkillProposalItem, error) {
	clean, err := normalizeSkillProposalRequest(req)
	if err != nil {
		return nil, err
	}

	var agentID *uuid.UUID
	if clean.AgentID != nil && strings.TrimSpace(*clean.AgentID) != "" {
		parsed, err := uuid.Parse(strings.TrimSpace(*clean.AgentID))
		if err != nil {
			return nil, httpx.BadRequest("agent_id 不是合法 uuid")
		}
		agent, err := s.q.GetAgentByID(ctx, parsed)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, httpx.NotFound("Agent 不存在")
			}
			log.Error().Err(err).Str("agent_id", parsed.String()).Msg("skill.CreateProposal: GetAgentByID")
			return nil, httpx.Internal("查询 Agent 失败")
		}
		if agent.CreatorID != ownerID {
			return nil, httpx.Forbidden("无权为该 Agent 提交 Skill Proposal")
		}
		agentID = &parsed
	}

	status := "pending"
	var matchedSkillID *string
	if existing, err := s.q.GetSkill(ctx, clean.ProposedSkillID); err == nil {
		status = "merged"
		matchedSkillID = &existing.ID
	} else if !errors.Is(err, pgx.ErrNoRows) {
		log.Error().Err(err).Str("skill_id", clean.ProposedSkillID).Msg("skill.CreateProposal: GetSkill")
		return nil, httpx.Internal("校验 Skill 失败")
	}

	row, err := s.q.CreateSkillProposal(ctx, db.CreateSkillProposalParams{
		OwnerUserID:     ownerID,
		AgentID:         agentID,
		ProposedSkillID: clean.ProposedSkillID,
		Category:        clean.Category,
		Name:            clean.Name,
		Description:     clean.Description,
		Source:          clean.Source,
		Status:          status,
		MatchedSkillID:  matchedSkillID,
	})
	if err != nil {
		log.Error().Err(err).Str("owner_user_id", ownerID.String()).Msg("skill.CreateProposal: CreateSkillProposal")
		return nil, httpx.Internal("提交 Skill Proposal 失败")
	}
	item := toSkillProposalItem(&row)
	return &item, nil
}

// ListProposals 返回当前用户最近提交或导入生成的 Skill Proposal。
func (s *Service) ListProposals(ctx context.Context, ownerID uuid.UUID) ([]SkillProposalItem, error) {
	resp, err := s.ListProposalsPage(ctx, ownerID, "", "", "updated_desc", 1, 100)
	if err != nil {
		return nil, err
	}
	return resp.Items, nil
}

// ListProposalsPage 返回当前用户 Skill Proposal 分页结果。
func (s *Service) ListProposalsPage(ctx context.Context, ownerID uuid.UUID, query, status, sort string, page, size int32) (*SkillProposalListResponse, error) {
	page, size = normalizeSkillPage(page, size, 10, 50)
	query = normalizeSkillListQuery(query)
	status = normalizeSkillProposalStatus(status)
	sort = normalizeSkillProposalSort(sort)
	offset := (page - 1) * size
	rows, err := s.q.ListSkillProposalsByOwner(ctx, db.ListSkillProposalsByOwnerParams{
		OwnerUserID: ownerID,
		Query:       query,
		Status:      status,
		Sort:        sort,
		Limit:       size,
		Offset:      offset,
	})
	if err != nil {
		log.Error().Err(err).Str("owner_user_id", ownerID.String()).Msg("skill.ListProposals: ListSkillProposalsByOwner")
		return nil, httpx.Internal("查询 Skill Proposal 失败")
	}
	total, err := s.q.CountSkillProposalsByOwner(ctx, db.CountSkillProposalsByOwnerParams{
		OwnerUserID: ownerID,
		Query:       query,
		Status:      status,
	})
	if err != nil {
		log.Error().Err(err).Str("owner_user_id", ownerID.String()).Msg("skill.ListProposals: CountSkillProposalsByOwner")
		return nil, httpx.Internal("查询 Skill Proposal 数量失败")
	}
	items := make([]SkillProposalItem, 0, len(rows))
	for i := range rows {
		items = append(items, toSkillProposalItem(&rows[i]))
	}
	return &SkillProposalListResponse{
		Items:        items,
		Total:        total,
		Page:         page,
		Size:         size,
		Query:        query,
		StatusFilter: status,
		Sort:         sort,
	}, nil
}

// SetAgentSkills 用 skillIDs 覆盖某 Agent 的关联（事务内 DELETE + 批量 INSERT）。
//
// 校验：
//  1. 数量 <= MaxSkillsPerAgent；
//  2. 去重后非空串；
//  3. 每个 id 必须存在于 skills 表（否则 400 报告第一个非法 id）。
//
// 调用方负责鉴权：仅 Agent.creator_id == 当前用户 时才允许调用。
func (s *Service) SetAgentSkills(ctx context.Context, agentID uuid.UUID, skillIDs []string) error {
	cleaned, err := normalizeSkillIDs(skillIDs)
	if err != nil {
		return err
	}

	// 校验每个 skill_id 存在性（5 条上限，N+1 查询可接受）。
	for _, sid := range cleaned {
		if _, err := s.q.GetSkill(ctx, sid); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return httpx.BadRequest(fmt.Sprintf("skill_id 不存在: %s", sid))
			}
			log.Error().Err(err).Str("skill_id", sid).Msg("skill.SetAgentSkills: GetSkill")
			return httpx.Internal("校验 skill 失败")
		}
	}

	// 事务覆盖：DELETE 旧的 + 批量 INSERT 新的。
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		return db.ReplaceAgentSkills(ctx, tx, agentID, cleaned)
	})
	if err != nil {
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("skill.SetAgentSkills: ReplaceAgentSkills")
		return httpx.Internal("更新 Agent skill 失败")
	}
	return nil
}

// RecommendAgentsBySkills 任务驱动推荐：按命中 skill 数量降序返回 Agent 列表。
//
// 由子轮 2.4 task 模块调用：传入 LLM 解析出的 skill_id 列表 + top-N，
// 拿到候选 Agent。limit <= 0 时不截断。
//
// runtime Agent 复用市场 readiness，并且必须有 PostgreSQL 证明的
// current-contract ready Session。周期在线性由 Redis Session lease 与 Runtime
// reaper 收敛；数据库 heartbeat 只用于排序。Agent Token 使用记录不是在线性证据。
// Direct/MCP Agent 则需要 healthy 或成功运行证据，避免推荐到当前不可执行的供给。
// 排序：match_count desc → availability → recent Session/success evidence → verified_count desc → total_calls desc → agent_id（稳定）。
// verified_count 来自 agent_skill_scores（模块 B 写入），把 verified 过的命中数当作信任加权。
func (s *Service) RecommendAgentsBySkills(ctx context.Context, skillIDs []string, limit int) ([]AgentMatch, error) {
	cleaned := dedupNonEmpty(skillIDs)
	if len(cleaned) == 0 {
		return []AgentMatch{}, nil
	}
	rows, err := s.q.ListAgentsBySkillsWithVerified(ctx, cleaned)
	if err != nil {
		log.Error().Err(err).Msg("skill.RecommendAgentsBySkills: ListAgentsBySkillsWithVerified")
		return nil, httpx.Internal("查询推荐 Agent 失败")
	}
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	out := make([]AgentMatch, len(rows))
	for i := range rows {
		out[i] = AgentMatch{
			AgentID:       rows[i].AgentID,
			MatchCount:    rows[i].MatchCount,
			VerifiedCount: rows[i].VerifiedCount,
			TotalCalls:    rows[i].TotalCalls,
		}
	}
	return out, nil
}

// normalizeSkillIDs trim + 去空 + 去重 + 上限检查；返回 [] 表示清空。
func normalizeSkillIDs(in []string) ([]string, error) {
	cleaned := dedupNonEmpty(in)
	if len(cleaned) > MaxSkillsPerAgent {
		return nil, httpx.BadRequest(fmt.Sprintf("最多只能选择 %d 个 skill", MaxSkillsPerAgent))
	}
	return cleaned, nil
}

func normalizeSkillProposalRequest(req *CreateSkillProposalRequest) (*CreateSkillProposalRequest, error) {
	if req == nil {
		return nil, httpx.BadRequest("请求体不能为空")
	}
	out := *req
	out.ProposedSkillID = strings.ToLower(strings.TrimSpace(out.ProposedSkillID))
	out.Category = strings.ToLower(strings.TrimSpace(out.Category))
	out.Name = strings.TrimSpace(out.Name)
	out.Description = strings.TrimSpace(out.Description)
	out.Source = strings.TrimSpace(out.Source)
	if out.Source == "" {
		out.Source = "manual"
	}
	if !proposedSkillIDPattern.MatchString(out.ProposedSkillID) {
		return nil, httpx.BadRequest("proposed_skill_id 只能使用小写字母、数字、/、_、-，并且不能以分隔符开头或结尾")
	}
	if !proposalCategoryPattern.MatchString(out.Category) {
		return nil, httpx.BadRequest("category 只能使用小写字母、数字、_、-，长度 2-80")
	}
	if out.Source != "manual" && out.Source != "imported_text" && out.Source != "imported_json" {
		return nil, httpx.BadRequest("source 不支持")
	}
	return &out, nil
}

func toSkillProposalItem(p *db.SkillProposal) SkillProposalItem {
	var agentID *string
	if p.AgentID != nil {
		agentID = stringPtr(p.AgentID.String())
	}
	return SkillProposalItem{
		ID:              p.ID.String(),
		AgentID:         agentID,
		ProposedSkillID: p.ProposedSkillID,
		Category:        p.Category,
		Name:            p.Name,
		Description:     p.Description,
		Source:          p.Source,
		Status:          p.Status,
		MatchedSkillID:  p.MatchedSkillID,
		CreatedAt:       p.CreatedAt.Format(time.RFC3339),
		UpdatedAt:       p.UpdatedAt.Format(time.RFC3339),
	}
}

// dedupNonEmpty trim 空白 + 去空串 + 去重，保持原顺序。
func dedupNonEmpty(in []string) []string {
	if len(in) == 0 {
		return []string{}
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, raw := range in {
		s := strings.TrimSpace(raw)
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

func normalizeSkillPage(page, size, defaultSize, maxSize int32) (int32, int32) {
	if page <= 0 {
		page = 1
	}
	if size <= 0 {
		size = defaultSize
	}
	if size > maxSize {
		size = maxSize
	}
	return page, size
}

func normalizeSkillListQuery(query string) string {
	query = strings.TrimSpace(query)
	if len(query) > 120 {
		query = query[:120]
	}
	return query
}

func normalizeSkillCategoryFilter(category string) string {
	category = strings.ToLower(strings.TrimSpace(category))
	switch category {
	case "content", "dev", "data", "media", "ops", "ai":
		return category
	default:
		return ""
	}
}

func normalizeSkillListSort(sort string) string {
	switch strings.ToLower(strings.TrimSpace(sort)) {
	case "name_asc", "name_desc", "category_asc", "category_desc", "created_desc", "created_asc":
		return strings.ToLower(strings.TrimSpace(sort))
	default:
		return "order"
	}
}

func normalizeSkillLocale(locale string) string {
	if strings.EqualFold(strings.TrimSpace(locale), "en") {
		return "en"
	}
	return "zh"
}

func normalizeSkillProposalStatus(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "pending", "merged", "rejected":
		return strings.ToLower(strings.TrimSpace(status))
	default:
		return ""
	}
}

func normalizeSkillProposalSort(sort string) string {
	switch strings.ToLower(strings.TrimSpace(sort)) {
	case "updated_desc", "updated_asc", "created_desc", "created_asc", "name_asc", "status_asc", "status_desc":
		return strings.ToLower(strings.TrimSpace(sort))
	default:
		return "updated_desc"
	}
}

func stringPtr(v string) *string {
	return &v
}
