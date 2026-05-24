package agent

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	db "github.com/kinzhi/openlinker-core/pkg/db/generated"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

// MarketService 市场（用户侧只读）业务逻辑。
//
// 设计与模块 2 (Agent 注册写入) 隔离：本 service 只调用 SELECT，
// 不持有事务，也不依赖任何写入逻辑。
type MarketService struct {
	queries *db.Queries
}

// NewMarketService 构造 MarketService，pool 仅用于读。
func NewMarketService(pool *pgxpool.Pool) *MarketService {
	return &MarketService{queries: db.New(pool)}
}

// 默认与上限分页参数。
//
// size 上限设为 50，避免恶意大查询拖慢数据库（agents.tags 是 GIN 索引但仍要 scan）。
const (
	defaultPage int32 = 1
	defaultSize int32 = 12
	maxSize     int32 = 50
)

// ListMarket 列出已公开 Agent。
//
//   - tags：空切片表示不按 tag 筛；非空时使用 Postgres 数组重叠运算（任意命中）。
//   - keyword：空串表示不搜；非空时对 name/description 做 ILIKE。
//   - page 从 1 开始；size 由调用方 clamp 到 [1, 50]，但这里再做一次防御。
func (s *MarketService) ListMarket(ctx context.Context, tags []string, keyword string, page, size int32) (*MarketListResponse, error) {
	if page < 1 {
		page = defaultPage
	}
	if size < 1 {
		size = defaultSize
	}
	if size > maxSize {
		size = maxSize
	}
	// pgx/v5 把 nil 序列化为 NULL；query 用 cardinality(text[]) = 0 判断空，
	// 因此这里要确保传入的是非 nil 的空切片。
	if tags == nil {
		tags = []string{}
	}

	offset := (page - 1) * size

	rows, err := s.queries.ListApprovedAgents(ctx, db.ListApprovedAgentsParams{
		Tags:    tags,
		Keyword: keyword,
		Limit:   size,
		Offset:  offset,
	})
	if err != nil {
		log.Error().Err(err).Msg("agent.MarketService.ListMarket: ListApprovedAgents")
		return nil, httpx.Internal("查询 Agent 列表失败")
	}

	total, err := s.queries.CountApprovedAgents(ctx, db.CountApprovedAgentsParams{
		Tags:    tags,
		Keyword: keyword,
	})
	if err != nil {
		log.Error().Err(err).Msg("agent.MarketService.ListMarket: CountApprovedAgents")
		return nil, httpx.Internal("统计 Agent 数量失败")
	}

	items := make([]MarketListItem, 0, len(rows))
	for _, r := range rows {
		items = append(items, MarketListItem{
			ID:                r.ID.String(),
			Slug:              r.Slug,
			Name:              r.Name,
			Description:       r.Description,
			PricePerCallCents: r.PricePerCallCents,
			Tags:              normalizeTags(r.Tags),
			TotalCalls:        r.TotalCalls,
			Creator:           CreatorMini{DisplayName: r.CreatorName},
		})
	}

	return &MarketListResponse{
		Items: items,
		Total: total,
		Page:  page,
		Size:  size,
	}, nil
}

// GetBySlug 按 slug 查询已公开 Agent 详情。
//
// 不存在 / 未公开 / 已禁用 → NotFound（统一返回 404，避免泄露状态信息）。
// endpoint_auth_header 永不暴露给前端。
func (s *MarketService) GetBySlug(ctx context.Context, slug string) (*AgentDetailResponse, error) {
	if slug == "" {
		return nil, httpx.NotFound("Agent 不存在")
	}

	r, err := s.queries.GetAgentBySlug(ctx, slug)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("Agent 不存在")
		}
		log.Error().Err(err).Str("slug", slug).Msg("agent.MarketService.GetBySlug")
		return nil, httpx.Internal("查询 Agent 详情失败")
	}

	resp := &AgentDetailResponse{
		ID:                r.ID.String(),
		Slug:              r.Slug,
		Name:              r.Name,
		Description:       r.Description,
		EndpointURL:       r.EndpointURL,
		PricePerCallCents: r.PricePerCallCents,
		Tags:              normalizeTags(r.Tags),
		TotalCalls:        r.TotalCalls,
		Creator:           CreatorMini{DisplayName: r.CreatorName},
		CreatedAt:         r.CreatedAt.UTC().Format(time.RFC3339),
		Skills:            []SkillMini{},
	}
	if r.ApprovedAt != nil {
		s := r.ApprovedAt.UTC().Format(time.RFC3339)
		resp.ApprovedAt = &s
	}

	skills, err := s.queries.ListAgentSkills(ctx, r.ID)
	if err != nil {
		log.Error().Err(err).Str("agent_id", r.ID.String()).Msg("agent.MarketService.GetBySlug: ListAgentSkills")
		return nil, httpx.Internal("查询 Agent skill 失败")
	}
	for i := range skills {
		resp.Skills = append(resp.Skills, SkillMini{
			ID:          skills[i].ID,
			Category:    skills[i].Category,
			Name:        skills[i].Name,
			Description: skills[i].Description,
		})
	}

	resp.Examples = []ExampleResponse{}
	cap, err := s.queries.GetAgentCapabilityBySlug(ctx, slug)
	if err == nil {
		c := toCapabilityResponse(&cap)
		resp.Capability = &c
	} else if !errors.Is(err, pgx.ErrNoRows) {
		log.Error().Err(err).Str("slug", slug).Msg("agent.MarketService.GetBySlug: GetAgentCapabilityBySlug")
		return nil, httpx.Internal("查询 Agent 能力声明失败")
	}

	examples, err := s.queries.ListAgentExamplesBySlug(ctx, slug)
	if err != nil {
		log.Error().Err(err).Str("slug", slug).Msg("agent.MarketService.GetBySlug: ListAgentExamplesBySlug")
		return nil, httpx.Internal("查询 Agent 示例失败")
	}
	for i := range examples {
		resp.Examples = append(resp.Examples, toExampleResponse(&examples[i]))
	}
	return resp, nil
}

// normalizeTags 把 nil 切片归一化成空切片，确保 JSON 输出 [] 而不是 null。
func normalizeTags(tags []string) []string {
	if tags == nil {
		return []string{}
	}
	return tags
}
