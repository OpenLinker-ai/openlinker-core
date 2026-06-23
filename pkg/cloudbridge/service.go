package cloudbridge

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

const (
	defaultPage = 1
	defaultSize = 20
	maxSize     = 100
	recentRuns  = 5
)

type Service struct {
	queries cloudBridgeQueries
}

type cloudBridgeQueries interface {
	ListRunsByUserWithAgent(context.Context, db.ListRunsByUserWithAgentParams) ([]db.ListRunsByUserWithAgentRow, error)
	CountRunsByUser(context.Context, uuid.UUID) (int32, error)
	GetAgentByIDForOwner(context.Context, db.GetAgentByIDForOwnerParams) (db.Agent, error)
	ListRunsByCreatorAgentWithAgent(context.Context, db.ListRunsByCreatorAgentWithAgentParams) ([]db.ListRunsByCreatorAgentWithAgentRow, error)
	CountRunsByCreatorAgent(context.Context, db.CountRunsByCreatorAgentParams) (int32, error)
	GetUserByID(context.Context, uuid.UUID) (db.User, error)
	CountRunsByUserThisMonth(context.Context, uuid.UUID) (int32, error)
	SumSpentByUserThisMonth(context.Context, uuid.UUID) (int64, error)
	CountRunsForCreatorThisMonth(context.Context, uuid.UUID) (int32, error)
	SumEarningsByCreatorThisMonth(context.Context, uuid.UUID) (int64, error)
	CountAgentsByCreator(context.Context, uuid.UUID) (int32, error)
	CountPendingAgentsByCreator(context.Context, uuid.UUID) (int32, error)
	ListAgentStatsForCreator(context.Context, uuid.UUID) ([]db.ListAgentStatsForCreatorRow, error)
}

func NewService(pool *pgxpool.Pool) *Service {
	return &Service{queries: db.New(pool)}
}

func (s *Service) ListUserRuns(ctx context.Context, userID uuid.UUID, page, size int32) (*RunListResponse, error) {
	page, size = normalizePage(page, size)
	offset := (page - 1) * size

	rows, err := s.queries.ListRunsByUserWithAgent(ctx, db.ListRunsByUserWithAgentParams{
		UserID: userID,
		Limit:  size,
		Offset: offset,
	})
	if err != nil {
		log.Error().Err(err).Str("user_id", userID.String()).Msg("cloudbridge.ListUserRuns: ListRunsByUserWithAgent")
		return nil, httpx.Internal("查询调用历史失败")
	}
	total, err := s.queries.CountRunsByUser(ctx, userID)
	if err != nil {
		log.Error().Err(err).Str("user_id", userID.String()).Msg("cloudbridge.ListUserRuns: CountRunsByUser")
		return nil, httpx.Internal("查询调用历史失败")
	}

	items := make([]RunListItem, 0, len(rows))
	for i := range rows {
		items = append(items, toRunListItem(rows[i].Run, rows[i].AgentSlug, rows[i].AgentName))
	}
	return &RunListResponse{Items: items, Total: total, Page: page, Size: size}, nil
}

func (s *Service) ListCreatorAgentRuns(ctx context.Context, creatorID, agentID uuid.UUID, page, size int32) (*RunListResponse, error) {
	page, size = normalizePage(page, size)
	offset := (page - 1) * size

	if _, err := s.queries.GetAgentByIDForOwner(ctx, db.GetAgentByIDForOwnerParams{
		ID:        agentID,
		CreatorID: creatorID,
	}); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("Agent 不存在或不属于当前用户")
		}
		log.Error().Err(err).Str("creator_id", creatorID.String()).Str("agent_id", agentID.String()).Msg("cloudbridge.ListCreatorAgentRuns: GetAgentByIDForOwner")
		return nil, httpx.Internal("查询 Agent 失败")
	}

	rows, err := s.queries.ListRunsByCreatorAgentWithAgent(ctx, db.ListRunsByCreatorAgentWithAgentParams{
		CreatorID: creatorID,
		AgentID:   agentID,
		Limit:     size,
		Offset:    offset,
	})
	if err != nil {
		log.Error().Err(err).Str("creator_id", creatorID.String()).Str("agent_id", agentID.String()).Msg("cloudbridge.ListCreatorAgentRuns: ListRunsByCreatorAgentWithAgent")
		return nil, httpx.Internal("查询 Agent 调用历史失败")
	}
	total, err := s.queries.CountRunsByCreatorAgent(ctx, db.CountRunsByCreatorAgentParams{
		CreatorID: creatorID,
		AgentID:   agentID,
	})
	if err != nil {
		log.Error().Err(err).Str("creator_id", creatorID.String()).Str("agent_id", agentID.String()).Msg("cloudbridge.ListCreatorAgentRuns: CountRunsByCreatorAgent")
		return nil, httpx.Internal("查询 Agent 调用历史失败")
	}

	items := make([]RunListItem, 0, len(rows))
	for i := range rows {
		items = append(items, toRunListItem(rows[i].Run, rows[i].AgentSlug, rows[i].AgentName))
	}
	return &RunListResponse{Items: items, Total: total, Page: page, Size: size}, nil
}

func (s *Service) GetUserDashboard(ctx context.Context, userID uuid.UUID) (*UserDashboardResponse, error) {
	user, err := s.queries.GetUserByID(ctx, userID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("用户不存在")
		}
		log.Error().Err(err).Str("user_id", userID.String()).Msg("cloudbridge.GetUserDashboard: GetUserByID")
		return nil, httpx.Internal("查询用户失败")
	}

	monthCalls, err := s.queries.CountRunsByUserThisMonth(ctx, userID)
	if err != nil {
		log.Error().Err(err).Msg("cloudbridge.GetUserDashboard: CountRunsByUserThisMonth")
		return nil, httpx.Internal("查询用量失败")
	}
	monthSpent, err := s.queries.SumSpentByUserThisMonth(ctx, userID)
	if err != nil {
		log.Error().Err(err).Msg("cloudbridge.GetUserDashboard: SumSpentByUserThisMonth")
		return nil, httpx.Internal("查询用量失败")
	}
	totalCalls, err := s.queries.CountRunsByUser(ctx, userID)
	if err != nil {
		log.Error().Err(err).Msg("cloudbridge.GetUserDashboard: CountRunsByUser")
		return nil, httpx.Internal("查询用量失败")
	}

	resp := &UserDashboardResponse{
		IsCreator: user.IsCreator,
		Usage: UsageStats{
			ThisMonthCalls: monthCalls,
			ThisMonthSpent: monthSpent,
			TotalCalls:     totalCalls,
		},
	}
	if user.IsCreator {
		summary, err := s.buildCreatorSummary(ctx, userID)
		if err != nil {
			return nil, err
		}
		resp.Creator = summary
	}

	rows, err := s.queries.ListRunsByUserWithAgent(ctx, db.ListRunsByUserWithAgentParams{
		UserID: userID,
		Limit:  recentRuns,
		Offset: 0,
	})
	if err != nil {
		log.Error().Err(err).Msg("cloudbridge.GetUserDashboard: ListRunsByUserWithAgent recent")
		return nil, httpx.Internal("查询最近调用失败")
	}
	resp.RecentRuns = make([]RunListItem, 0, len(rows))
	for i := range rows {
		resp.RecentRuns = append(resp.RecentRuns, toRunListItem(rows[i].Run, rows[i].AgentSlug, rows[i].AgentName))
	}
	return resp, nil
}

func (s *Service) GetCreatorDashboard(ctx context.Context, creatorID uuid.UUID) (*CreatorDashboardResponse, error) {
	user, err := s.queries.GetUserByID(ctx, creatorID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("用户不存在")
		}
		log.Error().Err(err).Str("user_id", creatorID.String()).Msg("cloudbridge.GetCreatorDashboard: GetUserByID")
		return nil, httpx.Internal("查询用户失败")
	}
	if !user.IsCreator {
		return nil, httpx.Forbidden("仅创作者可访问创作者中心")
	}

	summary, err := s.buildCreatorSummary(ctx, creatorID)
	if err != nil {
		return nil, err
	}
	statRows, err := s.queries.ListAgentStatsForCreator(ctx, creatorID)
	if err != nil {
		log.Error().Err(err).Str("user_id", creatorID.String()).Msg("cloudbridge.GetCreatorDashboard: ListAgentStatsForCreator")
		return nil, httpx.Internal("查询 Agent 列表失败")
	}
	agents := make([]AgentStatsItem, 0, len(statRows))
	for i := range statRows {
		agents = append(agents, toAgentStatsItem(&statRows[i]))
	}
	return &CreatorDashboardResponse{Summary: *summary, Agents: agents}, nil
}

func (s *Service) buildCreatorSummary(ctx context.Context, creatorID uuid.UUID) (*CreatorSummary, error) {
	monthCalls, err := s.queries.CountRunsForCreatorThisMonth(ctx, creatorID)
	if err != nil {
		log.Error().Err(err).Msg("cloudbridge.buildCreatorSummary: CountRunsForCreatorThisMonth")
		return nil, httpx.Internal("查询创作者用量失败")
	}
	monthRev, err := s.queries.SumEarningsByCreatorThisMonth(ctx, creatorID)
	if err != nil {
		log.Error().Err(err).Msg("cloudbridge.buildCreatorSummary: SumEarningsByCreatorThisMonth")
		return nil, httpx.Internal("查询创作者收入失败")
	}
	totalAgents, err := s.queries.CountAgentsByCreator(ctx, creatorID)
	if err != nil {
		log.Error().Err(err).Msg("cloudbridge.buildCreatorSummary: CountAgentsByCreator")
		return nil, httpx.Internal("查询 Agent 数失败")
	}
	pendingAgents, err := s.queries.CountPendingAgentsByCreator(ctx, creatorID)
	if err != nil {
		log.Error().Err(err).Msg("cloudbridge.buildCreatorSummary: CountPendingAgentsByCreator")
		return nil, httpx.Internal("查询待处理 Agent 数失败")
	}
	return &CreatorSummary{
		ThisMonthCalls:   monthCalls,
		ThisMonthRevenue: monthRev,
		TotalAgents:      totalAgents,
		PendingAgents:    pendingAgents,
	}, nil
}

func normalizePage(page, size int32) (int32, int32) {
	if page < 1 {
		page = defaultPage
	}
	if size < 1 {
		size = defaultSize
	}
	if size > maxSize {
		size = maxSize
	}
	return page, size
}

func toRunListItem(r db.Run, agentSlug, agentName string) RunListItem {
	return RunListItem{
		ID:         r.ID.String(),
		AgentID:    r.AgentID.String(),
		AgentSlug:  agentSlug,
		AgentName:  agentName,
		Status:     r.Status,
		CostCents:  r.CostCents,
		DurationMs: r.DurationMs,
		StartedAt:  r.StartedAt.UTC().Format(time.RFC3339),
		Source:     r.Source,
	}
}

func toAgentStatsItem(r *db.ListAgentStatsForCreatorRow) AgentStatsItem {
	return AgentStatsItem{
		ID:               r.ID.String(),
		Slug:             r.Slug,
		Name:             r.Name,
		Status:           r.Status,
		PriceCents:       r.PricePerCallCents,
		LifetimeCalls:    r.LifetimeCalls,
		LifetimeRevenue:  r.LifetimeRevenue,
		CallsThisMonth:   r.CallsThisMonth,
		RevenueThisMonth: r.RevenueThisMonth,
	}
}
