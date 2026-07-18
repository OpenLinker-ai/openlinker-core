package userdash

import (
	"context"
	"errors"
	"strings"
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
	queries dashboardQueries
}

type dashboardQueries interface {
	ListRunsByUserWithAgent(context.Context, db.ListRunsByUserWithAgentParams) ([]db.ListRunsByUserWithAgentRow, error)
	CountRunsByUser(context.Context, uuid.UUID) (int32, error)
	ListCallRecordsForUser(context.Context, db.ListCallRecordsForUserParams) ([]db.ListCallRecordsForUserRow, error)
	CountCallRecordsForUser(context.Context, db.CountCallRecordsForUserParams) (int32, error)
	GetAgentByIDForOwner(context.Context, db.GetAgentByIDForOwnerParams) (db.Agent, error)
	ListRunsByCreatorAgentWithAgent(context.Context, db.ListRunsByCreatorAgentWithAgentParams) ([]db.ListRunsByCreatorAgentWithAgentRow, error)
	CountRunsByCreatorAgent(context.Context, db.CountRunsByCreatorAgentParams) (int32, error)
	GetUserByID(context.Context, uuid.UUID) (db.User, error)
	GetUserDashboardUsage(context.Context, uuid.UUID) (db.GetUserDashboardUsageRow, error)
	GetCreatorDashboardSummary(context.Context, uuid.UUID) (db.GetCreatorDashboardSummaryRow, error)
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
		log.Error().Err(err).Str("user_id", userID.String()).Msg("userdash.ListUserRuns: ListRunsByUserWithAgent")
		return nil, httpx.Internal("查询调用历史失败")
	}
	total, err := s.queries.CountRunsByUser(ctx, userID)
	if err != nil {
		log.Error().Err(err).Str("user_id", userID.String()).Msg("userdash.ListUserRuns: CountRunsByUser")
		return nil, httpx.Internal("查询调用历史失败")
	}

	items := make([]RunListItem, 0, len(rows))
	for i := range rows {
		items = append(items, toRunListItem(rows[i].Run, rows[i].AgentSlug, rows[i].AgentName))
	}
	return &RunListResponse{Items: items, Total: total, Page: page, Size: size}, nil
}

func (s *Service) ListCallRecords(ctx context.Context, userID uuid.UUID, view, query, sort, status, source, relation string, page, size int32) (*CallRecordListResponse, error) {
	page, size = normalizePage(page, size)
	view = normalizeCallRecordView(view)
	query = normalizeCallRecordQuery(query)
	sort = normalizeCallRecordSort(sort)
	status = normalizeCallRecordStatus(status)
	source = normalizeCallRecordSource(source)
	relation = normalizeCallRecordRelation(relation)
	offset := (page - 1) * size

	rows, err := s.queries.ListCallRecordsForUser(ctx, db.ListCallRecordsForUserParams{
		UserID:   userID,
		View:     view,
		Query:    query,
		Status:   status,
		Source:   source,
		Relation: relation,
		Sort:     sort,
		Limit:    size,
		Offset:   offset,
	})
	if err != nil {
		log.Error().Err(err).Str("user_id", userID.String()).Str("view", view).Str("query", query).Str("sort", sort).Str("status", status).Str("source", source).Str("relation", relation).Msg("userdash.ListCallRecords: ListCallRecordsForUser")
		return nil, httpx.Internal("查询调用记录失败")
	}
	total, err := s.queries.CountCallRecordsForUser(ctx, db.CountCallRecordsForUserParams{
		UserID:   userID,
		View:     view,
		Query:    query,
		Status:   status,
		Source:   source,
		Relation: relation,
	})
	if err != nil {
		log.Error().Err(err).Str("user_id", userID.String()).Str("view", view).Str("query", query).Str("status", status).Str("source", source).Str("relation", relation).Msg("userdash.ListCallRecords: CountCallRecordsForUser")
		return nil, httpx.Internal("查询调用记录失败")
	}

	items := make([]CallRecordItem, 0, len(rows))
	for i := range rows {
		items = append(items, toCallRecordItem(rows[i]))
	}
	return &CallRecordListResponse{
		Items:          items,
		Total:          total,
		Page:           page,
		Size:           size,
		View:           view,
		Query:          query,
		Sort:           sort,
		StatusFilter:   status,
		SourceFilter:   source,
		RelationFilter: relation,
	}, nil
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
		log.Error().Err(err).Str("creator_id", creatorID.String()).Str("agent_id", agentID.String()).Msg("userdash.ListCreatorAgentRuns: GetAgentByIDForOwner")
		return nil, httpx.Internal("查询 Agent 失败")
	}

	rows, err := s.queries.ListRunsByCreatorAgentWithAgent(ctx, db.ListRunsByCreatorAgentWithAgentParams{
		CreatorID: creatorID,
		AgentID:   agentID,
		Limit:     size,
		Offset:    offset,
	})
	if err != nil {
		log.Error().Err(err).Str("creator_id", creatorID.String()).Str("agent_id", agentID.String()).Msg("userdash.ListCreatorAgentRuns: ListRunsByCreatorAgentWithAgent")
		return nil, httpx.Internal("查询 Agent 调用历史失败")
	}
	total, err := s.queries.CountRunsByCreatorAgent(ctx, db.CountRunsByCreatorAgentParams{
		CreatorID: creatorID,
		AgentID:   agentID,
	})
	if err != nil {
		log.Error().Err(err).Str("creator_id", creatorID.String()).Str("agent_id", agentID.String()).Msg("userdash.ListCreatorAgentRuns: CountRunsByCreatorAgent")
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
		log.Error().Err(err).Str("user_id", userID.String()).Msg("userdash.GetUserDashboard: GetUserByID")
		return nil, httpx.Internal("查询用户失败")
	}
	if user.DisabledAt != nil {
		return nil, httpx.Unauthorized("账号已禁用")
	}

	usage, err := s.queries.GetUserDashboardUsage(ctx, userID)
	if err != nil {
		log.Error().Err(err).Msg("userdash.GetUserDashboard: GetUserDashboardUsage")
		return nil, httpx.Internal("查询用量失败")
	}

	resp := &UserDashboardResponse{
		IsCreator: user.IsCreator,
		Usage: UsageStats{
			ThisMonthCalls: usage.ThisMonthCalls,
			ThisMonthSpent: usage.ThisMonthSpent,
			TotalCalls:     usage.TotalCalls,
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
		log.Error().Err(err).Msg("userdash.GetUserDashboard: ListRunsByUserWithAgent recent")
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
		log.Error().Err(err).Str("user_id", creatorID.String()).Msg("userdash.GetCreatorDashboard: GetUserByID")
		return nil, httpx.Internal("查询用户失败")
	}
	if user.DisabledAt != nil {
		return nil, httpx.Unauthorized("账号已禁用")
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
		log.Error().Err(err).Str("user_id", creatorID.String()).Msg("userdash.GetCreatorDashboard: ListAgentStatsForCreator")
		return nil, httpx.Internal("查询 Agent 列表失败")
	}
	agents := make([]AgentStatsItem, 0, len(statRows))
	for i := range statRows {
		agents = append(agents, toAgentStatsItem(&statRows[i]))
	}
	return &CreatorDashboardResponse{Summary: *summary, Agents: agents}, nil
}

func (s *Service) buildCreatorSummary(ctx context.Context, creatorID uuid.UUID) (*CreatorSummary, error) {
	summary, err := s.queries.GetCreatorDashboardSummary(ctx, creatorID)
	if err != nil {
		log.Error().Err(err).Msg("userdash.buildCreatorSummary: GetCreatorDashboardSummary")
		return nil, httpx.Internal("查询创作者用量失败")
	}
	return &CreatorSummary{
		ThisMonthCalls:   summary.ThisMonthCalls,
		ThisMonthRevenue: summary.ThisMonthRevenue,
		TotalAgents:      summary.TotalAgents,
		PublicAgents:     summary.PublicAgents,
		PendingAgents:    summary.PendingAgents,
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

func normalizeCallRecordView(view string) string {
	switch strings.ToLower(strings.TrimSpace(view)) {
	case "made", "received", "all":
		return strings.ToLower(strings.TrimSpace(view))
	default:
		return "all"
	}
}

func normalizeCallRecordQuery(query string) string {
	query = strings.TrimSpace(query)
	runes := []rune(query)
	if len(runes) > 200 {
		return string(runes[:200])
	}
	return query
}

func normalizeCallRecordSort(sort string) string {
	switch strings.ToLower(strings.TrimSpace(sort)) {
	case "started_asc", "started_desc", "amount_asc", "amount_desc", "duration_asc", "duration_desc":
		return strings.ToLower(strings.TrimSpace(sort))
	default:
		return "started_desc"
	}
}

func normalizeCallRecordStatus(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "running", "success", "failed", "timeout", "canceled":
		return strings.ToLower(strings.TrimSpace(status))
	default:
		return ""
	}
}

func normalizeCallRecordSource(source string) string {
	switch strings.ToLower(strings.TrimSpace(source)) {
	case "web", "api", "mcp", "runtime", "a2a":
		return strings.ToLower(strings.TrimSpace(source))
	default:
		return ""
	}
}

func normalizeCallRecordRelation(relation string) string {
	switch strings.ToLower(strings.TrimSpace(relation)) {
	case "direct", "a2a_parent", "a2a_child":
		return strings.ToLower(strings.TrimSpace(relation))
	default:
		return ""
	}
}

func toRunListItem(r db.Run, agentSlug, agentName string) RunListItem {
	return RunListItem{
		ID:                   r.ID.String(),
		AgentID:              r.AgentID.String(),
		AgentSlug:            agentSlug,
		AgentName:            agentName,
		Status:               r.Status,
		CostCents:            r.CostCents,
		DurationMs:           r.DurationMs,
		StartedAt:            r.StartedAt.UTC().Format(time.RFC3339),
		Source:               r.Source,
		RuntimeContractID:    r.RuntimeContractID,
		DispatchState:        r.DispatchState,
		AttemptCount:         r.AttemptCount,
		MaxAttempts:          r.MaxAttempts,
		NextAttemptAt:        r.NextAttemptAt,
		LatestAttemptID:      optionalDashboardUUID(r.LatestAttemptID),
		ActiveAttemptID:      optionalDashboardUUID(r.ActiveAttemptID),
		CancelState:          optionalDashboardString(r.CancelState),
		CancelRequestedAt:    r.CancelRequestedAt,
		CancelAcknowledgedAt: r.CancelAcknowledgedAt,
		CancelReason:         optionalDashboardString(r.CancelReason),
		DeadLetteredAt:       r.DeadLetteredAt,
		ReplayOfRunID:        optionalDashboardUUID(r.ReplayOfRunID),
	}
}

func toCallRecordItem(r db.ListCallRecordsForUserRow) CallRecordItem {
	relation := "direct"
	if r.ParentRunID != "" {
		relation = "a2a_child"
	} else if r.ChildCount > 0 {
		relation = "a2a_parent"
	}

	finishedAt := ""
	if r.FinishedAt != nil {
		finishedAt = r.FinishedAt.UTC().Format(time.RFC3339)
	}

	target := CallRecordAgentRef{
		ID:   r.AgentID.String(),
		Slug: r.AgentSlug,
		Name: r.AgentName,
	}
	var caller *CallRecordAgentRef
	if r.CallerAgentID != "" || r.CallerAgentSlug != "" || r.CallerAgentName != "" {
		caller = &CallRecordAgentRef{
			ID:   r.CallerAgentID,
			Slug: r.CallerAgentSlug,
			Name: r.CallerAgentName,
		}
	}

	return CallRecordItem{
		ID:                        r.ID.String(),
		RunID:                     r.ID.String(),
		Direction:                 r.Direction,
		Relation:                  relation,
		AgentID:                   r.AgentID.String(),
		AgentSlug:                 r.AgentSlug,
		AgentName:                 r.AgentName,
		TargetAgent:               target,
		CallerAgent:               caller,
		Status:                    r.Status,
		CostCents:                 r.CostCents,
		CreatorRevenueCents:       r.CreatorRevenueCents,
		DurationMs:                r.DurationMs,
		StartedAt:                 r.StartedAt.UTC().Format(time.RFC3339),
		FinishedAt:                finishedAt,
		Source:                    r.Source,
		RuntimeContractID:         r.RuntimeContractID,
		AgentConnectionMode:       r.AgentConnectionMode,
		RuntimeTransport:          r.RuntimeTransport,
		RuntimeTransportReason:    r.RuntimeTransportReason,
		RuntimeTransportChangedAt: r.RuntimeTransportChangedAt,
		DispatchState:             r.DispatchState,
		AttemptCount:              r.AttemptCount,
		MaxAttempts:               r.MaxAttempts,
		NextAttemptAt:             r.NextAttemptAt,
		LatestAttemptID:           optionalDashboardUUID(r.LatestAttemptID),
		ActiveAttemptID:           optionalDashboardUUID(r.ActiveAttemptID),
		CancelState:               optionalDashboardString(r.CancelState),
		CancelRequestedAt:         r.CancelRequestedAt,
		CancelAcknowledgedAt:      r.CancelAcknowledgedAt,
		CancelReason:              optionalDashboardString(r.CancelReason),
		DeadLetteredAt:            r.DeadLetteredAt,
		ReplayOfRunID:             optionalDashboardUUID(r.ReplayOfRunID),
		ParentRunID:               r.ParentRunID,
		ChildCount:                r.ChildCount,
		CallID:                    firstNonEmpty(r.CallID, r.ProtocolTaskID, r.ID.String()),
		A2AContext:                toCallRecordA2AContext(r),
	}
}

func optionalDashboardUUID(value *uuid.UUID) string {
	if value == nil {
		return ""
	}
	return value.String()
}

func optionalDashboardString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func toCallRecordA2AContext(r db.ListCallRecordsForUserRow) *CallRecordA2AContext {
	if r.ProtocolContextID == "" && r.ProtocolTaskID == "" && r.RootContextID == "" &&
		r.ParentContextID == "" && r.ParentTaskID == "" && r.TraceID == "" &&
		len(r.ReferenceTaskIDs) == 0 && r.ContextSource == "" {
		return nil
	}
	callID := firstNonEmpty(r.CallID, r.ProtocolTaskID, r.ID.String())
	return &CallRecordA2AContext{
		SessionID:         firstNonEmpty(r.RootContextID, r.ProtocolContextID, r.TraceID),
		CallID:            callID,
		ProtocolContextID: r.ProtocolContextID,
		ProtocolTaskID:    r.ProtocolTaskID,
		RootContextID:     r.RootContextID,
		ParentContextID:   r.ParentContextID,
		ParentTaskID:      r.ParentTaskID,
		TraceID:           r.TraceID,
		ReferenceTaskIDs:  append([]string(nil), r.ReferenceTaskIDs...),
		Source:            r.ContextSource,
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
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
