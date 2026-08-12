package a2a

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
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

const (
	maxDelegationDepth    = 8
	defaultParentPage     = 1
	defaultParentPageSize = 10
	maxParentPageSize     = 50
	maxParentSearchLen    = 120
)

type Service struct {
	queries             *db.Queries
	pool                *pgxpool.Pool
	runtime             *runtime.Service
	runtimePresence     runtime.RuntimePresenceStore
	runUpdates          runtime.RunUpdateSource
	taskCallbackManager taskCallbackManager
	maxDelegationDepth  int
}

// SetRunUpdateSource enables advisory event-driven waits for blocking A2A
// message/send requests. PostgreSQL remains the authoritative Run state.
func (s *Service) SetRunUpdateSource(source runtime.RunUpdateSource) {
	s.runUpdates = source
}

// SetRuntimePresenceStore lets owner diagnostics reuse the same expiring
// lease-backed presence that Runtime dispatch uses. PostgreSQL identity,
// attachment, contract, Node and credential rows remain authoritative.
func (s *Service) SetRuntimePresenceStore(store runtime.RuntimePresenceStore) {
	s.runtimePresence = store
}

func NewService(pool *pgxpool.Pool, runtimeSvc *runtime.Service) *Service {
	return &Service{
		queries:            db.New(pool),
		pool:               pool,
		runtime:            runtimeSvc,
		maxDelegationDepth: maxDelegationDepth,
	}
}

func isQueuedRuntimeConnectionMode(mode string) bool {
	return mode == "runtime"
}

func (s *Service) GetCallPolicy(ctx context.Context, userID, agentID uuid.UUID) (*CallPolicyResponse, error) {
	if _, err := s.ownerAgent(ctx, userID, agentID); err != nil {
		return nil, err
	}
	policy, err := s.queries.GetAgentCallPolicy(ctx, agentID)
	if err != nil {
		return nil, httpx.Internal("查询 A2A 策略失败")
	}
	return &CallPolicyResponse{AgentID: agentID.String(), CallableBy: policy}, nil
}

func (s *Service) UpdateCallPolicy(ctx context.Context, userID, agentID uuid.UUID, req *UpdateCallPolicyRequest) (*CallPolicyResponse, error) {
	policy, err := s.queries.UpsertAgentCallPolicyForOwner(ctx, db.UpsertAgentCallPolicyForOwnerParams{
		AgentID: agentID, UserID: userID, CallableBy: req.CallableBy,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("Agent 不存在")
	}
	if err != nil {
		return nil, httpx.Internal("更新 A2A 策略失败")
	}
	return &CallPolicyResponse{
		AgentID:    policy.AgentID.String(),
		CallableBy: policy.CallableBy,
		UpdatedAt:  policy.UpdatedAt.UTC().Format(time.RFC3339),
	}, nil
}

func (s *Service) ListChildren(ctx context.Context, userID, parentRunID uuid.UUID) ([]ChildRunResponse, error) {
	maxDepth := s.maxDelegationDepth
	if maxDepth <= 0 {
		maxDepth = maxDelegationDepth
	}
	return s.listChildrenTree(ctx, userID, parentRunID, maxDepth, map[uuid.UUID]struct{}{})
}

func (s *Service) listChildrenTree(ctx context.Context, userID, parentRunID uuid.UUID, remainingDepth int, seen map[uuid.UUID]struct{}) ([]ChildRunResponse, error) {
	if remainingDepth <= 0 {
		return []ChildRunResponse{}, nil
	}
	if _, ok := seen[parentRunID]; ok {
		return []ChildRunResponse{}, nil
	}
	seen[parentRunID] = struct{}{}
	rows, err := s.queries.ListChildRunsByParentAndUser(ctx, db.ListChildRunsByParentAndUserParams{
		ParentRunID: parentRunID, UserID: userID,
	})
	if err != nil {
		return nil, httpx.Internal("查询 Agent 协作运行失败")
	}
	items := make([]ChildRunResponse, 0, len(rows))
	for _, row := range rows {
		item := childRunResponseFromRow(row)
		children, err := s.listChildrenTree(ctx, userID, row.ChildRunID, remainingDepth-1, seen)
		if err != nil {
			return nil, err
		}
		if len(children) > 0 {
			item.Children = children
		}
		items = append(items, item)
	}
	return items, nil
}

func (s *Service) ListParentRuns(ctx context.Context, userID uuid.UUID, page, size int32, search string) (*ParentRunListResponse, error) {
	if page < 1 {
		page = defaultParentPage
	}
	if size < 1 {
		size = defaultParentPageSize
	}
	if size > maxParentPageSize {
		size = maxParentPageSize
	}
	search = normalizeParentSearch(search)
	rows, err := s.queries.ListParentRunsWithDelegationsByUser(ctx, db.ListParentRunsWithDelegationsByUserParams{
		UserID: userID,
		Search: search,
		Limit:  size,
		Offset: (page - 1) * size,
	})
	if err != nil {
		log.Error().Err(err).Str("user_id", userID.String()).Msg("a2a.ListParentRuns: list")
		return nil, httpx.Internal("查询 Parent 调用链失败")
	}
	total, err := s.queries.CountParentRunsWithDelegationsByUser(ctx, db.CountParentRunsWithDelegationsByUserParams{
		UserID: userID,
		Search: search,
	})
	if err != nil {
		log.Error().Err(err).Str("user_id", userID.String()).Msg("a2a.ListParentRuns: count")
		return nil, httpx.Internal("查询 Parent 调用链失败")
	}
	items := make([]ParentRunSummary, 0, len(rows))
	for _, row := range rows {
		item := ParentRunSummary{
			ParentRunID: row.ParentRunID.String(), CallerAgentID: row.CallerAgentID.String(),
			CallerAgentSlug: row.CallerAgentSlug, CallerAgentName: row.CallerAgentName,
			CallerAgentTags: row.CallerAgentTags, CallerSkills: skillRefs(row.CallerSkillIDs, row.CallerSkillNames),
			Source: row.ParentSource, ActiveAgentTokenCount: row.ActiveRuntimeTokenCount,
			Status: row.Status, DurationMs: row.DurationMs, StartedAt: row.StartedAt.UTC().Format(time.RFC3339),
			ChildCount: row.ChildCount, SuccessfulChildCount: row.SuccessfulChildCount,
			RunningChildCount: row.RunningChildCount,
			A2AContext:        contextRefFromParentRow(row),
		}
		if row.FinishedAt != nil {
			formatted := row.FinishedAt.UTC().Format(time.RFC3339)
			item.FinishedAt = &formatted
		}
		if row.LastRuntimeTokenUsedAt != nil {
			formatted := row.LastRuntimeTokenUsedAt.UTC().Format(time.RFC3339)
			item.LastAgentTokenUsedAt = &formatted
		}
		items = append(items, item)
	}
	return &ParentRunListResponse{Items: items, Total: total, Page: page, Size: size}, nil
}

func normalizeParentSearch(search string) string {
	search = strings.TrimSpace(search)
	runes := []rune(search)
	if len(runes) > maxParentSearchLen {
		return string(runes[:maxParentSearchLen])
	}
	return search
}

func skillRefs(ids, names []string) []SkillRef {
	if len(ids) == 0 {
		return []SkillRef{}
	}
	items := make([]SkillRef, 0, len(ids))
	for i, id := range ids {
		name := id
		if i < len(names) && strings.TrimSpace(names[i]) != "" {
			name = names[i]
		}
		items = append(items, SkillRef{ID: id, Name: name})
	}
	return items
}

func childRunResponseFromRow(row db.ListChildRunsByParentAndUserRow) ChildRunResponse {
	item := ChildRunResponse{
		ChildRunID: row.ChildRunID.String(), ParentRunID: row.ParentRunID.String(),
		CallerAgentID: row.CallerAgentID.String(), TargetAgentID: row.TargetAgentID.String(),
		CallerAgentSlug: row.CallerAgentSlug, CallerAgentName: row.CallerAgentName,
		CallerAgentTags: row.CallerAgentTags, CallerSkills: skillRefs(row.CallerSkillIDs, row.CallerSkillNames),
		TargetAgentSlug: row.TargetAgentSlug, TargetAgentName: row.TargetAgentName,
		TargetAgentTags: row.TargetAgentTags, TargetSkills: skillRefs(row.TargetSkillIDs, row.TargetSkillNames),
		Reason: row.Reason, Status: row.Status, CostCents: row.CostCents,
		DurationMs: row.DurationMs, StartedAt: row.StartedAt.UTC().Format(time.RFC3339),
		Source: row.Source, BillingMode: "free_delegation",
		A2AContext: contextRefFromChildRow(row),
	}
	if row.FinishedAt != nil {
		formatted := row.FinishedAt.UTC().Format(time.RFC3339)
		item.FinishedAt = &formatted
	}
	return item
}

func contextRefFromChildRow(row db.ListChildRunsByParentAndUserRow) *A2AContextRef {
	if row.ProtocolContextID == "" && row.RootContextID == "" && row.TraceID == "" {
		return nil
	}
	return &A2AContextRef{
		ProtocolContextID: row.ProtocolContextID,
		ProtocolTaskID:    row.ProtocolTaskID,
		RootContextID:     row.RootContextID,
		ParentContextID:   row.ParentContextID,
		ParentTaskID:      row.ParentTaskID,
		TraceID:           row.TraceID,
		ReferenceTaskIDs:  row.ReferenceTaskIDs,
		Source:            row.ContextSource,
	}
}

func contextRefFromParentRow(row db.ListParentRunsWithDelegationsByUserRow) *A2AContextRef {
	if row.ProtocolContextID == "" && row.RootContextID == "" && row.TraceID == "" {
		return nil
	}
	return &A2AContextRef{
		ProtocolContextID: row.ProtocolContextID,
		ProtocolTaskID:    row.ProtocolTaskID,
		RootContextID:     row.RootContextID,
		TraceID:           row.TraceID,
	}
}

func (s *Service) ownerAgent(ctx context.Context, userID, agentID uuid.UUID) (db.Agent, error) {
	agent, err := s.queries.GetAgentByIDForOwner(ctx, db.GetAgentByIDForOwnerParams{ID: agentID, CreatorID: userID})
	if errors.Is(err, pgx.ErrNoRows) {
		return db.Agent{}, httpx.NotFound("Agent 不存在")
	}
	if err != nil {
		return db.Agent{}, httpx.Internal("查询 Agent 失败")
	}
	return agent, nil
}
