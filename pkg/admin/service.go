package admin

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

const (
	defaultLimit = int32(25)
	maxLimit     = int32(100)
)

type Service struct {
	queries *db.Queries
}

func NewService(dbtx db.DBTX) *Service {
	return &Service{queries: db.New(dbtx)}
}

func (s *Service) Summary(ctx context.Context) (*SummaryResponse, error) {
	row, err := s.queries.GetAdminSummary(ctx)
	if err != nil {
		log.Error().Err(err).Msg("admin.Summary")
		return nil, httpx.Internal("加载管理概览失败")
	}
	return &SummaryResponse{
		TotalUsers:       row.TotalUsers,
		AdminUsers:       row.AdminUsers,
		CreatorUsers:     row.CreatorUsers,
		VerifiedCreators: row.VerifiedCreators,
		TotalAgents:      row.TotalAgents,
		ActiveAgents:     row.ActiveAgents,
		DisabledAgents:   row.DisabledAgents,
		PendingAgents:    row.PendingAgents,
		CertifiedAgents:  row.CertifiedAgents,
	}, nil
}

func (s *Service) ListUsers(ctx context.Context, query, role string, limit, offset int32) (*UserListResponse, error) {
	query = strings.TrimSpace(query)
	role = normalizeUserRole(role)
	limit, offset = normalizePage(limit, offset)

	params := db.ListAdminUsersParams{Query: query, Role: role, Limit: limit, Offset: offset}
	items, err := s.queries.ListAdminUsers(ctx, params)
	if err != nil {
		log.Error().Err(err).Msg("admin.ListUsers")
		return nil, httpx.Internal("加载用户列表失败")
	}
	total, err := s.queries.CountAdminUsers(ctx, db.CountAdminUsersParams{Query: query, Role: role})
	if err != nil {
		log.Error().Err(err).Msg("admin.CountUsers")
		return nil, httpx.Internal("加载用户总数失败")
	}

	out := make([]UserItem, 0, len(items))
	for _, user := range items {
		out = append(out, toUserItem(&user))
	}
	return &UserListResponse{Items: out, Total: total, Limit: limit, Offset: offset}, nil
}

func (s *Service) UpdateUserFlags(ctx context.Context, actorID, targetID uuid.UUID, req *UpdateUserFlagsRequest) (*UserItem, error) {
	current, err := s.queries.GetUserByID(ctx, targetID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("用户不存在")
		}
		log.Error().Err(err).Msg("admin.UpdateUserFlags: get user")
		return nil, httpx.Internal("查询用户失败")
	}

	isAdmin := current.IsAdmin
	isCreator := current.IsCreator
	creatorVerified := current.CreatorVerified
	if req.IsAdmin != nil {
		isAdmin = *req.IsAdmin
	}
	if req.IsCreator != nil {
		isCreator = *req.IsCreator
	}
	if req.CreatorVerified != nil {
		creatorVerified = *req.CreatorVerified
	}
	if creatorVerified {
		isCreator = true
	}
	if !isCreator {
		creatorVerified = false
	}
	if actorID == targetID && current.IsAdmin && !isAdmin {
		return nil, httpx.Unprocessable("不能移除自己的管理员权限")
	}

	updated, err := s.queries.UpdateAdminUserFlags(ctx, db.UpdateAdminUserFlagsParams{
		ID:              targetID,
		IsAdmin:         isAdmin,
		IsCreator:       isCreator,
		CreatorVerified: creatorVerified,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("用户不存在")
		}
		log.Error().Err(err).Msg("admin.UpdateUserFlags")
		return nil, httpx.Internal("更新用户权限失败")
	}
	item := toUserItem(&updated)
	return &item, nil
}

func (s *Service) ListAgents(ctx context.Context, query, lifecycle, visibility, certification string, limit, offset int32) (*AgentListResponse, error) {
	query = strings.TrimSpace(query)
	lifecycle = normalizeAgentFilter(lifecycle, lifecycleValues)
	visibility = normalizeAgentFilter(visibility, visibilityValues)
	certification = normalizeAgentFilter(certification, certificationValues)
	limit, offset = normalizePage(limit, offset)

	params := db.ListAdminAgentsParams{
		Query:               query,
		LifecycleStatus:     lifecycle,
		Visibility:          visibility,
		CertificationStatus: certification,
		Limit:               limit,
		Offset:              offset,
	}
	items, err := s.queries.ListAdminAgents(ctx, params)
	if err != nil {
		log.Error().Err(err).Msg("admin.ListAgents")
		return nil, httpx.Internal("加载 Agent 列表失败")
	}
	total, err := s.queries.CountAdminAgents(ctx, db.CountAdminAgentsParams{
		Query:               query,
		LifecycleStatus:     lifecycle,
		Visibility:          visibility,
		CertificationStatus: certification,
	})
	if err != nil {
		log.Error().Err(err).Msg("admin.CountAgents")
		return nil, httpx.Internal("加载 Agent 总数失败")
	}

	out := make([]AgentItem, 0, len(items))
	for _, row := range items {
		item := toAgentItem(&row.Agent)
		item.Creator = &AgentCreator{
			ID:          row.CreatorID.String(),
			Email:       row.CreatorEmail,
			DisplayName: row.CreatorName,
		}
		out = append(out, item)
	}
	return &AgentListResponse{Items: out, Total: total, Limit: limit, Offset: offset}, nil
}

func (s *Service) UpdateAgentModeration(ctx context.Context, agentID uuid.UUID, req *UpdateAgentModerationRequest) (*AgentItem, error) {
	current, err := s.queries.GetAgentByID(ctx, agentID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("Agent 不存在")
		}
		log.Error().Err(err).Msg("admin.UpdateAgentModeration: get agent")
		return nil, httpx.Internal("查询 Agent 失败")
	}

	lifecycle := firstNonEmpty(req.LifecycleStatus, current.LifecycleStatus)
	visibility := firstNonEmpty(req.Visibility, current.Visibility)
	certification := firstNonEmpty(req.CertificationStatus, current.CertificationStatus)
	if !allowed(lifecycle, lifecycleValues) {
		return nil, httpx.Unprocessable("lifecycle_status 不合法")
	}
	if !allowed(visibility, visibilityValues) {
		return nil, httpx.Unprocessable("visibility 不合法")
	}
	if !allowed(certification, certificationValues) {
		return nil, httpx.Unprocessable("certification_status 不合法")
	}

	reason := strings.TrimSpace(req.RejectionReason)
	if certification == "rejected" && reason == "" {
		if current.RejectionReason == nil || strings.TrimSpace(*current.RejectionReason) == "" {
			return nil, httpx.Unprocessable("拒绝认证时需要填写原因")
		}
		reason = strings.TrimSpace(*current.RejectionReason)
	}

	updated, err := s.queries.UpdateAdminAgentModeration(ctx, db.UpdateAdminAgentModerationParams{
		ID:                  agentID,
		LifecycleStatus:     lifecycle,
		Visibility:          visibility,
		CertificationStatus: certification,
		RejectionReason:     reason,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("Agent 不存在")
		}
		log.Error().Err(err).Msg("admin.UpdateAgentModeration")
		return nil, httpx.Internal("更新 Agent 状态失败")
	}
	item := toAgentItem(&updated)
	return &item, nil
}

var (
	lifecycleValues     = map[string]bool{"active": true, "disabled": true}
	visibilityValues    = map[string]bool{"public": true, "unlisted": true, "private": true}
	certificationValues = map[string]bool{"unreviewed": true, "pending": true, "certified": true, "rejected": true}
)

func normalizePage(limit, offset int32) (int32, int32) {
	if limit <= 0 {
		limit = defaultLimit
	}
	if limit > maxLimit {
		limit = maxLimit
	}
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}

func normalizeUserRole(role string) string {
	role = strings.TrimSpace(strings.ToLower(role))
	switch role {
	case "", "admin", "creator", "creator_verified", "regular":
		return role
	default:
		return ""
	}
}

func normalizeAgentFilter(value string, allowedValues map[string]bool) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if allowedValues[value] {
		return value
	}
	return ""
}

func allowed(value string, allowedValues map[string]bool) bool {
	return allowedValues[value]
}

func firstNonEmpty(value, fallback string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return fallback
	}
	return value
}

func toUserItem(user *db.User) UserItem {
	item := UserItem{
		ID:              user.ID.String(),
		Email:           user.Email,
		DisplayName:     user.DisplayName,
		IsCreator:       user.IsCreator,
		CreatorVerified: user.CreatorVerified,
		IsAdmin:         user.IsAdmin,
		CreatedAt:       formatTime(user.CreatedAt),
		UpdatedAt:       formatTime(user.UpdatedAt),
	}
	if user.AvatarURL != nil {
		item.AvatarURL = *user.AvatarURL
	}
	return item
}

func toAgentItem(agent *db.Agent) AgentItem {
	item := AgentItem{
		ID:                  agent.ID.String(),
		CreatorID:           agent.CreatorID.String(),
		Slug:                agent.Slug,
		Name:                agent.Name,
		Description:         agent.Description,
		EndpointURL:         agent.EndpointURL,
		PricePerCallCents:   agent.PricePerCallCents,
		Tags:                agent.Tags,
		LifecycleStatus:     agent.LifecycleStatus,
		Visibility:          agent.Visibility,
		CertificationStatus: agent.CertificationStatus,
		RejectionReason:     agent.RejectionReason,
		TotalCalls:          agent.TotalCalls,
		TotalRevenueCents:   agent.TotalRevenueCents,
		ConnectionMode:      agent.ConnectionMode,
		MCPToolName:         agent.MCPToolName,
		CreatedAt:           formatTime(agent.CreatedAt),
		UpdatedAt:           formatTime(agent.UpdatedAt),
	}
	if agent.CertifiedAt != nil {
		value := formatTime(*agent.CertifiedAt)
		item.CertifiedAt = &value
	}
	return item
}

func formatTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339)
}
