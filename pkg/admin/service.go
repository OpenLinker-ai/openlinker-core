package admin

import (
	"context"
	"errors"
	"net/mail"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"
	"golang.org/x/crypto/bcrypt"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

const (
	defaultLimit = int32(25)
	maxLimit     = int32(100)
	bcryptCost   = 12
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
		TotalUsers:             row.TotalUsers,
		AdminUsers:             row.AdminUsers,
		CreatorUsers:           row.CreatorUsers,
		VerifiedCreators:       row.VerifiedCreators,
		TotalAgents:            row.TotalAgents,
		ActiveAgents:           row.ActiveAgents,
		DisabledAgents:         row.DisabledAgents,
		PendingAgents:          row.PendingAgents,
		CertifiedAgents:        row.CertifiedAgents,
		TotalTasks:             row.TotalTasks,
		PublicTasks:            row.PublicTasks,
		PrivateTasks:           row.PrivateTasks,
		OpenTasks:              row.OpenTasks,
		ClaimedTasks:           row.ClaimedTasks,
		CompletedTasks:         row.CompletedTasks,
		AcceptedTasks:          row.AcceptedTasks,
		RevisionRequestedTasks: row.RevisionRequestedTasks,
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
		item := toUserItem(&user.User)
		item.AgentCount = user.AgentCount
		item.ActiveAgentCount = user.ActiveAgentCount
		item.TaskCount = user.TaskCount
		item.PublicTaskCount = user.PublicTaskCount
		item.RunCount = user.RunCount
		item.LastTaskAt = timePtrString(user.LastTaskAt)
		item.LastRunAt = timePtrString(user.LastRunAt)
		out = append(out, item)
	}
	return &UserListResponse{Items: out, Total: total, Limit: limit, Offset: offset}, nil
}

func (s *Service) CreateUser(ctx context.Context, req *CreateUserRequest) (*UserItem, error) {
	if req == nil {
		return nil, httpx.BadRequest("请求体不能为空")
	}
	email := strings.ToLower(strings.TrimSpace(req.Email))
	displayName := strings.TrimSpace(req.DisplayName)
	password := req.Password

	if !validEmail(email) {
		return nil, httpx.Unprocessable("邮箱格式不正确")
	}
	if len(displayName) < 2 || len(displayName) > 50 {
		return nil, httpx.Unprocessable("显示名称长度需为 2-50 个字符")
	}
	if len(password) < 8 || len(password) > 72 {
		return nil, httpx.Unprocessable("密码长度需为 8-72 个字符")
	}

	isAdmin := req.IsAdmin
	isCreator := req.IsCreator
	creatorVerified := req.CreatorVerified
	if creatorVerified {
		isCreator = true
	}
	if !isCreator {
		creatorVerified = false
	}

	hashed, err := bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
	if err != nil {
		log.Error().Err(err).Msg("admin.CreateUser: bcrypt")
		return nil, httpx.Internal("密码处理失败")
	}
	hashStr := string(hashed)

	created, err := s.queries.CreateAdminUser(ctx, db.CreateAdminUserParams{
		Email:           email,
		PasswordHash:    &hashStr,
		DisplayName:     displayName,
		IsAdmin:         isAdmin,
		IsCreator:       isCreator,
		CreatorVerified: creatorVerified,
	})
	if err != nil {
		if isUniqueViolation(err) {
			return nil, httpx.Conflict("邮箱已注册")
		}
		log.Error().Err(err).Msg("admin.CreateUser")
		return nil, httpx.Internal("创建用户失败")
	}
	item := toUserItem(&created)
	return &item, nil
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
	disabled := current.DisabledAt != nil
	if req.IsAdmin != nil {
		isAdmin = *req.IsAdmin
	}
	if req.IsCreator != nil {
		isCreator = *req.IsCreator
	}
	if req.CreatorVerified != nil {
		creatorVerified = *req.CreatorVerified
	}
	if req.Disabled != nil {
		disabled = *req.Disabled
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
	if actorID == targetID && disabled {
		return nil, httpx.Unprocessable("不能禁用当前登录账号")
	}

	updated, err := s.queries.UpdateAdminUserFlags(ctx, db.UpdateAdminUserFlagsParams{
		ID:              targetID,
		IsAdmin:         isAdmin,
		IsCreator:       isCreator,
		CreatorVerified: creatorVerified,
		Disabled:        disabled,
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
		item.RecommendedTaskCount = row.RecommendedTaskCount
		item.ChosenTaskCount = row.ChosenTaskCount
		item.ClaimedTaskCount = row.ClaimedTaskCount
		item.CompletedTaskCount = row.CompletedTaskCount
		item.LastRunAt = timePtrString(row.LastRunAt)
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

func (s *Service) ListTasks(ctx context.Context, query, visibility, deliveryStatus, status string, limit, offset int32) (*TaskListResponse, error) {
	query = strings.TrimSpace(query)
	visibility = normalizeTaskFilter(visibility, taskVisibilityValues)
	deliveryStatus = normalizeTaskFilter(deliveryStatus, taskDeliveryStatusValues)
	status = normalizeTaskFilter(status, taskStatusValues)
	limit, offset = normalizePage(limit, offset)

	params := db.ListAdminTasksParams{
		Query:          query,
		Visibility:     visibility,
		DeliveryStatus: deliveryStatus,
		Status:         status,
		Limit:          limit,
		Offset:         offset,
	}
	items, err := s.queries.ListAdminTasks(ctx, params)
	if err != nil {
		log.Error().Err(err).Msg("admin.ListTasks")
		return nil, httpx.Internal("加载任务列表失败")
	}
	total, err := s.queries.CountAdminTasks(ctx, db.CountAdminTasksParams{
		Query:          query,
		Visibility:     visibility,
		DeliveryStatus: deliveryStatus,
		Status:         status,
	})
	if err != nil {
		log.Error().Err(err).Msg("admin.CountTasks")
		return nil, httpx.Internal("加载任务总数失败")
	}

	out := make([]TaskItem, 0, len(items))
	for _, row := range items {
		out = append(out, toTaskItem(&row))
	}
	return &TaskListResponse{Items: out, Total: total, Limit: limit, Offset: offset}, nil
}

var (
	lifecycleValues          = map[string]bool{"active": true, "disabled": true}
	visibilityValues         = map[string]bool{"public": true, "unlisted": true, "private": true}
	certificationValues      = map[string]bool{"unreviewed": true, "pending": true, "certified": true, "rejected": true}
	taskVisibilityValues     = map[string]bool{"private": true, "public": true}
	taskDeliveryStatusValues = map[string]bool{"pending": true, "submitted": true, "revision_requested": true, "accepted": true, "failed": true}
	taskStatusValues         = map[string]bool{"open": true, "matched": true, "in_progress": true, "completed": true, "needs_agent": true, "accepted": true, "revision_requested": true}
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
	case "", "admin", "creator", "creator_verified", "regular", "disabled":
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

func normalizeTaskFilter(value string, allowedValues map[string]bool) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if allowedValues[value] {
		return value
	}
	return ""
}

func allowed(value string, allowedValues map[string]bool) bool {
	return allowedValues[value]
}

func validEmail(email string) bool {
	if email == "" || len(email) > 120 || strings.ContainsAny(email, " \t\r\n") {
		return false
	}
	address, err := mail.ParseAddress(email)
	if err != nil {
		return false
	}
	return address.Address == email
}

func isUniqueViolation(err error) bool {
	type sqlState interface {
		SQLState() string
	}
	var state sqlState
	return errors.As(err, &state) && state.SQLState() == "23505"
}

func firstNonEmpty(value, fallback string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return fallback
	}
	return value
}

func toUserItem(user *db.User) UserItem {
	hasPassword, isOAuthUser, oauthProvider, authMethod := userAuthSummary(user.PasswordHash, user.OauthProvider)
	item := UserItem{
		ID:              user.ID.String(),
		Email:           user.Email,
		DisplayName:     user.DisplayName,
		HasPassword:     hasPassword,
		IsOAuthUser:     isOAuthUser,
		OAuthProvider:   oauthProvider,
		AuthMethod:      authMethod,
		IsCreator:       user.IsCreator,
		CreatorVerified: user.CreatorVerified,
		IsAdmin:         user.IsAdmin,
		Disabled:        user.DisabledAt != nil,
		DisabledAt:      timePtrString(user.DisabledAt),
		CreatedAt:       formatTime(user.CreatedAt),
		UpdatedAt:       formatTime(user.UpdatedAt),
	}
	if user.AvatarURL != nil {
		item.AvatarURL = *user.AvatarURL
	}
	return item
}

func userAuthSummary(passwordHash, oauthProvider *string) (bool, bool, string, string) {
	hasPassword := passwordHash != nil && strings.TrimSpace(*passwordHash) != ""
	provider := ""
	if oauthProvider != nil {
		provider = strings.TrimSpace(*oauthProvider)
	}
	isOAuthUser := provider != ""
	authMethod := "unknown"
	switch {
	case hasPassword && isOAuthUser:
		authMethod = "password_oauth"
	case hasPassword:
		authMethod = "password"
	case isOAuthUser:
		authMethod = "oauth"
	}
	return hasPassword, isOAuthUser, provider, authMethod
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

func toTaskItem(row *db.ListAdminTasksRow) TaskItem {
	task := &row.TaskQuery
	item := TaskItem{
		ID:                    task.ID.String(),
		UserID:                task.UserID.String(),
		Query:                 task.Query,
		Visibility:            task.Visibility,
		PublicSummary:         task.PublicSummary,
		ParsedSkills:          task.ParsedSkills,
		MCPTools:              task.MCPTools,
		RecommendedAgentCount: len(task.RecommendedAgentIDs),
		Status:                taskStatus(task),
		DeliveryStatus:        task.DeliveryStatus,
		DeliveryVisibility:    task.DeliveryVisibility,
		CompletionSummary:     task.CompletionSummary,
		RevisionNote:          task.RevisionNote,
		CreatedAt:             formatTime(task.CreatedAt),
		User: &TaskUser{
			ID:          task.UserID.String(),
			Email:       row.UserEmail,
			DisplayName: row.UserDisplayName,
		},
	}
	item.ChosenAgentID = uuidPtrString(task.ChosenAgentID)
	item.ChosenAt = timePtrString(task.ChosenAt)
	item.ClaimedAgentID = uuidPtrString(task.ClaimedAgentID)
	item.ClaimedByUserID = uuidPtrString(task.ClaimedByUserID)
	item.ClaimedAt = timePtrString(task.ClaimedAt)
	item.ClaimRunID = uuidPtrString(task.ClaimRunID)
	item.CompletedAt = timePtrString(task.CompletedAt)
	item.CompletionRunID = uuidPtrString(task.CompletionRunID)
	item.AcceptedAt = timePtrString(task.AcceptedAt)
	item.RevisionRequestedAt = timePtrString(task.RevisionRequestedAt)
	item.PublishedAt = timePtrString(task.PublishedAt)
	if task.ChosenAgentID != nil && row.ChosenAgentSlug != nil && row.ChosenAgentName != nil {
		item.ChosenAgent = &TaskAgent{ID: task.ChosenAgentID.String(), Slug: *row.ChosenAgentSlug, Name: *row.ChosenAgentName}
	}
	if task.ClaimedAgentID != nil && row.ClaimedAgentSlug != nil && row.ClaimedAgentName != nil {
		item.ClaimedAgent = &TaskAgent{ID: task.ClaimedAgentID.String(), Slug: *row.ClaimedAgentSlug, Name: *row.ClaimedAgentName}
	}
	if task.ClaimedByUserID != nil && row.ClaimedByEmail != nil && row.ClaimedByDisplayName != nil {
		item.ClaimedBy = &TaskUser{ID: task.ClaimedByUserID.String(), Email: *row.ClaimedByEmail, DisplayName: *row.ClaimedByDisplayName}
	}
	return item
}

func taskStatus(t *db.TaskQuery) string {
	switch {
	case t.DeliveryStatus == "accepted":
		return "accepted"
	case t.DeliveryStatus == "revision_requested":
		return "revision_requested"
	case t.CompletedAt != nil:
		return "completed"
	case t.ClaimedAgentID != nil:
		return "in_progress"
	case t.ChosenAgentID != nil:
		return "matched"
	case len(t.RecommendedAgentIDs) == 0:
		return "needs_agent"
	default:
		return "open"
	}
}

func uuidPtrString(value *uuid.UUID) *string {
	if value == nil {
		return nil
	}
	out := value.String()
	return &out
}

func timePtrString(value *time.Time) *string {
	if value == nil {
		return nil
	}
	out := formatTime(*value)
	return &out
}

func formatTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339)
}
