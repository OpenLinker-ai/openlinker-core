package usertoken

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	"github.com/OpenLinker-ai/openlinker-core/pkg/auth"
	"github.com/OpenLinker-ai/openlinker-core/pkg/credential"
	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

const (
	maxActiveUserTokens = int32(10)
	maxCoreGrants       = 64
)

var ErrInvalidToken = errors.New("invalid user token")

var permissionResourceTypes = map[string]string{
	"agents:read":         "agent",
	"agents:run":          "agent",
	"agents:create":       "agent",
	"runs:read":           "run",
	"runs:cancel":         "run",
	"tasks:read":          "task",
	"tasks:create":        "task",
	"tasks:run":           "task",
	"workflows:read":      "workflow",
	"workflows:manage":    "workflow",
	"workflows:run":       "workflow",
	"agent-tokens:read":   "agent",
	"agent-tokens:issue":  "agent",
	"agent-tokens:revoke": "agent",
}

var resourceScopedPermissions = map[string]bool{
	"agents:run":          true,
	"workflows:read":      true,
	"workflows:manage":    true,
	"workflows:run":       true,
	"agent-tokens:read":   true,
	"agent-tokens:issue":  true,
	"agent-tokens:revoke": true,
}

var allowedListSorts = map[string]bool{
	"created_at": true, "last_used_at": true, "expires_at": true, "name": true,
}

type Service struct {
	pool    *pgxpool.Pool
	queries *db.Queries
	now     func() time.Time
}

func NewService(pool *pgxpool.Pool) *Service {
	return &Service{pool: pool, queries: db.New(pool), now: time.Now}
}

func (s *Service) Create(ctx context.Context, userID uuid.UUID, req *CreateRequest) (*TokenResponse, error) {
	if req == nil {
		return nil, httpx.BadRequest("请求体不能为空")
	}
	name, err := normalizeName(req.Name)
	if err != nil {
		return nil, err
	}
	grants, err := grantsFromCreateRequest(req)
	if err != nil {
		return nil, err
	}
	if err := s.validateGrantOwnership(ctx, userID, grants); err != nil {
		return nil, err
	}
	if req.ExpiresAt != nil && !req.ExpiresAt.After(s.now()) {
		return nil, httpx.Unprocessable("expires_at 必须晚于当前时间")
	}
	issuer, err := s.issuer(ctx)
	if err != nil {
		return nil, err
	}

	plaintext, prefix, err := credential.GenerateUserToken()
	if err != nil {
		return nil, httpx.Internal("生成 User Token 失败")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, httpx.Internal("数据库事务失败")
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := s.queries.WithTx(tx)
	// Serialize creation per user so concurrent requests cannot both observe
	// nine active tokens and create an eleventh one.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, userID.String()); err != nil {
		return nil, httpx.Internal("锁定 User Token 配额失败")
	}
	count, err := q.CountActiveUserTokensByUser(ctx, userID)
	if err != nil {
		log.Error().Err(err).Str("user_id", userID.String()).Msg("usertoken.Create: count")
		return nil, httpx.Internal("查询 User Token 失败")
	}
	if count >= maxActiveUserTokens {
		return nil, httpx.Conflict("有效 User Token 数量已达上限（10 个）")
	}
	token, err := q.CreateUserToken(ctx, db.CreateUserTokenParams{
		UserID: userID, Name: name, Prefix: prefix,
		TokenHash: credential.FastTokenHash(plaintext),
		Scopes:    permissionsFromGrants(grants),
		ExpiresAt: req.ExpiresAt,
	})
	if err != nil {
		log.Error().Err(err).Str("user_id", userID.String()).Msg("usertoken.Create: insert")
		return nil, httpx.Internal("创建 User Token 失败")
	}
	createdGrants, err := replaceGrants(ctx, q, token.ID, grants)
	if err != nil {
		log.Error().Err(err).Str("token_id", token.ID.String()).Msg("usertoken.Create: grants")
		return nil, httpx.Internal("保存 User Token 权限失败")
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, httpx.Internal("提交 User Token 失败")
	}
	resp := tokenResponse(token, createdGrants, issuer)
	resp.PlaintextToken = plaintext
	return &resp, nil
}

func (s *Service) List(ctx context.Context, userID uuid.UUID, opts ListOptions) (*ListResponse, error) {
	opts = normalizeListOptions(opts)
	tokens, err := s.queries.ListUserTokensByUser(ctx, db.ListUserTokensByUserParams{
		UserID: userID, Limit: opts.Limit, Offset: opts.Offset, SortBy: opts.SortBy, SortDir: opts.SortDir,
	})
	if err != nil {
		return nil, httpx.Internal("查询 User Token 失败")
	}
	total, err := s.queries.CountUserTokensByUser(ctx, userID)
	if err != nil {
		return nil, httpx.Internal("查询 User Token 失败")
	}
	issuer, err := s.issuer(ctx)
	if err != nil {
		return nil, err
	}
	items := make([]TokenResponse, 0, len(tokens))
	for _, token := range tokens {
		grants, err := s.queries.ListUserTokenCoreGrants(ctx, token.ID)
		if err != nil {
			return nil, httpx.Internal("查询 User Token 权限失败")
		}
		items = append(items, tokenResponse(token, grants, issuer))
	}
	return &ListResponse{
		Items: items, Total: total, Limit: opts.Limit, Offset: opts.Offset,
		SortBy: opts.SortBy, SortDir: opts.SortDir,
		HasMore: opts.Offset+int32(len(items)) < total,
	}, nil
}

func (s *Service) Get(ctx context.Context, userID, tokenID uuid.UUID) (*TokenResponse, error) {
	token, err := s.queries.GetUserTokenByIDForUser(ctx, db.GetUserTokenByIDForUserParams{ID: tokenID, UserID: userID})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("User Token 不存在")
	}
	if err != nil {
		return nil, httpx.Internal("查询 User Token 失败")
	}
	grants, err := s.queries.ListUserTokenCoreGrants(ctx, token.ID)
	if err != nil {
		return nil, httpx.Internal("查询 User Token 权限失败")
	}
	issuer, err := s.issuer(ctx)
	if err != nil {
		return nil, err
	}
	resp := tokenResponse(token, grants, issuer)
	return &resp, nil
}

func (s *Service) Update(ctx context.Context, userID, tokenID uuid.UUID, req *UpdateRequest) (*TokenResponse, error) {
	if req == nil {
		return nil, httpx.BadRequest("请求体不能为空")
	}
	if req.Grants != nil && req.Scopes != nil {
		return nil, httpx.Unprocessable("grants 与 legacy scopes 不能同时提交")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, httpx.Internal("数据库事务失败")
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, tokenID.String()); err != nil {
		return nil, httpx.Internal("锁定 User Token 更新失败")
	}
	q := s.queries.WithTx(tx)
	token, err := q.GetUserTokenByIDForUser(ctx, db.GetUserTokenByIDForUserParams{ID: tokenID, UserID: userID})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("User Token 不存在")
	}
	if err != nil {
		return nil, httpx.Internal("查询 User Token 失败")
	}
	if token.RevokedAt != nil {
		return nil, httpx.Conflict("已撤销的 User Token 不能修改")
	}
	current, err := q.ListUserTokenCoreGrants(ctx, token.ID)
	if err != nil {
		return nil, httpx.Internal("查询 User Token 权限失败")
	}

	name := token.Name
	if req.Name != nil {
		name, err = normalizeName(*req.Name)
		if err != nil {
			return nil, err
		}
	}
	next := dbGrantsToAuth(current)
	grantsChanged := false
	if req.Grants != nil {
		next, err = normalizeGrantRequests(*req.Grants)
		grantsChanged = true
	} else if req.Scopes != nil {
		next, err = grantsFromLegacyScopes(*req.Scopes)
		grantsChanged = true
	}
	if err != nil {
		return nil, err
	}
	if grantsChanged && !isGrantShrink(dbGrantsToAuth(current), next) {
		return nil, expansionConflict("TOKEN_PERMISSION_EXPANSION")
	}
	if err := s.validateGrantOwnership(ctx, userID, next); err != nil {
		return nil, err
	}
	expiresAt := token.ExpiresAt
	if req.ExpiresAt != nil {
		if token.ExpiresAt != nil && req.ExpiresAt.After(*token.ExpiresAt) {
			return nil, expansionConflict("TOKEN_EXPIRY_EXTENSION")
		}
		expiresAt = req.ExpiresAt
	}

	updated, err := q.UpdateUserTokenMetadata(ctx, db.UpdateUserTokenMetadataParams{
		ID: token.ID, UserID: userID, Name: name,
		Scopes: permissionsFromGrants(next), ExpiresAt: expiresAt,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.Conflict("User Token 已撤销")
	}
	if err != nil {
		return nil, httpx.Internal("更新 User Token 失败")
	}
	updatedGrants := current
	if grantsChanged {
		updatedGrants, err = replaceGrants(ctx, q, token.ID, next)
		if err != nil {
			return nil, httpx.Internal("更新 User Token 权限失败")
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, httpx.Internal("提交 User Token 更新失败")
	}
	issuer, err := s.issuer(ctx)
	if err != nil {
		return nil, err
	}
	resp := tokenResponse(updated, updatedGrants, issuer)
	return &resp, nil
}

func (s *Service) Revoke(ctx context.Context, userID, tokenID uuid.UUID) error {
	affected, err := s.queries.RevokeUserTokenForUser(ctx, db.RevokeUserTokenForUserParams{ID: tokenID, UserID: userID})
	if err != nil {
		return httpx.Internal("撤销 User Token 失败")
	}
	if affected == 0 {
		return httpx.NotFound("User Token 不存在或已撤销")
	}
	return nil
}

// Verify keeps the one-release verifier interface compatible while all new
// authorization reads the structured principal from VerifyPrincipal.
func (s *Service) Verify(ctx context.Context, plaintext string) (uuid.UUID, []string, error) {
	principal, err := s.VerifyPrincipal(ctx, plaintext)
	if err != nil {
		return uuid.Nil, nil, err
	}
	return principal.UserID, principal.Permissions(), nil
}

func (s *Service) VerifyPrincipal(ctx context.Context, plaintext string) (*auth.AuthPrincipal, error) {
	plaintext = strings.TrimSpace(plaintext)
	if !credential.HasAnyPrefix(plaintext, credential.UserTokenPrefix) ||
		!credential.ValidLengthForPrefix(plaintext, credential.UserTokenPrefix) {
		return nil, ErrInvalidToken
	}
	candidates, err := s.queries.ListActiveUserTokensByPrefix(ctx, plaintext[:credential.PrefixLen])
	if err != nil {
		log.Warn().Err(err).Msg("usertoken.Verify: prefix candidates")
		return nil, ErrInvalidToken
	}
	var matched *db.UserToken
	for i := range candidates {
		if credential.VerifyTokenHash(candidates[i].TokenHash, plaintext) {
			matched = &candidates[i]
			break
		}
	}
	if matched == nil {
		return nil, ErrInvalidToken
	}
	user, err := s.queries.GetUserByID(ctx, matched.UserID)
	if err != nil || user.DisabledAt != nil || user.DeletedAt != nil {
		return nil, ErrInvalidToken
	}
	grants, err := s.queries.ListUserTokenCoreGrants(ctx, matched.ID)
	if err != nil {
		return nil, ErrInvalidToken
	}
	issuer, err := s.queries.GetCoreIssuerInstanceID(ctx)
	if err != nil {
		return nil, ErrInvalidToken
	}
	if err := s.queries.TouchUserToken(ctx, matched.ID); err != nil {
		return nil, ErrInvalidToken
	}
	tokenID := matched.ID
	return &auth.AuthPrincipal{
		UserID: matched.UserID, AuthMethod: auth.AuthMethodUserToken,
		TokenID: &tokenID, IssuerInstanceID: issuer, Grants: dbGrantsToAuth(grants),
		UserStatusVerified: true,
	}, nil
}

func (s *Service) Introspect(ctx context.Context, plaintext string) IntrospectionResponse {
	principal, err := s.VerifyPrincipal(ctx, plaintext)
	if err != nil || principal == nil || principal.TokenID == nil {
		return IntrospectionResponse{Active: false}
	}
	token, err := s.queries.GetUserTokenByIDForUser(ctx, db.GetUserTokenByIDForUserParams{ID: *principal.TokenID, UserID: principal.UserID})
	if err != nil {
		return IntrospectionResponse{Active: false}
	}
	grants := authGrantsToResponses(principal.Grants)
	return IntrospectionResponse{
		Active: true, IssuerInstanceID: principal.IssuerInstanceID,
		TokenID: principal.TokenID.String(), UserID: principal.UserID.String(),
		Permissions: principal.Permissions(), Grants: grants, ExpiresAt: formatTime(token.ExpiresAt),
	}
}

func (s *Service) issuer(ctx context.Context) (string, error) {
	issuer, err := s.queries.GetCoreIssuerInstanceID(ctx)
	if err != nil {
		log.Error().Err(err).Msg("usertoken: issuer identity")
		return "", httpx.Internal("Core 实例标识不可用")
	}
	return issuer, nil
}

func (s *Service) validateGrantOwnership(ctx context.Context, userID uuid.UUID, grants []auth.Grant) error {
	for _, grant := range grants {
		if grant.ResourceID == nil {
			continue
		}
		switch grant.ResourceType {
		case "agent":
			if grant.Permission == "agents:run" {
				target, err := s.queries.GetAgentByID(ctx, *grant.ResourceID)
				if errors.Is(err, pgx.ErrNoRows) {
					return httpx.Unprocessable("resource_id 对应的 Agent 不存在")
				}
				if err != nil {
					return httpx.Internal("校验 Agent 权限范围失败")
				}
				if target.CreatorID != userID && (target.Visibility == "private" || target.LifecycleStatus == "disabled") {
					return httpx.Unprocessable("resource_id 对应的 Agent 当前不可访问")
				}
				continue
			}
			if _, err := s.queries.GetAgentByIDForOwner(ctx, db.GetAgentByIDForOwnerParams{ID: *grant.ResourceID, CreatorID: userID}); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return httpx.Unprocessable("resource_id 不是当前用户拥有的 Agent")
				}
				return httpx.Internal("校验 Agent 权限范围失败")
			}
		case "workflow":
			workflow, err := s.queries.GetWorkflowByID(ctx, *grant.ResourceID)
			if err != nil || workflow.UserID != userID {
				if errors.Is(err, pgx.ErrNoRows) || err == nil {
					return httpx.Unprocessable("resource_id 不是当前用户拥有的 Workflow")
				}
				return httpx.Internal("校验 Workflow 权限范围失败")
			}
		}
	}
	return nil
}

func grantsFromCreateRequest(req *CreateRequest) ([]auth.Grant, error) {
	if req.Grants != nil && req.Scopes != nil {
		return nil, httpx.Unprocessable("grants 与 legacy scopes 不能同时提交")
	}
	if req.Scopes != nil {
		return grantsFromLegacyScopes(req.Scopes)
	}
	return normalizeGrantRequests(req.Grants)
}

func normalizeGrantRequests(input []GrantRequest) ([]auth.Grant, error) {
	if len(input) > maxCoreGrants {
		return nil, httpx.Unprocessable("Core grant 数量不能超过 64")
	}
	seen := make(map[string]bool, len(input))
	out := make([]auth.Grant, 0, len(input))
	for _, in := range input {
		permission := strings.TrimSpace(in.Permission)
		expectedType, ok := permissionResourceTypes[permission]
		if !ok {
			return nil, httpx.Unprocessable("未知 Core permission: " + permission)
		}
		resourceType := strings.TrimSpace(in.ResourceType)
		if resourceType == "" {
			resourceType = expectedType
		}
		if resourceType != expectedType {
			return nil, httpx.Unprocessable("permission 与 resource_type 不匹配: " + permission)
		}
		var resourceID *uuid.UUID
		if in.ResourceID != nil && strings.TrimSpace(*in.ResourceID) != "" {
			if !resourceScopedPermissions[permission] {
				return nil, httpx.Unprocessable(permission + " 第一版只支持 wildcard 资源范围")
			}
			parsed, err := uuid.Parse(strings.TrimSpace(*in.ResourceID))
			if err != nil {
				return nil, httpx.Unprocessable("resource_id 不是合法 uuid")
			}
			resourceID = &parsed
		}
		if len(in.Constraints) > 0 {
			return nil, httpx.Unprocessable("constraints 已保留但第一版尚未启用")
		}
		key := permission + "|" + resourceType + "|"
		if resourceID != nil {
			key += resourceID.String()
		}
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, auth.Grant{
			Permission: permission, ResourceType: resourceType,
			ResourceID: resourceID, Constraints: json.RawMessage(`{}`),
		})
	}
	sort.Slice(out, func(i, j int) bool { return grantKey(out[i]) < grantKey(out[j]) })
	return out, nil
}

func grantsFromLegacyScopes(scopes []string) ([]auth.Grant, error) {
	requests := make([]GrantRequest, 0, len(scopes))
	for _, raw := range scopes {
		permission := strings.TrimSpace(raw)
		requests = append(requests, GrantRequest{Permission: permission})
	}
	return normalizeGrantRequests(requests)
}

func isGrantShrink(current, next []auth.Grant) bool {
	for _, candidate := range next {
		covered := false
		for _, existing := range current {
			if existing.Permission != candidate.Permission || existing.ResourceType != candidate.ResourceType {
				continue
			}
			if existing.ResourceID == nil || (candidate.ResourceID != nil && *existing.ResourceID == *candidate.ResourceID) {
				covered = true
				break
			}
		}
		if !covered {
			return false
		}
	}
	return true
}

func replaceGrants(ctx context.Context, q *db.Queries, tokenID uuid.UUID, grants []auth.Grant) ([]db.UserTokenCoreGrant, error) {
	if err := q.DeleteUserTokenCoreGrants(ctx, tokenID); err != nil {
		return nil, err
	}
	out := make([]db.UserTokenCoreGrant, 0, len(grants))
	for _, grant := range grants {
		constraints := grant.Constraints
		if len(constraints) == 0 {
			constraints = json.RawMessage(`{}`)
		}
		created, err := q.CreateUserTokenCoreGrant(ctx, db.CreateUserTokenCoreGrantParams{
			TokenID: tokenID, Permission: grant.Permission, ResourceType: grant.ResourceType,
			ResourceID: grant.ResourceID, Constraints: constraints,
		})
		if err != nil {
			return nil, err
		}
		out = append(out, created)
	}
	return out, nil
}

func dbGrantsToAuth(grants []db.UserTokenCoreGrant) []auth.Grant {
	out := make([]auth.Grant, 0, len(grants))
	for _, grant := range grants {
		constraints := json.RawMessage(grant.Constraints)
		if len(constraints) == 0 {
			constraints = json.RawMessage(`{}`)
		}
		out = append(out, auth.Grant{
			Permission: grant.Permission, ResourceType: grant.ResourceType,
			ResourceID: grant.ResourceID, Constraints: constraints,
		})
	}
	return out
}

func permissionsFromGrants(grants []auth.Grant) []string {
	seen := make(map[string]bool, len(grants))
	out := make([]string, 0, len(grants))
	for _, grant := range grants {
		if !seen[grant.Permission] {
			seen[grant.Permission] = true
			out = append(out, grant.Permission)
		}
	}
	sort.Strings(out)
	return out
}

func tokenResponse(token db.UserToken, grants []db.UserTokenCoreGrant, issuer string) TokenResponse {
	authGrants := dbGrantsToAuth(grants)
	return TokenResponse{
		ID: token.ID.String(), UserID: token.UserID.String(), IssuerInstanceID: issuer,
		Name: token.Name, Prefix: token.Prefix,
		Grants: authGrantsToResponses(authGrants), Scopes: permissionsFromGrants(authGrants),
		ExpiresAt: formatTime(token.ExpiresAt), LastUsedAt: formatTime(token.LastUsedAt),
		RevokedAt: formatTime(token.RevokedAt), CreatedAt: token.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt: token.UpdatedAt.UTC().Format(time.RFC3339),
	}
}

func authGrantsToResponses(grants []auth.Grant) []GrantResponse {
	out := make([]GrantResponse, 0, len(grants))
	for _, grant := range grants {
		var resourceID *string
		if grant.ResourceID != nil {
			value := grant.ResourceID.String()
			resourceID = &value
		}
		constraints := map[string]any{}
		if len(grant.Constraints) > 0 {
			_ = json.Unmarshal(grant.Constraints, &constraints)
		}
		out = append(out, GrantResponse{
			Permission: grant.Permission, ResourceType: grant.ResourceType,
			ResourceID: resourceID, Constraints: constraints,
		})
	}
	return out
}

func formatTime(value *time.Time) *string {
	if value == nil {
		return nil
	}
	formatted := value.UTC().Format(time.RFC3339)
	return &formatted
}

func normalizeName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if len(name) < 1 || len(name) > 80 {
		return "", httpx.Unprocessable("name 长度需在 1-80 字符之间")
	}
	return name, nil
}

func normalizeListOptions(opts ListOptions) ListOptions {
	if opts.Limit <= 0 {
		opts.Limit = 10
	}
	if opts.Limit > 50 {
		opts.Limit = 50
	}
	if opts.Offset < 0 {
		opts.Offset = 0
	}
	if !allowedListSorts[opts.SortBy] {
		opts.SortBy = "created_at"
	}
	if opts.SortDir != "asc" {
		opts.SortDir = "desc"
	}
	return opts
}

func expansionConflict(reason string) *httpx.HTTPError {
	return &httpx.HTTPError{
		Status: 409, Code: httpx.CodePermissionExpansionRequiresNewToken,
		Message: "User Token 权限或有效期只能收紧；请创建替代 Token",
		Details: map[string]any{"reason": reason, "replacement_required": true},
	}
}

func grantKey(grant auth.Grant) string {
	resourceID := ""
	if grant.ResourceID != nil {
		resourceID = grant.ResourceID.String()
	}
	return grant.Permission + "|" + grant.ResourceType + "|" + resourceID
}
