package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	"github.com/OpenLinker-ai/openlinker-core/pkg/credential"
	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	coreruntime "github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

const (
	agentTokenPrefixLen = credential.PrefixLen

	agentTokenDefaultMinutes = 30
	maxRegistrationSkills    = 5
	maxAgentTokens           = 10
)

var allowedAgentTokenListSorts = map[string]struct{}{
	"created_at":   {},
	"last_used_at": {},
	"expires_at":   {},
	"name":         {},
	"status":       {},
}

// 非字母数字字符在 slug 派生时统一替换成 '-'。
var slugDeriveSanitize = regexp.MustCompile(`[^a-z0-9]+`)

// RegistrationService 处理 Agent token 创建、自注册兑换和撤销。
type RegistrationService struct {
	queries                 *db.Queries
	pool                    *pgxpool.Pool
	allowLocalHTTPEndpoints bool
}

func NewRegistrationService(pool *pgxpool.Pool, cfg ...*config.Config) *RegistrationService {
	allowLocalHTTP := false
	if len(cfg) > 0 && cfg[0] != nil {
		allowLocalHTTP = cfg[0].AllowLocalHTTPEndpoints
	}
	return &RegistrationService{
		queries:                 db.New(pool),
		pool:                    pool,
		allowLocalHTTPEndpoints: allowLocalHTTP,
	}
}

// CreateAgentToken 创建统一 Agent 接入凭证。明文 token 仅本次返回。
func (s *RegistrationService) CreateAgentToken(ctx context.Context, creatorID uuid.UUID, req *CreateAgentTokenRequest) (*AgentTokenResponse, error) {
	if err := s.ensureCreator(ctx, creatorID); err != nil {
		return nil, err
	}
	name := strings.TrimSpace(req.Name)
	if name == "" || len(name) > 80 {
		return nil, httpx.Unprocessable("name 长度需在 1-80 字符之间")
	}

	var agent *db.Agent
	var agentID *uuid.UUID
	status := "pending_registration"
	var expiresAt *time.Time
	var redeemedAt *time.Time

	if strings.TrimSpace(req.AgentID) != "" {
		parsed, err := uuid.Parse(strings.TrimSpace(req.AgentID))
		if err != nil {
			return nil, httpx.BadRequest("agent_id 不是合法 uuid")
		}
		owned, err := s.queries.GetAgentByIDForOwner(ctx, db.GetAgentByIDForOwnerParams{ID: parsed, CreatorID: creatorID})
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("Agent 不存在")
		}
		if err != nil {
			log.Error().Err(err).Str("agent_id", parsed.String()).Msg("registration.CreateAgentToken: GetAgentByIDForOwner")
			return nil, httpx.Internal("查询 Agent 失败")
		}
		count, err := s.queries.CountActiveAgentTokensByAgent(ctx, parsed)
		if err != nil {
			log.Error().Err(err).Str("agent_id", parsed.String()).Msg("registration.CreateAgentToken: CountActiveAgentTokensByAgent")
			return nil, httpx.Internal("查询 Agent Token 失败")
		}
		if count >= maxAgentTokens {
			return nil, httpx.BadRequest("Agent Token 数量已达上限（10 个），请先撤销旧 token")
		}
		agent = &owned
		agentID = &parsed
		status = "active_runtime"
		now := time.Now()
		redeemedAt = &now
	} else {
		minutes := req.ExpiresInMinutes
		if minutes == 0 {
			minutes = agentTokenDefaultMinutes
		}
		expiry := time.Now().Add(time.Duration(minutes) * time.Minute)
		expiresAt = &expiry
	}

	scopes, err := normalizeAgentTokenScopes(req.Scopes, agent)
	if err != nil {
		return nil, err
	}
	plaintext, prefix, err := credential.GenerateAgentToken()
	if err != nil {
		return nil, httpx.Internal("生成 Agent Token 失败")
	}
	tokenHash := credential.FastTokenHash(plaintext)
	token, err := s.queries.CreateAgentToken(ctx, db.CreateAgentTokenParams{
		AgentID:       agentID,
		CreatorUserID: creatorID,
		Name:          name,
		Prefix:        prefix,
		TokenHash:     tokenHash,
		Scopes:        scopes,
		Status:        status,
		ExpiresAt:     expiresAt,
		RedeemedAt:    redeemedAt,
	})
	if err != nil {
		log.Error().Err(err).Msg("registration.CreateAgentToken: insert")
		return nil, httpx.Internal("创建 Agent Token 失败")
	}
	resp := agentTokenResponse(token)
	resp.PlaintextToken = plaintext
	return &resp, nil
}

func (s *RegistrationService) ListAgentTokens(ctx context.Context, creatorID uuid.UUID, agentID *uuid.UUID, opts ListAgentTokensOptions) (*AgentTokenListResponse, error) {
	if err := s.ensureCreator(ctx, creatorID); err != nil {
		return nil, err
	}
	opts = normalizeAgentTokenListOptions(opts)
	var (
		tokens []db.AgentToken
		total  int32
		err    error
	)
	if agentID != nil {
		if _, err := s.queries.GetAgentByIDForOwner(ctx, db.GetAgentByIDForOwnerParams{ID: *agentID, CreatorID: creatorID}); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, httpx.NotFound("Agent 不存在")
			}
			log.Error().Err(err).Str("agent_id", agentID.String()).Msg("registration.ListAgentTokens: GetAgentByIDForOwner")
			return nil, httpx.Internal("查询 Agent 失败")
		}
		tokens, err = s.queries.ListAgentTokensByCreatorAndAgentFiltered(ctx, db.ListAgentTokensByCreatorAndAgentFilteredParams{
			CreatorUserID: creatorID,
			AgentID:       *agentID,
			FilterStatus:  opts.Status,
			Search:        opts.Query,
			Limit:         opts.Limit,
			Offset:        opts.Offset,
			SortBy:        opts.SortBy,
			SortDir:       opts.SortDir,
		})
		if err == nil {
			total, err = s.queries.CountAgentTokensByCreatorAndAgentFiltered(ctx, db.CountAgentTokensByCreatorAndAgentFilteredParams{
				CreatorUserID: creatorID,
				AgentID:       *agentID,
				FilterStatus:  opts.Status,
				Search:        opts.Query,
			})
		}
	} else {
		tokens, err = s.queries.ListAgentTokensByCreatorFiltered(ctx, db.ListAgentTokensByCreatorFilteredParams{
			CreatorUserID: creatorID,
			FilterStatus:  opts.Status,
			Search:        opts.Query,
			Limit:         opts.Limit,
			Offset:        opts.Offset,
			SortBy:        opts.SortBy,
			SortDir:       opts.SortDir,
		})
		if err == nil {
			total, err = s.queries.CountAgentTokensByCreatorFiltered(ctx, db.CountAgentTokensByCreatorFilteredParams{
				CreatorUserID: creatorID,
				FilterStatus:  opts.Status,
				Search:        opts.Query,
			})
		}
	}
	if err != nil {
		log.Error().Err(err).Str("user_id", creatorID.String()).Msg("registration.ListAgentTokens")
		return nil, httpx.Internal("查询 Agent Token 失败")
	}
	items := make([]AgentTokenResponse, 0, len(tokens))
	for _, t := range tokens {
		items = append(items, agentTokenResponse(t))
	}
	return &AgentTokenListResponse{
		Items:   items,
		Total:   total,
		Limit:   opts.Limit,
		Offset:  opts.Offset,
		SortBy:  opts.SortBy,
		SortDir: opts.SortDir,
		HasMore: opts.Offset+int32(len(items)) < total,
	}, nil
}

func (s *RegistrationService) RevokeAgentToken(ctx context.Context, creatorID, tokenID uuid.UUID) error {
	revoked, err := coreruntime.RevokeAgentCredential(ctx, s.pool, creatorID, tokenID)
	if err != nil {
		if errors.Is(err, coreruntime.ErrRuntimeCredentialSessionScopeChanged) {
			return httpx.Conflict("Agent Token 的 Runtime Session 正在变化，请重试")
		}
		log.Error().Err(err).Str("token_id", tokenID.String()).Msg("registration.RevokeAgentToken")
		return httpx.Internal("撤销 Agent Token 失败")
	}
	if !revoked {
		return httpx.NotFound("Agent Token 不存在或已撤销")
	}
	return nil
}

// AgentTokenResource returns the owned Agent bound to a token. Pending
// registration tokens intentionally return nil and therefore require a
// wildcard agent-tokens grant.
func (s *RegistrationService) AgentTokenResource(ctx context.Context, creatorID, tokenID uuid.UUID) (*uuid.UUID, error) {
	token, err := s.queries.GetAgentTokenByIDForCreator(ctx, db.GetAgentTokenByIDForCreatorParams{
		ID: tokenID, CreatorUserID: creatorID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("Agent Token 不存在")
	}
	if err != nil {
		return nil, httpx.Internal("查询 Agent Token 失败")
	}
	return token.AgentID, nil
}

// RegisterAgentViaToken 用 pending Agent Token 完成 Agent 注册。
//
// visibility 省略时为 private。可选的 capability / examples 与创作者端同样校验，
// 和 Agent、skill 绑定、token 兑换在同一事务内写入；任一失败整体回滚。
//
// 已兑换的同一枚 token 带同一 slug 重试时（客户端丢失了首次 201 响应），按当前
// Agent 与 token 重放同形响应，不写库、不签发明文；其他情况保持原有拒绝语义。
func (s *RegistrationService) RegisterAgentViaToken(ctx context.Context, req *RegisterAgentViaTokenRequest) (*RegisterAgentViaTokenResponse, error) {
	matched, replayToken, err := s.verifyPendingAgentToken(ctx, req.AgentToken)
	if err != nil {
		return nil, err
	}
	if replayToken != nil {
		return s.replayCompletedRegistration(ctx, *replayToken, req)
	}
	if err := s.ensureCreator(ctx, matched.CreatorUserID); err != nil {
		return nil, err
	}

	slug := strings.TrimSpace(req.Slug)
	if slug == "" {
		slug = deriveSlug(req.Name)
	}
	if !isValidSlug(slug) {
		return nil, httpx.Unprocessable("slug 格式不合法：仅允许小写字母 / 数字 / 连字符，3..80 字符，且不能以连字符开头或结尾")
	}
	connection, err := normalizeConnectionSettings(
		slug,
		req.EndpointURL,
		req.ConnectionMode,
		req.MCPToolName,
		s.allowLocalHTTPEndpoints,
	)
	if err != nil {
		return nil, err
	}
	skillIDs, err := s.normalizeRegistrationSkillIDs(ctx, req.SkillIDs)
	if err != nil {
		return nil, err
	}
	capability, examples, err := prepareRegistrationOnboarding(req.Capability, req.Examples)
	if err != nil {
		return nil, err
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		log.Error().Err(err).Msg("registration.RegisterAgentViaToken: begin tx")
		return nil, httpx.Internal("数据库事务失败")
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := s.queries.WithTx(tx)

	authHeader := normalizeAuthHeader(req.EndpointAuthHeader)
	visibility := strings.TrimSpace(req.Visibility)
	if visibility == "" {
		visibility = "private"
	}
	created, err := q.CreateAgent(ctx, db.CreateAgentParams{
		CreatorID:          matched.CreatorUserID,
		Slug:               slug,
		Name:               strings.TrimSpace(req.Name),
		Description:        strings.TrimSpace(req.Description),
		EndpointURL:        connection.EndpointURL,
		EndpointAuthHeader: authHeader,
		PricePerCallCents:  req.PricePerCallCents,
		Tags:               normalizeTagsForInsert(req.Tags),
		Visibility:         visibility,
		ConnectionMode:     connection.Mode,
		MCPToolName:        connection.MCPToolName,
	})
	if err != nil {
		if isUniqueViolation(err) {
			return nil, httpx.Conflict("slug 已被占用")
		}
		if isCheckViolation(err) {
			return nil, httpx.Unprocessable("Agent 字段不符合约束")
		}
		log.Error().Err(err).Msg("registration.RegisterAgentViaToken: insert agent")
		return nil, httpx.Internal("创建 Agent 失败")
	}
	if len(skillIDs) > 0 {
		if err := db.ReplaceAgentSkills(ctx, tx, created.ID, skillIDs); err != nil {
			log.Error().Err(err).Str("agent_id", created.ID.String()).Msg("registration.RegisterAgentViaToken: replace skills")
			return nil, httpx.Internal("绑定 Agent skill 失败")
		}
	}
	if err := applyRegistrationOnboarding(ctx, q, created.ID, matched.CreatorUserID, capability, examples); err != nil {
		return nil, err
	}
	redeemed, err := q.RedeemPendingAgentToken(ctx, db.RedeemPendingAgentTokenParams{
		ID:            matched.ID,
		AgentID:       created.ID,
		Scopes:        runtimeScopesForConnection(created.ConnectionMode),
		CreatorUserID: matched.CreatorUserID,
		TokenHash:     credential.FastTokenHash(req.AgentToken),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.Unauthorized("Agent Token 已过期 / 已撤销 / 已被使用")
	}
	if err != nil {
		log.Error().Err(err).Str("token_id", matched.ID.String()).Msg("registration.RegisterAgentViaToken: redeem")
		return nil, httpx.Internal("兑换 Agent Token 失败")
	}
	if err := tx.Commit(ctx); err != nil {
		log.Error().Err(err).Msg("registration.RegisterAgentViaToken: commit")
		return nil, httpx.Internal("提交注册事务失败")
	}

	return s.registrationResponse(ctx, &created, redeemed), nil
}

// applyRegistrationOnboarding writes the onboarding row and the optional
// capability and examples inside the registration transaction, with the same
// queries the creator endpoints use. Any failure aborts the registration, so
// a registered Agent never has a partial declaration and the token is not
// redeemed.
func applyRegistrationOnboarding(
	ctx context.Context,
	q *db.Queries,
	agentID, creatorID uuid.UUID,
	capability *preparedCapability,
	examples []*preparedExample,
) error {
	if err := q.EnsureOnboardingStatus(ctx, agentID); err != nil {
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("registration.RegisterAgentViaToken: ensure onboarding status")
		return httpx.Internal("初始化接入状态失败")
	}
	if capability != nil {
		if _, err := q.UpsertAgentCapability(ctx, capability.upsertParams(agentID, creatorID)); err != nil {
			log.Error().Err(err).Str("agent_id", agentID.String()).Msg("registration.RegisterAgentViaToken: upsert capability")
			return httpx.Internal("保存能力声明失败")
		}
		if _, err := q.MarkCapabilitiesSet(ctx, agentID); err != nil {
			log.Error().Err(err).Str("agent_id", agentID.String()).Msg("registration.RegisterAgentViaToken: mark capabilities set")
			return httpx.Internal("保存能力声明失败")
		}
	}
	for _, example := range examples {
		if _, err := q.CreateAgentExample(ctx, example.createParams(agentID, creatorID)); err != nil {
			if isCheckViolation(err) {
				return httpx.Unprocessable("示例字段不符合约束")
			}
			log.Error().Err(err).Str("agent_id", agentID.String()).Msg("registration.RegisterAgentViaToken: create example")
			return httpx.Internal("创建示例失败")
		}
	}
	if len(examples) > 0 {
		if _, err := q.MarkExamplesSet(ctx, db.MarkExamplesSetParams{AgentID: agentID, ExamplesSet: true}); err != nil {
			log.Error().Err(err).Str("agent_id", agentID.String()).Msg("registration.RegisterAgentViaToken: mark examples set")
			return httpx.Internal("创建示例失败")
		}
	}
	return nil
}

// replayCompletedRegistration answers a retry whose first 201 was lost. It only
// succeeds when the token was redeemed by the registration that created the
// creator's Agent, is still bound to it, and the request names that Agent's
// slug; it performs no writes and never returns a plaintext token. Every other
// case, including tokens issued directly for an existing Agent, keeps the
// pre-existing 401.
func (s *RegistrationService) replayCompletedRegistration(
	ctx context.Context,
	token db.AgentToken,
	req *RegisterAgentViaTokenRequest,
) (*RegisterAgentViaTokenResponse, error) {
	rejected := httpx.Unauthorized("Agent Token 无效或已失效")
	// An omitted slug is derived with a random suffix during registration, so
	// it can never identify the Agent created by the first attempt.
	slug := strings.TrimSpace(req.Slug)
	if slug == "" || token.AgentID == nil {
		return nil, rejected
	}
	if err := s.ensureCreator(ctx, token.CreatorUserID); err != nil {
		var httpErr *httpx.HTTPError
		if errors.As(err, &httpErr) && httpErr.Status >= http.StatusInternalServerError {
			return nil, err
		}
		return nil, rejected
	}
	registered, err := s.queries.GetAgentByIDForOwner(ctx, db.GetAgentByIDForOwnerParams{
		ID:        *token.AgentID,
		CreatorID: token.CreatorUserID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, rejected
	}
	if err != nil {
		log.Error().Err(err).Str("token_id", token.ID.String()).Msg("registration.RegisterAgentViaToken: replay GetAgentByIDForOwner")
		return nil, httpx.Internal("查询 Agent 失败")
	}
	if registered.Slug != slug || !redeemedByRegistration(token, registered) {
		return nil, rejected
	}
	// capability / examples in the retried body are neither compared nor
	// applied: the first attempt wrote them atomically with the Agent, and
	// the owner may have edited them since. The response reports the stored
	// state, so onboarding and next_actions show what actually exists.
	return s.registrationResponse(ctx, &registered, token), nil
}

// redeemedByRegistration reports whether token became active by redeeming a
// pending registration token that created agent. Registration runs
// RedeemPendingAgentToken (redeemed_at = NOW()) and the Agent INSERT
// (created_at DEFAULT now()) in one transaction, so both timestamps are equal,
// and the pending token was minted in an earlier transaction. A token issued
// directly for an existing Agent stamps redeemed_at at issue time, after the
// Agent existed, so it can never satisfy both. The agent_tokens trigger keeps
// created_at and redeemed_at immutable once a token is active.
func redeemedByRegistration(token db.AgentToken, agent db.Agent) bool {
	return token.RedeemedAt != nil &&
		token.RedeemedAt.Equal(agent.CreatedAt) &&
		token.CreatedAt.Before(*token.RedeemedAt)
}

func normalizeAgentTokenListOptions(opts ListAgentTokensOptions) ListAgentTokensOptions {
	if opts.Limit <= 0 {
		opts.Limit = 10
	}
	if opts.Limit > 50 {
		opts.Limit = 50
	}
	if opts.Offset < 0 {
		opts.Offset = 0
	}
	if _, ok := allowedAgentTokenListSorts[opts.SortBy]; !ok {
		opts.SortBy = "created_at"
	}
	if opts.SortDir != "asc" {
		opts.SortDir = "desc"
	}
	opts.Status = strings.ToLower(strings.TrimSpace(opts.Status))
	switch opts.Status {
	case "active", "revoked", "expired", "all":
	default:
		opts.Status = "all"
	}
	opts.Query = strings.TrimSpace(opts.Query)
	if runes := []rune(opts.Query); len(runes) > 120 {
		opts.Query = string(runes[:120])
	}
	return opts
}

func (s *RegistrationService) ensureCreator(ctx context.Context, userID uuid.UUID) error {
	user, err := s.queries.GetUserByID(ctx, userID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return httpx.NotFound("用户不存在")
		}
		log.Error().Err(err).Str("user_id", userID.String()).Msg("registration.ensureCreator: GetUserByID")
		return httpx.Internal("查询用户失败")
	}
	if user.DisabledAt != nil {
		return httpx.Unauthorized("账号已禁用")
	}
	if !user.IsCreator {
		return httpx.Forbidden("仅创作者可生成 Agent Token")
	}
	return nil
}

// verifyPendingAgentToken 解析明文 token，按 prefix 拉候选后校验哈希。
// 不修改状态；调用方在事务里走 RedeemPendingAgentToken 原子兑换。
// 没有待兑换 token 命中时，再匹配已绑定 Agent、未撤销未过期的 active_runtime
// token，作为第二个返回值交给调用方判断是否属于注册重放。
func (s *RegistrationService) verifyPendingAgentToken(ctx context.Context, plaintext string) (db.AgentToken, *db.AgentToken, error) {
	plaintext = strings.TrimSpace(plaintext)
	if !credential.HasAnyPrefix(plaintext, credential.AgentTokenPrefix) ||
		!credential.ValidLengthForPrefix(plaintext, credential.AgentTokenPrefix) {
		return db.AgentToken{}, nil, httpx.Unauthorized("Agent Token 无效")
	}
	tokens, err := s.queries.ListActiveAgentTokensByPrefix(ctx, plaintext[:agentTokenPrefixLen])
	if err != nil {
		// A lookup outage must not look like a bad credential: clients discard
		// tokens on 401.
		log.Error().Err(err).Msg("registration.RegisterAgentViaToken: ListActiveAgentTokensByPrefix")
		return db.AgentToken{}, nil, httpx.Internal("查询 Agent Token 失败")
	}
	now := time.Now()
	for _, t := range tokens {
		if t.Status != "pending_registration" || t.AgentID != nil {
			continue
		}
		if t.ExpiresAt != nil && t.ExpiresAt.Before(now) {
			continue
		}
		if credential.VerifyTokenHash(t.TokenHash, plaintext) {
			return t, nil, nil
		}
	}
	for i := range tokens {
		t := tokens[i]
		if t.Status != "active_runtime" || t.AgentID == nil || t.RevokedAt != nil {
			continue
		}
		if t.ExpiresAt != nil && t.ExpiresAt.Before(now) {
			continue
		}
		if credential.VerifyTokenHash(t.TokenHash, plaintext) {
			return db.AgentToken{}, &t, nil
		}
	}
	return db.AgentToken{}, nil, httpx.Unauthorized("Agent Token 无效或已失效")
}

func normalizeAgentTokenScopes(scopes []string, agent *db.Agent) ([]string, error) {
	if len(scopes) == 0 {
		if agent == nil {
			return []string{"agent:call", "agent:pull"}, nil
		}
		return runtimeScopesForConnection(agent.ConnectionMode), nil
	}
	seen := make(map[string]struct{}, len(scopes))
	out := make([]string, 0, len(scopes))
	for _, raw := range scopes {
		scope := strings.TrimSpace(raw)
		if scope != "agent:call" && scope != "agent:pull" {
			return nil, httpx.Unprocessable("未知 Agent Token scope: " + scope)
		}
		if _, ok := seen[scope]; ok {
			continue
		}
		seen[scope] = struct{}{}
		out = append(out, scope)
	}
	if len(out) == 0 {
		return nil, httpx.Unprocessable("至少选择一个 Agent Token scope")
	}
	return out, nil
}

func runtimeScopesForConnection(connectionMode string) []string {
	scopes := []string{"agent:call"}
	if connectionMode == ConnectionModeRuntime {
		scopes = append(scopes, "agent:pull")
	}
	return scopes
}

func (s *RegistrationService) normalizeRegistrationSkillIDs(ctx context.Context, in []string) ([]string, error) {
	if len(in) == 0 {
		return []string{}, nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, raw := range in {
		skillID := strings.TrimSpace(raw)
		if skillID == "" {
			continue
		}
		if _, ok := seen[skillID]; ok {
			continue
		}
		seen[skillID] = struct{}{}
		out = append(out, skillID)
	}
	if len(out) > maxRegistrationSkills {
		return nil, httpx.BadRequest("最多只能声明 5 个 skill")
	}
	for _, skillID := range out {
		if _, err := s.queries.GetSkill(ctx, skillID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, httpx.BadRequest("skill_id 不存在: " + skillID)
			}
			log.Error().Err(err).Str("skill_id", skillID).Msg("registration.RegisterAgentViaToken: GetSkill")
			return nil, httpx.Internal("校验 skill 失败")
		}
	}
	return out, nil
}

// deriveSlug 把 name 转成合法 slug，并尾巴拼 6 hex 字符避免与已有 slug 撞车。
func deriveSlug(name string) string {
	lower := strings.ToLower(strings.TrimSpace(name))
	cleaned := slugDeriveSanitize.ReplaceAllString(lower, "-")
	cleaned = strings.Trim(cleaned, "-")
	suffix := randomHex6()
	if cleaned == "" {
		return "agent-" + suffix
	}
	if len(cleaned) > 73 {
		cleaned = cleaned[:73]
		cleaned = strings.TrimRight(cleaned, "-")
	}
	return cleaned + "-" + suffix
}

func randomHex6() string {
	raw := make([]byte, 3)
	if _, err := rand.Read(raw); err != nil {
		return time.Now().Format("150405")
	}
	return hex.EncodeToString(raw)
}

func agentTokenResponse(t db.AgentToken) AgentTokenResponse {
	resp := AgentTokenResponse{
		ID:        t.ID.String(),
		Name:      t.Name,
		Prefix:    t.Prefix,
		Status:    t.Status,
		Scopes:    t.Scopes,
		CreatedAt: t.CreatedAt.UTC().Format(time.RFC3339),
	}
	if t.AgentID != nil {
		value := t.AgentID.String()
		resp.AgentID = &value
	}
	if t.ExpiresAt != nil {
		value := t.ExpiresAt.UTC().Format(time.RFC3339)
		resp.ExpiresAt = &value
	}
	if t.RedeemedAt != nil {
		value := t.RedeemedAt.UTC().Format(time.RFC3339)
		resp.RedeemedAt = &value
	}
	if t.RevokedAt != nil {
		value := t.RevokedAt.UTC().Format(time.RFC3339)
		resp.RevokedAt = &value
	}
	if t.LastUsedAt != nil {
		value := t.LastUsedAt.UTC().Format(time.RFC3339)
		resp.LastUsedAt = &value
	}
	return resp
}
