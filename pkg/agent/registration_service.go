package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
	"golang.org/x/crypto/bcrypt"

	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	"github.com/OpenLinker-ai/openlinker-core/pkg/credential"
	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

const (
	agentTokenPrefixLen = credential.PrefixLen

	agentTokenDefaultMinutes = 30
	maxRegistrationSkills    = 5
	maxAgentTokens           = 10
)

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
	if status != "active_runtime" {
		hash, err := bcrypt.GenerateFromPassword(credential.BcryptTokenInput(plaintext), credential.BcryptCost)
		if err != nil {
			return nil, httpx.Internal("加密 Agent Token 失败")
		}
		tokenHash = string(hash)
	}
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

func (s *RegistrationService) ListAgentTokens(ctx context.Context, creatorID uuid.UUID, agentID *uuid.UUID) ([]AgentTokenResponse, error) {
	if err := s.ensureCreator(ctx, creatorID); err != nil {
		return nil, err
	}
	var (
		tokens []db.AgentToken
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
		tokens, err = s.queries.ListAgentTokensByCreatorAndAgent(ctx, db.ListAgentTokensByCreatorAndAgentParams{
			CreatorUserID: creatorID,
			AgentID:       *agentID,
		})
	} else {
		tokens, err = s.queries.ListAgentTokensByCreator(ctx, creatorID)
	}
	if err != nil {
		log.Error().Err(err).Str("user_id", creatorID.String()).Msg("registration.ListAgentTokens")
		return nil, httpx.Internal("查询 Agent Token 失败")
	}
	items := make([]AgentTokenResponse, 0, len(tokens))
	for _, t := range tokens {
		items = append(items, agentTokenResponse(t))
	}
	return items, nil
}

func (s *RegistrationService) RevokeAgentToken(ctx context.Context, creatorID, tokenID uuid.UUID) error {
	affected, err := s.queries.RevokeAgentTokenForCreator(ctx, db.RevokeAgentTokenForCreatorParams{
		ID:            tokenID,
		CreatorUserID: creatorID,
	})
	if err != nil {
		log.Error().Err(err).Str("token_id", tokenID.String()).Msg("registration.RevokeAgentToken")
		return httpx.Internal("撤销 Agent Token 失败")
	}
	if affected == 0 {
		return httpx.NotFound("Agent Token 不存在或已撤销")
	}
	return nil
}

// RegisterAgentViaToken 用 pending Agent Token 完成 Agent 注册。
func (s *RegistrationService) RegisterAgentViaToken(ctx context.Context, req *RegisterAgentViaTokenRequest) (*RegisterAgentViaTokenResponse, error) {
	matched, err := s.verifyPendingAgentToken(ctx, req.AgentToken)
	if err != nil {
		return nil, err
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
		visibility = "public"
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

	return &RegisterAgentViaTokenResponse{
		Agent:      toAgentResponse(&created),
		AgentToken: agentTokenResponse(redeemed),
	}, nil
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

// verifyPendingAgentToken 解析明文 token，按 prefix 拉候选后 bcrypt 校验。
// 不修改状态；调用方在事务里走 RedeemPendingAgentToken 原子兑换。
func (s *RegistrationService) verifyPendingAgentToken(ctx context.Context, plaintext string) (db.AgentToken, error) {
	plaintext = strings.TrimSpace(plaintext)
	if !credential.HasAnyPrefix(plaintext, credential.AgentTokenPrefix) ||
		!credential.ValidLengthForPrefix(plaintext, credential.AgentTokenPrefix) {
		return db.AgentToken{}, httpx.Unauthorized("Agent Token 无效")
	}
	tokens, err := s.queries.ListActiveAgentTokensByPrefix(ctx, plaintext[:agentTokenPrefixLen])
	if err != nil {
		return db.AgentToken{}, httpx.Unauthorized("Agent Token 无效")
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
			return t, nil
		}
	}
	return db.AgentToken{}, httpx.Unauthorized("Agent Token 无效或已失效")
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
	if connectionMode == "runtime_pull" || connectionMode == "runtime_ws" {
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
