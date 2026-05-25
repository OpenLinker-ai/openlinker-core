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

	db "github.com/kinzhi/openlinker-core/pkg/db/generated"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

const (
	bootstrapTokenPrefix    = "br_live_"
	bootstrapTokenPrefixLen = 12
	bootstrapRandomBytes    = 32

	bootstrapDefaultMinutes = 30
	bootstrapDefaultMaxAgents = 1

	runtimeTokenPrefix      = "rt_live_"
	runtimeTokenPrefixLen   = 12
	runtimeRandomBytes      = 32
	runtimeDefaultTokenName = "bootstrap-issued"
)

// 非字母数字字符在 slug 派生时统一替换成 '-'。
var slugDeriveSanitize = regexp.MustCompile(`[^a-z0-9]+`)

// RegistrationService 处理 Agent 自注册 Bootstrap Token 全流程。
//
// 与 agent.Service 拆开是因为：
//   - Bootstrap 验证不走 JWT，不复用 ownerAgent 等创作者侧检查
//   - 消费 token + 建 Agent + 发 Runtime Token 必须在同一个事务
type RegistrationService struct {
	queries *db.Queries
	pool    *pgxpool.Pool
}

func NewRegistrationService(pool *pgxpool.Pool) *RegistrationService {
	return &RegistrationService{queries: db.New(pool), pool: pool}
}

// MintBootstrapToken 创作者侧铸新 Bootstrap Token。明文 token 仅本次返回。
func (s *RegistrationService) MintBootstrapToken(ctx context.Context, creatorID uuid.UUID, req *CreateBootstrapTokenRequest) (*BootstrapTokenResponse, error) {
	user, err := s.queries.GetUserByID(ctx, creatorID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("用户不存在")
		}
		log.Error().Err(err).Str("user_id", creatorID.String()).Msg("registration.MintBootstrapToken: GetUserByID")
		return nil, httpx.Internal("查询用户失败")
	}
	if !user.IsCreator {
		return nil, httpx.Forbidden("仅创作者可铸 Bootstrap Token")
	}

	minutes := req.ExpiresInMinutes
	if minutes == 0 {
		minutes = bootstrapDefaultMinutes
	}
	maxAgents := req.MaxAgents
	if maxAgents == 0 {
		maxAgents = bootstrapDefaultMaxAgents
	}
	plaintext, prefix, err := generateBootstrapToken()
	if err != nil {
		return nil, httpx.Internal("生成 Bootstrap Token 失败")
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(plaintext), bcrypt.DefaultCost)
	if err != nil {
		return nil, httpx.Internal("加密 Bootstrap Token 失败")
	}
	token, err := s.queries.CreateAgentRegistrationToken(ctx, db.CreateAgentRegistrationTokenParams{
		CreatorUserID: creatorID,
		Label:         strings.TrimSpace(req.Label),
		Prefix:        prefix,
		TokenHash:     string(hash),
		MaxAgents:     maxAgents,
		ExpiresAt:     time.Now().Add(time.Duration(minutes) * time.Minute),
	})
	if err != nil {
		log.Error().Err(err).Msg("registration.MintBootstrapToken: insert")
		return nil, httpx.Internal("创建 Bootstrap Token 失败")
	}
	resp := bootstrapTokenResponse(token)
	resp.PlaintextToken = plaintext
	return &resp, nil
}

// ListBootstrapTokens 列出创作者可见的所有 Bootstrap Token（不含明文）。
func (s *RegistrationService) ListBootstrapTokens(ctx context.Context, creatorID uuid.UUID) ([]BootstrapTokenResponse, error) {
	tokens, err := s.queries.ListAgentRegistrationTokensByCreator(ctx, creatorID)
	if err != nil {
		return nil, httpx.Internal("查询 Bootstrap Token 失败")
	}
	items := make([]BootstrapTokenResponse, 0, len(tokens))
	for _, t := range tokens {
		items = append(items, bootstrapTokenResponse(t))
	}
	return items, nil
}

// RevokeBootstrapToken 创作者主动撤销。已撤销 / 不归我所有都视为 404。
func (s *RegistrationService) RevokeBootstrapToken(ctx context.Context, creatorID, tokenID uuid.UUID) error {
	affected, err := s.queries.RevokeAgentRegistrationTokenForCreator(ctx, db.RevokeAgentRegistrationTokenForCreatorParams{
		ID:            tokenID,
		CreatorUserID: creatorID,
	})
	if err != nil {
		return httpx.Internal("撤销 Bootstrap Token 失败")
	}
	if affected == 0 {
		return httpx.NotFound("Bootstrap Token 不存在或已撤销")
	}
	return nil
}

// RegisterAgentViaBootstrap 用 Bootstrap Token 一次性完成 Agent 注册 + Runtime Token 颁发。
//
// 事务内原子完成：
//  1. ConsumeAgentRegistrationToken 在 used_count < max_agents 时 +1，否则 0 行
//  2. CreateAgent
//  3. CreateAgentRuntimeToken
//
// 任一步骤失败回滚，Bootstrap 计数不会泄漏。
func (s *RegistrationService) RegisterAgentViaBootstrap(ctx context.Context, req *RegisterAgentViaBootstrapRequest) (*RegisterAgentViaBootstrapResponse, error) {
	matched, err := s.verifyBootstrapToken(ctx, req.BootstrapToken)
	if err != nil {
		return nil, err
	}

	slug := strings.TrimSpace(req.Slug)
	if slug == "" {
		slug = deriveSlug(req.Name)
	}
	if !isValidSlug(slug) {
		return nil, httpx.Unprocessable("slug 格式不合法：仅允许小写字母 / 数字 / 连字符，3..80 字符，且不能以连字符开头或结尾")
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		log.Error().Err(err).Msg("registration.RegisterAgentViaBootstrap: begin tx")
		return nil, httpx.Internal("数据库事务失败")
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := s.queries.WithTx(tx)

	consumed, err := q.ConsumeAgentRegistrationToken(ctx, matched.ID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.Unauthorized("Bootstrap Token 已用尽 / 过期 / 撤销")
	}
	if err != nil {
		log.Error().Err(err).Str("token_id", matched.ID.String()).Msg("registration.RegisterAgentViaBootstrap: consume")
		return nil, httpx.Internal("消费 Bootstrap Token 失败")
	}

	authHeader := normalizeAuthHeader(req.EndpointAuthHeader)
	created, err := q.CreateAgent(ctx, db.CreateAgentParams{
		CreatorID:          consumed.CreatorUserID,
		Slug:               slug,
		Name:               strings.TrimSpace(req.Name),
		Description:        strings.TrimSpace(req.Description),
		EndpointURL:        strings.TrimSpace(req.EndpointURL),
		EndpointAuthHeader: authHeader,
		PricePerCallCents:  req.PricePerCallCents,
		Tags:               normalizeTagsForInsert(req.Tags),
	})
	if err != nil {
		if isUniqueViolation(err) {
			return nil, httpx.Conflict("slug 已被占用")
		}
		if isCheckViolation(err) {
			return nil, httpx.Unprocessable("Agent 字段不符合约束")
		}
		log.Error().Err(err).Msg("registration.RegisterAgentViaBootstrap: insert agent")
		return nil, httpx.Internal("创建 Agent 失败")
	}

	tokenName := strings.TrimSpace(req.RuntimeTokenName)
	if tokenName == "" {
		tokenName = runtimeDefaultTokenName
	}
	rtPlain, rtPrefix, err := generateRuntimeTokenForBootstrap()
	if err != nil {
		return nil, httpx.Internal("生成 Runtime Token 失败")
	}
	rtHash, err := bcrypt.GenerateFromPassword([]byte(rtPlain), bcrypt.DefaultCost)
	if err != nil {
		return nil, httpx.Internal("加密 Runtime Token 失败")
	}
	rtToken, err := q.CreateAgentRuntimeToken(ctx, db.CreateAgentRuntimeTokenParams{
		AgentID:         created.ID,
		CreatedByUserID: consumed.CreatorUserID,
		Name:            tokenName,
		Prefix:          rtPrefix,
		TokenHash:       string(rtHash),
		Scopes:          []string{"agent:call"},
	})
	if err != nil {
		log.Error().Err(err).Msg("registration.RegisterAgentViaBootstrap: insert runtime token")
		return nil, httpx.Internal("创建 Runtime Token 失败")
	}

	if err := tx.Commit(ctx); err != nil {
		log.Error().Err(err).Msg("registration.RegisterAgentViaBootstrap: commit")
		return nil, httpx.Internal("提交注册事务失败")
	}

	return &RegisterAgentViaBootstrapResponse{
		Agent: toAgentResponse(&created),
		RuntimeToken: BootstrapRuntimeToken{
			ID:             rtToken.ID.String(),
			Prefix:         rtToken.Prefix,
			PlaintextToken: rtPlain,
			CreatedAt:      rtToken.CreatedAt.UTC().Format(time.RFC3339),
		},
		UsedCount: consumed.UsedCount,
		MaxAgents: consumed.MaxAgents,
	}, nil
}

// verifyBootstrapToken 解析明文 token，按 prefix 拉所有未撤销行后 bcrypt 校验。
// 不增加 used_count；调用方在事务里走 ConsumeAgentRegistrationToken 原子消费。
func (s *RegistrationService) verifyBootstrapToken(ctx context.Context, plaintext string) (db.AgentRegistrationToken, error) {
	plaintext = strings.TrimSpace(plaintext)
	if !strings.HasPrefix(plaintext, bootstrapTokenPrefix) ||
		len(plaintext) != len(bootstrapTokenPrefix)+bootstrapRandomBytes*2 {
		return db.AgentRegistrationToken{}, httpx.Unauthorized("Bootstrap Token 无效")
	}
	tokens, err := s.queries.ListActiveAgentRegistrationTokensByPrefix(ctx, plaintext[:bootstrapTokenPrefixLen])
	if err != nil {
		return db.AgentRegistrationToken{}, httpx.Unauthorized("Bootstrap Token 无效")
	}
	now := time.Now()
	for _, t := range tokens {
		if t.ExpiresAt.Before(now) {
			continue
		}
		if t.UsedCount >= t.MaxAgents {
			continue
		}
		if bcrypt.CompareHashAndPassword([]byte(t.TokenHash), []byte(plaintext)) == nil {
			return t, nil
		}
	}
	return db.AgentRegistrationToken{}, httpx.Unauthorized("Bootstrap Token 无效或已失效")
}

func generateBootstrapToken() (string, string, error) {
	raw := make([]byte, bootstrapRandomBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", "", err
	}
	plaintext := bootstrapTokenPrefix + hex.EncodeToString(raw)
	return plaintext, plaintext[:bootstrapTokenPrefixLen], nil
}

// generateRuntimeTokenForBootstrap 与 a2a 包内的 generateRuntimeToken 等价：
// 单独复制是为了避免 agent → a2a 反向依赖（a2a 已 import agent 间接组件）。
func generateRuntimeTokenForBootstrap() (string, string, error) {
	raw := make([]byte, runtimeRandomBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", "", err
	}
	plaintext := runtimeTokenPrefix + hex.EncodeToString(raw)
	return plaintext, plaintext[:runtimeTokenPrefixLen], nil
}

// deriveSlug 把 name 转成合法 slug，并尾巴拼 6 hex 字符避免与已有 slug 撞车。
//
// 失败兜底（name 全是非 ASCII 字符等）：用 6 hex 加 "agent" 前缀。
func deriveSlug(name string) string {
	lower := strings.ToLower(strings.TrimSpace(name))
	cleaned := slugDeriveSanitize.ReplaceAllString(lower, "-")
	cleaned = strings.Trim(cleaned, "-")
	suffix := randomHex6()
	if cleaned == "" {
		return "agent-" + suffix
	}
	// max 80, 留 7 字符给 "-xxxxxx"
	if len(cleaned) > 73 {
		cleaned = cleaned[:73]
		cleaned = strings.TrimRight(cleaned, "-")
	}
	return cleaned + "-" + suffix
}

func randomHex6() string {
	raw := make([]byte, 3)
	if _, err := rand.Read(raw); err != nil {
		// 极端兜底：用纳秒后 6 位
		return time.Now().Format("150405")
	}
	return hex.EncodeToString(raw)
}

func bootstrapTokenResponse(t db.AgentRegistrationToken) BootstrapTokenResponse {
	resp := BootstrapTokenResponse{
		ID:        t.ID.String(),
		Label:     t.Label,
		Prefix:    t.Prefix,
		MaxAgents: t.MaxAgents,
		UsedCount: t.UsedCount,
		ExpiresAt: t.ExpiresAt.UTC().Format(time.RFC3339),
		CreatedAt: t.CreatedAt.UTC().Format(time.RFC3339),
	}
	if t.RevokedAt != nil {
		v := t.RevokedAt.UTC().Format(time.RFC3339)
		resp.RevokedAt = &v
	}
	if t.LastUsedAt != nil {
		v := t.LastUsedAt.UTC().Format(time.RFC3339)
		resp.LastUsedAt = &v
	}
	return resp
}
