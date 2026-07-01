package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
	"golang.org/x/crypto/bcrypt"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

// bcryptCost = 12（生产配置；测试环境若想加速可单独覆盖）。
const bcryptCost = 12

const (
	oauthCodeBytes = 32
	oauthCodeTTL   = 2 * time.Minute
)

// UserProvisioner 是用户创建后的可选扩展点。
//
// core 单独部署时不注入任何实现；cloud 可在同一事务内创建 wallet 等商业化行。
type UserProvisioner interface {
	ProvisionUser(ctx context.Context, tx pgx.Tx, userID uuid.UUID) error
}

// Service 认证业务逻辑层。
type Service struct {
	queries         *db.Queries
	pool            *pgxpool.Pool
	jwtSecret       string
	jwtExpire       time.Duration
	userProvisioner UserProvisioner
}

// NewService 构造 Service。jwtTTL 是 token 有效期（time.Duration）。
func NewService(pool *pgxpool.Pool, jwtSecret string, jwtTTL time.Duration) *Service {
	return &Service{
		queries:   db.New(pool),
		pool:      pool,
		jwtSecret: jwtSecret,
		jwtExpire: jwtTTL,
	}
}

// SetUserProvisioner 注入用户创建后的扩展逻辑。传 nil 表示不做额外初始化。
func (s *Service) SetUserProvisioner(provisioner UserProvisioner) {
	s.userProvisioner = provisioner
}

// Register 邮箱密码注册。
//
// 流程：
//  1. email 已存在 -> Conflict
//  2. bcrypt(cost=12) 哈希密码
//  3. 事务内 CreateUser，并执行可选 UserProvisioner
//  4. 签 JWT 返回
func (s *Service) Register(ctx context.Context, req *RegisterRequest) (*AuthResponse, error) {
	email := strings.ToLower(strings.TrimSpace(req.Email))

	// 提前判重，给客户端友好错误（仍依赖 DB UNIQUE 兜底）
	if _, err := s.queries.GetUserByEmail(ctx, email); err == nil {
		return nil, httpx.Conflict("邮箱已注册")
	} else if !errors.Is(err, pgx.ErrNoRows) {
		log.Error().Err(err).Msg("auth.Register: GetUserByEmail")
		return nil, httpx.Internal("查询用户失败")
	}

	hashed, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcryptCost)
	if err != nil {
		log.Error().Err(err).Msg("auth.Register: bcrypt")
		return nil, httpx.Internal("密码处理失败")
	}
	hashStr := string(hashed)

	user, err := s.createUser(ctx, db.CreateUserParams{
		Email:        email,
		PasswordHash: &hashStr,
		DisplayName:  strings.TrimSpace(req.DisplayName),
	})
	if err != nil {
		// UNIQUE violation 兜底（并发场景）
		if isUniqueViolation(err) {
			return nil, httpx.Conflict("邮箱已注册")
		}
		log.Error().Err(err).Msg("auth.Register: createUser")
		return nil, httpx.Internal("创建用户失败")
	}

	token, err := GenerateToken(user.ID.String(), s.jwtSecret, s.jwtExpire)
	if err != nil {
		log.Error().Err(err).Msg("auth.Register: GenerateToken")
		return nil, httpx.Internal("签发 token 失败")
	}
	return &AuthResponse{
		UserID:      user.ID.String(),
		Email:       user.Email,
		DisplayName: user.DisplayName,
		JWT:         token,
	}, nil
}

// Login 邮箱 + 密码登录。
func (s *Service) Login(ctx context.Context, req *LoginRequest) (*AuthResponse, error) {
	email := strings.ToLower(strings.TrimSpace(req.Email))

	user, err := s.queries.GetUserByEmail(ctx, email)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.Unauthorized("邮箱或密码错误")
		}
		log.Error().Err(err).Msg("auth.Login: GetUserByEmail")
		return nil, httpx.Internal("查询用户失败")
	}

	// OAuth-only 用户没有 password_hash
	if user.PasswordHash == nil || *user.PasswordHash == "" {
		return nil, httpx.Unauthorized("该邮箱通过第三方登录，请使用对应方式登录")
	}
	if err := bcrypt.CompareHashAndPassword([]byte(*user.PasswordHash), []byte(req.Password)); err != nil {
		return nil, httpx.Unauthorized("邮箱或密码错误")
	}

	token, err := GenerateToken(user.ID.String(), s.jwtSecret, s.jwtExpire)
	if err != nil {
		log.Error().Err(err).Msg("auth.Login: GenerateToken")
		return nil, httpx.Internal("签发 token 失败")
	}
	return &AuthResponse{
		UserID:      user.ID.String(),
		Email:       user.Email,
		DisplayName: user.DisplayName,
		JWT:         token,
	}, nil
}

// RefreshToken issues a fresh JWT for the currently authenticated user.
func (s *Service) RefreshToken(ctx context.Context, userID uuid.UUID) (*AuthResponse, error) {
	user, err := s.queries.GetUserByID(ctx, userID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.Unauthorized("用户不存在或会话已失效")
		}
		log.Error().Err(err).Str("user_id", userID.String()).Msg("auth.RefreshToken: GetUserByID")
		return nil, httpx.Internal("刷新登录会话失败")
	}
	return s.respondWithToken(&user)
}

// FindOrCreateOAuthUser 处理 Google OAuth 回调用户。
//
// Phase 1 决策（docs/13）：邮箱已被密码用户占用 -> Conflict，不自动合并账号。
func (s *Service) FindOrCreateOAuthUser(
	ctx context.Context,
	provider, oauthID, email, displayName, avatarURL string,
) (*AuthResponse, error) {
	if provider == "" || oauthID == "" {
		return nil, httpx.BadRequest("OAuth 信息不完整")
	}
	email = strings.ToLower(strings.TrimSpace(email))

	// 1. 已绑定过 -> 直接返回
	prov := provider
	oid := oauthID
	user, err := s.queries.GetUserByOAuth(ctx, db.GetUserByOAuthParams{
		OauthProvider: &prov,
		OauthID:       &oid,
	})
	if err == nil {
		return s.respondWithToken(&user)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		log.Error().Err(err).Msg("auth.OAuth: GetUserByOAuth")
		return nil, httpx.Internal("查询 OAuth 用户失败")
	}

	// 2. email 已被占用 -> Conflict
	if email != "" {
		if _, err := s.queries.GetUserByEmail(ctx, email); err == nil {
			return nil, httpx.Conflict("该邮箱已被注册，请使用密码登录")
		} else if !errors.Is(err, pgx.ErrNoRows) {
			log.Error().Err(err).Msg("auth.OAuth: GetUserByEmail")
			return nil, httpx.Internal("查询用户失败")
		}
	}

	// 3. 创建新 OAuth 用户，并执行可选用户初始化
	if displayName == "" {
		// 邮箱前缀兜底 / 不行就 "user"
		if at := strings.IndexByte(email, '@'); at > 0 {
			displayName = email[:at]
		} else {
			displayName = "user"
		}
	}
	var avatarPtr *string
	if avatarURL != "" {
		avatarPtr = &avatarURL
	}
	created, err := s.createUser(ctx, db.CreateUserParams{
		Email:         email,
		OauthProvider: &prov,
		OauthID:       &oid,
		DisplayName:   displayName,
		AvatarURL:     avatarPtr,
	})
	if err != nil {
		if isUniqueViolation(err) {
			return nil, httpx.Conflict("OAuth 账号或邮箱已被使用")
		}
		log.Error().Err(err).Msg("auth.OAuth: createUser")
		return nil, httpx.Internal("创建 OAuth 用户失败")
	}
	return s.respondWithToken(&created)
}

// IssueOAuthCode stores a one-time redirect code for OAuth callback handoff.
func (s *Service) IssueOAuthCode(ctx context.Context, resp *AuthResponse) (string, error) {
	if resp == nil || strings.TrimSpace(resp.UserID) == "" || strings.TrimSpace(resp.JWT) == "" {
		return "", httpx.Internal("创建 OAuth 登录 code 失败")
	}
	userID, err := uuid.Parse(resp.UserID)
	if err != nil {
		return "", httpx.Internal("创建 OAuth 登录 code 失败")
	}
	code, err := randomOAuthCode()
	if err != nil {
		log.Error().Err(err).Msg("auth.IssueOAuthCode: random")
		return "", httpx.Internal("创建 OAuth 登录 code 失败")
	}
	codeHash := hashOAuthCode(code)
	_, _ = s.pool.Exec(ctx, `
DELETE FROM oauth_login_codes
WHERE expires_at < NOW() - INTERVAL '1 hour'
   OR consumed_at < NOW() - INTERVAL '1 hour'
`)
	_, err = s.pool.Exec(ctx, `
INSERT INTO oauth_login_codes (code_hash, user_id, jwt, expires_at)
VALUES ($1, $2, $3, $4)
`, codeHash, userID, resp.JWT, time.Now().UTC().Add(oauthCodeTTL))
	if err != nil {
		log.Error().Err(err).Str("user_id", userID.String()).Msg("auth.IssueOAuthCode: insert")
		return "", httpx.Internal("创建 OAuth 登录 code 失败")
	}
	return code, nil
}

// ExchangeOAuthCode consumes an OAuth redirect code and returns the stored JWT.
func (s *Service) ExchangeOAuthCode(ctx context.Context, code string) (*AuthResponse, error) {
	code = strings.TrimSpace(code)
	if len(code) != oauthCodeBytes*2 {
		return nil, httpx.Unauthorized("OAuth code 无效或已过期")
	}
	var userID uuid.UUID
	var jwt string
	err := s.pool.QueryRow(ctx, `
UPDATE oauth_login_codes
SET consumed_at = NOW()
WHERE code_hash = $1
  AND consumed_at IS NULL
  AND expires_at > NOW()
RETURNING user_id, jwt
`, hashOAuthCode(code)).Scan(&userID, &jwt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.Unauthorized("OAuth code 无效或已过期")
	}
	if err != nil {
		log.Error().Err(err).Msg("auth.ExchangeOAuthCode: consume")
		return nil, httpx.Internal("交换 OAuth code 失败")
	}

	user, err := s.queries.GetUserByID(ctx, userID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.Unauthorized("OAuth code 无效或已过期")
		}
		log.Error().Err(err).Str("user_id", userID.String()).Msg("auth.ExchangeOAuthCode: user")
		return nil, httpx.Internal("交换 OAuth code 失败")
	}
	return &AuthResponse{
		UserID:      user.ID.String(),
		Email:       user.Email,
		DisplayName: user.DisplayName,
		JWT:         jwt,
	}, nil
}

// GetMe 当前用户信息。
func (s *Service) GetMe(ctx context.Context, userID uuid.UUID) (*MeResponse, error) {
	user, err := s.queries.GetUserByID(ctx, userID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("用户不存在")
		}
		log.Error().Err(err).Msg("auth.GetMe: GetUserByID")
		return nil, httpx.Internal("查询用户失败")
	}
	resp := &MeResponse{
		UserID:      user.ID.String(),
		Email:       user.Email,
		DisplayName: user.DisplayName,
		IsCreator:   user.IsCreator,
		IsAdmin:     user.IsAdmin,
	}
	if user.AvatarURL != nil {
		resp.AvatarURL = *user.AvatarURL
	}
	return resp, nil
}

// UpdateMe 更新当前用户基础资料。
func (s *Service) UpdateMe(ctx context.Context, userID uuid.UUID, req *UpdateMeRequest) (*MeResponse, error) {
	displayName := strings.TrimSpace(req.DisplayName)
	if displayName == "" {
		return nil, httpx.Unprocessable("显示名称不能为空")
	}

	tag, err := s.pool.Exec(ctx, `
UPDATE users
SET display_name = $2,
    updated_at = NOW()
WHERE id = $1 AND deleted_at IS NULL
`, userID, displayName)
	if err != nil {
		log.Error().Err(err).Msg("auth.UpdateMe: update user")
		return nil, httpx.Internal("更新用户失败")
	}
	if tag.RowsAffected() == 0 {
		return nil, httpx.NotFound("用户不存在")
	}

	return s.GetMe(ctx, userID)
}

// ChangePassword 修改当前邮箱密码用户的密码。
func (s *Service) ChangePassword(ctx context.Context, userID uuid.UUID, req *ChangePasswordRequest) error {
	if req.NewPasswordConfirm != "" && req.NewPasswordConfirm != req.NewPassword {
		return httpx.Unprocessable("两次输入的新密码不一致")
	}
	if req.CurrentPassword == req.NewPassword {
		return httpx.Unprocessable("新密码不能与当前密码相同")
	}

	user, err := s.queries.GetUserByID(ctx, userID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return httpx.NotFound("用户不存在")
		}
		log.Error().Err(err).Msg("auth.ChangePassword: GetUserByID")
		return httpx.Internal("查询用户失败")
	}
	if user.PasswordHash == nil || *user.PasswordHash == "" {
		return httpx.BadRequest("第三方登录账号暂不支持设置密码")
	}
	if err := bcrypt.CompareHashAndPassword([]byte(*user.PasswordHash), []byte(req.CurrentPassword)); err != nil {
		return httpx.BadRequest("当前密码错误")
	}

	hashed, err := bcrypt.GenerateFromPassword([]byte(req.NewPassword), bcryptCost)
	if err != nil {
		log.Error().Err(err).Msg("auth.ChangePassword: bcrypt")
		return httpx.Internal("密码处理失败")
	}

	tag, err := s.pool.Exec(ctx, `
UPDATE users
SET password_hash = $2,
    updated_at = NOW()
WHERE id = $1 AND deleted_at IS NULL
`, userID, string(hashed))
	if err != nil {
		log.Error().Err(err).Msg("auth.ChangePassword: update password")
		return httpx.Internal("修改密码失败")
	}
	if tag.RowsAffected() == 0 {
		return httpx.NotFound("用户不存在")
	}
	return nil
}

// createUser 事务内创建 user 行，并调用可选 UserProvisioner。
func (s *Service) createUser(ctx context.Context, params db.CreateUserParams) (db.User, error) {
	var created db.User
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return created, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	q := s.queries.WithTx(tx)
	created, err = q.CreateUser(ctx, params)
	if err != nil {
		return created, fmt.Errorf("create user: %w", err)
	}
	if s.userProvisioner != nil {
		if err := s.userProvisioner.ProvisionUser(ctx, tx, created.ID); err != nil {
			return created, fmt.Errorf("provision user: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return created, fmt.Errorf("commit: %w", err)
	}
	return created, nil
}

// respondWithToken 把 db.User 转成 AuthResponse + JWT。
func (s *Service) respondWithToken(user *db.User) (*AuthResponse, error) {
	token, err := GenerateToken(user.ID.String(), s.jwtSecret, s.jwtExpire)
	if err != nil {
		log.Error().Err(err).Msg("auth: GenerateToken")
		return nil, httpx.Internal("签发 token 失败")
	}
	return &AuthResponse{
		UserID:      user.ID.String(),
		Email:       user.Email,
		DisplayName: user.DisplayName,
		JWT:         token,
	}, nil
}

func randomOAuthCode() (string, error) {
	raw := make([]byte, oauthCodeBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw), nil
}

func hashOAuthCode(code string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(code)))
	return hex.EncodeToString(sum[:])
}

// isUniqueViolation 检测 Postgres UNIQUE 约束冲突（SQLSTATE 23505）。
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	type sqlState interface{ SQLState() string }
	var ss sqlState
	if errors.As(err, &ss) {
		return ss.SQLState() == "23505"
	}
	return false
}
