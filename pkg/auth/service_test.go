// Package auth - Service 层集成测试
//
// 这些测试需要真实 Postgres，通过环境变量 TEST_DATABASE_URL 提供。
// 本地开发可通过 docker-compose 起 Postgres，然后：
//
//	export TEST_DATABASE_URL="postgres://postgres:postgres@localhost:5432/openlinker_test?sslmode=disable"
//	go test ./internal/auth/... -race -v
//
// 若 TEST_DATABASE_URL 未设置，所有 service 集成测试都会 t.Skip()。
//
// 实际 Service API（与 service.go 对齐）：
//
//	func NewService(pool *pgxpool.Pool, jwtSecret string, jwtTTL time.Duration) *Service
//	func (s *Service) Register(ctx, *RegisterRequest) (*AuthResponse, error)
//	func (s *Service) Login(ctx, *LoginRequest) (*AuthResponse, error)
//	func (s *Service) FindOrCreateOAuthUser(ctx, provider, oauthID, email, displayName, avatarURL string) (*AuthResponse, error)
//	func (s *Service) GetMe(ctx, userID uuid.UUID) (*MeResponse, error)
//
// 错误：service 层返回 *httpx.HTTPError，code 通过 SQLSTATE 区分：
//   - 邮箱重复 -> httpx.Conflict (409)
//   - 密码错误 / 邮箱不存在 / OAuth-only login -> httpx.Unauthorized (401)
//   - OAuth 邮箱占用 -> httpx.Conflict (409)
package auth

import (
	"context"
	"errors"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

const testServiceSecret = "test-secret-32-chars-aaaaaaaaaaaa"
const testServiceTTL = 1 * time.Hour

// setupTestDB 拿到一个 pool，并清理 users 表保证测试隔离。
// 若 TEST_DATABASE_URL 未设置则 skip。
func setupTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL 未设置，跳过 service 集成测试")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err, "connect test db")
	require.NoError(t, pool.Ping(ctx), "ping test db")

	_, err = pool.Exec(ctx, "TRUNCATE users RESTART IDENTITY CASCADE")
	require.NoError(t, err, "truncate test tables")

	t.Cleanup(func() {
		cleanCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = pool.Exec(cleanCtx, "TRUNCATE users RESTART IDENTITY CASCADE")
		pool.Close()
	})
	return pool
}

func newTestService(t *testing.T, pool *pgxpool.Pool) *Service {
	t.Helper()
	return NewService(pool, testServiceSecret, testServiceTTL)
}

func uniqueEmail(prefix string) string {
	return prefix + "-" + uuid.NewString()[:8] + "@example.com"
}

type recordingProvisioner struct {
	userIDs []uuid.UUID
}

func (p *recordingProvisioner) ProvisionUser(_ context.Context, _ pgx.Tx, userID uuid.UUID) error {
	p.userIDs = append(p.userIDs, userID)
	return nil
}

// assertHTTPStatus 把一个 error 当成 *httpx.HTTPError 取 status 码断言。
func assertHTTPStatus(t *testing.T, err error, want int) {
	t.Helper()
	require.Error(t, err)
	var he *httpx.HTTPError
	require.True(t, errors.As(err, &he), "expected *httpx.HTTPError, got %T (%v)", err, err)
	assert.Equal(t, want, he.Status)
}

// ─────────────────────────────────────────────────────────
// Register
// ─────────────────────────────────────────────────────────

func TestRegister_HappyPath(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	req := &RegisterRequest{
		Email:       uniqueEmail("reg-happy"),
		Password:    "supersecret123",
		DisplayName: "Alice",
	}
	resp, err := svc.Register(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.NotEmpty(t, resp.JWT, "JWT should be returned")
	assert.NotEqual(t, uuid.Nil.String(), resp.UserID, "user_id should be set")
	assert.Equal(t, req.Email, resp.Email)
	assert.Equal(t, req.DisplayName, resp.DisplayName)

	// 验证 JWT 能解析回同一 user_id
	parsed, err := ParseToken(resp.JWT, testServiceSecret)
	require.NoError(t, err)
	assert.Equal(t, resp.UserID, parsed)
}

func TestRegister_UserProvisionerCalled(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	provisioner := &recordingProvisioner{}
	svc.SetUserProvisioner(provisioner)
	ctx := context.Background()

	resp, err := svc.Register(ctx, &RegisterRequest{
		Email:       uniqueEmail("reg-provisioner"),
		Password:    "supersecret123",
		DisplayName: "Provisioned",
	})
	require.NoError(t, err)
	require.Len(t, provisioner.userIDs, 1)
	assert.Equal(t, resp.UserID, provisioner.userIDs[0].String())
}

func TestRegister_DuplicateEmail(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	email := uniqueEmail("reg-dup")
	req := &RegisterRequest{
		Email: email, Password: "supersecret123", DisplayName: "First",
	}
	_, err := svc.Register(ctx, req)
	require.NoError(t, err)

	req2 := &RegisterRequest{
		Email: email, Password: "anotherpass456", DisplayName: "Second",
	}
	_, err = svc.Register(ctx, req2)
	assertHTTPStatus(t, err, http.StatusConflict)
}

func TestRegister_EmailNormalization(t *testing.T) {
	// service 会 ToLower + TrimSpace email，验证大小写不敏感
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	base := uniqueEmail("Reg-Norm")
	_, err := svc.Register(ctx, &RegisterRequest{
		Email: base, Password: "supersecret123", DisplayName: "Lower",
	})
	require.NoError(t, err)

	// 大写形式应被识别为同一邮箱 -> 409
	_, err = svc.Register(ctx, &RegisterRequest{
		Email:       "  " + uppercase(base) + "  ",
		Password:    "supersecret123",
		DisplayName: "Upper",
	})
	assertHTTPStatus(t, err, http.StatusConflict)
}

// uppercase 转大写（不依赖 strings 库以最小化测试依赖）。
func uppercase(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'a' && c <= 'z' {
			b[i] = c - 32
		}
	}
	return string(b)
}

// ─────────────────────────────────────────────────────────
// Login
// ─────────────────────────────────────────────────────────

func TestLogin_HappyPath(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	email := uniqueEmail("login-happy")
	password := "supersecret123"
	regResp, err := svc.Register(ctx, &RegisterRequest{
		Email: email, Password: password, DisplayName: "Login Tester",
	})
	require.NoError(t, err)

	loginResp, err := svc.Login(ctx, &LoginRequest{Email: email, Password: password})
	require.NoError(t, err)
	require.NotNil(t, loginResp)
	assert.Equal(t, regResp.UserID, loginResp.UserID)
	assert.NotEmpty(t, loginResp.JWT)
	assert.Equal(t, email, loginResp.Email)
}

func TestLogin_WrongPassword(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	email := uniqueEmail("login-wrong-pwd")
	_, err := svc.Register(ctx, &RegisterRequest{
		Email: email, Password: "rightpassword", DisplayName: "PwdTester",
	})
	require.NoError(t, err)

	_, err = svc.Login(ctx, &LoginRequest{Email: email, Password: "wrongpassword"})
	assertHTTPStatus(t, err, http.StatusUnauthorized)
}

func TestLogin_NonExistentUser(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	_, err := svc.Login(ctx, &LoginRequest{
		Email: uniqueEmail("never-existed"), Password: "doesnotmatter",
	})
	// 401，不能泄露"用户不存在"
	assertHTTPStatus(t, err, http.StatusUnauthorized)
}

func TestLogin_OAuthOnlyUser(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	email := uniqueEmail("oauth-only")
	_, err := svc.FindOrCreateOAuthUser(ctx,
		"google",
		"google-id-"+uuid.NewString()[:8],
		email,
		"OAuth User",
		"",
	)
	require.NoError(t, err)

	// 用密码登录这个 OAuth-only 用户必须 401
	_, err = svc.Login(ctx, &LoginRequest{Email: email, Password: "anypass"})
	assertHTTPStatus(t, err, http.StatusUnauthorized)
}

// ─────────────────────────────────────────────────────────
// FindOrCreateOAuthUser
// ─────────────────────────────────────────────────────────

func TestFindOrCreateOAuthUser_NewUser(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	email := uniqueEmail("oauth-new")
	oauthID := "google-new-" + uuid.NewString()[:8]
	resp, err := svc.FindOrCreateOAuthUser(ctx,
		"google", oauthID, email, "New Google User", "https://example.com/a.png",
	)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.NotEmpty(t, resp.JWT)
	assert.NotEmpty(t, resp.UserID)
	assert.Equal(t, email, resp.Email)

}

func TestFindOrCreateOAuthUser_ExistingOAuthUser(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	email := uniqueEmail("oauth-existing")
	oauthID := "google-existing-" + uuid.NewString()[:8]

	first, err := svc.FindOrCreateOAuthUser(ctx,
		"google", oauthID, email, "Existing Google User", "")
	require.NoError(t, err)

	second, err := svc.FindOrCreateOAuthUser(ctx,
		"google", oauthID, email, "Existing Google User", "")
	require.NoError(t, err)
	assert.Equal(t, first.UserID, second.UserID, "second oauth login must return same user")

	// users 表只有一行
	var count int
	err = pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM users WHERE oauth_provider = $1 AND oauth_id = $2",
		"google", oauthID).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "exactly one row in users for this oauth identity")
}

func TestFindOrCreateOAuthUser_EmailConflict(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	email := uniqueEmail("conflict")
	// 先用密码注册
	_, err := svc.Register(ctx, &RegisterRequest{
		Email: email, Password: "aregularpassword", DisplayName: "Pwd User",
	})
	require.NoError(t, err)

	// OAuth 同邮箱：Phase 1 严格分开 -> Conflict 409
	_, err = svc.FindOrCreateOAuthUser(ctx,
		"google", "google-conflict-"+uuid.NewString()[:8], email, "Same Email Google User", "")
	assertHTTPStatus(t, err, http.StatusConflict)
}

func TestFindOrCreateOAuthUser_MissingProvider(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	_, err := svc.FindOrCreateOAuthUser(ctx, "", "id", "x@example.com", "x", "")
	assertHTTPStatus(t, err, http.StatusBadRequest)

	_, err = svc.FindOrCreateOAuthUser(ctx, "google", "", "x@example.com", "x", "")
	assertHTTPStatus(t, err, http.StatusBadRequest)
}

func TestOAuthCodeExchangeIsOneTime(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	resp, err := svc.FindOrCreateOAuthUser(ctx,
		"google",
		"google-code-"+uuid.NewString()[:8],
		uniqueEmail("oauth-code"),
		"OAuth Code",
		"",
	)
	require.NoError(t, err)

	code, err := svc.IssueOAuthCode(ctx, resp)
	require.NoError(t, err)
	require.Len(t, code, 64)

	exchanged, err := svc.ExchangeOAuthCode(ctx, code)
	require.NoError(t, err)
	assert.Equal(t, resp.UserID, exchanged.UserID)
	assert.Equal(t, resp.JWT, exchanged.JWT)

	_, err = svc.ExchangeOAuthCode(ctx, code)
	assertHTTPStatus(t, err, http.StatusUnauthorized)
}

// ─────────────────────────────────────────────────────────
// GetMe
// ─────────────────────────────────────────────────────────

func TestGetMe(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	email := uniqueEmail("me")
	regResp, err := svc.Register(ctx, &RegisterRequest{
		Email: email, Password: "supersecret123", DisplayName: "Me Tester",
	})
	require.NoError(t, err)

	uid, err := uuid.Parse(regResp.UserID)
	require.NoError(t, err)

	me, err := svc.GetMe(ctx, uid)
	require.NoError(t, err)
	require.NotNil(t, me)
	assert.Equal(t, regResp.UserID, me.UserID)
	assert.Equal(t, email, me.Email)
	assert.Equal(t, "Me Tester", me.DisplayName)
	assert.False(t, me.IsCreator, "is_creator default false")
	assert.False(t, me.IsAdmin, "is_admin default false")
}

func TestGetMe_NotFound(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	_, err := svc.GetMe(ctx, uuid.New())
	assertHTTPStatus(t, err, http.StatusNotFound)
}

func TestUpdateMe_HappyPathAndValidation(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	regResp, err := svc.Register(ctx, &RegisterRequest{
		Email:       uniqueEmail("me-update"),
		Password:    "supersecret123",
		DisplayName: "Before",
	})
	require.NoError(t, err)
	uid, err := uuid.Parse(regResp.UserID)
	require.NoError(t, err)

	updated, err := svc.UpdateMe(ctx, uid, &UpdateMeRequest{DisplayName: "  After  "})
	require.NoError(t, err)
	require.NotNil(t, updated)
	assert.Equal(t, "After", updated.DisplayName)
	assert.Equal(t, regResp.Email, updated.Email)

	_, err = svc.UpdateMe(ctx, uid, &UpdateMeRequest{DisplayName: "   "})
	assertHTTPStatus(t, err, http.StatusUnprocessableEntity)

	_, err = svc.UpdateMe(ctx, uuid.New(), &UpdateMeRequest{DisplayName: "Missing"})
	assertHTTPStatus(t, err, http.StatusNotFound)
}

func TestChangePassword_HappyPathAndGuards(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	email := uniqueEmail("password-change")
	oldPassword := "supersecret123"
	newPassword := "newsecret456"
	regResp, err := svc.Register(ctx, &RegisterRequest{
		Email:       email,
		Password:    oldPassword,
		DisplayName: "Password Tester",
	})
	require.NoError(t, err)
	uid, err := uuid.Parse(regResp.UserID)
	require.NoError(t, err)

	err = svc.ChangePassword(ctx, uid, &ChangePasswordRequest{
		CurrentPassword:    "wrong-password",
		NewPassword:        newPassword,
		NewPasswordConfirm: newPassword,
	})
	assertHTTPStatus(t, err, http.StatusBadRequest)

	err = svc.ChangePassword(ctx, uid, &ChangePasswordRequest{
		CurrentPassword:    oldPassword,
		NewPassword:        newPassword,
		NewPasswordConfirm: "different-secret",
	})
	assertHTTPStatus(t, err, http.StatusUnprocessableEntity)

	err = svc.ChangePassword(ctx, uid, &ChangePasswordRequest{
		CurrentPassword:    oldPassword,
		NewPassword:        oldPassword,
		NewPasswordConfirm: oldPassword,
	})
	assertHTTPStatus(t, err, http.StatusUnprocessableEntity)

	err = svc.ChangePassword(ctx, uid, &ChangePasswordRequest{
		CurrentPassword:    oldPassword,
		NewPassword:        newPassword,
		NewPasswordConfirm: newPassword,
	})
	require.NoError(t, err)

	_, err = svc.Login(ctx, &LoginRequest{Email: email, Password: oldPassword})
	assertHTTPStatus(t, err, http.StatusUnauthorized)
	loginResp, err := svc.Login(ctx, &LoginRequest{Email: email, Password: newPassword})
	require.NoError(t, err)
	assert.Equal(t, regResp.UserID, loginResp.UserID)

	err = svc.ChangePassword(ctx, uuid.New(), &ChangePasswordRequest{
		CurrentPassword: oldPassword,
		NewPassword:     newPassword,
	})
	assertHTTPStatus(t, err, http.StatusNotFound)
}

func TestChangePassword_OAuthOnlyUserRejected(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	resp, err := svc.FindOrCreateOAuthUser(ctx,
		"google",
		"google-password-"+uuid.NewString()[:8],
		uniqueEmail("oauth-password"),
		"OAuth Password",
		"",
	)
	require.NoError(t, err)
	uid, err := uuid.Parse(resp.UserID)
	require.NoError(t, err)

	err = svc.ChangePassword(ctx, uid, &ChangePasswordRequest{
		CurrentPassword: "anything",
		NewPassword:     "newsecret456",
	})
	assertHTTPStatus(t, err, http.StatusBadRequest)
}
