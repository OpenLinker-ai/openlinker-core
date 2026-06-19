// Package auth - HTTP handler 集成测试
//
// 与 service_test.go 一样，需要 TEST_DATABASE_URL，否则 t.Skip()。
//
// 实际 Handler API（与 handler.go 对齐）：
//
//	func NewHandler(svc *Service) *Handler
//	func (h *Handler) SetConfig(cfg *config.Config) *Handler
//	func (h *Handler) Register(api *echo.Group)
//	func (h *Handler) RegisterProtected(api *echo.Group, jwtMiddleware echo.MiddlewareFunc)
//
//	路由：
//	  POST /api/v1/auth/register   -> 201 AuthResponse
//	  POST /api/v1/auth/login      -> 200 AuthResponse
//	  GET  /api/v1/auth/google     -> 302（不在测试覆盖中）
//	  GET  /api/v1/auth/google/cb  -> 302（不在测试覆盖中）
//	  GET  /api/v1/me              -> 200 MeResponse
package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kinzhi/openlinker-core/pkg/config"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

const testHandlerSecret = "test-secret-32-chars-aaaaaaaaaaaa"

// setupTestServer 启动一个 echo + service + handler 组合，返回测试用 *httptest.Server。
func setupTestServer(t *testing.T) (*httptest.Server, *pgxpool.Pool) {
	t.Helper()

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL 未设置，跳过 handler 集成测试")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	require.NoError(t, pool.Ping(ctx))

	_, err = pool.Exec(ctx, "TRUNCATE users RESTART IDENTITY CASCADE")
	require.NoError(t, err)

	svc := NewService(pool, testHandlerSecret, 1*time.Hour)
	cfg := &config.Config{
		JWTSecret:      testHandlerSecret,
		JWTExpireHours: 1,
		FrontendURL:    "http://localhost:3000",
	}
	h := NewHandler(svc).SetConfig(cfg)

	e := echo.New()
	// 全局错误处理对齐 main.go
	e.HTTPErrorHandler = func(err error, c echo.Context) {
		if c.Response().Committed {
			return
		}
		_ = httpx.SendError(c, err)
	}

	api := e.Group("/api/v1")
	h.Register(api)
	h.RegisterProtected(api, JWTMiddleware(testHandlerSecret))

	srv := httptest.NewServer(e)
	t.Cleanup(func() {
		srv.Close()
		clean, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = pool.Exec(clean, "TRUNCATE users RESTART IDENTITY CASCADE")
		pool.Close()
	})
	return srv, pool
}

func postJSON(t *testing.T, url string, body any, headers map[string]string) (*http.Response, []byte) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		require.NoError(t, json.NewEncoder(&buf).Encode(body))
	}
	req, err := http.NewRequest(http.MethodPost, url, &buf)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp, respBody
}

func getJSON(t *testing.T, url string, headers map[string]string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	require.NoError(t, err)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp, body
}

// authResponseBody 与 dto.AuthResponse 字段保持兼容。
type authResponseBody struct {
	UserID      string `json:"user_id"`
	Email       string `json:"email"`
	DisplayName string `json:"display_name"`
	JWT         string `json:"jwt"`
}

// ─────────────────────────────────────────────────────────
// POST /auth/register
// ─────────────────────────────────────────────────────────

func TestPostRegister_HappyPath(t *testing.T) {
	srv, _ := setupTestServer(t)

	body := map[string]string{
		"email":        "handler-happy-" + uuid.NewString()[:8] + "@example.com",
		"password":     "supersecret123",
		"display_name": "Handler Tester",
	}
	resp, raw := postJSON(t, srv.URL+"/api/v1/auth/register", body, nil)
	assert.Equal(t, http.StatusCreated, resp.StatusCode, "body=%s", string(raw))

	var out authResponseBody
	require.NoError(t, json.Unmarshal(raw, &out))
	assert.NotEmpty(t, out.JWT)
	assert.NotEmpty(t, out.UserID)
	assert.Equal(t, body["email"], out.Email)
	assert.Equal(t, body["display_name"], out.DisplayName)
}

func TestPostRegister_InvalidJSON(t *testing.T) {
	srv, _ := setupTestServer(t)

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/api/v1/auth/register",
		bytes.NewBufferString("not-json{{{"))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "invalid JSON should yield 400")
}

func TestPostRegister_EmailFormat(t *testing.T) {
	srv, _ := setupTestServer(t)

	body := map[string]string{
		"email":        "not-an-email",
		"password":     "supersecret123",
		"display_name": "Bad Email",
	}
	resp, raw := postJSON(t, srv.URL+"/api/v1/auth/register", body, nil)
	// validator 失败 -> 422
	assert.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode,
		"invalid email format must be 422; body=%s", string(raw))
}

func TestPostRegister_ShortPassword(t *testing.T) {
	srv, _ := setupTestServer(t)

	body := map[string]string{
		"email":        "short-pwd-" + uuid.NewString()[:8] + "@example.com",
		"password":     "short",
		"display_name": "Short Pwd",
	}
	resp, raw := postJSON(t, srv.URL+"/api/v1/auth/register", body, nil)
	assert.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode,
		"password<8 must be 422; body=%s", string(raw))
}

func TestPostRegister_DuplicateEmail(t *testing.T) {
	srv, _ := setupTestServer(t)

	email := "handler-dup-" + uuid.NewString()[:8] + "@example.com"
	body := map[string]string{
		"email":        email,
		"password":     "supersecret123",
		"display_name": "Dup1",
	}
	resp, _ := postJSON(t, srv.URL+"/api/v1/auth/register", body, nil)
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	body["display_name"] = "Dup2"
	resp2, _ := postJSON(t, srv.URL+"/api/v1/auth/register", body, nil)
	assert.Equal(t, http.StatusConflict, resp2.StatusCode, "duplicate email must be 409")
}

// ─────────────────────────────────────────────────────────
// POST /auth/login
// ─────────────────────────────────────────────────────────

func TestPostLogin_HappyPath(t *testing.T) {
	srv, _ := setupTestServer(t)

	email := "login-handler-" + uuid.NewString()[:8] + "@example.com"
	password := "supersecret123"
	regResp, _ := postJSON(t, srv.URL+"/api/v1/auth/register", map[string]string{
		"email": email, "password": password, "display_name": "L Tester",
	}, nil)
	require.Equal(t, http.StatusCreated, regResp.StatusCode)

	loginResp, raw := postJSON(t, srv.URL+"/api/v1/auth/login", map[string]string{
		"email": email, "password": password,
	}, nil)
	assert.Equal(t, http.StatusOK, loginResp.StatusCode, "body=%s", string(raw))

	var out authResponseBody
	require.NoError(t, json.Unmarshal(raw, &out))
	assert.NotEmpty(t, out.JWT)
	assert.Equal(t, email, out.Email)
}

func TestPostLogin_WrongPassword(t *testing.T) {
	srv, _ := setupTestServer(t)

	email := "login-wrong-" + uuid.NewString()[:8] + "@example.com"
	postJSON(t, srv.URL+"/api/v1/auth/register", map[string]string{
		"email": email, "password": "rightpass1", "display_name": "x",
	}, nil)

	resp, _ := postJSON(t, srv.URL+"/api/v1/auth/login", map[string]string{
		"email": email, "password": "wrongpass",
	}, nil)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

// ─────────────────────────────────────────────────────────
// GET /me
// ─────────────────────────────────────────────────────────

func TestGetMe_NoAuth(t *testing.T) {
	srv, _ := setupTestServer(t)

	resp, _ := getJSON(t, srv.URL+"/api/v1/me", nil)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestGetMe_ValidToken(t *testing.T) {
	srv, _ := setupTestServer(t)

	email := "me-handler-" + uuid.NewString()[:8] + "@example.com"
	regResp, regBody := postJSON(t, srv.URL+"/api/v1/auth/register", map[string]string{
		"email": email, "password": "supersecret123", "display_name": "Me User",
	}, nil)
	require.Equal(t, http.StatusCreated, regResp.StatusCode)

	var auth authResponseBody
	require.NoError(t, json.Unmarshal(regBody, &auth))

	resp, raw := getJSON(t, srv.URL+"/api/v1/me", map[string]string{
		"Authorization": "Bearer " + auth.JWT,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode, "body=%s", string(raw))

	var me MeResponse
	require.NoError(t, json.Unmarshal(raw, &me))
	assert.Equal(t, auth.UserID, me.UserID)
	assert.Equal(t, email, me.Email)
	assert.Equal(t, "Me User", me.DisplayName)
}

func TestGetMe_InvalidToken(t *testing.T) {
	srv, _ := setupTestServer(t)

	resp, _ := getJSON(t, srv.URL+"/api/v1/me", map[string]string{
		"Authorization": "Bearer not.a.real.jwt",
	})
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}
