// Package auth - JWTMiddleware 单元测试
//
// 预期 API：
//
//	func JWTMiddleware(secret string) echo.MiddlewareFunc
//
// 中间件行为：
//   - 缺 Authorization → 401
//   - 不是 "Bearer xxx" 格式 → 401
//   - token 无效（签名/过期/格式错误）→ 401
//   - token 合法 → c.Set("user_id", uid)，调用下游
package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

const testMWSecret = "test-secret-32-chars-aaaaaaaaaaaa"

// invokeMiddleware 是辅助函数：用 echo 跑一次中间件 + 一个 echo handler。
// 返回 ResponseRecorder 和 capturedUserID（中间件设置的 user_id，若有）。
//
// 注意：中间件返回 *httpx.HTTPError，需要 httpx.SendError 把它渲染为正确状态码；
// 否则 echo 默认 handler 会把所有 error 当 500。
func invokeMiddleware(t *testing.T, secret, authHeader string) (*httptest.ResponseRecorder, string, bool) {
	t.Helper()

	e := echo.New()
	// 与 main.go 一致：用 httpx.SendError 把业务错误转成统一响应。
	e.HTTPErrorHandler = func(err error, c echo.Context) {
		if c.Response().Committed {
			return
		}
		_ = httpx.SendError(c, err)
	}

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	if authHeader != "" {
		req.Header.Set(echo.HeaderAuthorization, authHeader)
	}
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	var capturedUID string
	var nextCalled bool
	next := func(c echo.Context) error {
		nextCalled = true
		if v, ok := c.Get(string(httpx.CtxKeyUserID)).(string); ok {
			capturedUID = v
		}
		return c.NoContent(http.StatusOK)
	}

	mw := JWTMiddleware(secret)
	handler := mw(next)
	if err := handler(c); err != nil {
		e.HTTPErrorHandler(err, c)
	}
	return rec, capturedUID, nextCalled
}

func TestJWTMiddleware_ValidToken(t *testing.T) {
	t.Parallel()

	uid := uuid.NewString()
	tok, err := GenerateToken(uid, testMWSecret, 1*time.Hour)
	require.NoError(t, err)

	rec, capturedUID, nextCalled := invokeMiddleware(t, testMWSecret, "Bearer "+tok)

	assert.True(t, nextCalled, "next handler should run for valid token")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, uid, capturedUID, "middleware must set user_id in context")
}

func TestJWTMiddleware_NoAuthHeader(t *testing.T) {
	t.Parallel()

	rec, capturedUID, nextCalled := invokeMiddleware(t, testMWSecret, "")
	assert.False(t, nextCalled, "next handler must NOT run without auth")
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Empty(t, capturedUID)
}

func TestJWTMiddleware_InvalidScheme(t *testing.T) {
	t.Parallel()

	cases := []string{
		"Basic dXNlcjpwYXNz",
		"Token abc",
		"bearer lowercase-no-space",
		"BearerNoSpace",
	}
	for _, hdr := range cases {
		hdr := hdr
		t.Run(hdr, func(t *testing.T) {
			t.Parallel()
			rec, _, nextCalled := invokeMiddleware(t, testMWSecret, hdr)
			assert.False(t, nextCalled, "next must NOT run for non-Bearer scheme")
			assert.Equal(t, http.StatusUnauthorized, rec.Code, "scheme %q must yield 401", hdr)
		})
	}
}

func TestJWTMiddleware_InvalidToken(t *testing.T) {
	t.Parallel()

	rec, _, nextCalled := invokeMiddleware(t, testMWSecret, "Bearer not.a.jwt")
	assert.False(t, nextCalled, "next must NOT run for malformed token")
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestJWTMiddleware_ExpiredToken(t *testing.T) {
	t.Parallel()

	uid := uuid.NewString()
	expired, err := GenerateToken(uid, testMWSecret, -1*time.Hour)
	require.NoError(t, err)

	rec, _, nextCalled := invokeMiddleware(t, testMWSecret, "Bearer "+expired)
	assert.False(t, nextCalled, "next must NOT run for expired token")
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestJWTMiddleware_WrongSecret(t *testing.T) {
	t.Parallel()

	uid := uuid.NewString()
	tok, err := GenerateToken(uid, "another-secret-32-chars-zzzzzzzzz", 1*time.Hour)
	require.NoError(t, err)

	rec, _, nextCalled := invokeMiddleware(t, testMWSecret, "Bearer "+tok)
	assert.False(t, nextCalled, "next must NOT run when token signed by different secret")
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestJWTMiddlewareWithUserStatusRejectsDisabledUser(t *testing.T) {
	uid := uuid.New()
	tok, err := GenerateToken(uid.String(), testMWSecret, time.Hour)
	require.NoError(t, err)

	disabledAt := time.Now()
	q := &fakeUserByIDQuerier{user: db.User{ID: uid, DisabledAt: &disabledAt}}

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set(echo.HeaderAuthorization, "Bearer "+tok)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err = JWTMiddlewareWithUserStatus(testMWSecret, q)(func(c echo.Context) error {
		return c.NoContent(http.StatusOK)
	})(c)
	requireAuthHTTPStatus(t, err, http.StatusUnauthorized)
}
