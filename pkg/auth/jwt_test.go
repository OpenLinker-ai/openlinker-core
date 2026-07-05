// Package auth - JWT 单元测试
//
// 覆盖 GenerateToken / ParseToken。测试基于以下预期 API（来自任务规格 + docs/14）：
//
//	func GenerateToken(userID, secret string, expiresIn time.Duration) (string, error)
//	func ParseToken(token, secret string) (userID string, err error)
//
// 如果 implementation agent 使用别的签名（如 GenerateJWT/ParseJWT 或带 *Config 入参），
// 调整下方常量 / helper 即可。本文件不依赖具体实现细节，所有测试都通过 helper 间接调用。
package auth

import (
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 测试用 secret —— 32 字符常量，足够长以避免 HS256 警告。
const testSecret = "test-secret-32-chars-aaaaaaaaaaaa"
const otherSecret = "other-secret-32-chars-bbbbbbbbbbb"

// defaultTTL 测试默认过期时长。
const defaultTTL = 1 * time.Hour

func TestGenerateAndParseToken_HappyPath(t *testing.T) {
	t.Parallel()

	uid := uuid.NewString()

	tok, err := GenerateToken(uid, testSecret, defaultTTL)
	require.NoError(t, err, "GenerateToken should succeed")
	require.NotEmpty(t, tok, "token should not be empty")
	// JWT 格式：三段以 . 分隔
	assert.Equal(t, 2, strings.Count(tok, "."), "JWT must have 3 segments")

	got, err := ParseToken(tok, testSecret)
	require.NoError(t, err, "ParseToken should succeed for fresh token")
	assert.Equal(t, uid, got, "parsed user_id must match")

	parsed, err := jwt.ParseWithClaims(tok, &Claims{}, func(_ *jwt.Token) (interface{}, error) {
		return []byte(testSecret), nil
	})
	require.NoError(t, err)
	claims := parsed.Claims.(*Claims)
	assert.Equal(t, jwtIssuer, claims.Issuer)
}

func TestParseToken_InvalidSignature(t *testing.T) {
	t.Parallel()

	uid := uuid.NewString()
	tok, err := GenerateToken(uid, testSecret, defaultTTL)
	require.NoError(t, err)

	// 翻转签名段的第一个字符；不要改最后一个字符，base64url 最后一位
	// 可能只包含 padding bits，某些替换会解码成同一组签名字节。
	parts := strings.Split(tok, ".")
	require.Len(t, parts, 3)
	signature := []byte(parts[2])
	require.NotEmpty(t, signature)
	last := signature[0]
	if last == 'A' {
		signature[0] = 'B'
	} else {
		signature[0] = 'A'
	}
	parts[2] = string(signature)
	tampered := strings.Join(parts, ".")

	got, err := ParseToken(tampered, testSecret)
	assert.Error(t, err, "tampered token should fail parse")
	assert.Empty(t, got, "user_id must be empty on parse error")
}

func TestParseToken_Expired(t *testing.T) {
	t.Parallel()

	uid := uuid.NewString()
	// 负数 TTL = 已过期
	tok, err := GenerateToken(uid, testSecret, -1*time.Hour)
	require.NoError(t, err, "generation of expired-on-arrival token should still succeed")

	got, err := ParseToken(tok, testSecret)
	assert.Error(t, err, "expired token should fail parse")
	assert.Empty(t, got)
}

func TestParseToken_Malformed(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		token string
	}{
		{"plain string", "not-a-jwt"},
		{"empty", ""},
		{"two segments", "aaa.bbb"},
		{"random base64", "AAAA.BBBB.CCCC"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseToken(tc.token, testSecret)
			assert.Error(t, err, "malformed token should fail")
			assert.Empty(t, got)
		})
	}
}

func TestGenerateToken_DifferentSecrets(t *testing.T) {
	t.Parallel()

	uid := uuid.NewString()

	tok, err := GenerateToken(uid, testSecret, defaultTTL)
	require.NoError(t, err)

	// 用另一个 secret 解析必须失败
	got, err := ParseToken(tok, otherSecret)
	assert.Error(t, err, "token signed with secretA must NOT verify with secretB")
	assert.Empty(t, got)

	// 用原 secret 仍能解析成功（保证测试本身不是 false positive）
	got2, err := ParseToken(tok, testSecret)
	require.NoError(t, err)
	assert.Equal(t, uid, got2)
}

func TestParseTokenRejectsMissingIssuer(t *testing.T) {
	t.Parallel()

	uid := uuid.NewString()
	now := time.Now()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   uid,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(defaultTTL)),
		},
	})
	signed, err := token.SignedString([]byte(testSecret))
	require.NoError(t, err)

	got, err := ParseToken(signed, testSecret)
	assert.Error(t, err)
	assert.Empty(t, got)
}
