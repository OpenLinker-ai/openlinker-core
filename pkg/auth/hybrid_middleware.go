package auth

import (
	"context"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/credential"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

// ApiKeyVerifier 抽象 User Token 鉴权能力，避免 auth 与具体 Token 存储或桥接实现耦合。
//
// 实现方（internal/apikey.Service）应在命中后异步刷新 last_used_at，
// 失败时返回固定错误（不暴露内部细节）。
type ApiKeyVerifier interface {
	Verify(ctx context.Context, plaintextToken string) (uuid.UUID, []string, error)
}

// HybridAuthMiddleware 同时接受网页登录会话与访问令牌。
//
// 判定规则：
//   - Authorization: Bearer ol_user_xxx → 走 ApiKeyVerifier
//   - Authorization: Bearer eyJ... 或其他 → 走 JWT
//
// 命中后写入 c.Set(httpx.CtxKeyUserID, userID)，与 JWTMiddleware 行为一致；
// 这样下游 handler 用 httpx.UserIDFrom(c) 即可拿到当前用户。
func HybridAuthMiddleware(jwtSecret string, verifier ApiKeyVerifier) echo.MiddlewareFunc {
	return HybridAuthMiddlewareWithUserStatus(jwtSecret, verifier, nil)
}

// HybridAuthMiddlewareWithUserStatus additionally rejects deleted or disabled users.
func HybridAuthMiddlewareWithUserStatus(jwtSecret string, verifier ApiKeyVerifier, users userStatusQuerier) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			h := c.Request().Header.Get(echo.HeaderAuthorization)
			if h == "" {
				return httpx.Unauthorized("缺少 Authorization 头")
			}
			parts := strings.SplitN(h, " ", 2)
			if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] == "" {
				return httpx.Unauthorized("Authorization 格式错误")
			}
			token := parts[1]

			var userID string
			var authMethod string
			if credential.HasAnyPrefix(token, credential.UserTokenPrefix) {
				if verifier == nil {
					// 配置错误：访问令牌验证器未注入
					return httpx.Unauthorized("访问令牌鉴权未启用")
				}
				uid, scopes, err := verifier.Verify(c.Request().Context(), token)
				if err != nil {
					return httpx.Unauthorized("访问令牌无效或已撤销")
				}
				userID = uid.String()
				authMethod = "user_token"
				c.Set(string(httpx.CtxKeyAuthScopes), scopes)
			} else {
				uid, err := ParseToken(token, jwtSecret)
				if err != nil {
					return httpx.Unauthorized("token 无效或已过期")
				}
				userID = uid
				authMethod = "jwt"
			}
			if err := ensureTokenUserEnabled(c.Request().Context(), users, userID); err != nil {
				return err
			}

			c.Set(string(httpx.CtxKeyUserID), userID)
			c.Set(string(httpx.CtxKeyAuthMethod), authMethod)
			return next(c)
		}
	}
}
