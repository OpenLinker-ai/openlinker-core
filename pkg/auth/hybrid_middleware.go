package auth

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/credential"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

// ApiKeyVerifier 抽象 User Token 鉴权能力，避免 auth 与具体 Token 存储或桥接实现耦合。
//
// 实现方应在命中后合并刷新 last_used_at，失败时返回固定错误
// （不暴露内部细节）。
type ApiKeyVerifier interface {
	Verify(ctx context.Context, plaintextToken string) (uuid.UUID, []string, error)
}

// PrincipalAPIKeyVerifier is implemented by Core's local User Token service.
// The legacy Verify method remains temporarily for bridge compatibility.
type PrincipalAPIKeyVerifier interface {
	VerifyPrincipal(ctx context.Context, plaintextToken string) (*AuthPrincipal, error)
}

// HybridAuthMiddleware 同时接受网页登录会话与 User Token。
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

			var principal *AuthPrincipal
			if credential.HasAnyPrefix(token, credential.UserTokenPrefix) {
				if verifier == nil {
					// 配置错误：User Token 验证器未注入
					return httpx.Unauthorized("User Token 鉴权未启用")
				}
				if principalVerifier, ok := verifier.(PrincipalAPIKeyVerifier); ok {
					var err error
					principal, err = principalVerifier.VerifyPrincipal(c.Request().Context(), token)
					if err != nil {
						return httpx.Unauthorized("User Token 无效或已撤销")
					}
				} else {
					uid, scopes, err := verifier.Verify(c.Request().Context(), token)
					if err != nil {
						return httpx.Unauthorized("User Token 无效或已撤销")
					}
					grants := make([]Grant, 0, len(scopes))
					for _, scope := range scopes {
						grants = append(grants, Grant{Permission: scope, ResourceType: resourceTypeForLegacyScope(scope), Constraints: json.RawMessage(`{}`)})
					}
					principal = &AuthPrincipal{UserID: uid, AuthMethod: AuthMethodUserToken, Grants: grants}
				}
			} else {
				uid, err := ParseToken(token, jwtSecret)
				if err != nil {
					return httpx.Unauthorized("token 无效或已过期")
				}
				parsed, err := uuid.Parse(uid)
				if err != nil {
					return httpx.Unauthorized("token 无效")
				}
				principal = &AuthPrincipal{UserID: parsed, AuthMethod: AuthMethodJWT, Grants: []Grant{}}
			}
			if principal == nil {
				return httpx.Unauthorized("认证失败")
			}
			if !principal.UserStatusVerified {
				if err := ensureTokenUserEnabled(c.Request().Context(), users, principal.UserID.String()); err != nil {
					return err
				}
			}
			SetPrincipal(c, principal)
			return next(c)
		}
	}
}

func resourceTypeForLegacyScope(scope string) string {
	switch {
	case strings.HasPrefix(scope, "agents:"), strings.HasPrefix(scope, "agent-tokens:"):
		return "agent"
	case strings.HasPrefix(scope, "runs:"):
		return "run"
	case strings.HasPrefix(scope, "tasks:"):
		return "task"
	case strings.HasPrefix(scope, "workflows:"):
		return "workflow"
	default:
		return "core"
	}
}
