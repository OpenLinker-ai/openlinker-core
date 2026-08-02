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

// HybridAuthMiddlewareWithUserStatus accepts JWT sessions and User Tokens while
// requiring the durable user-status authority for every unverified principal.
func HybridAuthMiddlewareWithUserStatus(jwtSecret string, verifier ApiKeyVerifier, users UserStatusChecker) echo.MiddlewareFunc {
	requireUserStatusChecker(users)
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
				claims, err := ParseTokenClaims(token, jwtSecret)
				if err != nil {
					return httpx.Unauthorized("token 无效或已过期")
				}
				parsed, err := uuid.Parse(claims.Subject)
				if err != nil {
					return httpx.Unauthorized("token 无效")
				}
				if err := users.EnsureJWTUserVersion(c.Request().Context(), parsed, claims.TokenVersion); err != nil {
					return err
				}
				principal = &AuthPrincipal{UserID: parsed, AuthMethod: AuthMethodJWT, Grants: []Grant{}, UserStatusVerified: true}
			}
			if principal == nil {
				return httpx.Unauthorized("认证失败")
			}
			if !principal.UserStatusVerified {
				if err := users.EnsureUserEnabled(c.Request().Context(), principal.UserID); err != nil {
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
