package auth

import (
	"strings"

	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

// JWTMiddleware 校验 Authorization: Bearer <token>。
//
// 成功：c.Set(string(httpx.CtxKeyUserID), userID) 后 next。
// 失败：返回 httpx.Unauthorized()，由全局 ErrorHandler 转成统一错误响应。
func JWTMiddleware(secret string) echo.MiddlewareFunc {
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
			userID, err := ParseToken(parts[1], secret)
			if err != nil {
				return httpx.Unauthorized("token 无效或已过期")
			}
			c.Set(string(httpx.CtxKeyUserID), userID)
			return next(c)
		}
	}
}
