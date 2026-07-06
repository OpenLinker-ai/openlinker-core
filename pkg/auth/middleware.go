package auth

import (
	"context"
	"errors"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

type userStatusQuerier interface {
	GetUserByID(context.Context, uuid.UUID) (db.User, error)
}

// JWTMiddleware 校验 Authorization: Bearer <token>。
//
// 成功：c.Set(string(httpx.CtxKeyUserID), userID) 后 next。
// 失败：返回 httpx.Unauthorized()，由全局 ErrorHandler 转成统一错误响应。
func JWTMiddleware(secret string) echo.MiddlewareFunc {
	return JWTMiddlewareWithUserStatus(secret, nil)
}

// JWTMiddlewareWithUserStatus additionally rejects deleted or disabled users.
func JWTMiddlewareWithUserStatus(secret string, users userStatusQuerier) echo.MiddlewareFunc {
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
			if err := ensureTokenUserEnabled(c.Request().Context(), users, userID); err != nil {
				return err
			}
			c.Set(string(httpx.CtxKeyUserID), userID)
			return next(c)
		}
	}
}

func ensureTokenUserEnabled(ctx context.Context, users userStatusQuerier, userID string) error {
	if users == nil {
		return nil
	}
	uid, err := uuid.Parse(userID)
	if err != nil {
		return httpx.Unauthorized("token 无效")
	}
	user, err := users.GetUserByID(ctx, uid)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return httpx.Unauthorized("用户不存在或会话已失效")
		}
		log.Error().Err(err).Str("user_id", uid.String()).Msg("auth.middleware: GetUserByID")
		return httpx.Internal("认证状态校验失败")
	}
	if user.DisabledAt != nil {
		return httpx.Unauthorized("账号已禁用")
	}
	return nil
}
