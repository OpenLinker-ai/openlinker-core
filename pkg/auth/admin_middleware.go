package auth

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

type userByIDQuerier interface {
	GetUserByID(context.Context, uuid.UUID) (db.User, error)
}

// AdminMiddleware 校验当前登录用户的 is_admin 标志。
func AdminMiddleware(queries userByIDQuerier) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			idStr := httpx.UserIDFrom(c)
			if idStr == "" {
				return httpx.Unauthorized("")
			}
			uid, err := uuid.Parse(idStr)
			if err != nil {
				return httpx.Unauthorized("token 无效")
			}
			user, err := queries.GetUserByID(c.Request().Context(), uid)
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return httpx.Unauthorized("用户不存在")
				}
				log.Error().Err(err).Msg("auth.admin: GetUserByID")
				return httpx.Internal("权限校验失败")
			}
			if user.DisabledAt != nil {
				return httpx.Unauthorized("账号已禁用")
			}
			if !user.IsAdmin {
				return httpx.Forbidden("需要管理员权限")
			}
			c.Set(string(httpx.CtxKeyAdmin), true)
			return next(c)
		}
	}
}
