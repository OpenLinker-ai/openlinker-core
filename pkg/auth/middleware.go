package auth

import (
	"context"
	"errors"
	"reflect"
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

// JWTMiddlewareWithUserStatus validates a JWT and its current durable user
// session version before exposing the principal to a protected handler.
func JWTMiddlewareWithUserStatus(secret string, users UserStatusChecker) echo.MiddlewareFunc {
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
			claims, err := ParseTokenClaims(parts[1], secret)
			if err != nil {
				return httpx.Unauthorized("token 无效或已过期")
			}
			uid, err := uuid.Parse(claims.Subject)
			if err != nil {
				return httpx.Unauthorized("token 无效")
			}
			if err := users.EnsureJWTUserVersion(c.Request().Context(), uid, claims.TokenVersion); err != nil {
				return err
			}
			SetPrincipal(c, &AuthPrincipal{UserID: uid, AuthMethod: AuthMethodJWT, Grants: []Grant{}})
			return next(c)
		}
	}
}

func requireUserStatusChecker(users UserStatusChecker) {
	if err := ValidateUserStatusChecker(users); err != nil {
		panic(err)
	}
}

// ValidateUserStatusChecker rejects both a nil interface and an interface that
// contains a typed-nil checker.
func ValidateUserStatusChecker(users UserStatusChecker) error {
	if users == nil {
		return errors.New("auth: user status checker is required")
	}
	value := reflect.ValueOf(users)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if value.IsNil() {
			return errors.New("auth: user status checker is required")
		}
	}
	return nil
}

func loadEnabledUser(ctx context.Context, users userStatusQuerier, userID uuid.UUID) (db.User, error) {
	user, err := users.GetUserByID(ctx, userID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return db.User{}, httpx.Unauthorized("用户不存在或会话已失效")
		}
		log.Error().Err(err).Str("user_id", userID.String()).Msg("auth.middleware: GetUserByID")
		return db.User{}, httpx.Internal("认证状态校验失败")
	}
	if user.DisabledAt != nil {
		return db.User{}, httpx.Unauthorized("账号已禁用")
	}
	return user, nil
}
