package auth

import (
	"context"

	"github.com/google/uuid"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

// UserStatusChecker is the bounded user-session authority shared by HTTP,
// Hybrid HTTP, A2A gRPC, and hosted Cloud composition.
type UserStatusChecker interface {
	EnsureUserEnabled(context.Context, uuid.UUID) error
	EnsureJWTUserVersion(context.Context, uuid.UUID, int64) error
}

type DBUserStatusChecker struct {
	users userStatusQuerier
}

func NewDBUserStatusChecker(dbtx db.DBTX) *DBUserStatusChecker {
	return &DBUserStatusChecker{users: db.New(dbtx)}
}

func (c *DBUserStatusChecker) EnsureUserEnabled(ctx context.Context, userID uuid.UUID) error {
	if c == nil || c.users == nil {
		return httpx.Internal("认证状态校验失败")
	}
	_, err := loadEnabledUser(ctx, c.users, userID)
	if err != nil {
		return err
	}
	return nil
}

func (c *DBUserStatusChecker) EnsureJWTUserVersion(ctx context.Context, userID uuid.UUID, tokenVersion int64) error {
	if c == nil || c.users == nil {
		return httpx.Internal("认证状态校验失败")
	}
	user, err := loadEnabledUser(ctx, c.users, userID)
	if err != nil {
		return err
	}
	if tokenVersion < 0 || user.TokenVersion != tokenVersion {
		return httpx.Unauthorized("token 无效或已过期")
	}
	return nil
}

var _ UserStatusChecker = (*DBUserStatusChecker)(nil)
