package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/labstack/echo/v4"

	db "github.com/kinzhi/openlinker-core/pkg/db/generated"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

func TestAdminMiddleware(t *testing.T) {
	adminID := uuid.New()

	t.Run("allows admin", func(t *testing.T) {
		q := &fakeUserByIDQuerier{user: db.User{ID: adminID, IsAdmin: true}}
		c := newAdminMiddlewareContext(adminID.String())
		nextCalled := false

		err := AdminMiddleware(q)(func(c echo.Context) error {
			nextCalled = true
			if !httpx.IsAdmin(c) {
				t.Fatalf("admin flag should be set")
			}
			return c.NoContent(http.StatusOK)
		})(c)

		if err != nil {
			t.Fatalf("AdminMiddleware error = %v", err)
		}
		if !nextCalled || q.seenUserID != adminID {
			t.Fatalf("next=%v seen=%s", nextCalled, q.seenUserID)
		}
	})

	t.Run("rejects missing user context", func(t *testing.T) {
		c := newAdminMiddlewareContext("")
		requireAuthHTTPStatus(t, AdminMiddleware(&fakeUserByIDQuerier{})(noopAdminNext)(c), http.StatusUnauthorized)
	})

	t.Run("rejects invalid user id", func(t *testing.T) {
		c := newAdminMiddlewareContext("not-a-uuid")
		requireAuthHTTPStatus(t, AdminMiddleware(&fakeUserByIDQuerier{})(noopAdminNext)(c), http.StatusUnauthorized)
	})

	t.Run("rejects missing database user", func(t *testing.T) {
		c := newAdminMiddlewareContext(adminID.String())
		requireAuthHTTPStatus(t, AdminMiddleware(&fakeUserByIDQuerier{err: pgx.ErrNoRows})(noopAdminNext)(c), http.StatusUnauthorized)
	})

	t.Run("reports database errors", func(t *testing.T) {
		c := newAdminMiddlewareContext(adminID.String())
		requireAuthHTTPStatus(t, AdminMiddleware(&fakeUserByIDQuerier{err: errors.New("db down")})(noopAdminNext)(c), http.StatusInternalServerError)
	})

	t.Run("rejects non-admin", func(t *testing.T) {
		c := newAdminMiddlewareContext(adminID.String())
		requireAuthHTTPStatus(t, AdminMiddleware(&fakeUserByIDQuerier{user: db.User{ID: adminID}})(noopAdminNext)(c), http.StatusForbidden)
	})
}

type fakeUserByIDQuerier struct {
	user       db.User
	err        error
	seenUserID uuid.UUID
}

func (f *fakeUserByIDQuerier) GetUserByID(_ context.Context, userID uuid.UUID) (db.User, error) {
	f.seenUserID = userID
	return f.user, f.err
}

func newAdminMiddlewareContext(userID string) echo.Context {
	req := httptest.NewRequest(http.MethodGet, "/admin", nil)
	rec := httptest.NewRecorder()
	c := echo.New().NewContext(req, rec)
	if userID != "" {
		c.Set(string(httpx.CtxKeyUserID), userID)
	}
	return c
}

func noopAdminNext(c echo.Context) error {
	return c.NoContent(http.StatusOK)
}
