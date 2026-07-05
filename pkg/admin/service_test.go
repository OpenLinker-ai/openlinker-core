package admin

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"golang.org/x/crypto/bcrypt"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

func TestCreateUserNormalizesFlagsAndHashesPassword(t *testing.T) {
	userID := uuid.New()
	now := time.Date(2026, 6, 25, 10, 0, 0, 0, time.UTC)
	fake := &adminFakeDBTX{
		row: adminFakeRow{values: adminUserRow(userID, "manager@example.com", "Manager User", true, true, false, now)},
	}
	svc := NewService(fake)

	item, err := svc.CreateUser(context.Background(), &CreateUserRequest{
		Email:           "  Manager@Example.COM  ",
		DisplayName:     "  Manager User  ",
		Password:        "password123",
		CreatorVerified: true,
	})
	if err != nil {
		t.Fatalf("CreateUser error = %v", err)
	}
	if item.ID != userID.String() || item.Email != "manager@example.com" || !item.IsCreator || !item.CreatorVerified {
		t.Fatalf("CreateUser item = %#v", item)
	}
	if !strings.Contains(fake.queryRowSQL, "-- name: CreateAdminUser ") {
		t.Fatalf("CreateUser SQL = %q", fake.queryRowSQL)
	}
	if len(fake.queryRowArgs) != 6 {
		t.Fatalf("CreateUser args = %#v", fake.queryRowArgs)
	}
	if fake.queryRowArgs[0] != "manager@example.com" || fake.queryRowArgs[2] != "Manager User" {
		t.Fatalf("CreateUser normalized args = %#v", fake.queryRowArgs)
	}
	if fake.queryRowArgs[3] != false || fake.queryRowArgs[4] != true || fake.queryRowArgs[5] != true {
		t.Fatalf("CreateUser flag args = %#v", fake.queryRowArgs[3:])
	}
	hash, ok := fake.queryRowArgs[1].(*string)
	if !ok || hash == nil || *hash == "" {
		t.Fatalf("CreateUser password hash arg = %#v", fake.queryRowArgs[1])
	}
	if err := bcrypt.CompareHashAndPassword([]byte(*hash), []byte("password123")); err != nil {
		t.Fatalf("password hash does not match original password: %v", err)
	}
}

func TestCreateUserValidation(t *testing.T) {
	cases := []struct {
		name string
		req  *CreateUserRequest
		want int
	}{
		{name: "nil", req: nil, want: http.StatusBadRequest},
		{name: "bad email", req: &CreateUserRequest{Email: "bad", DisplayName: "User", Password: "password123"}, want: http.StatusUnprocessableEntity},
		{name: "short display name", req: &CreateUserRequest{Email: "user@example.com", DisplayName: "x", Password: "password123"}, want: http.StatusUnprocessableEntity},
		{name: "short password", req: &CreateUserRequest{Email: "user@example.com", DisplayName: "User", Password: "short"}, want: http.StatusUnprocessableEntity},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := &adminFakeDBTX{}
			svc := NewService(fake)
			_, err := svc.CreateUser(context.Background(), tc.req)
			assertAdminHTTPStatus(t, err, tc.want)
			if fake.queryRowSQL != "" {
				t.Fatalf("validation failure should not query db, got %q", fake.queryRowSQL)
			}
		})
	}
}

func TestCreateUserDuplicateEmail(t *testing.T) {
	fake := &adminFakeDBTX{row: adminFakeRow{err: adminUniqueErr{}}}
	svc := NewService(fake)

	_, err := svc.CreateUser(context.Background(), &CreateUserRequest{
		Email:       "dupe@example.com",
		DisplayName: "Dupe User",
		Password:    "password123",
	})
	assertAdminHTTPStatus(t, err, http.StatusConflict)
}

func TestSummaryIncludesTaskCounters(t *testing.T) {
	fake := &adminFakeDBTX{
		row: adminFakeRow{values: []any{
			int32(12), int32(2), int32(5), int32(3), int32(9), int32(8), int32(1), int32(4), int32(6),
			int32(17), int32(7), int32(10), int32(4), int32(3), int32(5), int32(2), int32(1),
		}},
	}
	svc := NewService(fake)

	summary, err := svc.Summary(context.Background())
	if err != nil {
		t.Fatalf("Summary error = %v", err)
	}
	if summary.TotalTasks != 17 || summary.PublicTasks != 7 || summary.CompletedTasks != 5 || summary.RevisionRequestedTasks != 1 {
		t.Fatalf("Summary task counters = %#v", summary)
	}
	if !strings.Contains(fake.queryRowSQL, "-- name: GetAdminSummary ") {
		t.Fatalf("Summary SQL = %q", fake.queryRowSQL)
	}
}

func TestToUserItemIncludesAuthSummary(t *testing.T) {
	now := time.Date(2026, 7, 6, 9, 30, 0, 0, time.UTC)
	hash := "stored-hash"
	provider := "google"

	cases := []struct {
		name            string
		passwordHash    *string
		oauthProvider   *string
		wantHasPassword bool
		wantIsOAuthUser bool
		wantProvider    string
		wantAuthMethod  string
	}{
		{name: "password", passwordHash: &hash, wantHasPassword: true, wantAuthMethod: "password"},
		{name: "oauth", oauthProvider: &provider, wantIsOAuthUser: true, wantProvider: "google", wantAuthMethod: "oauth"},
		{name: "linked", passwordHash: &hash, oauthProvider: &provider, wantHasPassword: true, wantIsOAuthUser: true, wantProvider: "google", wantAuthMethod: "password_oauth"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			item := toUserItem(&db.User{
				ID:            uuid.New(),
				Email:         "user@example.com",
				DisplayName:   "User",
				PasswordHash:  tc.passwordHash,
				OauthProvider: tc.oauthProvider,
				CreatedAt:     now,
				UpdatedAt:     now,
			})
			if item.HasPassword != tc.wantHasPassword ||
				item.IsOAuthUser != tc.wantIsOAuthUser ||
				item.OAuthProvider != tc.wantProvider ||
				item.AuthMethod != tc.wantAuthMethod {
				t.Fatalf("auth summary = %#v", item)
			}
		})
	}
}

func assertAdminHTTPStatus(t *testing.T, err error, want int) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error status %d, got nil", want)
	}
	var he *httpx.HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("expected *httpx.HTTPError, got %T (%v)", err, err)
	}
	if he.Status != want {
		t.Fatalf("status = %d, want %d", he.Status, want)
	}
}

func adminUserRow(id uuid.UUID, email, displayName string, isCreator, creatorVerified, isAdmin bool, now time.Time) []any {
	hash := "stored-hash"
	return []any{
		id,
		email,
		&hash,
		nil,
		nil,
		displayName,
		nil,
		isCreator,
		creatorVerified,
		isAdmin,
		now,
		now,
		nil,
	}
}

type adminFakeDBTX struct {
	queryRowSQL  string
	queryRowArgs []any
	row          pgx.Row
}

func (f *adminFakeDBTX) Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

func (f *adminFakeDBTX) Query(context.Context, string, ...interface{}) (pgx.Rows, error) {
	return nil, nil
}

func (f *adminFakeDBTX) QueryRow(_ context.Context, sql string, args ...interface{}) pgx.Row {
	f.queryRowSQL = sql
	f.queryRowArgs = append([]any(nil), args...)
	return f.row
}

type adminFakeRow struct {
	values []any
	err    error
}

func (r adminFakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(r.values) != len(dest) {
		return errors.New("scan destination count mismatch")
	}
	for i := range dest {
		target := reflect.ValueOf(dest[i])
		if target.Kind() != reflect.Ptr || target.IsNil() {
			return errors.New("scan target must be a non-nil pointer")
		}
		slot := target.Elem()
		if r.values[i] == nil {
			slot.Set(reflect.Zero(slot.Type()))
			continue
		}
		value := reflect.ValueOf(r.values[i])
		if value.Type().AssignableTo(slot.Type()) {
			slot.Set(value)
			continue
		}
		if value.Type().ConvertibleTo(slot.Type()) {
			slot.Set(value.Convert(slot.Type()))
			continue
		}
		return errors.New("scan value type mismatch")
	}
	return nil
}

type adminUniqueErr struct{}

func (adminUniqueErr) Error() string {
	return "duplicate"
}

func (adminUniqueErr) SQLState() string {
	return "23505"
}
