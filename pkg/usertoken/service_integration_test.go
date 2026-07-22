package usertoken

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

func TestServiceCreateVerifyShrinkRevokeIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL 未设置，跳过 User Token 集成测试")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var tableExists bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('public.user_tokens') IS NOT NULL`).Scan(&tableExists); err != nil || !tableExists {
		t.Skip("测试数据库尚未应用 migration 062")
	}

	userID := uuid.New()
	email := fmt.Sprintf("user-token-%s@example.test", userID)
	_, err = pool.Exec(ctx, `
		INSERT INTO users (id, email, password_hash, display_name)
		VALUES ($1, $2, 'test-hash', 'User Token Test')`, userID, email)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), `DELETE FROM users WHERE id=$1`, userID) })

	svc := NewService(pool)
	expires := time.Now().Add(24 * time.Hour).UTC().Truncate(time.Second)
	created, err := svc.Create(ctx, userID, &CreateRequest{
		Name: "integration", ExpiresAt: &expires,
		Grants: []GrantRequest{
			{Permission: "agents:run", ResourceType: "agent"},
			{Permission: "runs:read", ResourceType: "run"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.PlaintextToken == "" || created.IssuerInstanceID == "" {
		t.Fatalf("created = %#v", created)
	}
	principal, err := svc.VerifyPrincipal(ctx, created.PlaintextToken)
	if err != nil || principal.TokenID == nil || principal.TokenID.String() != created.ID || !principal.UserStatusVerified {
		t.Fatalf("principal = %#v, %v", principal, err)
	}
	var lastUsed, updatedAt *time.Time
	if err := pool.QueryRow(ctx, `SELECT last_used_at, updated_at FROM user_tokens WHERE id=$1`, created.ID).Scan(&lastUsed, &updatedAt); err != nil || lastUsed == nil || updatedAt == nil {
		t.Fatalf("token timestamps = last_used_at %v, updated_at %v, error %v", lastUsed, updatedAt, err)
	}
	if _, err := svc.VerifyPrincipal(ctx, created.PlaintextToken); err != nil {
		t.Fatalf("second VerifyPrincipal: %v", err)
	}
	var secondLastUsed, secondUpdatedAt *time.Time
	if err := pool.QueryRow(ctx, `SELECT last_used_at, updated_at FROM user_tokens WHERE id=$1`, created.ID).Scan(&secondLastUsed, &secondUpdatedAt); err != nil {
		t.Fatalf("second token timestamps: %v", err)
	}
	if secondLastUsed == nil || !secondLastUsed.Equal(*lastUsed) || secondUpdatedAt == nil || !secondUpdatedAt.Equal(*updatedAt) {
		t.Fatalf("coalesced touch changed timestamps: first=(%v,%v) second=(%v,%v)", lastUsed, updatedAt, secondLastUsed, secondUpdatedAt)
	}

	remaining := []GrantRequest{{Permission: "runs:read", ResourceType: "run"}}
	updated, err := svc.Update(ctx, userID, *principal.TokenID, &UpdateRequest{Grants: &remaining})
	if err != nil || len(updated.Grants) != 1 || updated.Grants[0].Permission != "runs:read" {
		t.Fatalf("shrink = %#v, %v", updated, err)
	}
	expanded := []GrantRequest{
		{Permission: "runs:read", ResourceType: "run"},
		{Permission: "agents:run", ResourceType: "agent"},
	}
	_, err = svc.Update(ctx, userID, *principal.TokenID, &UpdateRequest{Grants: &expanded})
	var httpErr *httpx.HTTPError
	if !errors.As(err, &httpErr) || httpErr.Code != httpx.CodePermissionExpansionRequiresNewToken {
		t.Fatalf("expansion error = %#v", err)
	}
	if err := svc.Revoke(ctx, userID, *principal.TokenID); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.VerifyPrincipal(ctx, created.PlaintextToken); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("revoked verify error = %v", err)
	}
}
