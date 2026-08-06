package db

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestUserTokenQueriesScanCredentialAndGrantRows(t *testing.T) {
	now := time.Date(2026, 7, 10, 1, 2, 3, 0, time.UTC)
	userID := uuid.New()
	tokenID := uuid.New()
	expires := now.Add(90 * 24 * time.Hour)
	tokenRow := []any{tokenID, userID, "ci", "ol_user_abcd", "sha256:hash", []string{"runs:read"}, &expires, nil, nil, now, now}
	dbtx := &fakeDBTX{row: fakeRow{values: tokenRow}, execTag: pgconn.NewCommandTag("UPDATE 1")}
	q := New(dbtx)

	created, err := q.CreateUserToken(context.Background(), CreateUserTokenParams{
		UserID: userID, Name: "ci", Prefix: "ol_user_abcd", TokenHash: "sha256:hash",
		Scopes: []string{"runs:read"}, ExpiresAt: &expires,
	})
	if err != nil || created.ID != tokenID || created.TokenHash != "sha256:hash" {
		t.Fatalf("CreateUserToken = %#v, %v", created, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateUserToken")
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{userID, "ci", "ol_user_abcd", "sha256:hash", []string{"runs:read"}, &expires}) {
		t.Fatalf("create args = %#v", dbtx.queryRowArgs)
	}

	dbtx.queryRows = &fakeRows{rows: [][]any{tokenRow, tokenRow}}
	candidates, err := q.ListActiveUserTokensByPrefix(context.Background(), "ol_user_abcd")
	if err != nil || len(candidates) != 2 {
		t.Fatalf("prefix candidates = %#v, %v", candidates, err)
	}
	requireSQLName(t, dbtx.querySQL, "ListActiveUserTokensByPrefix")

	grantID := uuid.New()
	agentID := uuid.New()
	dbtx.row = fakeRow{values: []any{grantID, tokenID, "agents:run", "agent", &agentID, []byte(`{}`), now}}
	grant, err := q.CreateUserTokenCoreGrant(context.Background(), CreateUserTokenCoreGrantParams{
		TokenID: tokenID, Permission: "agents:run", ResourceType: "agent", ResourceID: &agentID, Constraints: []byte(`{}`),
	})
	if err != nil || grant.ResourceID == nil || *grant.ResourceID != agentID {
		t.Fatalf("CreateUserTokenCoreGrant = %#v, %v", grant, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateUserTokenCoreGrant")

	secondTokenID := uuid.New()
	dbtx.queryRows = &fakeRows{rows: [][]any{
		{grantID, tokenID, "agents:run", "agent", &agentID, []byte(`{}`), now},
		{uuid.New(), secondTokenID, "runs:read", "run", nil, []byte(`{}`), now},
	}}
	grants, err := q.ListUserTokenCoreGrantsForTokens(context.Background(), []uuid.UUID{tokenID, secondTokenID})
	if err != nil || len(grants) != 2 || grants[0].TokenID != tokenID || grants[1].TokenID != secondTokenID {
		t.Fatalf("ListUserTokenCoreGrantsForTokens = %#v, %v", grants, err)
	}
	requireSQLName(t, dbtx.querySQL, "ListUserTokenCoreGrantsForTokens")
	if !reflect.DeepEqual(dbtx.queryArgs, []any{[]uuid.UUID{tokenID, secondTokenID}}) {
		t.Fatalf("batch grant args = %#v", dbtx.queryArgs)
	}

	if err := q.TouchUserToken(context.Background(), tokenID); err != nil {
		t.Fatal(err)
	}
	requireSQLName(t, dbtx.execSQL, "TouchUserToken")
}
