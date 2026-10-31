package runtime_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/credential"
)

// TestAgentTokenLastUsedTracksRuntimeAuthenticationAndHeartbeat proves the
// token list's "last used" value follows real Runtime use against PostgreSQL:
// plaintext authentication and the recorder that long-lived WebSocket
// heartbeats call both record it, at most once every five minutes per token,
// without letting a locked token row stall authentication.
func TestAgentTokenLastUsedTracksRuntimeAuthenticationAndHeartbeat(t *testing.T) {
	pool := setupTestDB(t)
	requireReliableRuntimeSchema(t, pool)
	resetRuntimeNodeAdminTables(t, pool)
	fixture := insertRuntimeNodeAdminFixture(t, pool)
	ctx := context.Background()

	// Token identity columns are immutable, so authentication uses its own
	// token while the recorder uses the fixture Session's credential.
	plaintext, prefix, err := credential.GenerateAgentToken()
	require.NoError(t, err)
	authTokenID := uuid.New()
	_, err = pool.Exec(ctx, `
INSERT INTO agent_tokens (
    id, agent_id, creator_user_id, name, prefix, token_hash, scopes,
    status, redeemed_at
)
SELECT $1, agent_id, creator_user_id, 'last-used-auth', $3, $4,
       ARRAY['agent:pull']::text[], 'active_runtime', clock_timestamp()
FROM agent_tokens WHERE id = $2`, authTokenID, fixture.credentialID, prefix, credential.FastTokenHash(plaintext))
	require.NoError(t, err)

	service := newTestService(t, pool)
	authenticate := func() {
		t.Helper()
		token, authErr := service.ValidateRuntimeToken(ctx, plaintext, "agent:pull")
		require.NoError(t, authErr)
		require.Equal(t, authTokenID, token.ID)
	}

	for _, use := range []struct {
		name    string
		tokenID uuid.UUID
		run     func()
	}{
		{name: "authentication", tokenID: authTokenID, run: authenticate},
		{name: "websocket heartbeat recorder", tokenID: fixture.credentialID, run: func() {
			service.RecordRuntimeTokenUse(ctx, fixture.credentialID)
		}},
	} {
		t.Run(use.name, func(t *testing.T) {
			setAgentTokenLastUsed(t, pool, use.tokenID, nil)
			use.run()
			first := agentTokenLastUsed(t, pool, use.tokenID)
			require.NotNil(t, first, "the first use must be recorded")

			recent := time.Now().Add(-time.Minute)
			setAgentTokenLastUsed(t, pool, use.tokenID, &recent)
			use.run()
			unchanged := agentTokenLastUsed(t, pool, use.tokenID)
			require.NotNil(t, unchanged)
			require.WithinDuration(t, recent, *unchanged, time.Millisecond,
				"uses within five minutes must not rewrite the token row")

			stale := time.Now().Add(-10 * time.Minute)
			setAgentTokenLastUsed(t, pool, use.tokenID, &stale)
			use.run()
			advanced := agentTokenLastUsed(t, pool, use.tokenID)
			require.NotNil(t, advanced)
			require.True(t, advanced.After(stale.Add(9*time.Minute)), "a stale value must advance, got %s", advanced)
		})
	}

	t.Run("locked token row does not stall authentication", func(t *testing.T) {
		stale := time.Now().Add(-10 * time.Minute)
		setAgentTokenLastUsed(t, pool, authTokenID, &stale)
		// Credential issuance and revocation hold the token row FOR UPDATE.
		lockTx, err := pool.Begin(ctx)
		require.NoError(t, err)
		defer func() { _ = lockTx.Rollback(ctx) }()
		_, err = lockTx.Exec(ctx, `SELECT id FROM agent_tokens WHERE id = $1 FOR UPDATE`, authTokenID)
		require.NoError(t, err)

		started := time.Now()
		authenticate()
		require.Less(t, time.Since(started), 5*time.Second, "the bookkeeping write must be time-bounded")
		unchanged := agentTokenLastUsed(t, pool, authTokenID)
		require.NotNil(t, unchanged)
		require.WithinDuration(t, stale, *unchanged, time.Millisecond)
	})

	t.Run("revoked token is never touched", func(t *testing.T) {
		setAgentTokenLastUsed(t, pool, authTokenID, nil)
		_, err := pool.Exec(ctx, `
UPDATE agent_tokens SET status = 'revoked', revoked_at = clock_timestamp(), revocation_kind = 'manual'
WHERE id = $1`, authTokenID)
		require.NoError(t, err)
		service.RecordRuntimeTokenUse(ctx, authTokenID)
		require.Nil(t, agentTokenLastUsed(t, pool, authTokenID))
	})
}

func setAgentTokenLastUsed(t *testing.T, pool *pgxpool.Pool, id uuid.UUID, at *time.Time) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `UPDATE agent_tokens SET last_used_at = $2 WHERE id = $1`, id, at)
	require.NoError(t, err)
}

func agentTokenLastUsed(t *testing.T, pool *pgxpool.Pool, id uuid.UUID) *time.Time {
	t.Helper()
	var at *time.Time
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT last_used_at FROM agent_tokens WHERE id = $1`, id).Scan(&at))
	return at
}
