package externalexecution

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/eventwake"
)

func TestExternalExecutionCancellationCommitEmitsPayloadFreeWake(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	listener, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer listener.Release()
	_, err = listener.Exec(ctx, `LISTEN openlinker_external_v1`)
	require.NoError(t, err)

	actorID, requestID := uuid.New(), uuid.New()
	caller := "wake-integration-" + actorID.String()
	_, err = pool.Exec(ctx,
		`INSERT INTO users (id,email,password_hash,display_name) VALUES ($1,$2,'x','Wake Integration')`,
		actorID, actorID.String()+"@external-wake.test")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(),
			`DELETE FROM external_execution_cancellations WHERE caller_service_id=$1`, caller)
		_, _ = pool.Exec(context.Background(),
			`DELETE FROM external_execution_keys WHERE caller_service_id=$1`, caller)
		_, _ = pool.Exec(context.Background(), `DELETE FROM users WHERE id=$1`, actorID)
	})
	_, err = pool.Exec(ctx, `
		INSERT INTO external_execution_keys (caller_service_id, external_request_id, actor_user_id)
		VALUES ($1,$2,$3)`, caller, requestID, actorID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO external_execution_cancellations (
			id, caller_service_id, external_request_id, actor_user_id, reason_code, state
		) VALUES ($1,$2,$3,$4,'CALLER_REQUESTED','requested')`,
		uuid.New(), caller, requestID, actorID)
	require.NoError(t, err)

	notification, err := listener.Conn().WaitForNotification(ctx)
	require.NoError(t, err)
	require.Equal(t, "openlinker_external_v1", notification.Channel)
	envelope, err := eventwake.ParseEnvelope([]byte(notification.Payload))
	require.NoError(t, err)
	require.Equal(t, externalCancellationWakeTopic, envelope.Topic)
	require.Equal(t, requestID.String(), envelope.ResourceID)
	for _, forbidden := range []string{caller, "input", "output", "token"} {
		require.False(t, strings.Contains(notification.Payload, forbidden),
			"wake payload must not contain %q", forbidden)
	}
}
