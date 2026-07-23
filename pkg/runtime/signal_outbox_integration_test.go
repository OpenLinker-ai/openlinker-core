package runtime

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

func TestRuntimeSignalOutboxRedisFailureAndRecoveryAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	require.NoError(t, err)
	defer pool.Close()
	_, err = pool.Exec(ctx, `DELETE FROM runtime_signal_outbox`)
	require.NoError(t, err)

	userID, agentID := uuid.New(), uuid.New()
	_, err = pool.Exec(ctx, `
		INSERT INTO users (id, email, password_hash, display_name, is_creator)
		VALUES ($1, $2, 'hash', 'Signal Outbox Test', TRUE)`,
		userID, userID.String()+"@example.test")
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO agents (
			id, creator_id, slug, name, description, endpoint_url,
			price_per_call_cents, connection_mode
		) VALUES ($1, $2, $3, 'Signal Outbox Agent', 'Signal outbox fixture',
			'openlinker-runtime://node', 0, 'runtime')`,
		agentID, userID, "signal-outbox-"+agentID.String())
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM runtime_signal_outbox WHERE agent_id = $1`, agentID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM agents WHERE id = $1`, agentID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM users WHERE id = $1`, userID)
	})

	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{
		Addr: server.Addr(), MaxRetries: -1,
		DialTimeout: 100 * time.Millisecond, ReadTimeout: 100 * time.Millisecond,
		WriteTimeout: 100 * time.Millisecond,
	})
	defer func() { _ = client.Close() }()
	bus, err := NewRedisSignalBus(client, RedisSignalBusConfig{
		Channel: "test:runtime:outbox:" + agentID.String(), InstanceID: uuid.New(),
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, bus.Close()) }()
	queries := db.New(pool)
	worker := NewRuntimeSignalOutboxWorker(queries, bus)

	subscriptionCtx, subscriptionCancel := context.WithCancel(ctx)
	received := make(chan RuntimeSignal, 1)
	subscriptionDone := make(chan error, 1)
	go func() {
		subscriptionDone <- bus.Subscribe(subscriptionCtx, func(_ context.Context, signal RuntimeSignal) error {
			received <- signal
			return nil
		})
	}()
	require.Eventually(t, func() bool {
		return server.PubSubNumSub(bus.channel)[bus.channel] == 1
	}, time.Second, time.Millisecond)

	first, err := queries.CreateRuntimeSignal(ctx, db.CreateRuntimeSignalParams{
		EventType: "run.available", AgentID: agentID,
		Payload: []byte(`{"input":"classified","token":"classified"}`),
	})
	require.NoError(t, err)
	result, err := worker.ProcessOnce(ctx, RuntimeSignalOutboxWorkerConfig{})
	require.NoError(t, err)
	require.Equal(t, RuntimeSignalOutboxBatchResult{Claimed: 1, Published: 1}, result)
	require.Equal(t, RuntimeSignal{
		SignalID: first.ID, Type: first.EventType, AgentID: agentID,
	}, <-received)
	persisted, err := queries.GetRuntimeSignalByID(ctx, first.ID)
	require.NoError(t, err)
	require.Equal(t, "published", persisted.Status)
	subscriptionCancel()
	require.ErrorIs(t, <-subscriptionDone, context.Canceled)

	second, err := queries.CreateRuntimeSignal(ctx, db.CreateRuntimeSignalParams{
		EventType: "run.cancel", AgentID: agentID, Payload: []byte(`{}`),
	})
	require.NoError(t, err)
	server.Close()
	result, err = worker.ProcessOnce(ctx, RuntimeSignalOutboxWorkerConfig{
		RetryBase: 50 * time.Millisecond, RetryMaximum: 100 * time.Millisecond,
	})
	require.NoError(t, err)
	require.Equal(t, RuntimeSignalOutboxBatchResult{Claimed: 1, Retried: 1}, result)
	persisted, err = queries.GetRuntimeSignalByID(ctx, second.ID)
	require.NoError(t, err)
	require.Equal(t, "pending", persisted.Status)
	require.Equal(t, "SIGNAL_PUBLISH_FAILED", requireStringPointer(t, persisted.LastError))
	require.NoError(t, server.Restart())
	require.Eventually(t, func() bool {
		healthCtx, healthCancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer healthCancel()
		return bus.Health(healthCtx) == nil
	}, 2*time.Second, 10*time.Millisecond)

	time.Sleep(75 * time.Millisecond)
	result, err = worker.ProcessOnce(ctx, RuntimeSignalOutboxWorkerConfig{
		RetryBase: 50 * time.Millisecond, RetryMaximum: 100 * time.Millisecond,
	})
	require.NoError(t, err)
	require.Equal(t, RuntimeSignalOutboxBatchResult{Claimed: 1, Published: 1}, result)
	persisted, err = queries.GetRuntimeSignalByID(ctx, second.ID)
	require.NoError(t, err)
	require.Equal(t, "published", persisted.Status)
	backlog, err := worker.Backlog(ctx)
	require.NoError(t, err)
	require.Zero(t, backlog)
}

func requireStringPointer(t *testing.T, value *string) string {
	t.Helper()
	require.NotNil(t, value)
	return *value
}
