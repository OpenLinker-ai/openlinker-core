package agent

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func newTestMetricDirtyStore(t *testing.T) (*RedisAgentMetricDirtyStore, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store, err := NewRedisAgentMetricDirtyStore(client, "test:metrics")
	require.NoError(t, err)
	return store, server
}

func TestRedisAgentMetricDirtyStoreFencesNewGenerationDuringClaim(t *testing.T) {
	store, _ := newTestMetricDirtyStore(t)
	ctx := context.Background()
	agentID := uuid.New()
	require.NoError(t, store.Mark(ctx, []uuid.UUID{agentID, agentID}))
	owner := uuid.New()
	claims, err := store.Claim(ctx, owner, time.Minute, 10)
	require.NoError(t, err)
	require.Equal(t, []AgentMetricDirtyClaim{{AgentID: agentID, Version: 1, Owner: owner}}, claims)

	require.NoError(t, store.Mark(ctx, []uuid.UUID{agentID}), "a newer Run event must remain dirty")
	acked, err := store.Ack(ctx, claims[0])
	require.NoError(t, err)
	require.True(t, acked)
	nextOwner := uuid.New()
	next, err := store.Claim(ctx, nextOwner, time.Minute, 10)
	require.NoError(t, err)
	require.Equal(t, []AgentMetricDirtyClaim{{AgentID: agentID, Version: 2, Owner: nextOwner}}, next)
	staleAck, err := store.Ack(ctx, claims[0])
	require.NoError(t, err)
	require.False(t, staleAck, "an old owner cannot acknowledge a newer claim")
}

func TestRedisAgentMetricDirtyStoreNackAndLeaseExpiryRequeue(t *testing.T) {
	store, server := newTestMetricDirtyStore(t)
	ctx := context.Background()
	baseTime := time.Now()
	server.SetTime(baseTime)
	firstID, secondID := uuid.New(), uuid.New()
	require.NoError(t, store.Mark(ctx, []uuid.UUID{firstID, secondID}))
	owner := uuid.New()
	claims, err := store.Claim(ctx, owner, time.Second, 2)
	require.NoError(t, err)
	require.Len(t, claims, 2)

	nacked, err := store.Nack(ctx, claims[0])
	require.NoError(t, err)
	require.True(t, nacked)
	reclaimed, err := store.Claim(ctx, uuid.New(), time.Second, 1)
	require.NoError(t, err)
	require.Len(t, reclaimed, 1)
	require.Equal(t, claims[0].AgentID, reclaimed[0].AgentID)

	server.SetTime(baseTime.Add(2 * time.Second))
	recovered, err := store.Claim(ctx, uuid.New(), time.Second, 2)
	require.NoError(t, err)
	ids := make([]string, 0, len(recovered))
	for _, claim := range recovered {
		ids = append(ids, claim.AgentID.String())
	}
	sort.Strings(ids)
	require.Contains(t, ids, claims[1].AgentID.String(), "expired claim must be made dirty again")
}

func TestRedisAgentMetricCursorAdvancesMonotonically(t *testing.T) {
	store, _ := newTestMetricDirtyStore(t)
	ctx := context.Background()
	_, ok, err := store.Cursor(ctx)
	require.NoError(t, err)
	require.False(t, ok)

	when := time.Date(2026, 7, 20, 12, 0, 0, 123456000, time.UTC)
	first := AgentMetricCursor{Time: when, ID: uuid.MustParse("00000000-0000-0000-0000-000000000001")}
	advanced, err := store.AdvanceCursor(ctx, first)
	require.NoError(t, err)
	require.True(t, advanced)
	advanced, err = store.AdvanceCursor(ctx, AgentMetricCursor{
		Time: when.Add(-time.Microsecond), ID: uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"),
	})
	require.NoError(t, err)
	require.False(t, advanced)
	second := AgentMetricCursor{Time: when, ID: uuid.MustParse("00000000-0000-0000-0000-000000000002")}
	advanced, err = store.AdvanceCursor(ctx, second)
	require.NoError(t, err)
	require.True(t, advanced)
	got, ok, err := store.Cursor(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, second, got)
}

func TestRedisAgentMetricDirtyStoreValidation(t *testing.T) {
	var nilClient *redis.Client
	_, err := NewRedisAgentMetricDirtyStore(nilClient, "")
	require.Error(t, err)
	store, _ := newTestMetricDirtyStore(t)
	require.Error(t, store.Mark(context.Background(), []uuid.UUID{uuid.Nil}))
	_, err = store.Claim(context.Background(), uuid.Nil, time.Minute, 1)
	require.Error(t, err)
	_, err = store.AdvanceCursor(context.Background(), AgentMetricCursor{})
	require.Error(t, err)
}
