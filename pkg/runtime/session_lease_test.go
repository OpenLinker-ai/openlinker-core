package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestRedisRuntimeSessionLeaseBatchRefreshesLeaseAndPresence(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	store, err := NewRedisRuntimeSessionLeaseStore(client, "test:lease", "test:presence")
	require.NoError(t, err)

	record := validTestRuntimeSessionLeaseRecord()
	require.NoError(t, store.RefreshBatch(context.Background(), []RuntimeSessionLeaseRecord{record}, 2*time.Second, 3*time.Second))

	lease, found, err := store.Lookup(context.Background(), record.Lease.RuntimeSessionID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, record.Lease.CoreInstanceID, lease.CoreInstanceID)
	require.Equal(t, record.Lease.AttachmentID, lease.AttachmentID)
	require.Equal(t, record.Lease.ConnectionID, lease.ConnectionID)
	require.False(t, lease.RefreshedAt.IsZero())

	presenceStore, err := NewRedisRuntimePresenceStore(client, "test:presence")
	require.NoError(t, err)
	presences, err := presenceStore.ListByAgent(context.Background(), record.Presence.AgentID)
	require.NoError(t, err)
	require.Equal(t, []RuntimePresence{record.Presence}, presences)

	server.FastForward(2500 * time.Millisecond)
	_, found, err = store.Lookup(context.Background(), record.Lease.RuntimeSessionID)
	require.NoError(t, err)
	require.False(t, found)
	presences, err = presenceStore.ListByAgent(context.Background(), record.Presence.AgentID)
	require.NoError(t, err)
	require.Equal(t, []RuntimePresence{record.Presence}, presences)
}

func TestRedisRuntimeSessionLeaseOldConnectionCannotRemoveReplacement(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	store, err := NewRedisRuntimeSessionLeaseStore(client, "test:lease", "test:presence")
	require.NoError(t, err)

	old := validTestRuntimeSessionLeaseRecord()
	replacement := old
	replacement.Lease.AttachmentID = uuid.New()
	replacement.Lease.ConnectionID = "connection-new"
	replacement.Presence.ConnectionID = replacement.Lease.ConnectionID
	require.NoError(t, store.RefreshBatch(context.Background(), []RuntimeSessionLeaseRecord{old}, time.Minute, time.Minute))
	require.NoError(t, store.RefreshBatch(context.Background(), []RuntimeSessionLeaseRecord{replacement}, time.Minute, time.Minute))
	require.NoError(t, store.Remove(context.Background(), old))

	lease, found, err := store.Lookup(context.Background(), replacement.Lease.RuntimeSessionID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, replacement.Lease.AttachmentID, lease.AttachmentID)
	require.Equal(t, replacement.Lease.ConnectionID, lease.ConnectionID)
	presenceStore, err := NewRedisRuntimePresenceStore(client, "test:presence")
	require.NoError(t, err)
	presences, err := presenceStore.ListByAgent(context.Background(), replacement.Presence.AgentID)
	require.NoError(t, err)
	require.Equal(t, []RuntimePresence{replacement.Presence}, presences)
	redisNow, err := client.Time(context.Background()).Result()
	require.NoError(t, err)
	server.FastForward(2 * time.Minute)
	server.SetTime(redisNow.Add(2 * time.Minute))
	expired, err := store.ListExpired(context.Background(), 10)
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{replacement.Lease.RuntimeSessionID}, expired,
		"stale cleanup must not remove the replacement expiry schedule")
}

func TestRedisRuntimeSessionLeaseExpiryIndexSchedulesOnlyMissingLeases(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	store, err := NewRedisRuntimeSessionLeaseStore(client, "test:lease", "test:presence")
	require.NoError(t, err)
	record := validTestRuntimeSessionLeaseRecord()
	require.NoError(t, store.RefreshBatch(context.Background(), []RuntimeSessionLeaseRecord{record}, 2*time.Second, 3*time.Second))

	require.NoError(t, store.ScheduleCheck(
		context.Background(), record.Lease.RuntimeSessionID, time.Second,
	))
	expired, err := store.ListExpired(context.Background(), 10)
	require.NoError(t, err)
	require.Empty(t, expired, "a live lease must fence a competing absence schedule")

	redisNow, err := client.Time(context.Background()).Result()
	require.NoError(t, err)
	server.FastForward(3 * time.Second)
	server.SetTime(redisNow.Add(3 * time.Second))
	expired, err = store.ListExpired(context.Background(), 10)
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{record.Lease.RuntimeSessionID}, expired)

	require.NoError(t, store.ScheduleCheck(context.Background(), record.Lease.RuntimeSessionID, time.Minute))
	expired, err = store.ListExpired(context.Background(), 10)
	require.NoError(t, err)
	require.Empty(t, expired)
	require.NoError(t, store.Forget(context.Background(), record.Lease.RuntimeSessionID))
	indexSize, err := client.ZCard(context.Background(), store.leaseIndexKey()).Result()
	require.NoError(t, err)
	require.Zero(t, indexSize)
}

func TestRedisRuntimeSessionLeaseRejectsInvalidHintsAndBatches(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	store, err := NewRedisRuntimeSessionLeaseStore(client, "test:lease", "test:presence")
	require.NoError(t, err)

	record := validTestRuntimeSessionLeaseRecord()
	require.NoError(t, store.RefreshBatch(context.Background(), []RuntimeSessionLeaseRecord{record}, time.Minute, time.Minute))
	key := store.leaseKey(record.Lease.RuntimeSessionID)
	require.NoError(t, client.Set(context.Background(), key, `{"version":1,"unknown":"rejected"}`, time.Minute).Err())
	_, found, err := store.Lookup(context.Background(), record.Lease.RuntimeSessionID)
	require.Error(t, err)
	require.False(t, found)

	require.Error(t, store.RefreshBatch(context.Background(), []RuntimeSessionLeaseRecord{record, record}, time.Minute, time.Minute))
	tooLarge := make([]RuntimeSessionLeaseRecord, maxRuntimeSessionLeaseBatch+1)
	require.Error(t, store.RefreshBatch(context.Background(), tooLarge, time.Minute, time.Minute))
	bad := record
	bad.Presence.ConnectionID = "different"
	require.Error(t, store.RefreshBatch(context.Background(), []RuntimeSessionLeaseRecord{bad}, time.Minute, time.Minute))
}

func TestRuntimeSessionLeaseManagerBatchesAndFailsClosedToDatabaseFallback(t *testing.T) {
	store := &runtimeSessionLeaseStoreFake{}
	manager, err := NewRuntimeSessionLeaseManager(store, RuntimeSessionLeaseManagerConfig{
		RefreshInterval: time.Second,
		LeaseTTL:        3 * time.Second,
		PresenceTTL:     4 * time.Second,
		Warmup:          time.Minute,
		BatchSize:       2,
		DisableJitter:   true,
	})
	require.NoError(t, err)

	records := []RuntimeSessionLeaseRecord{
		validTestRuntimeSessionLeaseRecord(),
		validTestRuntimeSessionLeaseRecord(),
		validTestRuntimeSessionLeaseRecord(),
	}
	for index := range records {
		records[index].Lease.ConnectionID = "connection-" + string(rune('a'+index))
		records[index].Presence.ConnectionID = records[index].Lease.ConnectionID
		record := records[index]
		require.NoError(t, manager.Register(record))
		require.False(t, manager.HealthyFor(record.Lease.ConnectionID))
	}
	manager.refresh(context.Background())
	require.Equal(t, []int{2, 1}, store.batchSizes())
	for _, record := range records {
		require.True(t, manager.HealthyFor(record.Lease.ConnectionID))
	}
	require.False(t, manager.AbsenceReady())

	manager.mu.Lock()
	manager.health.HealthySince = time.Now().Add(-time.Minute)
	manager.mu.Unlock()
	require.True(t, manager.AbsenceReady())

	store.setRefreshError(errors.New("redis unavailable"))
	manager.refresh(context.Background())
	require.False(t, manager.HealthyFor(records[0].Lease.ConnectionID))
	require.False(t, manager.AbsenceReady())
	require.Equal(t, "refresh_failed", manager.Health().Reason)

	store.setRefreshError(nil)
	manager.refresh(context.Background())
	require.True(t, manager.HealthyFor(records[0].Lease.ConnectionID))
	require.False(t, manager.AbsenceReady(), "Redis recovery must restart the absence warmup")

	mismatch := records[0].Presence
	mismatch.RuntimeSessionID = uuid.New()
	require.False(t, manager.UpdatePresence(records[0].Lease.ConnectionID, mismatch))
	removed, err := manager.Unregister(context.Background(), records[0].Lease.ConnectionID, records[0].Lease.AttachmentID)
	require.NoError(t, err)
	require.True(t, removed)
	require.False(t, manager.HealthyFor(records[0].Lease.ConnectionID))
	require.Equal(t, []string{records[0].Lease.ConnectionID}, store.removedConnections())
}

func TestRuntimeSessionLeaseManagerReplacementIsIdentityFenced(t *testing.T) {
	store := &runtimeSessionLeaseStoreFake{}
	manager, err := NewRuntimeSessionLeaseManager(store, RuntimeSessionLeaseManagerConfig{})
	require.NoError(t, err)
	old := validTestRuntimeSessionLeaseRecord()
	replacement := old
	replacement.Lease.AttachmentID = uuid.New()
	replacement.Lease.ConnectionID = "replacement"
	replacement.Presence.ConnectionID = "replacement"
	require.NoError(t, manager.Register(old))
	require.NoError(t, manager.Register(replacement))

	removed, err := manager.Unregister(context.Background(), old.Lease.ConnectionID, old.Lease.AttachmentID)
	require.NoError(t, err)
	require.False(t, removed)
	require.Equal(t, 1, manager.Health().Registered)
	manager.refresh(context.Background())
	require.True(t, manager.HealthyFor(replacement.Lease.ConnectionID))
}

func TestRuntimeSessionLeaseManagerDoesNotResurrectExpiredLocalLease(t *testing.T) {
	store := &runtimeSessionLeaseStoreFake{}
	manager, err := NewRuntimeSessionLeaseManager(store, RuntimeSessionLeaseManagerConfig{
		LeaseTTL: 2 * time.Second, PresenceTTL: 3 * time.Second, DisableJitter: true,
	})
	require.NoError(t, err)
	record := validTestRuntimeSessionLeaseRecord()
	require.NoError(t, manager.Register(record))
	require.NoError(t, manager.RefreshConnection(context.Background(), record.Lease.ConnectionID))
	require.True(t, manager.HealthyFor(record.Lease.ConnectionID))

	manager.mu.Lock()
	managed := manager.records[record.Lease.ConnectionID]
	managed.registeredAt = time.Now().Add(-time.Minute)
	managed.lastRefreshedAt = time.Now().Add(-time.Minute)
	manager.records[record.Lease.ConnectionID] = managed
	manager.mu.Unlock()
	require.False(t, manager.HealthyFor(record.Lease.ConnectionID))

	manager.refresh(context.Background())
	require.Equal(t, []int{1, 0}, store.batchSizes(),
		"an expired local record needs a database heartbeat before Redis can be recreated")
}

func validTestRuntimeSessionLeaseRecord() RuntimeSessionLeaseRecord {
	presence := validTestRuntimePresence()
	lease := RuntimeSessionLease{
		Version:          runtimeSessionLeaseVersion,
		CoreInstanceID:   presence.CoreInstanceID,
		NodeID:           presence.NodeID,
		AgentID:          presence.AgentID,
		RuntimeSessionID: presence.RuntimeSessionID,
		AttachmentID:     uuid.New(),
		ConnectionID:     presence.ConnectionID,
		WorkerID:         presence.WorkerID,
		SessionEpoch:     1,
	}
	return RuntimeSessionLeaseRecord{Lease: lease, Presence: presence}
}

type runtimeSessionLeaseStoreFake struct {
	mu         sync.Mutex
	batches    []int
	refreshErr error
	removed    []string
	leases     map[uuid.UUID]RuntimeSessionLease
	expired    []uuid.UUID
	scheduled  map[uuid.UUID]time.Time
	forgotten  []uuid.UUID
}

func (f *runtimeSessionLeaseStoreFake) RefreshBatch(
	_ context.Context,
	records []RuntimeSessionLeaseRecord,
	_, _ time.Duration,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.refreshErr != nil {
		return f.refreshErr
	}
	f.batches = append(f.batches, len(records))
	if f.leases == nil {
		f.leases = make(map[uuid.UUID]RuntimeSessionLease)
	}
	for _, record := range records {
		f.leases[record.Lease.RuntimeSessionID] = record.Lease
	}
	return nil
}

func (f *runtimeSessionLeaseStoreFake) Lookup(
	_ context.Context,
	runtimeSessionID uuid.UUID,
) (RuntimeSessionLease, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	lease, ok := f.leases[runtimeSessionID]
	return lease, ok, nil
}

func (f *runtimeSessionLeaseStoreFake) Remove(_ context.Context, record RuntimeSessionLeaseRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removed = append(f.removed, record.Lease.ConnectionID)
	delete(f.leases, record.Lease.RuntimeSessionID)
	return nil
}

func (f *runtimeSessionLeaseStoreFake) ListExpired(_ context.Context, limit int) ([]uuid.UUID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if limit > len(f.expired) {
		limit = len(f.expired)
	}
	return append([]uuid.UUID(nil), f.expired[:limit]...), nil
}

func (f *runtimeSessionLeaseStoreFake) ScheduleCheck(
	_ context.Context,
	runtimeSessionID uuid.UUID,
	after time.Duration,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.scheduled == nil {
		f.scheduled = make(map[uuid.UUID]time.Time)
	}
	f.scheduled[runtimeSessionID] = time.Now().Add(after)
	return nil
}

func (f *runtimeSessionLeaseStoreFake) Forget(_ context.Context, runtimeSessionID uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.forgotten = append(f.forgotten, runtimeSessionID)
	return nil
}

func (f *runtimeSessionLeaseStoreFake) setRefreshError(err error) {
	f.mu.Lock()
	f.refreshErr = err
	f.mu.Unlock()
}

func (f *runtimeSessionLeaseStoreFake) batchSizes() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int(nil), f.batches...)
}

func (f *runtimeSessionLeaseStoreFake) removedConnections() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.removed...)
}
