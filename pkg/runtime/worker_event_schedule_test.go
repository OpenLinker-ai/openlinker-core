package runtime

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/eventwake"
)

type workerWakeSourceFake struct {
	hub       *eventwake.Hub
	connected atomic.Bool
}

func newWorkerWakeSourceFake(connected bool) *workerWakeSourceFake {
	source := &workerWakeSourceFake{hub: eventwake.NewHub()}
	source.connected.Store(connected)
	return source
}

func (s *workerWakeSourceFake) Health() eventwake.ListenerHealth {
	return eventwake.ListenerHealth{Connected: s.connected.Load()}
}

func (s *workerWakeSourceFake) SubscribeTopic(topic string) (*eventwake.Subscription, error) {
	return s.hub.Subscribe(topic), nil
}

func (s *workerWakeSourceFake) publish(topic string) { s.hub.Publish(topic) }

type scheduledRuntimeSignalStore struct {
	*runtimeSignalOutboxStoreFake
	mu           sync.Mutex
	claimCalls   int
	nextDueCalls int
	nextDue      db.NextRuntimeSignalDueRow
}

func newScheduledRuntimeSignalStore() *scheduledRuntimeSignalStore {
	return &scheduledRuntimeSignalStore{
		runtimeSignalOutboxStoreFake: &runtimeSignalOutboxStoreFake{},
		nextDue:                      db.NextRuntimeSignalDueRow{DatabaseNow: time.Now()},
	}
}

func (s *scheduledRuntimeSignalStore) ClaimRuntimeSignals(
	ctx context.Context,
	params db.ClaimRuntimeSignalsParams,
) ([]db.RuntimeSignalOutbox, error) {
	s.mu.Lock()
	s.claimCalls++
	s.mu.Unlock()
	return s.runtimeSignalOutboxStoreFake.ClaimRuntimeSignals(ctx, params)
}

func (s *scheduledRuntimeSignalStore) NextRuntimeSignalDue(
	context.Context,
) (db.NextRuntimeSignalDueRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextDueCalls++
	return s.nextDue, nil
}

func (s *scheduledRuntimeSignalStore) counts() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.claimCalls, s.nextDueCalls
}

func TestRuntimeSignalEventWorkerHasNoIdleDatabaseSlopeAndWakesOnEvent(t *testing.T) {
	store := newScheduledRuntimeSignalStore()
	source := newWorkerWakeSourceFake(true)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		StartRuntimeSignalOutboxWorkerWithWake(
			ctx,
			NewRuntimeSignalOutboxWorker(store, &runtimeSignalBusFake{}),
			RuntimeSignalOutboxWorkerConfig{Interval: 10 * time.Millisecond},
			source,
		)
		close(done)
	}()

	require.Eventually(t, func() bool {
		claims, due := store.counts()
		return claims == 1 && due == 1
	}, time.Second, time.Millisecond)
	time.Sleep(40 * time.Millisecond)
	claims, due := store.counts()
	require.Equal(t, 1, claims, "health checks must not claim while idle")
	require.Equal(t, 1, due, "health checks must not read next-due while idle")

	source.publish(runtimeSignalWorkerWakeTopic)
	require.Eventually(t, func() bool {
		claims, due := store.counts()
		return claims == 2 && due == 2
	}, time.Second, time.Millisecond)
	cancel()
	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
}

func TestRuntimeSignalEventWorkerUsesDatabaseDueTimerAndPollingFallback(t *testing.T) {
	store := newScheduledRuntimeSignalStore()
	now := time.Now()
	due := now.Add(25 * time.Millisecond)
	store.nextDue = db.NextRuntimeSignalDueRow{NextDueAt: &due, DatabaseNow: now}
	source := newWorkerWakeSourceFake(true)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		StartRuntimeSignalOutboxWorkerWithWake(
			ctx,
			NewRuntimeSignalOutboxWorker(store, &runtimeSignalBusFake{}),
			RuntimeSignalOutboxWorkerConfig{Interval: 10 * time.Millisecond},
			source,
		)
		close(done)
	}()
	require.Eventually(t, func() bool {
		claims, _ := store.counts()
		return claims >= 2
	}, time.Second, time.Millisecond, "database due timer did not run")

	source.connected.Store(false)
	before, _ := store.counts()
	require.Eventually(t, func() bool {
		claims, _ := store.counts()
		return claims > before
	}, time.Second, time.Millisecond, "degraded source did not restore legacy polling")
	cancel()
	<-done
}

func TestRunEffectEventWorkerHasNoIdleDatabaseSlopeAndWakesOnEvent(t *testing.T) {
	store := &effectWorkerFakeStore{
		nextDue: db.NextRunEffectDueRow{DatabaseNow: time.Now()},
	}
	worker := NewRunEffectWorker(store, nil, nil)
	svc := &Service{effectWorker: worker}
	source := newWorkerWakeSourceFake(true)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		StartRunEffectWorkerWithWake(
			ctx, svc, RunEffectWorkerConfig{Interval: 10 * time.Millisecond}, source,
		)
		close(done)
	}()

	require.Eventually(t, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return len(store.claimCalls) == 1 && store.nextDueCalls == 1
	}, time.Second, time.Millisecond)
	time.Sleep(40 * time.Millisecond)
	store.mu.Lock()
	require.Len(t, store.claimCalls, 1, "health checks must not claim while idle")
	require.Equal(t, 1, store.nextDueCalls, "health checks must not read next-due while idle")
	store.mu.Unlock()

	source.publish(runEffectWorkerWakeTopic)
	require.Eventually(t, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return len(store.claimCalls) == 2 && store.nextDueCalls == 2
	}, time.Second, time.Millisecond)
	cancel()
	<-done
}

func TestDatabaseDueScheduleUsesDatabaseClockAndClampsOverdueWork(t *testing.T) {
	now := time.Now()
	future := now.Add(3 * time.Second)
	require.Equal(t, eventwake.SchedulerResult{
		NextDelay: 3 * time.Second, HasNext: true,
	}, databaseDueSchedule(&future, now))
	past := now.Add(-time.Second)
	require.Equal(t, eventwake.SchedulerResult{
		NextDelay: 0, HasNext: true,
	}, databaseDueSchedule(&past, now))
	require.Equal(t, eventwake.SchedulerResult{}, databaseDueSchedule(nil, now))
}
