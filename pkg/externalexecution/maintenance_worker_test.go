package externalexecution

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/eventwake"
)

type externalCancellationWorkerStoreFake struct {
	Store
	mu    sync.Mutex
	calls int
}

func (s *externalCancellationWorkerStoreFake) ListPendingCancellations(
	context.Context,
	int,
) ([]CancellationRecord, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	return []CancellationRecord{}, nil
}

func (s *externalCancellationWorkerStoreFake) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

type externalCancellationWakeSourceFake struct {
	hub       *eventwake.Hub
	connected atomic.Bool
}

func newExternalCancellationWakeSourceFake(connected bool) *externalCancellationWakeSourceFake {
	source := &externalCancellationWakeSourceFake{hub: eventwake.NewHub()}
	source.connected.Store(connected)
	return source
}

func (s *externalCancellationWakeSourceFake) Health() eventwake.ListenerHealth {
	return eventwake.ListenerHealth{Connected: s.connected.Load()}
}

func (s *externalCancellationWakeSourceFake) SubscribeTopic(topic string) (*eventwake.Subscription, error) {
	return s.hub.Subscribe(topic), nil
}

func TestExternalCancellationEventWorkerHasNoIdleDatabaseSlopeAndDegrades(t *testing.T) {
	store := &externalCancellationWorkerStoreFake{}
	svc := NewService(nil, nil, nil, store)
	source := newExternalCancellationWakeSourceFake(true)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		StartCancellationMaintenanceWorkerWithWake(ctx, svc, 10*time.Millisecond, 100, source)
		close(done)
	}()

	require.Eventually(t, func() bool { return store.callCount() == 1 }, time.Second, time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, 1, store.callCount(), "healthy idle worker must not poll PostgreSQL")

	source.hub.Publish(externalCancellationWakeTopic)
	require.Eventually(t, func() bool { return store.callCount() == 2 }, time.Second, time.Millisecond)
	source.connected.Store(false)
	require.Eventually(t, func() bool { return store.callCount() > 2 }, time.Second, time.Millisecond,
		"degraded listener must restore the legacy interval")

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("external cancellation event worker did not stop")
	}
}
