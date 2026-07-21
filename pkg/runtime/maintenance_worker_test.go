package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/eventwake"
)

type runtimeMaintenanceReconcilerFake struct {
	mu       sync.Mutex
	results  []RuntimeReconcileBatchResult
	err      error
	calls    int
	nextDue  *time.Time
	dbNow    time.Time
	dueErr   error
	dueCalls int
}

func (f *runtimeMaintenanceReconcilerFake) ReconcileBatch(_ context.Context, _ int) (RuntimeReconcileBatchResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if len(f.results) == 0 {
		return RuntimeReconcileBatchResult{}, f.err
	}
	result := f.results[0]
	f.results = f.results[1:]
	return result, f.err
}

func (f *runtimeMaintenanceReconcilerFake) nextReconcileDue(
	context.Context,
) (*time.Time, time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dueCalls++
	databaseNow := f.dbNow
	if databaseNow.IsZero() {
		databaseNow = time.Now()
	}
	return f.nextDue, databaseNow, f.dueErr
}

type runtimeMaintenanceCancellationFake struct {
	mu       sync.Mutex
	results  []int
	err      error
	calls    int
	nextDue  *time.Time
	dbNow    time.Time
	dueErr   error
	dueCalls int
}

func (f *runtimeMaintenanceCancellationFake) nextReapDue(
	context.Context,
) (*time.Time, time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dueCalls++
	databaseNow := f.dbNow
	if databaseNow.IsZero() {
		databaseNow = time.Now()
	}
	return f.nextDue, databaseNow, f.dueErr
}

type runtimeMaintenanceSessionFake struct {
	mu      sync.Mutex
	results []int
	err     error
	calls   int
}

func (f *runtimeMaintenanceSessionFake) ReapStaleSessions(_ context.Context, _ int) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if len(f.results) == 0 {
		return 0, f.err
	}
	result := f.results[0]
	f.results = f.results[1:]
	return result, f.err
}

func (f *runtimeMaintenanceCancellationFake) ReapExpiredCancellations(_ context.Context, _ int) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if len(f.results) == 0 {
		return 0, f.err
	}
	result := f.results[0]
	f.results = f.results[1:]
	return result, f.err
}

func TestRuntimeMaintenanceWorkerRunsBoundedCatchUp(t *testing.T) {
	reconciler := &runtimeMaintenanceReconcilerFake{results: []RuntimeReconcileBatchResult{
		{Scanned: 2, Reconciled: 2, Requeued: 1, TimedOut: 1},
		{Scanned: 1, Reconciled: 1, DeadLettered: 1},
	}}
	cancellations := &runtimeMaintenanceCancellationFake{results: []int{2, 1}}
	sessions := &runtimeMaintenanceSessionFake{results: []int{2, 1}}

	result, err := RunRuntimeMaintenanceOnce(context.Background(), reconciler, cancellations, sessions, RuntimeMaintenanceWorkerConfig{
		ReconcileBatchSize: 2, CancellationBatchSize: 2, SessionBatchSize: 2, MaxCatchUpBatches: 3,
	})
	require.NoError(t, err)
	require.Equal(t, RuntimeMaintenanceResult{
		ReconcileBatches: 2, CancellationBatches: 2,
		Reconciled: 3, Requeued: 1, TimedOut: 1, DeadLettered: 1,
		CancellationsReaped: 3, SessionsReaped: 3, SessionBatches: 2,
	}, result)
}

func TestRuntimeMaintenanceWorkerDoesNotHideIndependentFailure(t *testing.T) {
	reconcileErr := errors.New("reconcile failed")
	reconciler := &runtimeMaintenanceReconcilerFake{err: reconcileErr}
	cancellations := &runtimeMaintenanceCancellationFake{results: []int{1}}
	sessions := &runtimeMaintenanceSessionFake{results: []int{1}}

	result, err := RunRuntimeMaintenanceOnce(context.Background(), reconciler, cancellations, sessions, RuntimeMaintenanceWorkerConfig{
		ReconcileBatchSize: 2, CancellationBatchSize: 2,
	})
	require.ErrorIs(t, err, reconcileErr)
	require.Equal(t, 1, result.CancellationsReaped)
	require.Equal(t, 1, cancellations.calls)
}

func TestRuntimeMaintenanceWorkerStopsWithContext(t *testing.T) {
	reconciler := &runtimeMaintenanceReconcilerFake{}
	cancellations := &runtimeMaintenanceCancellationFake{}
	sessions := &runtimeMaintenanceSessionFake{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	observed := make(chan WorkerObservation, 4)
	go func() {
		StartRuntimeMaintenanceWorker(ctx, reconciler, cancellations, sessions, RuntimeMaintenanceWorkerConfig{
			Interval: time.Millisecond,
			Observer: WorkerObserverFunc(func(observation WorkerObservation) {
				observed <- observation
			}),
		})
		close(done)
	}()

	require.Eventually(t, func() bool {
		reconciler.mu.Lock()
		defer reconciler.mu.Unlock()
		return reconciler.calls > 0
	}, time.Second, time.Millisecond)
	select {
	case observation := <-observed:
		require.Equal(t, WorkerObservation{
			Category: "runtime.maintenance.scan", Reason: "startup", BatchSize: defaultRuntimeMaintenanceBatchSize,
		}, observation)
	case <-time.After(time.Second):
		t.Fatal("Runtime maintenance observer was not called")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Runtime maintenance worker did not stop")
	}
}

func TestRuntimeMaintenanceEventWorkerHasNoIdleDatabaseSlopeAndKeepsSessionCadence(t *testing.T) {
	now := time.Now()
	reconciler := &runtimeMaintenanceReconcilerFake{dbNow: now}
	cancellations := &runtimeMaintenanceCancellationFake{dbNow: now}
	sessions := &runtimeMaintenanceSessionFake{}
	source := newWorkerWakeSourceFake(true)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		StartRuntimeMaintenanceWorkerWithWake(
			ctx, reconciler, cancellations, sessions,
			RuntimeMaintenanceWorkerConfig{Interval: 10 * time.Millisecond}, source,
		)
		close(done)
	}()

	require.Eventually(t, func() bool {
		reconciler.mu.Lock()
		defer reconciler.mu.Unlock()
		cancellations.mu.Lock()
		defer cancellations.mu.Unlock()
		return reconciler.calls == 0 && reconciler.dueCalls == 1 &&
			cancellations.calls == 0 && cancellations.dueCalls == 1
	}, time.Second, time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	reconciler.mu.Lock()
	require.Zero(t, reconciler.calls, "idle healthy startup must not run an empty reconcile claim")
	require.Equal(t, 1, reconciler.dueCalls, "idle health checks must not query next deadline")
	reconciler.mu.Unlock()
	cancellations.mu.Lock()
	require.Zero(t, cancellations.calls, "idle healthy startup must not run an empty cancellation claim")
	require.Equal(t, 1, cancellations.dueCalls, "idle health checks must not query next cancellation")
	cancellations.mu.Unlock()
	sessions.mu.Lock()
	require.Greater(t, sessions.calls, 1, "Session expiry cadence must remain unchanged")
	sessions.mu.Unlock()

	source.connected.Store(false)
	require.Eventually(t, func() bool {
		reconciler.mu.Lock()
		defer reconciler.mu.Unlock()
		return reconciler.calls > 1
	}, time.Second, time.Millisecond, "degraded LISTEN must restore bounded polling")
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Runtime maintenance event worker did not stop")
	}
}

func TestRuntimeMaintenanceEventWorkerUsesEarliestDatabaseDueTimer(t *testing.T) {
	now := time.Now()
	deadlineDue := now.Add(200 * time.Millisecond)
	cancellationDue := now.Add(25 * time.Millisecond)
	reconciler := &runtimeMaintenanceReconcilerFake{nextDue: &deadlineDue}
	cancellations := &runtimeMaintenanceCancellationFake{nextDue: &cancellationDue}
	sessions := &runtimeMaintenanceSessionFake{}
	source := newWorkerWakeSourceFake(true)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		StartRuntimeMaintenanceWorkerWithWake(
			ctx, reconciler, cancellations, sessions,
			RuntimeMaintenanceWorkerConfig{Interval: 10 * time.Millisecond}, source,
		)
		close(done)
	}()
	require.Eventually(t, func() bool {
		cancellations.mu.Lock()
		defer cancellations.mu.Unlock()
		return cancellations.calls >= 2
	}, time.Second, time.Millisecond, "earliest cancellation deadline did not wake maintenance")
	cancel()
	<-done
}

func TestEarlierRuntimeMaintenanceSchedule(t *testing.T) {
	require.Equal(t, eventwake.SchedulerResult{HasNext: true, NextDelay: time.Second},
		earlierRuntimeMaintenanceSchedule(
			eventwake.SchedulerResult{HasNext: true, NextDelay: 2 * time.Second},
			eventwake.SchedulerResult{HasNext: true, NextDelay: time.Second},
		))
	require.Equal(t, eventwake.SchedulerResult{HasNext: true, NextDelay: time.Second},
		earlierRuntimeMaintenanceSchedule(
			eventwake.SchedulerResult{},
			eventwake.SchedulerResult{HasNext: true, NextDelay: time.Second},
		))
}
