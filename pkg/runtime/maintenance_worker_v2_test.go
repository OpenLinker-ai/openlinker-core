package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type runtimeV2MaintenanceReconcilerFake struct {
	mu      sync.Mutex
	results []RuntimeReconcileBatchResult
	err     error
	calls   int
}

func (f *runtimeV2MaintenanceReconcilerFake) ReconcileBatch(_ context.Context, _ int) (RuntimeReconcileBatchResult, error) {
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

type runtimeV2MaintenanceCancellationFake struct {
	mu      sync.Mutex
	results []int
	err     error
	calls   int
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

func (f *runtimeV2MaintenanceCancellationFake) ReapExpiredCancellations(_ context.Context, _ int) (int, error) {
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

func TestRuntimeV2MaintenanceWorkerRunsBoundedCatchUp(t *testing.T) {
	reconciler := &runtimeV2MaintenanceReconcilerFake{results: []RuntimeReconcileBatchResult{
		{Scanned: 2, Reconciled: 2, Requeued: 1, TimedOut: 1},
		{Scanned: 1, Reconciled: 1, DeadLettered: 1},
	}}
	cancellations := &runtimeV2MaintenanceCancellationFake{results: []int{2, 1}}
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

func TestRuntimeV2MaintenanceWorkerDoesNotHideIndependentFailure(t *testing.T) {
	reconcileErr := errors.New("reconcile failed")
	reconciler := &runtimeV2MaintenanceReconcilerFake{err: reconcileErr}
	cancellations := &runtimeV2MaintenanceCancellationFake{results: []int{1}}
	sessions := &runtimeMaintenanceSessionFake{results: []int{1}}

	result, err := RunRuntimeMaintenanceOnce(context.Background(), reconciler, cancellations, sessions, RuntimeMaintenanceWorkerConfig{
		ReconcileBatchSize: 2, CancellationBatchSize: 2,
	})
	require.ErrorIs(t, err, reconcileErr)
	require.Equal(t, 1, result.CancellationsReaped)
	require.Equal(t, 1, cancellations.calls)
}

func TestRuntimeV2MaintenanceWorkerStopsWithContext(t *testing.T) {
	reconciler := &runtimeV2MaintenanceReconcilerFake{}
	cancellations := &runtimeV2MaintenanceCancellationFake{}
	sessions := &runtimeMaintenanceSessionFake{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		StartRuntimeMaintenanceWorker(ctx, reconciler, cancellations, sessions, RuntimeMaintenanceWorkerConfig{Interval: time.Millisecond})
		close(done)
	}()

	require.Eventually(t, func() bool {
		reconciler.mu.Lock()
		defer reconciler.mu.Unlock()
		return reconciler.calls > 0
	}, time.Second, time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Runtime maintenance worker did not stop")
	}
}
