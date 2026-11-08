package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

type fakeSignalRetentionStore struct {
	mu      sync.Mutex
	results []int64
	err     error
	calls   []db.DeletePublishedRuntimeSignalsParams
	called  chan struct{}
}

func (f *fakeSignalRetentionStore) DeletePublishedRuntimeSignals(
	_ context.Context,
	params db.DeletePublishedRuntimeSignalsParams,
) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, params)
	if f.called != nil {
		select {
		case f.called <- struct{}{}:
		default:
		}
	}
	if f.err != nil {
		return 0, f.err
	}
	if len(f.results) == 0 {
		return 0, nil
	}
	next := f.results[0]
	f.results = f.results[1:]
	return next, nil
}

func (f *fakeSignalRetentionStore) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func TestNormalizeRuntimeSignalOutboxRetentionConfigIsConservative(t *testing.T) {
	cfg := normalizeRuntimeSignalOutboxRetentionConfig(RuntimeSignalOutboxRetentionConfig{})
	require.Equal(t, 7*24*time.Hour, cfg.Retention)
	require.Equal(t, defaultRuntimeSignalRetentionInterval, cfg.Interval)
	require.Equal(t, defaultRuntimeSignalRetentionBatchSize, cfg.BatchSize)
	require.Equal(t, defaultRuntimeSignalRetentionBatches, cfg.MaxBatches)
	require.Equal(t, defaultRuntimeSignalRetentionCatchUp, cfg.CatchUpDelay)

	// A window shorter than the minimum is never honored: it would discard
	// the evidence of an incident that is still being investigated.
	for _, retention := range []time.Duration{-time.Hour, time.Second, 59 * time.Minute} {
		cfg = normalizeRuntimeSignalOutboxRetentionConfig(RuntimeSignalOutboxRetentionConfig{Retention: retention})
		require.Equal(t, DefaultRuntimeSignalOutboxRetention, cfg.Retention, retention)
	}
	cfg = normalizeRuntimeSignalOutboxRetentionConfig(RuntimeSignalOutboxRetentionConfig{
		Retention: 48 * time.Hour, BatchSize: maxRuntimeSignalRetentionBatchSize + 1, MaxBatches: 1000,
		Interval: time.Second, CatchUpDelay: time.Minute,
	})
	require.Equal(t, 48*time.Hour, cfg.Retention)
	require.Equal(t, defaultRuntimeSignalRetentionBatchSize, cfg.BatchSize)
	require.Equal(t, defaultRuntimeSignalRetentionBatches, cfg.MaxBatches)
	require.Equal(t, time.Second, cfg.CatchUpDelay, "catch-up never waits longer than the interval")
}

func TestPruneRuntimeSignalOutboxStopsOnPartialBatchOrLimit(t *testing.T) {
	store := &fakeSignalRetentionStore{results: []int64{3, 3, 1}}
	result, err := PruneRuntimeSignalOutbox(context.Background(), store, RuntimeSignalOutboxRetentionConfig{
		Retention: 48 * time.Hour, BatchSize: 3, MaxBatches: 10,
	})
	require.NoError(t, err)
	require.Equal(t, RuntimeSignalOutboxRetentionResult{Batches: 3, Deleted: 7}, result)
	for _, call := range store.calls {
		require.Equal(t, db.DeletePublishedRuntimeSignalsParams{
			RetentionMs: (48 * time.Hour).Milliseconds(), Limit: 3,
		}, call)
	}

	store = &fakeSignalRetentionStore{results: []int64{2, 2, 2}}
	result, err = PruneRuntimeSignalOutbox(context.Background(), store, RuntimeSignalOutboxRetentionConfig{
		BatchSize: 2, MaxBatches: 2,
	})
	require.NoError(t, err)
	require.Equal(t, RuntimeSignalOutboxRetentionResult{Batches: 2, Deleted: 4, Backlog: true}, result)
	require.Equal(t, DefaultRuntimeSignalOutboxRetention.Milliseconds(), store.calls[0].RetentionMs)

	store = &fakeSignalRetentionStore{err: errors.New("database unavailable")}
	result, err = PruneRuntimeSignalOutbox(context.Background(), store, RuntimeSignalOutboxRetentionConfig{})
	require.ErrorContains(t, err, "database unavailable")
	require.Equal(t, RuntimeSignalOutboxRetentionResult{Batches: 1}, result)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	store = &fakeSignalRetentionStore{}
	_, err = PruneRuntimeSignalOutbox(ctx, store, RuntimeSignalOutboxRetentionConfig{})
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, store.callCount())

	_, err = PruneRuntimeSignalOutbox(context.Background(), nil, RuntimeSignalOutboxRetentionConfig{})
	require.Error(t, err)
}

func TestStartRuntimeSignalOutboxRetentionWorkerCatchesUpThenWaitsForInterval(t *testing.T) {
	// Two passes stop at MaxBatches with a full batch; the third drains.
	store := &fakeSignalRetentionStore{results: []int64{5, 5, 1}, called: make(chan struct{}, 8)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		StartRuntimeSignalOutboxRetentionWorker(ctx, store, RuntimeSignalOutboxRetentionConfig{
			Interval: time.Hour, CatchUpDelay: 10 * time.Millisecond, BatchSize: 5, MaxBatches: 1,
		})
	}()
	for range 3 {
		select {
		case <-store.called:
		case <-time.After(5 * time.Second):
			t.Fatalf("retention made %d calls, want a startup pass and two catch-up passes", store.callCount())
		}
	}
	// The drained pass schedules the next one an Interval away.
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, 3, store.callCount())
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("retention worker did not stop on cancellation")
	}
}

func TestStartRuntimeSignalOutboxRetentionWorkerKeepsIntervalAfterError(t *testing.T) {
	store := &fakeSignalRetentionStore{err: errors.New("database unavailable"), called: make(chan struct{}, 8)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		StartRuntimeSignalOutboxRetentionWorker(ctx, store, RuntimeSignalOutboxRetentionConfig{
			Interval: time.Hour, CatchUpDelay: time.Millisecond,
		})
	}()
	select {
	case <-store.called:
	case <-time.After(5 * time.Second):
		t.Fatal("retention worker did not run at startup")
	}
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, 1, store.callCount(), "a failed pass must not spin")
	cancel()
	<-done
}
