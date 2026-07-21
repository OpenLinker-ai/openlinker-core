package eventwake

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSchedulerWakesFromEventWithoutWaitingForReconciliation(t *testing.T) {
	hub := NewHub()
	subscription := hub.Subscribe("work")
	defer subscription.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reasons := make(chan string, 3)
	done := make(chan error, 1)
	go func() {
		done <- RunScheduler(ctx, subscription, SchedulerConfig{
			ReconcileInterval: time.Hour,
		}, func(_ context.Context, reason string) (SchedulerResult, error) {
			reasons <- reason
			return SchedulerResult{}, nil
		})
	}()
	require.Equal(t, "startup", <-reasons)
	hub.Publish("work")
	select {
	case reason := <-reasons:
		require.Equal(t, "event", reason)
	case <-time.After(time.Second):
		t.Fatal("scheduler did not wake from the topic event")
	}
	cancel()
	require.NoError(t, <-done)
}

func TestSchedulerUsesEarliestDueAndReconciliationTimers(t *testing.T) {
	tests := []struct {
		name       string
		result     SchedulerResult
		reconcile  time.Duration
		wantReason string
	}{
		{
			name: "earliest due", result: SchedulerResult{HasNext: true, NextDelay: 20 * time.Millisecond},
			reconcile: time.Hour, wantReason: "due",
		},
		{
			name: "reconciliation", reconcile: 20 * time.Millisecond, wantReason: "reconcile",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			hub := NewHub()
			subscription := hub.Subscribe("work")
			defer subscription.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reasons := make(chan string, 2)
			done := make(chan error, 1)
			go func() {
				done <- RunScheduler(ctx, subscription, SchedulerConfig{
					ReconcileInterval: test.reconcile, MinimumDelay: time.Millisecond,
				}, func(_ context.Context, reason string) (SchedulerResult, error) {
					reasons <- reason
					return test.result, nil
				})
			}()
			require.Equal(t, "startup", <-reasons)
			select {
			case reason := <-reasons:
				require.Equal(t, test.wantReason, reason)
			case <-time.After(time.Second):
				t.Fatal("scheduler timer did not fire")
			}
			cancel()
			require.NoError(t, <-done)
		})
	}
}

func TestSchedulerHealthChecksDoNotRunStorageCallbackAndDetectDegradation(t *testing.T) {
	hub := NewHub()
	subscription := hub.Subscribe("work")
	defer subscription.Close()
	var healthy atomic.Bool
	healthy.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reasons := make(chan string, 2)
	done := make(chan error, 1)
	go func() {
		done <- RunScheduler(ctx, subscription, SchedulerConfig{
			ReconcileInterval: time.Hour,
			HealthCheck:       10 * time.Millisecond,
			Healthy:           healthy.Load,
		}, func(_ context.Context, reason string) (SchedulerResult, error) {
			reasons <- reason
			return SchedulerResult{}, nil
		})
	}()
	require.Equal(t, "startup", <-reasons)
	time.Sleep(30 * time.Millisecond)
	select {
	case reason := <-reasons:
		t.Fatalf("health check unexpectedly ran storage callback: %s", reason)
	default:
	}
	healthy.Store(false)
	require.ErrorIs(t, <-done, ErrWakeSourceDegraded)
}
