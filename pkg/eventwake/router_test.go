package eventwake

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRouterDispatchesByTopicAndResourceWithoutCrossWake(t *testing.T) {
	router, err := NewRouter([]string{"run.changed", "work.run_effect.available"})
	if err != nil {
		t.Fatalf("NewRouter error = %v", err)
	}
	run, err := router.Subscribe("run.changed", "run-1")
	if err != nil {
		t.Fatalf("Subscribe run error = %v", err)
	}
	defer run.Close()
	other, err := router.Subscribe("run.changed", "run-2")
	if err != nil {
		t.Fatalf("Subscribe other error = %v", err)
	}
	defer other.Close()
	effect, err := router.Subscribe("work.run_effect.available", "effect-1")
	if err != nil {
		t.Fatalf("Subscribe effect error = %v", err)
	}
	defer effect.Close()

	producedAt := time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC)
	router.now = func() time.Time { return producedAt.Add(25 * time.Millisecond) }
	router.Dispatch(context.Background(), Envelope{
		Version: EnvelopeVersion, Topic: "run.changed", ResourceID: "run-1",
		Generation: 9, ProducedAt: producedAt,
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if generation, err := run.Wait(ctx); err != nil || generation != 1 {
		t.Fatalf("run Wait = %d, %v", generation, err)
	}
	for name, subscription := range map[string]*Subscription{"other run": other, "effect": effect} {
		waitCtx, waitCancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		_, waitErr := subscription.Wait(waitCtx)
		waitCancel()
		if !errors.Is(waitErr, context.DeadlineExceeded) {
			t.Fatalf("%s unexpectedly woke: %v", name, waitErr)
		}
	}
	stats := router.Stats()["run.changed"]
	if stats.Accepted != 1 || stats.LastGeneration != 9 || stats.LastWakeLag != 25*time.Millisecond {
		t.Fatalf("run topic stats = %#v", stats)
	}
}

func TestRouterRecoveryBroadcastsAndStatsAreCopies(t *testing.T) {
	router, err := NewRouter([]string{"run.changed"})
	if err != nil {
		t.Fatalf("NewRouter error = %v", err)
	}
	subscription, err := router.Subscribe("run.changed", "run-1")
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()
	router.Recover(4)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := subscription.Wait(ctx); err != nil {
		t.Fatalf("recovery Wait error = %v", err)
	}
	stats := router.Stats()
	stats["run.changed"] = TopicStats{}
	if router.Stats()["run.changed"].RecoveryWakes != 1 {
		t.Fatalf("router stats alias internal state: %#v", router.Stats())
	}
}

func TestRouterTopicSubscriptionCoalescesAllResourceWakes(t *testing.T) {
	router, err := NewRouter([]string{"work.run_effect.available", "run.changed"})
	require.NoError(t, err)
	worker, err := router.SubscribeTopic("work.run_effect.available")
	require.NoError(t, err)
	defer worker.Close()
	other, err := router.SubscribeTopic("run.changed")
	require.NoError(t, err)
	defer other.Close()

	producedAt := time.Now().UTC()
	router.Dispatch(context.Background(), Envelope{
		Version: EnvelopeVersion, Topic: "work.run_effect.available",
		ResourceID: "effect-1", Generation: 1, ProducedAt: producedAt,
	})
	router.Dispatch(context.Background(), Envelope{
		Version: EnvelopeVersion, Topic: "work.run_effect.available",
		ResourceID: "effect-2", Generation: 2, ProducedAt: producedAt,
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	generation, err := worker.Wait(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), generation)
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer waitCancel()
	_, err = other.Wait(waitCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestRouterRejectsUnknownTopicAndInvalidResource(t *testing.T) {
	router, err := NewRouter([]string{"run.changed"})
	if err != nil {
		t.Fatalf("NewRouter error = %v", err)
	}
	if _, err := router.Subscribe("unknown", "run-1"); !errors.Is(err, ErrUnknownTopic) {
		t.Fatalf("unknown topic error = %v", err)
	}
	if _, err := router.Subscribe("run.changed", " invalid "); err == nil {
		t.Fatal("invalid resource was accepted")
	}
}
