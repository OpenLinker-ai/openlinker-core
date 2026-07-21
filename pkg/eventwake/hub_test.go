package eventwake

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestHubSubscribeBeforeQueryDoesNotMissPublish(t *testing.T) {
	hub := NewHub()
	subscription := hub.Subscribe("run-1")
	defer subscription.Close()

	hub.Publish("run-1")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	generation, err := subscription.Wait(ctx)
	if err != nil {
		t.Fatalf("Wait error = %v", err)
	}
	if generation != 1 {
		t.Fatalf("generation = %d, want 1", generation)
	}
}

func TestHubCoalescesRepeatedWakeWithoutBlockingPublisher(t *testing.T) {
	hub := NewHub()
	subscription := hub.Subscribe("run-1")
	defer subscription.Close()

	for generation := 0; generation < 10_000; generation++ {
		hub.Publish("run-1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	generation, err := subscription.Wait(ctx)
	if err != nil {
		t.Fatalf("Wait error = %v", err)
	}
	if generation != 10_000 {
		t.Fatalf("generation = %d, want 10000", generation)
	}
}

func TestHubPublishWakesEverySubscriber(t *testing.T) {
	hub := NewHub()
	first := hub.Subscribe("run-1")
	second := hub.Subscribe("run-1")
	defer first.Close()
	defer second.Close()

	hub.Publish("run-1")
	for index, subscription := range []*Subscription{first, second} {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		generation, err := subscription.Wait(ctx)
		cancel()
		if err != nil {
			t.Fatalf("subscriber %d Wait error = %v", index, err)
		}
		if generation != 1 {
			t.Fatalf("subscriber %d generation = %d, want 1", index, generation)
		}
	}
}

func TestHubPublishAllWakesIndependentKeys(t *testing.T) {
	hub := NewHub()
	first := hub.Subscribe("run-1")
	second := hub.Subscribe("run-2")
	defer first.Close()
	defer second.Close()

	hub.PublishAll()
	for index, subscription := range []*Subscription{first, second} {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		generation, err := subscription.Wait(ctx)
		cancel()
		if err != nil {
			t.Fatalf("subscriber %d Wait error = %v", index, err)
		}
		if generation != 1 {
			t.Fatalf("subscriber %d generation = %d, want 1", index, generation)
		}
	}
}

func TestHubDeletesEntryAfterLastSubscriberCloses(t *testing.T) {
	hub := NewHub()
	first := hub.Subscribe("run-1")
	second := hub.Subscribe("run-1")
	if got := hub.entryCount(); got != 1 {
		t.Fatalf("entry count = %d, want 1", got)
	}
	first.Close()
	if got := hub.entryCount(); got != 1 {
		t.Fatalf("entry count after first close = %d, want 1", got)
	}
	second.Close()
	if got := hub.entryCount(); got != 0 {
		t.Fatalf("entry count after last close = %d, want 0", got)
	}

	// A wake without a subscriber is intentionally discarded. A later
	// subscriber must query PostgreSQL after subscribing.
	hub.Publish("run-1")
	if got := hub.entryCount(); got != 0 {
		t.Fatalf("publish without subscribers retained %d entries", got)
	}
}

func TestSubscriptionWaitHonorsContextAndClose(t *testing.T) {
	hub := NewHub()
	subscription := hub.Subscribe("run-1")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := subscription.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait error = %v, want context canceled", err)
	}
	subscription.Close()
	if _, err := subscription.Wait(context.Background()); !errors.Is(err, ErrSubscriptionClosed) {
		t.Fatalf("Wait after close error = %v", err)
	}
}
