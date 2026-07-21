package eventwake

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestParseEnvelopeStrictValidation(t *testing.T) {
	valid := `{"version":1,"topic":"run.changed","resource_id":"run-1","generation":4,"produced_at":"2026-07-20T10:00:00Z"}`
	envelope, err := ParseEnvelope([]byte(valid))
	if err != nil {
		t.Fatalf("ParseEnvelope error = %v", err)
	}
	if envelope.Topic != "run.changed" || envelope.ResourceID != "run-1" || envelope.Generation != 4 {
		t.Fatalf("envelope = %#v", envelope)
	}
	tests := []string{
		`{"version":2,"topic":"run.changed","resource_id":"run-1","generation":4,"produced_at":"2026-07-20T10:00:00Z"}`,
		`{"version":1,"topic":"RUN CHANGED","resource_id":"run-1","generation":4,"produced_at":"2026-07-20T10:00:00Z"}`,
		`{"version":1,"topic":"run.changed","resource_id":"","generation":4,"produced_at":"2026-07-20T10:00:00Z"}`,
		`{"version":1,"topic":"run.changed","resource_id":"run-1","generation":4,"produced_at":"2026-07-20T10:00:00Z","payload":{"secret":"x"}}`,
		valid + ` {}`,
	}
	for _, raw := range tests {
		if _, err := ParseEnvelope([]byte(raw)); err == nil {
			t.Fatalf("invalid envelope accepted: %s", raw)
		}
	}
	if _, err := ParseEnvelope([]byte(strings.Repeat("x", maxNotificationPayloadBytes+1))); err == nil {
		t.Fatal("oversized notification was accepted")
	}
}

func TestListenerReconnectsAndBroadcastsRecovery(t *testing.T) {
	first := &fakeNotificationConnection{
		wait: []fakeNotificationResult{
			{notification: Notification{Channel: "openlinker_run_v1", Payload: `{"version":1,"topic":"run.changed","resource_id":"run-1","generation":1,"produced_at":"2026-07-20T10:00:00Z"}`}},
			{err: errors.New("connection lost")},
		},
	}
	second := &fakeNotificationConnection{block: true}
	connector := &fakeNotificationConnector{
		results: []fakeConnectResult{
			{err: errors.New("not ready")},
			{connection: first},
			{connection: second},
		},
	}
	received := make(chan Envelope, 1)
	recoveries := make(chan uint64, 2)
	listener, err := newListener(connector, ListenerConfig{
		Channels:   []string{"openlinker_run_v1"},
		Topics:     []string{"run.changed"},
		MinBackoff: time.Millisecond,
		MaxBackoff: 2 * time.Millisecond,
		Dispatch: func(_ context.Context, envelope Envelope) {
			received <- envelope
		},
		OnRecovery: func(generation uint64) {
			recoveries <- generation
		},
	})
	if err != nil {
		t.Fatalf("newListener error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- listener.Run(ctx) }()

	select {
	case envelope := <-received:
		if envelope.ResourceID != "run-1" {
			t.Fatalf("received envelope = %#v", envelope)
		}
	case <-time.After(time.Second):
		t.Fatal("listener did not dispatch notification")
	}
	for want := uint64(1); want <= 2; want++ {
		select {
		case generation := <-recoveries:
			if generation != want {
				t.Fatalf("recovery generation = %d, want %d", generation, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("listener did not publish recovery generation %d", want)
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run error = %v", err)
	}
	if !first.closed() || !second.closed() {
		t.Fatalf("connections were not closed: first=%t second=%t", first.closed(), second.closed())
	}
	stats := listener.Stats()
	if stats.ConnectFailures != 1 || stats.Accepted != 1 || stats.Reconnects != 1 {
		t.Fatalf("listener stats = %#v", stats)
	}
}

func TestListenerRejectsInvalidPayloadWithoutDisconnecting(t *testing.T) {
	connection := &fakeNotificationConnection{
		wait: []fakeNotificationResult{
			{notification: Notification{Channel: "openlinker_run_v1", Payload: `{"version":1,"topic":"unknown","resource_id":"run-1","generation":1,"produced_at":"2026-07-20T10:00:00Z"}`}},
			{notification: Notification{Channel: "openlinker_run_v1", Payload: `{"version":1,"topic":"run.changed","resource_id":"run-2","generation":2,"produced_at":"2026-07-20T10:00:00Z"}`}},
		},
		block: true,
	}
	connector := &fakeNotificationConnector{results: []fakeConnectResult{{connection: connection}}}
	received := make(chan Envelope, 1)
	listener, err := newListener(connector, ListenerConfig{
		Channels: []string{"openlinker_run_v1"},
		Topics:   []string{"run.changed"},
		Dispatch: func(_ context.Context, envelope Envelope) {
			received <- envelope
		},
	})
	if err != nil {
		t.Fatalf("newListener error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- listener.Run(ctx) }()
	select {
	case envelope := <-received:
		if envelope.ResourceID != "run-2" {
			t.Fatalf("received envelope = %#v", envelope)
		}
	case <-time.After(time.Second):
		t.Fatal("valid notification after rejected payload was not dispatched")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run error = %v", err)
	}
	stats := listener.Stats()
	if stats.Rejected != 1 || stats.Accepted != 1 {
		t.Fatalf("listener stats = %#v", stats)
	}
	if stats.RejectedReasons["topic_unknown"] != 1 {
		t.Fatalf("listener rejection reasons = %#v", stats.RejectedReasons)
	}
	stats.RejectedReasons["topic_unknown"] = 99
	if listener.Stats().RejectedReasons["topic_unknown"] != 1 {
		t.Fatal("listener rejection stats alias internal state")
	}
}

func TestListenerConfigRejectsUnsafeNames(t *testing.T) {
	connector := &fakeNotificationConnector{}
	for _, config := range []ListenerConfig{
		{Channels: nil, Topics: []string{"run.changed"}},
		{Channels: []string{"unsafe;listen"}, Topics: []string{"run.changed"}},
		{Channels: []string{"openlinker_run_v1"}, Topics: []string{"RUN CHANGED"}},
		{Channels: []string{"openlinker_run_v1"}, Topics: []string{"run.changed"}},
	} {
		if _, err := newListener(connector, config); err == nil {
			t.Fatalf("invalid config accepted: %#v", config)
		}
	}
}

type fakeNotificationConnector struct {
	mu      sync.Mutex
	results []fakeConnectResult
	index   int
}

type fakeConnectResult struct {
	connection notificationConnection
	err        error
}

func (f *fakeNotificationConnector) Connect(context.Context) (notificationConnection, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.index >= len(f.results) {
		return nil, errors.New("no fake connection available")
	}
	result := f.results[f.index]
	f.index++
	return result.connection, result.err
}

type fakeNotificationConnection struct {
	mu         sync.Mutex
	wait       []fakeNotificationResult
	index      int
	block      bool
	isClosed   bool
	listenErr  error
	listenedTo []string
}

type fakeNotificationResult struct {
	notification Notification
	err          error
}

func (f *fakeNotificationConnection) Listen(_ context.Context, channels []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listenedTo = append([]string(nil), channels...)
	return f.listenErr
}

func (f *fakeNotificationConnection) Wait(ctx context.Context) (Notification, error) {
	f.mu.Lock()
	if f.index < len(f.wait) {
		result := f.wait[f.index]
		f.index++
		f.mu.Unlock()
		return result.notification, result.err
	}
	block := f.block
	f.mu.Unlock()
	if block {
		<-ctx.Done()
		return Notification{}, ctx.Err()
	}
	return Notification{}, errors.New("fake connection exhausted")
}

func (f *fakeNotificationConnection) Close(context.Context) error {
	f.mu.Lock()
	f.isClosed = true
	f.mu.Unlock()
	return nil
}

func (f *fakeNotificationConnection) closed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.isClosed
}
