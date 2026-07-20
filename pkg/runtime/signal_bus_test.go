package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestRuntimeSignalWireShapeIsAClosedSafeAllowlist(t *testing.T) {
	runID, targetID := uuid.New(), uuid.New()
	signal := RuntimeSignal{
		SignalID: uuid.New(), Type: "run.available", AgentID: uuid.New(),
		RunID: &runID, TargetInstanceID: &targetID,
	}
	encoded, err := MarshalRuntimeSignal(signal)
	require.NoError(t, err)

	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(encoded, &fields))
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	require.Equal(t, []string{
		"agent_id", "run_id", "signal_id", "target_instance_id", "type",
	}, keys)
	for _, forbidden := range []string{"payload", "input", "output", "token", "secret", "capability"} {
		require.NotContains(t, string(encoded), forbidden)
	}

	decoded, err := ParseRuntimeSignal(encoded)
	require.NoError(t, err)
	require.Equal(t, signal, decoded)

	for _, forbiddenField := range []string{"payload", "input", "output", "token", "secret"} {
		unsafe := strings.TrimSuffix(string(encoded), "}") + `,"` + forbiddenField + `":"classified"}`
		_, parseErr := ParseRuntimeSignal([]byte(unsafe))
		require.ErrorIs(t, parseErr, ErrRuntimeSignalInvalid)
	}
	_, err = ParseRuntimeSignal(append(encoded, []byte(` {}`)...))
	require.ErrorIs(t, err, ErrRuntimeSignalInvalid)
}

func TestRuntimeNodeCapacitySignalRequiresOnlyAValidNodeProjection(t *testing.T) {
	nodeID := uuid.New()
	signal := RuntimeSignal{
		SignalID: uuid.New(), Type: runtimeNodeCapacityAvailableSignal,
		AgentID: uuid.New(), NodeID: &nodeID,
	}
	encoded, err := MarshalRuntimeSignal(signal)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"node_id":"`+nodeID.String()+`"`)
	require.Equal(t, signal, requireRuntimeSignal(t, encoded))

	signal.NodeID = nil
	require.ErrorIs(t, ValidateRuntimeSignal(signal), ErrRuntimeSignalInvalid)
	otherType := signal
	otherType.Type = "run.available"
	otherType.NodeID = &nodeID
	require.ErrorIs(t, ValidateRuntimeSignal(otherType), ErrRuntimeSignalInvalid)
}

func requireRuntimeSignal(t *testing.T, encoded []byte) RuntimeSignal {
	t.Helper()
	signal, err := ParseRuntimeSignal(encoded)
	require.NoError(t, err)
	return signal
}

func TestLocalSignalBusBroadcastsAndFiltersTargetInstance(t *testing.T) {
	instanceID := uuid.New()
	bus := NewLocalSignalBus(instanceID)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	received := make(chan RuntimeSignal, 2)
	subscriptionDone := make(chan error, 1)
	go func() {
		subscriptionDone <- bus.Subscribe(ctx, func(_ context.Context, signal RuntimeSignal) error {
			received <- signal
			return nil
		})
	}()
	require.Eventually(t, func() bool {
		bus.mu.RLock()
		defer bus.mu.RUnlock()
		return len(bus.subscribers) == 1
	}, time.Second, time.Millisecond)

	agentID := uuid.New()
	otherInstance := uuid.New()
	require.NoError(t, bus.Publish(context.Background(), RuntimeSignal{
		SignalID: uuid.New(), Type: "run.cancel", AgentID: agentID,
		TargetInstanceID: &otherInstance,
	}))
	select {
	case unexpected := <-received:
		t.Fatalf("received signal targeted at another instance: %#v", unexpected)
	case <-time.After(20 * time.Millisecond):
	}

	wanted := RuntimeSignal{
		SignalID: uuid.New(), Type: "run.cancel", AgentID: agentID,
		TargetInstanceID: &instanceID,
	}
	require.NoError(t, bus.Publish(context.Background(), wanted))
	require.Equal(t, wanted, <-received)
	require.NoError(t, bus.Health(context.Background()))
	require.NoError(t, bus.Close())
	require.ErrorIs(t, <-subscriptionDone, ErrRuntimeSignalBusClosed)
	require.ErrorIs(t, bus.Health(context.Background()), ErrRuntimeSignalBusClosed)
	require.ErrorIs(t, bus.Publish(context.Background(), wanted), ErrRuntimeSignalBusClosed)
}

func TestRedisSignalBusConstructsOfflineRecoversAndRoutesStrictSignals(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{
		Addr:         server.Addr(),
		DialTimeout:  100 * time.Millisecond,
		ReadTimeout:  100 * time.Millisecond,
		WriteTimeout: 100 * time.Millisecond,
		MaxRetries:   -1,
		PoolSize:     1,
	})
	t.Cleanup(func() { _ = client.Close() })

	server.Close()
	bus, err := NewRedisSignalBus(client, RedisSignalBusConfig{InstanceID: uuid.New()})
	require.NoError(t, err, "construction must not ping Redis")
	healthCtx, healthCancel := context.WithTimeout(context.Background(), time.Second)
	require.ErrorIs(t, bus.Health(healthCtx), ErrRuntimeSignalBusUnavailable)
	healthCancel()
	require.NoError(t, server.Restart())
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		return bus.Health(ctx) == nil
	}, 2*time.Second, 10*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	received := make(chan RuntimeSignal, 1)
	subscriptionDone := make(chan error, 1)
	go func() {
		subscriptionDone <- bus.Subscribe(ctx, func(_ context.Context, signal RuntimeSignal) error {
			received <- signal
			return nil
		})
	}()
	require.Eventually(t, func() bool {
		return server.PubSubNumSub(defaultRuntimeSignalRedisChannel)[defaultRuntimeSignalRedisChannel] == 1
	}, time.Second, time.Millisecond)

	wanted := RuntimeSignal{SignalID: uuid.New(), Type: "run.available", AgentID: uuid.New()}
	require.NoError(t, bus.Publish(context.Background(), wanted))
	select {
	case got := <-received:
		require.Equal(t, wanted, got)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Redis signal")
	}

	cancel()
	require.ErrorIs(t, <-subscriptionDone, context.Canceled)
	require.NoError(t, bus.Close())
	require.ErrorIs(t, bus.Health(context.Background()), ErrRuntimeSignalBusClosed)
}

func TestRedisSignalBusRejectsTypedNilClientWithoutPanic(t *testing.T) {
	var client *redis.Client
	_, err := NewRedisSignalBus(client, RedisSignalBusConfig{InstanceID: uuid.New()})
	require.ErrorIs(t, err, ErrRuntimeSignalBusUnavailable)
	_, err = NewRedisRuntimePresenceStore(client, "")
	require.ErrorIs(t, err, ErrRuntimeSignalBusUnavailable)
	_, err = NewRedisRuntimeSessionLeaseStore(client, "", "")
	require.ErrorIs(t, err, ErrRuntimeSignalBusUnavailable)
}

func TestRedisSignalBusRequiresCoreIdentityForTargetFiltering(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	_, err := NewRedisSignalBus(client, RedisSignalBusConfig{})
	require.ErrorIs(t, err, ErrRuntimeSignalInvalid)
}

func TestRedisSignalBusDropsSignalsTargetedAtAnotherInstance(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	instanceID := uuid.New()
	bus, err := NewRedisSignalBus(client, RedisSignalBusConfig{
		Channel: "test:runtime:signals", InstanceID: instanceID,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, bus.Close()) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	received := make(chan RuntimeSignal, 1)
	subscriptionDone := make(chan error, 1)
	go func() {
		subscriptionDone <- bus.Subscribe(ctx, func(_ context.Context, signal RuntimeSignal) error {
			received <- signal
			return nil
		})
	}()
	require.Eventually(t, func() bool {
		return server.PubSubNumSub("test:runtime:signals")["test:runtime:signals"] == 1
	}, time.Second, time.Millisecond)

	other := uuid.New()
	require.NoError(t, bus.Publish(context.Background(), RuntimeSignal{
		SignalID: uuid.New(), Type: "run.cancel", AgentID: uuid.New(), TargetInstanceID: &other,
	}))
	time.Sleep(20 * time.Millisecond)
	select {
	case signal := <-received:
		t.Fatalf("received signal for another Core: %#v", signal)
	default:
	}

	wanted := RuntimeSignal{
		SignalID: uuid.New(), Type: "run.cancel", AgentID: uuid.New(), TargetInstanceID: &instanceID,
	}
	require.NoError(t, bus.Publish(context.Background(), wanted))
	require.Equal(t, wanted, <-received)
	cancel()
	require.ErrorIs(t, <-subscriptionDone, context.Canceled)
}

func TestLocalSignalBusReturnsAllSubscriberErrors(t *testing.T) {
	bus := NewLocalSignalBus(uuid.Nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	calls := 0
	wantA, wantB := errors.New("a"), errors.New("b")
	for _, want := range []error{wantA, wantB} {
		want := want
		go func() {
			_ = bus.Subscribe(ctx, func(context.Context, RuntimeSignal) error {
				mu.Lock()
				calls++
				mu.Unlock()
				return want
			})
		}()
	}
	require.Eventually(t, func() bool {
		bus.mu.RLock()
		defer bus.mu.RUnlock()
		return len(bus.subscribers) == 2
	}, time.Second, time.Millisecond)
	err := bus.Publish(context.Background(), RuntimeSignal{
		SignalID: uuid.New(), Type: "run.available", AgentID: uuid.New(),
	})
	require.ErrorIs(t, err, wantA)
	require.ErrorIs(t, err, wantB)
	mu.Lock()
	require.Equal(t, 2, calls)
	mu.Unlock()
	require.NoError(t, bus.Close())
}

func TestRuntimeSignalStructHasNoUnreviewedWireFields(t *testing.T) {
	typeOfSignal := reflect.TypeFor[RuntimeSignal]()
	require.Equal(t, 6, typeOfSignal.NumField())
}
