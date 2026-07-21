package runtime

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const defaultRuntimeSignalRedisChannel = "openlinker:runtime:v2:signals"

type RedisSignalBusConfig struct {
	Channel    string
	InstanceID uuid.UUID
}

// RedisSignalBus owns its Pub/Sub subscriptions but not the supplied Redis
// client. Construction performs no network I/O: temporary Redis loss must not
// prevent Core from starting its PostgreSQL reconciliation workers. Health is
// checked dynamically by readiness.
type RedisSignalBus struct {
	client     redis.UniversalClient
	channel    string
	instanceID uuid.UUID

	mu            sync.Mutex
	closed        bool
	subscriptions map[*redis.PubSub]struct{}
}

func NewRedisSignalBus(client redis.UniversalClient, cfg RedisSignalBusConfig) (*RedisSignalBus, error) {
	if runtimeRedisClientUnavailable(client) {
		return nil, fmt.Errorf("%w: Redis client is required", ErrRuntimeSignalBusUnavailable)
	}
	channel := strings.TrimSpace(cfg.Channel)
	if channel == "" {
		channel = defaultRuntimeSignalRedisChannel
	}
	if len(channel) > 200 || strings.ContainsAny(channel, "\x00\r\n") {
		return nil, fmt.Errorf("%w: Redis channel is invalid", ErrRuntimeSignalInvalid)
	}
	if cfg.InstanceID == uuid.Nil {
		return nil, fmt.Errorf("%w: Core instance_id is required", ErrRuntimeSignalInvalid)
	}
	return &RedisSignalBus{
		client:        client,
		channel:       channel,
		instanceID:    cfg.InstanceID,
		subscriptions: make(map[*redis.PubSub]struct{}),
	}, nil
}

func (b *RedisSignalBus) Publish(ctx context.Context, signal RuntimeSignal) error {
	if b == nil || runtimeRedisClientUnavailable(b.client) {
		return ErrRuntimeSignalBusUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	encoded, err := MarshalRuntimeSignal(signal)
	if err != nil {
		return err
	}
	if b.isClosed() {
		return ErrRuntimeSignalBusClosed
	}
	if err = b.client.Publish(ctx, b.channel, encoded).Err(); err != nil {
		return fmt.Errorf("%w: publish Redis signal: %w", ErrRuntimeSignalBusUnavailable, err)
	}
	return nil
}

func (b *RedisSignalBus) Subscribe(ctx context.Context, handler RuntimeSignalHandler) error {
	if b == nil || runtimeRedisClientUnavailable(b.client) {
		return ErrRuntimeSignalBusUnavailable
	}
	if handler == nil {
		return fmt.Errorf("%w: handler is required", ErrRuntimeSignalInvalid)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.isClosed() {
		return ErrRuntimeSignalBusClosed
	}

	pubsub := b.client.Subscribe(ctx, b.channel)
	if err := b.trackSubscription(pubsub); err != nil {
		_ = pubsub.Close()
		return err
	}
	defer func() {
		b.untrackSubscription(pubsub)
		_ = pubsub.Close()
	}()

	// Subscribe is lazy in go-redis. Receive proves that the subscription is
	// live before callers can treat this goroutine as ready.
	if _, err := pubsub.Receive(ctx); err != nil {
		if b.isClosed() {
			return ErrRuntimeSignalBusClosed
		}
		return fmt.Errorf("%w: subscribe Redis signal: %w", ErrRuntimeSignalBusUnavailable, err)
	}

	messages := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case message, ok := <-messages:
			if !ok {
				if b.isClosed() {
					return ErrRuntimeSignalBusClosed
				}
				return ErrRuntimeSignalBusUnavailable
			}
			signal, err := ParseRuntimeSignal([]byte(message.Payload))
			if err != nil {
				return fmt.Errorf("reject Redis runtime signal: %w", err)
			}
			if !runtimeSignalTargetsInstance(signal, b.instanceID) {
				continue
			}
			if err = handler(ctx, signal); err != nil {
				return fmt.Errorf("handle Redis runtime signal: %w", err)
			}
		}
	}
}

func (b *RedisSignalBus) Health(ctx context.Context) error {
	if b == nil || runtimeRedisClientUnavailable(b.client) {
		return ErrRuntimeSignalBusUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.isClosed() {
		return ErrRuntimeSignalBusClosed
	}
	if err := b.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("%w: Redis ping: %w", ErrRuntimeSignalBusUnavailable, err)
	}
	return nil
}

func (b *RedisSignalBus) RuntimePresenceStore() (RuntimePresenceStore, error) {
	if b == nil || runtimeRedisClientUnavailable(b.client) {
		return nil, ErrRuntimeSignalBusUnavailable
	}
	return NewRedisRuntimePresenceStore(b.client, "")
}

func (b *RedisSignalBus) RuntimeSessionLeaseStore() (RuntimeSessionLeaseStore, error) {
	if b == nil || runtimeRedisClientUnavailable(b.client) {
		return nil, ErrRuntimeSignalBusUnavailable
	}
	return NewRedisRuntimeSessionLeaseStore(b.client, "", "")
}

func (b *RedisSignalBus) Close() error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	subscriptions := make([]*redis.PubSub, 0, len(b.subscriptions))
	for subscription := range b.subscriptions {
		subscriptions = append(subscriptions, subscription)
	}
	b.mu.Unlock()

	var combined error
	for _, subscription := range subscriptions {
		combined = errors.Join(combined, subscription.Close())
	}
	return combined
}

func (b *RedisSignalBus) isClosed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}

func (b *RedisSignalBus) trackSubscription(subscription *redis.PubSub) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrRuntimeSignalBusClosed
	}
	b.subscriptions[subscription] = struct{}{}
	return nil
}

func (b *RedisSignalBus) untrackSubscription(subscription *redis.PubSub) {
	b.mu.Lock()
	delete(b.subscriptions, subscription)
	b.mu.Unlock()
}

func runtimeRedisClientUnavailable(client redis.UniversalClient) bool {
	if client == nil {
		return true
	}
	value := reflect.ValueOf(client)
	return (value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface) && value.IsNil()
}

var _ RuntimeSignalBus = (*RedisSignalBus)(nil)
var _ RuntimePresenceStoreProvider = (*RedisSignalBus)(nil)
var _ RuntimeSessionLeaseStoreProvider = (*RedisSignalBus)(nil)
