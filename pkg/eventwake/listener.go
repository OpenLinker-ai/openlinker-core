package eventwake

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	EnvelopeVersion             = 1
	maxNotificationPayloadBytes = 1024
	defaultListenerMinBackoff   = 250 * time.Millisecond
	defaultListenerMaxBackoff   = 15 * time.Second
)

var (
	channelNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)
	topicNamePattern   = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,79}$`)
)

type Envelope struct {
	Version    int       `json:"version"`
	Topic      string    `json:"topic"`
	ResourceID string    `json:"resource_id"`
	Generation uint64    `json:"generation"`
	ProducedAt time.Time `json:"produced_at"`
}

type Notification struct {
	Channel string
	Payload string
}

type ListenerConfig struct {
	Channels   []string
	Topics     []string
	MinBackoff time.Duration
	MaxBackoff time.Duration
	Dispatch   func(context.Context, Envelope)
	OnRecovery func(uint64)
}

type ListenerHealth struct {
	Connected  bool
	Generation uint64
	Reason     string
}

type ListenerStats struct {
	Accepted        uint64
	Rejected        uint64
	RejectedReasons map[string]uint64
	ConnectFailures uint64
	ListenFailures  uint64
	WaitFailures    uint64
	Reconnects      uint64
}

type Listener struct {
	connector notificationConnector
	config    ListenerConfig
	topics    map[string]struct{}
	channels  map[string]struct{}

	mu     sync.Mutex
	health ListenerHealth
	stats  ListenerStats
}

type notificationConnector interface {
	Connect(context.Context) (notificationConnection, error)
}

type notificationConnection interface {
	Listen(context.Context, []string) error
	Wait(context.Context) (Notification, error)
	Close(context.Context) error
}

func ParseEnvelope(encoded []byte) (Envelope, error) {
	envelope, _, err := parseEnvelope(encoded)
	return envelope, err
}

func parseEnvelope(encoded []byte) (Envelope, string, error) {
	if len(encoded) == 0 || len(encoded) > maxNotificationPayloadBytes {
		return Envelope{}, "payload_size_invalid", errors.New("event wake payload size is invalid")
	}
	var envelope Envelope
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return Envelope{}, "payload_json_invalid", errors.New("event wake payload is invalid JSON")
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Envelope{}, "payload_trailing_value", errors.New("event wake payload must contain one JSON value")
	}
	if envelope.Version != EnvelopeVersion {
		return Envelope{}, "version_unsupported", errors.New("event wake version is unsupported")
	}
	if !topicNamePattern.MatchString(envelope.Topic) {
		return Envelope{}, "topic_invalid", errors.New("event wake topic is invalid")
	}
	if !validResourceID(envelope.ResourceID) {
		return Envelope{}, "resource_id_invalid", errors.New("event wake resource_id is invalid")
	}
	if envelope.ProducedAt.IsZero() {
		return Envelope{}, "produced_at_missing", errors.New("event wake produced_at is required")
	}
	return envelope, "", nil
}

func NewPostgresListener(pool *pgxpool.Pool, config ListenerConfig) (*Listener, error) {
	if pool == nil {
		return nil, errors.New("event wake PostgreSQL pool is required")
	}
	return newListener(postgresNotificationConnector{pool: pool}, config)
}

func newListener(connector notificationConnector, config ListenerConfig) (*Listener, error) {
	if connector == nil {
		return nil, errors.New("event wake connector is required")
	}
	if len(config.Channels) == 0 || len(config.Channels) > 16 {
		return nil, errors.New("event wake channels must contain between 1 and 16 entries")
	}
	if len(config.Topics) == 0 || len(config.Topics) > 64 {
		return nil, errors.New("event wake topics must contain between 1 and 64 entries")
	}
	if config.Dispatch == nil {
		return nil, errors.New("event wake dispatch handler is required")
	}
	channels := make(map[string]struct{}, len(config.Channels))
	for _, channel := range config.Channels {
		if !channelNamePattern.MatchString(channel) {
			return nil, errors.New("event wake channel is invalid")
		}
		if _, duplicate := channels[channel]; duplicate {
			return nil, errors.New("event wake channel is duplicated")
		}
		channels[channel] = struct{}{}
	}
	topics := make(map[string]struct{}, len(config.Topics))
	for _, topic := range config.Topics {
		if !topicNamePattern.MatchString(topic) {
			return nil, errors.New("event wake topic is invalid")
		}
		if _, duplicate := topics[topic]; duplicate {
			return nil, errors.New("event wake topic is duplicated")
		}
		topics[topic] = struct{}{}
	}
	if config.MinBackoff <= 0 {
		config.MinBackoff = defaultListenerMinBackoff
	}
	if config.MaxBackoff <= 0 {
		config.MaxBackoff = defaultListenerMaxBackoff
	}
	if config.MaxBackoff < config.MinBackoff {
		return nil, errors.New("event wake maximum backoff must not be less than minimum")
	}
	config.Channels = append([]string(nil), config.Channels...)
	config.Topics = append([]string(nil), config.Topics...)
	return &Listener{
		connector: connector,
		config:    config,
		topics:    topics,
		channels:  channels,
		health:    ListenerHealth{Reason: "starting"},
	}, nil
}

func (l *Listener) Run(ctx context.Context) error {
	if l == nil {
		return errors.New("event wake listener is not configured")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	backoff := l.config.MinBackoff
	connectedOnce := false
	for {
		if ctx.Err() != nil {
			l.setDisconnected("stopped")
			return nil
		}
		connection, err := l.connector.Connect(ctx)
		if err != nil || connection == nil {
			l.recordConnectFailure()
			l.setDisconnected("connect_failed")
			if !waitForListenerBackoff(ctx, backoff) {
				l.setDisconnected("stopped")
				return nil
			}
			backoff = nextListenerBackoff(backoff, l.config.MaxBackoff)
			continue
		}
		if err = connection.Listen(ctx, l.config.Channels); err != nil {
			l.recordListenFailure()
			l.setDisconnected("listen_failed")
			_ = closeNotificationConnection(connection)
			if !waitForListenerBackoff(ctx, backoff) {
				l.setDisconnected("stopped")
				return nil
			}
			backoff = nextListenerBackoff(backoff, l.config.MaxBackoff)
			continue
		}
		generation := l.setConnected(connectedOnce)
		connectedOnce = true
		backoff = l.config.MinBackoff
		if l.config.OnRecovery != nil {
			l.config.OnRecovery(generation)
		}
		for {
			notification, waitErr := connection.Wait(ctx)
			if waitErr != nil {
				_ = closeNotificationConnection(connection)
				if ctx.Err() != nil {
					l.setDisconnected("stopped")
					return nil
				}
				l.recordWaitFailure()
				l.setDisconnected("wait_failed")
				break
			}
			l.handleNotification(ctx, notification)
		}
		if !waitForListenerBackoff(ctx, backoff) {
			l.setDisconnected("stopped")
			return nil
		}
		backoff = nextListenerBackoff(backoff, l.config.MaxBackoff)
	}
}

func (l *Listener) Health() ListenerHealth {
	if l == nil {
		return ListenerHealth{Reason: "not_configured"}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.health
}

func (l *Listener) Stats() ListenerStats {
	if l == nil {
		return ListenerStats{}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	stats := l.stats
	stats.RejectedReasons = make(map[string]uint64, len(l.stats.RejectedReasons))
	for reason, count := range l.stats.RejectedReasons {
		stats.RejectedReasons[reason] = count
	}
	return stats
}

func (l *Listener) handleNotification(ctx context.Context, notification Notification) {
	if _, allowed := l.channels[notification.Channel]; !allowed {
		l.recordRejected("channel_unknown")
		return
	}
	envelope, rejectReason, err := parseEnvelope([]byte(notification.Payload))
	if err != nil {
		l.recordRejected(rejectReason)
		return
	}
	if _, allowed := l.topics[envelope.Topic]; !allowed {
		l.recordRejected("topic_unknown")
		return
	}
	// Dispatch must be a bounded, non-blocking projection such as Hub.Publish.
	// Business reads and writes happen in the awakened consumer, never here.
	l.config.Dispatch(ctx, envelope)
	l.mu.Lock()
	l.stats.Accepted++
	l.mu.Unlock()
}

func (l *Listener) setConnected(reconnect bool) uint64 {
	l.mu.Lock()
	l.health.Connected = true
	l.health.Generation++
	l.health.Reason = "connected"
	if reconnect {
		l.stats.Reconnects++
	}
	generation := l.health.Generation
	l.mu.Unlock()
	return generation
}

func (l *Listener) setDisconnected(reason string) {
	l.mu.Lock()
	l.health.Connected = false
	l.health.Reason = reason
	l.mu.Unlock()
}

func (l *Listener) recordConnectFailure() {
	l.mu.Lock()
	l.stats.ConnectFailures++
	l.mu.Unlock()
}

func (l *Listener) recordListenFailure() {
	l.mu.Lock()
	l.stats.ListenFailures++
	l.mu.Unlock()
}

func (l *Listener) recordWaitFailure() {
	l.mu.Lock()
	l.stats.WaitFailures++
	l.mu.Unlock()
}

func (l *Listener) recordRejected(reason string) {
	l.mu.Lock()
	l.stats.Rejected++
	if l.stats.RejectedReasons == nil {
		l.stats.RejectedReasons = make(map[string]uint64)
	}
	l.stats.RejectedReasons[reason]++
	l.mu.Unlock()
}

func validResourceID(value string) bool {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" || trimmed != value || len(value) > 200 || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}

func waitForListenerBackoff(ctx context.Context, backoff time.Duration) bool {
	timer := time.NewTimer(backoff)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nextListenerBackoff(current, maximum time.Duration) time.Duration {
	if current >= maximum {
		return maximum
	}
	if current > maximum/2 {
		return maximum
	}
	return current * 2
}

func closeNotificationConnection(connection notificationConnection) error {
	if connection == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return connection.Close(ctx)
}

type postgresNotificationConnector struct {
	pool *pgxpool.Pool
}

func (c postgresNotificationConnector) Connect(ctx context.Context) (notificationConnection, error) {
	pooled, err := c.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire PostgreSQL LISTEN connection: %w", err)
	}
	return &postgresNotificationConnection{connection: pooled.Hijack()}, nil
}

type postgresNotificationConnection struct {
	connection *pgx.Conn
}

func (c *postgresNotificationConnection) Listen(ctx context.Context, channels []string) error {
	if c == nil || c.connection == nil {
		return errors.New("PostgreSQL LISTEN connection is unavailable")
	}
	for _, channel := range channels {
		if !channelNamePattern.MatchString(channel) {
			return errors.New("PostgreSQL LISTEN channel is invalid")
		}
		if _, err := c.connection.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize()); err != nil {
			return fmt.Errorf("LISTEN channel: %w", err)
		}
	}
	return nil
}

func (c *postgresNotificationConnection) Wait(ctx context.Context) (Notification, error) {
	if c == nil || c.connection == nil {
		return Notification{}, errors.New("PostgreSQL LISTEN connection is unavailable")
	}
	notification, err := c.connection.WaitForNotification(ctx)
	if err != nil {
		return Notification{}, err
	}
	return Notification{Channel: notification.Channel, Payload: notification.Payload}, nil
}

func (c *postgresNotificationConnection) Close(ctx context.Context) error {
	if c == nil || c.connection == nil {
		return nil
	}
	err := c.connection.Close(ctx)
	c.connection = nil
	return err
}
