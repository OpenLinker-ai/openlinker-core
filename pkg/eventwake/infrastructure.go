package eventwake

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Infrastructure owns one PostgreSQL LISTEN connection and a bounded local
// Router. It is advisory only and does not participate in readiness or replace
// existing worker scheduling until an explicit later cutover.
type Infrastructure struct {
	listener *Listener
	router   *Router
}

// TopicSource is the deliberately small worker-facing surface. Notifications
// are advisory: Health controls cutover/fallback, and PostgreSQL claims remain
// the only authoritative ownership transition.
type TopicSource interface {
	Health() ListenerHealth
	SubscribeTopic(string) (*Subscription, error)
}

func NewPostgresInfrastructure(
	pool *pgxpool.Pool,
	channels []string,
	topics []string,
) (*Infrastructure, error) {
	router, err := NewRouter(topics)
	if err != nil {
		return nil, err
	}
	listener, err := NewPostgresListener(pool, ListenerConfig{
		Channels:   channels,
		Topics:     topics,
		Dispatch:   router.Dispatch,
		OnRecovery: router.Recover,
	})
	if err != nil {
		return nil, err
	}
	return &Infrastructure{listener: listener, router: router}, nil
}

func (i *Infrastructure) Run(ctx context.Context) error {
	if i == nil || i.listener == nil {
		return errors.New("event wake infrastructure is not configured")
	}
	return i.listener.Run(ctx)
}

func (i *Infrastructure) Health() ListenerHealth {
	if i == nil {
		return ListenerHealth{Reason: "not_configured"}
	}
	return i.listener.Health()
}

func (i *Infrastructure) ListenerStats() ListenerStats {
	if i == nil {
		return ListenerStats{}
	}
	return i.listener.Stats()
}

func (i *Infrastructure) TopicStats() map[string]TopicStats {
	if i == nil {
		return nil
	}
	return i.router.Stats()
}

func (i *Infrastructure) Subscribe(topic, resourceID string) (*Subscription, error) {
	if i == nil {
		return nil, ErrUnknownTopic
	}
	return i.router.Subscribe(topic, resourceID)
}

func (i *Infrastructure) SubscribeTopic(topic string) (*Subscription, error) {
	if i == nil {
		return nil, ErrUnknownTopic
	}
	return i.router.SubscribeTopic(topic)
}

var _ TopicSource = (*Infrastructure)(nil)
