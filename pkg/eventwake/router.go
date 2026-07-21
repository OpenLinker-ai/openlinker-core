package eventwake

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrUnknownTopic = errors.New("event wake topic is not configured")

// Router projects advisory envelopes into independent process-local Hubs. It
// records only bounded counters and timestamps for shadow comparison; it never
// stores business payloads or reads PostgreSQL.
type Router struct {
	mu        sync.Mutex
	hubs      map[string]*Hub
	topicHubs map[string]*Hub
	stats     map[string]TopicStats
	now       func() time.Time
}

type TopicStats struct {
	Accepted       uint64
	RecoveryWakes  uint64
	LastGeneration uint64
	LastWakeAt     time.Time
	LastWakeLag    time.Duration
	MaxWakeLag     time.Duration
}

func NewRouter(topics []string) (*Router, error) {
	if len(topics) == 0 || len(topics) > 64 {
		return nil, errors.New("event wake router topics must contain between 1 and 64 entries")
	}
	router := &Router{
		hubs:      make(map[string]*Hub, len(topics)),
		topicHubs: make(map[string]*Hub, len(topics)),
		stats:     make(map[string]TopicStats, len(topics)),
		now:       time.Now,
	}
	for _, topic := range topics {
		if !topicNamePattern.MatchString(topic) {
			return nil, errors.New("event wake router topic is invalid")
		}
		if _, duplicate := router.hubs[topic]; duplicate {
			return nil, errors.New("event wake router topic is duplicated")
		}
		router.hubs[topic] = NewHub()
		router.topicHubs[topic] = NewHub()
		router.stats[topic] = TopicStats{}
	}
	return router, nil
}

// Dispatch is safe to call from Listener: Hub.Publish is non-blocking and the
// stats update is bounded. The awakened consumer remains responsible for
// querying the authoritative PostgreSQL state.
func (r *Router) Dispatch(_ context.Context, envelope Envelope) {
	if r == nil {
		return
	}
	r.mu.Lock()
	hub := r.hubs[envelope.Topic]
	topicHub := r.topicHubs[envelope.Topic]
	if hub == nil {
		r.mu.Unlock()
		return
	}
	now := r.now()
	lag := now.Sub(envelope.ProducedAt)
	if lag < 0 {
		lag = 0
	}
	stats := r.stats[envelope.Topic]
	stats.Accepted++
	stats.LastGeneration = envelope.Generation
	stats.LastWakeAt = now
	stats.LastWakeLag = lag
	if lag > stats.MaxWakeLag {
		stats.MaxWakeLag = lag
	}
	r.stats[envelope.Topic] = stats
	r.mu.Unlock()
	hub.Publish(envelope.ResourceID)
	topicHub.Publish(envelope.Topic)
}

// Recover broadcasts to every local waiter after a LISTEN (re)connect. Since
// NOTIFY is not durable, each awakened consumer must re-read PostgreSQL.
func (r *Router) Recover(_ uint64) {
	if r == nil {
		return
	}
	r.mu.Lock()
	hubs := make([]*Hub, 0, len(r.hubs)+len(r.topicHubs))
	for topic, hub := range r.hubs {
		stats := r.stats[topic]
		stats.RecoveryWakes++
		r.stats[topic] = stats
		hubs = append(hubs, hub)
		hubs = append(hubs, r.topicHubs[topic])
	}
	r.mu.Unlock()
	for _, hub := range hubs {
		hub.PublishAll()
	}
}

// SubscribeTopic coalesces every resource notification for one work type.
// It is intended for process-level workers; resource waiters must continue to
// use Subscribe so unrelated Runs do not wake one another.
func (r *Router) SubscribeTopic(topic string) (*Subscription, error) {
	if r == nil {
		return nil, ErrUnknownTopic
	}
	r.mu.Lock()
	hub := r.topicHubs[topic]
	r.mu.Unlock()
	if hub == nil {
		return nil, ErrUnknownTopic
	}
	return hub.Subscribe(topic), nil
}

func (r *Router) Subscribe(topic, resourceID string) (*Subscription, error) {
	if r == nil {
		return nil, ErrUnknownTopic
	}
	r.mu.Lock()
	hub := r.hubs[topic]
	r.mu.Unlock()
	if hub == nil {
		return nil, ErrUnknownTopic
	}
	if !validResourceID(resourceID) {
		return nil, errors.New("event wake subscription resource_id is invalid")
	}
	return hub.Subscribe(resourceID), nil
}

func (r *Router) Stats() map[string]TopicStats {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	stats := make(map[string]TopicStats, len(r.stats))
	for topic, value := range r.stats {
		stats[topic] = value
	}
	return stats
}
