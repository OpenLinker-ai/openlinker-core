package eventwake

import (
	"context"
	"errors"
	"sync"
)

var ErrSubscriptionClosed = errors.New("event wake subscription is closed")

// Hub is a process-local, non-authoritative wake broadcaster. It retains no
// business payload and intentionally drops wakes for keys with no subscribers.
// Consumers must subscribe before querying PostgreSQL.
type Hub struct {
	mu      sync.Mutex
	entries map[string]*hubEntry
}

type hubEntry struct {
	generation  uint64
	wait        chan struct{}
	subscribers int
}

type Subscription struct {
	hub *Hub
	key string

	mu         sync.Mutex
	generation uint64
	closed     bool
}

func NewHub() *Hub {
	return &Hub{entries: make(map[string]*hubEntry)}
}

func (h *Hub) Subscribe(key string) *Subscription {
	if h == nil {
		return &Subscription{closed: true}
	}
	h.mu.Lock()
	entry := h.entries[key]
	if entry == nil {
		entry = &hubEntry{wait: make(chan struct{})}
		h.entries[key] = entry
	}
	entry.subscribers++
	generation := entry.generation
	h.mu.Unlock()
	return &Subscription{hub: h, key: key, generation: generation}
}

// Publish advances the key generation and broadcasts without waiting for any
// consumer. Repeated publishes safely coalesce at the generation boundary.
func (h *Hub) Publish(key string) {
	if h == nil {
		return
	}
	h.mu.Lock()
	entry := h.entries[key]
	if entry != nil {
		entry.generation++
		close(entry.wait)
		entry.wait = make(chan struct{})
	}
	h.mu.Unlock()
}

// PublishAll is used after listener recovery so every local waiter re-reads
// PostgreSQL and closes any gap left by non-durable notifications.
func (h *Hub) PublishAll() {
	if h == nil {
		return
	}
	h.mu.Lock()
	for _, entry := range h.entries {
		entry.generation++
		close(entry.wait)
		entry.wait = make(chan struct{})
	}
	h.mu.Unlock()
}

func (s *Subscription) Generation() uint64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.generation
}

func (s *Subscription) Wait(ctx context.Context) (uint64, error) {
	if s == nil {
		return 0, ErrSubscriptionClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	if s.closed || s.hub == nil {
		s.mu.Unlock()
		return 0, ErrSubscriptionClosed
	}
	knownGeneration := s.generation
	hub := s.hub
	key := s.key
	s.mu.Unlock()

	hub.mu.Lock()
	entry := hub.entries[key]
	if entry == nil {
		hub.mu.Unlock()
		return 0, ErrSubscriptionClosed
	}
	if entry.generation != knownGeneration {
		generation := entry.generation
		hub.mu.Unlock()
		if !s.setGeneration(generation) {
			return 0, ErrSubscriptionClosed
		}
		return generation, nil
	}
	wait := entry.wait
	hub.mu.Unlock()

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-wait:
	}

	hub.mu.Lock()
	entry = hub.entries[key]
	if entry == nil {
		hub.mu.Unlock()
		return 0, ErrSubscriptionClosed
	}
	generation := entry.generation
	hub.mu.Unlock()
	if !s.setGeneration(generation) {
		return 0, ErrSubscriptionClosed
	}
	return generation, nil
}

func (s *Subscription) Close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	hub := s.hub
	key := s.key
	s.hub = nil
	s.mu.Unlock()
	if hub == nil {
		return
	}
	hub.mu.Lock()
	entry := hub.entries[key]
	if entry != nil {
		entry.subscribers--
		if entry.subscribers <= 0 {
			delete(hub.entries, key)
			close(entry.wait)
		}
	}
	hub.mu.Unlock()
}

func (s *Subscription) setGeneration(generation uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.generation = generation
		return true
	}
	return false
}

func (h *Hub) entryCount() int {
	if h == nil {
		return 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.entries)
}
