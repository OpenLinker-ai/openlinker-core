package runtime

import (
	"sync"

	"github.com/google/uuid"
)

type runtimePullNotifier struct {
	mu      sync.Mutex
	waiters map[uuid.UUID]map[chan struct{}]struct{}
}

func newRuntimePullNotifier() *runtimePullNotifier {
	return &runtimePullNotifier{waiters: map[uuid.UUID]map[chan struct{}]struct{}{}}
}

func (n *runtimePullNotifier) subscribe(agentID uuid.UUID) (<-chan struct{}, func()) {
	if n == nil || agentID == uuid.Nil {
		return nil, func() {}
	}
	ch := make(chan struct{}, 1)
	n.mu.Lock()
	if n.waiters[agentID] == nil {
		n.waiters[agentID] = map[chan struct{}]struct{}{}
	}
	n.waiters[agentID][ch] = struct{}{}
	n.mu.Unlock()
	return ch, func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		agentWaiters := n.waiters[agentID]
		if agentWaiters == nil {
			return
		}
		delete(agentWaiters, ch)
		if len(agentWaiters) == 0 {
			delete(n.waiters, agentID)
		}
	}
}

func (n *runtimePullNotifier) notify(agentID uuid.UUID) {
	if n == nil || agentID == uuid.Nil {
		return
	}
	n.mu.Lock()
	waiters := make([]chan struct{}, 0, len(n.waiters[agentID]))
	for ch := range n.waiters[agentID] {
		waiters = append(waiters, ch)
	}
	n.mu.Unlock()

	for _, ch := range waiters {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (s *Service) notifyRuntimePullRun(agentID uuid.UUID) {
	if s == nil || s.pullNotifier == nil {
		return
	}
	s.pullNotifier.notify(agentID)
}
