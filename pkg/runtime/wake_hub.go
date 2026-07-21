package runtime

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

const (
	runtimeSignalSubscribeRetryMin = 100 * time.Millisecond
	runtimeSignalSubscribeRetryMax = 5 * time.Second
)

var allowedRuntimeSignalTypes = map[string]struct{}{
	"run.available":           {},
	"run.cancel":              {},
	"node.capacity.available": {},
	"node.drain":              {},
	"node.revoke":             {},
	"credential.revoke":       {},
}

type runtimeWakeChannels struct {
	dispatch  chan struct{}
	control   chan struct{}
	wsWaiters int
}

func newRuntimeWakeChannels() *runtimeWakeChannels {
	return &runtimeWakeChannels{dispatch: make(chan struct{}, 1), control: make(chan struct{})}
}

// RuntimeWakeHub broadcasts typed, edge-triggered hints to all local HTTP Pull
// and WebSocket waiters for an Agent. PostgreSQL remains authoritative; the
// separate channels prevent a cancellation or lifecycle event from probing the
// dispatch queue and prevent a new Run from polling cancellation commands.
type RuntimeWakeHub struct {
	mu       sync.Mutex
	channels map[uuid.UUID]*runtimeWakeChannels
	nodes    map[uuid.UUID]chan struct{}
}

func NewRuntimeWakeHub() *RuntimeWakeHub {
	return &RuntimeWakeHub{
		channels: make(map[uuid.UUID]*runtimeWakeChannels),
		nodes:    make(map[uuid.UUID]chan struct{}),
	}
}

// Wait is the compatibility alias for dispatch waiters.
func (h *RuntimeWakeHub) Wait(agentID uuid.UUID) <-chan struct{} {
	return h.WaitDispatch(agentID)
}

func (h *RuntimeWakeHub) WaitDispatch(agentID uuid.UUID) <-chan struct{} {
	if h == nil || agentID == uuid.Nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	channels := h.channels[agentID]
	if channels == nil {
		channels = newRuntimeWakeChannels()
		h.channels[agentID] = channels
	}
	return channels.dispatch
}

// RegisterWebSocketDispatch registers one persistent WebSocket waiter and
// reports whether it is the first live Session for this Agent. Dispatch hints
// are coalesced tokens, so one waiter claims durable work without waking every
// sibling Session on the same Agent.
func (h *RuntimeWakeHub) RegisterWebSocketDispatch(agentID uuid.UUID) (<-chan struct{}, bool) {
	if h == nil || agentID == uuid.Nil {
		return nil, false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	channels := h.channels[agentID]
	if channels == nil {
		channels = newRuntimeWakeChannels()
		h.channels[agentID] = channels
	}
	channels.wsWaiters++
	return channels.dispatch, channels.wsWaiters == 1
}

func (h *RuntimeWakeHub) UnregisterWebSocketDispatch(agentID uuid.UUID) {
	if h == nil || agentID == uuid.Nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	channels := h.channels[agentID]
	if channels != nil && channels.wsWaiters > 0 {
		channels.wsWaiters--
	}
}

func (h *RuntimeWakeHub) WaitControl(agentID uuid.UUID) <-chan struct{} {
	if h == nil || agentID == uuid.Nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	channels := h.channels[agentID]
	if channels == nil {
		channels = newRuntimeWakeChannels()
		h.channels[agentID] = channels
	}
	return channels.control
}

// Wake preserves the previous broad-wake behavior for compatibility. New
// signal routing must use WakeDispatch or WakeControl explicitly.
func (h *RuntimeWakeHub) Wake(agentID uuid.UUID) {
	h.wake(agentID, true, true)
}

func (h *RuntimeWakeHub) WakeDispatch(agentID uuid.UUID) {
	h.wake(agentID, true, false)
}

// WakeDispatchIfRegistered wakes only an Agent that has registered a local
// waiter. Recovery scans use this form so a durable backlog owned by another
// Core cannot grow this process's in-memory wake map.
func (h *RuntimeWakeHub) WakeDispatchIfRegistered(agentID uuid.UUID) bool {
	if h == nil || agentID == uuid.Nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	channels := h.channels[agentID]
	if channels == nil {
		return false
	}
	select {
	case channels.dispatch <- struct{}{}:
	default:
	}
	return true
}

func (h *RuntimeWakeHub) WakeControl(agentID uuid.UUID) {
	h.wake(agentID, false, true)
}

func (h *RuntimeWakeHub) WaitNodeDispatch(nodeID uuid.UUID) <-chan struct{} {
	if h == nil || nodeID == uuid.Nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	wake := h.nodes[nodeID]
	if wake == nil {
		wake = make(chan struct{})
		h.nodes[nodeID] = wake
	}
	return wake
}

// WakeNodeDispatch announces that durable capacity was released on a Node.
// Connections without pending dispatch demand ignore it without touching the
// database; blocked demand on another Agent can immediately retry its Claim.
func (h *RuntimeWakeHub) WakeNodeDispatch(nodeID uuid.UUID) {
	if h == nil || nodeID == uuid.Nil {
		return
	}
	h.mu.Lock()
	wake := h.nodes[nodeID]
	if wake != nil {
		close(wake)
	}
	h.nodes[nodeID] = make(chan struct{})
	h.mu.Unlock()
}

func (h *RuntimeWakeHub) wake(agentID uuid.UUID, dispatch, control bool) {
	if h == nil || agentID == uuid.Nil {
		return
	}
	h.mu.Lock()
	channels := h.channels[agentID]
	if channels == nil {
		channels = newRuntimeWakeChannels()
		h.channels[agentID] = channels
	}
	if dispatch {
		select {
		case channels.dispatch <- struct{}{}:
		default:
		}
	}
	if control {
		close(channels.control)
		channels.control = make(chan struct{})
	}
	h.mu.Unlock()
}

// WakeAll broadcasts a one-shot recovery hint to every Agent that currently
// has a local waiter. It is called only when the signal subscription reconnects
// so missed Pub/Sub notifications converge without per-connection polling.
func (h *RuntimeWakeHub) WakeAll() {
	if h == nil {
		return
	}
	h.mu.Lock()
	for _, channels := range h.channels {
		select {
		case channels.dispatch <- struct{}{}:
		default:
		}
		close(channels.control)
		channels.control = make(chan struct{})
	}
	for nodeID, wake := range h.nodes {
		close(wake)
		h.nodes[nodeID] = make(chan struct{})
	}
	h.mu.Unlock()
}

// StartRuntimeSignalSubscriber supervises the blocking subscription and
// reconnects with bounded backoff. It deliberately does not expose a
// readiness bit: the existing signal-bus Health check fails HA readiness,
// while PostgreSQL polling and reconciliation continue to converge state.
func StartRuntimeSignalSubscriber(
	ctx context.Context,
	bus RuntimeSignalBus,
	instanceID uuid.UUID,
	hub *RuntimeWakeHub,
	service *Service,
) {
	if bus == nil || instanceID == uuid.Nil || hub == nil {
		return
	}
	delay := runtimeSignalSubscribeRetryMin
	recovering := false
	for ctx.Err() == nil {
		if recovering {
			hub.WakeAll()
		}
		err := bus.Subscribe(ctx, func(_ context.Context, signal RuntimeSignal) error {
			if err := ValidateRuntimeSignal(signal); err != nil {
				return err
			}
			if signal.TargetInstanceID != nil && *signal.TargetInstanceID != instanceID {
				return nil
			}
			if signal.Type == runtimeNodeCapacityAvailableSignal {
				hub.WakeNodeDispatch(*signal.NodeID)
			} else if signal.Type == "run.available" {
				hub.WakeDispatch(signal.AgentID)
			} else {
				hub.WakeControl(signal.AgentID)
			}
			if signal.Type == "run.cancel" && signal.RunID != nil && service != nil && service.coreExecutions != nil {
				service.coreExecutions.cancelRun(*signal.RunID)
			}
			return nil
		})
		if ctx.Err() != nil {
			return
		}
		if err != nil && !errors.Is(err, ErrRuntimeSignalBusClosed) {
			log.Warn().Err(err).Msg("runtime signal subscription interrupted")
		}
		recovering = true
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if delay < runtimeSignalSubscribeRetryMax/2 {
			delay *= 2
		} else {
			delay = runtimeSignalSubscribeRetryMax
		}
	}
}
