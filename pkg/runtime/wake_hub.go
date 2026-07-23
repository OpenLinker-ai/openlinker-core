package runtime

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

const (
	runtimeSignalSubscribeRetryMin       = 100 * time.Millisecond
	runtimeSignalSubscribeRetryMax       = 5 * time.Second
	runtimeConnectionRevocationRetention = 15 * time.Minute
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

type runtimeConnectionWake struct {
	wake         chan struct{}
	credentialID uuid.UUID
	waiters      int
	closed       bool
}

type runtimeRevokedConnection struct {
	credentialID uuid.UUID
	expiresAt    time.Time
}

func newRuntimeWakeChannels() *runtimeWakeChannels {
	return &runtimeWakeChannels{dispatch: make(chan struct{}, 1), control: make(chan struct{})}
}

// RuntimeWakeHub broadcasts typed, edge-triggered hints to all local HTTP Pull
// and WebSocket waiters for an Agent. PostgreSQL remains authoritative; the
// separate channels prevent a cancellation or lifecycle event from probing the
// dispatch queue and prevent a new Run from polling cancellation commands.
type RuntimeWakeHub struct {
	mu                     sync.Mutex
	channels               map[uuid.UUID]*runtimeWakeChannels
	nodes                  map[uuid.UUID]chan struct{}
	connections            map[RuntimeConnectionIdentity]*runtimeConnectionWake
	revoked                map[RuntimeConnectionIdentity]runtimeRevokedConnection
	credentialRevalidation chan struct{}
	now                    func() time.Time
}

func NewRuntimeWakeHub() *RuntimeWakeHub {
	return &RuntimeWakeHub{
		channels:               make(map[uuid.UUID]*runtimeWakeChannels),
		nodes:                  make(map[uuid.UUID]chan struct{}),
		connections:            make(map[RuntimeConnectionIdentity]*runtimeConnectionWake),
		revoked:                make(map[RuntimeConnectionIdentity]runtimeRevokedConnection),
		credentialRevalidation: make(chan struct{}, 1),
		now:                    time.Now,
	}
}

func validRuntimeConnectionIdentity(identity RuntimeConnectionIdentity) bool {
	return identity.RuntimeSessionID != uuid.Nil &&
		identity.SessionEpoch > 0 &&
		identity.AttachmentID != uuid.Nil
}

// RegisterConnection returns a close-on-revocation channel for one exact
// attachment generation. A recently delivered signal is retained long enough
// to close a connection that races registration after the database commit.
func (h *RuntimeWakeHub) RegisterConnection(
	identity RuntimeConnectionIdentity,
	credentialID uuid.UUID,
) <-chan struct{} {
	if h == nil || !validRuntimeConnectionIdentity(identity) || credentialID == uuid.Nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.purgeRevokedConnectionsLocked()
	if revoked, ok := h.revoked[identity]; ok &&
		revoked.credentialID == credentialID &&
		revoked.expiresAt.After(h.now()) {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	entry := h.connections[identity]
	if entry == nil {
		entry = &runtimeConnectionWake{wake: make(chan struct{}), credentialID: credentialID}
		h.connections[identity] = entry
	} else if entry.credentialID != credentialID {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	entry.waiters++
	return entry.wake
}

func (h *RuntimeWakeHub) UnregisterConnection(identity RuntimeConnectionIdentity) {
	if h == nil || !validRuntimeConnectionIdentity(identity) {
		return
	}
	h.mu.Lock()
	entry := h.connections[identity]
	if entry != nil {
		entry.waiters--
		if entry.waiters <= 0 {
			delete(h.connections, identity)
		}
	}
	h.mu.Unlock()
}

// RevokeConnections closes only exact registered attachment generations and
// retains a bounded race tombstone for connections that have not registered
// yet. It returns the number of live registrations that were signaled.
func (h *RuntimeWakeHub) RevokeConnections(identities []RuntimeConnectionIdentity) int {
	return h.RevokeCredentialConnections(uuid.Nil, identities)
}

func (h *RuntimeWakeHub) RevokeCredentialConnections(
	credentialID uuid.UUID,
	identities []RuntimeConnectionIdentity,
) int {
	if h == nil || len(identities) == 0 {
		return 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.purgeRevokedConnectionsLocked()
	expiresAt := h.now().Add(runtimeConnectionRevocationRetention)
	signaled := 0
	for _, identity := range identities {
		if !validRuntimeConnectionIdentity(identity) {
			continue
		}
		entry := h.connections[identity]
		effectiveCredentialID := credentialID
		if effectiveCredentialID == uuid.Nil && entry != nil {
			effectiveCredentialID = entry.credentialID
		}
		if effectiveCredentialID == uuid.Nil {
			continue
		}
		h.revoked[identity] = runtimeRevokedConnection{
			credentialID: effectiveCredentialID,
			expiresAt:    expiresAt,
		}
		if entry != nil && entry.credentialID != effectiveCredentialID {
			continue
		}
		if entry == nil || entry.closed {
			continue
		}
		close(entry.wake)
		entry.closed = true
		signaled += entry.waiters
	}
	return signaled
}

func (h *RuntimeWakeHub) ConnectionSnapshot() []RuntimeConnectionRegistration {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	registrations := make([]RuntimeConnectionRegistration, 0, len(h.connections))
	for identity, entry := range h.connections {
		if entry == nil || entry.waiters <= 0 || entry.closed || entry.credentialID == uuid.Nil {
			continue
		}
		registrations = append(registrations, RuntimeConnectionRegistration{
			Identity:     identity,
			CredentialID: entry.credentialID,
		})
	}
	h.mu.Unlock()
	sort.Slice(registrations, func(i, j int) bool {
		left, right := registrations[i].Identity, registrations[j].Identity
		if left.RuntimeSessionID != right.RuntimeSessionID {
			return left.RuntimeSessionID.String() < right.RuntimeSessionID.String()
		}
		if left.SessionEpoch != right.SessionEpoch {
			return left.SessionEpoch < right.SessionEpoch
		}
		return left.AttachmentID.String() < right.AttachmentID.String()
	})
	return registrations
}

func (h *RuntimeWakeHub) CredentialRevalidationWake() <-chan struct{} {
	if h == nil {
		return nil
	}
	return h.credentialRevalidation
}

func (h *RuntimeWakeHub) RequireCredentialRevalidation() {
	if h == nil {
		return
	}
	select {
	case h.credentialRevalidation <- struct{}{}:
	default:
	}
}

func (h *RuntimeWakeHub) purgeRevokedConnectionsLocked() {
	if h == nil {
		return
	}
	now := h.now()
	for identity, revoked := range h.revoked {
		if !revoked.expiresAt.After(now) {
			delete(h.revoked, identity)
		}
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
	h.RequireCredentialRevalidation()
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
			} else if signal.Type == "credential.revoke" && len(signal.Connections) > 0 {
				hub.RevokeCredentialConnections(*signal.CredentialID, signal.Connections)
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
		hub.RequireCredentialRevalidation()
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
