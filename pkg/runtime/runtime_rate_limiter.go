package runtime

import (
	"sync"
	"time"
)

const runtimeLimiterIdleTTL = 10 * time.Minute
const runtimeLimiterPurgeInterval = time.Minute
const runtimeMalformedAuthRetryAfter = 5 * time.Second
const runtimeMalformedAuthMinInterval = time.Second
const runtimePullHeartbeatMinInterval = 10 * time.Second
const runtimePullConcurrentClaimRetryAfter = 5 * time.Second

type EndpointLimiter interface {
	allowMalformedAuth(key string) time.Duration
	allowHeartbeat(key string) time.Duration
	beginClaim(key string, wait time.Duration) (time.Duration, func())
	markEmptyClaim(key string, wait time.Duration)
}

type runtimeEndpointLimiter struct {
	mu        sync.Mutex
	now       func() time.Time
	lastPurge time.Time
	states    map[string]*runtimeEndpointLimitState
}

type runtimeEndpointLimitState struct {
	lastSeen            time.Time
	malformedAllowedAt  time.Time
	heartbeatAllowedAt  time.Time
	emptyClaimAllowedAt time.Time
	activeLongPollClaim bool
}

func newRuntimeEndpointLimiter() *runtimeEndpointLimiter {
	return &runtimeEndpointLimiter{
		now:    time.Now,
		states: make(map[string]*runtimeEndpointLimitState),
	}
}

func (l *runtimeEndpointLimiter) allowMalformedAuth(key string) time.Duration {
	return l.allowAfter(key, runtimeMalformedAuthMinInterval, func(state *runtimeEndpointLimitState) *time.Time {
		return &state.malformedAllowedAt
	})
}

func (l *runtimeEndpointLimiter) allowHeartbeat(key string) time.Duration {
	return l.allowAfter(key, runtimePullHeartbeatMinInterval, func(state *runtimeEndpointLimitState) *time.Time {
		return &state.heartbeatAllowedAt
	})
}

func (l *runtimeEndpointLimiter) beginClaim(key string, wait time.Duration) (time.Duration, func()) {
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.purgeLocked(now)
	state := l.stateForLocked(key, now)
	if now.Before(state.emptyClaimAllowedAt) {
		return state.emptyClaimAllowedAt.Sub(now), func() {}
	}
	if wait > 0 && state.activeLongPollClaim {
		return runtimePullConcurrentClaimRetryAfter, func() {}
	}
	if wait <= 0 {
		return 0, func() {}
	}
	state.activeLongPollClaim = true
	return 0, func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		if current, ok := l.states[key]; ok {
			current.activeLongPollClaim = false
			current.lastSeen = l.now()
		}
	}
}

func (l *runtimeEndpointLimiter) markEmptyClaim(key string, wait time.Duration) {
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.purgeLocked(now)
	state := l.stateForLocked(key, now)
	state.emptyClaimAllowedAt = now.Add(runtimePullEmptyClaimRetryAfter)
}

func (l *runtimeEndpointLimiter) allowAfter(key string, interval time.Duration, field func(*runtimeEndpointLimitState) *time.Time) time.Duration {
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.purgeLocked(now)
	state := l.stateForLocked(key, now)
	allowedAt := field(state)
	if now.Before(*allowedAt) {
		return allowedAt.Sub(now)
	}
	*allowedAt = now.Add(interval)
	return 0
}

func (l *runtimeEndpointLimiter) stateForLocked(key string, now time.Time) *runtimeEndpointLimitState {
	state := l.states[key]
	if state == nil {
		state = &runtimeEndpointLimitState{}
		l.states[key] = state
	}
	state.lastSeen = now
	return state
}

func (l *runtimeEndpointLimiter) purgeLocked(now time.Time) {
	if !l.lastPurge.IsZero() && now.Sub(l.lastPurge) < runtimeLimiterPurgeInterval {
		return
	}
	l.lastPurge = now
	cutoff := now.Add(-runtimeLimiterIdleTTL)
	for key, state := range l.states {
		if state.lastSeen.Before(cutoff) && !state.activeLongPollClaim {
			delete(l.states, key)
		}
	}
}
