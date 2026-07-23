package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type runtimeCredentialFakeClock struct {
	mu      sync.Mutex
	now     time.Duration
	tickers []*runtimeCredentialFakeTicker
	timers  []*runtimeCredentialFakeTimer
}

type runtimeCredentialFakeTicker struct {
	clock   *runtimeCredentialFakeClock
	every   time.Duration
	next    time.Duration
	wake    chan time.Time
	stopped bool
}

func (ticker *runtimeCredentialFakeTicker) C() <-chan time.Time { return ticker.wake }
func (ticker *runtimeCredentialFakeTicker) Stop() {
	ticker.clock.mu.Lock()
	ticker.stopped = true
	ticker.clock.mu.Unlock()
}

type runtimeCredentialFakeTimer struct {
	clock   *runtimeCredentialFakeClock
	due     time.Duration
	wake    chan time.Time
	stopped bool
	fired   bool
}

func (timer *runtimeCredentialFakeTimer) C() <-chan time.Time { return timer.wake }
func (timer *runtimeCredentialFakeTimer) Stop() bool {
	timer.clock.mu.Lock()
	defer timer.clock.mu.Unlock()
	active := !timer.stopped && !timer.fired
	timer.stopped = true
	return active
}

func (clock *runtimeCredentialFakeClock) NewTicker(
	interval time.Duration,
) runtimeCredentialTicker {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	ticker := &runtimeCredentialFakeTicker{
		clock: clock,
		every: interval,
		next:  clock.now + interval,
		wake:  make(chan time.Time, 16),
	}
	clock.tickers = append(clock.tickers, ticker)
	return ticker
}

func (clock *runtimeCredentialFakeClock) NewTimer(
	interval time.Duration,
) runtimeCredentialTimer {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	timer := &runtimeCredentialFakeTimer{
		clock: clock,
		due:   clock.now + interval,
		wake:  make(chan time.Time, 1),
	}
	clock.timers = append(clock.timers, timer)
	return timer
}

func (clock *runtimeCredentialFakeClock) Advance(delta time.Duration) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	target := clock.now + delta
	for _, ticker := range clock.tickers {
		for !ticker.stopped && ticker.next <= target {
			ticker.wake <- time.Unix(0, int64(ticker.next))
			ticker.next += ticker.every
		}
	}
	for _, timer := range clock.timers {
		if !timer.stopped && !timer.fired && timer.due <= target {
			timer.fired = true
			timer.wake <- time.Unix(0, int64(timer.due))
		}
	}
	clock.now = target
}

func (clock *runtimeCredentialFakeClock) TimerCount() int {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return len(clock.timers)
}

type runtimeCredentialProjectionFake struct {
	mu       sync.Mutex
	states   map[RuntimeConnectionIdentity]RuntimeCredentialProjectionState
	checkErr error
	checked  [][]RuntimeConnectionRegistration
	marked   []RuntimeConnectionRegistration
}

func (f *runtimeCredentialProjectionFake) Check(
	_ context.Context,
	registrations []RuntimeConnectionRegistration,
) ([]RuntimeCredentialProjectionResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.checked = append(f.checked, append([]RuntimeConnectionRegistration(nil), registrations...))
	if f.checkErr != nil {
		return nil, f.checkErr
	}
	results := make([]RuntimeCredentialProjectionResult, len(registrations))
	for index, registration := range registrations {
		state := f.states[registration.Identity]
		if state == "" {
			state = RuntimeCredentialProjectionMissing
		}
		results[index] = RuntimeCredentialProjectionResult{
			Registration: registration,
			State:        state,
		}
	}
	return results, nil
}

func (f *runtimeCredentialProjectionFake) MarkActive(
	_ context.Context,
	registrations []RuntimeConnectionRegistration,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.marked = append(f.marked, registrations...)
	return nil
}

func (f *runtimeCredentialProjectionFake) SetState(
	identity RuntimeConnectionIdentity,
	state RuntimeCredentialProjectionState,
) {
	f.mu.Lock()
	if f.states == nil {
		f.states = make(map[RuntimeConnectionIdentity]RuntimeCredentialProjectionState)
	}
	f.states[identity] = state
	f.mu.Unlock()
}

func (f *runtimeCredentialProjectionFake) CheckCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.checked)
}

type runtimeCredentialValidatorFake struct {
	mu      sync.Mutex
	invalid map[RuntimeConnectionIdentity]bool
	calls   [][]RuntimeConnectionRegistration
	err     error
}

func (f *runtimeCredentialValidatorFake) Validate(
	_ context.Context,
	registrations []RuntimeConnectionRegistration,
) ([]RuntimeCredentialValidationResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, append([]RuntimeConnectionRegistration(nil), registrations...))
	if f.err != nil {
		return nil, f.err
	}
	results := make([]RuntimeCredentialValidationResult, len(registrations))
	for index, registration := range registrations {
		results[index] = RuntimeCredentialValidationResult{
			Registration: registration,
			Valid:        !f.invalid[registration.Identity],
		}
	}
	return results, nil
}

func (f *runtimeCredentialValidatorFake) CallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func testRuntimeConnectionRegistration() RuntimeConnectionRegistration {
	return RuntimeConnectionRegistration{
		Identity: RuntimeConnectionIdentity{
			RuntimeSessionID: uuid.New(),
			SessionEpoch:     1,
			AttachmentID:     uuid.New(),
		},
		CredentialID: uuid.New(),
	}
}

func registerRuntimeCredentialTestConnection(
	t *testing.T,
	hub *RuntimeWakeHub,
	registration RuntimeConnectionRegistration,
) <-chan struct{} {
	t.Helper()
	wake := hub.RegisterConnection(registration.Identity, registration.CredentialID)
	require.NotNil(t, wake)
	return wake
}

func TestRuntimeCredentialReconcilerUsesHealthyProjectionWithoutDatabase(t *testing.T) {
	hub := NewRuntimeWakeHub()
	registration := testRuntimeConnectionRegistration()
	wake := registerRuntimeCredentialTestConnection(t, hub, registration)
	projection := &runtimeCredentialProjectionFake{states: map[RuntimeConnectionIdentity]RuntimeCredentialProjectionState{
		registration.Identity: RuntimeCredentialProjectionActive,
	}}
	validator := &runtimeCredentialValidatorFake{}
	reconciler := NewRuntimeCredentialReconciler(
		hub,
		projection,
		validator,
		RuntimeCredentialReconcilerConfig{},
	)

	require.NoError(t, reconciler.reconcile(context.Background(), false, "test"))
	require.Len(t, projection.checked, 1)
	require.Empty(t, validator.calls)
	select {
	case <-wake:
		t.Fatal("healthy projection closed a valid connection")
	default:
	}
}

func TestRuntimeCredentialReconcilerRevalidatesCacheMissAndRepopulates(t *testing.T) {
	hub := NewRuntimeWakeHub()
	registration := testRuntimeConnectionRegistration()
	wake := registerRuntimeCredentialTestConnection(t, hub, registration)
	projection := &runtimeCredentialProjectionFake{}
	validator := &runtimeCredentialValidatorFake{}
	reconciler := NewRuntimeCredentialReconciler(
		hub,
		projection,
		validator,
		RuntimeCredentialReconcilerConfig{},
	)

	require.NoError(t, reconciler.reconcile(context.Background(), false, "test"))
	require.Equal(t, [][]RuntimeConnectionRegistration{{registration}}, validator.calls)
	require.Equal(t, []RuntimeConnectionRegistration{registration}, projection.marked)
	select {
	case <-wake:
		t.Fatal("valid database fallback closed the connection")
	default:
	}
}

func TestRuntimeCredentialReconcilerBatchesWithoutNPlusOne(t *testing.T) {
	hub := NewRuntimeWakeHub()
	for index := 0; index < 2501; index++ {
		registration := testRuntimeConnectionRegistration()
		registerRuntimeCredentialTestConnection(t, hub, registration)
	}
	projection := &runtimeCredentialProjectionFake{}
	validator := &runtimeCredentialValidatorFake{}
	reconciler := NewRuntimeCredentialReconciler(
		hub,
		projection,
		validator,
		RuntimeCredentialReconcilerConfig{BatchSize: 1000},
	)

	require.NoError(t, reconciler.reconcile(context.Background(), false, "test"))
	require.Len(t, projection.checked, 3)
	require.Len(t, validator.calls, 3)
	require.Len(t, projection.checked[0], 1000)
	require.Len(t, projection.checked[1], 1000)
	require.Len(t, projection.checked[2], 501)
}

func TestRuntimeCredentialReconcilerNoFailOpenMatrix(t *testing.T) {
	tests := []struct {
		name          string
		state         RuntimeCredentialProjectionState
		projectionErr error
		forceDatabase bool
	}{
		{name: "key missing", state: RuntimeCredentialProjectionMissing},
		{name: "key stale or malformed", state: RuntimeCredentialProjectionMalformed},
		{name: "Redis unavailable", projectionErr: errors.New("Redis unavailable")},
		{name: "flush", state: RuntimeCredentialProjectionMissing},
		{name: "primary failover", projectionErr: errors.New("Redis failover")},
		{name: "projection health lag", state: RuntimeCredentialProjectionActive, forceDatabase: true},
		{name: "revocation write absent", state: RuntimeCredentialProjectionActive, forceDatabase: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			hub := NewRuntimeWakeHub()
			registration := testRuntimeConnectionRegistration()
			wake := registerRuntimeCredentialTestConnection(t, hub, registration)
			projection := &runtimeCredentialProjectionFake{
				states: map[RuntimeConnectionIdentity]RuntimeCredentialProjectionState{
					registration.Identity: test.state,
				},
				checkErr: test.projectionErr,
			}
			validator := &runtimeCredentialValidatorFake{
				invalid: map[RuntimeConnectionIdentity]bool{registration.Identity: true},
			}
			reconciler := NewRuntimeCredentialReconciler(
				hub,
				projection,
				validator,
				RuntimeCredentialReconcilerConfig{},
			)

			require.NoError(t, reconciler.reconcile(
				context.Background(),
				test.forceDatabase,
				"no_fail_open",
			))
			require.Len(t, validator.calls, 1)
			select {
			case <-wake:
			default:
				t.Fatal("revoked connection survived an uncertain Redis state")
			}
		})
	}
}

func TestRuntimeCredentialReconcilerFakeClockClosesIdleWebSocketAtWorstProjectionPhase(
	t *testing.T,
) {
	hub := NewRuntimeWakeHub()
	registration := testRuntimeConnectionRegistration()
	wake := registerRuntimeCredentialTestConnection(t, hub, registration)
	projection := &runtimeCredentialProjectionFake{
		states: map[RuntimeConnectionIdentity]RuntimeCredentialProjectionState{
			registration.Identity: RuntimeCredentialProjectionActive,
		},
	}
	validator := &runtimeCredentialValidatorFake{}
	clock := &runtimeCredentialFakeClock{}
	reconciler := NewRuntimeCredentialReconciler(
		hub,
		projection,
		validator,
		RuntimeCredentialReconcilerConfig{clock: clock},
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- reconciler.Run(ctx) }()
	require.Eventually(t, func() bool {
		return projection.CheckCount() == 1
	}, time.Second, time.Millisecond)

	// The revocation projection becomes visible immediately after the previous
	// 20-second pass: this is the maximum ticker phase for an idle WebSocket.
	projection.SetState(registration.Identity, RuntimeCredentialProjectionRevoked)
	clock.Advance(runtimeCredentialReconcileInterval - time.Nanosecond)
	select {
	case <-wake:
		t.Fatal("connection closed before the next bounded projection pass")
	default:
	}
	clock.Advance(time.Nanosecond)
	require.Eventually(t, func() bool {
		select {
		case <-wake:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
	require.Zero(t, validator.CallCount())

	cancel()
	require.NoError(t, <-done)
}

func TestRuntimeCredentialReconcilerFakeClockClosesIdleWebSocketAfterCompoundFailure(
	t *testing.T,
) {
	hub := NewRuntimeWakeHub()
	registration := testRuntimeConnectionRegistration()
	wake := registerRuntimeCredentialTestConnection(t, hub, registration)
	projection := &runtimeCredentialProjectionFake{
		states: map[RuntimeConnectionIdentity]RuntimeCredentialProjectionState{
			registration.Identity: RuntimeCredentialProjectionActive,
		},
	}
	validator := &runtimeCredentialValidatorFake{
		invalid: map[RuntimeConnectionIdentity]bool{registration.Identity: true},
	}
	clock := &runtimeCredentialFakeClock{}
	reconciler := NewRuntimeCredentialReconciler(
		hub,
		projection,
		validator,
		RuntimeCredentialReconcilerConfig{clock: clock},
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- reconciler.Run(ctx) }()
	require.Eventually(t, func() bool {
		return projection.CheckCount() == 1
	}, time.Second, time.Millisecond)

	// Pub/Sub is suppressed and Redis never receives the revocation write.
	// The event-wake health fence detects uncertainty at its 10-second limit.
	clock.Advance(10 * time.Second)
	hub.RequireCredentialRevalidation()
	require.Eventually(t, func() bool {
		return clock.TimerCount() == 1
	}, time.Second, time.Millisecond)
	clock.Advance(runtimeCredentialProjectionSettle - time.Nanosecond)
	select {
	case <-wake:
		t.Fatal("connection closed before the bounded fallback timer")
	default:
	}
	clock.Advance(time.Nanosecond)
	require.Eventually(t, func() bool {
		select {
		case <-wake:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
	require.Equal(t, 1, validator.CallCount())

	cancel()
	require.NoError(t, <-done)
}

func TestRuntimeCredentialReconcilerLowFrequencyAuditFindsSilentProjectionDivergence(
	t *testing.T,
) {
	hub := NewRuntimeWakeHub()
	registration := testRuntimeConnectionRegistration()
	wake := registerRuntimeCredentialTestConnection(t, hub, registration)
	projection := &runtimeCredentialProjectionFake{
		states: map[RuntimeConnectionIdentity]RuntimeCredentialProjectionState{
			registration.Identity: RuntimeCredentialProjectionActive,
		},
	}
	validator := &runtimeCredentialValidatorFake{
		invalid: map[RuntimeConnectionIdentity]bool{registration.Identity: true},
	}
	clock := &runtimeCredentialFakeClock{}
	reconciler := NewRuntimeCredentialReconciler(
		hub,
		projection,
		validator,
		RuntimeCredentialReconcilerConfig{clock: clock},
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- reconciler.Run(ctx) }()
	require.Eventually(t, func() bool {
		return projection.CheckCount() == 1
	}, time.Second, time.Millisecond)

	// A syntactically valid but silently stale "active" value is not used as
	// a 45-second authority. The low-frequency database audit is independent
	// defense in depth and must not add one query per connection.
	clock.Advance(runtimeCredentialAuditInterval - time.Nanosecond)
	require.Zero(t, validator.CallCount())
	select {
	case <-wake:
		t.Fatal("defense-in-depth audit fired before its documented interval")
	default:
	}
	clock.Advance(time.Nanosecond)
	require.Eventually(t, func() bool {
		return validator.CallCount() == 1
	}, time.Second, time.Millisecond)
	select {
	case <-wake:
	default:
		t.Fatal("database audit did not close the silently divergent connection")
	}

	cancel()
	require.NoError(t, <-done)
}

func TestRuntimeCredentialTimingBudgetsStayBelowContract(t *testing.T) {
	tierTwo := 5*time.Second +
		runtimeCredentialReconcileInterval +
		runtimeCredentialQueryTimeout +
		runtimeWSCleanupTimeout
	tierThree := 10*time.Second +
		runtimeCredentialReconcileInterval +
		runtimeCredentialQueryTimeout +
		runtimeWSCleanupTimeout
	require.Equal(t, 35*time.Second, tierTwo)
	require.Equal(t, 40*time.Second, tierThree)
	require.LessOrEqual(t, tierTwo, RuntimeSessionStaleAfter)
	require.LessOrEqual(t, tierThree, RuntimeSessionStaleAfter)
}
