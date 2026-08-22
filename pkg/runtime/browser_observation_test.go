package runtime

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

func observerIdentity() BrowserObserverIdentity {
	return BrowserObserverIdentity{
		RunID:                uuid.New(),
		AttemptID:            uuid.New(),
		RuntimeLeaseID:       uuid.New(),
		FencingToken:         7,
		NodeID:               uuid.New(),
		AgentID:              uuid.New(),
		WorkerID:             uuid.NewString(),
		SessionEpoch:         3,
		BrowserSessionSHA256: strings.Repeat("a", 64),
		AttachmentSHA256:     strings.Repeat("b", 64),
		RuntimeSessionID:     uuid.New(),
	}
}

func observerCommand(action BrowserObserverAction) BrowserObserverCommandPayload {
	now := time.Now().UTC()
	identity := observerIdentity()
	return BrowserObserverCommandPayload{
		AttemptIdentity:      identity.RuntimeIdentity(),
		SessionEpoch:         identity.SessionEpoch,
		BrowserSessionSHA256: identity.BrowserSessionSHA256,
		AttachmentSHA256:     identity.AttachmentSHA256,
		CommandID:            uuid.New(),
		Action:               action,
		LeaseID:              uuid.New(),
		LeaseExpiresAt:       now.Add(time.Minute),
		DeadlineAt:           now.Add(time.Minute),
		FrameIntervalMS:      observationDefaultFrameIntervalMS,
	}
}

func TestBrowserObserverCommandValidation(t *testing.T) {
	t.Parallel()
	if err := observerCommand(BrowserObserverStart).Validate(); err != nil {
		t.Fatalf("valid start rejected: %v", err)
	}

	// Stop carries no schedule, so it must not inherit the start requirements.
	stop := observerCommand(BrowserObserverStop)
	stop.LeaseExpiresAt = time.Time{}
	stop.DeadlineAt = time.Time{}
	stop.FrameIntervalMS = 0
	if err := stop.Validate(); err != nil {
		t.Fatalf("stop rejected: %v", err)
	}

	for name, mutate := range map[string]func(*BrowserObserverCommandPayload){
		"no deadline":   func(c *BrowserObserverCommandPayload) { c.DeadlineAt = time.Time{} },
		"fast frames":   func(c *BrowserObserverCommandPayload) { c.FrameIntervalMS = BrowserObserverMinFrameIntervalMS - 1 },
		"slow frames":   func(c *BrowserObserverCommandPayload) { c.FrameIntervalMS = BrowserObserverMaxFrameIntervalMS + 1 },
		"no lease":      func(c *BrowserObserverCommandPayload) { c.LeaseID = uuid.Nil },
		"bad action":    func(c *BrowserObserverCommandPayload) { c.Action = "observe" },
		"zero epoch":    func(c *BrowserObserverCommandPayload) { c.SessionEpoch = 0 },
		"no attachment": func(c *BrowserObserverCommandPayload) { c.AttachmentSHA256 = "" },
		"bad digest":    func(c *BrowserObserverCommandPayload) { c.BrowserSessionSHA256 = "zz" },
	} {
		t.Run(name, func(t *testing.T) {
			command := observerCommand(BrowserObserverStart)
			mutate(&command)
			if command.Validate() == nil {
				t.Fatalf("%s was accepted", name)
			}
		})
	}
}

// Runtime extensions reserve attempt_identity for the complete SDK Attempt
// identity. Browser hashes are additional evidence; replacing the reserved
// field with that custom shape makes the SDK reject the push and disconnect.
func TestBrowserObserverCommandKeepsRuntimeAttemptIdentityAtReservedField(t *testing.T) {
	t.Parallel()
	command := observerCommand(BrowserObserverStart)
	payload, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(payload, &object); err != nil {
		t.Fatal(err)
	}
	var decoded AttemptIdentity
	decoder := json.NewDecoder(bytes.NewReader(object["attempt_identity"]))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		t.Fatalf("Runtime attempt identity rejected: %v", err)
	}
	if decoded != command.AttemptIdentity {
		t.Fatalf("Runtime attempt identity changed: %#v", decoded)
	}
}

func TestBrowserObserverEventValidationPerKind(t *testing.T) {
	t.Parallel()
	captured := time.Now().UTC()
	frame := &BrowserObserverFramePayload{
		MIMEType: "image/jpeg",
		Data:     []byte{0xff, 0xd8, 0xff, 0xd9},
		Width:    1280,
		Height:   720,
	}
	identity := observerIdentity()
	base := BrowserObserverEventPayload{
		AttemptIdentity:      identity.RuntimeIdentity(),
		SessionEpoch:         identity.SessionEpoch,
		BrowserSessionSHA256: identity.BrowserSessionSHA256,
		AttachmentSHA256:     identity.AttachmentSHA256,
		CommandID:            uuid.New(),
		LeaseID:              uuid.New(),
		EventSeq:             1,
	}

	started := base
	started.Kind = BrowserObserverStarted
	if err := started.Validate(); err != nil {
		t.Fatalf("started rejected: %v", err)
	}
	// A lifecycle event carrying a frame would slip page content past a consumer
	// that only inspects frame events for it.
	loaded := started
	loaded.Frame = frame
	if loaded.Validate() == nil {
		t.Fatal("a started event carrying a frame was accepted")
	}

	frameEvent := base
	frameEvent.Kind = BrowserObserverFrame
	frameEvent.CapturedAt = &captured
	frameEvent.Frame = frame
	if err := frameEvent.Validate(); err != nil {
		t.Fatalf("frame rejected: %v", err)
	}
	for name, mutate := range map[string]func(*BrowserObserverEventPayload){
		"no capture time": func(e *BrowserObserverEventPayload) { e.CapturedAt = nil },
		"no frame":        func(e *BrowserObserverEventPayload) { e.Frame = nil },
		"wrong mime": func(e *BrowserObserverEventPayload) {
			clone := *frame
			clone.MIMEType = "image/png"
			e.Frame = &clone
		},
		"empty data": func(e *BrowserObserverEventPayload) {
			clone := *frame
			clone.Data = nil
			e.Frame = &clone
		},
	} {
		t.Run(name, func(t *testing.T) {
			event := frameEvent
			mutate(&event)
			if event.Validate() == nil {
				t.Fatalf("%s was accepted", name)
			}
		})
	}

	silent := base
	silent.Kind = BrowserObserverError
	if silent.Validate() == nil {
		t.Fatal("an error event without a code was accepted")
	}
}

// Observation must fail closed when this process cannot reach the Worker, rather
// than opening a record that could never produce a frame.
func TestBrowserObservationFailsClosedWithoutAChannel(t *testing.T) {
	t.Parallel()
	unbound := NewBrowserObservation(nil, nil, uuid.New(), 0)
	_, err := unbound.Start(
		t.Context(),
		uuid.New(),
		uuid.New(),
		false,
		"",
		observerIdentity(),
	)
	if err != ErrObservationChannelUnavailable {
		t.Fatalf("start error = %v, want %v", err, ErrObservationChannelUnavailable)
	}
}

// Cross-user observation is an operator action, so it cannot proceed without a
// recorded reason.
func TestBrowserObservationRequiresAdminReason(t *testing.T) {
	t.Parallel()
	observation := NewBrowserObservation(nil, nil, uuid.New(), 0)
	observation.BindCommandSender(stubObserverSender{})
	if _, err := observation.Start(
		t.Context(), uuid.New(), uuid.New(), true, "", observerIdentity(),
	); err == nil {
		t.Fatal("an admin observation without a reason was accepted")
	}
}

type stubObserverSender struct{}

func (stubObserverSender) SendBrowserObserverCommand(
	uuid.UUID,
	BrowserObserverCommandPayload,
) error {
	return nil
}

// The feature is read from the current Session's declaration, so a Runtime that
// reconnects without it stops being observable immediately.
func TestObservationFeatureDeclaration(t *testing.T) {
	t.Parallel()
	if !observationFeatureDeclared([]string{"other", BrowserObservationFeature}) {
		t.Fatal("a declared feature was not detected")
	}
	for name, features := range map[string][]string{
		"empty":         nil,
		"unrelated":     {"browser_human_control.v1"},
		"near miss":     {"browser_authenticated_observation"},
		"wrong version": {"browser_authenticated_observation.v2"},
	} {
		t.Run(name, func(t *testing.T) {
			if observationFeatureDeclared(features) {
				t.Fatalf("%v was treated as declaring observation", features)
			}
		})
	}
}

// Each failure mode has to reach the caller as its own status: an old Runtime,
// a Worker on another Core instance and a Run someone else owns are different
// problems with different fixes.
func TestObservationErrorsMapToDistinctStatuses(t *testing.T) {
	t.Parallel()
	seen := map[int]string{}
	for _, err := range []error{
		ErrObservationChannelUnavailable,
		ErrObservationAlreadyActive,
		ErrObservationUnsupported,
		ErrObservationForbidden,
	} {
		mapped := browserObservationHTTPError(err)
		status := httpStatusOf(t, mapped)
		if existing, clash := seen[status]; clash {
			t.Fatalf("%v and %s share status %d", err, existing, status)
		}
		seen[status] = err.Error()
	}
	if len(seen) != 4 {
		t.Fatalf("expected four distinct statuses, got %d", len(seen))
	}
}

func httpStatusOf(t *testing.T, err error) int {
	t.Helper()
	var coreErr *httpx.HTTPError
	if errors.As(err, &coreErr) {
		return coreErr.Status
	}
	var echoErr *echo.HTTPError
	if errors.As(err, &echoErr) {
		return echoErr.Code
	}
	t.Fatalf("error %v is not an HTTP error", err)
	return 0
}

func observationFrame(seq int64) BrowserObservationFrame {
	return BrowserObservationFrame{
		FrameSeq:   seq,
		CapturedAt: time.Now().UTC(),
		MIMEType:   "image/jpeg",
		Data:       []byte{0xff, 0xd8, 0xff, 0xd9},
		Width:      1280,
		Height:     720,
	}
}

// observationBufferIdentity is the identity every frame in these tests is
// published under. Frames must name it exactly, so the tests carry it rather
// than letting a zero value pass by accident.
func observationBufferIdentity() BrowserObserverIdentity {
	return observerIdentity()
}

// A frame from a lease that no longer owns the Run belongs to an observation
// that already ended, so it must not reach the current viewer.
func TestObservationFrameBufferRejectsStaleLeases(t *testing.T) {
	t.Parallel()
	buffer := newObservationFrameBuffer(4)
	identity := observationBufferIdentity()
	runID := identity.RunID
	current := uuid.New()
	commandID := uuid.New()
	if !buffer.open(runID, current, commandID, identity) {
		t.Fatal("the first observation was refused")
	}

	if !buffer.admit(runID, current, commandID, identity, 1) {
		t.Fatal("the first event was not admitted")
	}
	if err := buffer.publish(runID, current, commandID, identity, observationFrame(1)); err != nil {
		t.Fatalf("current lease frame rejected: %v", err)
	}

	// The Worker numbers every event it sends from one counter, so a sequence
	// that does not advance is a replay whatever kind it carries.
	if buffer.admit(runID, current, commandID, identity, 1) {
		t.Fatal("a replayed event sequence was admitted")
	}
	if !buffer.admit(runID, current, commandID, identity, 2) {
		t.Fatal("an advancing event sequence was refused")
	}
	if buffer.admit(runID, current, uuid.New(), identity, 3) {
		t.Fatal("an event naming another command was admitted")
	}
	if buffer.admit(runID, uuid.New(), commandID, identity, 3) {
		t.Fatal("an event naming another lease was admitted")
	}
	if err := buffer.publish(runID, uuid.New(), commandID, identity, observationFrame(2)); err == nil {
		t.Fatal("a frame from a superseded lease was accepted")
	}
	// The lease alone is not the whole correlation: a Worker still answering a
	// superseded command, or one that moved to another Attempt, must be refused
	// even while the lease it names is current.
	if err := buffer.publish(runID, current, uuid.New(), identity, observationFrame(2)); err == nil {
		t.Fatal("a frame naming another command was accepted")
	}
	drifted := identity
	drifted.SessionEpoch++
	if err := buffer.publish(runID, current, commandID, drifted, observationFrame(2)); err == nil {
		t.Fatal("a frame naming another Attempt identity was accepted")
	}
	// A regressing sequence would let a replayed frame overwrite a newer one.
	if err := buffer.publish(runID, current, commandID, identity, observationFrame(1)); err == nil {
		t.Fatal("a regressing frame sequence was accepted")
	}
	if err := buffer.publish(uuid.New(), current, commandID, identity, observationFrame(2)); err == nil {
		t.Fatal("a frame for an unobserved Run was accepted")
	}
}

// The ceiling has to be enforced where the slot is taken. Checking capacity and
// opening as two steps lets concurrent starts all pass the check first.
func TestObservationFrameBufferAdmitsWithinItsQuota(t *testing.T) {
	t.Parallel()
	buffer := newObservationFrameBuffer(2)
	identities := []BrowserObserverIdentity{
		observationBufferIdentity(),
		observationBufferIdentity(),
		observationBufferIdentity(),
	}
	for index, identity := range identities[:2] {
		if !buffer.open(identity.RunID, uuid.New(), uuid.New(), identity) {
			t.Fatalf("observation %d was refused inside the quota", index)
		}
	}
	if buffer.open(identities[2].RunID, uuid.New(), uuid.New(), identities[2]) {
		t.Fatal("an observation beyond the quota was admitted")
	}
	// Replacing a Run already held takes no new slot, so a Run whose buffer
	// outlived its audit is not stranded at the ceiling.
	if !buffer.open(identities[0].RunID, uuid.New(), uuid.New(), identities[0]) {
		t.Fatal("replacing an existing observation was refused")
	}
	buffer.close(identities[0].RunID)
	if !buffer.open(identities[2].RunID, uuid.New(), uuid.New(), identities[2]) {
		t.Fatal("a released slot was not reused")
	}
}

func TestObservationFrameBufferRejectsMalformedFrames(t *testing.T) {
	t.Parallel()
	buffer := newObservationFrameBuffer(4)
	identity := observationBufferIdentity()
	runID := identity.RunID
	leaseID := uuid.New()
	commandID := uuid.New()
	buffer.open(runID, leaseID, commandID, identity)
	for name, mutate := range map[string]func(*BrowserObservationFrame){
		"wrong mime": func(f *BrowserObservationFrame) { f.MIMEType = "image/png" },
		"no data":    func(f *BrowserObservationFrame) { f.Data = nil },
		"zero width": func(f *BrowserObservationFrame) { f.Width = 0 },
		"zero seq":   func(f *BrowserObservationFrame) { f.FrameSeq = 0 },
		"oversized": func(f *BrowserObservationFrame) {
			f.Data = make([]byte, observationFrameBytesLimit+1)
		},
	} {
		t.Run(name, func(t *testing.T) {
			frame := observationFrame(1)
			mutate(&frame)
			if buffer.publish(runID, leaseID, commandID, identity, frame) == nil {
				t.Fatalf("%s frame was accepted", name)
			}
		})
	}
}

// A waiter must see a frame published after it started waiting, and must be
// woken when the observation closes rather than holding until the timeout.
func TestObservationFrameBufferWakesWaiters(t *testing.T) {
	t.Parallel()
	buffer := newObservationFrameBuffer(4)
	identity := observationBufferIdentity()
	runID := identity.RunID
	leaseID := uuid.New()
	commandID := uuid.New()
	buffer.open(runID, leaseID, commandID, identity)

	delivered := make(chan *BrowserObservationFrame, 1)
	go func() {
		frame, _ := buffer.wait(t.Context(), runID, 0)
		delivered <- frame
	}()
	time.Sleep(20 * time.Millisecond)
	if err := buffer.publish(runID, leaseID, commandID, identity, observationFrame(7)); err != nil {
		t.Fatal(err)
	}
	select {
	case frame := <-delivered:
		if frame == nil || frame.FrameSeq != 7 {
			t.Fatalf("waiter received %#v", frame)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a published frame never reached the waiter")
	}

	ended := make(chan error, 1)
	go func() {
		_, err := buffer.wait(t.Context(), runID, 7)
		ended <- err
	}()
	time.Sleep(20 * time.Millisecond)
	buffer.close(runID)
	select {
	case err := <-ended:
		if err == nil {
			t.Fatal("closing the observation did not end the wait")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("closing the observation left a waiter hanging")
	}
}

// The public frame shape must not leak the identities the Worker and Core use
// between themselves.
func TestObservationFrameCarriesNoIdentity(t *testing.T) {
	t.Parallel()
	raw, err := json.Marshal(observationFrame(1))
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	expected := []string{"frame_seq", "captured_at", "mime_type", "data", "width", "height"}
	if len(fields) != len(expected) {
		t.Fatalf("frame fields = %v, want exactly %v", fields, expected)
	}
	for _, name := range expected {
		if _, ok := fields[name]; !ok {
			t.Fatalf("frame is missing %s", name)
		}
	}
}

// A tombstone exists to settle the one terminal event that races an explicit
// stop. It must not answer that event twice, and it must not still be answering
// long after the observation ended.
func TestObservationRetiredTombstonesAreBounded(t *testing.T) {
	t.Parallel()
	clock := time.Now()
	buffer := newObservationFrameBuffer(4)
	buffer.now = func() time.Time { return clock }
	identity := observationBufferIdentity()
	leaseID := uuid.New()
	commandID := uuid.New()
	buffer.open(identity.RunID, leaseID, commandID, identity)
	if !buffer.admit(identity.RunID, leaseID, commandID, identity, 4) {
		t.Fatal("a live event was not admitted")
	}
	buffer.close(identity.RunID)

	// The stopped that trails the stop settles once.
	if !buffer.settleRetired(leaseID, commandID, identity, 5) {
		t.Fatal("the terminal event racing the stop was not settled")
	}
	if buffer.settleRetired(leaseID, commandID, identity, 5) {
		t.Fatal("the same terminal event settled twice")
	}
	// A sequence from before the observation ended is a replay either way.
	if buffer.settleRetired(leaseID, commandID, identity, 4) {
		t.Fatal("a sequence already consumed while live was settled")
	}
	// Another command or another Attempt is not this observation.
	if buffer.settleRetired(leaseID, uuid.New(), identity, 6) {
		t.Fatal("a terminal event naming another command was settled")
	}
	drifted := identity
	drifted.SessionEpoch++
	if buffer.settleRetired(leaseID, commandID, drifted, 6) {
		t.Fatal("a terminal event naming another Attempt was settled")
	}

	clock = clock.Add(observationRetiredTTL + time.Second)
	if buffer.settleRetired(leaseID, commandID, identity, 7) {
		t.Fatal("a tombstone outside its window still settled")
	}
	// And it is dropped rather than left to be re-checked forever.
	buffer.mu.Lock()
	_, known := buffer.retired[leaseID]
	order := len(buffer.retiredOrder)
	buffer.mu.Unlock()
	if known || order != 0 {
		t.Fatalf("an expired tombstone was kept: known=%v order=%d", known, order)
	}
}

// The lease check exists so a teardown can only close the observation it names.
// Checking and closing under separate locks lets a successor open in the gap and
// be closed by its predecessor's teardown.
func TestObservationCloseLeaseOnlyClosesItsOwn(t *testing.T) {
	t.Parallel()
	buffer := newObservationFrameBuffer(4)
	identity := observationBufferIdentity()
	successor := uuid.New()
	successorCommand := uuid.New()
	buffer.open(identity.RunID, uuid.New(), uuid.New(), identity)
	buffer.open(identity.RunID, successor, successorCommand, identity)

	if closed := buffer.closeLease(identity.RunID, uuid.New()); closed != 0 {
		t.Fatal("a teardown for an unrelated lease closed something")
	}
	if !buffer.admit(identity.RunID, successor, successorCommand, identity, 1) {
		t.Fatal("the successor observation was closed by another lease teardown")
	}
	buffer.closeLease(identity.RunID, successor)
	if buffer.admit(identity.RunID, successor, successorCommand, identity, 2) {
		t.Fatal("the successor survived its own teardown")
	}
}

// Selecting abandoned observations and closing them must be one step. A viewer
// polling in between would have its live observation torn down on evidence that
// stopped being true.
func TestObservationCloseAbandonedIsOneStep(t *testing.T) {
	t.Parallel()
	clock := time.Now()
	buffer := newObservationFrameBuffer(4)
	buffer.now = func() time.Time { return clock }
	watched := observationBufferIdentity()
	forgotten := observationBufferIdentity()
	watchedLease := uuid.New()
	forgottenLease := uuid.New()
	buffer.open(watched.RunID, watchedLease, uuid.New(), watched)
	buffer.open(forgotten.RunID, forgottenLease, uuid.New(), forgotten)

	// Both are inside the grace period to begin with.
	if closed := buffer.closeAbandoned(time.Minute); len(closed) != 0 {
		t.Fatalf("closed %d observations that were still being polled", len(closed))
	}

	clock = clock.Add(2 * time.Minute)
	// One viewer comes back; a poll is what proves someone is still reading.
	go func() { _, _ = buffer.wait(t.Context(), watched.RunID, 0) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		buffer.mu.Lock()
		polled := buffer.live[watched.RunID].lastPolledAt.Equal(clock)
		buffer.mu.Unlock()
		if polled {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the poll never registered")
		}
	}

	closed := buffer.closeAbandoned(time.Minute)
	if len(closed) != 1 {
		t.Fatalf("closed %d observations, want only the unpolled one", len(closed))
	}
	if closed[0].runID != forgotten.RunID || closed[0].leaseID != forgottenLease {
		t.Fatal("the wrong observation was reclaimed")
	}
	// Returned as closed, so the caller never has to close it a second time and
	// cannot close a successor by doing so.
	if buffer.admit(forgotten.RunID, forgottenLease, uuid.New(), forgotten, 1) {
		t.Fatal("the reclaimed observation was still live")
	}
	if held := buffer.count(); held != 1 {
		t.Fatalf("the buffer holds %d observations, want the polled one", held)
	}
}
