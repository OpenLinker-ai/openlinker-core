package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
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

func TestBrowserObserverFrameRoundTripsStandardByteEncoding(t *testing.T) {
	t.Parallel()
	captured := time.Now().UTC()
	event := BrowserObserverEventPayload{
		AttemptIdentity:      observerIdentity().RuntimeIdentity(),
		SessionEpoch:         3,
		BrowserSessionSHA256: strings.Repeat("a", 64),
		AttachmentSHA256:     strings.Repeat("b", 64),
		CommandID:            uuid.New(),
		LeaseID:              uuid.New(),
		EventSeq:             2,
		Kind:                 BrowserObserverFrame,
		CapturedAt:           &captured,
		Frame: &BrowserObserverFramePayload{
			MIMEType: "image/jpeg",
			Data:     []byte{0xff, 0xd8, 0xff, 0xd9},
			Width:    1280,
			Height:   720,
		},
	}

	raw, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal frame event: %v", err)
	}
	if !bytes.Contains(raw, []byte(`"data":"/9j/2Q=="`)) {
		t.Fatalf("frame bytes did not use the standard base64 wire encoding: %s", raw)
	}

	decoded, err := DecodeRuntimeBody[BrowserObserverEventPayload](bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("decode frame event: %v", err)
	}
	if !bytes.Equal(decoded.Frame.Data, event.Frame.Data) {
		t.Fatalf("decoded frame bytes = %v, want %v", decoded.Frame.Data, event.Frame.Data)
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

func TestObservationViewerCapacityMapsToRunScopedMessage(t *testing.T) {
	t.Parallel()
	mapped := browserObservationHTTPError(ErrObservationViewerCapacity)
	var coreErr *httpx.HTTPError
	if !errors.As(mapped, &coreErr) {
		t.Fatalf("capacity error = %T, want *httpx.HTTPError", mapped)
	}
	if coreErr.Status != 429 || coreErr.Message != "该 Run 的并发观察入口已达上限" {
		t.Fatalf("capacity error = status %d message %q", coreErr.Status, coreErr.Message)
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
	if err := buffer.publish(runID, current, commandID, identity, observationFrame(1), false); err != nil {
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
	if err := buffer.publish(runID, uuid.New(), commandID, identity, observationFrame(2), false); err == nil {
		t.Fatal("a frame from a superseded lease was accepted")
	}
	// The lease alone is not the whole correlation: a Worker still answering a
	// superseded command, or one that moved to another Attempt, must be refused
	// even while the lease it names is current.
	if err := buffer.publish(runID, current, uuid.New(), identity, observationFrame(2), false); err == nil {
		t.Fatal("a frame naming another command was accepted")
	}
	drifted := identity
	drifted.SessionEpoch++
	if err := buffer.publish(runID, current, commandID, drifted, observationFrame(2), false); err == nil {
		t.Fatal("a frame naming another Attempt identity was accepted")
	}
	// A regressing sequence would let a replayed frame overwrite a newer one.
	if err := buffer.publish(runID, current, commandID, identity, observationFrame(1), false); err == nil {
		t.Fatal("a regressing frame sequence was accepted")
	}
	if err := buffer.publish(uuid.New(), current, commandID, identity, observationFrame(2), false); err == nil {
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
			if buffer.publish(runID, leaseID, commandID, identity, frame, false) == nil {
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
	if err := buffer.publish(runID, leaseID, commandID, identity, observationFrame(7), false); err != nil {
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

func TestObservationFrameBufferFansOutToIndependentWaiters(t *testing.T) {
	t.Parallel()
	buffer := newObservationFrameBuffer(4)
	identity := observationBufferIdentity()
	runID := identity.RunID
	leaseID := uuid.New()
	commandID := uuid.New()
	buffer.open(runID, leaseID, commandID, identity)

	type result struct {
		frame *BrowserObservationFrame
		err   error
	}
	results := make(chan result, 2)
	for range 2 {
		go func() {
			frame, err := buffer.wait(t.Context(), runID, 0)
			results <- result{frame: frame, err: err}
		}()
	}
	waitForObservationFrameWaiters(t, buffer, runID, 2)
	if err := buffer.publish(runID, leaseID, commandID, identity, observationFrame(7), false); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		select {
		case got := <-results:
			if got.err != nil || got.frame == nil || got.frame.FrameSeq != 7 {
				t.Fatalf("fan-out waiter = frame %#v err %v", got.frame, got.err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("a fan-out waiter did not receive the published frame")
		}
	}
	waitForObservationFrameWaiters(t, buffer, runID, 0)
}

func TestObservationFrameBufferBoundsAndReusesWaiterCapacity(t *testing.T) {
	t.Parallel()
	buffer := newObservationFrameBuffer(4)
	identity := observationBufferIdentity()
	runID := identity.RunID
	buffer.open(runID, uuid.New(), uuid.New(), identity)

	results := make(chan error, observationMaxFrameWaitersPerRun+1)
	cancels := make([]context.CancelFunc, 0, observationMaxFrameWaitersPerRun+1)
	startWaiter := func() {
		ctx, cancel := context.WithCancel(t.Context())
		cancels = append(cancels, cancel)
		go func() {
			_, err := buffer.wait(ctx, runID, 0)
			results <- err
		}()
	}
	for range observationMaxFrameWaitersPerRun {
		startWaiter()
	}
	waitForObservationFrameWaiters(t, buffer, runID, observationMaxFrameWaitersPerRun)
	if _, err := buffer.wait(t.Context(), runID, 0); !errors.Is(err, ErrObservationViewerCapacity) {
		t.Fatalf("ninth waiter error = %v, want viewer capacity", err)
	}

	cancels[0]()
	select {
	case err := <-results:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled waiter error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled waiter did not return")
	}
	waitForObservationFrameWaiters(t, buffer, runID, observationMaxFrameWaitersPerRun-1)
	startWaiter()
	waitForObservationFrameWaiters(t, buffer, runID, observationMaxFrameWaitersPerRun)

	for _, cancel := range cancels[1:] {
		cancel()
	}
	for range observationMaxFrameWaitersPerRun {
		select {
		case err := <-results:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("released waiter error = %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("released waiter did not return")
		}
	}
	waitForObservationFrameWaiters(t, buffer, runID, 0)
}

func TestObservationFrameBufferWaiterCannotCrossReplacementLease(t *testing.T) {
	t.Parallel()
	buffer := newObservationFrameBuffer(4)
	identity := observationBufferIdentity()
	runID := identity.RunID
	buffer.open(runID, uuid.New(), uuid.New(), identity)

	ended := make(chan error, 1)
	go func() {
		_, err := buffer.wait(t.Context(), runID, 0)
		ended <- err
	}()
	waitForObservationFrameWaiters(t, buffer, runID, 1)

	successorLease := uuid.New()
	successorCommand := uuid.New()
	buffer.open(runID, successorLease, successorCommand, identity)
	select {
	case err := <-ended:
		if !errors.Is(err, ErrObservationInactive) {
			t.Fatalf("replaced waiter error = %v, want inactive", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replaced waiter did not leave its old lease")
	}
	if err := buffer.publish(runID, successorLease, successorCommand, identity, observationFrame(1), false); err != nil {
		t.Fatal(err)
	}
	frame, err := buffer.wait(t.Context(), runID, 0)
	if err != nil || frame == nil || frame.FrameSeq != 1 {
		t.Fatalf("replacement frame = %#v err %v", frame, err)
	}
}

func waitForObservationFrameWaiters(
	t *testing.T,
	buffer *observationFrameBuffer,
	runID uuid.UUID,
	want int,
) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		buffer.mu.Lock()
		got := 0
		if live := buffer.live[runID]; live != nil {
			got = live.waiters
		}
		buffer.mu.Unlock()
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("frame waiters = %d, want %d", got, want)
		}
		time.Sleep(time.Millisecond)
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

// stubRoundStatus makes the Run's own state available without a database. The
// production value queries runs and run_attempts; every rule that reads it is
// the same either way.
func stubRoundStatus(observation *BrowserObservation, round observationRoundStatus) {
	observation.roundStatusFn = func(
		context.Context,
		uuid.UUID,
	) (observationRoundStatus, error) {
		return round, nil
	}
}

// deliverFrame opens nothing: it admits and publishes one frame on an open lease.
func deliverFrame(
	t *testing.T,
	buffer *observationFrameBuffer,
	runID, leaseID, commandID uuid.UUID,
	identity BrowserObserverIdentity,
	seq int64,
	roundFinal bool,
) {
	t.Helper()
	if !buffer.admit(runID, leaseID, commandID, identity, seq) {
		t.Fatalf("frame %d was not admitted", seq)
	}
	if err := buffer.publish(
		runID, leaseID, commandID, identity, observationFrame(seq), roundFinal,
	); err != nil {
		t.Fatalf("frame %d rejected: %v", seq, err)
	}
}

// The final frame is kept the moment the Worker delivers it, not when the
// observation closes. Deciding at the close made the picture depend on the order
// the observation's ending arrived in -- an error from the Engine being torn down,
// the viewer's stop, the browser's closed event -- and every one of those orders
// has lost the frame at some point.
func TestObservationRetainsTheMarkedFrameOnReceipt(t *testing.T) {
	t.Parallel()
	clock := time.Now()
	buffer := newObservationFrameBuffer(4)
	buffer.now = func() time.Time { return clock }
	identity := observationBufferIdentity()
	runID := identity.RunID
	leaseID, commandID := uuid.New(), uuid.New()
	buffer.open(runID, leaseID, commandID, identity)

	// Ordinary frames are live content and nothing more.
	deliverFrame(t, buffer, runID, leaseID, commandID, identity, 1, false)
	if buffer.finalFrame(runID).frame != nil {
		t.Fatal("an unmarked frame was retained as final")
	}

	// The marked capture is retained immediately, while the observation is open.
	deliverFrame(t, buffer, runID, leaseID, commandID, identity, 2, true)
	read := buffer.finalFrame(runID)
	if read.frame == nil || read.frame.FrameSeq != 2 {
		t.Fatalf("the marked frame was not retained on receipt: %+v", read)
	}
	if read.attemptID != identity.AttemptID {
		t.Fatalf("retention attempt = %s, want %s", read.attemptID, identity.AttemptID)
	}
	if want := clock.Add(observationFinalFrameTTL); !read.retainedUntil.Equal(want) {
		t.Fatalf("retained_until = %s, want %s", read.retainedUntil, want)
	}

	// Copied, so a caller cannot reach into the buffer.
	read.frame.Data[0] ^= 0xff
	if again := buffer.finalFrame(runID).frame; again == nil || again.Data[0] == read.frame.Data[0] {
		t.Fatal("the retained frame shares its bytes with the caller")
	}

	// However the observation ends, the retention is untouched: the live poll
	// reports the ending, the retention keeps answering.
	buffer.close(runID)
	if _, err := buffer.wait(context.Background(), runID, 0); !errors.Is(err, ErrObservationInactive) {
		t.Fatalf("live poll after close returned %v", err)
	}
	if buffer.finalFrame(runID).frame == nil {
		t.Fatal("closing the observation discarded the round's final frame")
	}

	clock = clock.Add(observationFinalFrameTTL + time.Second)
	if buffer.finalFrame(runID).frame != nil {
		t.Fatal("a retained frame outside its window was still served")
	}
	buffer.mu.Lock()
	entries, bytes := len(buffer.finalFrames), buffer.finalBytes
	buffer.mu.Unlock()
	if entries != 0 || bytes != 0 {
		t.Fatalf("expired retention was kept: entries=%d bytes=%d", entries, bytes)
	}
}

// A retried Attempt that delivers its own marked capture replaces the earlier one:
// the Run is the key a viewer reads with, and the latest round is the one it asks
// about.
func TestObservationLaterMarkedFrameReplacesTheEarlierOne(t *testing.T) {
	t.Parallel()
	buffer := newObservationFrameBuffer(4)
	first := observationBufferIdentity()
	runID := first.RunID
	lease, command := uuid.New(), uuid.New()
	buffer.open(runID, lease, command, first)
	deliverFrame(t, buffer, runID, lease, command, first, 1, true)
	buffer.close(runID)

	retried := first
	retried.AttemptID = uuid.New()
	retried.SessionEpoch++
	lease, command = uuid.New(), uuid.New()
	buffer.open(runID, lease, command, retried)
	deliverFrame(t, buffer, runID, lease, command, retried, 1, true)

	read := buffer.finalFrame(runID)
	if read.attemptID != retried.AttemptID {
		t.Fatalf("retention attempt = %s, want the retried Attempt %s", read.attemptID, retried.AttemptID)
	}
	buffer.mu.Lock()
	entries := len(buffer.finalFrames)
	buffer.mu.Unlock()
	if entries != 1 {
		t.Fatalf("one Run holds %d retentions", entries)
	}
}

// Retention is memory this process holds after a round ended, so both bounds have
// to hold. The oldest goes first, because the Run a viewer is looking at is the one
// that just finished.
func TestObservationRetainedFinalFramesAreBounded(t *testing.T) {
	t.Parallel()
	buffer := newObservationFrameBuffer(observationFinalFrameLimit + 4)
	var runIDs []uuid.UUID
	for index := 0; index < observationFinalFrameLimit+3; index++ {
		identity := observationBufferIdentity()
		runIDs = append(runIDs, identity.RunID)
		lease, command := uuid.New(), uuid.New()
		buffer.open(identity.RunID, lease, command, identity)
		deliverFrame(t, buffer, identity.RunID, lease, command, identity, 1, true)
		buffer.close(identity.RunID)
	}
	buffer.mu.Lock()
	entries, order, bytes := len(buffer.finalFrames), len(buffer.finalOrder), buffer.finalBytes
	buffer.mu.Unlock()
	if entries != observationFinalFrameLimit || order != observationFinalFrameLimit {
		t.Fatalf("retained %d entries (%d ordered), want %d", entries, order, observationFinalFrameLimit)
	}
	if bytes > observationFinalFrameBytesLimit {
		t.Fatalf("retained %d bytes, over the %d ceiling", bytes, observationFinalFrameBytesLimit)
	}
	for _, evicted := range runIDs[:3] {
		if buffer.finalFrame(evicted).frame != nil {
			t.Fatal("an evicted retention was still served")
		}
	}
	for _, kept := range runIDs[3:] {
		if buffer.finalFrame(kept).frame == nil {
			t.Fatal("a retention inside both bounds was evicted")
		}
	}
}

// What a read supports, with no I/O. None of these rows may be answered with a
// picture that is not this round's, and "the Run is still running" is never
// answered as "no picture".
func TestFinalFrameVerdictRules(t *testing.T) {
	t.Parallel()
	attempt := uuid.New()
	retried := uuid.New()
	frame := observationFrame(1)
	held := observationFinalFrameRead{frame: &frame, attemptID: attempt}
	empty := observationFinalFrameRead{}
	ended := observationRoundStatus{known: true, terminal: true, attemptID: attempt}

	for name, expect := range map[string]struct {
		read    observationFinalFrameRead
		round   observationRoundStatus
		verdict finalFrameVerdict
	}{
		"retained by the latest Attempt of an ended round": {held, ended, finalFrameServe},
		// The final frame does not exist yet; this is also what keeps a retry from
		// being answered with the previous Attempt's page while it runs.
		"retained, but the Run is running again": {
			held, observationRoundStatus{known: true, attemptID: retried}, finalFrameUnsettled,
		},
		"nothing retained, still running": {
			empty, observationRoundStatus{known: true}, finalFrameUnsettled,
		},
		// The Worker delivers and waits for the acknowledgement before it reports
		// the result, so a terminal Run with nothing retained kept nothing.
		"nothing retained, round ended": {empty, ended, finalFrameAbsent},
		"retained by an earlier Attempt": {
			held, observationRoundStatus{known: true, terminal: true, attemptID: retried}, finalFrameAbsent,
		},
		"round unknown": {held, observationRoundStatus{}, finalFrameAbsent},
	} {
		if got := finalFrameVerdictFor(expect.read, expect.round); got != expect.verdict {
			t.Fatalf("%s: verdict=%d, want %d", name, got, expect.verdict)
		}
	}
}

// The service answer names which situation it is in: a snapshot, a round still
// running, or a round that kept nothing -- including a retry that superseded an
// earlier Attempt's picture.
func TestBrowserObservationFinalFrameAnswers(t *testing.T) {
	t.Parallel()
	observation := NewBrowserObservation(nil, nil, uuid.New(), 0)
	identity := observationBufferIdentity()
	runID := identity.RunID

	stubRoundStatus(observation, observationRoundStatus{known: true, attemptID: identity.AttemptID})
	if _, err := observation.FinalFrame(t.Context(), runID); !errors.Is(err, ErrObservationFinalFrameUnsettled) {
		t.Fatalf("running Run returned %v, want unsettled", err)
	}

	ended := observationRoundStatus{known: true, terminal: true, attemptID: identity.AttemptID}
	stubRoundStatus(observation, ended)
	if _, err := observation.FinalFrame(t.Context(), runID); !errors.Is(err, ErrObservationNoFinalFrame) {
		t.Fatalf("ended Run with nothing retained returned %v", err)
	}

	lease, command := uuid.New(), uuid.New()
	observation.frames.open(runID, lease, command, identity)
	deliverFrame(t, observation.frames, runID, lease, command, identity, 9, true)
	observation.frames.close(runID)

	final, err := observation.FinalFrame(t.Context(), runID)
	if err != nil {
		t.Fatalf("final frame: %v", err)
	}
	if !final.Final || final.RunID != runID || final.Frame.FrameSeq != 9 {
		t.Fatalf("unexpected final frame: %+v", final)
	}
	if final.RetainedUntil.IsZero() || final.RetainedUntil.Location() != time.UTC {
		t.Fatalf("retained_until = %s", final.RetainedUntil)
	}

	// A later Attempt ended the round without a picture of its own.
	stubRoundStatus(observation, observationRoundStatus{known: true, terminal: true, attemptID: uuid.New()})
	if _, err := observation.FinalFrame(t.Context(), runID); !errors.Is(err, ErrObservationNoFinalFrame) {
		t.Fatalf("superseded retention returned %v", err)
	}
}

// Only the routing answer depends on the audit: another instance watched the Run,
// so only that process could have held the retention.
func TestFinalFrameAbsenceRouting(t *testing.T) {
	t.Parallel()
	instance := uuid.New()
	if err := finalFrameAbsence(instance, instance); !errors.Is(err, ErrObservationNoFinalFrame) {
		t.Fatalf("owned here returned %v", err)
	}
	if err := finalFrameAbsence(uuid.New(), instance); !errors.Is(err, ErrObservationChannelUnavailable) {
		t.Fatalf("owned elsewhere returned %v", err)
	}
}

// 425 while the round runs, 204 when it kept nothing, the snapshot otherwise --
// all no-store, because a retained frame is still live page content.
func TestBrowserObservationFinalFrameEndpointAnswers(t *testing.T) {
	t.Parallel()
	observation := NewBrowserObservation(nil, nil, uuid.New(), 0)
	handler := &Handler{browserObservation: observation}
	identity := observationBufferIdentity()
	runID := identity.RunID
	respond := func() (*httptest.ResponseRecorder, error) {
		recorder := httptest.NewRecorder()
		context := echo.New().NewContext(httptest.NewRequest(http.MethodGet, "/", nil), recorder)
		return recorder, handler.respondBrowserObservationFinalFrame(context, runID)
	}

	stubRoundStatus(observation, observationRoundStatus{known: true, attemptID: identity.AttemptID})
	_, err := respond()
	var early *echo.HTTPError
	if !errors.As(err, &early) || early.Code != http.StatusTooEarly {
		t.Fatalf("running Run returned %v, want 425", err)
	}

	stubRoundStatus(observation, observationRoundStatus{known: true, terminal: true, attemptID: identity.AttemptID})
	empty, err := respond()
	if err != nil || empty.Code != http.StatusNoContent {
		t.Fatalf("ended Run with nothing retained: status=%d err=%v", empty.Code, err)
	}
	if empty.Header().Get("Cache-Control") != "private, no-store" {
		t.Fatalf("204 Cache-Control = %q", empty.Header().Get("Cache-Control"))
	}

	lease, command := uuid.New(), uuid.New()
	observation.frames.open(runID, lease, command, identity)
	deliverFrame(t, observation.frames, runID, lease, command, identity, 3, true)
	served, err := respond()
	if err != nil || served.Code != http.StatusOK {
		t.Fatalf("retained snapshot: status=%d err=%v", served.Code, err)
	}
	if served.Header().Get("Cache-Control") != "private, no-store" {
		t.Fatalf("200 Cache-Control = %q", served.Header().Get("Cache-Control"))
	}
	var body struct {
		RunID string `json:"run_id"`
		Final bool   `json:"final"`
		Frame struct {
			FrameSeq int64  `json:"frame_seq"`
			Data     []byte `json:"data"`
		} `json:"frame"`
	}
	if err := json.Unmarshal(served.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if body.RunID != runID.String() || !body.Final || body.Frame.FrameSeq != 3 || len(body.Frame.Data) == 0 {
		t.Fatalf("snapshot body = %+v", body)
	}
}

// The marker belongs to a frame. A lifecycle or error event carrying it is a
// contract violation, not a frame to keep.
func TestBrowserObserverFinalFrameMarkerRequiresAFrame(t *testing.T) {
	t.Parallel()
	captured := time.Now().UTC()
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
	marked := base
	marked.Kind = BrowserObserverFrame
	marked.CapturedAt = &captured
	marked.Frame = &BrowserObserverFramePayload{
		MIMEType: "image/jpeg",
		Data:     []byte{0xff, 0xd8, 0xff, 0xd9},
		Width:    1280,
		Height:   720,
	}
	marked.FinalFrame = true
	if err := marked.Validate(); err != nil {
		t.Fatalf("a marked frame event was rejected: %v", err)
	}
	for _, kind := range []BrowserObserverEventKind{
		BrowserObserverStarted,
		BrowserObserverStopped,
		BrowserObserverError,
	} {
		event := base
		event.Kind = kind
		if kind == BrowserObserverError {
			event.ErrorCode = "BROWSER_RUNTIME_UNAVAILABLE"
		}
		event.FinalFrame = true
		if err := event.Validate(); err == nil {
			t.Fatalf("%s carried the final-frame marker", kind)
		}
	}
}

// Why the Worker waits for Core to declare the marker: a Core that predates an
// event field rejects it as a validation failure, and on the Runtime WebSocket
// that closes the connection. This pins that consequence.
func TestUnknownObserverEventFieldIsFatalToTheRuntimeConnection(t *testing.T) {
	t.Parallel()
	identity := observerIdentity()
	raw, err := json.Marshal(map[string]any{
		"attempt_identity":          identity.RuntimeIdentity(),
		"session_epoch":             identity.SessionEpoch,
		"browser_session_sha256":    identity.BrowserSessionSHA256,
		"browser_attachment_sha256": identity.AttachmentSHA256,
		"command_id":                uuid.New(),
		"lease_id":                  uuid.New(),
		"event_seq":                 1,
		"kind":                      "started",
		"field_from_the_future":     true,
	})
	if err != nil {
		t.Fatal(err)
	}
	var payload BrowserObserverEventPayload
	decodeErr := decodeRuntimeJSON(raw, &payload)
	var transport *RuntimeTransportError
	if !errors.As(decodeErr, &transport) {
		t.Fatalf("decode error = %v, want a Runtime transport error", decodeErr)
	}
	if _, fatal := RuntimeWebSocketCloseCode(transport.Body.Code); !fatal {
		t.Fatalf("code %s does not close the connection; update the compatibility reasoning", transport.Body.Code)
	}
}
