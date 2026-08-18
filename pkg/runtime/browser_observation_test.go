package runtime

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

func observerIdentity() BrowserObserverIdentity {
	return BrowserObserverIdentity{
		RunID:            uuid.New(),
		AttemptID:        uuid.New(),
		SessionEpoch:     3,
		AttachmentID:     uuid.New(),
		RuntimeSessionID: uuid.New(),
	}
}

func observerCommand(action BrowserObserverAction) BrowserObserverCommandPayload {
	now := time.Now().UTC()
	return BrowserObserverCommandPayload{
		AttemptIdentity: observerIdentity(),
		CommandID:       uuid.New(),
		Action:          action,
		LeaseID:         uuid.New(),
		LeaseExpiresAt:  now.Add(time.Minute),
		DeadlineAt:      now.Add(time.Minute),
		FrameIntervalMS: observationDefaultFrameIntervalMS,
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
		"zero epoch":    func(c *BrowserObserverCommandPayload) { c.AttemptIdentity.SessionEpoch = 0 },
		"no attachment": func(c *BrowserObserverCommandPayload) { c.AttemptIdentity.AttachmentID = uuid.Nil },
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

func TestBrowserObserverEventValidationPerKind(t *testing.T) {
	t.Parallel()
	captured := time.Now().UTC()
	frame := &BrowserObserverFramePayload{
		MIMEType: "image/jpeg",
		Data:     []byte{0xff, 0xd8, 0xff, 0xd9},
		Width:    1280,
		Height:   720,
	}
	base := BrowserObserverEventPayload{
		AttemptIdentity: observerIdentity(),
		CommandID:       uuid.New(),
		LeaseID:         uuid.New(),
		EventSeq:        1,
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
	unbound := NewBrowserObservation(nil, nil, uuid.New())
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
	observation := NewBrowserObservation(nil, nil, uuid.New())
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
