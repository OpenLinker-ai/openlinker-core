package runtime

import (
	"time"

	"github.com/google/uuid"
)

// Observation payloads. A Runtime command has no reply channel, so the outcome
// of a start -- started, or the error that stopped it -- travels back on the
// event stream rather than as a command reply.

type BrowserObserverAction string

const (
	BrowserObserverStart BrowserObserverAction = "start"
	BrowserObserverStop  BrowserObserverAction = "stop"
)

type BrowserObserverEventKind string

const (
	BrowserObserverStarted BrowserObserverEventKind = "started"
	BrowserObserverFrame   BrowserObserverEventKind = "frame"
	BrowserObserverStopped BrowserObserverEventKind = "stopped"
	BrowserObserverError   BrowserObserverEventKind = "error"
)

const (
	BrowserObserverMinFrameIntervalMS = 100
	BrowserObserverMaxFrameIntervalMS = 5000
	BrowserObserverMaxFrameBytes      = 1 << 20
)

// BrowserObserverIdentity carries hashed Browser identity. The ready lifecycle
// event publishes browser_session_sha256 and browser_attachment_sha256 and never
// the underlying UUIDs, so Core cannot send raw IDs here without being told
// something it is deliberately not told. The Worker rehashes its own identity to
// verify, which is no weaker.
type BrowserObserverIdentity struct {
	RunID                uuid.UUID `json:"run_id"`
	AttemptID            uuid.UUID `json:"attempt_id"`
	SessionEpoch         int64     `json:"session_epoch"`
	BrowserSessionSHA256 string    `json:"browser_session_sha256"`
	AttachmentSHA256     string    `json:"browser_attachment_sha256"`
	RuntimeSessionID     uuid.UUID `json:"runtime_session_id"`
}

type BrowserObserverCommandPayload struct {
	AttemptIdentity BrowserObserverIdentity `json:"attempt_identity"`
	CommandID       uuid.UUID               `json:"command_id"`
	Action          BrowserObserverAction   `json:"action"`
	LeaseID         uuid.UUID               `json:"lease_id"`
	LeaseExpiresAt  time.Time               `json:"lease_expires_at"`
	DeadlineAt      time.Time               `json:"deadline_at"`
	FrameIntervalMS int                     `json:"frame_interval_ms"`
}

type BrowserObserverFramePayload struct {
	MIMEType string `json:"mime_type"`
	Data     []byte `json:"data"`
	Width    int    `json:"width"`
	Height   int    `json:"height"`
}

type BrowserObserverEventPayload struct {
	AttemptIdentity BrowserObserverIdentity      `json:"attempt_identity"`
	CommandID       uuid.UUID                    `json:"command_id"`
	LeaseID         uuid.UUID                    `json:"lease_id"`
	EventSeq        int64                        `json:"event_seq"`
	Kind            BrowserObserverEventKind     `json:"kind"`
	CapturedAt      *time.Time                   `json:"captured_at,omitempty"`
	Frame           *BrowserObserverFramePayload `json:"frame,omitempty"`
	ErrorCode       string                       `json:"error_code,omitempty"`
}

type BrowserObserverEventAckPayload struct {
	AttemptIdentity BrowserObserverIdentity `json:"attempt_identity"`
	LeaseID         uuid.UUID               `json:"lease_id"`
	EventSeq        int64                   `json:"event_seq"`
}

func (identity BrowserObserverIdentity) validate() error {
	if identity.RunID == uuid.Nil || identity.AttemptID == uuid.Nil ||
		identity.SessionEpoch < 1 ||
		!validSHA256Hex(identity.BrowserSessionSHA256) ||
		!validSHA256Hex(identity.AttachmentSHA256) ||
		identity.RuntimeSessionID == uuid.Nil {
		return runtimeValidationError("browser observer identity is invalid", nil)
	}
	return nil
}

func (payload BrowserObserverCommandPayload) Validate() error {
	if err := payload.AttemptIdentity.validate(); err != nil {
		return err
	}
	if payload.CommandID == uuid.Nil || payload.LeaseID == uuid.Nil {
		return runtimeValidationError("browser observer command identity is invalid", nil)
	}
	switch payload.Action {
	case BrowserObserverStop:
		return nil
	case BrowserObserverStart:
	default:
		return runtimeValidationError("browser observer action is invalid", nil)
	}
	if payload.LeaseExpiresAt.IsZero() || payload.DeadlineAt.IsZero() {
		return runtimeValidationError("browser observer start requires bounded deadlines", nil)
	}
	if payload.FrameIntervalMS < BrowserObserverMinFrameIntervalMS ||
		payload.FrameIntervalMS > BrowserObserverMaxFrameIntervalMS {
		return runtimeValidationError("browser observer frame interval is out of range", nil)
	}
	return nil
}

func (payload BrowserObserverEventPayload) Validate() error {
	if err := payload.AttemptIdentity.validate(); err != nil {
		return err
	}
	if payload.CommandID == uuid.Nil || payload.LeaseID == uuid.Nil || payload.EventSeq < 1 {
		return runtimeValidationError("browser observer event identity is invalid", nil)
	}
	switch payload.Kind {
	case BrowserObserverStarted, BrowserObserverStopped:
		// A lifecycle event carrying a frame would smuggle page content past a
		// consumer that only inspects frame events for it.
		if payload.Frame != nil {
			return runtimeValidationError("browser observer lifecycle event carries a frame", nil)
		}
	case BrowserObserverFrame:
		if payload.Frame == nil || payload.CapturedAt == nil || payload.CapturedAt.IsZero() {
			return runtimeValidationError("browser observer frame event is incomplete", nil)
		}
		if payload.Frame.MIMEType != "image/jpeg" ||
			len(payload.Frame.Data) == 0 ||
			len(payload.Frame.Data) > BrowserObserverMaxFrameBytes ||
			payload.Frame.Width < 1 || payload.Frame.Height < 1 {
			return runtimeValidationError("browser observer frame is invalid", nil)
		}
	case BrowserObserverError:
		if !validRequiredString(payload.ErrorCode, 100) {
			return runtimeValidationError("browser observer error event has no code", nil)
		}
	default:
		return runtimeValidationError("browser observer event kind is invalid", nil)
	}
	return nil
}

func (payload BrowserObserverEventAckPayload) Validate() error {
	if err := payload.AttemptIdentity.validate(); err != nil {
		return err
	}
	if payload.LeaseID == uuid.Nil || payload.EventSeq < 1 {
		return runtimeValidationError("browser observer ack identity is invalid", nil)
	}
	return nil
}
