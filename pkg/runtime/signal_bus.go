package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"

	"github.com/google/uuid"
)

var (
	ErrRuntimeSignalBusClosed      = errors.New("runtime signal bus is closed")
	ErrRuntimeSignalBusUnavailable = errors.New("runtime signal bus is unavailable")
	ErrRuntimeSignalInvalid        = errors.New("runtime signal is invalid")
	runtimeSignalTypePattern       = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$`)
)

const MaxRuntimeSignalConnections = 64

// RuntimeConnectionIdentity is the immutable attachment-generation fence for
// one live Runtime transport. A lifecycle signal must match every field before
// it can affect a connection.
type RuntimeConnectionIdentity struct {
	RuntimeSessionID uuid.UUID `json:"runtime_session_id"`
	SessionEpoch     int64     `json:"session_epoch"`
	AttachmentID     uuid.UUID `json:"attachment_id"`
}

// RuntimeSignal is deliberately a wake-up hint, never a data transport. Its
// wire shape is the complete allowlist: Run input/output, token material,
// invocation capabilities, payloads and secrets have no field through which
// they can reach Redis or an in-process subscriber.
type RuntimeSignal struct {
	SignalID         uuid.UUID                   `json:"signal_id"`
	Type             string                      `json:"type"`
	AgentID          uuid.UUID                   `json:"agent_id"`
	RunID            *uuid.UUID                  `json:"run_id,omitempty"`
	NodeID           *uuid.UUID                  `json:"node_id,omitempty"`
	TargetInstanceID *uuid.UUID                  `json:"target_instance_id,omitempty"`
	CredentialID     *uuid.UUID                  `json:"credential_id,omitempty"`
	Connections      []RuntimeConnectionIdentity `json:"connections,omitempty"`
}

type RuntimeSignalHandler func(context.Context, RuntimeSignal) error

// RuntimeSignalBus accelerates database-backed work discovery. Subscribe is
// blocking and returns when ctx is canceled, the bus closes, or the transport
// fails. Callers must supervise and retry subscriptions; correctness must not
// depend on receiving any particular signal.
type RuntimeSignalBus interface {
	Publish(context.Context, RuntimeSignal) error
	Subscribe(context.Context, RuntimeSignalHandler) error
	Health(context.Context) error
	Close() error
}

func ValidateRuntimeSignal(signal RuntimeSignal) error {
	if signal.SignalID == uuid.Nil {
		return fmt.Errorf("%w: signal_id is required", ErrRuntimeSignalInvalid)
	}
	if signal.AgentID == uuid.Nil {
		return fmt.Errorf("%w: agent_id is required", ErrRuntimeSignalInvalid)
	}
	if !runtimeSignalTypePattern.MatchString(signal.Type) {
		return fmt.Errorf("%w: type is invalid", ErrRuntimeSignalInvalid)
	}
	if _, allowed := allowedRuntimeSignalTypes[signal.Type]; !allowed {
		return fmt.Errorf("%w: type is not allowlisted", ErrRuntimeSignalInvalid)
	}
	if signal.RunID != nil && *signal.RunID == uuid.Nil {
		return fmt.Errorf("%w: run_id is invalid", ErrRuntimeSignalInvalid)
	}
	if signal.NodeID != nil && *signal.NodeID == uuid.Nil {
		return fmt.Errorf("%w: node_id is invalid", ErrRuntimeSignalInvalid)
	}
	if signal.Type == runtimeNodeCapacityAvailableSignal {
		if signal.NodeID == nil {
			return fmt.Errorf("%w: node_id is required", ErrRuntimeSignalInvalid)
		}
	} else if signal.NodeID != nil {
		return fmt.Errorf("%w: node_id is not allowed for this type", ErrRuntimeSignalInvalid)
	}
	if signal.TargetInstanceID != nil && *signal.TargetInstanceID == uuid.Nil {
		return fmt.Errorf("%w: target_instance_id is invalid", ErrRuntimeSignalInvalid)
	}
	if signal.Type != "credential.revoke" {
		if signal.CredentialID != nil || len(signal.Connections) > 0 {
			return fmt.Errorf("%w: credential revocation identity is not allowed", ErrRuntimeSignalInvalid)
		}
		return nil
	}
	if signal.CredentialID != nil && *signal.CredentialID == uuid.Nil {
		return fmt.Errorf("%w: credential_id is invalid", ErrRuntimeSignalInvalid)
	}
	if len(signal.Connections) > MaxRuntimeSignalConnections {
		return fmt.Errorf("%w: too many connection identities", ErrRuntimeSignalInvalid)
	}
	if len(signal.Connections) > 0 && signal.CredentialID == nil {
		return fmt.Errorf("%w: credential_id is required with connections", ErrRuntimeSignalInvalid)
	}
	seen := make(map[RuntimeConnectionIdentity]struct{}, len(signal.Connections))
	for _, identity := range signal.Connections {
		if identity.RuntimeSessionID == uuid.Nil || identity.SessionEpoch <= 0 ||
			identity.AttachmentID == uuid.Nil {
			return fmt.Errorf("%w: connection identity is invalid", ErrRuntimeSignalInvalid)
		}
		if _, duplicate := seen[identity]; duplicate {
			return fmt.Errorf("%w: connection identity is duplicated", ErrRuntimeSignalInvalid)
		}
		seen[identity] = struct{}{}
	}
	return nil
}

func MarshalRuntimeSignal(signal RuntimeSignal) ([]byte, error) {
	if err := ValidateRuntimeSignal(signal); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(signal)
	if err != nil {
		return nil, fmt.Errorf("encode runtime signal: %w", err)
	}
	return encoded, nil
}

// ParseRuntimeSignal rejects unknown fields and multiple JSON values. This is
// important even for trusted publishers: accepting an accidental payload
// field would silently widen the Redis data-classification boundary.
func ParseRuntimeSignal(encoded []byte) (RuntimeSignal, error) {
	var signal RuntimeSignal
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&signal); err != nil {
		return RuntimeSignal{}, fmt.Errorf("%w: invalid JSON", ErrRuntimeSignalInvalid)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return RuntimeSignal{}, fmt.Errorf("%w: expected one JSON value", ErrRuntimeSignalInvalid)
	}
	if err := ValidateRuntimeSignal(signal); err != nil {
		return RuntimeSignal{}, err
	}
	return signal, nil
}

func runtimeSignalTargetsInstance(signal RuntimeSignal, instanceID uuid.UUID) bool {
	return signal.TargetInstanceID == nil || instanceID == uuid.Nil || *signal.TargetInstanceID == instanceID
}
