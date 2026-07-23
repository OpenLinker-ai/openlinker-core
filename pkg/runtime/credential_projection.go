package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

const RuntimeCredentialProjectionTTL = 15 * time.Minute

type RuntimeConnectionRegistration struct {
	Identity     RuntimeConnectionIdentity
	CredentialID uuid.UUID
}

type RuntimeCredentialProjectionState string

const (
	RuntimeCredentialProjectionMissing   RuntimeCredentialProjectionState = "missing"
	RuntimeCredentialProjectionActive    RuntimeCredentialProjectionState = "active"
	RuntimeCredentialProjectionRevoked   RuntimeCredentialProjectionState = "revoked"
	RuntimeCredentialProjectionMalformed RuntimeCredentialProjectionState = "malformed"
)

type RuntimeCredentialProjectionResult struct {
	Registration RuntimeConnectionRegistration
	State        RuntimeCredentialProjectionState
}

// RuntimeCredentialProjectionStore is a bounded Redis cache. PostgreSQL
// remains authoritative: missing or malformed values must be revalidated
// there, never interpreted as active.
type RuntimeCredentialProjectionStore interface {
	Check(context.Context, []RuntimeConnectionRegistration) ([]RuntimeCredentialProjectionResult, error)
	MarkActive(context.Context, []RuntimeConnectionRegistration) error
}

type RuntimeCredentialProjectionStoreProvider interface {
	RuntimeCredentialProjectionStore() (RuntimeCredentialProjectionStore, error)
}

func runtimeCredentialProjectionValue(
	state RuntimeCredentialProjectionState,
	credentialID uuid.UUID,
) (string, error) {
	if credentialID == uuid.Nil {
		return "", errors.New("runtime credential projection credential_id is required")
	}
	switch state {
	case RuntimeCredentialProjectionActive, RuntimeCredentialProjectionRevoked:
		return string(state) + ":" + credentialID.String(), nil
	default:
		return "", fmt.Errorf("runtime credential projection state %q cannot be stored", state)
	}
}

func parseRuntimeCredentialProjectionValue(
	value string,
	expectedCredentialID uuid.UUID,
) RuntimeCredentialProjectionState {
	active, _ := runtimeCredentialProjectionValue(
		RuntimeCredentialProjectionActive, expectedCredentialID,
	)
	if value == active {
		return RuntimeCredentialProjectionActive
	}
	revoked, _ := runtimeCredentialProjectionValue(
		RuntimeCredentialProjectionRevoked, expectedCredentialID,
	)
	if value == revoked {
		return RuntimeCredentialProjectionRevoked
	}
	return RuntimeCredentialProjectionMalformed
}
