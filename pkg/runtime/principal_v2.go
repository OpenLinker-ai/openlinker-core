package runtime

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

// runtimePrincipalLockQueries is intentionally shared by claim, lease,
// Event, and Result transactions. Every Agent Node write must acquire these
// locks in Session -> Node -> Credential order before it locks a Run.
type runtimePrincipalLockQueries interface {
	LockRuntimeSessionForPrincipalValidation(context.Context, db.LockRuntimeSessionForPrincipalValidationParams) (db.LockRuntimeSessionForPrincipalValidationRow, error)
	LockRuntimeNodeForPrincipalValidation(context.Context, db.LockRuntimeNodeForPrincipalValidationParams) (db.LockRuntimeNodeForPrincipalValidationRow, error)
	LockRuntimeCredentialForPrincipalValidation(context.Context, db.LockRuntimeCredentialForPrincipalValidationParams) (db.LockRuntimeCredentialForPrincipalValidationRow, error)
}

type lockedRuntimePrincipal struct {
	nodePrincipal bool
	session       db.LockRuntimeSessionForPrincipalValidationRow
	node          db.LockRuntimeNodeForPrincipalValidationRow
	credential    db.LockRuntimeCredentialForPrincipalValidationRow
}

func lockRuntimePrincipal(
	ctx context.Context,
	queries runtimePrincipalLockQueries,
	principal RuntimeEventPrincipal,
) (lockedRuntimePrincipal, error) {
	if queries == nil || !runtimeEventPrincipalShapeValid(principal) {
		return lockedRuntimePrincipal{}, ErrInvalidRuntimeEvent
	}
	if principal.NodeID == nil {
		return lockedRuntimePrincipal{}, nil
	}

	session, err := queries.LockRuntimeSessionForPrincipalValidation(ctx, db.LockRuntimeSessionForPrincipalValidationParams{
		RuntimeSessionID:       *principal.RuntimeSessionID,
		NodeID:                 *principal.NodeID,
		AgentID:                principal.AgentID,
		CredentialID:           *principal.CredentialID,
		WorkerID:               *principal.WorkerID,
		AttachedCoreInstanceID: principal.CoreInstanceID,
	})
	if err != nil {
		return lockedRuntimePrincipal{}, normalizeRuntimePrincipalLockError(err)
	}
	node, err := queries.LockRuntimeNodeForPrincipalValidation(ctx, db.LockRuntimeNodeForPrincipalValidationParams{
		NodeID:                    *principal.NodeID,
		DeviceCertificateSerial:   *principal.DeviceCertificateSerial,
		DevicePublicKeyThumbprint: *principal.DevicePublicKeyThumbprintSHA256,
	})
	if err != nil {
		return lockedRuntimePrincipal{}, normalizeRuntimePrincipalLockError(err)
	}
	agentID := principal.AgentID
	credential, err := queries.LockRuntimeCredentialForPrincipalValidation(ctx, db.LockRuntimeCredentialForPrincipalValidationParams{
		CredentialID: *principal.CredentialID,
		AgentID:      &agentID,
	})
	if err != nil {
		return lockedRuntimePrincipal{}, normalizeRuntimePrincipalLockError(err)
	}
	locked := lockedRuntimePrincipal{nodePrincipal: true, session: session, node: node, credential: credential}
	if !locked.matches(principal) {
		return lockedRuntimePrincipal{}, newRuntimeEventError(RuntimeEventErrorLeaseIdentityMismatch, nil)
	}
	return locked, nil
}

func normalizeRuntimePrincipalLockError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return newRuntimeEventError(RuntimeEventErrorLeaseIdentityMismatch, err)
	}
	return err
}

func (p lockedRuntimePrincipal) matches(principal RuntimeEventPrincipal) bool {
	return principal.CredentialID != nil && principal.NodeID != nil && principal.WorkerID != nil &&
		principal.RuntimeSessionID != nil && principal.CoreInstanceID != nil &&
		principal.DeviceCertificateSerial != nil && principal.DevicePublicKeyThumbprintSHA256 != nil &&
		p.session.RuntimeSessionID == *principal.RuntimeSessionID &&
		p.session.NodeID == *principal.NodeID && p.session.AgentID == principal.AgentID &&
		p.session.CredentialID == *principal.CredentialID && p.session.WorkerID == *principal.WorkerID &&
		p.session.AttachedCoreInstanceID != nil && *p.session.AttachedCoreInstanceID == *principal.CoreInstanceID &&
		p.node.NodeID == *principal.NodeID &&
		p.node.DeviceCertificateSerial == *principal.DeviceCertificateSerial &&
		p.node.DevicePublicKeyThumbprint == *principal.DevicePublicKeyThumbprintSHA256 &&
		p.credential.ID == *principal.CredentialID && p.credential.AgentID != nil &&
		*p.credential.AgentID == principal.AgentID
}

// validAt closes the natural-expiry race between the Credential lock query
// and a later Run mutation in the same transaction. Status changes are
// blocked by row locks; wall-clock expiry still needs comparison with the
// newest PostgreSQL timestamp obtained after the Run lock.
func (p lockedRuntimePrincipal) validAt(databaseNow time.Time) bool {
	if !p.nodePrincipal {
		return true
	}
	return !databaseNow.IsZero() &&
		(p.credential.ExpiresAt == nil || databaseNow.Before(*p.credential.ExpiresAt))
}

func runtimeEventPrincipalFromSession(principal RuntimeSessionPrincipal) RuntimeEventPrincipal {
	return principal.EventPrincipal()
}

func runtimePrincipalTargetIdentity(principal RuntimeEventPrincipal) (sessionID, credentialID uuid.UUID, ok bool) {
	if principal.RuntimeSessionID == nil || principal.CredentialID == nil {
		return uuid.Nil, uuid.Nil, false
	}
	return *principal.RuntimeSessionID, *principal.CredentialID, true
}
