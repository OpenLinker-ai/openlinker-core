package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

func TestLockRuntimePrincipalUsesFixedOrderAndDatabaseExpiry(t *testing.T) {
	principal := runtimeNodePrincipalFixture()
	now := time.Date(2026, 7, 11, 8, 0, 0, 0, time.UTC)
	expiresAt := now.Add(time.Minute)
	fake := &runtimePrincipalLockFake{
		session: db.LockRuntimeSessionForPrincipalValidationRow{
			RuntimeSessionID:       *principal.RuntimeSessionID,
			NodeID:                 *principal.NodeID,
			AgentID:                principal.AgentID,
			CredentialID:           *principal.CredentialID,
			WorkerID:               *principal.WorkerID,
			AttachedCoreInstanceID: principal.CoreInstanceID,
		},
		node: db.LockRuntimeNodeForPrincipalValidationRow{
			NodeID:                    *principal.NodeID,
			DeviceCertificateSerial:   *principal.DeviceCertificateSerial,
			DevicePublicKeyThumbprint: *principal.DevicePublicKeyThumbprintSHA256,
		},
		credential: db.LockRuntimeCredentialForPrincipalValidationRow{
			ID:        *principal.CredentialID,
			AgentID:   &principal.AgentID,
			ExpiresAt: &expiresAt,
		},
	}

	locked, err := lockRuntimePrincipal(context.Background(), fake, principal)
	require.NoError(t, err)
	require.Equal(t, []string{"session", "node", "credential"}, fake.calls)
	require.True(t, locked.validAt(now))
	require.False(t, locked.validAt(expiresAt), "credential expiry is exclusive")
}

func TestLockRuntimePrincipalFailsClosedAndAllowsCoreShape(t *testing.T) {
	principal := runtimeNodePrincipalFixture()
	fake := &runtimePrincipalLockFake{sessionErr: pgx.ErrNoRows}
	_, err := lockRuntimePrincipal(context.Background(), fake, principal)
	require.True(t, IsRuntimeEventError(err, RuntimeEventErrorLeaseIdentityMismatch))
	require.Equal(t, []string{"session"}, fake.calls)

	core := RuntimeEventPrincipal{AgentID: uuid.New()}
	fake = &runtimePrincipalLockFake{}
	locked, err := lockRuntimePrincipal(context.Background(), fake, core)
	require.NoError(t, err)
	require.Empty(t, fake.calls)
	require.True(t, locked.validAt(time.Now()))

	partial := principal
	partial.CredentialID = nil
	_, err = lockRuntimePrincipal(context.Background(), fake, partial)
	require.ErrorIs(t, err, ErrInvalidRuntimeEvent)
}

func runtimeNodePrincipalFixture() RuntimeEventPrincipal {
	agentID := uuid.New()
	credentialID := uuid.New()
	nodeID := uuid.New()
	sessionID := uuid.New()
	coreID := uuid.New()
	workerID := "worker-a"
	serial := "abc123"
	thumbprint := repeatedHex("a")
	return RuntimeEventPrincipal{
		AgentID:                         agentID,
		CredentialID:                    &credentialID,
		NodeID:                          &nodeID,
		WorkerID:                        &workerID,
		RuntimeSessionID:                &sessionID,
		CoreInstanceID:                  &coreID,
		DeviceCertificateSerial:         &serial,
		DevicePublicKeyThumbprintSHA256: &thumbprint,
	}
}

type runtimePrincipalLockFake struct {
	calls         []string
	session       db.LockRuntimeSessionForPrincipalValidationRow
	node          db.LockRuntimeNodeForPrincipalValidationRow
	credential    db.LockRuntimeCredentialForPrincipalValidationRow
	sessionErr    error
	nodeErr       error
	credentialErr error
}

func (f *runtimePrincipalLockFake) LockRuntimeSessionForPrincipalValidation(context.Context, db.LockRuntimeSessionForPrincipalValidationParams) (db.LockRuntimeSessionForPrincipalValidationRow, error) {
	f.calls = append(f.calls, "session")
	return f.session, f.sessionErr
}

func (f *runtimePrincipalLockFake) LockRuntimeNodeForPrincipalValidation(context.Context, db.LockRuntimeNodeForPrincipalValidationParams) (db.LockRuntimeNodeForPrincipalValidationRow, error) {
	f.calls = append(f.calls, "node")
	return f.node, f.nodeErr
}

func (f *runtimePrincipalLockFake) LockRuntimeCredentialForPrincipalValidation(context.Context, db.LockRuntimeCredentialForPrincipalValidationParams) (db.LockRuntimeCredentialForPrincipalValidationRow, error) {
	f.calls = append(f.calls, "credential")
	return f.credential, f.credentialErr
}

var _ runtimePrincipalLockQueries = (*runtimePrincipalLockFake)(nil)
