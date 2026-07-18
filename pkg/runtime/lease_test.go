package runtime

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

func TestRuntimeLeaseClaimUsesGlobalLockAndCapacityOrder(t *testing.T) {
	fixture := newRuntimeLeaseFixture(t)
	assigned, err := fixture.service.ClaimOffer(context.Background(), fixture.principal)
	require.NoError(t, err)
	require.NotNil(t, assigned)
	require.Equal(t, []string{
		"cluster_gate", "lock_session", "lock_node", "lock_credential", "lock_attachment", "existing_offer", "lock_candidate",
		"claim_session_slot", "claim_node_slot", "create_offer", "mirror_offer",
	}, fixture.tx.calls)
	require.NotEqual(t, uuid.Nil, assigned.AttemptIdentity.AttemptID)
	require.NotEqual(t, uuid.Nil, assigned.AttemptIdentity.LeaseID)
	require.NotEqual(t, assigned.AttemptIdentity.AttemptID, assigned.AttemptIdentity.LeaseID)
	require.Equal(t, map[string]any{"prompt": "hello"}, assigned.Input)
	require.Equal(t, map[string]any{"trace": "a"}, assigned.Metadata)
	require.Equal(t, fixture.databaseNow.Add(-fixture.service.config.HeartbeatTTL), fixture.tx.sessionSlotParams.HeartbeatAfter)
	require.Equal(t, fixture.tx.sessionSlotParams.HeartbeatAfter, fixture.tx.nodeSlotParams.LastSeenAfter)
	require.Equal(t, fixture.principal.CoreInstanceID, fixture.tx.mirrorOfferParams.CoreInstanceID)
	require.Equal(t, int32(1), fixture.tx.sessionInflight)
	require.Equal(t, int32(1), fixture.tx.nodeInflight)
	require.NotNil(t, fixture.tx.createdAttempt.SlotAcquiredAt)
	require.Nil(t, fixture.tx.createdAttempt.SlotReleasedAt)
	require.Equal(t, fixture.principal.RuntimeSessionID, *fixture.tx.createdAttempt.ActiveRuntimeSessionID)

	canonical, err := CanonicalizeRFC8785(assigned.Input)
	require.NoError(t, err)
	require.Equal(t, sha256.Sum256(canonical), fixture.issuer.capability.InputSHA256)
	require.Equal(t, fixture.tx.createdAttempt.OfferedAt, fixture.issuer.capability.IssuedAt)
	require.Equal(t, fixture.tx.createdAttempt.AttemptDeadlineAt, fixture.issuer.capability.ExpiresAt)
}

func TestRuntimeLeaseRepeatedClaimReturnsIdenticalOfferWithoutCapacityMutation(t *testing.T) {
	fixture := newRuntimeLeaseFixture(t)
	fixture.tx.existing = existingOfferFixture(fixture)
	fixture.tx.candidateErr = errors.New("candidate must not be read")

	first, err := fixture.service.ClaimOffer(context.Background(), fixture.principal)
	require.NoError(t, err)
	second, err := fixture.service.ClaimOffer(context.Background(), fixture.principal)
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Equal(t, 2, fixture.issuer.calls)
	require.Zero(t, fixture.tx.sessionInflight)
	require.Zero(t, fixture.tx.nodeInflight)
	require.NotContains(t, fixture.tx.calls, "claim_session_slot")
	require.NotContains(t, fixture.tx.calls, "create_offer")
}

func TestRuntimeLeaseExpiredOfferIsReleasedBeforeNextCandidate(t *testing.T) {
	fixture := newRuntimeLeaseFixture(t)
	fixture.tx.existing = existingOfferFixture(fixture)
	fixture.tx.existing.OfferExpiresAt = fixture.databaseNow
	fixture.tx.sessionInflight = 1
	fixture.tx.nodeInflight = 1

	assigned, err := fixture.service.ClaimOffer(context.Background(), fixture.principal)
	require.NoError(t, err)
	require.NotNil(t, assigned)
	require.Equal(t, fixture.tx.candidate.ID, assigned.AttemptIdentity.RunID)
	require.Equal(t, int32(1), fixture.tx.sessionInflight, "old capacity is released before the replacement is reserved")
	require.Equal(t, int32(1), fixture.tx.nodeInflight)
	require.Equal(t, 1, fixture.tx.finishCalls)
	require.Equal(t, 1, fixture.tx.capacityReleaseCASCalls)
	require.Equal(t, 1, fixture.tx.resetCalls)
	require.Equal(t, 1, fixture.tx.signalCalls)
	require.Equal(t, []string{
		"cluster_gate", "lock_session", "lock_node", "lock_credential", "lock_attachment", "existing_offer",
		"finish_offer", "mark_capacity_released", "release_session_slot", "release_node_slot",
		"reset_run", "create_signal", "lock_candidate", "claim_session_slot", "claim_node_slot",
		"create_offer", "mirror_offer",
	}, fixture.tx.calls)
}

func TestRuntimeLeaseClaimCapacityFailureRollsBackSessionReservation(t *testing.T) {
	fixture := newRuntimeLeaseFixture(t)
	fixture.tx.nodeSlotErr = pgx.ErrNoRows

	assigned, err := fixture.service.ClaimOffer(context.Background(), fixture.principal)
	require.Nil(t, assigned)
	require.True(t, IsRuntimeLeaseError(err, RuntimeLeaseErrorNodeAtCapacity), err)
	require.Zero(t, fixture.tx.sessionInflight, "failed node reservation must roll back the session increment")
	require.Zero(t, fixture.tx.nodeInflight)
	require.False(t, fixture.repository.committed)
	require.Equal(t, []string{
		"cluster_gate", "lock_session", "lock_node", "lock_credential", "lock_attachment", "existing_offer", "lock_candidate",
		"claim_session_slot", "claim_node_slot",
	}, fixture.tx.calls)
}

func TestRuntimeLeaseHardMaintenanceRejectsClaimBeforePrincipalLocks(t *testing.T) {
	fixture := newRuntimeLeaseFixture(t)
	gateErr := errors.New("hard maintenance")
	fixture.tx.clusterGateErr = gateErr

	assigned, err := fixture.service.ClaimOffer(context.Background(), fixture.principal)
	require.Nil(t, assigned)
	require.ErrorIs(t, err, gateErr)
	require.Equal(t, RuntimeClusterClaim, fixture.tx.clusterGateOperation)
	require.Equal(t, []string{"cluster_gate"}, fixture.tx.calls)
	require.Zero(t, fixture.tx.sessionInflight)
	require.Zero(t, fixture.tx.nodeInflight)
}

func TestRuntimeLeaseOfferMirrorFailureRollsBackBothCapacityReservations(t *testing.T) {
	fixture := newRuntimeLeaseFixture(t)
	fixture.tx.mirrorOfferErr = pgx.ErrNoRows

	assigned, err := fixture.service.ClaimOffer(context.Background(), fixture.principal)
	require.Nil(t, assigned)
	require.True(t, IsRuntimeLeaseError(err, RuntimeLeaseErrorStaleLease), err)
	require.Zero(t, fixture.tx.sessionInflight)
	require.Zero(t, fixture.tx.nodeInflight)
	require.False(t, fixture.repository.committed)
}

func TestRuntimeLeaseClaimRejectsNonObjectAssignmentJSON(t *testing.T) {
	fixture := newRuntimeLeaseFixture(t)
	fixture.tx.candidate.Input = []byte(`["not-an-object"]`)

	assigned, err := fixture.service.ClaimOffer(context.Background(), fixture.principal)
	require.Nil(t, assigned)
	require.True(t, IsRuntimeLeaseError(err, RuntimeLeaseErrorValidationFailed), err)
	require.Zero(t, fixture.tx.sessionInflight)
	require.Zero(t, fixture.tx.nodeInflight)
	require.False(t, fixture.repository.committed)
}

func TestRuntimeLeaseAckFirstConfirmationAndReplay(t *testing.T) {
	fixture := newRuntimeLeaseFixture(t)
	identity := fixture.activeIdentity()
	fixture.tx.run = offeredRunFixture(fixture, identity)
	fixture.tx.attempt = offeredAttemptFixture(fixture, identity)

	confirmed, err := fixture.service.AckAssignment(context.Background(), fixture.principal, RunAssignmentAckPayload{AttemptIdentity: identity})
	require.NoError(t, err)
	require.Equal(t, int64(1), confirmed.AttemptNo)
	require.Equal(t, fixture.databaseNow.Add(time.Minute), confirmed.LeaseExpiresAt)
	require.Equal(t, 1, fixture.tx.confirmCalls)
	require.Equal(t, fixture.principal.AttachmentID, fixture.tx.confirmParams.AttachmentID)
	require.Equal(t, 1, fixture.tx.mirrorConfirmCalls)
	require.Equal(t, []string{"lock_session", "lock_node", "lock_credential", "lock_attachment", "lock_run", "lock_attempt", "confirm", "mirror_confirm"}, fixture.tx.calls)

	fixture.tx.calls = nil
	attemptNo := int32(1)
	acceptedAt := fixture.databaseNow
	fixture.tx.attempt.AttemptNo = &attemptNo
	fixture.tx.attempt.AcceptedAt = &acceptedAt
	fixture.tx.attempt.LeaseExpiresAt = confirmed.LeaseExpiresAt
	fixture.tx.run.DispatchState = string(RuntimeDispatchExecuting)
	fixture.tx.run.LeaseAcceptedAt = &acceptedAt
	fixture.tx.run.LeaseExpiresAt = &confirmed.LeaseExpiresAt

	replayed, err := fixture.service.AckAssignment(context.Background(), fixture.principal, RunAssignmentAckPayload{AttemptIdentity: identity})
	require.NoError(t, err)
	require.Equal(t, confirmed, replayed)
	require.Equal(t, 1, fixture.tx.confirmCalls, "accepted ACK replay must not increment attempt_count twice")
	require.Equal(t, 1, fixture.tx.mirrorConfirmCalls)
	require.Equal(t, []string{"lock_session", "lock_node", "lock_credential", "lock_attachment", "lock_run", "lock_attempt"}, fixture.tx.calls)
}

func TestRuntimeLeaseAckRejectsExpiredOfferUsingDatabaseClock(t *testing.T) {
	fixture := newRuntimeLeaseFixture(t)
	identity := fixture.activeIdentity()
	fixture.tx.run = offeredRunFixture(fixture, identity)
	fixture.tx.attempt = offeredAttemptFixture(fixture, identity)
	fixture.tx.attempt.OfferExpiresAt = fixture.databaseNow
	fixture.principal.DatabaseTime = fixture.databaseNow.Add(-24 * time.Hour)

	_, err := fixture.service.AckAssignment(context.Background(), fixture.principal, RunAssignmentAckPayload{AttemptIdentity: identity})
	require.True(t, IsRuntimeLeaseError(err, RuntimeLeaseErrorLeaseExpired), err)
	require.Zero(t, fixture.tx.confirmCalls)
}

func TestRuntimeLeaseRejectIsIdempotentAndReleasesCapacityOnce(t *testing.T) {
	fixture := newRuntimeLeaseFixture(t)
	identity := fixture.activeIdentity()
	fixture.tx.run = offeredRunFixture(fixture, identity)
	fixture.tx.attempt = offeredAttemptFixture(fixture, identity)
	fixture.tx.sessionInflight = 1
	fixture.tx.nodeInflight = 1
	request := RunAssignmentRejectPayload{
		AttemptIdentity: identity,
		ReasonCode:      RuntimeRejectNodeAtCapacity,
		Capacity:        1,
		Inflight:        1,
	}

	rejected, err := fixture.service.RejectAssignment(context.Background(), fixture.principal, request)
	require.NoError(t, err)
	require.Equal(t, RuntimeOfferRejected, rejected.Outcome)
	require.Equal(t, RuntimeDispatchPending, rejected.DispatchState)
	require.Zero(t, fixture.tx.sessionInflight)
	require.Zero(t, fixture.tx.nodeInflight)
	require.Equal(t, 1, fixture.tx.finishCalls)
	require.Equal(t, 1, fixture.tx.resetCalls)
	require.Equal(t, 1, fixture.tx.capacityReleaseCASCalls)
	require.Equal(t, 1, fixture.tx.signalCalls)
	require.Equal(t, []string{
		"lock_session", "lock_node", "lock_credential", "lock_attachment", "lock_run", "lock_attempt",
		"finish_offer", "mark_capacity_released", "release_session_slot", "release_node_slot",
		"reset_run", "create_signal",
	}, fixture.tx.calls)

	finishedAt := fixture.databaseNow
	outcome := "offer_rejected"
	fixture.tx.attempt.FinishedAt = &finishedAt
	fixture.tx.attempt.Outcome = &outcome
	fixture.tx.run.DispatchState = string(RuntimeDispatchPending)
	fixture.tx.run.ActiveAttemptID = nil
	fixture.tx.run.LeaseID = nil
	fixture.tx.calls = nil

	replayed, err := fixture.service.RejectAssignment(context.Background(), fixture.principal, request)
	require.NoError(t, err)
	require.Equal(t, rejected, replayed)
	require.Equal(t, 1, fixture.tx.finishCalls)
	require.Equal(t, 1, fixture.tx.resetCalls)
	require.Equal(t, 1, fixture.tx.capacityReleaseCASCalls)
	require.Equal(t, 1, fixture.tx.signalCalls)
	require.NotContains(t, fixture.tx.calls, "release_session_slot")
}

func TestRuntimeLeaseRenewRequiresLiveAcceptedExecutionAndMirrorsBoundedExpiry(t *testing.T) {
	fixture := newRuntimeLeaseFixture(t)
	identity := fixture.activeIdentity()
	fixture.tx.run = offeredRunFixture(fixture, identity)
	fixture.tx.run.DispatchState = string(RuntimeDispatchExecuting)
	fixture.tx.attempt = offeredAttemptFixture(fixture, identity)
	attemptNo := int32(1)
	acceptedAt := fixture.databaseNow.Add(-time.Second)
	fixture.tx.attempt.AttemptNo = &attemptNo
	fixture.tx.attempt.AcceptedAt = &acceptedAt
	fixture.tx.attempt.LeaseExpiresAt = fixture.databaseNow.Add(30 * time.Second)
	fixture.tx.run.LeaseExpiresAt = runtimeLeaseTimePointer(fixture.tx.attempt.LeaseExpiresAt)

	renewed, err := fixture.service.RenewLease(context.Background(), fixture.principal, RunLeaseRenewPayload{
		AttemptIdentity: identity,
		Capacity:        1,
		Inflight:        1,
	})
	require.NoError(t, err)
	require.Equal(t, fixture.databaseNow.Add(time.Minute), renewed.LeaseExpiresAt)
	require.Nil(t, renewed.PendingCommand)
	require.Equal(t, 1, fixture.tx.renewCalls)
	require.Equal(t, 1, fixture.tx.mirrorRenewCalls)

	fixture.tx.attempt.LeaseExpiresAt = fixture.databaseNow
	fixture.tx.run.LeaseExpiresAt = runtimeLeaseTimePointer(fixture.databaseNow)
	_, err = fixture.service.RenewLease(context.Background(), fixture.principal, RunLeaseRenewPayload{
		AttemptIdentity: identity,
		Capacity:        1,
		Inflight:        1,
	})
	require.True(t, IsRuntimeLeaseError(err, RuntimeLeaseErrorLeaseExpired), err)
	require.Equal(t, 1, fixture.tx.renewCalls)
}

func TestRuntimeLeaseSessionClosePreservesExecutingAttempt(t *testing.T) {
	fixture := newRuntimeLeaseFixture(t)
	fixture.tx.existingErr = pgx.ErrNoRows // The query deliberately excludes accepted Attempts.
	fixture.tx.sessionInflight = 1
	fixture.tx.nodeInflight = 1

	err := fixture.service.ReleaseUnackedOffer(context.Background(), fixture.principal, "SESSION_DISCONNECTED")
	require.NoError(t, err)
	require.Equal(t, int32(1), fixture.tx.sessionInflight)
	require.Equal(t, int32(1), fixture.tx.nodeInflight)
	require.Zero(t, fixture.tx.finishCalls)
	require.Zero(t, fixture.tx.resetCalls)
	require.Zero(t, fixture.tx.signalCalls)
}

func TestRuntimeLeaseSessionCloseReleasesOnlyUnacceptedOfferAfterOfflineCommit(t *testing.T) {
	fixture := newRuntimeLeaseFixture(t)
	fixture.tx.offerReleaseStatus = "offline"
	fixture.tx.existing = existingOfferFixture(fixture)
	fixture.tx.sessionInflight = 1
	fixture.tx.nodeInflight = 1

	err := fixture.service.ReleaseUnackedOffer(context.Background(), fixture.principal, "SESSION_DISCONNECTED")
	require.NoError(t, err)
	require.Zero(t, fixture.tx.sessionInflight)
	require.Zero(t, fixture.tx.nodeInflight)
	require.Equal(t, 1, fixture.tx.finishCalls)
	require.Equal(t, 1, fixture.tx.resetCalls)
	require.Equal(t, []string{
		"lock_offer_release_session", "lock_node", "lock_credential", "lock_offer_release_attachment", "existing_offer",
		"finish_offer", "mark_capacity_released", "release_session_slot", "release_node_slot",
		"reset_run", "create_signal",
	}, fixture.tx.calls)
}

func TestRuntimeLeaseOldCoreCannotReleaseAfterSessionReattach(t *testing.T) {
	fixture := newRuntimeLeaseFixture(t)
	fixture.tx.offerReleaseErr = pgx.ErrNoRows
	fixture.tx.existing = existingOfferFixture(fixture)
	fixture.tx.sessionInflight = 1
	fixture.tx.nodeInflight = 1

	err := fixture.service.ReleaseUnackedOffer(context.Background(), fixture.principal, "SESSION_DISCONNECTED")
	require.True(t, IsRuntimeLeaseError(err, RuntimeLeaseErrorIdentityMismatch), err)
	require.Equal(t, int32(1), fixture.tx.sessionInflight)
	require.Equal(t, int32(1), fixture.tx.nodeInflight)
	require.Zero(t, fixture.tx.finishCalls)
	require.Equal(t, []string{"lock_offer_release_session"}, fixture.tx.calls)
}

func TestRuntimeLeaseCapacityCASLoserNeverDecrementsCounters(t *testing.T) {
	fixture := newRuntimeLeaseFixture(t)
	fixture.tx.existing = existingOfferFixture(fixture)
	fixture.tx.sessionInflight = 1
	fixture.tx.nodeInflight = 1
	fixture.tx.capacityReleaseCASErr = pgx.ErrNoRows

	err := fixture.service.ReleaseUnackedOffer(context.Background(), fixture.principal, "ACK_TIMEOUT")
	require.NoError(t, err)
	require.Equal(t, int32(1), fixture.tx.sessionInflight)
	require.Equal(t, int32(1), fixture.tx.nodeInflight)
	require.Equal(t, 1, fixture.tx.capacityReleaseCASCalls)
	require.Zero(t, fixture.tx.resetCalls)
	require.Zero(t, fixture.tx.signalCalls)
	require.NotContains(t, fixture.tx.calls, "release_session_slot")
	require.NotContains(t, fixture.tx.calls, "release_node_slot")
}

type runtimeLeaseFixture struct {
	databaseNow time.Time
	principal   RuntimeSessionPrincipal
	tx          *runtimeLeaseTransactionFake
	repository  *runtimeLeaseRepositoryFake
	issuer      *runtimeLeaseIssuerFake
	service     *RuntimeLeaseService
}

func newRuntimeLeaseFixture(t *testing.T) *runtimeLeaseFixture {
	t.Helper()
	databaseNow := time.Date(2026, 7, 11, 12, 0, 0, 123456000, time.UTC)
	coreID := uuid.New()
	principal := RuntimeSessionPrincipal{
		RuntimeSessionID:                uuid.New(),
		NodeID:                          uuid.New(),
		AgentID:                         uuid.New(),
		CredentialID:                    uuid.New(),
		WorkerID:                        "worker-a",
		SessionEpoch:                    7,
		RuntimeContractDigest:           RuntimeContractDigest,
		CoreInstanceID:                  coreID,
		AttachmentID:                    uuid.New(),
		DeviceCertificateSerial:         "abc123",
		DevicePublicKeyThumbprintSHA256: fmt.Sprintf("%064x", 42),
		Status:                          "active",
		DatabaseTime:                    databaseNow,
	}
	tx := &runtimeLeaseTransactionFake{
		principal:   principal,
		databaseNow: databaseNow,
		existingErr: pgx.ErrNoRows,
		candidate: db.LockNextClaimableRuntimeRunForAgentRow{
			ID: uuid.New(), AgentID: principal.AgentID,
			Input: []byte(`{"prompt":"hello"}`), RequestMetadata: []byte(`{"trace":"a"}`),
			DispatchState: string(RuntimeDispatchPending), OfferCount: 0, MaxOfferCount: 5,
			AttemptCount: 0, MaxAttempts: 3, DispatchDeadlineAt: runtimeLeaseTimePointer(databaseNow.Add(10 * time.Minute)),
			RunDeadlineAt: runtimeLeaseTimePointer(databaseNow.Add(time.Hour)), DatabaseNow: databaseNow,
		},
	}
	repository := &runtimeLeaseRepositoryFake{tx: tx}
	issuer := &runtimeLeaseIssuerFake{}
	service := newRuntimeLeaseService(repository, coreID, issuer, RuntimeLeaseConfig{})
	return &runtimeLeaseFixture{
		databaseNow: databaseNow, principal: principal, tx: tx, repository: repository, issuer: issuer, service: service,
	}
}

func (f *runtimeLeaseFixture) activeIdentity() AttemptIdentity {
	return AttemptIdentity{
		RunID: uuid.New(), AttemptID: uuid.New(), LeaseID: uuid.New(), FencingToken: 11,
		NodeID: f.principal.NodeID, AgentID: f.principal.AgentID, WorkerID: f.principal.WorkerID,
		RuntimeSessionID: f.principal.RuntimeSessionID,
	}
}

func existingOfferFixture(f *runtimeLeaseFixture) *db.GetExistingUnacceptedRunOfferForSessionRow {
	workerID, sessionID, nodeID, credentialID := f.principal.WorkerID, f.principal.RuntimeSessionID, f.principal.NodeID, f.principal.CredentialID
	return &db.GetExistingUnacceptedRunOfferForSessionRow{
		AttemptID: uuid.New(), RunID: uuid.New(), AgentID: f.principal.AgentID, OfferNo: 2,
		LeaseID: uuid.New(), FencingToken: 4, RuntimeTokenID: &credentialID, RuntimeWorkerID: &workerID,
		RuntimeSessionID: &sessionID, NodeID: &nodeID, OfferedByCoreInstanceID: f.principal.CoreInstanceID,
		AttachedCoreInstanceID: f.principal.CoreInstanceID, OfferedAt: f.databaseNow.Add(-time.Second),
		OfferExpiresAt: f.databaseNow.Add(29 * time.Second), LeaseExpiresAt: f.databaseNow.Add(time.Minute),
		AttemptDeadlineAt: f.databaseNow.Add(30 * time.Minute), Input: []byte(`{"prompt":"hello"}`),
		RequestMetadata: []byte(`{"trace":"a"}`), DispatchState: string(RuntimeDispatchOffered),
		RunDeadlineAt: runtimeLeaseTimePointer(f.databaseNow.Add(time.Hour)), DatabaseNow: f.databaseNow,
	}
}

func offeredRunFixture(f *runtimeLeaseFixture, identity AttemptIdentity) db.LockRunForLeaseMutationRow {
	credentialID := f.principal.CredentialID
	acceptedDeadline := f.databaseNow.Add(30 * time.Minute)
	leaseExpiry := f.databaseNow.Add(time.Minute)
	return db.LockRunForLeaseMutationRow{
		ID: identity.RunID, AgentID: f.principal.AgentID, Status: string(RuntimeRunRunning),
		DispatchState: string(RuntimeDispatchOffered), ActiveAttemptID: &identity.AttemptID,
		LeaseID: &identity.LeaseID, FencingToken: identity.FencingToken, RuntimeNodeID: &identity.NodeID,
		RuntimeWorkerID: &identity.WorkerID, RuntimeSessionID: &identity.RuntimeSessionID,
		LeaseTokenID: &credentialID, LeaseExpiresAt: &leaseExpiry, AttemptDeadlineAt: &acceptedDeadline,
		RunDeadlineAt: runtimeLeaseTimePointer(f.databaseNow.Add(time.Hour)), DatabaseNow: f.databaseNow,
	}
}

func offeredAttemptFixture(f *runtimeLeaseFixture, identity AttemptIdentity) db.RunAttempt {
	credentialID := f.principal.CredentialID
	return db.RunAttempt{
		ID: identity.AttemptID, RunID: identity.RunID, AgentID: identity.AgentID, OfferNo: 1,
		ExecutorType: "runtime", LeaseID: identity.LeaseID, FencingToken: identity.FencingToken,
		RuntimeTokenID: &credentialID, RuntimeWorkerID: &identity.WorkerID, RuntimeSessionID: &identity.RuntimeSessionID,
		NodeID: &identity.NodeID, OfferedByCoreInstanceID: f.principal.CoreInstanceID,
		AttachedCoreInstanceID: f.principal.CoreInstanceID, OfferedAt: f.databaseNow.Add(-time.Second),
		OfferExpiresAt: f.databaseNow.Add(29 * time.Second), LeaseExpiresAt: f.databaseNow.Add(time.Minute),
		AttemptDeadlineAt: f.databaseNow.Add(30 * time.Minute),
	}
}

func runtimeLeaseTimePointer(value time.Time) *time.Time { return &value }

type runtimeLeaseIssuerFake struct {
	calls      int
	capability RuntimeInvocationCapability
}

func (i *runtimeLeaseIssuerFake) Issue(capability RuntimeInvocationCapability) (string, string, error) {
	i.calls++
	i.capability = capability
	return "node:" + capability.AttemptID.String(), "token:" + capability.AttemptID.String(), nil
}

type runtimeLeaseRepositoryFake struct {
	tx        *runtimeLeaseTransactionFake
	committed bool
}

func (r *runtimeLeaseRepositoryFake) WithTransaction(_ context.Context, fn func(runtimeLeaseTransaction) error) error {
	sessionInflight, nodeInflight := r.tx.sessionInflight, r.tx.nodeInflight
	err := fn(r.tx)
	if err != nil {
		r.tx.sessionInflight = sessionInflight
		r.tx.nodeInflight = nodeInflight
		r.committed = false
		return err
	}
	r.committed = true
	return nil
}

type runtimeLeaseTransactionFake struct {
	runtimeLeaseTransaction
	principal            RuntimeSessionPrincipal
	databaseNow          time.Time
	calls                []string
	clusterGateErr       error
	clusterGateOperation RuntimeClusterOperation

	existing     *db.GetExistingUnacceptedRunOfferForSessionRow
	existingErr  error
	candidate    db.LockNextClaimableRuntimeRunForAgentRow
	candidateErr error

	sessionSlotParams db.ClaimRuntimeSessionSlotParams
	nodeSlotParams    db.ClaimRuntimeNodeSlotParams
	sessionInflight   int32
	nodeInflight      int32
	nodeSlotErr       error

	createdAttempt    db.RunAttempt
	mirrorOfferParams db.MirrorRuntimeRunOfferParams
	mirrorOfferErr    error
	run               db.LockRunForLeaseMutationRow
	attempt           db.RunAttempt

	confirmCalls            int
	confirmParams           db.ConfirmRunAssignmentParams
	mirrorConfirmCalls      int
	renewCalls              int
	mirrorRenewCalls        int
	finishCalls             int
	resetCalls              int
	capacityReleaseCASCalls int
	capacityReleaseCASErr   error
	signalCalls             int
	offerReleaseStatus      string
	offerReleaseErr         error
}

func (f *runtimeLeaseTransactionFake) call(name string) { f.calls = append(f.calls, name) }

func (f *runtimeLeaseTransactionFake) RequireRuntimeClusterOperation(_ context.Context, operation RuntimeClusterOperation) error {
	f.call("cluster_gate")
	f.clusterGateOperation = operation
	return f.clusterGateErr
}

func (f *runtimeLeaseTransactionFake) LockRuntimeSessionForPrincipalValidation(_ context.Context, _ db.LockRuntimeSessionForPrincipalValidationParams) (db.LockRuntimeSessionForPrincipalValidationRow, error) {
	f.call("lock_session")
	coreID := f.principal.CoreInstanceID
	return db.LockRuntimeSessionForPrincipalValidationRow{
		RuntimeSessionID: f.principal.RuntimeSessionID, NodeID: f.principal.NodeID, AgentID: f.principal.AgentID,
		CredentialID: f.principal.CredentialID, WorkerID: f.principal.WorkerID, SessionEpoch: f.principal.SessionEpoch,
		RuntimeContractDigest:   f.principal.RuntimeContractDigest,
		DeviceCertificateSerial: f.principal.DeviceCertificateSerial, Status: "active", AttachedCoreInstanceID: &coreID,
		HeartbeatAt: f.databaseNow, DatabaseNow: f.databaseNow,
	}, nil
}

func (f *runtimeLeaseTransactionFake) LockRuntimeSessionForOfferRelease(_ context.Context, _ db.LockRuntimeSessionForOfferReleaseParams) (db.LockRuntimeSessionForOfferReleaseRow, error) {
	f.call("lock_offer_release_session")
	if f.offerReleaseErr != nil {
		return db.LockRuntimeSessionForOfferReleaseRow{}, f.offerReleaseErr
	}
	status := f.offerReleaseStatus
	if status == "" {
		status = "active"
	}
	coreID := f.principal.CoreInstanceID
	attachedCoreID := &coreID
	if status == "offline" || status == "closed" {
		attachedCoreID = nil
	}
	return db.LockRuntimeSessionForOfferReleaseRow{
		RuntimeSessionID: f.principal.RuntimeSessionID,
		NodeID:           f.principal.NodeID, AgentID: f.principal.AgentID, CredentialID: f.principal.CredentialID,
		WorkerID: f.principal.WorkerID, SessionEpoch: f.principal.SessionEpoch,
		DeviceCertificateSerial: f.principal.DeviceCertificateSerial, Status: status,
		AttachedCoreInstanceID: attachedCoreID, HeartbeatAt: f.databaseNow, DatabaseNow: f.databaseNow,
	}, nil
}

func (f *runtimeLeaseTransactionFake) LockRuntimeSessionAttachmentForPrincipalValidation(_ context.Context, params db.LockRuntimeSessionAttachmentForPrincipalValidationParams) (db.RuntimeSessionAttachment, error) {
	f.call("lock_attachment")
	return db.RuntimeSessionAttachment{ID: params.AttachmentID, RuntimeSessionID: params.RuntimeSessionID, CoreInstanceID: params.CoreInstanceID}, nil
}

func (f *runtimeLeaseTransactionFake) LockRuntimeSessionAttachmentForOfferRelease(_ context.Context, params db.LockRuntimeSessionAttachmentForOfferReleaseParams) (db.RuntimeSessionAttachment, error) {
	f.call("lock_offer_release_attachment")
	attachment := db.RuntimeSessionAttachment{ID: params.AttachmentID, RuntimeSessionID: params.RuntimeSessionID, CoreInstanceID: params.CoreInstanceID}
	if params.Detached {
		detachedAt := f.databaseNow
		attachment.DetachedAt = &detachedAt
	}
	return attachment, nil
}

func (f *runtimeLeaseTransactionFake) LockRuntimeNodeForPrincipalValidation(_ context.Context, _ db.LockRuntimeNodeForPrincipalValidationParams) (db.LockRuntimeNodeForPrincipalValidationRow, error) {
	f.call("lock_node")
	return db.LockRuntimeNodeForPrincipalValidationRow{
		NodeID: f.principal.NodeID, DeviceCertificateSerial: f.principal.DeviceCertificateSerial,
		DevicePublicKeyThumbprint: f.principal.DevicePublicKeyThumbprintSHA256,
		RuntimeContractDigest:     f.principal.RuntimeContractDigest, Status: "active", DatabaseNow: f.databaseNow,
	}, nil
}

func (f *runtimeLeaseTransactionFake) LockRuntimeCredentialForPrincipalValidation(_ context.Context, _ db.LockRuntimeCredentialForPrincipalValidationParams) (db.LockRuntimeCredentialForPrincipalValidationRow, error) {
	f.call("lock_credential")
	agentID := f.principal.AgentID
	return db.LockRuntimeCredentialForPrincipalValidationRow{
		ID: f.principal.CredentialID, AgentID: &agentID, Status: "active_runtime", DatabaseNow: f.databaseNow,
	}, nil
}

func (f *runtimeLeaseTransactionFake) GetExistingUnacceptedRunOfferForSession(_ context.Context, _ db.GetExistingUnacceptedRunOfferForSessionParams) (db.GetExistingUnacceptedRunOfferForSessionRow, error) {
	f.call("existing_offer")
	if f.existing != nil {
		return *f.existing, nil
	}
	return db.GetExistingUnacceptedRunOfferForSessionRow{}, f.existingErr
}

func (f *runtimeLeaseTransactionFake) LockNextClaimableRuntimeRunForAgent(_ context.Context, _ uuid.UUID) (db.LockNextClaimableRuntimeRunForAgentRow, error) {
	f.call("lock_candidate")
	return f.candidate, f.candidateErr
}

func (f *runtimeLeaseTransactionFake) ClaimRuntimeSessionSlot(_ context.Context, params db.ClaimRuntimeSessionSlotParams) (db.RuntimeSession, error) {
	f.call("claim_session_slot")
	f.sessionSlotParams = params
	f.sessionInflight++
	return db.RuntimeSession{RuntimeSessionID: f.principal.RuntimeSessionID, Inflight: f.sessionInflight}, nil
}

func (f *runtimeLeaseTransactionFake) ClaimRuntimeNodeSlot(_ context.Context, params db.ClaimRuntimeNodeSlotParams) (db.RuntimeNode, error) {
	f.call("claim_node_slot")
	f.nodeSlotParams = params
	if f.nodeSlotErr != nil {
		return db.RuntimeNode{}, f.nodeSlotErr
	}
	f.nodeInflight++
	return db.RuntimeNode{NodeID: f.principal.NodeID, Inflight: f.nodeInflight}, nil
}

func (f *runtimeLeaseTransactionFake) CreateRuntimeRunOffer(_ context.Context, params db.CreateRuntimeRunOfferParams) (db.RunAttempt, error) {
	f.call("create_offer")
	workerID, sessionID, nodeID, credentialID := f.principal.WorkerID, f.principal.RuntimeSessionID, f.principal.NodeID, f.principal.CredentialID
	f.createdAttempt = db.RunAttempt{
		ID: params.AttemptID, RunID: params.RunID, AgentID: f.principal.AgentID, OfferNo: f.candidate.OfferCount + 1,
		ExecutorType: "runtime", LeaseID: params.LeaseID, FencingToken: f.candidate.FencingToken + 1,
		RuntimeTokenID: &credentialID, RuntimeWorkerID: &workerID, RuntimeSessionID: &sessionID, NodeID: &nodeID,
		OfferedByCoreInstanceID: params.CoreInstanceID, AttachedCoreInstanceID: params.CoreInstanceID,
		OfferedAt: f.databaseNow, OfferExpiresAt: f.databaseNow.Add(time.Duration(params.OfferTtlMs) * time.Millisecond),
		LeaseExpiresAt:         f.databaseNow.Add(time.Duration(params.LeaseTtlMs) * time.Millisecond),
		AttemptDeadlineAt:      f.databaseNow.Add(time.Duration(params.AttemptTtlMs) * time.Millisecond),
		SlotAcquiredAt:         &f.databaseNow,
		ActiveRuntimeSessionID: &sessionID,
	}
	return f.createdAttempt, nil
}

func (f *runtimeLeaseTransactionFake) MirrorRuntimeRunOffer(_ context.Context, params db.MirrorRuntimeRunOfferParams) (db.MirrorRuntimeRunOfferRow, error) {
	f.call("mirror_offer")
	f.mirrorOfferParams = params
	if f.mirrorOfferErr != nil {
		return db.MirrorRuntimeRunOfferRow{}, f.mirrorOfferErr
	}
	return db.MirrorRuntimeRunOfferRow{
		ID: f.createdAttempt.RunID, DispatchState: string(RuntimeDispatchOffered),
		ActiveAttemptID: &f.createdAttempt.ID, LeaseID: &f.createdAttempt.LeaseID, FencingToken: f.createdAttempt.FencingToken,
	}, nil
}

func (f *runtimeLeaseTransactionFake) LockRunForLeaseMutation(_ context.Context, _ uuid.UUID) (db.LockRunForLeaseMutationRow, error) {
	f.call("lock_run")
	return f.run, nil
}

func (f *runtimeLeaseTransactionFake) LockRuntimeRunAttemptForLeaseMutation(_ context.Context, _ db.LockRuntimeRunAttemptForLeaseMutationParams) (db.RunAttempt, error) {
	f.call("lock_attempt")
	return f.attempt, nil
}

func (f *runtimeLeaseTransactionFake) ConfirmRunAssignment(_ context.Context, params db.ConfirmRunAssignmentParams) (db.RunAttempt, error) {
	f.call("confirm")
	f.confirmCalls++
	f.confirmParams = params
	attempt := f.attempt
	attemptNo := int32(1)
	acceptedAt := f.databaseNow
	attempt.AttemptNo = &attemptNo
	attempt.AcceptedAt = &acceptedAt
	attempt.LeaseExpiresAt = f.databaseNow.Add(time.Minute)
	attempt.RuntimeAttachmentID = &params.AttachmentID
	return attempt, nil
}

func (f *runtimeLeaseTransactionFake) MirrorRunConfirmedAssignment(_ context.Context, _ db.MirrorRunConfirmedAssignmentParams) (db.MirrorRunConfirmedAssignmentRow, error) {
	f.call("mirror_confirm")
	f.mirrorConfirmCalls++
	return db.MirrorRunConfirmedAssignmentRow{ID: f.run.ID, DispatchState: string(RuntimeDispatchExecuting), AttemptCount: 1}, nil
}

func (f *runtimeLeaseTransactionFake) RenewRuntimeRunAttempt(_ context.Context, _ db.RenewRuntimeRunAttemptParams) (db.RunAttempt, error) {
	f.call("renew")
	f.renewCalls++
	attempt := f.attempt
	attempt.LeaseExpiresAt = f.databaseNow.Add(time.Minute)
	return attempt, nil
}

func (f *runtimeLeaseTransactionFake) MirrorRunLeaseRenewal(_ context.Context, _ db.MirrorRunLeaseRenewalParams) (db.MirrorRunLeaseRenewalRow, error) {
	f.call("mirror_renew")
	f.mirrorRenewCalls++
	expiresAt := f.databaseNow.Add(time.Minute)
	return db.MirrorRunLeaseRenewalRow{ID: f.run.ID, DispatchState: string(RuntimeDispatchExecuting), LeaseExpiresAt: &expiresAt}, nil
}

func (f *runtimeLeaseTransactionFake) FinishUnacceptedRunOffer(_ context.Context, params db.FinishUnacceptedRunOfferParams) (db.RunAttempt, error) {
	f.call("finish_offer")
	f.finishCalls++
	attempt := f.attempt
	if attempt.ID == uuid.Nil && f.existing != nil {
		attempt.ID, attempt.RunID, attempt.LeaseID, attempt.FencingToken = f.existing.AttemptID, f.existing.RunID, f.existing.LeaseID, f.existing.FencingToken
	}
	attempt.FinishedAt = runtimeLeaseTimePointer(f.databaseNow)
	attempt.Outcome = params.Outcome
	return attempt, nil
}

func (f *runtimeLeaseTransactionFake) ResetRunAfterUnacceptedOffer(_ context.Context, _ db.ResetRunAfterUnacceptedOfferParams) (db.ResetRunAfterUnacceptedOfferRow, error) {
	f.call("reset_run")
	f.resetCalls++
	return db.ResetRunAfterUnacceptedOfferRow{ID: f.run.ID, DispatchState: string(RuntimeDispatchPending)}, nil
}

func (f *runtimeLeaseTransactionFake) MarkRunAttemptCapacityReleased(_ context.Context, _ db.MarkRunAttemptCapacityReleasedParams) (db.MarkRunAttemptCapacityReleasedRow, error) {
	f.call("mark_capacity_released")
	f.capacityReleaseCASCalls++
	if f.capacityReleaseCASErr != nil {
		return db.MarkRunAttemptCapacityReleasedRow{}, f.capacityReleaseCASErr
	}
	sessionID, nodeID := f.principal.RuntimeSessionID, f.principal.NodeID
	return db.MarkRunAttemptCapacityReleasedRow{
		RuntimeSessionID: sessionID,
		NodeID:           nodeID,
		SlotAcquiredAt:   f.databaseNow.Add(-time.Second),
		SlotReleasedAt:   f.databaseNow,
	}, nil
}

func (f *runtimeLeaseTransactionFake) ReleaseRuntimeSessionSlot(_ context.Context, _ uuid.UUID) (db.RuntimeSession, error) {
	f.call("release_session_slot")
	if f.sessionInflight > 0 {
		f.sessionInflight--
	}
	return db.RuntimeSession{Inflight: f.sessionInflight}, nil
}

func (f *runtimeLeaseTransactionFake) ReleaseRuntimeNodeSlot(_ context.Context, _ uuid.UUID) (db.RuntimeNode, error) {
	f.call("release_node_slot")
	if f.nodeInflight > 0 {
		f.nodeInflight--
	}
	return db.RuntimeNode{Inflight: f.nodeInflight}, nil
}

func (f *runtimeLeaseTransactionFake) CreateRuntimeSignal(_ context.Context, _ db.CreateRuntimeSignalParams) (db.RuntimeSignalOutbox, error) {
	f.call("create_signal")
	f.signalCalls++
	return db.RuntimeSignalOutbox{ID: uuid.New()}, nil
}
