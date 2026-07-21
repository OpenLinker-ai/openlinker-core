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

func TestRuntimeCancellationOwnerPendingRunFinalizesAtomically(t *testing.T) {
	fixture := newRuntimeCancellationFixture(t)
	fixture.tx.lockedRun.DispatchState = string(RuntimeDispatchPending)
	fixture.tx.lockedRun.ActiveAttemptID = nil
	fixture.tx.lockedRun.LatestAttemptID = nil

	result, err := fixture.coordinator.CancelOwnedRun(context.Background(), fixture.ownerID, fixture.runID, "Owner stopped the run")
	require.NoError(t, err)
	require.False(t, result.Replayed)
	require.Equal(t, string(RuntimeCancelStopped), result.Cancellation.State)
	require.Equal(t, string(RuntimeRunCanceled), result.Run.Status)
	require.Equal(t, []string{"lock_run", "create_cancellation", "advance_cancellation", "persist_terminal"}, fixture.tx.calls)
	require.Equal(t, 1, fixture.tx.persistCalls)
	require.Equal(t, 0, fixture.tx.finishCalls)
}

func TestRuntimeCancellationOwnerActiveAgentKeepsAttemptAndCapacity(t *testing.T) {
	fixture := newRuntimeCancellationFixture(t)

	result, err := fixture.coordinator.CancelOwnedRun(context.Background(), fixture.ownerID, fixture.runID, "Stop")
	require.NoError(t, err)
	require.Equal(t, string(RuntimeCancelRequested), result.Cancellation.State)
	require.Equal(t, fixture.attempt.ID, *result.Cancellation.TargetAttemptID)
	require.Nil(t, fixture.tx.attempt.FinishedAt)
	require.Equal(t, int32(1), fixture.tx.sessionInflight)
	require.Equal(t, int32(1), fixture.tx.nodeInflight)
	require.Equal(t, []string{"lock_run", "lock_attempt", "create_cancellation", "persist_terminal"}, fixture.tx.calls)
}

func TestRuntimeCancellationOwnerReplayDoesNotDuplicateTerminalFacts(t *testing.T) {
	fixture := newRuntimeCancellationFixture(t)
	fixture.tx.lockedRun.Status = string(RuntimeRunCanceled)
	fixture.tx.lockedRun.DispatchState = string(RuntimeDispatchTerminal)
	fixture.tx.lockedRun.CancelRequestID = &fixture.cancellation.ID
	fixture.tx.lockedRun.CancelState = &fixture.cancellation.State
	fixture.tx.cancellation = fixture.cancellation
	fixture.tx.publicRun.Status = string(RuntimeRunCanceled)

	result, err := fixture.coordinator.CancelOwnedRun(context.Background(), fixture.ownerID, fixture.runID, "different replay reason")
	require.NoError(t, err)
	require.True(t, result.Replayed)
	require.Equal(t, fixture.cancellation.ID, result.Cancellation.ID)
	require.Zero(t, fixture.tx.persistCalls)
	require.Equal(t, []string{"lock_run", "get_cancellation", "lock_attempt", "lock_cancellation", "get_run"}, fixture.tx.calls)
}

func TestRuntimeCancellationPollUsesPrincipalRunAttemptCancellationLockOrder(t *testing.T) {
	fixture := newRuntimeCancellationFixture(t)
	fixture.tx.cancellation = fixture.cancellation

	response, err := fixture.coordinator.PollCommands(context.Background(), fixture.principal)
	require.NoError(t, err)
	require.Len(t, response.Commands, 1)
	require.Equal(t, fixture.databaseNow, response.DatabaseTime)
	require.Equal(t, []string{
		"lock_session", "lock_node", "lock_credential", "lock_attachment", "lock_command_run",
		"lock_attempt", "lock_cancellation", "advance_cancellation", "mirror_cancellation",
	}, fixture.tx.calls)

	decoded, err := DecodePendingCommand(response.Commands[0])
	require.NoError(t, err)
	require.NotNil(t, decoded.Cancel)
	require.Equal(t, fixture.cancellation.ID, decoded.Cancel.CancellationID)
	require.Equal(t, fixture.databaseNow.Add(defaultRuntimeCancellationDeadline), decoded.Cancel.DeadlineAt)
	require.Equal(t, string(RuntimeCancelDelivered), fixture.tx.cancellation.State)
}

func TestRuntimeCancellationRedeliveryTouchesOrderingWithoutLosingState(t *testing.T) {
	fixture := newRuntimeCancellationFixture(t)
	fixture.tx.cancellation = deliveredRuntimeCancellation(fixture)
	errorCode := "STOP_IN_PROGRESS"
	fixture.tx.cancellation.State = string(RuntimeCancelStopping)
	fixture.tx.cancellation.ErrorCode = &errorCode

	response, err := fixture.coordinator.PollCommands(context.Background(), fixture.principal)
	require.NoError(t, err)
	require.Len(t, response.Commands, 1)
	require.Equal(t, string(RuntimeCancelStopping), fixture.tx.cancellation.State)
	require.NotNil(t, fixture.tx.cancellation.ErrorCode)
	require.Equal(t, errorCode, *fixture.tx.cancellation.ErrorCode)
	require.Equal(t, []string{
		"lock_session", "lock_node", "lock_credential", "lock_attachment", "lock_command_run",
		"lock_attempt", "lock_cancellation", "advance_cancellation", "mirror_cancellation",
	}, fixture.tx.calls)
}

func TestRuntimeCancellationStoppingAckKeepsAttemptCapacity(t *testing.T) {
	fixture := newRuntimeCancellationFixture(t)
	fixture.tx.cancellation = deliveredRuntimeCancellation(fixture)
	request := fixture.cancelAck(RuntimeCancelStopping, "")

	state, err := fixture.coordinator.AckCancel(context.Background(), fixture.principal, request)
	require.NoError(t, err)
	require.Equal(t, RuntimeCancelStopping, state.CancelState)
	require.Zero(t, fixture.tx.finishCalls)
	require.Equal(t, int32(1), fixture.tx.sessionInflight)
	require.Equal(t, int32(1), fixture.tx.nodeInflight)
	require.Equal(t, []string{
		"lock_session", "lock_node", "lock_credential", "lock_attachment", "lock_run", "lock_attempt",
		"lock_cancellation", "advance_cancellation", "mirror_cancellation",
	}, fixture.tx.calls)
}

func TestRuntimeCancellationStoppedAckReleasesCapacityExactlyOnce(t *testing.T) {
	fixture := newRuntimeCancellationFixture(t)
	fixture.tx.cancellation = deliveredRuntimeCancellation(fixture)
	request := fixture.cancelAck(RuntimeCancelStopped, "")

	state, err := fixture.coordinator.AckCancel(context.Background(), fixture.principal, request)
	require.NoError(t, err)
	require.Equal(t, RuntimeCancelStopped, state.CancelState)
	require.Equal(t, int32(0), fixture.tx.sessionInflight)
	require.Equal(t, int32(0), fixture.tx.nodeInflight)
	require.Equal(t, 1, fixture.tx.finishCalls)
	require.Equal(t, 1, fixture.tx.capacityCASCalls)
	require.Equal(t, []string{
		"lock_session", "lock_node", "lock_credential", "lock_attachment", "lock_run", "lock_attempt",
		"lock_cancellation", "advance_cancellation", "finish_attempt", "capacity_cas",
		"release_session", "release_node", "create_capacity_signal", "mirror_cancellation",
	}, fixture.tx.calls)
	require.Len(t, fixture.tx.signals, 1)
	require.Equal(t, runtimeNodeCapacityAvailableSignal, fixture.tx.signals[0].EventType)
	require.Equal(t, fixture.principal.AgentID, fixture.tx.signals[0].AgentID)
	require.Equal(t, &fixture.tx.attempt.RunID, fixture.tx.signals[0].RunID)

	fixture.tx.calls = nil
	fixture.tx.cancellation.State = string(RuntimeCancelStopped)
	fixture.tx.cancellation.UpdatedAt = state.UpdatedAt
	finishedAt := fixture.databaseNow
	outcome := "canceled"
	fixture.tx.attempt.FinishedAt = &finishedAt
	fixture.tx.attempt.Outcome = &outcome
	fixture.tx.attempt.SlotReleasedAt = &finishedAt
	fixture.tx.attempt.ActiveRuntimeSessionID = nil

	replayed, err := fixture.coordinator.AckCancel(context.Background(), fixture.principal, request)
	require.NoError(t, err)
	require.Equal(t, state, replayed)
	require.Equal(t, 1, fixture.tx.finishCalls)
	require.Equal(t, 1, fixture.tx.capacityCASCalls)
	require.NotContains(t, fixture.tx.calls, "release_session")
}

func TestRuntimeCancellationWrongFenceFailsBeforeMutation(t *testing.T) {
	fixture := newRuntimeCancellationFixture(t)
	fixture.tx.cancellation = deliveredRuntimeCancellation(fixture)
	request := fixture.cancelAck(RuntimeCancelStopped, "")
	request.AttemptIdentity.FencingToken++

	_, err := fixture.coordinator.AckCancel(context.Background(), fixture.principal, request)
	require.True(t, IsRuntimeLeaseError(err, RuntimeLeaseErrorStaleLease), err)
	require.Zero(t, fixture.tx.finishCalls)
	require.Zero(t, fixture.tx.capacityCASCalls)
}

func TestRuntimeCancellationFailedAckRequiresStableErrorCode(t *testing.T) {
	fixture := newRuntimeCancellationFixture(t)
	fixture.tx.cancellation = deliveredRuntimeCancellation(fixture)

	_, err := fixture.coordinator.AckCancel(
		context.Background(), fixture.principal, fixture.cancelAck(RuntimeCancelFailed, ""),
	)
	require.True(t, IsRuntimeLeaseError(err, RuntimeLeaseErrorValidationFailed), err)
	require.Empty(t, fixture.tx.calls)
}

func TestRuntimeCancellationUnsupportedAckKeepsAttemptCapacity(t *testing.T) {
	fixture := newRuntimeCancellationFixture(t)
	fixture.tx.cancellation = deliveredRuntimeCancellation(fixture)

	state, err := fixture.coordinator.AckCancel(
		context.Background(), fixture.principal,
		fixture.cancelAck(RuntimeCancelUnsupported, "CANCEL_NOT_SUPPORTED"),
	)
	require.NoError(t, err)
	require.Equal(t, RuntimeCancelUnsupported, state.CancelState)
	require.Equal(t, "CANCEL_NOT_SUPPORTED", state.ErrorCode)
	require.Zero(t, fixture.tx.finishCalls)
	require.Zero(t, fixture.tx.capacityCASCalls)
	require.Equal(t, int32(1), fixture.tx.sessionInflight)
	require.Equal(t, int32(1), fixture.tx.nodeInflight)
}

func TestRuntimeCancellationDeadlineReaperMarksUnconfirmedAndReleasesOnce(t *testing.T) {
	fixture := newRuntimeCancellationFixture(t)
	fixture.tx.cancellation = deliveredRuntimeCancellation(fixture)
	fixture.tx.cancellation.RequestedAt = fixture.databaseNow.Add(-defaultRuntimeCancellationDeadline)

	state, err := fixture.coordinator.ReapExpiredCancellation(context.Background())
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, RuntimeCancelUnconfirmed, state.CancelState)
	require.Equal(t, runtimeCancellationUnconfirmedCode, state.ErrorCode)
	require.Equal(t, 1, fixture.tx.finishCalls)
	require.Equal(t, 1, fixture.tx.capacityCASCalls)
	require.Equal(t, int32(0), fixture.tx.sessionInflight)
	require.Equal(t, int32(0), fixture.tx.nodeInflight)
	require.Equal(t, []string{
		"find_due", "lock_reap_session", "lock_reap_node", "lock_due_run",
		"lock_attempt", "lock_cancellation", "advance_cancellation",
		"finish_attempt", "capacity_cas", "release_session", "release_node", "create_capacity_signal", "mirror_cancellation",
	}, fixture.tx.calls)
	require.Len(t, fixture.tx.signals, 1)

	fixture.tx.calls = nil
	replayed, err := fixture.coordinator.ReapExpiredCancellation(context.Background())
	require.NoError(t, err)
	require.Nil(t, replayed)
	require.Equal(t, 1, fixture.tx.finishCalls)
	require.Equal(t, 1, fixture.tx.capacityCASCalls)
	require.Equal(t, []string{"find_due"}, fixture.tx.calls)
}

func TestRuntimeCancellationDeadlineReaperPreservesNegativeTerminalEvidenceAndReleasesOnce(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		state     RuntimeCancelState
		errorCode string
	}{
		{name: "failed", state: RuntimeCancelFailed, errorCode: "ATTEMPT_IDENTITY_MISMATCH"},
		{name: "unsupported", state: RuntimeCancelUnsupported, errorCode: "CANCEL_NOT_SUPPORTED"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			fixture := newRuntimeCancellationFixture(t)
			fixture.tx.cancellation = deliveredRuntimeCancellation(fixture)
			fixture.tx.cancellation.State = string(testCase.state)
			fixture.tx.cancellation.ErrorCode = runtimeCancellationStringPointer(testCase.errorCode)
			fixture.tx.cancellation.AcknowledgedAt = runtimeCancellationTimePointer(fixture.databaseNow)
			fixture.tx.cancellation.RequestedAt = fixture.databaseNow.Add(-defaultRuntimeCancellationDeadline)
			originalUpdatedAt := fixture.tx.cancellation.UpdatedAt

			state, err := fixture.coordinator.ReapExpiredCancellation(context.Background())
			require.NoError(t, err)
			require.NotNil(t, state)
			require.Equal(t, testCase.state, state.CancelState)
			require.Equal(t, testCase.errorCode, state.ErrorCode)
			require.Equal(t, string(testCase.state), fixture.tx.cancellation.State)
			require.Equal(t, testCase.errorCode, *fixture.tx.cancellation.ErrorCode)
			require.Equal(t, originalUpdatedAt, fixture.tx.cancellation.UpdatedAt)
			require.Equal(t, runtimeCancellationUnconfirmedCode, fixture.tx.finishedErrorCode)
			require.Equal(t, 1, fixture.tx.finishCalls)
			require.Equal(t, 1, fixture.tx.capacityCASCalls)
			require.Equal(t, int32(0), fixture.tx.sessionInflight)
			require.Equal(t, int32(0), fixture.tx.nodeInflight)
			require.Equal(t, []string{
				"find_due", "lock_reap_session", "lock_reap_node", "lock_due_run",
				"lock_attempt", "lock_cancellation", "finish_attempt", "capacity_cas",
				"release_session", "release_node", "create_capacity_signal", "mirror_cancellation",
			}, fixture.tx.calls)
			require.Len(t, fixture.tx.signals, 1)

			fixture.tx.calls = nil
			replayed, err := fixture.coordinator.ReapExpiredCancellation(context.Background())
			require.NoError(t, err)
			require.Nil(t, replayed)
			require.Equal(t, 1, fixture.tx.finishCalls)
			require.Equal(t, 1, fixture.tx.capacityCASCalls)
			require.Equal(t, []string{"find_due"}, fixture.tx.calls)
		})
	}
}

func TestRuntimeCancellationNegativeTerminalStatesCannotTransition(t *testing.T) {
	require.False(t, runtimeCancellationTransitionAllowed(RuntimeCancelFailed, RuntimeCancelStopped))
	require.False(t, runtimeCancellationTransitionAllowed(RuntimeCancelUnsupported, RuntimeCancelStopped))
}

type runtimeCancellationFixture struct {
	databaseNow  time.Time
	ownerID      uuid.UUID
	runID        uuid.UUID
	principal    RuntimeSessionPrincipal
	attempt      db.RunAttempt
	cancellation db.RunCancellation
	tx           *runtimeCancellationTransactionFake
	repository   *runtimeCancellationRepositoryFake
	coordinator  *RuntimeCancellationCoordinator
}

func newRuntimeCancellationFixture(t *testing.T) *runtimeCancellationFixture {
	t.Helper()
	databaseNow := time.Date(2026, 7, 11, 14, 0, 0, 123000000, time.UTC)
	ownerID, runID, agentID := uuid.New(), uuid.New(), uuid.New()
	coreID, sessionID, nodeID, credentialID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	workerID := "worker-cancel"
	principal := RuntimeSessionPrincipal{
		RuntimeSessionID: sessionID, NodeID: nodeID, AgentID: agentID, CredentialID: credentialID,
		WorkerID: workerID, SessionEpoch: 5, RuntimeContractDigest: RuntimeContractDigest, CoreInstanceID: coreID,
		AttachmentID:            uuid.New(),
		DeviceCertificateSerial: "abc123", DevicePublicKeyThumbprintSHA256: runtimeCancellationSHA256Fixture(),
		Status: "active", DatabaseTime: databaseNow,
	}
	attemptID, leaseID := uuid.New(), uuid.New()
	attempt := db.RunAttempt{
		ID: attemptID, RunID: runID, AgentID: agentID, OfferNo: 1, AttemptNo: runtimeCancellationInt32Pointer(1),
		ExecutorType: "runtime", LeaseID: leaseID, FencingToken: 3,
		RuntimeTokenID: &credentialID, RuntimeWorkerID: &workerID, RuntimeSessionID: &sessionID, NodeID: &nodeID,
		OfferedByCoreInstanceID: coreID, AttachedCoreInstanceID: coreID,
		OfferedAt: databaseNow.Add(-time.Minute), OfferExpiresAt: databaseNow.Add(time.Minute),
		AcceptedAt:     runtimeCancellationTimePointer(databaseNow.Add(-50 * time.Second)),
		LeaseExpiresAt: databaseNow.Add(time.Minute), AttemptDeadlineAt: databaseNow.Add(10 * time.Minute),
		SlotAcquiredAt: runtimeCancellationTimePointer(databaseNow.Add(-time.Minute)), ActiveRuntimeSessionID: &sessionID,
	}
	cancellationID := deterministicRuntimeCancellationID(runID)
	cancellation := db.RunCancellation{
		ID: cancellationID, RunID: runID, TargetAttemptID: &attemptID,
		State: string(RuntimeCancelRequested), RequestedByType: "user", RequestedByID: ownerID,
		Reason: runtimeCancellationStringPointer("Stop"), RequestedAt: databaseNow,
		UpdatedAt: databaseNow,
	}
	lockedRun := db.LockRunForResultFinalizationRow{
		ID: runID, UserID: ownerID, AgentID: agentID, Status: string(RuntimeRunRunning),
		DispatchState: string(RuntimeDispatchExecuting), RuntimeContractID: "openlinker.runtime.v2",
		ConnectionModeSnapshot: runtimeCancellationStringPointer(connectionModeRuntime),
		AttemptCount:           1, MaxAttempts: 3, LatestAttemptID: &attemptID, ActiveAttemptID: &attemptID,
		LeaseID: &leaseID, FencingToken: 3, RuntimeNodeID: &nodeID, RuntimeWorkerID: &workerID,
		RuntimeSessionID: &sessionID, LeaseTokenID: &credentialID,
		RunDeadlineAt: runtimeCancellationTimePointer(databaseNow.Add(time.Hour)),
		StartedAt:     databaseNow.Add(-time.Minute), DatabaseNow: databaseNow,
	}
	tx := &runtimeCancellationTransactionFake{
		principal: principal, databaseNow: databaseNow, lockedRun: lockedRun,
		attempt: attempt, cancellation: cancellation, sessionInflight: 1, nodeInflight: 1,
		publicRun: db.Run{ID: runID, UserID: ownerID, AgentID: agentID, Status: string(RuntimeRunCanceled), StartedAt: lockedRun.StartedAt},
	}
	repository := &runtimeCancellationRepositoryFake{tx: tx}
	return &runtimeCancellationFixture{
		databaseNow: databaseNow, ownerID: ownerID, runID: runID, principal: principal,
		attempt: attempt, cancellation: cancellation, tx: tx, repository: repository,
		coordinator: newRuntimeCancellationCoordinator(repository, defaultRuntimeCancellationDeadline),
	}
}

func (f *runtimeCancellationFixture) cancelAck(state RuntimeCancelState, errorCode string) RunCancelAckPayload {
	identity, err := attemptIdentityFromRow(f.attempt)
	if err != nil {
		panic(err)
	}
	return RunCancelAckPayload{
		CancellationID: f.cancellation.ID, AttemptIdentity: identity,
		CancelState: state, ErrorCode: errorCode,
	}
}

func deliveredRuntimeCancellation(f *runtimeCancellationFixture) db.RunCancellation {
	cancellation := f.cancellation
	cancellation.State = string(RuntimeCancelDelivered)
	cancellation.DeliveredAt = runtimeCancellationTimePointer(f.databaseNow)
	f.tx.lockedRun.Status = string(RuntimeRunCanceled)
	f.tx.lockedRun.DispatchState = string(RuntimeDispatchTerminal)
	f.tx.lockedRun.ActiveAttemptID = nil
	f.tx.lockedRun.LeaseID = nil
	f.tx.lockedRun.FencingToken = 0
	f.tx.lockedRun.RuntimeNodeID = nil
	f.tx.lockedRun.RuntimeWorkerID = nil
	f.tx.lockedRun.RuntimeSessionID = nil
	f.tx.lockedRun.LeaseTokenID = nil
	f.tx.lockedRun.CancelRequestID = &cancellation.ID
	f.tx.lockedRun.CancelState = &cancellation.State
	return cancellation
}

func runtimeCancellationSHA256Fixture() string {
	return "000000000000000000000000000000000000000000000000000000000000002a"
}

func runtimeCancellationTimePointer(value time.Time) *time.Time { return &value }
func runtimeCancellationStringPointer(value string) *string     { return &value }
func runtimeCancellationInt32Pointer(value int32) *int32        { return &value }

type runtimeCancellationRepositoryFake struct {
	tx        *runtimeCancellationTransactionFake
	committed bool
	nextDue   db.NextRuntimeCancellationReapDueRow
	nextErr   error
}

func (r *runtimeCancellationRepositoryFake) nextReapDue(
	_ context.Context,
	_ int64,
) (db.NextRuntimeCancellationReapDueRow, error) {
	return r.nextDue, r.nextErr
}

func (r *runtimeCancellationRepositoryFake) WithTransaction(_ context.Context, fn func(runtimeCancellationTransaction) error) error {
	sessionInflight, nodeInflight := r.tx.sessionInflight, r.tx.nodeInflight
	err := fn(r.tx)
	if err != nil {
		r.tx.sessionInflight, r.tx.nodeInflight = sessionInflight, nodeInflight
		r.committed = false
		return err
	}
	r.committed = true
	return nil
}

type runtimeCancellationTransactionFake struct {
	runtimeCancellationTransaction
	principal    RuntimeSessionPrincipal
	databaseNow  time.Time
	lockedRun    db.LockRunForResultFinalizationRow
	attempt      db.RunAttempt
	cancellation db.RunCancellation
	publicRun    db.Run
	calls        []string

	sessionInflight   int32
	nodeInflight      int32
	persistCalls      int
	finishCalls       int
	capacityCASCalls  int
	finishedErrorCode string
	signals           []db.CreateRuntimeSignalParams
}

func (f *runtimeCancellationTransactionFake) FindNextDueRuntimeCoreCancellation(
	context.Context,
	int64,
) (db.FindNextDueRuntimeCoreCancellationRow, error) {
	return db.FindNextDueRuntimeCoreCancellationRow{}, pgx.ErrNoRows
}

func (f *runtimeCancellationTransactionFake) call(name string) { f.calls = append(f.calls, name) }

func (f *runtimeCancellationTransactionFake) LockRuntimeSessionForPrincipalValidation(_ context.Context, _ db.LockRuntimeSessionForPrincipalValidationParams) (db.LockRuntimeSessionForPrincipalValidationRow, error) {
	f.call("lock_session")
	coreID := f.principal.CoreInstanceID
	return db.LockRuntimeSessionForPrincipalValidationRow{
		RuntimeSessionID: f.principal.RuntimeSessionID, NodeID: f.principal.NodeID, AgentID: f.principal.AgentID,
		CredentialID: f.principal.CredentialID, WorkerID: f.principal.WorkerID, SessionEpoch: f.principal.SessionEpoch,
		RuntimeContractDigest:   f.principal.RuntimeContractDigest,
		DeviceCertificateSerial: f.principal.DeviceCertificateSerial, AttachedCoreInstanceID: &coreID,
		Status: "active", DatabaseNow: f.databaseNow,
	}, nil
}

func (f *runtimeCancellationTransactionFake) LockRuntimeNodeForPrincipalValidation(_ context.Context, _ db.LockRuntimeNodeForPrincipalValidationParams) (db.LockRuntimeNodeForPrincipalValidationRow, error) {
	f.call("lock_node")
	return db.LockRuntimeNodeForPrincipalValidationRow{
		NodeID: f.principal.NodeID, DeviceCertificateSerial: f.principal.DeviceCertificateSerial,
		DevicePublicKeyThumbprint: f.principal.DevicePublicKeyThumbprintSHA256,
		RuntimeContractDigest:     f.principal.RuntimeContractDigest,
	}, nil
}

func (f *runtimeCancellationTransactionFake) LockRuntimeCredentialForPrincipalValidation(_ context.Context, _ db.LockRuntimeCredentialForPrincipalValidationParams) (db.LockRuntimeCredentialForPrincipalValidationRow, error) {
	f.call("lock_credential")
	agentID := f.principal.AgentID
	return db.LockRuntimeCredentialForPrincipalValidationRow{
		ID: f.principal.CredentialID, AgentID: &agentID, Status: "active_runtime", DatabaseNow: f.databaseNow,
	}, nil
}

func (f *runtimeCancellationTransactionFake) LockRuntimeSessionAttachmentForPrincipalValidation(_ context.Context, params db.LockRuntimeSessionAttachmentForPrincipalValidationParams) (db.RuntimeSessionAttachment, error) {
	f.call("lock_attachment")
	return db.RuntimeSessionAttachment{ID: params.AttachmentID, RuntimeSessionID: params.RuntimeSessionID, CoreInstanceID: params.CoreInstanceID}, nil
}

func (f *runtimeCancellationTransactionFake) LockRunForResultFinalization(_ context.Context, _ uuid.UUID) (db.LockRunForResultFinalizationRow, error) {
	f.call("lock_run")
	return f.lockedRun, nil
}

func (f *runtimeCancellationTransactionFake) LockRunAttemptForResult(_ context.Context, _ db.LockRunAttemptForResultParams) (db.RunAttempt, error) {
	f.call("lock_attempt")
	return f.attempt, nil
}

func (f *runtimeCancellationTransactionFake) CreateRunCancellation(_ context.Context, params db.CreateRunCancellationParams) (db.RunCancellation, error) {
	f.call("create_cancellation")
	f.cancellation = db.RunCancellation{
		ID: params.ID, RunID: params.RunID, TargetAttemptID: params.TargetAttemptID,
		State: string(RuntimeCancelRequested), RequestedByType: params.RequestedByType,
		RequestedByID: params.RequestedByID, Reason: params.Reason,
		RequestedAt: f.databaseNow, UpdatedAt: f.databaseNow,
	}
	return f.cancellation, nil
}

func (f *runtimeCancellationTransactionFake) GetRunCancellationByRun(_ context.Context, _ uuid.UUID) (db.RunCancellation, error) {
	f.call("get_cancellation")
	return f.cancellation, nil
}

func (f *runtimeCancellationTransactionFake) LockRunCancellationForMutation(_ context.Context, _ db.LockRunCancellationForMutationParams) (db.RunCancellation, error) {
	f.call("lock_cancellation")
	return f.cancellation, nil
}

func (f *runtimeCancellationTransactionFake) AdvanceRuntimeRunCancellation(_ context.Context, params db.AdvanceRuntimeRunCancellationParams) (db.RunCancellation, error) {
	f.call("advance_cancellation")
	if f.cancellation.State != params.ExpectedState {
		return db.RunCancellation{}, pgx.ErrNoRows
	}
	f.cancellation.State = params.NextState
	f.cancellation.ErrorCode = params.ErrorCode
	f.cancellation.UpdatedAt = f.databaseNow.Add(time.Duration(len(f.calls)) * time.Millisecond)
	if params.NextState == string(RuntimeCancelDelivered) || params.NextState == string(RuntimeCancelStopping) ||
		params.NextState == string(RuntimeCancelStopped) || params.NextState == string(RuntimeCancelUnsupported) ||
		params.NextState == string(RuntimeCancelFailed) {
		f.cancellation.DeliveredAt = runtimeCancellationTimePointer(f.databaseNow)
	}
	if params.NextState == string(RuntimeCancelStopping) || params.NextState == string(RuntimeCancelStopped) {
		f.cancellation.StoppingAt = runtimeCancellationTimePointer(f.databaseNow)
		f.cancellation.AcknowledgedAt = runtimeCancellationTimePointer(f.databaseNow)
	}
	if params.NextState == string(RuntimeCancelStopped) {
		f.cancellation.StoppedAt = runtimeCancellationTimePointer(f.databaseNow)
	}
	return f.cancellation, nil
}

func (f *runtimeCancellationTransactionFake) PersistCancellationTerminal(_ context.Context, _ db.LockRunForResultFinalizationRow, cancellation db.RunCancellation, _ *db.RunAttempt) (db.Run, error) {
	f.call("persist_terminal")
	f.persistCalls++
	f.publicRun.Status = string(RuntimeRunCanceled)
	f.cancellation = cancellation
	return f.publicRun, nil
}

func (f *runtimeCancellationTransactionFake) GetRunByID(_ context.Context, _ uuid.UUID) (db.Run, error) {
	f.call("get_run")
	return f.publicRun, nil
}

func (f *runtimeCancellationTransactionFake) LockNextRuntimeCancellationCommandRun(_ context.Context, params db.LockNextRuntimeCancellationCommandRunParams) (db.LockNextRuntimeCancellationCommandRunRow, error) {
	f.call("lock_command_run")
	if params.CommandDeadlineMs < 1 ||
		!f.databaseNow.Before(f.cancellation.RequestedAt.Add(time.Duration(params.CommandDeadlineMs)*time.Millisecond)) ||
		(f.cancellation.State != string(RuntimeCancelRequested) &&
			f.cancellation.State != string(RuntimeCancelDelivered) &&
			f.cancellation.State != string(RuntimeCancelStopping)) {
		return db.LockNextRuntimeCancellationCommandRunRow{}, pgx.ErrNoRows
	}
	return db.LockNextRuntimeCancellationCommandRunRow{
		RunID: f.attempt.RunID, AgentID: f.attempt.AgentID, CancellationID: f.cancellation.ID,
		TargetAttemptID: f.attempt.ID, DatabaseNow: f.databaseNow,
	}, nil
}

func (f *runtimeCancellationTransactionFake) FindNextDueRuntimeCancellation(_ context.Context, commandDeadlineMS int64) (db.FindNextDueRuntimeCancellationRow, error) {
	f.call("find_due")
	state := RuntimeCancelState(f.cancellation.State)
	dueState := state == RuntimeCancelRequested || state == RuntimeCancelDelivered || state == RuntimeCancelStopping ||
		state == RuntimeCancelUnsupported || state == RuntimeCancelFailed
	if commandDeadlineMS < 1 || !dueState || f.attempt.FinishedAt != nil || f.attempt.Outcome != nil ||
		f.databaseNow.Before(f.cancellation.RequestedAt.Add(time.Duration(commandDeadlineMS)*time.Millisecond)) {
		return db.FindNextDueRuntimeCancellationRow{}, pgx.ErrNoRows
	}
	return db.FindNextDueRuntimeCancellationRow{
		RunID: f.attempt.RunID, AgentID: f.attempt.AgentID, CancellationID: f.cancellation.ID,
		TargetAttemptID: f.attempt.ID, RuntimeSessionID: f.principal.RuntimeSessionID,
		NodeID: f.principal.NodeID,
	}, nil
}

func (f *runtimeCancellationTransactionFake) LockRuntimeSessionForCancellationReap(_ context.Context, sessionID uuid.UUID) (uuid.UUID, error) {
	f.call("lock_reap_session")
	return sessionID, nil
}

func (f *runtimeCancellationTransactionFake) LockRuntimeNodeForCancellationReap(_ context.Context, nodeID uuid.UUID) (uuid.UUID, error) {
	f.call("lock_reap_node")
	return nodeID, nil
}

func (f *runtimeCancellationTransactionFake) LockDueRuntimeCancellationRun(_ context.Context, params db.LockDueRuntimeCancellationRunParams) (db.LockDueRuntimeCancellationRunRow, error) {
	f.call("lock_due_run")
	state := RuntimeCancelState(f.cancellation.State)
	dueState := state == RuntimeCancelRequested || state == RuntimeCancelDelivered || state == RuntimeCancelStopping ||
		state == RuntimeCancelUnsupported || state == RuntimeCancelFailed
	if !dueState || f.attempt.FinishedAt != nil || params.RunID != f.attempt.RunID ||
		params.TargetAttemptID != f.attempt.ID || params.CancellationID != f.cancellation.ID {
		return db.LockDueRuntimeCancellationRunRow{}, pgx.ErrNoRows
	}
	return db.LockDueRuntimeCancellationRunRow{
		RunID: f.attempt.RunID, AgentID: f.attempt.AgentID, CancellationID: f.cancellation.ID,
		TargetAttemptID: f.attempt.ID, DatabaseNow: f.databaseNow,
	}, nil
}

func (f *runtimeCancellationTransactionFake) MirrorRuntimeRunCancellationState(_ context.Context, _ db.MirrorRuntimeRunCancellationStateParams) (db.MirrorRuntimeRunCancellationStateRow, error) {
	f.call("mirror_cancellation")
	state := f.cancellation.State
	return db.MirrorRuntimeRunCancellationStateRow{
		ID: f.attempt.RunID, CancelRequestID: &f.cancellation.ID, CancelState: &state,
		CancelAcknowledgedAt: f.cancellation.AcknowledgedAt, DatabaseNow: f.databaseNow,
	}, nil
}

func (f *runtimeCancellationTransactionFake) FinishRuntimeCanceledAttempt(_ context.Context, params db.FinishRuntimeCanceledAttemptParams) (db.RunAttempt, error) {
	f.call("finish_attempt")
	f.finishCalls++
	finished := f.attempt
	finishedAt := f.databaseNow
	outcome := "canceled"
	finished.FinishedAt, finished.Outcome = &finishedAt, &outcome
	finished.ErrorCode = params.ErrorCode
	if params.ErrorCode != nil {
		f.finishedErrorCode = *params.ErrorCode
	}
	f.attempt = finished
	return finished, nil
}

func (f *runtimeCancellationTransactionFake) FinishRuntimeCoreCanceledAttempt(_ context.Context, _ db.FinishRuntimeCoreCanceledAttemptParams) (db.RunAttempt, error) {
	f.call("finish_core_attempt")
	return f.attempt, nil
}

func (f *runtimeCancellationTransactionFake) MarkRunAttemptCapacityReleased(_ context.Context, _ db.MarkRunAttemptCapacityReleasedParams) (db.MarkRunAttemptCapacityReleasedRow, error) {
	f.call("capacity_cas")
	f.capacityCASCalls++
	f.attempt.SlotReleasedAt = runtimeCancellationTimePointer(f.databaseNow)
	f.attempt.ActiveRuntimeSessionID = nil
	return db.MarkRunAttemptCapacityReleasedRow{
		RuntimeSessionID: f.principal.RuntimeSessionID, NodeID: f.principal.NodeID,
		SlotAcquiredAt: *f.attempt.SlotAcquiredAt, SlotReleasedAt: f.databaseNow,
	}, nil
}

func (f *runtimeCancellationTransactionFake) ReleaseRuntimeSessionSlot(_ context.Context, _ uuid.UUID) (db.RuntimeSession, error) {
	f.call("release_session")
	f.sessionInflight--
	return db.RuntimeSession{Inflight: f.sessionInflight}, nil
}

func (f *runtimeCancellationTransactionFake) ReleaseRuntimeNodeSlot(_ context.Context, _ uuid.UUID) (db.RuntimeNode, error) {
	f.call("release_node")
	f.nodeInflight--
	return db.RuntimeNode{Inflight: f.nodeInflight}, nil
}

func (f *runtimeCancellationTransactionFake) CreateRuntimeSignal(_ context.Context, params db.CreateRuntimeSignalParams) (db.RuntimeSignalOutbox, error) {
	f.call("create_capacity_signal")
	f.signals = append(f.signals, params)
	return db.RuntimeSignalOutbox{ID: uuid.New()}, nil
}
