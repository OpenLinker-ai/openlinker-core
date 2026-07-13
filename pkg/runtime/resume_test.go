package runtime

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

func TestRuntimeResumeSameSessionContinuesOnlyLiveAttempt(t *testing.T) {
	fixture := newRuntimeResumeFixture(t)

	response, err := fixture.service.Resume(context.Background(), fixture.target, fixture.payload())
	require.NoError(t, err)
	require.Len(t, response.Decisions, 1)
	require.Equal(t, RuntimeResumeContinueExecution, response.Decisions[0].Decision)
	require.Equal(t, fixture.attempt.LeaseExpiresAt, *response.Decisions[0].LeaseExpiresAt)
	require.Equal(t, []RuntimeResumeAction{
		RuntimeActionContinueExecution,
		RuntimeActionUploadEvents,
		RuntimeActionUploadResult,
	}, response.Decisions[0].AllowedActions)
	require.Equal(t, []string{
		"lock_session", "lock_node", "lock_credential", "lock_attachment",
		"lock_run:" + fixture.identity.RunID.String(),
		"lock_attempt:" + fixture.identity.AttemptID.String(),
	}, fixture.tx.calls)
	require.Zero(t, fixture.tx.createCalls)
	require.Zero(t, fixture.tx.consumeCalls)
	require.Zero(t, fixture.tx.revokeCalls)
	require.Equal(t, 1, fixture.repository.commits)
}

func TestRuntimeResumeUsesDatabaseClockAndStrictDeadline(t *testing.T) {
	fixture := newRuntimeResumeFixture(t)
	fixture.run.DatabaseNow = fixture.attempt.LeaseExpiresAt
	fixture.target.DatabaseTime = fixture.now.Add(-24 * time.Hour)
	fixture.tx.runs[fixture.run.ID] = fixture.run

	response, err := fixture.service.Resume(context.Background(), fixture.target, fixture.payload())
	require.NoError(t, err)
	require.Equal(t, RuntimeResumeLeaseRevoked, response.Decisions[0].Decision)
	require.Nil(t, response.Decisions[0].LeaseExpiresAt)
}

func TestRuntimeResumeCrossSessionNeverContinuesAndAllowsExpiredSourceLease(t *testing.T) {
	fixture := newRuntimeResumeFixture(t)
	fixture.moveToReplacementSession()
	fixture.run.LeaseExpiresAt = runtimeResumeTimePointer(fixture.now)
	fixture.attempt.LeaseExpiresAt = fixture.now
	fixture.tx.runs[fixture.run.ID] = fixture.run
	fixture.tx.putAttempt(fixture.attempt)
	resultID := uuid.New()
	finalSequence := int64(2)
	payload := fixture.payload()
	payload.Attempts[0].PendingClientEventRanges = []EventRange{{Start: 1, End: 2}}
	payload.Attempts[0].PendingResultID = &resultID
	payload.Attempts[0].FinalClientEventSeq = &finalSequence

	response, err := fixture.service.Resume(context.Background(), fixture.target, payload)
	require.NoError(t, err)
	require.Equal(t, RuntimeResumeUploadSpoolOnly, response.Decisions[0].Decision)
	require.Nil(t, response.Decisions[0].LeaseExpiresAt)
	require.Equal(t, []RuntimeResumeAction{
		RuntimeActionUploadEvents,
		RuntimeActionUploadResult,
	}, response.Decisions[0].AllowedActions)
	require.NotContains(t, response.Decisions[0].AllowedActions, RuntimeActionContinueExecution)
	require.Equal(t, 1, fixture.tx.createCalls)
	require.Equal(t, runtimeResumeUploadPermission, fixture.tx.createParams.Permission)
	require.Equal(t, defaultRuntimeResumeGrantTTL.Milliseconds(), fixture.tx.createParams.GrantTtlMs)
	require.Equal(t, fixture.sourceSessionID, *fixture.attempt.ActiveRuntimeSessionID,
		"Resume must not move capacity ownership to the target Session")
}

func TestRuntimeResumeCrossSessionStoredResultIsAckedBeforeTerminalChecks(t *testing.T) {
	fixture := newRuntimeResumeFixture(t)
	fixture.moveToReplacementSession()
	resultID := uuid.New()
	fixture.attempt.ResultID = &resultID
	fixture.tx.putAttempt(fixture.attempt)
	fixture.run.Status = string(RuntimeRunSuccess)
	fixture.run.DispatchState = string(RuntimeDispatchTerminal)
	fixture.tx.runs[fixture.run.ID] = fixture.run
	finalSequence := int64(0)
	payload := fixture.payload()
	payload.Attempts[0].PendingResultID = &resultID
	payload.Attempts[0].FinalClientEventSeq = &finalSequence

	response, err := fixture.service.Resume(context.Background(), fixture.target, payload)
	require.NoError(t, err)
	require.Equal(t, RuntimeResumeResultAcked, response.Decisions[0].Decision)
	require.Equal(t, []RuntimeResumeAction{RuntimeActionClearSpool}, response.Decisions[0].AllowedActions)
	require.Equal(t, 1, fixture.tx.createCalls)
	require.Equal(t, 1, fixture.tx.consumeCalls)
	require.Zero(t, fixture.tx.revokeCalls)
}

func TestRuntimeResumeCrossSessionRevokesGrantWhenSpoolIsNotWritable(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*runtimeResumeFixture)
	}{
		{
			name: "cancel requested",
			mutate: func(f *runtimeResumeFixture) {
				cancelID := uuid.New()
				f.run.CancelRequestID = &cancelID
			},
		},
		{
			name: "terminal run",
			mutate: func(f *runtimeResumeFixture) {
				f.run.Status = string(RuntimeRunFailed)
				f.run.DispatchState = string(RuntimeDispatchTerminal)
			},
		},
		{
			name: "attempt deadline reached",
			mutate: func(f *runtimeResumeFixture) {
				f.attempt.AttemptDeadlineAt = f.now
				f.run.AttemptDeadlineAt = runtimeResumeTimePointer(f.now)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRuntimeResumeFixture(t)
			fixture.moveToReplacementSession()
			test.mutate(fixture)
			fixture.tx.runs[fixture.run.ID] = fixture.run
			fixture.tx.putAttempt(fixture.attempt)
			payload := fixture.payload()
			payload.Attempts[0].PendingClientEventRanges = []EventRange{{Start: 1, End: 1}}

			response, err := fixture.service.Resume(context.Background(), fixture.target, payload)
			require.NoError(t, err)
			require.Equal(t, RuntimeResumeLeaseRevoked, response.Decisions[0].Decision)
			require.Equal(t, 1, fixture.tx.createCalls, "relationship is proved before writability is disclosed")
			require.Equal(t, 1, fixture.tx.revokeCalls)
			require.NotNil(t, fixture.tx.activeGrant)
			require.NotNil(t, fixture.tx.activeGrant.RevokedAt)
			require.Equal(t, "spool_upload_no_longer_writable", *fixture.tx.activeGrant.RevokeReason)
		})
	}
}

func TestRuntimeResumeReusesGrantAcrossCoreInstances(t *testing.T) {
	fixture := newRuntimeResumeFixture(t)
	fixture.moveToReplacementSession()
	oldCoreID := uuid.New()
	fixture.tx.seedActiveGrant(oldCoreID)
	payload := fixture.payload()
	payload.Attempts[0].PendingClientEventRanges = []EventRange{{Start: 1, End: 1}}

	response, err := fixture.service.Resume(context.Background(), fixture.target, payload)
	require.NoError(t, err)
	require.Equal(t, RuntimeResumeUploadSpoolOnly, response.Decisions[0].Decision)
	require.Zero(t, fixture.tx.createCalls, "a durable grant must not be tied to the issuing Core process")
	require.Equal(t, oldCoreID, fixture.tx.activeGrant.GrantedByCoreInstanceID)
	require.Equal(t, fixture.target.CoreInstanceID, *fixture.tx.activeParams.CoreInstanceID,
		"the target Session is still validated against its currently attached Core")
}

func TestRuntimeResumeRepeatedAndConcurrentCallsReuseOneGrant(t *testing.T) {
	fixture := newRuntimeResumeFixture(t)
	fixture.moveToReplacementSession()
	payload := fixture.payload()
	payload.Attempts[0].PendingClientEventRanges = []EventRange{{Start: 1, End: 1}}

	const callers = 8
	responses := make(chan RuntimeResumeResponse, callers)
	errors := make(chan error, callers)
	var group sync.WaitGroup
	group.Add(callers)
	for range callers {
		go func() {
			defer group.Done()
			response, err := fixture.service.Resume(context.Background(), fixture.target, payload)
			responses <- response
			errors <- err
		}()
	}
	group.Wait()
	close(responses)
	close(errors)

	for err := range errors {
		require.NoError(t, err)
	}
	for response := range responses {
		require.Equal(t, RuntimeResumeUploadSpoolOnly, response.Decisions[0].Decision)
	}
	require.Equal(t, 1, fixture.tx.createCalls)
	require.Equal(t, callers-1, fixture.tx.activeCalls)
	require.Equal(t, callers, fixture.repository.commits)
}

func TestRuntimeResumeResultIDConflictIsHardErrorAndRollsBack(t *testing.T) {
	fixture := newRuntimeResumeFixture(t)
	storedResultID := uuid.New()
	pendingResultID := uuid.New()
	fixture.attempt.ResultID = &storedResultID
	fixture.tx.putAttempt(fixture.attempt)
	finalSequence := int64(0)
	payload := fixture.payload()
	payload.Attempts[0].PendingResultID = &pendingResultID
	payload.Attempts[0].FinalClientEventSeq = &finalSequence

	response, err := fixture.service.Resume(context.Background(), fixture.target, payload)
	require.Empty(t, response.Decisions)
	require.True(t, IsRuntimeResultError(err, RuntimeResultErrorResultIDConflict), err)
	require.Zero(t, fixture.repository.commits)
	require.Equal(t, 1, fixture.repository.rollbacks)
}

func TestRuntimeResumeCrossSessionResultConflictRollsBackNewGrant(t *testing.T) {
	fixture := newRuntimeResumeFixture(t)
	fixture.moveToReplacementSession()
	storedResultID := uuid.New()
	pendingResultID := uuid.New()
	fixture.attempt.ResultID = &storedResultID
	fixture.tx.putAttempt(fixture.attempt)
	finalSequence := int64(0)
	payload := fixture.payload()
	payload.Attempts[0].PendingResultID = &pendingResultID
	payload.Attempts[0].FinalClientEventSeq = &finalSequence

	_, err := fixture.service.Resume(context.Background(), fixture.target, payload)
	require.True(t, IsRuntimeResultError(err, RuntimeResultErrorResultIDConflict), err)
	require.Equal(t, 1, fixture.tx.createCalls, "grant proof precedes cross-Session Result disclosure")
	require.Equal(t, 1, fixture.repository.rollbacks)
	require.Nil(t, fixture.tx.activeGrant, "a hard conflict must roll back the newly inserted grant")
}

func TestRuntimeResumeCrossSessionNeverReusesContinuePermission(t *testing.T) {
	fixture := newRuntimeResumeFixture(t)
	fixture.moveToReplacementSession()
	fixture.tx.seedActiveGrant(uuid.New())
	fixture.tx.activeGrant.Permission = "continue_execution"
	payload := fixture.payload()
	payload.Attempts[0].PendingClientEventRanges = []EventRange{{Start: 1, End: 1}}

	response, err := fixture.service.Resume(context.Background(), fixture.target, payload)
	require.NoError(t, err)
	require.Equal(t, RuntimeResumeLeaseRevoked, response.Decisions[0].Decision)
	require.NotContains(t, response.Decisions[0].AllowedActions, RuntimeActionContinueExecution)
	require.Zero(t, fixture.tx.createCalls)
}

func TestRuntimeResumeLocksSortedButRestoresPayloadOrder(t *testing.T) {
	fixture := newRuntimeResumeFixture(t)
	lowRunID := uuid.MustParse("00000000-0000-0000-0000-000000000010")
	highRunID := uuid.MustParse("00000000-0000-0000-0000-000000000020")
	lowAttemptID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	highAttemptID := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	otherAttemptID := uuid.MustParse("00000000-0000-0000-0000-000000000003")

	lowIdentity, lowRun, lowAttempt := fixture.execution(lowRunID, lowAttemptID)
	highIdentity := lowIdentity
	highIdentity.AttemptID = highAttemptID
	highIdentity.LeaseID = uuid.New()
	highIdentity.FencingToken++
	highAttempt := lowAttempt
	highAttempt.ID = highIdentity.AttemptID
	highAttempt.LeaseID = highIdentity.LeaseID
	highAttempt.FencingToken = highIdentity.FencingToken
	otherIdentity, otherRun, otherAttempt := fixture.execution(highRunID, otherAttemptID)
	fixture.tx.runs = map[uuid.UUID]db.LockRunForLeaseMutationRow{
		lowRunID:  lowRun,
		highRunID: otherRun,
	}
	fixture.tx.attempts = make(map[runtimeResumeAttemptKey]db.RunAttempt)
	fixture.tx.putAttempt(lowAttempt)
	fixture.tx.putAttempt(highAttempt)
	fixture.tx.putAttempt(otherAttempt)
	payload := fixture.payload()
	payload.Attempts = []ResumeAttempt{
		{AttemptIdentity: otherIdentity, PendingClientEventRanges: []EventRange{}},
		{AttemptIdentity: highIdentity, PendingClientEventRanges: []EventRange{}},
		{AttemptIdentity: lowIdentity, PendingClientEventRanges: []EventRange{}},
	}

	response, err := fixture.service.Resume(context.Background(), fixture.target, payload)
	require.NoError(t, err)
	require.Equal(t, []AttemptIdentity{otherIdentity, highIdentity, lowIdentity}, []AttemptIdentity{
		response.Decisions[0].AttemptIdentity,
		response.Decisions[1].AttemptIdentity,
		response.Decisions[2].AttemptIdentity,
	})
	require.Equal(t, []uuid.UUID{lowRunID, highRunID}, fixture.tx.runLocks)
	require.Equal(t, []uuid.UUID{lowAttemptID, highAttemptID, otherAttemptID}, fixture.tx.attemptLocks)
	require.Equal(t, RuntimeResumeContinueExecution, response.Decisions[0].Decision)
	require.Equal(t, RuntimeResumeLeaseRevoked, response.Decisions[1].Decision)
	require.Equal(t, RuntimeResumeContinueExecution, response.Decisions[2].Decision)
}

func TestRuntimeResumeValidatesPayloadAndTargetBeforeTransaction(t *testing.T) {
	fixture := newRuntimeResumeFixture(t)
	payload := fixture.payload()
	payload.RuntimeSessionID = uuid.New()

	_, err := fixture.service.Resume(context.Background(), fixture.target, payload)
	require.True(t, IsRuntimeLeaseError(err, RuntimeLeaseErrorIdentityMismatch), err)
	require.Zero(t, fixture.repository.transactions)

	fixture.target.Status = "offline"
	_, err = fixture.service.Resume(context.Background(), fixture.target, fixture.payload())
	require.True(t, IsRuntimeLeaseError(err, RuntimeLeaseErrorValidationFailed), err)
	require.Zero(t, fixture.repository.transactions)
}

type runtimeResumeFixture struct {
	now             time.Time
	target          RuntimeSessionPrincipal
	sourceSessionID uuid.UUID
	identity        AttemptIdentity
	run             db.LockRunForLeaseMutationRow
	attempt         db.RunAttempt
	tx              *runtimeResumeTransactionFake
	repository      *runtimeResumeRepositoryFake
	service         *RuntimeResumeService
}

func newRuntimeResumeFixture(t *testing.T) *runtimeResumeFixture {
	t.Helper()
	now := time.Date(2026, 7, 11, 14, 30, 0, 123456000, time.UTC)
	coreID := uuid.New()
	target := RuntimeSessionPrincipal{
		RuntimeSessionID:                uuid.New(),
		NodeID:                          uuid.New(),
		AgentID:                         uuid.New(),
		CredentialID:                    uuid.New(),
		WorkerID:                        "worker-resume-a",
		SessionEpoch:                    3,
		CoreInstanceID:                  coreID,
		AttachmentID:                    uuid.New(),
		DeviceCertificateSerial:         "abc123",
		DevicePublicKeyThumbprintSHA256: fmt.Sprintf("%064x", 42),
		Status:                          "active",
		DatabaseTime:                    now,
	}
	fixture := &runtimeResumeFixture{now: now, target: target, sourceSessionID: target.RuntimeSessionID}
	fixture.identity, fixture.run, fixture.attempt = fixture.execution(uuid.New(), uuid.New())
	tx := &runtimeResumeTransactionFake{
		now:       now,
		target:    target,
		runs:      map[uuid.UUID]db.LockRunForLeaseMutationRow{fixture.run.ID: fixture.run},
		attempts:  make(map[runtimeResumeAttemptKey]db.RunAttempt),
		resultErr: pgx.ErrNoRows,
	}
	tx.putAttempt(fixture.attempt)
	repository := &runtimeResumeRepositoryFake{tx: tx}
	fixture.tx = tx
	fixture.repository = repository
	fixture.service = newRuntimeResumeService(repository, coreID, 0)
	return fixture
}

func (f *runtimeResumeFixture) execution(runID, attemptID uuid.UUID) (AttemptIdentity, db.LockRunForLeaseMutationRow, db.RunAttempt) {
	identity := AttemptIdentity{
		RunID: runID, AttemptID: attemptID, LeaseID: uuid.New(), FencingToken: 9,
		NodeID: f.target.NodeID, AgentID: f.target.AgentID,
		WorkerID: f.target.WorkerID, RuntimeSessionID: f.sourceSessionID,
	}
	acceptedAt := f.now.Add(-time.Minute)
	leaseExpiresAt := f.now.Add(5 * time.Minute)
	attemptDeadlineAt := f.now.Add(30 * time.Minute)
	runDeadlineAt := f.now.Add(time.Hour)
	attemptNo := int32(1)
	slotAcquiredAt := acceptedAt.Add(-time.Second)
	run := db.LockRunForLeaseMutationRow{
		ID: runID, AgentID: f.target.AgentID, Status: string(RuntimeRunRunning),
		DispatchState: string(RuntimeDispatchExecuting), LatestAttemptID: &identity.AttemptID,
		ActiveAttemptID: &identity.AttemptID, LeaseID: &identity.LeaseID,
		FencingToken: identity.FencingToken, RuntimeNodeID: &identity.NodeID,
		RuntimeWorkerID: &identity.WorkerID, RuntimeSessionID: &identity.RuntimeSessionID,
		LeaseTokenID: &f.target.CredentialID, LeaseAcceptedAt: &acceptedAt,
		LeaseExpiresAt: &leaseExpiresAt, AttemptDeadlineAt: &attemptDeadlineAt,
		RunDeadlineAt: &runDeadlineAt, DatabaseNow: f.now,
	}
	attempt := db.RunAttempt{
		ID: attemptID, RunID: runID, AgentID: f.target.AgentID, OfferNo: 1,
		AttemptNo: &attemptNo, ExecutorType: "runtime", LeaseID: identity.LeaseID,
		FencingToken: identity.FencingToken, RuntimeTokenID: &f.target.CredentialID,
		RuntimeWorkerID: &identity.WorkerID, RuntimeSessionID: &identity.RuntimeSessionID,
		NodeID: &identity.NodeID, OfferedByCoreInstanceID: f.target.CoreInstanceID,
		AttachedCoreInstanceID: f.target.CoreInstanceID, OfferedAt: acceptedAt.Add(-time.Second),
		OfferExpiresAt: acceptedAt, AcceptedAt: &acceptedAt, LastRenewedAt: &acceptedAt,
		LeaseExpiresAt: leaseExpiresAt, AttemptDeadlineAt: attemptDeadlineAt,
		SlotAcquiredAt: &slotAcquiredAt, ActiveRuntimeSessionID: &identity.RuntimeSessionID,
	}
	return identity, run, attempt
}

func (f *runtimeResumeFixture) moveToReplacementSession() {
	f.sourceSessionID = uuid.New()
	f.identity.RuntimeSessionID = f.sourceSessionID
	f.run.RuntimeSessionID = &f.sourceSessionID
	f.attempt.RuntimeSessionID = &f.sourceSessionID
	f.attempt.ActiveRuntimeSessionID = &f.sourceSessionID
	f.tx.runs[f.run.ID] = f.run
	f.tx.putAttempt(f.attempt)
}

func (f *runtimeResumeFixture) payload() RuntimeResumePayload {
	return RuntimeResumePayload{
		NodeID: f.target.NodeID, AgentID: f.target.AgentID, WorkerID: f.target.WorkerID,
		RuntimeSessionID: f.target.RuntimeSessionID,
		Attempts: []ResumeAttempt{{
			AttemptIdentity: f.identity, PendingClientEventRanges: []EventRange{},
		}},
	}
}

func runtimeResumeTimePointer(value time.Time) *time.Time { return &value }

type runtimeResumeAttemptKey struct {
	runID     uuid.UUID
	attemptID uuid.UUID
}

type runtimeResumeRepositoryFake struct {
	mu           sync.Mutex
	tx           *runtimeResumeTransactionFake
	transactions int
	commits      int
	rollbacks    int
}

func (r *runtimeResumeRepositoryFake) WithTransaction(_ context.Context, fn func(runtimeResumeTransaction) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.transactions++
	activeSnapshot := cloneRuntimeResumeActiveGrant(r.tx.activeGrant)
	err := fn(r.tx)
	if err != nil {
		r.tx.activeGrant = activeSnapshot
		r.rollbacks++
		return err
	}
	r.commits++
	return nil
}

type runtimeResumeTransactionFake struct {
	now    time.Time
	target RuntimeSessionPrincipal
	calls  []string

	runs          map[uuid.UUID]db.LockRunForLeaseMutationRow
	attempts      map[runtimeResumeAttemptKey]db.RunAttempt
	resultErr     error
	resultAttempt db.RunAttempt

	runLocks     []uuid.UUID
	attemptLocks []uuid.UUID

	activeGrant  *db.LockActiveRuntimeResumeGrantForAttemptTargetRow
	activeParams db.LockActiveRuntimeResumeGrantForAttemptTargetParams
	activeCalls  int
	createParams db.CreateRuntimeResumeGrantParams
	createCalls  int
	createErr    error
	consumeCalls int
	revokeCalls  int
}

func (f *runtimeResumeTransactionFake) call(value string) { f.calls = append(f.calls, value) }

func (f *runtimeResumeTransactionFake) putAttempt(attempt db.RunAttempt) {
	f.attempts[runtimeResumeAttemptKey{runID: attempt.RunID, attemptID: attempt.ID}] = attempt
}

func (f *runtimeResumeTransactionFake) LockRuntimeSessionForPrincipalValidation(_ context.Context, _ db.LockRuntimeSessionForPrincipalValidationParams) (db.LockRuntimeSessionForPrincipalValidationRow, error) {
	f.call("lock_session")
	coreID := f.target.CoreInstanceID
	return db.LockRuntimeSessionForPrincipalValidationRow{
		RuntimeSessionID: f.target.RuntimeSessionID, NodeID: f.target.NodeID,
		AgentID: f.target.AgentID, CredentialID: f.target.CredentialID,
		WorkerID: f.target.WorkerID, SessionEpoch: f.target.SessionEpoch,
		DeviceCertificateSerial: f.target.DeviceCertificateSerial,
		Status:                  f.target.Status, AttachedCoreInstanceID: &coreID, DatabaseNow: f.now,
	}, nil
}

func (f *runtimeResumeTransactionFake) LockRuntimeNodeForPrincipalValidation(_ context.Context, _ db.LockRuntimeNodeForPrincipalValidationParams) (db.LockRuntimeNodeForPrincipalValidationRow, error) {
	f.call("lock_node")
	return db.LockRuntimeNodeForPrincipalValidationRow{
		NodeID: f.target.NodeID, DeviceCertificateSerial: f.target.DeviceCertificateSerial,
		DevicePublicKeyThumbprint: f.target.DevicePublicKeyThumbprintSHA256,
		Status:                    "active", DatabaseNow: f.now,
	}, nil
}

func (f *runtimeResumeTransactionFake) LockRuntimeCredentialForPrincipalValidation(_ context.Context, _ db.LockRuntimeCredentialForPrincipalValidationParams) (db.LockRuntimeCredentialForPrincipalValidationRow, error) {
	f.call("lock_credential")
	agentID := f.target.AgentID
	return db.LockRuntimeCredentialForPrincipalValidationRow{
		ID: f.target.CredentialID, AgentID: &agentID, Status: "active_runtime",
		Scopes: []string{"agent:pull"}, DatabaseNow: f.now,
	}, nil
}

func (f *runtimeResumeTransactionFake) LockRuntimeSessionAttachmentForPrincipalValidation(_ context.Context, params db.LockRuntimeSessionAttachmentForPrincipalValidationParams) (db.RuntimeSessionAttachment, error) {
	f.call("lock_attachment")
	return db.RuntimeSessionAttachment{ID: params.AttachmentID, RuntimeSessionID: params.RuntimeSessionID, CoreInstanceID: params.CoreInstanceID}, nil
}

func (f *runtimeResumeTransactionFake) LockRunForLeaseMutation(_ context.Context, id uuid.UUID) (db.LockRunForLeaseMutationRow, error) {
	f.call("lock_run:" + id.String())
	f.runLocks = append(f.runLocks, id)
	run, ok := f.runs[id]
	if !ok {
		return db.LockRunForLeaseMutationRow{}, pgx.ErrNoRows
	}
	return run, nil
}

func (f *runtimeResumeTransactionFake) LockRunAttemptForResult(_ context.Context, params db.LockRunAttemptForResultParams) (db.RunAttempt, error) {
	f.call("lock_attempt:" + params.ID.String())
	f.attemptLocks = append(f.attemptLocks, params.ID)
	attempt, ok := f.attempts[runtimeResumeAttemptKey{runID: params.RunID, attemptID: params.ID}]
	if !ok {
		return db.RunAttempt{}, pgx.ErrNoRows
	}
	return attempt, nil
}

func (f *runtimeResumeTransactionFake) GetRunAttemptByResultID(_ context.Context, params db.GetRunAttemptByResultIDParams) (db.RunAttempt, error) {
	f.call("get_result:" + params.ResultID.String())
	if f.resultErr == nil {
		return f.resultAttempt, nil
	}
	for _, attempt := range f.attempts {
		if attempt.RunID == params.RunID && attempt.ResultID != nil && *attempt.ResultID == params.ResultID {
			return attempt, nil
		}
	}
	return db.RunAttempt{}, f.resultErr
}

func (f *runtimeResumeTransactionFake) LockActiveRuntimeResumeGrantForAttemptTarget(_ context.Context, params db.LockActiveRuntimeResumeGrantForAttemptTargetParams) (db.LockActiveRuntimeResumeGrantForAttemptTargetRow, error) {
	f.call("lock_active_grant")
	f.activeParams = params
	if f.activeGrant == nil || f.activeGrant.RevokedAt != nil || !f.now.Before(f.activeGrant.ExpiresAt) {
		return db.LockActiveRuntimeResumeGrantForAttemptTargetRow{}, pgx.ErrNoRows
	}
	f.activeCalls++
	copy := *f.activeGrant
	copy.DatabaseNow = f.now
	return copy, nil
}

func (f *runtimeResumeTransactionFake) CreateRuntimeResumeGrant(_ context.Context, params db.CreateRuntimeResumeGrantParams) (db.RuntimeResumeGrant, error) {
	f.call("create_grant")
	f.createCalls++
	f.createParams = params
	if f.createErr != nil {
		return db.RuntimeResumeGrant{}, f.createErr
	}
	if f.activeGrant != nil {
		return db.RuntimeResumeGrant{}, pgx.ErrNoRows
	}
	attempt := f.attempts[runtimeResumeAttemptKey{runID: params.RunID, attemptID: params.AttemptID}]
	grant := db.LockActiveRuntimeResumeGrantForAttemptTargetRow{
		ID: params.GrantID, RunID: params.RunID, AttemptID: params.AttemptID,
		LeaseID: params.LeaseID, FencingToken: params.FencingToken,
		AgentID: attempt.AgentID, NodeID: *attempt.NodeID, WorkerID: *attempt.RuntimeWorkerID,
		SourceSessionID: *attempt.RuntimeSessionID, SourceCredentialID: *attempt.RuntimeTokenID,
		TargetSessionID: f.target.RuntimeSessionID, TargetCredentialID: f.target.CredentialID,
		Permission: params.Permission, GrantedByCoreInstanceID: params.CoreInstanceID,
		GrantedAt: f.now, ExpiresAt: f.now.Add(time.Duration(params.GrantTtlMs) * time.Millisecond),
		DatabaseNow: f.now,
	}
	f.activeGrant = &grant
	return runtimeResumeGrantFromActive(grant), nil
}

func (f *runtimeResumeTransactionFake) ConsumeRuntimeResumeGrant(_ context.Context, params db.ConsumeRuntimeResumeGrantParams) (db.RuntimeResumeGrant, error) {
	f.call("consume_grant")
	f.consumeCalls++
	if f.activeGrant == nil || f.activeGrant.ID != params.GrantID ||
		f.activeGrant.RevokedAt != nil || !f.now.Before(f.activeGrant.ExpiresAt) {
		return db.RuntimeResumeGrant{}, pgx.ErrNoRows
	}
	firstUsedAt := f.now
	f.activeGrant.FirstUsedAt = &firstUsedAt
	return runtimeResumeGrantFromActive(*f.activeGrant), nil
}

func (f *runtimeResumeTransactionFake) RevokeRuntimeResumeGrant(_ context.Context, params db.RevokeRuntimeResumeGrantParams) (db.RuntimeResumeGrant, error) {
	f.call("revoke_grant")
	f.revokeCalls++
	if f.activeGrant == nil || f.activeGrant.ID != params.GrantID || f.activeGrant.RevokedAt != nil {
		return db.RuntimeResumeGrant{}, pgx.ErrNoRows
	}
	revokedAt := f.now
	f.activeGrant.RevokedAt = &revokedAt
	f.activeGrant.RevokedByType = params.RevokedByType
	f.activeGrant.RevokedByID = params.RevokedByID
	f.activeGrant.RevokeReason = params.RevokeReason
	return runtimeResumeGrantFromActive(*f.activeGrant), nil
}

func (f *runtimeResumeTransactionFake) seedActiveGrant(coreInstanceID uuid.UUID) {
	var attempt db.RunAttempt
	for _, candidate := range f.attempts {
		attempt = candidate
		break
	}
	f.activeGrant = &db.LockActiveRuntimeResumeGrantForAttemptTargetRow{
		ID: uuid.New(), RunID: attempt.RunID, AttemptID: attempt.ID,
		LeaseID: attempt.LeaseID, FencingToken: attempt.FencingToken,
		AgentID: attempt.AgentID, NodeID: *attempt.NodeID, WorkerID: *attempt.RuntimeWorkerID,
		SourceSessionID: *attempt.RuntimeSessionID, SourceCredentialID: *attempt.RuntimeTokenID,
		TargetSessionID: f.target.RuntimeSessionID, TargetCredentialID: f.target.CredentialID,
		Permission: runtimeResumeUploadPermission, GrantedByCoreInstanceID: coreInstanceID,
		GrantedAt: f.now.Add(-time.Minute), ExpiresAt: f.now.Add(time.Minute), DatabaseNow: f.now,
	}
}

func cloneRuntimeResumeActiveGrant(value *db.LockActiveRuntimeResumeGrantForAttemptTargetRow) *db.LockActiveRuntimeResumeGrantForAttemptTargetRow {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
