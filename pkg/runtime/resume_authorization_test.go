package runtime

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

func TestRuntimeAttemptAuthorizationConsumesActiveTakeoverGrant(t *testing.T) {
	principal, identity, attempt, grant := runtimeResumeAuthorizationFixture()
	fake := &runtimeResumeAuthorizationFake{active: grant}
	authorization, err := authorizeRuntimeAttemptPrincipal(
		context.Background(), fake, principal, identity, attempt, false,
	)
	require.NoError(t, err)
	require.True(t, authorization.resumed)
	require.Equal(t, grant.ID, authorization.grantID)
	require.Equal(t, 1, fake.activeCalls)
	require.Equal(t, 1, fake.consumeCalls)
	require.Equal(t, *principal.RuntimeSessionID, fake.consumeParams.TargetSessionID)
}

func TestRuntimeAttemptAuthorizationUsesConsumedEvidenceOnlyForReplay(t *testing.T) {
	principal, identity, attempt, grant := runtimeResumeAuthorizationFixture()
	fake := &runtimeResumeAuthorizationFake{replay: db.LockConsumedRuntimeResumeGrantForStoredReplayRow{
		ID:                 grant.ID,
		Permission:         grant.Permission,
		SourceSessionID:    grant.SourceSessionID,
		SourceCredentialID: grant.SourceCredentialID,
	}}
	authorization, err := authorizeRuntimeAttemptPrincipal(
		context.Background(), fake, principal, identity, attempt, true,
	)
	require.NoError(t, err)
	require.True(t, authorization.resumed)
	require.Equal(t, 1, fake.replayCalls)
	require.Zero(t, fake.activeCalls)
	require.Zero(t, fake.consumeCalls)
}

func TestRuntimeAttemptAuthorizationDirectSessionNeedsNoGrant(t *testing.T) {
	principal, identity, attempt, _ := runtimeResumeAuthorizationFixture()
	principal.RuntimeSessionID = attempt.runtimeSessionID
	principal.CredentialID = attempt.credentialID
	fake := &runtimeResumeAuthorizationFake{}
	authorization, err := authorizeRuntimeAttemptPrincipal(
		context.Background(), fake, principal, identity, attempt, false,
	)
	require.NoError(t, err)
	require.False(t, authorization.resumed)
	require.Zero(t, fake.activeCalls+fake.replayCalls+fake.consumeCalls)
}

func runtimeResumeAuthorizationFixture() (RuntimeEventPrincipal, RuntimeAttemptIdentity, runtimeEventAttempt, db.LockActiveRuntimeResumeGrantForAttemptTargetRow) {
	principal := runtimeNodePrincipalFixture()
	sourceSessionID := uuid.New()
	sourceCredentialID := uuid.New()
	runID := uuid.New()
	attemptID := uuid.New()
	leaseID := uuid.New()
	attempt := runtimeEventAttempt{
		id:               attemptID,
		runID:            runID,
		agentID:          principal.AgentID,
		executorType:     "agent_node",
		leaseID:          leaseID,
		fencingToken:     4,
		workerID:         principal.WorkerID,
		runtimeSessionID: &sourceSessionID,
		nodeID:           principal.NodeID,
		credentialID:     &sourceCredentialID,
	}
	identity := RuntimeAttemptIdentity{
		RunID:            runID,
		AttemptID:        attemptID,
		LeaseID:          leaseID,
		FencingToken:     4,
		AgentID:          principal.AgentID,
		NodeID:           principal.NodeID,
		WorkerID:         principal.WorkerID,
		RuntimeSessionID: &sourceSessionID,
	}
	grant := db.LockActiveRuntimeResumeGrantForAttemptTargetRow{
		ID:                 uuid.New(),
		Permission:         runtimeResumeUploadPermission,
		SourceSessionID:    sourceSessionID,
		SourceCredentialID: sourceCredentialID,
	}
	return principal, identity, attempt, grant
}

type runtimeResumeAuthorizationFake struct {
	active        db.LockActiveRuntimeResumeGrantForAttemptTargetRow
	replay        db.LockConsumedRuntimeResumeGrantForStoredReplayRow
	activeCalls   int
	replayCalls   int
	consumeCalls  int
	consumeParams db.ConsumeRuntimeResumeGrantParams
}

func (f *runtimeResumeAuthorizationFake) LockActiveRuntimeResumeGrantForAttemptTarget(context.Context, db.LockActiveRuntimeResumeGrantForAttemptTargetParams) (db.LockActiveRuntimeResumeGrantForAttemptTargetRow, error) {
	f.activeCalls++
	return f.active, nil
}

func (f *runtimeResumeAuthorizationFake) LockConsumedRuntimeResumeGrantForStoredReplay(context.Context, db.LockConsumedRuntimeResumeGrantForStoredReplayParams) (db.LockConsumedRuntimeResumeGrantForStoredReplayRow, error) {
	f.replayCalls++
	return f.replay, nil
}

func (f *runtimeResumeAuthorizationFake) ConsumeRuntimeResumeGrant(_ context.Context, params db.ConsumeRuntimeResumeGrantParams) (db.RuntimeResumeGrant, error) {
	f.consumeCalls++
	f.consumeParams = params
	return db.RuntimeResumeGrant{ID: f.active.ID, Permission: f.active.Permission}, nil
}

var _ runtimeResumeAuthorizationQueries = (*runtimeResumeAuthorizationFake)(nil)
