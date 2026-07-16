package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

func TestRuntimeSchemaChecksumMatchesCurrentContractTuple(t *testing.T) {
	sum := sha256.Sum256([]byte(
		strconv.FormatInt(int64(RuntimeSchemaVersion), 10) + ":" + RuntimeSchemaMigrationName + ":" + RuntimeContractID + ":" + RuntimeContractDigest,
	))
	if got := hex.EncodeToString(sum[:]); got != RuntimeSchemaChecksum {
		t.Fatalf("schema checksum = %s, want %s", RuntimeSchemaChecksum, got)
	}
}

func TestRuntimeSchemaIdentityTracksTerminalCancellationReapMigration(t *testing.T) {
	if RuntimeSchemaVersion != 77 || RuntimeSchemaMigrationName != "077_external_execution_cancellation" {
		t.Fatalf("runtime schema identity = %d:%s", RuntimeSchemaVersion, RuntimeSchemaMigrationName)
	}
}

func TestRuntimeClusterMemberLiveWindowIsCanonical(t *testing.T) {
	if RuntimeClusterMemberLiveWindow != 15*time.Second {
		t.Fatalf("RuntimeClusterMemberLiveWindow = %s", RuntimeClusterMemberLiveWindow)
	}
}

func TestRuntimeClusterReadinessHealthyReplicaSet(t *testing.T) {
	identity := runtimeClusterTestIdentity()
	repository := newRuntimeClusterRepositoryFake(identity, 2)
	peer := identity
	peer.InstanceID = uuid.New()
	repository.snapshot.LiveMembers = []RuntimeClusterMemberSnapshot{
		{RuntimeClusterIdentity: identity, HeartbeatAt: repository.snapshot.DatabaseTime, Ready: true},
		{RuntimeClusterIdentity: peer, HeartbeatAt: repository.snapshot.DatabaseTime, Ready: true},
	}
	coordinator := mustRuntimeClusterCoordinator(t, repository, &runtimeSignalBusHealthFake{}, identity, true)

	got := coordinator.Readiness(context.Background())
	if !got.Ready || got.Status != "ready" || got.HTTPStatus() != 200 {
		t.Fatalf("readiness = %#v, want ready", got)
	}
	if got.LiveReplicas != 2 || got.ExpectedReplicas != 2 || got.DatabaseTime == nil {
		t.Fatalf("readiness evidence = %#v", got)
	}
}

func TestRuntimeClusterReadinessFailsClosedForReplicaContractAndDependencyFaults(t *testing.T) {
	identity := runtimeClusterTestIdentity()
	tests := []struct {
		name       string
		mutate     func(*runtimeClusterRepositoryFake, *runtimeSignalBusHealthFake)
		wantReason string
	}{
		{
			name: "replica missing",
			mutate: func(repository *runtimeClusterRepositoryFake, _ *runtimeSignalBusHealthFake) {
				repository.snapshot.Control.ExpectedReplicas = 2
			},
			wantReason: "replicas_unavailable",
		},
		{
			name: "schema contract mismatch",
			mutate: func(repository *runtimeClusterRepositoryFake, _ *runtimeSignalBusHealthFake) {
				repository.snapshot.CurrentSchema.SchemaVersion--
			},
			wantReason: "schema_contract_mismatch",
		},
		{
			name: "release mismatch",
			mutate: func(repository *runtimeClusterRepositoryFake, _ *runtimeSignalBusHealthFake) {
				peer := identity
				peer.InstanceID = uuid.New()
				peer.ReleaseCommit = "different"
				repository.snapshot.LiveMembers = append(repository.snapshot.LiveMembers,
					RuntimeClusterMemberSnapshot{RuntimeClusterIdentity: peer, HeartbeatAt: repository.snapshot.DatabaseTime, Ready: true})
				repository.snapshot.Control.ExpectedReplicas = 2
			},
			wantReason: "member_contract_mismatch",
		},
		{
			name: "schema checksum mismatch",
			mutate: func(repository *runtimeClusterRepositoryFake, _ *runtimeSignalBusHealthFake) {
				peer := identity
				peer.InstanceID = uuid.New()
				peer.SchemaChecksum = strings.Repeat("f", 64)
				repository.snapshot.LiveMembers = append(repository.snapshot.LiveMembers,
					RuntimeClusterMemberSnapshot{RuntimeClusterIdentity: peer, HeartbeatAt: repository.snapshot.DatabaseTime, Ready: true})
				repository.snapshot.Control.ExpectedReplicas = 2
			},
			wantReason: "member_contract_mismatch",
		},
		{
			name: "redis signal unavailable",
			mutate: func(_ *runtimeClusterRepositoryFake, signal *runtimeSignalBusHealthFake) {
				signal.err = errors.New("redis unavailable")
			},
			wantReason: "signal_dependency_unavailable",
		},
		{
			name: "maintenance",
			mutate: func(repository *runtimeClusterRepositoryFake, _ *runtimeSignalBusHealthFake) {
				repository.snapshot.Control.Mode = RuntimeClusterModeHardMaintenance
			},
			wantReason: "maintenance",
		},
		{
			name: "peer local dependency unavailable",
			mutate: func(repository *runtimeClusterRepositoryFake, _ *runtimeSignalBusHealthFake) {
				repository.snapshot.LiveMembers[0].Ready = false
			},
			wantReason: "member_not_ready",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repository := newRuntimeClusterRepositoryFake(identity, 1)
			signal := &runtimeSignalBusHealthFake{}
			tt.mutate(repository, signal)
			coordinator := mustRuntimeClusterCoordinator(t, repository, signal, identity, true)

			got := coordinator.Readiness(context.Background())
			if got.Ready || got.HTTPStatus() != 503 || !containsRuntimeClusterReason(got.Reasons, tt.wantReason) {
				t.Fatalf("readiness = %#v, want reason %q", got, tt.wantReason)
			}
		})
	}
}

func TestRuntimeClusterReadinessIgnoresSignalHealthInSingleInstanceMode(t *testing.T) {
	identity := runtimeClusterTestIdentity()
	repository := newRuntimeClusterRepositoryFake(identity, 1)
	signal := &runtimeSignalBusHealthFake{err: errors.New("not configured")}
	coordinator := mustRuntimeClusterCoordinator(t, repository, signal, identity, false)
	if got := coordinator.Readiness(context.Background()); !got.Ready {
		t.Fatalf("single-instance readiness = %#v", got)
	}
	if signal.healthCalls != 0 {
		t.Fatalf("single-instance mode called signal health %d times", signal.healthCalls)
	}
}

func TestRuntimeClusterReadinessRejectsMultipleReplicasWithoutDistributedSignalMode(t *testing.T) {
	identity := runtimeClusterTestIdentity()
	repository := newRuntimeClusterRepositoryFake(identity, 2)
	peer := identity
	peer.InstanceID = uuid.New()
	repository.snapshot.LiveMembers = append(repository.snapshot.LiveMembers,
		RuntimeClusterMemberSnapshot{RuntimeClusterIdentity: peer, HeartbeatAt: repository.snapshot.DatabaseTime, Ready: true})
	coordinator := mustRuntimeClusterCoordinator(t, repository, &runtimeSignalBusHealthFake{}, identity, false)

	got := coordinator.Readiness(context.Background())
	if got.Ready || !containsRuntimeClusterReason(got.Reasons, "signal_dependency_unavailable") {
		t.Fatalf("multi-replica local-bus readiness = %#v", got)
	}
}

func TestRuntimeClusterHeartbeatRegistersAndCloseDeletesMember(t *testing.T) {
	identity := runtimeClusterTestIdentity()
	repository := newRuntimeClusterRepositoryFake(identity, 1)
	repository.snapshot.LiveMembers = nil
	coordinator, err := newRuntimeClusterCoordinator(
		repository,
		&runtimeSignalBusHealthFake{},
		identity,
		true,
		5*time.Millisecond,
		15*time.Millisecond,
	)
	if err != nil {
		t.Fatalf("new coordinator: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	coordinator.Start(ctx)
	deadline := time.Now().Add(time.Second)
	for repository.upsertCount() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if repository.upsertCount() < 2 {
		t.Fatalf("heartbeat upserts = %d, want at least 2", repository.upsertCount())
	}
	cancel()
	closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
	defer closeCancel()
	if err = coordinator.Close(closeCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if repository.closedID != identity.InstanceID {
		t.Fatalf("closed member = %s, want %s", repository.closedID, identity.InstanceID)
	}
}

func TestRequireRuntimeClusterOperationModes(t *testing.T) {
	tests := []struct {
		mode      RuntimeClusterMode
		operation RuntimeClusterOperation
		wantError bool
	}{
		{RuntimeClusterModeNormal, RuntimeClusterNewRun, false},
		{RuntimeClusterModeNormal, RuntimeClusterNewSession, false},
		{RuntimeClusterModeNormal, RuntimeClusterClaim, false},
		{RuntimeClusterModeDraining, RuntimeClusterNewRun, true},
		{RuntimeClusterModeDraining, RuntimeClusterNewSession, false},
		{RuntimeClusterModeDraining, RuntimeClusterClaim, false},
		{RuntimeClusterModeHardMaintenance, RuntimeClusterNewRun, true},
		{RuntimeClusterModeHardMaintenance, RuntimeClusterNewSession, true},
		{RuntimeClusterModeHardMaintenance, RuntimeClusterClaim, true},
	}
	for _, tt := range tests {
		t.Run(string(tt.mode)+"/"+string(tt.operation), func(t *testing.T) {
			querier := &runtimeClusterGateQuerierFake{mode: tt.mode}
			err := RequireRuntimeClusterOperation(context.Background(), querier, tt.operation)
			if tt.wantError {
				var httpErr *httpx.HTTPError
				if !errors.As(err, &httpErr) || httpErr.Status != 503 || httpErr.Code != httpx.CodeServiceUnavailable {
					t.Fatalf("gate error = %#v", err)
				}
			} else if err != nil {
				t.Fatalf("gate error = %v", err)
			}
			if querier.queryCalls != 1 || !strings.Contains(querier.statement, "FOR SHARE") {
				t.Fatalf("gate query calls/sql = %d/%q", querier.queryCalls, querier.statement)
			}
		})
	}
}

func TestRequireRuntimeClusterOperationFailsClosedWhenControlCannotBeRead(t *testing.T) {
	err := RequireRuntimeClusterOperation(context.Background(), &runtimeClusterGateQuerierFake{
		err: errors.New("database unavailable"),
	}, RuntimeClusterNewRun)
	var httpErr *httpx.HTTPError
	if !errors.As(err, &httpErr) || httpErr.Status != 503 || httpErr.Code != httpx.CodeServiceUnavailable {
		t.Fatalf("gate error = %#v", err)
	}
}

func TestNewRuntimeClusterCoordinatorRejectsUnsafeIdentity(t *testing.T) {
	identity := runtimeClusterTestIdentity()
	identity.SchemaChecksum = "not-a-digest"
	if _, err := newRuntimeClusterCoordinator(
		newRuntimeClusterRepositoryFake(runtimeClusterTestIdentity(), 1),
		nil,
		identity,
		false,
		time.Second,
		3*time.Second,
	); err == nil {
		t.Fatal("invalid schema checksum should fail")
	}
}

func runtimeClusterTestIdentity() RuntimeClusterIdentity {
	return RuntimeClusterIdentity{
		InstanceID:            uuid.New(),
		ReleaseVersion:        "20260712-test",
		ReleaseCommit:         "0123456789abcdef",
		SchemaVersion:         RuntimeSchemaVersion,
		SchemaChecksum:        RuntimeSchemaChecksum,
		RuntimeContractID:     RuntimeContractID,
		RuntimeContractDigest: RuntimeContractDigest,
	}
}

func mustRuntimeClusterCoordinator(
	t *testing.T,
	repository RuntimeClusterRepository,
	signal RuntimeSignalBus,
	identity RuntimeClusterIdentity,
	requireSignal bool,
) *RuntimeClusterCoordinator {
	t.Helper()
	coordinator, err := newRuntimeClusterCoordinator(
		repository,
		signal,
		identity,
		requireSignal,
		time.Second,
		3*time.Second,
	)
	if err != nil {
		t.Fatalf("new coordinator: %v", err)
	}
	return coordinator
}

func containsRuntimeClusterReason(reasons []string, want string) bool {
	for _, reason := range reasons {
		if reason == want {
			return true
		}
	}
	return false
}

type runtimeClusterRepositoryFake struct {
	mu          sync.Mutex
	snapshot    RuntimeClusterSnapshot
	snapshotErr error
	upserts     []RuntimeClusterMemberSnapshot
	upsertErr   error
	closedID    uuid.UUID
	closeErr    error
}

func newRuntimeClusterRepositoryFake(identity RuntimeClusterIdentity, expected int32) *runtimeClusterRepositoryFake {
	now := time.Date(2026, 7, 12, 1, 2, 3, 0, time.UTC)
	return &runtimeClusterRepositoryFake{snapshot: RuntimeClusterSnapshot{
		DatabaseTime: now,
		Control: RuntimeClusterControlSnapshot{
			Mode: RuntimeClusterModeNormal, ExpectedReplicas: expected,
		},
		CurrentSchema: RuntimeSchemaContractSnapshot{
			SchemaVersion: identity.SchemaVersion, MigrationName: RuntimeSchemaMigrationName,
			RuntimeContractID: identity.RuntimeContractID, RuntimeContractDigest: identity.RuntimeContractDigest,
		},
		LiveMembers: []RuntimeClusterMemberSnapshot{
			{RuntimeClusterIdentity: identity, HeartbeatAt: now, Ready: true},
		},
	}}
}

func (f *runtimeClusterRepositoryFake) UpsertMember(_ context.Context, identity RuntimeClusterIdentity, draining, ready bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.upsertErr != nil {
		return f.upsertErr
	}
	member := RuntimeClusterMemberSnapshot{
		RuntimeClusterIdentity: identity,
		HeartbeatAt:            f.snapshot.DatabaseTime,
		Draining:               draining,
		Ready:                  ready,
	}
	f.upserts = append(f.upserts, member)
	found := false
	for i := range f.snapshot.LiveMembers {
		if f.snapshot.LiveMembers[i].InstanceID == identity.InstanceID {
			f.snapshot.LiveMembers[i] = member
			found = true
		}
	}
	if !found {
		f.snapshot.LiveMembers = append(f.snapshot.LiveMembers, member)
	}
	return nil
}

func (f *runtimeClusterRepositoryFake) Snapshot(context.Context, time.Duration) (RuntimeClusterSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.snapshotErr != nil {
		return RuntimeClusterSnapshot{}, f.snapshotErr
	}
	copySnapshot := f.snapshot
	copySnapshot.LiveMembers = append([]RuntimeClusterMemberSnapshot(nil), f.snapshot.LiveMembers...)
	return copySnapshot, nil
}

func (f *runtimeClusterRepositoryFake) CloseMember(_ context.Context, id uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closedID = id
	return f.closeErr
}

func (f *runtimeClusterRepositoryFake) upsertCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.upserts)
}

type runtimeSignalBusHealthFake struct {
	err         error
	healthCalls int
}

func (*runtimeSignalBusHealthFake) Publish(context.Context, RuntimeSignal) error { return nil }
func (*runtimeSignalBusHealthFake) Subscribe(ctx context.Context, _ RuntimeSignalHandler) error {
	<-ctx.Done()
	return ctx.Err()
}
func (f *runtimeSignalBusHealthFake) Health(context.Context) error {
	f.healthCalls++
	return f.err
}
func (*runtimeSignalBusHealthFake) Close() error { return nil }

type runtimeClusterGateQuerierFake struct {
	mode       RuntimeClusterMode
	err        error
	queryCalls int
	statement  string
}

func (f *runtimeClusterGateQuerierFake) QueryRow(_ context.Context, statement string, _ ...any) pgx.Row {
	f.queryCalls++
	f.statement = statement
	return runtimeClusterGateRowFake{mode: f.mode, err: f.err}
}

type runtimeClusterGateRowFake struct {
	mode RuntimeClusterMode
	err  error
}

func (f runtimeClusterGateRowFake) Scan(dest ...any) error {
	if f.err != nil {
		return f.err
	}
	mode, ok := dest[0].(*RuntimeClusterMode)
	if !ok {
		return errors.New("unexpected scan destination")
	}
	*mode = f.mode
	return nil
}
