package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

func TestNewRuntimeSessionReaperNormalizesNilLeaseManager(t *testing.T) {
	reaper := NewRuntimeSessionReaperWithLeases(nil, 2*time.Minute, nil)
	require.Nil(t, reaper.leases)
	require.ErrorIs(t, func() error {
		_, err := reaper.ReapStaleSessions(context.Background(), 32)
		return err
	}(), ErrRuntimeSessionReaperNotConfigured)
}

func TestRuntimeSessionReaperClosesExactStaleAttachment(t *testing.T) {
	fixture := newSessionFixture()
	tx := newSessionTransactionFake(fixture)
	candidate := tx.session
	repository := &sessionRepositoryFake{tx: tx, staleCandidates: []db.RuntimeSession{candidate}}
	reaper := newRuntimeSessionReaper(repository, 2*time.Minute)

	reaped, err := reaper.ReapStaleSessions(context.Background(), 32)
	require.NoError(t, err)
	require.Equal(t, 1, reaped)
	require.Equal(t, 2*time.Minute, repository.staleTTL)
	require.Equal(t, 32, repository.staleLimit)
	require.Equal(t, []string{
		"lock_session_identity", "lock_lifecycle_sessions", "lock_nodes", "lock_tokens", "lock_attachments",
		"get_session_for_update", "get_attachment", "close_attachment", "close_stale_session",
	}, tx.operations)
	require.Equal(t, tx.attachment.ID, tx.closeAttachmentParams.AttachmentID)
	require.Equal(t, "offline", tx.session.Status)
}

func TestRuntimeSessionReaperLeaseEvidenceIsFailSafe(t *testing.T) {
	fixture := newSessionFixture()
	candidate := newSessionTransactionFake(fixture).session
	tests := []struct {
		name         string
		leases       *runtimeSessionLeaseEvidenceFake
		wantReaped   int
		wantErr      bool
		wantDBAccess bool
		wantLookups  int
	}{
		{
			name: "live lease skips database close",
			leases: &runtimeSessionLeaseEvidenceFake{
				lease: RuntimeSessionLease{RuntimeSessionID: candidate.RuntimeSessionID}, found: true, absenceReady: true,
			},
			wantLookups: 1,
		},
		{
			name: "startup absence is not trusted",
			leases: &runtimeSessionLeaseEvidenceFake{
				absenceReady: false,
			},
			wantLookups: 1,
		},
		{
			name: "lookup failure cannot close session",
			leases: &runtimeSessionLeaseEvidenceFake{
				lookupErr: errors.New("redis unavailable"), absenceReady: true,
			},
			wantErr:     true,
			wantLookups: 1,
		},
		{
			name: "warmed healthy absence permits fenced database close",
			leases: &runtimeSessionLeaseEvidenceFake{
				absenceReady: true,
			},
			wantReaped: 1, wantDBAccess: true, wantLookups: 2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.leases.expired = []uuid.UUID{candidate.RuntimeSessionID}
			tx := newSessionTransactionFake(fixture)
			repository := &sessionRepositoryFake{tx: tx, staleCandidates: []db.RuntimeSession{candidate}}
			reaper := newRuntimeSessionReaperWithLeases(repository, 2*time.Minute, test.leases)
			reaped, err := reaper.ReapStaleSessions(context.Background(), 32)
			if test.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, test.wantReaped, reaped)
			require.Equal(t, test.wantDBAccess, len(tx.operations) > 0)
			require.Equal(t, test.wantLookups, test.leases.lookups)
		})
	}
}

func TestRuntimeSessionReaperDefersRedisAbsenceWhileDatabaseHeartbeatIsFresh(t *testing.T) {
	fixture := newSessionFixture()
	tx := newSessionTransactionFake(fixture)
	candidate := tx.session
	repository := &sessionRepositoryFake{
		tx: tx, reapCandidateNow: candidate.HeartbeatAt.Add(time.Minute),
	}
	leases := &runtimeSessionLeaseEvidenceFake{
		expired: []uuid.UUID{candidate.RuntimeSessionID}, absenceReady: true,
	}
	reaped, err := newRuntimeSessionReaperWithLeases(
		repository, 2*time.Minute, leases,
	).ReapStaleSessions(context.Background(), 32)
	require.NoError(t, err)
	require.Zero(t, reaped)
	require.Empty(t, tx.operations)
	require.Len(t, leases.scheduled, 1)
	require.InDelta(t, float64(time.Minute), float64(leases.scheduled[0]), float64(time.Millisecond))
}

func TestRuntimeSessionReaperReconcilesDatabaseAfterRedisExpiryIndexLoss(t *testing.T) {
	fixture := newSessionFixture()
	tx := newSessionTransactionFake(fixture)
	candidate := tx.session
	repository := &sessionRepositoryFake{tx: tx, staleCandidates: []db.RuntimeSession{candidate}}
	leases := &runtimeSessionLeaseEvidenceFake{absenceReady: true}
	reaper := newRuntimeSessionReaperWithLeases(repository, 2*time.Minute, leases)
	reaper.nextDatabaseReconcileAt = time.Time{}

	reaped, err := reaper.ReapStaleSessions(context.Background(), 32)
	require.NoError(t, err)
	require.Equal(t, 1, reaped)
	require.Equal(t, 1, repository.staleListCalls)
	require.Equal(t, 1, leases.lookups)
	require.Equal(t, "offline", tx.session.Status)

	_, err = reaper.ReapStaleSessions(context.Background(), 32)
	require.NoError(t, err)
	require.Equal(t, 1, repository.staleListCalls, "database reconciliation must remain process-level and low-frequency")
}

func TestRuntimeSessionReaperDoesNotReconcileDatabaseBeforeRedisAbsenceWarmup(t *testing.T) {
	fixture := newSessionFixture()
	repository := &sessionRepositoryFake{
		tx:              newSessionTransactionFake(fixture),
		staleCandidates: []db.RuntimeSession{newSessionTransactionFake(fixture).session},
	}
	reaper := newRuntimeSessionReaperWithLeases(
		repository, 2*time.Minute, &runtimeSessionLeaseEvidenceFake{absenceReady: false},
	)
	reaper.nextDatabaseReconcileAt = time.Time{}

	reaped, err := reaper.ReapStaleSessions(context.Background(), 32)
	require.NoError(t, err)
	require.Zero(t, reaped)
	require.Zero(t, repository.staleListCalls)
}

func TestRuntimeSessionReaperDoesNotHotLoopOnFullPageOfLiveRedisLeases(t *testing.T) {
	fixture := newSessionFixture()
	tx := newSessionTransactionFake(fixture)
	repository := &sessionRepositoryFake{tx: tx, staleCandidates: []db.RuntimeSession{tx.session}}
	reaper := newRuntimeSessionReaperWithLeases(
		repository,
		2*time.Minute,
		&runtimeSessionLeaseEvidenceFake{found: true, absenceReady: true},
	)
	reaper.nextDatabaseReconcileAt = time.Time{}

	reaped, err := reaper.ReapStaleSessions(context.Background(), 1)
	require.NoError(t, err)
	require.Zero(t, reaped)
	require.Equal(t, 1, repository.staleListCalls)

	_, err = reaper.ReapStaleSessions(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, 1, repository.staleListCalls, "live lease pages must wait for the next minute reconciliation")
}

func TestRuntimeSessionReaperSkipsHeartbeatAdvancedAfterDiscovery(t *testing.T) {
	fixture := newSessionFixture()
	tx := newSessionTransactionFake(fixture)
	candidate := tx.session
	tx.session.HeartbeatAt = tx.session.HeartbeatAt.Add(time.Second)
	repository := &sessionRepositoryFake{tx: tx, staleCandidates: []db.RuntimeSession{candidate}}

	reaped, err := newRuntimeSessionReaper(repository, 2*time.Minute).ReapStaleSessions(context.Background(), 32)
	require.NoError(t, err)
	require.Zero(t, reaped)
	require.NotContains(t, tx.operations, "close_attachment")
	require.NotContains(t, tx.operations, "close_stale_session")
}

func TestRuntimeSessionReaperConcurrentPassesCloseOnlyOnce(t *testing.T) {
	fixture := newSessionFixture()
	tx := newSessionTransactionFake(fixture)
	candidate := tx.session
	repository := &sessionRepositoryFake{tx: tx, staleCandidates: []db.RuntimeSession{candidate}}
	reaper := newRuntimeSessionReaper(repository, 2*time.Minute)

	var wait sync.WaitGroup
	results := make(chan int, 2)
	errors := make(chan error, 2)
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			count, err := reaper.ReapStaleSessions(context.Background(), 32)
			results <- count
			errors <- err
		}()
	}
	wait.Wait()
	close(results)
	close(errors)

	total := 0
	for count := range results {
		total += count
	}
	for err := range errors {
		require.NoError(t, err)
	}
	require.Equal(t, 1, total)
}

type runtimeSessionLeaseEvidenceFake struct {
	lease        RuntimeSessionLease
	found        bool
	lookupErr    error
	absenceReady bool
	lookups      int
	expired      []uuid.UUID
	scheduled    []time.Duration
	forgotten    []uuid.UUID
}

func (f *runtimeSessionLeaseEvidenceFake) ListExpired(_ context.Context, limit int) ([]uuid.UUID, error) {
	if limit > len(f.expired) {
		limit = len(f.expired)
	}
	return append([]uuid.UUID(nil), f.expired[:limit]...), nil
}

func (f *runtimeSessionLeaseEvidenceFake) ScheduleCheck(
	_ context.Context,
	_ uuid.UUID,
	after time.Duration,
) error {
	f.scheduled = append(f.scheduled, after)
	return nil
}

func (f *runtimeSessionLeaseEvidenceFake) Forget(_ context.Context, runtimeSessionID uuid.UUID) error {
	f.forgotten = append(f.forgotten, runtimeSessionID)
	return nil
}

func (f *runtimeSessionLeaseEvidenceFake) Lookup(
	_ context.Context,
	_ uuid.UUID,
) (RuntimeSessionLease, bool, error) {
	f.lookups++
	return f.lease, f.found, f.lookupErr
}

func (f *runtimeSessionLeaseEvidenceFake) AbsenceReady() bool {
	return f.absenceReady
}
