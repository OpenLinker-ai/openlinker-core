package runtime

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

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
		"lock_session_identity", "lock_sessions", "lock_nodes", "lock_tokens", "lock_attachments",
		"get_session_for_update", "get_attachment", "close_attachment", "close_stale_session",
	}, tx.operations)
	require.Equal(t, tx.attachment.ID, tx.closeAttachmentParams.AttachmentID)
	require.Equal(t, "offline", tx.session.Status)
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
