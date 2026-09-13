package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestRuntimeDispatchWakeReconcilerWakesOnlyRegisteredPendingAgents(t *testing.T) {
	localAgentID := uuid.New()
	remoteAgentID := uuid.New()
	repository := &runtimeDispatchWakeRepositoryFake{pages: [][]uuid.UUID{{localAgentID, remoteAgentID}}}
	hub := NewRuntimeWakeHub()
	dispatch := hub.WaitDispatch(localAgentID)
	reconciler := newRuntimeDispatchWakeReconciler(repository, hub)

	result, err := reconciler.ReconcileOnce(context.Background(), 32)
	require.NoError(t, err)
	require.Equal(t, RuntimeDispatchWakeReconcileResult{Scanned: 2, Woken: 1, Queued: 1, AgentIDs: []string{localAgentID.String()}}, result)
	select {
	case <-dispatch:
	default:
		t.Fatal("pending local Agent was not woken")
	}
	require.Len(t, hub.channels, 1, "remote backlog must not allocate a local wake entry")
}

func TestRuntimeDispatchWakeReconcilerBoundsAgentEvidenceAndReportsCoalescing(t *testing.T) {
	agents := make([]uuid.UUID, maxRuntimeDispatchWakeLogAgentIDs+3)
	hub := NewRuntimeWakeHub()
	for i := range agents {
		agents[i] = uuid.New()
		hub.WaitDispatch(agents[i])
		if i%2 == 0 {
			hub.WakeDispatch(agents[i])
		}
	}
	repository := &runtimeDispatchWakeRepositoryFake{pages: [][]uuid.UUID{agents}}
	result, err := newRuntimeDispatchWakeReconciler(repository, hub).ReconcileOnce(context.Background(), 100)
	require.NoError(t, err)
	require.Equal(t, len(agents), result.Woken)
	require.Equal(t, len(agents)/2, result.Queued)
	require.Equal(t, (len(agents)+1)/2, result.Coalesced)
	require.Len(t, result.AgentIDs, maxRuntimeDispatchWakeLogAgentIDs)
	for i, id := range result.AgentIDs {
		require.Equal(t, agents[i].String(), id)
	}
	for _, agent := range agents {
		select {
		case <-hub.WaitDispatch(agent):
		default:
			t.Fatal("all registered Agents still receive a token beyond the logging limit")
		}
	}
}

func TestRuntimeDispatchWakeReconcilerAdvancesAndWrapsBoundedCursor(t *testing.T) {
	first := uuid.MustParse("10000000-0000-0000-0000-000000000000")
	second := uuid.MustParse("20000000-0000-0000-0000-000000000000")
	repository := &runtimeDispatchWakeRepositoryFake{pages: [][]uuid.UUID{{first, second}, {}, {first}}}
	reconciler := newRuntimeDispatchWakeReconciler(repository, NewRuntimeWakeHub())

	result, err := reconciler.ReconcileOnce(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, 2, result.Scanned)
	require.False(t, result.Wrapped)
	require.Len(t, repository.after, 1)
	require.Nil(t, repository.after[0])

	result, err = reconciler.ReconcileOnce(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, 1, result.Scanned)
	require.True(t, result.Wrapped)
	require.Len(t, repository.after, 3)
	require.Equal(t, second, *repository.after[1])
	require.Nil(t, repository.after[2])
}

func TestRuntimeDispatchWakeReconcilerPreservesCursorAfterRepositoryFailure(t *testing.T) {
	last := uuid.MustParse("30000000-0000-0000-0000-000000000000")
	repository := &runtimeDispatchWakeRepositoryFake{
		pages: [][]uuid.UUID{{last}, nil}, errs: []error{nil, errors.New("database unavailable")},
	}
	reconciler := newRuntimeDispatchWakeReconciler(repository, NewRuntimeWakeHub())
	_, err := reconciler.ReconcileOnce(context.Background(), 1)
	require.NoError(t, err)
	_, err = reconciler.ReconcileOnce(context.Background(), 1)
	require.ErrorContains(t, err, "database unavailable")
	require.Equal(t, last, *reconciler.cursor)
}

type runtimeDispatchWakeRepositoryFake struct {
	mu    sync.Mutex
	pages [][]uuid.UUID
	errs  []error
	after []*uuid.UUID
}

func (f *runtimeDispatchWakeRepositoryFake) ListClaimableRuntimeAgentIDs(
	_ context.Context,
	after *uuid.UUID,
	_ int,
) ([]uuid.UUID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if after == nil {
		f.after = append(f.after, nil)
	} else {
		copyAfter := *after
		f.after = append(f.after, &copyAfter)
	}
	index := len(f.after) - 1
	if index < len(f.errs) && f.errs[index] != nil {
		return nil, f.errs[index]
	}
	if index >= len(f.pages) {
		return nil, nil
	}
	return append([]uuid.UUID(nil), f.pages[index]...), nil
}
