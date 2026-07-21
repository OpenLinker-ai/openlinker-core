package runtime

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

func TestCoreAttemptRegistrySignalCancelsImmediately(t *testing.T) {
	registry := newCoreAttemptRegistry(&coreCancellationPollFake{}, time.Second)
	execution := testCoreAttemptExecution()
	callCtx, unregister := registry.register(context.Background(), execution)
	registry.cancelRun(execution.identity.RunID)

	select {
	case <-callCtx.Done():
		require.ErrorIs(t, context.Cause(callCtx), errCoreAttemptOwnerCanceled)
	case <-time.After(time.Second):
		t.Fatal("local run.cancel signal did not cancel the Core attempt")
	}
	unregister()
}

func TestCoreAttemptRegistryFallsBackToDatabasePoll(t *testing.T) {
	queries := &coreCancellationPollFake{}
	registry := newCoreAttemptRegistry(queries, 10*time.Millisecond)
	execution := testCoreAttemptExecution()
	queries.rows = []db.ListRequestedCoreAttemptCancellationsRow{{
		RunID: execution.identity.RunID, AttemptID: execution.identity.AttemptID,
		LeaseID: execution.identity.LeaseID, FencingToken: execution.identity.FencingToken,
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry.startCancellationCoordinator(ctx)
	callCtx, unregister := registry.register(context.Background(), execution)
	queries.requested.Store(true)

	select {
	case <-callCtx.Done():
		require.ErrorIs(t, context.Cause(callCtx), errCoreAttemptOwnerCanceled)
	case <-time.After(time.Second):
		t.Fatal("database cancellation poll did not stop the Core attempt")
	}
	require.Greater(t, queries.calls.Load(), int32(0))
	unregister()
}

func TestCoreAttemptRegistryBatchesCancellationFallback(t *testing.T) {
	queries := &coreCancellationPollFake{}
	registry := newCoreAttemptRegistry(queries, time.Second)
	executions := []coreAttemptExecution{testCoreAttemptExecution(), testCoreAttemptExecution()}
	contexts := make([]context.Context, 0, len(executions))
	for _, execution := range executions {
		callCtx, unregister := registry.register(context.Background(), execution)
		defer unregister()
		contexts = append(contexts, callCtx)
		queries.rows = append(queries.rows, db.ListRequestedCoreAttemptCancellationsRow{
			RunID: execution.identity.RunID, AttemptID: execution.identity.AttemptID,
			LeaseID: execution.identity.LeaseID, FencingToken: execution.identity.FencingToken,
		})
	}
	queries.requested.Store(true)
	registry.reconcileCancellations(context.Background())
	require.Equal(t, int32(1), queries.calls.Load(), "one coordinator pass must issue one query")
	for _, callCtx := range contexts {
		require.ErrorIs(t, context.Cause(callCtx), errCoreAttemptOwnerCanceled)
	}
}

func TestCoreAttemptRegistryRejectsStaleBatchIdentity(t *testing.T) {
	queries := &coreCancellationPollFake{requested: atomic.Bool{}}
	registry := newCoreAttemptRegistry(queries, time.Second)
	execution := testCoreAttemptExecution()
	callCtx, unregister := registry.register(context.Background(), execution)
	defer unregister()
	queries.rows = []db.ListRequestedCoreAttemptCancellationsRow{{
		RunID: execution.identity.RunID, AttemptID: execution.identity.AttemptID,
		LeaseID: uuid.New(), FencingToken: execution.identity.FencingToken,
	}}
	queries.requested.Store(true)
	registry.reconcileCancellations(context.Background())
	select {
	case <-callCtx.Done():
		t.Fatal("stale lease identity canceled the active Core Attempt")
	default:
	}
}

func TestCoreAttemptRegistryCapsCancellationPollAtTwoSeconds(t *testing.T) {
	registry := newCoreAttemptRegistry(&coreCancellationPollFake{}, time.Hour)
	require.Equal(t, defaultCoreCancellationPollInterval, registry.pollEvery)
}

func testCoreAttemptExecution() coreAttemptExecution {
	return coreAttemptExecution{
		identity: RuntimeAttemptIdentity{
			RunID: uuid.New(), AttemptID: uuid.New(), LeaseID: uuid.New(),
			FencingToken: 1, AgentID: uuid.New(),
		},
		attemptNo:  1,
		deadlineAt: time.Now().Add(time.Minute),
	}
}

type coreCancellationPollFake struct {
	requested atomic.Bool
	calls     atomic.Int32
	rows      []db.ListRequestedCoreAttemptCancellationsRow
}

func (f *coreCancellationPollFake) ListRequestedCoreAttemptCancellations(
	context.Context,
	[]uuid.UUID,
) ([]db.ListRequestedCoreAttemptCancellationsRow, error) {
	f.calls.Add(1)
	if !f.requested.Load() {
		return nil, nil
	}
	return append([]db.ListRequestedCoreAttemptCancellationsRow(nil), f.rows...), nil
}
