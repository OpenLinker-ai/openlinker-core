package workflow

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	coreruntime "github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

func TestWorkflowChildRunWaitersAreIndependentAndQueryOnlyAfterWake(t *testing.T) {
	userID, runA, runB := uuid.New(), uuid.New(), uuid.New()
	runtimeFake := newWorkflowRunUpdateRuntimeFake(runA, runB)
	updates := newWorkflowRunUpdateSourceFake()
	service := &Service{runtime: runtimeFake, runUpdates: updates}

	type result struct {
		run *coreruntime.RunResponse
		err error
	}
	resultsA := make(chan result, 1)
	resultsB := make(chan result, 1)
	go func() {
		run, err := service.waitForRuntimeRunCompletion(context.Background(), userID, runA)
		resultsA <- result{run: run, err: err}
	}()
	go func() {
		run, err := service.waitForRuntimeRunCompletion(context.Background(), userID, runB)
		resultsB <- result{run: run, err: err}
	}()

	require.Eventually(t, func() bool {
		return runtimeFake.calls(runA) == 1 && runtimeFake.calls(runB) == 1
	}, time.Second, time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, 1, runtimeFake.calls(runA), "an idle child waiter must not poll PostgreSQL")
	require.Equal(t, 1, runtimeFake.calls(runB), "an idle child waiter must not poll PostgreSQL")

	runtimeFake.complete(runA)
	updates.publish(runA)
	select {
	case completed := <-resultsA:
		require.NoError(t, completed.err)
		require.Equal(t, runtimeRunStatusSuccess, completed.run.Status)
	case <-time.After(time.Second):
		t.Fatal("first child Run did not wake")
	}
	require.Equal(t, 1, runtimeFake.calls(runB), "one branch wake must not be consumed by or wake another branch")

	runtimeFake.complete(runB)
	updates.publish(runB)
	select {
	case completed := <-resultsB:
		require.NoError(t, completed.err)
		require.Equal(t, runtimeRunStatusSuccess, completed.run.Status)
	case <-time.After(time.Second):
		t.Fatal("second child Run did not wake")
	}
	require.Equal(t, 2, runtimeFake.calls(runA))
	require.Equal(t, 2, runtimeFake.calls(runB))
}

func TestWorkflowChildRunWakeBetweenSubscribeAndQueryIsNotLost(t *testing.T) {
	userID, runID := uuid.New(), uuid.New()
	runtimeFake := newWorkflowRunUpdateRuntimeFake(runID)
	updates := newWorkflowRunUpdateSourceFake()
	updates.onSubscribe = func(subscribed uuid.UUID) {
		require.Equal(t, runID, subscribed)
		runtimeFake.complete(runID)
		updates.publish(runID)
	}
	service := &Service{runtime: runtimeFake, runUpdates: updates}
	completed, err := service.waitForRuntimeRunCompletion(context.Background(), userID, runID)
	require.NoError(t, err)
	require.Equal(t, runtimeRunStatusSuccess, completed.Status)
	require.Equal(t, 1, runtimeFake.calls(runID))
}

type workflowRunUpdateRuntimeFake struct {
	mu       sync.Mutex
	statuses map[uuid.UUID]string
	queries  map[uuid.UUID]int
}

func newWorkflowRunUpdateRuntimeFake(runIDs ...uuid.UUID) *workflowRunUpdateRuntimeFake {
	statuses := make(map[uuid.UUID]string, len(runIDs))
	for _, runID := range runIDs {
		statuses[runID] = runtimeRunStatusRunning
	}
	return &workflowRunUpdateRuntimeFake{statuses: statuses, queries: make(map[uuid.UUID]int)}
}

func (f *workflowRunUpdateRuntimeFake) Run(
	context.Context,
	uuid.UUID,
	*coreruntime.RunRequest,
	string,
) (*coreruntime.RunResponse, error) {
	return nil, nil
}

func (f *workflowRunUpdateRuntimeFake) GetRun(
	_ context.Context,
	_ uuid.UUID,
	runID uuid.UUID,
) (*coreruntime.RunResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queries[runID]++
	return &coreruntime.RunResponse{RunID: runID.String(), Status: f.statuses[runID]}, nil
}

func (f *workflowRunUpdateRuntimeFake) complete(runID uuid.UUID) {
	f.mu.Lock()
	f.statuses[runID] = runtimeRunStatusSuccess
	f.mu.Unlock()
}

func (f *workflowRunUpdateRuntimeFake) calls(runID uuid.UUID) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.queries[runID]
}

type workflowRunUpdateSourceFake struct {
	mu            sync.Mutex
	subscriptions map[uuid.UUID]*workflowRunUpdateSubscriptionFake
	onSubscribe   func(uuid.UUID)
}

func newWorkflowRunUpdateSourceFake() *workflowRunUpdateSourceFake {
	return &workflowRunUpdateSourceFake{
		subscriptions: make(map[uuid.UUID]*workflowRunUpdateSubscriptionFake),
	}
}

func (f *workflowRunUpdateSourceFake) SubscribeRun(runID uuid.UUID) (coreruntime.RunUpdateSubscription, error) {
	f.mu.Lock()
	subscription := &workflowRunUpdateSubscriptionFake{wake: make(chan struct{}, 1)}
	f.subscriptions[runID] = subscription
	onSubscribe := f.onSubscribe
	f.mu.Unlock()
	if onSubscribe != nil {
		onSubscribe(runID)
	}
	return subscription, nil
}

func (f *workflowRunUpdateSourceFake) Healthy() bool { return true }

func (f *workflowRunUpdateSourceFake) publish(runID uuid.UUID) {
	f.mu.Lock()
	subscription := f.subscriptions[runID]
	f.mu.Unlock()
	if subscription != nil {
		select {
		case subscription.wake <- struct{}{}:
		default:
		}
	}
}

type workflowRunUpdateSubscriptionFake struct {
	wake chan struct{}
}

func (s *workflowRunUpdateSubscriptionFake) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.wake:
		return nil
	}
}

func (s *workflowRunUpdateSubscriptionFake) Close() {}
