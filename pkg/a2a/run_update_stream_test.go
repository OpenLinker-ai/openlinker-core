package a2a

import (
	"context"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	a2apb "github.com/OpenLinker-ai/openlinker-core/pkg/a2a/pb"
	coreruntime "github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

func TestA2ASSEQueriesOnlyInitiallyAndAfterRunWake(t *testing.T) {
	runID, userID := uuid.New(), uuid.New()
	service := newA2ARunUpdateServiceFake(runID.String())
	updates := newA2ARunUpdateSourceFake()
	handler := NewHandler(service)
	handler.SetRunUpdateSource(updates)
	request := httptest.NewRequest("GET", "/stream", nil)
	recorder := httptest.NewRecorder()
	echoContext := echo.New().NewContext(request, recorder)
	done := make(chan error, 1)
	go func() {
		done <- handler.streamProtocolTask(
			echoContext, userID, "agent", runID.String(), nil, false, nil, a2aProtocolVersionCurrent,
		)
	}()

	service.waitForFirstQuery(t)
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, 1, service.queryCount())
	service.complete()
	updates.publish(runID)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("A2A SSE did not wake for the terminal Run event")
	}
	require.Equal(t, 2, service.queryCount())
	require.Contains(t, recorder.Body.String(), "status-update")
}

func TestA2AGRPCQueriesOnlyInitiallyAndAfterRunWake(t *testing.T) {
	runID, userID := uuid.New(), uuid.New()
	service := newA2ARunUpdateServiceFake(runID.String())
	updates := newA2ARunUpdateSourceFake()
	server := NewGRPCServer(service, nil, nil)
	server.SetRunUpdateSource(updates)
	var sentMu sync.Mutex
	var sent []*a2apb.StreamResponse
	done := make(chan error, 1)
	go func() {
		done <- server.streamTask(
			context.Background(),
			func(response *a2apb.StreamResponse) error {
				sentMu.Lock()
				sent = append(sent, response)
				sentMu.Unlock()
				return nil
			},
			userID, "agent", runID.String(), nil,
		)
	}()

	service.waitForFirstQuery(t)
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, 1, service.queryCount())
	service.complete()
	updates.publish(runID)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("A2A gRPC did not wake for the terminal Run event")
	}
	require.Equal(t, 2, service.queryCount())
	sentMu.Lock()
	defer sentMu.Unlock()
	require.Len(t, sent, 1)
	require.Equal(t, a2apb.TaskState_TASK_STATE_COMPLETED, sent[0].GetStatusUpdate().GetStatus().GetState())
}

type a2aRunUpdateServiceFake struct {
	*fakeA2AService
	mu         sync.Mutex
	queries    int
	terminal   bool
	firstQuery chan struct{}
	firstOnce  sync.Once
}

func newA2ARunUpdateServiceFake(taskID string) *a2aRunUpdateServiceFake {
	return &a2aRunUpdateServiceFake{
		fakeA2AService: newFakeA2AService(taskID), firstQuery: make(chan struct{}),
	}
}

func (f *a2aRunUpdateServiceFake) ListProtocolTaskEvents(
	_ context.Context,
	_ uuid.UUID,
	_ string,
	taskID string,
	afterSequence int32,
) ([]interface{}, bool, int32, error) {
	f.mu.Lock()
	f.queries++
	terminal := f.terminal
	f.mu.Unlock()
	f.firstOnce.Do(func() { close(f.firstQuery) })
	if !terminal {
		return nil, false, afterSequence, nil
	}
	return []interface{}{&A2ATaskStatusUpdateEvent{
		Kind: "status-update", TaskID: taskID, ContextID: "ctx-event",
		Status: A2ATaskStatus{State: a2aTaskStateCompleted, Timestamp: "2026-07-20T00:00:00Z"},
		Final:  true, Metadata: map[string]interface{}{"openlinker_sequence": afterSequence + 1},
	}}, true, afterSequence + 1, nil
}

func (f *a2aRunUpdateServiceFake) waitForFirstQuery(t *testing.T) {
	t.Helper()
	select {
	case <-f.firstQuery:
	case <-time.After(time.Second):
		t.Fatal("A2A stream did not perform its initial query")
	}
}

func (f *a2aRunUpdateServiceFake) complete() {
	f.mu.Lock()
	f.terminal = true
	f.mu.Unlock()
}

func (f *a2aRunUpdateServiceFake) queryCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.queries
}

type a2aRunUpdateSourceFake struct {
	mu            sync.Mutex
	subscriptions map[uuid.UUID]*a2aRunUpdateSubscriptionFake
}

func newA2ARunUpdateSourceFake() *a2aRunUpdateSourceFake {
	return &a2aRunUpdateSourceFake{
		subscriptions: make(map[uuid.UUID]*a2aRunUpdateSubscriptionFake),
	}
}

func (f *a2aRunUpdateSourceFake) SubscribeRun(runID uuid.UUID) (coreruntime.RunUpdateSubscription, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	subscription := &a2aRunUpdateSubscriptionFake{wake: make(chan struct{}, 1)}
	f.subscriptions[runID] = subscription
	return subscription, nil
}

func (f *a2aRunUpdateSourceFake) Healthy() bool { return true }

func (f *a2aRunUpdateSourceFake) publish(runID uuid.UUID) {
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

type a2aRunUpdateSubscriptionFake struct {
	wake chan struct{}
}

func (s *a2aRunUpdateSubscriptionFake) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.wake:
		return nil
	}
}

func (s *a2aRunUpdateSubscriptionFake) Close() {}
