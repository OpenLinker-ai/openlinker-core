package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

func TestPreferWaitQueriesOnlyInitiallyAndAfterRunWake(t *testing.T) {
	runID, userID := uuid.New(), uuid.New()
	service := &runUpdateRuntimeServiceFake{
		mockRuntimeService: &mockRuntimeService{},
		status:             "running",
		firstQuery:         make(chan struct{}),
	}
	updates := newRuntimeRunUpdateSourceFake()
	handler := NewHandler(service)
	handler.SetRunUpdateSource(updates)
	var observationsMu sync.Mutex
	var observations []WorkerObservation
	handler.SetWorkerObserver(WorkerObserverFunc(func(observation WorkerObservation) {
		observationsMu.Lock()
		observations = append(observations, observation)
		observationsMu.Unlock()
	}))

	e := echo.New()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/runs", nil)
	request.Header.Set("Prefer", "wait=1")
	recorder := httptest.NewRecorder()
	echoContext := e.NewContext(request, recorder)
	done := make(chan error, 1)
	go func() {
		done <- handler.sendRunCreationResponse(echoContext, userID, &RunResponse{
			RunID: runID.String(), Status: "running",
		})
	}()

	select {
	case <-service.firstQuery:
	case <-time.After(time.Second):
		t.Fatal("Prefer wait did not perform its subscribe-before-query read")
	}
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, 1, service.calls(), "healthy event wait must not run the 100ms database ticker")
	service.complete()
	updates.publish(runID)
	require.NoError(t, <-done)
	require.Equal(t, http.StatusCreated, recorder.Code)
	var response RunResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.Equal(t, "success", response.Status)
	require.Equal(t, "wait=1", recorder.Header().Get("Preference-Applied"))
	require.Equal(t, 2, service.calls())

	observationsMu.Lock()
	defer observationsMu.Unlock()
	require.Equal(t, []WorkerObservation{
		{Category: "runtime.prefer_wait.run_query", Reason: "event_initial", BatchSize: 1},
		{Category: "runtime.prefer_wait.run_query", Reason: "event_wake", BatchSize: 1},
	}, observations)
}

func TestRunSSEQueriesOnlyInitiallyAndAfterRunWake(t *testing.T) {
	runID, userID := uuid.New(), uuid.New()
	service := &runUpdateEventPageServiceFake{
		mockRuntimeService: &mockRuntimeService{}, firstQuery: make(chan struct{}),
	}
	updates := newRuntimeRunUpdateSourceFake()
	handler := NewHandler(service)
	handler.SetRunUpdateSource(updates)
	var observationsMu sync.Mutex
	var observations []WorkerObservation
	handler.SetWorkerObserver(WorkerObserverFunc(func(observation WorkerObservation) {
		observationsMu.Lock()
		observations = append(observations, observation)
		observationsMu.Unlock()
	}))
	streamContext, recorder := newRuntimeDispatchContext(&runtimeDispatchRequest{
		method: http.MethodGet, target: "/api/v1/runs/" + runID.String() + "/stream",
		userID: userID.String(), authMethod: "jwt", params: map[string]string{"id": runID.String()},
	})
	done := make(chan error, 1)
	go func() { done <- handler.StreamRunEvents(streamContext) }()
	select {
	case <-service.firstQuery:
	case <-time.After(time.Second):
		t.Fatal("Run SSE did not perform its initial replay query")
	}
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, 1, service.calls(), "healthy Run SSE must not poll PostgreSQL while idle")
	service.complete(runID)
	updates.publish(runID)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Run SSE did not wake for the terminal event")
	}
	require.Equal(t, 2, service.calls())
	require.Contains(t, recorder.Body.String(), "event: run.completed")
	observationsMu.Lock()
	defer observationsMu.Unlock()
	require.Equal(t, []WorkerObservation{
		{Category: "runtime.sse.run_events_query", Reason: "initial", BatchSize: 1},
		{Category: "runtime.sse.run_events_query", Reason: "event_wake", BatchSize: 1},
	}, observations)
}

type runUpdateRuntimeServiceFake struct {
	*mockRuntimeService
	mu         sync.Mutex
	status     string
	queryCalls int
	firstQuery chan struct{}
	firstOnce  sync.Once
}

type runUpdateEventPageServiceFake struct {
	*mockRuntimeService
	mu         sync.Mutex
	queryCalls int
	terminal   *RunEventResponse
	firstQuery chan struct{}
	firstOnce  sync.Once
}

func (f *runUpdateEventPageServiceFake) ListRunEventsPage(
	_ context.Context,
	_ uuid.UUID,
	runID uuid.UUID,
	afterSequence, _ int32,
) (*RunEventPageResponse, error) {
	f.mu.Lock()
	f.queryCalls++
	terminal := f.terminal
	f.mu.Unlock()
	f.firstOnce.Do(func() { close(f.firstQuery) })
	page := &RunEventPageResponse{Meta: RunEventPageMeta{
		RequestedAfterSequence: afterSequence, EffectiveAfterSequence: afterSequence,
	}}
	if terminal != nil {
		page.Items = []RunEventResponse{*terminal}
		page.Meta.Terminal = true
		page.Meta.StreamComplete = true
	}
	return page, nil
}

func (f *runUpdateEventPageServiceFake) complete(runID uuid.UUID) {
	f.mu.Lock()
	f.terminal = &RunEventResponse{
		EventID: uuid.NewString(), RunID: runID.String(), Sequence: 1,
		EventType: "run.completed", Payload: map[string]interface{}{"status": "success"},
		CreatedAt: time.Now().UTC(),
	}
	f.mu.Unlock()
}

func (f *runUpdateEventPageServiceFake) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.queryCalls
}

func (f *runUpdateRuntimeServiceFake) GetRun(
	_ context.Context,
	_ uuid.UUID,
	runID uuid.UUID,
) (*RunResponse, error) {
	f.mu.Lock()
	f.queryCalls++
	status := f.status
	f.mu.Unlock()
	f.firstOnce.Do(func() { close(f.firstQuery) })
	return &RunResponse{RunID: runID.String(), Status: status}, nil
}

func (f *runUpdateRuntimeServiceFake) complete() {
	f.mu.Lock()
	f.status = "success"
	f.mu.Unlock()
}

func (f *runUpdateRuntimeServiceFake) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.queryCalls
}

type runtimeRunUpdateSourceFake struct {
	mu            sync.Mutex
	subscriptions map[uuid.UUID]*runtimeRunUpdateSubscriptionFake
	healthy       bool
}

func newRuntimeRunUpdateSourceFake() *runtimeRunUpdateSourceFake {
	return &runtimeRunUpdateSourceFake{
		subscriptions: make(map[uuid.UUID]*runtimeRunUpdateSubscriptionFake), healthy: true,
	}
}

func (f *runtimeRunUpdateSourceFake) SubscribeRun(runID uuid.UUID) (RunUpdateSubscription, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	subscription := &runtimeRunUpdateSubscriptionFake{wake: make(chan struct{}, 1)}
	f.subscriptions[runID] = subscription
	return subscription, nil
}

func (f *runtimeRunUpdateSourceFake) Healthy() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.healthy
}

func (f *runtimeRunUpdateSourceFake) publish(runID uuid.UUID) {
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

type runtimeRunUpdateSubscriptionFake struct {
	wake chan struct{}
}

func (s *runtimeRunUpdateSubscriptionFake) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.wake:
		return nil
	}
}

func (s *runtimeRunUpdateSubscriptionFake) Close() {}
