package runtime

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestRuntimeWSInboundSchedulerRunsIndependentAttemptsConcurrently(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := uuid.MustParse("00000000-0000-4000-8000-000000000000")
	second := uuid.MustParse("00000000-0000-4000-8000-000000000001")
	started := make(chan uuid.UUID, 2)
	release := make(chan struct{})
	scheduler := newRuntimeWSInboundScheduler(ctx, runtimeWSInboundSchedulerConfig{
		LaneCount: 2, QueueDepth: 2, ProcessLimiter: newRuntimeWSProcessLimiter(4),
		Handle: func(work runtimeWSInboundWork) {
			started <- work.attemptID
			<-release
		},
	})
	defer scheduler.stop()

	require.NoError(t, scheduler.enqueue(runtimeWSInboundWork{attemptID: first}))
	require.NoError(t, scheduler.enqueue(runtimeWSInboundWork{attemptID: second}))
	require.ElementsMatch(t, []uuid.UUID{first, second}, []uuid.UUID{
		receiveUUID(t, started), receiveUUID(t, started),
	})
	close(release)
	require.NoError(t, scheduler.barrier(ctx))
}

func TestRuntimeWSInboundSchedulerPreservesAttemptFIFO(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	attemptID := uuid.MustParse("00000000-0000-4000-8000-000000000002")
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var mu sync.Mutex
	var order []RuntimeMessageType
	scheduler := newRuntimeWSInboundScheduler(ctx, runtimeWSInboundSchedulerConfig{
		LaneCount: 4, QueueDepth: 4, ProcessLimiter: newRuntimeWSProcessLimiter(4),
		Handle: func(work runtimeWSInboundWork) {
			if work.envelope.Type == RuntimeMessageRunEvent {
				close(firstStarted)
				<-releaseFirst
			}
			mu.Lock()
			order = append(order, work.envelope.Type)
			mu.Unlock()
		},
	})
	defer scheduler.stop()

	require.NoError(t, scheduler.enqueue(runtimeWSInboundWork{
		attemptID: attemptID, envelope: RuntimeEnvelope{RuntimeEnvelopeFields: RuntimeEnvelopeFields{Type: RuntimeMessageRunEvent}},
	}))
	require.NoError(t, scheduler.enqueue(runtimeWSInboundWork{
		attemptID: attemptID, envelope: RuntimeEnvelope{RuntimeEnvelopeFields: RuntimeEnvelopeFields{Type: RuntimeMessageRunResult}},
	}))
	requireReceive(t, firstStarted)
	time.Sleep(20 * time.Millisecond)
	mu.Lock()
	require.Empty(t, order)
	mu.Unlock()
	close(releaseFirst)
	require.NoError(t, scheduler.barrier(ctx))
	mu.Lock()
	require.Equal(t, []RuntimeMessageType{RuntimeMessageRunEvent, RuntimeMessageRunResult}, order)
	mu.Unlock()
}

func TestRuntimeWSInboundSchedulerEmitsBoundedLifecycleObservations(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	observed := make(chan WorkerObservation, 4)
	scheduler := newRuntimeWSInboundScheduler(ctx, runtimeWSInboundSchedulerConfig{
		LaneCount: 1, QueueDepth: 1, ProcessLimiter: newRuntimeWSProcessLimiter(1),
		Observer: WorkerObserverFunc(func(observation WorkerObservation) {
			observed <- observation
		}),
	})
	defer scheduler.stop()

	require.NoError(t, scheduler.enqueue(runtimeWSInboundWork{
		attemptID: uuid.New(),
		envelope:  RuntimeEnvelope{RuntimeEnvelopeFields: RuntimeEnvelopeFields{Type: RuntimeMessageRunEvent}},
	}))
	require.NoError(t, scheduler.barrier(ctx))

	require.Equal(t, []WorkerObservation{
		{Category: "runtime.websocket.inbound_enqueue", Reason: string(RuntimeMessageRunEvent), BatchSize: 1},
		{Category: "runtime.websocket.inbound_queue_wait", Reason: string(RuntimeMessageRunEvent), BatchSize: 1},
		{Category: "runtime.websocket.inbound_active", Reason: string(RuntimeMessageRunEvent), BatchSize: 1},
		{Category: "runtime.websocket.inbound_complete", Reason: string(RuntimeMessageRunEvent), BatchSize: 1},
	}, []WorkerObservation{<-observed, <-observed, <-observed, <-observed})
}

func TestRuntimeWSInboundSchedulerSharesProcessLimitAcrossConnections(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	limiter := newRuntimeWSProcessLimiter(1)
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseAll()
	var processBackpressure atomic.Bool
	handle := func(runtimeWSInboundWork) {
		started <- struct{}{}
		<-release
	}
	first := newRuntimeWSInboundScheduler(ctx, runtimeWSInboundSchedulerConfig{
		LaneCount: 1, QueueDepth: 1, ProcessLimiter: limiter, Handle: handle,
	})
	second := newRuntimeWSInboundScheduler(ctx, runtimeWSInboundSchedulerConfig{
		LaneCount: 1, QueueDepth: 1, ProcessLimiter: limiter, Handle: handle,
		Observer: WorkerObserverFunc(func(observation WorkerObservation) {
			if observation.Category == "runtime.websocket.process_backpressure" {
				processBackpressure.Store(true)
			}
		}),
	})
	defer first.stop()
	defer second.stop()

	require.NoError(t, first.enqueue(runtimeWSInboundWork{attemptID: uuid.New()}))
	requireReceive(t, started)
	require.NoError(t, second.enqueue(runtimeWSInboundWork{attemptID: uuid.New()}))
	select {
	case <-started:
		t.Fatal("process limiter allowed two concurrent handlers")
	case <-time.After(30 * time.Millisecond):
	}
	require.Eventually(t, processBackpressure.Load, time.Second, 10*time.Millisecond)
	releaseAll()
	requireReceive(t, started)
	require.NoError(t, first.barrier(ctx))
	require.NoError(t, second.barrier(ctx))
}

func TestRuntimeWSInboundSchedulerBackpressuresFullLaneWithoutLoss(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	attemptID := uuid.New()
	firstStarted := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseAll()
	var handled atomic.Int32
	var laneBackpressure atomic.Bool
	scheduler := newRuntimeWSInboundScheduler(ctx, runtimeWSInboundSchedulerConfig{
		LaneCount: 1, QueueDepth: 1, ProcessLimiter: newRuntimeWSProcessLimiter(1),
		Observer: WorkerObserverFunc(func(observation WorkerObservation) {
			if observation.Category == "runtime.websocket.inbound_backpressure" {
				laneBackpressure.Store(true)
			}
		}),
		Handle: func(runtimeWSInboundWork) {
			if handled.Add(1) == 1 {
				close(firstStarted)
				<-release
			}
		},
	})
	defer scheduler.stop()

	require.NoError(t, scheduler.enqueue(runtimeWSInboundWork{attemptID: attemptID}))
	requireReceive(t, firstStarted)
	require.NoError(t, scheduler.enqueue(runtimeWSInboundWork{attemptID: attemptID}))
	thirdQueued := make(chan error, 1)
	go func() { thirdQueued <- scheduler.enqueue(runtimeWSInboundWork{attemptID: attemptID}) }()
	select {
	case err := <-thirdQueued:
		t.Fatalf("third enqueue returned before lane capacity was available: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	require.True(t, laneBackpressure.Load())
	releaseAll()
	require.NoError(t, <-thirdQueued)
	require.NoError(t, scheduler.barrier(ctx))
	require.Equal(t, int32(3), handled.Load())
}

func TestRuntimeWSInboundSchedulerContainsPanicAndCancelsQueuedWork(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	release := make(chan struct{})
	var handled atomic.Int32
	var panics atomic.Int32
	scheduler := newRuntimeWSInboundScheduler(ctx, runtimeWSInboundSchedulerConfig{
		LaneCount: 1, QueueDepth: 1, ProcessLimiter: newRuntimeWSProcessLimiter(1),
		Handle: func(runtimeWSInboundWork) {
			if handled.Add(1) == 1 {
				close(started)
				<-release
				panic("test handler panic")
			}
		},
		HandlePanic: func() {
			panics.Add(1)
			cancel()
		},
	})
	defer scheduler.stop()

	require.NoError(t, scheduler.enqueue(runtimeWSInboundWork{attemptID: uuid.New()}))
	requireReceive(t, started)
	require.NoError(t, scheduler.enqueue(runtimeWSInboundWork{attemptID: uuid.New()}))
	close(release)
	require.NoError(t, scheduler.barrier(context.Background()))
	require.Equal(t, int32(1), handled.Load(), "queued work must not continue after an unknown partial state")
	require.Equal(t, int32(1), panics.Load())
}

func TestRuntimeWSInboundSchedulerStopHonorsCleanupDeadline(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	scheduler := newRuntimeWSInboundScheduler(context.Background(), runtimeWSInboundSchedulerConfig{
		LaneCount: 1, QueueDepth: 1, ProcessLimiter: newRuntimeWSProcessLimiter(1),
		Handle: func(runtimeWSInboundWork) {
			close(started)
			<-release
		},
	})
	require.NoError(t, scheduler.enqueue(runtimeWSInboundWork{attemptID: uuid.New()}))
	requireReceive(t, started)

	deadline, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.False(t, scheduler.stopWithin(deadline), "cleanup must not wait forever for a stuck dependency")
	close(release)
	require.True(t, scheduler.stopWithin(context.Background()))
}

func TestRuntimeWebSocketProcessesIndependentAttemptResultsConcurrently(t *testing.T) {
	fixture := newRuntimeWSTestFixture()
	fixture.hello.Capacity = 2
	fixture.sessions.state.Session.Capacity = 2
	fixture.webSocketConfig = RuntimeWebSocketConcurrencyConfig{
		ConnectionMaxInflight: 2,
		ProcessMaxInflight:    2,
		LaneQueueDepth:        2,
	}
	started := make(chan uuid.UUID, 2)
	releaseFirst := make(chan struct{})
	releaseSecond := make(chan struct{})
	firstID := uuid.MustParse("00000000-0000-4000-8000-000000000000")
	secondID := uuid.MustParse("00000000-0000-4000-8000-000000000001")
	fixture.finalizer.finalize = func(_ RuntimeResultPrincipal, request RuntimeResultRequest) (RuntimeResultAck, error) {
		started <- request.AttemptIdentity.AttemptID
		if request.AttemptIdentity.AttemptID == firstID {
			<-releaseFirst
		} else {
			<-releaseSecond
		}
		return RuntimeResultAck{
			ResultID: request.ResultID, Classification: RuntimeResultClassificationSuccess,
			RunStatus: string(RuntimeRunSuccess), DispatchState: string(RuntimeDispatchTerminal),
		}, nil
	}
	server, target := fixture.server(t)
	defer server.Close()
	connection := dialRuntimeWS(t, target)
	defer connection.Close()
	writeRuntimeWSHello(t, connection, fixture.hello)
	require.Equal(t, RuntimeMessageReady, readRuntimeWSEnvelope(t, connection).Type)

	first := fixture.assignment().AttemptIdentity
	first.AttemptID = firstID
	first.RunID = uuid.New()
	second := fixture.assignment().AttemptIdentity
	second.AttemptID = secondID
	second.RunID = uuid.New()
	requests := make(map[uuid.UUID]RuntimeEnvelope, 2)
	for _, identity := range []AttemptIdentity{first, second} {
		message, err := NewRuntimeTypedMessage(RuntimeMessageRunResult, nil, RunResultPayload{
			AttemptIdentity: identity,
			ResultID:        uuid.New(),
			Status:          "success",
			Output:          map[string]any{"ok": true},
			DurationMS:      1,
		})
		require.NoError(t, err)
		request := runtimeEnvelopeFromTyped(t, message)
		requests[identity.AttemptID] = request
		require.NoError(t, connection.WriteJSON(message))
	}
	require.ElementsMatch(t, []uuid.UUID{first.AttemptID, second.AttemptID}, []uuid.UUID{
		receiveUUID(t, started), receiveUUID(t, started),
	})
	close(releaseSecond)
	secondReply := readRuntimeWSEnvelope(t, connection)
	require.Equal(t, RuntimeMessageRunResultAck, secondReply.Type)
	require.NotNil(t, secondReply.ReplyToMessageID)
	require.Equal(t, requests[second.AttemptID].MessageID, *secondReply.ReplyToMessageID)
	require.NoError(t, ValidateRuntimeReplyCorrelation(requests[second.AttemptID], secondReply))
	close(releaseFirst)
	firstReply := readRuntimeWSEnvelope(t, connection)
	require.Equal(t, RuntimeMessageRunResultAck, firstReply.Type)
	require.NotNil(t, firstReply.ReplyToMessageID)
	require.Equal(t, requests[first.AttemptID].MessageID, *firstReply.ReplyToMessageID)
	require.NoError(t, ValidateRuntimeReplyCorrelation(requests[first.AttemptID], firstReply))
	require.NotEqual(t, *firstReply.ReplyToMessageID, *secondReply.ReplyToMessageID)
}

func receiveUUID(t *testing.T, values <-chan uuid.UUID) uuid.UUID {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for UUID")
		return uuid.Nil
	}
}

func requireReceive(t *testing.T, values <-chan struct{}) {
	t.Helper()
	select {
	case <-values:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for signal")
	}
}
