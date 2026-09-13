package runtime

import (
	"context"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

func TestRuntimeWebSocketCancellationRetryStopsWithQueueOrConnection(t *testing.T) {
	for _, stopWith := range []string{"empty_queue", "connection_shutdown"} {
		t.Run(stopWith, func(t *testing.T) {
			fixture := newRuntimeWSTestFixture()
			fixture.cancellations.nextErr = errRuntimeCancellationContended
			controller := fixture.controller()
			controller.dependencies.TransportPolicy = CurrentRuntimeTransportPolicy
			loopReady := make(chan struct{})
			var once sync.Once
			controller.dependencies.Observer = WorkerObserverFunc(func(o WorkerObservation) {
				if o.Category == "runtime.websocket.policy_check" {
					once.Do(func() { close(loopReady) })
				}
			})
			e := echo.New()
			controller.Register(e.Group("/api/v1"))
			server := httptest.NewServer(e)
			defer server.Close()
			shutdown := func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				require.NoError(t, controller.Shutdown(ctx))
			}
			defer shutdown()
			conn := dialRuntimeWS(t, "ws"+strings.TrimPrefix(server.URL, "http")+"/api/v1/agent-runtime/ws")
			defer conn.Close()
			writeRuntimeWSHello(t, conn, fixture.hello)
			require.Equal(t, RuntimeMessageReady, readRuntimeWSEnvelope(t, conn).Type)
			select {
			case <-loopReady:
			case <-time.After(time.Second):
				t.Fatal("maintenance loop did not start")
			}
			fixture.wakeHub.WakeControl(fixture.principal.AgentID)
			require.Eventually(t, func() bool { return fixture.cancellations.nextCallCount() >= 3 }, time.Second, time.Millisecond)
			if stopWith == "empty_queue" {
				fixture.cancellations.mu.Lock()
				before := fixture.cancellations.nextCalls
				fixture.cancellations.nextErr = nil
				fixture.cancellations.mu.Unlock()
				require.Eventually(t, func() bool { return fixture.cancellations.nextCallCount() > before }, time.Second, time.Millisecond)
			} else {
				shutdown()
			}
			calls := fixture.cancellations.nextCallCount()
			require.Less(t, calls, 12, "contention must back off, not spin")
			require.Never(t, func() bool { return fixture.cancellations.nextCallCount() != calls }, 350*time.Millisecond, 10*time.Millisecond)
		})
	}
}

type cancellationPollStub struct {
	*runtimeCancellationServiceFake
	poll func(context.Context, RuntimeSessionPrincipal) (RuntimeCommandsResponse, error)
}

func (s cancellationPollStub) PollCommands(ctx context.Context, p RuntimeSessionPrincipal) (RuntimeCommandsResponse, error) {
	return s.poll(ctx, p)
}

func TestRuntimePullCancellationContentionWaitBounds(t *testing.T) {
	for _, tc := range []struct {
		name              string
		wait, cancelAfter time.Duration
		emptyAfter        int
	}{
		{"nonblocking", 0, 0, 0},
		{"wait_deadline", 120 * time.Millisecond, 0, 0},
		{"request_canceled", time.Second, 60 * time.Millisecond, 0},
		{"candidate_expired", 450 * time.Millisecond, 0, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			if tc.cancelAfter != 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tc.cancelAfter)
				defer cancel()
			}
			databaseTime := time.Now().UTC()
			calls := 0
			controller := NewRuntimeHTTPController(RuntimeHTTPDependencies{
				Cancellations: cancellationPollStub{poll: func(context.Context, RuntimeSessionPrincipal) (RuntimeCommandsResponse, error) {
					calls++
					response := RuntimeCommandsResponse{Commands: []PendingCommand{}, DatabaseTime: databaseTime}
					if tc.emptyAfter != 0 && calls > tc.emptyAfter {
						return response, nil
					}
					return response, errRuntimeCancellationContended
				}},
			})
			response, err := controller.pollCommandsWithWait(ctx, RuntimeSessionPrincipal{}, tc.wait)
			if tc.cancelAfter != 0 {
				require.ErrorIs(t, err, context.DeadlineExceeded)
			} else {
				require.NoError(t, err, "internal contention must not become a wire error")
				require.Empty(t, response.Commands)
				require.Equal(t, databaseTime, response.DatabaseTime)
			}
			if tc.wait == 0 {
				require.Equal(t, 1, calls)
			} else {
				require.GreaterOrEqual(t, calls, 2)
				require.Less(t, calls, 8)
			}
			if tc.emptyAfter != 0 {
				require.Equal(t, tc.emptyAfter+1, calls, "stop querying when the candidate disappears")
			}
		})
	}
}
