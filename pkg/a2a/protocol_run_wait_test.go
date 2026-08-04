package a2a

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

func TestWaitForProtocolRunClosesMissedWakeRaceWithImmediateRead(t *testing.T) {
	runID := uuid.New()
	updates := newProtocolRunUpdateSourceFake(true)
	var loads atomic.Int32
	terminal := &runtime.RunResponse{RunID: runID.String(), Status: "success", Output: map[string]any{"answer": "done"}}

	result, err := waitForProtocolRun(context.Background(), uuid.New(), runID,
		&runtime.RunResponse{RunID: runID.String(), Status: "running"}, updates,
		func(context.Context, uuid.UUID, uuid.UUID) (*runtime.RunResponse, error) {
			loads.Add(1)
			return terminal, nil
		}, 10*time.Millisecond, time.Millisecond)

	require.NoError(t, err)
	assert.Same(t, terminal, result)
	assert.Equal(t, int32(1), loads.Load())
	assert.Equal(t, int32(1), updates.closed.Load())
}

func TestWaitForProtocolRunUsesWakeAndDurableReread(t *testing.T) {
	runID := uuid.New()
	updates := newProtocolRunUpdateSourceFake(true)
	var loads atomic.Int32
	status := atomic.Pointer[string]{}
	running := "running"
	success := "success"
	status.Store(&running)
	loader := func(context.Context, uuid.UUID, uuid.UUID) (*runtime.RunResponse, error) {
		loads.Add(1)
		return &runtime.RunResponse{RunID: runID.String(), Status: *status.Load()}, nil
	}
	go func() {
		time.Sleep(5 * time.Millisecond)
		status.Store(&success)
		updates.publish()
	}()

	result, err := waitForProtocolRun(context.Background(), uuid.New(), runID,
		&runtime.RunResponse{RunID: runID.String(), Status: "running"}, updates, loader,
		50*time.Millisecond, time.Millisecond)

	require.NoError(t, err)
	assert.Equal(t, "success", result.Status)
	assert.GreaterOrEqual(t, loads.Load(), int32(2))
}

func TestWaitForProtocolRunDegradesToBoundedPolling(t *testing.T) {
	runID := uuid.New()
	updates := newProtocolRunUpdateSourceFake(false)
	var loads atomic.Int32
	loader := func(context.Context, uuid.UUID, uuid.UUID) (*runtime.RunResponse, error) {
		if loads.Add(1) >= 2 {
			return &runtime.RunResponse{RunID: runID.String(), Status: "failed", ErrorCode: "TEST"}, nil
		}
		return &runtime.RunResponse{RunID: runID.String(), Status: "running"}, nil
	}

	result, err := waitForProtocolRun(context.Background(), uuid.New(), runID,
		&runtime.RunResponse{RunID: runID.String(), Status: "running"}, updates, loader,
		10*time.Millisecond, time.Millisecond)

	require.NoError(t, err)
	assert.Equal(t, "failed", result.Status)
	assert.Equal(t, int32(2), loads.Load())
}

func TestWaitForProtocolRunHonorsContextCancellation(t *testing.T) {
	runID := uuid.New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := waitForProtocolRun(ctx, uuid.New(), runID,
		&runtime.RunResponse{RunID: runID.String(), Status: "running"}, nil,
		func(context.Context, uuid.UUID, uuid.UUID) (*runtime.RunResponse, error) {
			return &runtime.RunResponse{RunID: runID.String(), Status: "running"}, nil
		}, time.Millisecond, time.Millisecond)

	require.ErrorIs(t, err, context.Canceled)
}

type protocolRunUpdateSourceFake struct {
	healthy bool
	wake    chan struct{}
	closed  atomic.Int32
	once    sync.Once
}

func newProtocolRunUpdateSourceFake(healthy bool) *protocolRunUpdateSourceFake {
	return &protocolRunUpdateSourceFake{healthy: healthy, wake: make(chan struct{}, 1)}
}

func (f *protocolRunUpdateSourceFake) Healthy() bool { return f != nil && f.healthy }

func (f *protocolRunUpdateSourceFake) SubscribeRun(uuid.UUID) (runtime.RunUpdateSubscription, error) {
	return &protocolRunUpdateSubscriptionFake{owner: f}, nil
}

func (f *protocolRunUpdateSourceFake) publish() {
	select {
	case f.wake <- struct{}{}:
	default:
	}
}

type protocolRunUpdateSubscriptionFake struct {
	owner *protocolRunUpdateSourceFake
}

func (s *protocolRunUpdateSubscriptionFake) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.owner.wake:
		return nil
	}
}

func (s *protocolRunUpdateSubscriptionFake) Close() {
	s.owner.once.Do(func() { s.owner.closed.Add(1) })
}
