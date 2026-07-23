package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

func TestRuntimeSignalOutboxWorkerPublishesOnlySafeProjectionThenMarks(t *testing.T) {
	store := &runtimeSignalOutboxStoreFake{}
	bus := &runtimeSignalBusFake{}
	worker := NewRuntimeSignalOutboxWorker(store, bus)
	row := testRuntimeSignalOutbox(`{
		"input":{"prompt":"classified"},
		"output":"classified",
		"token":"classified",
		"secret":"classified"
	}`)
	store.claimed = []db.RuntimeSignalOutbox{row}

	result, err := worker.ProcessOnce(context.Background(), RuntimeSignalOutboxWorkerConfig{})
	require.NoError(t, err)
	require.Equal(t, RuntimeSignalOutboxBatchResult{Claimed: 1, Published: 1}, result)
	require.Len(t, bus.published, 1)
	require.Equal(t, RuntimeSignal{
		SignalID: row.ID, Type: row.EventType, AgentID: row.AgentID, RunID: row.RunID,
	}, bus.published[0])
	require.Len(t, store.marked, 1)
	require.Equal(t, row.ID, store.marked[0].ID)
	require.Empty(t, store.retried)
}

func TestRuntimeSignalOutboxWorkerSchedulesDatabaseClockRetry(t *testing.T) {
	store := &runtimeSignalOutboxStoreFake{}
	bus := &runtimeSignalBusFake{publishErr: errors.New("Redis unavailable at classified address")}
	worker := NewRuntimeSignalOutboxWorker(store, bus)
	row := testRuntimeSignalOutbox(`{}`)
	row.AttemptCount = 3
	store.claimed = []db.RuntimeSignalOutbox{row}

	result, err := worker.ProcessOnce(context.Background(), RuntimeSignalOutboxWorkerConfig{
		RetryBase: 100 * time.Millisecond, RetryMaximum: time.Second,
	})
	require.NoError(t, err, "a durable scheduled retry is a handled outcome")
	require.Equal(t, RuntimeSignalOutboxBatchResult{Claimed: 1, Retried: 1}, result)
	require.Len(t, store.retried, 1)
	require.Equal(t, int64(400), store.retried[0].RetryAfterMs)
	require.Equal(t, "SIGNAL_PUBLISH_FAILED", store.retried[0].LastError)
	require.NotContains(t, store.retried[0].LastError, "classified")
	require.Empty(t, store.marked)
}

func TestRuntimeSignalOutboxWorkerAllowsPublishMarkCrashReplay(t *testing.T) {
	store := &runtimeSignalOutboxStoreFake{markErr: pgx.ErrNoRows}
	bus := &runtimeSignalBusFake{}
	worker := NewRuntimeSignalOutboxWorker(store, bus)
	store.claimed = []db.RuntimeSignalOutbox{testRuntimeSignalOutbox(`{}`)}

	result, err := worker.ProcessOnce(context.Background(), RuntimeSignalOutboxWorkerConfig{})
	require.NoError(t, err)
	require.Equal(t, RuntimeSignalOutboxBatchResult{Claimed: 1}, result)
	require.Len(t, bus.published, 1, "publish happened before the simulated mark crash")
	require.Empty(t, store.retried, "expired processing lease, not an inline retry, owns replay")
}

func TestRuntimeSignalOutboxWorkerExtractsOnlyTargetFromPayload(t *testing.T) {
	target := uuid.New()
	row := testRuntimeSignalOutbox(`{"target_instance_id":"` + target.String() + `","token":"classified"}`)
	store := &runtimeSignalOutboxStoreFake{claimed: []db.RuntimeSignalOutbox{row}}
	bus := &runtimeSignalBusFake{}

	result, err := NewRuntimeSignalOutboxWorker(store, bus).ProcessOnce(
		context.Background(), RuntimeSignalOutboxWorkerConfig{},
	)
	require.NoError(t, err)
	require.Equal(t, 1, result.Published)
	require.Equal(t, &target, bus.published[0].TargetInstanceID)
	encoded, err := MarshalRuntimeSignal(bus.published[0])
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "classified")
	require.NotContains(t, string(encoded), "token")
}

func TestRuntimeSignalOutboxWorkerProjectsCredentialConnectionIdentities(t *testing.T) {
	targetID, credentialID := uuid.New(), uuid.New()
	identity := RuntimeConnectionIdentity{
		RuntimeSessionID: uuid.New(),
		SessionEpoch:     4,
		AttachmentID:     uuid.New(),
	}
	payload := `{"target_instance_id":"` + targetID.String() +
		`","credential_id":"` + credentialID.String() +
		`","connections":[{"runtime_session_id":"` + identity.RuntimeSessionID.String() +
		`","session_epoch":4,"attachment_id":"` + identity.AttachmentID.String() + `"}]}`
	row := testRuntimeSignalOutbox(payload)
	row.EventType = "credential.revoke"
	store := &runtimeSignalOutboxStoreFake{claimed: []db.RuntimeSignalOutbox{row}}
	bus := &runtimeSignalBusFake{}

	result, err := NewRuntimeSignalOutboxWorker(store, bus).ProcessOnce(
		context.Background(), RuntimeSignalOutboxWorkerConfig{},
	)
	require.NoError(t, err)
	require.Equal(t, 1, result.Published)
	require.Equal(t, &targetID, bus.published[0].TargetInstanceID)
	require.Equal(t, &credentialID, bus.published[0].CredentialID)
	require.Equal(t, []RuntimeConnectionIdentity{identity}, bus.published[0].Connections)
}

func TestRuntimeSignalOutboxWorkerProjectsNodeCapacityIdentity(t *testing.T) {
	nodeID := uuid.New()
	row := testRuntimeSignalOutbox(`{"node_id":"` + nodeID.String() + `","input":"classified"}`)
	row.EventType = runtimeNodeCapacityAvailableSignal
	store := &runtimeSignalOutboxStoreFake{claimed: []db.RuntimeSignalOutbox{row}}
	bus := &runtimeSignalBusFake{}

	result, err := NewRuntimeSignalOutboxWorker(store, bus).ProcessOnce(
		context.Background(), RuntimeSignalOutboxWorkerConfig{},
	)
	require.NoError(t, err)
	require.Equal(t, 1, result.Published)
	require.Equal(t, &nodeID, bus.published[0].NodeID)
	encoded, err := MarshalRuntimeSignal(bus.published[0])
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "classified")
	require.NotContains(t, string(encoded), "input")
}

func TestRuntimeSignalOutboxWorkerInvalidTargetRetriesWithoutBroadcast(t *testing.T) {
	row := testRuntimeSignalOutbox(`{"target_instance_id":"not-a-uuid","input":"classified"}`)
	store := &runtimeSignalOutboxStoreFake{claimed: []db.RuntimeSignalOutbox{row}}
	bus := &runtimeSignalBusFake{}

	result, err := NewRuntimeSignalOutboxWorker(store, bus).ProcessOnce(
		context.Background(), RuntimeSignalOutboxWorkerConfig{},
	)
	require.NoError(t, err)
	require.Equal(t, 1, result.Retried)
	require.Empty(t, bus.published)
	require.Equal(t, "SIGNAL_INVALID", store.retried[0].LastError)
}

func TestRuntimeSignalRetryDelayIsBounded(t *testing.T) {
	cfg := normalizeRuntimeSignalOutboxWorkerConfig(RuntimeSignalOutboxWorkerConfig{
		RetryBase: 250 * time.Millisecond, RetryMaximum: 2 * time.Second,
	})
	require.Equal(t, 250*time.Millisecond, runtimeSignalRetryDelay(cfg, 1))
	require.Equal(t, 500*time.Millisecond, runtimeSignalRetryDelay(cfg, 2))
	require.Equal(t, 2*time.Second, runtimeSignalRetryDelay(cfg, 10_000))
}

func TestRuntimeSignalOutboxWorkerObserverMarksStartupWithoutChangingStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	observed := make(chan WorkerObservation, 1)
	done := make(chan struct{})
	go func() {
		StartRuntimeSignalOutboxWorker(ctx, &RuntimeSignalOutboxWorker{}, RuntimeSignalOutboxWorkerConfig{
			Observer: WorkerObserverFunc(func(observation WorkerObservation) {
				observed <- observation
				cancel()
			}),
		})
		close(done)
	}()
	select {
	case observation := <-observed:
		require.Equal(t, WorkerObservation{
			Category: "runtime.signal_outbox.claim", Reason: "startup", BatchSize: 64,
		}, observation)
	case <-time.After(time.Second):
		t.Fatal("runtime signal observer was not called")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runtime signal worker did not stop after observer canceled context")
	}
}

type runtimeSignalOutboxStoreFake struct {
	claimed    []db.RuntimeSignalOutbox
	claimErr   error
	marked     []db.MarkRuntimeSignalPublishedParams
	markErr    error
	retried    []db.RetryRuntimeSignalParams
	retryErr   error
	backlog    int32
	backlogErr error
}

func (f *runtimeSignalOutboxStoreFake) ClaimRuntimeSignals(_ context.Context, params db.ClaimRuntimeSignalsParams) ([]db.RuntimeSignalOutbox, error) {
	if f.claimErr != nil {
		return nil, f.claimErr
	}
	rows := append([]db.RuntimeSignalOutbox(nil), f.claimed...)
	for index := range rows {
		rows[index].Status = "processing"
		rows[index].LeaseOwner = &params.LeaseOwner
		if rows[index].AttemptCount <= 0 {
			rows[index].AttemptCount = 1
		}
	}
	return rows, nil
}

func (f *runtimeSignalOutboxStoreFake) MarkRuntimeSignalPublished(_ context.Context, params db.MarkRuntimeSignalPublishedParams) (db.RuntimeSignalOutbox, error) {
	f.marked = append(f.marked, params)
	return db.RuntimeSignalOutbox{}, f.markErr
}

func (f *runtimeSignalOutboxStoreFake) RetryRuntimeSignal(_ context.Context, params db.RetryRuntimeSignalParams) (db.RuntimeSignalOutbox, error) {
	f.retried = append(f.retried, params)
	return db.RuntimeSignalOutbox{}, f.retryErr
}

func (f *runtimeSignalOutboxStoreFake) CountPendingRuntimeSignals(context.Context) (int32, error) {
	return f.backlog, f.backlogErr
}

type runtimeSignalBusFake struct {
	published  []RuntimeSignal
	publishErr error
}

func (f *runtimeSignalBusFake) Publish(_ context.Context, signal RuntimeSignal) error {
	f.published = append(f.published, signal)
	return f.publishErr
}

func (f *runtimeSignalBusFake) Subscribe(context.Context, RuntimeSignalHandler) error { return nil }
func (f *runtimeSignalBusFake) Health(context.Context) error                          { return nil }
func (f *runtimeSignalBusFake) Close() error                                          { return nil }

func testRuntimeSignalOutbox(payload string) db.RuntimeSignalOutbox {
	runID := uuid.New()
	return db.RuntimeSignalOutbox{
		ID: uuid.New(), EventType: "run.available", AgentID: uuid.New(), RunID: &runID,
		Payload: []byte(payload), Status: "pending", AttemptCount: 1,
	}
}
