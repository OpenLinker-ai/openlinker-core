package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

type effectWorkerCallRecorder struct {
	mu    sync.Mutex
	calls []string
}

func (r *effectWorkerCallRecorder) add(call string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, call)
}

func (r *effectWorkerCallRecorder) snapshot() []string {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

type effectWorkerFakeStore struct {
	mu       sync.Mutex
	recorder *effectWorkerCallRecorder

	claimCalls       []db.ClaimRunEffectsParams
	markCalls        []db.MarkRunEffectSucceededParams
	retryCalls       []db.RetryOrDeadLetterRunEffectParams
	deadLetterCalls  []db.DeadLetterRunEffectParams
	getEffectCalls   []uuid.UUID
	replayCalls      []db.ReplayRunEffectParams
	delegationCalls  []uuid.UUID
	runCalls         []uuid.UUID
	parentEventCalls []db.CreateRunEffectParentEventParams
	nextDueCalls     int
	nextDue          db.NextRunEffectDueRow
	nextDueErr       error

	claimFn       func(int, db.ClaimRunEffectsParams) ([]db.RunEffectOutbox, error)
	markFn        func(int, db.MarkRunEffectSucceededParams) (db.RunEffectOutbox, error)
	retryFn       func(int, db.RetryOrDeadLetterRunEffectParams) (db.RunEffectOutbox, error)
	deadLetterFn  func(int, db.DeadLetterRunEffectParams) (db.RunEffectOutbox, error)
	getEffectFn   func(uuid.UUID) (db.RunEffectOutbox, error)
	replayFn      func(db.ReplayRunEffectParams) (db.RunEffectOutbox, error)
	delegationFn  func(uuid.UUID) (db.RunDelegation, error)
	runFn         func(uuid.UUID) (db.Run, error)
	parentEventFn func(int, db.CreateRunEffectParentEventParams) (db.RunEvent, error)

	reapEffects []db.RunEffectOutbox
	reapErr     error
}

func (s *effectWorkerFakeStore) NextRunEffectDue(
	context.Context,
) (db.NextRunEffectDueRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextDueCalls++
	return s.nextDue, s.nextDueErr
}

func (s *effectWorkerFakeStore) ClaimRunEffects(
	_ context.Context,
	arg db.ClaimRunEffectsParams,
) ([]db.RunEffectOutbox, error) {
	s.recorder.add("claim")
	s.mu.Lock()
	s.claimCalls = append(s.claimCalls, arg)
	call := len(s.claimCalls)
	fn := s.claimFn
	s.mu.Unlock()
	if fn == nil {
		return nil, nil
	}
	return fn(call, arg)
}

func (s *effectWorkerFakeStore) DeadLetterExpiredRunEffectsAtLimit(
	context.Context,
) ([]db.RunEffectOutbox, error) {
	s.recorder.add("reap")
	return append([]db.RunEffectOutbox(nil), s.reapEffects...), s.reapErr
}

func (s *effectWorkerFakeStore) DeadLetterRunEffect(
	_ context.Context,
	arg db.DeadLetterRunEffectParams,
) (db.RunEffectOutbox, error) {
	s.recorder.add("dead_letter")
	s.mu.Lock()
	s.deadLetterCalls = append(s.deadLetterCalls, arg)
	call := len(s.deadLetterCalls)
	fn := s.deadLetterFn
	s.mu.Unlock()
	if fn == nil {
		return db.RunEffectOutbox{ID: arg.ID}, nil
	}
	return fn(call, arg)
}

func (s *effectWorkerFakeStore) MarkRunEffectSucceeded(
	_ context.Context,
	arg db.MarkRunEffectSucceededParams,
) (db.RunEffectOutbox, error) {
	s.recorder.add("mark")
	s.mu.Lock()
	s.markCalls = append(s.markCalls, arg)
	call := len(s.markCalls)
	fn := s.markFn
	s.mu.Unlock()
	if fn == nil {
		return db.RunEffectOutbox{ID: arg.ID}, nil
	}
	return fn(call, arg)
}

func (s *effectWorkerFakeStore) RetryOrDeadLetterRunEffect(
	_ context.Context,
	arg db.RetryOrDeadLetterRunEffectParams,
) (db.RunEffectOutbox, error) {
	s.recorder.add("retry")
	s.mu.Lock()
	s.retryCalls = append(s.retryCalls, arg)
	call := len(s.retryCalls)
	fn := s.retryFn
	s.mu.Unlock()
	if fn == nil {
		return db.RunEffectOutbox{ID: arg.ID}, nil
	}
	return fn(call, arg)
}

func (s *effectWorkerFakeStore) GetRunEffectByID(
	_ context.Context,
	effectID uuid.UUID,
) (db.RunEffectOutbox, error) {
	s.recorder.add("get_effect")
	s.mu.Lock()
	s.getEffectCalls = append(s.getEffectCalls, effectID)
	fn := s.getEffectFn
	s.mu.Unlock()
	if fn == nil {
		return db.RunEffectOutbox{}, pgx.ErrNoRows
	}
	return fn(effectID)
}

func (s *effectWorkerFakeStore) ReplayRunEffect(
	_ context.Context,
	arg db.ReplayRunEffectParams,
) (db.RunEffectOutbox, error) {
	s.recorder.add("replay")
	s.mu.Lock()
	s.replayCalls = append(s.replayCalls, arg)
	fn := s.replayFn
	s.mu.Unlock()
	if fn == nil {
		return db.RunEffectOutbox{ID: arg.ID, Status: "pending"}, nil
	}
	return fn(arg)
}

func (s *effectWorkerFakeStore) GetRunDelegationByChild(
	_ context.Context,
	childRunID uuid.UUID,
) (db.RunDelegation, error) {
	s.recorder.add("get_delegation")
	s.mu.Lock()
	s.delegationCalls = append(s.delegationCalls, childRunID)
	fn := s.delegationFn
	s.mu.Unlock()
	if fn == nil {
		return db.RunDelegation{}, pgx.ErrNoRows
	}
	return fn(childRunID)
}

func (s *effectWorkerFakeStore) GetRunByID(
	_ context.Context,
	runID uuid.UUID,
) (db.Run, error) {
	s.recorder.add("get_run")
	s.mu.Lock()
	s.runCalls = append(s.runCalls, runID)
	fn := s.runFn
	s.mu.Unlock()
	if fn == nil {
		return db.Run{}, pgx.ErrNoRows
	}
	return fn(runID)
}

func (s *effectWorkerFakeStore) CreateRunEffectParentEvent(
	_ context.Context,
	arg db.CreateRunEffectParentEventParams,
) (db.RunEvent, error) {
	s.recorder.add("create_parent_event")
	s.mu.Lock()
	s.parentEventCalls = append(s.parentEventCalls, arg)
	call := len(s.parentEventCalls)
	fn := s.parentEventFn
	s.mu.Unlock()
	if fn == nil {
		return db.RunEvent{}, pgx.ErrNoRows
	}
	return fn(call, arg)
}

type effectWorkerFakeWebhook struct {
	mu       sync.Mutex
	recorder *effectWorkerCallRecorder

	agentCalls   []db.RunEffectOutbox
	taskCalls    []db.RunEffectOutbox
	resetCalls   []db.RunEffectOutbox
	enqueueCalls []db.RunEvent

	agentFn   func(db.RunEffectOutbox) RunEffectAttemptResult
	taskFn    func(db.RunEffectOutbox) RunEffectAttemptResult
	resetFn   func(db.RunEffectOutbox) error
	enqueueFn func(db.RunEvent) error
}

func (h *effectWorkerFakeWebhook) AttemptAgentWebhookEffect(
	_ context.Context,
	effect db.RunEffectOutbox,
) RunEffectAttemptResult {
	h.recorder.add("attempt_agent_webhook")
	h.mu.Lock()
	h.agentCalls = append(h.agentCalls, effect)
	fn := h.agentFn
	h.mu.Unlock()
	if fn == nil {
		return RunEffectAttemptSucceeded()
	}
	return fn(effect)
}

func (h *effectWorkerFakeWebhook) AttemptTaskCallbackEffect(
	_ context.Context,
	effect db.RunEffectOutbox,
) RunEffectAttemptResult {
	h.recorder.add("attempt_task_callback")
	h.mu.Lock()
	h.taskCalls = append(h.taskCalls, effect)
	fn := h.taskFn
	h.mu.Unlock()
	if fn == nil {
		return RunEffectAttemptSucceeded()
	}
	return fn(effect)
}

func (h *effectWorkerFakeWebhook) ResetWebhookEffectDelivery(
	_ context.Context,
	effect db.RunEffectOutbox,
) error {
	h.recorder.add("reset")
	h.mu.Lock()
	h.resetCalls = append(h.resetCalls, effect)
	fn := h.resetFn
	h.mu.Unlock()
	if fn == nil {
		return nil
	}
	return fn(effect)
}

func (h *effectWorkerFakeWebhook) EnqueueRunEventDurable(
	_ context.Context,
	event db.RunEvent,
) error {
	h.recorder.add("enqueue")
	h.mu.Lock()
	h.enqueueCalls = append(h.enqueueCalls, event)
	fn := h.enqueueFn
	h.mu.Unlock()
	if fn == nil {
		return nil
	}
	return fn(event)
}

func effectWorkerClaimedEffect(
	effect db.RunEffectOutbox,
	arg db.ClaimRunEffectsParams,
	attemptCount int32,
) db.RunEffectOutbox {
	owner := arg.LeaseOwner
	effect.Status = "processing"
	effect.LeaseOwner = &owner
	effect.AttemptCount = attemptCount
	return effect
}

func TestRunEffectWorkerUsesFreshLeaseOwnerForEveryBatch(t *testing.T) {
	store := &effectWorkerFakeStore{}
	worker := NewRunEffectWorker(store, nil, nil)

	for range 2 {
		processed, err := worker.ProcessOnce(context.Background(), RunEffectWorkerConfig{})
		require.NoError(t, err)
		assert.Zero(t, processed)
	}

	require.Len(t, store.claimCalls, 2)
	assert.NotEqual(t, uuid.Nil, store.claimCalls[0].LeaseOwner)
	assert.NotEqual(t, uuid.Nil, store.claimCalls[1].LeaseOwner)
	assert.NotEqual(t, store.claimCalls[0].LeaseOwner, store.claimCalls[1].LeaseOwner)
}

func TestRunEffectWorkerTransitionsUseClaimLeaseAndAttemptFence(t *testing.T) {
	tests := []struct {
		name           string
		attemptCount   int32
		result         RunEffectAttemptResult
		transition     string
		retryAfterMs   int64
		persistedError string
	}{
		{
			name:         "success",
			attemptCount: 7,
			result:       RunEffectAttemptSucceeded(),
			transition:   "mark",
		},
		{
			name:           "retry first attempt",
			attemptCount:   1,
			result:         RetryableRunEffectAttempt("temporarily-unavailable", "try again later", errors.New("private cause")),
			transition:     "retry",
			retryAfterMs:   60_000,
			persistedError: "TEMPORARILY_UNAVAILABLE: try again later",
		},
		{
			name:           "retry second attempt",
			attemptCount:   2,
			result:         RetryableRunEffectAttempt("temporarily-unavailable", "try again later", nil),
			transition:     "retry",
			retryAfterMs:   300_000,
			persistedError: "TEMPORARILY_UNAVAILABLE: try again later",
		},
		{
			name:           "retry later attempt",
			attemptCount:   3,
			result:         RetryableRunEffectAttempt("temporarily-unavailable", "try again later", nil),
			transition:     "retry",
			retryAfterMs:   1_800_000,
			persistedError: "TEMPORARILY_UNAVAILABLE: try again later",
		},
		{
			name:           "permanent failure",
			attemptCount:   4,
			result:         PermanentRunEffectFailure("invalid-target", "target is unavailable", errors.New("private cause")),
			transition:     "dead_letter",
			persistedError: "INVALID_TARGET: target is unavailable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			effectID := uuid.New()
			baseEffect := db.RunEffectOutbox{
				ID:         effectID,
				RunID:      uuid.New(),
				EffectType: RunEffectTypeAgentWebhook,
			}
			store := &effectWorkerFakeStore{}
			store.claimFn = func(_ int, arg db.ClaimRunEffectsParams) ([]db.RunEffectOutbox, error) {
				return []db.RunEffectOutbox{
					effectWorkerClaimedEffect(baseEffect, arg, tt.attemptCount),
				}, nil
			}
			webhook := &effectWorkerFakeWebhook{
				agentFn: func(db.RunEffectOutbox) RunEffectAttemptResult { return tt.result },
			}
			worker := NewRunEffectWorker(store, webhook, nil)

			processed, err := worker.ProcessOnce(context.Background(), RunEffectWorkerConfig{})
			require.NoError(t, err)
			require.Equal(t, 1, processed)
			require.Len(t, store.claimCalls, 1)
			leaseOwner := store.claimCalls[0].LeaseOwner

			switch tt.transition {
			case "mark":
				require.Len(t, store.markCalls, 1)
				assert.Equal(t, db.MarkRunEffectSucceededParams{
					ID: effectID, LeaseOwner: leaseOwner, AttemptCount: tt.attemptCount,
				}, store.markCalls[0])
				assert.Empty(t, store.retryCalls)
				assert.Empty(t, store.deadLetterCalls)
			case "retry":
				require.Len(t, store.retryCalls, 1)
				assert.Equal(t, db.RetryOrDeadLetterRunEffectParams{
					ID: effectID, LeaseOwner: leaseOwner, AttemptCount: tt.attemptCount,
					RetryAfterMs: tt.retryAfterMs, LastError: tt.persistedError,
				}, store.retryCalls[0])
				assert.Empty(t, store.markCalls)
				assert.Empty(t, store.deadLetterCalls)
			case "dead_letter":
				require.Len(t, store.deadLetterCalls, 1)
				assert.Equal(t, db.DeadLetterRunEffectParams{
					ID: effectID, LeaseOwner: leaseOwner, AttemptCount: tt.attemptCount,
					LastError: tt.persistedError,
				}, store.deadLetterCalls[0])
				assert.Empty(t, store.markCalls)
				assert.Empty(t, store.retryCalls)
			}
		})
	}
}

func TestRunEffectWorkerTreatsLateFencedTransitionAsBenign(t *testing.T) {
	tests := []struct {
		name      string
		result    RunEffectAttemptResult
		configure func(*effectWorkerFakeStore)
	}{
		{
			name:   "success",
			result: RunEffectAttemptSucceeded(),
			configure: func(store *effectWorkerFakeStore) {
				store.markFn = func(int, db.MarkRunEffectSucceededParams) (db.RunEffectOutbox, error) {
					return db.RunEffectOutbox{}, pgx.ErrNoRows
				}
			},
		},
		{
			name:   "retry",
			result: RetryableRunEffectAttempt("temporary", "retry later", nil),
			configure: func(store *effectWorkerFakeStore) {
				store.retryFn = func(int, db.RetryOrDeadLetterRunEffectParams) (db.RunEffectOutbox, error) {
					return db.RunEffectOutbox{}, pgx.ErrNoRows
				}
			},
		},
		{
			name:   "permanent failure",
			result: PermanentRunEffectFailure("invalid", "cannot deliver", nil),
			configure: func(store *effectWorkerFakeStore) {
				store.deadLetterFn = func(int, db.DeadLetterRunEffectParams) (db.RunEffectOutbox, error) {
					return db.RunEffectOutbox{}, pgx.ErrNoRows
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			effect := db.RunEffectOutbox{
				ID: uuid.New(), RunID: uuid.New(), EffectType: RunEffectTypeAgentWebhook,
			}
			store := &effectWorkerFakeStore{}
			store.claimFn = func(_ int, arg db.ClaimRunEffectsParams) ([]db.RunEffectOutbox, error) {
				return []db.RunEffectOutbox{effectWorkerClaimedEffect(effect, arg, 2)}, nil
			}
			tt.configure(store)
			webhook := &effectWorkerFakeWebhook{
				agentFn: func(db.RunEffectOutbox) RunEffectAttemptResult { return tt.result },
			}

			processed, err := NewRunEffectWorker(store, webhook, nil).
				ProcessOnce(context.Background(), RunEffectWorkerConfig{})
			require.NoError(t, err)
			assert.Equal(t, 1, processed)
		})
	}
}

func TestRunEffectWorkerProcessesLeaseTakeoverWithNewFence(t *testing.T) {
	oldLeaseOwner := uuid.New()
	effect := db.RunEffectOutbox{
		ID:           uuid.New(),
		RunID:        uuid.New(),
		EffectType:   RunEffectTypeAgentWebhook,
		Status:       "processing",
		LeaseOwner:   &oldLeaseOwner,
		AttemptCount: 3,
	}
	store := &effectWorkerFakeStore{}
	store.claimFn = func(_ int, arg db.ClaimRunEffectsParams) ([]db.RunEffectOutbox, error) {
		require.NotEqual(t, oldLeaseOwner, arg.LeaseOwner)
		return []db.RunEffectOutbox{effectWorkerClaimedEffect(effect, arg, 4)}, nil
	}
	webhook := &effectWorkerFakeWebhook{}

	processed, err := NewRunEffectWorker(store, webhook, nil).
		ProcessOnce(context.Background(), RunEffectWorkerConfig{})
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Len(t, webhook.agentCalls, 1)
	require.NotNil(t, webhook.agentCalls[0].LeaseOwner)
	newLeaseOwner := *webhook.agentCalls[0].LeaseOwner
	assert.NotEqual(t, oldLeaseOwner, newLeaseOwner)
	assert.Equal(t, int32(4), webhook.agentCalls[0].AttemptCount)
	require.Len(t, store.markCalls, 1)
	assert.Equal(t, db.MarkRunEffectSucceededParams{
		ID: effect.ID, LeaseOwner: newLeaseOwner, AttemptCount: 4,
	}, store.markCalls[0])
}

func TestRunEffectWorkerReplayResetsBeforeAuditedReplay(t *testing.T) {
	recorder := &effectWorkerCallRecorder{}
	effectID := uuid.New()
	actorID := uuid.New()
	effect := db.RunEffectOutbox{
		ID: effectID, RunID: uuid.New(), EffectType: RunEffectTypeAgentWebhook, Status: "dead_letter",
	}
	store := &effectWorkerFakeStore{recorder: recorder}
	store.getEffectFn = func(id uuid.UUID) (db.RunEffectOutbox, error) {
		require.Equal(t, effectID, id)
		return effect, nil
	}
	store.replayFn = func(arg db.ReplayRunEffectParams) (db.RunEffectOutbox, error) {
		assert.Equal(t, db.ReplayRunEffectParams{
			ID: effectID, ActorType: "operator", ActorID: &actorID, Reason: "target repaired",
		}, arg)
		replayed := effect
		replayed.Status = "pending"
		return replayed, nil
	}
	webhook := &effectWorkerFakeWebhook{recorder: recorder}
	worker := NewRunEffectWorker(store, webhook, nil)

	replayed, err := worker.Replay(
		context.Background(), effectID, "operator", &actorID, "target repaired",
	)
	require.NoError(t, err)
	require.NotNil(t, replayed)
	assert.Equal(t, "pending", replayed.Status)
	assert.Equal(t, []string{"get_effect", "reset", "replay"}, recorder.snapshot())
	require.Len(t, webhook.resetCalls, 1)
	assert.Equal(t, effect, webhook.resetCalls[0])
	require.Len(t, store.replayCalls, 1)
}

func TestRunEffectWorkerReplayStopsWhenDownstreamResetFails(t *testing.T) {
	recorder := &effectWorkerCallRecorder{}
	resetErr := errors.New("downstream reset failed")
	effect := db.RunEffectOutbox{
		ID: uuid.New(), RunID: uuid.New(), EffectType: RunEffectTypeTaskCallback, Status: "dead_letter",
	}
	store := &effectWorkerFakeStore{recorder: recorder}
	store.getEffectFn = func(uuid.UUID) (db.RunEffectOutbox, error) { return effect, nil }
	webhook := &effectWorkerFakeWebhook{
		recorder: recorder,
		resetFn: func(db.RunEffectOutbox) error {
			return resetErr
		},
	}

	replayed, err := NewRunEffectWorker(store, webhook, nil).Replay(
		context.Background(), effect.ID, "operator", nil, "retry",
	)
	require.ErrorIs(t, err, resetErr)
	assert.Nil(t, replayed)
	assert.Equal(t, []string{"get_effect", "reset"}, recorder.snapshot())
	assert.Empty(t, store.replayCalls)
}

func TestRunEffectWorkerParentCompletionIsDeterministicAndIdempotent(t *testing.T) {
	recorder := &effectWorkerCallRecorder{}
	parentRunID := uuid.New()
	childRunID := uuid.New()
	callerAgentID := uuid.New()
	targetAgentID := uuid.New()
	effect := db.RunEffectOutbox{
		ID:         uuid.New(),
		RunID:      childRunID,
		EffectType: RunEffectTypeParentCompletion,
		TargetKey:  "parent_run:" + parentRunID.String(),
	}
	effect.Metadata, _ = json.Marshal(map[string]string{
		"child_run_id":  childRunID.String(),
		"parent_run_id": parentRunID.String(),
	})
	store := &effectWorkerFakeStore{recorder: recorder}
	store.claimFn = func(call int, arg db.ClaimRunEffectsParams) ([]db.RunEffectOutbox, error) {
		return []db.RunEffectOutbox{effectWorkerClaimedEffect(effect, arg, int32(call))}, nil
	}
	store.delegationFn = func(id uuid.UUID) (db.RunDelegation, error) {
		require.Equal(t, childRunID, id)
		return db.RunDelegation{
			ChildRunID: childRunID, ParentRunID: parentRunID, CallerAgentID: callerAgentID,
		}, nil
	}
	store.runFn = func(id uuid.UUID) (db.Run, error) {
		require.Equal(t, childRunID, id)
		return db.Run{ID: childRunID, AgentID: targetAgentID, Status: "success"}, nil
	}
	store.parentEventFn = func(_ int, arg db.CreateRunEffectParentEventParams) (db.RunEvent, error) {
		return db.RunEvent{
			ID: arg.ID, RunID: arg.ParentRunID, EventType: "run.child.completed", Payload: append([]byte(nil), arg.Payload...),
		}, nil
	}
	// The first completion write loses its Effect lease after the immutable
	// event and callback rows exist. Reclaiming the Effect must use the same
	// event identity and safely enqueue the same durable callback again.
	store.markFn = func(call int, arg db.MarkRunEffectSucceededParams) (db.RunEffectOutbox, error) {
		if call == 1 {
			return db.RunEffectOutbox{}, pgx.ErrNoRows
		}
		return db.RunEffectOutbox{ID: arg.ID}, nil
	}
	webhook := &effectWorkerFakeWebhook{recorder: recorder}
	worker := NewRunEffectWorker(store, webhook, nil)

	for range 2 {
		processed, err := worker.ProcessOnce(context.Background(), RunEffectWorkerConfig{})
		require.NoError(t, err)
		require.Equal(t, 1, processed)
	}

	require.Len(t, store.parentEventCalls, 2)
	expectedEventID := deterministicRunEffectChildEventID(effect.ID)
	first := store.parentEventCalls[0]
	second := store.parentEventCalls[1]
	assert.Equal(t, expectedEventID, first.ID)
	assert.Equal(t, first.ID, second.ID)
	assert.Equal(t, parentRunID, first.ParentRunID)
	assert.Equal(t, first.ParentRunID, second.ParentRunID)
	assert.JSONEq(t, string(first.Payload), string(second.Payload))
	assert.JSONEq(t, `{
		"child_run_id":"`+childRunID.String()+`",
		"caller_agent_id":"`+callerAgentID.String()+`",
		"target_agent_id":"`+targetAgentID.String()+`",
		"status":"success"
	}`, string(first.Payload))

	require.Len(t, webhook.enqueueCalls, 2)
	assert.Equal(t, expectedEventID, webhook.enqueueCalls[0].ID)
	assert.Equal(t, webhook.enqueueCalls[0].ID, webhook.enqueueCalls[1].ID)
	assert.Equal(t, parentRunID, webhook.enqueueCalls[0].RunID)
	assertSubsequence(t, recorder.snapshot(), []string{
		"create_parent_event", "enqueue", "mark",
		"create_parent_event", "enqueue", "mark",
	})
}

func TestRunEffectWorkerParentCompletionWithoutWebhookHandlerRetries(t *testing.T) {
	parentRunID := uuid.New()
	childRunID := uuid.New()
	effect := db.RunEffectOutbox{
		ID:         uuid.New(),
		RunID:      childRunID,
		EffectType: RunEffectTypeParentCompletion,
		TargetKey:  "parent_run:" + parentRunID.String(),
	}
	effect.Metadata, _ = json.Marshal(map[string]string{"parent_run_id": parentRunID.String()})
	store := &effectWorkerFakeStore{}
	store.claimFn = func(_ int, arg db.ClaimRunEffectsParams) ([]db.RunEffectOutbox, error) {
		return []db.RunEffectOutbox{effectWorkerClaimedEffect(effect, arg, 1)}, nil
	}
	store.delegationFn = func(uuid.UUID) (db.RunDelegation, error) {
		return db.RunDelegation{
			ChildRunID: childRunID, ParentRunID: parentRunID, CallerAgentID: uuid.New(),
		}, nil
	}
	store.runFn = func(uuid.UUID) (db.Run, error) {
		return db.Run{ID: childRunID, AgentID: uuid.New(), Status: "failed"}, nil
	}
	store.parentEventFn = func(_ int, arg db.CreateRunEffectParentEventParams) (db.RunEvent, error) {
		return db.RunEvent{
			ID: arg.ID, RunID: arg.ParentRunID, EventType: "run.child.completed", Payload: arg.Payload,
		}, nil
	}

	processed, err := NewRunEffectWorker(store, nil, nil).
		ProcessOnce(context.Background(), RunEffectWorkerConfig{})
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	assert.Empty(t, store.markCalls)
	assert.Empty(t, store.deadLetterCalls)
	require.Len(t, store.retryCalls, 1)
	assert.Equal(t, effect.ID, store.retryCalls[0].ID)
	assert.Equal(t, store.claimCalls[0].LeaseOwner, store.retryCalls[0].LeaseOwner)
	assert.Equal(t, int32(1), store.retryCalls[0].AttemptCount)
	assert.Equal(t, int64(60_000), store.retryCalls[0].RetryAfterMs)
	assert.Equal(t, "HANDLER_UNAVAILABLE: parent callback handler unavailable", store.retryCalls[0].LastError)
}

func assertSubsequence(t *testing.T, actual, expected []string) {
	t.Helper()
	next := 0
	for _, call := range actual {
		if next < len(expected) && call == expected[next] {
			next++
		}
	}
	assert.Equal(t, len(expected), next, "expected subsequence %v in %v", expected, actual)
}
