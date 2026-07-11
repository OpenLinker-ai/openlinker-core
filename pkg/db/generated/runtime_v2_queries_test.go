package db

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestRuntimeV2AttemptAndCancellationQueries(t *testing.T) {
	now := time.Date(2026, 7, 11, 8, 0, 0, 0, time.UTC)
	runID, agentID, attemptID := uuid.New(), uuid.New(), uuid.New()
	leaseID, coreID := uuid.New(), uuid.New()
	lockDBTX := &fakeDBTX{row: fakeRow{values: []any{runID}}}
	lockQueries := New(lockDBTX)
	lockedRunID, err := lockQueries.LockNextPendingRuntimeRun(context.Background())
	if err != nil || lockedRunID != runID {
		t.Fatalf("LockNextPendingRuntimeRun = %s, %v", lockedRunID, err)
	}
	requireSQLName(t, lockDBTX.queryRowSQL, "LockNextPendingRuntimeRun")
	if !strings.Contains(lockDBTX.queryRowSQL, "ORDER BY started_at ASC, id ASC") ||
		!strings.Contains(lockDBTX.queryRowSQL, "FOR UPDATE SKIP LOCKED") {
		t.Fatal("LockNextPendingRuntimeRun must match the global pending index")
	}

	lockDBTX.row = fakeRow{values: []any{runID}}
	lockedRunID, err = lockQueries.LockNextDueRetryRuntimeRun(context.Background())
	if err != nil || lockedRunID != runID {
		t.Fatalf("LockNextDueRetryRuntimeRun = %s, %v", lockedRunID, err)
	}
	requireSQLName(t, lockDBTX.queryRowSQL, "LockNextDueRetryRuntimeRun")
	if !strings.Contains(lockDBTX.queryRowSQL, "ORDER BY next_attempt_at ASC, started_at ASC, id ASC") ||
		!strings.Contains(lockDBTX.queryRowSQL, "FOR UPDATE SKIP LOCKED") {
		t.Fatal("LockNextDueRetryRuntimeRun must match the global retry index")
	}

	attemptValues := []any{
		attemptID, runID, agentID, int32(1), (*int32)(nil), "core_http",
		leaseID, int64(1), (*uuid.UUID)(nil), (*string)(nil), (*uuid.UUID)(nil),
		(*uuid.UUID)(nil), coreID, coreID, now, now.Add(time.Minute),
		(*time.Time)(nil), (*time.Time)(nil), now.Add(time.Minute),
		now.Add(5 * time.Minute), (*time.Time)(nil), (*string)(nil),
		(*uuid.UUID)(nil), []byte(nil), (*string)(nil), (*time.Time)(nil),
		int64(0), (*int64)(nil), (*string)(nil), (*string)(nil), now,
	}
	dbtx := &fakeDBTX{row: fakeRow{values: attemptValues}}
	q := New(dbtx)

	attempt, err := q.CreateRunAttempt(context.Background(), CreateRunAttemptParams{
		ID: attemptID, RunID: runID, AgentID: agentID, OfferNo: 1,
		ExecutorType: "core_http", LeaseID: leaseID, FencingToken: 1,
		OfferedByCoreInstanceID: coreID, AttachedCoreInstanceID: coreID,
		OfferExpiresAt: now.Add(time.Minute), LeaseExpiresAt: now.Add(time.Minute),
		AttemptDeadlineAt: now.Add(5 * time.Minute),
	})
	if err != nil || attempt.ID != attemptID || attempt.FencingToken != 1 {
		t.Fatalf("CreateRunAttempt = %#v, %v", attempt, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateRunAttempt")
	if len(dbtx.queryRowArgs) != 16 {
		t.Fatalf("CreateRunAttempt arg count = %d", len(dbtx.queryRowArgs))
	}

	dbtx.queryRows = &fakeRows{rows: [][]any{attemptValues}}
	listed, err := q.ListRunAttemptsByRun(context.Background(), runID)
	if err != nil || len(listed) != 1 || listed[0].LeaseID != leaseID {
		t.Fatalf("ListRunAttemptsByRun = %#v, %v", listed, err)
	}

	cancellationID := uuid.New()
	cancellationValues := []any{
		cancellationID, runID, &attemptID, "requested", "user", uuid.New(),
		(*string)(nil), now, (*time.Time)(nil), (*time.Time)(nil),
		(*time.Time)(nil), (*time.Time)(nil), (*string)(nil), now,
	}
	dbtx.row = fakeRow{values: cancellationValues}
	cancellation, err := q.CreateRunCancellation(context.Background(), CreateRunCancellationParams{
		ID: cancellationID, RunID: runID, TargetAttemptID: &attemptID,
		RequestedByType: "user", RequestedByID: cancellationValues[5].(uuid.UUID),
	})
	if err != nil || cancellation.ID != cancellationID || cancellation.TargetAttemptID == nil {
		t.Fatalf("CreateRunCancellation = %#v, %v", cancellation, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateRunCancellation")
}

func TestResultFinalizationQueries(t *testing.T) {
	now := time.Date(2026, 7, 11, 8, 30, 0, 0, time.UTC)
	runID, userID, agentID := uuid.New(), uuid.New(), uuid.New()
	attemptID, leaseID, coreID := uuid.New(), uuid.New(), uuid.New()
	resultID, terminalEventID := uuid.New(), uuid.New()
	attemptNo, finalSequence := int32(1), int64(4)
	fence, durationMs := int64(7), int32(1234)
	executorType, connectionMode := "agent_node", "runtime_ws"
	workerID, classification, outcome := "worker-a", "success", "success"
	fingerprint := []byte("0123456789abcdef0123456789abcdef")
	acceptedAt, finishedAt := now.Add(-time.Minute), now

	attemptValues := []any{
		attemptID, runID, agentID, int32(1), &attemptNo, executorType,
		leaseID, fence, (*uuid.UUID)(nil), &workerID, (*uuid.UUID)(nil),
		(*uuid.UUID)(nil), coreID, coreID, now.Add(-2 * time.Minute),
		now.Add(time.Minute), &acceptedAt, &acceptedAt, now.Add(time.Minute),
		now.Add(2 * time.Minute), &finishedAt, &outcome, &resultID,
		fingerprint, &classification, &finishedAt, finalSequence, &finalSequence,
		(*string)(nil), (*string)(nil), now.Add(-2 * time.Minute),
	}
	dbtx := &fakeDBTX{row: fakeRow{values: attemptValues}}
	q := New(dbtx)

	lockedAttempt, err := q.LockRunAttemptForResult(context.Background(), LockRunAttemptForResultParams{
		RunID: runID, ID: attemptID,
	})
	if err != nil || lockedAttempt.ID != attemptID || !strings.Contains(dbtx.queryRowSQL, "FOR UPDATE") {
		t.Fatalf("LockRunAttemptForResult = %#v, %v", lockedAttempt, err)
	}

	dbtx.row = fakeRow{values: attemptValues}
	byResult, err := q.GetRunAttemptByResultID(context.Background(), GetRunAttemptByResultIDParams{
		RunID: runID, ResultID: resultID,
	})
	if err != nil || byResult.ResultID == nil || *byResult.ResultID != resultID {
		t.Fatalf("GetRunAttemptByResultID = %#v, %v", byResult, err)
	}

	dbtx.row = fakeRow{values: attemptValues}
	_, err = q.FinishRunAttempt(context.Background(), FinishRunAttemptParams{
		RunID: runID, ID: attemptID, LeaseID: leaseID, FencingToken: fence,
		Outcome: outcome, ResultID: resultID, ResultFingerprint: fingerprint,
		ResultClassification: classification, FinalClientEventSeq: finalSequence,
	})
	if err != nil {
		t.Fatalf("FinishRunAttempt = %v", err)
	}
	for _, guard := range []string{
		"accepted_at IS NOT NULL", "finished_at IS NULL", "result_id IS NULL",
		"result_acknowledged_at = clock_timestamp()",
	} {
		if !strings.Contains(dbtx.queryRowSQL, guard) {
			t.Fatalf("FinishRunAttempt missing guard/write %q", guard)
		}
	}

	runDeadline := now.Add(5 * time.Minute)
	endpointIdempotent := true
	runLockValues := []any{
		runID, userID, agentID, "running", "executing", "openlinker.runtime.v2",
		&connectionMode, &endpointIdempotent, []byte(nil), (*string)(nil),
		(*string)(nil), int32(1), int32(3), &attemptID, &attemptID, &leaseID,
		fence, (*uuid.UUID)(nil), &workerID, (*uuid.UUID)(nil), (*uuid.UUID)(nil),
		&runDeadline, (*time.Time)(nil), (*uuid.UUID)(nil), []byte(nil),
		(*uuid.UUID)(nil), (*time.Time)(nil), (*uuid.UUID)(nil), (*string)(nil),
		int32(25), now.Add(-2 * time.Minute), (*time.Time)(nil), now,
	}
	dbtx.row = fakeRow{values: runLockValues}
	lockedRun, err := q.LockRunForResultFinalization(context.Background(), runID)
	if err != nil || lockedRun.ID != runID || lockedRun.DatabaseNow != now ||
		!strings.Contains(dbtx.queryRowSQL, "FOR UPDATE") {
		t.Fatalf("LockRunForResultFinalization = %#v, %v", lockedRun, err)
	}

	nextAttemptAt := now.Add(2 * time.Second)
	dbtx.row = fakeRow{values: []any{
		runID, "running", "retry_wait", &nextAttemptAt, (*uuid.UUID)(nil),
		(*uuid.UUID)(nil), (*uuid.UUID)(nil), (*time.Time)(nil),
	}}
	retryRun, err := q.TransitionRunToRetryWait(context.Background(), TransitionRunToRetryWaitParams{
		RunID: runID, AttemptID: attemptID, RetryAfterMs: 2_000,
	})
	if err != nil || retryRun.DispatchState != "retry_wait" ||
		!strings.Contains(dbtx.queryRowSQL, "clock_timestamp() +") {
		t.Fatalf("TransitionRunToRetryWait = %#v, %v", retryRun, err)
	}

	output := []byte(`{"answer":42}`)
	dbtx.row = fakeRow{values: []any{
		runID, "success", "terminal", output, (*string)(nil), (*string)(nil),
		&durationMs, &finishedAt, (*time.Time)(nil), &resultID, fingerprint,
		&terminalEventID, (*time.Time)(nil),
	}}
	finalized, err := q.FinalizeRunFromResult(context.Background(), FinalizeRunFromResultParams{
		RunID: runID, AttemptID: attemptID, Status: "success", DispatchState: "terminal",
		Output: output, ResultID: &resultID, ResultFingerprint: fingerprint,
		DurationMs: durationMs, TerminalEventID: terminalEventID,
	})
	if err != nil || finalized.DurationMs == nil || *finalized.DurationMs != durationMs {
		t.Fatalf("FinalizeRunFromResult = %#v, %v", finalized, err)
	}
	if len(dbtx.queryRowArgs) != 11 || dbtx.queryRowArgs[9] != durationMs ||
		!strings.Contains(dbtx.queryRowSQL, "duration_ms = $10") {
		t.Fatalf("FinalizeRunFromResult must persist request duration: args=%#v", dbtx.queryRowArgs)
	}

	eventValues := runEventRow(
		terminalEventID, runID, nil, 5, "run.completed", []byte(`{"status":"success"}`), now,
	)
	dbtx.row = fakeRow{values: eventValues}
	event, err := q.InsertTerminalRunEvent(context.Background(), InsertTerminalRunEventParams{
		ID: terminalEventID, RunID: runID, EventType: "run.completed",
		Payload: []byte(`{"status":"success"}`),
	})
	if err != nil || event.ID != terminalEventID || len(dbtx.queryRowArgs) != 5 {
		t.Fatalf("InsertTerminalRunEvent = %#v, %v", event, err)
	}
	if !strings.Contains(dbtx.queryRowSQL, "INSERT INTO run_events") ||
		!strings.Contains(dbtx.queryRowSQL, "SELECT $1, target_run.id") {
		t.Fatal("InsertTerminalRunEvent must use the caller-supplied deterministic ID")
	}
}

func TestRuntimeV2RunEventQueriesAndLockOrder(t *testing.T) {
	now := time.Date(2026, 7, 11, 9, 0, 0, 0, time.UTC)
	runID, agentID, attemptID := uuid.New(), uuid.New(), uuid.New()
	clientEventID, leaseID, sessionID := uuid.New(), uuid.New(), uuid.New()
	attemptNo, clientSequence, fence := int32(2), int64(7), int64(3)
	fingerprint := []byte("0123456789abcdef0123456789abcdef")

	eventValues := runEventRow(
		uuid.New(), runID, nil, 11, "run.progress", []byte(`{"progress":50}`), now,
	)
	eventValues[7] = &clientEventID
	eventValues[8] = &clientSequence
	eventValues[9] = fingerprint
	eventValues[10] = &attemptID
	eventValues[11] = &attemptNo
	eventValues[12] = &fence

	// System Events must acquire the Run row before the advisory sequence lock.
	tx := &fakeTx{
		queryRows: []pgx.Row{
			fakeRow{values: []any{runID}},
			fakeRow{values: eventValues},
		},
	}
	systemEvent, err := New(tx).CreateRunEvent(context.Background(), CreateRunEventParams{
		RunID: runID, EventType: "run.created", Payload: []byte(`{"created":true}`),
	})
	if err != nil || systemEvent.ID != eventValues[0].(uuid.UUID) {
		t.Fatalf("CreateRunEvent in tx = %#v, %v", systemEvent, err)
	}
	if len(tx.queryRowSQLs) != 2 || len(tx.execSQLs) != 1 {
		t.Fatalf("CreateRunEvent lock operations = queryRows:%d execs:%d", len(tx.queryRowSQLs), len(tx.execSQLs))
	}
	requireSQLName(t, tx.queryRowSQLs[0], "LockRunForSystemEventAppend")
	requireSQLName(t, tx.execSQLs[0], "LockRunEventSequence")
	requireSQLName(t, tx.queryRowSQLs[1], "CreateRunEvent")

	leaseAcceptedAt := now.Add(-time.Minute)
	leaseExpiresAt := now.Add(time.Minute)
	attemptDeadlineAt := now.Add(2 * time.Minute)
	runDeadlineAt := now.Add(5 * time.Minute)
	executorType, workerID := "agent_node", "worker-a"
	dbtx := &fakeDBTX{row: fakeRow{values: []any{
		runID, agentID, "running", "openlinker.runtime.v2", "executing",
		&attemptID, &leaseID, fence, &executorType,
		(*uuid.UUID)(nil), &workerID, &sessionID, (*uuid.UUID)(nil),
		&leaseAcceptedAt, &leaseExpiresAt, &attemptDeadlineAt, &runDeadlineAt, now,
	}}}
	q := New(dbtx)
	locked, err := q.LockRunForEventAppend(context.Background(), runID)
	if err != nil || locked.ID != runID || locked.ActiveAttemptID == nil || locked.RuntimeSessionID == nil {
		t.Fatalf("LockRunForEventAppend = %#v, %v", locked, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "LockRunForEventAppend")
	if !strings.Contains(dbtx.queryRowSQL, "FOR UPDATE") {
		t.Fatal("LockRunForEventAppend must take a Run row lock")
	}

	if err := q.LockRunEventSequence(context.Background(), runID); err != nil {
		t.Fatalf("LockRunEventSequence = %v", err)
	}
	requireSQLName(t, dbtx.execSQL, "LockRunEventSequence")

	dbtx.row = fakeRow{values: eventValues}
	byClientID, err := q.GetRunEventByClientID(context.Background(), GetRunEventByClientIDParams{
		RunID: runID, ClientEventID: clientEventID,
	})
	if err != nil || byClientID.ClientEventID == nil || byClientID.AttemptID == nil {
		t.Fatalf("GetRunEventByClientID = %#v, %v", byClientID, err)
	}
	if len(dbtx.queryRowArgs) != 2 {
		t.Fatalf("GetRunEventByClientID args = %#v", dbtx.queryRowArgs)
	}

	dbtx.row = fakeRow{values: eventValues}
	bySequence, err := q.GetRunEventByAttemptSequence(context.Background(), GetRunEventByAttemptSequenceParams{
		RunID: runID, AttemptID: attemptID, AttemptNo: attemptNo, ClientEventSeq: clientSequence,
	})
	if err != nil || bySequence.ClientEventSeq == nil || *bySequence.ClientEventSeq != clientSequence {
		t.Fatalf("GetRunEventByAttemptSequence = %#v, %v", bySequence, err)
	}

	dbtx.row = fakeRow{values: eventValues}
	created, err := q.CreateRuntimeRunEvent(context.Background(), CreateRuntimeRunEventParams{
		RunID: runID, EventType: "run.progress", Payload: []byte(`{"progress":50}`),
		ClientEventID: clientEventID, ClientEventSeq: clientSequence,
		PayloadFingerprint: fingerprint, AttemptID: attemptID,
		AttemptNo: attemptNo, FencingToken: fence,
	})
	if err != nil || created.PayloadFingerprint == nil || created.FencingToken == nil {
		t.Fatalf("CreateRuntimeRunEvent = %#v, %v", created, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateRuntimeRunEvent")
	if len(dbtx.queryRowArgs) != 10 {
		t.Fatalf("CreateRuntimeRunEvent args = %#v", dbtx.queryRowArgs)
	}

	dbtx.row = fakeRow{values: []any{runID, int32(0), (*time.Time)(nil)}}
	watermark, err := q.GetRunEventRetentionWatermark(context.Background(), runID)
	if err != nil || watermark.RunID != runID || watermark.RetainedThroughSequence != 0 || watermark.UpdatedAt != nil {
		t.Fatalf("GetRunEventRetentionWatermark = %#v, %v", watermark, err)
	}

	dbtx.row = fakeRow{values: []any{runID, int32(5), now}}
	storedWatermark, err := q.UpsertRetentionWatermark(context.Background(), UpsertRetentionWatermarkParams{
		RunID: runID, RetainedThroughSequence: 5,
	})
	if err != nil || storedWatermark.RetainedThroughSequence != 5 {
		t.Fatalf("UpsertRetentionWatermark = %#v, %v", storedWatermark, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "UpsertRetentionWatermark")
	rowLockAt := strings.Index(dbtx.queryRowSQL, "FOR UPDATE")
	advisoryLockAt := strings.Index(dbtx.queryRowSQL, "pg_advisory_xact_lock")
	if rowLockAt < 0 || advisoryLockAt < 0 || rowLockAt > advisoryLockAt {
		t.Fatal("UpsertRetentionWatermark must lock Run row before advisory lock")
	}

	dbtx.queryRows = &fakeRows{rows: [][]any{eventValues}}
	listed, err := q.ListRunEvents(context.Background(), ListRunEventsParams{
		RunID: runID, AfterSequence: 3, Limit: 20,
	})
	if err != nil || len(listed) != 1 || listed[0].ClientEventID == nil {
		t.Fatalf("ListRunEvents = %#v, %v", listed, err)
	}
	if !strings.Contains(dbtx.querySQL, "GREATEST($2, COALESCE(w.retained_through_sequence, 0))") {
		t.Fatal("ListRunEvents must apply the logical retention watermark")
	}

	firstAvailable := int32(6)
	dbtx.row = fakeRow{values: []any{int32(5), &firstAvailable, int32(11)}}
	bounds, err := q.GetRunEventBounds(context.Background(), runID)
	if err != nil || bounds.FirstAvailableSequence == nil || bounds.LastSequence != 11 {
		t.Fatalf("GetRunEventBounds = %#v, %v", bounds, err)
	}

	dbtx.queryRows = &fakeRows{rows: [][]any{{int64(1)}, {int64(2)}, {int64(4)}}}
	sequences, err := q.ListClientEventSequencesThrough(context.Background(), ListClientEventSequencesThroughParams{
		RunID: runID, AttemptID: attemptID, AttemptNo: attemptNo, ThroughSequence: 4,
	})
	if err != nil || !reflect.DeepEqual(sequences, []int64{1, 2, 4}) {
		t.Fatalf("ListClientEventSequencesThrough = %#v, %v", sequences, err)
	}

	attemptValues := []any{
		attemptID, runID, agentID, int32(1), &attemptNo, "agent_node",
		leaseID, fence, (*uuid.UUID)(nil), &workerID, &sessionID,
		(*uuid.UUID)(nil), uuid.New(), uuid.New(), now, now.Add(time.Minute),
		&leaseAcceptedAt, &leaseAcceptedAt, leaseExpiresAt, attemptDeadlineAt,
		(*time.Time)(nil), (*string)(nil), (*uuid.UUID)(nil), []byte(nil),
		(*string)(nil), (*time.Time)(nil), clientSequence, (*int64)(nil),
		(*string)(nil), (*string)(nil), now,
	}
	dbtx.row = fakeRow{values: attemptValues}
	_, err = q.AdvanceRunAttemptEventSequence(context.Background(), AdvanceRunAttemptEventSequenceParams{
		RunID: runID, ID: attemptID, LeaseID: leaseID,
		FencingToken: fence, ClientEventSeq: clientSequence,
	})
	if err != nil {
		t.Fatalf("AdvanceRunAttemptEventSequence = %v", err)
	}
	for _, guard := range []string{"accepted_at IS NOT NULL", "finished_at IS NULL", "result_id IS NULL"} {
		if !strings.Contains(dbtx.queryRowSQL, guard) {
			t.Fatalf("AdvanceRunAttemptEventSequence missing guard %q", guard)
		}
	}
}

func TestRuntimeV2OutboxLedgerAndDLQQueries(t *testing.T) {
	now := time.Date(2026, 7, 11, 8, 0, 0, 0, time.UTC)
	runID, agentID, eventID := uuid.New(), uuid.New(), uuid.New()
	owner := uuid.New()

	signalID := uuid.New()
	signalValues := []any{
		signalID, "run.available", agentID, &runID, []byte(`{"run_id":"x"}`),
		now, now, "processing", &owner, ptrTime(now.Add(time.Minute)),
		(*time.Time)(nil), int32(1), (*string)(nil),
	}
	dbtx := &fakeDBTX{queryRows: &fakeRows{rows: [][]any{signalValues}}}
	q := New(dbtx)
	signals, err := q.ClaimRuntimeSignals(context.Background(), ClaimRuntimeSignalsParams{
		LeaseOwner: owner, LeaseDurationMs: 30_000, Limit: 10,
	})
	if err != nil || len(signals) != 1 || signals[0].ID != signalID {
		t.Fatalf("ClaimRuntimeSignals = %#v, %v", signals, err)
	}
	requireSQLName(t, dbtx.querySQL, "ClaimRuntimeSignals")
	if !strings.Contains(dbtx.querySQL, "FOR UPDATE SKIP LOCKED") {
		t.Fatal("ClaimRuntimeSignals must use SKIP LOCKED")
	}
	if !reflect.DeepEqual(dbtx.queryArgs, []any{owner, int64(30_000), int32(10)}) {
		t.Fatalf("ClaimRuntimeSignals args = %#v", dbtx.queryArgs)
	}

	effectID := uuid.New()
	effectValues := []any{
		effectID, runID, eventID, "delivery.webhook", "agent:" + agentID.String(),
		[]byte(`{}`), "processing", now, &owner, ptrTime(now.Add(time.Minute)),
		int32(1), int32(12), (*time.Time)(nil), (*time.Time)(nil), (*string)(nil), now,
	}
	dbtx.queryRows = &fakeRows{rows: [][]any{effectValues}}
	effects, err := q.ClaimRunEffects(context.Background(), ClaimRunEffectsParams{
		LeaseOwner: owner, LeaseDurationMs: 30_000, Limit: 5,
	})
	if err != nil || len(effects) != 1 || effects[0].TerminalEventID != eventID {
		t.Fatalf("ClaimRunEffects = %#v, %v", effects, err)
	}
	requireSQLName(t, dbtx.querySQL, "ClaimRunEffects")

	dbtx.row = fakeRow{values: effectValues}
	createdEffect, err := q.CreateRunEffect(context.Background(), CreateRunEffectParams{
		ID: effectID, RunID: runID, TerminalEventID: eventID,
		EffectType: "delivery.webhook", TargetKey: "agent:" + agentID.String(),
		Metadata: []byte(`{}`), MaxAttempts: 12,
	})
	if err != nil || createdEffect.ID != effectID {
		t.Fatalf("CreateRunEffect = %#v, %v", createdEffect, err)
	}
	if !strings.Contains(dbtx.queryRowSQL, "DO NOTHING") || strings.Contains(dbtx.queryRowSQL, "DO UPDATE") {
		t.Fatal("CreateRunEffect must not mutate the existing business-key row")
	}

	dbtx.row = fakeRow{values: effectValues}
	byBusinessKey, err := q.GetRunEffectByBusinessKey(context.Background(), GetRunEffectByBusinessKeyParams{
		RunID: runID, EffectType: "delivery.webhook", TargetKey: "agent:" + agentID.String(),
	})
	if err != nil || byBusinessKey.ID != effectID {
		t.Fatalf("GetRunEffectByBusinessKey = %#v, %v", byBusinessKey, err)
	}

	dbtx.row = fakeRow{values: effectValues}
	_, err = q.MarkRunEffectSucceeded(context.Background(), MarkRunEffectSucceededParams{
		ID: effectID, LeaseOwner: owner, AttemptCount: 1,
	})
	if err != nil || !strings.Contains(dbtx.queryRowSQL, "attempt_count = $3") || len(dbtx.queryRowArgs) != 3 {
		t.Fatalf("MarkRunEffectSucceeded fence = args:%#v sql:%s err:%v", dbtx.queryRowArgs, dbtx.queryRowSQL, err)
	}

	dbtx.row = fakeRow{values: effectValues}
	_, err = q.RetryOrDeadLetterRunEffect(context.Background(), RetryOrDeadLetterRunEffectParams{
		ID: effectID, LeaseOwner: owner, AttemptCount: 1,
		RetryAfterMs: 2_000, LastError: "temporary failure",
	})
	if err != nil || !strings.Contains(dbtx.queryRowSQL, "clock_timestamp() +") ||
		!strings.Contains(dbtx.queryRowSQL, "attempt_count = $3") || len(dbtx.queryRowArgs) != 5 {
		t.Fatalf("RetryOrDeadLetterRunEffect = args:%#v sql:%s err:%v", dbtx.queryRowArgs, dbtx.queryRowSQL, err)
	}

	dbtx.row = fakeRow{values: effectValues}
	_, err = q.DeadLetterRunEffect(context.Background(), DeadLetterRunEffectParams{
		ID: effectID, LeaseOwner: owner, AttemptCount: 1, LastError: "permanent failure",
	})
	if err != nil || !strings.Contains(dbtx.queryRowSQL, "dead_lettered_at = clock_timestamp()") ||
		!strings.Contains(dbtx.queryRowSQL, "attempt_count = $3") {
		t.Fatalf("DeadLetterRunEffect = args:%#v sql:%s err:%v", dbtx.queryRowArgs, dbtx.queryRowSQL, err)
	}

	dbtx.queryRows = &fakeRows{rows: [][]any{effectValues}}
	expiredEffects, err := q.DeadLetterExpiredRunEffectsAtLimit(context.Background())
	if err != nil || len(expiredEffects) != 1 ||
		!strings.Contains(dbtx.querySQL, "lease_expires_at <= clock_timestamp()") {
		t.Fatalf("DeadLetterExpiredRunEffectsAtLimit = %#v, %v", expiredEffects, err)
	}

	actorID := uuid.New()
	dbtx.row = fakeRow{values: effectValues}
	_, err = q.ReplayRunEffect(context.Background(), ReplayRunEffectParams{
		ID: effectID, ActorType: "admin", ActorID: &actorID, Reason: "operator approved replay",
	})
	if err != nil || !strings.Contains(dbtx.queryRowSQL, "INSERT INTO run_effect_replays") ||
		!strings.Contains(dbtx.queryRowSQL, "FROM replay_audit") || len(dbtx.queryRowArgs) != 4 {
		t.Fatalf("ReplayRunEffect audit = args:%#v sql:%s err:%v", dbtx.queryRowArgs, dbtx.queryRowSQL, err)
	}

	replayID := uuid.New()
	dbtx.queryRows = &fakeRows{rows: [][]any{{
		replayID, effectID, "admin", &actorID, "operator approved replay", now,
	}}}
	replays, err := q.ListRunEffectReplaysByEffect(context.Background(), effectID)
	if err != nil || len(replays) != 1 || replays[0].ID != replayID {
		t.Fatalf("ListRunEffectReplaysByEffect = %#v, %v", replays, err)
	}

	ledgerValues := []any{runID, eventID, agentID, int32(1), int64(25), now}
	dbtx.row = fakeRow{values: ledgerValues}
	ledger, err := q.InsertRunAccountingLedger(context.Background(), InsertRunAccountingLedgerParams{
		RunID: runID, TerminalEventID: eventID, AgentID: agentID,
		SuccessDelta: 1, RevenueDeltaCents: 25,
	})
	if err != nil || ledger.RevenueDeltaCents != 25 {
		t.Fatalf("InsertRunAccountingLedger = %#v, %v", ledger, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "InsertRunAccountingLedger")

	deadLetterID := uuid.New()
	dbtx.row = fakeRow{values: []any{deadLetterID, runID, int32(3), "RETRY_EXHAUSTED", (*string)(nil), now}}
	deadLetter, err := q.CreateRunDeadLetter(context.Background(), CreateRunDeadLetterParams{
		RunID: runID, FinalAttemptNo: 3, ReasonCode: "RETRY_EXHAUSTED",
	})
	if err != nil || deadLetter.ID != deadLetterID {
		t.Fatalf("CreateRunDeadLetter = %#v, %v", deadLetter, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateRunDeadLetter")
	if !strings.Contains(dbtx.queryRowSQL, "DO NOTHING") || strings.Contains(dbtx.queryRowSQL, "DO UPDATE") {
		t.Fatal("CreateRunDeadLetter must not perform a no-op update on conflict")
	}
}

func TestRuntimeV2NodeSessionAndClusterQueries(t *testing.T) {
	now := time.Date(2026, 7, 11, 8, 0, 0, 0, time.UTC)
	digest := "d83e011870cf40bf67723fac1c58ca785d37954bf83638b8f67f69240d20dd4f"
	features := []string{
		"lease_fence", "assignment_confirm", "renew", "resume",
		"event_ack", "result_ack", "cancel", "persistent_spool",
	}
	dbtx := &fakeDBTX{}
	q := New(dbtx)

	dbtx.row = fakeRow{values: []any{int32(63), "063_reliable_runtime_v2", "openlinker.runtime.v2", digest, now, true}}
	contract, err := q.CreateRuntimeSchemaContract(context.Background(), CreateRuntimeSchemaContractParams{
		SchemaVersion: 63, MigrationName: "063_reliable_runtime_v2",
		RuntimeContractID: "openlinker.runtime.v2", RuntimeContractDigest: digest,
		IsCurrent: true,
	})
	if err != nil || contract.SchemaVersion != 63 {
		t.Fatalf("CreateRuntimeSchemaContract = %#v, %v", contract, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateRuntimeSchemaContract")

	cutoverID := uuid.New()
	dbtx.row = fakeRow{values: []any{
		int16(1), "hard_maintenance", int32(2), cutoverID, (*time.Time)(nil),
		(*time.Time)(nil), now, (*time.Time)(nil), int64(1), "migration",
		(*uuid.UUID)(nil), now,
	}}
	control, err := q.UpsertRuntimeClusterControl(context.Background(), UpsertRuntimeClusterControlParams{
		Mode: "hard_maintenance", ExpectedReplicas: 2, CutoverID: cutoverID,
		HardMaintenanceAt: now, UpdatedByType: "migration",
	})
	if err != nil || control.CutoverID != cutoverID || control.ExpectedReplicas != 2 {
		t.Fatalf("UpsertRuntimeClusterControl = %#v, %v", control, err)
	}

	nodeID := uuid.New()
	nodeValues := []any{
		nodeID, "node-a", "serial-a", "thumb-a", "0.2.0", int32(2),
		"openlinker.runtime.v2", digest, features, int32(2),
		int32(0), "active", &now, (*time.Time)(nil), (*time.Time)(nil),
		(*string)(nil), now, now,
	}
	dbtx.row = fakeRow{values: nodeValues}
	node, err := q.UpsertRuntimeNode(context.Background(), UpsertRuntimeNodeParams{
		NodeID: nodeID, DisplayName: "node-a", DeviceCertificateSerial: "serial-a",
		DevicePublicKeyThumbprint: "thumb-a", NodeVersion: "0.2.0",
		ProtocolVersion: 2, RuntimeContractID: "openlinker.runtime.v2",
		RuntimeContractDigest: digest, Features: features, Capacity: 2,
	})
	if err != nil || node.NodeID != nodeID || node.LastSeenAt == nil {
		t.Fatalf("UpsertRuntimeNode = %#v, %v", node, err)
	}

	sessionID, agentID, credentialID, coreID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	sessionValues := []any{
		sessionID, nodeID, agentID, credentialID, "worker-a", int64(1),
		"serial-a", "0.2.0", int32(2), "openlinker.runtime.v2", digest,
		features, int32(2), int32(0), "active", &coreID,
		now, now, (*time.Time)(nil), now, now,
	}
	dbtx.row = fakeRow{values: sessionValues}
	session, err := q.CreateRuntimeSession(context.Background(), CreateRuntimeSessionParams{
		RuntimeSessionID: sessionID, NodeID: nodeID, AgentID: agentID,
		CredentialID: credentialID, WorkerID: "worker-a", SessionEpoch: 1,
		DeviceCertificateSerial: "serial-a", NodeVersion: "0.2.0",
		ProtocolVersion: 2, RuntimeContractID: "openlinker.runtime.v2",
		RuntimeContractDigest: digest, Features: features,
		Capacity: 2, AttachedCoreInstanceID: coreID,
	})
	if err != nil || session.RuntimeSessionID != sessionID || session.AttachedCoreInstanceID == nil {
		t.Fatalf("CreateRuntimeSession = %#v, %v", session, err)
	}

	attachmentID := uuid.New()
	dbtx.row = fakeRow{values: []any{attachmentID, sessionID, coreID, "connected", now, (*time.Time)(nil), (*string)(nil)}}
	attachment, err := q.CreateRuntimeSessionAttachment(context.Background(), CreateRuntimeSessionAttachmentParams{
		RuntimeSessionID: sessionID, CoreInstanceID: coreID, AttachmentKind: "connected",
	})
	if err != nil || attachment.ID != attachmentID {
		t.Fatalf("CreateRuntimeSessionAttachment = %#v, %v", attachment, err)
	}
}

func TestRuntimeV2PrincipalRevocationLockOrderQueries(t *testing.T) {
	nodeA, nodeB := uuid.New(), uuid.New()
	tokenA, tokenB := uuid.New(), uuid.New()
	sessionA, sessionB := uuid.New(), uuid.New()
	attachmentA, attachmentB := uuid.New(), uuid.New()

	sessionRows := &fakeRows{rows: [][]any{
		{sessionA, nodeA, tokenA, "active"},
		{sessionB, nodeB, tokenB, "offline"},
	}}
	dbtx := &fakeDBTX{queryRows: sessionRows}
	q := New(dbtx)

	nodeScope := []uuid.UUID{nodeB, nodeA}
	tokenScope := []uuid.UUID{tokenB, tokenA}
	lockedSessions, err := q.LockRuntimeSessionsForPrincipalRevocation(
		context.Background(),
		LockRuntimeSessionsForPrincipalRevocationParams{
			NodeIDs:  nodeScope,
			TokenIDs: tokenScope,
		},
	)
	if err != nil || len(lockedSessions) != 2 || lockedSessions[0].RuntimeSessionID != sessionA || lockedSessions[1].CredentialID != tokenB {
		t.Fatalf("LockRuntimeSessionsForPrincipalRevocation = %#v, %v", lockedSessions, err)
	}
	requireSQLName(t, dbtx.querySQL, "LockRuntimeSessionsForPrincipalRevocation")
	if !sessionRows.closed ||
		!strings.Contains(dbtx.querySQL, "node_id = ANY($1::uuid[])") ||
		!strings.Contains(dbtx.querySQL, "credential_id = ANY($2::uuid[])") ||
		!strings.Contains(dbtx.querySQL, "ORDER BY runtime_session_id ASC\nFOR UPDATE") {
		t.Fatalf("Session revocation lock must cover the node/token union in UUID order: %s", dbtx.querySQL)
	}
	if !reflect.DeepEqual(dbtx.queryArgs, []any{nodeScope, tokenScope}) {
		t.Fatalf("LockRuntimeSessionsForPrincipalRevocation args = %#v", dbtx.queryArgs)
	}

	nodeRows := &fakeRows{rows: [][]any{{nodeA}, {nodeB}}}
	dbtx.queryRows = nodeRows
	lockedNodes, err := q.LockRuntimeNodesForPrincipalRevocation(context.Background(), nodeScope)
	if err != nil || len(lockedNodes) != 2 || lockedNodes[0] != nodeA || lockedNodes[1] != nodeB {
		t.Fatalf("LockRuntimeNodesForPrincipalRevocation = %#v, %v", lockedNodes, err)
	}
	requireSQLName(t, dbtx.querySQL, "LockRuntimeNodesForPrincipalRevocation")
	if !nodeRows.closed || !strings.Contains(dbtx.querySQL, "ORDER BY node_id ASC\nFOR UPDATE") {
		t.Fatalf("Node revocation locks must be ordered by node_id: %s", dbtx.querySQL)
	}
	if !reflect.DeepEqual(dbtx.queryArgs, []any{nodeScope}) {
		t.Fatalf("LockRuntimeNodesForPrincipalRevocation args = %#v", dbtx.queryArgs)
	}

	tokenRows := &fakeRows{rows: [][]any{{tokenA}, {tokenB}}}
	dbtx.queryRows = tokenRows
	lockedTokens, err := q.LockAgentTokensForPrincipalRevocation(context.Background(), tokenScope)
	if err != nil || len(lockedTokens) != 2 || lockedTokens[0] != tokenA || lockedTokens[1] != tokenB {
		t.Fatalf("LockAgentTokensForPrincipalRevocation = %#v, %v", lockedTokens, err)
	}
	requireSQLName(t, dbtx.querySQL, "LockAgentTokensForPrincipalRevocation")
	if !tokenRows.closed || !strings.Contains(dbtx.querySQL, "ORDER BY id ASC\nFOR UPDATE") {
		t.Fatalf("Token revocation locks must be ordered by token id: %s", dbtx.querySQL)
	}
	if !reflect.DeepEqual(dbtx.queryArgs, []any{tokenScope}) {
		t.Fatalf("LockAgentTokensForPrincipalRevocation args = %#v", dbtx.queryArgs)
	}

	attachmentRows := &fakeRows{rows: [][]any{{attachmentA}, {attachmentB}}}
	dbtx.queryRows = attachmentRows
	sessionScope := []uuid.UUID{sessionB, sessionA}
	lockedAttachments, err := q.LockActiveRuntimeSessionAttachmentsForPrincipalRevocation(context.Background(), sessionScope)
	if err != nil || len(lockedAttachments) != 2 || lockedAttachments[0] != attachmentA || lockedAttachments[1] != attachmentB {
		t.Fatalf("LockActiveRuntimeSessionAttachmentsForPrincipalRevocation = %#v, %v", lockedAttachments, err)
	}
	requireSQLName(t, dbtx.querySQL, "LockActiveRuntimeSessionAttachmentsForPrincipalRevocation")
	if !attachmentRows.closed ||
		!strings.Contains(dbtx.querySQL, "detached_at IS NULL") ||
		!strings.Contains(dbtx.querySQL, "ORDER BY id ASC\nFOR UPDATE") {
		t.Fatalf("Attachment revocation locks must cover active rows in attachment id order: %s", dbtx.querySQL)
	}
	if !reflect.DeepEqual(dbtx.queryArgs, []any{sessionScope}) {
		t.Fatalf("LockActiveRuntimeSessionAttachmentsForPrincipalRevocation args = %#v", dbtx.queryArgs)
	}
}

func ptrTime(value time.Time) *time.Time { return &value }
