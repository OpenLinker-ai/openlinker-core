package db

import (
	"context"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestRuntimeReconcilerQueriesUseDatabaseClockAndCapacityFirstSkipLocks(t *testing.T) {
	t.Parallel()
	for _, fragment := range []string{
		"WITH database_clock AS MATERIALIZED",
		"SELECT (SELECT MIN(due_at) FROM eligible) AS next_due_at",
		"r.dispatch_state = 'offered'",
		"r.dispatch_state = 'executing'",
		"r.dispatch_state IN ('pending', 'retry_wait')",
	} {
		if !strings.Contains(nextRuntimeReconcileDue, fragment) {
			t.Fatalf("next reconcile due query missing %q", fragment)
		}
	}

	for _, fragment := range []string{
		"WITH database_clock AS MATERIALIZED",
		"clock_timestamp() AS database_now",
		"r.runtime_contract_id = 'openlinker.runtime.v2'",
		"r.cancel_request_id IS NULL",
		"a.offer_expires_at",
		"a.lease_expires_at",
		"a.attempt_deadline_at",
		"r.dispatch_deadline_at",
		"r.run_deadline_at",
		"LIMIT $1",
	} {
		if !strings.Contains(listDueRuntimeReconcileCandidates, fragment) {
			t.Fatalf("candidate discovery missing %q", fragment)
		}
	}
	if strings.Contains(listDueRuntimeReconcileCandidates, "FOR UPDATE") {
		t.Fatal("candidate discovery must not lock Run or Attempt before capacity owners")
	}
	for name, query := range map[string]string{
		"Session": lockRuntimeSessionForReconcile,
		"Node":    lockRuntimeNodeForReconcile,
	} {
		if !strings.Contains(query, "FOR UPDATE SKIP LOCKED") {
			t.Fatalf("%s capacity lock does not use SKIP LOCKED", name)
		}
	}
	for _, fragment := range []string{
		"a.active_runtime_session_id IS NOT DISTINCT FROM $4",
		"a.node_id IS NOT DISTINCT FROM $5",
		"r.cancel_request_id IS NULL",
		"r.active_attempt_id = a.id",
		"r.lease_id = a.lease_id",
		"r.fencing_token = a.fencing_token",
		"FOR UPDATE OF r SKIP LOCKED",
	} {
		if !strings.Contains(lockDueRuntimeRunWithAttempt, fragment) {
			t.Fatalf("exact Attempt Run lock missing %q", fragment)
		}
	}
	for _, fragment := range []string{
		"r.dispatch_state IN ('pending', 'retry_wait')",
		"r.active_attempt_id IS NULL",
		"LEAST(r.dispatch_deadline_at, r.run_deadline_at)",
		"FOR UPDATE OF r SKIP LOCKED",
	} {
		if !strings.Contains(lockDueRuntimeRunWithoutAttempt, fragment) {
			t.Fatalf("deadline-only Run lock missing %q", fragment)
		}
	}
}

func TestRuntimeReconcilerQueriesFenceFinishTransitionsAndTerminalFacts(t *testing.T) {
	t.Parallel()

	for _, fragment := range []string{
		"a.lease_id = $5", "a.fencing_token = $6",
		"a.finished_at IS NULL", "a.result_id IS NULL",
		"r.cancel_request_id IS NULL", "r.active_attempt_id = a.id",
		"$1 = 'offer_expired'", "$1 IN ('lease_expired', 'timeout', 'result_unknown')",
	} {
		if !strings.Contains(finishRuntimeReconciledAttempt, fragment) {
			t.Fatalf("Attempt finish query missing %q", fragment)
		}
	}
	for _, fragment := range []string{
		"dispatch_state = 'pending'", "r.offer_count < r.max_offer_count",
		"r.dispatch_deadline_at > c.database_now", "r.run_deadline_at > c.database_now",
	} {
		if !strings.Contains(resetRuntimeRunAfterReconciledOffer, fragment) {
			t.Fatalf("offer reset query missing %q", fragment)
		}
	}
	for _, fragment := range []string{
		"dispatch_state = 'retry_wait'", "LEAST(",
		"$1::bigint BETWEEN 1 AND 60000", "a.outcome = 'lease_expired'",
		"r.attempt_count < r.max_attempts",
	} {
		if !strings.Contains(transitionRuntimeRunAfterExpiredAttempt, fragment) {
			t.Fatalf("retry transition query missing %q", fragment)
		}
	}
	for _, fragment := range []string{
		"r.cancel_request_id IS NULL", "r.terminal_event_id IS NULL",
		"$8::uuid IS NULL", "r.active_attempt_id = $8",
		"$1 = 'timeout'", "$2 = 'terminal'",
		"$1 = 'failed'", "$2 = 'dead_letter'",
		"$3 = 'RUNTIME_RETRY_EXHAUSTED'", "r.attempt_count >= r.max_attempts",
	} {
		if !strings.Contains(finalizeRuntimeReconciledRun, fragment) {
			t.Fatalf("terminal Run query missing %q", fragment)
		}
	}
}

func TestRuntimeReconcilerGeneratedScanAndArgumentOrder(t *testing.T) {
	now := time.Date(2026, 7, 11, 21, 0, 0, 0, time.UTC)
	runID, attemptID, sessionID, nodeID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	executor := "runtime"
	rows := &fakeRows{rows: [][]any{{
		runID, &attemptID, &executor, &sessionID, &nodeID, now, now,
	}}}
	dbtx := &fakeDBTX{queryRows: rows}
	queries := New(dbtx)
	dueAt := now.Add(time.Minute)
	dbtx.row = fakeRow{values: []any{&dueAt, now}}
	next, err := queries.NextRuntimeReconcileDue(context.Background())
	if err != nil || next.NextDueAt == nil || !next.NextDueAt.Equal(dueAt) || next.DatabaseNow != now {
		t.Fatalf("NextRuntimeReconcileDue = %#v, %v", next, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "NextRuntimeReconcileDue")

	candidates, err := queries.ListDueRuntimeReconcileCandidates(context.Background(), 25)
	if err != nil || len(candidates) != 1 || candidates[0].RunID != runID ||
		candidates[0].AttemptID == nil || *candidates[0].AttemptID != attemptID {
		t.Fatalf("ListDueRuntimeReconcileCandidates = %#v, %v", candidates, err)
	}
	requireSQLName(t, dbtx.querySQL, "ListDueRuntimeReconcileCandidates")
	if !reflect.DeepEqual(dbtx.queryArgs, []any{int32(25)}) {
		t.Fatalf("candidate query args = %#v", dbtx.queryArgs)
	}

	lockedValues := []any{
		runID, uuid.New(), uuid.New(), "running", "executing",
		stringPointer("runtime"), (*bool)(nil), int32(1), int32(3),
		int32(1), int32(3), &attemptID, &attemptID, uuidPointer(uuid.New()),
		int64(4), &executor, &nodeID, stringPointer("worker"), &sessionID,
		now.Add(time.Minute), now.Add(2 * time.Minute), (*uuid.UUID)(nil),
		int32(0), now.Add(-time.Second), now,
	}
	dbtx.row = fakeRow{values: lockedValues}
	locked, err := queries.LockDueRuntimeRunWithAttempt(context.Background(), LockDueRuntimeRunWithAttemptParams{
		RunID: runID, AttemptID: attemptID, ExecutorType: executor,
		RuntimeSessionID: &sessionID, NodeID: &nodeID,
	})
	if err != nil || locked.ID != runID || locked.DatabaseNow != now {
		t.Fatalf("LockDueRuntimeRunWithAttempt = %#v, %v", locked, err)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{runID, attemptID, executor, &sessionID, &nodeID}) {
		t.Fatalf("exact Run lock args = %#v", dbtx.queryRowArgs)
	}

	dbtx.row = fakeRow{values: []any{sessionID}}
	gotSessionID, err := queries.LockRuntimeSessionForReconcile(context.Background(), sessionID)
	if err != nil || gotSessionID != sessionID {
		t.Fatalf("LockRuntimeSessionForReconcile = %s, %v", gotSessionID, err)
	}
	dbtx.row = fakeRow{values: []any{nodeID}}
	gotNodeID, err := queries.LockRuntimeNodeForReconcile(context.Background(), nodeID)
	if err != nil || gotNodeID != nodeID {
		t.Fatalf("LockRuntimeNodeForReconcile = %s, %v", gotNodeID, err)
	}
}

func TestRuntimeReconcilerMigration066Shape(t *testing.T) {
	t.Parallel()

	up, err := os.ReadFile("../../../migrations/066_runtime_v2_deadline_reconciler.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	down, err := os.ReadFile("../../../migrations/066_runtime_v2_deadline_reconciler.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	verify, err := os.ReadFile("../../../migrations/066_runtime_v2_deadline_reconciler_verify.sql")
	if err != nil {
		t.Fatal(err)
	}
	oldDigest := "d83e011870cf40bf67723fac1c58ca785d37954bf83638b8f67f69240d20dd4f"
	newDigest := "60bef5cec7eeab563937187f48a458059995aebee161765032cddc17d0cdfa61"
	for _, fragment := range []string{
		"schema_version = 65", "schema_version = 66",
		"066_runtime_v2_deadline_reconciler", oldDigest, newDigest,
		"runtime contract digest cutover", "status = 'closed'",
		"status IN ('active', 'draining')", "status IN ('offline', 'revoked', 'closed')",
		"fenced slot release", "NEW.inflight = OLD.inflight - 1",
		"idx_runs_runtime_v2_dispatch_due", "idx_runs_runtime_v2_run_deadline_due",
		"idx_run_attempts_runtime_v2_offer_due", "idx_run_attempts_runtime_v2_execution_due",
	} {
		if !strings.Contains(string(up), fragment) {
			t.Fatalf("066 up migration missing %q", fragment)
		}
	}
	for _, fragment := range []string{
		"down refuses immutable new-digest Session or revoked Node evidence",
		"schema_version = 65", "065_runtime_cancellation_lifecycle",
		"Restore the exact pre-066 terminal Session immutability rule",
		oldDigest, newDigest,
	} {
		if !strings.Contains(string(down), fragment) {
			t.Fatalf("066 down migration missing %q", fragment)
		}
	}
	for _, fragment := range []string{
		"runtime schema contract 66 is missing or mismatched",
		"active runtime principal retained an obsolete digest",
		"runtime v2 deadline reconcile indexes are missing",
		oldDigest, newDigest,
	} {
		if !strings.Contains(string(verify), fragment) {
			t.Fatalf("066 verify migration missing %q", fragment)
		}
	}
}

func stringPointer(value string) *string { return &value }

func uuidPointer(value uuid.UUID) *uuid.UUID { return &value }
