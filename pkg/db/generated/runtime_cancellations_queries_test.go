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

func TestRuntimeCancellationQueriesAreFencedAndOrdered(t *testing.T) {
	t.Parallel()
	for _, fragment := range []string{
		"WITH database_clock AS MATERIALIZED",
		"eligible_attempts AS MATERIALIZED",
		"FROM run_attempts",
		"WHERE finished_at IS NULL",
		"SELECT (SELECT MIN(due_at) FROM eligible) AS next_due_at",
		"c.state IN ('requested', 'delivered', 'stopping', 'unsupported', 'failed')",
		"a.executor_type IN ('core_http', 'core_mcp')",
	} {
		if !strings.Contains(nextRuntimeCancellationReapDue, fragment) {
			t.Fatalf("next cancellation reap query missing %q", fragment)
		}
	}

	for _, fragment := range []string{
		"r.status = 'canceled'",
		"r.dispatch_state = 'terminal'",
		"r.cancel_state = c.state",
		"c.state IN ('requested', 'delivered', 'stopping')",
		"a.executor_type = 'runtime'",
		"a.finished_at IS NULL",
		"a.agent_id = $1",
		"a.node_id = $2",
		"a.runtime_token_id = $3",
		"a.runtime_worker_id = $4",
		"a.runtime_session_id = $5",
		"$6::bigint * INTERVAL '1 millisecond'",
		"> clock_timestamp()",
		"FOR UPDATE OF r SKIP LOCKED",
	} {
		if !strings.Contains(lockNextRuntimeCancellationCommandRun, fragment) {
			t.Fatalf("command Run lock missing %q", fragment)
		}
	}
	for _, fragment := range []string{
		"c.state IN ('requested', 'delivered', 'stopping', 'unsupported', 'failed')",
		"$1::bigint * INTERVAL '1 millisecond'",
		"<= clock_timestamp()",
		"a.finished_at IS NULL", "a.outcome IS NULL",
		"a.slot_acquired_at IS NOT NULL", "a.slot_released_at IS NULL",
		"a.active_runtime_session_id IS NOT NULL", "a.node_id IS NOT NULL",
	} {
		if !strings.Contains(findNextDueRuntimeCancellation, fragment) {
			t.Fatalf("due cancellation discovery missing %q", fragment)
		}
	}
	for _, query := range []string{lockRuntimeSessionForCancellationReap, lockRuntimeNodeForCancellationReap} {
		if !strings.Contains(query, "FOR UPDATE") {
			t.Fatal("reaper capacity owner query must take a blocking row lock")
		}
	}
	for _, fragment := range []string{
		"r.id = $1", "c.id = $2", "a.id = $3",
		"a.active_runtime_session_id = $4", "a.node_id = $5",
		"$6::bigint * INTERVAL '1 millisecond'", "<= clock_timestamp()",
		"FOR UPDATE OF r",
	} {
		if !strings.Contains(lockDueRuntimeCancellationRun, fragment) {
			t.Fatalf("due cancellation Run lock missing %q", fragment)
		}
	}
	for _, fragment := range []string{
		"run_id = $1", "id = $2", "FOR UPDATE",
	} {
		if !strings.Contains(lockRunCancellationForMutation, fragment) {
			t.Fatalf("cancellation row lock missing %q", fragment)
		}
	}
	for _, fragment := range []string{
		"state = $1", "updated_at = clock_timestamp()",
		"run_id = $3", "id = $4", "state = $5",
		"state NOT IN ('stopped', 'unsupported', 'failed')",
		"('delivered', 'stopping', 'stopped', 'unsupported', 'failed', 'unconfirmed')",
	} {
		if !strings.Contains(advanceRuntimeRunCancellation, fragment) {
			t.Fatalf("cancellation transition fence missing %q", fragment)
		}
	}
	for _, fragment := range []string{
		"status = 'canceled'", "dispatch_state = 'terminal'",
		"terminal_event_id = $2", "cancel_request_id = c.id",
		"r.active_attempt_id = c.target_attempt_id",
		"a.finished_at IS NULL", "a.outcome IS NULL",
		"a.executor_type IN ('core_http', 'core_mcp')",
		"a.finished_at IS NOT NULL", "a.outcome = 'canceled'",
	} {
		if !strings.Contains(finalizeRuntimeRunCancellation, fragment) {
			t.Fatalf("Run cancellation finalization missing %q", fragment)
		}
	}
	for _, fragment := range []string{
		"a.lease_id = $4", "a.fencing_token = $5",
		"a.executor_type = 'runtime'", "a.finished_at IS NULL",
		"c.target_attempt_id = a.id",
		"c.state IN ('stopped', 'unconfirmed')",
		"c.state IN ('unsupported', 'failed')", "c.error_code IS NOT NULL",
		"$6::bigint * INTERVAL '1 millisecond'", "<= clock_timestamp()",
	} {
		if !strings.Contains(finishRuntimeCanceledAttempt, fragment) {
			t.Fatalf("terminal cancellation Attempt fence missing %q", fragment)
		}
	}
}

func TestRuntimeCancellationGeneratedCommandScanAndArgumentOrder(t *testing.T) {
	now := time.Date(2026, 7, 11, 18, 30, 0, 0, time.UTC)
	runID, agentID, cancellationID, attemptID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	nodeID, credentialID, sessionID := uuid.New(), uuid.New(), uuid.New()
	commandValues := []any{
		runID, agentID, cancellationID, attemptID, now,
	}
	dbtx := &fakeDBTX{row: fakeRow{values: commandValues}}
	q := New(dbtx)
	dueAt := now.Add(time.Minute)
	dbtx.row = fakeRow{values: []any{&dueAt, now}}
	next, err := q.NextRuntimeCancellationReapDue(context.Background(), 30_000)
	if err != nil || next.NextDueAt == nil || !next.NextDueAt.Equal(dueAt) || next.DatabaseNow != now {
		t.Fatalf("NextRuntimeCancellationReapDue = %#v, %v", next, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "NextRuntimeCancellationReapDue")
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{int64(30_000)}) {
		t.Fatalf("next cancellation reap args = %#v", dbtx.queryRowArgs)
	}
	dbtx.row = fakeRow{values: commandValues}
	params := LockNextRuntimeCancellationCommandRunParams{
		AgentID: agentID, NodeID: nodeID, CredentialID: credentialID,
		WorkerID: "cancel-worker", RuntimeSessionID: sessionID, CommandDeadlineMs: 30_000,
	}
	row, err := q.LockNextRuntimeCancellationCommandRun(context.Background(), params)
	if err != nil || row.RunID != runID || row.CancellationID != cancellationID || row.DatabaseNow != now {
		t.Fatalf("LockNextRuntimeCancellationCommandRun = %#v, %v", row, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "LockNextRuntimeCancellationCommandRun")
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{
		agentID, nodeID, credentialID, "cancel-worker", sessionID, int64(30_000),
	}) {
		t.Fatalf("command Run lock args = %#v", dbtx.queryRowArgs)
	}

	dbtx.row = fakeRow{values: []any{runID, agentID, cancellationID, attemptID, sessionID, nodeID}}
	due, err := q.FindNextDueRuntimeCancellation(context.Background(), 30_000)
	if err != nil || due.RunID != runID || due.TargetAttemptID != attemptID || due.RuntimeSessionID != sessionID {
		t.Fatalf("FindNextDueRuntimeCancellation = %#v, %v", due, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "FindNextDueRuntimeCancellation")
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{int64(30_000)}) {
		t.Fatalf("due cancellation discovery args = %#v", dbtx.queryRowArgs)
	}

	dbtx.row = fakeRow{values: []any{sessionID}}
	lockedSessionID, err := q.LockRuntimeSessionForCancellationReap(context.Background(), sessionID)
	if err != nil || lockedSessionID != sessionID {
		t.Fatalf("LockRuntimeSessionForCancellationReap = %s, %v", lockedSessionID, err)
	}
	dbtx.row = fakeRow{values: []any{nodeID}}
	lockedNodeID, err := q.LockRuntimeNodeForCancellationReap(context.Background(), nodeID)
	if err != nil || lockedNodeID != nodeID {
		t.Fatalf("LockRuntimeNodeForCancellationReap = %s, %v", lockedNodeID, err)
	}

	dbtx.row = fakeRow{values: []any{runID, agentID, cancellationID, attemptID, now}}
	lockedDue, err := q.LockDueRuntimeCancellationRun(context.Background(), LockDueRuntimeCancellationRunParams{
		RunID: runID, CancellationID: cancellationID, TargetAttemptID: attemptID,
		RuntimeSessionID: sessionID, NodeID: nodeID, CommandDeadlineMs: 30_000,
	})
	if err != nil || lockedDue.RunID != runID || lockedDue.DatabaseNow != now {
		t.Fatalf("LockDueRuntimeCancellationRun = %#v, %v", lockedDue, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "LockDueRuntimeCancellationRun")
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{
		runID, cancellationID, attemptID, sessionID, nodeID, int64(30_000),
	}) {
		t.Fatalf("due cancellation Run lock args = %#v", dbtx.queryRowArgs)
	}

	reason := "owner canceled"
	cancellationValues := []any{
		cancellationID, runID, &attemptID, "delivered", "user", uuid.New(),
		&reason, now.Add(-time.Second), &now, (*time.Time)(nil),
		(*time.Time)(nil), (*time.Time)(nil), (*string)(nil), now,
	}
	dbtx.row = fakeRow{values: cancellationValues}
	advanced, err := q.AdvanceRuntimeRunCancellation(context.Background(), AdvanceRuntimeRunCancellationParams{
		NextState: "delivered", RunID: runID, CancellationID: cancellationID,
		ExpectedState: "requested",
	})
	if err != nil || advanced.ID != cancellationID || advanced.State != "delivered" {
		t.Fatalf("AdvanceRuntimeRunCancellation = %#v, %v", advanced, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "AdvanceRuntimeRunCancellation")
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{
		"delivered", (*string)(nil), runID, cancellationID, "requested",
	}) {
		t.Fatalf("cancellation transition args = %#v", dbtx.queryRowArgs)
	}
}

func TestRuntimeCancellationMigrationShape(t *testing.T) {
	t.Parallel()

	up, err := os.ReadFile("../../../migrations/065_runtime_cancellation_lifecycle.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	down, err := os.ReadFile("../../../migrations/065_runtime_cancellation_lifecycle.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	verify, err := os.ReadFile("../../../migrations/065_runtime_cancellation_lifecycle_verify.sql")
	if err != nil {
		t.Fatal(err)
	}

	for _, fragment := range []string{
		"SET schema_version = 65",
		"migration_name = '065_runtime_cancellation_lifecycle'",
		"unsettled cancellation target",
		"cancellation_state IN ('requested', 'delivered', 'stopping', 'unsupported', 'failed')",
		"cancellation_state IN ('stopped', 'unconfirmed')",
	} {
		if !strings.Contains(string(up), fragment) {
			t.Fatalf("up migration missing %q", fragment)
		}
	}
	for _, fragment := range []string{
		"down refuses unsettled canceled Attempt capacity evidence",
		"SET schema_version = 64",
		"migration_name = '064_runtime_lease_resume_primitives'",
	} {
		if !strings.Contains(string(down), fragment) {
			t.Fatalf("down migration missing %q", fragment)
		}
	}
	for _, fragment := range []string{
		"runtime schema contract 65 is missing or mismatched",
		"runtime cancellation lifecycle invariants are missing",
		"stored runtime cancellation lifecycle evidence is inconsistent",
	} {
		if !strings.Contains(string(verify), fragment) {
			t.Fatalf("verify migration missing %q", fragment)
		}
	}
}

func TestRuntimeCancellationTerminalReapMigrationShape(t *testing.T) {
	t.Parallel()

	up, err := os.ReadFile("../../../migrations/076_runtime_cancellation_terminal_reap.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	down, err := os.ReadFile("../../../migrations/076_runtime_cancellation_terminal_reap.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	verify, err := os.ReadFile("../../../migrations/076_runtime_cancellation_terminal_reap_verify.sql")
	if err != nil {
		t.Fatal(err)
	}

	for _, fragment := range []string{
		"migration 076 requires the exact current schema contract 75",
		"76,\n    '076_runtime_cancellation_terminal_reap'",
		"cancellation_state IN ('requested', 'delivered', 'stopping')",
		"cancellation_state IN ('unsupported', 'failed')",
		"latest_attempt.error_code IS DISTINCT FROM 'CANCEL_UNCONFIRMED'",
		"latest_attempt.finished_at\n                                           < cancellation_requested_at + INTERVAL '30 seconds'",
	} {
		if !strings.Contains(string(up), fragment) {
			t.Fatalf("up migration missing %q", fragment)
		}
	}
	for _, fragment := range []string{
		"rollback refuses reaped negative terminal cancellation evidence",
		"migration 076 rollback requires the exact current schema contract 76",
		"DELETE FROM runtime_schema_contracts",
		"schema_version = 75",
	} {
		if !strings.Contains(string(down), fragment) {
			t.Fatalf("down migration missing %q", fragment)
		}
	}
	for _, fragment := range []string{
		"runtime schema contract 76 is missing or mismatched",
		"negative terminal cancellation deadline invariant is missing or over-broad",
		"terminal cancellation state or original error evidence is not immutable",
		"stored negative terminal cancellation reap evidence is inconsistent",
	} {
		if !strings.Contains(string(verify), fragment) {
			t.Fatalf("verify migration missing %q", fragment)
		}
	}
}
