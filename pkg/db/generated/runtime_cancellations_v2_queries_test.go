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

func TestRuntimeCancellationV2QueriesAreFencedAndOrdered(t *testing.T) {
	t.Parallel()

	for _, fragment := range []string{
		"r.status = 'canceled'",
		"r.dispatch_state = 'terminal'",
		"r.cancel_state = c.state",
		"c.state IN ('requested', 'delivered', 'stopping')",
		"a.executor_type = 'agent_node'",
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
		if !strings.Contains(findNextDueRuntimeV2Cancellation, fragment) {
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
		if !strings.Contains(lockDueRuntimeV2CancellationRun, fragment) {
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
		"('delivered', 'stopping', 'stopped', 'unsupported', 'failed', 'unconfirmed')",
	} {
		if !strings.Contains(advanceRuntimeV2RunCancellation, fragment) {
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
		if !strings.Contains(finalizeRuntimeV2RunCancellation, fragment) {
			t.Fatalf("Run cancellation finalization missing %q", fragment)
		}
	}
	for _, fragment := range []string{
		"a.lease_id = $4", "a.fencing_token = $5",
		"a.executor_type = 'agent_node'", "a.finished_at IS NULL",
		"c.target_attempt_id = a.id",
		"c.state IN ('stopped', 'unconfirmed')",
	} {
		if !strings.Contains(finishRuntimeV2CanceledAttempt, fragment) {
			t.Fatalf("terminal cancellation Attempt fence missing %q", fragment)
		}
	}
}

func TestRuntimeCancellationV2GeneratedCommandScanAndArgumentOrder(t *testing.T) {
	now := time.Date(2026, 7, 11, 18, 30, 0, 0, time.UTC)
	runID, agentID, cancellationID, attemptID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	nodeID, credentialID, sessionID := uuid.New(), uuid.New(), uuid.New()
	dbtx := &fakeDBTX{row: fakeRow{values: []any{
		runID, agentID, cancellationID, attemptID, now,
	}}}
	q := New(dbtx)
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
	due, err := q.FindNextDueRuntimeV2Cancellation(context.Background(), 30_000)
	if err != nil || due.RunID != runID || due.TargetAttemptID != attemptID || due.RuntimeSessionID != sessionID {
		t.Fatalf("FindNextDueRuntimeV2Cancellation = %#v, %v", due, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "FindNextDueRuntimeV2Cancellation")
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
	lockedDue, err := q.LockDueRuntimeV2CancellationRun(context.Background(), LockDueRuntimeV2CancellationRunParams{
		RunID: runID, CancellationID: cancellationID, TargetAttemptID: attemptID,
		RuntimeSessionID: sessionID, NodeID: nodeID, CommandDeadlineMs: 30_000,
	})
	if err != nil || lockedDue.RunID != runID || lockedDue.DatabaseNow != now {
		t.Fatalf("LockDueRuntimeV2CancellationRun = %#v, %v", lockedDue, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "LockDueRuntimeV2CancellationRun")
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
	advanced, err := q.AdvanceRuntimeV2RunCancellation(context.Background(), AdvanceRuntimeV2RunCancellationParams{
		NextState: "delivered", RunID: runID, CancellationID: cancellationID,
		ExpectedState: "requested",
	})
	if err != nil || advanced.ID != cancellationID || advanced.State != "delivered" {
		t.Fatalf("AdvanceRuntimeV2RunCancellation = %#v, %v", advanced, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "AdvanceRuntimeV2RunCancellation")
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{
		"delivered", (*string)(nil), runID, cancellationID, "requested",
	}) {
		t.Fatalf("cancellation transition args = %#v", dbtx.queryRowArgs)
	}
}

func TestRuntimeCancellationV2MigrationShape(t *testing.T) {
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
