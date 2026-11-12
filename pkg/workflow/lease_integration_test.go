package workflow_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/workflow"
)

type runLeaseRow struct {
	status     string
	owner      *uuid.UUID
	epoch      int64
	recoveries int32
}

func (f *admissionFixture) runLease(t *testing.T, runID uuid.UUID) runLeaseRow {
	t.Helper()
	var row runLeaseRow
	require.NoError(t, f.pool.QueryRow(context.Background(), `
		SELECT status, lease_owner, lease_epoch, recovery_count FROM workflow_runs WHERE id = $1`, runID,
	).Scan(&row.status, &row.owner, &row.epoch, &row.recoveries))
	return row
}

func (f *admissionFixture) startBlockedRun(t *testing.T, maxAttempts int32) (uuid.UUID, chan error) {
	t.Helper()
	queued, err := f.svc.StartWorkflowRun(context.Background(), f.ownerID, f.workflowID, &workflow.RunWorkflowRequest{
		MaxAttempts: maxAttempts,
	})
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() {
		_, err := f.svc.ClaimAndRunPendingWorkflow(context.Background())
		done <- err
	}()
	select {
	case <-f.agentCalls:
	case <-time.After(5 * time.Second):
		t.Fatal("workflow worker did not call the Agent")
	}
	return uuid.MustParse(queued.ID), done
}

func waitWorker(t *testing.T, done chan error) {
	t.Helper()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("workflow worker did not stop")
	}
}

// Another executor took the run (epoch moved on). The original executor
// notices at its next renewal, stops, and none of its later writes land.
func TestWorkflowExecutorStopsAndWritesNothingAfterLosingItsLease(t *testing.T) {
	f := newAdmissionFixture(t)
	f.svc.SetWorkflowRunLeaseTiming(30*time.Second, 200*time.Millisecond)
	ctx := context.Background()

	runID, done := f.startBlockedRun(t, 3)
	held := f.runLease(t, runID)
	require.Equal(t, "running", held.status)
	require.NotNil(t, held.owner)
	require.EqualValues(t, 1, held.epoch)

	_, err := f.pool.Exec(ctx, `UPDATE workflow_runs SET lease_epoch = lease_epoch + 1 WHERE id = $1`, runID)
	require.NoError(t, err)
	waitWorker(t, done)

	after := f.runLease(t, runID)
	require.Equal(t, "running", after.status, "the stale executor must not fail or finish the run")
	require.EqualValues(t, 2, after.epoch)
	require.NotNil(t, after.owner, "the stale executor must not release a lease it no longer holds")
	run, err := f.svc.GetWorkflowRun(ctx, f.ownerID, runID)
	require.NoError(t, err)
	require.Len(t, run.Steps, 1)
	require.Equal(t, "running", run.Steps[0].Status, "the stale executor must not write step evidence")
}

// A lease that expires while its executor is stuck (it has not renewed yet)
// is recovered. With max_attempts 1 the run fails, and its in-flight child
// gets a stop request in the same transaction. The stuck executor, when it
// wakes, writes nothing.
func TestWorkflowLeaseRecoveryFailsSpentRunWithStopRequests(t *testing.T) {
	f := newAdmissionFixture(t)
	f.svc.SetWorkflowRunLeaseTiming(30*time.Second, 30*time.Second)
	ctx := context.Background()

	runID, done := f.startBlockedRun(t, 1)
	_, err := f.pool.Exec(ctx, `UPDATE workflow_runs SET lease_expires_at = clock_timestamp() - INTERVAL '1 second' WHERE id = $1`, runID)
	require.NoError(t, err)

	recovered, err := f.svc.RequeueStaleWorkflowRuns(ctx, 0)
	require.NoError(t, err)
	require.EqualValues(t, 1, recovered)
	after := f.runLease(t, runID)
	require.Equal(t, "failed", after.status)
	require.Nil(t, after.owner)

	var reason, state string
	var childRunID uuid.UUID
	require.NoError(t, f.pool.QueryRow(ctx, `
		SELECT reason, state, child_run_id FROM workflow_step_stops WHERE workflow_run_id = $1`, runID,
	).Scan(&reason, &state, &childRunID))
	require.Equal(t, "workflow_failure", reason)
	require.Equal(t, "requested", state)
	require.NotEqual(t, uuid.Nil, childRunID)

	f.release()
	waitWorker(t, done)
	run, err := f.svc.GetWorkflowRun(ctx, f.ownerID, runID)
	require.NoError(t, err)
	require.Equal(t, "failed", run.Status)
	require.Equal(t, "running", run.Steps[0].Status, "the stuck executor must not write after losing the run")
}

// With recovery budget left the run goes back to pending. The stuck
// executor's late writes are rejected, and the next claim (a new epoch)
// finishes the run by reusing the child the first executor launched.
func TestWorkflowLeaseRecoveryWithBudgetRequeuesTheRun(t *testing.T) {
	f := newAdmissionFixture(t)
	f.svc.SetWorkflowRunLeaseTiming(30*time.Second, 30*time.Second)
	ctx := context.Background()

	runID, done := f.startBlockedRun(t, 3)
	_, err := f.pool.Exec(ctx, `UPDATE workflow_runs SET lease_expires_at = clock_timestamp() - INTERVAL '1 second' WHERE id = $1`, runID)
	require.NoError(t, err)

	recovered, err := f.svc.RequeueStaleWorkflowRuns(ctx, 0)
	require.NoError(t, err)
	require.EqualValues(t, 1, recovered)
	after := f.runLease(t, runID)
	require.Equal(t, "pending", after.status)
	require.EqualValues(t, 1, after.recoveries)
	require.Nil(t, after.owner)

	f.release()
	waitWorker(t, done)
	stillPending := f.runLease(t, runID)
	require.Equal(t, "pending", stillPending.status, "the stuck executor must not finish a run it lost")

	claimed, err := f.svc.ClaimAndRunPendingWorkflow(ctx)
	require.NoError(t, err)
	require.True(t, claimed)
	final := f.runLease(t, runID)
	require.Equal(t, "success", final.status)
	require.EqualValues(t, 2, final.epoch)
	require.Nil(t, final.owner, "a finished run releases its lease")
}

func TestSynchronousWorkflowRunExecutesUnderALease(t *testing.T) {
	f := newAdmissionFixture(t)
	f.release()
	resp, err := f.svc.RunWorkflow(context.Background(), f.ownerID, f.workflowID, &workflow.RunWorkflowRequest{})
	require.NoError(t, err)
	require.Equal(t, "success", resp.Status)
	final := f.runLease(t, uuid.MustParse(resp.ID))
	require.EqualValues(t, 1, final.epoch)
	require.Nil(t, final.owner)
}
