package workflow_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	runtimemod "github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/OpenLinker-ai/openlinker-core/pkg/workflow"
)

type engineFixture struct {
	pool       *pgxpool.Pool
	svc        *workflow.Service
	runtime    *runtimemod.Service
	ownerID    uuid.UUID
	workflowID uuid.UUID
	nodeID     uuid.UUID
	agentID    uuid.UUID
}

// newEngineFixture creates a one-node workflow whose Agent runs handler with
// the 1-based call number.
func newEngineFixture(t *testing.T, handler func(call int64, w http.ResponseWriter, r *http.Request)) *engineFixture {
	t.Helper()
	pool := setupWorkflowTestDB(t)
	resetWorkflowAdmission(t, pool)
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		handler(calls.Add(1), w, r)
	}))
	t.Cleanup(server.Close)
	ownerID := insertWorkflowUser(t, pool, "wf-engine-owner")
	creatorID := insertWorkflowUser(t, pool, "wf-engine-creator")
	agentID := insertWorkflowAgent(t, pool, creatorID, server.URL)
	runtimeSvc := runtimemod.NewService(pool, newWorkflowRuntimeTestConfig(10, true))
	runtimeSvc.ConfigureCoreRuntime(uuid.New())
	// As in production: Runtime stops inline Core attempts it cancels.
	coordinatorCtx, stopCoordinator := context.WithCancel(context.Background())
	t.Cleanup(stopCoordinator)
	runtimeSvc.StartCoreAttemptCancellationCoordinator(coordinatorCtx)
	svc := workflow.NewService(pool, runtimeSvc)
	created, err := svc.CreateWorkflow(context.Background(), ownerID, &workflow.CreateWorkflowRequest{
		Name:  "Engine workflow",
		Nodes: []workflow.WorkflowNodeRequest{{Key: "worker", Title: "Worker", AgentID: agentID}},
	})
	require.NoError(t, err)
	return &engineFixture{
		pool: pool, svc: svc, runtime: runtimeSvc, ownerID: ownerID,
		workflowID: uuid.MustParse(created.ID), nodeID: uuid.MustParse(created.Nodes[0].ID), agentID: agentID,
	}
}

func writeAgentOutput(w http.ResponseWriter, output map[string]interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"output": output})
}

type logicalNodeRow struct {
	state      string
	attempt    *int32
	terminalAt *time.Time
}

func readLogicalNode(t *testing.T, pool *pgxpool.Pool, runID, nodeID uuid.UUID) logicalNodeRow {
	t.Helper()
	var row logicalNodeRow
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT state, effective_attempt, terminal_at FROM workflow_run_nodes
		WHERE workflow_run_id = $1 AND workflow_node_id = $2`, runID, nodeID,
	).Scan(&row.state, &row.attempt, &row.terminalAt))
	return row
}

func readStepStop(t *testing.T, pool *pgxpool.Pool, runID uuid.UUID) (reason, state string) {
	t.Helper()
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT reason, state FROM workflow_step_stops WHERE workflow_run_id = $1`, runID,
	).Scan(&reason, &state))
	return reason, state
}

// A node that exceeds its deadline is timed out at once: the logical node is
// terminal, the run fails without waiting for the child, the attempt keeps
// its child, and the stop record is driven to its end afterwards. Runtime
// cannot prove the backend stopped (the child ends as result-unknown), so
// the child is unconfirmed and the timeout is not retried. Stop progress
// never revives the node.
func TestWorkflowNodeTimeoutIsTerminalAndStopsTheChild(t *testing.T) {
	f := newEngineFixture(t, func(_ int64, w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(4 * time.Second):
		}
	})
	f.svc.SetWorkflowNodeTimeout(1500 * time.Millisecond)
	f.svc.SetWorkflowNodeRetryDefaults(3, 0)
	ctx := context.Background()

	started := time.Now()
	_, err := f.svc.RunWorkflow(ctx, f.ownerID, f.workflowID, &workflow.RunWorkflowRequest{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "workflow 执行失败")
	require.Less(t, time.Since(started), 4*time.Second, "the run does not wait for the child")

	var runID uuid.UUID
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT id FROM workflow_runs WHERE workflow_id = $1`, f.workflowID).Scan(&runID))
	run, err := f.svc.GetWorkflowRun(ctx, f.ownerID, runID)
	require.NoError(t, err)
	require.Equal(t, "failed", run.Status)
	require.Len(t, run.Steps, 1)
	require.Equal(t, "timeout", run.Steps[0].Status)
	require.NotEmpty(t, run.Steps[0].RunID, "the timed-out attempt keeps its child")

	node := readLogicalNode(t, f.pool, runID, f.nodeID)
	require.Equal(t, "timeout", node.state)
	require.NotNil(t, node.terminalAt)

	childID := uuid.MustParse(run.Steps[0].RunID)
	evidence, err := runtimemod.ClassifyWorkflowChildStop(ctx, f.pool, childID)
	require.NoError(t, err)
	require.Equal(t, runtimemod.WorkflowChildStopUnconfirmed, evidence.State)

	require.Eventually(t, func() bool {
		_, _ = f.svc.ReconcileWorkflowStepStops(ctx, 10)
		_, state := readStepStop(t, f.pool, runID)
		return state == "stopped" || state == "unconfirmed"
	}, 10*time.Second, 200*time.Millisecond)
	reason, _ := readStepStop(t, f.pool, runID)
	require.Equal(t, "node_timeout", reason)

	after := readLogicalNode(t, f.pool, runID, f.nodeID)
	require.Equal(t, "timeout", after.state, "stop progress never revives a terminal node")
	require.Equal(t, node.terminalAt.UnixNano(), after.terminalAt.UnixNano())
	var attempts int
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM workflow_run_steps WHERE workflow_run_id = $1`, runID).Scan(&attempts))
	require.Equal(t, 1, attempts, "a timeout is never retried")
}

// A natural failure is retried as a new attempt with its own launch and
// child Run when the node has budget left. Workflow children snapshot a
// single accepted execution: retries happen only in the workflow.
func TestWorkflowNodeRetryCreatesNewAttemptLaunchAndChild(t *testing.T) {
	f := newEngineFixture(t, func(call int64, w http.ResponseWriter, _ *http.Request) {
		if call == 1 {
			http.Error(w, "first attempt fails", http.StatusInternalServerError)
			return
		}
		writeAgentOutput(w, map[string]interface{}{"attempt": call})
	})
	f.svc.SetWorkflowNodeRetryDefaults(2, 0)
	ctx := context.Background()

	resp, err := f.svc.RunWorkflow(ctx, f.ownerID, f.workflowID, &workflow.RunWorkflowRequest{})
	require.NoError(t, err)
	require.Equal(t, "success", resp.Status)
	require.Len(t, resp.Steps, 2)
	require.Equal(t, "failed", resp.Steps[0].Status)
	require.Equal(t, "success", resp.Steps[1].Status)
	require.NotEqual(t, resp.Steps[0].RunID, resp.Steps[1].RunID)
	require.EqualValues(t, 2, resp.Steps[1].Output["attempt"])

	runID := uuid.MustParse(resp.ID)
	rows, err := f.pool.Query(ctx, `
		SELECT l.attempt, l.state, r.max_attempts
		FROM workflow_step_launches l JOIN runs r ON r.id = l.run_id
		WHERE l.workflow_run_id = $1 ORDER BY l.attempt`, runID)
	require.NoError(t, err)
	defer rows.Close()
	seen := 0
	for rows.Next() {
		var attempt, maxAttempts int32
		var state string
		require.NoError(t, rows.Scan(&attempt, &state, &maxAttempts))
		seen++
		require.EqualValues(t, seen, attempt)
		require.Equal(t, "attached", state)
		require.EqualValues(t, 1, maxAttempts, "workflow children are never redispatched after acceptance")
	}
	require.NoError(t, rows.Err())
	require.Equal(t, 2, seen)
	node := readLogicalNode(t, f.pool, runID, f.nodeID)
	require.Equal(t, "success", node.state)
	require.EqualValues(t, 2, *node.attempt)
}

// pausedRunWithFinishedChild runs the workflow to success, then puts the run
// back into the state of a paused run whose attempt is still open while its
// child has finished (Core executes direct HTTP children inline, so a real
// pause cannot leave a finished child unsettled deterministically).
func pausedRunWithFinishedChild(t *testing.T, f *admissionFixture) (uuid.UUID, uuid.UUID, time.Time) {
	t.Helper()
	ctx := context.Background()
	f.release()
	resp, err := f.svc.RunWorkflow(ctx, f.ownerID, f.workflowID, &workflow.RunWorkflowRequest{})
	require.NoError(t, err)
	require.Equal(t, "success", resp.Status)
	runID := uuid.MustParse(resp.ID)
	var childID uuid.UUID
	var finishedAt time.Time
	require.NoError(t, f.pool.QueryRow(ctx, `
		SELECT r.id, r.finished_at FROM workflow_step_launches l JOIN runs r ON r.id = l.run_id
		WHERE l.workflow_run_id = $1`, runID).Scan(&childID, &finishedAt))
	_, err = f.pool.Exec(ctx, `
		UPDATE workflow_runs SET status = 'paused', finished_at = NULL, output = NULL WHERE id = $1`, runID)
	require.NoError(t, err)
	_, err = f.pool.Exec(ctx, `
		UPDATE workflow_run_steps SET status = 'running', output = NULL, finished_at = NULL WHERE workflow_run_id = $1`, runID)
	require.NoError(t, err)
	_, err = f.pool.Exec(ctx, `
		UPDATE workflow_run_nodes SET state = 'running', terminal_at = NULL WHERE workflow_run_id = $1`, runID)
	require.NoError(t, err)
	return runID, childID, finishedAt
}

// While a run is paused the deadline scan still settles its attempts, and
// the child's finish time decides: at or before the deadline the attempt
// succeeds with the child's output.
func TestWorkflowPausedAttemptSettlesSuccessWhenChildFinishedBeforeDeadline(t *testing.T) {
	f := newAdmissionFixture(t)
	ctx := context.Background()
	runID, _, finishedAt := pausedRunWithFinishedChild(t, f)
	_, err := f.pool.Exec(ctx, `
		UPDATE workflow_run_steps SET deadline_at = $2::timestamptz + INTERVAL '1 millisecond'
		WHERE workflow_run_id = $1`, runID, finishedAt)
	require.NoError(t, err)

	settled, err := f.svc.SettleExpiredWorkflowAttempts(ctx, 10)
	require.NoError(t, err)
	require.GreaterOrEqual(t, settled, 1)
	run, err := f.svc.GetWorkflowRun(ctx, f.ownerID, runID)
	require.NoError(t, err)
	require.Equal(t, "paused", run.Status)
	require.Equal(t, "success", run.Steps[0].Status)
	require.Equal(t, true, run.Steps[0].Output["ok"])
	require.Equal(t, "success", readLogicalNode(t, f.pool, runID, f.nodeIDFor(t)).state)
	var stops int
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM workflow_step_stops WHERE workflow_run_id = $1`, runID).Scan(&stops))
	require.Zero(t, stops)
}

// A child that finished after the deadline does not count: the attempt is a
// timeout (its output is not adopted) and a stop is recorded.
func TestWorkflowPausedAttemptTimesOutWhenChildFinishedAfterDeadline(t *testing.T) {
	f := newAdmissionFixture(t)
	ctx := context.Background()
	runID, _, finishedAt := pausedRunWithFinishedChild(t, f)
	_, err := f.pool.Exec(ctx, `
		UPDATE workflow_run_steps SET deadline_at = $2::timestamptz - INTERVAL '1 second'
		WHERE workflow_run_id = $1`, runID, finishedAt)
	require.NoError(t, err)

	_, err = f.svc.SettleExpiredWorkflowAttempts(ctx, 10)
	require.NoError(t, err)
	run, err := f.svc.GetWorkflowRun(ctx, f.ownerID, runID)
	require.NoError(t, err)
	require.Equal(t, "paused", run.Status)
	require.Equal(t, "timeout", run.Steps[0].Status)
	require.Empty(t, run.Steps[0].Output)
	require.Equal(t, "timeout", readLogicalNode(t, f.pool, runID, f.nodeIDFor(t)).state)
	reason, state := readStepStop(t, f.pool, runID)
	require.Equal(t, "node_timeout", reason)
	require.Equal(t, "requested", state)

	// The child already ended, so the stop reconciler closes the record.
	_, err = f.svc.ReconcileWorkflowStepStops(ctx, 10)
	require.NoError(t, err)
	_, state = readStepStop(t, f.pool, runID)
	require.Contains(t, []string{"stopped", "unconfirmed"}, state)
}

func (f *admissionFixture) nodeIDFor(t *testing.T) uuid.UUID {
	t.Helper()
	var nodeID uuid.UUID
	require.NoError(t, f.pool.QueryRow(context.Background(),
		`SELECT id FROM workflow_nodes WHERE workflow_id = $1`, f.workflowID).Scan(&nodeID))
	return nodeID
}

// leasedRunWithAttempt makes a running run leased by svc (epoch 1) with a
// running attempt 1 whose deadline is deadlineOffset from now.
func leasedRunWithAttempt(t *testing.T, f *admissionFixture, deadlineOffset time.Duration) (uuid.UUID, uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	queued, err := f.svc.StartWorkflowRun(ctx, f.ownerID, f.workflowID, &workflow.RunWorkflowRequest{})
	require.NoError(t, err)
	runID := uuid.MustParse(queued.ID)
	nodeID := f.nodeIDFor(t)
	_, err = f.pool.Exec(ctx, `
		UPDATE workflow_runs
		SET status = 'running', claimed_at = NOW(), attempt_count = 1,
		    lease_owner = $2, lease_epoch = 1, lease_mode = 'execute',
		    lease_expires_at = clock_timestamp() + INTERVAL '1 minute'
		WHERE id = $1`, runID, f.svc.InstanceIDForTest())
	require.NoError(t, err)
	_, err = f.pool.Exec(ctx, `
		INSERT INTO workflow_run_steps (workflow_run_id, workflow_node_id, node_key, agent_id, status, input, sequence, attempt, deadline_at)
		SELECT $1, n.id, n.node_key, n.agent_id, 'running', '{}'::jsonb, 0, 1, clock_timestamp() + make_interval(secs => $3)
		FROM workflow_nodes n WHERE n.id = $2`, runID, nodeID, deadlineOffset.Seconds())
	require.NoError(t, err)
	_, err = f.pool.Exec(ctx, `
		INSERT INTO workflow_run_nodes (workflow_run_id, workflow_node_id, node_key, state, effective_attempt)
		VALUES ($1, $2, 'worker', 'running', 1)`, runID, nodeID)
	require.NoError(t, err)
	return runID, nodeID
}

// A failure before any child exists is decided under the parent lock with
// the database clock: before the deadline it is failed, after it timeout.
// Without the run's lease nothing is written.
func TestWorkflowFailAttemptLocalAcrossDeadline(t *testing.T) {
	f := newAdmissionFixture(t)
	f.release()
	ctx := context.Background()

	runID, nodeID := leasedRunWithAttempt(t, f, time.Minute)
	stale := workflow.WithWorkflowRunLeaseForTest(ctx, runID, f.svc.InstanceIDForTest(), 2)
	require.ErrorIs(t, f.svc.FailAttemptLocalForTest(stale, runID, nodeID, 1, "mapping failed"), workflow.ErrWorkflowRunLeaseLostForTest)
	require.Equal(t, "running", readLogicalNode(t, f.pool, runID, nodeID).state)

	leased := workflow.WithWorkflowRunLeaseForTest(ctx, runID, f.svc.InstanceIDForTest(), 1)
	require.NoError(t, f.svc.FailAttemptLocalForTest(leased, runID, nodeID, 1, "mapping failed"))
	var status string
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT status FROM workflow_run_steps WHERE workflow_run_id = $1`, runID).Scan(&status))
	require.Equal(t, "failed", status)
	require.Equal(t, "failed", readLogicalNode(t, f.pool, runID, nodeID).state)

	lateRunID, lateNodeID := leasedRunWithAttempt(t, f, -time.Second)
	lateLease := workflow.WithWorkflowRunLeaseForTest(ctx, lateRunID, f.svc.InstanceIDForTest(), 1)
	require.NoError(t, f.svc.FailAttemptLocalForTest(lateLease, lateRunID, lateNodeID, 1, "mapping failed"))
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT status FROM workflow_run_steps WHERE workflow_run_id = $1`, lateRunID).Scan(&status))
	require.Equal(t, "timeout", status)
	require.Equal(t, "timeout", readLogicalNode(t, f.pool, lateRunID, lateNodeID).state)
}

// A child creation that arrives after its attempt's deadline is rejected
// inside Runtime's creation transaction; nothing is created.
func TestWorkflowChildCreationAfterAttemptDeadlineIsRejected(t *testing.T) {
	f := newAdmissionFixture(t)
	f.release()
	ctx := context.Background()
	runID, nodeID := leasedRunWithAttempt(t, f, -time.Second)

	var agentID uuid.UUID
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT agent_id FROM workflow_nodes WHERE id = $1`, nodeID).Scan(&agentID))
	req := &runtimemod.RunRequest{
		AgentID: agentID.String(), Input: map[string]interface{}{},
		IdempotencyKey:   "workflow/" + runID.String() + "/late",
		CreationProtocol: "workflow", CreationMethod: "node.run",
	}
	identity, err := f.runtime.PrepareRunCreationIdentity(req, "api")
	require.NoError(t, err)
	token := uuid.New()
	_, err = f.pool.Exec(ctx, `
		INSERT INTO workflow_step_launches (workflow_run_id, workflow_node_id, workflow_run_step_id, node_key,
		    launch_token, idempotency_key_hash, creation_fingerprint, state, attempt)
		SELECT $1, $2, s.id, 'worker', $3, $4, $5, 'claimed', 1
		FROM workflow_run_steps s WHERE s.workflow_run_id = $1 AND s.attempt = 1`,
		runID, nodeID, token, identity.IdempotencyKeyHash, identity.CreationFingerprint)
	require.NoError(t, err)

	_, err = f.runtime.RunWorkflowChild(ctx, f.ownerID, req, "api", runtimemod.WorkflowChildLaunchFence{
		WorkflowRunID: runID, WorkflowNodeID: nodeID, Attempt: 1, LaunchToken: token,
		LeaseOwner: f.svc.InstanceIDForTest(), LeaseEpoch: 1,
	})
	require.Error(t, err)
	var launchState string
	var childID *uuid.UUID
	require.NoError(t, f.pool.QueryRow(ctx, `
		SELECT state, run_id FROM workflow_step_launches WHERE workflow_run_id = $1`, runID).Scan(&launchState, &childID))
	require.Equal(t, "claimed", launchState)
	require.Nil(t, childID)
}

// Only the holder of a stop record's claim can cancel the child, and the
// claim is checked with the database clock in Runtime's own transaction.
func TestWorkflowChildCancellationRequiresTheStopClaim(t *testing.T) {
	f := newAdmissionFixture(t)
	ctx := context.Background()
	runID, done := f.startBlockedRun(t, 3)
	defer func() {
		f.release()
		waitWorker(t, done)
	}()

	nodeID := f.nodeIDFor(t)
	var childID uuid.UUID
	require.Eventually(t, func() bool {
		return f.pool.QueryRow(ctx, `
			SELECT run_id FROM workflow_step_launches WHERE workflow_run_id = $1 AND run_id IS NOT NULL`, runID).Scan(&childID) == nil
	}, 5*time.Second, 50*time.Millisecond)
	claimOwner := uuid.New()
	_, err := f.pool.Exec(ctx, `
		INSERT INTO workflow_step_stops (workflow_run_id, workflow_node_id, attempt, child_run_id, reason, state,
		                                 claim_owner, claim_epoch, claim_expires_at)
		VALUES ($1, $2, 1, $3, 'node_timeout', 'requested', $4, 5, clock_timestamp() + INTERVAL '1 minute')`,
		runID, nodeID, childID, claimOwner)
	require.NoError(t, err)
	fence := runtimemod.WorkflowChildStopFence{
		WorkflowRunID: runID, WorkflowNodeID: nodeID, Attempt: 1, ChildRunID: childID,
		ClaimOwner: claimOwner, ClaimEpoch: 4,
	}
	require.ErrorIs(t, f.runtime.CancelWorkflowChildFenced(ctx, fence), runtimemod.ErrWorkflowChildStopFenceRejected)
	var childStatus string
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT status FROM runs WHERE id = $1`, childID).Scan(&childStatus))
	require.Equal(t, "running", childStatus, "a stale claim cancels nothing")

	fence.ClaimEpoch = 5
	require.NoError(t, f.runtime.CancelWorkflowChildFenced(ctx, fence))
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT status FROM runs WHERE id = $1`, childID).Scan(&childStatus))
	require.Equal(t, "canceled", childStatus)
	_, state := readStepStop(t, f.pool, runID)
	require.Equal(t, "stopping", state)
}

func requireWorkflowConflictCode(t *testing.T, err error, code string) {
	t.Helper()
	var httpErr *httpx.HTTPError
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, http.StatusConflict, httpErr.Status)
	require.Equal(t, httpx.ErrorCode(code), httpErr.Code)
}

// timedOutRun fails a run by node timeout and waits until its stop record
// is finished. Its child ended result-unknown, so it is unconfirmed.
func timedOutRun(t *testing.T) (*engineFixture, uuid.UUID) {
	t.Helper()
	f := newEngineFixture(t, func(_ int64, w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(4 * time.Second):
		}
	})
	f.svc.SetWorkflowNodeTimeout(time.Second)
	ctx := context.Background()
	_, err := f.svc.RunWorkflow(ctx, f.ownerID, f.workflowID, &workflow.RunWorkflowRequest{})
	require.Error(t, err)
	var runID uuid.UUID
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT id FROM workflow_runs WHERE workflow_id = $1`, f.workflowID).Scan(&runID))
	require.Eventually(t, func() bool {
		_, _ = f.svc.ReconcileWorkflowStepStops(ctx, 10)
		_, state := readStepStop(t, f.pool, runID)
		return state == "stopped" || state == "unconfirmed"
	}, 10*time.Second, 200*time.Millisecond)
	return f, runID
}

// Retry needs the owner's explicit acknowledgement when a child of the source
// run cannot be shown to have stopped; the acknowledgement is recorded.
func TestWorkflowRetryOfUnconfirmedChildrenNeedsAcknowledgement(t *testing.T) {
	f, runID := timedOutRun(t)
	ctx := context.Background()

	_, err := f.svc.RetryWorkflowRun(ctx, f.ownerID, runID, nil)
	requireWorkflowConflictCode(t, err, "WORKFLOW_CHILDREN_UNCONFIRMED")

	retried, err := f.svc.RetryWorkflowRun(ctx, f.ownerID, runID, &workflow.RetryWorkflowRunRequest{AcknowledgeUnconfirmedChildren: true})
	require.NoError(t, err)
	require.Equal(t, "pending", retried.Status)
	require.Equal(t, f.ownerID.String(), retried.UnconfirmedChildrenAcknowledgedBy)
}

// While a stop of the source run is still open, Retry and Rerun refuse even
// with the acknowledgement.
func TestWorkflowRetryAndRerunRefuseWhileStopsAreOpen(t *testing.T) {
	f, runID := timedOutRun(t)
	ctx := context.Background()
	_, err := f.pool.Exec(ctx, `
		UPDATE workflow_step_stops SET state = 'requested', applied_at = NULL, finished_at = NULL
		WHERE workflow_run_id = $1`, runID)
	require.NoError(t, err)

	_, err = f.svc.RetryWorkflowRun(ctx, f.ownerID, runID, &workflow.RetryWorkflowRunRequest{AcknowledgeUnconfirmedChildren: true})
	requireWorkflowConflictCode(t, err, "WORKFLOW_CHILDREN_STOPPING")
	_, err = f.svc.RerunWorkflowStep(ctx, f.ownerID, runID, &workflow.RerunWorkflowStepRequest{
		NodeKey: "worker", AcknowledgeUnconfirmedChildren: true,
	})
	requireWorkflowConflictCode(t, err, "WORKFLOW_CHILDREN_STOPPING")
}

// A node outside the rerun set that failed the workflow cannot be reused.
func TestWorkflowRerunRefusesReusingANodeThatFailedTheWorkflow(t *testing.T) {
	f := newEngineFixture(t, func(_ int64, w http.ResponseWriter, r *http.Request) {
		var req runtimemod.AgentRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Input["node_key"] == "fails" {
			http.Error(w, "fails", http.StatusInternalServerError)
			return
		}
		writeAgentOutput(w, map[string]interface{}{"ok": true})
	})
	ctx := context.Background()
	created, err := f.svc.CreateWorkflow(ctx, f.ownerID, &workflow.CreateWorkflowRequest{
		Name: "Two roots",
		Nodes: []workflow.WorkflowNodeRequest{
			{Key: "fails", Title: "Fails", AgentID: f.agentID},
			{Key: "other", Title: "Other", AgentID: f.agentID},
		},
		Edges: []map[string]interface{}{},
	})
	require.NoError(t, err)
	_, err = f.svc.RunWorkflow(ctx, f.ownerID, uuid.MustParse(created.ID), &workflow.RunWorkflowRequest{})
	require.Error(t, err)
	var runID uuid.UUID
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT id FROM workflow_runs WHERE workflow_id = $1`, created.ID).Scan(&runID))

	_, err = f.svc.RerunWorkflowStep(ctx, f.ownerID, runID, &workflow.RerunWorkflowStepRequest{
		NodeKey: "other", AcknowledgeUnconfirmedChildren: true,
	})
	requireWorkflowConflictCode(t, err, "WORKFLOW_RERUN_NOT_REUSABLE")
}
