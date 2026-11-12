package workflow_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	runtimemod "github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/OpenLinker-ai/openlinker-core/pkg/workflow"
)

type admissionFixture struct {
	pool       *pgxpool.Pool
	svc        *workflow.Service
	runtime    *runtimemod.Service
	ownerID    uuid.UUID
	adminID    uuid.UUID
	workflowID uuid.UUID
	release    func()
	agentCalls chan struct{}
}

// newAdmissionFixture creates one owner, one administrator and a one-node
// workflow whose Agent blocks until release is called.
func newAdmissionFixture(t *testing.T) *admissionFixture {
	t.Helper()
	pool := setupWorkflowTestDB(t)
	resetWorkflowAdmission(t, pool)
	t.Cleanup(func() { resetWorkflowAdmission(t, pool) })

	calls := make(chan struct{}, 8)
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		select {
		case calls <- struct{}{}:
		default:
		}
		select {
		case <-gate:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"output": map[string]interface{}{"ok": true}})
	}))
	// Cleanups run last-in first-out: release the blocked handler before
	// closing the server, which waits for in-flight handlers.
	t.Cleanup(server.Close)
	t.Cleanup(release)

	ownerID := insertWorkflowUser(t, pool, "wf-admission-owner")
	adminID := insertWorkflowUser(t, pool, "wf-admission-admin")
	_, err := pool.Exec(context.Background(), `UPDATE users SET is_admin = TRUE WHERE id = $1`, adminID)
	require.NoError(t, err)
	creatorID := insertWorkflowUser(t, pool, "wf-admission-creator")
	agentID := insertWorkflowAgent(t, pool, creatorID, server.URL)
	runtimeSvc := runtimemod.NewService(pool, newWorkflowRuntimeTestConfig(5, true))
	runtimeSvc.ConfigureCoreRuntime(uuid.New())
	svc := workflow.NewService(pool, runtimeSvc)
	created, err := svc.CreateWorkflow(context.Background(), ownerID, &workflow.CreateWorkflowRequest{
		Name:  "Admission workflow",
		Nodes: []workflow.WorkflowNodeRequest{{Key: "worker", Title: "Worker", AgentID: agentID}},
	})
	require.NoError(t, err)
	return &admissionFixture{
		pool: pool, svc: svc, runtime: runtimeSvc, ownerID: ownerID, adminID: adminID,
		workflowID: uuid.MustParse(created.ID), release: release, agentCalls: calls,
	}
}

func resetWorkflowAdmission(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`UPDATE workflow_admission SET state = 'open', reason = NULL, changed_by = NULL WHERE singleton`)
	require.NoError(t, err)
}

func (f *admissionFixture) setAdmission(t *testing.T, state string) *workflow.WorkflowAdmissionResponse {
	t.Helper()
	resp, err := f.svc.SetWorkflowAdmission(context.Background(), f.adminID, &workflow.SetWorkflowAdmissionRequest{
		State: state, Reason: "release drain test",
	})
	require.NoError(t, err)
	require.Equal(t, state, resp.State)
	return resp
}

func requireWorkflowDraining(t *testing.T, err error) {
	t.Helper()
	var httpErr *httpx.HTTPError
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, http.StatusServiceUnavailable, httpErr.Status)
	require.Equal(t, httpx.ErrorCode("WORKFLOW_DRAINING"), httpErr.Code)
}

func TestWorkflowAdmissionDrainingRefusesEveryEntryAndPersists(t *testing.T) {
	f := newAdmissionFixture(t)
	ctx := context.Background()
	f.release()

	pending, err := f.svc.StartWorkflowRun(ctx, f.ownerID, f.workflowID, &workflow.RunWorkflowRequest{
		Input: map[string]interface{}{"topic": "before drain"},
	})
	require.NoError(t, err)
	failedRunID := uuid.New()
	_, err = f.pool.Exec(ctx, `
		INSERT INTO workflow_runs (id, workflow_id, user_id, status, input, finished_at, error_message)
		VALUES ($1, $2, $3, 'failed', '{"topic":"retry me"}', now(), 'synthetic failure')`,
		failedRunID, f.workflowID, f.ownerID)
	require.NoError(t, err)

	drained := f.setAdmission(t, workflow.WorkflowAdmissionDraining)
	require.Equal(t, "release drain test", drained.Reason)
	require.NotNil(t, drained.ChangedBy)
	require.Equal(t, f.adminID.String(), *drained.ChangedBy)
	require.EqualValues(t, 1, drained.NonterminalRuns)

	_, err = f.svc.StartWorkflowRun(ctx, f.ownerID, f.workflowID, &workflow.RunWorkflowRequest{
		Input: map[string]interface{}{"topic": "async"},
	})
	requireWorkflowDraining(t, err)
	_, err = f.svc.RunWorkflow(ctx, f.ownerID, f.workflowID, &workflow.RunWorkflowRequest{
		Input: map[string]interface{}{"topic": "sync"},
	})
	requireWorkflowDraining(t, err)
	_, err = f.svc.RetryWorkflowRun(ctx, f.ownerID, failedRunID, nil)
	requireWorkflowDraining(t, err)

	// The worker does not claim the pending run while draining.
	claimed, err := f.svc.ClaimAndRunPendingWorkflow(ctx)
	require.NoError(t, err)
	require.False(t, claimed)
	run, err := f.svc.GetWorkflowRun(ctx, f.ownerID, uuid.MustParse(pending.ID))
	require.NoError(t, err)
	require.Equal(t, "pending", run.Status)

	// The state is durable: a new Service, as after a restart, still drains.
	restarted := workflow.NewService(f.pool, f.runtime)
	state, err := restarted.GetWorkflowAdmission(ctx)
	require.NoError(t, err)
	require.Equal(t, workflow.WorkflowAdmissionDraining, state.State)
	_, err = restarted.StartWorkflowRun(ctx, f.ownerID, f.workflowID, &workflow.RunWorkflowRequest{})
	requireWorkflowDraining(t, err)

	f.setAdmission(t, workflow.WorkflowAdmissionOpen)
	claimed, err = f.svc.ClaimAndRunPendingWorkflow(ctx)
	require.NoError(t, err)
	require.True(t, claimed)
}

func TestWorkflowAdmissionSwitchWaitsForInFlightCreation(t *testing.T) {
	f := newAdmissionFixture(t)
	ctx := context.Background()

	tx, err := f.pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	inFlight, err := db.New(f.pool).WithTx(tx).CreatePendingWorkflowRun(ctx, db.CreatePendingWorkflowRunParams{
		WorkflowID: f.workflowID, UserID: f.ownerID, Input: []byte(`{}`), MaxAttempts: 3,
	})
	require.NoError(t, err)

	switched := make(chan error, 1)
	go func() {
		_, err := f.svc.SetWorkflowAdmission(ctx, f.adminID, &workflow.SetWorkflowAdmissionRequest{
			State: workflow.WorkflowAdmissionDraining,
		})
		switched <- err
	}()
	select {
	case err := <-switched:
		t.Fatalf("switch committed while a creation that read 'open' was in flight: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	require.NoError(t, tx.Commit(ctx))
	select {
	case err := <-switched:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("switch did not complete after the creation committed")
	}

	// The in-flight run exists; nothing can be created after the switch.
	_, err = f.svc.GetWorkflowRun(ctx, f.ownerID, inFlight.ID)
	require.NoError(t, err)
	_, err = db.New(f.pool).CreatePendingWorkflowRun(ctx, db.CreatePendingWorkflowRunParams{
		WorkflowID: f.workflowID, UserID: f.ownerID, Input: []byte(`{}`), MaxAttempts: 3,
	})
	require.ErrorIs(t, err, pgx.ErrNoRows)
}

func TestAdminCancelWorkflowRunAcrossUsersRecordsRequesterAndKeepsOwner(t *testing.T) {
	f := newAdmissionFixture(t)
	ctx := context.Background()
	f.release()

	pending, err := f.svc.StartWorkflowRun(ctx, f.ownerID, f.workflowID, &workflow.RunWorkflowRequest{})
	require.NoError(t, err)
	runID := uuid.MustParse(pending.ID)

	// The owner-facing cancel path still refuses a non-owner.
	_, err = f.svc.CancelWorkflowRun(ctx, f.adminID, runID)
	requireWorkflowHTTPStatus(t, err, http.StatusNotFound)

	listed, err := f.svc.ListAdminWorkflowRuns(ctx, "nonterminal", 10)
	require.NoError(t, err)
	require.Len(t, listed.Items, 1)
	require.Equal(t, f.ownerID.String(), listed.Items[0].OwnerID)

	canceled, err := f.svc.AdminCancelWorkflowRun(ctx, f.adminID, runID)
	require.NoError(t, err)
	require.Equal(t, "canceled", canceled.Status)
	require.Equal(t, "stopped", canceled.CancellationState)
	require.Equal(t, f.ownerID.String(), canceled.OwnerID)
	require.Equal(t, f.adminID.String(), canceled.RequestedBy)
	require.Equal(t, "admin", canceled.RequestedVia)

	// The owner remains the authorization subject and can read the evidence.
	evidence, err := f.svc.GetWorkflowCancellationEvidence(ctx, f.ownerID, runID)
	require.NoError(t, err)
	require.Equal(t, f.ownerID, evidence.ActorUserID)
	require.Equal(t, "stopped", evidence.State)

	again, err := f.svc.AdminCancelWorkflowRun(ctx, f.adminID, runID)
	require.NoError(t, err)
	require.Equal(t, "stopped", again.CancellationState)

	listed, err = f.svc.ListAdminWorkflowRuns(ctx, "nonterminal", 10)
	require.NoError(t, err)
	require.Empty(t, listed.Items)
}

// A running run with an in-flight child: the admin cancellation returns while
// the child is still stopping, and the scan finishes it once the child ends.
func TestWorkflowCancellationScanFinishesAdminCancelAfterChildStops(t *testing.T) {
	f := newAdmissionFixture(t)
	ctx := context.Background()

	// Runtime's own database fallback stops the Core-executed child and records
	// the physical stop evidence the workflow cancellation waits for.
	coordinatorCtx, stopCoordinator := context.WithCancel(ctx)
	t.Cleanup(stopCoordinator)
	f.runtime.StartCoreAttemptCancellationCoordinator(coordinatorCtx)

	queued, err := f.svc.StartWorkflowRun(ctx, f.ownerID, f.workflowID, &workflow.RunWorkflowRequest{})
	require.NoError(t, err)
	runID := uuid.MustParse(queued.ID)
	workerDone := make(chan error, 1)
	go func() {
		_, err := f.svc.ClaimAndRunPendingWorkflow(ctx)
		workerDone <- err
	}()
	select {
	case <-f.agentCalls:
	case <-time.After(3 * time.Second):
		t.Fatal("workflow worker did not call the Agent")
	}

	f.setAdmission(t, workflow.WorkflowAdmissionDraining)
	canceled, err := f.svc.AdminCancelWorkflowRun(ctx, f.adminID, runID)
	require.NoError(t, err)
	require.Equal(t, "canceled", canceled.Status)
	require.Equal(t, "stopping", canceled.CancellationState)

	cancelling, err := f.svc.ListAdminWorkflowRuns(ctx, "cancelling", 10)
	require.NoError(t, err)
	require.Len(t, cancelling.Items, 1)
	admission, err := f.svc.GetWorkflowAdmission(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 0, admission.NonterminalRuns)
	require.EqualValues(t, 1, admission.CancellingRuns)

	// Fresh cancellations are left to the request that wrote them.
	attempted, err := f.svc.ReconcileWorkflowCancellations(ctx, 10)
	require.NoError(t, err)
	require.Zero(t, attempted)

	// The Agent never answers; Runtime's coordinator cancels the in-flight call
	// and records the stop, which lets the workflow worker finish.
	select {
	case err := <-workerDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("workflow worker did not finish after Runtime stopped the child")
	}

	ageWorkflowCancellations(t, f.pool)
	require.Eventually(t, func() bool {
		_, err := f.svc.ReconcileWorkflowCancellations(ctx, 10)
		require.NoError(t, err)
		ageWorkflowCancellations(t, f.pool)
		evidence, err := f.svc.GetWorkflowCancellationEvidence(ctx, f.ownerID, runID)
		require.NoError(t, err)
		return evidence.State == "stopped" || evidence.State == "unconfirmed"
	}, 10*time.Second, 200*time.Millisecond)

	cancelling, err = f.svc.ListAdminWorkflowRuns(ctx, "cancelling", 10)
	require.NoError(t, err)
	require.Empty(t, cancelling.Items)
}

// A crash after the cancellation committed but before its first
// reconciliation leaves it in requested with a canceled parent. The scan
// does not filter on the parent and finishes it.
func TestWorkflowCancellationScanFinishesRequestedLeftByCrash(t *testing.T) {
	f := newAdmissionFixture(t)
	ctx := context.Background()

	runID := uuid.New()
	_, err := f.pool.Exec(ctx, `
		INSERT INTO workflow_runs (id, workflow_id, user_id, status, finished_at, error_message)
		VALUES ($1, $2, $3, 'canceled', now(), 'workflow run canceled')`,
		runID, f.workflowID, f.ownerID)
	require.NoError(t, err)
	_, err = f.pool.Exec(ctx, `
		INSERT INTO workflow_run_cancellations (
		    workflow_run_id, id, actor_user_id, reason_code, state, requested_by, requested_via
		) VALUES ($1, $2, $3, 'OWNER_CANCEL_REQUESTED', 'requested', $3, 'owner')`,
		runID, uuid.New(), f.ownerID)
	require.NoError(t, err)
	ageWorkflowCancellations(t, f.pool)

	attempted, err := f.svc.ReconcileWorkflowCancellations(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, attempted)
	evidence, err := f.svc.GetWorkflowCancellationEvidence(ctx, f.ownerID, runID)
	require.NoError(t, err)
	require.Equal(t, "stopped", evidence.State)
}

func TestWorkflowRunStepsOrderIsDeterministicForDuplicateSequence(t *testing.T) {
	f := newAdmissionFixture(t)
	ctx := context.Background()

	var nodeID, agentID uuid.UUID
	require.NoError(t, f.pool.QueryRow(ctx,
		`SELECT id, agent_id FROM workflow_nodes WHERE workflow_id = $1`, f.workflowID,
	).Scan(&nodeID, &agentID))
	runID := uuid.New()
	_, err := f.pool.Exec(ctx, `
		INSERT INTO workflow_runs (id, workflow_id, user_id, status, finished_at)
		VALUES ($1, $2, $3, 'failed', now())`, runID, f.workflowID, f.ownerID)
	require.NoError(t, err)
	// Two attempts for the same node with the same sequence and created_at, as
	// migration 104 numbers the duplicates the old recovery gap could leave.
	// The attempt orders them, whatever their ids.
	low := uuid.MustParse("00000000-0000-4000-8000-000000000001")
	high := uuid.MustParse("ffffffff-ffff-4fff-bfff-ffffffffffff")
	for _, item := range []struct {
		id      uuid.UUID
		status  string
		attempt int
	}{{high, "success", 1}, {low, "failed", 2}} {
		_, err := f.pool.Exec(ctx, `
			INSERT INTO workflow_run_steps (
			    id, workflow_run_id, workflow_node_id, node_key, agent_id, status, sequence,
			    attempt, created_at, updated_at
			) VALUES ($1, $2, $3, 'worker', $4, $5, 1, $6, '2026-11-11T00:00:00Z', '2026-11-11T00:00:00Z')`,
			item.id, runID, nodeID, agentID, item.status, item.attempt)
		require.NoError(t, err)
	}
	for i := 0; i < 5; i++ {
		steps, err := db.New(f.pool).ListWorkflowRunSteps(ctx, runID)
		require.NoError(t, err)
		require.Len(t, steps, 2)
		require.Equal(t, high, steps[0].ID)
		require.Equal(t, low, steps[1].ID)
	}
}

func ageWorkflowCancellations(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
		UPDATE workflow_run_cancellations
		SET updated_at = updated_at - interval '1 minute',
		    requested_at = LEAST(requested_at, updated_at - interval '1 minute')
		WHERE state IN ('requested', 'stopping')`)
	require.NoError(t, err)
}
