package workflow_test

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"

	"github.com/kinzhi/openlinker-core/pkg/config"
	runtimemod "github.com/kinzhi/openlinker-core/pkg/runtime"
	"github.com/kinzhi/openlinker-core/pkg/workflow"
)

func TestWorkflowRunExecutesAgentNodesAndPersistsChildRuns(t *testing.T) {
	pool := setupWorkflowTestDB(t)

	var mu sync.Mutex
	var inputs []map[string]interface{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req runtimemod.AgentRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))

		mu.Lock()
		inputs = append(inputs, req.Input)
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(map[string]interface{}{
			"output": map[string]interface{}{
				"step":          req.Input["node_key"],
				"previous_node": req.Input["previous_node"],
				"summary":       fmt.Sprintf("completed %v", req.Input["node_key"]),
			},
			"events": []map[string]interface{}{
				{
					"event_type": "run.message.delta",
					"payload": map[string]interface{}{
						"text": fmt.Sprintf("workflow node %v done", req.Input["node_key"]),
					},
				},
			},
		})
		require.NoError(t, err)
	}))
	t.Cleanup(server.Close)

	userID := insertWorkflowUser(t, pool, "wf-user")
	creatorID := insertWorkflowUser(t, pool, "wf-creator")
	agentID := insertWorkflowAgent(t, pool, creatorID, server.URL)

	runtimeSvc := runtimemod.NewService(pool, &config.Config{
		PlatformFeeRate:         0,
		RunTimeoutSeconds:       5,
		AllowLocalHTTPEndpoints: true,
	})
	svc := workflow.NewService(pool, runtimeSvc)

	created, err := svc.CreateWorkflow(context.Background(), userID, &workflow.CreateWorkflowRequest{
		Name:        "Two step workflow",
		Description: "executes two real agent runs",
		Nodes: []workflow.WorkflowNodeRequest{
			{Key: "extract", Title: "Extract", AgentID: agentID},
			{Key: "summarize", Title: "Summarize", AgentID: agentID},
		},
	})
	require.NoError(t, err)
	require.Len(t, created.Nodes, 2)

	run, err := svc.RunWorkflow(context.Background(), userID, uuid.MustParse(created.ID), &workflow.RunWorkflowRequest{
		Input: map[string]interface{}{"topic": "workflow validation"},
	})
	require.NoError(t, err)
	require.Equal(t, "success", run.Status)
	require.Len(t, run.Steps, 2)
	require.NotEmpty(t, run.Steps[0].RunID)
	require.NotEmpty(t, run.Steps[1].RunID)
	require.Equal(t, "extract", run.Steps[0].Output["step"])
	require.Equal(t, "summarize", run.Steps[1].Output["step"])
	require.Equal(t, "extract", run.Steps[1].Input["previous_node"])
	require.Equal(t, "summarize", run.Output["step"])

	mu.Lock()
	require.Len(t, inputs, 2)
	require.Equal(t, "extract", inputs[0]["node_key"])
	require.Equal(t, "summarize", inputs[1]["node_key"])
	require.Equal(t, "extract", inputs[1]["previous_node"])
	mu.Unlock()

	reloaded, err := svc.GetWorkflowRun(context.Background(), userID, uuid.MustParse(run.ID))
	require.NoError(t, err)
	require.Equal(t, "success", reloaded.Status)
	require.Len(t, reloaded.Steps, 2)

	listed, err := svc.ListWorkflows(context.Background(), userID, 20)
	require.NoError(t, err)
	require.Len(t, listed.Items, 1)
	require.Equal(t, int32(1), listed.Total)
	require.Equal(t, created.ID, listed.Items[0].ID)
	require.Len(t, listed.Items[0].Nodes, 2)

	var runCount int
	err = pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM runs WHERE user_id = $1 AND agent_id = $2`, userID, agentID).
		Scan(&runCount)
	require.NoError(t, err)
	require.Equal(t, 2, runCount)
}

func TestWorkflowRunExecutesIndependentBranchesInParallelAndAggregatesOutputs(t *testing.T) {
	pool := setupWorkflowTestDB(t)

	var mu sync.Mutex
	active := 0
	maxActive := 0
	var inputs []map[string]interface{}
	branchStarted := make(chan string, 2)
	releaseBranches := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseBranches) }) }
	t.Cleanup(release)
	go func() {
		seen := map[string]struct{}{}
		deadline := time.After(3 * time.Second)
		for len(seen) < 2 {
			select {
			case key := <-branchStarted:
				seen[key] = struct{}{}
			case <-deadline:
				return
			}
		}
		release()
	}()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req runtimemod.AgentRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		nodeKey, _ := req.Input["node_key"].(string)

		mu.Lock()
		active++
		if active > maxActive {
			maxActive = active
		}
		inputs = append(inputs, req.Input)
		mu.Unlock()

		if nodeKey == "collect" || nodeKey == "analyze" {
			branchStarted <- nodeKey
			select {
			case <-releaseBranches:
			case <-time.After(3 * time.Second):
			}
		}

		mu.Lock()
		active--
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(map[string]interface{}{
			"output": map[string]interface{}{
				"step":         nodeKey,
				"dependencies": req.Input["dependencies"],
				"summary":      fmt.Sprintf("completed %v", nodeKey),
			},
		})
		require.NoError(t, err)
	}))
	t.Cleanup(server.Close)

	userID := insertWorkflowUser(t, pool, "wf-dag-user")
	creatorID := insertWorkflowUser(t, pool, "wf-dag-creator")
	agentID := insertWorkflowAgent(t, pool, creatorID, server.URL)

	runtimeSvc := runtimemod.NewService(pool, &config.Config{
		PlatformFeeRate:         0,
		RunTimeoutSeconds:       5,
		AllowLocalHTTPEndpoints: true,
	})
	svc := workflow.NewService(pool, runtimeSvc)

	created, err := svc.CreateWorkflow(context.Background(), userID, &workflow.CreateWorkflowRequest{
		Name:        "DAG workflow",
		Description: "runs independent branches in parallel",
		Nodes: []workflow.WorkflowNodeRequest{
			{Key: "collect", Title: "Collect", AgentID: agentID},
			{Key: "analyze", Title: "Analyze", AgentID: agentID},
			{Key: "synthesize", Title: "Synthesize", AgentID: agentID},
		},
		Edges: []map[string]interface{}{
			{"from": "collect", "to": "synthesize"},
			{"source": "analyze", "target": "synthesize"},
		},
	})
	require.NoError(t, err)
	require.Equal(t, []map[string]interface{}{
		{"from": "collect", "to": "synthesize"},
		{"from": "analyze", "to": "synthesize"},
	}, created.Edges)

	run, err := svc.RunWorkflow(context.Background(), userID, uuid.MustParse(created.ID), &workflow.RunWorkflowRequest{
		Input: map[string]interface{}{"topic": "parallel workflow validation"},
	})
	require.NoError(t, err)
	require.Equal(t, "success", run.Status)
	require.Len(t, run.Steps, 3)
	require.Equal(t, "collect", run.Steps[0].NodeKey)
	require.Equal(t, "analyze", run.Steps[1].NodeKey)
	require.Equal(t, "synthesize", run.Steps[2].NodeKey)
	require.Equal(t, "synthesize", run.Output["step"])

	terminalNodes, ok := run.Output["terminal_nodes"].([]interface{})
	require.True(t, ok)
	require.Equal(t, "synthesize", terminalNodes[0])
	workflowOutputs, ok := run.Output["workflow_outputs"].(map[string]interface{})
	require.True(t, ok)
	require.Contains(t, workflowOutputs, "collect")
	require.Contains(t, workflowOutputs, "analyze")
	require.Contains(t, workflowOutputs, "synthesize")

	deps, ok := run.Steps[2].Input["dependencies"].(map[string]interface{})
	require.True(t, ok)
	require.Contains(t, deps, "collect")
	require.Contains(t, deps, "analyze")

	mu.Lock()
	require.GreaterOrEqual(t, maxActive, 2)
	require.Len(t, inputs, 3)
	mu.Unlock()
}

func TestRerunWorkflowStepReusesUnaffectedStepsAndComparesRuns(t *testing.T) {
	pool := setupWorkflowTestDB(t)

	var mu sync.Mutex
	callCountByNode := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req runtimemod.AgentRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		nodeKey, _ := req.Input["node_key"].(string)
		mu.Lock()
		callCountByNode[nodeKey]++
		callNumber := callCountByNode[nodeKey]
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(map[string]interface{}{
			"output": map[string]interface{}{
				"step":         nodeKey,
				"node_call":    callNumber,
				"dependencies": req.Input["dependencies"],
			},
		}))
	}))
	t.Cleanup(server.Close)

	userID := insertWorkflowUser(t, pool, "wf-step-rerun-user")
	creatorID := insertWorkflowUser(t, pool, "wf-step-rerun-creator")
	agentID := insertWorkflowAgent(t, pool, creatorID, server.URL)
	runtimeSvc := runtimemod.NewService(pool, &config.Config{
		PlatformFeeRate:         0,
		RunTimeoutSeconds:       5,
		AllowLocalHTTPEndpoints: true,
	})
	svc := workflow.NewService(pool, runtimeSvc)

	created, err := svc.CreateWorkflow(context.Background(), userID, &workflow.CreateWorkflowRequest{
		Name: "Step rerun workflow",
		Nodes: []workflow.WorkflowNodeRequest{
			{Key: "collect", Title: "Collect", AgentID: agentID},
			{Key: "analyze", Title: "Analyze", AgentID: agentID},
			{Key: "synthesize", Title: "Synthesize", AgentID: agentID},
		},
		Edges: []map[string]interface{}{
			{"from": "collect", "to": "synthesize"},
			{"from": "analyze", "to": "synthesize"},
		},
	})
	require.NoError(t, err)

	original, err := svc.RunWorkflow(context.Background(), userID, uuid.MustParse(created.ID), &workflow.RunWorkflowRequest{
		Input: map[string]interface{}{"topic": "step rerun"},
	})
	require.NoError(t, err)
	require.Equal(t, "success", original.Status)
	require.Len(t, original.Steps, 3)

	rerun, err := svc.RerunWorkflowStep(context.Background(), userID, uuid.MustParse(original.ID), &workflow.RerunWorkflowStepRequest{
		NodeKey: "analyze",
	})
	require.NoError(t, err)
	require.Equal(t, original.ID, rerun.SourceRunID)
	require.NotEqual(t, original.ID, rerun.RerunRunID)
	require.Equal(t, "success", rerun.Run.Status)
	require.Equal(t, []string{"collect"}, rerun.ReusedNodeKeys)
	require.Equal(t, []string{"analyze", "synthesize"}, rerun.RerunNodeKeys)
	require.Equal(t, "collect", rerun.Run.Steps[0].NodeKey)
	require.Equal(t, original.Steps[0].RunID, rerun.Run.Steps[0].RunID)
	require.Equal(t, original.Steps[0].Output, rerun.Run.Steps[0].Output)
	require.NotEqual(t, original.Steps[1].RunID, rerun.Run.Steps[1].RunID)
	require.NotEqual(t, original.Steps[2].RunID, rerun.Run.Steps[2].RunID)
	require.Equal(t, float64(2), rerun.Run.Steps[1].Output["node_call"])
	require.Equal(t, float64(2), rerun.Run.Steps[2].Output["node_call"])
	require.True(t, rerun.Comparison.OutputChanged)
	require.ElementsMatch(t, []string{"analyze", "synthesize"}, rerun.Comparison.ChangedNodeKeys)

	comparison, err := svc.CompareWorkflowRuns(context.Background(), userID, uuid.MustParse(original.ID), uuid.MustParse(rerun.RerunRunID))
	require.NoError(t, err)
	require.False(t, comparison.StatusChanged)
	require.True(t, comparison.OutputChanged)
	require.Len(t, comparison.Steps, 3)
	require.False(t, comparison.Steps[0].Changed)
	require.True(t, comparison.Steps[1].RunChanged)
	require.True(t, comparison.Steps[2].RunChanged)

	var runCount int
	err = pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM runs WHERE user_id = $1 AND agent_id = $2`, userID, agentID).
		Scan(&runCount)
	require.NoError(t, err)
	require.Equal(t, 5, runCount)
}

func TestStartWorkflowRunQueuesAndWorkerExecutes(t *testing.T) {
	pool := setupWorkflowTestDB(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req runtimemod.AgentRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(map[string]interface{}{
			"output": map[string]interface{}{
				"step":    req.Input["node_key"],
				"summary": "async workflow done",
			},
		}))
	}))
	t.Cleanup(server.Close)

	userID := insertWorkflowUser(t, pool, "wf-async-user")
	creatorID := insertWorkflowUser(t, pool, "wf-async-creator")
	agentID := insertWorkflowAgent(t, pool, creatorID, server.URL)

	runtimeSvc := runtimemod.NewService(pool, &config.Config{
		PlatformFeeRate:         0,
		RunTimeoutSeconds:       5,
		AllowLocalHTTPEndpoints: true,
	})
	svc := workflow.NewService(pool, runtimeSvc)

	created, err := svc.CreateWorkflow(context.Background(), userID, &workflow.CreateWorkflowRequest{
		Name: "Async workflow",
		Nodes: []workflow.WorkflowNodeRequest{
			{Key: "worker", Title: "Worker", AgentID: agentID},
		},
	})
	require.NoError(t, err)

	queued, err := svc.StartWorkflowRun(context.Background(), userID, uuid.MustParse(created.ID), &workflow.RunWorkflowRequest{
		Input:       map[string]interface{}{"topic": "async"},
		MaxAttempts: 2,
	})
	require.NoError(t, err)
	require.Equal(t, "pending", queued.Status)
	require.Equal(t, int32(0), queued.AttemptCount)
	require.Equal(t, int32(2), queued.MaxAttempts)
	require.Empty(t, queued.Steps)

	claimed, err := svc.ClaimAndRunPendingWorkflow(context.Background())
	require.NoError(t, err)
	require.True(t, claimed)

	done, err := svc.GetWorkflowRun(context.Background(), userID, uuid.MustParse(queued.ID))
	require.NoError(t, err)
	require.Equal(t, "success", done.Status)
	require.Equal(t, int32(1), done.AttemptCount)
	require.Len(t, done.Steps, 1)
	require.Equal(t, "success", done.Steps[0].Status)

	history, err := svc.ListWorkflowRuns(context.Background(), userID, uuid.MustParse(created.ID), 10)
	require.NoError(t, err)
	require.Equal(t, int32(1), history.Total)
	require.Len(t, history.Items, 1)
	require.Equal(t, queued.ID, history.Items[0].ID)
}

func TestStartRunWorkerProcessesPendingRunsInBurst(t *testing.T) {
	pool := setupWorkflowTestDB(t)

	var mu sync.Mutex
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req runtimemod.AgentRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		mu.Lock()
		callCount++
		callNumber := callCount
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(map[string]interface{}{
			"output": map[string]interface{}{
				"step":       req.Input["node_key"],
				"call_index": callNumber,
			},
		}))
	}))
	t.Cleanup(server.Close)

	ctx := context.Background()
	userID := insertWorkflowUser(t, pool, "wf-worker-user")
	creatorID := insertWorkflowUser(t, pool, "wf-worker-creator")
	agentID := insertWorkflowAgent(t, pool, creatorID, server.URL)
	runtimeSvc := runtimemod.NewService(pool, &config.Config{
		PlatformFeeRate:         0,
		RunTimeoutSeconds:       5,
		AllowLocalHTTPEndpoints: true,
	})
	svc := workflow.NewService(pool, runtimeSvc)

	created, err := svc.CreateWorkflow(ctx, userID, &workflow.CreateWorkflowRequest{
		Name: "Worker burst workflow",
		Nodes: []workflow.WorkflowNodeRequest{
			{Key: "worker", Title: "Worker", AgentID: agentID},
		},
	})
	require.NoError(t, err)
	workflowID := uuid.MustParse(created.ID)

	queuedA, err := svc.StartWorkflowRun(ctx, userID, workflowID, &workflow.RunWorkflowRequest{
		Input: map[string]interface{}{"run": "a"},
	})
	require.NoError(t, err)
	queuedB, err := svc.StartWorkflowRun(ctx, userID, workflowID, &workflow.RunWorkflowRequest{
		Input: map[string]interface{}{"run": "b"},
	})
	require.NoError(t, err)

	workerCtx, cancelWorker := context.WithCancel(ctx)
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		workflow.StartRunWorker(workerCtx, svc, workflow.RunWorkerConfig{
			Interval:   time.Hour,
			StaleAfter: time.Minute,
			ClaimBurst: 3,
		})
	}()

	doneA := waitForWorkflowRunStatus(t, svc, userID, uuid.MustParse(queuedA.ID), "success")
	doneB := waitForWorkflowRunStatus(t, svc, userID, uuid.MustParse(queuedB.ID), "success")
	cancelWorker()
	select {
	case <-workerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("workflow worker did not stop after context cancellation")
	}

	require.Len(t, doneA.Steps, 1)
	require.Len(t, doneB.Steps, 1)
	require.Equal(t, "success", doneA.Steps[0].Status)
	require.Equal(t, "success", doneB.Steps[0].Status)
	mu.Lock()
	require.Equal(t, 2, callCount)
	mu.Unlock()

	claimed, err := svc.ClaimAndRunPendingWorkflow(ctx)
	require.NoError(t, err)
	require.False(t, claimed)
}

func TestWorkflowRunWaitsForRuntimePullCompletion(t *testing.T) {
	pool := setupWorkflowTestDB(t)
	ctx := context.Background()

	userID := insertWorkflowUser(t, pool, "wf-runtime-pull-user")
	creatorID := insertWorkflowUser(t, pool, "wf-runtime-pull-creator")
	agentID := insertWorkflowAgent(t, pool, creatorID, "https://example.com/not-used")
	setWorkflowAgentRuntimePullMode(t, pool, agentID)

	runtimeSvc := runtimemod.NewService(pool, &config.Config{
		PlatformFeeRate:         0,
		RunTimeoutSeconds:       5,
		AllowLocalHTTPEndpoints: true,
	})
	token := insertWorkflowRuntimeToken(t, pool, agentID, creatorID)
	_, err := runtimeSvc.HeartbeatAgent(ctx, token)
	require.NoError(t, err)
	svc := workflow.NewService(pool, runtimeSvc)

	created, err := svc.CreateWorkflow(ctx, userID, &workflow.CreateWorkflowRequest{
		Name: "Runtime pull workflow",
		Nodes: []workflow.WorkflowNodeRequest{
			{Key: "worker", Title: "Worker", AgentID: agentID},
		},
	})
	require.NoError(t, err)

	type workflowRunResult struct {
		run *workflow.WorkflowRunResponse
		err error
	}
	resultC := make(chan workflowRunResult, 1)
	go func() {
		run, runErr := svc.RunWorkflow(ctx, userID, uuid.MustParse(created.ID), &workflow.RunWorkflowRequest{
			Input: map[string]interface{}{"topic": "runtime pull"},
		})
		resultC <- workflowRunResult{run: run, err: runErr}
	}()

	claimed, err := claimWorkflowRuntimeRun(t, runtimeSvc, token)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.Equal(t, agentID.String(), claimed.AgentID)
	require.Equal(t, "worker", claimed.Input["node_key"])
	workflowInput, ok := claimed.Input["workflow_input"].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "runtime pull", workflowInput["topic"])
	require.NotNil(t, claimed.A2A)
	require.Equal(t, claimed.RunID, claimed.A2A.CurrentRunID)

	childRunID := uuid.MustParse(claimed.RunID)
	completed, err := runtimeSvc.CompleteRuntimePullRun(ctx, token, childRunID, &runtimemod.RuntimePullResultRequest{
		Status: "success",
		Output: map[string]interface{}{
			"answer": "done by runtime pull",
			"step":   claimed.Input["node_key"],
		},
		Events: []runtimemod.AgentEvent{{
			EventType: "run.message.delta",
			Payload:   map[string]interface{}{"text": "runtime pull step done"},
		}},
	})
	require.NoError(t, err)
	require.Equal(t, "success", completed.Status)

	var result workflowRunResult
	select {
	case result = <-resultC:
	case <-time.After(3 * time.Second):
		t.Fatal("workflow run did not finish after runtime pull result")
	}
	require.NoError(t, result.err)
	require.NotNil(t, result.run)
	require.Equal(t, "success", result.run.Status)
	require.Len(t, result.run.Steps, 1)
	require.Equal(t, "success", result.run.Steps[0].Status)
	require.Equal(t, claimed.RunID, result.run.Steps[0].RunID)
	require.Equal(t, "done by runtime pull", result.run.Steps[0].Output["answer"])
	require.Equal(t, "done by runtime pull", result.run.Output["answer"])
}

func TestWorkflowRunPropagatesRuntimePullFailure(t *testing.T) {
	pool := setupWorkflowTestDB(t)
	ctx := context.Background()

	userID := insertWorkflowUser(t, pool, "wf-runtime-pull-fail-user")
	creatorID := insertWorkflowUser(t, pool, "wf-runtime-pull-fail-creator")
	agentID := insertWorkflowAgent(t, pool, creatorID, "https://example.com/not-used")
	setWorkflowAgentRuntimePullMode(t, pool, agentID)

	runtimeSvc := runtimemod.NewService(pool, &config.Config{
		PlatformFeeRate:         0,
		RunTimeoutSeconds:       5,
		AllowLocalHTTPEndpoints: true,
	})
	token := insertWorkflowRuntimeToken(t, pool, agentID, creatorID)
	_, err := runtimeSvc.HeartbeatAgent(ctx, token)
	require.NoError(t, err)
	svc := workflow.NewService(pool, runtimeSvc)

	created, err := svc.CreateWorkflow(ctx, userID, &workflow.CreateWorkflowRequest{
		Name: "Runtime pull failure workflow",
		Nodes: []workflow.WorkflowNodeRequest{
			{Key: "worker", Title: "Worker", AgentID: agentID},
		},
	})
	require.NoError(t, err)

	errC := make(chan error, 1)
	go func() {
		_, runErr := svc.RunWorkflow(ctx, userID, uuid.MustParse(created.ID), &workflow.RunWorkflowRequest{
			Input: map[string]interface{}{"topic": "runtime pull failure"},
		})
		errC <- runErr
	}()

	claimed, err := claimWorkflowRuntimeRun(t, runtimeSvc, token)
	require.NoError(t, err)
	require.NotNil(t, claimed)

	childRunID := uuid.MustParse(claimed.RunID)
	failed, err := runtimeSvc.CompleteRuntimePullRun(ctx, token, childRunID, &runtimemod.RuntimePullResultRequest{
		Status: "failed",
		Error:  &runtimemod.AgentError{Code: "WORKER_FAILED", Message: "runtime worker failed"},
	})
	require.NoError(t, err)
	require.Equal(t, "failed", failed.Status)

	select {
	case err = <-errC:
	case <-time.After(3 * time.Second):
		t.Fatal("workflow run did not fail after runtime pull failure")
	}
	require.Error(t, err)
	require.Contains(t, err.Error(), "runtime worker failed")

	history, err := svc.ListWorkflowRuns(ctx, userID, uuid.MustParse(created.ID), 10)
	require.NoError(t, err)
	require.Equal(t, int32(1), history.Total)
	require.Len(t, history.Items, 1)
	require.Equal(t, "failed", history.Items[0].Status)
	require.Contains(t, history.Items[0].Error, "runtime worker failed")
	require.Len(t, history.Items[0].Steps, 1)
	require.Equal(t, "failed", history.Items[0].Steps[0].Status)
	require.Equal(t, claimed.RunID, history.Items[0].Steps[0].RunID)
	require.Contains(t, history.Items[0].Steps[0].Error, "runtime worker failed")
}

func TestPauseResumeWorkflowRunControlsWorkerClaim(t *testing.T) {
	pool := setupWorkflowTestDB(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req runtimemod.AgentRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(map[string]interface{}{
			"output": map[string]interface{}{
				"step":    req.Input["node_key"],
				"summary": "resumed workflow done",
			},
		}))
	}))
	t.Cleanup(server.Close)

	userID := insertWorkflowUser(t, pool, "wf-pause-user")
	creatorID := insertWorkflowUser(t, pool, "wf-pause-creator")
	agentID := insertWorkflowAgent(t, pool, creatorID, server.URL)
	runtimeSvc := runtimemod.NewService(pool, &config.Config{
		PlatformFeeRate:         0,
		RunTimeoutSeconds:       5,
		AllowLocalHTTPEndpoints: true,
	})
	svc := workflow.NewService(pool, runtimeSvc)

	created, err := svc.CreateWorkflow(context.Background(), userID, &workflow.CreateWorkflowRequest{
		Name: "Pausable workflow",
		Nodes: []workflow.WorkflowNodeRequest{
			{Key: "worker", Title: "Worker", AgentID: agentID},
		},
	})
	require.NoError(t, err)

	queued, err := svc.StartWorkflowRun(context.Background(), userID, uuid.MustParse(created.ID), &workflow.RunWorkflowRequest{
		Input: map[string]interface{}{"topic": "pause"},
	})
	require.NoError(t, err)
	paused, err := svc.PauseWorkflowRun(context.Background(), userID, uuid.MustParse(queued.ID))
	require.NoError(t, err)
	require.Equal(t, "paused", paused.Status)

	claimed, err := svc.ClaimAndRunPendingWorkflow(context.Background())
	require.NoError(t, err)
	require.False(t, claimed)

	resumed, err := svc.ResumeWorkflowRun(context.Background(), userID, uuid.MustParse(queued.ID))
	require.NoError(t, err)
	require.Equal(t, "pending", resumed.Status)
	require.NotEmpty(t, resumed.NextRetryAt)

	claimed, err = svc.ClaimAndRunPendingWorkflow(context.Background())
	require.NoError(t, err)
	require.True(t, claimed)

	done, err := svc.GetWorkflowRun(context.Background(), userID, uuid.MustParse(queued.ID))
	require.NoError(t, err)
	require.Equal(t, "success", done.Status)
	require.Equal(t, int32(1), done.AttemptCount)
	require.Len(t, done.Steps, 1)
}

func TestCancelRunningWorkflowRunPreventsSuccessOverwrite(t *testing.T) {
	pool := setupWorkflowTestDB(t)

	agentCalled := make(chan struct{}, 1)
	releaseAgent := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseAgent) }) }
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req runtimemod.AgentRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		select {
		case agentCalled <- struct{}{}:
		default:
		}
		<-releaseAgent
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(map[string]interface{}{
			"output": map[string]interface{}{
				"step":    req.Input["node_key"],
				"summary": "late success should not overwrite cancel",
			},
		}))
	}))
	t.Cleanup(release)
	t.Cleanup(server.Close)

	userID := insertWorkflowUser(t, pool, "wf-cancel-user")
	creatorID := insertWorkflowUser(t, pool, "wf-cancel-creator")
	agentID := insertWorkflowAgent(t, pool, creatorID, server.URL)
	runtimeSvc := runtimemod.NewService(pool, &config.Config{
		PlatformFeeRate:         0,
		RunTimeoutSeconds:       5,
		AllowLocalHTTPEndpoints: true,
	})
	svc := workflow.NewService(pool, runtimeSvc)

	created, err := svc.CreateWorkflow(context.Background(), userID, &workflow.CreateWorkflowRequest{
		Name: "Cancelable workflow",
		Nodes: []workflow.WorkflowNodeRequest{
			{Key: "worker", Title: "Worker", AgentID: agentID},
		},
	})
	require.NoError(t, err)
	queued, err := svc.StartWorkflowRun(context.Background(), userID, uuid.MustParse(created.ID), &workflow.RunWorkflowRequest{
		Input: map[string]interface{}{"topic": "cancel"},
	})
	require.NoError(t, err)

	workerDone := make(chan error, 1)
	go func() {
		_, err := svc.ClaimAndRunPendingWorkflow(context.Background())
		workerDone <- err
	}()

	select {
	case <-agentCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("workflow worker did not call agent")
	}

	canceled, err := svc.CancelWorkflowRun(context.Background(), userID, uuid.MustParse(queued.ID))
	require.NoError(t, err)
	require.Equal(t, "canceled", canceled.Status)
	release()

	select {
	case err := <-workerDone:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("workflow worker did not finish after cancel")
	}

	final, err := svc.GetWorkflowRun(context.Background(), userID, uuid.MustParse(queued.ID))
	require.NoError(t, err)
	require.Equal(t, "canceled", final.Status)
	require.Equal(t, "workflow run canceled by user", final.Error)
	require.Len(t, final.Steps, 1)
	require.Equal(t, "success", final.Steps[0].Status)

	claimed, err := svc.ClaimAndRunPendingWorkflow(context.Background())
	require.NoError(t, err)
	require.False(t, claimed)
}

func TestWorkflowWorkerRequeuesStaleRunAndCleansRetrySteps(t *testing.T) {
	pool := setupWorkflowTestDB(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req runtimemod.AgentRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(map[string]interface{}{
			"output": map[string]interface{}{
				"step":    req.Input["node_key"],
				"summary": "retry workflow done",
			},
		}))
	}))
	t.Cleanup(server.Close)

	userID := insertWorkflowUser(t, pool, "wf-retry-user")
	creatorID := insertWorkflowUser(t, pool, "wf-retry-creator")
	agentID := insertWorkflowAgent(t, pool, creatorID, server.URL)

	runtimeSvc := runtimemod.NewService(pool, &config.Config{
		PlatformFeeRate:         0,
		RunTimeoutSeconds:       5,
		AllowLocalHTTPEndpoints: true,
	})
	svc := workflow.NewService(pool, runtimeSvc)

	created, err := svc.CreateWorkflow(context.Background(), userID, &workflow.CreateWorkflowRequest{
		Name: "Recover stale workflow",
		Nodes: []workflow.WorkflowNodeRequest{
			{Key: "worker", Title: "Worker", AgentID: agentID},
		},
	})
	require.NoError(t, err)

	queued, err := svc.StartWorkflowRun(context.Background(), userID, uuid.MustParse(created.ID), &workflow.RunWorkflowRequest{
		Input: map[string]interface{}{"topic": "retry"},
	})
	require.NoError(t, err)

	nodeID := uuid.MustParse(created.Nodes[0].ID)
	runID := uuid.MustParse(queued.ID)
	_, err = pool.Exec(context.Background(), `
		UPDATE workflow_runs
		SET status='running', attempt_count=1, claimed_at=NOW() - INTERVAL '1 hour'
		WHERE id=$1`, runID)
	require.NoError(t, err)
	_, err = pool.Exec(context.Background(), `
		INSERT INTO workflow_run_steps (workflow_run_id, workflow_node_id, node_key, agent_id, status, input, sequence)
		VALUES ($1, $2, 'worker', $3, 'running', '{}'::jsonb, 0)`, runID, nodeID, agentID)
	require.NoError(t, err)

	requeued, err := svc.RequeueStaleWorkflowRuns(context.Background(), 0)
	require.NoError(t, err)
	require.Equal(t, int64(1), requeued)

	recovered, err := svc.GetWorkflowRun(context.Background(), userID, runID)
	require.NoError(t, err)
	require.Equal(t, "pending", recovered.Status)
	require.Equal(t, int32(1), recovered.AttemptCount)
	require.Equal(t, "workflow worker stale claim timed out", recovered.LastWorkerError)
	require.Len(t, recovered.Steps, 1)

	claimed, err := svc.ClaimAndRunPendingWorkflow(context.Background())
	require.NoError(t, err)
	require.True(t, claimed)

	done, err := svc.GetWorkflowRun(context.Background(), userID, runID)
	require.NoError(t, err)
	require.Equal(t, "success", done.Status)
	require.Equal(t, int32(2), done.AttemptCount)
	require.Len(t, done.Steps, 1)
	require.Equal(t, "success", done.Steps[0].Status)
}

func TestRetryWorkflowRunCreatesNewPendingRun(t *testing.T) {
	pool := setupWorkflowTestDB(t)

	userID := insertWorkflowUser(t, pool, "wf-manual-retry-user")
	creatorID := insertWorkflowUser(t, pool, "wf-manual-retry-creator")
	agentID := insertWorkflowAgent(t, pool, creatorID, "http://127.0.0.1:18080")
	runtimeSvc := runtimemod.NewService(pool, &config.Config{
		PlatformFeeRate:         0,
		RunTimeoutSeconds:       5,
		AllowLocalHTTPEndpoints: true,
	})
	svc := workflow.NewService(pool, runtimeSvc)

	created, err := svc.CreateWorkflow(context.Background(), userID, &workflow.CreateWorkflowRequest{
		Name: "Manual retry workflow",
		Nodes: []workflow.WorkflowNodeRequest{
			{Key: "worker", Title: "Worker", AgentID: agentID},
		},
	})
	require.NoError(t, err)

	queued, err := svc.StartWorkflowRun(context.Background(), userID, uuid.MustParse(created.ID), &workflow.RunWorkflowRequest{
		Input:       map[string]interface{}{"topic": "manual retry"},
		MaxAttempts: 4,
	})
	require.NoError(t, err)
	msg := "original failure"
	_, err = pool.Exec(context.Background(),
		`UPDATE workflow_runs SET status='failed', error_message=$2, finished_at=NOW() WHERE id=$1`,
		uuid.MustParse(queued.ID), msg)
	require.NoError(t, err)

	retry, err := svc.RetryWorkflowRun(context.Background(), userID, uuid.MustParse(queued.ID))
	require.NoError(t, err)
	require.Equal(t, "pending", retry.Status)
	require.NotEqual(t, queued.ID, retry.ID)
	require.Equal(t, int32(4), retry.MaxAttempts)
	require.Equal(t, "manual retry", retry.Input["topic"])
}

func TestWorkflowControlPlaneReadsListsAndRunStateTransitions(t *testing.T) {
	pool := setupWorkflowTestDB(t)

	ctx := context.Background()
	userID := insertWorkflowUser(t, pool, "wf-control-user")
	creatorID := insertWorkflowUser(t, pool, "wf-control-creator")
	agentID := insertWorkflowAgent(t, pool, creatorID, "http://127.0.0.1:18080")
	svc := workflow.NewService(pool, nil)

	created, err := svc.CreateWorkflow(ctx, userID, &workflow.CreateWorkflowRequest{
		Name:        "Control plane workflow",
		Description: "covers workflow read and run state controls",
		Nodes: []workflow.WorkflowNodeRequest{
			{Key: "review", Title: "Review", AgentID: agentID},
		},
	})
	require.NoError(t, err)

	workflowID := uuid.MustParse(created.ID)
	fetched, err := svc.GetWorkflow(ctx, userID, workflowID)
	require.NoError(t, err)
	require.Equal(t, created.ID, fetched.ID)
	require.Equal(t, "Control plane workflow", fetched.Name)
	require.Len(t, fetched.Nodes, 1)

	listed, err := svc.ListWorkflows(ctx, userID, 0)
	require.NoError(t, err)
	require.Equal(t, int32(1), listed.Total)
	require.Len(t, listed.Items, 1)
	require.Equal(t, fetched.ID, listed.Items[0].ID)

	runID := uuid.New()
	_, err = pool.Exec(ctx, `
		INSERT INTO workflow_runs (id, workflow_id, user_id, status, input, max_attempts)
		VALUES ($1, $2, $3, 'pending', $4, 5)`,
		runID, workflowID, userID, []byte(`{"topic":"control plane"}`))
	require.NoError(t, err)

	runs, err := svc.ListWorkflowRuns(ctx, userID, workflowID, 0)
	require.NoError(t, err)
	require.Equal(t, int32(1), runs.Total)
	require.Len(t, runs.Items, 1)
	require.Equal(t, runID.String(), runs.Items[0].ID)
	require.Equal(t, "pending", runs.Items[0].Status)
	require.Equal(t, "control plane", runs.Items[0].Input["topic"])

	gotRun, err := svc.GetWorkflowRun(ctx, userID, runID)
	require.NoError(t, err)
	require.Equal(t, "pending", gotRun.Status)
	require.Equal(t, int32(5), gotRun.MaxAttempts)

	paused, err := svc.PauseWorkflowRun(ctx, userID, runID)
	require.NoError(t, err)
	require.Equal(t, "paused", paused.Status)

	pausedAgain, err := svc.PauseWorkflowRun(ctx, userID, runID)
	require.NoError(t, err)
	require.Equal(t, "paused", pausedAgain.Status)

	resumed, err := svc.ResumeWorkflowRun(ctx, userID, runID)
	require.NoError(t, err)
	require.Equal(t, "pending", resumed.Status)

	canceled, err := svc.CancelWorkflowRun(ctx, userID, runID)
	require.NoError(t, err)
	require.Equal(t, "canceled", canceled.Status)

	canceledAgain, err := svc.CancelWorkflowRun(ctx, userID, runID)
	require.NoError(t, err)
	require.Equal(t, "canceled", canceledAgain.Status)

	_, err = svc.GetWorkflow(ctx, uuid.New(), workflowID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "workflow 不存在")
}

func TestCreateWorkflowRejectsCyclicEdges(t *testing.T) {
	svc := workflow.NewService(nil, nil)
	agentID := uuid.New()
	_, err := svc.CreateWorkflow(context.Background(), uuid.New(), &workflow.CreateWorkflowRequest{
		Name: "Cyclic workflow",
		Nodes: []workflow.WorkflowNodeRequest{
			{Key: "a", AgentID: agentID},
			{Key: "b", AgentID: agentID},
		},
		Edges: []map[string]interface{}{
			{"from": "a", "to": "b"},
			{"from": "b", "to": "a"},
		},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "存在环")
}

func setupWorkflowTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL 未设置，跳过 workflow 集成测试")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	require.NoError(t, pool.Ping(ctx))
	truncateWorkflowTables(t, pool)
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, _ = pool.Exec(c, `TRUNCATE workflows, runs, agents, users RESTART IDENTITY CASCADE`)
		pool.Close()
	})
	return pool
}

func truncateWorkflowTables(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`TRUNCATE workflows, runs, agents, users RESTART IDENTITY CASCADE`)
	require.NoError(t, err)
}

func insertWorkflowUser(t *testing.T, pool *pgxpool.Pool, prefix string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO users (id, email, password_hash, display_name, is_creator, creator_verified)
		 VALUES ($1, $2, $3, $4, TRUE, TRUE)`,
		id, prefix+"-"+id.String()[:8]+"@example.com", "x", prefix)
	require.NoError(t, err)
	return id
}

func insertWorkflowAgent(t *testing.T, pool *pgxpool.Pool, creatorID uuid.UUID, endpoint string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO agents (
			id, creator_id, slug, name, description, endpoint_url, endpoint_auth_header,
			price_per_call_cents, tags, lifecycle_status, visibility, certification_status
		) VALUES ($1, $2, $3, $4, $5, $6, NULL, 0, '{}', 'active', 'public', 'unreviewed')`,
		id, creatorID, "wf-agent-"+id.String()[:8], "Workflow Agent", "test workflow agent", endpoint)
	require.NoError(t, err)
	return id
}

func setWorkflowAgentRuntimePullMode(t *testing.T, pool *pgxpool.Pool, agentID uuid.UUID) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`UPDATE agents
		 SET connection_mode='runtime_pull',
		     endpoint_url=$2
		 WHERE id=$1`,
		agentID,
		"openlinker-runtime-pull://"+agentID.String(),
	)
	require.NoError(t, err)
}

func insertWorkflowRuntimeToken(t *testing.T, pool *pgxpool.Pool, agentID, creatorID uuid.UUID) string {
	t.Helper()
	raw := make([]byte, 32)
	_, err := rand.Read(raw)
	require.NoError(t, err)
	plaintext := "rt_live_" + hex.EncodeToString(raw)
	hash, err := bcrypt.GenerateFromPassword([]byte(plaintext), bcrypt.DefaultCost)
	require.NoError(t, err)
	_, err = pool.Exec(context.Background(),
		`INSERT INTO agent_runtime_tokens (
			agent_id, created_by_user_id, name, prefix, token_hash, scopes
		) VALUES ($1, $2, 'workflow-runtime', $3, $4, $5)`,
		agentID,
		creatorID,
		plaintext[:12],
		string(hash),
		[]string{"agent:call", "agent:pull"},
	)
	require.NoError(t, err)
	return plaintext
}

func claimWorkflowRuntimeRun(t *testing.T, svc *runtimemod.Service, token string) (*runtimemod.RuntimePullRunResponse, error) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		claimed, err := svc.ClaimRuntimePullRun(context.Background(), token)
		if err != nil || claimed != nil {
			return claimed, err
		}
		select {
		case <-deadline:
			return nil, nil
		case <-tick.C:
		}
	}
}

func waitForWorkflowRunStatus(t *testing.T, svc *workflow.Service, userID, runID uuid.UUID, status string) *workflow.WorkflowRunResponse {
	t.Helper()
	deadline := time.After(3 * time.Second)
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		got, err := svc.GetWorkflowRun(context.Background(), userID, runID)
		require.NoError(t, err)
		if got.Status == status {
			return got
		}
		select {
		case <-deadline:
			t.Fatalf("workflow run %s did not reach %s; last status=%s", runID, status, got.Status)
		case <-tick.C:
		}
	}
}
