package workflow_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	runtimemod "github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/OpenLinker-ai/openlinker-core/pkg/workflow"
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
		RunTimeoutSeconds:       5,
		AllowLocalHTTPEndpoints: true,
	})
	runtimeSvc.ConfigureCoreRuntime(uuid.New())
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

func TestWorkflowRunMapsExplicitInputBeforeCreatingChildRun(t *testing.T) {
	pool := setupWorkflowTestDB(t)

	var received map[string]interface{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req runtimemod.AgentRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		received = req.Input
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(map[string]interface{}{
			"output": map[string]interface{}{"answer": "mapped"},
		}))
	}))
	t.Cleanup(server.Close)

	userID := insertWorkflowUser(t, pool, "wf-mapping-user")
	creatorID := insertWorkflowUser(t, pool, "wf-mapping-creator")
	agentID := insertWorkflowAgent(t, pool, creatorID, server.URL)
	insertWorkflowAgentCapability(t, pool, agentID, `{
		"type":"object",
		"properties":{"task":{"type":"string"},"format":{"type":"string"}},
		"required":["task"],
		"additionalProperties":false
	}`)

	runtimeSvc := runtimemod.NewService(pool, &config.Config{
		RunTimeoutSeconds:       5,
		AllowLocalHTTPEndpoints: true,
	})
	runtimeSvc.ConfigureCoreRuntime(uuid.New())
	svc := workflow.NewService(pool, runtimeSvc)
	created, err := svc.CreateWorkflow(context.Background(), userID, &workflow.CreateWorkflowRequest{
		Name: "Mapped workflow",
		Nodes: []workflow.WorkflowNodeRequest{{
			Key: "analyze", AgentID: agentID,
			Config: map[string]interface{}{"input_mapping": map[string]interface{}{
				"version": "v1",
				"fields": map[string]interface{}{
					"task":   map[string]interface{}{"source": "workflow_input", "path": []interface{}{"text"}},
					"format": map[string]interface{}{"source": "constant", "value": "markdown"},
				},
			}},
		}},
	})
	require.NoError(t, err)

	run, err := svc.RunWorkflow(context.Background(), userID, uuid.MustParse(created.ID), &workflow.RunWorkflowRequest{
		Input: map[string]interface{}{"text": "audit the boundary", "ignored": true},
	})
	require.NoError(t, err)
	require.Equal(t, "success", run.Status)
	require.Equal(t, map[string]interface{}{"task": "audit the boundary", "format": "markdown"}, received)
	require.NotContains(t, received, "workflow_input")
	require.NotContains(t, received, "node_key")
}

func TestWorkflowMappedInputSchemaMismatchFailsBeforeChildRun(t *testing.T) {
	pool := setupWorkflowTestDB(t)
	userID := insertWorkflowUser(t, pool, "wf-mismatch-user")
	creatorID := insertWorkflowUser(t, pool, "wf-mismatch-creator")
	agentID := insertWorkflowAgent(t, pool, creatorID, "http://127.0.0.1:18080")
	insertWorkflowAgentCapability(t, pool, agentID, `{
		"type":"object",
		"properties":{"task":{"type":"string"}},
		"required":["task"],
		"additionalProperties":false
	}`)
	runtimeSvc := runtimemod.NewService(pool, &config.Config{
		RunTimeoutSeconds:       5,
		AllowLocalHTTPEndpoints: true,
	})
	runtimeSvc.ConfigureCoreRuntime(uuid.New())
	svc := workflow.NewService(pool, runtimeSvc)
	created, err := svc.CreateWorkflow(context.Background(), userID, &workflow.CreateWorkflowRequest{
		Name: "Schema mismatch workflow",
		Nodes: []workflow.WorkflowNodeRequest{{
			Key: "analyze", AgentID: agentID,
			Config: map[string]interface{}{"input_mapping": map[string]interface{}{
				"version": "v1",
				"fields": map[string]interface{}{
					"task": map[string]interface{}{"source": "workflow_input", "path": []interface{}{"task"}},
				},
			}},
		}},
	})
	require.NoError(t, err)

	run, err := svc.RunWorkflow(context.Background(), userID, uuid.MustParse(created.ID), &workflow.RunWorkflowRequest{
		Input: map[string]interface{}{"task": 42},
	})
	require.Nil(t, run)
	require.Error(t, err)
	require.Contains(t, err.Error(), "WORKFLOW_NODE_INPUT_SCHEMA_MISMATCH")

	var childRuns int
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM runs WHERE user_id=$1 AND agent_id=$2`, userID, agentID).Scan(&childRuns))
	require.Zero(t, childRuns)

	runs, err := svc.ListWorkflowRuns(context.Background(), userID, uuid.MustParse(created.ID), 10)
	require.NoError(t, err)
	require.Len(t, runs.Items, 1)
	require.Equal(t, "failed", runs.Items[0].Status)
	require.Len(t, runs.Items[0].Steps, 1)
	require.Equal(t, "failed", runs.Items[0].Steps[0].Status)
}

func TestWorkflowCreateRejectsUncallableAgent(t *testing.T) {
	pool := setupWorkflowTestDB(t)
	userID := insertWorkflowUser(t, pool, "wf-uncallable-user")
	creatorID := insertWorkflowUser(t, pool, "wf-uncallable-creator")
	agentID := insertWorkflowAgent(t, pool, creatorID, "https://example.com/offline")
	setWorkflowAgentUnreachable(t, pool, agentID)

	svc := workflow.NewService(pool, nil)
	created, err := svc.CreateWorkflow(context.Background(), userID, &workflow.CreateWorkflowRequest{
		Name: "Uncallable workflow",
		Nodes: []workflow.WorkflowNodeRequest{
			{Key: "offline", Title: "Offline Agent", AgentID: agentID},
		},
	})
	require.Nil(t, created)
	requireWorkflowHTTPStatus(t, err, http.StatusConflict)
}

func TestWorkflowCreateRejectsReservedPublicAgentTags(t *testing.T) {
	pool := setupWorkflowTestDB(t)
	userID := insertWorkflowUser(t, pool, "wf-validation-user")
	creatorID := insertWorkflowUser(t, pool, "wf-validation-creator")
	agentID := insertWorkflowAgent(t, pool, creatorID, "https://example.com/validation")
	setWorkflowAgentTags(t, pool, agentID, []string{"workflow", "testing"})

	svc := workflow.NewService(pool, nil)
	created, err := svc.CreateWorkflow(context.Background(), userID, &workflow.CreateWorkflowRequest{
		Name: "Validation tagged workflow",
		Nodes: []workflow.WorkflowNodeRequest{
			{Key: "validate", Title: "Validate", AgentID: agentID},
		},
	})
	require.Nil(t, created)
	requireWorkflowHTTPStatus(t, err, http.StatusConflict)
}

func TestWorkflowCreateAllowsOwnReservedTaggedAgent(t *testing.T) {
	pool := setupWorkflowTestDB(t)
	userID := insertWorkflowUser(t, pool, "wf-validation-owner")
	agentID := insertWorkflowAgent(t, pool, userID, "https://example.com/validation")
	setWorkflowAgentTags(t, pool, agentID, []string{"workflow", "testing"})

	svc := workflow.NewService(pool, nil)
	created, err := svc.CreateWorkflow(context.Background(), userID, &workflow.CreateWorkflowRequest{
		Name: "Own reserved tagged workflow",
		Nodes: []workflow.WorkflowNodeRequest{
			{Key: "validate", Title: "Validate", AgentID: agentID},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created)
}

func TestWorkflowCreateAllowsOwnPrivateAgent(t *testing.T) {
	pool := setupWorkflowTestDB(t)
	userID := insertWorkflowUser(t, pool, "wf-private-owner")
	agentID := insertWorkflowAgent(t, pool, userID, "https://example.com/private")
	setWorkflowAgentVisibility(t, pool, agentID, "private")

	svc := workflow.NewService(pool, nil)
	created, err := svc.CreateWorkflow(context.Background(), userID, &workflow.CreateWorkflowRequest{
		Name: "Private own workflow",
		Nodes: []workflow.WorkflowNodeRequest{
			{Key: "own_private", Title: "Own Private", AgentID: agentID},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created)
}

func TestWorkflowStartRejectsAgentThatBecameUncallable(t *testing.T) {
	pool := setupWorkflowTestDB(t)
	userID := insertWorkflowUser(t, pool, "wf-stale-user")
	creatorID := insertWorkflowUser(t, pool, "wf-stale-creator")
	agentID := insertWorkflowAgent(t, pool, creatorID, "https://example.com/offline")

	runtimeSvc := runtimemod.NewService(pool, &config.Config{
		RunTimeoutSeconds:       5,
		AllowLocalHTTPEndpoints: true,
	})
	runtimeSvc.ConfigureCoreRuntime(uuid.New())
	svc := workflow.NewService(pool, runtimeSvc)
	created, err := svc.CreateWorkflow(context.Background(), userID, &workflow.CreateWorkflowRequest{
		Name: "Stale workflow",
		Nodes: []workflow.WorkflowNodeRequest{
			{Key: "offline", Title: "Offline Agent", AgentID: agentID},
		},
	})
	require.NoError(t, err)
	setWorkflowAgentUnreachable(t, pool, agentID)

	run, err := svc.StartWorkflowRun(context.Background(), userID, uuid.MustParse(created.ID), &workflow.RunWorkflowRequest{
		Input: map[string]interface{}{"topic": "should not queue"},
	})
	require.Nil(t, run)
	requireWorkflowHTTPStatus(t, err, http.StatusConflict)
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
		RunTimeoutSeconds:       5,
		AllowLocalHTTPEndpoints: true,
	})
	runtimeSvc.ConfigureCoreRuntime(uuid.New())
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

func TestWorkflowParallelBranchFailureCancelsSiblingHTTPRun(t *testing.T) {
	pool := setupWorkflowTestDB(t)

	var mu sync.Mutex
	calls := []string{}
	failStarted := make(chan struct{})
	slowStarted := make(chan struct{})
	slowCanceled := make(chan struct{})
	var closeFailStarted sync.Once
	var closeSlowStarted sync.Once
	var closeSlowCanceled sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req runtimemod.AgentRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		nodeKey, _ := req.Input["node_key"].(string)

		mu.Lock()
		calls = append(calls, nodeKey)
		mu.Unlock()

		switch nodeKey {
		case "fail":
			closeFailStarted.Do(func() { close(failStarted) })
			select {
			case <-slowStarted:
			case <-time.After(2 * time.Second):
			}
			http.Error(w, "branch failed", http.StatusInternalServerError)
		case "slow":
			closeSlowStarted.Do(func() { close(slowStarted) })
			select {
			case <-r.Context().Done():
				closeSlowCanceled.Do(func() { close(slowCanceled) })
				return
			case <-time.After(3 * time.Second):
				w.Header().Set("Content-Type", "application/json")
				require.NoError(t, json.NewEncoder(w).Encode(map[string]interface{}{
					"output": map[string]interface{}{"step": nodeKey},
				}))
			}
		default:
			w.Header().Set("Content-Type", "application/json")
			require.NoError(t, json.NewEncoder(w).Encode(map[string]interface{}{
				"output": map[string]interface{}{"step": nodeKey},
			}))
		}
	}))
	t.Cleanup(server.Close)

	userID := insertWorkflowUser(t, pool, "wf-cancel-sibling-user")
	creatorID := insertWorkflowUser(t, pool, "wf-cancel-sibling-creator")
	agentID := insertWorkflowAgent(t, pool, creatorID, server.URL)

	runtimeSvc := runtimemod.NewService(pool, &config.Config{
		RunTimeoutSeconds:       10,
		AllowLocalHTTPEndpoints: true,
	})
	runtimeSvc.ConfigureCoreRuntime(uuid.New())
	svc := workflow.NewService(pool, runtimeSvc)

	created, err := svc.CreateWorkflow(context.Background(), userID, &workflow.CreateWorkflowRequest{
		Name: "Fail-fast DAG workflow",
		Nodes: []workflow.WorkflowNodeRequest{
			{Key: "fail", Title: "Fail", AgentID: agentID},
			{Key: "slow", Title: "Slow", AgentID: agentID},
			{Key: "join", Title: "Join", AgentID: agentID},
		},
		Edges: []map[string]interface{}{
			{"from": "fail", "to": "join"},
			{"from": "slow", "to": "join"},
		},
	})
	require.NoError(t, err)

	_, err = svc.RunWorkflow(context.Background(), userID, uuid.MustParse(created.ID), &workflow.RunWorkflowRequest{
		Input: map[string]interface{}{"topic": "cancel sibling"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "workflow 执行失败")
	select {
	case <-failStarted:
	default:
		t.Fatal("fail branch was not called")
	}
	select {
	case <-slowStarted:
	default:
		t.Fatal("slow branch was not called")
	}
	select {
	case <-slowCanceled:
	case <-time.After(2 * time.Second):
		t.Fatal("slow branch HTTP request was not canceled after sibling failure")
	}

	mu.Lock()
	require.ElementsMatch(t, []string{"fail", "slow"}, calls)
	require.NotContains(t, calls, "join")
	mu.Unlock()

	history, err := svc.ListWorkflowRuns(context.Background(), userID, uuid.MustParse(created.ID), 10)
	require.NoError(t, err)
	require.Len(t, history.Items, 1)
	require.Equal(t, "failed", history.Items[0].Status)
	require.Len(t, history.Items[0].Steps, 2)
	require.NotContains(t, []string{history.Items[0].Steps[0].NodeKey, history.Items[0].Steps[1].NodeKey}, "join")
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
		RunTimeoutSeconds:       5,
		AllowLocalHTTPEndpoints: true,
	})
	runtimeSvc.ConfigureCoreRuntime(uuid.New())
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
		RunTimeoutSeconds:       5,
		AllowLocalHTTPEndpoints: true,
	})
	runtimeSvc.ConfigureCoreRuntime(uuid.New())
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

func TestRuntimeWorkflowStepExposesChildRunIDWhileRunning(t *testing.T) {
	pool := setupWorkflowTestDB(t)
	ctx := context.Background()
	coreID := uuid.New()
	userID := insertWorkflowUser(t, pool, "wf-runtime-user")
	creatorID := insertWorkflowUser(t, pool, "wf-runtime-creator")
	agentID := insertWorkflowAgent(t, pool, creatorID, "openlinker-runtime://workflow-running-step")
	_, err := pool.Exec(ctx, `UPDATE agents SET connection_mode = 'runtime' WHERE id = $1`, agentID)
	require.NoError(t, err)
	insertWorkflowRuntimeSession(t, pool, coreID, creatorID, agentID)

	runtimeSvc := runtimemod.NewService(pool, &config.Config{RunTimeoutSeconds: 5})
	runtimeSvc.ConfigureCoreRuntime(coreID)
	svc := workflow.NewService(pool, runtimeSvc)
	created, err := svc.CreateWorkflow(ctx, userID, &workflow.CreateWorkflowRequest{
		Name:  "Runtime running step visibility",
		Nodes: []workflow.WorkflowNodeRequest{{Key: "worker", Title: "Worker", AgentID: agentID}},
	})
	require.NoError(t, err)
	queued, err := svc.StartWorkflowRun(ctx, userID, uuid.MustParse(created.ID), &workflow.RunWorkflowRequest{
		Input: map[string]interface{}{"topic": "observe child run"},
	})
	require.NoError(t, err)

	workerCtx, cancelWorker := context.WithCancel(ctx)
	type workerResult struct {
		claimed bool
		err     error
	}
	workerDone := make(chan workerResult, 1)
	go func() {
		claimed, claimErr := svc.ClaimAndRunPendingWorkflow(workerCtx)
		workerDone <- workerResult{claimed: claimed, err: claimErr}
	}()
	defer cancelWorker()

	workflowRunID := uuid.MustParse(queued.ID)
	var stepID uuid.UUID
	var childRunIDText string
	require.Eventually(t, func() bool {
		var status string
		err := pool.QueryRow(ctx, `
			SELECT id, status, COALESCE(run_id::text, '')
			FROM workflow_run_steps
			WHERE workflow_run_id = $1`, workflowRunID).Scan(&stepID, &status, &childRunIDText)
		return err == nil && status == "running" && childRunIDText != ""
	}, 5*time.Second, 25*time.Millisecond, "running workflow step never exposed its child run_id")
	childRunID, err := uuid.Parse(childRunIDText)
	require.NoError(t, err)
	require.NotEqual(t, uuid.Nil, childRunID)

	var childStatus, dispatchState string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT status, dispatch_state
		FROM runs
		WHERE id = $1`, childRunID).Scan(&childStatus, &dispatchState))
	require.Equal(t, "running", childStatus)
	require.Equal(t, "pending", dispatchState)

	queries := db.New(pool)
	attached, err := queries.AttachWorkflowRunStepRun(ctx, db.AttachWorkflowRunStepRunParams{ID: stepID, RunID: childRunID})
	require.NoError(t, err)
	require.Equal(t, int64(1), attached, "same child run attachment must be idempotent")
	attached, err = queries.AttachWorkflowRunStepRun(ctx, db.AttachWorkflowRunStepRunParams{ID: stepID, RunID: uuid.New()})
	require.NoError(t, err)
	require.Equal(t, int64(0), attached, "a different child run must not replace the attachment")

	conflictingRunID := uuid.New()
	msg := "must not replace child run evidence"
	_, err = queries.MarkWorkflowRunStepFailed(ctx, db.MarkWorkflowRunStepFailedParams{
		ID:           stepID,
		ErrorMessage: &msg,
	})
	require.ErrorIs(t, err, pgx.ErrNoRows, "nil child run must not clear attached evidence")
	_, err = queries.MarkWorkflowRunStepFailed(ctx, db.MarkWorkflowRunStepFailedParams{
		ID:           stepID,
		RunID:        &conflictingRunID,
		ErrorMessage: &msg,
	})
	require.ErrorIs(t, err, pgx.ErrNoRows, "different child run must not replace attached evidence")
	_, err = queries.MarkWorkflowRunStepSuccess(ctx, db.MarkWorkflowRunStepSuccessParams{
		ID:     stepID,
		RunID:  &conflictingRunID,
		Output: []byte(`{}`),
	})
	require.ErrorIs(t, err, pgx.ErrNoRows, "different child run must not claim terminal success")

	var storedRunID uuid.UUID
	var storedStatus string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT run_id, status
		FROM workflow_run_steps
		WHERE id = $1`, stepID).Scan(&storedRunID, &storedStatus))
	require.Equal(t, childRunID, storedRunID)
	require.Equal(t, "running", storedStatus)

	_, err = queries.MarkWorkflowRunStepSuccess(ctx, db.MarkWorkflowRunStepSuccessParams{
		ID:     stepID,
		RunID:  &childRunID,
		Output: []byte(`{"ok":true}`),
	})
	require.NoError(t, err)
	_, err = queries.MarkWorkflowRunStepFailed(ctx, db.MarkWorkflowRunStepFailedParams{
		ID:           stepID,
		RunID:        &childRunID,
		ErrorMessage: &msg,
	})
	require.ErrorIs(t, err, pgx.ErrNoRows, "terminal success must not be reversed")
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT run_id, status
		FROM workflow_run_steps
		WHERE id = $1`, stepID).Scan(&storedRunID, &storedStatus))
	require.Equal(t, childRunID, storedRunID)
	require.Equal(t, "success", storedStatus)

	cancelWorker()
	select {
	case result := <-workerDone:
		require.True(t, result.claimed)
		require.NoError(t, result.err)
	case <-time.After(5 * time.Second):
		t.Fatal("workflow worker did not stop after context cancellation")
	}
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
		RunTimeoutSeconds:       5,
		AllowLocalHTTPEndpoints: true,
	})
	runtimeSvc.ConfigureCoreRuntime(uuid.New())
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
		RunTimeoutSeconds:       5,
		AllowLocalHTTPEndpoints: true,
	})
	runtimeSvc.ConfigureCoreRuntime(uuid.New())
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

func TestPauseResumeRunningWorkflowWaitsForClaimReleaseAndReusesChildRun(t *testing.T) {
	pool := setupWorkflowTestDB(t)

	agentCalled := make(chan struct{}, 1)
	releaseAgent := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseAgent) }) }
	var callsMu sync.Mutex
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req runtimemod.AgentRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		callsMu.Lock()
		callCount++
		callsMu.Unlock()
		select {
		case agentCalled <- struct{}{}:
		default:
		}
		<-releaseAgent
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(map[string]interface{}{
			"output": map[string]interface{}{
				"step":    req.Input["node_key"],
				"summary": "paused child completed once",
			},
		}))
	}))
	t.Cleanup(release)
	t.Cleanup(server.Close)

	userID := insertWorkflowUser(t, pool, "wf-running-pause-user")
	creatorID := insertWorkflowUser(t, pool, "wf-running-pause-creator")
	agentID := insertWorkflowAgent(t, pool, creatorID, server.URL)
	runtimeSvc := runtimemod.NewService(pool, &config.Config{
		RunTimeoutSeconds:       5,
		AllowLocalHTTPEndpoints: true,
	})
	runtimeSvc.ConfigureCoreRuntime(uuid.New())
	svc := workflow.NewService(pool, runtimeSvc)

	created, err := svc.CreateWorkflow(context.Background(), userID, &workflow.CreateWorkflowRequest{
		Name: "Running pause handshake",
		Nodes: []workflow.WorkflowNodeRequest{
			{Key: "worker", Title: "Worker", AgentID: agentID},
		},
	})
	require.NoError(t, err)
	queued, err := svc.StartWorkflowRun(context.Background(), userID, uuid.MustParse(created.ID), &workflow.RunWorkflowRequest{
		Input: map[string]interface{}{"topic": "running pause"},
	})
	require.NoError(t, err)
	runID := uuid.MustParse(queued.ID)

	type claimResult struct {
		claimed bool
		err     error
	}
	workerDone := make(chan claimResult, 1)
	go func() {
		claimed, claimErr := svc.ClaimAndRunPendingWorkflow(context.Background())
		workerDone <- claimResult{claimed: claimed, err: claimErr}
	}()

	select {
	case <-agentCalled:
	case <-time.After(3 * time.Second):
		t.Fatal("workflow worker did not call Agent")
	}

	paused, err := svc.PauseWorkflowRun(context.Background(), userID, runID)
	require.NoError(t, err)
	require.Equal(t, "paused", paused.Status)
	require.NotEmpty(t, paused.ClaimedAt, "running pause must retain the active worker claim")

	_, err = svc.ResumeWorkflowRun(context.Background(), userID, runID)
	requireWorkflowHTTPStatus(t, err, http.StatusConflict)

	release()
	select {
	case result := <-workerDone:
		require.NoError(t, result.err)
		require.True(t, result.claimed)
	case <-time.After(5 * time.Second):
		t.Fatal("workflow worker did not converge after pause")
	}

	var settled *workflow.WorkflowRunResponse
	require.Eventually(t, func() bool {
		got, getErr := svc.GetWorkflowRun(context.Background(), userID, runID)
		if getErr != nil {
			return false
		}
		settled = got
		return got.Status == "paused" && got.ClaimedAt == "" && len(got.Steps) == 1 && got.Steps[0].Status == "success" && got.Steps[0].RunID != ""
	}, 3*time.Second, 20*time.Millisecond)
	pausedChildRunID := settled.Steps[0].RunID

	resumed, err := svc.ResumeWorkflowRun(context.Background(), userID, runID)
	require.NoError(t, err)
	require.Equal(t, "pending", resumed.Status)
	claimed, err := svc.ClaimAndRunPendingWorkflow(context.Background())
	require.NoError(t, err)
	require.True(t, claimed)

	done, err := svc.GetWorkflowRun(context.Background(), userID, runID)
	require.NoError(t, err)
	require.Equal(t, "success", done.Status)
	require.Equal(t, int32(2), done.AttemptCount)
	require.Len(t, done.Steps, 1)
	require.Equal(t, pausedChildRunID, done.Steps[0].RunID)
	callsMu.Lock()
	require.Equal(t, 1, callCount, "resume must replay the durable child Run instead of invoking the Agent twice")
	callsMu.Unlock()
}

func TestReleasePausedWorkflowRunClaimRejectsStaleFence(t *testing.T) {
	pool := setupWorkflowTestDB(t)

	userID := insertWorkflowUser(t, pool, "wf-claim-fence-user")
	creatorID := insertWorkflowUser(t, pool, "wf-claim-fence-creator")
	agentID := insertWorkflowAgent(t, pool, creatorID, "http://127.0.0.1:18080")
	svc := workflow.NewService(pool, nil)
	created, err := svc.CreateWorkflow(context.Background(), userID, &workflow.CreateWorkflowRequest{
		Name: "Claim fence workflow",
		Nodes: []workflow.WorkflowNodeRequest{
			{Key: "worker", Title: "Worker", AgentID: agentID},
		},
	})
	require.NoError(t, err)

	runID := uuid.New()
	claimA := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	claimB := claimA.Add(30 * time.Second)
	_, err = pool.Exec(context.Background(), `
		INSERT INTO workflow_runs (
			id, workflow_id, user_id, status, input, attempt_count, max_attempts, claimed_at
		) VALUES ($1, $2, $3, 'paused', '{}'::jsonb, 2, 3, $4)`,
		runID, uuid.MustParse(created.ID), userID, claimB)
	require.NoError(t, err)

	queries := db.New(pool)
	requeued, err := svc.RequeueStaleWorkflowRuns(context.Background(), 0)
	require.NoError(t, err)
	require.Zero(t, requeued, "stale recovery must leave paused claims fail-closed")

	released, err := queries.ReleasePausedWorkflowRunClaim(context.Background(), db.ReleasePausedWorkflowRunClaimParams{
		ID: runID, ExpectedClaimedAt: claimA, ExpectedAttemptCount: 1,
	})
	require.NoError(t, err)
	require.Zero(t, released, "stale worker A must not release worker B's claim")

	released, err = queries.ReleasePausedWorkflowRunClaim(context.Background(), db.ReleasePausedWorkflowRunClaimParams{
		ID: runID, ExpectedClaimedAt: claimB, ExpectedAttemptCount: 1,
	})
	require.NoError(t, err)
	require.Zero(t, released, "claim timestamp alone must not bypass the attempt fence")

	var storedClaim time.Time
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT claimed_at FROM workflow_runs WHERE id=$1`, runID).Scan(&storedClaim))
	require.True(t, storedClaim.Equal(claimB))

	released, err = queries.ReleasePausedWorkflowRunClaim(context.Background(), db.ReleasePausedWorkflowRunClaimParams{
		ID: runID, ExpectedClaimedAt: claimB, ExpectedAttemptCount: 2,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), released)
	var claimReleased bool
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT claimed_at IS NULL FROM workflow_runs WHERE id=$1`, runID).Scan(&claimReleased))
	require.True(t, claimReleased)
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
		RunTimeoutSeconds:       5,
		AllowLocalHTTPEndpoints: true,
	})
	runtimeSvc.ConfigureCoreRuntime(uuid.New())
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
		RunTimeoutSeconds:       5,
		AllowLocalHTTPEndpoints: true,
	})
	runtimeSvc.ConfigureCoreRuntime(uuid.New())
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
		RunTimeoutSeconds:       5,
		AllowLocalHTTPEndpoints: true,
	})
	runtimeSvc.ConfigureCoreRuntime(uuid.New())
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

func TestExternalExecutionWorkflowContractTracksNodeAgentCapability(t *testing.T) {
	pool := setupWorkflowTestDB(t)
	ownerID := insertWorkflowUser(t, pool, "external-execution-owner")
	agentID := insertWorkflowAgent(t, pool, ownerID, "https://agent.example/run")
	_, err := pool.Exec(context.Background(), `INSERT INTO agent_capabilities
(agent_id,input_schema,output_schema,summary,version,published_at,updated_at)
VALUES ($1,'{"type":"object"}'::jsonb,'{"type":"object"}'::jsonb,'external execution',1,NOW(),NOW())`, agentID)
	require.NoError(t, err)
	svc := workflow.NewService(pool, nil)
	created, err := svc.CreateWorkflow(context.Background(), ownerID, &workflow.CreateWorkflowRequest{
		Name: "Execution contract", Nodes: []workflow.WorkflowNodeRequest{{Key: "run", AgentID: agentID, Config: map[string]interface{}{"mode": "strict"}}},
	})
	require.NoError(t, err)
	workflowID := uuid.MustParse(created.ID)
	first, err := svc.ValidateExternalExecutionTarget(context.Background(), ownerID, workflowID)
	require.NoError(t, err)
	require.True(t, first.Executable)
	require.Regexp(t, `^hct:v1:[a-f0-9]{64}$`, first.ContractHash)

	_, err = pool.Exec(context.Background(), `UPDATE workflow_nodes SET config=$2::jsonb WHERE workflow_id=$1`, workflowID,
		`{"mode":"strict","input_mapping":{"version":"v1","fields":{"task":{"source":"workflow_input","path":["text"]}}}}`)
	require.NoError(t, err)
	mapped, err := svc.ValidateExternalExecutionTarget(context.Background(), ownerID, workflowID)
	require.NoError(t, err)
	require.True(t, mapped.Executable)
	require.NotEqual(t, first.ContractHash, mapped.ContractHash)

	_, err = pool.Exec(context.Background(), `UPDATE agent_capabilities SET version=version+1, updated_at=NOW() WHERE agent_id=$1`, agentID)
	require.NoError(t, err)
	second, err := svc.ValidateExternalExecutionTarget(context.Background(), ownerID, workflowID)
	require.NoError(t, err)
	require.True(t, second.Executable)
	require.NotEqual(t, mapped.ContractHash, second.ContractHash)
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
	_, err = pool.Exec(ctx, `
UPDATE runtime_cluster_control
SET mode = 'normal', expected_replicas = 1, reopened_at = clock_timestamp(),
    version = version + 1, updated_at = clock_timestamp()
WHERE singleton_id = 1`)
	require.NoError(t, err)
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, _ = pool.Exec(c, `TRUNCATE workflows, runs, agents, users RESTART IDENTITY CASCADE`)
		_, _ = pool.Exec(c, `
UPDATE runtime_cluster_control
SET mode = 'hard_maintenance', expected_replicas = 1,
    hard_maintenance_at = clock_timestamp(), reopened_at = NULL,
    version = version + 1, updated_at = clock_timestamp()
WHERE singleton_id = 1`)
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
	_, err = pool.Exec(context.Background(),
		`INSERT INTO agent_availability_snapshots (
			agent_id, availability_status, last_successful_run_at, last_checked_at, consecutive_failures
		) VALUES ($1, 'healthy', NOW(), NOW(), 0)`,
		id,
	)
	require.NoError(t, err)
	return id
}

func insertWorkflowAgentCapability(t *testing.T, pool *pgxpool.Pool, agentID uuid.UUID, inputSchema string) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
		INSERT INTO agent_capabilities (
			agent_id, input_schema, output_schema, summary, version, published_at, updated_at
		) VALUES ($1, $2::jsonb, '{"type":"object"}'::jsonb, 'workflow mapping test', 1, NOW(), NOW())
	`, agentID, inputSchema)
	require.NoError(t, err)
}

func insertWorkflowRuntimeSession(
	t *testing.T,
	pool *pgxpool.Pool,
	coreID, creatorID, agentID uuid.UUID,
) {
	t.Helper()
	ctx := context.Background()
	tokenID, nodeID, sessionID, attachmentID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	workerID := uuid.NewString()
	certificateSerial := hex.EncodeToString(nodeID[:])
	thumbprintDigest := sha256.Sum256(nodeID[:])
	thumbprint := hex.EncodeToString(thumbprintDigest[:])
	prefix := "ol_agent_" + hex.EncodeToString(tokenID[:6])

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, `
		INSERT INTO agent_tokens (
			id, agent_id, creator_user_id, name, prefix, token_hash,
			scopes, status, redeemed_at
		) VALUES ($1, $2, $3, 'Workflow Runtime Session', $4, $5,
			ARRAY['agent:call', 'agent:pull']::text[], 'active_runtime', clock_timestamp())`,
		tokenID, agentID, creatorID, prefix, "hash-"+tokenID.String())
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `
		INSERT INTO runtime_nodes (
			node_id, display_name, device_certificate_serial,
			device_public_key_thumbprint, node_version, protocol_version,
			runtime_contract_id, runtime_contract_digest, features,
			capacity, last_seen_at
		) VALUES ($1, 'Workflow Runtime Session', $2, $3, 'test-v2', 2,
			'openlinker.runtime.v2', $4, $5, 1, clock_timestamp())`,
		nodeID, certificateSerial, thumbprint, runtimemod.RuntimeContractDigest,
		runtimemod.RuntimeRequiredFeatures())
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `
		INSERT INTO runtime_sessions (
			runtime_session_id, node_id, agent_id, credential_id, worker_id,
			session_epoch, device_certificate_serial, node_version,
			protocol_version, runtime_contract_id, runtime_contract_digest,
			features, capacity, attached_core_instance_id
		) VALUES ($1, $2, $3, $4, $5, 1, $6, 'test-v2', 2,
			'openlinker.runtime.v2', $7, $8, 1, $9)`,
		sessionID, nodeID, agentID, tokenID, workerID, certificateSerial,
		runtimemod.RuntimeContractDigest, runtimemod.RuntimeRequiredFeatures(), coreID)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `
		INSERT INTO runtime_session_attachments (
			id, runtime_session_id, core_instance_id, attachment_kind
		) VALUES ($1, $2, $3, 'connected')`, attachmentID, sessionID, coreID)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))
}

func setWorkflowAgentUnreachable(t *testing.T, pool *pgxpool.Pool, agentID uuid.UUID) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`UPDATE agent_availability_snapshots
		 SET availability_status='unreachable',
		     last_successful_run_at=NULL,
		     last_failed_run_at=NOW(),
		     last_checked_at=NOW(),
		     consecutive_failures=3
		 WHERE agent_id=$1`,
		agentID,
	)
	require.NoError(t, err)
}

func setWorkflowAgentTags(t *testing.T, pool *pgxpool.Pool, agentID uuid.UUID, tags []string) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`UPDATE agents SET tags=$2 WHERE id=$1`,
		agentID,
		tags,
	)
	require.NoError(t, err)
}

func setWorkflowAgentVisibility(t *testing.T, pool *pgxpool.Pool, agentID uuid.UUID, visibility string) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`UPDATE agents SET visibility=$2 WHERE id=$1`,
		agentID,
		visibility,
	)
	require.NoError(t, err)
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

func requireWorkflowHTTPStatus(t *testing.T, err error, want int) {
	t.Helper()
	var httpErr *httpx.HTTPError
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, want, httpErr.Status)
}
