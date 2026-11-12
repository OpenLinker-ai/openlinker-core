package workflow_test

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	runtimemod "github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/OpenLinker-ai/openlinker-core/pkg/workflow"
)

// routedAgent answers per node key (from the legacy step input); a node
// listed in failOnce fails its first call.
func routedAgent(t *testing.T, outputs map[string]map[string]interface{}, failAlways, failOnce map[string]bool) *engineFixture {
	var mu sync.Mutex
	calls := map[string]int{}
	return newEngineFixture(t, func(_ int64, w http.ResponseWriter, r *http.Request) {
		var req runtimemod.AgentRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		key, _ := req.Input["node_key"].(string)
		mu.Lock()
		calls[key]++
		n := calls[key]
		mu.Unlock()
		if failAlways[key] || (failOnce[key] && n == 1) {
			http.Error(w, "node failed", http.StatusInternalServerError)
			return
		}
		writeAgentOutput(w, outputs[key])
	})
}

func createV2Workflow(t *testing.T, f *engineFixture, nodes []workflow.WorkflowNodeRequest, edges []map[string]interface{}, output map[string]interface{}) uuid.UUID {
	t.Helper()
	for i := range nodes {
		nodes[i].AgentID = f.agentID
	}
	created, err := f.svc.CreateWorkflow(context.Background(), f.ownerID, &workflow.CreateWorkflowRequest{
		Name: "v2", Semantics: "v2", Nodes: nodes, Edges: edges, OutputMapping: output,
	})
	require.NoError(t, err)
	require.Equal(t, "v2", created.Semantics)
	return uuid.MustParse(created.ID)
}

func stepsByKey(run *workflow.WorkflowRunResponse) map[string][]workflow.WorkflowRunStepResponse {
	result := map[string][]workflow.WorkflowRunStepResponse{}
	for _, step := range run.Steps {
		result[step.NodeKey] = append(result[step.NodeKey], step)
	}
	return result
}

func TestWorkflowV2ConditionalBranchSkipsTheOtherBranch(t *testing.T) {
	f := routedAgent(t, map[string]map[string]interface{}{
		"classify": {"category": "refund"}, "refund": {"handled": "refund"}, "other": {"handled": "other"},
	}, nil, nil)
	workflowID := createV2Workflow(t, f, []workflow.WorkflowNodeRequest{{Key: "classify"}, {Key: "refund"}, {Key: "other"}},
		[]map[string]interface{}{
			{"from": "classify", "to": "refund", "when": map[string]interface{}{"path": []interface{}{"category"}, "op": "eq", "value": "refund"}},
			{"from": "classify", "to": "other", "when": map[string]interface{}{"path": []interface{}{"category"}, "op": "ne", "value": "refund"}},
		}, nil)

	run, err := f.svc.RunWorkflow(context.Background(), f.ownerID, workflowID, &workflow.RunWorkflowRequest{})
	require.NoError(t, err)
	require.Equal(t, "success", run.Status)
	steps := stepsByKey(run)
	require.Contains(t, steps, "refund")
	require.NotContains(t, steps, "other", "a skipped node never runs")
	require.Equal(t, []interface{}{"refund"}, run.Output["terminal_nodes"], "only successful sinks are terminal outputs")

	var state, reason string
	require.NoError(t, f.pool.QueryRow(context.Background(), `
		SELECT state, skip_reason FROM workflow_run_nodes WHERE workflow_run_id = $1 AND node_key = 'other'`, uuid.MustParse(run.ID),
	).Scan(&state, &reason))
	require.Equal(t, "skipped", state)
	require.Contains(t, reason, "classify")
}

func TestWorkflowV2ContinueAndOutputMappingDefaults(t *testing.T) {
	f := routedAgent(t, map[string]map[string]interface{}{"b": {"value": 7}}, map[string]bool{"a": true}, nil)
	workflowID := createV2Workflow(t, f,
		[]workflow.WorkflowNodeRequest{
			{Key: "a", Config: map[string]interface{}{"policy": map[string]interface{}{"on_failure": "continue"}}},
			{Key: "b"},
		}, []map[string]interface{}{}, // independent roots
		map[string]interface{}{"version": "v2", "fields": map[string]interface{}{
			"from_a": map[string]interface{}{"source": "dependency", "node": "a", "path": []interface{}{"value"}, "default": "none"},
			"from_b": map[string]interface{}{"source": "dependency", "node": "b", "path": []interface{}{"value"}},
			"fixed":  map[string]interface{}{"source": "constant", "value": "x"},
		}})
	run, err := f.svc.RunWorkflow(context.Background(), f.ownerID, workflowID, &workflow.RunWorkflowRequest{})
	require.NoError(t, err)
	require.Equal(t, "success", run.Status, "a continue failure does not fail the run")
	require.Equal(t, map[string]interface{}{"from_a": "none", "from_b": float64(7), "fixed": "x"}, run.Output)
}

func TestWorkflowV2RetryPolicyAndNoUsableOutput(t *testing.T) {
	f := routedAgent(t, map[string]map[string]interface{}{"flaky": {"ok": true}}, map[string]bool{"broken": true}, map[string]bool{"flaky": true})
	retrying := createV2Workflow(t, f, []workflow.WorkflowNodeRequest{
		{Key: "flaky", Config: map[string]interface{}{"policy": map[string]interface{}{"retry": map[string]interface{}{"max_attempts": 2, "on": []interface{}{"failed"}}}}},
	}, nil, nil)
	run, err := f.svc.RunWorkflow(context.Background(), f.ownerID, retrying, &workflow.RunWorkflowRequest{})
	require.NoError(t, err)
	require.Equal(t, "success", run.Status)
	require.Len(t, stepsByKey(run)["flaky"], 2)

	nothing := createV2Workflow(t, f, []workflow.WorkflowNodeRequest{
		{Key: "broken", Config: map[string]interface{}{"policy": map[string]interface{}{"on_failure": "continue"}}},
	}, nil, nil)
	_, err = f.svc.RunWorkflow(context.Background(), f.ownerID, nothing, &workflow.RunWorkflowRequest{})
	require.ErrorContains(t, err, "没有可用的终端输出")
}

func TestWorkflowV2DefinitionsAreValidatedAndNotExternallyExecutable(t *testing.T) {
	f := okAgentFixture(t)
	ctx := context.Background()
	_, err := f.svc.CreateWorkflow(ctx, f.ownerID, &workflow.CreateWorkflowRequest{
		Name: "bad", Semantics: "v2",
		Nodes: []workflow.WorkflowNodeRequest{{Key: "a", AgentID: f.agentID, Config: map[string]interface{}{
			"policy": map[string]interface{}{"retry": map[string]interface{}{"max_attempts": 2, "on": []interface{}{"timeout"}}},
		}}},
	})
	require.ErrorContains(t, err, "timeout")
	_, err = f.svc.CreateWorkflow(ctx, f.ownerID, &workflow.CreateWorkflowRequest{
		Name: "v1 with output", Nodes: []workflow.WorkflowNodeRequest{{Key: "a", AgentID: f.agentID}},
		OutputMapping: map[string]interface{}{"version": "v2"},
	})
	require.ErrorContains(t, err, "semantics v2")

	workflowID := createV2Workflow(t, f, []workflow.WorkflowNodeRequest{{Key: "a"}}, nil, nil)
	insertWorkflowAgentCapability(t, f.pool, f.agentID, `{"type":"object"}`)
	validation, err := f.svc.ValidateExternalExecutionTargetVersion(ctx, f.ownerID, workflowID, nil)
	require.NoError(t, err)
	require.False(t, validation.Executable)
	require.Equal(t, "semantics_unsupported", validation.UnavailableReason)
}
