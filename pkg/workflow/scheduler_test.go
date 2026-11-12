package workflow

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

func schedulingGraph(parents map[string][]string, sequence map[string]int32) *workflowGraph {
	return &workflowGraph{Parents: parents, Sequence: sequence}
}

func schedulingSnapshot(nodes map[string]workflowNodeSnapshot) workflowRunSnapshot {
	for key, node := range nodes {
		node.nodeKey = key
		if node.attempt == 0 {
			node.attempt = 1
		}
		nodes[key] = node
	}
	return workflowRunSnapshot{runStatus: workflowRunStatusRunning, nodes: nodes}
}

func waitingNode() workflowNodeSnapshot {
	return workflowNodeSnapshot{state: nodeWaiting, status: attemptPending, startable: true}
}

func failWorkflowPolicy(string) string { return onFailureFailWorkflow }

func TestWorkflowSchedulingDiamondHasNoLevelBarrier(t *testing.T) {
	// a -> b, a -> c, b -> d, c -> d. While c is still running, b's success
	// does not start d (d waits for both parents), but nothing waits for a
	// level: once c succeeds, d is ready.
	graph := schedulingGraph(
		map[string][]string{"b": {"a"}, "c": {"a"}, "d": {"b", "c"}},
		map[string]int32{"a": 0, "b": 1, "c": 1, "d": 2},
	)
	order := workflowTopologicalOrder(graph)

	start := decideWorkflowScheduling(graph, order, schedulingSnapshot(map[string]workflowNodeSnapshot{
		"a": waitingNode(), "b": waitingNode(), "c": waitingNode(), "d": waitingNode(),
	}), map[string]bool{}, 4, failWorkflowPolicy, workflowRunSemantics{})
	require.Equal(t, []string{"a"}, start.drive, "only the root is ready")

	branches := decideWorkflowScheduling(graph, order, schedulingSnapshot(map[string]workflowNodeSnapshot{
		"a": {state: nodeSuccess, status: attemptSuccess},
		"b": waitingNode(), "c": waitingNode(), "d": waitingNode(),
	}), map[string]bool{}, 4, failWorkflowPolicy, workflowRunSemantics{})
	require.Equal(t, []string{"b", "c"}, branches.drive)

	oneBranch := decideWorkflowScheduling(graph, order, schedulingSnapshot(map[string]workflowNodeSnapshot{
		"a": {state: nodeSuccess, status: attemptSuccess},
		"b": {state: nodeSuccess, status: attemptSuccess},
		"c": {state: nodeRunning, status: attemptRunning},
		"d": waitingNode(),
	}), map[string]bool{"c": true}, 4, failWorkflowPolicy, workflowRunSemantics{})
	require.Empty(t, oneBranch.drive, "d waits for c")

	join := decideWorkflowScheduling(graph, order, schedulingSnapshot(map[string]workflowNodeSnapshot{
		"a": {state: nodeSuccess, status: attemptSuccess},
		"b": {state: nodeSuccess, status: attemptSuccess},
		"c": {state: nodeSuccess, status: attemptSuccess},
		"d": waitingNode(),
	}), map[string]bool{}, 4, failWorkflowPolicy, workflowRunSemantics{})
	require.Equal(t, []string{"d"}, join.drive)

	done := decideWorkflowScheduling(graph, order, schedulingSnapshot(map[string]workflowNodeSnapshot{
		"a": {state: nodeSuccess}, "b": {state: nodeSuccess}, "c": {state: nodeSuccess}, "d": {state: nodeSuccess},
	}), map[string]bool{}, 4, failWorkflowPolicy, workflowRunSemantics{})
	require.True(t, done.done)
}

func TestWorkflowSchedulingWideFanOutRespectsParallelism(t *testing.T) {
	parents := map[string][]string{}
	sequence := map[string]int32{"root": 0}
	nodes := map[string]workflowNodeSnapshot{"root": {state: nodeSuccess, status: attemptSuccess}}
	for _, key := range []string{"n1", "n2", "n3", "n4", "n5", "n6"} {
		parents[key] = []string{"root"}
		sequence[key] = 1
		nodes[key] = waitingNode()
	}
	graph := schedulingGraph(parents, sequence)
	order := workflowTopologicalOrder(graph)

	decision := decideWorkflowScheduling(graph, order, schedulingSnapshot(nodes), map[string]bool{}, 4, failWorkflowPolicy, workflowRunSemantics{})
	require.Len(t, decision.drive, 4)

	// Two already in flight leave room for two more.
	decision = decideWorkflowScheduling(graph, order, schedulingSnapshot(nodes),
		map[string]bool{"n1": true, "n2": true}, 4, failWorkflowPolicy, workflowRunSemantics{})
	require.Equal(t, []string{"n3", "n4"}, decision.drive)
}

func TestWorkflowSchedulingRecoveredRunningAttemptsIgnoreTheLimit(t *testing.T) {
	graph := schedulingGraph(map[string][]string{}, map[string]int32{"a": 0, "b": 0, "c": 0})
	order := workflowTopologicalOrder(graph)
	decision := decideWorkflowScheduling(graph, order, schedulingSnapshot(map[string]workflowNodeSnapshot{
		"a": {state: nodeRunning, status: attemptRunning},
		"b": {state: nodeRunning, status: attemptRunning},
		"c": waitingNode(),
	}), map[string]bool{}, 1, failWorkflowPolicy, workflowRunSemantics{})
	require.Equal(t, []string{"a", "b"}, decision.drive, "in-flight attempts are resumed; c waits for a slot")
}

func TestWorkflowSchedulingRetryAndStoppingKeepChildrenWaiting(t *testing.T) {
	graph := schedulingGraph(map[string][]string{"b": {"a"}}, map[string]int32{"a": 0, "b": 1})
	order := workflowTopologicalOrder(graph)

	backoff := decideWorkflowScheduling(graph, order, schedulingSnapshot(map[string]workflowNodeSnapshot{
		"a": {state: nodeRetryWait, status: attemptPending, attempt: 2, startable: false},
		"b": waitingNode(),
	}), map[string]bool{}, 4, failWorkflowPolicy, workflowRunSemantics{})
	require.Empty(t, backoff.drive, "a is backing off; its child stays unresolved")
	require.False(t, backoff.failed)
	require.False(t, backoff.done)

	due := decideWorkflowScheduling(graph, order, schedulingSnapshot(map[string]workflowNodeSnapshot{
		"a": {state: nodeRetryWait, status: attemptPending, attempt: 2, startable: true},
		"b": waitingNode(),
	}), map[string]bool{}, 4, failWorkflowPolicy, workflowRunSemantics{})
	require.Equal(t, []string{"a"}, due.drive)

	stopping := decideWorkflowScheduling(graph, order, schedulingSnapshot(map[string]workflowNodeSnapshot{
		"a": {state: nodeStopping, status: attemptFailed},
		"b": waitingNode(),
	}), map[string]bool{}, 4, failWorkflowPolicy, workflowRunSemantics{})
	require.Empty(t, stopping.drive)
	require.Equal(t, []string{"a"}, stopping.recompute)
	require.False(t, stopping.failed, "stopping is not a logical failure yet")
}

func TestWorkflowSchedulingLogicalFailureFailsTheWorkflow(t *testing.T) {
	graph := schedulingGraph(map[string][]string{"c": {"a", "b"}}, map[string]int32{"a": 0, "b": 0, "c": 1})
	order := workflowTopologicalOrder(graph)
	for _, state := range []string{nodeFailed, nodeTimeout, nodeCanceled} {
		decision := decideWorkflowScheduling(graph, order, schedulingSnapshot(map[string]workflowNodeSnapshot{
			"a": {state: state, status: state},
			"b": {state: nodeRunning, status: attemptRunning},
			"c": waitingNode(),
		}), map[string]bool{"b": true}, 4, failWorkflowPolicy, workflowRunSemantics{})
		require.True(t, decision.failed, state)
		require.Equal(t, "a", decision.failedKey)
		require.Empty(t, decision.drive)
	}
}

func TestWorkflowNodeAttemptIdempotencyKeyKeepsAttemptOne(t *testing.T) {
	runID := testWorkflowRunID()
	require.Equal(t, workflowNodeRunIdempotencyKey(runID, "worker"), workflowNodeAttemptIdempotencyKey(runID, "worker", 1))
	require.Equal(t, workflowNodeRunIdempotencyKey(runID, "worker")+"/attempt/2", workflowNodeAttemptIdempotencyKey(runID, "worker", 2))
	first := workflowNodeRunMetadata(runID, runID, testWorkflowNode(), 1)
	require.NotContains(t, first, "workflow_node_attempt", "attempt 1 keeps its frozen metadata field set")
	require.Equal(t, int32(2), workflowNodeRunMetadata(runID, runID, testWorkflowNode(), 2)["workflow_node_attempt"])
}

func testWorkflowRunID() uuid.UUID { return uuid.MustParse("6b0f2d4e-3f8a-4c56-9d1e-2a7b8c9d0e1f") }

func testWorkflowNode() db.WorkflowNode {
	return db.WorkflowNode{ID: uuid.MustParse("1c2d3e4f-5a6b-4c7d-8e9f-0a1b2c3d4e5f"), NodeKey: "worker"}
}

// A join:any node reached through one live branch keeps retrying even though
// its other parent failed under continue: retry_wait is past activation.
func TestWorkflowSchedulingJoinAnyRetryIgnoresFailedParent(t *testing.T) {
	edges := []map[string]interface{}{{"from": "a", "to": "c"}, {"from": "b", "to": "c"}}
	def, err := parseWorkflowV2Definition(v2Nodes(map[string]string{
		"a": `{"policy":{"on_failure":"continue"}}`, "b": `{}`,
		"c": `{"join":"any","policy":{"retry":{"max_attempts":2}}}`,
	}), edges, nil)
	require.NoError(t, err)
	graph := schedulingGraph(
		map[string][]string{"c": {"a", "b"}},
		map[string]int32{"a": 0, "b": 0, "c": 1},
	)
	order := workflowTopologicalOrder(graph)
	onFailure := func(key string) string {
		if key == "a" {
			return onFailureContinue
		}
		return onFailureFailWorkflow
	}
	decision := decideWorkflowScheduling(graph, order, schedulingSnapshot(map[string]workflowNodeSnapshot{
		"a": {state: nodeFailed, status: attemptFailed},
		"b": {state: nodeSuccess, status: attemptSuccess},
		"c": {state: nodeRetryWait, status: attemptPending, startable: true, attempt: 2},
	}), map[string]bool{}, 4, onFailure, workflowRunSemantics{semantics: workflowSemanticsV2, v2: def})
	require.Equal(t, []string{"c"}, decision.drive)
}
