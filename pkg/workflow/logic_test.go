package workflow

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/labstack/echo/v4"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	runtimemod "github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

func TestWorkflowGraphAndEdgeHelpers(t *testing.T) {
	if got := normalizeRunStatus(" Success "); got != "success" {
		t.Fatalf("normalizeRunStatus = %q", got)
	}

	emptyEdges, err := normalizeWorkflowEdges([]string{"extract"}, nil)
	if err != nil || len(emptyEdges) != 0 {
		t.Fatalf("normalizeWorkflowEdges empty = %#v %v", emptyEdges, err)
	}

	edges, err := normalizeWorkflowEdges([]string{"extract", "summarize"}, []map[string]interface{}{
		{"source": " extract ", "targetKey": "summarize", "label": "handoff"},
	})
	if err != nil {
		t.Fatalf("normalizeWorkflowEdges error: %v", err)
	}
	if len(edges) != 1 || edges[0]["from"] != "extract" || edges[0]["to"] != "summarize" || edges[0]["label"] != "handoff" {
		t.Fatalf("normalized edges = %#v", edges)
	}
	if got := workflowEdgeEndpoint(map[string]interface{}{"from": 123, "sourceKey": " fallback "}, "from", "sourceKey"); got != "fallback" {
		t.Fatalf("workflowEdgeEndpoint fallback = %q", got)
	}
	if workflowEdgeEndpoint(map[string]interface{}{"from": 123}, "from") != "" {
		t.Fatalf("workflowEdgeEndpoint should ignore non-string endpoints")
	}
	if !isWorkflowEndpointKey("sourceKey") || isWorkflowEndpointKey("label") {
		t.Fatalf("isWorkflowEndpointKey failed")
	}

	for _, tc := range []struct {
		name     string
		nodeKeys []string
		edges    []map[string]interface{}
	}{
		{name: "duplicate node", nodeKeys: []string{"a", "a"}, edges: []map[string]interface{}{{"from": "a", "to": "b"}}},
		{name: "missing endpoint", nodeKeys: []string{"a", "b"}, edges: []map[string]interface{}{{"from": "a"}}},
		{name: "self edge", nodeKeys: []string{"a"}, edges: []map[string]interface{}{{"from": "a", "to": "a"}}},
		{name: "unknown from", nodeKeys: []string{"a", "b"}, edges: []map[string]interface{}{{"from": "x", "to": "b"}}},
		{name: "unknown to", nodeKeys: []string{"a", "b"}, edges: []map[string]interface{}{{"from": "a", "to": "x"}}},
		{name: "duplicate edge", nodeKeys: []string{"a", "b"}, edges: []map[string]interface{}{{"from": "a", "to": "b"}, {"from": "a", "to": "b"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := normalizeWorkflowEdges(tc.nodeKeys, tc.edges); err == nil {
				t.Fatalf("expected normalizeWorkflowEdges to fail")
			}
		})
	}

	nodes := []db.WorkflowNode{
		{NodeKey: "extract", Position: 0},
		{NodeKey: "summarize", Position: 1},
		{NodeKey: "publish", Position: 2},
	}
	graph, err := buildWorkflowGraph(nodes, nil)
	if err != nil {
		t.Fatalf("buildWorkflowGraph sequential: %v", err)
	}
	if !reflect.DeepEqual(graph.Parents["summarize"], []string{"extract"}) || !reflect.DeepEqual(graph.Parents["publish"], []string{"summarize"}) {
		t.Fatalf("sequential parents = %#v", graph.Parents)
	}
	if !reflect.DeepEqual(graph.Sinks, []string{"publish"}) || graph.Sequence["extract"] != 0 || graph.Sequence["publish"] != 2 {
		t.Fatalf("sequential graph = %+v", graph)
	}

	dagNodes := []db.WorkflowNode{
		{NodeKey: "collect", Position: 0},
		{NodeKey: "analyze", Position: 1},
		{NodeKey: "synthesize", Position: 2},
	}
	dag, err := buildWorkflowGraph(dagNodes, []map[string]interface{}{
		{"from": "collect", "to": "synthesize"},
		{"from": "analyze", "to": "synthesize"},
	})
	if err != nil {
		t.Fatalf("buildWorkflowGraph DAG: %v", err)
	}
	if len(dag.Levels) != 2 || len(dag.Levels[0]) != 2 || dag.Levels[0][0].NodeKey != "collect" || dag.Levels[0][1].NodeKey != "analyze" {
		t.Fatalf("DAG levels = %#v", dag.Levels)
	}
	if !reflect.DeepEqual(dag.Parents["synthesize"], []string{"collect", "analyze"}) || !reflect.DeepEqual(dag.Sinks, []string{"synthesize"}) {
		t.Fatalf("DAG parents/sinks = %#v / %#v", dag.Parents, dag.Sinks)
	}
	if _, err := buildWorkflowGraph(dagNodes, []map[string]interface{}{
		{"from": "collect", "to": "analyze"},
		{"from": "analyze", "to": "collect"},
	}); err == nil {
		t.Fatalf("cycle should fail")
	}
	if _, err := buildWorkflowGraph(dagNodes, []map[string]interface{}{{"from": "missing", "to": "analyze"}}); err == nil {
		t.Fatalf("direct graph build with unknown from should fail")
	}
	if _, err := buildWorkflowGraph(dagNodes, []map[string]interface{}{{"from": "collect", "to": "missing"}}); err == nil {
		t.Fatalf("direct graph build with unknown to should fail")
	}
	emptyGraph, err := buildWorkflowGraph(nil, nil)
	if err != nil || len(emptyGraph.Levels) != 0 || len(emptyGraph.Sinks) != 0 {
		t.Fatalf("empty graph = %+v %v", emptyGraph, err)
	}

	requestNodes := []WorkflowNodeRequest{
		{Key: " collect ", AgentID: uuid.New()},
		{Key: "synthesize", AgentID: uuid.New()},
	}
	requestEdges, err := normalizeWorkflowEdgesFromRequest(requestNodes, []map[string]interface{}{{"from": "collect", "to": "synthesize"}})
	if err != nil || requestEdges[0]["from"] != "collect" {
		t.Fatalf("normalizeWorkflowEdgesFromRequest = %#v %v", requestEdges, err)
	}
	if err := validateWorkflowGraphFromRequest(requestNodes, requestEdges); err != nil {
		t.Fatalf("validateWorkflowGraphFromRequest: %v", err)
	}

	defined, err := workflowGraphFromDefinition(db.Workflow{
		Edges: []byte(`[
			{"sourceKey":"collect","targetKey":"synthesize","condition":"ok"},
			{"from":"analyze","to":"synthesize"}
		]`),
	}, dagNodes)
	if err != nil {
		t.Fatalf("workflowGraphFromDefinition: %v", err)
	}
	if !reflect.DeepEqual(defined.Parents["synthesize"], []string{"collect", "analyze"}) || !reflect.DeepEqual(defined.Sinks, []string{"synthesize"}) {
		t.Fatalf("defined graph parents/sinks = %#v / %#v", defined.Parents, defined.Sinks)
	}
	if _, err := workflowGraphFromDefinition(db.Workflow{Edges: []byte(`{bad json`)}, dagNodes); err == nil {
		t.Fatalf("invalid stored workflow edges should fail")
	}
	if _, err := workflowGraphFromDefinition(db.Workflow{Edges: []byte(`[{"from":"collect","to":"missing"}]`)}, dagNodes); err == nil {
		t.Fatalf("stored workflow edges with unknown endpoint should fail")
	}
	singleDefined, err := workflowGraphFromDefinition(db.Workflow{}, []db.WorkflowNode{{NodeKey: "only", Position: 0}})
	if err != nil || !reflect.DeepEqual(singleDefined.Sinks, []string{"only"}) {
		t.Fatalf("single node definition graph = %+v %v", singleDefined, err)
	}
}

func TestWorkflowCreateValidatesBeforePersistence(t *testing.T) {
	svc := &Service{}
	userID := uuid.New()
	validAgentID := uuid.New()
	validNode := WorkflowNodeRequest{Key: "extract", AgentID: validAgentID}

	for _, tc := range []struct {
		name string
		req  *CreateWorkflowRequest
		want int
	}{
		{name: "nil request", req: nil, want: http.StatusBadRequest},
		{name: "missing nodes", req: &CreateWorkflowRequest{Name: "Draft"}, want: http.StatusBadRequest},
		{name: "too many nodes", req: &CreateWorkflowRequest{Name: "Draft", Nodes: []WorkflowNodeRequest{
			{Key: "n0", AgentID: validAgentID},
			{Key: "n1", AgentID: validAgentID},
			{Key: "n2", AgentID: validAgentID},
			{Key: "n3", AgentID: validAgentID},
			{Key: "n4", AgentID: validAgentID},
			{Key: "n5", AgentID: validAgentID},
			{Key: "n6", AgentID: validAgentID},
			{Key: "n7", AgentID: validAgentID},
			{Key: "n8", AgentID: validAgentID},
			{Key: "n9", AgentID: validAgentID},
			{Key: "n10", AgentID: validAgentID},
		}}, want: http.StatusBadRequest},
		{name: "blank name", req: &CreateWorkflowRequest{Name: "   ", Nodes: []WorkflowNodeRequest{validNode}}, want: http.StatusBadRequest},
		{name: "bad edge", req: &CreateWorkflowRequest{
			Name:  "Draft",
			Nodes: []WorkflowNodeRequest{validNode, {Key: "review", AgentID: validAgentID}},
			Edges: []map[string]interface{}{{"from": "extract", "to": "missing"}},
		}, want: http.StatusBadRequest},
		{name: "cyclic graph", req: &CreateWorkflowRequest{
			Name: "Draft",
			Nodes: []WorkflowNodeRequest{
				{Key: "extract", AgentID: validAgentID},
				{Key: "review", AgentID: validAgentID},
			},
			Edges: []map[string]interface{}{
				{"from": "extract", "to": "review"},
				{"from": "review", "to": "extract"},
			},
		}, want: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.CreateWorkflow(context.Background(), userID, tc.req)
			requireWorkflowHTTPStatus(t, err, tc.want)
		})
	}
}

func TestWorkflowResponseAndDataHelpers(t *testing.T) {
	original := map[string]interface{}{"topic": "a2a"}
	outputsByNode := map[string]map[string]interface{}{
		"extract": {"summary": "done"},
		"review":  {"approved": true},
	}
	firstInput := workflowStepInput(original, outputsByNode, nil, db.WorkflowNode{NodeKey: "extract"})
	if firstInput["node_key"] != "extract" || !reflect.DeepEqual(firstInput["workflow_input"], original) {
		t.Fatalf("first step input = %#v", firstInput)
	}
	nextInput := workflowStepInput(original, outputsByNode, []string{"extract"}, db.WorkflowNode{NodeKey: "review"})
	if nextInput["previous_node"] != "extract" || !reflect.DeepEqual(nextInput["previous_output"], outputsByNode["extract"]) {
		t.Fatalf("single parent input = %#v", nextInput)
	}
	multiInput := workflowStepInput(original, outputsByNode, []string{"extract", "review"}, db.WorkflowNode{NodeKey: "publish"})
	deps, ok := multiInput["dependencies"].(map[string]interface{})
	if !ok || deps["extract"] == nil || deps["review"] == nil {
		t.Fatalf("multi parent dependencies = %#v", multiInput)
	}

	singleFinal := workflowFinalOutput(outputsByNode, []string{"extract"})
	if singleFinal["summary"] != "done" || !reflect.DeepEqual(singleFinal["terminal_nodes"], []string{"extract"}) {
		t.Fatalf("single final output = %#v", singleFinal)
	}
	multiFinal := workflowFinalOutput(outputsByNode, []string{"extract", "review"})
	if multiFinal["terminal_outputs"] == nil || multiFinal["workflow_outputs"] == nil {
		t.Fatalf("multi final output = %#v", multiFinal)
	}

	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	workflowID := uuid.New()
	agentID := uuid.New()
	workflowResp := workflowToResponse(db.Workflow{
		ID:          workflowID,
		Name:        "A2A Workflow",
		Description: "interconnect",
		Status:      "active",
		Edges:       []byte(`[{"from":"extract","to":"review"}]`),
		CreatedAt:   now,
		UpdatedAt:   now.Add(time.Second),
	}, []db.WorkflowNode{{
		ID:         uuid.New(),
		WorkflowID: workflowID,
		NodeKey:    "extract",
		NodeType:   "agent",
		AgentID:    agentID,
		Title:      "Extract",
		Config:     []byte(`{"mode":"fast"}`),
		Position:   0,
	}})
	if workflowResp.ID != workflowID.String() || workflowResp.Nodes[0].AgentID != agentID.String() || workflowResp.Nodes[0].Config["mode"] != "fast" {
		t.Fatalf("workflow response = %+v", workflowResp)
	}
	if workflowResp.Edges[0]["from"] != "extract" || workflowResp.CreatedAt != now.Format(time.RFC3339) {
		t.Fatalf("workflow response edges/time = %+v", workflowResp)
	}
	invalidWorkflowResp := workflowToResponse(db.Workflow{ID: workflowID, Edges: []byte(`bad`), CreatedAt: now, UpdatedAt: now}, nil)
	if len(invalidWorkflowResp.Edges) != 0 {
		t.Fatalf("invalid workflow edges should be dropped, got %+v", invalidWorkflowResp.Edges)
	}
	invalidNodeResp := workflowNodeToResponse(db.WorkflowNode{ID: uuid.New(), AgentID: agentID, Config: []byte(`bad`)})
	if len(invalidNodeResp.Config) != 0 {
		t.Fatalf("invalid node config should decode to empty map, got %+v", invalidNodeResp.Config)
	}

	errMsg := "failed once"
	nextRetry := now.Add(time.Minute)
	claimed := now.Add(2 * time.Minute)
	finished := now.Add(3 * time.Minute)
	workerErr := "worker lost claim"
	childRunID := uuid.New()
	stepErr := "step failed"
	runResp := workflowRunToResponse(db.WorkflowRun{
		ID:              uuid.New(),
		WorkflowID:      workflowID,
		Status:          "failed",
		Input:           []byte(`{"prompt":"ship"}`),
		Output:          []byte(`{"summary":"partial"}`),
		ErrorMessage:    &errMsg,
		AttemptCount:    2,
		MaxAttempts:     3,
		NextRetryAt:     &nextRetry,
		ClaimedAt:       &claimed,
		LastWorkerError: &workerErr,
		StartedAt:       now,
		FinishedAt:      &finished,
		CreatedAt:       now,
		UpdatedAt:       now,
	}, []db.WorkflowRunStep{{
		ID:             uuid.New(),
		WorkflowNodeID: uuid.New(),
		NodeKey:        "extract",
		AgentID:        agentID,
		RunID:          &childRunID,
		Status:         "failed",
		Input:          []byte(`{"node_key":"extract"}`),
		Output:         []byte(`{"summary":"partial"}`),
		ErrorMessage:   &stepErr,
		Sequence:       1,
		StartedAt:      now,
		FinishedAt:     &finished,
	}})
	if runResp.Error != errMsg || runResp.NextRetryAt != nextRetry.Format(time.RFC3339) || runResp.ClaimedAt != claimed.Format(time.RFC3339) || runResp.LastWorkerError != workerErr {
		t.Fatalf("run response retry fields = %+v", runResp)
	}
	if runResp.Input["prompt"] != "ship" || runResp.Output["summary"] != "partial" || len(runResp.Steps) != 1 {
		t.Fatalf("run response payload = %+v", runResp)
	}
	if runResp.Steps[0].RunID != childRunID.String() || runResp.Steps[0].Error != stepErr || runResp.Steps[0].Output["summary"] != "partial" {
		t.Fatalf("step response = %+v", runResp.Steps[0])
	}
	invalidRunResp := workflowRunToResponse(db.WorkflowRun{
		ID:         uuid.New(),
		WorkflowID: workflowID,
		Status:     "running",
		Input:      []byte(`bad`),
		Output:     []byte(`bad`),
		StartedAt:  now,
		CreatedAt:  now,
		UpdatedAt:  now,
	}, []db.WorkflowRunStep{{
		ID:             uuid.New(),
		WorkflowNodeID: uuid.New(),
		NodeKey:        "bad",
		AgentID:        agentID,
		Status:         "running",
		Input:          []byte(`bad`),
		Output:         []byte(`bad`),
		StartedAt:      now,
	}})
	if len(invalidRunResp.Input) != 0 || len(invalidRunResp.Output) != 0 || invalidRunResp.Steps[0].RunID != "" || len(invalidRunResp.Steps[0].Input) != 0 || len(invalidRunResp.Steps[0].Output) != 0 {
		t.Fatalf("invalid run/step JSON response = %+v", invalidRunResp)
	}

	stepOutput, err := workflowStepOutputMap(db.WorkflowRunStep{NodeKey: "extract", Output: []byte(`{"ok":true}`)})
	if err != nil || stepOutput["ok"] != true {
		t.Fatalf("workflowStepOutputMap = %#v %v", stepOutput, err)
	}
	emptyStepOutput, err := workflowStepOutputMap(db.WorkflowRunStep{NodeKey: "empty"})
	if err != nil || len(emptyStepOutput) != 0 {
		t.Fatalf("empty workflowStepOutputMap = %#v %v", emptyStepOutput, err)
	}
	if _, err := workflowStepOutputMap(db.WorkflowRunStep{NodeKey: "extract", Output: []byte(`bad`)}); err == nil {
		t.Fatalf("invalid step output should fail")
	}
}

func TestWorkflowNodeErrorPreferenceKeepsBusinessFailure(t *testing.T) {
	businessErr := errors.New("agent branch failed")
	if got := preferWorkflowNodeError(nil, businessErr); got != businessErr {
		t.Fatalf("nil current should accept candidate, got %v", got)
	}
	if got := preferWorkflowNodeError(context.Canceled, businessErr); got != businessErr {
		t.Fatalf("business error should replace context cancellation, got %v", got)
	}
	if got := preferWorkflowNodeError(businessErr, context.Canceled); got != businessErr {
		t.Fatalf("context cancellation should not replace business error, got %v", got)
	}
	if got := preferWorkflowNodeError(context.DeadlineExceeded, context.Canceled); !errors.Is(got, context.DeadlineExceeded) {
		t.Fatalf("first context error should be kept, got %v", got)
	}
}

func TestWorkflowComparisonAndRerunHelpers(t *testing.T) {
	graph := &workflowGraph{
		Children: map[string][]string{
			"extract":   {"review"},
			"review":    {"publish"},
			"publish":   {},
			"unrelated": {},
		},
	}
	affected := workflowAffectedNodeKeys(graph, "review")
	if _, ok := affected["review"]; !ok {
		t.Fatalf("rerun root should be affected: %#v", affected)
	}
	if _, ok := affected["publish"]; !ok {
		t.Fatalf("downstream child should be affected: %#v", affected)
	}
	if _, ok := affected["extract"]; ok {
		t.Fatalf("upstream parent should not be affected: %#v", affected)
	}
	dupChildAffected := workflowAffectedNodeKeys(&workflowGraph{Children: map[string][]string{"root": {"child", "child"}, "child": {}}}, "root")
	if len(dupChildAffected) != 2 {
		t.Fatalf("duplicate child should be visited once: %#v", dupChildAffected)
	}

	first := db.WorkflowRunStep{NodeKey: "extract", Status: "running"}
	second := db.WorkflowRunStep{NodeKey: "extract", Status: "success"}
	if got := latestWorkflowStepByNodeKey([]db.WorkflowRunStep{first, second})["extract"]; got.Status != "success" {
		t.Fatalf("latestWorkflowStepByNodeKey should keep the latest step, got %#v", got)
	}

	if !reflect.DeepEqual(orderedWorkflowStepKeys(
		[]db.WorkflowRunStep{{NodeKey: "extract"}, {NodeKey: "review"}},
		[]db.WorkflowRunStep{{NodeKey: "review"}, {NodeKey: "publish"}},
	), []string{"extract", "review", "publish"}) {
		t.Fatalf("orderedWorkflowStepKeys did not preserve base then candidate order")
	}

	if !jsonBytesEqual([]byte(`{"a":1,"b":2}`), []byte(`{"b":2,"a":1}`)) {
		t.Fatalf("jsonBytesEqual should compare decoded JSON values")
	}
	if jsonBytesEqual([]byte(`not-json`), []byte(`{"raw":"not-json"}`)) {
		t.Fatalf("invalid JSON should compare as raw text")
	}
	if !jsonBytesEqual(nil, []byte(`{}`)) {
		t.Fatalf("empty JSON bytes should compare as empty object")
	}

	workflowID := uuid.New()
	baseRunID := uuid.New()
	candidateRunID := uuid.New()
	baseChildRunID := uuid.New()
	candidateChildRunID := uuid.New()
	errMsg := "needs revision"
	baseRun := db.WorkflowRun{
		ID:         baseRunID,
		WorkflowID: workflowID,
		Status:     "success",
		Output:     []byte(`{"summary":"done","count":2}`),
	}
	candidateRun := db.WorkflowRun{
		ID:         candidateRunID,
		WorkflowID: workflowID,
		Status:     "success",
		Output:     []byte(`{"count":2,"summary":"done"}`),
	}
	baseSteps := []db.WorkflowRunStep{
		{NodeKey: "extract", Status: "success", RunID: &baseChildRunID, Output: []byte(`{"text":"same"}`)},
		{NodeKey: "review", Status: "failed", RunID: &baseChildRunID, Output: []byte(`{"approved":false}`), ErrorMessage: &errMsg},
	}
	candidateSteps := []db.WorkflowRunStep{
		{NodeKey: "extract", Status: "success", RunID: &baseChildRunID, Output: []byte(`{"text":"same"}`)},
		{NodeKey: "review", Status: "success", RunID: &candidateChildRunID, Output: []byte(`{"approved":true}`)},
		{NodeKey: "publish", Status: "success", RunID: &candidateChildRunID, Output: []byte(`{"posted":true}`)},
	}

	comparison := compareWorkflowRuns(baseRun, candidateRun, baseSteps, candidateSteps)
	if comparison.BaseRunID != baseRunID.String() || comparison.CandidateRunID != candidateRunID.String() || comparison.WorkflowID != workflowID.String() {
		t.Fatalf("comparison identifiers = %+v", comparison)
	}
	if comparison.OutputChanged || comparison.StatusChanged {
		t.Fatalf("equivalent run outputs/status should not be marked changed: %+v", comparison)
	}
	if !reflect.DeepEqual(comparison.ChangedNodeKeys, []string{"review", "publish"}) {
		t.Fatalf("changed nodes = %#v", comparison.ChangedNodeKeys)
	}
	if len(comparison.Steps) != 3 || comparison.Steps[0].Changed || !comparison.Steps[1].StatusChanged || !comparison.Steps[1].RunChanged || !comparison.Steps[1].OutputChanged || !comparison.Steps[1].ErrorChanged || !comparison.Steps[2].Changed {
		t.Fatalf("unexpected step comparison = %+v", comparison.Steps)
	}
	missingCandidate := compareWorkflowRuns(
		db.WorkflowRun{ID: baseRunID, WorkflowID: workflowID, Status: "success", Output: []byte(`{"ok":true}`)},
		db.WorkflowRun{ID: candidateRunID, WorkflowID: workflowID, Status: "failed", Output: []byte(`{"ok":false}`)},
		[]db.WorkflowRunStep{{NodeKey: "archive", Status: "success", RunID: &baseChildRunID, Output: []byte(`{"done":true}`)}},
		nil,
	)
	if !missingCandidate.StatusChanged || !missingCandidate.OutputChanged || !reflect.DeepEqual(missingCandidate.ChangedNodeKeys, []string{"archive"}) || len(missingCandidate.Steps) != 1 || !missingCandidate.Steps[0].Changed {
		t.Fatalf("missing candidate comparison = %+v", missingCandidate)
	}

	if workflowRunIDString(nil) != "" || workflowRunIDString(&baseChildRunID) != baseChildRunID.String() {
		t.Fatalf("workflowRunIDString failed")
	}
	if stringPtrValue(nil) != "" || stringPtrValue(&errMsg) != errMsg {
		t.Fatalf("stringPtrValue failed")
	}
	if truncate("abcdef", 3) != "abc" || truncate("abc", 3) != "abc" {
		t.Fatalf("truncate failed")
	}
}

func TestWorkflowFailureStatusMarking(t *testing.T) {
	runID := uuid.New()
	workflowID := uuid.New()
	userID := uuid.New()
	longMessage := strings.Repeat("x", 1205)
	storedMessage := strings.Repeat("x", 1000)

	dbtx := &workflowFakeDBTX{
		row: workflowFakeRow{
			values: workflowFakeRunValues(runID, workflowID, userID, workflowRunStatusFailed, &storedMessage),
		},
	}
	svc := &Service{queries: db.New(dbtx)}

	err := svc.failWorkflowRun(context.Background(), runID, longMessage)
	requireWorkflowHTTPStatus(t, err, http.StatusInternalServerError)
	if !strings.Contains(dbtx.queryRowSQL, "-- name: MarkWorkflowRunFailed") {
		t.Fatalf("expected MarkWorkflowRunFailed query, got %q", dbtx.queryRowSQL)
	}
	if len(dbtx.queryRowArgs) != 2 || dbtx.queryRowArgs[0] != runID {
		t.Fatalf("unexpected MarkWorkflowRunFailed args: %#v", dbtx.queryRowArgs)
	}
	messageArg, ok := dbtx.queryRowArgs[1].(*string)
	if !ok || messageArg == nil || *messageArg != storedMessage {
		t.Fatalf("expected truncated error message arg, got %#v", dbtx.queryRowArgs[1])
	}

	conflictSvc := &Service{queries: db.New(&workflowFakeDBTX{row: workflowFakeRow{err: pgx.ErrNoRows}})}
	err = conflictSvc.markWorkflowRunFailedStatus(context.Background(), runID, "already stopped")
	requireWorkflowHTTPStatus(t, err, http.StatusConflict)

	dbErr := errors.New("db down")
	failingSvc := &Service{queries: db.New(&workflowFakeDBTX{row: workflowFakeRow{err: dbErr}})}
	err = failingSvc.markWorkflowRunFailedStatus(context.Background(), runID, "boom")
	requireWorkflowHTTPStatus(t, err, http.StatusInternalServerError)
}

func TestWorkflowRunOwnershipAndDBErrorEdges(t *testing.T) {
	runID := uuid.New()
	workflowID := uuid.New()
	userID := uuid.New()

	notFoundSvc := &Service{queries: db.New(&workflowFakeDBTX{queryRowRows: []workflowFakeRow{{err: pgx.ErrNoRows}}})}
	_, err := notFoundSvc.GetWorkflowRun(context.Background(), userID, runID)
	requireWorkflowHTTPStatus(t, err, http.StatusNotFound)

	dbErrSvc := &Service{queries: db.New(&workflowFakeDBTX{queryRowRows: []workflowFakeRow{{err: errors.New("db down")}}})}
	_, err = dbErrSvc.GetWorkflowRun(context.Background(), userID, runID)
	requireWorkflowHTTPStatus(t, err, http.StatusInternalServerError)

	foreignSvc := &Service{queries: db.New(&workflowFakeDBTX{queryRowRows: []workflowFakeRow{{
		values: workflowFakeRunValues(runID, workflowID, uuid.New(), workflowRunStatusPending, nil),
	}}})}
	_, err = foreignSvc.GetWorkflowRun(context.Background(), userID, runID)
	requireWorkflowHTTPStatus(t, err, http.StatusNotFound)

	stepErrSvc := &Service{queries: db.New(&workflowFakeDBTX{
		queryRowRows: []workflowFakeRow{{
			values: workflowFakeRunValues(runID, workflowID, userID, workflowRunStatusPending, nil),
		}},
		queryResults: []workflowFakeQueryResult{{err: errors.New("steps unavailable")}},
	})}
	_, err = stepErrSvc.GetWorkflowRun(context.Background(), userID, runID)
	requireWorkflowHTTPStatus(t, err, http.StatusInternalServerError)
}

func TestWorkflowStateControlErrorEdges(t *testing.T) {
	runID := uuid.New()
	workflowID := uuid.New()
	userID := uuid.New()

	for _, tc := range []struct {
		name   string
		status string
		call   func(*Service) (*WorkflowRunResponse, error)
	}{
		{
			name:   "pause rejects terminal run",
			status: workflowRunStatusSuccess,
			call: func(s *Service) (*WorkflowRunResponse, error) {
				return s.PauseWorkflowRun(context.Background(), userID, runID)
			},
		},
		{
			name:   "resume rejects non paused run",
			status: workflowRunStatusPending,
			call: func(s *Service) (*WorkflowRunResponse, error) {
				return s.ResumeWorkflowRun(context.Background(), userID, runID)
			},
		},
		{
			name:   "cancel rejects terminal run",
			status: workflowRunStatusSuccess,
			call: func(s *Service) (*WorkflowRunResponse, error) {
				return s.CancelWorkflowRun(context.Background(), userID, runID)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := &Service{queries: db.New(&workflowFakeDBTX{queryRowRows: []workflowFakeRow{{
				values: workflowFakeRunValues(runID, workflowID, userID, tc.status, nil),
			}}})}
			_, err := tc.call(svc)
			requireWorkflowHTTPStatus(t, err, http.StatusConflict)
		})
	}

	for _, tc := range []struct {
		name   string
		status string
		call   func(*Service) (*WorkflowRunResponse, error)
	}{
		{
			name:   "pause update fails",
			status: workflowRunStatusPending,
			call: func(s *Service) (*WorkflowRunResponse, error) {
				return s.PauseWorkflowRun(context.Background(), userID, runID)
			},
		},
		{
			name:   "resume update fails",
			status: workflowRunStatusPaused,
			call: func(s *Service) (*WorkflowRunResponse, error) {
				return s.ResumeWorkflowRun(context.Background(), userID, runID)
			},
		},
		{
			name:   "cancel update fails",
			status: workflowRunStatusRunning,
			call: func(s *Service) (*WorkflowRunResponse, error) {
				return s.CancelWorkflowRun(context.Background(), userID, runID)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := &Service{queries: db.New(&workflowFakeDBTX{queryRowRows: []workflowFakeRow{
				{values: workflowFakeRunValues(runID, workflowID, userID, tc.status, nil)},
				{err: errors.New("state update failed")},
			}})}
			_, err := tc.call(svc)
			requireWorkflowHTTPStatus(t, err, http.StatusInternalServerError)
		})
	}

	for _, tc := range []struct {
		name       string
		status     string
		statusBack string
		call       func(*Service) (*WorkflowRunResponse, error)
	}{
		{
			name:       "pause lost update reloads current run",
			status:     workflowRunStatusPending,
			statusBack: workflowRunStatusPending,
			call: func(s *Service) (*WorkflowRunResponse, error) {
				return s.PauseWorkflowRun(context.Background(), userID, runID)
			},
		},
		{
			name:       "resume lost update reloads current run",
			status:     workflowRunStatusPaused,
			statusBack: workflowRunStatusPaused,
			call: func(s *Service) (*WorkflowRunResponse, error) {
				return s.ResumeWorkflowRun(context.Background(), userID, runID)
			},
		},
		{
			name:       "cancel lost update reloads current run",
			status:     workflowRunStatusRunning,
			statusBack: workflowRunStatusRunning,
			call: func(s *Service) (*WorkflowRunResponse, error) {
				return s.CancelWorkflowRun(context.Background(), userID, runID)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := &Service{queries: db.New(&workflowFakeDBTX{
				queryRowRows: []workflowFakeRow{
					{values: workflowFakeRunValues(runID, workflowID, userID, tc.status, nil)},
					{err: pgx.ErrNoRows},
					{values: workflowFakeRunValues(runID, workflowID, userID, tc.statusBack, nil)},
				},
				queryResults: []workflowFakeQueryResult{{}},
			})}
			resp, err := tc.call(svc)
			if err != nil || resp.Status != tc.statusBack {
				t.Fatalf("%s response = %#v, %v", tc.name, resp, err)
			}
		})
	}
}

func TestWorkflowQueueAndDefinitionErrorEdges(t *testing.T) {
	workflowID := uuid.New()
	userID := uuid.New()

	claimed, err := (&Service{}).ClaimAndRunPendingWorkflow(context.Background())
	if claimed {
		t.Fatalf("ClaimAndRunPendingWorkflow without runtime claimed work")
	}
	requireWorkflowHTTPStatus(t, err, http.StatusInternalServerError)

	noWorkSvc := &Service{
		runtime: &runtimemod.Service{},
		queries: db.New(&workflowFakeDBTX{queryRowRows: []workflowFakeRow{{err: pgx.ErrNoRows}}}),
	}
	claimed, err = noWorkSvc.ClaimAndRunPendingWorkflow(context.Background())
	if err != nil || claimed {
		t.Fatalf("ClaimAndRunPendingWorkflow no rows = %v, %v", claimed, err)
	}

	claimErrSvc := &Service{
		runtime: &runtimemod.Service{},
		queries: db.New(&workflowFakeDBTX{queryRowRows: []workflowFakeRow{{err: errors.New("claim failed")}}}),
	}
	claimed, err = claimErrSvc.ClaimAndRunPendingWorkflow(context.Background())
	if claimed {
		t.Fatalf("ClaimAndRunPendingWorkflow claimed on DB error")
	}
	requireWorkflowHTTPStatus(t, err, http.StatusInternalServerError)

	requeueSvc := &Service{queries: db.New(&workflowFakeDBTX{execRowsAffected: 2})}
	requeued, err := requeueSvc.RequeueStaleWorkflowRuns(context.Background(), -time.Second)
	if err != nil || requeued != 2 {
		t.Fatalf("RequeueStaleWorkflowRuns = %d, %v", requeued, err)
	}

	requeueErrSvc := &Service{queries: db.New(&workflowFakeDBTX{execErr: errors.New("requeue failed")})}
	_, err = requeueErrSvc.RequeueStaleWorkflowRuns(context.Background(), time.Minute)
	requireWorkflowHTTPStatus(t, err, http.StatusInternalServerError)

	notFoundSvc := &Service{queries: db.New(&workflowFakeDBTX{queryRowRows: []workflowFakeRow{{err: pgx.ErrNoRows}}})}
	_, _, err = notFoundSvc.getWorkflowDefinition(context.Background(), workflowID)
	requireWorkflowHTTPStatus(t, err, http.StatusNotFound)

	dbErrSvc := &Service{queries: db.New(&workflowFakeDBTX{queryRowRows: []workflowFakeRow{{err: errors.New("workflow lookup failed")}}})}
	_, _, err = dbErrSvc.getWorkflowDefinition(context.Background(), workflowID)
	requireWorkflowHTTPStatus(t, err, http.StatusInternalServerError)

	nodeErrSvc := &Service{queries: db.New(&workflowFakeDBTX{
		queryRowRows: []workflowFakeRow{{
			values: workflowFakeWorkflowValues(workflowID, userID),
		}},
		queryResults: []workflowFakeQueryResult{{err: errors.New("node lookup failed")}},
	})}
	_, _, err = nodeErrSvc.getWorkflowDefinition(context.Background(), workflowID)
	requireWorkflowHTTPStatus(t, err, http.StatusInternalServerError)
}

func TestWorkflowListCompareAndPrepareErrorEdges(t *testing.T) {
	workflowID := uuid.New()
	userID := uuid.New()
	agentID := uuid.New()
	baseRunID := uuid.New()
	candidateRunID := uuid.New()

	listErrSvc := &Service{queries: db.New(&workflowFakeDBTX{queryResults: []workflowFakeQueryResult{{err: errors.New("list failed")}}})}
	_, err := listErrSvc.ListWorkflows(context.Background(), userID, 999)
	requireWorkflowHTTPStatus(t, err, http.StatusInternalServerError)

	countErrSvc := &Service{queries: db.New(&workflowFakeDBTX{
		queryResults: []workflowFakeQueryResult{{rows: []workflowFakeRow{{values: workflowFakeWorkflowValues(workflowID, userID)}}}},
		queryRowRows: []workflowFakeRow{{err: errors.New("count failed")}},
	})}
	_, err = countErrSvc.ListWorkflows(context.Background(), userID, 10)
	requireWorkflowHTTPStatus(t, err, http.StatusInternalServerError)

	nodeListErrSvc := &Service{queries: db.New(&workflowFakeDBTX{
		queryResults: []workflowFakeQueryResult{
			{rows: []workflowFakeRow{{values: workflowFakeWorkflowValues(workflowID, userID)}}},
			{err: errors.New("nodes failed")},
		},
		queryRowRows: []workflowFakeRow{{values: []any{int32(1)}}},
	})}
	_, err = nodeListErrSvc.ListWorkflows(context.Background(), userID, 10)
	requireWorkflowHTTPStatus(t, err, http.StatusInternalServerError)

	compareConflictSvc := &Service{queries: db.New(&workflowFakeDBTX{queryRowRows: []workflowFakeRow{
		{values: workflowFakeRunValues(baseRunID, workflowID, userID, workflowRunStatusSuccess, nil)},
		{values: workflowFakeRunValues(candidateRunID, uuid.New(), userID, workflowRunStatusSuccess, nil)},
	}})}
	_, err = compareConflictSvc.CompareWorkflowRuns(context.Background(), userID, baseRunID, candidateRunID)
	requireWorkflowHTTPStatus(t, err, http.StatusConflict)

	compareBaseStepsErrSvc := &Service{queries: db.New(&workflowFakeDBTX{
		queryRowRows: []workflowFakeRow{
			{values: workflowFakeRunValues(baseRunID, workflowID, userID, workflowRunStatusSuccess, nil)},
			{values: workflowFakeRunValues(candidateRunID, workflowID, userID, workflowRunStatusSuccess, nil)},
		},
		queryResults: []workflowFakeQueryResult{{err: errors.New("base steps failed")}},
	})}
	_, err = compareBaseStepsErrSvc.CompareWorkflowRuns(context.Background(), userID, baseRunID, candidateRunID)
	requireWorkflowHTTPStatus(t, err, http.StatusInternalServerError)

	compareCandidateStepsErrSvc := &Service{queries: db.New(&workflowFakeDBTX{
		queryRowRows: []workflowFakeRow{
			{values: workflowFakeRunValues(baseRunID, workflowID, userID, workflowRunStatusSuccess, nil)},
			{values: workflowFakeRunValues(candidateRunID, workflowID, userID, workflowRunStatusSuccess, nil)},
		},
		queryResults: []workflowFakeQueryResult{
			{},
			{err: errors.New("candidate steps failed")},
		},
	})}
	_, err = compareCandidateStepsErrSvc.CompareWorkflowRuns(context.Background(), userID, baseRunID, candidateRunID)
	requireWorkflowHTTPStatus(t, err, http.StatusInternalServerError)

	noNodeSvc := &Service{queries: db.New(&workflowFakeDBTX{
		queryRowRows: []workflowFakeRow{{values: workflowFakeWorkflowValues(workflowID, userID)}},
		queryResults: []workflowFakeQueryResult{{}},
	})}
	_, _, _, _, _, _, err = noNodeSvc.prepareWorkflowExecution(context.Background(), userID, workflowID, nil)
	requireWorkflowHTTPStatus(t, err, http.StatusBadRequest)

	validNodeRows := []workflowFakeRow{{values: workflowFakeNodeValues(workflowID, agentID, "extract", 0)}}
	maxAttemptsSvc := &Service{queries: db.New(&workflowFakeDBTX{
		queryRowRows: []workflowFakeRow{{values: workflowFakeWorkflowValues(workflowID, userID)}},
		queryResults: []workflowFakeQueryResult{{rows: validNodeRows}},
	})}
	_, _, _, _, _, _, err = maxAttemptsSvc.prepareWorkflowExecution(context.Background(), userID, workflowID, &RunWorkflowRequest{MaxAttempts: 11})
	requireWorkflowHTTPStatus(t, err, http.StatusUnprocessableEntity)

	badInputSvc := &Service{queries: db.New(&workflowFakeDBTX{
		queryRowRows: []workflowFakeRow{{values: workflowFakeWorkflowValues(workflowID, userID)}},
		queryResults: []workflowFakeQueryResult{{rows: validNodeRows}},
	})}
	_, _, _, _, _, _, err = badInputSvc.prepareWorkflowExecution(context.Background(), userID, workflowID, &RunWorkflowRequest{
		Input: map[string]interface{}{"bad": make(chan int)},
	})
	requireWorkflowHTTPStatus(t, err, http.StatusBadRequest)
}

func TestWorkflowExecutionEntrypointErrorEdges(t *testing.T) {
	workflowID := uuid.New()
	userID := uuid.New()
	agentID := uuid.New()

	nilRuntimeSvc := &Service{}
	_, err := nilRuntimeSvc.RunWorkflow(context.Background(), userID, workflowID, nil)
	requireWorkflowHTTPStatus(t, err, http.StatusInternalServerError)
	_, err = nilRuntimeSvc.StartWorkflowRun(context.Background(), userID, workflowID, nil)
	requireWorkflowHTTPStatus(t, err, http.StatusInternalServerError)

	validNodeRows := []workflowFakeRow{{values: workflowFakeNodeValues(workflowID, agentID, "extract", 0)}}
	runCreateErrSvc := &Service{
		runtime: &runtimemod.Service{},
		queries: db.New(&workflowFakeDBTX{
			queryRowRows: []workflowFakeRow{
				{values: workflowFakeWorkflowValues(workflowID, userID)},
				{err: errors.New("create run failed")},
			},
			queryResults: []workflowFakeQueryResult{{rows: validNodeRows}},
		}),
	}
	_, err = runCreateErrSvc.RunWorkflow(context.Background(), userID, workflowID, nil)
	requireWorkflowHTTPStatus(t, err, http.StatusInternalServerError)

	startCreateErrSvc := &Service{
		runtime: &runtimemod.Service{},
		queries: db.New(&workflowFakeDBTX{
			queryRowRows: []workflowFakeRow{
				{values: workflowFakeWorkflowValues(workflowID, userID)},
				{err: errors.New("create pending run failed")},
			},
			queryResults: []workflowFakeQueryResult{{rows: validNodeRows}},
		}),
	}
	_, err = startCreateErrSvc.StartWorkflowRun(context.Background(), userID, workflowID, nil)
	requireWorkflowHTTPStatus(t, err, http.StatusInternalServerError)
}

func TestWorkflowListRunsAndOwnerErrorEdges(t *testing.T) {
	workflowID := uuid.New()
	runID := uuid.New()
	userID := uuid.New()
	agentID := uuid.New()
	nodeRows := []workflowFakeRow{{values: workflowFakeNodeValues(workflowID, agentID, "extract", 0)}}
	runRows := []workflowFakeRow{{values: workflowFakeRunValues(runID, workflowID, userID, workflowRunStatusPending, nil)}}

	for _, tc := range []struct {
		name string
		row  workflowFakeRow
		want int
	}{
		{name: "not found", row: workflowFakeRow{err: pgx.ErrNoRows}, want: http.StatusNotFound},
		{name: "db error", row: workflowFakeRow{err: errors.New("workflow lookup failed")}, want: http.StatusInternalServerError},
		{name: "foreign owner", row: workflowFakeRow{values: workflowFakeWorkflowValues(workflowID, uuid.New())}, want: http.StatusNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := &Service{queries: db.New(&workflowFakeDBTX{queryRowRows: []workflowFakeRow{tc.row}})}
			_, _, err := svc.getWorkflowForOwner(context.Background(), userID, workflowID)
			requireWorkflowHTTPStatus(t, err, tc.want)
		})
	}

	nodeErrSvc := &Service{queries: db.New(&workflowFakeDBTX{
		queryRowRows: []workflowFakeRow{{values: workflowFakeWorkflowValues(workflowID, userID)}},
		queryResults: []workflowFakeQueryResult{{err: errors.New("nodes failed")}},
	})}
	_, _, err := nodeErrSvc.getWorkflowForOwner(context.Background(), userID, workflowID)
	requireWorkflowHTTPStatus(t, err, http.StatusInternalServerError)

	runsQueryErrSvc := &Service{queries: db.New(&workflowFakeDBTX{
		queryRowRows: []workflowFakeRow{{values: workflowFakeWorkflowValues(workflowID, userID)}},
		queryResults: []workflowFakeQueryResult{
			{rows: nodeRows},
			{err: errors.New("runs failed")},
		},
	})}
	_, err = runsQueryErrSvc.ListWorkflowRuns(context.Background(), userID, workflowID, 999)
	requireWorkflowHTTPStatus(t, err, http.StatusInternalServerError)

	countErrSvc := &Service{queries: db.New(&workflowFakeDBTX{
		queryRowRows: []workflowFakeRow{
			{values: workflowFakeWorkflowValues(workflowID, userID)},
			{err: errors.New("count failed")},
		},
		queryResults: []workflowFakeQueryResult{
			{rows: nodeRows},
			{rows: runRows},
		},
	})}
	_, err = countErrSvc.ListWorkflowRuns(context.Background(), userID, workflowID, 10)
	requireWorkflowHTTPStatus(t, err, http.StatusInternalServerError)

	stepErrSvc := &Service{queries: db.New(&workflowFakeDBTX{
		queryRowRows: []workflowFakeRow{
			{values: workflowFakeWorkflowValues(workflowID, userID)},
			{values: []any{int32(1)}},
		},
		queryResults: []workflowFakeQueryResult{
			{rows: nodeRows},
			{rows: runRows},
			{err: errors.New("steps failed")},
		},
	})}
	_, err = stepErrSvc.ListWorkflowRuns(context.Background(), userID, workflowID, 10)
	requireWorkflowHTTPStatus(t, err, http.StatusInternalServerError)
}

func TestWorkflowRetryRerunWaitAndClaimedRunErrorEdges(t *testing.T) {
	workflowID := uuid.New()
	runID := uuid.New()
	userID := uuid.New()
	agentID := uuid.New()
	nodeRows := []workflowFakeRow{{values: workflowFakeNodeValues(workflowID, agentID, "extract", 0)}}

	retryConflictSvc := &Service{queries: db.New(&workflowFakeDBTX{queryRowRows: []workflowFakeRow{{
		values: workflowFakeRunValues(runID, workflowID, userID, workflowRunStatusSuccess, nil),
	}}})}
	_, err := retryConflictSvc.RetryWorkflowRun(context.Background(), userID, runID)
	requireWorkflowHTTPStatus(t, err, http.StatusConflict)

	badInputRun := workflowFakeRunValues(runID, workflowID, userID, workflowRunStatusFailed, nil)
	badInputRun[4] = []byte(`{bad`)
	retryBadInputSvc := &Service{queries: db.New(&workflowFakeDBTX{queryRowRows: []workflowFakeRow{{values: badInputRun}}})}
	_, err = retryBadInputSvc.RetryWorkflowRun(context.Background(), userID, runID)
	requireWorkflowHTTPStatus(t, err, http.StatusInternalServerError)

	_, err = (&Service{}).RerunWorkflowStep(context.Background(), userID, runID, &RerunWorkflowStepRequest{NodeKey: "extract"})
	requireWorkflowHTTPStatus(t, err, http.StatusInternalServerError)
	rerunValidationSvc := &Service{runtime: &runtimemod.Service{}}
	_, err = rerunValidationSvc.RerunWorkflowStep(context.Background(), userID, runID, nil)
	requireWorkflowHTTPStatus(t, err, http.StatusBadRequest)
	_, err = rerunValidationSvc.RerunWorkflowStep(context.Background(), userID, runID, &RerunWorkflowStepRequest{NodeKey: "   "})
	requireWorkflowHTTPStatus(t, err, http.StatusBadRequest)

	_, err = (&Service{}).waitForRuntimeRunCompletion(context.Background(), userID, uuid.Nil)
	if err == nil || !strings.Contains(err.Error(), "runID") {
		t.Fatalf("waitForRuntimeRunCompletion nil runID error = %v", err)
	}

	claimedBadInput := db.WorkflowRun{
		ID:           runID,
		WorkflowID:   workflowID,
		UserID:       userID,
		Status:       workflowRunStatusRunning,
		Input:        []byte(`{bad`),
		AttemptCount: 1,
		MaxAttempts:  3,
	}
	claimedBadInputSvc := &Service{queries: db.New(&workflowFakeDBTX{
		queryRowRows: []workflowFakeRow{
			{values: workflowFakeWorkflowValues(workflowID, userID)},
			{values: workflowFakeRunValues(runID, workflowID, userID, workflowRunStatusFailed, nil)},
		},
		queryResults: []workflowFakeQueryResult{{rows: nodeRows}},
	})}
	err = claimedBadInputSvc.executeClaimedWorkflowRun(context.Background(), claimedBadInput)
	requireWorkflowHTTPStatus(t, err, http.StatusInternalServerError)

	claimedRetry := claimedBadInput
	claimedRetry.Input = []byte(`{"prompt":"retry"}`)
	claimedRetry.AttemptCount = 2
	deleteErrSvc := &Service{queries: db.New(&workflowFakeDBTX{
		queryRowRows: []workflowFakeRow{
			{values: workflowFakeWorkflowValues(workflowID, userID)},
			{values: workflowFakeRunValues(runID, workflowID, userID, workflowRunStatusFailed, nil)},
		},
		queryResults: []workflowFakeQueryResult{{rows: nodeRows}},
		execErr:      errors.New("delete steps failed"),
	})}
	err = deleteErrSvc.executeClaimedWorkflowRun(context.Background(), claimedRetry)
	requireWorkflowHTTPStatus(t, err, http.StatusInternalServerError)
}

func TestWorkflowRerunStepPlanningErrorEdges(t *testing.T) {
	workflowID := uuid.New()
	runID := uuid.New()
	userID := uuid.New()
	agentID := uuid.New()
	extractNodeRows := []workflowFakeRow{{values: workflowFakeNodeValues(workflowID, agentID, "extract", 0)}}
	twoNodeRows := []workflowFakeRow{
		{values: workflowFakeNodeValues(workflowID, agentID, "extract", 0)},
		{values: workflowFakeNodeValues(workflowID, agentID, "review", 1)},
	}
	reviewStep := workflowFakeStepValues(runID, twoNodeRows[1].values[0].(uuid.UUID), agentID, "review", workflowRunStatusSuccess)
	extractStep := workflowFakeStepValues(runID, twoNodeRows[0].values[0].(uuid.UUID), agentID, "extract", workflowRunStatusSuccess)

	statusConflictSvc := &Service{
		runtime: &runtimemod.Service{},
		queries: db.New(&workflowFakeDBTX{queryRowRows: []workflowFakeRow{{
			values: workflowFakeRunValues(runID, workflowID, userID, workflowRunStatusPending, nil),
		}}}),
	}
	_, err := statusConflictSvc.RerunWorkflowStep(context.Background(), userID, runID, &RerunWorkflowStepRequest{NodeKey: "extract"})
	requireWorkflowHTTPStatus(t, err, http.StatusConflict)

	workflowLookupErrSvc := &Service{
		runtime: &runtimemod.Service{},
		queries: db.New(&workflowFakeDBTX{queryRowRows: []workflowFakeRow{
			{values: workflowFakeRunValues(runID, workflowID, userID, workflowRunStatusSuccess, nil)},
			{err: pgx.ErrNoRows},
		}}),
	}
	_, err = workflowLookupErrSvc.RerunWorkflowStep(context.Background(), userID, runID, &RerunWorkflowStepRequest{NodeKey: "extract"})
	requireWorkflowHTTPStatus(t, err, http.StatusNotFound)

	missingNodeSvc := &Service{
		runtime: &runtimemod.Service{},
		queries: db.New(&workflowFakeDBTX{
			queryRowRows: []workflowFakeRow{
				{values: workflowFakeRunValues(runID, workflowID, userID, workflowRunStatusSuccess, nil)},
				{values: workflowFakeWorkflowValues(workflowID, userID)},
			},
			queryResults: []workflowFakeQueryResult{{rows: extractNodeRows}},
		}),
	}
	_, err = missingNodeSvc.RerunWorkflowStep(context.Background(), userID, runID, &RerunWorkflowStepRequest{NodeKey: "missing"})
	requireWorkflowHTTPStatus(t, err, http.StatusNotFound)

	stepsErrSvc := &Service{
		runtime: &runtimemod.Service{},
		queries: db.New(&workflowFakeDBTX{
			queryRowRows: []workflowFakeRow{
				{values: workflowFakeRunValues(runID, workflowID, userID, workflowRunStatusSuccess, nil)},
				{values: workflowFakeWorkflowValues(workflowID, userID)},
			},
			queryResults: []workflowFakeQueryResult{
				{rows: extractNodeRows},
				{err: errors.New("steps failed")},
			},
		}),
	}
	_, err = stepsErrSvc.RerunWorkflowStep(context.Background(), userID, runID, &RerunWorkflowStepRequest{NodeKey: "extract"})
	requireWorkflowHTTPStatus(t, err, http.StatusInternalServerError)

	sourceStepMissingSvc := &Service{
		runtime: &runtimemod.Service{},
		queries: db.New(&workflowFakeDBTX{
			queryRowRows: []workflowFakeRow{
				{values: workflowFakeRunValues(runID, workflowID, userID, workflowRunStatusSuccess, nil)},
				{values: workflowFakeWorkflowValues(workflowID, userID)},
			},
			queryResults: []workflowFakeQueryResult{
				{rows: extractNodeRows},
				{},
			},
		}),
	}
	_, err = sourceStepMissingSvc.RerunWorkflowStep(context.Background(), userID, runID, &RerunWorkflowStepRequest{NodeKey: "extract"})
	requireWorkflowHTTPStatus(t, err, http.StatusConflict)

	badInputRun := workflowFakeRunValues(runID, workflowID, userID, workflowRunStatusSuccess, nil)
	badInputRun[4] = []byte(`{bad`)
	badInputSvc := &Service{
		runtime: &runtimemod.Service{},
		queries: db.New(&workflowFakeDBTX{
			queryRowRows: []workflowFakeRow{
				{values: badInputRun},
				{values: workflowFakeWorkflowValues(workflowID, userID)},
			},
			queryResults: []workflowFakeQueryResult{
				{rows: extractNodeRows},
				{rows: []workflowFakeRow{{values: extractStep}}},
			},
		}),
	}
	_, err = badInputSvc.RerunWorkflowStep(context.Background(), userID, runID, &RerunWorkflowStepRequest{NodeKey: "extract"})
	requireWorkflowHTTPStatus(t, err, http.StatusInternalServerError)

	createRunErrSvc := &Service{
		runtime: &runtimemod.Service{},
		queries: db.New(&workflowFakeDBTX{
			queryRowRows: []workflowFakeRow{
				{values: workflowFakeRunValues(runID, workflowID, userID, workflowRunStatusSuccess, nil)},
				{values: workflowFakeWorkflowValues(workflowID, userID)},
				{err: errors.New("create rerun failed")},
			},
			queryResults: []workflowFakeQueryResult{
				{rows: extractNodeRows},
				{rows: []workflowFakeRow{{values: extractStep}}},
			},
		}),
	}
	_, err = createRunErrSvc.RerunWorkflowStep(context.Background(), userID, runID, &RerunWorkflowStepRequest{NodeKey: "extract"})
	requireWorkflowHTTPStatus(t, err, http.StatusInternalServerError)

	failedExtractStep := append([]any(nil), extractStep...)
	failedExtractStep[6] = workflowRunStatusFailed
	unaffectedFailedSvc := &Service{
		runtime: &runtimemod.Service{},
		queries: db.New(&workflowFakeDBTX{
			queryRowRows: []workflowFakeRow{
				{values: workflowFakeRunValues(runID, workflowID, userID, workflowRunStatusSuccess, nil)},
				{values: workflowFakeWorkflowValues(workflowID, userID)},
			},
			queryResults: []workflowFakeQueryResult{
				{rows: twoNodeRows},
				{rows: []workflowFakeRow{{values: failedExtractStep}, {values: reviewStep}}},
			},
		}),
	}
	_, err = unaffectedFailedSvc.RerunWorkflowStep(context.Background(), userID, runID, &RerunWorkflowStepRequest{NodeKey: "review"})
	requireWorkflowHTTPStatus(t, err, http.StatusConflict)
}

func TestWorkflowStoppedAndWorkerTickErrorEdges(t *testing.T) {
	runID := uuid.New()
	workflowID := uuid.New()
	userID := uuid.New()

	statusErrSvc := &Service{queries: db.New(&workflowFakeDBTX{queryRowRows: []workflowFakeRow{{err: errors.New("status failed")}}})}
	stopped, resp, err := statusErrSvc.workflowRunStopped(context.Background(), userID, runID)
	if stopped || resp != nil {
		t.Fatalf("workflowRunStopped DB error returned stopped=%v resp=%#v", stopped, resp)
	}
	requireWorkflowHTTPStatus(t, err, http.StatusInternalServerError)

	pausedSvc := &Service{queries: db.New(&workflowFakeDBTX{
		queryRowRows: []workflowFakeRow{
			{values: workflowFakeRunValues(runID, workflowID, userID, workflowRunStatusPaused, nil)},
			{values: workflowFakeRunValues(runID, workflowID, userID, workflowRunStatusPaused, nil)},
		},
		queryResults: []workflowFakeQueryResult{{}},
	})}
	stopped, resp, err = pausedSvc.workflowRunStopped(context.Background(), userID, runID)
	if err != nil || !stopped || resp == nil || resp.Status != workflowRunStatusPaused {
		t.Fatalf("workflowRunStopped paused = stopped=%v resp=%#v err=%v", stopped, resp, err)
	}

	runWorkflowWorkerTick(context.Background(), &Service{
		runtime: &runtimemod.Service{},
		queries: db.New(&workflowFakeDBTX{execErr: errors.New("requeue failed")}),
	}, RunWorkerConfig{ClaimBurst: 1})
}

func TestWorkflowStepCopyAndRunNodeErrorEdges(t *testing.T) {
	runID := uuid.New()
	workflowID := uuid.New()
	userID := uuid.New()
	nodeID := uuid.New()
	agentID := uuid.New()
	sourceStep := db.WorkflowRunStep{
		ID:             uuid.New(),
		WorkflowRunID:  runID,
		WorkflowNodeID: nodeID,
		NodeKey:        "extract",
		AgentID:        agentID,
		Input:          []byte(`{"node_key":"extract"}`),
		Output:         []byte(`{"summary":"ok"}`),
		Sequence:       1,
	}

	createErrSvc := &Service{queries: db.New(&workflowFakeDBTX{queryRowRows: []workflowFakeRow{{err: errors.New("create step failed")}}})}
	err := createErrSvc.copyWorkflowRunStep(context.Background(), runID, sourceStep)
	requireWorkflowHTTPStatus(t, err, http.StatusInternalServerError)

	successErrSvc := &Service{queries: db.New(&workflowFakeDBTX{queryRowRows: []workflowFakeRow{
		{values: workflowFakeStepValues(runID, nodeID, agentID, "extract", workflowRunStatusRunning)},
		{err: errors.New("mark success failed")},
	}})}
	err = successErrSvc.copyWorkflowRunStep(context.Background(), runID, sourceStep)
	requireWorkflowHTTPStatus(t, err, http.StatusInternalServerError)

	runNodeSvc := &Service{queries: db.New(&workflowFakeDBTX{queryRowRows: []workflowFakeRow{{err: errors.New("create step failed")}}})}
	result := runNodeSvc.runWorkflowNode(
		context.Background(),
		userID,
		db.Workflow{ID: workflowID},
		db.WorkflowRun{ID: runID, UserID: userID},
		db.WorkflowNode{ID: nodeID, NodeKey: "extract", AgentID: agentID},
		map[string]interface{}{"node_key": "extract"},
		0,
	)
	if result.Err == nil || !strings.Contains(result.Err.Error(), "创建 step 失败") {
		t.Fatalf("runWorkflowNode error = %#v", result)
	}
}

func TestStartRunWorkerNoopsWithoutService(t *testing.T) {
	StartRunWorker(context.Background(), nil, RunWorkerConfig{})
}

func TestWorkflowHandlerValidationAndRoutes(t *testing.T) {
	h := NewHandler(&Service{})
	userID := uuid.NewString()
	id := uuid.NewString()
	otherID := uuid.NewString()

	for _, tc := range []struct {
		name   string
		method func(echo.Context) error
		req    *workflowHandlerRequest
		want   int
	}{
		{name: "create missing user", method: h.Create, req: &workflowHandlerRequest{method: http.MethodPost, target: "/"}, want: http.StatusUnauthorized},
		{name: "create invalid json", method: h.Create, req: &workflowHandlerRequest{method: http.MethodPost, target: "/", userID: userID, body: "{"}, want: http.StatusBadRequest},
		{name: "create validation", method: h.Create, req: &workflowHandlerRequest{method: http.MethodPost, target: "/", userID: userID, body: `{}`}, want: http.StatusUnprocessableEntity},
		{name: "list missing user", method: h.List, req: &workflowHandlerRequest{method: http.MethodGet, target: "/"}, want: http.StatusUnauthorized},
		{name: "get invalid id", method: h.Get, req: &workflowHandlerRequest{method: http.MethodGet, target: "/", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "run invalid id", method: h.Run, req: &workflowHandlerRequest{method: http.MethodPost, target: "/", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "run invalid json", method: h.Run, req: &workflowHandlerRequest{method: http.MethodPost, target: "/", userID: userID, params: map[string]string{"id": id}, body: "{"}, want: http.StatusBadRequest},
		{name: "run validation", method: h.Run, req: &workflowHandlerRequest{method: http.MethodPost, target: "/", userID: userID, params: map[string]string{"id": id}, body: `{"max_attempts":11}`}, want: http.StatusUnprocessableEntity},
		{name: "start run validation", method: h.StartRun, req: &workflowHandlerRequest{method: http.MethodPost, target: "/", userID: userID, params: map[string]string{"id": id}, body: `{"max_attempts":11}`}, want: http.StatusUnprocessableEntity},
		{name: "list runs invalid id", method: h.ListRuns, req: &workflowHandlerRequest{method: http.MethodGet, target: "/", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "get run invalid id", method: h.GetRun, req: &workflowHandlerRequest{method: http.MethodGet, target: "/", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "retry invalid id", method: h.RetryRun, req: &workflowHandlerRequest{method: http.MethodPost, target: "/", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "rerun invalid id", method: h.RerunStep, req: &workflowHandlerRequest{method: http.MethodPost, target: "/", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "rerun invalid json", method: h.RerunStep, req: &workflowHandlerRequest{method: http.MethodPost, target: "/", userID: userID, params: map[string]string{"id": id}, body: "{"}, want: http.StatusBadRequest},
		{name: "rerun validation", method: h.RerunStep, req: &workflowHandlerRequest{method: http.MethodPost, target: "/", userID: userID, params: map[string]string{"id": id}, body: `{}`}, want: http.StatusUnprocessableEntity},
		{name: "compare invalid id", method: h.CompareRuns, req: &workflowHandlerRequest{method: http.MethodGet, target: "/", userID: userID, params: map[string]string{"id": "bad", "other_id": otherID}}, want: http.StatusBadRequest},
		{name: "compare invalid other id", method: h.CompareRuns, req: &workflowHandlerRequest{method: http.MethodGet, target: "/", userID: userID, params: map[string]string{"id": id, "other_id": "bad"}}, want: http.StatusBadRequest},
		{name: "pause invalid id", method: h.PauseRun, req: &workflowHandlerRequest{method: http.MethodPost, target: "/", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "resume invalid id", method: h.ResumeRun, req: &workflowHandlerRequest{method: http.MethodPost, target: "/", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "cancel invalid id", method: h.CancelRun, req: &workflowHandlerRequest{method: http.MethodPost, target: "/", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newWorkflowTestContext(tc.req)
			requireWorkflowHTTPStatus(t, tc.method(c), tc.want)
		})
	}

	c := newWorkflowTestContext(&workflowHandlerRequest{method: http.MethodGet, target: "/", userID: userID})
	if got, err := userIDFromCtx(c); err != nil || got.String() != userID {
		t.Fatalf("userIDFromCtx valid = %s %v", got, err)
	}
	c = newWorkflowTestContext(&workflowHandlerRequest{method: http.MethodGet, target: "/", userID: "bad"})
	requireWorkflowHTTPStatus(t, userIDFromCtxOnly(c), http.StatusUnauthorized)
	c = newWorkflowTestContext(&workflowHandlerRequest{method: http.MethodGet, target: "/", params: map[string]string{"id": id}})
	if got, err := pathUUID(c); err != nil || got.String() != id {
		t.Fatalf("pathUUID valid = %s %v", got, err)
	}

	e := echo.New()
	api := e.Group("/api/v1")
	noop := func(next echo.HandlerFunc) echo.HandlerFunc { return next }
	h.RegisterProtected(api, noop)
	routes := map[string]bool{}
	for _, route := range e.Routes() {
		routes[route.Method+" "+route.Path] = true
	}
	for _, route := range []string{
		"POST /api/v1/workflows",
		"GET /api/v1/workflows",
		"GET /api/v1/workflows/:id",
		"POST /api/v1/workflows/:id/run",
		"POST /api/v1/workflows/:id/runs",
		"GET /api/v1/workflows/:id/runs",
		"GET /api/v1/workflow-runs/:id",
		"POST /api/v1/workflow-runs/:id/retry",
		"POST /api/v1/workflow-runs/:id/steps/rerun",
		"GET /api/v1/workflow-runs/:id/compare/:other_id",
		"POST /api/v1/workflow-runs/:id/pause",
		"POST /api/v1/workflow-runs/:id/resume",
		"POST /api/v1/workflow-runs/:id/cancel",
	} {
		if !routes[route] {
			t.Fatalf("missing route %s", route)
		}
	}
}

type workflowHandlerRequest struct {
	method  string
	target  string
	body    string
	userID  string
	params  map[string]string
	headers map[string]string
}

func newWorkflowTestContext(spec *workflowHandlerRequest) echo.Context {
	method := spec.method
	if method == "" {
		method = http.MethodGet
	}
	req := httptest.NewRequest(method, spec.target, strings.NewReader(spec.body))
	if spec.body != "" {
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	}
	for key, value := range spec.headers {
		req.Header.Set(key, value)
	}
	rec := httptest.NewRecorder()
	c := echo.New().NewContext(req, rec)
	if spec.userID != "" {
		c.Set(string(httpx.CtxKeyUserID), spec.userID)
	}
	if len(spec.params) > 0 {
		names := make([]string, 0, len(spec.params))
		values := make([]string, 0, len(spec.params))
		for name, value := range spec.params {
			names = append(names, name)
			values = append(values, value)
		}
		c.SetParamNames(names...)
		c.SetParamValues(values...)
	}
	return c
}

func requireWorkflowHTTPStatus(t *testing.T, err error, want int) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected HTTP error %d, got nil", want)
	}
	var he *httpx.HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("expected *httpx.HTTPError, got %T (%v)", err, err)
	}
	if he.Status != want {
		t.Fatalf("HTTP status = %d (%s), want %d", he.Status, he.Message, want)
	}
}

func userIDFromCtxOnly(c echo.Context) error {
	_, err := userIDFromCtx(c)
	return err
}

type workflowFakeDBTX struct {
	row              workflowFakeRow
	queryRowRows     []workflowFakeRow
	queryRowCalls    int
	queryResults     []workflowFakeQueryResult
	queryCalls       int
	execErr          error
	execRowsAffected int64
	queryRowSQL      string
	queryRowArgs     []any
	querySQL         string
	queryArgs        []any
	execSQL          string
	execArgs         []any
}

func (f *workflowFakeDBTX) Exec(_ context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	f.execSQL = sql
	f.execArgs = append([]any(nil), args...)
	if f.execErr != nil {
		return pgconn.CommandTag{}, f.execErr
	}
	return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", f.execRowsAffected)), nil
}

func (f *workflowFakeDBTX) Query(_ context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	f.querySQL = sql
	f.queryArgs = append([]any(nil), args...)
	result := workflowFakeQueryResult{err: errors.New("workflow fake query is not implemented")}
	if f.queryCalls < len(f.queryResults) {
		result = f.queryResults[f.queryCalls]
	}
	f.queryCalls++
	if result.err != nil {
		return nil, result.err
	}
	return &workflowFakeRows{rows: result.rows, err: result.rowsErr}, nil
}

func (f *workflowFakeDBTX) QueryRow(_ context.Context, sql string, args ...interface{}) pgx.Row {
	f.queryRowSQL = sql
	f.queryRowArgs = append([]any(nil), args...)
	if f.queryRowCalls < len(f.queryRowRows) {
		row := f.queryRowRows[f.queryRowCalls]
		f.queryRowCalls++
		return row
	}
	f.queryRowCalls++
	return f.row
}

type workflowFakeQueryResult struct {
	rows    []workflowFakeRow
	err     error
	rowsErr error
}

type workflowFakeRow struct {
	values []any
	err    error
}

func (r workflowFakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != len(r.values) {
		return errors.New("workflow fake row scan destination mismatch")
	}
	for i, value := range r.values {
		target := reflect.ValueOf(dest[i])
		if target.Kind() != reflect.Ptr || target.IsNil() {
			return errors.New("workflow fake row scan target must be a non-nil pointer")
		}
		slot := target.Elem()
		if value == nil {
			slot.Set(reflect.Zero(slot.Type()))
			continue
		}
		source := reflect.ValueOf(value)
		if source.Type().AssignableTo(slot.Type()) {
			slot.Set(source)
			continue
		}
		if source.Type().ConvertibleTo(slot.Type()) {
			slot.Set(source.Convert(slot.Type()))
			continue
		}
		return errors.New("workflow fake row scan value type mismatch")
	}
	return nil
}

type workflowFakeRows struct {
	rows    []workflowFakeRow
	current int
	closed  bool
	err     error
}

func (r *workflowFakeRows) Close() {
	r.closed = true
}

func (r *workflowFakeRows) Err() error {
	if !r.closed {
		return nil
	}
	return r.err
}

func (r *workflowFakeRows) CommandTag() pgconn.CommandTag {
	return pgconn.CommandTag{}
}

func (r *workflowFakeRows) FieldDescriptions() []pgconn.FieldDescription {
	return nil
}

func (r *workflowFakeRows) Next() bool {
	if r.current >= len(r.rows) {
		r.Close()
		return false
	}
	r.current++
	return true
}

func (r *workflowFakeRows) Scan(dest ...any) error {
	if r.current == 0 || r.current > len(r.rows) {
		return errors.New("workflow fake rows scan without current row")
	}
	if err := r.rows[r.current-1].Scan(dest...); err != nil {
		r.Close()
		return err
	}
	return nil
}

func (r *workflowFakeRows) Values() ([]any, error) {
	if r.current == 0 || r.current > len(r.rows) {
		return nil, errors.New("workflow fake rows values without current row")
	}
	return append([]any(nil), r.rows[r.current-1].values...), nil
}

func (r *workflowFakeRows) RawValues() [][]byte {
	return nil
}

func (r *workflowFakeRows) Conn() *pgx.Conn {
	return nil
}

func workflowFakeWorkflowValues(workflowID, userID uuid.UUID) []any {
	now := time.Date(2026, 6, 22, 9, 30, 0, 0, time.UTC)
	return []any{
		workflowID,
		userID,
		"Workflow",
		"test workflow",
		"active",
		[]byte(`[]`),
		now,
		now,
	}
}

func workflowFakeNodeValues(workflowID, agentID uuid.UUID, key string, position int32) []any {
	now := time.Date(2026, 6, 22, 9, 30, 0, 0, time.UTC)
	return []any{
		uuid.New(),
		workflowID,
		key,
		"agent",
		agentID,
		"Node " + key,
		[]byte(`{}`),
		position,
		now,
	}
}

func workflowFakeStepValues(runID, nodeID, agentID uuid.UUID, key, status string) []any {
	now := time.Date(2026, 6, 22, 9, 30, 0, 0, time.UTC)
	return []any{
		uuid.New(),
		runID,
		nodeID,
		key,
		agentID,
		nil,
		status,
		[]byte(`{"node_key":"` + key + `"}`),
		[]byte(`{}`),
		nil,
		int32(0),
		now,
		nil,
		now,
		now,
	}
}

func workflowFakeRunValues(runID, workflowID, userID uuid.UUID, status string, errorMessage *string) []any {
	now := time.Date(2026, 6, 22, 9, 30, 0, 0, time.UTC)
	finished := now.Add(time.Second)
	return []any{
		runID,
		workflowID,
		userID,
		status,
		[]byte(`{"prompt":"a2a"}`),
		[]byte(`{}`),
		errorMessage,
		now,
		&finished,
		now,
		now,
		int32(1),
		int32(3),
		nil,
		nil,
		nil,
	}
}
