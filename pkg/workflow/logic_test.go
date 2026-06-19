package workflow

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	db "github.com/kinzhi/openlinker-core/pkg/db/generated"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

func TestWorkflowGraphAndEdgeHelpers(t *testing.T) {
	if got := normalizeRunStatus(" Success "); got != "success" {
		t.Fatalf("normalizeRunStatus = %q", got)
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

	stepOutput, err := workflowStepOutputMap(db.WorkflowRunStep{NodeKey: "extract", Output: []byte(`{"ok":true}`)})
	if err != nil || stepOutput["ok"] != true {
		t.Fatalf("workflowStepOutputMap = %#v %v", stepOutput, err)
	}
	if _, err := workflowStepOutputMap(db.WorkflowRunStep{NodeKey: "extract", Output: []byte(`bad`)}); err == nil {
		t.Fatalf("invalid step output should fail")
	}
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
