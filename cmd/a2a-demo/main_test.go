package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunMainCompletesWithoutServe(t *testing.T) {
	api := newRunValidationServer(t,
		`{"run_id":"parent-run","status":"success"}`,
		`{"items":[{"child_run_id":"worker-run","status":"success"},{"child_run_id":"reviewer-run","status":"success"}]}`,
		`{"events":[{"event_type":"run.child.created"},{"event_type":"run.child.created"},{"event_type":"run.child.completed"},{"event_type":"run.child.completed"}]}`,
	)
	defer api.Close()

	var stdout, stderr bytes.Buffer
	code := runMain([]string{"-api", api.URL}, &stdout, &stderr, func() {
		t.Fatal("non-serve run should not wait for a stop signal")
	})

	require.Equal(t, 0, code)
	require.Empty(t, stderr.String())
	require.Empty(t, stdout.String())
}

func TestRunMainHandlesFlagAndRunErrors(t *testing.T) {
	var stderr bytes.Buffer
	code := runMain([]string{"-unknown"}, &bytes.Buffer{}, &stderr, func() {})
	require.Equal(t, 2, code)
	require.Contains(t, stderr.String(), "flag provided but not defined")

	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "register failed", http.StatusBadGateway)
	}))
	defer api.Close()
	stderr.Reset()
	code = runMain([]string{"-api", api.URL}, &bytes.Buffer{}, &stderr, func() {})
	require.Equal(t, 1, code)
	require.Contains(t, stderr.String(), "A2A demo failed: register user")
}

func TestPrintServeInstructions(t *testing.T) {
	var out bytes.Buffer
	printServeInstructions(&out, &demoResult{
		ParentAgent: agentResponse{Slug: "demo-planner"},
		TaskID:      "task-1",
		ParentRunID: "parent-run",
	})

	require.Contains(t, out.String(), "Local demo endpoints are online until interrupted.")
	require.Contains(t, out.String(), "playground: /playground/demo-planner")
	require.Contains(t, out.String(), "task: /tasks/task-1")
	require.Contains(t, out.String(), "a2a trace: /a2a?run_id=parent-run")
}

func TestClientDoSendsJSONAuthAndDecodesResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "/api/v1/test", r.URL.Path)
		require.Equal(t, "Bearer demo-token", r.Header.Get("Authorization"))
		require.Equal(t, "application/json", r.Header.Get("Content-Type"))

		var body map[string]string
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		require.Equal(t, "ping", body["message"])

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jwt":"jwt-token"}`))
	}))
	defer srv.Close()

	api := &client{baseURL: srv.URL, token: "demo-token", http: srv.Client()}
	var out authResponse
	require.NoError(t, api.do(http.MethodPost, "/api/v1/test", map[string]string{"message": "ping"}, &out))
	require.Equal(t, "jwt-token", out.JWT)
}

func TestClientDoHandlesNoBodyAndHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/empty":
			w.WriteHeader(http.StatusNoContent)
		case "/bad":
			http.Error(w, "broken", http.StatusBadGateway)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	api := &client{baseURL: srv.URL, http: srv.Client()}
	require.NoError(t, api.do(http.MethodGet, "/empty", nil, nil))

	err := api.do(http.MethodGet, "/bad", nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "GET /bad returned 502: broken")
}

func TestRegisterAgentBuildsExpectedPayload(t *testing.T) {
	var req map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v1/agent-registration/agents", r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"agent":{"id":"agent-1","slug":"demo"},"runtime_token":{"plaintext_token":"rt_live_test"}}`))
	}))
	defer srv.Close()

	api := &client{baseURL: srv.URL, http: srv.Client()}
	resp, err := registerAgent(api, "boot-token", "demo", "Demo Agent", "https://agent.example/run", []string{"ai/demo"})
	require.NoError(t, err)
	require.Equal(t, "agent-1", resp.Agent.ID)
	require.Equal(t, "rt_live_test", resp.RuntimeToken.PlaintextToken)
	require.Equal(t, "boot-token", req["bootstrap_token"])
	require.Equal(t, "demo", req["slug"])
	require.Equal(t, "https://agent.example/run", req["endpoint_url"])
	require.Equal(t, []any{"ai/demo"}, req["skill_ids"])
}

func TestPublishTaskChoosesRecommendedPlanner(t *testing.T) {
	chooseCalled := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/tasks/recommend":
			require.Equal(t, http.MethodPost, r.Method)
			_, _ = w.Write([]byte(`{"task_id":"task-1","recommendations":[{"agent":{"id":"planner-1","slug":"planner"}}]}`))
		case "/api/v1/tasks/task-1/choose":
			require.Equal(t, http.MethodPost, r.Method)
			var body map[string]string
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Equal(t, "planner-1", body["agent_id"])
			chooseCalled = true
			_, _ = w.Write([]byte(`{}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	result, err := publishTask(&client{baseURL: srv.URL, http: srv.Client()}, "planner-1")
	require.NoError(t, err)
	require.Equal(t, "task-1", result.TaskID)
	require.True(t, result.PlannerChosen)
	require.True(t, chooseCalled)
}

func TestPublishTaskContinuesWhenPlannerMissingAndRejectsEmptyTask(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		wantResult *taskDemoResult
		wantErr    string
	}{
		{
			name:       "planner not recommended",
			body:       `{"task_id":"task-2","recommendations":[{"agent":{"id":"other","slug":"other"}}]}`,
			wantResult: &taskDemoResult{TaskID: "task-2"},
		},
		{
			name:    "task id missing",
			body:    `{"recommendations":[]}`,
			wantErr: "published task did not return task_id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "/api/v1/tasks/recommend", r.URL.Path)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(tt.body))
			}))
			defer srv.Close()

			result, err := publishTask(&client{baseURL: srv.URL, http: srv.Client()}, "planner-1")
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantResult, result)
		})
	}
}

func TestRunCompletesLocalA2AFlow(t *testing.T) {
	cfg := &endpointConfig{}
	var registered []map[string]any
	chooseCalled := false

	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/auth/register":
			require.Equal(t, http.MethodPost, r.Method)
			var body map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Contains(t, body["email"], "a2a-demo-")
			require.Equal(t, "local-demo-pass-123", body["password"])
			_, _ = w.Write([]byte(`{"jwt":"jwt-token"}`))
		case "/api/v1/me/become-creator":
			require.Equal(t, "Bearer jwt-token", r.Header.Get("Authorization"))
			_, _ = w.Write([]byte(`{}`))
		case "/api/v1/creator/agent-registration-tokens":
			require.Equal(t, "Bearer jwt-token", r.Header.Get("Authorization"))
			var body map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Equal(t, "local a2a demo", body["label"])
			require.Equal(t, float64(3), body["max_agents"])
			_, _ = w.Write([]byte(`{"plaintext_token":"bootstrap-token"}`))
		case "/api/v1/agent-registration/agents":
			require.Equal(t, "Bearer jwt-token", r.Header.Get("Authorization"))
			var body map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			registered = append(registered, body)
			require.Equal(t, "bootstrap-token", body["bootstrap_token"])
			require.Equal(t, "public", body["visibility"])

			switch {
			case strings.HasPrefix(body["slug"].(string), "demo-worker-"):
				_, _ = w.Write([]byte(`{"agent":{"id":"worker-agent","slug":"demo-worker"},"runtime_token":{"plaintext_token":"worker-runtime"}}`))
			case strings.HasPrefix(body["slug"].(string), "demo-reviewer-"):
				_, _ = w.Write([]byte(`{"agent":{"id":"reviewer-agent","slug":"demo-reviewer"},"runtime_token":{"plaintext_token":"reviewer-runtime"}}`))
			case strings.HasPrefix(body["slug"].(string), "demo-planner-"):
				_, _ = w.Write([]byte(`{"agent":{"id":"planner-agent","slug":"demo-planner"},"runtime_token":{"plaintext_token":"planner-runtime"}}`))
			default:
				t.Fatalf("unexpected registration slug: %v", body["slug"])
			}
		case "/api/v1/tasks/recommend":
			require.Equal(t, "Bearer jwt-token", r.Header.Get("Authorization"))
			var body map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Contains(t, body["mcp_tools"], "run_agent")
			_, _ = w.Write([]byte(`{"task_id":"task-1","recommendations":[{"agent":{"id":"planner-agent","slug":"demo-planner"}}]}`))
		case "/api/v1/tasks/task-1/choose":
			require.Equal(t, "Bearer jwt-token", r.Header.Get("Authorization"))
			var body map[string]string
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Equal(t, "planner-agent", body["agent_id"])
			chooseCalled = true
			_, _ = w.Write([]byte(`{}`))
		case "/api/v1/run":
			require.Equal(t, "Bearer jwt-token", r.Header.Get("Authorization"))
			var body map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Equal(t, "planner-agent", body["agent_id"])
			_, _ = w.Write([]byte(`{"run_id":"parent-run","status":"success"}`))
		case "/api/v1/runs/parent-run/children":
			require.Equal(t, "Bearer jwt-token", r.Header.Get("Authorization"))
			_, _ = w.Write([]byte(`{"items":[{"child_run_id":"worker-run","status":"success"},{"child_run_id":"reviewer-run","status":"success"}]}`))
		case "/api/v1/runs/parent-run/events":
			require.Equal(t, "Bearer jwt-token", r.Header.Get("Authorization"))
			_, _ = w.Write([]byte(`{"events":[{"event_type":"run.child.created"},{"event_type":"run.child.created"},{"event_type":"run.child.completed"},{"event_type":"run.child.completed"}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer api.Close()
	cfg.apiURL = api.URL

	result, err := run(&client{baseURL: api.URL, http: api.Client()}, cfg, "http://caller/run", "http://worker/run", "http://reviewer/run")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Contains(t, result.Email, "a2a-demo-")
	require.Equal(t, agentResponse{ID: "planner-agent", Slug: "demo-planner"}, result.ParentAgent)
	require.Equal(t, agentResponse{ID: "worker-agent", Slug: "demo-worker"}, result.WorkerAgent)
	require.Equal(t, agentResponse{ID: "reviewer-agent", Slug: "demo-reviewer"}, result.ReviewAgent)
	require.Equal(t, "task-1", result.TaskID)
	require.Equal(t, "parent-run", result.ParentRunID)
	require.Equal(t, []string{"worker-run", "reviewer-run"}, result.ChildRunIDs)
	require.True(t, chooseCalled)
	require.Len(t, registered, 3)

	cfg.mu.RLock()
	require.Equal(t, "planner-runtime", cfg.runtimeToken)
	require.Equal(t, "worker-agent", cfg.workerID)
	require.Equal(t, "reviewer-agent", cfg.reviewerID)
	cfg.mu.RUnlock()
}

func TestRunValidatesParentChildrenAndTrace(t *testing.T) {
	tests := []struct {
		name       string
		parentBody string
		childBody  string
		eventsBody string
		wantErr    string
	}{
		{
			name:       "parent failed",
			parentBody: `{"run_id":"parent-run","status":"failed"}`,
			wantErr:    `parent run ended with status "failed"`,
		},
		{
			name:       "missing child",
			parentBody: `{"run_id":"parent-run","status":"success"}`,
			childBody:  `{"items":[{"child_run_id":"worker-run","status":"success"}]}`,
			wantErr:    "expected two child runs",
		},
		{
			name:       "child failed",
			parentBody: `{"run_id":"parent-run","status":"success"}`,
			childBody:  `{"items":[{"child_run_id":"worker-run","status":"success"},{"child_run_id":"reviewer-run","status":"failed"}]}`,
			wantErr:    "expected successful child runs",
		},
		{
			name:       "trace missing lifecycle",
			parentBody: `{"run_id":"parent-run","status":"success"}`,
			childBody:  `{"items":[{"child_run_id":"worker-run","status":"success"},{"child_run_id":"reviewer-run","status":"success"}]}`,
			eventsBody: `{"events":[{"event_type":"run.child.created"}]}`,
			wantErr:    "parent trace is missing delegation lifecycle events",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := newRunValidationServer(t, tt.parentBody, tt.childBody, tt.eventsBody)
			defer api.Close()
			cfg := &endpointConfig{apiURL: api.URL}

			_, err := run(&client{baseURL: api.URL, http: api.Client()}, cfg, "http://caller/run", "http://worker/run", "http://reviewer/run")
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestDemoEndpointsReturnOutput(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
		want    string
	}{
		{name: "worker", handler: workerEndpoint, want: "worker generated the report draft"},
		{name: "reviewer", handler: reviewerEndpoint, want: "reviewer summarized and checked the draft"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			tt.handler(rec, httptest.NewRequest(http.MethodPost, "/", nil))
			require.Equal(t, http.StatusOK, rec.Code)

			var body struct {
				Output map[string]string `json:"output"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
			require.Equal(t, tt.want, body.Output["result"])
		})
	}
}

func TestCallerEndpointDelegatesToWorkerAndReviewer(t *testing.T) {
	var calls []map[string]any
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer runtime-token", r.Header.Get("Authorization"))
		require.Equal(t, "/api/v1/agent-runtime/call-agent", r.URL.Path)
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		calls = append(calls, body)

		w.Header().Set("Content-Type", "application/json")
		if len(calls) == 1 {
			_, _ = w.Write([]byte(`{"run_id":"worker-run","status":"success"}`))
			return
		}
		_, _ = w.Write([]byte(`{"run_id":"reviewer-run","status":"success"}`))
	}))
	defer api.Close()

	cfg := &endpointConfig{
		apiURL:       api.URL,
		runtimeToken: "runtime-token",
		workerID:     "worker-agent",
		reviewerID:   "reviewer-agent",
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"run_id":"parent-run","input":{"task":"draft"}}`))

	cfg.callerEndpoint(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Len(t, calls, 2)
	require.Equal(t, "parent-run", calls[0]["parent_run_id"])
	require.Equal(t, "worker-agent", calls[0]["target_agent_id"])
	require.Equal(t, "reviewer-agent", calls[1]["target_agent_id"])

	reviewerInput := calls[1]["input"].(map[string]any)
	require.Equal(t, "worker-run", reviewerInput["worker_run_id"])

	var body struct {
		Output map[string]any `json:"output"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
	require.Equal(t, "planner completed after two delegations", body.Output["result"])
	require.Equal(t, "reviewer-run", body.Output["reviewer_run_id"])
}

func TestCallerEndpointReportsDecodeAndDelegationFailures(t *testing.T) {
	cfg := &endpointConfig{apiURL: "http://127.0.0.1:1"}
	badJSON := httptest.NewRecorder()
	cfg.callerEndpoint(badJSON, httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{")))
	require.Equal(t, http.StatusBadRequest, badJSON.Code)

	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "upstream failed", http.StatusBadGateway)
	}))
	defer api.Close()
	cfg = &endpointConfig{apiURL: api.URL, runtimeToken: "runtime-token", workerID: "worker-agent", reviewerID: "reviewer-agent"}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"run_id":"parent-run","input":{"task":"draft"}}`))

	cfg.callerEndpoint(rec, req)
	require.Equal(t, http.StatusBadGateway, rec.Code)
	require.Contains(t, rec.Body.String(), "DELEGATION_FAILED")
	require.Contains(t, rec.Body.String(), "returned 502")
}

func TestCountEvent(t *testing.T) {
	payload := eventsResponse{}
	payload.Events = append(payload.Events,
		struct {
			EventType string `json:"event_type"`
		}{EventType: "run.child.created"},
		struct {
			EventType string `json:"event_type"`
		}{EventType: "run.child.completed"},
		struct {
			EventType string `json:"event_type"`
		}{EventType: "run.child.created"},
	)

	require.Equal(t, 2, countEvent(payload, "run.child.created"))
	require.Equal(t, 0, countEvent(payload, "missing"))
}

func newRunValidationServer(t *testing.T, parentBody, childBody, eventsBody string) *httptest.Server {
	t.Helper()
	registrationIndex := 0
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/auth/register":
			_, _ = w.Write([]byte(`{"jwt":"jwt-token"}`))
		case "/api/v1/me/become-creator":
			_, _ = w.Write([]byte(`{}`))
		case "/api/v1/creator/agent-registration-tokens":
			_, _ = w.Write([]byte(`{"plaintext_token":"bootstrap-token"}`))
		case "/api/v1/agent-registration/agents":
			responses := []string{
				`{"agent":{"id":"worker-agent","slug":"demo-worker"},"runtime_token":{"plaintext_token":"worker-runtime"}}`,
				`{"agent":{"id":"reviewer-agent","slug":"demo-reviewer"},"runtime_token":{"plaintext_token":"reviewer-runtime"}}`,
				`{"agent":{"id":"planner-agent","slug":"demo-planner"},"runtime_token":{"plaintext_token":"planner-runtime"}}`,
			}
			require.Less(t, registrationIndex, len(responses))
			_, _ = w.Write([]byte(responses[registrationIndex]))
			registrationIndex++
		case "/api/v1/tasks/recommend":
			_, _ = w.Write([]byte(`{"task_id":"task-1","recommendations":[{"agent":{"id":"planner-agent","slug":"demo-planner"}}]}`))
		case "/api/v1/tasks/task-1/choose":
			_, _ = w.Write([]byte(`{}`))
		case "/api/v1/run":
			_, _ = w.Write([]byte(parentBody))
		case "/api/v1/runs/parent-run/children":
			_, _ = w.Write([]byte(childBody))
		case "/api/v1/runs/parent-run/events":
			_, _ = w.Write([]byte(eventsBody))
		default:
			http.NotFound(w, r)
		}
	}))
}
