package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

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
