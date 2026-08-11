package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

const thirdPartyTransportTestMasterSecret = "third-party-transport-test-master-secret"

func newThirdPartyTransportTestService(client *http.Client) *Service {
	svc := NewService(nil, &config.Config{
		APIURL:                 "https://api.example.test",
		RuntimePKIMasterSecret: thirdPartyTransportTestMasterSecret,
	})
	svc.SetHTTPClient(client)
	return svc
}

func TestCallAgentEndpointSendsDirectHTTPEnvelope(t *testing.T) {
	token := "direct-secret"
	runID := uuid.New()
	userID := uuid.New()
	var captured AgentRequest
	var capturedHeader http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		capturedHeader = r.Header.Clone()
		require.NoError(t, json.NewDecoder(r.Body).Decode(&captured))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(AgentResponse{
			Output: map[string]interface{}{"answer": "direct ok"},
			Events: []AgentEvent{{
				EventType: "run.message.delta",
				Payload:   map[string]interface{}{"text": "direct progress"},
			}},
		})
	}))
	defer server.Close()

	svc := newThirdPartyTransportTestService(server.Client())
	agent := &db.Agent{
		ID:                 uuid.New(),
		EndpointURL:        server.URL,
		EndpointAuthHeader: &token,
		ConnectionMode:     connectionModeDirectHTTP,
	}

	requestMetadata := map[string]interface{}{
		"client_version":                "1.2.3",
		"trace_id":                      "trace-direct",
		"seller_user_id":                uuid.NewString(),
		"Caller_Service_ID":             "openlinker-cloud",
		" external_request_id ":         uuid.NewString(),
		"X-OpenLinker-User-Id":          uuid.NewString(),
		"_openlinker_runtime_authority": map[string]interface{}{"principal_scope_id": "spoofed"},
		"principal_scope_id":            "spoofed",
		"caller_payload":                map[string]interface{}{"seller_user_id": "caller-owned-nested-value"},
	}
	output, events, agentErr, callErr := svc.callAgentEndpoint(context.Background(), agent, runID, userID, &RunRequest{
		Input:    map[string]interface{}{"q": "hello"},
		Metadata: requestMetadata,
	}, nil)

	require.NoError(t, callErr)
	require.Nil(t, agentErr)
	assert.Equal(t, "direct ok", output["answer"])
	require.Len(t, events, 2)
	assert.Equal(t, "run.status.changed", events[0].EventType)
	assert.Equal(t, "endpoint_response_received", events[0].Payload["status"])
	assert.Equal(t, "output_object", events[0].Payload["response_shape"])
	assert.Equal(t, []string{"answer"}, events[0].Payload["output_keys"])
	assert.Equal(t, "run.message.delta", events[1].EventType)
	assert.Equal(t, "direct progress", events[1].Payload["text"])

	assert.Equal(t, token, capturedHeader.Get("X-OpenLinker-Token"))
	assert.Equal(t, runID.String(), capturedHeader.Get("X-OpenLinker-Run-Id"))
	assert.Empty(t, capturedHeader.Get("X-OpenLinker-User-Id"))
	assert.Equal(t, "application/json", capturedHeader.Get("Accept"))
	assert.Equal(t, "OpenLinker/1.0", capturedHeader.Get("User-Agent"))

	assert.Equal(t, runID.String(), captured.RunID)
	assert.Equal(t, "hello", captured.Input["q"])
	expectedScopeID, err := runtimePrincipalScopeID(svc.runtimePrincipalScopeKey, userID, agent.ID)
	require.NoError(t, err)
	assert.Equal(t, expectedScopeID, captured.Metadata["principal_scope_id"])
	assert.Equal(t, "1.2.3", captured.Metadata["client_version"])
	assert.Equal(t, map[string]interface{}{"seller_user_id": "caller-owned-nested-value"}, captured.Metadata["caller_payload"])
	for _, key := range []string{
		"trace_id", "seller_user_id", "Caller_Service_ID", " external_request_id ",
		"X-OpenLinker-User-Id", "_openlinker_runtime_authority",
	} {
		assert.NotContains(t, captured.Metadata, key)
	}
	assert.Equal(t, "trace-direct", requestMetadata["trace_id"], "outbound projection must not mutate caller metadata")
	require.NotNil(t, captured.A2A)
	assert.Equal(t, runID.String(), captured.A2A.CurrentRunID)
}

func TestCallAgentEndpointPreservesTopLevelJSONResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"json":{"text":"hello"},"url":"https://agent.example/run","headers":{"X-Test":"ok"}}`))
	}))
	defer server.Close()

	svc := newThirdPartyTransportTestService(server.Client())
	agent := &db.Agent{
		ID:             uuid.New(),
		EndpointURL:    server.URL,
		ConnectionMode: connectionModeDirectHTTP,
	}

	output, events, agentErr, callErr := svc.callAgentEndpoint(context.Background(), agent, uuid.New(), uuid.New(), &RunRequest{
		Input: map[string]interface{}{"text": "hello"},
	}, nil)

	require.NoError(t, callErr)
	require.Nil(t, agentErr)
	assert.Equal(t, "https://agent.example/run", output["url"])
	jsonBody, ok := output["json"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "hello", jsonBody["text"])
	require.Len(t, events, 1)
	assert.Equal(t, "run.status.changed", events[0].EventType)
	assert.Equal(t, "top_level_object", events[0].Payload["response_shape"])
	assert.Contains(t, events[0].Payload["output_keys"], "json")
}

func TestCallMCPServerUsesToolsCall(t *testing.T) {
	token := "Bearer mcp-secret"
	var captured map[string]interface{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, token, r.Header.Get("Authorization"))
		require.NoError(t, json.NewDecoder(r.Body).Decode(&captured))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":{"structuredContent":{"answer":"ok"}}}`))
	}))
	defer server.Close()

	svc := newThirdPartyTransportTestService(server.Client())
	toolName := "analyze_contract"
	agent := &db.Agent{
		ID:                 uuid.New(),
		EndpointURL:        server.URL,
		EndpointAuthHeader: &token,
		ConnectionMode:     connectionModeMCPServer,
		MCPToolName:        &toolName,
	}
	runID := uuid.New()

	userID := uuid.New()
	output, events, agentErr, callErr := svc.callMCPServer(context.Background(), agent, runID, userID, &RunRequest{
		Input: map[string]interface{}{"text": "hello"},
		Metadata: map[string]interface{}{
			"client_version": "1.2.3",
			"user_id":        uuid.NewString(),
			"Seller_User_ID": uuid.NewString(),
			"trace_id":       "internal-trace",
		},
	}, nil)

	require.NoError(t, callErr)
	require.Nil(t, agentErr)
	require.Empty(t, events)
	assert.Equal(t, "ok", output["answer"])
	assert.Equal(t, "2.0", captured["jsonrpc"])
	assert.Equal(t, "tools/call", captured["method"])

	params, ok := captured["params"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, toolName, params["name"])
	args, ok := params["arguments"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "hello", args["text"])
	metadata, ok := params["_meta"].(map[string]interface{})
	if !ok {
		metadata, ok = params["metadata"].(map[string]interface{})
	}
	require.True(t, ok)
	expectedScopeID, err := runtimePrincipalScopeID(svc.runtimePrincipalScopeKey, userID, agent.ID)
	require.NoError(t, err)
	assert.Equal(t, expectedScopeID, metadata["principal_scope_id"])
	assert.Equal(t, runID.String(), metadata["run_id"])
	assert.Equal(t, "openlinker", metadata["platform"])
	assert.Equal(t, "1.2.3", metadata["client_version"])
	assert.NotContains(t, metadata, "user_id")
	assert.NotContains(t, metadata, "Seller_User_ID")
	assert.NotContains(t, metadata, "trace_id")
}

func TestThirdPartyTransportsFailClosedWithoutPrincipalScopeKey(t *testing.T) {
	tests := []struct {
		name string
		call func(*Service, *db.Agent) error
	}{
		{
			name: "direct http",
			call: func(svc *Service, agent *db.Agent) error {
				_, _, _, err := svc.callAgentEndpoint(context.Background(), agent, uuid.New(), uuid.New(), &RunRequest{Input: map[string]interface{}{}}, nil)
				return err
			},
		},
		{
			name: "mcp server",
			call: func(svc *Service, agent *db.Agent) error {
				toolName := "safe_tool"
				agent.MCPToolName = &toolName
				_, _, _, err := svc.callMCPServer(context.Background(), agent, uuid.New(), uuid.New(), &RunRequest{Input: map[string]interface{}{}}, nil)
				return err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls++ }))
			defer server.Close()
			svc := &Service{httpClient: server.Client()}
			agent := &db.Agent{ID: uuid.New(), EndpointURL: server.URL}
			err := tt.call(svc, agent)
			require.ErrorContains(t, err, "principal scope")
			assert.Zero(t, calls)
		})
	}
}

func TestCallAgentEndpointRejectsOversizedResponseBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", maxAgentResponseBodyBytes+1)))
	}))
	defer server.Close()

	svc := newThirdPartyTransportTestService(server.Client())
	agent := &db.Agent{
		ID:             uuid.New(),
		EndpointURL:    server.URL,
		ConnectionMode: connectionModeDirectHTTP,
	}

	_, _, agentErr, callErr := svc.callAgentEndpoint(context.Background(), agent, uuid.New(), uuid.New(), &RunRequest{
		Input: map[string]interface{}{"text": "hello"},
	}, nil)

	require.Error(t, callErr)
	require.Nil(t, agentErr)
	assert.Contains(t, callErr.Error(), "response body exceeds")
}

func TestCallMCPServerRejectsOversizedResponseBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", maxAgentResponseBodyBytes+1)))
	}))
	defer server.Close()

	svc := newThirdPartyTransportTestService(server.Client())
	toolName := "analyze_contract"
	agent := &db.Agent{
		ID:             uuid.New(),
		EndpointURL:    server.URL,
		ConnectionMode: connectionModeMCPServer,
		MCPToolName:    &toolName,
	}

	_, _, agentErr, callErr := svc.callMCPServer(context.Background(), agent, uuid.New(), uuid.New(), &RunRequest{
		Input: map[string]interface{}{"text": "hello"},
	}, nil)

	require.Error(t, callErr)
	require.Nil(t, agentErr)
	assert.Contains(t, callErr.Error(), "response body exceeds")
}

func TestNormalizeMCPResultPrefersStructuredContent(t *testing.T) {
	out := normalizeMCPResult(map[string]interface{}{
		"structuredContent": map[string]interface{}{"summary": "done"},
		"content":           []interface{}{map[string]interface{}{"type": "text", "text": "done"}},
	})
	assert.Equal(t, "done", out["summary"])
}
