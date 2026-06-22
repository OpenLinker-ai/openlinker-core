package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kinzhi/openlinker-core/pkg/config"
	db "github.com/kinzhi/openlinker-core/pkg/db/generated"
)

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

	svc := &Service{
		cfg:        &config.Config{APIURL: "https://api.example.test"},
		httpClient: server.Client(),
	}
	agent := &db.Agent{
		EndpointURL:        server.URL,
		EndpointAuthHeader: &token,
		ConnectionMode:     connectionModeDirectHTTP,
	}

	output, events, agentErr, callErr := svc.callAgentEndpoint(context.Background(), agent, runID, userID, &RunRequest{
		Input:    map[string]interface{}{"q": "hello"},
		Metadata: map[string]interface{}{"trace_id": "trace-direct"},
	}, nil)

	require.NoError(t, callErr)
	require.Nil(t, agentErr)
	assert.Equal(t, "direct ok", output["answer"])
	require.Len(t, events, 1)
	assert.Equal(t, "run.message.delta", events[0].EventType)
	assert.Equal(t, "direct progress", events[0].Payload["text"])

	assert.Equal(t, token, capturedHeader.Get("X-OpenLinker-Token"))
	assert.Equal(t, runID.String(), capturedHeader.Get("X-OpenLinker-Run-Id"))
	assert.Equal(t, userID.String(), capturedHeader.Get("X-OpenLinker-User-Id"))
	assert.Equal(t, "application/json", capturedHeader.Get("Accept"))
	assert.Equal(t, "OpenLinker/1.0", capturedHeader.Get("User-Agent"))

	assert.Equal(t, runID.String(), captured.RunID)
	assert.Equal(t, "hello", captured.Input["q"])
	assert.Equal(t, "trace-direct", captured.Metadata["trace_id"])
	require.NotNil(t, captured.A2A)
	assert.Equal(t, runID.String(), captured.A2A.CurrentRunID)
	assert.Equal(t, "https://api.example.test/api/v1/agent-runtime/call-agent", captured.A2A.CallAgentEndpoint)
	assert.Equal(t, http.MethodPost, captured.A2A.CallAgentMethod)
	assert.Equal(t, "ol_live", captured.A2A.RuntimeTokenType)
	assert.Equal(t, []string{"agent:call"}, captured.A2A.RuntimeScopes)
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

	svc := &Service{httpClient: server.Client()}
	toolName := "analyze_contract"
	agent := &db.Agent{
		EndpointURL:        server.URL,
		EndpointAuthHeader: &token,
		ConnectionMode:     connectionModeMCPServer,
		MCPToolName:        &toolName,
	}
	runID := uuid.New()

	output, events, agentErr, callErr := svc.callMCPServer(context.Background(), agent, runID, uuid.New(), &RunRequest{
		Input: map[string]interface{}{"text": "hello"},
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
}

func TestNormalizeMCPResultPrefersStructuredContent(t *testing.T) {
	out := normalizeMCPResult(map[string]interface{}{
		"structuredContent": map[string]interface{}{"summary": "done"},
		"content":           []interface{}{map[string]interface{}{"type": "text", "text": "done"}},
	})
	assert.Equal(t, "done", out["summary"])
}
