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

	db "github.com/kinzhi/openlinker-core/pkg/db/generated"
)

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
