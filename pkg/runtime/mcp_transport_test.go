package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/endpointurl"
	"github.com/OpenLinker-ai/openlinker-core/pkg/mcpclient"
)

// mcpParamsRecorder serves the three MCP transports and records the
// tools/call params and headers it received.
type mcpParamsRecorder struct {
	mu      sync.Mutex
	params  json.RawMessage
	headers http.Header
	stream  chan string
}

func (r *mcpParamsRecorder) record(raw json.RawMessage, header http.Header) {
	r.mu.Lock()
	r.params = raw
	r.headers = header.Clone()
	r.mu.Unlock()
}

func (r *mcpParamsRecorder) handler() http.Handler {
	r.stream = make(chan string, 16)
	result := map[string]interface{}{"structuredContent": map[string]interface{}{"answer": "ok"}}
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodGet && req.URL.Path == "/sse" {
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "event: endpoint\ndata: /messages\n\n")
			w.(http.Flusher).Flush()
			for {
				select {
				case chunk := <-r.stream:
					fmt.Fprint(w, chunk)
					w.(http.Flusher).Flush()
				case <-req.Context().Done():
					return
				}
			}
		}
		if req.Method == http.MethodDelete {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		var m struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		_ = json.NewDecoder(req.Body).Decode(&m)
		var response interface{}
		switch m.Method {
		case "initialize":
			response = map[string]interface{}{"jsonrpc": "2.0", "id": m.ID, "result": map[string]interface{}{"protocolVersion": "2025-06-18"}}
		case "tools/call":
			r.record(m.Params, req.Header)
			response = map[string]interface{}{"jsonrpc": "2.0", "id": m.ID, "result": result}
		default:
			w.WriteHeader(http.StatusAccepted)
			return
		}
		raw, _ := json.Marshal(response)
		if req.URL.Path == "/messages" {
			w.WriteHeader(http.StatusAccepted)
			r.stream <- "event: message\ndata: " + string(raw) + "\n\n"
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(raw)
	})
}

// The tools/call arguments and Core's authoritative _meta must not depend on
// the transport.
func TestCallMCPServerSendsIdenticalParamsOnEveryTransport(t *testing.T) {
	recorder := &mcpParamsRecorder{}
	server := httptest.NewServer(recorder.handler())
	defer server.Close()
	svc := newLocalEndpointRuntimeService()
	token := "Bearer mcp-secret"
	toolName := "analyze"
	agentID, runID, userID := uuid.New(), uuid.New(), uuid.New()

	var baseline map[string]interface{}
	for _, tc := range []struct{ transport, endpoint string }{
		{"", server.URL + "/mcp"},
		{mcpclient.TransportLegacyJSON, server.URL + "/mcp"},
		{mcpclient.TransportStreamableHTTP, server.URL + "/mcp"},
		{mcpclient.TransportLegacySSE, server.URL + "/sse"},
	} {
		agent := &db.Agent{
			ID: agentID, EndpointURL: tc.endpoint, EndpointAuthHeader: &token,
			ConnectionMode: connectionModeMCPServer, MCPToolName: &toolName, MCPTransport: tc.transport,
		}
		output, _, agentErr, callErr := svc.callAgent(context.Background(), agent, runID, userID, &RunRequest{
			Input:    map[string]interface{}{"text": "hello"},
			Metadata: map[string]interface{}{"client_version": "1.2.3"},
		}, nil)
		require.NoError(t, callErr, tc.transport)
		require.Nil(t, agentErr, tc.transport)
		require.Equal(t, "ok", output["answer"], tc.transport)

		recorder.mu.Lock()
		var params map[string]interface{}
		require.NoError(t, json.Unmarshal(recorder.params, &params))
		headers := recorder.headers
		recorder.mu.Unlock()
		require.Equal(t, token, headers.Get("Authorization"), tc.transport)
		require.Equal(t, runID.String(), headers.Get("X-OpenLinker-Run-Id"), tc.transport)
		if baseline == nil {
			baseline = params
			continue
		}
		require.Equal(t, baseline, params, "transport %q changed tools/call params", tc.transport)
	}
	meta := baseline["_meta"].(map[string]interface{})
	require.Equal(t, runID.String(), meta["run_id"])
	require.Equal(t, "openlinker", meta["platform"])
}

func TestCallMCPServerCustomHeaderNameRoutesCredential(t *testing.T) {
	recorder := &mcpParamsRecorder{}
	server := httptest.NewServer(recorder.handler())
	defer server.Close()
	svc := newLocalEndpointRuntimeService()
	token := "Bearer not-for-authorization"
	headerName := "X-Api-Key"
	toolName := "analyze"
	for _, transport := range []string{mcpclient.TransportLegacyJSON, mcpclient.TransportStreamableHTTP} {
		agent := &db.Agent{
			ID: uuid.New(), EndpointURL: server.URL + "/mcp", EndpointAuthHeader: &token, EndpointAuthHeaderName: &headerName,
			ConnectionMode: connectionModeMCPServer, MCPToolName: &toolName, MCPTransport: transport,
		}
		_, _, agentErr, callErr := svc.callAgent(context.Background(), agent, uuid.New(), uuid.New(), &RunRequest{Input: map[string]interface{}{}}, nil)
		require.NoError(t, callErr)
		require.Nil(t, agentErr)
		recorder.mu.Lock()
		require.Equal(t, token, recorder.headers.Get("X-Api-Key"), transport)
		require.Empty(t, recorder.headers.Get("Authorization"), transport)
		require.Empty(t, recorder.headers.Get("X-OpenLinker-Token"), transport)
		recorder.mu.Unlock()
	}
}

// A custom header name, a session id and the new transports are not on the
// cross-origin strip list, so those calls refuse cross-origin redirects; a
// legacy_json call without a header name keeps following them with stripping.
func TestCallMCPServerRefusesCrossOriginRedirectForCustomHeaderAndNewTransports(t *testing.T) {
	const mcpBody = `{"jsonrpc":"2.0","id":"1","result":{"structuredContent":{"answer":"ok"}}}`
	otherRec := &endpointRedirectRecorder{}
	other := httptest.NewServer(otherRec.handler(nil, mcpBody))
	defer other.Close()
	originRec := &endpointRedirectRecorder{}
	origin := httptest.NewServer(originRec.handler(map[string]func() string{
		"/cross": func() string { return other.URL + "/landing" },
	}, mcpBody))
	defer origin.Close()

	svc := newLocalEndpointRuntimeService()
	token := "secret-key"
	headerName := "X-Api-Key"
	toolName := "answer"
	for _, tc := range []struct {
		name       string
		transport  string
		headerName *string
	}{
		{"legacy_json with header name", mcpclient.TransportLegacyJSON, &headerName},
		{"streamable_http", mcpclient.TransportStreamableHTTP, nil},
		{"legacy_sse", mcpclient.TransportLegacySSE, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			agent := &db.Agent{
				ID: uuid.New(), EndpointURL: origin.URL + "/cross", EndpointAuthHeader: &token,
				EndpointAuthHeaderName: tc.headerName, ConnectionMode: connectionModeMCPServer,
				MCPToolName: &toolName, MCPTransport: tc.transport,
			}
			_, _, agentErr, callErr := svc.callAgent(context.Background(), agent, uuid.New(), uuid.New(), &RunRequest{Input: map[string]interface{}{}}, nil)
			require.Nil(t, agentErr)
			require.Error(t, callErr)
			require.True(t, errors.Is(callErr, endpointurl.ErrCrossOriginRedirect), "err = %v", callErr)
		})
	}
	otherRec.mu.Lock()
	defer otherRec.mu.Unlock()
	require.Empty(t, otherRec.hits, "a request reached another origin")
}

func TestCallMCPServerSessionMapsEndpointAnswers(t *testing.T) {
	require.Equal(t, "HTTP_401", mcpSessionAgentError(&mcpclient.HTTPStatusError{StatusCode: 401, Body: "no"}).Code)
	require.Equal(t, "MCP_-32601", mcpSessionAgentError(&mcpclient.RPCError{Code: -32601, Message: "x"}).Code)
	require.Equal(t, "INVALID_MCP_RESPONSE", mcpSessionAgentError(&mcpclient.ProtocolError{Stage: "tools/call", Message: "x"}).Code)
	require.Equal(t, "MCP_ENDPOINT_REJECTED", mcpSessionAgentError(fmt.Errorf("%w: other origin", mcpclient.ErrEndpointRejected)).Code)
	require.Nil(t, mcpSessionAgentError(context.Canceled))
	require.Nil(t, mcpSessionAgentError(endpointurl.ErrCrossOriginRedirect))
}

func TestDryRunUsesStreamableHTTPAndReportsToolErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var m struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.NewDecoder(req.Body).Decode(&m)
		switch m.Method {
		case "initialize":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":"2025-06-18"}}`, m.ID)
		case "tools/call":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":{"isError":true,"content":[]}}`, m.ID)
		default:
			w.WriteHeader(http.StatusAccepted)
		}
	}))
	defer server.Close()
	svc := newLocalEndpointRuntimeService()
	toolName := "analyze"
	_, errMsg := svc.DryRun(context.Background(), &db.Agent{
		ID: uuid.New(), EndpointURL: server.URL, ConnectionMode: connectionModeMCPServer,
		MCPToolName: &toolName, MCPTransport: mcpclient.TransportStreamableHTTP,
	}, map[string]interface{}{})
	require.Contains(t, errMsg, "MCP_TOOL_FAILED")
}
