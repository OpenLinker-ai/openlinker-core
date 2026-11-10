package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/mcpclient"
)

func TestDiscoverMCPToolsListsToolsWithManualSchemaValidation(t *testing.T) {
	var seenKey string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var m struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&m)
		switch m.Method {
		case "initialize":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":"2025-06-18"}}`, m.ID)
		case "tools/list":
			seenKey = r.Header.Get("X-Api-Key")
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":{"tools":[
{"name":"lookup","description":"Look up","inputSchema":{"type":"object","properties":{"q":{"type":"string"}}}},
{"name":"broken","inputSchema":{"type":"object","properties":{"q":{"type":"no-such-type"}}}},
{"name":"","inputSchema":{"type":"object"}}]}}`, m.ID)
		default:
			w.WriteHeader(http.StatusAccepted)
		}
	}))
	defer server.Close()
	svc := &Service{cfg: &config.Config{AllowLocalHTTPEndpoints: true}}
	resp, err := svc.discoverMCPTools(context.Background(), &DiscoverMCPToolsRequest{
		EndpointURL: server.URL, MCPTransport: "streamable_http",
		EndpointAuthHeader: "secret", EndpointAuthHeaderName: "X-Api-Key",
	}, mcpclient.ListTools)
	require.NoError(t, err)
	assert.Equal(t, "secret", seenKey)
	require.Len(t, resp.Tools, 2)
	assert.Equal(t, "lookup", resp.Tools[0].Name)
	assert.Equal(t, "Look up", resp.Tools[0].Description)
	assert.Empty(t, resp.Tools[0].InputSchemaError)
	assert.Equal(t, "object", resp.Tools[0].InputSchema["type"])
	wantErr := validateCapabilitySchema(resp.Tools[1].InputSchema, "input_schema")
	if wantErr == nil {
		assert.Empty(t, resp.Tools[1].InputSchemaError)
	} else {
		assert.Equal(t, wantErr.Error(), resp.Tools[1].InputSchemaError)
	}
}

func TestDiscoverMCPToolsValidatesInputAndHidesUpstreamBodies(t *testing.T) {
	svc := &Service{cfg: &config.Config{AllowLocalHTTPEndpoints: true}}
	never := func(context.Context, mcpclient.Config) ([]mcpclient.Tool, error) {
		t.Fatal("must not call the endpoint")
		return nil, nil
	}
	for _, req := range []DiscoverMCPToolsRequest{
		{EndpointURL: "http://10.0.0.1/mcp", MCPTransport: "streamable_http"},
		{EndpointURL: "https://example.com/mcp", MCPTransport: "legacy_json"},
		{EndpointURL: "https://example.com/mcp", MCPTransport: "streamable_http", EndpointAuthHeaderName: "Cookie"},
	} {
		_, err := svc.discoverMCPTools(context.Background(), &req, never)
		var he *httpx.HTTPError
		require.True(t, errors.As(err, &he), "%v", err)
		assert.Equal(t, http.StatusUnprocessableEntity, he.Status)
	}
	failing := func(context.Context, mcpclient.Config) ([]mcpclient.Tool, error) {
		return nil, &mcpclient.HTTPStatusError{Stage: "initialize", StatusCode: 401, Body: "echo secret-value"}
	}
	_, err := svc.discoverMCPTools(context.Background(), &DiscoverMCPToolsRequest{
		EndpointURL: "https://example.com/mcp", MCPTransport: "legacy_sse", EndpointAuthHeader: "secret-value",
	}, failing)
	var he *httpx.HTTPError
	require.True(t, errors.As(err, &he))
	assert.Equal(t, http.StatusBadGateway, he.Status)
	assert.NotContains(t, he.Error(), "secret-value")
	assert.Contains(t, he.Error(), "401")
}
