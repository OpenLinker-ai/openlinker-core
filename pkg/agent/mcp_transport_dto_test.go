package agent

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

// Default MCP Agents must serialize exactly as they did before transports
// existed; only non-default values add the new fields.
func TestAgentResponseOmitsDefaultMCPTransportFields(t *testing.T) {
	tool := "lookup"
	base := db.Agent{
		ID: uuid.New(), Slug: "mcp-agent", Name: "MCP Agent", EndpointURL: "https://mcp.example/mcp",
		Tags: []string{"data"}, LifecycleStatus: "active", Visibility: "private", CertificationStatus: "unreviewed",
		ConnectionMode: ConnectionModeMCPServer, MCPToolName: &tool, CreatedAt: time.Unix(0, 0),
	}
	marshal := func(a db.Agent) string {
		resp := toAgentResponse(&a)
		raw, err := json.Marshal(resp)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	legacy := marshal(base)
	for _, transport := range []string{"", "legacy_json"} {
		a := base
		a.MCPTransport = transport
		if got := marshal(a); got != legacy {
			t.Fatalf("transport %q changed the response:\n%s\n%s", transport, got, legacy)
		}
	}
	if strings.Contains(legacy, "mcp_transport") || strings.Contains(legacy, "endpoint_auth_header_name") {
		t.Fatalf("default response carries new fields: %s", legacy)
	}
	streamable := base
	streamable.MCPTransport = "streamable_http"
	if got := marshal(streamable); !strings.Contains(got, `"mcp_transport":"streamable_http"`) || strings.Contains(got, "endpoint_auth_header_name") {
		t.Fatalf("streamable response = %s", got)
	}
	named := base
	named.MCPTransport = "legacy_json"
	name := "X-Api-Key"
	secret := "secret-value"
	named.EndpointAuthHeaderName = &name
	named.EndpointAuthHeader = &secret
	got := marshal(named)
	if !strings.Contains(got, `"endpoint_auth_header_name":"X-Api-Key"`) || strings.Contains(got, "mcp_transport") || strings.Contains(got, secret) {
		t.Fatalf("header-name response = %s", got)
	}
}
