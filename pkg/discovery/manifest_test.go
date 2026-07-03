package discovery

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
)

func TestNewManifestUsesStablePublicEntrypoints(t *testing.T) {
	manifest := NewManifest(&config.Config{
		Env:         "test",
		APIURL:      "https://api.openlinker.test/",
		FrontendURL: "https://openlinker.test/",
	})

	require.Equal(t, "OpenLinker", manifest.Name)
	require.Equal(t, "v1", manifest.Version)
	require.Equal(t, "https://api.openlinker.test/skill/publish-agent", manifest.Docs.PublishAgent)
	require.Equal(t, "https://api.openlinker.test/skill/consume-agent", manifest.Docs.ConsumeAgent)
	require.Equal(t, "https://api.openlinker.test/api/v1/agents/{slug}/agent-card.json", manifest.Docs.AgentCard)
	require.Equal(t, "https://api.openlinker.test/api/v1/mcp/tools", manifest.Tools.MCPTools)
	require.Equal(t, "https://api.openlinker.test/api/v1/a2a/agents/{slug}", manifest.Protocols.A2A)
	require.Equal(t, "https://api.openlinker.test/api/v1/runs/{run_id}/events", manifest.Protocols.RunEvents)
	require.Contains(t, manifest.Tools.Names, "run_agent")
	require.Contains(t, manifest.Auth.APIScopes, "agents:run")
	require.Contains(t, manifest.Auth.AgentScopes, "agent:pull")
	require.Equal(t, "run public agents through REST, MCP, A2A, or delegated calls", manifest.TokenScopes["agents:run"])
	require.Equal(t, "no_pre_review", manifest.Policies["public_listing"])
	require.Equal(t, "not_enabled", manifest.Policies["payments"])
	require.Equal(t, "not_enabled", manifest.Policies["agent_autonomous_purchase"])
	require.Contains(t, manifest.States.Run, "success")
	require.Contains(t, manifest.States.Task, "revision_requested")
	require.Equal(t, "dag_async_agent_workflow_api", manifest.Workflows.Builder)
}

func TestNewManifestFallsBackToLocalPublicEntrypoints(t *testing.T) {
	manifest := NewManifest(&config.Config{})

	require.Equal(t, "http://localhost:8080", manifest.BaseURLs.API)
	require.Equal(t, "http://localhost:3000", manifest.BaseURLs.Web)
	require.Equal(t, "http://localhost:8080/api/v1/a2a/agents/{slug}", manifest.Protocols.A2A)
	require.Equal(t, "http://localhost:3000/connect", manifest.Docs.Connect)
}

func TestServeOpenLinkerManifest(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/.well-known/openlinker.json", nil)
	rec := httptest.NewRecorder()

	handler := ServeOpenLinkerManifest(&config.Config{
		Env:         "production",
		APIURL:      " https://api.openlinker.test/// ",
		FrontendURL: " https://openlinker.test/// ",
	})

	require.NoError(t, handler(e.NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "public, max-age=300", rec.Header().Get(echo.HeaderCacheControl))

	var manifest OpenLinkerManifest
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &manifest))
	require.Equal(t, "production", manifest.Environment)
	require.Equal(t, "https://api.openlinker.test", manifest.BaseURLs.API)
	require.Equal(t, "https://openlinker.test", manifest.BaseURLs.Web)
	require.Equal(t, "https://api.openlinker.test/api/v1/mcp", manifest.Protocols.MCP)
}
