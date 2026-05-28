package discovery

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kinzhi/openlinker-core/pkg/config"
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
	require.Equal(t, "https://api.openlinker.test/api/v1/agents/{slug}/agent-card.json", manifest.Docs.AgentCard)
	require.Equal(t, "https://api.openlinker.test/api/v1/mcp/tools", manifest.Tools.MCPTools)
	require.Contains(t, manifest.Tools.Names, "run_agent")
	require.Contains(t, manifest.Auth.APIScopes, "agents:run")
	require.Contains(t, manifest.Auth.RuntimeScopes, "agent:pull")
	require.Equal(t, "no_pre_review", manifest.Policies["public_listing"])
	require.Equal(t, "dag_async_agent_workflow_api", manifest.Workflows.Builder)
}
