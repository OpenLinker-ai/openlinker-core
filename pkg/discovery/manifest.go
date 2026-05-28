package discovery

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"

	"github.com/kinzhi/openlinker-core/pkg/config"
)

const manifestVersion = "v1"

// OpenLinkerManifest is the public, machine-readable entrypoint for agents and
// external clients that want to discover stable OpenLinker docs and tools.
type OpenLinkerManifest struct {
	Name        string                 `json:"name"`
	Version     string                 `json:"version"`
	Environment string                 `json:"environment,omitempty"`
	BaseURLs    ManifestBaseURLs       `json:"base_urls"`
	Docs        ManifestDocs           `json:"docs"`
	Tools       ManifestTools          `json:"tools"`
	Auth        ManifestAuth           `json:"auth"`
	Policies    map[string]string      `json:"policies"`
	Workflows   ManifestWorkflowStatus `json:"workflows"`
}

type ManifestBaseURLs struct {
	API string `json:"api"`
	Web string `json:"web"`
}

type ManifestDocs struct {
	PublishAgent string `json:"publish_agent"`
	Connect      string `json:"connect"`
	APIKeys      string `json:"api_keys"`
	MCP          string `json:"mcp"`
	Tasks        string `json:"tasks"`
	A2A          string `json:"a2a"`
	AgentCard    string `json:"agent_card"`
}

type ManifestTools struct {
	MCPTools string   `json:"mcp_tools"`
	Names    []string `json:"names"`
}

type ManifestAuth struct {
	AccessTokenHeader   string   `json:"access_token_header"`
	AccessTokenType     string   `json:"access_token_type"`
	AccessTokenPurposes []string `json:"access_token_purposes"`
	APIScopes           []string `json:"api_scopes"`
	RuntimeScopes       []string `json:"runtime_scopes"`
}

type ManifestWorkflowStatus struct {
	ProductionA2A string `json:"production_a2a"`
	Builder       string `json:"builder"`
}

// NewManifest builds the public discovery manifest. It intentionally contains
// no per-user secrets, no private endpoint URLs, and no deployment internals.
func NewManifest(cfg *config.Config) OpenLinkerManifest {
	apiBase := trimTrailingSlash(cfg.APIURL)
	webBase := trimTrailingSlash(cfg.FrontendURL)
	if apiBase == "" {
		apiBase = "http://localhost:8080"
	}
	if webBase == "" {
		webBase = "http://localhost:3000"
	}

	return OpenLinkerManifest{
		Name:        "OpenLinker",
		Version:     manifestVersion,
		Environment: cfg.Env,
		BaseURLs: ManifestBaseURLs{
			API: apiBase,
			Web: webBase,
		},
		Docs: ManifestDocs{
			PublishAgent: apiBase + "/skill/publish-agent",
			Connect:      webBase + "/connect",
			APIKeys:      webBase + "/settings/api-keys",
			MCP:          apiBase + "/api/v1/mcp/tools",
			Tasks:        webBase + "/tasks",
			A2A:          webBase + "/a2a",
			AgentCard:    apiBase + "/api/v1/agents/{slug}/agent-card.json",
		},
		Tools: ManifestTools{
			MCPTools: apiBase + "/api/v1/mcp/tools",
			Names: []string{
				"search_agents",
				"get_agent",
				"run_agent",
				"get_run",
				"create_task",
			},
		},
		Auth: ManifestAuth{
			AccessTokenHeader:   "Authorization: Bearer ol_live_...",
			AccessTokenType:     "ol_live_...",
			AccessTokenPurposes: []string{"api_mcp", "agent_registration", "agent_runtime_a2a"},
			APIScopes:           []string{"agents:read", "agents:run", "runs:read", "tasks:write"},
			RuntimeScopes:       []string{"agent:call", "agent:pull"},
		},
		Policies: map[string]string{
			"public_listing": "no_pre_review",
			"certification":  "review_required",
			"benchmark":      "creator_triggered_public_read",
			"payments":       "not_enabled",
			"human_session":  "jwt_only",
			"agent_tokens":   "single_ol_live_token_scoped_by_purpose_binding_and_expiry",
		},
		Workflows: ManifestWorkflowStatus{
			ProductionA2A: "platform_parent_child_runs",
			Builder:       "dag_async_agent_workflow_api",
		},
	}
}

// ServeOpenLinkerManifest returns an Echo handler for /.well-known/openlinker.json.
func ServeOpenLinkerManifest(cfg *config.Config) echo.HandlerFunc {
	return func(c echo.Context) error {
		c.Response().Header().Set(echo.HeaderCacheControl, "public, max-age=300")
		return c.JSON(http.StatusOK, NewManifest(cfg))
	}
}

func trimTrailingSlash(value string) string {
	return strings.TrimRight(strings.TrimSpace(value), "/")
}
