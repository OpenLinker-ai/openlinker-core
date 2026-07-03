package discovery

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
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
	Protocols   ManifestProtocols      `json:"protocols"`
	Auth        ManifestAuth           `json:"auth"`
	TokenScopes map[string]string      `json:"token_scopes"`
	Policies    map[string]string      `json:"policies"`
	States      ManifestStates         `json:"states"`
	Workflows   ManifestWorkflowStatus `json:"workflows"`
}

type ManifestBaseURLs struct {
	API string `json:"api"`
	Web string `json:"web"`
}

type ManifestDocs struct {
	PublishAgent string `json:"publish_agent"`
	ConsumeAgent string `json:"consume_agent"`
	Connect      string `json:"connect"`
	UserTokens   string `json:"user_tokens"`
	MCP          string `json:"mcp"`
	Tasks        string `json:"tasks"`
	A2A          string `json:"a2a"`
	AgentCard    string `json:"agent_card"`
}

type ManifestTools struct {
	MCPTools string   `json:"mcp_tools"`
	Names    []string `json:"names"`
}

type ManifestProtocols struct {
	AgentCard     string `json:"agent_card"`
	ExtendedCard  string `json:"extended_agent_card"`
	A2A           string `json:"a2a"`
	A2ATaskLookup string `json:"a2a_task_lookup"`
	A2ASubscribe  string `json:"a2a_subscribe"`
	RunEvents     string `json:"run_events"`
	MCP           string `json:"mcp"`
}

type ManifestAuth struct {
	UserTokenHeader    string   `json:"user_token_header"`
	UserTokenType      string   `json:"user_token_type"`
	UserTokenPurposes  []string `json:"user_token_purposes"`
	AgentTokenHeader   string   `json:"agent_token_header"`
	AgentTokenType     string   `json:"agent_token_type"`
	AgentTokenPurposes []string `json:"agent_token_purposes"`
	APIScopes          []string `json:"api_scopes"`
	AgentScopes        []string `json:"agent_scopes"`
}

type ManifestWorkflowStatus struct {
	ProductionA2A string `json:"production_a2a"`
	Builder       string `json:"builder"`
}

type ManifestStates struct {
	Run      []string `json:"run"`
	Task     []string `json:"task"`
	Delivery []string `json:"delivery"`
	Workflow []string `json:"workflow"`
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
			ConsumeAgent: apiBase + "/skill/consume-agent",
			Connect:      webBase + "/connect",
			UserTokens:   webBase + "/settings/user-tokens",
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
		Protocols: ManifestProtocols{
			AgentCard:     apiBase + "/api/v1/agents/{slug}/agent-card.json",
			ExtendedCard:  apiBase + "/api/v1/agents/{slug}/agent-card.extended.json",
			A2A:           apiBase + "/api/v1/a2a/agents/{slug}",
			A2ATaskLookup: apiBase + "/api/v1/a2a/agents/{slug}/tasks/{task_id}",
			A2ASubscribe:  apiBase + "/api/v1/a2a/agents/{slug}/tasks/{task_id}:subscribe",
			RunEvents:     apiBase + "/api/v1/runs/{run_id}/events",
			MCP:           apiBase + "/api/v1/mcp",
		},
		Auth: ManifestAuth{
			UserTokenHeader:    "Authorization: Bearer ol_user_...",
			UserTokenType:      "ol_user_...",
			UserTokenPurposes:  []string{"platform_api", "mcp", "external_scripts"},
			AgentTokenHeader:   "Authorization: Bearer ol_agent_...",
			AgentTokenType:     "ol_agent_...",
			AgentTokenPurposes: []string{"agent_registration", "agent_runtime", "a2a_delegation"},
			APIScopes:          []string{"agents:read", "agents:run", "runs:read", "tasks:write"},
			AgentScopes:        []string{"agent:call", "agent:pull"},
		},
		TokenScopes: map[string]string{
			"agents:read":    "search and inspect public agents through REST or MCP",
			"agents:run":     "run public agents through REST, MCP, A2A, or delegated calls",
			"runs:read":      "read run status, output, events, children and artifacts allowed to the owner",
			"tasks:write":    "create tasks and task recommendations through OpenLinker tools",
			"agent:pull":     "queued runtime agents can open WebSocket, heartbeat, claim runs, and submit results",
			"agent:call":     "an agent currently handling a run can delegate to another agent",
			"register:agent": "one-time or short-lived creator invitation for agent self-registration",
		},
		Policies: map[string]string{
			"public_listing":            "no_pre_review",
			"certification":             "review_required",
			"benchmark":                 "creator_triggered_public_read",
			"payments":                  "not_enabled",
			"agent_autonomous_purchase": "not_enabled",
			"public_artifacts":          "explicit_only",
			"human_session":             "jwt_only",
			"agent_tokens":              "single_ol_agent_token_scoped_by_purpose_binding_and_expiry",
		},
		States: ManifestStates{
			Run:      []string{"pending", "running", "success", "failed", "timeout", "canceled"},
			Task:     []string{"open", "claimed", "running", "submitted", "revision_requested", "accepted", "rejected", "canceled"},
			Delivery: []string{"pending", "succeeded", "failed"},
			Workflow: []string{"pending", "running", "paused", "success", "failed", "canceled"},
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
