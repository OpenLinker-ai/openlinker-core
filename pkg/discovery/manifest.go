package discovery

import (
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	coreruntime "github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

const manifestVersion = "v1"

// OpenLinkerManifest is the public, machine-readable entrypoint for agents and
// external clients that want to discover stable OpenLinker docs and tools.
type OpenLinkerManifest struct {
	Name        string                 `json:"name"`
	Version     string                 `json:"version"`
	Environment string                 `json:"environment,omitempty"`
	BaseURLs    ManifestBaseURLs       `json:"base_urls"`
	Runtime     ManifestRuntime        `json:"runtime"`
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
	API     string `json:"api"`
	Web     string `json:"web"`
	Runtime string `json:"runtime,omitempty"`
}

type ManifestRuntime struct {
	Enabled                  bool                           `json:"enabled"`
	MTLSRequired             bool                           `json:"mtls_required"`
	CredentialEndpoint       string                         `json:"credential_endpoint,omitempty"`
	TrustBundleEndpoint      string                         `json:"trust_bundle_endpoint,omitempty"`
	CertificateLifetimeHours int                            `json:"certificate_lifetime_hours,omitempty"`
	Transports               []string                       `json:"transports"`
	DefaultTransport         string                         `json:"default_transport"`
	CurrentContractDigest    string                         `json:"current_contract_digest"`
	SupportedContractDigests []string                       `json:"supported_contract_digests"`
	PreviousSupportedUntil   string                         `json:"previous_supported_until"`
	TransportPolicy          ManifestRuntimeTransportPolicy `json:"transport_policy"`
}

type ManifestRuntimeTransportPolicy struct {
	Version                  int   `json:"version"`
	HeartbeatIntervalSeconds int64 `json:"heartbeat_interval_seconds"`
	SessionStaleAfterSeconds int64 `json:"session_stale_after_seconds"`
	RetryMinimumMilliseconds int64 `json:"retry_minimum_ms"`
	RetryMaximumMilliseconds int64 `json:"retry_maximum_ms"`
	WebSocketProbeIntervalMS int64 `json:"websocket_probe_interval_ms"`
	WebSocketProbeTimeoutMS  int64 `json:"websocket_probe_timeout_ms"`
}

type ManifestDocs struct {
	PublishAgent string `json:"publish_agent"`
	ConsumeAgent string `json:"consume_agent"`
	Connect      string `json:"connect"`
	UserTokens   string `json:"user_tokens"`
	MCP          string `json:"mcp"`
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
// only explicitly configured, validated public origins; it contains no
// per-user secrets, credentials, certificate paths, or internal topology.
func NewManifest(cfg *config.Config) OpenLinkerManifest {
	if cfg == nil {
		cfg = &config.Config{}
	}
	apiBase := trimTrailingSlash(cfg.APIURL)
	webBase := trimTrailingSlash(cfg.FrontendURL)
	if apiBase == "" {
		apiBase = "http://localhost:8080"
	}
	if webBase == "" {
		webBase = "http://localhost:3000"
	}
	runtimeOrigin, runtimeEnabled := manifestRuntimeOrigin(cfg)
	transportPolicy := coreruntime.CurrentRuntimeTransportPolicy()
	livenessPolicy := coreruntime.CurrentRuntimeLivenessPolicy()
	wireCompatibility := coreruntime.CurrentRuntimeWireCompatibility()
	transports := make([]string, 0, len(transportPolicy.OrderedTransports))
	for _, transport := range transportPolicy.OrderedTransports {
		transports = append(transports, string(transport))
	}

	credentialEndpoint := ""
	trustBundleEndpoint := ""
	certificateLifetimeHours := 0
	if cfg.RuntimeMTLSEnabled && strings.ToLower(strings.TrimSpace(cfg.RuntimePKIMode)) != "files" {
		credentialEndpoint = apiBase + "/api/v1/runtime-credentials"
		trustBundleEndpoint = apiBase + "/.well-known/openlinker-runtime-ca.pem"
		certificateLifetimeHours = 24
	}
	return OpenLinkerManifest{
		Name:        "OpenLinker",
		Version:     manifestVersion,
		Environment: cfg.Env,
		BaseURLs: ManifestBaseURLs{
			API:     apiBase,
			Web:     webBase,
			Runtime: runtimeOrigin,
		},
		Docs: ManifestDocs{
			PublishAgent: apiBase + "/skill/publish-agent",
			ConsumeAgent: apiBase + "/skill/consume-agent",
			Connect:      webBase + "/connect",
			UserTokens:   webBase + "/settings/user-tokens",
			MCP:          apiBase + "/api/v1/mcp/tools",
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
			APIScopes: []string{
				"agents:read", "agents:run", "agents:create",
				"runs:read", "runs:cancel",
				"tasks:read", "tasks:create", "tasks:run",
				"workflows:read", "workflows:manage", "workflows:run",
				"agent-tokens:read", "agent-tokens:issue", "agent-tokens:revoke",
			},
			AgentScopes: []string{"agent:call", "agent:pull"},
		},
		TokenScopes: map[string]string{
			"agents:read":         "search and inspect public agents through REST or MCP",
			"agents:run":          "run public agents through REST, MCP, A2A, or delegated calls",
			"runs:read":           "read run status, output, events, children and artifacts allowed to the owner",
			"agents:create":       "enter the pending Agent registration flow when combined with agent-tokens:issue",
			"runs:cancel":         "cancel an owned running invocation",
			"tasks:read":          "read tasks owned by the User Token principal",
			"tasks:create":        "create private tasks and task recommendations through OpenLinker tools",
			"tasks:run":           "start a run from an owned private task",
			"workflows:read":      "read owned workflows and workflow runs",
			"workflows:manage":    "create or update owned workflow definitions",
			"workflows:run":       "start and control owned workflow runs",
			"agent-tokens:read":   "read non-secret metadata for owned Agent Tokens",
			"agent-tokens:issue":  "issue or rotate credentials for owned Agents",
			"agent-tokens:revoke": "revoke credentials for owned Agents",
			"agent:pull":          "Runtime Workers use WebSocket first and automatically fall back to long polling under the same authenticated Session, ACK, lease, resume, fence, and persistent-spool contract",
			"agent:call":          "an agent currently handling a run can delegate to another agent",
			"register:agent":      "one-time or short-lived creator invitation for agent self-registration",
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
			Task:     []string{"needs_agent", "open", "matched", "completed"},
			Delivery: []string{"pending", "succeeded", "failed"},
			Workflow: []string{"pending", "running", "paused", "success", "failed", "canceled"},
		},
		Workflows: ManifestWorkflowStatus{
			ProductionA2A: "platform_parent_child_runs",
			Builder:       "dag_async_agent_workflow_api",
		},
		Runtime: ManifestRuntime{
			Enabled:                  runtimeEnabled,
			MTLSRequired:             cfg.RuntimeMTLSEnabled,
			CredentialEndpoint:       credentialEndpoint,
			TrustBundleEndpoint:      trustBundleEndpoint,
			CertificateLifetimeHours: certificateLifetimeHours,
			Transports:               transports,
			DefaultTransport:         transportPolicy.DefaultTransport,
			CurrentContractDigest:    wireCompatibility.CurrentContractDigest,
			SupportedContractDigests: wireCompatibility.SupportedContractDigests,
			PreviousSupportedUntil:   wireCompatibility.PreviousSupportedUntilRFC,
			TransportPolicy: ManifestRuntimeTransportPolicy{
				Version:                  transportPolicy.Version,
				HeartbeatIntervalSeconds: int64(livenessPolicy.HeartbeatInterval / time.Second),
				SessionStaleAfterSeconds: int64(livenessPolicy.SessionStaleAfter / time.Second),
				RetryMinimumMilliseconds: transportPolicy.RetryMinimum.Milliseconds(),
				RetryMaximumMilliseconds: transportPolicy.RetryMaximum.Milliseconds(),
				WebSocketProbeIntervalMS: transportPolicy.WebSocketProbeInterval.Milliseconds(),
				WebSocketProbeTimeoutMS:  transportPolicy.WebSocketProbeTimeout.Milliseconds(),
			},
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

// manifestRuntimeOrigin publishes only a validated public origin. Runtime is
// deliberately fail-closed: a disabled or malformed configuration never
// falls back to the ordinary API origin.
func manifestRuntimeOrigin(cfg *config.Config) (string, bool) {
	if cfg == nil {
		return "", false
	}
	if !cfg.RuntimeMTLSEnabled {
		origin := trimTrailingSlash(cfg.APIURL)
		return origin, origin != ""
	}
	origin, err := config.NormalizeRuntimePublicOrigin(cfg.RuntimeMTLSAPIURL)
	if err != nil {
		return "", false
	}
	return origin, true
}
