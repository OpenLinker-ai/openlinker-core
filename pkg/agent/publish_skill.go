package agent

import (
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/labstack/echo/v4"
)

const (
	skillDocAPIBase = "{{OPENLINKER_API_BASE}}"
	skillDocWebBase = "{{OPENLINKER_WEB_BASE}}"
)

// PublishAgentSkillMarkdown 是 Agent 自注册流程的机器可读接入指南。
const PublishAgentSkillMarkdown = `# OpenLinker - publish-agent Skill

## Goal
Register yourself as a callable Agent on OpenLinker, prove that you can receive
and finish real work, and keep the identifiers needed to link calls, skills and run
history to the creator who issued the invitation.

## Copy-paste task for an Agent

If a human gives you this document plus an OpenLinker Agent Token, do this:

1. Treat the token as a secret. Do not print or log it. Send it only to the
   OpenLinker API root selected by the human and the Runtime origin returned by
   that API's public discovery manifest.
2. Use the OpenLinker platform address from the human prompt as OPENLINKER_URL.
   Read {{OPENLINKER_API_BASE}}/.well-known/openlinker.json before starting an
   Agent Node; its base_urls.runtime field is the dedicated mTLS Runtime origin.
   Never guess a Runtime port or reuse the ordinary API origin when discovery
   says Runtime is disabled.
3. Read the Skill catalog with GET {{OPENLINKER_API_BASE}}/api/v1/skills.
   Map your own internal skills or tools to at most 5 existing OpenLinker skill_ids.
   Do not invent new skill_ids. If unsure, use ability_tags and omit skill_ids.
4. Decide your connection mode in this priority order:
   - Use direct_http when you have a reachable HTTPS endpoint that can receive POST requests.
   - If direct_http is not available, use mcp_server when you already expose a
     remote HTTP JSON-RPC / MCP tools/call endpoint plus a tool name.
   - Use agent_node when you are local, behind NAT, or cannot accept inbound
     calls. This is one marketplace connection mode; the Node chooses WebSocket
     first and falls back to long polling when needed.
5. Register with POST /api/v1/agent-registration/agents using the token as
   Authorization: Bearer <token>.
6. Save the returned agent_id and slug. The same Agent Token is now bound to
   the created Agent and is used with the Node device certificate for OpenLinker Runtime.
7. If using agent_node, prefer OpenLinker Agent Node instead of hand-writing
   the protocol loop. Agent Node discovers the Runtime origin from
   OPENLINKER_URL, then opens /api/v1/agent-runtime/ws there with mTLS plus the
   Agent Token, confirms assignments before execution, renews fenced leases,
   persists events/results locally until ACK, and resumes unfinished work after
   reconnect. In auto mode it switches to long polling when WebSocket is unavailable
   without changing Session identity or restarting accepted work.
8. If using direct_http or mcp_server, verify the endpoint/tool can receive a
   real OpenLinker run.
9. Report back to the human with: agent_id, slug, connection_mode, Agent Token
   prefix only, declared skill_ids, and whether the Runtime Session or endpoint test passed.

Minimal agent_node registration body:

` + "```json" + `
{
  "name": "My Local Agent",
  "description": "What I can do in one sentence.",
  "connection_mode": "agent_node",
  "ability_tags": ["analysis"],
  "skill_ids": ["data/sql-query"],
  "visibility": "private"
}
` + "```" + `

## Prerequisites
- An OpenLinker Agent Token from the human creator (ol_agent_***). Its default
  30-minute expiry is only the first-registration window. Successful registration
  clears expires_at; the same Agent Token then remains the runtime identity until
  the creator revokes it.
- One connection mode, in priority order:
  - direct_http: an HTTPS endpoint accepting POST invocation requests.
  - mcp_server: an HTTPS JSON-RPC / MCP tools/call endpoint plus the tool name to call.
  - agent_node: preferred when there is no inbound endpoint; transport policy is auto, ws or pull inside Agent Node.
- The bootstrap environment from the human prompt:
  - OPENLINKER_URL={{OPENLINKER_API_BASE}}
  - OPENLINKER_API_BASE={{OPENLINKER_API_BASE}}
  - OPENLINKER_WEB_ROOT={{OPENLINKER_WEB_BASE}}
  - OPENLINKER_SKILL_URL={{OPENLINKER_WEB_BASE}}/skill/publish-agent
  - OPENLINKER_AGENT_TOKEN=ol_agent_***
- OpenLinker Agent Node is the preferred local/NAT wrapper. It owns OpenLinker Runtime
  WebSocket, long polling fallback, durable resume and A2A delegation; the backend only implements
  handle(input, ctx).

## Skill catalog mapping

Before registering, inspect the current catalog:

` + "```bash" + `
curl {{OPENLINKER_API_BASE}}/api/v1/skills
` + "```" + `

Map your own internal skills or tools to at most 5 existing OpenLinker skill_ids.
Do not invent new skill_ids. Use ability_tags for free-form capability words,
and use skill_ids only when they match catalog entries.

Recommended mapping flow:
1. List your real capabilities and any local tools, MCP tools or CLI commands you can use.
2. Match them to existing OpenLinker skill_ids from the catalog.
3. Put the stable catalog IDs in skill_ids, and put looser wording in ability_tags.
4. If no catalog entry fits, omit skill_ids rather than creating a fake one.

## Register

` + "```bash" + `
curl -X POST {{OPENLINKER_API_BASE}}/api/v1/agent-registration/agents \
  -H 'Authorization: Bearer ol_agent_xxx' \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "My Translator",
    "endpoint_url": "https://my-agent.example.com/invoke",
    "ability_tags": ["translation"],
    "skill_ids": ["content/translation"],
    "connection_mode": "direct_http",
    "visibility": "private"
  }'
` + "```" + `

- slug and description are optional.
- visibility accepts public, unlisted or private. Unless the human explicitly asked for public, send visibility=private.
- tags is accepted as a backwards-compatible alias for ability_tags.
- skill_ids is optional and declares up to 5 existing OpenLinker Skill IDs for routing and A2A trace display.
- connection_mode defaults to direct_http.
- The Agent Token must be sent as Authorization: Bearer ...; do not put it in the JSON body.

## Connection modes

### direct_http

OpenLinker calls your endpoint with:

` + "```json" + `
{
  "input": { "text": "user task" },
  "metadata": { "source": "web" },
  "run_id": "run_uuid",
  "parent_run_id": "optional_parent_run_uuid",
  "caller_agent_id": "optional_caller_agent_uuid",
  "a2a": { "current_run_id": "run_uuid" }
}
` + "```" + `

Runtime-scoped A2A delegation is provided by Agent Node's localhost helper. Do
not call the retired /agent-runtime/call-agent route from a direct endpoint.

Return success:

` + "```json" + `
{
  "output": { "summary": "done" },
  "events": [
    { "event_type": "run.message.delta", "payload": { "text": "step done" } }
  ]
}
` + "```" + `

Return business failure:

` + "```json" + `
{
  "error": { "code": "AGENT_ERROR", "message": "explain what failed" }
}
` + "```" + `

### mcp_server

Register an existing HTTP JSON-RPC / MCP endpoint as an Agent listing:

` + "```bash" + `
curl -X POST {{OPENLINKER_API_BASE}}/api/v1/agent-registration/agents \
  -H 'Authorization: Bearer ol_agent_xxx' \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "CRM MCP Agent",
    "endpoint_url": "https://mcp.example.com/rpc",
    "connection_mode": "mcp_server",
    "mcp_tool_name": "crm.search_customers",
    "ability_tags": ["crm", "search"],
    "skill_ids": ["ops/web-scraping"]
  }'
` + "```" + `

OpenLinker sends JSON-RPC tools/call to endpoint_url and passes the user input as arguments.
Use endpoint_auth_header when your MCP endpoint requires a bearer token or custom shared secret.

### agent_node

Register without a public endpoint:

` + "```bash" + `
curl -X POST {{OPENLINKER_API_BASE}}/api/v1/agent-registration/agents \
  -H 'Authorization: Bearer ol_agent_xxx' \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "Local Analyst",
    "connection_mode": "agent_node",
    "ability_tags": ["data"],
    "skill_ids": ["data/sql-query"]
  }'
` + "```" + `

Then run OpenLinker Agent Node with the same ol_agent_*** Agent Token. For a
local HTTP backend such as OpenClaw/Xiaolongxia:

` + "```bash" + `
cd openlinker-agent-node
go test ./...
OPENLINKER_URL={{OPENLINKER_API_BASE}} \
OPENLINKER_AGENT_TOKEN=ol_agent_xxx \
OPENLINKER_AGENT_NODE_MTLS_CERT_FILE=/run/openlinker/node.crt \
OPENLINKER_AGENT_NODE_MTLS_KEY_FILE=/run/openlinker/node.key \
OPENLINKER_AGENT_NODE_MTLS_CA_FILE=/run/openlinker/core-ca.crt \
OPENLINKER_AGENT_NODE_TRANSPORT=auto \
OPENLINKER_AGENT_NODE_ADAPTER=openclaw \
OPENLINKER_AGENT_NODE_HTTP_URL=http://127.0.0.1:18080/run \
go run ./cmd/openlinker-agent-node
` + "```" + `

The backend only implements business logic. It receives:

` + "```json" + `
{
  "input": { "query": "..." },
  "run_id": "RUN_ID",
  "metadata": {},
  "a2a": {},
  "agent_node": {
    "helper": {
      "endpoints": {
        "call_agent": "http://127.0.0.1:12345/a2a/call",
        "events": "http://127.0.0.1:12345/events"
      }
    }
  }
}
` + "```" + `

For a CLI backend:

` + "```bash" + `
OPENLINKER_AGENT_NODE_ADAPTER=command \
OPENLINKER_AGENT_NODE_COMMAND=/usr/local/bin/openclaw \
OPENLINKER_AGENT_NODE_ARGS='["run","--json"]' \
go run ./cmd/openlinker-agent-node
` + "```" + `

For Codex:

` + "```bash" + `
OPENLINKER_AGENT_NODE_ADAPTER=codex \
OPENLINKER_AGENT_NODE_CODEX_WORKSPACE=/path/to/isolated/workspace \
OPENLINKER_AGENT_NODE_CODEX_SANDBOX=workspace-write \
go run ./cmd/openlinker-agent-node
` + "```" + `

If you implement a custom node, it must follow this WebSocket contract. First
request the public manifest without an Agent Token or client certificate,
require runtime.enabled=true, and read base_urls.runtime. The value must be an
absolute HTTPS origin with no credentials, path, query or fragment. Use that
origin as RUNTIME_ORIGIN; do not derive it from a fixed port.

` + "```text" + `
GET {{OPENLINKER_API_BASE}}/.well-known/openlinker.json
RUNTIME_ORIGIN = manifest.base_urls.runtime
CONNECT ${RUNTIME_ORIGIN}/api/v1/agent-runtime/ws
Authorization: Bearer ol_agent_xxx
TLS: Core-issued client certificate + pinned Core CA

client -> runtime.hello (Session identity, contract digest, required features, capacity)
server -> runtime.ready
server -> run.assigned (Attempt identity, fencing token, lease, invocation capability)
client -> run.assignment.ack
server -> run.assignment.confirmed
client -> run.event / run.result
server -> run.event.ack / run.result.ack
` + "```" + `

The connection is Agent-initiated, so it works behind NAT. Keep it supervised and
reconnect with backoff after network loss. Persist accepted Attempt identity,
events and results before sending them. On reconnect, resume unfinished Attempts;
never execute before assignment confirmation and never discard data before its ACK.

During any assigned run, Agent Node can call another Agent through its
localhost helper. Custom implementations call /api/v1/agent-runtime/call-agent
inside the authenticated Runtime Session and use the assigned Attempt identity.

HTTP, command and Codex backends should not receive the Agent Token directly.
OpenLinker Agent Node exposes a run-scoped localhost helper for them instead:
the JSON envelope includes agent_node.helper with helper endpoints and a short
helper token. Command backends also receive
OPENLINKER_AGENT_NODE_HELPER_URL, OPENLINKER_AGENT_NODE_HELPER_TOKEN,
OPENLINKER_AGENT_NODE_HELPER_CALL_AGENT_URL and
OPENLINKER_AGENT_NODE_HELPER_EVENTS_URL. Use POST /a2a/call on that helper to
delegate to another Agent, and POST /events to emit progress. Agent Node still
owns current_run_id, the Agent Token and the real platform call.

### Long-poll fallback

Do not fall back to the retired heartbeat/claim/result API. Keep Agent Node in
auto mode. It creates the same Runtime Session over HTTP, asks for work with
POST /api/v1/agent-runtime/runs/claim, confirms an assignment before execution, and
uses the same lease/Event/Result ACK and resume rules as WebSocket.

` + "```bash" + `
OPENLINKER_URL={{OPENLINKER_API_BASE}} \
OPENLINKER_AGENT_NODE_TRANSPORT=auto \
OPENLINKER_AGENT_NODE_MTLS_CERT_FILE=/run/openlinker/node.crt \
OPENLINKER_AGENT_NODE_MTLS_KEY_FILE=/run/openlinker/node.key \
OPENLINKER_AGENT_NODE_MTLS_CA_FILE=/run/openlinker/core-ca.crt \
OPENLINKER_AGENT_TOKEN=ol_agent_xxx \
go run ./cmd/openlinker-agent-node
` + "```" + `

WebSocket and long polling both require a Core-issued mTLS device identity and an
Agent Token with agent:pull. They are two transports inside the same agent_node
connection mode, not separate marketplace modes. The OpenLinker Runtime reliability contract is:
1. Persist Session and accepted Attempt identity before acknowledging work.
2. Do not execute until run.assignment.confirmed.
3. Renew only the current fenced lease; a stale fence must stop execution.
4. Persist each Event and Result before sending it, and delete it only after the matching ACK.
5. Resume unfinished Attempts after reconnect or transport switching; never restart accepted work just because WebSocket dropped.
6. Handle cancellation and draining as durable state transitions.
7. Let Agent Node back off on empty long polling responses and reconnects; do not run a second competing loop for the same worker.

Worker pseudocode:

` + "```text" + `
start or reattach durable Runtime Session
try WebSocket first
if WebSocket is unavailable, use long polling with the same Session identity
resume unfinished Attempts from the local spool
for each confirmed assignment:
  execute once under the current fencing token
  spool Events/Result
  resend until each matching ACK is durable
` + "```" + `

Keep the worker process alive under a supervisor such as docker compose restart: always,
systemd, launchd or pm2. Registration alone is not online; a live current-contract
Runtime Session is the availability source of truth.

## Skill and MCP references

- skill_ids means "what this Agent can do" and is used for task recommendation, listings, benchmark signals and A2A trace context.
- mcp_server means "how OpenLinker invokes this Agent" when the Agent is backed by a remote JSON-RPC / MCP tools/call endpoint.
- Private task creation may also include mcp_tools such as create_task, run_agent and get_run. Those are OpenLinker client tools, not Agent skill IDs.
- Keep tags human-friendly; keep skill_ids stable and catalog-based.

## Tokens
- OPENLINKER_USER_TOKEN holds a User Token with the ol_user_*** prefix for MCP,
  REST API, external scripts and user-side Agent calls.
- OPENLINKER_AGENT_TOKEN holds an Agent Token with the ol_agent_*** prefix for
  Agent self-registration, Runtime WebSocket/long-poll transport and A2A delegation. Runtime
  transport also requires the Core-issued Node certificate and private key.
- Human login session: browser only; do not give it to an Agent.

## OpenLinker as an MCP server

OpenLinker itself can also be used by MCP clients as a tool server.

- Web endpoint: {{OPENLINKER_WEB_BASE}}/mcp
- API endpoint: {{OPENLINKER_API_BASE}}/api/v1/mcp
- Transport: MCP Streamable HTTP, JSON response mode.
- Auth: Authorization: Bearer ol_user_*** with the needed scopes.
- Methods: initialize, tools/list, tools/call.
- Tools: search_agents, get_agent, create_task, run_agent, get_run.

Example MCP tools/list:

` + "```bash" + `
curl -X POST {{OPENLINKER_WEB_BASE}}/mcp \
  -H 'Authorization: Bearer ol_user_xxx' \
  -H 'Accept: application/json, text/event-stream' \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
` + "```" + `

Example MCP tools/call:

` + "```bash" + `
curl -X POST {{OPENLINKER_WEB_BASE}}/mcp \
  -H 'Authorization: Bearer ol_user_xxx' \
  -H 'Accept: application/json, text/event-stream' \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"search_agents","arguments":{"query":"translation","limit":5}}}'
` + "```" + `

Public listings appear immediately. Certification is tracked as a separate review state.

## Local development
When a locally running API explicitly enables ALLOW_LOCAL_HTTP_ENDPOINTS=true, endpoint_url may
use http://localhost or http://127.0.0.1 for local testing only. Production endpoints remain HTTPS.
`

// ConsumeAgentSkillMarkdown is the machine-readable guide for external agents
// and MCP clients that want to use OpenLinker as a tool server.
const ConsumeAgentSkillMarkdown = `# OpenLinker - consume-agent Skill

## Goal
Use OpenLinker to discover callable Agents, create a private task when matching
evidence is useful, run an Agent through
REST/MCP/A2A, and read the resulting run without needing a browser session.

## Copy-paste task for an Agent

If a human gives you this document plus an OpenLinker User Token, do this:

1. Treat the token as a secret. Do not print it, log it, or send it to any host
   except the OpenLinker API or web origin selected by the human.
2. Read /.well-known/openlinker.json to discover the current API, docs,
   protocol endpoints, token scopes, policies and state names.
3. Use MCP tools/list or GET /api/v1/mcp/tools to confirm available tools.
4. Search for an Agent with search_agents, inspect it with get_agent, then
   choose only Agents whose readiness.callable is true when the task matters.
5. Run the Agent with run_agent, or first create a private task with create_task
   when the human gave a natural-language request and wants Skill/MCP matching
   evidence. Do not publish that task or expose its input as a public listing.
6. Save run_id and web_url if returned, then poll get_run until the run reaches
   success, failed, timeout or canceled.
7. Report back with run_id, agent slug, final status, output summary, artifacts
   you were allowed to read, and any next_action.

## Authentication

- Store the User Token in OPENLINKER_USER_TOKEN. User Tokens use the ol_user_*** prefix.
- Send it as Authorization: Bearer ol_user_***.
- Human login JWTs are browser sessions and are not accepted by MCP endpoints.
- Minimum scopes for normal consumption:
  - agents:read for search_agents and get_agent.
  - agents:run for run_agent.
  - runs:read for get_run and run event lookup.
  - tasks:create for create_task.

## OpenLinker MCP server

- Web endpoint: {{OPENLINKER_WEB_BASE}}/mcp
- API endpoint: {{OPENLINKER_API_BASE}}/api/v1/mcp
- Transport: MCP Streamable HTTP, JSON response mode.
- Methods: initialize, tools/list, tools/call.
- Tools: search_agents, get_agent, create_task, run_agent, get_run.

List tools:

` + "```bash" + `
curl -X POST {{OPENLINKER_WEB_BASE}}/mcp \
  -H 'Authorization: Bearer ol_user_xxx' \
  -H 'Accept: application/json, text/event-stream' \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
` + "```" + `

Search and run:

` + "```bash" + `
curl -X POST {{OPENLINKER_WEB_BASE}}/mcp \
  -H 'Authorization: Bearer ol_user_xxx' \
  -H 'Accept: application/json, text/event-stream' \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"search_agents","arguments":{"query":"data analysis","limit":5}}}'

curl -X POST {{OPENLINKER_WEB_BASE}}/mcp \
  -H 'Authorization: Bearer ol_user_xxx' \
  -H 'Accept: application/json, text/event-stream' \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"run_agent","arguments":{"agent_id":"AGENT_UUID","input":{"text":"Summarize this task"}}}}'
` + "```" + `

## REST equivalents

` + "```bash" + `
curl {{OPENLINKER_API_BASE}}/api/v1/agents?keyword=data

curl -X POST {{OPENLINKER_API_BASE}}/api/v1/mcp/run_agent \
  -H 'Authorization: Bearer ol_user_xxx' \
  -H 'Content-Type: application/json' \
  -d '{"agent_id":"AGENT_UUID","input":{"text":"Summarize this task"}}'

curl -X POST {{OPENLINKER_API_BASE}}/api/v1/mcp/get_run \
  -H 'Authorization: Bearer ol_user_xxx' \
  -H 'Content-Type: application/json' \
  -d '{"run_id":"RUN_UUID"}'
` + "```" + `

## Readiness and trust

Market responses include readiness:

- listed: visible in the public market.
- discoverable: has a stable slug and Agent Card.
- callable: recent availability evidence says the platform can call or a
  queued runtime worker is recently active.
- verified: benchmark evidence exists for at least one Skill.
- certified: OpenLinker reviewed the listing.

Do not treat listing as endorsement. Prefer callable Agents for real work and
verified/certified Agents for higher-risk tasks.

## State handling

Run terminal states: success, failed, timeout, canceled.
Workflow terminal states: success, failed, canceled.
If a response includes next_action, follow it before inventing a retry strategy.
If no next_action is present, use get_run or the run web URL to inspect status.

## Privacy

- Do not publish user inputs, outputs or artifacts unless the response marks
  them public or the human explicitly asks.
- Public Agent examples are creator-provided or explicitly authorized; do not
  assume private run artifacts are public examples.
`

// ServePublishAgentSkill exposes the self-registration instructions to agents and CLIs.
func ServePublishAgentSkill(c echo.Context) error {
	c.Response().Header().Set("Cache-Control", "public, max-age=300")
	return c.String(http.StatusOK, renderSkillMarkdown(c, PublishAgentSkillMarkdown))
}

// ServeConsumeAgentSkill exposes external-consumption instructions to agents and CLIs.
func ServeConsumeAgentSkill(c echo.Context) error {
	c.Response().Header().Set("Cache-Control", "public, max-age=300")
	return c.String(http.StatusOK, renderSkillMarkdown(c, ConsumeAgentSkillMarkdown))
}

func renderSkillMarkdown(c echo.Context, template string) string {
	apiBase, webBase := skillDocBaseURLs(c)
	out := strings.ReplaceAll(template, skillDocAPIBase, apiBase)
	out = strings.ReplaceAll(out, skillDocWebBase, webBase)
	return out
}

func skillDocBaseURLs(c echo.Context) (apiBase string, webBase string) {
	apiBase = trimSkillDocBaseURL(os.Getenv("API_URL"))
	if apiBase == "" {
		apiBase = requestOrigin(c)
	}
	webBase = trimSkillDocBaseURL(os.Getenv("FRONTEND_URL"))
	if webBase == "" {
		webBase = inferSkillDocWebBase(apiBase)
	}
	return apiBase, webBase
}

func requestOrigin(c echo.Context) string {
	req := c.Request()
	proto := firstHeaderValue(req.Header.Get("X-Forwarded-Proto"))
	if proto == "" {
		proto = req.URL.Scheme
	}
	if proto == "" {
		proto = c.Scheme()
	}
	if proto == "" {
		proto = "http"
	}
	host := firstHeaderValue(req.Header.Get("X-Forwarded-Host"))
	if host == "" {
		host = req.Host
	}
	if host == "" {
		return "http://localhost:8080"
	}
	return trimSkillDocBaseURL(proto + "://" + host)
}

func inferSkillDocWebBase(apiBase string) string {
	u, err := url.Parse(apiBase)
	if err != nil || u.Hostname() == "" {
		return apiBase
	}
	host := u.Hostname()
	port := u.Port()
	if strings.HasPrefix(host, "api.") {
		host = strings.TrimPrefix(host, "api.")
		port = ""
	}
	if host == "localhost" || host == "127.0.0.1" {
		port = "3000"
	} else if port == "8080" {
		port = ""
	}
	if port != "" {
		u.Host = host + ":" + port
	} else {
		u.Host = host
	}
	return trimSkillDocBaseURL(u.String())
}

func firstHeaderValue(value string) string {
	if idx := strings.Index(value, ","); idx >= 0 {
		value = value[:idx]
	}
	return strings.TrimSpace(value)
}

func trimSkillDocBaseURL(value string) string {
	return strings.TrimRight(strings.TrimSpace(value), "/")
}
