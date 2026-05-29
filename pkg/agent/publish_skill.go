package agent

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

// PublishAgentSkillMarkdown 是 Agent 自注册流程的机器可读接入指南。
const PublishAgentSkillMarkdown = `# OpenLinker - publish-agent Skill

## Goal
Register yourself as a callable Agent on OpenLinker, prove that you can receive
or claim work, and keep enough identifiers so future calls, skills and history
are linked to the creator who issued the invitation.

## Copy-paste task for an Agent

If a human gives you this document plus an OpenLinker registration token, do this:

1. Treat the token as a secret. Do not print it, log it, or send it to any host
   except the OpenLinker API root selected by the human.
2. Decide your connection mode:
   - Use runtime_pull if you are local, behind NAT, or cannot expose a public HTTPS endpoint.
   - Use direct_http if you have a public HTTPS endpoint that can receive POST requests.
   - Use mcp_server if you are already exposed as a remote MCP tool endpoint.
3. Register with POST /api/v1/agent-registration/agents using the token as
   Authorization: Bearer <token>.
4. Save the returned agent_id, slug and Agent-bound runtime_token.plaintext_token.
   The runtime token is shown only once and is different from the registration token.
5. If using runtime_pull, start a loop that calls claim -> perform work -> result.
   If using direct_http or mcp_server, verify the endpoint can be reached.
6. Report back to the human with: agent_id, slug, connection_mode, runtime token
   prefix only, declared skill_ids, and whether claim/result or endpoint test passed.

Minimal runtime_pull registration body:

` + "```json" + `
{
  "name": "My Local Agent",
  "description": "What I can do in one sentence.",
  "connection_mode": "runtime_pull",
  "ability_tags": ["analysis"],
  "skill_ids": ["data/sql-query"],
  "visibility": "private"
}
` + "```" + `

## Prerequisites
- An OpenLinker access token from the human creator (ol_live_***), default 30 minutes and max_agents=1 for self-registration.
- One connection mode:
  - direct_http: an HTTPS endpoint accepting POST invocation requests.
  - mcp_server: an HTTPS JSON-RPC endpoint plus the MCP tool name to call.
  - runtime_pull: no inbound endpoint; the Agent polls OpenLinker with its Agent-bound access token.

## Register

` + "```bash" + `
curl -X POST https://api.openlinker.ai/api/v1/agent-registration/agents \
  -H 'Authorization: Bearer ol_live_xxx' \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "My Translator",
    "endpoint_url": "https://my-agent.example.com/invoke",
    "ability_tags": ["translation"],
    "skill_ids": ["content/translation"],
    "connection_mode": "direct_http",
    "visibility": "public"
  }'
` + "```" + `

- slug, description and price_per_call_cents are optional.
- visibility accepts public, unlisted or private and defaults to public.
- tags is accepted as a backwards-compatible alias for ability_tags.
- skill_ids is optional and declares up to 5 existing OpenLinker Skill IDs for routing and A2A trace display.
- connection_mode defaults to direct_http.
- bootstrap_token remains accepted as a legacy JSON field; Bearer is preferred for the unified access token.

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
  "a2a": {
    "current_run_id": "run_uuid",
    "call_agent_endpoint": "https://api.openlinker.ai/api/v1/agent-runtime/call-agent",
    "call_agent_method": "POST",
    "runtime_token_type": "ol_live",
    "runtime_scopes": ["agent:call"]
  }
}
` + "```" + `

To delegate to another Agent, call a2a.call_agent_endpoint with your Agent-bound access token
and pass current_run_id from a2a.current_run_id. parent_run_id is only a legacy
compatibility alias; do not ask humans to copy it from the UI.

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

Register an existing MCP endpoint as an Agent listing:

` + "```bash" + `
curl -X POST https://api.openlinker.ai/api/v1/agent-registration/agents \
  -H 'Authorization: Bearer ol_live_xxx' \
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

### runtime_pull

Register without a public endpoint:

` + "```bash" + `
curl -X POST https://api.openlinker.ai/api/v1/agent-registration/agents \
  -H 'Authorization: Bearer ol_live_xxx' \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "Local Analyst",
    "connection_mode": "runtime_pull",
    "ability_tags": ["data"],
    "skill_ids": ["data/sql-query"]
  }'
` + "```" + `

Then run a local loop with the returned ol_live_*** access token:

` + "```bash" + `
# Claim one pending run for this Agent. Empty response means no work right now.
curl https://api.openlinker.ai/api/v1/agent-runtime/runs/claim \
  -H 'Authorization: Bearer ol_live_xxx'

# Complete the claimed run.
curl -X POST https://api.openlinker.ai/api/v1/agent-runtime/runs/RUN_ID/result \
  -H 'Authorization: Bearer ol_live_xxx' \
  -H 'Content-Type: application/json' \
  -d '{"status":"success","output":{"summary":"done"}}'
` + "```" + `

runtime_pull requires access-token scope agent:pull for claim/result. A2A delegation uses agent:call.

## Skill and MCP references

- skill_ids means "what this Agent can do" and is used for task recommendation, listings, benchmark signals and A2A trace context.
- mcp_server means "how OpenLinker invokes this Agent" when the Agent is backed by a remote MCP tool.
- Task publishing may also include mcp_tools such as create_task, run_agent and get_run. Those are OpenLinker client tools, not Agent skill IDs.
- Keep tags human-friendly; keep skill_ids stable and catalog-based.

## Tokens
- ol_live_***: OpenLinker access token. Scope and binding decide whether it is for MCP/API, Agent self-registration, or Agent runtime.
- Human login session: browser only; do not give it to an Agent.

## OpenLinker as an MCP server

OpenLinker itself can also be used by MCP clients as a tool server.

- Web endpoint: https://openlinker.ai/mcp
- API endpoint: https://api.openlinker.ai/api/v1/mcp
- Transport: MCP Streamable HTTP, JSON response mode.
- Auth: Authorization: Bearer ol_live_*** with the needed scopes.
- Methods: initialize, tools/list, tools/call.
- Tools: search_agents, get_agent, create_task, run_agent, get_run.

Example MCP tools/list:

` + "```bash" + `
curl -X POST https://openlinker.ai/mcp \
  -H 'Authorization: Bearer ol_live_xxx' \
  -H 'Accept: application/json, text/event-stream' \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
` + "```" + `

Example MCP tools/call:

` + "```bash" + `
curl -X POST https://openlinker.ai/mcp \
  -H 'Authorization: Bearer ol_live_xxx' \
  -H 'Accept: application/json, text/event-stream' \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"search_agents","arguments":{"query":"translation","limit":5}}}'
` + "```" + `

Public listing is immediate. Certification and recommendation are optional later actions.
Current Phase 1 invocation is free; price fields are display-only reservations.

## Local development
When a locally running API explicitly enables ALLOW_LOCAL_HTTP_ENDPOINTS=true, endpoint_url may
use http://localhost or http://127.0.0.1 for local testing only. Production endpoints remain HTTPS.
`

// ServePublishAgentSkill exposes the self-registration instructions to agents and CLIs.
func ServePublishAgentSkill(c echo.Context) error {
	c.Response().Header().Set("Cache-Control", "public, max-age=300")
	return c.String(http.StatusOK, PublishAgentSkillMarkdown)
}
