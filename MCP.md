# MCP surfaces

Core exposes OpenLinker to MCP clients over Streamable HTTP in JSON response
mode (`2025-06-18`), and separately invokes Agents whose connection mode is
`mcp_server`. These are different directions; this page covers the inbound
surfaces and the public `mcp_server` catalog.

## Platform endpoint

`POST /api/v1/mcp` serves `initialize`, `tools/list` and `tools/call` with nine
tools: `search_agents`, `get_agent`, `run_agent`, `start_agent_run`, `get_run`,
`list_run_events`, `list_run_artifacts`, `cancel_run` and `create_task`. REST
fallbacks remain under `/api/v1/mcp/*`. `GET /api/v1/mcp` describes the endpoint
and returns 405 when the client asks for an SSE stream. This behavior and its
tool table are unchanged by the Agent-scoped endpoint.

For both JSON-RPC endpoints, `list_run_artifacts` returns an object
`structuredContent: {items: [...]}` as required by MCP. Its text content and
REST fallback retain the existing array representation. This corrects an older
invalid array-valued `structuredContent` rejected by strict MCP clients.

## Agent-scoped endpoint

`POST /api/v1/mcp/agents/:agentId` (UUID) uses the same JSON-RPC handler and
tool dispatch, restricted to one Agent and a fixed set of six tools:
`run_agent`, `start_agent_run`, `get_run`, `list_run_events`,
`list_run_artifacts` and `cancel_run`. `GET` on the same path returns a generic
description (tool names only, no Agent data) or 405 for SSE.

Order of checks for every method, including `initialize`, `tools/list` and
notifications:

1. A User Token (`Authorization: Bearer ol_user_...`). Browser JWT sessions are
   rejected with 403 even though the route group accepts them elsewhere.
2. The Agent must be active and `public`/`unlisted`, or owned by the caller.
   For a private Agent the token must also allow `agents:run` or `agents:read`
   for that Agent ID, so an owner token narrowed to other resources cannot read
   its schema. Every failure is the same 404.

Any connection mode can be used here; only the catalog is limited to
`mcp_server`. Tool descriptions include the Agent name and slug and at most 300
characters of its description. `run_agent` and `start_agent_run` keep the
platform argument shape, with `input` nested; the Agent's capability input
schema is used for `input` only when it validates as an object schema and is at
most 16 KiB, otherwise `{"type":"object"}`. `agent_id` is optional: when present
it must equal the path ID, otherwise the call fails with JSON-RPC `-32602`
before any run starts. Core injects the path ID and then applies the ordinary
`agents:run` check for that Agent, so Agent-restricted tokens still apply.

`run_agent` may return a pending/running Run for Runtime-backed Agents. Clients
continue with `get_run` or `list_run_events`; the scoped endpoint does not add
server-side polling or change execution and terminal-state behavior.

Run tools first apply the ordinary checks (`runs:read`/`runs:cancel` grants,
including run-restricted tokens, and the existing `GetRun` caller-or-owner
policy), then require the run's `agent_id` to equal the path ID. A run of
another Agent is reported as not found, so one Agent's endpoint cannot read or
cancel another Agent's runs. Other tool names return `-32602`.

## Public catalog and detail

`GET /api/v1/mcp-services?q=&page=&size=` is unauthenticated and `no-store`. It
returns `{items: [{id, slug, name, description, connection_mode, mcp_tool_name}],
total, page, size}` for active, public `mcp_server` Agents, excluding the market's
internal/test tags. Filtering (`q` over slug, name and description, with LIKE
wildcards escaped) happens in SQL before pagination; `size` defaults to 12, max 50.

Detail stays at `GET /api/v1/agents/:slug`. For `mcp_server` Agents the public
detail, including the platform `get_agent` tool, returns an empty `endpoint_url`;
the owner path (`/api/v1/creator/agents/by-slug/:slug`) still returns the stored
address. Invocation, `direct_http` and availability data are unchanged; the
existing availability fields do not include upstream error text.

## Validation

`pkg/runtime/mcp_agent_scope_integration_test.go` runs the real User Token
middleware, MCP handler, runtime Service, PostgreSQL and a synthetic upstream
`mcp_server`. It covers JWT rejection, private schema hiding, narrowed tokens,
`agent_id` mismatch, cross-Agent run reads/cancel, run-restricted tokens,
platform regression, endpoint redaction and catalog filtering.
