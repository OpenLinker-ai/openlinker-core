# OpenLinker Core

OpenLinker Core is the open-source control plane for registering, finding, and
running Agents. A self-hosted deployment gets one run model across REST, SDK,
MCP, and A2A calls, plus routing to public endpoints, remote MCP servers, and
Agents connected from local or private networks.

Core runs independently with its own Web UI, database, and deployment policy.

Chinese documentation: [README.zh-CN.md](./README.zh-CN.md)

## Status

OpenLinker Core is pre-1.0 software. The runtime model is usable, but API
details, SDK contracts, migrations, and operational defaults can still change.
Pin commits or release tags for deployments, and read `CHANGELOG.md` before
upgrading.

Scoped User Tokens are part of the open-source Core product contract for
user-initiated REST, SDK, MCP, and A2A calls. This revision still relies on the
optional external verifier for `ol_user_*` tokens; local issuance and
verification are the next implementation slice.

## Scope

Included:

- user authentication and JWT sessions
- scoped User Token contract for user-side API and protocol calls
- Agent registry, visibility, categories, skills, and benchmarks
- Agent Tokens for self-registration and runtime access
- run creation, run state, event streams, artifacts, and messages
- direct HTTP, MCP server, runtime WebSocket, and runtime pull invocation modes
- A2A JSON-RPC / HTTP+JSON surfaces, Agent Card support, and optional gRPC
- MCP HTTP entrypoints and REST fallback APIs
- task, workflow, delivery, webhook, and local admin APIs
- self-hosted deployment support with Postgres and Redis

Hosted product boundary:

- wallet balances, charges, withdrawals, and Stripe flows
- hosted marketplace ranking and commercial dashboard composition
- managed account, token-policy, and commercial access dashboards
- official certification, recommendation, and abuse-policy internals

These services stay in the hosted product layer and are not Core dependencies.

## Open-source Architecture

The open-source repositories use Core as the shared registry and run control
plane. Hosted deployments can attach an optional bridge at the Core API
boundary, but closed product modules are intentionally not part of this diagram.

```mermaid
flowchart LR
  CoreWeb["openlinker-core-web<br/>self-hosted UI"] -->|"REST / session APIs"| Core
  SDKs["openlinker-js / openlinker-go<br/>client and runtime SDKs"] -->|"HTTP / A2A / MCP bindings"| Core
  MCPCaller["MCP or A2A caller"] -->|"tool call / message/send"| Core

  HostedBridge["Hosted Bridge<br/>optional deployment adapter"] -.->|"authorized Core APIs"| Core

  Core["openlinker-core<br/>auth / registry / runs / events"]

  Core -->|"direct_http"| HTTPAgent["Public HTTPS Agent"]
  Core -->|"mcp_server"| MCPAgent["Remote MCP / JSON-RPC server"]
  Core -->|"runtime_ws / runtime_pull"| AgentNode["openlinker-agent-node"]
  AgentNode -->|"http / command / a2a / codex adapter"| Backend["Agent backend"]
  Backend -->|"events / result"| AgentNode
```

## Quick Start

Prerequisites:

- Go 1.25 or newer
- Docker or a local Postgres and Redis installation
- `make`

Start dependencies:

```bash
docker compose up -d postgres redis
```

Create local configuration:

```bash
cp .env.example .env
```

Set at least these values in `.env`:

```bash
DATABASE_URL=postgres://dev:dev@127.0.0.1:5432/openlinker?sslmode=disable
JWT_SECRET=replace-with-32-byte-random-secret
FRONTEND_URL=http://localhost:3000
ALLOW_LOCAL_HTTP_ENDPOINTS=true
```

Generate a development secret with:

```bash
openssl rand -hex 32
```

Apply migrations and run the API:

```bash
make migrate-up
make run
```

The default API origin is `http://localhost:8080`.

Health check:

```bash
curl http://localhost:8080/healthz
```

## Initial Admin Bootstrap

After migrations are applied, Core checks whether any active admin user exists.
If not, it creates the default bootstrap admin during normal API startup:

- Email: `admin@openlinker.ai`
- Display name: `OpenLinker Admin`
- Default password: `openlinker-admin`

Set `OPENLINKER_BOOTSTRAP_ADMIN_PASSWORD` to override the default password for
the first startup. If an admin already exists, startup bootstrap is skipped and
the password is not reset.

The manual repair command remains available:

```bash
make bootstrap-admin
```

It is idempotent. If the configured email already exists, it promotes that user
to admin and updates the password.

Change the default password immediately after first login.

## Configuration

Required in normal deployments:

- `DATABASE_URL`
- `JWT_SECRET`
- `FRONTEND_URL`

Common optional values:

- `REDIS_URL`
- `API_URL`
- `OAUTH_CALLBACK_BASE_URL`
- `OAUTH_ALLOWED_FRONTEND_ORIGINS`
- `OAUTH_SESSION_SECRET`
- `GOOGLE_OAUTH_CLIENT_ID` / `GITHUB_OAUTH_CLIENT_ID` (OAuth login)
- `GOOGLE_OAUTH_CLIENT_SECRET` / `GITHUB_OAUTH_CLIENT_SECRET`
- `ALLOW_LOCAL_HTTP_ENDPOINTS` — set `true` for local development
- `RUNTIME_ENDPOINT_RUN_*` — run timeout worker tuning

### LLM configuration (optional, for task routing and benchmarks)

When no LLM is configured, task routing falls back to keyword matching. To
enable LLM-assisted routing and skill benchmarks:

```bash
# Option A: any OpenAI-compatible API (self-hosters, Ollama, Azure, etc.)
LLM_OPENAI_URL=https://api.openai.com/v1
LLM_OPENAI_API_KEY=sk-...
LLM_OPENAI_MODEL=gpt-4o-mini       # optional, default is gpt-4o-mini

# Option B: internal proxy (openlinker.ai cloud deployment only)
LLM_COMPLETE_URL=http://internal-llm-proxy/complete
```

Option A takes effect when `LLM_COMPLETE_URL` is empty. Option B is only useful
for the private cloud deployment of openlinker.ai.

### Optional hosted-bridge environment variables

These variables connect Core to private hosted services. They are not the local
User Token implementation. Self-hosted deployments should leave them empty
unless they intentionally operate a compatible external service.

| Variable | Purpose | Self-host |
|----------|---------|----------|
| `USER_TOKEN_VERIFY_URL` | Delegates `ol_user_*` verification to an external service in the current revision | Leave empty unless using that bridge; local User Token issuance and verification are being added separately |
| `OPENLINKER_INTERNAL_TOKEN` | Shared secret between Core and private cloud services (LLM proxy, token verifier) | Leave empty |

## Common Commands

```bash
make help              # list Makefile targets
make deps              # download and tidy Go modules
make build             # build bin/api
make run               # build and run with .env
make test              # go test ./... -race -cover
make fmt               # gofmt and go vet
make migrate-up        # apply migrations
make migrate-down      # roll back one migration
make demo-a2a          # run the local A2A demo against a running API
make runtime-loadtest  # run runtime_ws/runtime_pull load checks
```

## Runtime Modes

Use the simplest reachable mode for each Agent:

1. `direct_http`: Core calls a stable HTTPS Agent endpoint.
2. `mcp_server`: Core calls an existing remote HTTP JSON-RPC or MCP endpoint.
3. `runtime_ws`: Agent Node opens an outbound WebSocket and receives assigned
   runs. This is preferred for local, private-network, and NAT Agents.
4. `runtime_pull`: fallback long-poll mode when WebSocket is unavailable.

Every assigned or claimed run must finish with exactly one terminal result.

## Invocation Architecture

Core separates caller-facing protocol bindings from callee-facing Agent
connection modes. Callers always enter Core first; Core then routes the run to
the target Agent according to `connection_mode`.

```mermaid
flowchart TB
  subgraph CallerBindings["Caller-facing bindings"]
    REST["REST / SDK<br/>POST /run, GET /runs/:id"]
    MCP["MCP tools<br/>search_agents, run_agent, get_run"]
    A2AHTTP["A2A JSON-RPC / HTTP+JSON<br/>message/send, message:send"]
    A2AGRPC["A2A gRPC<br/>optional SendMessage, SubscribeToTask"]
  end

  Core["OpenLinker Core<br/>auth, registry, run state, events, artifacts"]

  REST --> Core
  MCP --> Core
  A2AHTTP --> Core
  A2AGRPC --> Core

  subgraph CalleeModes["Callee connection modes"]
    Direct["direct_http<br/>Core calls HTTPS endpoint"]
    MCPServer["mcp_server<br/>Core calls remote JSON-RPC / MCP tool"]
    RuntimeWS["runtime_ws<br/>Agent Node holds outbound WebSocket"]
    RuntimePull["runtime_pull<br/>Agent Node long-polls"]
  end

  Core --> Direct
  Core --> MCPServer
  Core --> RuntimeWS
  Core --> RuntimePull
```

Important rules:

- A2A bindings are external caller-facing transports. They are not the private
  Agent Node runtime channel.
- `message/send` creates a real Core run. Synchronous endpoints may complete
  immediately; runtime connectors normally return a working task first.
- `runtime_ws` is outbound from Agent Node to Core. Callers never connect
  directly to Agent Node.
- `runtime_pull` uses the same run state and result path as `runtime_ws`, but
  claims work through heartbeat and long-poll HTTP endpoints.

## API Areas

- `/api/v1/auth/*`
- `/api/v1/me`
- `/api/v1/agents`
- `/api/v1/agent-registration/*`
- `/api/v1/agent-runtime/*`
- `/api/v1/runs`
- `/api/v1/runs/:id/stream`
- `/api/v1/a2a/*`
- `/api/v1/mcp`
- `/api/v1/skills`
- `/api/v1/tasks`
- `/api/v1/workflows`
- `/api/v1/delivery/*`
- `/api/v1/admin/*`

The exact contract is still being stabilized through SDK contract files and
tests.

## Testing

```bash
go test ./...
go test ./... -race -cover
```

The parent workspace also contains cross-repository validators for SDK,
runtime, and A2A flows.

## Security

- Do not log or expose plaintext Agent Tokens.
- Do not pass Agent Tokens to backend subprocesses.
- Keep `ALLOW_LOCAL_HTTP_ENDPOINTS=false` in production.
- Use HTTPS for public `direct_http` and `mcp_server` endpoints.
- Rotate any token that was printed, committed, or shared outside the intended
  trust boundary.

Report vulnerabilities through [SECURITY.md](./SECURITY.md), not public issues.

## Contributing

Read [CONTRIBUTING.md](./CONTRIBUTING.md) before opening a pull request. Keep
Core independent from commercial Cloud modules and update SDK contracts or tests
when changing public behavior.

## Support and Releases

- Help and issue guidance: [SUPPORT.md](./SUPPORT.md)
- Release checklist: [RELEASE.md](./RELEASE.md)
- Notable changes: [CHANGELOG.md](./CHANGELOG.md)
- Conduct expectations: [CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md)

## License

Apache-2.0. See [LICENSE](./LICENSE).
