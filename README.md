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

User Tokens with fine-grained permission grants are part of the open-source Core product contract for
user-initiated REST, SDK, MCP, and A2A calls. Core issues and verifies
`ol_user_*` locally, stores resource-aware Core grants, and exposes JWT-only
management under `/api/v1/user-tokens`. Hosted services can validate the same
token through Core's authenticated internal introspection endpoint.

## Scope

Included:

- user authentication and JWT sessions
- fine-grained User Token permissions for user-side API and protocol calls
- Agent registry, visibility, categories, skills, and benchmarks
- Agent Tokens for self-registration and runtime access
- run creation, run state, event streams, artifacts, and messages
- direct HTTP, MCP server, and transport-neutral Runtime Worker invocation modes
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
  SDKs["openlinker-go / openlinker-js / openlinker-python<br/>client and runtime SDKs"] -->|"HTTP / A2A / MCP bindings"| Core
  MCPCaller["MCP or A2A caller"] -->|"tool call / message/send"| Core

  HostedBridge["Hosted Bridge<br/>optional deployment adapter"] -.->|"authorized Core APIs"| Core

  Core["openlinker-core<br/>auth / registry / runs / events"]

  Core -->|"direct_http"| HTTPAgent["Public HTTPS Agent"]
  Core -->|"mcp_server"| MCPAgent["Remote MCP / JSON-RPC server"]
  Core -->|"runtime<br/>WebSocket first, long polling fallback"| RuntimeWorker["SDK Runtime Worker"]
  RuntimeWorker -->|"typed RuntimeContext"| Handler["Application handler"]

  Core -.->|"runtime<br/>optional compatibility path"| AdapterWorker["Go SDK Runtime Worker"]
  AdapterWorker --> AgentNode["Agent Node Adapter"]
  AgentNode -->|"http / command / a2a / codex"| Backend["Existing agent backend"]
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
curl --fail http://localhost:8080/readyz
```

`/healthz` is process liveness. `/readyz` also verifies the persisted cluster
mode, expected live replicas, release/schema/runtime-contract agreement, and
the Redis signal dependency in HA mode. A Redis outage makes an HA instance
not ready without stopping PostgreSQL reconciliation.

## Initial Admin Bootstrap

After migrations are applied, Core checks whether any active admin user exists.
In `local`, `dev`, `development`, or `test`, it can create the local bootstrap
admin during normal API startup:

- Email: `admin@openlinker.ai`
- Display name: `OpenLinker Admin`
- Local-only password: `openlinker-admin`

For every other `ENV` value, including staging and production, both
`OPENLINKER_BOOTSTRAP_ADMIN_EMAIL` and
`OPENLINKER_BOOTSTRAP_ADMIN_PASSWORD` must be explicitly set before Core checks
for an existing admin. The password must be 12–72 bytes and cannot equal the
local default; the email must not use a `.local` domain. Missing or unsafe
bootstrap credentials fail startup closed. If an active admin already exists,
bootstrap is skipped after validation and no password is reset.

The manual repair command remains available:

```bash
make bootstrap-admin
```

It accepts the same environment variables, plus `-env`, `-email`, and
`-password`. It is idempotent: if the configured email already exists, it
promotes that user to admin and updates the password.

Change the default password immediately after first login.

## Configuration

Required in normal deployments:

- `DATABASE_URL`
- `JWT_SECRET`
- `FRONTEND_URL`

Common optional values:

- `REDIS_URL`
- `RUNTIME_HA_MODE` — set `true` when `expected_replicas` is greater than one
- `OPENLINKER_RELEASE_ID` / `OPENLINKER_GIT_SHA` — injected by the image build;
  production rejects placeholder values
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

### User Token introspection and private-service variables

User Token issuance and verification are local Core capabilities and require no
external verifier. Hosted services that add their own incremental permissions
can introspect the same token through Core.

| Variable | Purpose | Self-host |
|----------|---------|----------|
| `OPENLINKER_INTERNAL_TOKEN` | Protects `POST /internal/user-tokens/introspect`; it may also authenticate trusted private services such as an LLM proxy | Leave empty unless exposing an internal service integration |

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
make runtime-loadtest  # exercise Runtime Worker over WebSocket and long polling
```

## Runtime Modes

Runtime cluster membership is refreshed with PostgreSQL time every five
seconds. Multi-replica deployments require `RUNTIME_HA_MODE=true`; all live
replicas must advertise the same release, schema checksum, and OpenLinker Runtime
contract before `/readyz` succeeds. The migration deliberately starts in
`hard_maintenance`, so it is never silently treated as a serving state.

Breaking Runtime migrations use the image-bundled `runtime-cutover` command.
`status` and `preflight` expose redacted JSON evidence; `drain`,
`hard-maintenance`, and `reopen` require an explicit cluster-control CAS
version, and `reopen` also requires the active cutover ID. Reopen only succeeds
when the database contract, exact live replica count, release identity, schema
checksum, and Redis HA dependency agree. The admin API exposes the same status
read-only at `GET /api/v1/admin/runtime/maintenance`; it never changes mode.

```bash
./runtime-cutover preflight --require-exclusive --require-no-members
./runtime-cutover status
./runtime-cutover reopen --expected-version=<version> --cutover-id=<uuid>
```

Use the simplest reachable mode for each Agent:

1. `direct_http`: Core calls a stable HTTPS Agent endpoint.
2. `mcp_server`: Core calls an existing remote HTTP JSON-RPC or MCP endpoint.
3. `runtime`: Runtime Worker receives assigned runs. Its transport policy is
   `auto` by default: outbound WebSocket first, long polling when the network cannot
   keep the socket alive. Both transports reuse one Session, lease, ACK, resume,
   fence, and local spool contract.

Normal Runtime Worker setup only needs `OPENLINKER_URL`, the public OpenLinker
origin. The SDK reads `/.well-known/openlinker.json` without Runtime
credentials and obtains the dedicated mTLS origin from `base_urls.runtime`.
`RUNTIME_MTLS_API_URL` is deployment-side publication metadata, not a second
address that Agent creators need to enter.

Every assigned or claimed run must finish with exactly one terminal result.

### Runtime Node certificate provisioning

Reliable OpenLinker Runtime authenticates every Runtime Worker with a dedicated client
certificate and a matching `runtime_nodes` record. Keep the client CA private
key on an operator-controlled provisioning host; never copy it into the Core
container, put it in `.env`, or mount it beside the serving keys. Core only
needs the CA certificate configured as `RUNTIME_MTLS_CLIENT_CA_FILE`.

After applying the current migrations, build the Core binary and issue a Node
identity from a host that can temporarily reach Postgres:

```bash
make build
DATABASE_URL='postgres://...' ./bin/api runtime-node issue \
  --ca-cert /secure/runtime-client-ca.crt \
  --ca-key /secure/runtime-client-ca.key \
  --display-name 'Singapore worker 01' \
  --capacity 4 \
  --cert-out ./node-pki/runtime-node.crt \
  --key-out ./node-pki/runtime-node.key
```

The CA private-key file must be owner-only (`0600` or `0400`) on Unix. The
output directories must already exist. The command generates an ECDSA
P-256 key and a client-auth-only certificate, registers its random serial and
SPKI SHA-256 thumbprint against the current OpenLinker Runtime contract, and then emits
an audit record as JSON. It refuses to overwrite any file. The private key is
written with mode `0600`; the certificate uses `0644`. `--node-id` is optional
and otherwise generated. `--node-version` defaults to
`openlinker-go/runtime-worker`. An Adapter that advertises another implementation,
such as Agent Node, must enroll with that implementation's exact version.

Inspect a delivered pair before installing it on a Runtime Worker:

```bash
./bin/api runtime-node inspect \
  --cert ./node-pki/runtime-node.crt \
  --key ./node-pki/runtime-node.key \
  --ca-cert /secure/runtime-client-ca.crt
```

Pass the JSON `node_id`, the registered capacity, the delivered certificate and
private key, and the Runtime server trust CA into the SDK `RuntimeWorker`
configuration. The optional Agent Node Adapter exposes the same values through
`OPENLINKER_NODE_ID`, `OPENLINKER_AGENT_NODE_CAPACITY`,
`OPENLINKER_AGENT_NODE_MTLS_CERT_FILE`, `OPENLINKER_AGENT_NODE_MTLS_KEY_FILE`,
and `OPENLINKER_AGENT_NODE_MTLS_CA_FILE`. Distribute the client CA certificate
to Core only; its private key remains outside all running OpenLinker services.

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
    RuntimeWorker["runtime<br/>SDK Runtime Worker"]
  end

  Core --> Direct
  Core --> MCPServer
  Core --> RuntimeWorker
```

Important rules:

- A2A bindings are external caller-facing transports. They are not the private
  Runtime Worker channel.
- `message/send` creates a real Core run. Synchronous endpoints may complete
  immediately; runtime connectors normally return a working task first.
- `runtime` is the marketplace connection mode. WebSocket and long polling are
  transport choices inside the Runtime Worker, never separate seller-facing modes.
- WebSocket is outbound from Runtime Worker to Core. Long polling is its fallback; both
  keep PostgreSQL as truth and share the same Session, lease, ACK and resume state.

## API Areas

- `/api/v1/auth/*`
- `/api/v1/me`
- `/api/v1/agents`
- `/api/v1/agent-registration/*`
- `/api/v1/agent-runtime/*` (dedicated mTLS listener only; the ordinary API listener returns 404)
- `/api/v1/runs`
- `/api/v1/runs/:id/stream`
- `/api/v1/a2a/*`
- `/api/v1/mcp`
- `/api/v1/skills`
- `/api/v1/tasks`
- `/api/v1/workflows`
- `/api/v1/delivery/*`
- `/api/v1/admin/*`

The canonical Runtime contract is embedded in Core and mirrored byte-for-byte by
the official SDKs; tests lock its ID, protocol version, digest and feature set.

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
