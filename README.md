# OpenLinker Core

OpenLinker Core is the open-source backend for the OpenLinker Agent registry
and runtime gateway. It stores Agent metadata, authenticates users and runtime
tokens, starts runs, records events and results, exposes A2A/MCP surfaces, and
supports private-network Agents through runtime WebSocket or pull connectors.

Core is designed to be reusable without the commercial Cloud modules.

## Scope

Included:

- user auth and JWT sessions
- Agent registry and visibility controls
- registration tokens and Agent-bound runtime tokens
- run creation, run state, event stream, artifacts, and messages
- direct HTTP, MCP server, runtime WebSocket, and runtime pull invocation modes
- A2A protocol endpoints and Agent Card support
- MCP HTTP entrypoints and REST fallback
- skills, benchmarks, workflows, delivery/webhook metadata
- local admin and self-hosted deployment support

Excluded:

- wallet balances, charges, withdrawals, and Stripe payment flows
- hosted marketplace ranking and commercial dashboard composition
- cloud-only API key product surfaces
- official recommendation, certification, and abuse policy internals

Those belong in `openlinker-cloud` or hosted product services.

## Quick Start

Start local Postgres and Redis:

```bash
docker compose up -d postgres redis
```

Create a local env file:

```bash
cd openlinker-core
cp .env.example .env
```

Apply migrations and run:

```bash
make migrate-up
make run
```

The API listens on `http://localhost:8080` by default.

Health check:

```bash
curl http://localhost:8080/healthz
```

## Configuration

Required:

- `DATABASE_URL`
- `JWT_SECRET`

Common optional values:

- `REDIS_URL`
- `FRONTEND_URL`
- `API_URL`
- `OAUTH_CALLBACK_BASE_URL`
- `GOOGLE_OAUTH_CLIENT_ID`
- `GOOGLE_OAUTH_CLIENT_SECRET`
- `GITHUB_OAUTH_CLIENT_ID`
- `GITHUB_OAUTH_CLIENT_SECRET`
- `API_KEY_VERIFY_URL`
- `ALLOW_LOCAL_HTTP_ENDPOINTS`

Generate a local JWT secret with:

```bash
openssl rand -hex 32
```

Keep real `.env` files out of Git.

## Commands

```bash
make help
make build
make run
make test
make migrate-up
make migrate-down
make demo-a2a
make demo-a2a-live
```

## Runtime Modes

Use the simplest reachable mode:

1. `direct_http`: Core calls a stable HTTPS Agent endpoint.
2. `mcp_server`: Core calls an existing remote HTTP JSON-RPC/MCP endpoint.
3. `runtime_ws`: Agent Node opens an outbound WebSocket and receives assigned
   runs. This is preferred for local, private-network, and NAT Agents.
4. `runtime_pull`: fallback long-poll mode when WebSocket is unavailable.

Every assigned or claimed run must end with exactly one terminal result.

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

The exact contract is still being stabilized through SDK contract files and
tests.

## Tests

```bash
go test ./...
go test ./... -race -cover
```

The parent workspace also provides protocol and closed-loop validators:

```bash
make validate-agent-self-registration
make validate-a2a-protocol-conformance
make demo-a2a
```

## Security Notes

- Do not log or expose plaintext runtime tokens.
- Do not pass runtime tokens to backend subprocesses; use Agent Node's localhost
  helper when possible.
- Keep `ALLOW_LOCAL_HTTP_ENDPOINTS=false` in production.
- Use HTTPS for public `direct_http` and `mcp_server` endpoints.
- Rotate any token that was printed, committed, or shared outside the intended
  trust boundary.

See [SECURITY.md](./SECURITY.md) for vulnerability reporting.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) and
[CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md).
Use [SUPPORT.md](./SUPPORT.md) for help and [CHANGELOG.md](./CHANGELOG.md) for
release notes.

## License

Apache-2.0. See [LICENSE](./LICENSE).
