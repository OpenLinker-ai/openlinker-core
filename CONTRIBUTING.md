# Contributing to OpenLinker Core

Chinese documentation: [CONTRIBUTING.zh-CN.md](./CONTRIBUTING.zh-CN.md)

Thanks for helping improve OpenLinker Core. This repository is the open-source
backend boundary for the Agent registry, runtime gateway, A2A/MCP surfaces, and
self-hosted Core APIs.

## Development Setup

```bash
docker compose up -d postgres redis
cp .env.example .env
make migrate-up
make test
```

Use placeholder secrets in local examples. Never commit a real `.env` file,
Agent Token, OAuth secret, customer payload, or private endpoint.

## Scope Boundaries

Keep Core usable without hosted cloud services.

Allowed here:

- auth, sessions, User Tokens, Agent Tokens, registry, runs, A2A, MCP, tasks, workflows,
  delivery, and local admin APIs
- migrations and storage needed by those open-source surfaces
- SDK contract updates that describe open-source Core behavior

Out of scope:

- wallet balances, charges, withdrawals, Stripe, pricing, and billing
- hosted marketplace ranking, commercial dashboards, and managed Hosted
  account products
- private abuse, certification, or recommendation internals

## Pull Request Expectations

- Keep changes focused and explain the user-visible behavior.
- Include tests for API, runtime, migration, or protocol behavior changes.
- Update README, SDK contracts, or examples when public behavior changes.
- Keep generated files consistent with the source files that produce them.
- Redact tokens and private URLs from logs, screenshots, and fixtures.

## Checks

Run the most relevant checks before opening a PR:

```bash
gofmt -w .
go test ./...
go test ./... -race -cover
go vet ./...
```

Repository helpers:

```bash
make test
make fmt
make demo-a2a
make runtime-loadtest
```

If a check needs external services or credentials, document what you skipped
and why.

Integration tests skip themselves when their environment is missing, so a green
`make test` is not the same as a complete run. `make test-integration` fails
loudly instead, and needs all four:

```bash
TEST_DATABASE_URL=postgres://…/openlinker
RUNTIME_V2_TEST_DATABASE_URL=postgres://…/runtime_v2
CUTOVER_RETIREMENT_TEST_DATABASE_URL=postgres://…/cutover_retire
TEST_REDIS_URL=redis://…/0
```

Each database must be disposable and at the current schema; the last two are
truncated and rewritten by the tests that use them.

## Migrations

- Add forward and rollback migrations together.
- Keep migrations deterministic and safe for repeatable local testing.
- Mention destructive or long-running migrations in the PR description.

## Security

Do not open public issues for vulnerabilities. Follow [SECURITY.md](./SECURITY.md).

## License

By contributing, you agree that your contribution is licensed under the
Apache-2.0 license used by this repository.
