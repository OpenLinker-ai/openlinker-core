# Contributing to OpenLinker Core

OpenLinker Core is the open-source backend boundary for the registry and Agent
runtime. Keep Core usable without `openlinker-cloud`.

## Setup

```bash
docker compose up -d postgres redis
cp .env.example .env
make migrate-up
make test
```

## Boundary Rules

- Do not import `openlinker-cloud`.
- Keep wallet, Stripe, withdrawals, hosted marketplace ranking, and commercial
  dashboards out of Core.
- Keep API and runtime protocol changes covered by tests and SDK contract
  updates.
- Use placeholders in docs and tests. Never commit real tokens or `.env` files.

## Checks

```bash
make test
gofmt -w .
go vet ./...
```
