# Changelog

All notable changes to OpenLinker Core will be documented in this file.

This project is currently pre-1.0. Breaking changes may happen before the API,
runtime protocol, and migration contract are declared stable.

## Unreleased

### Added

- Added an Owner-controlled Browser interaction policy for private Runtime
  Agents. `full` policy is bound to an exact canonical HTTPS mutation-origin
  scope; `restricted` remains read-only.
- Added migration 091 with transactional policy staging for Browser Agents that
  have not established their first Session, plus durable policy generation and
  mutation-scope evidence on execution profiles, leases, Runs, and CallRecords.
- Added durable Runtime v2 Core membership, release/schema/contract readiness
  evidence, and GET/HEAD `/readyz` probes for single-instance and HA operation.
- Added persistent `normal`, `draining`, and `hard_maintenance` gates that
  serialize with new Run, Session, and claim transactions.
- Added the `runtime-cutover` CLI for pre-migration evidence, CAS-protected
  drain/maintenance/reopen transitions, exact replica and contract gates, and
  a read-only admin maintenance endpoint.
- Added automatic first-admin bootstrap on Core startup when no admin user
  exists, plus an idempotent `./api bootstrap-admin` command for manual repair.
- Made Core the authoritative issuer, verifier, and lifecycle owner for
  fine-grained User Tokens.
- Added resource-scoped User Token grants across Agent, Run, Task, Workflow,
  MCP, A2A, and Agent Token APIs.

### Changed

- Browser dispatch now requires Workers to declare both Browser execution and
  full-interaction capabilities when required. Policy generation and origin
  scope participate in lease fencing, so stale Attachments cannot retain or
  broaden mutation authority.
- Browser action journals remain attachment-local and emit one bounded terminal
  aggregate, preserving constant database row growth between one-action and
  500-action Runs.
- Production images now expose their release ID and Git commit to Core. HA
  readiness requires a healthy Redis signal bus, while PostgreSQL reconciliation
  continues during Redis outages.
- Added migration 062 to adopt legacy User Token records in place while
  preserving identifiers, hashes, prefixes, scopes, timestamps, usage, and
  revocation state. Legacy bcrypt-backed tokens remain verifiable during the
  compatibility window. Migration 067 removes the retired Task-market grants,
  including `tasks:write`; callers must request the explicit `tasks:create`
  permission.
- Removed the public Task board, publishing, claiming, delivery, and review
  contract, including its database columns and generated query model. Tasks are
  private demand context for recommendation and Run; seller supply remains the
  public Agent market.
- Routed Pull v2 and WebSocket execution Events through one projected append
  boundary, so durable Events update messages, artifacts, callbacks, and
  evidence consistently across both Agent Node transports.

### Fixed

- Ignored one-character ASCII letter and digit tokens in fallback Task Skill
  parsing, preventing ordinary English prose from selecting unrelated Skills
  while preserving Chinese single-character matching.
- Restored Skill recommendations for active public Agents with any valid
  readiness evidence: direct/MCP health or successful execution, or a live
  current-contract Agent Node Session. Token use alone no longer counts as
  online presence.
- Aligned the runtime pull claim test contract with idempotent in-flight Run
  replay, including the absence of a retry cooldown for successful claims.

### Documentation

- Limited the predictable bootstrap admin to local development and made
  staging/production require explicit email and a non-default strong password.
- Split Chinese documentation into dedicated `*.zh-CN.md` files and kept the
  default GitHub-facing documentation English-only.
- Strengthened the README introduction for AI agent registry, agent marketplace,
  A2A, MCP, runtime gateway, and self-hosted Agent discoverability.
- Expanded the README into an English-first open-source entry point with a
  Chinese overview, scope boundaries, quick start, configuration, runtime mode,
  architecture, testing, security, and contribution guidance.
- Expanded contributing, security, support, and release documents for public
  self-hosted use.
- Documented that wallet, Stripe, withdrawals, hosted ranking, and commercial
  dashboards are outside the Core repository boundary.

### Repository

- Added open-source governance files, issue templates, pull request template,
  and CI workflow.
- Added standalone local Postgres and Redis Compose setup for Core quick start.
- Added Apache-2.0 license, contributing guide, security policy, code of
  conduct, and support guidance.
