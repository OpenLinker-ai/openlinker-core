# Changelog

All notable changes to OpenLinker Core will be documented in this file.

This project is currently pre-1.0. Breaking changes may happen before the API,
runtime protocol, and migration contract are declared stable.

## Unreleased

### Added

- Added automatic first-admin bootstrap on Core startup when no admin user
  exists, plus an idempotent `./api bootstrap-admin` command for manual repair.
- Made Core the authoritative issuer, verifier, and lifecycle owner for
  fine-grained User Tokens.
- Added resource-scoped User Token grants across Agent, Run, Task, Workflow,
  MCP, A2A, and Agent Token APIs.

### Changed

- Added migration 062 to adopt legacy User Token records in place while
  preserving identifiers, hashes, prefixes, scopes, timestamps, usage, and
  revocation state. Legacy bcrypt-backed tokens remain verifiable during the
  compatibility window. Legacy `tasks:write` is narrowed to `tasks:create`; it
  does not grant `tasks:publish`, `tasks:run`, `tasks:work`, or `tasks:review`.

### Fixed

- Ignored one-character ASCII letter and digit tokens in fallback Task Skill
  parsing, preventing ordinary English prose from selecting unrelated Skills
  while preserving Chinese single-character matching.

### Documentation

- Documented the default `admin@openlinker.ai` / `openlinker-admin` bootstrap
  admin identity and the `OPENLINKER_BOOTSTRAP_ADMIN_PASSWORD` override.
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
