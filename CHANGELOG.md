# Changelog

All notable changes to OpenLinker Core will be documented in this file.

This project is currently pre-1.0. Breaking changes may happen before the API,
runtime protocol, and migration contract are declared stable.

## Unreleased

### Added

- Added automatic first-admin bootstrap on Core startup when no admin user
  exists, plus an idempotent `./api bootstrap-admin` command for manual repair.

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
