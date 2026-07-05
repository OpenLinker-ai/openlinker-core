# Support

Chinese documentation: [SUPPORT.zh-CN.md](./SUPPORT.zh-CN.md)

Use GitHub issues for reproducible bugs, documentation problems, and feature
requests that fit OpenLinker Core's open-source scope.

## Good Issue Topics

- API or runtime behavior that differs from the README or tests
- migration, local setup, or Docker Compose problems
- A2A, MCP, runtime WebSocket, runtime pull, task, workflow, or delivery issues
- documentation gaps that block self-hosted use
- focused feature requests for open-source Core surfaces

## Before Opening an Issue

- Search existing issues and recent commits.
- Confirm the problem on the latest `main` branch or a named release.
- Include operating system, Go version, database version, and commit SHA.
- Include reproduction steps, expected behavior, actual behavior, and sanitized
  logs.
- Redact JWTs, runtime tokens, OAuth secrets, private URLs, customer data, and
  local `.env` values.

## Not Supported Here

- vulnerabilities; follow [SECURITY.md](./SECURITY.md)
- commercial billing, wallet, Stripe, withdrawal, or hosted dashboard requests
- private deployment debugging without reproducible public details
- questions that require access to private data, private logs, or live secrets

## Cross-Repository Questions

For issues that span multiple OpenLinker repositories, include:

- affected component names
- commit SHAs or package versions
- which component receives the request
- which component returns the unexpected response

This makes routing and reproduction much faster.
