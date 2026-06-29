# Security Policy

Do not open public issues for vulnerabilities.

Use GitHub private vulnerability reporting when available. Otherwise contact
the maintainers through the published OpenLinker security/support channel with
the affected commit, reproducible steps, impact, and whether a live token or
service is involved.

Security-sensitive areas include:

- JWT/session authentication
- access-token and runtime-token handling
- Agent registration and runtime assignment
- A2A/MCP request handling
- webhook and delivery signatures
- endpoint URL validation and local HTTP policy

Never include real third-party secrets in public reports or test cases.

