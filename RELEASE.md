# Release Process

Chinese documentation: [RELEASE.zh-CN.md](./RELEASE.zh-CN.md)

OpenLinker Core releases are cut from `main` after CI and local release gates
pass. Until the public API is stable enough for strict semantic versioning,
document notable changes under `Unreleased` in `CHANGELOG.md` and use tags only
when maintainers intentionally publish a release.

## Pre-Release Checklist

1. Confirm `README.md`, `CONTRIBUTING.md`, `SECURITY.md`, `SUPPORT.md`, and
   examples are current.
2. Confirm `CHANGELOG.md` describes user-visible changes, migrations, and
   compatibility notes.
3. Run `docker compose config -q`.
4. Run `make test`.
5. Run `go test ./...` if `make test` was narrowed locally.
6. Run `go vet ./...` or `make fmt`.
7. Run a current-source secret scan on a clean checkout, for example
   `gitleaks dir --redact .`.
8. Confirm generated artifacts, `.env` files, coverage output, local binaries,
   and temporary logs are not tracked.
9. Confirm production notes do not suggest `ALLOW_LOCAL_HTTP_ENDPOINTS=true`.

## Tagging

Use semantic version tags when the API is stable enough for versioned consumers:

```bash
git tag v0.x.y
git push origin v0.x.y
```

Pre-1.0 releases may include breaking changes, but they must be called out in
`CHANGELOG.md`.
