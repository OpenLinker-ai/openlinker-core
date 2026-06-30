# Release Process

This repository is released from `main` after CI and local release gates pass.

Before tagging a release:

1. Confirm `README.md`, `CHANGELOG.md`, `SECURITY.md`, and examples are current.
2. Run `docker compose config -q`.
3. Run `make test`.
4. Run a current-source secret scan with `gitleaks dir --redact .`.
5. Confirm generated artifacts, `.env` files, coverage, and local binaries are
   not tracked.

Tag releases with semantic versions once the public API is stable enough for
versioned consumers. Until then, document notable changes under the
`Unreleased` section in `CHANGELOG.md`.
