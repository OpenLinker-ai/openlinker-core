# Registry catalog

Core can show the Skills and MCP servers of one or more
[openlinker-registry](https://github.com/OpenLinker-ai/openlinker-registry)
instances, import a Registry Skill version into a user's private package
library, and register an MCP server from a Registry entry. Core talks to a
Registry over HTTP only; neither depends on the other's code.

Without `CATALOG_REGISTRIES` none of this exists: no route is mounted and
Core behaves as before. Runs never depend on a Registry: an import copies the
bytes into an ordinary immutable `skill_package_versions` row, and delivery,
Run snapshots and the `_openlinker_skill_packages` envelope are unchanged.

## Configuration

`CATALOG_REGISTRIES` is a JSON array. Core refuses to start when it is not
valid; errors never echo the token or key.

```json
[{"name": "openlinker", "url": "http://registry:8080/registry",
  "consumer": "core", "token": "olr_…", "audience": "public",
  "assertion_audience": "https://openlinker.ai/registry",
  "signing_key_id": "k1", "signing_key": "<base64 Ed25519 seed, 32 bytes>",
  "allow_http": true, "allow_private_network": true}]
```

| Field | Meaning |
|---|---|
| `name` | Short name used in Core URLs (`[a-z0-9-]`, 1–32) |
| `url` | Registry root including its base path; no credentials, query or fragment |
| `consumer`, `token` | The gateway registered at the Registry and its API token |
| `audience` | Who may use this catalog in Core: `public` (anyone), `members` (signed-in users), `admins` |
| `signing_key_id`, `signing_key`, `assertion_audience` | Optional. With them Core signs a user assertion for signed-in users, so they also see their own `internal`/`private` entries and can import indexed content whose license does not allow the Registry to redistribute it |
| `allow_http` | Allow an `http://` URL (internal network) |
| `allow_private_network` | Allow private addresses (still not unspecified, multicast or link-local such as `169.254.169.254`) |
| `writes` | Optional, default `false`. Lets signed-in users publish to the Registry through Core (see below). Needs the signing fields; the gateway token at the Registry must carry `skills:write` |

On the Registry: create a read token, register it as a gateway with the
public key of `signing_key`, and set `REGISTRY_ASSERTION_AUDIENCE` to the same
value as `assertion_audience`:

```sh
curl -H "Authorization: Bearer $ADMIN" -d '{"name":"core","scopes":["skills:read"]}' $R/v1/admin/tokens
curl -H "Authorization: Bearer $ADMIN" -d '{"consumer":"core","token_id":"<id>","identities":["openlinker","github"]}' $R/v1/admin/gateways
curl -H "Authorization: Bearer $ADMIN" -d '{"kid":"k1","public_key":"<base64 public key>"}' $R/v1/admin/gateways/core/keys
```

## Outbound policy

Requests go only to the configured origin. Addresses are checked when the
connection is made (so a name that later resolves to a private address is
refused unless `allow_private_network` is set), environment proxies are not
used, and responses are capped (1 MiB JSON,
256 KiB artifacts). The Agent endpoint policy is unchanged.

## Reading

All routes return `Cache-Control: private, no-store`. A catalog the caller's
audience does not allow is 404. An invalid `Authorization` header is 401,
never anonymous.

| Method and path | |
|---|---|
| `GET /api/v1/catalog/registries` | Catalogs available to the caller |
| `GET /api/v1/catalog/registries/{r}/skills?q=&namespace=&mode=&compatible=&limit=&cursor=` | Registry listing |
| `GET …/skills/{ns}/{name}`, `…/versions/{v}` | Detail and version (artifact URLs removed) |
| `GET …/versions/{v}/files/{path}` | One text file as `text/plain` with a sandbox CSP; 403 `CATALOG_CONTENT_WITHHELD` with `source_url` when the Registry does not redistribute it |
| `GET /api/v1/catalog/registries/{r}/mcp/servers`, `…/{ns}/{name}`, `…/versions/{v}` | MCP entries |

Answers obtained without a user assertion are cached in-process for five
minutes per catalog, audience and request; answers for a signed-in user of a
signing catalog are never cached. When the Registry cannot be reached the
answer is 502 `CATALOG_UNAVAILABLE`.

Reads follow up to three `307`/`308` redirects (a claimed entry moves to its
author's namespace) only when the new location is under the same Registry
origin and base path; each hop is a new request with a freshly signed
assertion, and an import keeps `purpose=import` and the digest check. Any other
redirect, and every redirect of a write, is `CATALOG_UNAVAILABLE`.

## Importing a Skill

`POST /api/v1/creator/skill-packages/registry-imports` (user JWT):

```json
{"registry": "openlinker", "namespace": "io.github.acme", "name": "release-notes",
 "version": "1.0.0", "digest": "sha256:…", "providers": ["codex"]}
```

Core re-reads the version (no cache), refuses yanked (409
`SKILL_PACKAGE_SOURCE_YANKED`) and incompatible entries (422
`SKILL_PACKAGE_INCOMPATIBLE`), and requires the version digest to equal
`digest` (409 `SKILL_PACKAGE_DIGEST_MISMATCH`). It downloads the artifact with
`?purpose=import`, so the Registry re-verifies indexed content against its
source, checks the sha256 itself, converts it (`skill-md`, canonical `tar`, or
a Core bundle whose digest stays the Core digest) and stores a new private
package through the same validation, limits and transaction as any import.
Empty `providers`, `capability_ids` or `required_commands` take the bundle's
values. A repeated import of the same Registry version returns the existing
copy (200); the same version with options that produce a different bundle is
409 `SKILL_PACKAGE_IMPORT_CONFLICT`. Indexed content whose license withholds
redistribution needs a signing catalog (otherwise 409
`SKILL_PACKAGE_SOURCE_UNAVAILABLE`).

Each imported version records its source (schema 107): Registry URL, entry,
version, digest, mode, license (`spdx:<id>`, `custom` or `unknown`), check
time and whether it may be redistributed (an SPDX license, or a custom
license the Registry approved). A version that may not be redistributed can
never be published, and a package holding one cannot become `unlisted` or
`public` (409 `SKILL_PACKAGE_SOURCE_NOT_REDISTRIBUTABLE`; also enforced by a
CHECK constraint). Copies made with `POST /creator/skill-packages/imports`
keep the source.

## Registering an MCP server

`GET …/mcp/servers/{ns}/{name}/versions/{v}/registration-draft[?remote=N]`
returns the fields of an `mcp_server` Agent taken from the entry's
`server.json`: endpoint, `mcp_transport` (`streamable-http` →
`streamable_http`, `sse` → `legacy_sse`) and the name of the credential
header (never a value). Entries with only `packages` are 422
`CATALOG_MCP_SELF_HOST_REQUIRED`; templated URLs are 422
`CATALOG_MCP_TEMPLATE_URL`; `deprecated` or `deleted` versions are 409
`CATALOG_MCP_INACTIVE`.

`POST …/versions/{v}/registrations` with `{"slug","name","tags","mcp_tool_name",
"endpoint_auth_header","visibility","description","remote"}` re-reads the
entry (refusing it if it became inactive since the draft), creates the Agent
through the ordinary creation path (creator role, slug, endpoint policy) and
records the source in `agent_catalog_sources` in the same transaction.
Existing Agents are never changed when a Registry entry changes.

## Publishing through Core

With `"writes": true` (and the signing fields), Core forwards these requests of
a signed-in user to the Registry with a `read`+`write` assertion for that
user. The Registry decides with the user's own roles only; the gateway token
never grants a write. All return `Cache-Control: private, no-store`; a catalog
without `writes` answers 404, an anonymous caller 401.

| Core path (under `/api/v1/catalog/registries/{r}`) | Registry path |
|---|---|
| `GET /me` | `GET /v1/whoami`, reduced to `{writes, mode, github, namespaces:[{name, role}]}` |
| `POST /namespaces` | `POST /v1/namespaces` (`ai.openlinker.<handle>` or `io.github.<login>`) |
| `POST /skills/{ns}/{name}/versions?version=&visibility=&listed=&license=` | Upload; `Content-Type` one of `text/markdown`, `application/x-tar`, `application/gzip`, `application/zip`, `application/vnd.openlinker.skill-bundle.v1+json`; at most 8 MiB |
| `POST /skills/{ns}/github-imports/preview`, `…/github-imports`, `…/indexes/preview`, `…/indexes` | Same |
| `POST /skills/{ns}/{name}/deprecate`, `…/yank`, `…/claim`, `…/versions/{v}/yank`; `PATCH /skills/{ns}/{name}` | Same |
| `POST /mcp/servers/{ns}?visibility=&listed=&indexed=` | Register or index a `server.json` |
| `POST /mcp/servers/{ns}/{name}/status`, `…/versions/{v}/status`; `PATCH /mcp/servers/{ns}/{name}` | Same |
| `POST /reports` | `POST /v1/reports` |

Only the listed paths and query parameters are forwarded; other content types
are 415 and larger bodies 413 before anything is sent. Registry refusals keep
their status (400, 403, 404, 409, 413, 415, 422, 429) with the code prefixed,
for example `CATALOG_NAMESPACE_EXISTS`, `CATALOG_FORBIDDEN`,
`CATALOG_CLAIM_NAME_TAKEN`; a Registry 401 or 5xx is 502 `CATALOG_UNAVAILABLE`.
Each user may send 30 writes a minute through one Core process (429
`CATALOG_RATE_LIMITED`). Nothing written this way changes Core's own Skill
packages, Agents or Runs.

## Projecting the public catalog

Core can publish its public Skill packages and public MCP services to one
Registry, so the Registry becomes their public catalog. Core stays the source of
truth; Workers keep receiving the same bound versions and Runs never call the
Registry.

```bash
CATALOG_PROJECTION_REGISTRY=public            # a signing entry of CATALOG_REGISTRIES
CATALOG_PROJECTION_PUBLIC_URL=https://openlinker.example
```

The Registry gateway must have projection enabled
(`PATCH /v1/admin/gateways/{consumer}` with `{"projection": true}`). Without
these variables Core adds no route, starts no worker and behaves as before.

Migration `108` adds a mode row (`local`, `registry`, `draining`), a dirty
queue, entry and version links, version errors and exclusions. In `local` mode
the triggers write nothing. In `registry` mode every change that can alter the
public catalog (package visibility, version publication, owner status, MCP
Agent visibility, lifecycle, tags and metadata) marks the subject in the same
transaction; run counters, passwords and profile edits mark nothing.

The worker sends each subject's complete desired state with a revision: first
the restrictive part (hide or withdraw), then the expanding part (new versions
and visibility). Versions are ingested once, immutably, before they become
visible. Entries are named `p-<instance>-<package>` and `m-<instance>-<agent>`
in a namespace the Registry generates per owner; packages without an SPDX
license use `LicenseRef-openlinker-platform-terms`. Drafts, private packages,
unpublished versions and non-public MCP Agents are never sent, and MCP entries
point at Core's `/mcp/agents/{id}` endpoint rather than the Agent's own URL.
An hourly reconciliation compares Registry state with Core and re-marks drift.

```text
api registry-migrate inventory      # read-only counts
api registry-migrate preflight      # validates every version with the Registry
api registry-migrate exclude --add --kind skill --subject <id> [--version <id>] --reason <text>
api registry-migrate exclude --remove --kind skill --subject <id> [--version <id>]
api registry-migrate cutover [--timeout 10m]
api registry-migrate reconcile
api registry-migrate status
api registry-migrate rollback [--timeout 10m]
```

`cutover` refuses while unexcluded preflight blockers remain, switches the mode
row to `registry` under a row lock, marks every candidate and drives delivery;
it reports incomplete while work is queued or a version error remains.
`rollback` switches to `draining`, waits for the queue, reconciles, and returns
to `local` only when nothing is pending, claimed or failing; otherwise it stays
`draining`. Neither command deletes Registry entries or Core's links.

In `registry` and `draining` mode the old public endpoints keep their answers
(decided by Core's own state) and add `Deprecation: true` and
`Link: <registry entry>; rel="alternate"` when an entry exists (built from the
catalog's optional `public_url`, else its `assertion_audience`, so it is the
Registry's public address rather than the internal `url`);
`/mcp-services` and the public metadata endpoint also hide services whose
owner is disabled. `GET /api/v1/catalog/legacy/skill-packages/{id}` and
`/catalog/legacy/mcp-services/{slug}` return `{registry, kind, namespace, name}`
for a projected, still public subject, otherwise 404.
