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
used, redirects are not followed, and responses are capped (1 MiB JSON,
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
