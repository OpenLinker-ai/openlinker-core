# Skill packages

This source implements an optional `skill_packages.v1` execution extension.
Execution requires Core schema 093 and a compatible execution host; publication,
public reads and private-copy imports require schema 096. Source availability
does not imply that a released host, image or deployed Core supports it.

## Capability declarations and executable packages

The existing `skills` catalog and `agent_skills` declarations remain the public
vocabulary for discovery and benchmarks. A package is owner-managed content with
immutable versions and explicit Agent associations. Mapping a package to catalog
IDs does not change an Agent's capability declarations or certify its ability.

Core owns the registry, ownership checks, associations, Run snapshots and fenced
load receipts. Plugin owns package materialization and Codex/Claude execution.
The SDK's existing Worker owns delivery, cancellation, replay and recovery. This
extension does not add an execution host to Core or CLI.

## Owner API

All endpoints below are under `/api/v1/creator`, require a user JWT, and return
`Cache-Control: private, no-store`. Cross-owner resources return 404.

| Method | Path | Result |
| --- | --- | --- |
| GET | `/skill-packages` | `{items: [...]}` with version metadata |
| POST | `/skill-packages` | Create a package and its first version |
| GET | `/skill-packages/:id` | Package and version metadata, without file bodies |
| GET | `/skill-packages/:id/versions/:versionId` | The selected immutable version payload |
| POST | `/skill-packages/:id/versions` | Add an immutable version |
| PATCH | `/skill-packages/:id` | Set `{visibility: "private" \| "unlisted" \| "public"}`; returns package detail |
| PUT | `/skill-packages/:id/versions/:versionId/publication` | Publish one version (empty body or `{}`); returns package detail |
| DELETE | `/skill-packages/:id/versions/:versionId/publication` | Withdraw that version; returns package detail |
| POST | `/skill-packages/imports` | Copy a published version into a new private package |
| GET | `/agents/:id/skill-packages` | `{items, supported, providers}` |
| PUT | `/agents/:id/skill-packages/:packageId` | Pin `{version_id: "<uuid>"}` |
| DELETE | `/agents/:id/skill-packages/:packageId` | Remove the association; 204 |

Import example:

```json
{
  "version": "1.0.0",
  "files": {
    "SKILL.md": "---\nname: release-notes\ndescription: Prepare release notes from changes.\n---\nSummarize changes and their verified tests.\n",
    "references/style.md": "Group entries by feature, fix and compatibility impact.\n"
  },
  "capability_ids": ["content/summarization"],
  "providers": ["codex", "claude"],
  "required_commands": ["git"]
}
```

Package list/detail items include `visibility` (default `private`) and
`source_package_id`; version metadata includes `published_at` and
`source_version_id` (both `null` unless published or imported).

Successful imports return 201 with `{id, version_id, digest}`. Duplicate version
labels return 409; existing content cannot be overwritten. Name and description
are read from SKILL.md YAML frontmatter. Other frontmatter fields never grant
tool permissions. Importing a new version does not move existing associations.
Re-saving the same pinned version is idempotent and retains load evidence.
Binding names come from their pinned version; `latest_version_id` enables an
explicit upgrade prompt. Owners can read or remove bindings on disabled Agents;
creating or changing a binding still requires an active Agent.

Validation failures use stable `SKILL_PACKAGE_*` codes with localized Web copy,
including `FRONTMATTER_REQUIRED`, `PATH_UNSAFE`, `CAPABILITY_UNKNOWN`,
`PAYLOAD_TOO_LARGE`, `PACKAGE_LIMIT`, `BINDING_LIMIT` and `HOST_INCOMPATIBLE`.
Web folder import skips and lists hidden/system files before decoding; size
feedback includes JSON escaping and identifies the remaining manifest overhead.
Name/description limits count Unicode characters (120/2000), not UTF-8 bytes.

V1 limits: 200 packages per owner, 50 versions per package, five associations per
Agent, 32 UTF-8 text files per version, and 64 KiB for the encoded version payload.
The import request body has a separate 128 KiB limit. Files require safe relative
ASCII paths; absolute paths, traversal, dotfiles, binary content, NULs and
file/directory collisions are rejected. A package can map to five existing
capabilities and declare up to 16 plain executable names as prerequisites.
There is no marketplace ranking, binary archive import, dependency installation,
or package/version deletion API in this version.

## Publication and public reads

Packages are private by default. Publication has two independent switches:
package `visibility` and per-version `published_at`. Anonymous reads require all
of: an enabled owner (not deleted or disabled), a `public` or `unlisted`
package, and a published version. Every other state returns the same 404. The
public list contains only `public` packages; `unlisted` packages are readable by
ID. Only published versions are listed. Public names and descriptions come from
the newest published version's payload, never from the package row, which a
later private draft may rename. Publishing re-verifies the stored bytes first.

All public responses use `Cache-Control: no-store` and `nosniff`:

| Method | Path under `/api/v1` | Result |
| --- | --- | --- |
| GET | `/skill-packages?q=&page=&size=` | `{items, total, page, size}`; `size` defaults to 12, max 50; `q` matches published name/description |
| GET | `/skill-packages/:id` | `{id, name, description, visibility, versions: [{id, version, digest, capability_ids, providers, created_at, published_at}]}` |
| GET | `/skill-packages/:id/versions/:versionId` | Version metadata plus `package_id`, `visibility` and `contents` (the bundle object) |
| GET | `.../bundle.json` | The exact stored payload bytes as an attachment; `sha256sum` equals `digest` |
| GET | `.../files/*path` | One file from the verified file map by exact key, `text/plain; charset=utf-8` |
| GET | `.../archive.zip` | Deterministic ZIP of the package files only |

`bundle.json` is an archival and verification format. It is not an owner import
request and cannot be POSTed to the create endpoint unchanged. Every public
read hashes the stored payload, compares it with the stored digest and re-runs
import validation; a mismatch is a 500 and nothing is served. File lookups never
resolve paths. The ZIP stores files uncompressed in sorted order with a fixed
1980-01-01 timestamp and mode 0644, without added metadata files. Its top
directory is the SKILL.md `name` when that is lowercase letters, digits and
single hyphens, at most 64 characters and without `anthropic`/`claude`;
otherwise it is `skill-<package-id>`. SKILL.md is never rewritten. There is no
owner-only ZIP endpoint yet.

## Importing a published version

`POST /api/v1/creator/skill-packages/imports` accepts
`{source_package_id, source_version_id, expected_digest}` under the same JWT and
private no-store rules as other owner endpoints. Core locks the importer's user
row, then the source package and version (`FOR SHARE`), so withdrawal,
visibility changes and import are linearized: an import either completes before
a withdrawal commits or returns 404. Publishers lock only their own package or
version row, so the lock order cannot invert. Core requires stored hash ==
stored digest == `expected_digest` (otherwise 409 `SKILL_PACKAGE_DIGEST_MISMATCH`
or `SKILL_PACKAGE_SOURCE_INVALID`), re-runs import validation, checks the
200-package quota and capability IDs, and copies the exact payload bytes into a
new private package owned by the caller. The copy records `source_package_id`
and `source_version_id` without foreign keys, so a later withdrawal or deletion
of the source never changes it. While the source remains published and readable,
retrying the same source version returns the existing copy with **200** and the
same body; a first import returns 201. After withdrawal a new import request
returns 404, including retries; the already imported private copy remains usable.

Publication does not permit cross-owner bindings. An Agent can bind only its
owner's packages, including imported copies; the copy then uses the existing
Run snapshot, assignment and receipt path unchanged.

## Runtime assignment extension

A host advertises `skill_packages.v1` plus its supported provider feature:
`skill_packages.codex.v1` or `skill_packages.claude.v1`. Core preserves these as
optional Session features; the base Runtime protocol and its required feature
set remain unchanged. A binding requires an active owned runtime Agent and a
compatible latest non-closed/non-revoked Session. Agents using other connection
modes and Workers without this extension cannot receive package-bearing Runs.

Run creation snapshots the current associations in the same transaction as the
Run, storing only package/version/binding IDs and digest. A restrictive foreign
key prevents deletion of a referenced version. Its `(package_id, version_id)`
index supports reference checks; it does not permit deleting referenced versions
or bypass restrictions during cascading owner deletion. Retries retain those versions even
after an upgrade or association removal. Private payloads are stored once on the
immutable version, separately from caller-visible `request_metadata`.
New Run admission checks the latest known Session, including closed Sessions.
No Session history or a compatible offline/closed Session preserves the existing
offline queue policy. Only an explicitly incompatible latest Session causes
admission to return HTTP 503 `SERVICE_UNAVAILABLE`, with a generic execution
environment message that does not disclose private package configuration. The
owner-only binding API retains its specific `SKILL_PACKAGE_HOST_INCOMPATIBLE`
validation error and current compatible Session requirement.
Already queued Runs retain their snapshot and scheduler eligibility checks; a
later host change does not retroactively fail them. The owner UI shows current
host incompatibility and supports explicit/focus refresh, without idle polling.
Only authenticated assignment queries join version contents and add this reserved
metadata key:

```json
{
  "_openlinker_skill_packages": {
    "schema_version": 1,
    "bundles": [{
      "binding_id": "<association-generation-uuid>",
      "package_id": "<package-uuid>",
      "version_id": "<version-uuid>",
      "version": "1.0.0",
      "digest": "<sha256-of-exact-payload-bytes>",
      "payload": "<JSON string containing name, description, files, capability_ids, providers, required_commands>"
    }]
  }
}
```

The payload is a string to preserve exact bytes across JSON transport. An empty
snapshot has `bundles: []`. Caller-supplied reserved metadata is discarded. The
host validates identifiers, digest, paths and provider compatibility before
using content. The registry API's privacy boundary does not sandbox an Agent's
tools or guarantee that a model will never reproduce instructions in its output.
Packages are not listed publicly unless their owner sets `public` visibility and
publishes a version. Binding a public or unlisted Agent shows that callers may obtain SKILL.md and supporting files through
model output; owners must not associate content that must remain secret from those
callers. The notice also appears on existing bindings and follows visibility changes.

## Load evidence

Hosts use the existing durable event channel with `run.skill_packages.loaded` or
`run.skill_packages.failed`. Payloads contain `bindings`, an array of exact
`{binding_id, version_id, digest}` entries matching every Run snapshot entry.
Failures additionally contain `error_code`: `package_invalid`,
`package_materialization_failed`, or `dependency_missing`.

Core accepts evidence only after normal principal, Attempt, lease, fence and
sequence checks. Invalid optional receipt data does not update load evidence;
the original authenticated/fenced event is still persisted and ACKed. This keeps
SDK durable event delivery moving (a rejected event otherwise blocks later events
and results; a permanent error can stop the Worker). Database/transport failures
retain the existing retry path. All receipt entries are validated before any
binding update. Replay is idempotent. Upgrades and remove/rebind operations change the binding generation,
so an older Run cannot mark the new association loaded. For an unchanged binding,
older Runs cannot overwrite evidence from a later-created Run.

`loaded` means that the host prepared the files and instructions for execution.
It does not assert that a model used the skill, that the Run succeeded, or that a
capability benchmark passed. Invalid snapshots rejected before their identities
can be trusted fail the Run without generating a package receipt.

## Public capability detail

`GET /api/v1/skills/:category/:name` (or encoded `:id`) reads an exact catalog
identifier independently of list pagination. Both Web apps use it for capability
pages, metadata and top verified Agents from the existing `top-agents` endpoint.
Catalog enumeration for mappings and sitemap consumes every page; Core also reads
all batches before filtering/paginating the English catalog.

## Validation

`pkg/runtime/skill_packages_integration_test.go` uses PostgreSQL and the production
Run creation, assignment SQL and fenced EventStore. Its publication test covers
the private default, the visibility/publication matrix, draft-name isolation,
exact bundle bytes, file lookup, deterministic ZIP, digest mismatch, a
withdrawal holding the version lock while an import waits, copies surviving
withdrawal, and a Run -> assignment -> receipt loop on an imported copy. Plugin's
`packages/agent-adapters/agentexec/skill_packages_test.go` verifies prompt delivery
through its actual Codex/Claude process adapters using deterministic fake client
processes, version session isolation, concurrent materialization and failure paths.
The offline queue tests cover unknown history and compatible/incompatible offline
and closed Sessions through production Run creation and assignment queries.
The current Node/Plugin source supports native Codex/Claude and correctly
provisioned Plugin Provider images. Shared image caches must pass both permission
and actual-write probes before capability advertisement. Prerequisite lookup uses
the Provider environment/identity and applicable native tool read roots; it does
not install or execute dependencies. Runtime/image releases and real-model
acceptance must be recorded separately from these source-level tests.
