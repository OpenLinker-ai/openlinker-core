# Changelog

All notable changes to OpenLinker Core will be documented in this file.

This project is currently pre-1.0. Breaking changes may happen before the API,
runtime protocol, and migration contract are declared stable.

## Unreleased

### Fixed

- When Runtime rejects an External Execution input against the target
  capability at launch, the start is now rejected durably, under its launch
  token, as `TARGET_CONTRACT_CHANGED`.
  - Before, it returned a 503 that the caller kept retrying.
  - Every later Start and GetExecution returns the same rejection. The
    request never launches, even after the capability changes.
  - A retry after an expired launch fence now re-launches instead of
    returning 500.
- Runtime dispatch no longer loses a wake to a WebSocket Session that cannot
  claim. Wake tokens are coalesced per eligibility class; a Session that is full
  or still holds an unacknowledged offer hands a real wake to another listening
  Session of the same class and parks until a capacity wake, progress or its
  20s heartbeat. A Session makes one bounded claim right after Ready, so work
  that predates attachment no longer waits for the 1-minute reconciler.
  Transient claim database errors retry with 100ms–5s backoff instead of
  closing the WebSocket; authentication, conflict, contract and maintenance
  closes are unchanged, and claims not driven by a real signal never close.
  Such background claims check for a free slot before locking a queued Run, so
  they cannot make a sibling Session skip it; Pull and signalled claims keep
  their existing path and responses.
  The deadline reconciler now isolates a failing candidate instead of stopping
  the batch. No protocol message, API, configuration or migration changed.
- The availability monitor probes each direct HTTP/MCP Agent once per staleness
  window across Core replicas (per-Agent advisory lock), and stores a result
  only if the Agent was not edited during the probe; any guard or write failure
  stores nothing and raises no alert.
- Saving Agent settings no longer reverts a concurrent visibility or connection
  change made elsewhere; the read, carry-over, validation and update run in one
  transaction.
- Retrying self-registration with the same Agent Token and slug after a lost
  response returns the original `{agent, agent_token}` (201) instead of 401.
  This applies only to tokens redeemed by that registration; every other case is
  unchanged. A database error while looking up the token now returns 500
  instead of 401.
- Agent Token `last_used_at` is recorded again for Runtime v2. It stopped
  updating when the v1 execution paths were removed, so every token showed as
  never used. Runtime authentication records it, and established WebSocket
  connections refresh it from their heartbeat loop, including when healthy
  Redis leases skip PostgreSQL heartbeats. Writes happen at most once every
  five minutes per token, as for User Tokens, and wait at most about one second
  when another transaction holds the token row.

- New MCP calls to queued Runtime Agents now receive an atomic Core-owned
  conversation mapping, enabling native adapters that require trusted session
  authority. Independent calls get separate contexts; retries preserve the
  original Run. Application input and pre-existing idempotency fingerprints
  stay unchanged; committed historical Runs are not rewritten.

### Added

- Expose lifecycle, binding limit, and host compatibility hints on the owner-only
  Skill binding read response without changing binding admission or Run snapshots.

- Public Skill package directories and immutable published version links, exact
  bundle downloads, verified private-copy imports, and explicit withdrawal.
- Scoped MCP service endpoints and public service discovery, retaining Core
  authorization and the existing Run lifecycle.
- Publisher-supplied source, license and release notes, stored separately from
  execution bundles. Skill declarations freeze on first publication; MCP owner
  edits use revision conflicts. Catalog filters run before pagination.
- External Execution target validation returns `unavailable_detail`
  (`property`, `rule`, `capability_value`) with `input_schema_incompatible`.
  - It names the first capability rule the check could not prove.
  - It is returned to the target owner only. Start rejections keep
    `TARGET_CONTRACT_CHANGED` and carry no detail.
- External Execution targets whose capability uses `const`, `minLength`,
  `maxLength`, `pattern`, numeric bounds or `multipleOf` are executable when
  the caller's `input_schema` declares an equal or stricter keyword. Some
  rules can also be proven from a listing `enum`.
- External Execution start enforces the caller's own `input_schema` on the
  input before anything is reserved.
  - It covers type, enum, required, additionalProperties and items with
    Core's run-time semantics. It also checks the keywords above: lengths in
    code points, Go RE2 patterns (one that does not compile rejects), and
    numbers.
  - Numbers compare by their exact decimal JSON text, so `1` is a multiple of
    `0.01`.
  - A violation returns 422 `EXECUTION_INPUT_INVALID`. `details` has
    `source: input_schema`, `rule` and, when the schema declares it,
    `property`. The input is never echoed.
  - Attached executions replay unchanged. An existing reservation is checked
    only after its downstream recovery finds no committed work.
- Failed External Executions return a bounded `failure_category`. It is mapped
  from Core run statuses and an explicit allowlist of run error codes:
  - unlisted codes map to `unknown`;
  - a workflow uses the first failed step whose child run failed, timed out
    or was canceled;
  - an unconfirmed cancellation maps to `result_unknown`.
  `error_code` and `error_message` are unchanged.

### Compatibility

- Schemas 096/097 are forward-only. Coordinate Core and Web rollout; legacy empty
  publication requests remain valid and freeze empty display information.
  Currently published versions receive empty declarations, while historically
  withdrawn versions first freeze declarations on their next publication.
- Publisher declarations are unverified and excluded from bundle SHA-256 and
  imported private copies. Existing runtime, identity and session scopes remain
  unchanged. Public MCP metadata excludes private upstream configuration.
- Ordinary Run, A2A, MCP and Workflow input validation is unchanged. The new
  enforcement applies only to External Execution starts.
- No External Execution target that was executable before becomes
  unavailable.
- New External Execution starts now fail if their input breaks their own
  `input_schema`. Before, Core passed that input on to Runtime. Example: `null`
  or `""` for an optional enum field.


## 0.3.0 - 2026-10-05

### Added

- Administrator-only controlled Runtime Node version changes and restart after
  drain, with revision CAS, immutable operation receipts, expiring per-Worker
  successor admissions and database enforcement. Original Node/Worker/credential
  and DataDir remain authoritative; closed Session IDs never revive.
- Fail-closed PostgreSQL migration, authorization, lifecycle, contention and
  capacity gates, including normal Run execution under database delay/load.

### Compatibility

- Schema 095 advances the Runtime schema identity from 80 to 95 without changing
  the wire contract. Upgrade Core in a coordinated maintenance window. Mixed
  Core releases and rollback of this durable migration are unsupported.
- Replacing an enrolled Node binary without an authorized version operation is
  unsupported. Credential renewal can no longer silently change `node_version`;
  it returns `RUNTIME_NODE_UPDATE_REJECTED`. The target must keep the same
  protocol, contract and advertised feature set. See the
  [operator guide](docs/runtime-node-upgrade.md) and exact binary/state matrix.
- The upgrade scope supports at most 10000 live/latest candidate Session rows.
  Closed epochs and superseded offline generations stay preserved outside the
  lock/fence scope; the existing monotonic-generation check prevents their reuse.
  Whole-Node quiescence still includes all historical inflight/Attempt/attachment
  evidence. The larger-history capacity gate uses 100 Workers and 100000 epochs.

## 0.2.1 - 2026-10-04

### Fixed

- Core shutdown closes the HTTP server that actually owns the listener and
  rejects new Runtime authentication before draining WebSockets. Existing HTTP
  requests finish before attach-only Sessions are detached for handoff.
  Long polls and event streams inherit the process cancellation context.
- Runtime authentication lookup failures return retryable `SERVICE_UNAVAILABLE`
  instead of permanent `UNAUTHORIZED`, preserving Worker process Sessions across
  temporary database failures. Missing, revoked, expired and mismatched
  credentials remain rejected; SDK authentication behavior is unchanged.

## 0.2.0 - 2026-10-04

The following accumulated changes were already included in the `v0.2.0`
baseline. Version 0.2.1 adds no database migration or Runtime contract change.

- Add private, immutable skill package versions and owner-managed Agent bindings
  in schema 093. Runs retain version references; compatible Runtime hosts receive
  private contents through authenticated assignment delivery. Offline queuing is
  preserved, and incompatible hosts return a generic service-unavailable error.
- Add exact public capability detail lookup independently of catalog pagination.

- Pin the verified Go SDK module at `63fc87d73406`, adopting the gRPC
  1.83.2 and protobuf dependency updates while retaining the SDK Go 1.25 baseline.
  Command, credential, session and persistent-state contracts are unchanged.

### Fixed

- Runtime cancellation delivery now retries confirmed Run lock contention in
  WebSocket and HTTP long polling. A skipped row no longer consumes the only
  control wake; retries release transaction locks, back off and stop when the
  candidate disappears/expires or the transport exits. Empty queues still wait
  for signals. PostgreSQL regressions cover system event writers and stop ACKs
  without a second wake or cancellation reaper.
- Runtime claims no longer skip newly created Runs merely because a concurrent
  Workflow/message attachment holds a foreign-key reference lock. Claim locks
  still exclude competing claims and writers. PostgreSQL/WebSocket regressions
  cover concurrent Agents sharing a Node, capacity release and reference locks
  without starting the compensating dispatch scan.
- Explicit workflow `edges: []` now runs independent nodes in parallel, including
  saved definitions. Omitted/null request edges retain the sequential default,
  persisted as explicit edges. Existing empty-edge multi-node definitions also
  adopt parallel semantics: add explicit links if a sequential chain is wanted.
  Their external execution contract hashes change so old service snapshots must
  be revalidated before dispatch, rather than silently changing an ordered job.
- Initial Runtime dispatch events now commit atomically with Run creation and
  the signal outbox, preventing post-notification event row locks from making
  `SKIP LOCKED` discovery miss a new Run until the recovery scan.

### Added

- WebSocket cancellation contention emits a retry counter through the optional
  Worker observer and structured production logs. Logs report the first retry,
  at most one update per five seconds during contention, and the final count
  when retrying stops or the connection closes; no Run payload or key is logged.
- Dispatch compensation logs include up to 32 Agent IDs and distinguish queued
  from coalesced wake hints. Counts refer to Agent queues, not dispatched Runs;
  truncated evidence is explicitly marked and includes no payloads or credentials.
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
