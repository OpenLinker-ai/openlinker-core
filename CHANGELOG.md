# Changelog

All notable changes to OpenLinker Core will be documented in this file.

This project is currently pre-1.0. Breaking changes may happen before the API,
runtime protocol, and migration contract are declared stable.

## Unreleased

### Workflow release drain

- Migration `103` adds a durable workflow admission state (`open` or
  `draining`) so a release can stop new workflow work before migrating.
  While draining, creating a workflow run (async, synchronous `/run`, Retry,
  step Rerun, External Execution) returns 503 `WORKFLOW_DRAINING`, and the
  worker claims no pending run; running runs finish. Creation and claiming
  read the admission row `FOR SHARE`, so a committed switch to `draining` is
  a barrier. The state survives restarts.
- Platform administrators can read and switch admission at
  `GET/POST /admin/workflow-admission`, list blocking runs at
  `GET /admin/workflow-runs?state=nonterminal|cancelling`, and cancel any
  user's run at `POST /admin/workflow-runs/:id/cancel`. The run owner stays
  the authorization subject for Runtime; workflow cancellations now record
  `requested_by` and `requested_via` (`owner`, `external` or `admin`).
  Owner-facing cancellation is unchanged.
- The workflow worker now reconciles cancellations left in `requested` or
  `stopping` (for example after a crash between the cancellation commit and
  its first reconciliation), regardless of admission or the parent run state.
- Workflow run steps are listed in a deterministic order
  (`sequence, created_at, id`), so duplicate step rows for one node always
  resolve to the same latest row.

- Migration `104` adds workflow attempts, logical node state, per-attempt
  stop records and run lease columns for the coming execution engine. It
  refuses unless every workflow run is finished and backfills history only;
  the current engine keeps running unchanged on the new schema.
- `workflow-cutover preflight|verify|reopen` (shipped in the image) checks a
  workflow release drain before and after migrating and reopens admission
  only when nothing blocks. It prints a JSON report and exits 3 when blocked.
  `drain` switches admission to draining against the release being replaced
  (schema 103) and is ready once every run finished; `abort-drain` reopens
  admission for a release abandoned before migrating.

- Workflow runs now execute under a database lease (owner, epoch, expiry
  from the database clock), renewed every 15 seconds with a 60-second TTL.
  Every executor write (steps, child launches, run outcome) locks the parent
  run and checks the lease first; Runtime checks it again before creating a
  workflow child Run, together with the launch attempt and any stop request.
  The lease is checked in a separate statement after the row lock is taken,
  because a single `SELECT ... FOR UPDATE` evaluates `clock_timestamp()`
  before its lock wait ends and would accept a lease that expired during the
  wait. Synchronous `/run` and step rerun take a lease for the run they
  create. **Behaviour change:** stale recovery uses lease expiry instead of
  `WORKFLOW_RUN_STALE_SECONDS`: an expired run goes back to pending while its
  recovery budget (`max_attempts - 1`) lasts, and otherwise fails in one
  transaction that invalidates unlaunched children and records stop requests
  for in-flight ones. An executor that lost its lease stops and writes
  nothing.
- New workflow execution engine. Every node has a logical state
  (`workflow_run_nodes`) separate from its attempts, and nodes start as soon
  as all their parents succeeded (ready-set scheduling, at most
  `WORKFLOW_RUN_PARALLELISM`, default 4, per run) instead of level by level.
  A reclaimed run resumes from its persisted attempts; steps are no longer
  deleted on retry.
- **Behaviour change:** node attempts have a deadline
  (`WORKFLOW_NODE_TIMEOUT_SECONDS`, default 1800) instead of the fixed
  60-second wait. The executor waits for the child until the deadline; a
  child that finishes after it does not count. A timed-out node is
  immediately terminal (`timeout`) and its child is stopped in the
  background. Deadlines are enforced by the worker even while the run is
  paused or has no executor.
- Attempts change state only through fixed transitions under the parent
  row lock. Settling from Runtime facts uses the child's finish time against
  the deadline and needs no lease; failures before any child exists are
  judged by the database clock (past the deadline they are timeouts). A
  logically terminal node never changes again.
- `fail_workflow` is atomic: the run fails, every open attempt is canceled,
  unlaunched children are invalidated and in-flight children get stop
  requests in one transaction. Public cancellation now also cancels open
  attempts in its transaction (**behaviour change:** such attempts show
  `canceled` instead of `failed`).
- Per-attempt stop records are driven by a stop reconciler with its own
  claim, independent of the run lease. Only the claim holder can cancel the
  child: Runtime checks the claim in its own cancellation transaction
  (`CancelWorkflowChildFenced`). Runtime classifies whether a child is known
  to have stopped (`in_progress`, `confirmed`, `unconfirmed`); any accepted
  Attempt that was canceled or ended without a definite result is
  unconfirmed and is never retried automatically.
- Workflow child Runs snapshot a single accepted execution (`max_attempts`
  1): an unaccepted offer is still redispatched, but Runtime never re-executes
  an accepted Attempt; node retries happen only in the workflow, as new
  attempts with their own launch and child Run. Node retry policies arrive
  with node policies; until then every node has one attempt.
- The worker claims runs on its tick and executes them in goroutines (at
  most `WORKFLOW_RUN_MAX_CONCURRENT`, default 8, per process). Step lists
  omit attempts that never started.
- Retry and step Rerun lock the source run and, in the same transaction,
  check every child Run the source ever referenced (launches and step
  run_ids, including children reused from earlier reruns). While a stop or
  the run's cancellation is still open they return 409
  `WORKFLOW_CHILDREN_STOPPING`; when a child cannot be shown to have stopped
  they return 409 `WORKFLOW_CHILDREN_UNCONFIRMED` unless the owner sends
  `acknowledge_unconfirmed_children: true`, which is recorded on the new run
  (`unconfirmed_children_acknowledged_by`). Runs report
  `has_unconfirmed_children`.
- **Behaviour change:** step Rerun no longer executes synchronously. It
  returns 202 with a pending run whose reused nodes are copied with their
  logical result (steps marked `source: reused`, keeping the source child
  Run and output); the worker executes the rest. A node outside the rerun
  set must have ended successfully (or skipped); otherwise 409
  `WORKFLOW_RERUN_NOT_REUSABLE`. Steps now carry `attempt`, `effective` and
  `source`.

### Security

- **Behaviour change:** the production HTTP rate limiter no longer trusts a
  client-supplied `X-Forwarded-For`. It now identifies clients the same way
  CLI login does: from `X-Forwarded-For` only when the request comes from a
  peer listed in `CLI_AUTH_TRUSTED_PROXY_CIDRS`, otherwise from the connection
  peer. Before, any client could pick its own rate-limit bucket by sending the
  header. A proxied production deployment must list its proxy peers there, or
  every client behind the proxy shares one bucket.
- Outbound endpoint validation rejects IPv6 addresses that carry a zone,
  whether given literally (for example `https://[fd00::1%25eth0]/`, including
  IPv4-mapped forms) or returned by DNS. A zone made the private-network
  blocklist miss unique local and link-local addresses.
- The Agent Runtime A2A proxy now rejects requests whose Agent creator is
  disabled or deleted, as that user's own JWT and User Tokens already are. The
  proxy acts as the creator, so a disabled account could previously still
  create Runs through it. The rejection uses Runtime's standard 403 envelope
  (`FORBIDDEN`, "Runtime permission denied").
- Creating an Agent Token for an existing Agent now checks the ten-token cap
  and inserts the token in one serialized transaction; concurrent requests
  could previously exceed the cap.
- Upgraded `github.com/labstack/echo/v4` to v4.16.3, `golang.org/x/net` to
  v0.62.0 and `golang.org/x/crypto` to v0.58.0, and build with Go 1.26.10
  (`toolchain go1.26.10`; the Docker image uses `golang:1.26.10-alpine` and
  `alpine:3.23`). `govulncheck` now reports 2 reachable advisories instead of
  12. Both are in Echo v4, which has no fixed release: GO-2026-6872 (forged
  forwarding headers choosing a rate-limit bucket) is handled by the trusted
  proxy change above, and GO-2026-6875 affects Echo's proxy middleware, which
  Core does not use. The module now requires Go 1.26.
- Outbound redirects no longer forward credentials to another origin. Agent
  endpoint (direct HTTP, MCP, dry-run, availability), webhook / task callback /
  A2A push, delivery, registry and LLM clients drop `X-OpenLinker-Token`,
  `X-OpenLinker-Internal-Token`, `Authorization`, `Proxy-Authorization`,
  `Cookie` and `Referer` once a redirect leaves the first request's scheme,
  host and port, for the rest of the chain. Hosts are compared after the IDNA
  conversions net/http dials and sends (an internationalized host whose dial
  and Host forms disagree, or that cannot be converted, counts as another
  origin) and ports as numbers. Same-origin redirects are
  unchanged; every hop is still re-validated (public addresses only, no plain
  HTTP outside opt-in loopback development). An endpoint that redirects to
  another origin now receives the request without its token; register the
  final URL.

### Changed

- **Behaviour change:** a new Agent whose request omits `visibility` is now
  `private`, both for `POST /creator/agents` and for self-registration
  (`POST /agent-registration/agents`, which used to force `public`). The Hub
  already tells creators new Agents are private, the docs say to verify while
  private, and the SDKs already send `private`; an unverified Agent should not
  be listed or callable by others by default. Explicit `public`, `unlisted` and
  `private` are unchanged, and so are existing Agents and updates that omit
  `visibility`. Callers that need a public Agent must send `visibility: "public"`.

### Fixed

- Server-Sent Event streams (Run events, A2A `message/stream` and
  `tasks/resubscribe`) are no longer cut off after the server's 120-second
  write timeout. Each write now moves the connection write deadline forward
  by 60 seconds, so a healthy stream with 15-second heartbeats stays open and
  a client that stops reading is still dropped.
- A blocking A2A `message/send` whose Run takes longer than 120 seconds now
  delivers its response; the write deadline is kept ahead while it waits.
- Task callbacks no longer miss events after an unsubscribed event type. The
  "no subscription" cache was kept per Run while subscriptions filter by event
  type, so for ten seconds after an event nobody subscribed to, events the
  Run's subscribers did want were skipped without a delivery. Misses are now
  cached per Run and event type, with bounded size, and resuming or listing
  subscriptions (including batch resume) clears the Run's cached misses on
  that replica. Other replicas still keep their misses for up to ten seconds.

- When Runtime rejects an External Execution input against the target
  capability at launch, the start is now rejected durably, under its launch
  token, as `TARGET_CONTRACT_CHANGED`.
  - Before, it returned a 503 that the caller kept retrying.
  - Every later Start and GetExecution returns the same rejection. The
    request never launches, even after the capability changes.
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
- Repointing an Agent no longer keeps evidence about the old target. When
  `connection_mode`, the normalized `endpoint_url` or the MCP tool changes (a
  Runtime Agent's target is its mode; its display endpoint is not one), the
  same transaction:
  - resets the availability snapshot to `unknown` with `last_checked_at`
    cleared, so the monitor re-checks on its next pass;
  - resets the dry-run / health-check result to `pending`;
  - moves a `certified` Agent back to `pending` review with `certified_at`
    cleared;
  - resets Skill Benchmark scores to `pending`.
- A changed or cleared endpoint credential resets only availability and dry-run;
  certification and benchmark scores are kept. Other edits leave all evidence
  intact. Run history, benchmark case results and alerts are never deleted.
- Creator dry-runs and benchmark batches that probed the previous target no
  longer store their result after the Agent is repointed. A dry-run in that
  case returns 409.
- Run results, including Runtime dry-runs and benchmark cases, update
  availability only if the Agent still has the target the Run used. The check
  runs under a share lock in the finalization transaction.
  - Runtime Runs are matched by their stored connection mode.
  - Core-executed Runs are matched by the endpoint, MCP tool and credential
    they called.
  - Core attempts that the reconciler finalizes without seeing the endpoint's
    outcome no longer change availability.
  - Call statistics are unchanged.
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

- Self-registration accepts optional `capability` and `examples` with the same
  shapes, validation and limits as the creator endpoints. They are written in
  the registration transaction; an invalid declaration creates nothing and
  leaves the token unredeemed.
- The registration response adds `agent.availability` / `agent.readiness`,
  `onboarding`, and `next_actions`: up to five ordered steps (declare
  capability, add example, start runtime worker, run health check, make public)
  in the existing Run next-action shape.
- Registration replay neither applies nor compares `capability` / `examples`,
  and reports the stored state.

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
  - Each JSON number is read as its nearest IEEE-754 double, the value
    Agents receive. Comparisons then use that double's shortest round-trip
    decimal, so `1` is a multiple of `0.01`. Numbers that round to the same
    double are equal: `0.10000000000000001` satisfies `"maximum": 0.1`.
  - A violation returns 422 `EXECUTION_INPUT_INVALID`. `details` has
    `source: input_schema`, `rule` and, when the schema declares it,
    `property`. The input is never echoed.
  - Attached executions replay unchanged. An existing pending, authorized or
    expired-launching reservation is checked only after its downstream
    recovery finds no committed work.
- Failed External Executions return a bounded `failure_category`. It is mapped
  from Core run statuses and an explicit allowlist of run error codes:
  - unlisted codes map to `unknown`;
  - a workflow uses the first failed step whose child run failed, timed out
    or was canceled; if a child run cannot be read, the category is `unknown`
    and the status response is otherwise unchanged;
  - an unconfirmed cancellation maps to `result_unknown`.
  `error_code` and `error_message` are unchanged.

### Compatibility

- Registration response fields are additive; the Go, JS and Python SDK
  registration clients ignore unknown fields. Evidence invalidation and the
  finalizer guard use existing columns; no migration.
- Migration 101 adds a concurrent partial index on published
  `runtime_signal_outbox` rows. Core deletes published signals older than seven
  days in batches of 1,000 (oldest first, `SKIP LOCKED`, safe on every
  replica); pending and in-flight signals are never deleted, and nothing is
  deleted until the index is valid. The Runtime schema contract stays at 100,
  so Nodes and readiness are unaffected; Core schema version becomes 101 with
  100 as a supported predecessor. Cloud must adopt this Core before running
  its migration against a version 101 database.
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
