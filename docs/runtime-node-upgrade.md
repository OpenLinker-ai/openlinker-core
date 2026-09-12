# Controlled Runtime Node upgrades

Status: documentation-only candidate; not implemented on the PR's `main` base
(`7fd39eb6e7624a0638dffd36ea641248c48833d6`). This document is extracted from
local candidate `b28c75e` with the `21eec42` documentation follow-up; neither the
implementation nor migration 093 is included in this documentation PR. The API,
error codes and limits below describe that deferred candidate, not current
`main` or an available release. Do not run the operator sequence against current
`main`. Historical candidate checks are not rerun or certified by this PR.

Status: unreleased. This describes the candidate **same-Node controlled upgrade**
API, not a published Node build or a completed live migration. Its release gates
apply only to that capability. They are not prerequisites for the separately
approved pre-1.0 test prereleases using a new NodeID, new unbound credential and
new SDK DataDir; that fresh-enrollment path does not use this API.

## Authority and scope

Only an enabled Core administrator with a current user-session JWT may inspect
or create an upgrade operation. Agent owners, User Tokens, Runtime credentials,
and `agent:pull` permission do not grant this authority. A Node can host several
Agents or Workers; one Agent's permission is not whole-Node authority. Hosted
owners need operator assistance in this first version.

| Kind | Intended change |
| --- | --- |
| `version_change` | Change the exact implementation version within the same product, using an expected version and monotonically increasing revision. |
| `restart_after_drain` | Keep the exact version and authorize a new successor after an administratively drained Worker stopped. |

Neither changes the Node/Agent/Worker identity, authentication mode, credential,
device certificate/public key, Runtime protocol, contract or features. This is
not a contract upgrade or cross-product migration. CLI-to-Node migration still
needs its separately approved new Node identity.

The enrolled certificate serial is a stable Session identity, distinct from a
rotating mTLS leaf serial. Normal renewal of the same Node/key remains valid;
the actual presented leaf is still verified by the mTLS authenticator. Do not
mistake an expired original leaf for revocation of an otherwise valid renewed
credential, or change the enrolled serial to follow leaf rotation.

An old Session is never reopened. The controlled path admits a **new** Session
ID with a strictly higher epoch, in `draining` state with zero capacity. Existing
Admin activation remains a separate guarded operation. WebSocket disconnection
ordinarily leaves `offline`, while normal pull/HTTP close may leave `closed`.
Unpermitted `closed` predecessors remain rejected. Do not force WebSocket, edit
Session rows or clear SDK state to bypass that rule.

## HTTP contract

Both routes use the existing ordinary Core Admin API listener:

```text
GET  /api/v1/admin/runtime/nodes/{node_id}/upgrade
POST /api/v1/admin/runtime/nodes/{node_id}/upgrade
```

Do not send an Agent Token here. Keep JWTs out of shell history, screenshots,
committed request files and acceptance reports.

GET returns `node_id`, `current_version`, `revision`, `status`, `database_time`,
`eligible`, `blockers`, `session_count`, `unfinished_attempt_count`,
`attached_count`, `candidate_count`, and optional `active_operation_id`.
This is a read-only snapshot, not a reservation: POST repeats every safety
check. Core counts do not prove local process exit or empty local spools.
Responses do not include credentials or task/session payloads.

POST requires `Content-Type: application/json` and exactly these six fields:

| Field | Meaning |
| --- | --- |
| `operation_id` | Nonzero UUID for this logical operation; reuse only for identical retries. |
| `kind` | `version_change` or `restart_after_drain`. |
| `expected_version` | Exact enrolled version, not a guessed release label. |
| `expected_revision` | Integer from 0 through `9223372036854775806`, read with the version; explicitly present zero is valid. |
| `target_version` | Exact successor version; unchanged for `restart_after_drain`. |
| `deadline_at` | RFC3339 deadline authorized against PostgreSQL time, not the caller or HTTP clock. |

Versions use `product/version`, are unpadded and at most 100 bytes, and must keep
the exact same product. The product matches `[a-z][a-z0-9._-]*`; the version
matches `[A-Za-z0-9][A-Za-z0-9._+\-]*`. This syntax alone is not a tested release
compatibility matrix: the API neither compares semantic-version ordering nor
maintains a built-in supported-version list. Accepting a version string or an
operation does not establish upgrade or downgrade support. The deadline must be
in the database's future and no more than
15 minutes away. The body is limited to 4 KiB. Unknown, duplicate, missing or
null fields, extra JSON values and malformed values are rejected. There is no
`actor_user_id`, credential, contract or `process_stopped` override: the actor
comes from the JWT and authoritative state is re-read.

A successful receipt contains `operation_id`, `node_id`, `kind`,
`previous_version`, `target_version`, `revision`, `admitted_worker_count`,
`deadline_at`, `created_at` and `replayed`. An identical retry may return a
replayed receipt without another version change or success audit. A receipt
does **not** prove a successor connected, became ready or was activated.

Errors use Core's `{ "error": { "code": "…", "message": "…" } }` envelope.
A conflict is not permission to blindly generate another operation ID. Read
current state and reconcile whether the intended operation committed. After an
uncertain response, retry the same intent and ID; do not convert uncertainty
into a second operation. Expired/conflicting operations require reconciliation
against actual durable state. Handler responses are `private, no-store`.

Relevant conflict codes include `RUNTIME_NODE_REVISION_CONFLICT` (refresh the
version/revision), `RUNTIME_NODE_OPERATION_CONFLICT` (an operation ID or revision
was used for another intent), `RUNTIME_NODE_OPERATION_EXPIRED` (deadline outside
the allowed window), and the read-side blocker codes. For example,
`RUNTIME_NODE_NOT_QUIESCENT`, `RUNTIME_NODE_DRAIN_EVIDENCE_REQUIRED`,
`RUNTIME_NODE_IDENTITY_INVALID`, `RUNTIME_NODE_NO_SUCCESSOR`, and
`RUNTIME_NODE_CORE_NOT_READY` require resolving that particular safety condition,
not relaxing a check. `RUNTIME_NODE_SCOPE_LIMIT` requires operator investigation
of the bounded whole-Node history instead of checking only one Worker.

## Coordinated Core release and eligibility

Use a coordinated hard cutover, not a mixed-version rolling service deployment.
Migration 093 changes the current Runtime schema identity from 80 to 93 while
retaining historical rows; the unchanged Runtime wire does not make an old
schema-80 Core ready against the new catalog. Its readiness fails with
`schema_contract_mismatch`. Fence existing work, apply the coordinated migration
and replace the old Core processes through the existing cutover procedure;
do not relax readiness or upgrade checks to keep old and new processes serving
the same upgraded database.

The upgrade gate requires cluster mode `normal`, the live-member count exactly
equal to `expected_replicas`, and this Core's own fresh membership. A member is
live when its heartbeat is within the database's 15-second window, including
the cutoff. Every live member must match the current schema version/checksum
and Runtime contract ID/digest, be ready and not draining; all must report one
identical `(release_version, release_commit)` pair. The current schema catalog
must also match the compiled migration identity.

For an otherwise inspectable Node, cluster ineligibility is reported by a
successful GET with `eligible=false` and `RUNTIME_NODE_CORE_NOT_READY` among its
blockers. A POST that reaches eligibility checking returns the **first** blocker
as HTTP 409. `RUNTIME_NODE_CORE_NOT_READY` is therefore typical when other
conditions are satisfied, not the guaranteed error for every incompatible
cluster. Neither response advertises support for mixed-version operation.

## Operational budgets and capacity evidence

Each GET inspection or POST mutation has one 10-second service context budget.
POST makes at most three total attempts sharing that budget, and retries only a
changed Session scope. Its PostgreSQL `lock_timeout=2s` limits each lock wait,
not the whole transaction. Lock and execution timeout failures can return
HTTP 500 / `INTERNAL_ERROR`; they are not the separate 15-minute admission
deadline rejection, HTTP 409 / `RUNTIME_NODE_OPERATION_EXPIRED`.

Session history is read with `LIMIT 10001`; POST adds `FOR UPDATE`. Only after
reading does a count above 10000 cause HTTP 409 / `RUNTIME_NODE_SCOPE_LIMIT`.
There is no preliminary COUNT or pre-scan rejection: the mutation can acquire
those row locks before rejecting, and the result limit does not bound physical
database scanning. The 10000 limit is a defensive cap, **not a demonstrated
capacity SLO**. Existing small-Worker and concurrency tests provide no
large-history performance evidence; keep the current cap provisional rather
than replacing it with another unmeasured constant.

Before releasing this Core capability, measure 1, representative, 10000 and
10001 historical Sessions, covering both many epochs of one Worker and many
Workers. Include realistic database latency and lock contention; verify timeout
failure atomicity, lock release, GET/POST P95/P99 and impact on existing task
traffic. Set the supported capacity or change the implementation from those
results, not from the configured timeouts alone.

## Supported operator sequence

1. Verify source binary, embedded SDK and Runtime contract against the release's
   tested matrix. Confirm Admin access and credential validity for the entire
   rollback window. Historical hardcoded Node versions do not prove SDK identity.
2. Complete the coordinated Core schema/control-plane release. New and old Core
   processes must not concurrently serve an upgrade-enabled database. Follow
   migration, cluster-membership and readiness gates.
3. Enter the approved bounded maintenance window. Drain through the existing
   Admin route, prove the expected Sessions reached the server drain fence, and
   let accepted work settle.
4. Stop the managed Worker. Independently prove process exit and empty local
   assignment/event/result spools without exposing contents. GET must also show
   the whole Node quiescent: no unfinished Attempts, active/draining Sessions,
   inflight work or attached connections.
5. POST the appropriate exact-version/revision operation. A rejected operation
   must not change the version, authorize a successor or record a success.
6. Start only the intended binary with the same identity and SDK DataDir for a
   same-Node operation. Verify a new higher-epoch draining Session, then call the
   existing Admin activate route and verify its result.
7. Run bounded smoke and continuation checks. Record operation metadata,
   versions, hashes and counts, not credentials or raw Provider Session IDs.

Never run two Workers against one DataDir, restore an old SDK snapshot, delete
spool records, revive a closed Session or edit version columns. On failure
before activation, preserve the latest durable generation and follow controlled
successor retry rules rather than resurrecting its parent.

A reverse version change is a **new operation at a higher revision**, followed
by a new higher-epoch Session, not an undo. First settle accepted work from the
currently admitted identity's own DataDir and stop that Worker. There is no
general downgrade promise: the exact reverse version pair must have verified
compatibility with both SDK state and the Provider's private state. If the old
binary cannot read the newer state, stop the downgrade, preserve the data and
use a compatible version for controlled recovery. Do not clear DataDir, restore
an old SDK/Provider snapshot or roll back Core schema 093 as a workaround. Node
or credential revocation is not upgrade rollback; do not promise immediate
restoration after revocation.

## Release gates and limitations

No verified release compatibility matrix is currently supplied for this
candidate. Before claiming same-Node upgrade or downgrade support, publish
exact tested source/target binary digests, SDK versions, Runtime protocol and
contract/features, plus SDK and Provider-private-state compatibility for each
version pair. Candidate binaries can be tested before publication. An unlisted
pair is unsupported; API acceptance is not a substitute for this evidence.
This requirement does not reinstate a Core-upgrade dependency for the independent
fresh-enrollment test prerelease path described above.

Required evidence covers real database CAS/idempotency and failure atomicity,
old-Session fencing, multi-Worker scope, successors from `offline` and permitted
`closed`, failure before activation and retry, Admin/Owner/Runtime-token
authorization, and unchanged unpermitted-closed rejection. Exercise supported
SDK builds over **both WebSocket and pull**, plus automatic fallback. Fake
handler tests are not transport or real-Provider evidence.

Schema gates must prove fresh and predecessor upgrades converge, incompatible
old Core processes are rejected, and the Hosted migration consumer works.
Local builds or a successful POST do not imply a published binary, deployment,
daily-use Agent migration or real Provider continuation.
