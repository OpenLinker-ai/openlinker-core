# Controlled Runtime Node version changes

Schema 095 adds Core-owned authorization for replacing the binary of an enrolled
Node and for restarting an administratively drained Node. The SDK remains the
only Worker implementation. Neither this API nor credential renewal chooses,
downloads, starts or rolls back a binary. This feature does not change the Hello
wire contract, Node identity, Worker ID, credential, SDK DataDir or provider
session scope.

## Core migration

Use a coordinated Core maintenance window. Drain and stop the old Core members,
apply the normal forward migrations, start all members with schema 095 support,
and verify readiness before returning the cluster to normal mode. The runtime
schema identity advances from 80 to 95; the wire contract remains unchanged.
Old Core must fail readiness on the new schema. Mixed old/new Core
serving and a rolling schema rollout are unsupported. Migration 095 cannot be
rolled back: operation history and admission fences must survive a Node binary
rollback. Back up the database and follow the normal migration incident process
if a migration fails; do not force-clear dirty bookkeeping.

Credential issue/renewal for an existing Node can no longer change its version.
An uncontrolled change returns `RUNTIME_NODE_UPDATE_REJECTED`. Renew an expiring
certificate with the current version before an upgrade, and then use this API.
Starting an arbitrary new binary before the operation is authorized is not a
supported upgrade procedure. Fresh enrollment is a separate operation.

## Operator procedure

1. Choose a source/target pair from the release's tested binary/state matrix.
   The API requires a same-product `product/version` string, at most 100 ASCII
   characters. Other historical version formats are unsupported. Syntax
   accepted by the API is not compatibility proof.
2. Drain the whole Node through `POST /api/v1/admin/runtime/nodes/:id/drain`.
   Let all Runs, Attempts, acknowledgements and SDK spool settle under the
   existing identity and DataDir. Stop every Worker process on that Node.
3. Read `GET /api/v1/admin/runtime/nodes/:id/upgrade` using an administrator JWT.
   `eligible` must be true. This is a read-only snapshot, not a reservation.
4. POST the following six fields to the same URL, using a new UUID and the
   exact observed version/revision. Choose a deadline after `database_time`
   and no more than 15 minutes later. Actor identity comes from the JWT.

   ```json
   {
     "operation_id": "ae9be3fd-c4ce-44f2-b633-29bd81d55c42",
     "kind": "version_change",
     "expected_version": "openlinker-agent-node/v0.1.62-rc.1",
     "expected_revision": 0,
     "target_version": "openlinker-agent-node/v0.2.0",
     "deadline_at": "2026-10-05T08:10:00Z"
   }
   ```

   Replace the example values, especially the deadline; use the binary's exact
   reported version. For restart without changing the binary, use
   `restart_after_drain` and equal expected/target versions. Unknown, omitted,
   duplicate, null and trailing JSON fields are rejected. Runtime tokens,
   platform User Tokens and non-admin JWTs do not authorize either endpoint.
5. Replace/start the selected binary with the same identity, credential and
   current DataDir. One process may own a DataDir at a time. New Sessions use
   higher persisted epochs, enter `draining` with zero capacity, and must match
   the operation's Agent/Worker/credential/device and contract. Verify all
   intended Workers attached, then call the existing Node `activate` endpoint.
   Activation clears the permit; Workers not started before activation cannot
   later reuse it.
6. Verify task completion and durable result acknowledgement after activation.
   Keep the operation receipt with the exact artifact hashes and state scope.

A start that fails before activation may retry while the permit is valid, with
new Session IDs and increasing epochs. Expiry blocks new successors and offline
reattachment even if an ordinary successor query or old Core attempts it. An
already attached Session can still drain/heartbeat. Closed IDs never revive.
If the permit expires before any successor attaches, a new operation is required;
activation alone cannot recover an unattached Node. Restarting a Core member
during admission can temporarily fail the full-cluster readiness check (retryable
503). Workers already offline before drain may receive no admission and can
return with a new Session only after valid admitted Workers activate the Node.
If all Workers were offline before drain, first restart the exact current binary
with its existing DataDir: the ordinary draining-successor path can attach it.
Stop it cleanly after it has received the administrative drain, then request the
controlled operation. No version or identity change is needed for that recovery.
A new operation replaces the old permit. An unstarted version change can be
reversed using a new revision and a tested original binary; after a target has
run, settle its work and drain/stop it before requesting the reverse operation.
Keep the latest state; do not clear spool or restore older data snapshots to
make an older binary start. If it cannot read that state, stop and recover with
a compatible version through a new controlled operation.

## Preconditions, errors and capacity

The Node must be administratively draining with no live Sessions, inflight
counters, unfinished/unreleased Attempts or attached transports. Candidate
Workers require a valid Runtime credential and durable device binding, current
protocol/contract and the exact same advertised features (including `session_drain`), and an administratively
drained offline/closed predecessor. Revoked or incompatible terminal history is
not an admission candidate. Invalid offline identities are fenced without receiving a successor
admission; they cannot block valid sibling Workers. Latest offline generations are fenced on success; historical version/epoch
fields are never rewritten.

The complete Core cluster must be normal and ready: exactly `expected_replicas`
live members (heartbeat within 15 seconds), one release version/commit, schema
095 checksum and current wire identity, including the serving member. Do not
reduce the expected member count to bypass this check. POST holds the cluster
control shared lock through commit, serializing with entry into maintenance.

GET reports the scoped Session count, admission count, all-Node inflight,
unfinished Attempts, live attachments and blocker codes. `session_count` counts
all live Sessions, the latest offline generation and latest eligible terminal
Session per Worker, up to 10001. Old terminal epochs and superseded offline
generations do not accumulate toward
the 10000 scope guard; the latter already fail the monotonic-generation check.
This limit is a safety bound, not a latency promise; unbounded history still
costs database work. More than 10000 live/latest candidate rows requires operational
investigation, not bypassing the limit or deleting identity history.

Each service call has a total 10-second budget. POST has at most three attempts,
only for changed Session scope, sharing that budget; each lock wait is bounded
by two seconds. Database timing and lock contention return retryable HTTP 503
`RUNTIME_NODE_OPERATION_BUSY`. Other principal/eligibility/CAS failures return
409, including `RUNTIME_NODE_NOT_DRAINING`, `RUNTIME_NODE_REVOKED`,
`RUNTIME_NODE_NOT_QUIESCENT`, `RUNTIME_NODE_IDENTITY_INVALID`,
`RUNTIME_NODE_CORE_NOT_READY`, `RUNTIME_NODE_SCOPE_LIMIT`,
`RUNTIME_NODE_REVISION_CONFLICT`, `RUNTIME_NODE_SESSION_CHANGED`,
`RUNTIME_NODE_OPERATION_EXPIRED` and `RUNTIME_NODE_OPERATION_CONFLICT`.

After a timeout, replay the same six fields and operation ID with the same
administrator identity. A committed identical request returns the original
receipt with `replayed: true`, even after expiry; it does not extend permission.
Changing any field or the actor while reusing the ID is a conflict. Never infer
rollback from a timeout or blindly create a second operation. Revisions only
increase, including A→B→A transitions.

## Reproducible gates and support limits

Run PostgreSQL tests against disposable databases with the current migrations:

```sh
RUNTIME_NODE_UPGRADE_CAPACITY=1 go test -race -v ./pkg/runtime -run '^TestRuntimeNodeControlled' -count=1
RUNTIME_NODE_UPGRADE_CAPACITY=1 RUNTIME_NODE_UPGRADE_CAPACITY_LOAD=1 go test -v ./pkg/runtime -run '^TestRuntimeNodeControlledUpgrade(Capacity|SupersededHistory)$' -count=1
```

The load profile inserts 1/100/10000/10001 history rows for one or many Workers,
uses the production schema/indexes, adds 2 ms delay to each database read/write,
and runs two independent normal Run create/claim/ACK/finalize loops. A separate
100-Worker / 100000-epoch case proves superseded offline history stays preserved
and cannot resume. The tests report sample counts, P95/P99/max for GET, POST and normal Runs. With 20 samples, P99 equals the observed maximum. Concurrent Runs use unrelated
Nodes, measuring database-wide interference rather than same-Node contention.
This controlled profile is not a production SLA or a substitute for sizing a specific deployment.
Contention tests exercise a two-second lock wait, ten-second execution budget,
changed-scope retry, maintenance serialization, rollback and lock release.
Migration tests verify fresh and released predecessor schemas. Root integration
owns the actual old/new Node binary and SDK DataDir compatibility matrix;
provider-specific state is supported only to the extent recorded there. This
Core API does not certify arbitrary historical binaries or native client login
state, install new software, or switch running Agents.
