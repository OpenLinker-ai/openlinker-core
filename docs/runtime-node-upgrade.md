# Controlled Runtime Node version changes

Schema 095 added Core-owned authorization for replacing the binary of an enrolled
Node and for restarting an administratively drained Node. The SDK remains the
only Worker implementation. Neither this API nor credential renewal chooses,
downloads, starts or rolls back a binary. This feature does not change the Hello
wire contract, Node identity, Worker ID, credential, SDK DataDir or provider
session scope.

Schema 099 adds explicit feature-set authorization for version changes. Omitting
`target_features` preserves the enrolled set; no successor can negotiate extra
features implicitly.

## Core migration

Use a coordinated Core maintenance window. Drain and stop the old Core members,
apply the normal forward migrations, start all members with schema 099 support,
and verify readiness before returning the cluster to normal mode. The runtime
schema identity advances to 99; the wire contract remains unchanged.
Old Core must fail readiness on the new schema. Mixed old/new Core
serving and a rolling schema rollout are unsupported. Migrations 095 and 099 cannot be
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
4. POST the following six required fields to the same URL, using a new UUID and the
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

   When the selected binary changes its advertised extensions, also supply
   `target_features` with its **complete exact feature set**, including all required
   Runtime features. GET includes the current `features` and outstanding `current_operation`
   (including its recorded before/after sets); compare them with the
   release's tested target set. This optional array rejects null, duplicates,
   missing required features and more than 64 entries. Omission keeps the current
   set. A version change with an unstarted feature-changing operation requires
   an explicit target set, preventing an ambiguous reverse from inheriting the
   never-started binary's features. Same-version restart cannot add/remove features. The immutable receipt
   includes `from_features` and `target_features`; a different target set with the
   same operation ID conflicts, while reordering the same set replays safely.

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
protocol/contract and the exact authorized advertised features (including `session_drain`), and an administratively
drained offline/closed predecessor. Revoked or incompatible terminal history is
not an admission candidate. Invalid offline identities are fenced without receiving a successor
admission; they cannot block valid sibling Workers. Latest offline generations are fenced on success; historical version/epoch
fields are never rewritten.

The complete Core cluster must be normal and ready: exactly `expected_replicas`
live members (heartbeat within 15 seconds), one release version/commit, schema
099 checksum and current wire identity, including the serving member. Do not
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

## Certificate renewal after a controlled upgrade

Issuance for a new mTLS identity retains the baseline-only enrollment gate. For
an existing binding, both issuance retries and `/runtime-credentials/renew` compare
the requested version, protocol and contract to the locked Node row. Features
must be the exact approved set or the protocol baseline sent by older SDKs.
That legacy certificate format grants no feature changes: Hello and controlled
upgrades still require their exact approved sets. The approved extensions survive replay and rotation, with active
or offline Sessions and while a controlled upgrade permit is pending. Renewal
never changes Node metadata, capability grants, capacity, identity or upgrade state.
A renewal-only request cannot enroll a new Node. Use the administrative upgrade
operation for a version/feature change; arbitrary partial sets and unapproved
features are rejected. The old leaf remains valid
according to the existing certificate rotation policy.

`TestRuntimeNodeControlledUpgradeMTLSRenewal` covers HTTP enrollment and renewal,
controlled MCP feature expansion, active/offline replay, identity/feature/version
rejection, legacy SDK requests, a new signed leaf, a real mutually authenticated
TLS connection, original identity binding and exact-feature reconnect.
The controlled-upgrade CI job requires this test to pass without skipping.

Deployment order matters for mTLS: deploy the Core certificate-renewal
compatibility fix before switching Node or Plugin Host to the corresponding SDK.
An older Core rejects renewal requests containing approved extensions. Release
notes must pin the minimum compatible Core commit; schema 099 alone does not
identify this fix. Preserve the existing DataDir and private key during upgrade.

A retained local certificate is evidence of a prior enrollment. If Core loses
the associated binding/Node records during a database reset or restore, the SDK
does not silently downgrade the renewal request to a fresh enrollment. Restore
the matching Core identity state or use an explicit administrative recovery plan;
do not clear the DataDir or spool to bypass this mismatch.
