# Controlled migration of an existing Runtime identity

Core owns every authorization. SDK owns the one Worker, durable credential file
and transport lifecycle. Node/Plugin expose an offline preparation command and
consume the SDK; the platform CLI does not start Workers.

This capability requires schema 100 and a Core build containing this change.
Source changes alone do not mean any released binary or deployed environment
supports it. Preserve all existing isolation, capacity and authorization gates.

## Deployment and compatibility

`RUNTIME_MTLS_TRANSITION_ENABLED=false` is the default. Opting in requires
`RUNTIME_PKI_MODE=auto` and a reachable HTTPS `RUNTIME_MTLS_API_URL`. The dedicated
Core listener requires TLS 1.3 and a verified client certificate. It serves only
Runtime routes. The normal API keeps serving unmigrated token-only identities.
Global `RUNTIME_MTLS_ENABLED=true` still requires mTLS for every Runtime.

All live Core members must report the same release, schema, contract and
normalized authentication policy before an authentication operation can proceed.
A missing policy advertisement fails closed. Changing only the transition flag
or its Runtime origin also changes the member contract: use the same maintenance,
cluster-drain and readiness procedure as a version rollout. Mixed live policy
hashes temporarily make the cluster unready; do not activate Nodes or accept new
work until every live replica reports the same intended policy. Public discovery keeps
`mtls_required=false` during transition and advertises a canonical authenticated
`POST /api/v1/runtime-security-policy` endpoint. The request's `node_id` must
match the Agent Token's stored binding. The response is private and `no-store`.
Old SDKs ignore the optional endpoint and keep working until their identity is
explicitly authorized to migrate; afterward ordinary Token and delegation
requests for that identity are rejected even if a migration never completes.

The VPS template keeps 9443 unpublished. Internal deployments may use
`https://core-api:9443` with reviewed DNS/network reachability for the selected
Hosts. Enabling transition does not publish a host port or change SDK supply
identities. A public listener needs its own reviewed deployment topology.

## Operator sequence

1. Upgrade Core and consumers using the existing controlled version upgrade.
   Retain Node/Agent/credential/Worker IDs, DataDir, spool and Provider sessions.
2. Drain the exact Node. Wait for zero work and acknowledged empty spool, stop
   its Worker and verify that no second process shares the DataDir.
3. Run `runtime-credential prepare --data-dir PATH --node-id ID --agent-id ID
   --worker-id ID` using the existing Runtime data owner. The SDK locks the
   directory, validates the Worker and spool, and preserves its epoch. It emits
   only a public CSR and SPKI digest. Node reads its existing OPENLINKER_NODE_ID
   (or derives the same default from OPENLINKER_AGENT_TOKEN); Plugin reads its
   persisted node-id beside the runtime directory. These must agree with
   --node-id before a credential file is written. A prepared key alone does not pin mTLS.
   Do not run preparation as root for another UID's state or copy a private key
   into the Provider workspace. Include key readability negatives in acceptance.
4. As administrator, POST to the existing `/api/v1/admin/runtime/nodes/{id}/upgrade`
   with `kind=token_to_mtls`, a fresh `operation_id`, exact `expected_revision`,
   equal `expected_version` and `target_version`, a deadline at most 15 minutes
   away, `credential_id`, and `public_key_thumbprint` from preparation. Existing
   drain, quiescence, scope, cluster, CAS, exact-feature and capacity gates apply.
   Authentication operations cannot change version or capabilities.
5. Start the same installation. SDK queries the authenticated identity policy,
   pins its authoritative mTLS requirement before requesting a certificate, and
   sends the operation ID, CSR and exact Hello features. Core commits the first
   certificate, binding transition and immutable receipt together. A retry of
   that operation replays its exact valid certificate; it never mints another.
6. The successor Session starts draining at zero capacity. Verify identity,
   TLS certificate, features and isolation, then use the normal activation API.
   Activation clears the pending operation; later starts use ordinary renewal.

The old synthetic token-only key marker becomes the actual certificate SPKI
fingerprint. The stable Node certificate-serial anchor and all installation
identifiers stay unchanged. No SQL rebinding, deleting history, resetting spool
or impersonating a version is part of this procedure.

## Rotation and recovery

`kind=certificate_rotation` uses the same drained, stopped, empty-spool sequence,
exact version/revision/credential and current public key. It intentionally issues
a different certificate serial with the same key, without changing the system
clock or ordinary 12-hour issuance replay window. The receipt is immutable and
retryable; expired or revoked receipts cannot trigger an accidental reissuance.
Ordinary scheduled renewal remains available while the migrated Node is active.

Before any authentication authorization exists, schema 100 may be rolled back
and reapplied with the exact catalog fingerprint preserved. Once authorization
exists, the down script rejects rollback. Expiry, cancellation by a later
operation, or a lost issuance response never restores token-only access. Keep
state and the private key; drain/stop and authorize a fresh exact operation if
needed. A persisted mTLS requirement or certificate rejects missing/downgraded
policy. Do not disable TLS checks or edit credential files as recovery.

Tests cover real PostgreSQL and TLS 1.3, historical permit replay, exact Skill/MCP
features, wrong key/credential, expiry/revocation, quiescence, cluster drift,
ordinary transport fencing and migration rollback boundaries. Separate published
artifact, deployment and real-model evidence is required before declaring a
particular environment accepted.
