-- runtime_nodes.sql
--
-- Durable Runtime Worker presence, session ownership, and Core cluster coordination.
-- Session/attachment create, resume, close and revoke methods are transaction
-- primitives. The paired rows must reach one consistent state before commit;
-- Node/Token revoke is the final step after attachments and Sessions close.
--
-- Every Session reactivation or principal revocation transaction must acquire
-- locks in this exact global order: Session -> Node -> Token -> Attachment.
-- Pass the complete scope to each phase; every query sorts UUIDs itself, so
-- callers must not rely on input-array order or lock a principal first.

-- name: HasActiveRuntimeV2SessionForAgent :one
-- Availability is PostgreSQL truth. Redis presence is only an advisory hint.
-- Keep this query parameter-free beyond agent_id so every caller applies the
-- same heartbeat and current-contract boundary.
SELECT EXISTS (
    SELECT 1
    FROM runtime_sessions s
    JOIN runtime_nodes n
      ON n.node_id = s.node_id
    JOIN agent_tokens t
      ON t.id = s.credential_id
     AND t.agent_id = s.agent_id
    JOIN runtime_schema_contracts contract
      ON contract.runtime_contract_id = s.runtime_contract_id
     AND contract.runtime_contract_digest = s.runtime_contract_digest
     AND contract.is_current
    WHERE s.agent_id = $1
      AND s.status IN ('active', 'draining')
      AND s.attached_core_instance_id IS NOT NULL
      AND s.disconnected_at IS NULL
      AND s.heartbeat_at >= clock_timestamp() - INTERVAL '45 seconds'
      AND s.protocol_version = 2
      AND s.runtime_contract_id = 'openlinker.runtime.v2'
      AND s.runtime_contract_digest = '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9'
      AND s.features @> ARRAY[
          'lease_fence',
          'assignment_confirm',
          'renew',
          'resume',
          'event_ack',
          'result_ack',
          'cancel',
          'persistent_spool'
      ]::text[]
      AND n.status IN ('active', 'draining')
      AND n.revoked_at IS NULL
      AND n.protocol_version = s.protocol_version
      AND n.runtime_contract_id = s.runtime_contract_id
      AND n.runtime_contract_digest = s.runtime_contract_digest
      AND n.device_certificate_serial = s.device_certificate_serial
      AND n.node_version = s.node_version
      AND n.features @> s.features
      AND s.features @> n.features
      AND n.last_seen_at IS NOT NULL
      -- WS Session heartbeats run every 20 seconds. Keep the Node freshness
      -- window aligned with the 45-second Session window to avoid a periodic
      -- false-offline gap between otherwise healthy heartbeats.
      AND n.last_seen_at >= clock_timestamp() - INTERVAL '45 seconds'
      AND t.status = 'active_runtime'
      AND t.revoked_at IS NULL
      AND t.scopes @> ARRAY['agent:pull']::text[]
      AND (t.expires_at IS NULL OR t.expires_at > clock_timestamp())
      AND EXISTS (
          SELECT 1
          FROM runtime_session_attachments attachment
          WHERE attachment.runtime_session_id = s.runtime_session_id
            AND attachment.core_instance_id = s.attached_core_instance_id
            AND attachment.detached_at IS NULL
      )
);

-- name: ResolveRuntimeWorkerSessionPrincipal :one
-- Transport authentication proves Node/Agent/Credential/worker. Resolve the
-- currently attached target Session from those facts; never accept an
-- Attempt's immutable source Session ID as the acting Session after resume.
SELECT s.runtime_session_id, s.node_id, s.agent_id, s.credential_id,
       s.worker_id, s.session_epoch, s.attached_core_instance_id,
       s.device_certificate_serial, n.device_public_key_thumbprint,
       s.node_version, s.protocol_version, s.runtime_contract_id,
       s.runtime_contract_digest, s.features, s.status, s.heartbeat_at,
       attachment.id AS attachment_id,
       clock_timestamp() AS database_now
FROM runtime_sessions s
JOIN runtime_nodes n ON n.node_id = s.node_id
JOIN agent_tokens t
  ON t.id = s.credential_id
 AND t.agent_id = s.agent_id
JOIN runtime_session_attachments attachment
  ON attachment.runtime_session_id = s.runtime_session_id
 AND attachment.core_instance_id = s.attached_core_instance_id
 AND attachment.detached_at IS NULL
WHERE s.node_id = sqlc.arg(node_id)
  AND s.agent_id = sqlc.arg(agent_id)
  AND s.credential_id = sqlc.arg(credential_id)
  AND s.worker_id = sqlc.arg(worker_id)
  AND s.device_certificate_serial = sqlc.arg(device_certificate_serial)
  AND n.device_public_key_thumbprint = sqlc.arg(device_public_key_thumbprint)
  AND s.attached_core_instance_id = sqlc.arg(core_instance_id)
  AND s.status IN ('active', 'draining')
  AND s.protocol_version = 2
  AND s.runtime_contract_id = 'openlinker.runtime.v2'
  AND n.status IN ('active', 'draining')
  AND n.revoked_at IS NULL
  AND n.device_certificate_serial = s.device_certificate_serial
  AND n.node_version = s.node_version
  AND n.protocol_version = s.protocol_version
  AND n.runtime_contract_id = s.runtime_contract_id
  AND n.runtime_contract_digest = s.runtime_contract_digest
  AND n.features @> s.features
  AND s.features @> n.features
  AND t.status = 'active_runtime'
  AND t.revoked_at IS NULL
  AND t.scopes @> ARRAY['agent:pull']::text[]
  AND (t.expires_at IS NULL OR t.expires_at > clock_timestamp());

-- name: CreateRuntimeSchemaContract :one
INSERT INTO runtime_schema_contracts (
    schema_version,
    migration_name,
    runtime_contract_id,
    runtime_contract_digest,
    is_current
) VALUES ($1, $2, $3, $4, $5)
RETURNING schema_version, migration_name, runtime_contract_id,
          runtime_contract_digest, applied_at, is_current;

-- name: GetCurrentRuntimeSchemaContract :one
SELECT schema_version, migration_name, runtime_contract_id,
       runtime_contract_digest, applied_at, is_current
FROM runtime_schema_contracts
WHERE is_current;

-- name: GetRuntimeSchemaContract :one
SELECT schema_version, migration_name, runtime_contract_id,
       runtime_contract_digest, applied_at, is_current
FROM runtime_schema_contracts
WHERE schema_version = $1;

-- name: ListRuntimeSchemaContracts :many
SELECT schema_version, migration_name, runtime_contract_id,
       runtime_contract_digest, applied_at, is_current
FROM runtime_schema_contracts
ORDER BY schema_version DESC
LIMIT $1 OFFSET $2;

-- name: ClaimCurrentRuntimeSchemaContract :one
SELECT schema_version, migration_name, runtime_contract_id,
       runtime_contract_digest, applied_at, is_current
FROM runtime_schema_contracts
WHERE schema_version = $1
  AND runtime_contract_id = $2
  AND runtime_contract_digest = $3
  AND is_current
FOR SHARE;

-- name: GetRuntimeClusterControl :one
SELECT singleton_id, mode, expected_replicas, cutover_id,
       drain_started_at, drain_deadline_at, hard_maintenance_at, reopened_at,
       version, updated_by_type, updated_by_id, updated_at
FROM runtime_cluster_control
WHERE singleton_id = 1;

-- name: UpsertRuntimeClusterControl :one
INSERT INTO runtime_cluster_control (
    singleton_id,
    mode,
    expected_replicas,
    cutover_id,
    drain_started_at,
    drain_deadline_at,
    hard_maintenance_at,
    reopened_at,
    updated_by_type,
    updated_by_id
) VALUES (1, $1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (singleton_id) DO UPDATE
SET mode = EXCLUDED.mode,
    expected_replicas = EXCLUDED.expected_replicas,
    cutover_id = EXCLUDED.cutover_id,
    drain_started_at = EXCLUDED.drain_started_at,
    drain_deadline_at = EXCLUDED.drain_deadline_at,
    hard_maintenance_at = EXCLUDED.hard_maintenance_at,
    reopened_at = EXCLUDED.reopened_at,
    version = runtime_cluster_control.version + 1,
    updated_by_type = EXCLUDED.updated_by_type,
    updated_by_id = EXCLUDED.updated_by_id,
    updated_at = clock_timestamp()
RETURNING singleton_id, mode, expected_replicas, cutover_id,
          drain_started_at, drain_deadline_at, hard_maintenance_at, reopened_at,
          version, updated_by_type, updated_by_id, updated_at;

-- name: ClaimRuntimeClusterControl :one
UPDATE runtime_cluster_control
SET mode = $2,
    expected_replicas = $3,
    cutover_id = $4,
    drain_started_at = $5,
    drain_deadline_at = $6,
    hard_maintenance_at = $7,
    reopened_at = $8,
    version = version + 1,
    updated_by_type = $9,
    updated_by_id = $10,
    updated_at = clock_timestamp()
WHERE singleton_id = 1
  AND version = $1
RETURNING singleton_id, mode, expected_replicas, cutover_id,
          drain_started_at, drain_deadline_at, hard_maintenance_at, reopened_at,
          version, updated_by_type, updated_by_id, updated_at;

-- name: UpsertRuntimeClusterMember :one
INSERT INTO runtime_cluster_members (
    instance_id,
    release_version,
    release_commit,
    schema_version,
    schema_checksum,
    runtime_contract_id,
    runtime_contract_digest,
    draining,
    ready
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (instance_id) DO UPDATE
SET release_version = EXCLUDED.release_version,
    release_commit = EXCLUDED.release_commit,
    schema_version = EXCLUDED.schema_version,
    schema_checksum = EXCLUDED.schema_checksum,
    runtime_contract_id = EXCLUDED.runtime_contract_id,
    runtime_contract_digest = EXCLUDED.runtime_contract_digest,
    heartbeat_at = clock_timestamp(),
    draining = EXCLUDED.draining,
    ready = EXCLUDED.ready
RETURNING instance_id, release_version, release_commit, schema_version,
          schema_checksum, runtime_contract_id, runtime_contract_digest,
          started_at, heartbeat_at, draining, ready;

-- name: GetRuntimeClusterMember :one
SELECT instance_id, release_version, release_commit, schema_version,
       schema_checksum, runtime_contract_id, runtime_contract_digest,
       started_at, heartbeat_at, draining, ready
FROM runtime_cluster_members
WHERE instance_id = $1;

-- name: ListRuntimeClusterMembers :many
SELECT instance_id, release_version, release_commit, schema_version,
       schema_checksum, runtime_contract_id, runtime_contract_digest,
       started_at, heartbeat_at, draining, ready
FROM runtime_cluster_members
ORDER BY started_at ASC, instance_id ASC;

-- name: ListLiveRuntimeClusterMembers :many
SELECT instance_id, release_version, release_commit, schema_version,
       schema_checksum, runtime_contract_id, runtime_contract_digest,
       started_at, heartbeat_at, draining, ready
FROM runtime_cluster_members
WHERE heartbeat_at >= $1
ORDER BY started_at ASC, instance_id ASC;

-- name: HeartbeatRuntimeClusterMember :one
UPDATE runtime_cluster_members
SET heartbeat_at = clock_timestamp(),
    draining = $2,
    ready = $3
WHERE instance_id = $1
RETURNING instance_id, release_version, release_commit, schema_version,
          schema_checksum, runtime_contract_id, runtime_contract_digest,
          started_at, heartbeat_at, draining, ready;

-- name: CloseRuntimeClusterMember :execrows
DELETE FROM runtime_cluster_members
WHERE instance_id = $1;

-- name: DeleteStaleRuntimeClusterMembers :execrows
DELETE FROM runtime_cluster_members
WHERE heartbeat_at < $1
  AND instance_id <> $2;

-- name: UpsertRuntimeNode :one
INSERT INTO runtime_nodes (
    node_id,
    display_name,
    device_certificate_serial,
    device_public_key_thumbprint,
    node_version,
    protocol_version,
    runtime_contract_id,
    runtime_contract_digest,
    features,
    capacity,
    last_seen_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, clock_timestamp())
ON CONFLICT (node_id) DO UPDATE
SET display_name = EXCLUDED.display_name,
    node_version = EXCLUDED.node_version,
    protocol_version = EXCLUDED.protocol_version,
    runtime_contract_id = EXCLUDED.runtime_contract_id,
    runtime_contract_digest = EXCLUDED.runtime_contract_digest,
    features = EXCLUDED.features,
    capacity = GREATEST(EXCLUDED.capacity, runtime_nodes.inflight),
    last_seen_at = clock_timestamp(),
    updated_at = clock_timestamp()
WHERE runtime_nodes.device_certificate_serial = EXCLUDED.device_certificate_serial
  AND runtime_nodes.device_public_key_thumbprint = EXCLUDED.device_public_key_thumbprint
  AND runtime_nodes.status <> 'revoked'
RETURNING node_id, display_name, device_certificate_serial,
          device_public_key_thumbprint, node_version, protocol_version,
          runtime_contract_id, runtime_contract_digest, features, capacity,
          inflight, status, last_seen_at, draining_at, revoked_at, revoke_reason,
          created_at, updated_at;

-- name: GetRuntimeNode :one
SELECT node_id, display_name, device_certificate_serial,
       device_public_key_thumbprint, node_version, protocol_version,
       runtime_contract_id, runtime_contract_digest, features, capacity,
       inflight, status, last_seen_at, draining_at, revoked_at, revoke_reason,
       created_at, updated_at
FROM runtime_nodes
WHERE node_id = $1;

-- name: GetRuntimeNodeByCertificate :one
SELECT node_id, display_name, device_certificate_serial,
       device_public_key_thumbprint, node_version, protocol_version,
       runtime_contract_id, runtime_contract_digest, features, capacity,
       inflight, status, last_seen_at, draining_at, revoked_at, revoke_reason,
       created_at, updated_at
FROM runtime_nodes
WHERE device_certificate_serial = $1
  AND device_public_key_thumbprint = $2;

-- name: ListRuntimeNodes :many
SELECT node_id, display_name, device_certificate_serial,
       device_public_key_thumbprint, node_version, protocol_version,
       runtime_contract_id, runtime_contract_digest, features, capacity,
       inflight, status, last_seen_at, draining_at, revoked_at, revoke_reason,
       created_at, updated_at
FROM runtime_nodes
ORDER BY created_at DESC, node_id DESC
LIMIT $1 OFFSET $2;

-- name: HeartbeatRuntimeNode :one
UPDATE runtime_nodes
SET node_version = $2,
    protocol_version = $3,
    runtime_contract_id = $4,
    runtime_contract_digest = $5,
    features = $6,
    capacity = GREATEST($7, inflight),
    last_seen_at = clock_timestamp(),
    updated_at = clock_timestamp()
WHERE node_id = $1
  AND device_certificate_serial = $8
  AND device_public_key_thumbprint = $9
  AND status IN ('active', 'draining')
RETURNING node_id, display_name, device_certificate_serial,
          device_public_key_thumbprint, node_version, protocol_version,
          runtime_contract_id, runtime_contract_digest, features, capacity,
          inflight, status, last_seen_at, draining_at, revoked_at, revoke_reason,
          created_at, updated_at;

-- name: ClaimRuntimeNodeSlot :one
WITH candidate AS (
    SELECT node_id
    FROM runtime_nodes
    WHERE node_id = $1
      AND status = 'active'
      AND last_seen_at >= $2
      AND inflight < capacity
    FOR UPDATE SKIP LOCKED
)
UPDATE runtime_nodes n
SET inflight = n.inflight + 1,
    updated_at = clock_timestamp()
FROM candidate
WHERE n.node_id = candidate.node_id
RETURNING n.node_id, n.display_name, n.device_certificate_serial,
          n.device_public_key_thumbprint, n.node_version, n.protocol_version,
          n.runtime_contract_id, n.runtime_contract_digest, n.features, n.capacity,
          n.inflight, n.status, n.last_seen_at, n.draining_at, n.revoked_at,
          n.revoke_reason, n.created_at, n.updated_at;

-- name: ReleaseRuntimeNodeSlot :one
UPDATE runtime_nodes
SET inflight = inflight - 1,
    updated_at = clock_timestamp()
WHERE node_id = $1
  AND inflight > 0
RETURNING node_id, display_name, device_certificate_serial,
          device_public_key_thumbprint, node_version, protocol_version,
          runtime_contract_id, runtime_contract_digest, features, capacity,
          inflight, status, last_seen_at, draining_at, revoked_at, revoke_reason,
          created_at, updated_at;

-- name: MarkRuntimeNodeDraining :one
UPDATE runtime_nodes
SET status = 'draining',
    draining_at = COALESCE(draining_at, clock_timestamp()),
    updated_at = clock_timestamp()
WHERE node_id = $1
  AND status = 'active'
RETURNING node_id, display_name, device_certificate_serial,
          device_public_key_thumbprint, node_version, protocol_version,
          runtime_contract_id, runtime_contract_digest, features, capacity,
          inflight, status, last_seen_at, draining_at, revoked_at, revoke_reason,
          created_at, updated_at;

-- name: LockRuntimeSessionsForPrincipalRevocation :many
SELECT runtime_session_id, node_id, credential_id, status
FROM runtime_sessions
WHERE status IN ('active', 'draining', 'offline')
  AND (
      node_id = ANY($1::uuid[])
      OR credential_id = ANY($2::uuid[])
  )
ORDER BY runtime_session_id ASC
FOR UPDATE;

-- name: LockRuntimeNodesForPrincipalRevocation :many
SELECT node_id
FROM runtime_nodes
WHERE node_id = ANY($1::uuid[])
ORDER BY node_id ASC
FOR UPDATE;

-- name: LockAgentTokensForPrincipalRevocation :many
SELECT id
FROM agent_tokens
WHERE id = ANY($1::uuid[])
ORDER BY id ASC
FOR UPDATE;

-- name: LockActiveRuntimeSessionAttachmentsForPrincipalRevocation :many
SELECT id
FROM runtime_session_attachments
WHERE runtime_session_id = ANY($1::uuid[])
  AND detached_at IS NULL
ORDER BY id ASC
FOR UPDATE;

-- name: RevokeRuntimeNode :one
UPDATE runtime_nodes
SET status = 'revoked',
    capacity = GREATEST(capacity, inflight),
    revoked_at = clock_timestamp(),
    revoke_reason = $2,
    updated_at = clock_timestamp()
WHERE node_id = $1
  AND status <> 'revoked'
RETURNING node_id, display_name, device_certificate_serial,
          device_public_key_thumbprint, node_version, protocol_version,
          runtime_contract_id, runtime_contract_digest, features, capacity,
          inflight, status, last_seen_at, draining_at, revoked_at, revoke_reason,
          created_at, updated_at;

-- name: CreateRuntimeSession :one
INSERT INTO runtime_sessions (
    runtime_session_id,
    node_id,
    agent_id,
    credential_id,
    worker_id,
    session_epoch,
    device_certificate_serial,
    node_version,
    protocol_version,
    runtime_contract_id,
    runtime_contract_digest,
    features,
    capacity,
    attached_core_instance_id
) SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
FROM runtime_nodes n
JOIN agent_tokens t
  ON t.id = $4
 AND t.agent_id = $3
WHERE n.node_id = $2
  AND n.device_certificate_serial = $7
  AND n.status = 'active'
  AND t.status = 'active_runtime'
  AND t.revoked_at IS NULL
  AND t.scopes @> ARRAY['agent:pull']::text[]
  AND (t.expires_at IS NULL OR t.expires_at > clock_timestamp())
RETURNING runtime_session_id, node_id, agent_id, credential_id, worker_id,
          session_epoch, device_certificate_serial, node_version,
          protocol_version, runtime_contract_id, runtime_contract_digest,
          features, capacity, inflight, status, attached_core_instance_id,
          connected_at, heartbeat_at, disconnected_at, created_at, updated_at;

-- name: GetRuntimeSession :one
SELECT runtime_session_id, node_id, agent_id, credential_id, worker_id,
       session_epoch, device_certificate_serial, node_version,
       protocol_version, runtime_contract_id, runtime_contract_digest,
       features, capacity, inflight, status, attached_core_instance_id,
       connected_at, heartbeat_at, disconnected_at, created_at, updated_at
FROM runtime_sessions
WHERE runtime_session_id = $1;

-- name: GetRuntimeSessionForUpdate :one
SELECT runtime_session_id, node_id, agent_id, credential_id, worker_id,
       session_epoch, device_certificate_serial, node_version,
       protocol_version, runtime_contract_id, runtime_contract_digest,
       features, capacity, inflight, status, attached_core_instance_id,
       connected_at, heartbeat_at, disconnected_at, created_at, updated_at
FROM runtime_sessions
WHERE runtime_session_id = $1
FOR UPDATE;

-- name: ListActiveRuntimeSessionsByAgent :many
SELECT runtime_session_id, node_id, agent_id, credential_id, worker_id,
       session_epoch, device_certificate_serial, node_version,
       protocol_version, runtime_contract_id, runtime_contract_digest,
       features, capacity, inflight, status, attached_core_instance_id,
       connected_at, heartbeat_at, disconnected_at, created_at, updated_at
FROM runtime_sessions
WHERE agent_id = $1
  AND status IN ('active', 'draining')
ORDER BY heartbeat_at DESC, runtime_session_id ASC;

-- name: ListActiveRuntimeSessionsByNode :many
SELECT runtime_session_id, node_id, agent_id, credential_id, worker_id,
       session_epoch, device_certificate_serial, node_version,
       protocol_version, runtime_contract_id, runtime_contract_digest,
       features, capacity, inflight, status, attached_core_instance_id,
       connected_at, heartbeat_at, disconnected_at, created_at, updated_at
FROM runtime_sessions
WHERE node_id = $1
  AND status IN ('active', 'draining')
ORDER BY heartbeat_at DESC, runtime_session_id ASC;

-- name: ListStaleRuntimeSessionCandidates :many
SELECT runtime_session_id, node_id, agent_id, credential_id, worker_id,
       session_epoch, device_certificate_serial, node_version,
       protocol_version, runtime_contract_id, runtime_contract_digest,
       features, capacity, inflight, status, attached_core_instance_id,
       connected_at, heartbeat_at, disconnected_at, created_at, updated_at
FROM runtime_sessions
WHERE status IN ('active', 'draining')
  AND heartbeat_at < clock_timestamp() - (sqlc.arg(heartbeat_ttl_ms)::bigint * INTERVAL '1 millisecond')
ORDER BY heartbeat_at ASC, runtime_session_id ASC
LIMIT sqlc.arg(candidate_limit);

-- name: ClaimRuntimeSessionForCore :one
WITH candidate AS (
    SELECT s.runtime_session_id, n.status AS node_status
    FROM runtime_sessions s
    JOIN runtime_nodes n ON n.node_id = s.node_id
    JOIN agent_tokens t
      ON t.id = s.credential_id
     AND t.agent_id = s.agent_id
    WHERE s.runtime_session_id = $1
      AND s.node_id = $2
      AND s.agent_id = $3
      AND s.credential_id = $4
      AND s.worker_id = $5
      AND s.session_epoch = $6
      AND s.status IN ('active', 'draining', 'offline')
      AND n.status IN ('active', 'draining')
      AND t.status = 'active_runtime'
      AND t.revoked_at IS NULL
      AND t.scopes @> ARRAY['agent:pull']::text[]
      AND (t.expires_at IS NULL OR t.expires_at > clock_timestamp())
      AND (
          s.attached_core_instance_id IS NULL
          OR s.attached_core_instance_id = $7
          OR s.status = 'offline'
      )
    FOR UPDATE SKIP LOCKED
)
UPDATE runtime_sessions s
SET status = CASE
        WHEN candidate.node_status = 'draining' OR s.status = 'draining'
            THEN 'draining'
        ELSE 'active'
    END,
    attached_core_instance_id = $7,
    heartbeat_at = clock_timestamp(),
    disconnected_at = NULL,
    updated_at = clock_timestamp()
FROM candidate
WHERE s.runtime_session_id = candidate.runtime_session_id
RETURNING s.runtime_session_id, s.node_id, s.agent_id, s.credential_id,
          s.worker_id, s.session_epoch, s.device_certificate_serial,
          s.node_version, s.protocol_version, s.runtime_contract_id,
          s.runtime_contract_digest, s.features, s.capacity, s.inflight,
          s.status, s.attached_core_instance_id, s.connected_at,
          s.heartbeat_at, s.disconnected_at, s.created_at, s.updated_at;

-- name: HeartbeatRuntimeSession :one
UPDATE runtime_sessions
SET node_version = $3,
    protocol_version = $4,
    runtime_contract_id = $5,
    runtime_contract_digest = $6,
    features = $7,
    capacity = GREATEST($8, inflight),
    heartbeat_at = clock_timestamp(),
    updated_at = clock_timestamp()
WHERE runtime_session_id = $1
  AND attached_core_instance_id = $2
  AND status IN ('active', 'draining')
  AND EXISTS (
      SELECT 1
      FROM runtime_nodes n
      JOIN agent_tokens t
        ON t.id = runtime_sessions.credential_id
       AND t.agent_id = runtime_sessions.agent_id
      WHERE n.node_id = runtime_sessions.node_id
        AND n.status IN ('active', 'draining')
        AND t.status = 'active_runtime'
        AND t.revoked_at IS NULL
        AND t.scopes @> ARRAY['agent:pull']::text[]
        AND (t.expires_at IS NULL OR t.expires_at > clock_timestamp())
  )
RETURNING runtime_session_id, node_id, agent_id, credential_id, worker_id,
          session_epoch, device_certificate_serial, node_version,
          protocol_version, runtime_contract_id, runtime_contract_digest,
          features, capacity, inflight, status, attached_core_instance_id,
          connected_at, heartbeat_at, disconnected_at, created_at, updated_at;

-- name: ClaimRuntimeSessionSlot :one
WITH candidate AS (
    SELECT s.runtime_session_id
    FROM runtime_sessions s
    JOIN runtime_nodes n ON n.node_id = s.node_id
    JOIN agent_tokens t
      ON t.id = s.credential_id
     AND t.agent_id = s.agent_id
    WHERE s.runtime_session_id = $1
      AND s.agent_id = $2
      AND s.attached_core_instance_id = $3
      AND s.status = 'active'
      AND s.heartbeat_at >= $4
      AND s.inflight < s.capacity
      AND n.status = 'active'
      AND t.status = 'active_runtime'
      AND t.revoked_at IS NULL
      AND t.scopes @> ARRAY['agent:pull']::text[]
      AND (t.expires_at IS NULL OR t.expires_at > clock_timestamp())
    FOR UPDATE SKIP LOCKED
)
UPDATE runtime_sessions s
SET inflight = s.inflight + 1,
    updated_at = clock_timestamp()
FROM candidate
WHERE s.runtime_session_id = candidate.runtime_session_id
RETURNING s.runtime_session_id, s.node_id, s.agent_id, s.credential_id,
          s.worker_id, s.session_epoch, s.device_certificate_serial,
          s.node_version, s.protocol_version, s.runtime_contract_id,
          s.runtime_contract_digest, s.features, s.capacity, s.inflight,
          s.status, s.attached_core_instance_id, s.connected_at,
          s.heartbeat_at, s.disconnected_at, s.created_at, s.updated_at;

-- name: ReleaseRuntimeSessionSlot :one
UPDATE runtime_sessions
SET inflight = inflight - 1,
    updated_at = clock_timestamp()
WHERE runtime_session_id = $1
  AND inflight > 0
RETURNING runtime_session_id, node_id, agent_id, credential_id, worker_id,
          session_epoch, device_certificate_serial, node_version,
          protocol_version, runtime_contract_id, runtime_contract_digest,
          features, capacity, inflight, status, attached_core_instance_id,
          connected_at, heartbeat_at, disconnected_at, created_at, updated_at;

-- name: MarkRuntimeSessionDraining :one
UPDATE runtime_sessions
SET status = 'draining',
    updated_at = clock_timestamp()
WHERE runtime_session_id = $1
  AND attached_core_instance_id = $2
  AND status = 'active'
RETURNING runtime_session_id, node_id, agent_id, credential_id, worker_id,
          session_epoch, device_certificate_serial, node_version,
          protocol_version, runtime_contract_id, runtime_contract_digest,
          features, capacity, inflight, status, attached_core_instance_id,
          connected_at, heartbeat_at, disconnected_at, created_at, updated_at;

-- name: CloseRuntimeSession :one
UPDATE runtime_sessions
SET status = $3,
    capacity = GREATEST(capacity, inflight),
    attached_core_instance_id = NULL,
    disconnected_at = clock_timestamp(),
    updated_at = clock_timestamp()
WHERE runtime_session_id = $1
  AND attached_core_instance_id = $2
  AND status IN ('active', 'draining')
  AND $3 IN ('offline', 'revoked', 'closed')
RETURNING runtime_session_id, node_id, agent_id, credential_id, worker_id,
          session_epoch, device_certificate_serial, node_version,
          protocol_version, runtime_contract_id, runtime_contract_digest,
          features, capacity, inflight, status, attached_core_instance_id,
          connected_at, heartbeat_at, disconnected_at, created_at, updated_at;

-- name: CloseStaleRuntimeSession :one
UPDATE runtime_sessions
SET status = 'offline',
    capacity = GREATEST(capacity, inflight),
    attached_core_instance_id = NULL,
    disconnected_at = clock_timestamp(),
    updated_at = clock_timestamp()
WHERE runtime_session_id = $1
  AND heartbeat_at = $2
  AND attached_core_instance_id = $3
  AND status IN ('active', 'draining')
RETURNING runtime_session_id, node_id, agent_id, credential_id, worker_id,
          session_epoch, device_certificate_serial, node_version,
          protocol_version, runtime_contract_id, runtime_contract_digest,
          features, capacity, inflight, status, attached_core_instance_id,
          connected_at, heartbeat_at, disconnected_at, created_at, updated_at;

-- name: CreateRuntimeSessionAttachment :one
INSERT INTO runtime_session_attachments (
    runtime_session_id,
    core_instance_id,
    attachment_kind
)
SELECT $1, $2, $3
FROM runtime_sessions s
WHERE s.runtime_session_id = $1
  AND s.attached_core_instance_id = $2
  AND s.status IN ('active', 'draining')
RETURNING id, runtime_session_id, core_instance_id, attachment_kind,
          attached_at, detached_at, disconnect_reason;

-- name: GetActiveRuntimeSessionAttachment :one
SELECT id, runtime_session_id, core_instance_id, attachment_kind,
       attached_at, detached_at, disconnect_reason
FROM runtime_session_attachments
WHERE runtime_session_id = $1
  AND detached_at IS NULL;

-- name: ListRuntimeSessionAttachments :many
SELECT id, runtime_session_id, core_instance_id, attachment_kind,
       attached_at, detached_at, disconnect_reason
FROM runtime_session_attachments
WHERE runtime_session_id = $1
ORDER BY attached_at DESC, id DESC
LIMIT $2 OFFSET $3;

-- name: ListActiveRuntimeSessionAttachmentsByCore :many
SELECT id, runtime_session_id, core_instance_id, attachment_kind,
       attached_at, detached_at, disconnect_reason
FROM runtime_session_attachments
WHERE core_instance_id = $1
  AND detached_at IS NULL
ORDER BY attached_at ASC, id ASC;

-- name: CloseRuntimeSessionAttachment :one
UPDATE runtime_session_attachments
SET detached_at = clock_timestamp(),
    disconnect_reason = $4
WHERE runtime_session_id = $1
  AND core_instance_id = $2
  AND id = $3
  AND detached_at IS NULL
RETURNING id, runtime_session_id, core_instance_id, attachment_kind,
          attached_at, detached_at, disconnect_reason;

-- name: CloseRuntimeSessionAttachmentsByCore :execrows
UPDATE runtime_session_attachments
SET detached_at = clock_timestamp(),
    disconnect_reason = $2
WHERE core_instance_id = $1
  AND detached_at IS NULL;
