-- Reliable Runtime Session principal, assignment offer, ACK and lease
-- primitives.
--
-- Runtime transactions must acquire principal locks in this order before any
-- Run mutation:
--   1. LockRuntimeSessionForPrincipalValidation (FOR UPDATE)
--   2. LockRuntimeNodeForPrincipalValidation    (FOR UPDATE)
--   3. LockRuntimeCredentialForPrincipalValidation (FOR SHARE)
-- They may then use the global Run -> Attempt lock order. Splitting these
-- locks is intentional: a joined SELECT does not guarantee cross-table row
-- lock order and would leave revoke/status mutation races ambiguous.

-- name: LockRuntimeSessionForPrincipalValidation :one
SELECT s.runtime_session_id, s.node_id, s.agent_id, s.credential_id,
       s.worker_id, s.session_epoch, s.device_certificate_serial,
       s.node_version, s.protocol_version, s.runtime_contract_id,
       s.runtime_contract_digest, s.features, s.capacity, s.inflight,
       s.status, s.attached_core_instance_id, s.connected_at,
       s.heartbeat_at, s.disconnected_at, s.created_at, s.updated_at,
       clock_timestamp() AS database_now
FROM runtime_sessions s
WHERE s.runtime_session_id = sqlc.arg(runtime_session_id)
  AND s.node_id = sqlc.arg(node_id)
  AND s.agent_id = sqlc.arg(agent_id)
  AND s.credential_id = sqlc.arg(credential_id)
  AND s.worker_id = sqlc.arg(worker_id)
  AND s.attached_core_instance_id = sqlc.arg(attached_core_instance_id)
  AND s.status IN ('active', 'draining')
  AND s.protocol_version = 2
  AND s.runtime_contract_id = 'openlinker.runtime.v2'
  AND EXISTS (
      SELECT 1 FROM runtime_wire_contracts wire
      WHERE wire.runtime_contract_id = s.runtime_contract_id
        AND wire.runtime_contract_digest = s.runtime_contract_digest
        AND wire.support_tier IN ('current', 'previous')
  )
FOR UPDATE OF s;

-- name: LockRuntimeNodeForPrincipalValidation :one
SELECT n.node_id, n.display_name, n.device_certificate_serial,
       n.device_public_key_thumbprint, n.node_version, n.protocol_version,
       n.runtime_contract_id, n.runtime_contract_digest, n.features,
       n.capacity, n.inflight, n.status, n.last_seen_at, n.draining_at,
       n.revoked_at, n.revoke_reason, n.created_at, n.updated_at,
       clock_timestamp() AS database_now
FROM runtime_nodes n
WHERE n.node_id = sqlc.arg(node_id)
  AND n.device_certificate_serial = sqlc.arg(device_certificate_serial)
  AND n.device_public_key_thumbprint = sqlc.arg(device_public_key_thumbprint)
  AND n.status IN ('active', 'draining')
  AND n.protocol_version = 2
  AND n.runtime_contract_id = 'openlinker.runtime.v2'
  AND n.revoked_at IS NULL
  AND EXISTS (
      SELECT 1 FROM runtime_wire_contracts wire
      WHERE wire.runtime_contract_id = n.runtime_contract_id
        AND wire.runtime_contract_digest = n.runtime_contract_digest
        AND wire.support_tier IN ('current', 'previous')
  )
FOR UPDATE OF n;

-- name: LockRuntimeCredentialForPrincipalValidation :one
SELECT t.id, t.agent_id, t.status, t.scopes, t.expires_at, t.revoked_at,
       t.rotation_predecessor_id, t.revocation_kind,
       clock_timestamp() AS database_now
FROM agent_tokens t
WHERE t.id = sqlc.arg(credential_id)
  AND t.agent_id = sqlc.arg(agent_id)
  AND t.status = 'active_runtime'
  AND t.revoked_at IS NULL
  AND (t.expires_at IS NULL OR t.expires_at > clock_timestamp())
  AND t.scopes @> ARRAY['agent:pull']::text[]
FOR SHARE OF t;

-- name: LockRuntimeSessionAttachmentForPrincipalValidation :one
SELECT id, runtime_session_id, core_instance_id, attachment_kind,
       attached_at, detached_at, disconnect_reason,
       transport, transport_reason, transport_changed_at
FROM runtime_session_attachments
WHERE id = sqlc.arg(attachment_id)
  AND runtime_session_id = sqlc.arg(runtime_session_id)
  AND core_instance_id = sqlc.arg(core_instance_id)
  AND detached_at IS NULL
FOR UPDATE;

-- name: LockRuntimeSessionForOfferRelease :one
-- Disconnect cleanup may run only after Session close committed. Preserve the
-- global Session -> Node -> Token lock order while accepting either the still
-- attached live Session (send failure/ACK timeout) or an exact offline Session
-- with detached attachment evidence for this Core. If another Core has already
-- reattached the Session, neither branch matches and the old Core cannot clear
-- its offer underneath the new owner.
SELECT s.runtime_session_id, s.node_id, s.agent_id, s.credential_id,
       s.worker_id, s.session_epoch, s.device_certificate_serial,
       s.node_version, s.protocol_version, s.runtime_contract_id,
       s.runtime_contract_digest, s.features, s.capacity, s.inflight,
       s.status, s.attached_core_instance_id, s.connected_at,
       s.heartbeat_at, s.disconnected_at, s.created_at, s.updated_at,
       clock_timestamp() AS database_now
FROM runtime_sessions s
WHERE s.runtime_session_id = sqlc.arg(runtime_session_id)
  AND s.node_id = sqlc.arg(node_id)
  AND s.agent_id = sqlc.arg(agent_id)
  AND s.credential_id = sqlc.arg(credential_id)
  AND s.worker_id = sqlc.arg(worker_id)
  AND s.protocol_version = 2
  AND s.runtime_contract_id = 'openlinker.runtime.v2'
  AND (
      (
          s.status IN ('active', 'draining')
          AND s.attached_core_instance_id = sqlc.arg(core_instance_id)
      )
      OR (
          s.status IN ('offline', 'closed')
          AND s.attached_core_instance_id IS NULL
      )
  )
FOR UPDATE OF s;

-- name: LockRuntimeSessionAttachmentForOfferRelease :one
SELECT id, runtime_session_id, core_instance_id, attachment_kind,
       attached_at, detached_at, disconnect_reason,
       transport, transport_reason, transport_changed_at
FROM runtime_session_attachments
WHERE id = sqlc.arg(attachment_id)
  AND runtime_session_id = sqlc.arg(runtime_session_id)
  AND core_instance_id = sqlc.arg(core_instance_id)
  AND (
      (sqlc.arg(detached)::boolean AND detached_at IS NOT NULL)
      OR (NOT sqlc.arg(detached)::boolean AND detached_at IS NULL)
  )
FOR UPDATE;

-- name: GetExistingUnacceptedRunOfferForSession :one
WITH existing_offer AS MATERIALIZED (
    SELECT a.run_id, a.id AS attempt_id
    FROM run_attempts a
    JOIN runs r
      ON r.id = a.run_id
     AND r.active_attempt_id = a.id
    WHERE a.runtime_session_id = sqlc.arg(runtime_session_id)
      AND a.node_id = sqlc.arg(node_id)
      AND a.agent_id = sqlc.arg(agent_id)
      AND a.runtime_token_id = sqlc.arg(credential_id)
      AND a.runtime_worker_id = sqlc.arg(worker_id)
      AND a.accepted_at IS NULL
      AND a.finished_at IS NULL
      AND a.offer_expires_at > clock_timestamp()
      AND r.status = 'running'
      AND r.runtime_contract_id = 'openlinker.runtime.v2'
      AND r.dispatch_state = 'offered'
    LIMIT 1
), locked_run AS MATERIALIZED (
    SELECT r.id
    FROM runs r
    JOIN existing_offer e ON e.run_id = r.id
    FOR UPDATE OF r
)
SELECT a.id AS attempt_id, a.run_id, a.agent_id, a.offer_no, a.lease_id,
       a.fencing_token, a.runtime_token_id, a.runtime_worker_id,
       a.runtime_session_id, a.node_id, a.offered_by_core_instance_id,
       a.attached_core_instance_id, a.offered_at, a.offer_expires_at,
       a.lease_expires_at, a.attempt_deadline_at,
       r.input, r.request_metadata, r.connection_mode_snapshot,
       r.dispatch_state, r.offer_count, r.max_offer_count,
       r.attempt_count, r.max_attempts, r.dispatch_deadline_at,
       r.run_deadline_at, clock_timestamp() AS database_now
FROM locked_run l
JOIN runs r ON r.id = l.id
JOIN existing_offer e ON e.run_id = r.id
JOIN run_attempts a ON a.id = e.attempt_id
WHERE r.active_attempt_id = a.id
  AND r.lease_id = a.lease_id
  AND r.fencing_token = a.fencing_token
  AND r.runtime_session_id = a.runtime_session_id
  AND r.status = 'running'
  AND r.dispatch_state = 'offered'
  AND a.accepted_at IS NULL
  AND a.finished_at IS NULL
  AND a.offer_expires_at > clock_timestamp()
  AND r.dispatch_deadline_at > clock_timestamp()
  AND r.run_deadline_at > clock_timestamp()
FOR UPDATE OF a;

-- name: LockNextClaimableRuntimeRunForAgent :one
SELECT r.id, r.user_id, r.agent_id, r.input, r.request_metadata,
       r.connection_mode_snapshot, r.dispatch_state, r.offer_count,
       r.max_offer_count, r.attempt_count, r.max_attempts,
       r.next_attempt_at, r.dispatch_deadline_at, r.run_deadline_at,
       r.fencing_token, r.started_at, clock_timestamp() AS database_now
FROM runs r
WHERE r.agent_id = sqlc.arg(agent_id)
  AND r.status = 'running'
  AND r.runtime_contract_id = 'openlinker.runtime.v2'
  AND r.connection_mode_snapshot = 'runtime'
  AND (
      r.dispatch_state = 'pending'
      OR (
          r.dispatch_state = 'retry_wait'
          AND r.next_attempt_at <= clock_timestamp()
      )
  )
  AND r.active_attempt_id IS NULL
  AND r.offer_count < r.max_offer_count
  AND r.attempt_count < r.max_attempts
  AND r.dispatch_deadline_at > clock_timestamp()
  AND r.run_deadline_at > clock_timestamp()
ORDER BY
    CASE WHEN r.dispatch_state = 'retry_wait' THEN r.next_attempt_at ELSE r.started_at END ASC,
    r.started_at ASC,
    r.id ASC
LIMIT 1
FOR UPDATE OF r SKIP LOCKED;

-- name: CreateRuntimeRunOffer :one
WITH database_clock AS MATERIALIZED (
    SELECT clock_timestamp() AS database_now
)
INSERT INTO run_attempts (
    id, run_id, agent_id, offer_no, executor_type, lease_id, fencing_token,
    runtime_token_id, runtime_worker_id, runtime_session_id, node_id,
    offered_by_core_instance_id, attached_core_instance_id, offered_at,
    offer_expires_at, lease_expires_at, attempt_deadline_at,
    slot_acquired_at, active_runtime_session_id
)
SELECT
    sqlc.arg(attempt_id), r.id, r.agent_id, r.offer_count + 1,
    'runtime', sqlc.arg(lease_id), r.fencing_token + 1,
    s.credential_id, s.worker_id, s.runtime_session_id, s.node_id,
    sqlc.arg(core_instance_id), sqlc.arg(core_instance_id), c.database_now,
    LEAST(
        c.database_now + (sqlc.arg(offer_ttl_ms)::bigint * INTERVAL '1 millisecond'),
        c.database_now + (sqlc.arg(lease_ttl_ms)::bigint * INTERVAL '1 millisecond'),
        c.database_now + (sqlc.arg(attempt_ttl_ms)::bigint * INTERVAL '1 millisecond'),
        r.dispatch_deadline_at,
        r.run_deadline_at
    ),
    LEAST(
        c.database_now + (sqlc.arg(lease_ttl_ms)::bigint * INTERVAL '1 millisecond'),
        c.database_now + (sqlc.arg(attempt_ttl_ms)::bigint * INTERVAL '1 millisecond'),
        r.run_deadline_at
    ),
    LEAST(
        c.database_now + (sqlc.arg(attempt_ttl_ms)::bigint * INTERVAL '1 millisecond'),
        r.run_deadline_at
    ),
    c.database_now,
    s.runtime_session_id
FROM runs r
JOIN runtime_sessions s
  ON s.runtime_session_id = sqlc.arg(runtime_session_id)
 AND s.agent_id = r.agent_id
JOIN runtime_nodes n ON n.node_id = s.node_id
JOIN agent_tokens t
  ON t.id = s.credential_id
 AND t.agent_id = s.agent_id
CROSS JOIN database_clock c
WHERE r.id = sqlc.arg(run_id)
  AND r.status = 'running'
  AND r.runtime_contract_id = 'openlinker.runtime.v2'
  AND r.connection_mode_snapshot = 'runtime'
  AND (
      r.dispatch_state = 'pending'
      OR (
          r.dispatch_state = 'retry_wait'
          AND r.next_attempt_at <= c.database_now
      )
  )
  AND r.active_attempt_id IS NULL
  AND r.offer_count < r.max_offer_count
  AND r.attempt_count < r.max_attempts
  AND r.dispatch_deadline_at > c.database_now
  AND r.run_deadline_at > c.database_now
  AND s.node_id = sqlc.arg(node_id)
  AND s.credential_id = sqlc.arg(credential_id)
  AND s.worker_id = sqlc.arg(worker_id)
  AND s.attached_core_instance_id = sqlc.arg(core_instance_id)
  AND s.status = 'active'
  AND n.status = 'active'
  AND n.revoked_at IS NULL
  AND t.status = 'active_runtime'
  AND t.revoked_at IS NULL
  AND (t.expires_at IS NULL OR t.expires_at > c.database_now)
  AND t.scopes @> ARRAY['agent:pull']::text[]
  AND sqlc.arg(offer_ttl_ms)::bigint BETWEEN 1 AND 300000
  AND sqlc.arg(lease_ttl_ms)::bigint BETWEEN 1 AND 3600000
  AND sqlc.arg(attempt_ttl_ms)::bigint BETWEEN 1 AND 86400000
  AND NOT EXISTS (
      SELECT 1
      FROM run_attempts outstanding
      WHERE outstanding.runtime_session_id = s.runtime_session_id
        AND outstanding.accepted_at IS NULL
        AND outstanding.finished_at IS NULL
  )
RETURNING *;

-- name: MirrorRuntimeRunOffer :one
UPDATE runs r
SET dispatch_state = 'offered',
    next_attempt_at = NULL,
    offer_count = a.offer_no,
    latest_attempt_id = a.id,
    active_attempt_id = a.id,
    lease_id = a.lease_id,
    fencing_token = a.fencing_token,
    executor_type = a.executor_type,
    active_core_instance_id = a.attached_core_instance_id,
    runtime_node_id = a.node_id,
    runtime_worker_id = a.runtime_worker_id,
    runtime_session_id = a.runtime_session_id,
    lease_token_id = a.runtime_token_id,
    lease_offered_at = a.offered_at,
    lease_accepted_at = a.accepted_at,
    lease_expires_at = a.lease_expires_at,
    attempt_deadline_at = a.attempt_deadline_at
FROM run_attempts a
WHERE r.id = sqlc.arg(run_id)
  AND a.run_id = r.id
  AND a.id = sqlc.arg(attempt_id)
  AND a.lease_id = sqlc.arg(lease_id)
  AND a.fencing_token = sqlc.arg(fencing_token)
  AND a.runtime_session_id = sqlc.arg(runtime_session_id)
  AND a.node_id = sqlc.arg(node_id)
  AND a.runtime_token_id = sqlc.arg(credential_id)
  AND a.runtime_worker_id = sqlc.arg(worker_id)
  AND a.attached_core_instance_id = sqlc.arg(core_instance_id)
  AND a.executor_type = 'runtime'
  AND a.accepted_at IS NULL
  AND a.finished_at IS NULL
  AND r.status = 'running'
  AND r.runtime_contract_id = 'openlinker.runtime.v2'
  AND (
      r.dispatch_state = 'pending'
      OR (
          r.dispatch_state = 'retry_wait'
          AND r.next_attempt_at <= clock_timestamp()
      )
  )
  AND r.active_attempt_id IS NULL
  AND r.offer_count + 1 = a.offer_no
  AND r.fencing_token + 1 = a.fencing_token
RETURNING r.id, r.dispatch_state, r.offer_count, r.attempt_count,
          r.latest_attempt_id, r.active_attempt_id, r.lease_id,
          r.fencing_token, r.runtime_node_id, r.runtime_worker_id,
          r.runtime_session_id, r.lease_token_id, r.lease_offered_at,
          r.lease_accepted_at, r.lease_expires_at, r.attempt_deadline_at,
          clock_timestamp() AS database_now;

-- name: LockRunForLeaseMutation :one
SELECT r.id, r.agent_id, r.status, r.dispatch_state, r.offer_count,
       r.max_offer_count, r.attempt_count, r.max_attempts,
       r.latest_attempt_id, r.active_attempt_id, r.lease_id,
       r.fencing_token, r.active_core_instance_id, r.runtime_node_id,
       r.runtime_worker_id, r.runtime_session_id, r.lease_token_id,
       r.lease_offered_at, r.lease_accepted_at, r.lease_expires_at,
       r.attempt_deadline_at, r.dispatch_deadline_at, r.run_deadline_at,
       r.cancel_request_id, r.cancel_state, r.terminal_event_id,
       clock_timestamp() AS database_now
FROM runs r
WHERE r.id = sqlc.arg(run_id)
  AND r.runtime_contract_id = 'openlinker.runtime.v2'
FOR UPDATE OF r;

-- name: LockRuntimeRunAttemptForLeaseMutation :one
SELECT a.*
FROM run_attempts a
WHERE a.run_id = sqlc.arg(run_id)
  AND a.id = sqlc.arg(attempt_id)
  AND a.lease_id = sqlc.arg(lease_id)
  AND a.fencing_token = sqlc.arg(fencing_token)
  AND a.runtime_session_id = sqlc.arg(runtime_session_id)
  AND a.node_id = sqlc.arg(node_id)
  AND a.runtime_token_id = sqlc.arg(credential_id)
  AND a.runtime_worker_id = sqlc.arg(worker_id)
  AND a.executor_type = 'runtime'
FOR UPDATE OF a;

-- name: ConfirmRunAssignment :one
WITH database_clock AS MATERIALIZED (
    SELECT clock_timestamp() AS database_now
)
UPDATE run_attempts a
SET attempt_no = r.attempt_count + 1,
    accepted_at = c.database_now,
    last_renewed_at = c.database_now,
    lease_expires_at = LEAST(
        c.database_now + (sqlc.arg(lease_ttl_ms)::bigint * INTERVAL '1 millisecond'),
        a.attempt_deadline_at,
        r.run_deadline_at
    ),
    attached_core_instance_id = sqlc.arg(core_instance_id),
    runtime_attachment_id = attachment.id
FROM runs r, runtime_sessions s, runtime_nodes n, agent_tokens t,
     runtime_session_attachments attachment,
     database_clock c
WHERE a.run_id = sqlc.arg(run_id)
  AND a.id = sqlc.arg(attempt_id)
  AND a.lease_id = sqlc.arg(lease_id)
  AND a.fencing_token = sqlc.arg(fencing_token)
  AND a.runtime_session_id = sqlc.arg(runtime_session_id)
  AND a.node_id = sqlc.arg(node_id)
  AND a.runtime_token_id = sqlc.arg(credential_id)
  AND a.runtime_worker_id = sqlc.arg(worker_id)
  AND a.executor_type = 'runtime'
  AND a.accepted_at IS NULL
  AND a.finished_at IS NULL
  AND a.offer_expires_at > c.database_now
  AND a.attempt_deadline_at > c.database_now
  AND r.id = a.run_id
  AND r.agent_id = a.agent_id
  AND r.status = 'running'
  AND r.runtime_contract_id = 'openlinker.runtime.v2'
  AND r.dispatch_state = 'offered'
  AND r.active_attempt_id = a.id
  AND r.lease_id = a.lease_id
  AND r.fencing_token = a.fencing_token
  AND r.runtime_session_id = a.runtime_session_id
  AND r.runtime_node_id = a.node_id
  AND r.lease_token_id = a.runtime_token_id
  AND r.runtime_worker_id = a.runtime_worker_id
  AND r.attempt_count < r.max_attempts
  AND r.run_deadline_at > c.database_now
  AND s.runtime_session_id = sqlc.arg(runtime_session_id)
  AND s.node_id = sqlc.arg(node_id)
  AND s.agent_id = r.agent_id
  AND s.credential_id = sqlc.arg(credential_id)
  AND s.worker_id = sqlc.arg(worker_id)
  AND s.attached_core_instance_id = sqlc.arg(core_instance_id)
  AND s.status IN ('active', 'draining')
  AND n.node_id = s.node_id
  AND n.status IN ('active', 'draining')
  AND n.revoked_at IS NULL
  AND t.id = s.credential_id
  AND t.agent_id = s.agent_id
  AND t.status = 'active_runtime'
  AND t.revoked_at IS NULL
  AND (t.expires_at IS NULL OR t.expires_at > c.database_now)
  AND t.scopes @> ARRAY['agent:pull']::text[]
  AND attachment.id = sqlc.arg(attachment_id)
  AND attachment.runtime_session_id = s.runtime_session_id
  AND attachment.core_instance_id = sqlc.arg(core_instance_id)
  AND attachment.detached_at IS NULL
  AND attachment.transport IN ('websocket', 'long_poll')
  AND attachment.transport_reason IS NOT NULL
  AND sqlc.arg(lease_ttl_ms)::bigint BETWEEN 1 AND 3600000
RETURNING a.*;

-- name: MirrorRunConfirmedAssignment :one
UPDATE runs r
SET dispatch_state = 'executing',
    attempt_count = a.attempt_no,
    active_core_instance_id = a.attached_core_instance_id,
    lease_accepted_at = a.accepted_at,
    lease_expires_at = a.lease_expires_at
FROM run_attempts a
WHERE r.id = sqlc.arg(run_id)
  AND a.run_id = r.id
  AND a.id = sqlc.arg(attempt_id)
  AND a.lease_id = sqlc.arg(lease_id)
  AND a.fencing_token = sqlc.arg(fencing_token)
  AND a.runtime_session_id = sqlc.arg(runtime_session_id)
  AND a.node_id = sqlc.arg(node_id)
  AND a.runtime_token_id = sqlc.arg(credential_id)
  AND a.runtime_worker_id = sqlc.arg(worker_id)
  AND a.attached_core_instance_id = sqlc.arg(core_instance_id)
  AND a.accepted_at IS NOT NULL
  AND a.attempt_no IS NOT NULL
  AND a.finished_at IS NULL
  AND r.status = 'running'
  AND r.dispatch_state = 'offered'
  AND r.active_attempt_id = a.id
  AND r.lease_id = a.lease_id
  AND r.fencing_token = a.fencing_token
  AND r.attempt_count + 1 = a.attempt_no
RETURNING r.id, r.dispatch_state, r.offer_count, r.attempt_count,
          r.latest_attempt_id, r.active_attempt_id, r.lease_id,
          r.fencing_token, r.runtime_node_id, r.runtime_worker_id,
          r.runtime_session_id, r.lease_token_id, r.lease_offered_at,
          r.lease_accepted_at, r.lease_expires_at, r.attempt_deadline_at,
          clock_timestamp() AS database_now;

-- name: RenewRuntimeRunAttempt :one
WITH database_clock AS MATERIALIZED (
    SELECT clock_timestamp() AS database_now
)
UPDATE run_attempts a
SET last_renewed_at = c.database_now,
    lease_expires_at = GREATEST(
        a.lease_expires_at,
        LEAST(
            c.database_now + (sqlc.arg(lease_ttl_ms)::bigint * INTERVAL '1 millisecond'),
            a.attempt_deadline_at,
            r.run_deadline_at
        )
    ),
    attached_core_instance_id = sqlc.arg(core_instance_id)
FROM runs r, runtime_sessions s, runtime_nodes n, agent_tokens t,
     database_clock c
WHERE a.run_id = sqlc.arg(run_id)
  AND a.id = sqlc.arg(attempt_id)
  AND a.lease_id = sqlc.arg(lease_id)
  AND a.fencing_token = sqlc.arg(fencing_token)
  AND a.runtime_session_id = sqlc.arg(runtime_session_id)
  AND a.node_id = sqlc.arg(node_id)
  AND a.runtime_token_id = sqlc.arg(credential_id)
  AND a.runtime_worker_id = sqlc.arg(worker_id)
  AND a.executor_type = 'runtime'
  AND a.accepted_at IS NOT NULL
  AND a.finished_at IS NULL
  AND a.lease_expires_at > c.database_now
  AND a.attempt_deadline_at > c.database_now
  AND r.id = a.run_id
  AND r.status = 'running'
  AND r.runtime_contract_id = 'openlinker.runtime.v2'
  AND r.dispatch_state = 'executing'
  AND r.active_attempt_id = a.id
  AND r.lease_id = a.lease_id
  AND r.fencing_token = a.fencing_token
  AND r.runtime_session_id = a.runtime_session_id
  AND r.runtime_node_id = a.node_id
  AND r.lease_token_id = a.runtime_token_id
  AND r.runtime_worker_id = a.runtime_worker_id
  AND r.lease_expires_at > c.database_now
  AND r.run_deadline_at > c.database_now
  AND r.cancel_request_id IS NULL
  AND s.runtime_session_id = sqlc.arg(runtime_session_id)
  AND s.node_id = sqlc.arg(node_id)
  AND s.agent_id = r.agent_id
  AND s.credential_id = sqlc.arg(credential_id)
  AND s.worker_id = sqlc.arg(worker_id)
  AND s.attached_core_instance_id = sqlc.arg(core_instance_id)
  AND s.status IN ('active', 'draining')
  AND n.node_id = s.node_id
  AND n.status IN ('active', 'draining')
  AND n.revoked_at IS NULL
  AND t.id = s.credential_id
  AND t.agent_id = s.agent_id
  AND t.status = 'active_runtime'
  AND t.revoked_at IS NULL
  AND (t.expires_at IS NULL OR t.expires_at > c.database_now)
  AND t.scopes @> ARRAY['agent:pull']::text[]
  AND sqlc.arg(lease_ttl_ms)::bigint BETWEEN 1 AND 3600000
RETURNING a.*;

-- name: MirrorRunLeaseRenewal :one
UPDATE runs r
SET active_core_instance_id = a.attached_core_instance_id,
    lease_expires_at = a.lease_expires_at
FROM run_attempts a
WHERE r.id = sqlc.arg(run_id)
  AND a.run_id = r.id
  AND a.id = sqlc.arg(attempt_id)
  AND a.lease_id = sqlc.arg(lease_id)
  AND a.fencing_token = sqlc.arg(fencing_token)
  AND a.attached_core_instance_id = sqlc.arg(core_instance_id)
  AND a.accepted_at IS NOT NULL
  AND a.finished_at IS NULL
  AND r.status = 'running'
  AND r.dispatch_state = 'executing'
  AND r.active_attempt_id = a.id
  AND r.lease_id = a.lease_id
  AND r.fencing_token = a.fencing_token
RETURNING r.id, r.dispatch_state, r.offer_count, r.attempt_count,
          r.latest_attempt_id, r.active_attempt_id, r.lease_id,
          r.fencing_token, r.runtime_node_id, r.runtime_worker_id,
          r.runtime_session_id, r.lease_token_id, r.lease_offered_at,
          r.lease_accepted_at, r.lease_expires_at, r.attempt_deadline_at,
          clock_timestamp() AS database_now;

-- name: FinishUnacceptedRunOffer :one
UPDATE run_attempts a
SET finished_at = clock_timestamp(),
    outcome = sqlc.arg(outcome),
    error_code = sqlc.narg(error_code),
    error_detail_redacted = sqlc.narg(error_detail_redacted)
WHERE a.run_id = sqlc.arg(run_id)
  AND a.id = sqlc.arg(attempt_id)
  AND a.lease_id = sqlc.arg(lease_id)
  AND a.fencing_token = sqlc.arg(fencing_token)
  AND a.runtime_session_id = sqlc.arg(runtime_session_id)
  AND a.node_id = sqlc.arg(node_id)
  AND a.runtime_token_id = sqlc.arg(credential_id)
  AND a.runtime_worker_id = sqlc.arg(worker_id)
  AND a.executor_type = 'runtime'
  AND a.accepted_at IS NULL
  AND a.attempt_no IS NULL
  AND a.finished_at IS NULL
  AND sqlc.arg(outcome) IN ('offer_rejected', 'offer_expired')
  AND EXISTS (
      SELECT 1
      FROM runs r
      WHERE r.id = a.run_id
        AND r.status = 'running'
        AND r.dispatch_state = 'offered'
        AND r.active_attempt_id = a.id
        AND r.lease_id = a.lease_id
        AND r.fencing_token = a.fencing_token
        AND r.runtime_session_id = a.runtime_session_id
        AND r.runtime_node_id = a.node_id
        AND r.lease_token_id = a.runtime_token_id
        AND r.runtime_worker_id = a.runtime_worker_id
  )
  AND EXISTS (
      SELECT 1
      FROM runtime_sessions current_owner
      WHERE current_owner.runtime_session_id = a.runtime_session_id
        AND current_owner.node_id = a.node_id
        AND current_owner.agent_id = a.agent_id
        AND current_owner.credential_id = a.runtime_token_id
        AND current_owner.worker_id = a.runtime_worker_id
        AND (
            (
                current_owner.attached_core_instance_id = sqlc.arg(core_instance_id)
                AND current_owner.status IN ('active', 'draining')
            )
            OR (
                current_owner.attached_core_instance_id IS NULL
                AND current_owner.status IN ('offline', 'closed')
                AND a.attached_core_instance_id = sqlc.arg(core_instance_id)
                AND EXISTS (
                    SELECT 1
                    FROM runtime_session_attachments detached
                    WHERE detached.runtime_session_id = current_owner.runtime_session_id
                      AND detached.core_instance_id = sqlc.arg(core_instance_id)
                      AND detached.detached_at IS NOT NULL
                )
            )
        )
  )
RETURNING a.*;

-- name: ResetRunAfterUnacceptedOffer :one
UPDATE runs r
SET dispatch_state = 'pending',
    next_attempt_at = NULL,
    active_attempt_id = NULL,
    lease_id = NULL,
    executor_type = NULL,
    active_core_instance_id = NULL,
    runtime_node_id = NULL,
    runtime_worker_id = NULL,
    runtime_session_id = NULL,
    lease_token_id = NULL,
    lease_offered_at = NULL,
    lease_accepted_at = NULL,
    lease_expires_at = NULL,
    attempt_deadline_at = NULL
FROM run_attempts a
WHERE r.id = sqlc.arg(run_id)
  AND a.run_id = r.id
  AND a.id = sqlc.arg(attempt_id)
  AND a.lease_id = sqlc.arg(lease_id)
  AND a.fencing_token = sqlc.arg(fencing_token)
  AND a.finished_at IS NOT NULL
  AND a.outcome IN ('offer_rejected', 'offer_expired')
  AND a.accepted_at IS NULL
  AND a.attempt_no IS NULL
  AND r.status = 'running'
  AND r.dispatch_state = 'offered'
  AND r.active_attempt_id = a.id
  AND r.latest_attempt_id = a.id
  AND r.lease_id = a.lease_id
  AND r.fencing_token = a.fencing_token
RETURNING r.id, r.dispatch_state, r.offer_count, r.attempt_count,
          r.latest_attempt_id, r.active_attempt_id, r.lease_id,
          r.fencing_token, r.runtime_node_id, r.runtime_worker_id,
          r.runtime_session_id, r.lease_token_id, r.lease_offered_at,
          r.lease_accepted_at, r.lease_expires_at, r.attempt_deadline_at,
          clock_timestamp() AS database_now;

-- name: MarkRunAttemptCapacityReleased :one
-- This is the only successful path that authorizes decrementing Session and
-- Node inflight counters. A replay loses the CAS and must not decrement.
WITH capacity_owner AS MATERIALIZED (
    SELECT a.id, a.active_runtime_session_id AS runtime_session_id,
           a.node_id, a.slot_acquired_at
    FROM run_attempts a
    WHERE a.run_id = sqlc.arg(run_id)
      AND a.id = sqlc.arg(attempt_id)
      AND a.lease_id = sqlc.arg(lease_id)
      AND a.fencing_token = sqlc.arg(fencing_token)
      AND a.executor_type = 'runtime'
      AND a.slot_acquired_at IS NOT NULL
      AND a.slot_released_at IS NULL
      AND a.active_runtime_session_id IS NOT NULL
    FOR UPDATE OF a
), released AS (
    UPDATE run_attempts a
    SET slot_released_at = clock_timestamp(),
        active_runtime_session_id = NULL
    FROM capacity_owner owner
    WHERE a.id = owner.id
    RETURNING owner.runtime_session_id, owner.node_id,
              owner.slot_acquired_at, a.slot_released_at
)
SELECT runtime_session_id, node_id, slot_acquired_at, slot_released_at
FROM released;
