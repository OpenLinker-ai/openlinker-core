-- registry_bridge.sql
--
-- Registry Node + Cloud Listing Link first slice.

-- name: CreateRegistryNode :one
INSERT INTO registry_nodes (
    owner_user_id, node_name, node_type, base_url,
    secret_prefix, secret_hash, scopes
) VALUES (
    $1, $2, $3, $4,
    $5, $6, $7
)
RETURNING id, owner_user_id, node_name, node_type, base_url,
          secret_prefix, secret_hash, scopes, heartbeat_status,
          last_heartbeat_at, revoked_at, created_at, updated_at;

-- name: ListRegistryNodesByOwner :many
SELECT id, owner_user_id, node_name, node_type, base_url,
       secret_prefix, secret_hash, scopes, heartbeat_status,
       last_heartbeat_at, revoked_at, created_at, updated_at
FROM registry_nodes
WHERE owner_user_id = $1
ORDER BY created_at DESC;

-- name: GetRegistryNodeByIDForOwner :one
SELECT id, owner_user_id, node_name, node_type, base_url,
       secret_prefix, secret_hash, scopes, heartbeat_status,
       last_heartbeat_at, revoked_at, created_at, updated_at
FROM registry_nodes
WHERE id = $1 AND owner_user_id = $2;

-- name: ListActiveRegistryNodesBySecretPrefix :many
SELECT id, owner_user_id, node_name, node_type, base_url,
       secret_prefix, secret_hash, scopes, heartbeat_status,
       last_heartbeat_at, revoked_at, created_at, updated_at
FROM registry_nodes
WHERE secret_prefix = $1 AND revoked_at IS NULL;

-- name: MarkRegistryNodeHeartbeat :one
UPDATE registry_nodes
SET heartbeat_status = 'healthy',
    last_heartbeat_at = NOW()
WHERE id = $1 AND revoked_at IS NULL
RETURNING id, owner_user_id, node_name, node_type, base_url,
          secret_prefix, secret_hash, scopes, heartbeat_status,
          last_heartbeat_at, revoked_at, created_at, updated_at;

-- name: RevokeRegistryNodeForOwner :one
WITH revoked AS (
    UPDATE registry_nodes
    SET revoked_at = COALESCE(revoked_at, NOW()),
        heartbeat_status = 'revoked'
    WHERE id = $1 AND owner_user_id = $2
    RETURNING id, owner_user_id, node_name, node_type, base_url,
              secret_prefix, secret_hash, scopes, heartbeat_status,
              last_heartbeat_at, revoked_at, created_at, updated_at
),
paused_links AS (
    UPDATE cloud_listing_links l
    SET sync_status = 'paused',
        last_sync_at = NOW()
    FROM revoked r
    WHERE l.registry_node_id = r.id
      AND l.sync_status = 'linked'
    RETURNING l.id
)
SELECT id, owner_user_id, node_name, node_type, base_url,
       secret_prefix, secret_hash, scopes, heartbeat_status,
       last_heartbeat_at, revoked_at, created_at, updated_at
FROM revoked;

-- name: RotateRegistryNodeSecretForOwner :one
UPDATE registry_nodes
SET secret_prefix = $3,
    secret_hash = $4
WHERE id = $1
  AND owner_user_id = $2
  AND revoked_at IS NULL
RETURNING id, owner_user_id, node_name, node_type, base_url,
          secret_prefix, secret_hash, scopes, heartbeat_status,
          last_heartbeat_at, revoked_at, created_at, updated_at;

-- name: CountCloudListingLinksByNode :one
SELECT COUNT(*)::int AS total
FROM cloud_listing_links
WHERE registry_node_id = $1;

-- name: CountPendingProxyRunsByNode :one
SELECT COUNT(*)::int AS total
FROM proxy_runs
WHERE registry_node_id = $1
  AND status = 'pending';

-- name: UpsertCloudListingLink :one
INSERT INTO cloud_listing_links (
    registry_node_id, local_agent_id, routing_mode, payload_policy,
    sync_status, last_sync_at
) VALUES (
    $1, $2, $3, $4,
    'linked', NOW()
)
ON CONFLICT (registry_node_id, local_agent_id) DO UPDATE
SET routing_mode = EXCLUDED.routing_mode,
    payload_policy = EXCLUDED.payload_policy,
    sync_status = 'linked',
    last_sync_at = NOW()
RETURNING id, cloud_listing_id, registry_node_id, local_agent_id,
          routing_mode, payload_policy, sync_status,
          last_sync_at, created_at, updated_at;

-- name: ListCloudListingLinksByOwner :many
SELECT l.id, l.cloud_listing_id, l.registry_node_id, n.node_name,
       l.local_agent_id, a.slug AS agent_slug, a.name AS agent_name,
       l.routing_mode, l.payload_policy, l.sync_status,
       l.last_sync_at, l.created_at, l.updated_at
FROM cloud_listing_links l
JOIN registry_nodes n ON n.id = l.registry_node_id
JOIN agents a ON a.id = l.local_agent_id
WHERE n.owner_user_id = $1
ORDER BY l.created_at DESC;

-- name: UpdateCloudListingLinkStatusForOwner :one
UPDATE cloud_listing_links l
SET sync_status = $3,
    last_sync_at = NOW()
FROM registry_nodes n, agents a
WHERE l.cloud_listing_id = $1
  AND l.registry_node_id = n.id
  AND a.id = l.local_agent_id
  AND n.owner_user_id = $2
  AND ($3 <> 'linked' OR n.revoked_at IS NULL)
RETURNING l.id, l.cloud_listing_id, l.registry_node_id, n.node_name,
          l.local_agent_id, a.slug AS agent_slug, a.name AS agent_name,
          l.routing_mode, l.payload_policy, l.sync_status,
          l.last_sync_at, l.created_at, l.updated_at;

-- name: GetCloudListingLinkForProxyRun :one
SELECT l.id, l.cloud_listing_id, l.registry_node_id, l.local_agent_id,
       l.routing_mode, l.payload_policy, l.sync_status,
       l.last_sync_at, l.created_at, l.updated_at
FROM cloud_listing_links l
JOIN registry_nodes n ON n.id = l.registry_node_id
WHERE l.cloud_listing_id = $1
  AND l.sync_status = 'linked'
  AND n.revoked_at IS NULL;

-- name: CreateProxyRun :one
WITH link AS (
    SELECT l.id, l.cloud_listing_id, l.registry_node_id, l.local_agent_id, l.payload_policy
    FROM cloud_listing_links l
    JOIN registry_nodes n ON n.id = l.registry_node_id
    WHERE l.cloud_listing_id = $1
      AND l.sync_status = 'linked'
      AND n.revoked_at IS NULL
),
inserted AS (
    INSERT INTO proxy_runs (
        cloud_listing_link_id, cloud_listing_id, registry_node_id, local_agent_id,
        requesting_user_id, idempotency_key, payload_policy, input, input_summary
    )
    SELECT id, cloud_listing_id, registry_node_id, local_agent_id,
           $2, $3, payload_policy, $4::jsonb, $5
    FROM link
    ON CONFLICT (registry_node_id, idempotency_key) DO NOTHING
    RETURNING id, cloud_run_id, cloud_listing_link_id, cloud_listing_id,
              registry_node_id, local_agent_id, requesting_user_id,
              idempotency_key, status, payload_policy, input, input_summary,
              output, output_summary, error_code, error_message,
              claimed_at, finished_at, created_at, updated_at
)
SELECT id, cloud_run_id, cloud_listing_link_id, cloud_listing_id,
       registry_node_id, local_agent_id, requesting_user_id,
       idempotency_key, status, payload_policy, input, input_summary,
       output, output_summary, error_code, error_message,
       claimed_at, finished_at, created_at, updated_at
FROM inserted
UNION ALL
SELECT p.id, p.cloud_run_id, p.cloud_listing_link_id, p.cloud_listing_id,
       p.registry_node_id, p.local_agent_id, p.requesting_user_id,
       p.idempotency_key, p.status, p.payload_policy, p.input, p.input_summary,
       p.output, p.output_summary, p.error_code, p.error_message,
       p.claimed_at, p.finished_at, p.created_at, p.updated_at
FROM proxy_runs p
JOIN link l ON l.registry_node_id = p.registry_node_id
WHERE p.idempotency_key = $3
  AND NOT EXISTS (SELECT 1 FROM inserted)
LIMIT 1;

-- name: GetProxyRunForRequester :one
SELECT id, cloud_run_id, cloud_listing_link_id, cloud_listing_id,
       registry_node_id, local_agent_id, requesting_user_id,
       idempotency_key, status, payload_policy, input, input_summary,
       output, output_summary, error_code, error_message,
       claimed_at, finished_at, created_at, updated_at
FROM proxy_runs
WHERE id = $1 AND requesting_user_id = $2;

-- name: ClaimPendingProxyRun :one
WITH candidate AS (
    SELECT id
    FROM proxy_runs
    WHERE registry_node_id = $1
      AND status = 'pending'
    ORDER BY created_at ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
UPDATE proxy_runs p
SET status = 'claimed',
    claimed_at = NOW()
FROM candidate
WHERE p.id = candidate.id
RETURNING p.id, p.cloud_run_id, p.cloud_listing_link_id, p.cloud_listing_id,
          p.registry_node_id, p.local_agent_id, p.requesting_user_id,
          p.idempotency_key, p.status, p.payload_policy, p.input, p.input_summary,
          p.output, p.output_summary, p.error_code, p.error_message,
          p.claimed_at, p.finished_at, p.created_at, p.updated_at;

-- name: CompleteProxyRun :one
UPDATE proxy_runs
SET status = $3,
    output = $4::jsonb,
    output_summary = $5,
    error_code = $6,
    error_message = $7,
    claimed_at = COALESCE(claimed_at, NOW()),
    finished_at = NOW()
WHERE id = $1
  AND registry_node_id = $2
  AND status IN ('pending', 'claimed')
RETURNING id, cloud_run_id, cloud_listing_link_id, cloud_listing_id,
          registry_node_id, local_agent_id, requesting_user_id,
          idempotency_key, status, payload_policy, input, input_summary,
          output, output_summary, error_code, error_message,
          claimed_at, finished_at, created_at, updated_at;

-- name: TimeoutStaleProxyRuns :one
WITH expired AS (
    UPDATE proxy_runs
    SET status = 'timeout',
        output = '{}'::jsonb,
        output_summary = COALESCE(output_summary, 'Proxy Run timed out before Registry Node returned a result'),
        error_code = COALESCE(error_code, 'PROXY_RUN_TIMEOUT'),
        error_message = COALESCE(error_message, 'Registry Node did not complete the run before timeout'),
        claimed_at = COALESCE(claimed_at, NOW()),
        finished_at = NOW()
    WHERE status IN ('pending', 'claimed')
      AND COALESCE(claimed_at, created_at) < $1
    RETURNING id
)
SELECT COUNT(*)::int AS total
FROM expired;
