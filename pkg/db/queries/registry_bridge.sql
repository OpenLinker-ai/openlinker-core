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
    cloud_listing_id, registry_node_id, local_agent_id, routing_mode, payload_policy, payload_redaction_keys,
    sync_status, last_sync_at
) VALUES (
    $1, $2, $3, $4, $5, $6,
    'linked', NOW()
)
ON CONFLICT (registry_node_id, local_agent_id) DO UPDATE
SET routing_mode = EXCLUDED.routing_mode,
    payload_policy = EXCLUDED.payload_policy,
    payload_redaction_keys = EXCLUDED.payload_redaction_keys,
    sync_status = 'linked',
    last_sync_at = NOW()
RETURNING id, cloud_listing_id, registry_node_id, local_agent_id,
          routing_mode, payload_policy, payload_redaction_keys, sync_status,
          synced_agent_slug, synced_agent_name, synced_agent_description,
          synced_agent_tags, synced_availability_status,
          metadata_synced_at, metadata_sync_error,
          last_sync_at, created_at, updated_at;

-- name: GetCloudListingLinkForOwner :one
SELECT l.id, l.cloud_listing_id, l.registry_node_id, l.local_agent_id,
       l.routing_mode, l.payload_policy, l.payload_redaction_keys, l.sync_status,
       l.synced_agent_slug, l.synced_agent_name, l.synced_agent_description,
       l.synced_agent_tags, l.synced_availability_status,
       l.metadata_synced_at, l.metadata_sync_error,
       l.last_sync_at, l.created_at, l.updated_at
FROM cloud_listing_links l
JOIN registry_nodes n ON n.id = l.registry_node_id
WHERE l.cloud_listing_id = $1
  AND n.owner_user_id = $2
LIMIT 1;

-- name: ListCloudListingLinksByOwner :many
SELECT l.id, l.cloud_listing_id, l.registry_node_id, n.node_name,
       l.local_agent_id,
       COALESCE(NULLIF(l.synced_agent_slug, ''), a.slug) AS agent_slug,
       COALESCE(NULLIF(l.synced_agent_name, ''), a.name) AS agent_name,
       l.routing_mode, l.payload_policy, l.payload_redaction_keys, l.sync_status,
       l.synced_agent_description AS agent_description,
       l.synced_agent_tags AS agent_tags,
       l.synced_availability_status AS availability_status,
       l.metadata_synced_at, l.metadata_sync_error,
       l.last_sync_at, l.created_at, l.updated_at
FROM cloud_listing_links l
JOIN registry_nodes n ON n.id = l.registry_node_id
JOIN agents a ON a.id = l.local_agent_id
WHERE n.owner_user_id = $1
ORDER BY l.created_at DESC;

-- name: GetCloudListingLinkRowForOwner :one
SELECT l.id, l.cloud_listing_id, l.registry_node_id, n.node_name,
       l.local_agent_id,
       COALESCE(NULLIF(l.synced_agent_slug, ''), a.slug) AS agent_slug,
       COALESCE(NULLIF(l.synced_agent_name, ''), a.name) AS agent_name,
       l.routing_mode, l.payload_policy, l.payload_redaction_keys, l.sync_status,
       l.synced_agent_description AS agent_description,
       l.synced_agent_tags AS agent_tags,
       l.synced_availability_status AS availability_status,
       l.metadata_synced_at, l.metadata_sync_error,
       l.last_sync_at, l.created_at, l.updated_at
FROM cloud_listing_links l
JOIN registry_nodes n ON n.id = l.registry_node_id
JOIN agents a ON a.id = l.local_agent_id
WHERE l.id = $1
  AND n.owner_user_id = $2;

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
          l.local_agent_id,
          COALESCE(NULLIF(l.synced_agent_slug, ''), a.slug) AS agent_slug,
          COALESCE(NULLIF(l.synced_agent_name, ''), a.name) AS agent_name,
          l.routing_mode, l.payload_policy, l.payload_redaction_keys, l.sync_status,
          l.synced_agent_description AS agent_description,
          l.synced_agent_tags AS agent_tags,
          l.synced_availability_status AS availability_status,
          l.metadata_synced_at, l.metadata_sync_error,
          l.last_sync_at, l.created_at, l.updated_at;

-- name: SyncCloudListingMetadataForOwner :one
UPDATE cloud_listing_links l
SET synced_agent_slug = a.slug,
    synced_agent_name = a.name,
    synced_agent_description = a.description,
    synced_agent_tags = a.tags,
    synced_availability_status = COALESCE(av.availability_status, 'unknown'),
    metadata_synced_at = NOW(),
    metadata_sync_error = NULL,
    last_sync_at = NOW()
FROM registry_nodes n, agents a
LEFT JOIN agent_availability_snapshots av ON av.agent_id = a.id
WHERE l.cloud_listing_id = $1
  AND l.registry_node_id = n.id
  AND a.id = l.local_agent_id
  AND n.owner_user_id = $2
  AND n.revoked_at IS NULL
RETURNING l.id, l.cloud_listing_id, l.registry_node_id, n.node_name,
          l.local_agent_id,
          l.synced_agent_slug AS agent_slug,
          l.synced_agent_name AS agent_name,
          l.routing_mode, l.payload_policy, l.payload_redaction_keys, l.sync_status,
          l.synced_agent_description AS agent_description,
          l.synced_agent_tags AS agent_tags,
          l.synced_availability_status AS availability_status,
          l.metadata_synced_at, l.metadata_sync_error,
          l.last_sync_at, l.created_at, l.updated_at;

-- name: SyncCloudListingMetadataByNode :one
WITH synced AS (
    UPDATE cloud_listing_links l
    SET synced_agent_slug = a.slug,
        synced_agent_name = a.name,
        synced_agent_description = a.description,
        synced_agent_tags = a.tags,
        synced_availability_status = COALESCE(av.availability_status, 'unknown'),
        metadata_synced_at = NOW(),
        metadata_sync_error = NULL,
        last_sync_at = NOW()
    FROM agents a
    LEFT JOIN agent_availability_snapshots av ON av.agent_id = a.id
    WHERE l.registry_node_id = $1
      AND a.id = l.local_agent_id
      AND l.sync_status IN ('linked', 'paused')
    RETURNING l.id
)
SELECT COUNT(*)::int AS total
FROM synced;

-- name: GetCloudListingLinkForProxyRun :one
SELECT l.id, l.cloud_listing_id, l.registry_node_id, l.local_agent_id,
       l.routing_mode, l.payload_policy, l.payload_redaction_keys, l.sync_status,
       l.synced_agent_slug, l.synced_agent_name, l.synced_agent_description,
       l.synced_agent_tags, l.synced_availability_status,
       l.metadata_synced_at, l.metadata_sync_error,
       l.last_sync_at, l.created_at, l.updated_at
FROM cloud_listing_links l
JOIN registry_nodes n ON n.id = l.registry_node_id
LEFT JOIN LATERAL (
    SELECT COUNT(*)::int AS active_run_count
    FROM proxy_runs p
    WHERE p.registry_node_id = n.id
      AND p.status IN ('pending', 'claimed')
) load ON TRUE
WHERE l.cloud_listing_id = $1
  AND l.sync_status = 'linked'
  AND l.routing_mode = 'pull_proxy'
  AND n.revoked_at IS NULL
ORDER BY
  CASE
    WHEN n.heartbeat_status = 'healthy' THEN 0
    WHEN n.heartbeat_status = 'unknown' THEN 1
    ELSE 2
  END,
  COALESCE(load.active_run_count, 0) ASC,
  n.last_heartbeat_at DESC NULLS LAST,
  l.created_at ASC
LIMIT 1;

-- name: CreateProxyRun :one
WITH link AS (
    SELECT l.id, l.cloud_listing_id, l.registry_node_id, l.local_agent_id, l.payload_policy,
           l.payload_redaction_keys
    FROM cloud_listing_links l
    JOIN registry_nodes n ON n.id = l.registry_node_id
    WHERE l.cloud_listing_id = $1
      AND l.id = $2
      AND l.sync_status = 'linked'
      AND l.routing_mode = 'pull_proxy'
      AND n.revoked_at IS NULL
),
inserted AS (
    INSERT INTO proxy_runs (
        cloud_listing_link_id, cloud_listing_id, registry_node_id, local_agent_id,
        requesting_user_id, idempotency_key, payload_policy, payload_redaction_keys,
        input, input_summary, node_input
    )
    SELECT id, cloud_listing_id, registry_node_id, local_agent_id,
           $3, $4, payload_policy, payload_redaction_keys, $5::jsonb, $6, $7::jsonb
    FROM link
    ON CONFLICT (cloud_listing_id, idempotency_key) DO NOTHING
    RETURNING id, cloud_run_id, cloud_listing_link_id, cloud_listing_id,
              registry_node_id, local_agent_id, requesting_user_id,
              idempotency_key, status, payload_policy, payload_redaction_keys,
              input, input_summary,
              output, output_summary, error_code, error_message,
              attempt_count, max_attempts, next_retry_at,
              claimed_at, finished_at, created_at, updated_at
)
SELECT id, cloud_run_id, cloud_listing_link_id, cloud_listing_id,
       registry_node_id, local_agent_id, requesting_user_id,
       idempotency_key, status, payload_policy, payload_redaction_keys,
       input, input_summary,
       output, output_summary, error_code, error_message,
       attempt_count, max_attempts, next_retry_at,
       claimed_at, finished_at, created_at, updated_at
FROM inserted
UNION ALL
SELECT p.id, p.cloud_run_id, p.cloud_listing_link_id, p.cloud_listing_id,
       p.registry_node_id, p.local_agent_id, p.requesting_user_id,
       p.idempotency_key, p.status, p.payload_policy, p.payload_redaction_keys,
       p.input, p.input_summary,
       p.output, p.output_summary, p.error_code, p.error_message,
       p.attempt_count, p.max_attempts, p.next_retry_at,
       p.claimed_at, p.finished_at, p.created_at, p.updated_at
FROM proxy_runs p
WHERE p.cloud_listing_id = $1
  AND p.idempotency_key = $4
  AND NOT EXISTS (SELECT 1 FROM inserted)
LIMIT 1;

-- name: GetProxyRunForRequester :one
SELECT id, cloud_run_id, cloud_listing_link_id, cloud_listing_id,
       registry_node_id, local_agent_id, requesting_user_id,
       idempotency_key, status, payload_policy, payload_redaction_keys,
       input, input_summary,
       output, output_summary, error_code, error_message,
       attempt_count, max_attempts, next_retry_at,
       claimed_at, finished_at, created_at, updated_at
FROM proxy_runs
WHERE id = $1 AND requesting_user_id = $2;

-- name: GetProxyRunForNode :one
SELECT id, cloud_run_id, cloud_listing_link_id, cloud_listing_id,
       registry_node_id, local_agent_id, requesting_user_id,
       idempotency_key, status, payload_policy, payload_redaction_keys,
       input, input_summary,
       output, output_summary, error_code, error_message,
       attempt_count, max_attempts, next_retry_at,
       claimed_at, finished_at, created_at, updated_at
FROM proxy_runs
WHERE id = $1 AND registry_node_id = $2;

-- name: ClaimPendingProxyRun :one
WITH candidate AS (
    SELECT id, COALESCE(node_input, input, '{}'::jsonb) AS claim_input
    FROM proxy_runs
    WHERE registry_node_id = $1
      AND status = 'pending'
      AND (next_retry_at IS NULL OR next_retry_at <= NOW())
    ORDER BY created_at ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
UPDATE proxy_runs p
SET status = 'claimed',
    claimed_at = NOW(),
    next_retry_at = NULL,
    attempt_count = p.attempt_count + 1
FROM candidate
WHERE p.id = candidate.id
RETURNING p.id, p.cloud_run_id, p.cloud_listing_link_id, p.cloud_listing_id,
          p.registry_node_id, p.local_agent_id, p.requesting_user_id,
          p.idempotency_key, p.status, p.payload_policy, p.payload_redaction_keys,
          candidate.claim_input AS input, p.input_summary,
          p.output, p.output_summary, p.error_code, p.error_message,
          p.attempt_count, p.max_attempts, p.next_retry_at,
          p.claimed_at, p.finished_at, p.created_at, p.updated_at;

-- name: CompleteProxyRun :one
UPDATE proxy_runs
SET status = CASE
        WHEN $8::boolean AND $3 = 'failed' AND attempt_count < max_attempts AND node_input IS NOT NULL THEN 'pending'
        ELSE $3
    END,
    output = CASE
        WHEN $8::boolean AND $3 = 'failed' AND attempt_count < max_attempts AND node_input IS NOT NULL THEN '{}'::jsonb
        ELSE $4::jsonb
    END,
    output_summary = $5,
    error_code = $6,
    error_message = $7,
    claimed_at = CASE
        WHEN $8::boolean AND $3 = 'failed' AND attempt_count < max_attempts AND node_input IS NOT NULL THEN NULL
        ELSE COALESCE(claimed_at, NOW())
    END,
    finished_at = CASE
        WHEN $8::boolean AND $3 = 'failed' AND attempt_count < max_attempts AND node_input IS NOT NULL THEN NULL
        ELSE NOW()
    END,
    next_retry_at = CASE
        WHEN $8::boolean AND $3 = 'failed' AND attempt_count < max_attempts AND node_input IS NOT NULL THEN NOW() + make_interval(secs => $9::int)
        ELSE NULL
    END,
    node_input = CASE
        WHEN $8::boolean AND $3 = 'failed' AND attempt_count < max_attempts AND node_input IS NOT NULL THEN node_input
        ELSE NULL
    END
WHERE id = $1
  AND registry_node_id = $2
  AND status IN ('pending', 'claimed')
RETURNING id, cloud_run_id, cloud_listing_link_id, cloud_listing_id,
          registry_node_id, local_agent_id, requesting_user_id,
          idempotency_key, status, payload_policy, payload_redaction_keys,
          input, input_summary,
          output, output_summary, error_code, error_message,
          attempt_count, max_attempts, next_retry_at,
          claimed_at, finished_at, created_at, updated_at;

-- name: DeleteProxyRunArtifacts :exec
DELETE FROM proxy_run_artifacts
WHERE proxy_run_id = $1;

-- name: CreateProxyRunArtifact :one
INSERT INTO proxy_run_artifacts (
    proxy_run_id, cloud_run_id, source_artifact_id, artifact_type, title, content,
    mime_type, file_uri, file_name, file_sha256, file_size_bytes
) VALUES (
    $1, $2, $3, $4, $5, $6::jsonb,
    $7, $8, $9, $10, $11
)
ON CONFLICT (proxy_run_id, source_artifact_id) DO UPDATE
SET artifact_type = EXCLUDED.artifact_type,
    title = EXCLUDED.title,
    content = EXCLUDED.content,
    mime_type = EXCLUDED.mime_type,
    file_uri = EXCLUDED.file_uri,
    file_name = EXCLUDED.file_name,
    file_sha256 = EXCLUDED.file_sha256,
    file_size_bytes = EXCLUDED.file_size_bytes
RETURNING id, proxy_run_id, cloud_run_id, source_artifact_id, artifact_type, title, content,
          mime_type, file_uri, file_name, file_sha256, file_size_bytes, created_at;

-- name: ListProxyRunArtifactsForRequester :many
SELECT a.id, a.proxy_run_id, a.cloud_run_id, a.source_artifact_id, a.artifact_type,
       a.title, a.content, a.mime_type, a.file_uri, a.file_name, a.file_sha256,
       a.file_size_bytes, a.created_at
FROM proxy_run_artifacts a
JOIN proxy_runs p ON p.id = a.proxy_run_id
WHERE a.proxy_run_id = $1
  AND p.requesting_user_id = $2
ORDER BY a.created_at ASC, a.id ASC;

-- name: TimeoutStaleProxyRuns :one
WITH expired AS (
    UPDATE proxy_runs
    SET status = 'timeout',
        output = '{}'::jsonb,
        output_summary = COALESCE(output_summary, 'Proxy Run timed out before Registry Node returned a result'),
        error_code = COALESCE(error_code, 'PROXY_RUN_TIMEOUT'),
        error_message = COALESCE(error_message, 'Registry Node did not complete the run before timeout'),
        claimed_at = COALESCE(claimed_at, NOW()),
        finished_at = NOW(),
        next_retry_at = NULL,
        node_input = NULL
    WHERE status IN ('pending', 'claimed')
      AND COALESCE(claimed_at, created_at) < $1
    RETURNING id
)
SELECT COUNT(*)::int AS total
FROM expired;
