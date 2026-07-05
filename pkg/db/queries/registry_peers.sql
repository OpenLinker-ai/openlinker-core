-- registry_peers.sql
--
-- Registry peer federation and remote routing credentials.

-- name: CreateRegistryPeer :one
INSERT INTO registry_peers (owner_user_id, name, api_base_url, bearer_token, credential_hint, status)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING id, owner_user_id, name, api_base_url, bearer_token, credential_hint, status,
          last_used_at, created_at, updated_at;

-- name: ListRegistryPeersByOwner :many
SELECT id, owner_user_id, name, api_base_url, bearer_token, credential_hint, status,
       last_used_at, created_at, updated_at
FROM registry_peers
WHERE owner_user_id = $1
ORDER BY created_at DESC;

-- name: DeleteRegistryPeerForOwner :execrows
DELETE FROM registry_peers
WHERE id = $1 AND owner_user_id = $2;

-- name: GetActiveRegistryPeerForOwner :one
SELECT id, owner_user_id, name, api_base_url, bearer_token, credential_hint, status,
       last_used_at, created_at, updated_at
FROM registry_peers
WHERE id = $1 AND owner_user_id = $2 AND status = 'active';

-- name: ListActiveRegistryPeersForAutoRoute :many
SELECT id, owner_user_id, name, api_base_url, bearer_token, credential_hint, status,
       last_used_at, created_at, updated_at
FROM registry_peers
WHERE owner_user_id = $1 AND status = 'active'
ORDER BY last_used_at ASC NULLS FIRST, created_at ASC
LIMIT 2;

-- name: MarkRegistryPeerUsed :exec
UPDATE registry_peers
SET last_used_at = NOW()
WHERE id = $1 AND owner_user_id = $2;

-- name: CreateRegistryFederationInvite :one
INSERT INTO registry_federation_invites (
    owner_user_id, name, api_base_url, bearer_token,
    token_prefix, token_hash, credential_hint, expires_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, NOW() + ($8::int * INTERVAL '1 second')
)
RETURNING id, owner_user_id, name, api_base_url, bearer_token,
          token_prefix, token_hash, credential_hint, status,
          expires_at, consumed_at, created_at, updated_at;

-- name: ListActiveRegistryFederationInvitesByPrefixForUpdate :many
SELECT id, owner_user_id, name, api_base_url, bearer_token,
       token_prefix, token_hash, credential_hint, status,
       expires_at, consumed_at, created_at, updated_at
FROM registry_federation_invites
WHERE token_prefix = $1 AND status = 'active'
FOR UPDATE;

-- name: MarkRegistryFederationInviteExpired :exec
UPDATE registry_federation_invites
SET status = 'expired'
WHERE id = $1 AND status = 'active';

-- name: MarkRegistryFederationInviteConsumed :execrows
UPDATE registry_federation_invites
SET status = 'consumed', consumed_at = NOW()
WHERE id = $1 AND status = 'active';
