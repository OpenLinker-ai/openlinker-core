// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/registry_bridge.sql）。

package db

import (
	"context"
	"time"

	"github.com/google/uuid"
)

func scanRegistryNode(row interface {
	Scan(dest ...any) error
}, n *RegistryNode) error {
	return row.Scan(
		&n.ID,
		&n.OwnerUserID,
		&n.NodeName,
		&n.NodeType,
		&n.BaseURL,
		&n.SecretPrefix,
		&n.SecretHash,
		&n.Scopes,
		&n.HeartbeatStatus,
		&n.LastHeartbeatAt,
		&n.RevokedAt,
		&n.CreatedAt,
		&n.UpdatedAt,
	)
}

const createRegistryNode = `-- name: CreateRegistryNode :one
INSERT INTO registry_nodes (
    owner_user_id, node_name, node_type, base_url,
    secret_prefix, secret_hash, scopes
) VALUES (
    $1, $2, $3, $4,
    $5, $6, $7
)
RETURNING id, owner_user_id, node_name, node_type, base_url,
          secret_prefix, secret_hash, scopes, heartbeat_status,
          last_heartbeat_at, revoked_at, created_at, updated_at`

type CreateRegistryNodeParams struct {
	OwnerUserID  uuid.UUID `db:"owner_user_id" json:"owner_user_id"`
	NodeName     string    `db:"node_name" json:"node_name"`
	NodeType     string    `db:"node_type" json:"node_type"`
	BaseURL      *string   `db:"base_url" json:"base_url"`
	SecretPrefix string    `db:"secret_prefix" json:"secret_prefix"`
	SecretHash   string    `db:"secret_hash" json:"secret_hash"`
	Scopes       []string  `db:"scopes" json:"scopes"`
}

func (q *Queries) CreateRegistryNode(ctx context.Context, arg CreateRegistryNodeParams) (RegistryNode, error) {
	row := q.db.QueryRow(ctx, createRegistryNode,
		arg.OwnerUserID,
		arg.NodeName,
		arg.NodeType,
		arg.BaseURL,
		arg.SecretPrefix,
		arg.SecretHash,
		arg.Scopes,
	)
	var n RegistryNode
	err := scanRegistryNode(row, &n)
	return n, err
}

const listRegistryNodesByOwner = `-- name: ListRegistryNodesByOwner :many
SELECT id, owner_user_id, node_name, node_type, base_url,
       secret_prefix, secret_hash, scopes, heartbeat_status,
       last_heartbeat_at, revoked_at, created_at, updated_at
FROM registry_nodes
WHERE owner_user_id = $1
ORDER BY created_at DESC`

func (q *Queries) ListRegistryNodesByOwner(ctx context.Context, ownerUserID uuid.UUID) ([]RegistryNode, error) {
	rows, err := q.db.Query(ctx, listRegistryNodesByOwner, ownerUserID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []RegistryNode
	for rows.Next() {
		var n RegistryNode
		if err := scanRegistryNode(rows, &n); err != nil {
			return nil, err
		}
		items = append(items, n)
	}
	return items, rows.Err()
}

const getRegistryNodeByIDForOwner = `-- name: GetRegistryNodeByIDForOwner :one
SELECT id, owner_user_id, node_name, node_type, base_url,
       secret_prefix, secret_hash, scopes, heartbeat_status,
       last_heartbeat_at, revoked_at, created_at, updated_at
FROM registry_nodes
WHERE id = $1 AND owner_user_id = $2`

type GetRegistryNodeByIDForOwnerParams struct {
	ID          uuid.UUID `db:"id" json:"id"`
	OwnerUserID uuid.UUID `db:"owner_user_id" json:"owner_user_id"`
}

func (q *Queries) GetRegistryNodeByIDForOwner(ctx context.Context, arg GetRegistryNodeByIDForOwnerParams) (RegistryNode, error) {
	row := q.db.QueryRow(ctx, getRegistryNodeByIDForOwner, arg.ID, arg.OwnerUserID)
	var n RegistryNode
	err := scanRegistryNode(row, &n)
	return n, err
}

const listActiveRegistryNodesBySecretPrefix = `-- name: ListActiveRegistryNodesBySecretPrefix :many
SELECT id, owner_user_id, node_name, node_type, base_url,
       secret_prefix, secret_hash, scopes, heartbeat_status,
       last_heartbeat_at, revoked_at, created_at, updated_at
FROM registry_nodes
WHERE secret_prefix = $1 AND revoked_at IS NULL`

func (q *Queries) ListActiveRegistryNodesBySecretPrefix(ctx context.Context, secretPrefix string) ([]RegistryNode, error) {
	rows, err := q.db.Query(ctx, listActiveRegistryNodesBySecretPrefix, secretPrefix)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []RegistryNode
	for rows.Next() {
		var n RegistryNode
		if err := scanRegistryNode(rows, &n); err != nil {
			return nil, err
		}
		items = append(items, n)
	}
	return items, rows.Err()
}

const markRegistryNodeHeartbeat = `-- name: MarkRegistryNodeHeartbeat :one
UPDATE registry_nodes
SET heartbeat_status = 'healthy',
    last_heartbeat_at = NOW()
WHERE id = $1 AND revoked_at IS NULL
RETURNING id, owner_user_id, node_name, node_type, base_url,
          secret_prefix, secret_hash, scopes, heartbeat_status,
          last_heartbeat_at, revoked_at, created_at, updated_at`

func (q *Queries) MarkRegistryNodeHeartbeat(ctx context.Context, id uuid.UUID) (RegistryNode, error) {
	row := q.db.QueryRow(ctx, markRegistryNodeHeartbeat, id)
	var n RegistryNode
	err := scanRegistryNode(row, &n)
	return n, err
}

const revokeRegistryNodeForOwner = `-- name: RevokeRegistryNodeForOwner :one
WITH revoked AS (
    UPDATE registry_nodes rn
    SET revoked_at = COALESCE(revoked_at, NOW()),
        heartbeat_status = 'revoked'
    WHERE rn.id = $1 AND rn.owner_user_id = $2
    RETURNING rn.id, rn.owner_user_id, rn.node_name, rn.node_type, rn.base_url,
              rn.secret_prefix, rn.secret_hash, rn.scopes, rn.heartbeat_status,
              rn.last_heartbeat_at, rn.revoked_at, rn.created_at, rn.updated_at
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
SELECT r.id, r.owner_user_id, r.node_name, r.node_type, r.base_url,
       r.secret_prefix, r.secret_hash, r.scopes, r.heartbeat_status,
       r.last_heartbeat_at, r.revoked_at, r.created_at, r.updated_at
FROM revoked r`

type RevokeRegistryNodeForOwnerParams struct {
	ID          uuid.UUID `db:"id" json:"id"`
	OwnerUserID uuid.UUID `db:"owner_user_id" json:"owner_user_id"`
}

func (q *Queries) RevokeRegistryNodeForOwner(ctx context.Context, arg RevokeRegistryNodeForOwnerParams) (RegistryNode, error) {
	row := q.db.QueryRow(ctx, revokeRegistryNodeForOwner, arg.ID, arg.OwnerUserID)
	var n RegistryNode
	err := scanRegistryNode(row, &n)
	return n, err
}

const rotateRegistryNodeSecretForOwner = `-- name: RotateRegistryNodeSecretForOwner :one
UPDATE registry_nodes
SET secret_prefix = $3,
    secret_hash = $4
WHERE id = $1
  AND owner_user_id = $2
  AND revoked_at IS NULL
RETURNING id, owner_user_id, node_name, node_type, base_url,
          secret_prefix, secret_hash, scopes, heartbeat_status,
          last_heartbeat_at, revoked_at, created_at, updated_at`

type RotateRegistryNodeSecretForOwnerParams struct {
	ID           uuid.UUID `db:"id" json:"id"`
	OwnerUserID  uuid.UUID `db:"owner_user_id" json:"owner_user_id"`
	SecretPrefix string    `db:"secret_prefix" json:"secret_prefix"`
	SecretHash   string    `db:"secret_hash" json:"secret_hash"`
}

func (q *Queries) RotateRegistryNodeSecretForOwner(ctx context.Context, arg RotateRegistryNodeSecretForOwnerParams) (RegistryNode, error) {
	row := q.db.QueryRow(ctx, rotateRegistryNodeSecretForOwner,
		arg.ID,
		arg.OwnerUserID,
		arg.SecretPrefix,
		arg.SecretHash,
	)
	var n RegistryNode
	err := scanRegistryNode(row, &n)
	return n, err
}

const countCloudListingLinksByNode = `-- name: CountCloudListingLinksByNode :one
SELECT COUNT(*)::int AS total
FROM cloud_listing_links
WHERE registry_node_id = $1`

func (q *Queries) CountCloudListingLinksByNode(ctx context.Context, registryNodeID uuid.UUID) (int32, error) {
	row := q.db.QueryRow(ctx, countCloudListingLinksByNode, registryNodeID)
	var total int32
	err := row.Scan(&total)
	return total, err
}

const countPendingProxyRunsByNode = `-- name: CountPendingProxyRunsByNode :one
SELECT COUNT(*)::int AS total
FROM proxy_runs
WHERE registry_node_id = $1
  AND status = 'pending'`

func (q *Queries) CountPendingProxyRunsByNode(ctx context.Context, registryNodeID uuid.UUID) (int32, error) {
	row := q.db.QueryRow(ctx, countPendingProxyRunsByNode, registryNodeID)
	var total int32
	err := row.Scan(&total)
	return total, err
}

func scanCloudListingLink(row interface {
	Scan(dest ...any) error
}, l *CloudListingLink) error {
	return row.Scan(
		&l.ID,
		&l.CloudListingID,
		&l.RegistryNodeID,
		&l.LocalAgentID,
		&l.RoutingMode,
		&l.PayloadPolicy,
		&l.PayloadRedactionKeys,
		&l.SyncStatus,
		&l.SyncedAgentSlug,
		&l.SyncedAgentName,
		&l.SyncedAgentDescription,
		&l.SyncedAgentTags,
		&l.SyncedAvailabilityStatus,
		&l.MetadataSyncedAt,
		&l.MetadataSyncError,
		&l.LastSyncAt,
		&l.CreatedAt,
		&l.UpdatedAt,
	)
}

const upsertCloudListingLink = `-- name: UpsertCloudListingLink :one
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
          last_sync_at, created_at, updated_at`

type UpsertCloudListingLinkParams struct {
	CloudListingID       uuid.UUID `db:"cloud_listing_id" json:"cloud_listing_id"`
	RegistryNodeID       uuid.UUID `db:"registry_node_id" json:"registry_node_id"`
	LocalAgentID         uuid.UUID `db:"local_agent_id" json:"local_agent_id"`
	RoutingMode          string    `db:"routing_mode" json:"routing_mode"`
	PayloadPolicy        string    `db:"payload_policy" json:"payload_policy"`
	PayloadRedactionKeys []string  `db:"payload_redaction_keys" json:"payload_redaction_keys"`
}

func (q *Queries) UpsertCloudListingLink(ctx context.Context, arg UpsertCloudListingLinkParams) (CloudListingLink, error) {
	row := q.db.QueryRow(ctx, upsertCloudListingLink,
		arg.CloudListingID,
		arg.RegistryNodeID,
		arg.LocalAgentID,
		arg.RoutingMode,
		arg.PayloadPolicy,
		arg.PayloadRedactionKeys,
	)
	var l CloudListingLink
	err := scanCloudListingLink(row, &l)
	return l, err
}

const getCloudListingLinkForOwner = `-- name: GetCloudListingLinkForOwner :one
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
LIMIT 1`

type GetCloudListingLinkForOwnerParams struct {
	CloudListingID uuid.UUID `db:"cloud_listing_id" json:"cloud_listing_id"`
	OwnerUserID    uuid.UUID `db:"owner_user_id" json:"owner_user_id"`
}

func (q *Queries) GetCloudListingLinkForOwner(ctx context.Context, arg GetCloudListingLinkForOwnerParams) (CloudListingLink, error) {
	row := q.db.QueryRow(ctx, getCloudListingLinkForOwner, arg.CloudListingID, arg.OwnerUserID)
	var l CloudListingLink
	err := scanCloudListingLink(row, &l)
	return l, err
}

const listCloudListingLinksByOwner = `-- name: ListCloudListingLinksByOwner :many
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
ORDER BY l.created_at DESC`

type ListCloudListingLinksByOwnerRow struct {
	ID                   uuid.UUID  `db:"id" json:"id"`
	CloudListingID       uuid.UUID  `db:"cloud_listing_id" json:"cloud_listing_id"`
	RegistryNodeID       uuid.UUID  `db:"registry_node_id" json:"registry_node_id"`
	NodeName             string     `db:"node_name" json:"node_name"`
	LocalAgentID         uuid.UUID  `db:"local_agent_id" json:"local_agent_id"`
	AgentSlug            string     `db:"agent_slug" json:"agent_slug"`
	AgentName            string     `db:"agent_name" json:"agent_name"`
	RoutingMode          string     `db:"routing_mode" json:"routing_mode"`
	PayloadPolicy        string     `db:"payload_policy" json:"payload_policy"`
	PayloadRedactionKeys []string   `db:"payload_redaction_keys" json:"payload_redaction_keys"`
	SyncStatus           string     `db:"sync_status" json:"sync_status"`
	AgentDescription     string     `db:"agent_description" json:"agent_description"`
	AgentTags            []string   `db:"agent_tags" json:"agent_tags"`
	AvailabilityStatus   string     `db:"availability_status" json:"availability_status"`
	MetadataSyncedAt     *time.Time `db:"metadata_synced_at" json:"metadata_synced_at"`
	MetadataSyncError    *string    `db:"metadata_sync_error" json:"metadata_sync_error"`
	LastSyncAt           time.Time  `db:"last_sync_at" json:"last_sync_at"`
	CreatedAt            time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt            time.Time  `db:"updated_at" json:"updated_at"`
}

func (q *Queries) ListCloudListingLinksByOwner(ctx context.Context, ownerUserID uuid.UUID) ([]ListCloudListingLinksByOwnerRow, error) {
	rows, err := q.db.Query(ctx, listCloudListingLinksByOwner, ownerUserID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []ListCloudListingLinksByOwnerRow
	for rows.Next() {
		var l ListCloudListingLinksByOwnerRow
		if err := rows.Scan(
			&l.ID,
			&l.CloudListingID,
			&l.RegistryNodeID,
			&l.NodeName,
			&l.LocalAgentID,
			&l.AgentSlug,
			&l.AgentName,
			&l.RoutingMode,
			&l.PayloadPolicy,
			&l.PayloadRedactionKeys,
			&l.SyncStatus,
			&l.AgentDescription,
			&l.AgentTags,
			&l.AvailabilityStatus,
			&l.MetadataSyncedAt,
			&l.MetadataSyncError,
			&l.LastSyncAt,
			&l.CreatedAt,
			&l.UpdatedAt,
		); err != nil {
			return nil, err
		}
		items = append(items, l)
	}
	return items, rows.Err()
}

const getCloudListingLinkRowForOwner = `-- name: GetCloudListingLinkRowForOwner :one
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
  AND n.owner_user_id = $2`

type GetCloudListingLinkRowForOwnerParams struct {
	ID          uuid.UUID `db:"id" json:"id"`
	OwnerUserID uuid.UUID `db:"owner_user_id" json:"owner_user_id"`
}

func (q *Queries) GetCloudListingLinkRowForOwner(ctx context.Context, arg GetCloudListingLinkRowForOwnerParams) (ListCloudListingLinksByOwnerRow, error) {
	row := q.db.QueryRow(ctx, getCloudListingLinkRowForOwner, arg.ID, arg.OwnerUserID)
	var l ListCloudListingLinksByOwnerRow
	err := row.Scan(
		&l.ID,
		&l.CloudListingID,
		&l.RegistryNodeID,
		&l.NodeName,
		&l.LocalAgentID,
		&l.AgentSlug,
		&l.AgentName,
		&l.RoutingMode,
		&l.PayloadPolicy,
		&l.PayloadRedactionKeys,
		&l.SyncStatus,
		&l.AgentDescription,
		&l.AgentTags,
		&l.AvailabilityStatus,
		&l.MetadataSyncedAt,
		&l.MetadataSyncError,
		&l.LastSyncAt,
		&l.CreatedAt,
		&l.UpdatedAt,
	)
	return l, err
}

const updateCloudListingLinkStatusForOwner = `-- name: UpdateCloudListingLinkStatusForOwner :one
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
          l.last_sync_at, l.created_at, l.updated_at`

type UpdateCloudListingLinkStatusForOwnerParams struct {
	CloudListingID uuid.UUID `db:"cloud_listing_id" json:"cloud_listing_id"`
	OwnerUserID    uuid.UUID `db:"owner_user_id" json:"owner_user_id"`
	SyncStatus     string    `db:"sync_status" json:"sync_status"`
}

func (q *Queries) UpdateCloudListingLinkStatusForOwner(ctx context.Context, arg UpdateCloudListingLinkStatusForOwnerParams) (ListCloudListingLinksByOwnerRow, error) {
	row := q.db.QueryRow(ctx, updateCloudListingLinkStatusForOwner, arg.CloudListingID, arg.OwnerUserID, arg.SyncStatus)
	var l ListCloudListingLinksByOwnerRow
	err := row.Scan(
		&l.ID,
		&l.CloudListingID,
		&l.RegistryNodeID,
		&l.NodeName,
		&l.LocalAgentID,
		&l.AgentSlug,
		&l.AgentName,
		&l.RoutingMode,
		&l.PayloadPolicy,
		&l.PayloadRedactionKeys,
		&l.SyncStatus,
		&l.AgentDescription,
		&l.AgentTags,
		&l.AvailabilityStatus,
		&l.MetadataSyncedAt,
		&l.MetadataSyncError,
		&l.LastSyncAt,
		&l.CreatedAt,
		&l.UpdatedAt,
	)
	return l, err
}

const syncCloudListingMetadataForOwner = `-- name: SyncCloudListingMetadataForOwner :one
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
          l.last_sync_at, l.created_at, l.updated_at`

type SyncCloudListingMetadataForOwnerParams struct {
	CloudListingID uuid.UUID `db:"cloud_listing_id" json:"cloud_listing_id"`
	OwnerUserID    uuid.UUID `db:"owner_user_id" json:"owner_user_id"`
}

func (q *Queries) SyncCloudListingMetadataForOwner(ctx context.Context, arg SyncCloudListingMetadataForOwnerParams) (ListCloudListingLinksByOwnerRow, error) {
	row := q.db.QueryRow(ctx, syncCloudListingMetadataForOwner, arg.CloudListingID, arg.OwnerUserID)
	var l ListCloudListingLinksByOwnerRow
	err := row.Scan(
		&l.ID,
		&l.CloudListingID,
		&l.RegistryNodeID,
		&l.NodeName,
		&l.LocalAgentID,
		&l.AgentSlug,
		&l.AgentName,
		&l.RoutingMode,
		&l.PayloadPolicy,
		&l.PayloadRedactionKeys,
		&l.SyncStatus,
		&l.AgentDescription,
		&l.AgentTags,
		&l.AvailabilityStatus,
		&l.MetadataSyncedAt,
		&l.MetadataSyncError,
		&l.LastSyncAt,
		&l.CreatedAt,
		&l.UpdatedAt,
	)
	return l, err
}

const syncCloudListingMetadataByNode = `-- name: SyncCloudListingMetadataByNode :one
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
FROM synced`

func (q *Queries) SyncCloudListingMetadataByNode(ctx context.Context, registryNodeID uuid.UUID) (int32, error) {
	row := q.db.QueryRow(ctx, syncCloudListingMetadataByNode, registryNodeID)
	var total int32
	err := row.Scan(&total)
	return total, err
}

const getCloudListingLinkForProxyRun = `-- name: GetCloudListingLinkForProxyRun :one
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
LIMIT 1`

func (q *Queries) GetCloudListingLinkForProxyRun(ctx context.Context, cloudListingID uuid.UUID) (CloudListingLink, error) {
	row := q.db.QueryRow(ctx, getCloudListingLinkForProxyRun, cloudListingID)
	var l CloudListingLink
	err := scanCloudListingLink(row, &l)
	return l, err
}

func scanProxyRun(row interface {
	Scan(dest ...any) error
}, r *ProxyRun) error {
	return row.Scan(
		&r.ID,
		&r.CloudRunID,
		&r.CloudListingLinkID,
		&r.CloudListingID,
		&r.RegistryNodeID,
		&r.LocalAgentID,
		&r.RequestingUserID,
		&r.IdempotencyKey,
		&r.Status,
		&r.PayloadPolicy,
		&r.PayloadRedactionKeys,
		&r.Input,
		&r.InputSummary,
		&r.Output,
		&r.OutputSummary,
		&r.ErrorCode,
		&r.ErrorMessage,
		&r.AttemptCount,
		&r.MaxAttempts,
		&r.NextRetryAt,
		&r.ClaimedAt,
		&r.FinishedAt,
		&r.CreatedAt,
		&r.UpdatedAt,
	)
}

const createProxyRun = `-- name: CreateProxyRun :one
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
LIMIT 1`

type CreateProxyRunParams struct {
	CloudListingID     uuid.UUID `db:"cloud_listing_id" json:"cloud_listing_id"`
	CloudListingLinkID uuid.UUID `db:"cloud_listing_link_id" json:"cloud_listing_link_id"`
	RequestingUserID   uuid.UUID `db:"requesting_user_id" json:"requesting_user_id"`
	IdempotencyKey     string    `db:"idempotency_key" json:"idempotency_key"`
	Input              []byte    `db:"input" json:"input"`
	InputSummary       *string   `db:"input_summary" json:"input_summary"`
	NodeInput          []byte    `db:"node_input" json:"node_input"`
}

func (q *Queries) CreateProxyRun(ctx context.Context, arg CreateProxyRunParams) (ProxyRun, error) {
	row := q.db.QueryRow(ctx, createProxyRun,
		arg.CloudListingID,
		arg.CloudListingLinkID,
		arg.RequestingUserID,
		arg.IdempotencyKey,
		arg.Input,
		arg.InputSummary,
		arg.NodeInput,
	)
	var r ProxyRun
	err := scanProxyRun(row, &r)
	return r, err
}

const getProxyRunForRequester = `-- name: GetProxyRunForRequester :one
SELECT id, cloud_run_id, cloud_listing_link_id, cloud_listing_id,
       registry_node_id, local_agent_id, requesting_user_id,
       idempotency_key, status, payload_policy, payload_redaction_keys,
       input, input_summary,
       output, output_summary, error_code, error_message,
       attempt_count, max_attempts, next_retry_at,
       claimed_at, finished_at, created_at, updated_at
FROM proxy_runs
WHERE id = $1 AND requesting_user_id = $2`

type GetProxyRunForRequesterParams struct {
	ID               uuid.UUID `db:"id" json:"id"`
	RequestingUserID uuid.UUID `db:"requesting_user_id" json:"requesting_user_id"`
}

func (q *Queries) GetProxyRunForRequester(ctx context.Context, arg GetProxyRunForRequesterParams) (ProxyRun, error) {
	row := q.db.QueryRow(ctx, getProxyRunForRequester, arg.ID, arg.RequestingUserID)
	var r ProxyRun
	err := scanProxyRun(row, &r)
	return r, err
}

const getProxyRunForNode = `-- name: GetProxyRunForNode :one
SELECT id, cloud_run_id, cloud_listing_link_id, cloud_listing_id,
       registry_node_id, local_agent_id, requesting_user_id,
       idempotency_key, status, payload_policy, payload_redaction_keys,
       input, input_summary,
       output, output_summary, error_code, error_message,
       attempt_count, max_attempts, next_retry_at,
       claimed_at, finished_at, created_at, updated_at
FROM proxy_runs
WHERE id = $1 AND registry_node_id = $2`

type GetProxyRunForNodeParams struct {
	ID             uuid.UUID `db:"id" json:"id"`
	RegistryNodeID uuid.UUID `db:"registry_node_id" json:"registry_node_id"`
}

func (q *Queries) GetProxyRunForNode(ctx context.Context, arg GetProxyRunForNodeParams) (ProxyRun, error) {
	row := q.db.QueryRow(ctx, getProxyRunForNode, arg.ID, arg.RegistryNodeID)
	var r ProxyRun
	err := scanProxyRun(row, &r)
	return r, err
}

const claimPendingProxyRun = `-- name: ClaimPendingProxyRun :one
WITH candidate AS (
    SELECT p.id, COALESCE(p.node_input, p.input, '{}'::jsonb) AS claim_input
    FROM proxy_runs p
    WHERE p.registry_node_id = $1
      AND p.status = 'pending'
      AND (p.next_retry_at IS NULL OR p.next_retry_at <= NOW())
    ORDER BY p.created_at ASC
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
          p.claimed_at, p.finished_at, p.created_at, p.updated_at`

func (q *Queries) ClaimPendingProxyRun(ctx context.Context, registryNodeID uuid.UUID) (ProxyRun, error) {
	row := q.db.QueryRow(ctx, claimPendingProxyRun, registryNodeID)
	var r ProxyRun
	err := scanProxyRun(row, &r)
	return r, err
}

const completeProxyRun = `-- name: CompleteProxyRun :one
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
          claimed_at, finished_at, created_at, updated_at`

type CompleteProxyRunParams struct {
	ID             uuid.UUID `db:"id" json:"id"`
	RegistryNodeID uuid.UUID `db:"registry_node_id" json:"registry_node_id"`
	Status         string    `db:"status" json:"status"`
	Output         []byte    `db:"output" json:"output"`
	OutputSummary  *string   `db:"output_summary" json:"output_summary"`
	ErrorCode      *string   `db:"error_code" json:"error_code"`
	ErrorMessage   *string   `db:"error_message" json:"error_message"`
	Retryable      bool      `db:"retryable" json:"retryable"`
	RetryAfterSecs int32     `db:"retry_after_secs" json:"retry_after_secs"`
}

func (q *Queries) CompleteProxyRun(ctx context.Context, arg CompleteProxyRunParams) (ProxyRun, error) {
	row := q.db.QueryRow(ctx, completeProxyRun,
		arg.ID,
		arg.RegistryNodeID,
		arg.Status,
		arg.Output,
		arg.OutputSummary,
		arg.ErrorCode,
		arg.ErrorMessage,
		arg.Retryable,
		arg.RetryAfterSecs,
	)
	var r ProxyRun
	err := scanProxyRun(row, &r)
	return r, err
}

func scanProxyRunArtifact(row interface {
	Scan(dest ...any) error
}, a *ProxyRunArtifact) error {
	return row.Scan(
		&a.ID,
		&a.ProxyRunID,
		&a.CloudRunID,
		&a.SourceArtifactID,
		&a.ArtifactType,
		&a.Title,
		&a.Content,
		&a.MimeType,
		&a.FileURI,
		&a.FileName,
		&a.FileSHA256,
		&a.FileSizeBytes,
		&a.CreatedAt,
	)
}

const deleteProxyRunArtifacts = `-- name: DeleteProxyRunArtifacts :exec
DELETE FROM proxy_run_artifacts
WHERE proxy_run_id = $1`

func (q *Queries) DeleteProxyRunArtifacts(ctx context.Context, proxyRunID uuid.UUID) error {
	_, err := q.db.Exec(ctx, deleteProxyRunArtifacts, proxyRunID)
	return err
}

const createProxyRunArtifact = `-- name: CreateProxyRunArtifact :one
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
          mime_type, file_uri, file_name, file_sha256, file_size_bytes, created_at`

type CreateProxyRunArtifactParams struct {
	ProxyRunID       uuid.UUID `db:"proxy_run_id" json:"proxy_run_id"`
	CloudRunID       uuid.UUID `db:"cloud_run_id" json:"cloud_run_id"`
	SourceArtifactID string    `db:"source_artifact_id" json:"source_artifact_id"`
	ArtifactType     string    `db:"artifact_type" json:"artifact_type"`
	Title            string    `db:"title" json:"title"`
	Content          []byte    `db:"content" json:"content"`
	MimeType         *string   `db:"mime_type" json:"mime_type"`
	FileURI          *string   `db:"file_uri" json:"file_uri"`
	FileName         *string   `db:"file_name" json:"file_name"`
	FileSHA256       *string   `db:"file_sha256" json:"file_sha256"`
	FileSizeBytes    *int64    `db:"file_size_bytes" json:"file_size_bytes"`
}

func (q *Queries) CreateProxyRunArtifact(ctx context.Context, arg CreateProxyRunArtifactParams) (ProxyRunArtifact, error) {
	row := q.db.QueryRow(ctx, createProxyRunArtifact,
		arg.ProxyRunID,
		arg.CloudRunID,
		arg.SourceArtifactID,
		arg.ArtifactType,
		arg.Title,
		arg.Content,
		arg.MimeType,
		arg.FileURI,
		arg.FileName,
		arg.FileSHA256,
		arg.FileSizeBytes,
	)
	var a ProxyRunArtifact
	err := scanProxyRunArtifact(row, &a)
	return a, err
}

const listProxyRunArtifactsForRequester = `-- name: ListProxyRunArtifactsForRequester :many
SELECT a.id, a.proxy_run_id, a.cloud_run_id, a.source_artifact_id, a.artifact_type,
       a.title, a.content, a.mime_type, a.file_uri, a.file_name, a.file_sha256,
       a.file_size_bytes, a.created_at
FROM proxy_run_artifacts a
JOIN proxy_runs p ON p.id = a.proxy_run_id
WHERE a.proxy_run_id = $1
  AND p.requesting_user_id = $2
ORDER BY a.created_at ASC, a.id ASC`

type ListProxyRunArtifactsForRequesterParams struct {
	ProxyRunID       uuid.UUID `db:"proxy_run_id" json:"proxy_run_id"`
	RequestingUserID uuid.UUID `db:"requesting_user_id" json:"requesting_user_id"`
}

func (q *Queries) ListProxyRunArtifactsForRequester(ctx context.Context, arg ListProxyRunArtifactsForRequesterParams) ([]ProxyRunArtifact, error) {
	rows, err := q.db.Query(ctx, listProxyRunArtifactsForRequester, arg.ProxyRunID, arg.RequestingUserID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []ProxyRunArtifact
	for rows.Next() {
		var artifact ProxyRunArtifact
		if err := scanProxyRunArtifact(rows, &artifact); err != nil {
			return nil, err
		}
		items = append(items, artifact)
	}
	return items, rows.Err()
}

const getProxyRunArtifactForRequester = `-- name: GetProxyRunArtifactForRequester :one
SELECT a.id, a.proxy_run_id, a.cloud_run_id, a.source_artifact_id, a.artifact_type,
       a.title, a.content, a.mime_type, a.file_uri, a.file_name, a.file_sha256,
       a.file_size_bytes, a.created_at
FROM proxy_run_artifacts a
JOIN proxy_runs p ON p.id = a.proxy_run_id
WHERE a.id = $1
  AND a.proxy_run_id = $2
  AND p.requesting_user_id = $3`

type GetProxyRunArtifactForRequesterParams struct {
	ID               uuid.UUID `db:"id" json:"id"`
	ProxyRunID       uuid.UUID `db:"proxy_run_id" json:"proxy_run_id"`
	RequestingUserID uuid.UUID `db:"requesting_user_id" json:"requesting_user_id"`
}

func (q *Queries) GetProxyRunArtifactForRequester(ctx context.Context, arg GetProxyRunArtifactForRequesterParams) (ProxyRunArtifact, error) {
	row := q.db.QueryRow(ctx, getProxyRunArtifactForRequester, arg.ID, arg.ProxyRunID, arg.RequestingUserID)
	var artifact ProxyRunArtifact
	err := scanProxyRunArtifact(row, &artifact)
	return artifact, err
}

const timeoutStaleProxyRuns = `-- name: TimeoutStaleProxyRuns :one
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
FROM expired`

func (q *Queries) TimeoutStaleProxyRuns(ctx context.Context, staleBefore time.Time) (int32, error) {
	row := q.db.QueryRow(ctx, timeoutStaleProxyRuns, staleBefore)
	var total int32
	err := row.Scan(&total)
	return total, err
}
