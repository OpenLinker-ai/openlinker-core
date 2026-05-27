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
FROM revoked`

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
		&l.SyncStatus,
		&l.LastSyncAt,
		&l.CreatedAt,
		&l.UpdatedAt,
	)
}

const upsertCloudListingLink = `-- name: UpsertCloudListingLink :one
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
          last_sync_at, created_at, updated_at`

type UpsertCloudListingLinkParams struct {
	RegistryNodeID uuid.UUID `db:"registry_node_id" json:"registry_node_id"`
	LocalAgentID   uuid.UUID `db:"local_agent_id" json:"local_agent_id"`
	RoutingMode    string    `db:"routing_mode" json:"routing_mode"`
	PayloadPolicy  string    `db:"payload_policy" json:"payload_policy"`
}

func (q *Queries) UpsertCloudListingLink(ctx context.Context, arg UpsertCloudListingLinkParams) (CloudListingLink, error) {
	row := q.db.QueryRow(ctx, upsertCloudListingLink,
		arg.RegistryNodeID,
		arg.LocalAgentID,
		arg.RoutingMode,
		arg.PayloadPolicy,
	)
	var l CloudListingLink
	err := scanCloudListingLink(row, &l)
	return l, err
}

const listCloudListingLinksByOwner = `-- name: ListCloudListingLinksByOwner :many
SELECT l.id, l.cloud_listing_id, l.registry_node_id, n.node_name,
       l.local_agent_id, a.slug AS agent_slug, a.name AS agent_name,
       l.routing_mode, l.payload_policy, l.sync_status,
       l.last_sync_at, l.created_at, l.updated_at
FROM cloud_listing_links l
JOIN registry_nodes n ON n.id = l.registry_node_id
JOIN agents a ON a.id = l.local_agent_id
WHERE n.owner_user_id = $1
ORDER BY l.created_at DESC`

type ListCloudListingLinksByOwnerRow struct {
	ID             uuid.UUID `db:"id" json:"id"`
	CloudListingID uuid.UUID `db:"cloud_listing_id" json:"cloud_listing_id"`
	RegistryNodeID uuid.UUID `db:"registry_node_id" json:"registry_node_id"`
	NodeName       string    `db:"node_name" json:"node_name"`
	LocalAgentID   uuid.UUID `db:"local_agent_id" json:"local_agent_id"`
	AgentSlug      string    `db:"agent_slug" json:"agent_slug"`
	AgentName      string    `db:"agent_name" json:"agent_name"`
	RoutingMode    string    `db:"routing_mode" json:"routing_mode"`
	PayloadPolicy  string    `db:"payload_policy" json:"payload_policy"`
	SyncStatus     string    `db:"sync_status" json:"sync_status"`
	LastSyncAt     time.Time `db:"last_sync_at" json:"last_sync_at"`
	CreatedAt      time.Time `db:"created_at" json:"created_at"`
	UpdatedAt      time.Time `db:"updated_at" json:"updated_at"`
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
			&l.SyncStatus,
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
          l.local_agent_id, a.slug AS agent_slug, a.name AS agent_name,
          l.routing_mode, l.payload_policy, l.sync_status,
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
		&l.SyncStatus,
		&l.LastSyncAt,
		&l.CreatedAt,
		&l.UpdatedAt,
	)
	return l, err
}

const getCloudListingLinkForProxyRun = `-- name: GetCloudListingLinkForProxyRun :one
SELECT l.id, l.cloud_listing_id, l.registry_node_id, l.local_agent_id,
       l.routing_mode, l.payload_policy, l.sync_status,
       l.last_sync_at, l.created_at, l.updated_at
FROM cloud_listing_links l
JOIN registry_nodes n ON n.id = l.registry_node_id
WHERE l.cloud_listing_id = $1
  AND l.sync_status = 'linked'
  AND n.revoked_at IS NULL`

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
		&r.Input,
		&r.InputSummary,
		&r.Output,
		&r.OutputSummary,
		&r.ErrorCode,
		&r.ErrorMessage,
		&r.ClaimedAt,
		&r.FinishedAt,
		&r.CreatedAt,
		&r.UpdatedAt,
	)
}

const createProxyRun = `-- name: CreateProxyRun :one
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
LIMIT 1`

type CreateProxyRunParams struct {
	CloudListingID   uuid.UUID `db:"cloud_listing_id" json:"cloud_listing_id"`
	RequestingUserID uuid.UUID `db:"requesting_user_id" json:"requesting_user_id"`
	IdempotencyKey   string    `db:"idempotency_key" json:"idempotency_key"`
	Input            []byte    `db:"input" json:"input"`
	InputSummary     *string   `db:"input_summary" json:"input_summary"`
}

func (q *Queries) CreateProxyRun(ctx context.Context, arg CreateProxyRunParams) (ProxyRun, error) {
	row := q.db.QueryRow(ctx, createProxyRun,
		arg.CloudListingID,
		arg.RequestingUserID,
		arg.IdempotencyKey,
		arg.Input,
		arg.InputSummary,
	)
	var r ProxyRun
	err := scanProxyRun(row, &r)
	return r, err
}

const getProxyRunForRequester = `-- name: GetProxyRunForRequester :one
SELECT id, cloud_run_id, cloud_listing_link_id, cloud_listing_id,
       registry_node_id, local_agent_id, requesting_user_id,
       idempotency_key, status, payload_policy, input, input_summary,
       output, output_summary, error_code, error_message,
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

const claimPendingProxyRun = `-- name: ClaimPendingProxyRun :one
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
          p.claimed_at, p.finished_at, p.created_at, p.updated_at`

func (q *Queries) ClaimPendingProxyRun(ctx context.Context, registryNodeID uuid.UUID) (ProxyRun, error) {
	row := q.db.QueryRow(ctx, claimPendingProxyRun, registryNodeID)
	var r ProxyRun
	err := scanProxyRun(row, &r)
	return r, err
}

const completeProxyRun = `-- name: CompleteProxyRun :one
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
          claimed_at, finished_at, created_at, updated_at`

type CompleteProxyRunParams struct {
	ID             uuid.UUID `db:"id" json:"id"`
	RegistryNodeID uuid.UUID `db:"registry_node_id" json:"registry_node_id"`
	Status         string    `db:"status" json:"status"`
	Output         []byte    `db:"output" json:"output"`
	OutputSummary  *string   `db:"output_summary" json:"output_summary"`
	ErrorCode      *string   `db:"error_code" json:"error_code"`
	ErrorMessage   *string   `db:"error_message" json:"error_message"`
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
	)
	var r ProxyRun
	err := scanProxyRun(row, &r)
	return r, err
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
        finished_at = NOW()
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
