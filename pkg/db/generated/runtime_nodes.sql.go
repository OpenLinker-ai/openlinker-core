// Code generated manually as a placeholder for sqlc output.
// TODO: Generate with sqlc from pkg/db/queries/runtime_nodes.sql.

package db

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type runtimeNodeScanner interface {
	Scan(dest ...any) error
}

func scanRuntimeSchemaContract(row runtimeNodeScanner, c *RuntimeSchemaContract) error {
	return row.Scan(
		&c.SchemaVersion,
		&c.MigrationName,
		&c.RuntimeContractID,
		&c.RuntimeContractDigest,
		&c.AppliedAt,
		&c.IsCurrent,
	)
}

func scanRuntimeClusterControl(row runtimeNodeScanner, c *RuntimeClusterControl) error {
	return row.Scan(
		&c.SingletonID,
		&c.Mode,
		&c.ExpectedReplicas,
		&c.CutoverID,
		&c.DrainStartedAt,
		&c.DrainDeadlineAt,
		&c.HardMaintenanceAt,
		&c.ReopenedAt,
		&c.Version,
		&c.UpdatedByType,
		&c.UpdatedByID,
		&c.UpdatedAt,
	)
}

func scanRuntimeClusterMember(row runtimeNodeScanner, m *RuntimeClusterMember) error {
	return row.Scan(
		&m.InstanceID,
		&m.ReleaseVersion,
		&m.ReleaseCommit,
		&m.SchemaVersion,
		&m.SchemaChecksum,
		&m.RuntimeContractID,
		&m.RuntimeContractDigest,
		&m.StartedAt,
		&m.HeartbeatAt,
		&m.Draining,
		&m.Ready,
	)
}

func scanRuntimeNode(row runtimeNodeScanner, n *RuntimeNode) error {
	return row.Scan(
		&n.NodeID,
		&n.DisplayName,
		&n.DeviceCertificateSerial,
		&n.DevicePublicKeyThumbprint,
		&n.NodeVersion,
		&n.ProtocolVersion,
		&n.RuntimeContractID,
		&n.RuntimeContractDigest,
		&n.Features,
		&n.Capacity,
		&n.Inflight,
		&n.Status,
		&n.LastSeenAt,
		&n.DrainingAt,
		&n.RevokedAt,
		&n.RevokeReason,
		&n.CreatedAt,
		&n.UpdatedAt,
	)
}

func scanRuntimeSession(row runtimeNodeScanner, s *RuntimeSession) error {
	return row.Scan(
		&s.RuntimeSessionID,
		&s.NodeID,
		&s.AgentID,
		&s.CredentialID,
		&s.WorkerID,
		&s.SessionEpoch,
		&s.DeviceCertificateSerial,
		&s.NodeVersion,
		&s.ProtocolVersion,
		&s.RuntimeContractID,
		&s.RuntimeContractDigest,
		&s.Features,
		&s.Capacity,
		&s.Inflight,
		&s.Status,
		&s.AttachedCoreInstanceID,
		&s.ConnectedAt,
		&s.HeartbeatAt,
		&s.DisconnectedAt,
		&s.CreatedAt,
		&s.UpdatedAt,
	)
}

func scanRuntimeSessionAttachment(row runtimeNodeScanner, a *RuntimeSessionAttachment) error {
	return row.Scan(
		&a.ID,
		&a.RuntimeSessionID,
		&a.CoreInstanceID,
		&a.AttachmentKind,
		&a.AttachedAt,
		&a.DetachedAt,
		&a.DisconnectReason,
	)
}

const hasActiveRuntimeV2SessionForAgent = `-- name: HasActiveRuntimeV2SessionForAgent :one
SELECT EXISTS (
    SELECT 1
    FROM runtime_sessions s
    JOIN runtime_nodes n ON n.node_id = s.node_id
    JOIN agent_tokens t ON t.id = s.credential_id AND t.agent_id = s.agent_id
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
      AND s.runtime_contract_digest = 'fb92bb6ddbc65bd3353b5d7c63ad148dd510e4d0ac0a6ca6110461d91e2dec53'
      AND s.features @> ARRAY[
          'lease_fence', 'assignment_confirm', 'renew', 'resume',
          'event_ack', 'result_ack', 'cancel', 'persistent_spool'
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
)`

func (q *Queries) HasActiveRuntimeV2SessionForAgent(ctx context.Context, agentID uuid.UUID) (bool, error) {
	var active bool
	err := q.db.QueryRow(ctx, hasActiveRuntimeV2SessionForAgent, agentID).Scan(&active)
	return active, err
}

const resolveRuntimeWorkerSessionPrincipal = `-- name: ResolveRuntimeWorkerSessionPrincipal :one
SELECT s.runtime_session_id, s.node_id, s.agent_id, s.credential_id,
       s.worker_id, s.session_epoch, s.attached_core_instance_id,
       s.device_certificate_serial, n.device_public_key_thumbprint,
       s.node_version, s.protocol_version, s.runtime_contract_id,
       s.runtime_contract_digest, s.features, s.status, s.heartbeat_at,
       clock_timestamp() AS database_now
FROM runtime_sessions s
JOIN runtime_nodes n ON n.node_id = s.node_id
JOIN agent_tokens t
  ON t.id = s.credential_id
 AND t.agent_id = s.agent_id
WHERE s.node_id = $1
  AND s.agent_id = $2
  AND s.credential_id = $3
  AND s.worker_id = $4
  AND s.device_certificate_serial = $5
  AND n.device_public_key_thumbprint = $6
  AND s.attached_core_instance_id = $7
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
  AND (t.expires_at IS NULL OR t.expires_at > clock_timestamp())`

type ResolveRuntimeWorkerSessionPrincipalParams struct {
	NodeID                    uuid.UUID `db:"node_id" json:"node_id"`
	AgentID                   uuid.UUID `db:"agent_id" json:"agent_id"`
	CredentialID              uuid.UUID `db:"credential_id" json:"credential_id"`
	WorkerID                  string    `db:"worker_id" json:"worker_id"`
	DeviceCertificateSerial   string    `db:"device_certificate_serial" json:"device_certificate_serial"`
	DevicePublicKeyThumbprint string    `db:"device_public_key_thumbprint" json:"device_public_key_thumbprint"`
	CoreInstanceID            uuid.UUID `db:"core_instance_id" json:"core_instance_id"`
}

type ResolveRuntimeWorkerSessionPrincipalRow struct {
	RuntimeSessionID          uuid.UUID `db:"runtime_session_id" json:"runtime_session_id"`
	NodeID                    uuid.UUID `db:"node_id" json:"node_id"`
	AgentID                   uuid.UUID `db:"agent_id" json:"agent_id"`
	CredentialID              uuid.UUID `db:"credential_id" json:"credential_id"`
	WorkerID                  string    `db:"worker_id" json:"worker_id"`
	SessionEpoch              int64     `db:"session_epoch" json:"session_epoch"`
	AttachedCoreInstanceID    uuid.UUID `db:"attached_core_instance_id" json:"attached_core_instance_id"`
	DeviceCertificateSerial   string    `db:"device_certificate_serial" json:"device_certificate_serial"`
	DevicePublicKeyThumbprint string    `db:"device_public_key_thumbprint" json:"device_public_key_thumbprint"`
	NodeVersion               string    `db:"node_version" json:"node_version"`
	ProtocolVersion           int32     `db:"protocol_version" json:"protocol_version"`
	RuntimeContractID         string    `db:"runtime_contract_id" json:"runtime_contract_id"`
	RuntimeContractDigest     string    `db:"runtime_contract_digest" json:"runtime_contract_digest"`
	Features                  []string  `db:"features" json:"features"`
	Status                    string    `db:"status" json:"status"`
	HeartbeatAt               time.Time `db:"heartbeat_at" json:"heartbeat_at"`
	DatabaseNow               time.Time `db:"database_now" json:"database_now"`
}

func (q *Queries) ResolveRuntimeWorkerSessionPrincipal(ctx context.Context, arg ResolveRuntimeWorkerSessionPrincipalParams) (ResolveRuntimeWorkerSessionPrincipalRow, error) {
	row := q.db.QueryRow(ctx, resolveRuntimeWorkerSessionPrincipal,
		arg.NodeID,
		arg.AgentID,
		arg.CredentialID,
		arg.WorkerID,
		arg.DeviceCertificateSerial,
		arg.DevicePublicKeyThumbprint,
		arg.CoreInstanceID,
	)
	var principal ResolveRuntimeWorkerSessionPrincipalRow
	err := row.Scan(
		&principal.RuntimeSessionID,
		&principal.NodeID,
		&principal.AgentID,
		&principal.CredentialID,
		&principal.WorkerID,
		&principal.SessionEpoch,
		&principal.AttachedCoreInstanceID,
		&principal.DeviceCertificateSerial,
		&principal.DevicePublicKeyThumbprint,
		&principal.NodeVersion,
		&principal.ProtocolVersion,
		&principal.RuntimeContractID,
		&principal.RuntimeContractDigest,
		&principal.Features,
		&principal.Status,
		&principal.HeartbeatAt,
		&principal.DatabaseNow,
	)
	return principal, err
}

const createRuntimeSchemaContract = `-- name: CreateRuntimeSchemaContract :one
INSERT INTO runtime_schema_contracts (
    schema_version,
    migration_name,
    runtime_contract_id,
    runtime_contract_digest,
    is_current
) VALUES ($1, $2, $3, $4, $5)
RETURNING schema_version, migration_name, runtime_contract_id,
          runtime_contract_digest, applied_at, is_current`

type CreateRuntimeSchemaContractParams struct {
	SchemaVersion         int32  `db:"schema_version" json:"schema_version"`
	MigrationName         string `db:"migration_name" json:"migration_name"`
	RuntimeContractID     string `db:"runtime_contract_id" json:"runtime_contract_id"`
	RuntimeContractDigest string `db:"runtime_contract_digest" json:"runtime_contract_digest"`
	IsCurrent             bool   `db:"is_current" json:"is_current"`
}

func (q *Queries) CreateRuntimeSchemaContract(ctx context.Context, arg CreateRuntimeSchemaContractParams) (RuntimeSchemaContract, error) {
	row := q.db.QueryRow(ctx, createRuntimeSchemaContract,
		arg.SchemaVersion,
		arg.MigrationName,
		arg.RuntimeContractID,
		arg.RuntimeContractDigest,
		arg.IsCurrent,
	)
	var contract RuntimeSchemaContract
	err := scanRuntimeSchemaContract(row, &contract)
	return contract, err
}

const getCurrentRuntimeSchemaContract = `-- name: GetCurrentRuntimeSchemaContract :one
SELECT schema_version, migration_name, runtime_contract_id,
       runtime_contract_digest, applied_at, is_current
FROM runtime_schema_contracts
WHERE is_current`

func (q *Queries) GetCurrentRuntimeSchemaContract(ctx context.Context) (RuntimeSchemaContract, error) {
	row := q.db.QueryRow(ctx, getCurrentRuntimeSchemaContract)
	var contract RuntimeSchemaContract
	err := scanRuntimeSchemaContract(row, &contract)
	return contract, err
}

const getRuntimeSchemaContract = `-- name: GetRuntimeSchemaContract :one
SELECT schema_version, migration_name, runtime_contract_id,
       runtime_contract_digest, applied_at, is_current
FROM runtime_schema_contracts
WHERE schema_version = $1`

func (q *Queries) GetRuntimeSchemaContract(ctx context.Context, schemaVersion int32) (RuntimeSchemaContract, error) {
	row := q.db.QueryRow(ctx, getRuntimeSchemaContract, schemaVersion)
	var contract RuntimeSchemaContract
	err := scanRuntimeSchemaContract(row, &contract)
	return contract, err
}

const listRuntimeSchemaContracts = `-- name: ListRuntimeSchemaContracts :many
SELECT schema_version, migration_name, runtime_contract_id,
       runtime_contract_digest, applied_at, is_current
FROM runtime_schema_contracts
ORDER BY schema_version DESC
LIMIT $1 OFFSET $2`

type ListRuntimeSchemaContractsParams struct {
	Limit  int32 `db:"limit" json:"limit"`
	Offset int32 `db:"offset" json:"offset"`
}

func (q *Queries) ListRuntimeSchemaContracts(ctx context.Context, arg ListRuntimeSchemaContractsParams) ([]RuntimeSchemaContract, error) {
	rows, err := q.db.Query(ctx, listRuntimeSchemaContracts, arg.Limit, arg.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []RuntimeSchemaContract
	for rows.Next() {
		var contract RuntimeSchemaContract
		if err := scanRuntimeSchemaContract(rows, &contract); err != nil {
			return nil, err
		}
		items = append(items, contract)
	}
	return items, rows.Err()
}

const claimCurrentRuntimeSchemaContract = `-- name: ClaimCurrentRuntimeSchemaContract :one
SELECT schema_version, migration_name, runtime_contract_id,
       runtime_contract_digest, applied_at, is_current
FROM runtime_schema_contracts
WHERE schema_version = $1
  AND runtime_contract_id = $2
  AND runtime_contract_digest = $3
  AND is_current
FOR SHARE`

type ClaimCurrentRuntimeSchemaContractParams struct {
	SchemaVersion         int32  `db:"schema_version" json:"schema_version"`
	RuntimeContractID     string `db:"runtime_contract_id" json:"runtime_contract_id"`
	RuntimeContractDigest string `db:"runtime_contract_digest" json:"runtime_contract_digest"`
}

func (q *Queries) ClaimCurrentRuntimeSchemaContract(ctx context.Context, arg ClaimCurrentRuntimeSchemaContractParams) (RuntimeSchemaContract, error) {
	row := q.db.QueryRow(ctx, claimCurrentRuntimeSchemaContract,
		arg.SchemaVersion,
		arg.RuntimeContractID,
		arg.RuntimeContractDigest,
	)
	var contract RuntimeSchemaContract
	err := scanRuntimeSchemaContract(row, &contract)
	return contract, err
}

const getRuntimeClusterControl = `-- name: GetRuntimeClusterControl :one
SELECT singleton_id, mode, expected_replicas, cutover_id,
       drain_started_at, drain_deadline_at, hard_maintenance_at, reopened_at,
       version, updated_by_type, updated_by_id, updated_at
FROM runtime_cluster_control
WHERE singleton_id = 1`

func (q *Queries) GetRuntimeClusterControl(ctx context.Context) (RuntimeClusterControl, error) {
	row := q.db.QueryRow(ctx, getRuntimeClusterControl)
	var control RuntimeClusterControl
	err := scanRuntimeClusterControl(row, &control)
	return control, err
}

const upsertRuntimeClusterControl = `-- name: UpsertRuntimeClusterControl :one
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
          version, updated_by_type, updated_by_id, updated_at`

type UpsertRuntimeClusterControlParams struct {
	Mode              string     `db:"mode" json:"mode"`
	ExpectedReplicas  int32      `db:"expected_replicas" json:"expected_replicas"`
	CutoverID         uuid.UUID  `db:"cutover_id" json:"cutover_id"`
	DrainStartedAt    *time.Time `db:"drain_started_at" json:"drain_started_at"`
	DrainDeadlineAt   *time.Time `db:"drain_deadline_at" json:"drain_deadline_at"`
	HardMaintenanceAt time.Time  `db:"hard_maintenance_at" json:"hard_maintenance_at"`
	ReopenedAt        *time.Time `db:"reopened_at" json:"reopened_at"`
	UpdatedByType     string     `db:"updated_by_type" json:"updated_by_type"`
	UpdatedByID       *uuid.UUID `db:"updated_by_id" json:"updated_by_id"`
}

func (q *Queries) UpsertRuntimeClusterControl(ctx context.Context, arg UpsertRuntimeClusterControlParams) (RuntimeClusterControl, error) {
	row := q.db.QueryRow(ctx, upsertRuntimeClusterControl,
		arg.Mode,
		arg.ExpectedReplicas,
		arg.CutoverID,
		arg.DrainStartedAt,
		arg.DrainDeadlineAt,
		arg.HardMaintenanceAt,
		arg.ReopenedAt,
		arg.UpdatedByType,
		arg.UpdatedByID,
	)
	var control RuntimeClusterControl
	err := scanRuntimeClusterControl(row, &control)
	return control, err
}

const claimRuntimeClusterControl = `-- name: ClaimRuntimeClusterControl :one
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
          version, updated_by_type, updated_by_id, updated_at`

type ClaimRuntimeClusterControlParams struct {
	ExpectedVersion   int64      `db:"expected_version" json:"expected_version"`
	Mode              string     `db:"mode" json:"mode"`
	ExpectedReplicas  int32      `db:"expected_replicas" json:"expected_replicas"`
	CutoverID         uuid.UUID  `db:"cutover_id" json:"cutover_id"`
	DrainStartedAt    *time.Time `db:"drain_started_at" json:"drain_started_at"`
	DrainDeadlineAt   *time.Time `db:"drain_deadline_at" json:"drain_deadline_at"`
	HardMaintenanceAt time.Time  `db:"hard_maintenance_at" json:"hard_maintenance_at"`
	ReopenedAt        *time.Time `db:"reopened_at" json:"reopened_at"`
	UpdatedByType     string     `db:"updated_by_type" json:"updated_by_type"`
	UpdatedByID       *uuid.UUID `db:"updated_by_id" json:"updated_by_id"`
}

func (q *Queries) ClaimRuntimeClusterControl(ctx context.Context, arg ClaimRuntimeClusterControlParams) (RuntimeClusterControl, error) {
	row := q.db.QueryRow(ctx, claimRuntimeClusterControl,
		arg.ExpectedVersion,
		arg.Mode,
		arg.ExpectedReplicas,
		arg.CutoverID,
		arg.DrainStartedAt,
		arg.DrainDeadlineAt,
		arg.HardMaintenanceAt,
		arg.ReopenedAt,
		arg.UpdatedByType,
		arg.UpdatedByID,
	)
	var control RuntimeClusterControl
	err := scanRuntimeClusterControl(row, &control)
	return control, err
}

const upsertRuntimeClusterMember = `-- name: UpsertRuntimeClusterMember :one
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
          started_at, heartbeat_at, draining, ready`

type UpsertRuntimeClusterMemberParams struct {
	InstanceID            uuid.UUID `db:"instance_id" json:"instance_id"`
	ReleaseVersion        string    `db:"release_version" json:"release_version"`
	ReleaseCommit         string    `db:"release_commit" json:"release_commit"`
	SchemaVersion         int32     `db:"schema_version" json:"schema_version"`
	SchemaChecksum        string    `db:"schema_checksum" json:"schema_checksum"`
	RuntimeContractID     string    `db:"runtime_contract_id" json:"runtime_contract_id"`
	RuntimeContractDigest string    `db:"runtime_contract_digest" json:"runtime_contract_digest"`
	Draining              bool      `db:"draining" json:"draining"`
	Ready                 bool      `db:"ready" json:"ready"`
}

func (q *Queries) UpsertRuntimeClusterMember(ctx context.Context, arg UpsertRuntimeClusterMemberParams) (RuntimeClusterMember, error) {
	row := q.db.QueryRow(ctx, upsertRuntimeClusterMember,
		arg.InstanceID,
		arg.ReleaseVersion,
		arg.ReleaseCommit,
		arg.SchemaVersion,
		arg.SchemaChecksum,
		arg.RuntimeContractID,
		arg.RuntimeContractDigest,
		arg.Draining,
		arg.Ready,
	)
	var member RuntimeClusterMember
	err := scanRuntimeClusterMember(row, &member)
	return member, err
}

const getRuntimeClusterMember = `-- name: GetRuntimeClusterMember :one
SELECT instance_id, release_version, release_commit, schema_version,
       schema_checksum, runtime_contract_id, runtime_contract_digest,
       started_at, heartbeat_at, draining, ready
FROM runtime_cluster_members
WHERE instance_id = $1`

func (q *Queries) GetRuntimeClusterMember(ctx context.Context, instanceID uuid.UUID) (RuntimeClusterMember, error) {
	row := q.db.QueryRow(ctx, getRuntimeClusterMember, instanceID)
	var member RuntimeClusterMember
	err := scanRuntimeClusterMember(row, &member)
	return member, err
}

const listRuntimeClusterMembers = `-- name: ListRuntimeClusterMembers :many
SELECT instance_id, release_version, release_commit, schema_version,
       schema_checksum, runtime_contract_id, runtime_contract_digest,
       started_at, heartbeat_at, draining, ready
FROM runtime_cluster_members
ORDER BY started_at ASC, instance_id ASC`

func (q *Queries) ListRuntimeClusterMembers(ctx context.Context) ([]RuntimeClusterMember, error) {
	rows, err := q.db.Query(ctx, listRuntimeClusterMembers)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []RuntimeClusterMember
	for rows.Next() {
		var member RuntimeClusterMember
		if err := scanRuntimeClusterMember(rows, &member); err != nil {
			return nil, err
		}
		items = append(items, member)
	}
	return items, rows.Err()
}

const listLiveRuntimeClusterMembers = `-- name: ListLiveRuntimeClusterMembers :many
SELECT instance_id, release_version, release_commit, schema_version,
       schema_checksum, runtime_contract_id, runtime_contract_digest,
       started_at, heartbeat_at, draining, ready
FROM runtime_cluster_members
WHERE heartbeat_at >= $1
ORDER BY started_at ASC, instance_id ASC`

func (q *Queries) ListLiveRuntimeClusterMembers(ctx context.Context, heartbeatAfter time.Time) ([]RuntimeClusterMember, error) {
	rows, err := q.db.Query(ctx, listLiveRuntimeClusterMembers, heartbeatAfter)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []RuntimeClusterMember
	for rows.Next() {
		var member RuntimeClusterMember
		if err := scanRuntimeClusterMember(rows, &member); err != nil {
			return nil, err
		}
		items = append(items, member)
	}
	return items, rows.Err()
}

const heartbeatRuntimeClusterMember = `-- name: HeartbeatRuntimeClusterMember :one
UPDATE runtime_cluster_members
SET heartbeat_at = clock_timestamp(),
    draining = $2,
    ready = $3
WHERE instance_id = $1
RETURNING instance_id, release_version, release_commit, schema_version,
          schema_checksum, runtime_contract_id, runtime_contract_digest,
          started_at, heartbeat_at, draining, ready`

type HeartbeatRuntimeClusterMemberParams struct {
	InstanceID uuid.UUID `db:"instance_id" json:"instance_id"`
	Draining   bool      `db:"draining" json:"draining"`
	Ready      bool      `db:"ready" json:"ready"`
}

func (q *Queries) HeartbeatRuntimeClusterMember(ctx context.Context, arg HeartbeatRuntimeClusterMemberParams) (RuntimeClusterMember, error) {
	row := q.db.QueryRow(ctx, heartbeatRuntimeClusterMember, arg.InstanceID, arg.Draining, arg.Ready)
	var member RuntimeClusterMember
	err := scanRuntimeClusterMember(row, &member)
	return member, err
}

const closeRuntimeClusterMember = `-- name: CloseRuntimeClusterMember :execrows
DELETE FROM runtime_cluster_members
WHERE instance_id = $1`

func (q *Queries) CloseRuntimeClusterMember(ctx context.Context, instanceID uuid.UUID) (int64, error) {
	tag, err := q.db.Exec(ctx, closeRuntimeClusterMember, instanceID)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

const deleteStaleRuntimeClusterMembers = `-- name: DeleteStaleRuntimeClusterMembers :execrows
DELETE FROM runtime_cluster_members
WHERE heartbeat_at < $1
  AND instance_id <> $2`

type DeleteStaleRuntimeClusterMembersParams struct {
	StaleBefore      time.Time `db:"stale_before" json:"stale_before"`
	ExceptInstanceID uuid.UUID `db:"except_instance_id" json:"except_instance_id"`
}

func (q *Queries) DeleteStaleRuntimeClusterMembers(ctx context.Context, arg DeleteStaleRuntimeClusterMembersParams) (int64, error) {
	tag, err := q.db.Exec(ctx, deleteStaleRuntimeClusterMembers, arg.StaleBefore, arg.ExceptInstanceID)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

const upsertRuntimeNode = `-- name: UpsertRuntimeNode :one
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
          created_at, updated_at`

type UpsertRuntimeNodeParams struct {
	NodeID                    uuid.UUID `db:"node_id" json:"node_id"`
	DisplayName               string    `db:"display_name" json:"display_name"`
	DeviceCertificateSerial   string    `db:"device_certificate_serial" json:"device_certificate_serial"`
	DevicePublicKeyThumbprint string    `db:"device_public_key_thumbprint" json:"device_public_key_thumbprint"`
	NodeVersion               string    `db:"node_version" json:"node_version"`
	ProtocolVersion           int32     `db:"protocol_version" json:"protocol_version"`
	RuntimeContractID         string    `db:"runtime_contract_id" json:"runtime_contract_id"`
	RuntimeContractDigest     string    `db:"runtime_contract_digest" json:"runtime_contract_digest"`
	Features                  []string  `db:"features" json:"features"`
	Capacity                  int32     `db:"capacity" json:"capacity"`
}

func (q *Queries) UpsertRuntimeNode(ctx context.Context, arg UpsertRuntimeNodeParams) (RuntimeNode, error) {
	row := q.db.QueryRow(ctx, upsertRuntimeNode,
		arg.NodeID,
		arg.DisplayName,
		arg.DeviceCertificateSerial,
		arg.DevicePublicKeyThumbprint,
		arg.NodeVersion,
		arg.ProtocolVersion,
		arg.RuntimeContractID,
		arg.RuntimeContractDigest,
		arg.Features,
		arg.Capacity,
	)
	var node RuntimeNode
	err := scanRuntimeNode(row, &node)
	return node, err
}

const getRuntimeNode = `-- name: GetRuntimeNode :one
SELECT node_id, display_name, device_certificate_serial,
       device_public_key_thumbprint, node_version, protocol_version,
       runtime_contract_id, runtime_contract_digest, features, capacity,
       inflight, status, last_seen_at, draining_at, revoked_at, revoke_reason,
       created_at, updated_at
FROM runtime_nodes
WHERE node_id = $1`

func (q *Queries) GetRuntimeNode(ctx context.Context, nodeID uuid.UUID) (RuntimeNode, error) {
	row := q.db.QueryRow(ctx, getRuntimeNode, nodeID)
	var node RuntimeNode
	err := scanRuntimeNode(row, &node)
	return node, err
}

const getRuntimeNodeByCertificate = `-- name: GetRuntimeNodeByCertificate :one
SELECT node_id, display_name, device_certificate_serial,
       device_public_key_thumbprint, node_version, protocol_version,
       runtime_contract_id, runtime_contract_digest, features, capacity,
       inflight, status, last_seen_at, draining_at, revoked_at, revoke_reason,
       created_at, updated_at
FROM runtime_nodes
WHERE device_certificate_serial = $1
  AND device_public_key_thumbprint = $2`

type GetRuntimeNodeByCertificateParams struct {
	DeviceCertificateSerial   string `db:"device_certificate_serial" json:"device_certificate_serial"`
	DevicePublicKeyThumbprint string `db:"device_public_key_thumbprint" json:"device_public_key_thumbprint"`
}

func (q *Queries) GetRuntimeNodeByCertificate(ctx context.Context, arg GetRuntimeNodeByCertificateParams) (RuntimeNode, error) {
	row := q.db.QueryRow(ctx, getRuntimeNodeByCertificate, arg.DeviceCertificateSerial, arg.DevicePublicKeyThumbprint)
	var node RuntimeNode
	err := scanRuntimeNode(row, &node)
	return node, err
}

const listRuntimeNodes = `-- name: ListRuntimeNodes :many
SELECT node_id, display_name, device_certificate_serial,
       device_public_key_thumbprint, node_version, protocol_version,
       runtime_contract_id, runtime_contract_digest, features, capacity,
       inflight, status, last_seen_at, draining_at, revoked_at, revoke_reason,
       created_at, updated_at
FROM runtime_nodes
ORDER BY created_at DESC, node_id DESC
LIMIT $1 OFFSET $2`

type ListRuntimeNodesParams struct {
	Limit  int32 `db:"limit" json:"limit"`
	Offset int32 `db:"offset" json:"offset"`
}

func (q *Queries) ListRuntimeNodes(ctx context.Context, arg ListRuntimeNodesParams) ([]RuntimeNode, error) {
	rows, err := q.db.Query(ctx, listRuntimeNodes, arg.Limit, arg.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []RuntimeNode
	for rows.Next() {
		var node RuntimeNode
		if err := scanRuntimeNode(rows, &node); err != nil {
			return nil, err
		}
		items = append(items, node)
	}
	return items, rows.Err()
}

const heartbeatRuntimeNode = `-- name: HeartbeatRuntimeNode :one
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
          created_at, updated_at`

type HeartbeatRuntimeNodeParams struct {
	NodeID                    uuid.UUID `db:"node_id" json:"node_id"`
	NodeVersion               string    `db:"node_version" json:"node_version"`
	ProtocolVersion           int32     `db:"protocol_version" json:"protocol_version"`
	RuntimeContractID         string    `db:"runtime_contract_id" json:"runtime_contract_id"`
	RuntimeContractDigest     string    `db:"runtime_contract_digest" json:"runtime_contract_digest"`
	Features                  []string  `db:"features" json:"features"`
	Capacity                  int32     `db:"capacity" json:"capacity"`
	DeviceCertificateSerial   string    `db:"device_certificate_serial" json:"device_certificate_serial"`
	DevicePublicKeyThumbprint string    `db:"device_public_key_thumbprint" json:"device_public_key_thumbprint"`
}

func (q *Queries) HeartbeatRuntimeNode(ctx context.Context, arg HeartbeatRuntimeNodeParams) (RuntimeNode, error) {
	row := q.db.QueryRow(ctx, heartbeatRuntimeNode,
		arg.NodeID,
		arg.NodeVersion,
		arg.ProtocolVersion,
		arg.RuntimeContractID,
		arg.RuntimeContractDigest,
		arg.Features,
		arg.Capacity,
		arg.DeviceCertificateSerial,
		arg.DevicePublicKeyThumbprint,
	)
	var node RuntimeNode
	err := scanRuntimeNode(row, &node)
	return node, err
}

const claimRuntimeNodeSlot = `-- name: ClaimRuntimeNodeSlot :one
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
          n.revoke_reason, n.created_at, n.updated_at`

type ClaimRuntimeNodeSlotParams struct {
	NodeID        uuid.UUID `db:"node_id" json:"node_id"`
	LastSeenAfter time.Time `db:"last_seen_after" json:"last_seen_after"`
}

func (q *Queries) ClaimRuntimeNodeSlot(ctx context.Context, arg ClaimRuntimeNodeSlotParams) (RuntimeNode, error) {
	row := q.db.QueryRow(ctx, claimRuntimeNodeSlot, arg.NodeID, arg.LastSeenAfter)
	var node RuntimeNode
	err := scanRuntimeNode(row, &node)
	return node, err
}

const releaseRuntimeNodeSlot = `-- name: ReleaseRuntimeNodeSlot :one
UPDATE runtime_nodes
SET inflight = inflight - 1,
    updated_at = clock_timestamp()
WHERE node_id = $1
  AND inflight > 0
RETURNING node_id, display_name, device_certificate_serial,
          device_public_key_thumbprint, node_version, protocol_version,
          runtime_contract_id, runtime_contract_digest, features, capacity,
          inflight, status, last_seen_at, draining_at, revoked_at, revoke_reason,
          created_at, updated_at`

func (q *Queries) ReleaseRuntimeNodeSlot(ctx context.Context, nodeID uuid.UUID) (RuntimeNode, error) {
	row := q.db.QueryRow(ctx, releaseRuntimeNodeSlot, nodeID)
	var node RuntimeNode
	err := scanRuntimeNode(row, &node)
	return node, err
}

const markRuntimeNodeDraining = `-- name: MarkRuntimeNodeDraining :one
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
          created_at, updated_at`

func (q *Queries) MarkRuntimeNodeDraining(ctx context.Context, nodeID uuid.UUID) (RuntimeNode, error) {
	row := q.db.QueryRow(ctx, markRuntimeNodeDraining, nodeID)
	var node RuntimeNode
	err := scanRuntimeNode(row, &node)
	return node, err
}

// Principal revocation and Session reactivation transactions must invoke the
// following lock primitives in this exact global order:
// Session -> Node -> Token -> Attachment. Each query sorts its UUID keys in SQL.
const lockRuntimeSessionsForPrincipalRevocation = `-- name: LockRuntimeSessionsForPrincipalRevocation :many
SELECT runtime_session_id, node_id, credential_id, status
FROM runtime_sessions
WHERE status IN ('active', 'draining', 'offline')
  AND (
      node_id = ANY($1::uuid[])
      OR credential_id = ANY($2::uuid[])
  )
ORDER BY runtime_session_id ASC
FOR UPDATE`

type LockRuntimeSessionsForPrincipalRevocationParams struct {
	NodeIDs  []uuid.UUID `db:"node_ids" json:"node_ids"`
	TokenIDs []uuid.UUID `db:"token_ids" json:"token_ids"`
}

type LockRuntimeSessionsForPrincipalRevocationRow struct {
	RuntimeSessionID uuid.UUID `db:"runtime_session_id" json:"runtime_session_id"`
	NodeID           uuid.UUID `db:"node_id" json:"node_id"`
	CredentialID     uuid.UUID `db:"credential_id" json:"credential_id"`
	Status           string    `db:"status" json:"status"`
}

func (q *Queries) LockRuntimeSessionsForPrincipalRevocation(ctx context.Context, arg LockRuntimeSessionsForPrincipalRevocationParams) ([]LockRuntimeSessionsForPrincipalRevocationRow, error) {
	rows, err := q.db.Query(ctx, lockRuntimeSessionsForPrincipalRevocation, arg.NodeIDs, arg.TokenIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []LockRuntimeSessionsForPrincipalRevocationRow
	for rows.Next() {
		var item LockRuntimeSessionsForPrincipalRevocationRow
		if err := rows.Scan(&item.RuntimeSessionID, &item.NodeID, &item.CredentialID, &item.Status); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

const lockRuntimeNodesForPrincipalRevocation = `-- name: LockRuntimeNodesForPrincipalRevocation :many
SELECT node_id
FROM runtime_nodes
WHERE node_id = ANY($1::uuid[])
ORDER BY node_id ASC
FOR UPDATE`

func (q *Queries) LockRuntimeNodesForPrincipalRevocation(ctx context.Context, nodeIDs []uuid.UUID) ([]uuid.UUID, error) {
	rows, err := q.db.Query(ctx, lockRuntimeNodesForPrincipalRevocation, nodeIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []uuid.UUID
	for rows.Next() {
		var item uuid.UUID
		if err := rows.Scan(&item); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

const lockAgentTokensForPrincipalRevocation = `-- name: LockAgentTokensForPrincipalRevocation :many
SELECT id
FROM agent_tokens
WHERE id = ANY($1::uuid[])
ORDER BY id ASC
FOR UPDATE`

func (q *Queries) LockAgentTokensForPrincipalRevocation(ctx context.Context, tokenIDs []uuid.UUID) ([]uuid.UUID, error) {
	rows, err := q.db.Query(ctx, lockAgentTokensForPrincipalRevocation, tokenIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []uuid.UUID
	for rows.Next() {
		var item uuid.UUID
		if err := rows.Scan(&item); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

const lockActiveRuntimeSessionAttachmentsForPrincipalRevocation = `-- name: LockActiveRuntimeSessionAttachmentsForPrincipalRevocation :many
SELECT id
FROM runtime_session_attachments
WHERE runtime_session_id = ANY($1::uuid[])
  AND detached_at IS NULL
ORDER BY id ASC
FOR UPDATE`

func (q *Queries) LockActiveRuntimeSessionAttachmentsForPrincipalRevocation(ctx context.Context, runtimeSessionIDs []uuid.UUID) ([]uuid.UUID, error) {
	rows, err := q.db.Query(ctx, lockActiveRuntimeSessionAttachmentsForPrincipalRevocation, runtimeSessionIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []uuid.UUID
	for rows.Next() {
		var item uuid.UUID
		if err := rows.Scan(&item); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

const revokeRuntimeNode = `-- name: RevokeRuntimeNode :one
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
          created_at, updated_at`

type RevokeRuntimeNodeParams struct {
	NodeID       uuid.UUID `db:"node_id" json:"node_id"`
	RevokeReason *string   `db:"revoke_reason" json:"revoke_reason"`
}

func (q *Queries) RevokeRuntimeNode(ctx context.Context, arg RevokeRuntimeNodeParams) (RuntimeNode, error) {
	row := q.db.QueryRow(ctx, revokeRuntimeNode, arg.NodeID, arg.RevokeReason)
	var node RuntimeNode
	err := scanRuntimeNode(row, &node)
	return node, err
}

const createRuntimeSession = `-- name: CreateRuntimeSession :one
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
          connected_at, heartbeat_at, disconnected_at, created_at, updated_at`

type CreateRuntimeSessionParams struct {
	RuntimeSessionID        uuid.UUID `db:"runtime_session_id" json:"runtime_session_id"`
	NodeID                  uuid.UUID `db:"node_id" json:"node_id"`
	AgentID                 uuid.UUID `db:"agent_id" json:"agent_id"`
	CredentialID            uuid.UUID `db:"credential_id" json:"credential_id"`
	WorkerID                string    `db:"worker_id" json:"worker_id"`
	SessionEpoch            int64     `db:"session_epoch" json:"session_epoch"`
	DeviceCertificateSerial string    `db:"device_certificate_serial" json:"device_certificate_serial"`
	NodeVersion             string    `db:"node_version" json:"node_version"`
	ProtocolVersion         int32     `db:"protocol_version" json:"protocol_version"`
	RuntimeContractID       string    `db:"runtime_contract_id" json:"runtime_contract_id"`
	RuntimeContractDigest   string    `db:"runtime_contract_digest" json:"runtime_contract_digest"`
	Features                []string  `db:"features" json:"features"`
	Capacity                int32     `db:"capacity" json:"capacity"`
	AttachedCoreInstanceID  uuid.UUID `db:"attached_core_instance_id" json:"attached_core_instance_id"`
}

func (q *Queries) CreateRuntimeSession(ctx context.Context, arg CreateRuntimeSessionParams) (RuntimeSession, error) {
	row := q.db.QueryRow(ctx, createRuntimeSession,
		arg.RuntimeSessionID,
		arg.NodeID,
		arg.AgentID,
		arg.CredentialID,
		arg.WorkerID,
		arg.SessionEpoch,
		arg.DeviceCertificateSerial,
		arg.NodeVersion,
		arg.ProtocolVersion,
		arg.RuntimeContractID,
		arg.RuntimeContractDigest,
		arg.Features,
		arg.Capacity,
		arg.AttachedCoreInstanceID,
	)
	var session RuntimeSession
	err := scanRuntimeSession(row, &session)
	return session, err
}

const getRuntimeSession = `-- name: GetRuntimeSession :one
SELECT runtime_session_id, node_id, agent_id, credential_id, worker_id,
       session_epoch, device_certificate_serial, node_version,
       protocol_version, runtime_contract_id, runtime_contract_digest,
       features, capacity, inflight, status, attached_core_instance_id,
       connected_at, heartbeat_at, disconnected_at, created_at, updated_at
FROM runtime_sessions
WHERE runtime_session_id = $1`

func (q *Queries) GetRuntimeSession(ctx context.Context, runtimeSessionID uuid.UUID) (RuntimeSession, error) {
	row := q.db.QueryRow(ctx, getRuntimeSession, runtimeSessionID)
	var session RuntimeSession
	err := scanRuntimeSession(row, &session)
	return session, err
}

const getRuntimeSessionForUpdate = `-- name: GetRuntimeSessionForUpdate :one
SELECT runtime_session_id, node_id, agent_id, credential_id, worker_id,
       session_epoch, device_certificate_serial, node_version,
       protocol_version, runtime_contract_id, runtime_contract_digest,
       features, capacity, inflight, status, attached_core_instance_id,
       connected_at, heartbeat_at, disconnected_at, created_at, updated_at
FROM runtime_sessions
WHERE runtime_session_id = $1
FOR UPDATE`

func (q *Queries) GetRuntimeSessionForUpdate(ctx context.Context, runtimeSessionID uuid.UUID) (RuntimeSession, error) {
	row := q.db.QueryRow(ctx, getRuntimeSessionForUpdate, runtimeSessionID)
	var session RuntimeSession
	err := scanRuntimeSession(row, &session)
	return session, err
}

const listActiveRuntimeSessionsByAgent = `-- name: ListActiveRuntimeSessionsByAgent :many
SELECT runtime_session_id, node_id, agent_id, credential_id, worker_id,
       session_epoch, device_certificate_serial, node_version,
       protocol_version, runtime_contract_id, runtime_contract_digest,
       features, capacity, inflight, status, attached_core_instance_id,
       connected_at, heartbeat_at, disconnected_at, created_at, updated_at
FROM runtime_sessions
WHERE agent_id = $1
  AND status IN ('active', 'draining')
ORDER BY heartbeat_at DESC, runtime_session_id ASC`

func (q *Queries) ListActiveRuntimeSessionsByAgent(ctx context.Context, agentID uuid.UUID) ([]RuntimeSession, error) {
	rows, err := q.db.Query(ctx, listActiveRuntimeSessionsByAgent, agentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []RuntimeSession
	for rows.Next() {
		var session RuntimeSession
		if err := scanRuntimeSession(rows, &session); err != nil {
			return nil, err
		}
		items = append(items, session)
	}
	return items, rows.Err()
}

const listActiveRuntimeSessionsByNode = `-- name: ListActiveRuntimeSessionsByNode :many
SELECT runtime_session_id, node_id, agent_id, credential_id, worker_id,
       session_epoch, device_certificate_serial, node_version,
       protocol_version, runtime_contract_id, runtime_contract_digest,
       features, capacity, inflight, status, attached_core_instance_id,
       connected_at, heartbeat_at, disconnected_at, created_at, updated_at
FROM runtime_sessions
WHERE node_id = $1
  AND status IN ('active', 'draining')
ORDER BY heartbeat_at DESC, runtime_session_id ASC`

func (q *Queries) ListActiveRuntimeSessionsByNode(ctx context.Context, nodeID uuid.UUID) ([]RuntimeSession, error) {
	rows, err := q.db.Query(ctx, listActiveRuntimeSessionsByNode, nodeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []RuntimeSession
	for rows.Next() {
		var session RuntimeSession
		if err := scanRuntimeSession(rows, &session); err != nil {
			return nil, err
		}
		items = append(items, session)
	}
	return items, rows.Err()
}

const listStaleRuntimeSessionsForUpdate = `-- name: ListStaleRuntimeSessionsForUpdate :many
SELECT runtime_session_id, node_id, agent_id, credential_id, worker_id,
       session_epoch, device_certificate_serial, node_version,
       protocol_version, runtime_contract_id, runtime_contract_digest,
       features, capacity, inflight, status, attached_core_instance_id,
       connected_at, heartbeat_at, disconnected_at, created_at, updated_at
FROM runtime_sessions
WHERE status IN ('active', 'draining')
  AND heartbeat_at < $1
ORDER BY heartbeat_at ASC, runtime_session_id ASC
LIMIT $2
FOR UPDATE SKIP LOCKED`

type ListStaleRuntimeSessionsForUpdateParams struct {
	StaleBefore time.Time `db:"stale_before" json:"stale_before"`
	Limit       int32     `db:"limit" json:"limit"`
}

func (q *Queries) ListStaleRuntimeSessionsForUpdate(ctx context.Context, arg ListStaleRuntimeSessionsForUpdateParams) ([]RuntimeSession, error) {
	rows, err := q.db.Query(ctx, listStaleRuntimeSessionsForUpdate, arg.StaleBefore, arg.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []RuntimeSession
	for rows.Next() {
		var session RuntimeSession
		if err := scanRuntimeSession(rows, &session); err != nil {
			return nil, err
		}
		items = append(items, session)
	}
	return items, rows.Err()
}

const claimRuntimeSessionForCore = `-- name: ClaimRuntimeSessionForCore :one
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
          s.heartbeat_at, s.disconnected_at, s.created_at, s.updated_at`

type ClaimRuntimeSessionForCoreParams struct {
	RuntimeSessionID uuid.UUID `db:"runtime_session_id" json:"runtime_session_id"`
	NodeID           uuid.UUID `db:"node_id" json:"node_id"`
	AgentID          uuid.UUID `db:"agent_id" json:"agent_id"`
	CredentialID     uuid.UUID `db:"credential_id" json:"credential_id"`
	WorkerID         string    `db:"worker_id" json:"worker_id"`
	SessionEpoch     int64     `db:"session_epoch" json:"session_epoch"`
	CoreInstanceID   uuid.UUID `db:"core_instance_id" json:"core_instance_id"`
}

func (q *Queries) ClaimRuntimeSessionForCore(ctx context.Context, arg ClaimRuntimeSessionForCoreParams) (RuntimeSession, error) {
	row := q.db.QueryRow(ctx, claimRuntimeSessionForCore,
		arg.RuntimeSessionID,
		arg.NodeID,
		arg.AgentID,
		arg.CredentialID,
		arg.WorkerID,
		arg.SessionEpoch,
		arg.CoreInstanceID,
	)
	var session RuntimeSession
	err := scanRuntimeSession(row, &session)
	return session, err
}

const heartbeatRuntimeSession = `-- name: HeartbeatRuntimeSession :one
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
          connected_at, heartbeat_at, disconnected_at, created_at, updated_at`

type HeartbeatRuntimeSessionParams struct {
	RuntimeSessionID      uuid.UUID `db:"runtime_session_id" json:"runtime_session_id"`
	CoreInstanceID        uuid.UUID `db:"core_instance_id" json:"core_instance_id"`
	NodeVersion           string    `db:"node_version" json:"node_version"`
	ProtocolVersion       int32     `db:"protocol_version" json:"protocol_version"`
	RuntimeContractID     string    `db:"runtime_contract_id" json:"runtime_contract_id"`
	RuntimeContractDigest string    `db:"runtime_contract_digest" json:"runtime_contract_digest"`
	Features              []string  `db:"features" json:"features"`
	Capacity              int32     `db:"capacity" json:"capacity"`
}

func (q *Queries) HeartbeatRuntimeSession(ctx context.Context, arg HeartbeatRuntimeSessionParams) (RuntimeSession, error) {
	row := q.db.QueryRow(ctx, heartbeatRuntimeSession,
		arg.RuntimeSessionID,
		arg.CoreInstanceID,
		arg.NodeVersion,
		arg.ProtocolVersion,
		arg.RuntimeContractID,
		arg.RuntimeContractDigest,
		arg.Features,
		arg.Capacity,
	)
	var session RuntimeSession
	err := scanRuntimeSession(row, &session)
	return session, err
}

const claimRuntimeSessionSlot = `-- name: ClaimRuntimeSessionSlot :one
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
          s.heartbeat_at, s.disconnected_at, s.created_at, s.updated_at`

type ClaimRuntimeSessionSlotParams struct {
	RuntimeSessionID uuid.UUID `db:"runtime_session_id" json:"runtime_session_id"`
	AgentID          uuid.UUID `db:"agent_id" json:"agent_id"`
	CoreInstanceID   uuid.UUID `db:"core_instance_id" json:"core_instance_id"`
	HeartbeatAfter   time.Time `db:"heartbeat_after" json:"heartbeat_after"`
}

func (q *Queries) ClaimRuntimeSessionSlot(ctx context.Context, arg ClaimRuntimeSessionSlotParams) (RuntimeSession, error) {
	row := q.db.QueryRow(ctx, claimRuntimeSessionSlot,
		arg.RuntimeSessionID,
		arg.AgentID,
		arg.CoreInstanceID,
		arg.HeartbeatAfter,
	)
	var session RuntimeSession
	err := scanRuntimeSession(row, &session)
	return session, err
}

const releaseRuntimeSessionSlot = `-- name: ReleaseRuntimeSessionSlot :one
UPDATE runtime_sessions
SET inflight = inflight - 1,
    updated_at = clock_timestamp()
WHERE runtime_session_id = $1
  AND inflight > 0
RETURNING runtime_session_id, node_id, agent_id, credential_id, worker_id,
          session_epoch, device_certificate_serial, node_version,
          protocol_version, runtime_contract_id, runtime_contract_digest,
          features, capacity, inflight, status, attached_core_instance_id,
          connected_at, heartbeat_at, disconnected_at, created_at, updated_at`

func (q *Queries) ReleaseRuntimeSessionSlot(ctx context.Context, runtimeSessionID uuid.UUID) (RuntimeSession, error) {
	row := q.db.QueryRow(ctx, releaseRuntimeSessionSlot, runtimeSessionID)
	var session RuntimeSession
	err := scanRuntimeSession(row, &session)
	return session, err
}

const markRuntimeSessionDraining = `-- name: MarkRuntimeSessionDraining :one
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
          connected_at, heartbeat_at, disconnected_at, created_at, updated_at`

type MarkRuntimeSessionDrainingParams struct {
	RuntimeSessionID uuid.UUID `db:"runtime_session_id" json:"runtime_session_id"`
	CoreInstanceID   uuid.UUID `db:"core_instance_id" json:"core_instance_id"`
}

func (q *Queries) MarkRuntimeSessionDraining(ctx context.Context, arg MarkRuntimeSessionDrainingParams) (RuntimeSession, error) {
	row := q.db.QueryRow(ctx, markRuntimeSessionDraining, arg.RuntimeSessionID, arg.CoreInstanceID)
	var session RuntimeSession
	err := scanRuntimeSession(row, &session)
	return session, err
}

const closeRuntimeSession = `-- name: CloseRuntimeSession :one
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
          connected_at, heartbeat_at, disconnected_at, created_at, updated_at`

type CloseRuntimeSessionParams struct {
	RuntimeSessionID uuid.UUID `db:"runtime_session_id" json:"runtime_session_id"`
	CoreInstanceID   uuid.UUID `db:"core_instance_id" json:"core_instance_id"`
	Status           string    `db:"status" json:"status"`
}

func (q *Queries) CloseRuntimeSession(ctx context.Context, arg CloseRuntimeSessionParams) (RuntimeSession, error) {
	row := q.db.QueryRow(ctx, closeRuntimeSession, arg.RuntimeSessionID, arg.CoreInstanceID, arg.Status)
	var session RuntimeSession
	err := scanRuntimeSession(row, &session)
	return session, err
}

const closeStaleRuntimeSession = `-- name: CloseStaleRuntimeSession :one
UPDATE runtime_sessions
SET status = 'offline',
    capacity = GREATEST(capacity, inflight),
    attached_core_instance_id = NULL,
    disconnected_at = clock_timestamp(),
    updated_at = clock_timestamp()
WHERE runtime_session_id = $1
  AND heartbeat_at = $2
  AND status IN ('active', 'draining')
RETURNING runtime_session_id, node_id, agent_id, credential_id, worker_id,
          session_epoch, device_certificate_serial, node_version,
          protocol_version, runtime_contract_id, runtime_contract_digest,
          features, capacity, inflight, status, attached_core_instance_id,
          connected_at, heartbeat_at, disconnected_at, created_at, updated_at`

type CloseStaleRuntimeSessionParams struct {
	RuntimeSessionID uuid.UUID `db:"runtime_session_id" json:"runtime_session_id"`
	HeartbeatAt      time.Time `db:"heartbeat_at" json:"heartbeat_at"`
}

func (q *Queries) CloseStaleRuntimeSession(ctx context.Context, arg CloseStaleRuntimeSessionParams) (RuntimeSession, error) {
	row := q.db.QueryRow(ctx, closeStaleRuntimeSession, arg.RuntimeSessionID, arg.HeartbeatAt)
	var session RuntimeSession
	err := scanRuntimeSession(row, &session)
	return session, err
}

const createRuntimeSessionAttachment = `-- name: CreateRuntimeSessionAttachment :one
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
          attached_at, detached_at, disconnect_reason`

type CreateRuntimeSessionAttachmentParams struct {
	RuntimeSessionID uuid.UUID `db:"runtime_session_id" json:"runtime_session_id"`
	CoreInstanceID   uuid.UUID `db:"core_instance_id" json:"core_instance_id"`
	AttachmentKind   string    `db:"attachment_kind" json:"attachment_kind"`
}

func (q *Queries) CreateRuntimeSessionAttachment(ctx context.Context, arg CreateRuntimeSessionAttachmentParams) (RuntimeSessionAttachment, error) {
	row := q.db.QueryRow(ctx, createRuntimeSessionAttachment,
		arg.RuntimeSessionID,
		arg.CoreInstanceID,
		arg.AttachmentKind,
	)
	var attachment RuntimeSessionAttachment
	err := scanRuntimeSessionAttachment(row, &attachment)
	return attachment, err
}

const getActiveRuntimeSessionAttachment = `-- name: GetActiveRuntimeSessionAttachment :one
SELECT id, runtime_session_id, core_instance_id, attachment_kind,
       attached_at, detached_at, disconnect_reason
FROM runtime_session_attachments
WHERE runtime_session_id = $1
  AND detached_at IS NULL`

func (q *Queries) GetActiveRuntimeSessionAttachment(ctx context.Context, runtimeSessionID uuid.UUID) (RuntimeSessionAttachment, error) {
	row := q.db.QueryRow(ctx, getActiveRuntimeSessionAttachment, runtimeSessionID)
	var attachment RuntimeSessionAttachment
	err := scanRuntimeSessionAttachment(row, &attachment)
	return attachment, err
}

const listRuntimeSessionAttachments = `-- name: ListRuntimeSessionAttachments :many
SELECT id, runtime_session_id, core_instance_id, attachment_kind,
       attached_at, detached_at, disconnect_reason
FROM runtime_session_attachments
WHERE runtime_session_id = $1
ORDER BY attached_at DESC, id DESC
LIMIT $2 OFFSET $3`

type ListRuntimeSessionAttachmentsParams struct {
	RuntimeSessionID uuid.UUID `db:"runtime_session_id" json:"runtime_session_id"`
	Limit            int32     `db:"limit" json:"limit"`
	Offset           int32     `db:"offset" json:"offset"`
}

func (q *Queries) ListRuntimeSessionAttachments(ctx context.Context, arg ListRuntimeSessionAttachmentsParams) ([]RuntimeSessionAttachment, error) {
	rows, err := q.db.Query(ctx, listRuntimeSessionAttachments, arg.RuntimeSessionID, arg.Limit, arg.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []RuntimeSessionAttachment
	for rows.Next() {
		var attachment RuntimeSessionAttachment
		if err := scanRuntimeSessionAttachment(rows, &attachment); err != nil {
			return nil, err
		}
		items = append(items, attachment)
	}
	return items, rows.Err()
}

const listActiveRuntimeSessionAttachmentsByCore = `-- name: ListActiveRuntimeSessionAttachmentsByCore :many
SELECT id, runtime_session_id, core_instance_id, attachment_kind,
       attached_at, detached_at, disconnect_reason
FROM runtime_session_attachments
WHERE core_instance_id = $1
  AND detached_at IS NULL
ORDER BY attached_at ASC, id ASC`

func (q *Queries) ListActiveRuntimeSessionAttachmentsByCore(ctx context.Context, coreInstanceID uuid.UUID) ([]RuntimeSessionAttachment, error) {
	rows, err := q.db.Query(ctx, listActiveRuntimeSessionAttachmentsByCore, coreInstanceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []RuntimeSessionAttachment
	for rows.Next() {
		var attachment RuntimeSessionAttachment
		if err := scanRuntimeSessionAttachment(rows, &attachment); err != nil {
			return nil, err
		}
		items = append(items, attachment)
	}
	return items, rows.Err()
}

const closeRuntimeSessionAttachment = `-- name: CloseRuntimeSessionAttachment :one
UPDATE runtime_session_attachments
SET detached_at = clock_timestamp(),
    disconnect_reason = $3
WHERE runtime_session_id = $1
  AND core_instance_id = $2
  AND detached_at IS NULL
RETURNING id, runtime_session_id, core_instance_id, attachment_kind,
          attached_at, detached_at, disconnect_reason`

type CloseRuntimeSessionAttachmentParams struct {
	RuntimeSessionID uuid.UUID `db:"runtime_session_id" json:"runtime_session_id"`
	CoreInstanceID   uuid.UUID `db:"core_instance_id" json:"core_instance_id"`
	DisconnectReason *string   `db:"disconnect_reason" json:"disconnect_reason"`
}

func (q *Queries) CloseRuntimeSessionAttachment(ctx context.Context, arg CloseRuntimeSessionAttachmentParams) (RuntimeSessionAttachment, error) {
	row := q.db.QueryRow(ctx, closeRuntimeSessionAttachment,
		arg.RuntimeSessionID,
		arg.CoreInstanceID,
		arg.DisconnectReason,
	)
	var attachment RuntimeSessionAttachment
	err := scanRuntimeSessionAttachment(row, &attachment)
	return attachment, err
}

const closeRuntimeSessionAttachmentsByCore = `-- name: CloseRuntimeSessionAttachmentsByCore :execrows
UPDATE runtime_session_attachments
SET detached_at = clock_timestamp(),
    disconnect_reason = $2
WHERE core_instance_id = $1
  AND detached_at IS NULL`

type CloseRuntimeSessionAttachmentsByCoreParams struct {
	CoreInstanceID   uuid.UUID `db:"core_instance_id" json:"core_instance_id"`
	DisconnectReason *string   `db:"disconnect_reason" json:"disconnect_reason"`
}

func (q *Queries) CloseRuntimeSessionAttachmentsByCore(ctx context.Context, arg CloseRuntimeSessionAttachmentsByCoreParams) (int64, error) {
	tag, err := q.db.Exec(ctx, closeRuntimeSessionAttachmentsByCore, arg.CoreInstanceID, arg.DisconnectReason)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
