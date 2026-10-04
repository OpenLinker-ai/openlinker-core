package runtime

import (
	"context"
	"errors"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// The credential's mutable Sessions, the addressed Session and Node are already
// locked by the normal Hello transaction. An expired/mismatched admission never
// falls back to the ordinary offline-successor path.
func (t *postgresRuntimeSessionTransaction) createControlledRuntimeSessionSuccessor(ctx context.Context, p db.CreateDrainingRuntimeSessionSuccessorParams) (db.RuntimeSession, bool, error) {
	var operationID uuid.UUID
	err := t.tx.QueryRow(ctx, `SELECT current_operation_id FROM runtime_node_upgrade_state WHERE node_id=$1 AND current_operation_id IS NOT NULL`, p.NodeID).Scan(&operationID)
	if errors.Is(err, pgx.ErrNoRows) {
		return db.RuntimeSession{}, false, nil
	}
	if err != nil {
		return db.RuntimeSession{}, true, err
	}
	ready, err := runtimeNodeUpgradeClusterReady(ctx, t.tx, p.AttachedCoreInstanceID)
	if err != nil {
		return db.RuntimeSession{}, true, err
	}
	if !ready {
		return db.RuntimeSession{}, true, runtimeUnavailableError()
	}
	var allowed bool
	err = t.tx.QueryRow(ctx, `SELECT EXISTS (
 SELECT 1 FROM runtime_node_upgrade_admissions a
 JOIN runtime_node_upgrade_operations op USING(operation_id)
 JOIN runtime_node_upgrade_state st ON st.current_operation_id=op.operation_id AND st.node_id=op.node_id AND st.revision=op.revision
 JOIN runtime_nodes n ON n.node_id=op.node_id
 JOIN runtime_sessions source ON source.runtime_session_id=a.source_session_id
 JOIN runtime_node_bindings b ON b.credential_id=a.credential_id AND b.node_id=n.node_id AND b.agent_id=a.agent_id
 JOIN agent_tokens token ON token.id=a.credential_id AND token.agent_id=a.agent_id
 WHERE op.operation_id=$1 AND op.node_id=$2 AND op.deadline_at>clock_timestamp() AND op.target_version=$3
 AND a.agent_id=$4 AND a.worker_id=$5 AND a.credential_id=$6 AND a.last_admitted_epoch<$7
 AND n.status='draining' AND n.revoked_at IS NULL
 AND token.status='active_runtime' AND token.revoked_at IS NULL AND token.scopes @> ARRAY['agent:pull']::text[]
 AND (token.expires_at IS NULL OR token.expires_at>clock_timestamp())
 AND b.public_key_thumbprint=$8 AND n.device_public_key_thumbprint=$8 AND n.device_certificate_serial=$9
 AND source.node_id=n.node_id AND source.agent_id=a.agent_id AND source.worker_id=a.worker_id
 AND source.credential_id=a.credential_id AND source.session_epoch=a.minimum_epoch AND source.status='closed'
 AND NOT EXISTS(SELECT 1 FROM runtime_sessions s WHERE s.node_id=n.node_id AND s.agent_id=a.agent_id AND s.worker_id=a.worker_id
   AND (s.session_epoch >= $7 OR s.status IN ('active','draining') OR s.inflight<>0))
)`, operationID, p.NodeID, p.NodeVersion, p.AgentID, p.WorkerID, p.CredentialID, p.SessionEpoch, p.DevicePublicKeyThumbprint, p.DeviceCertificateSerial).Scan(&allowed)
	if err != nil {
		return db.RuntimeSession{}, true, err
	}
	if !allowed {
		return db.RuntimeSession{}, true, pgx.ErrNoRows
	}
	if _, err = t.tx.Exec(ctx, `SELECT set_config('openlinker.runtime_node_upgrade_admission',$1,true)`, operationID.String()); err != nil {
		return db.RuntimeSession{}, true, err
	}
	// Fence failed previous starts too. Their immutable identity stays intact.
	if _, err = t.tx.Exec(ctx, `UPDATE runtime_sessions SET status='closed',updated_at=clock_timestamp()
 WHERE node_id=$1 AND agent_id=$2 AND worker_id=$3 AND credential_id=$4 AND status='offline'
 AND session_epoch>(SELECT minimum_epoch FROM runtime_node_upgrade_admissions
   WHERE operation_id=$5 AND agent_id=$2 AND worker_id=$3 AND credential_id=$4)`, p.NodeID, p.AgentID, p.WorkerID, p.CredentialID, operationID); err != nil {
		return db.RuntimeSession{}, true, err
	}
	_, err = t.tx.Exec(ctx, `INSERT INTO runtime_sessions
 (runtime_session_id,node_id,agent_id,credential_id,worker_id,session_epoch,device_certificate_serial,node_version,
 protocol_version,runtime_contract_id,runtime_contract_digest,features,capacity,status,attached_core_instance_id,
 drain_requested_at,drain_deadline_at,drain_reason_code,resume_capacity)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,0,'draining',$13,
 clock_timestamp(),clock_timestamp()+($14::bigint*INTERVAL '1 millisecond'),'ADMIN_REQUESTED',$15)`,
		p.RuntimeSessionID, p.NodeID, p.AgentID, p.CredentialID, p.WorkerID, p.SessionEpoch, p.DeviceCertificateSerial, p.NodeVersion,
		p.ProtocolVersion, p.RuntimeContractID, p.RuntimeContractDigest, p.Features, p.AttachedCoreInstanceID, p.DrainDeadlineMS, p.ResumeCapacity)
	if err != nil {
		return db.RuntimeSession{}, true, err
	}
	_, err = t.tx.Exec(ctx, `UPDATE runtime_node_upgrade_admissions SET last_admitted_epoch=$4
 WHERE operation_id=$1 AND agent_id=$2 AND worker_id=$3`, operationID, p.AgentID, p.WorkerID, p.SessionEpoch)
	if err != nil {
		return db.RuntimeSession{}, true, err
	}
	session, err := t.queries.GetRuntimeSession(ctx, p.RuntimeSessionID)
	return session, true, err
}

// Offline reattachment of an admitted Session is still admission. Keep a live
// attachment usable for heartbeat/drain, but never let an expired ticket turn
// an offline Session into an attached one through the ordinary claim query.
func (t *postgresRuntimeSessionTransaction) authorizeControlledSessionClaim(ctx context.Context, p db.ClaimRuntimeSessionForCoreParams) error {
	var operationID uuid.UUID
	var allowed bool
	err := t.tx.QueryRow(ctx, `SELECT st.current_operation_id, EXISTS (
 SELECT 1 FROM runtime_node_upgrade_operations op
 JOIN runtime_node_upgrade_admissions a USING(operation_id)
 WHERE op.operation_id=st.current_operation_id AND op.node_id=s.node_id AND op.revision=st.revision
 AND op.deadline_at>clock_timestamp() AND op.target_version=s.node_version
 AND a.agent_id=s.agent_id AND a.worker_id=s.worker_id AND a.credential_id=s.credential_id
 AND a.minimum_epoch<s.session_epoch AND a.last_admitted_epoch=s.session_epoch)
 FROM runtime_sessions s JOIN runtime_node_upgrade_state st ON st.node_id=s.node_id
 WHERE s.runtime_session_id=$1 AND s.status='offline' AND st.current_operation_id IS NOT NULL`, p.RuntimeSessionID).Scan(&operationID, &allowed)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if !allowed {
		return pgx.ErrNoRows
	}
	ready, err := runtimeNodeUpgradeClusterReady(ctx, t.tx, p.CoreInstanceID)
	if err != nil {
		return err
	}
	if !ready {
		return runtimeUnavailableError()
	}
	_, err = t.tx.Exec(ctx, `SELECT set_config('openlinker.runtime_node_upgrade_admission',$1,true)`, operationID.String())
	return err
}
