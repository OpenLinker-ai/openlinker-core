package runtimepki

import (
	"context"
	"crypto/x509"
	"errors"
	"net/http"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	coreruntime "github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// The ordinary enrollment/renewal API cannot change authentication mode. Only
// an exact current administrative operation can reach this transaction branch.
func (s *CredentialService) controlledCredential(ctx context.Context, tx pgx.Tx, token db.AgentRuntimeToken, nodeID uuid.UUID, request CredentialRequest, csr *x509.CertificateRequest, bindingMode, boundKey, requestedKey string) (CredentialResponse, error) {
	conflict := func() (CredentialResponse, error) {
		return CredentialResponse{}, httpx.NewError(http.StatusConflict, "RUNTIME_CREDENTIAL_OPERATION_REJECTED", "认证操作已过期、已变更或不满足执行条件")
	}
	var kind, fromKey, targetKey string
	var current, quiescent bool
	err := tx.QueryRow(ctx, `SELECT op.kind,auth.from_public_key_thumbprint,auth.target_public_key_thumbprint,
 COALESCE(st.current_operation_id=op.operation_id AND st.revision=op.revision AND op.deadline_at>clock_timestamp(),false),
 NOT EXISTS(SELECT 1 FROM runtime_sessions WHERE node_id=$2 AND (status IN ('active','draining') OR inflight<>0))
 AND NOT EXISTS(SELECT 1 FROM run_attempts a WHERE (a.node_id=$2 OR a.active_runtime_session_id IN (SELECT runtime_session_id FROM runtime_sessions WHERE node_id=$2)) AND (a.finished_at IS NULL OR (a.slot_acquired_at IS NOT NULL AND a.slot_released_at IS NULL)))
 AND NOT EXISTS(SELECT 1 FROM runtime_session_attachments a JOIN runtime_sessions se USING(runtime_session_id) WHERE se.node_id=$2 AND a.detached_at IS NULL)
 AND n.inflight=0 AND n.status='draining' AND n.revoked_at IS NULL
 FROM runtime_node_upgrade_operations op
 JOIN runtime_node_credential_authorizations auth USING(operation_id)
 JOIN runtime_nodes n ON n.node_id=op.node_id
 LEFT JOIN runtime_node_upgrade_state st ON st.node_id=op.node_id
 WHERE op.operation_id=$1 AND op.node_id=$2 AND auth.credential_id=$3
 AND op.kind IN ('token_to_mtls','certificate_rotation')
 AND op.target_version=$4 AND op.target_features @> $5::text[] AND $5::text[] @> op.target_features`, request.OperationID, nodeID, token.ID, request.NodeVersion, request.Features).Scan(&kind, &fromKey, &targetKey, &current, &quiescent)
	if errors.Is(err, pgx.ErrNoRows) {
		return conflict()
	}
	if err != nil {
		return CredentialResponse{}, httpx.Internal("读取认证操作失败")
	}
	if requestedKey != targetKey {
		return conflict()
	}
	// Completed operations replay exactly their certificate, never whichever
	// certificate happened to be issued most recently by another request.
	var replay ClientCertificate
	err = tx.QueryRow(ctx, `SELECT c.certificate_pem,c.certificate_chain_pem,c.trust_bundle_pem,c.certificate_serial,c.certificate_fingerprint,c.public_key_thumbprint,c.not_before,c.not_after,c.renew_after
 FROM runtime_node_credential_operations done JOIN runtime_node_certificates c USING(certificate_serial)
 WHERE done.operation_id=$1 AND c.node_id=$2 AND c.public_key_thumbprint=$3
 AND c.revoked_at IS NULL AND c.not_before<=clock_timestamp() AND c.not_after>clock_timestamp()`, request.OperationID, nodeID, requestedKey).Scan(
		&replay.CertificatePEM, &replay.CertificateChainPEM, &replay.TrustBundlePEM, &replay.Serial, &replay.FingerprintSHA256, &replay.PublicKeySHA256, &replay.NotBefore, &replay.NotAfter, &replay.RenewAfter)
	if err == nil {
		if bindingMode != "mtls" || boundKey != targetKey {
			return conflict()
		}
		if err = tx.Commit(ctx); err != nil {
			return CredentialResponse{}, httpx.Internal("重放认证操作失败")
		}
		return credentialResponse(token.AgentID, nodeID, replay), nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return CredentialResponse{}, httpx.Internal("读取认证操作回执失败")
	}
	var completed bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM runtime_node_credential_operations WHERE operation_id=$1)`, request.OperationID).Scan(&completed); err != nil {
		return CredentialResponse{}, httpx.Internal("读取认证操作回执失败")
	}
	if completed || !current || !quiescent || boundKey != fromKey ||
		(kind == "token_to_mtls" && bindingMode != "token_only") ||
		(kind == "certificate_rotation" && (bindingMode != "mtls" || fromKey != targetKey)) {
		return conflict()
	}
	if err = coreruntime.CheckRuntimeCredentialReadiness(ctx, tx, s.coreInstanceID, s.authenticationPolicyID); err != nil {
		return CredentialResponse{}, err
	}
	issued, err := s.authority.IssueClientCertificate(csr, nodeID)
	if err != nil {
		return CredentialResponse{}, httpx.BadRequest("CSR 无法签发")
	}
	if err = insertRuntimeNodeCertificate(ctx, tx, nodeID, issued); err != nil {
		return CredentialResponse{}, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO runtime_node_credential_operations(operation_id,certificate_serial) VALUES($1,$2)`, request.OperationID, issued.Serial); err != nil {
		return CredentialResponse{}, httpx.Internal("记录认证操作失败")
	}
	if kind == "token_to_mtls" {
		if _, err = tx.Exec(ctx, `SELECT set_config('openlinker.runtime_credential_operation',$1,true)`, request.OperationID); err != nil {
			return CredentialResponse{}, httpx.Internal("认证操作事务失败")
		}
		// The stable serial anchor and every installation identity are preserved.
		if _, err = tx.Exec(ctx, `UPDATE runtime_nodes SET device_public_key_thumbprint=$2,updated_at=clock_timestamp() WHERE node_id=$1`, nodeID, targetKey); err != nil {
			return CredentialResponse{}, httpx.Internal("转换 Node 认证失败")
		}
		if _, err = tx.Exec(ctx, `UPDATE runtime_node_bindings SET public_key_thumbprint=$2,binding_mode='mtls' WHERE credential_id=$1`, token.ID, targetKey); err != nil {
			return CredentialResponse{}, httpx.Internal("转换 Runtime 凭据认证失败")
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return CredentialResponse{}, httpx.Internal("提交认证操作失败；请按原 operation_id 重试")
	}
	return credentialResponse(token.AgentID, nodeID, issued), nil
}
