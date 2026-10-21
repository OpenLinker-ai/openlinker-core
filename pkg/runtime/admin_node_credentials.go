package runtime

import (
	"context"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Called only under the existing Session -> Node -> Token -> Attachment locks,
// after all ordinary controlled-upgrade gates and the version/revision CAS.
func (s *Service) checkCredentialUpgrade(ctx context.Context, tx pgx.Tx, node runtimeNodeRecord, r RuntimeNodeUpgradeRequest, candidates []uuid.UUID) (string, error) {
	if s.cfg == nil || !s.cfg.RuntimeMTLSListenerEnabled() || s.cfg.RuntimePKIMode == "files" {
		return "", nodeUpgradeConflict("RUNTIME_NODE_MTLS_UNAVAILABLE")
	}
	if err := CheckRuntimeCredentialReadiness(ctx, tx, s.coreInstanceID, s.cfg.RuntimeAuthenticationPolicyID()); err != nil {
		return "", err
	}
	var oldKey, mode string
	var credential uuid.UUID
	err := tx.QueryRow(ctx, `SELECT b.credential_id,b.public_key_thumbprint,b.binding_mode
 FROM runtime_node_bindings b JOIN runtime_nodes n USING(node_id)
 WHERE b.node_id=$1 AND b.public_key_thumbprint=n.device_public_key_thumbprint`, node.nodeID).Scan(&credential, &oldKey, &mode)
	if err != nil {
		if err == pgx.ErrNoRows {
			return "", nodeUpgradeConflict("RUNTIME_NODE_IDENTITY_INVALID")
		}
		return "", nodeUpgradeDatabaseError(err, "读取认证绑定失败")
	}
	if credential != r.CredentialID || (r.Kind == "token_to_mtls" && mode != "token_only") ||
		(r.Kind == "certificate_rotation" && (mode != "mtls" || oldKey != r.PublicKeyThumbprint)) {
		return "", nodeUpgradeConflict("RUNTIME_NODE_CREDENTIAL_CONFLICT")
	}
	var exact bool
	err = tx.QueryRow(ctx, `SELECT count(*)>0 AND bool_and(credential_id=$2) FROM runtime_sessions WHERE runtime_session_id=ANY($1::uuid[])`, candidates, credential).Scan(&exact)
	if err != nil {
		return "", nodeUpgradeDatabaseError(err, "读取认证升级准入失败")
	}
	if !exact {
		return "", nodeUpgradeConflict("RUNTIME_NODE_CREDENTIAL_CONFLICT")
	}
	return oldKey, nil
}

// CheckRuntimeCredentialReadiness shares the exact upgrade cluster gate with
// credential issuance. The caller already holds the Node and credential locks.
func CheckRuntimeCredentialReadiness(ctx context.Context, tx pgx.Tx, instanceID uuid.UUID, policyID string) error {
	if _, err := tx.Exec(ctx, `SELECT singleton_id FROM runtime_cluster_control WHERE singleton_id=1 FOR SHARE`); err != nil {
		return nodeUpgradeDatabaseError(err, "锁定集群控制面失败")
	}
	ready, err := runtimeNodeUpgradeClusterReady(ctx, tx, instanceID)
	if err != nil {
		return nodeUpgradeDatabaseError(err, "检查 Runtime 集群失败")
	}
	if !ready {
		return nodeUpgradeConflict("RUNTIME_NODE_CORE_NOT_READY")
	}
	var consistent bool
	if !validSHA256Hex(policyID) {
		return nodeUpgradeConflict("RUNTIME_NODE_MTLS_POLICY_MISMATCH")
	}
	if err = tx.QueryRow(ctx, `SELECT count(*)>0 AND bool_and(COALESCE(authentication_policy_id,'')=$1) FROM runtime_cluster_members LEFT JOIN runtime_cluster_authentication_policies USING(instance_id) WHERE heartbeat_at>=clock_timestamp()-INTERVAL '15 seconds'`, policyID).Scan(&consistent); err != nil {
		return nodeUpgradeDatabaseError(err, "检查 Runtime 认证策略失败")
	}
	if !consistent {
		return nodeUpgradeConflict("RUNTIME_NODE_MTLS_POLICY_MISMATCH")
	}
	return nil
}
