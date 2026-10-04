package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const runtimeNodeUpgradeScopeLimit = 10000
const runtimeNodeUpgradeBudget = 10 * time.Second

// These operations authorize new Sessions, never resurrection of a closed ID.
type RuntimeNodeUpgradeRequest struct {
	OperationID      uuid.UUID `json:"operation_id"`
	Kind             string    `json:"kind"`
	ExpectedVersion  string    `json:"expected_version"`
	ExpectedRevision int64     `json:"expected_revision"`
	TargetVersion    string    `json:"target_version"`
	DeadlineAt       time.Time `json:"deadline_at"`
}

type RuntimeNodeUpgradeStatus struct {
	NodeID             uuid.UUID         `json:"node_id"`
	Version            string            `json:"version"`
	Revision           int64             `json:"revision"`
	DatabaseTime       time.Time         `json:"database_time"`
	SessionCount       int               `json:"session_count"`
	LiveSessions       int               `json:"live_sessions"`
	Inflight           int64             `json:"inflight"`
	UnfinishedAttempts int               `json:"unfinished_attempts"`
	LiveAttachments    int               `json:"live_attachments"`
	AdmissionCount     int               `json:"admission_count"`
	Eligible           bool              `json:"eligible"`
	Blockers           []httpx.ErrorCode `json:"blockers"`
}

type RuntimeNodeUpgradeReceipt struct {
	OperationID    uuid.UUID `json:"operation_id"`
	NodeID         uuid.UUID `json:"node_id"`
	Kind           string    `json:"kind"`
	FromVersion    string    `json:"from_version"`
	TargetVersion  string    `json:"target_version"`
	Revision       int64     `json:"revision"`
	AdmissionCount int       `json:"admission_count"`
	CreatedAt      time.Time `json:"created_at"`
	DeadlineAt     time.Time `json:"deadline_at"`
	Replayed       bool      `json:"replayed"`
}

var runtimeNodeVersionPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]*/[a-zA-Z0-9][a-zA-Z0-9.+_-]*$`)

func validRuntimeNodeUpgradeRequest(r RuntimeNodeUpgradeRequest) bool {
	if r.OperationID == uuid.Nil || r.ExpectedRevision < 0 || r.DeadlineAt.IsZero() ||
		len(r.ExpectedVersion) > 100 || len(r.TargetVersion) > 100 ||
		!runtimeNodeVersionPattern.MatchString(r.ExpectedVersion) || !runtimeNodeVersionPattern.MatchString(r.TargetVersion) ||
		strings.SplitN(r.ExpectedVersion, "/", 2)[0] != strings.SplitN(r.TargetVersion, "/", 2)[0] {
		return false
	}
	return (r.Kind == "version_change" && r.ExpectedVersion != r.TargetVersion) ||
		(r.Kind == "restart_after_drain" && r.ExpectedVersion == r.TargetVersion)
}

func nodeUpgradeConflict(code httpx.ErrorCode) error {
	return httpx.NewError(409, code, "Runtime Node 不满足受控升级条件")
}

func (s *Service) CheckRuntimeNodeUpgrade(ctx context.Context, nodeID uuid.UUID) (*RuntimeNodeUpgradeStatus, error) {
	if s == nil || s.pool == nil {
		return nil, httpx.ServiceUnavailable("Runtime Node 管理能力不可用")
	}
	ctx, cancel := context.WithTimeout(ctx, runtimeNodeUpgradeBudget)
	defer cancel()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, nodeUpgradeDatabaseError(err, "查询 Runtime Node 失败")
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	node, err := getRuntimeNodeRecord(ctx, tx, nodeID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("Runtime Node 不存在")
	}
	if err != nil {
		return nil, nodeUpgradeDatabaseError(err, "查询 Runtime Node 失败")
	}
	ids, _, err := runtimeNodeUpgradeScope(ctx, tx, nodeID, false)
	if err != nil {
		return nil, nodeUpgradeDatabaseError(err, "读取 Runtime Node Session 失败")
	}
	status, _, err := inspectRuntimeNodeUpgrade(ctx, tx, node, ids, s.coreInstanceID)
	if err != nil {
		return nil, nodeUpgradeDatabaseError(err, "检查 Runtime Node 升级失败")
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, nodeUpgradeDatabaseError(err, "检查 Runtime Node 升级失败")
	}
	return status, nil
}

// UpgradeRuntimeNode holds the existing Session -> Node -> Token -> Attachment
// lock order. All retries share one deadline; only a changed scope is retried.
func (s *Service) UpgradeRuntimeNode(ctx context.Context, nodeID, actorID uuid.UUID, r RuntimeNodeUpgradeRequest) (*RuntimeNodeUpgradeReceipt, error) {
	if s == nil || s.pool == nil {
		return nil, httpx.ServiceUnavailable("Runtime Node 管理能力不可用")
	}
	if nodeID == uuid.Nil || actorID == uuid.Nil || !validRuntimeNodeUpgradeRequest(r) {
		return nil, httpx.BadRequest("受控升级参数无效")
	}
	r.DeadlineAt = r.DeadlineAt.UTC().Truncate(time.Microsecond)
	canonical, _ := json.Marshal(struct {
		Node, Actor uuid.UUID
		Request     RuntimeNodeUpgradeRequest
	}{nodeID, actorID, r})
	digest := sha256.Sum256(canonical)
	fingerprint := hex.EncodeToString(digest[:])
	ctx, cancel := context.WithTimeout(ctx, runtimeNodeUpgradeBudget)
	defer cancel()
	for attempt := 0; attempt < runtimeNodeMutationRetries; attempt++ {
		receipt, err := s.upgradeRuntimeNodeOnce(ctx, nodeID, actorID, r, fingerprint)
		if !errors.Is(err, errRuntimeNodeSessionScopeChanged) {
			return receipt, err
		}
	}
	return nil, nodeUpgradeConflict(runtimeNodeErrorSessionChanged)
}

func (s *Service) upgradeRuntimeNodeOnce(ctx context.Context, nodeID, actorID uuid.UUID, r RuntimeNodeUpgradeRequest, fingerprint string) (*RuntimeNodeUpgradeReceipt, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, nodeUpgradeDatabaseError(err, "开始 Runtime Node 升级失败")
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err = tx.Exec(ctx, `SET LOCAL lock_timeout = '2s'`); err != nil {
		return nil, nodeUpgradeDatabaseError(err, "Runtime Node 升级事务失败")
	}
	previous, oldFingerprint, err := runtimeNodeUpgradeReceipt(ctx, tx, r.OperationID)
	if err == nil {
		if fingerprint != oldFingerprint {
			return nil, nodeUpgradeConflict("RUNTIME_NODE_OPERATION_CONFLICT")
		}
		previous.Replayed = true
		return previous, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, nodeUpgradeDatabaseError(err, "查询升级回执失败")
	}
	ids, credentials, err := runtimeNodeUpgradeScope(ctx, tx, nodeID, true)
	if err != nil {
		return nil, nodeUpgradeDatabaseError(err, "锁定 Runtime Node Session 失败")
	}
	if len(ids) > runtimeNodeUpgradeScopeLimit {
		return nil, nodeUpgradeConflict("RUNTIME_NODE_SCOPE_LIMIT")
	}
	node, err := lockRuntimeNode(ctx, tx, nodeID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("Runtime Node 不存在")
	}
	if err != nil {
		return nil, nodeUpgradeDatabaseError(err, "锁定 Runtime Node 失败")
	}
	// Another identical request may have committed while this call waited for
	// the Node. Recheck the immutable receipt before applying version/revision CAS.
	previous, oldFingerprint, err = runtimeNodeUpgradeReceipt(ctx, tx, r.OperationID)
	if err == nil {
		if fingerprint != oldFingerprint {
			return nil, nodeUpgradeConflict("RUNTIME_NODE_OPERATION_CONFLICT")
		}
		previous.Replayed = true
		return previous, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, nodeUpgradeDatabaseError(err, "查询升级回执失败")
	}
	if err = lockRuntimeNodeTokens(ctx, tx, credentials); err != nil {
		return nil, nodeUpgradeDatabaseError(err, "锁定 Runtime 凭据失败")
	}
	if err = lockRuntimeNodeAttachments(ctx, tx, ids); err != nil {
		return nil, nodeUpgradeDatabaseError(err, "锁定 Runtime 连接失败")
	}
	freshIDs, _, err := runtimeNodeUpgradeScope(ctx, tx, nodeID, false)
	if err != nil {
		return nil, nodeUpgradeDatabaseError(err, "复核 Session 范围失败")
	}
	if !slices.Equal(ids, freshIDs) {
		return nil, errRuntimeNodeSessionScopeChanged
	}

	// Linearize authorization against maintenance; a snapshot of normal is insufficient.
	if _, err = tx.Exec(ctx, `SELECT singleton_id FROM runtime_cluster_control WHERE singleton_id=1 FOR SHARE`); err != nil {
		return nil, nodeUpgradeDatabaseError(err, "锁定集群控制面失败")
	}
	status, candidates, err := inspectRuntimeNodeUpgrade(ctx, tx, node, ids, s.coreInstanceID)
	if err != nil {
		return nil, nodeUpgradeDatabaseError(err, "检查 Runtime Node 升级失败")
	}
	if !status.Eligible {
		return nil, nodeUpgradeConflict(status.Blockers[0])
	}
	if !r.DeadlineAt.After(status.DatabaseTime) || r.DeadlineAt.After(status.DatabaseTime.Add(15*time.Minute)) {
		return nil, nodeUpgradeConflict("RUNTIME_NODE_OPERATION_EXPIRED")
	}
	if r.ExpectedVersion != node.nodeVersion || r.ExpectedRevision != status.Revision {
		return nil, nodeUpgradeConflict("RUNTIME_NODE_REVISION_CONFLICT")
	}
	_, err = tx.Exec(ctx, `INSERT INTO runtime_node_upgrade_operations
 (operation_id,node_id,actor_id,request_fingerprint,kind,from_version,target_version,revision,admission_count,deadline_at)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, r.OperationID, nodeID, actorID, fingerprint, r.Kind, node.nodeVersion, r.TargetVersion, status.Revision+1, len(candidates), r.DeadlineAt)
	if err != nil {
		var pe *pgconn.PgError
		if errors.As(err, &pe) && pe.Code == "23514" && pe.ConstraintName == "runtime_node_upgrade_operations_check" {
			return nil, nodeUpgradeConflict("RUNTIME_NODE_OPERATION_EXPIRED")
		}
		if errors.As(err, &pe) && pe.Code == "23505" {
			return nil, nodeUpgradeConflict("RUNTIME_NODE_OPERATION_CONFLICT")
		}
		return nil, nodeUpgradeDatabaseError(err, "记录 Runtime Node 升级失败")
	}
	if _, err = tx.Exec(ctx, `INSERT INTO runtime_node_upgrade_state (node_id,revision,current_operation_id) VALUES ($1,$2,$3)
 ON CONFLICT (node_id) DO UPDATE SET revision=EXCLUDED.revision,current_operation_id=EXCLUDED.current_operation_id`, nodeID, status.Revision+1, r.OperationID); err != nil {
		return nil, nodeUpgradeDatabaseError(err, "更新升级 revision 失败")
	}
	if _, err = tx.Exec(ctx, `INSERT INTO runtime_node_upgrade_admissions
 (operation_id,agent_id,worker_id,credential_id,source_session_id,minimum_epoch,last_admitted_epoch)
 SELECT $1,agent_id,worker_id,credential_id,runtime_session_id,session_epoch,session_epoch
 FROM runtime_sessions WHERE runtime_session_id=ANY($2::uuid[])`, r.OperationID, candidates); err != nil {
		return nil, nodeUpgradeDatabaseError(err, "记录后继准入失败")
	}
	// Fence the locked latest offline generations, including invalid Workers.
	// Superseded offline IDs already fail the monotonic-generation check and
	// remain historical; writing them here would break the Session-first locks.
	if _, err = tx.Exec(ctx, `UPDATE runtime_sessions SET status='closed',updated_at=clock_timestamp()
 WHERE node_id=$1 AND status='offline' AND runtime_session_id=ANY($2::uuid[])`, nodeID, ids); err != nil {
		return nil, nodeUpgradeDatabaseError(err, "围栏旧 Session 失败")
	}
	if _, err = tx.Exec(ctx, `SELECT set_config('openlinker.runtime_node_upgrade',$1,true)`, r.OperationID.String()); err != nil {
		return nil, nodeUpgradeDatabaseError(err, "Runtime Node 升级事务失败")
	}
	if _, err = tx.Exec(ctx, `UPDATE runtime_nodes SET node_version=$2,updated_at=clock_timestamp() WHERE node_id=$1`, nodeID, r.TargetVersion); err != nil {
		return nil, nodeUpgradeDatabaseError(err, "更新 Runtime Node 版本失败")
	}
	receipt, _, err := runtimeNodeUpgradeReceipt(ctx, tx, r.OperationID)
	if err != nil {
		return nil, nodeUpgradeDatabaseError(err, "读取升级回执失败")
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, nodeUpgradeDatabaseError(err, "提交 Runtime Node 升级失败；请按原 operation_id 查询重试")
	}
	return receipt, nil
}

func runtimeNodeUpgradeReceipt(ctx context.Context, tx pgx.Tx, id uuid.UUID) (*RuntimeNodeUpgradeReceipt, string, error) {
	r := &RuntimeNodeUpgradeReceipt{}
	var fingerprint string
	err := tx.QueryRow(ctx, `SELECT operation_id,node_id,kind,from_version,target_version,revision,admission_count,created_at,deadline_at,request_fingerprint
 FROM runtime_node_upgrade_operations WHERE operation_id=$1`, id).Scan(&r.OperationID, &r.NodeID, &r.Kind, &r.FromVersion, &r.TargetVersion, &r.Revision, &r.AdmissionCount, &r.CreatedAt, &r.DeadlineAt, &fingerprint)
	return r, fingerprint, err
}

func runtimeNodeUpgradeScope(ctx context.Context, tx pgx.Tx, nodeID uuid.UUID, lock bool) ([]uuid.UUID, []uuid.UUID, error) {
	// All live rows and latest offline generations can still mutate/resume.
	// Superseded offline IDs cannot resume (HasNewerRuntimeSessionGeneration),
	// so they and old terminal epochs remain history outside the lock scope.
	q := `WITH latest AS (
 SELECT DISTINCT ON (agent_id,worker_id) runtime_session_id FROM runtime_sessions
 WHERE node_id=$1 ORDER BY agent_id,worker_id,session_epoch DESC
 ) SELECT s.runtime_session_id,s.credential_id FROM runtime_sessions s
 JOIN runtime_nodes n ON n.node_id=s.node_id
 WHERE s.node_id=$1 AND (s.status IN ('active','draining')
 OR (s.status='offline' AND s.runtime_session_id IN (SELECT runtime_session_id FROM latest)) OR (
 s.status='closed' AND s.runtime_session_id IN (SELECT runtime_session_id FROM latest)
 AND s.drain_reason_code='ADMIN_REQUESTED' AND s.protocol_version=n.protocol_version
 AND s.runtime_contract_id=n.runtime_contract_id AND s.runtime_contract_digest=n.runtime_contract_digest
 AND (s.node_version=n.node_version OR EXISTS (
   SELECT 1 FROM runtime_node_upgrade_state st JOIN runtime_node_upgrade_admissions a ON a.operation_id=st.current_operation_id
   WHERE st.node_id=n.node_id AND a.source_session_id=s.runtime_session_id))
 AND EXISTS (SELECT 1 FROM agent_tokens t WHERE t.id=s.credential_id AND t.agent_id=s.agent_id AND t.status='active_runtime'
   AND t.revoked_at IS NULL AND t.scopes @> ARRAY['agent:pull']::text[] AND (t.expires_at IS NULL OR t.expires_at>clock_timestamp()))))
 ORDER BY s.runtime_session_id LIMIT 10001`
	if lock {
		q += " FOR UPDATE OF s"
	}
	rows, err := tx.Query(ctx, q, nodeID)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	ids := []uuid.UUID{}
	credentials := []uuid.UUID{}
	seen := map[uuid.UUID]bool{}
	for rows.Next() {
		var id, credential uuid.UUID
		if err = rows.Scan(&id, &credential); err != nil {
			return nil, nil, err
		}
		ids = append(ids, id)
		if !seen[credential] {
			credentials = append(credentials, credential)
			seen[credential] = true
		}
	}
	return ids, credentials, rows.Err()
}

func inspectRuntimeNodeUpgrade(ctx context.Context, tx pgx.Tx, node runtimeNodeRecord, ids []uuid.UUID, instanceID uuid.UUID) (*RuntimeNodeUpgradeStatus, []uuid.UUID, error) {
	r := &RuntimeNodeUpgradeStatus{NodeID: node.nodeID, Version: node.nodeVersion, SessionCount: len(ids), Blockers: []httpx.ErrorCode{}}
	err := tx.QueryRow(ctx, `SELECT clock_timestamp(),COALESCE((SELECT revision FROM runtime_node_upgrade_state WHERE node_id=$1),0)`, node.nodeID).Scan(&r.DatabaseTime, &r.Revision)
	if err != nil {
		return nil, nil, err
	}
	block := func(code httpx.ErrorCode) { r.Blockers = append(r.Blockers, code) }
	if node.status == "revoked" || node.revokedAt != nil {
		block(runtimeNodeErrorRevoked)
	}
	if node.status != "draining" || node.drainingAt == nil {
		block(runtimeNodeErrorNotDraining)
	}
	if len(ids) > runtimeNodeUpgradeScopeLimit {
		block("RUNTIME_NODE_SCOPE_LIMIT")
		return r, nil, nil
	}
	err = tx.QueryRow(ctx, `SELECT count(*) FILTER (WHERE status IN ('active','draining')),
 COALESCE(sum(inflight),0),
 (SELECT count(*) FROM (
   SELECT id FROM run_attempts WHERE node_id=$1 AND finished_at IS NULL
   UNION ALL
   SELECT a.id FROM runtime_sessions history JOIN run_attempts a ON a.active_runtime_session_id=history.runtime_session_id
   WHERE history.node_id=$1 AND a.finished_at IS NOT NULL AND a.slot_acquired_at IS NOT NULL AND a.slot_released_at IS NULL
 ) unfinished),
 (SELECT count(*) FROM runtime_session_attachments a JOIN runtime_sessions s USING(runtime_session_id) WHERE s.node_id=$1 AND a.detached_at IS NULL)
 FROM runtime_sessions WHERE node_id=$1`, node.nodeID).Scan(&r.LiveSessions, &r.Inflight, &r.UnfinishedAttempts, &r.LiveAttachments)
	if err != nil {
		return nil, nil, err
	}
	if r.LiveSessions != 0 || r.Inflight != 0 || node.inflight != 0 || r.UnfinishedAttempts != 0 || r.LiveAttachments != 0 {
		block(runtimeNodeErrorNotQuiescent)
	}
	ready, err := runtimeNodeUpgradeClusterReady(ctx, tx, instanceID)
	if err != nil {
		return nil, nil, err
	}
	if !ready {
		block("RUNTIME_NODE_CORE_NOT_READY")
	}
	if node.protocolVersion != RuntimeProtocolVersion || node.runtimeContractID != RuntimeContractID || node.runtimeContractDigest != RuntimeContractDigest || !containsRuntimeFeature(node.features, "session_drain") {
		block(runtimeNodeErrorIdentityInvalid)
	}
	// Validate latest Workers in one set query. Revoked terminal history is not
	// an authorization candidate. Quiescent invalid offline identities are fenced
	// without admission, so a retired Worker cannot block its valid siblings.
	rows, err := tx.Query(ctx, `WITH latest AS (
 SELECT DISTINCT ON (agent_id,worker_id) * FROM runtime_sessions WHERE node_id=$1 ORDER BY agent_id,worker_id,session_epoch DESC
 ), candidates AS (
 SELECT s.*,COALESCE(t.status='active_runtime' AND t.revoked_at IS NULL AND t.scopes @> ARRAY['agent:pull']::text[] AND (t.expires_at IS NULL OR t.expires_at>$2),false) token_valid
 FROM latest s LEFT JOIN agent_tokens t ON t.id=s.credential_id AND t.agent_id=s.agent_id
 )
 SELECT s.runtime_session_id,s.status,COALESCE(
 s.token_valid AND s.status IN ('offline','closed') AND s.inflight=0
 AND s.attached_core_instance_id IS NULL AND s.disconnected_at IS NOT NULL
 AND s.drain_reason_code='ADMIN_REQUESTED' AND s.drain_requested_at IS NOT NULL
 AND s.device_certificate_serial=n.device_certificate_serial
 AND s.protocol_version=n.protocol_version AND s.runtime_contract_id=n.runtime_contract_id AND s.runtime_contract_digest=n.runtime_contract_digest
 AND s.features @> n.features AND n.features @> s.features
 AND (s.node_version=n.node_version OR EXISTS (
   SELECT 1 FROM runtime_node_upgrade_state st JOIN runtime_node_upgrade_admissions a ON a.operation_id=st.current_operation_id
   WHERE st.node_id=n.node_id AND a.source_session_id=s.runtime_session_id AND a.credential_id=s.credential_id))
 AND EXISTS (SELECT 1 FROM runtime_node_bindings b WHERE b.credential_id=s.credential_id AND b.agent_id=s.agent_id AND b.node_id=n.node_id
   AND b.public_key_thumbprint=n.device_public_key_thumbprint AND (b.binding_mode='token_only' OR (b.binding_mode='mtls' AND EXISTS (
     SELECT 1 FROM runtime_node_certificates cert WHERE cert.node_id=n.node_id AND cert.public_key_thumbprint=b.public_key_thumbprint
     AND cert.revoked_at IS NULL AND cert.not_before<=$2 AND cert.not_after>$2)))),false)
 FROM candidates s JOIN runtime_nodes n ON n.node_id=s.node_id
 WHERE s.status='offline' OR (s.status<>'revoked' AND s.token_valid)`, node.nodeID, r.DatabaseTime)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	candidates := []uuid.UUID{}
	for rows.Next() {
		var id uuid.UUID
		var valid bool
		var status string
		if err = rows.Scan(&id, &status, &valid); err != nil {
			return nil, nil, err
		}
		if valid {
			candidates = append(candidates, id)
		}
	}
	if err = rows.Err(); err != nil {
		return nil, nil, err
	}
	r.AdmissionCount = len(candidates)
	if len(candidates) == 0 {
		block(runtimeNodeErrorIdentityInvalid)
	}
	r.Eligible = len(r.Blockers) == 0
	return r, candidates, nil
}

func runtimeNodeUpgradeClusterReady(ctx context.Context, tx pgx.Tx, instanceID uuid.UUID) (bool, error) {
	var ready bool
	err := tx.QueryRow(ctx, `WITH live AS (SELECT * FROM runtime_cluster_members WHERE heartbeat_at>=clock_timestamp()-INTERVAL '15 seconds')
 SELECT COALESCE(c.mode='normal'
 AND (SELECT count(*) FROM live)=c.expected_replicas
 AND EXISTS(SELECT 1 FROM live WHERE instance_id=$1)
 AND (SELECT count(DISTINCT (release_version,release_commit)) FROM live)=1
 AND NOT EXISTS(SELECT 1 FROM live WHERE NOT ready OR draining OR schema_version<>$2 OR schema_checksum<>$3 OR runtime_contract_id<>$4 OR runtime_contract_digest<>$5)
 AND EXISTS(SELECT 1 FROM runtime_schema_contracts WHERE is_current AND schema_version=$2 AND migration_name=$6 AND runtime_contract_id=$4 AND runtime_contract_digest=$5),false)
 FROM runtime_cluster_control c WHERE singleton_id=1`, instanceID, RuntimeSchemaVersion, RuntimeSchemaChecksum, RuntimeContractID, RuntimeContractDigest, RuntimeSchemaMigrationName).Scan(&ready)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return ready, err
}

// Contention and bounded-call exhaustion are retryable. Reuse the operation ID
// because a timeout alone cannot prove that a commit did not reach PostgreSQL.
func nodeUpgradeDatabaseError(err error, message string) error {
	var pe *pgconn.PgError
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		(errors.As(err, &pe) && (pe.Code == "55P03" || pe.Code == "40P01" || pe.Code == "57014")) {
		return httpx.NewError(503, "RUNTIME_NODE_OPERATION_BUSY", "升级事务繁忙或超时；请使用原 operation_id 重试")
	}
	return httpx.Internal(message)
}
