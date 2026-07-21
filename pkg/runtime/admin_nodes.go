package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

const (
	defaultRuntimeNodeLimit    int32 = 50
	maxRuntimeNodeLimit        int32 = 200
	runtimeNodeLiveWindow            = RuntimeSessionStaleAfter
	runtimeNodeMutationRetries       = 3
	runtimeNodeDrainDeadline         = 60 * time.Second
)

var errRuntimeNodeSessionScopeChanged = errors.New("runtime node session scope changed")

const (
	runtimeNodeErrorRevoked         httpx.ErrorCode = "RUNTIME_NODE_REVOKED"
	runtimeNodeErrorNotDraining     httpx.ErrorCode = "RUNTIME_NODE_NOT_DRAINING"
	runtimeNodeErrorNotQuiescent    httpx.ErrorCode = "RUNTIME_NODE_NOT_QUIESCENT"
	runtimeNodeErrorIdentityInvalid httpx.ErrorCode = "RUNTIME_NODE_IDENTITY_INVALID"
	runtimeNodeErrorSessionChanged  httpx.ErrorCode = "RUNTIME_NODE_SESSION_CHANGED"
)

type lockedRuntimeNodeSession struct {
	runtimeSessionID  uuid.UUID
	agentID           uuid.UUID
	credentialID      uuid.UUID
	coreInstanceID    *uuid.UUID
	workerID          string
	sessionEpoch      int64
	status            string
	capacity          int32
	inflight          int32
	heartbeatAt       time.Time
	disconnectedAt    *time.Time
	protocolVersion   int32
	contractID        string
	contractDigest    string
	certificateSerial string
	resumeCapacity    *int32
}

type runtimeNodeRecord struct {
	nodeID                uuid.UUID
	displayName           string
	nodeVersion           string
	protocolVersion       int32
	runtimeContractID     string
	runtimeContractDigest string
	features              []string
	capacity              int32
	inflight              int32
	status                string
	lastSeenAt            *time.Time
	drainingAt            *time.Time
	revokedAt             *time.Time
	revokeReason          *string
	createdAt             time.Time
	updatedAt             time.Time
}

// ListRuntimeNodes returns a database-clock snapshot of enrolled Runtime
// Nodes. Session counts intentionally use the canonical Runtime liveness window
// as runtime availability; an attached but stale Session is not presented as
// online to an operator.
func (s *Service) ListRuntimeNodes(
	ctx context.Context,
	limit, offset int32,
) (*RuntimeNodeListResponse, error) {
	if s == nil || s.pool == nil {
		return nil, httpx.ServiceUnavailable("Runtime Node 管理能力不可用")
	}
	if limit <= 0 {
		limit = defaultRuntimeNodeLimit
	}
	if limit > maxRuntimeNodeLimit {
		limit = maxRuntimeNodeLimit
	}
	if offset < 0 {
		return nil, httpx.BadRequest("offset 不能小于 0")
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, httpx.Internal("查询 Runtime Node 失败")
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var databaseTime time.Time
	var currentContractID, currentContractDigest string
	err = tx.QueryRow(ctx, `
SELECT clock_timestamp(), runtime_contract_id, runtime_contract_digest
FROM runtime_schema_contracts
WHERE is_current`).Scan(&databaseTime, &currentContractID, &currentContractDigest)
	if err != nil {
		return nil, httpx.Internal("查询 Runtime contract 失败")
	}

	var total int32
	if err = tx.QueryRow(ctx, `SELECT COUNT(*)::int FROM runtime_nodes`).Scan(&total); err != nil {
		return nil, httpx.Internal("查询 Runtime Node 失败")
	}

	rows, err := tx.Query(ctx, `
WITH live_sessions AS (
    SELECT s.node_id,
           COUNT(*)::int AS active_session_count,
           COUNT(DISTINCT s.agent_id)::int AS active_agent_count
    FROM runtime_sessions s
    JOIN runtime_nodes live_node
      ON live_node.node_id = s.node_id
     AND live_node.status IN ('active', 'draining')
     AND live_node.revoked_at IS NULL
     AND live_node.protocol_version = s.protocol_version
     AND live_node.runtime_contract_id = s.runtime_contract_id
     AND live_node.runtime_contract_digest = s.runtime_contract_digest
    JOIN runtime_wire_contracts wire
      ON wire.runtime_contract_id = s.runtime_contract_id
     AND wire.runtime_contract_digest = s.runtime_contract_digest
     AND wire.support_tier IN ('current', 'previous')
    WHERE s.status IN ('active', 'draining')
      AND s.attached_core_instance_id IS NOT NULL
      AND s.disconnected_at IS NULL
      AND s.heartbeat_at >= $3::timestamptz - ($4::bigint * INTERVAL '1 millisecond')
      AND live_node.last_seen_at >= $3::timestamptz - ($4::bigint * INTERVAL '1 millisecond')
      AND EXISTS (
          SELECT 1
          FROM runtime_session_attachments attachment
          WHERE attachment.runtime_session_id = s.runtime_session_id
            AND attachment.core_instance_id = s.attached_core_instance_id
            AND attachment.detached_at IS NULL
      )
    GROUP BY s.node_id
)
SELECT n.node_id, n.display_name, n.node_version, n.protocol_version,
       n.runtime_contract_id, n.runtime_contract_digest, n.features,
       n.capacity, n.inflight, n.status, n.last_seen_at, n.draining_at,
       n.revoked_at, n.revoke_reason, n.created_at, n.updated_at,
       COALESCE(live.active_session_count, 0)::int,
       COALESCE(live.active_agent_count, 0)::int
FROM runtime_nodes n
LEFT JOIN live_sessions live ON live.node_id = n.node_id
ORDER BY n.created_at DESC, n.node_id DESC
LIMIT $1 OFFSET $2`, limit, offset, databaseTime, runtimeNodeLiveWindow.Milliseconds())
	if err != nil {
		return nil, httpx.Internal("查询 Runtime Node 失败")
	}
	defer rows.Close()

	items := make([]RuntimeNodeListItem, 0, limit)
	for rows.Next() {
		var record runtimeNodeRecord
		var activeSessions, activeAgents int32
		if err = scanRuntimeNodeRecord(rows, &record, &activeSessions, &activeAgents); err != nil {
			return nil, httpx.Internal("读取 Runtime Node 失败")
		}
		items = append(items, runtimeNodeListItem(
			record,
			currentContractID,
			currentContractDigest,
			activeSessions,
			activeAgents,
		))
	}
	if err = rows.Err(); err != nil {
		return nil, httpx.Internal("读取 Runtime Node 失败")
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, httpx.Internal("查询 Runtime Node 失败")
	}

	return &RuntimeNodeListResponse{
		Items:                 items,
		Total:                 total,
		Limit:                 limit,
		Offset:                offset,
		CurrentContractID:     currentContractID,
		CurrentContractDigest: currentContractDigest,
		DatabaseTime:          databaseTime,
	}, nil
}

func (s *Service) DrainRuntimeNode(ctx context.Context, nodeID uuid.UUID) (*RuntimeNodeListItem, error) {
	return s.mutateRuntimeNode(ctx, nodeID, "", false)
}

// ActivateRuntimeNode is a guarded rollback of an administrative drain. It is
// deliberately separate from heartbeat/session attach: only a quiescent,
// current, non-revoked Node can move backwards from draining to active.
func (s *Service) ActivateRuntimeNode(ctx context.Context, nodeID uuid.UUID) (*RuntimeNodeListItem, error) {
	if s == nil || s.pool == nil {
		return nil, httpx.ServiceUnavailable("Runtime Node 管理能力不可用")
	}
	if nodeID == uuid.Nil {
		return nil, httpx.BadRequest("node id 不是合法 uuid")
	}
	for attempt := 0; attempt < runtimeNodeMutationRetries; attempt++ {
		item, err := s.activateRuntimeNodeOnce(ctx, nodeID)
		if !errors.Is(err, errRuntimeNodeSessionScopeChanged) {
			return item, err
		}
	}
	return nil, httpx.NewError(
		409, runtimeNodeErrorSessionChanged, "Runtime Node 的 Session 正在变化，请重试",
	)
}

func (s *Service) activateRuntimeNodeOnce(ctx context.Context, nodeID uuid.UUID) (*RuntimeNodeListItem, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, httpx.Internal("激活 Runtime Node 失败")
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Keep the global principal lock order shared with drain/revoke and Session
	// lifecycle: Session -> Node -> Token -> Attachment.
	sessions, err := lockRuntimeNodeSessions(ctx, tx, nodeID)
	if err != nil {
		return nil, httpx.Internal("锁定 Runtime Session 失败")
	}
	record, err := lockRuntimeNode(ctx, tx, nodeID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("Runtime Node 不存在")
	}
	if err != nil {
		return nil, httpx.Internal("锁定 Runtime Node 失败")
	}
	if record.status == "revoked" || record.revokedAt != nil {
		return nil, httpx.NewError(409, runtimeNodeErrorRevoked, "已撤销的 Runtime Node 不可恢复")
	}
	if record.status != "draining" || record.drainingAt == nil {
		return nil, httpx.NewError(409, runtimeNodeErrorNotDraining, "Runtime Node 当前不在排空状态")
	}
	credentialIDs := uniqueRuntimeNodeCredentials(sessions)
	if err = lockRuntimeNodeTokens(ctx, tx, credentialIDs); err != nil {
		return nil, httpx.NewError(409, runtimeNodeErrorIdentityInvalid, "Runtime credential 已失效")
	}
	sessionIDs := runtimeNodeSessionIDs(sessions)
	if err = lockRuntimeNodeAttachments(ctx, tx, sessionIDs); err != nil {
		return nil, httpx.Internal("锁定 Runtime attachment 失败")
	}
	changed, err := runtimeNodeSessionScopeChanged(ctx, tx, nodeID, sessionIDs)
	if err != nil {
		return nil, httpx.Internal("确认 Runtime Session 范围失败")
	}
	if changed {
		return nil, errRuntimeNodeSessionScopeChanged
	}

	var databaseTime time.Time
	var currentContractID, currentContractDigest string
	if err = tx.QueryRow(ctx, `
SELECT clock_timestamp(), runtime_contract_id, runtime_contract_digest
FROM runtime_schema_contracts WHERE is_current`).Scan(
		&databaseTime, &currentContractID, &currentContractDigest,
	); err != nil {
		return nil, httpx.Internal("读取 Runtime contract 失败")
	}
	if record.protocolVersion != RuntimeProtocolVersion ||
		record.runtimeContractID != currentContractID ||
		record.runtimeContractDigest != currentContractDigest ||
		!containsRuntimeFeature(record.features, "session_drain") {
		return nil, httpx.NewError(409, runtimeNodeErrorIdentityInvalid, "Runtime Node contract 不可激活")
	}
	var unfinished bool
	if err = tx.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM run_attempts
    WHERE node_id = $1 AND finished_at IS NULL
)`, nodeID).Scan(&unfinished); err != nil {
		return nil, httpx.Internal("检查 Runtime Attempt 失败")
	}
	if unfinished {
		return nil, httpx.NewError(409, runtimeNodeErrorNotQuiescent, "Runtime Node 仍有未完成的 offer 或 Attempt")
	}
	if record.inflight != 0 {
		return nil, httpx.NewError(409, runtimeNodeErrorNotQuiescent, "Runtime Node 仍有执行中的任务")
	}
	for _, session := range sessions {
		if session.inflight != 0 {
			return nil, httpx.NewError(409, runtimeNodeErrorNotQuiescent, "Runtime Session 仍有执行中的任务")
		}
	}

	invalidLive, err := runtimeNodeHasInvalidLiveActivationSession(
		ctx, tx, nodeID, databaseTime, currentContractID, currentContractDigest,
	)
	if err != nil {
		return nil, httpx.Internal("验证 Runtime Session 身份失败")
	}
	if invalidLive {
		return nil, httpx.NewError(409, runtimeNodeErrorIdentityInvalid, "Runtime Session 身份或 contract 已失效")
	}
	candidateCount, err := runtimeNodeActivationCandidateCount(
		ctx, tx, nodeID, databaseTime, currentContractID, currentContractDigest,
	)
	if err != nil {
		return nil, httpx.Internal("检查 Runtime Session 激活候选失败")
	}
	if candidateCount < 1 {
		return nil, httpx.NewError(409, runtimeNodeErrorNotQuiescent, "Runtime Node 没有可安全恢复的在线 Session")
	}
	tag, err := tx.Exec(ctx, `
UPDATE runtime_sessions s
SET status = 'active',
    capacity = s.resume_capacity,
    drain_requested_at = NULL,
    drain_deadline_at = NULL,
    drain_reason_code = NULL,
    resume_capacity = NULL,
    updated_at = clock_timestamp()
WHERE s.node_id = $1
  AND s.status = 'draining'
  AND s.inflight = 0
  AND s.resume_capacity IS NOT NULL
  AND s.attached_core_instance_id IS NOT NULL
  AND s.disconnected_at IS NULL
  AND s.heartbeat_at >= $2::timestamptz - ($3::bigint * INTERVAL '1 millisecond')
  AND s.protocol_version = $4
  AND s.runtime_contract_id = $5
  AND s.runtime_contract_digest = $6
	AND s.features @> ARRAY['session_drain']::text[]
	AND EXISTS (
	    SELECT 1 FROM runtime_nodes n
	    WHERE n.node_id = s.node_id
	      AND n.status = 'draining'
	      AND n.revoked_at IS NULL
	      AND n.device_certificate_serial = s.device_certificate_serial
	      AND n.protocol_version = s.protocol_version
	      AND n.runtime_contract_id = s.runtime_contract_id
	      AND n.runtime_contract_digest = s.runtime_contract_digest
	)
	AND EXISTS (
	    SELECT 1 FROM agent_tokens token
	    WHERE token.id = s.credential_id
	      AND token.agent_id = s.agent_id
	      AND token.status = 'active_runtime'
	      AND token.revoked_at IS NULL
	      AND token.scopes @> ARRAY['agent:pull']::text[]
	      AND (token.expires_at IS NULL OR token.expires_at > $2)
	)
  AND NOT EXISTS (
      SELECT 1 FROM runtime_sessions newer
      WHERE newer.node_id = s.node_id
        AND newer.agent_id = s.agent_id
        AND newer.worker_id = s.worker_id
        AND newer.session_epoch > s.session_epoch
        AND newer.status IN ('active', 'draining', 'offline')
  )
  AND EXISTS (
      SELECT 1 FROM runtime_session_attachments attachment
      WHERE attachment.runtime_session_id = s.runtime_session_id
        AND attachment.core_instance_id = s.attached_core_instance_id
        AND attachment.detached_at IS NULL
  )`, nodeID, databaseTime, runtimeNodeLiveWindow.Milliseconds(),
		RuntimeProtocolVersion, currentContractID, currentContractDigest)
	if err != nil {
		return nil, httpx.Internal("恢复 Runtime Session 失败")
	}
	if tag.RowsAffected() != int64(candidateCount) {
		return nil, httpx.NewError(409, runtimeNodeErrorSessionChanged, "Runtime Session 激活范围发生变化")
	}
	if _, err = tx.Exec(
		ctx,
		`SELECT set_config('openlinker.runtime_node_activation', $1, true)`,
		nodeID.String(),
	); err != nil {
		return nil, httpx.Internal("建立 Runtime Node 激活防线失败")
	}
	tag, err = tx.Exec(ctx, `
UPDATE runtime_nodes
SET status = 'active', draining_at = NULL, updated_at = clock_timestamp()
WHERE node_id = $1 AND status = 'draining' AND revoked_at IS NULL`, nodeID)
	if err != nil {
		return nil, httpx.Internal("激活 Runtime Node 失败")
	}
	if tag.RowsAffected() != 1 {
		return nil, httpx.NewError(409, runtimeNodeErrorSessionChanged, "Runtime Node 激活范围发生变化")
	}

	record, err = getRuntimeNodeRecord(ctx, tx, nodeID)
	if err != nil {
		return nil, httpx.Internal("读取 Runtime Node 失败")
	}
	activeSessions, activeAgents, err := runtimeNodeLiveCounts(ctx, tx, nodeID, runtimeNodeLiveWindow)
	if err != nil {
		return nil, httpx.Internal("读取 Runtime Session 失败")
	}
	item := runtimeNodeListItem(record, currentContractID, currentContractDigest, activeSessions, activeAgents)
	if err = tx.Commit(ctx); err != nil {
		return nil, httpx.Internal("激活 Runtime Node 失败")
	}
	return &item, nil
}

func (s *Service) RevokeRuntimeNode(
	ctx context.Context,
	nodeID uuid.UUID,
	reason string,
) (*RuntimeNodeListItem, error) {
	reason = strings.TrimSpace(reason)
	if !utf8.ValidString(reason) || utf8.RuneCountInString(reason) < 1 || utf8.RuneCountInString(reason) > 500 {
		return nil, httpx.Unprocessable("撤销原因长度必须为 1 到 500 个字符")
	}
	return s.mutateRuntimeNode(ctx, nodeID, reason, true)
}

func (s *Service) mutateRuntimeNode(
	ctx context.Context,
	nodeID uuid.UUID,
	reason string,
	revoke bool,
) (*RuntimeNodeListItem, error) {
	if s == nil || s.pool == nil {
		return nil, httpx.ServiceUnavailable("Runtime Node 管理能力不可用")
	}
	if nodeID == uuid.Nil {
		return nil, httpx.BadRequest("node id 不是合法 uuid")
	}

	for attempt := 0; attempt < runtimeNodeMutationRetries; attempt++ {
		item, err := s.mutateRuntimeNodeOnce(ctx, nodeID, reason, revoke)
		if !errors.Is(err, errRuntimeNodeSessionScopeChanged) {
			return item, err
		}
	}
	return nil, httpx.Conflict("Runtime Node 的 Session 正在变化，请重试")
}

func (s *Service) mutateRuntimeNodeOnce(
	ctx context.Context,
	nodeID uuid.UUID,
	reason string,
	revoke bool,
) (*RuntimeNodeListItem, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, httpx.Internal("更新 Runtime Node 失败")
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Global principal lock order: Session -> Node -> Token -> Attachment.
	sessions, err := lockRuntimeNodeSessions(ctx, tx, nodeID)
	if err != nil {
		return nil, httpx.Internal("锁定 Runtime Session 失败")
	}
	record, err := lockRuntimeNode(ctx, tx, nodeID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("Runtime Node 不存在")
	}
	if err != nil {
		return nil, httpx.Internal("锁定 Runtime Node 失败")
	}
	if !revoke && record.status == "revoked" {
		return nil, httpx.Conflict("已撤销的 Runtime Node 不能排空")
	}
	if revoke && record.status == "revoked" {
		item := runtimeNodeListItem(record, record.runtimeContractID, record.runtimeContractDigest, 0, 0)
		if err = tx.Commit(ctx); err != nil {
			return nil, httpx.Internal("读取 Runtime Node 失败")
		}
		return &item, nil
	}

	credentialIDs := uniqueRuntimeNodeCredentials(sessions)
	if err = lockRuntimeNodeTokens(ctx, tx, credentialIDs); err != nil {
		return nil, httpx.Internal("锁定 Runtime credential 失败")
	}
	sessionIDs := runtimeNodeSessionIDs(sessions)
	if err = lockRuntimeNodeAttachments(ctx, tx, sessionIDs); err != nil {
		return nil, httpx.Internal("锁定 Runtime attachment 失败")
	}
	// A Session may have committed while this transaction was waiting for the
	// Node row. Roll back and restart instead of acquiring it after the Node,
	// which would violate the global lock order.
	changed, err := runtimeNodeSessionScopeChanged(ctx, tx, nodeID, sessionIDs)
	if err != nil {
		return nil, httpx.Internal("确认 Runtime Session 范围失败")
	}
	if changed {
		return nil, errRuntimeNodeSessionScopeChanged
	}

	if revoke {
		if err = revokeLockedRuntimeNode(ctx, tx, nodeID, reason, sessionIDs); err != nil {
			return nil, httpx.Internal("撤销 Runtime Node 失败")
		}
	} else if err = drainLockedRuntimeNode(ctx, tx, nodeID, sessionIDs); err != nil {
		return nil, httpx.Internal("排空 Runtime Node 失败")
	}
	if err = enqueueRuntimeNodeSignals(ctx, tx, sessions, revoke); err != nil {
		return nil, httpx.Internal("创建 Runtime Node 通知失败")
	}

	record, err = getRuntimeNodeRecord(ctx, tx, nodeID)
	if err != nil {
		return nil, httpx.Internal("读取 Runtime Node 失败")
	}
	activeSessions, activeAgents, err := runtimeNodeLiveCounts(ctx, tx, nodeID, runtimeNodeLiveWindow)
	if err != nil {
		return nil, httpx.Internal("读取 Runtime Session 失败")
	}
	var currentContractID, currentContractDigest string
	if err = tx.QueryRow(ctx, `
SELECT runtime_contract_id, runtime_contract_digest
FROM runtime_schema_contracts WHERE is_current`).Scan(
		&currentContractID, &currentContractDigest,
	); err != nil {
		return nil, httpx.Internal("读取 Runtime contract 失败")
	}
	item := runtimeNodeListItem(record, currentContractID, currentContractDigest, activeSessions, activeAgents)
	if err = tx.Commit(ctx); err != nil {
		return nil, httpx.Internal("更新 Runtime Node 失败")
	}
	return &item, nil
}

func lockRuntimeNodeSessions(ctx context.Context, tx pgx.Tx, nodeID uuid.UUID) ([]lockedRuntimeNodeSession, error) {
	rows, err := tx.Query(ctx, `
SELECT runtime_session_id, agent_id, credential_id, attached_core_instance_id,
       worker_id, session_epoch, status, capacity, inflight, heartbeat_at,
       disconnected_at, protocol_version, runtime_contract_id,
       runtime_contract_digest, device_certificate_serial, resume_capacity
FROM runtime_sessions
WHERE node_id = $1
  AND status IN ('active', 'draining', 'offline')
ORDER BY runtime_session_id ASC
FOR UPDATE`, nodeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]lockedRuntimeNodeSession, 0)
	for rows.Next() {
		var item lockedRuntimeNodeSession
		if err = rows.Scan(
			&item.runtimeSessionID, &item.agentID, &item.credentialID, &item.coreInstanceID,
			&item.workerID, &item.sessionEpoch, &item.status, &item.capacity,
			&item.inflight, &item.heartbeatAt, &item.disconnectedAt,
			&item.protocolVersion, &item.contractID, &item.contractDigest,
			&item.certificateSerial, &item.resumeCapacity,
		); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func lockRuntimeNode(ctx context.Context, tx pgx.Tx, nodeID uuid.UUID) (runtimeNodeRecord, error) {
	return queryRuntimeNodeRecord(ctx, tx, `
SELECT node_id, display_name, node_version, protocol_version,
       runtime_contract_id, runtime_contract_digest, features, capacity,
       inflight, status, last_seen_at, draining_at, revoked_at, revoke_reason,
       created_at, updated_at
FROM runtime_nodes WHERE node_id = $1 FOR UPDATE`, nodeID)
}

func getRuntimeNodeRecord(ctx context.Context, tx pgx.Tx, nodeID uuid.UUID) (runtimeNodeRecord, error) {
	return queryRuntimeNodeRecord(ctx, tx, `
SELECT node_id, display_name, node_version, protocol_version,
       runtime_contract_id, runtime_contract_digest, features, capacity,
       inflight, status, last_seen_at, draining_at, revoked_at, revoke_reason,
       created_at, updated_at
FROM runtime_nodes WHERE node_id = $1`, nodeID)
}

func queryRuntimeNodeRecord(ctx context.Context, tx pgx.Tx, statement string, nodeID uuid.UUID) (runtimeNodeRecord, error) {
	var record runtimeNodeRecord
	err := tx.QueryRow(ctx, statement, nodeID).Scan(
		&record.nodeID, &record.displayName, &record.nodeVersion,
		&record.protocolVersion, &record.runtimeContractID,
		&record.runtimeContractDigest, &record.features, &record.capacity,
		&record.inflight, &record.status, &record.lastSeenAt,
		&record.drainingAt, &record.revokedAt, &record.revokeReason,
		&record.createdAt, &record.updatedAt,
	)
	return record, err
}

func lockRuntimeNodeTokens(ctx context.Context, tx pgx.Tx, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	rows, err := tx.Query(ctx, `
SELECT id FROM agent_tokens
WHERE id = ANY($1::uuid[])
ORDER BY id ASC
FOR UPDATE`, ids)
	if err != nil {
		return err
	}
	defer rows.Close()
	locked := 0
	for rows.Next() {
		var ignored uuid.UUID
		if err = rows.Scan(&ignored); err != nil {
			return err
		}
		locked++
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if locked != len(ids) {
		return errors.New("runtime session credential disappeared")
	}
	return nil
}

func lockRuntimeNodeAttachments(ctx context.Context, tx pgx.Tx, sessionIDs []uuid.UUID) error {
	if len(sessionIDs) == 0 {
		return nil
	}
	rows, err := tx.Query(ctx, `
SELECT id
FROM runtime_session_attachments
WHERE runtime_session_id = ANY($1::uuid[])
  AND detached_at IS NULL
ORDER BY id ASC
FOR UPDATE`, sessionIDs)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var ignored uuid.UUID
		if err = rows.Scan(&ignored); err != nil {
			return err
		}
	}
	return rows.Err()
}

func runtimeNodeSessionScopeChanged(
	ctx context.Context,
	tx pgx.Tx,
	nodeID uuid.UUID,
	locked []uuid.UUID,
) (bool, error) {
	var changed bool
	err := tx.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM runtime_sessions
    WHERE node_id = $1
      AND status IN ('active', 'draining', 'offline')
      AND NOT (runtime_session_id = ANY($2::uuid[]))
)`, nodeID, locked).Scan(&changed)
	return changed, err
}

func drainLockedRuntimeNode(ctx context.Context, tx pgx.Tx, nodeID uuid.UUID, sessionIDs []uuid.UUID) error {
	if len(sessionIDs) > 0 {
		if _, err := tx.Exec(ctx, `
UPDATE runtime_sessions
SET drain_requested_at = COALESCE(drain_requested_at, clock_timestamp()),
    drain_deadline_at = COALESCE(drain_deadline_at, clock_timestamp() + ($2::bigint * INTERVAL '1 millisecond')),
    drain_reason_code = COALESCE(drain_reason_code, 'ADMIN_REQUESTED'),
    resume_capacity = COALESCE(resume_capacity, capacity),
    status = 'draining',
    capacity = 0,
    updated_at = clock_timestamp()
WHERE runtime_session_id = ANY($1::uuid[])
	  AND status IN ('active', 'draining')`, sessionIDs, runtimeNodeDrainDeadline.Milliseconds()); err != nil {
			return err
		}
	}
	_, err := tx.Exec(ctx, `
UPDATE runtime_nodes
SET status = 'draining',
    draining_at = COALESCE(draining_at, clock_timestamp()),
    updated_at = clock_timestamp()
WHERE node_id = $1
  AND status IN ('active', 'draining')`, nodeID)
	return err
}

func runtimeNodeHasInvalidLiveActivationSession(
	ctx context.Context,
	tx pgx.Tx,
	nodeID uuid.UUID,
	databaseTime time.Time,
	contractID, contractDigest string,
) (bool, error) {
	var invalid bool
	err := tx.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1
    FROM runtime_sessions s
    JOIN runtime_nodes n ON n.node_id = s.node_id
    WHERE s.node_id = $1
      AND s.status IN ('active', 'draining')
      AND s.attached_core_instance_id IS NOT NULL
      AND s.disconnected_at IS NULL
      AND s.heartbeat_at >= $2::timestamptz - ($3::bigint * INTERVAL '1 millisecond')
      AND (
          s.status <> 'draining'
          OR
          s.inflight <> 0
          OR s.capacity <> 0
          OR s.resume_capacity IS NULL
          OR s.drain_requested_at IS NULL
          OR s.drain_deadline_at IS NULL
          OR s.drain_reason_code IS NULL
          OR s.protocol_version <> $4
          OR s.runtime_contract_id <> $5
          OR s.runtime_contract_digest <> $6
          OR s.device_certificate_serial <> n.device_certificate_serial
          OR NOT (s.features @> ARRAY['session_drain']::text[])
          OR EXISTS (
              SELECT 1 FROM runtime_sessions newer
              WHERE newer.node_id = s.node_id
                AND newer.agent_id = s.agent_id
                AND newer.worker_id = s.worker_id
                AND newer.session_epoch > s.session_epoch
                AND newer.status IN ('active', 'draining', 'offline')
          )
          OR NOT EXISTS (
              SELECT 1 FROM agent_tokens token
              WHERE token.id = s.credential_id
                AND token.agent_id = s.agent_id
                AND token.status = 'active_runtime'
                AND token.revoked_at IS NULL
                AND token.scopes @> ARRAY['agent:pull']::text[]
                AND (token.expires_at IS NULL OR token.expires_at > $2)
          )
          OR NOT EXISTS (
              SELECT 1 FROM runtime_session_attachments attachment
              WHERE attachment.runtime_session_id = s.runtime_session_id
                AND attachment.core_instance_id = s.attached_core_instance_id
                AND attachment.detached_at IS NULL
          )
      )
)`, nodeID, databaseTime, runtimeNodeLiveWindow.Milliseconds(),
		RuntimeProtocolVersion, contractID, contractDigest).Scan(&invalid)
	return invalid, err
}

func runtimeNodeActivationCandidateCount(
	ctx context.Context,
	tx pgx.Tx,
	nodeID uuid.UUID,
	databaseTime time.Time,
	contractID, contractDigest string,
) (int32, error) {
	var count int32
	err := tx.QueryRow(ctx, `
SELECT COUNT(*)::int
FROM runtime_sessions s
JOIN runtime_nodes n ON n.node_id = s.node_id
JOIN agent_tokens token
  ON token.id = s.credential_id
 AND token.agent_id = s.agent_id
WHERE s.node_id = $1
  AND s.status = 'draining'
  AND s.inflight = 0
  AND s.capacity = 0
  AND s.resume_capacity IS NOT NULL
  AND s.drain_requested_at IS NOT NULL
  AND s.drain_deadline_at IS NOT NULL
  AND s.drain_reason_code IS NOT NULL
  AND s.attached_core_instance_id IS NOT NULL
  AND s.disconnected_at IS NULL
  AND s.heartbeat_at >= $2::timestamptz - ($3::bigint * INTERVAL '1 millisecond')
  AND s.protocol_version = $4
  AND s.runtime_contract_id = $5
  AND s.runtime_contract_digest = $6
  AND s.features @> ARRAY['session_drain']::text[]
  AND n.status = 'draining'
  AND n.revoked_at IS NULL
  AND n.device_certificate_serial = s.device_certificate_serial
  AND n.protocol_version = s.protocol_version
  AND n.runtime_contract_id = s.runtime_contract_id
  AND n.runtime_contract_digest = s.runtime_contract_digest
  AND token.status = 'active_runtime'
  AND token.revoked_at IS NULL
  AND token.scopes @> ARRAY['agent:pull']::text[]
  AND (token.expires_at IS NULL OR token.expires_at > $2)
  AND NOT EXISTS (
      SELECT 1 FROM runtime_sessions newer
      WHERE newer.node_id = s.node_id
        AND newer.agent_id = s.agent_id
        AND newer.worker_id = s.worker_id
        AND newer.session_epoch > s.session_epoch
        AND newer.status IN ('active', 'draining', 'offline')
  )
  AND EXISTS (
      SELECT 1 FROM runtime_session_attachments attachment
      WHERE attachment.runtime_session_id = s.runtime_session_id
        AND attachment.core_instance_id = s.attached_core_instance_id
        AND attachment.detached_at IS NULL
  )`, nodeID, databaseTime, runtimeNodeLiveWindow.Milliseconds(),
		RuntimeProtocolVersion, contractID, contractDigest).Scan(&count)
	return count, err
}

func containsRuntimeFeature(features []string, feature string) bool {
	for _, candidate := range features {
		if candidate == feature {
			return true
		}
	}
	return false
}

func revokeLockedRuntimeNode(
	ctx context.Context,
	tx pgx.Tx,
	nodeID uuid.UUID,
	reason string,
	sessionIDs []uuid.UUID,
) error {
	if len(sessionIDs) > 0 {
		if _, err := tx.Exec(ctx, `
UPDATE runtime_session_attachments
SET detached_at = clock_timestamp(), disconnect_reason = 'node_revoked'
WHERE runtime_session_id = ANY($1::uuid[])
  AND detached_at IS NULL`, sessionIDs); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
UPDATE runtime_sessions
SET status = 'revoked',
    capacity = GREATEST(capacity, inflight),
    attached_core_instance_id = NULL,
    disconnected_at = COALESCE(disconnected_at, clock_timestamp()),
    updated_at = clock_timestamp()
WHERE runtime_session_id = ANY($1::uuid[])
  AND status IN ('active', 'draining', 'offline')`, sessionIDs); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `
UPDATE runtime_nodes
SET status = 'revoked',
    capacity = GREATEST(capacity, inflight),
    revoked_at = clock_timestamp(),
    revoke_reason = $2,
    updated_at = clock_timestamp()
WHERE node_id = $1
  AND status <> 'revoked'`, nodeID, reason); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `
UPDATE runtime_node_certificates
SET revoked_at = COALESCE(revoked_at, clock_timestamp())
WHERE node_id = $1`, nodeID)
	return err
}

func enqueueRuntimeNodeSignals(
	ctx context.Context,
	tx pgx.Tx,
	sessions []lockedRuntimeNodeSession,
	revoke bool,
) error {
	type signalTarget struct {
		agentID uuid.UUID
		coreID  uuid.UUID
	}
	targets := make(map[signalTarget]struct{})
	for _, session := range sessions {
		if session.coreInstanceID == nil || *session.coreInstanceID == uuid.Nil {
			continue
		}
		targets[signalTarget{agentID: session.agentID, coreID: *session.coreInstanceID}] = struct{}{}
	}
	ordered := make([]signalTarget, 0, len(targets))
	for target := range targets {
		ordered = append(ordered, target)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].agentID != ordered[j].agentID {
			return ordered[i].agentID.String() < ordered[j].agentID.String()
		}
		return ordered[i].coreID.String() < ordered[j].coreID.String()
	})
	eventType := "node.drain"
	if revoke {
		eventType = "node.revoke"
	}
	for _, target := range ordered {
		payload, err := json.Marshal(map[string]string{"target_instance_id": target.coreID.String()})
		if err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `
INSERT INTO runtime_signal_outbox (event_type, agent_id, payload, available_at)
VALUES ($1, $2, $3, clock_timestamp())`, eventType, target.agentID, payload); err != nil {
			return err
		}
	}
	return nil
}

func runtimeNodeLiveCounts(
	ctx context.Context,
	tx pgx.Tx,
	nodeID uuid.UUID,
	liveWindow time.Duration,
) (int32, int32, error) {
	var sessions, agents int32
	err := tx.QueryRow(ctx, `
SELECT COUNT(*)::int, COUNT(DISTINCT s.agent_id)::int
FROM runtime_sessions s
JOIN runtime_nodes n
  ON n.node_id = s.node_id
 AND n.status IN ('active', 'draining')
 AND n.revoked_at IS NULL
 AND n.protocol_version = s.protocol_version
 AND n.runtime_contract_id = s.runtime_contract_id
 AND n.runtime_contract_digest = s.runtime_contract_digest
JOIN runtime_wire_contracts wire
  ON wire.runtime_contract_id = s.runtime_contract_id
 AND wire.runtime_contract_digest = s.runtime_contract_digest
 AND wire.support_tier IN ('current', 'previous')
WHERE s.node_id = $1
  AND s.status IN ('active', 'draining')
  AND s.attached_core_instance_id IS NOT NULL
  AND s.disconnected_at IS NULL
  AND s.heartbeat_at >= clock_timestamp() - ($2::bigint * INTERVAL '1 millisecond')
  AND n.last_seen_at >= clock_timestamp() - ($2::bigint * INTERVAL '1 millisecond')
  AND EXISTS (
      SELECT 1 FROM runtime_session_attachments attachment
      WHERE attachment.runtime_session_id = s.runtime_session_id
        AND attachment.core_instance_id = s.attached_core_instance_id
        AND attachment.detached_at IS NULL
  )`, nodeID, liveWindow.Milliseconds()).Scan(&sessions, &agents)
	return sessions, agents, err
}

func uniqueRuntimeNodeCredentials(sessions []lockedRuntimeNodeSession) []uuid.UUID {
	seen := make(map[uuid.UUID]struct{}, len(sessions))
	items := make([]uuid.UUID, 0, len(sessions))
	for _, session := range sessions {
		if _, ok := seen[session.credentialID]; ok {
			continue
		}
		seen[session.credentialID] = struct{}{}
		items = append(items, session.credentialID)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].String() < items[j].String() })
	return items
}

func runtimeNodeSessionIDs(sessions []lockedRuntimeNodeSession) []uuid.UUID {
	items := make([]uuid.UUID, 0, len(sessions))
	for _, session := range sessions {
		items = append(items, session.runtimeSessionID)
	}
	return items
}

func runtimeNodeListItem(
	record runtimeNodeRecord,
	currentContractID, currentContractDigest string,
	activeSessions, activeAgents int32,
) RuntimeNodeListItem {
	return RuntimeNodeListItem{
		NodeID:                record.nodeID.String(),
		DisplayName:           record.displayName,
		NodeVersion:           record.nodeVersion,
		ProtocolVersion:       record.protocolVersion,
		RuntimeContractID:     record.runtimeContractID,
		RuntimeContractDigest: record.runtimeContractDigest,
		ContractMatch: record.runtimeContractID == currentContractID &&
			record.runtimeContractDigest == currentContractDigest,
		Features:           append([]string(nil), record.features...),
		Capacity:           record.capacity,
		Inflight:           record.inflight,
		Status:             record.status,
		LastSeenAt:         record.lastSeenAt,
		DrainingAt:         record.drainingAt,
		RevokedAt:          record.revokedAt,
		RevokeReason:       record.revokeReason,
		CreatedAt:          record.createdAt,
		UpdatedAt:          record.updatedAt,
		ActiveSessionCount: activeSessions,
		ActiveAgentCount:   activeAgents,
	}
}

type runtimeNodeRecordScanner interface {
	Scan(dest ...any) error
}

func scanRuntimeNodeRecord(
	row runtimeNodeRecordScanner,
	record *runtimeNodeRecord,
	activeSessions, activeAgents *int32,
) error {
	return row.Scan(
		&record.nodeID, &record.displayName, &record.nodeVersion,
		&record.protocolVersion, &record.runtimeContractID,
		&record.runtimeContractDigest, &record.features, &record.capacity,
		&record.inflight, &record.status, &record.lastSeenAt,
		&record.drainingAt, &record.revokedAt, &record.revokeReason,
		&record.createdAt, &record.updatedAt, activeSessions, activeAgents,
	)
}
