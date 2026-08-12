package a2a

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	coreruntime "github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

const runtimeWorkbenchRecentRunLimit = 10

type runtimeWorkbenchSnapshot struct {
	runtimeContractID     string
	runtimeContractDigest string
	activeNodeCount       int32
	activeSessionCount    int32
	readySessionCount     int32
	workerFeatures        []string
	drainingSessionCount  int32
	totalCapacity         int32
	totalInflight         int32
	pendingRunCount       int32
	retryWaitRunCount     int32
	offeredRunCount       int32
	executingRunCount     int32
	websocketCount        int32
	longPollCount         int32
	transportChangedAt    *time.Time
	latestLongPollReason  *string
	lastSessionActivityAt *time.Time
	lastAssignmentAt      *time.Time
	lastResultAt          *time.Time
}

func (s *Service) GetRuntimeWorkbench(
	ctx context.Context,
	userID, agentID uuid.UUID,
) (*RuntimeWorkbenchResponse, error) {
	agent, err := s.ownerAgent(ctx, userID, agentID)
	if err != nil {
		return nil, err
	}
	if s == nil || s.pool == nil {
		return nil, httpx.ServiceUnavailable("Runtime 工作台暂不可用")
	}

	presences := []coreruntime.RuntimePresence{}
	if s.runtimePresence != nil {
		presences, err = s.runtimePresence.ListByAgent(ctx, agentID)
		if err != nil {
			log.Warn().Err(err).Str("agent_id", agentID.String()).Msg("a2a.GetRuntimeWorkbench: lease-backed presence unavailable")
			presences = nil
		}
	}
	snapshot, err := s.runtimeWorkbenchSnapshot(ctx, agentID, presences)
	if err != nil {
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("a2a.GetRuntimeWorkbench: snapshot")
		return nil, httpx.Internal("查询 Runtime Worker 状态失败")
	}
	recentRuns, err := s.runtimeWorkbenchRecentRuns(ctx, userID, agentID)
	if err != nil {
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("a2a.GetRuntimeWorkbench: recent runs")
		return nil, httpx.Internal("查询 Agent 运行记录失败")
	}

	connectionStatus, availability, callable := runtimeWorkbenchState(agent, snapshot)
	currentTransport := runtimeWorkbenchCurrentTransport(snapshot)
	runtimeState := RuntimeWorkbenchRuntime{
		RuntimeContractID:     snapshot.runtimeContractID,
		RuntimeContractDigest: snapshot.runtimeContractDigest,
		TransportPolicy:       "ws_primary_long_poll_fallback",
		PrimaryTransport:      "websocket",
		FallbackTransport:     "long_poll",
		CurrentTransport:      currentTransport,
		TransportCounts: map[string]int32{
			string(coreruntime.RuntimeTransportWebSocket): snapshot.websocketCount,
			string(coreruntime.RuntimeTransportLongPoll):  snapshot.longPollCount,
		},
		TransportChangedAt:    formatOptionalRuntimeTime(snapshot.transportChangedAt),
		FallbackReason:        runtimeWorkbenchFallbackReason(currentTransport, snapshot.latestLongPollReason),
		ConnectionStatus:      connectionStatus,
		ActiveNodeCount:       snapshot.activeNodeCount,
		ActiveSessionCount:    snapshot.activeSessionCount,
		ReadySessionCount:     snapshot.readySessionCount,
		WorkerFeatures:        append([]string{}, snapshot.workerFeatures...),
		DrainingSessionCount:  snapshot.drainingSessionCount,
		TotalCapacity:         snapshot.totalCapacity,
		TotalInflight:         snapshot.totalInflight,
		PendingRunCount:       snapshot.pendingRunCount,
		RetryWaitRunCount:     snapshot.retryWaitRunCount,
		OfferedRunCount:       snapshot.offeredRunCount,
		ExecutingRunCount:     snapshot.executingRunCount,
		LastSessionActivityAt: formatOptionalRuntimeTime(snapshot.lastSessionActivityAt),
		LastAssignmentAt:      formatOptionalRuntimeTime(snapshot.lastAssignmentAt),
		LastResultAt:          formatOptionalRuntimeTime(snapshot.lastResultAt),
	}

	return &RuntimeWorkbenchResponse{
		Agent: RuntimeWorkbenchAgent{
			ID:                  agent.ID.String(),
			Slug:                agent.Slug,
			Name:                agent.Name,
			ConnectionMode:      agent.ConnectionMode,
			LifecycleStatus:     agent.LifecycleStatus,
			Visibility:          agent.Visibility,
			CertificationStatus: agent.CertificationStatus,
			ReadinessCallable:   callable,
			AvailabilityStatus:  availability,
		},
		Runtime:     runtimeState,
		RecentRuns:  recentRuns,
		Diagnostics: runtimeWorkbenchDiagnostics(agent, snapshot, recentRuns),
	}, nil
}

func (s *Service) runtimeWorkbenchSnapshot(
	ctx context.Context,
	agentID uuid.UUID,
	presences []coreruntime.RuntimePresence,
) (runtimeWorkbenchSnapshot, error) {
	presenceSessionIDs := make([]uuid.UUID, 0, len(presences))
	presenceCoreIDs := make([]uuid.UUID, 0, len(presences))
	presenceNodeIDs := make([]uuid.UUID, 0, len(presences))
	for _, presence := range presences {
		if presence.AgentID != agentID || presence.RuntimeSessionID == uuid.Nil ||
			presence.CoreInstanceID == uuid.Nil || presence.NodeID == uuid.Nil {
			continue
		}
		presenceSessionIDs = append(presenceSessionIDs, presence.RuntimeSessionID)
		presenceCoreIDs = append(presenceCoreIDs, presence.CoreInstanceID)
		presenceNodeIDs = append(presenceNodeIDs, presence.NodeID)
	}
	const statement = `
WITH current_contract AS (
    SELECT runtime_contract_id, runtime_contract_digest
    FROM runtime_schema_contracts
    WHERE is_current
), lease_backed_presence AS (
    SELECT DISTINCT runtime_session_id, core_instance_id, node_id
    FROM unnest($3::uuid[], $4::uuid[], $5::uuid[])
         AS presence(runtime_session_id, core_instance_id, node_id)
), live_sessions AS (
    SELECT s.runtime_session_id, s.node_id, s.agent_id, s.status, s.features,
           s.capacity, s.inflight, s.heartbeat_at,
           attachment.transport, attachment.transport_reason,
           attachment.transport_changed_at,
           n.status AS node_status, n.capacity AS node_capacity,
           n.inflight AS node_inflight
    FROM runtime_sessions s
    JOIN runtime_nodes n ON n.node_id = s.node_id
    JOIN agent_tokens token
      ON token.id = s.credential_id
     AND token.agent_id = s.agent_id
    JOIN runtime_wire_contracts wire
      ON wire.runtime_contract_id = s.runtime_contract_id
     AND wire.runtime_contract_digest = s.runtime_contract_digest
     AND wire.support_tier IN ('current', 'previous')
    JOIN runtime_session_attachments attachment
      ON attachment.runtime_session_id = s.runtime_session_id
     AND attachment.core_instance_id = s.attached_core_instance_id
     AND attachment.detached_at IS NULL
    LEFT JOIN lease_backed_presence presence
      ON presence.runtime_session_id = s.runtime_session_id
     AND presence.core_instance_id = s.attached_core_instance_id
     AND presence.node_id = s.node_id
    WHERE s.agent_id = $1
      AND s.status IN ('active', 'draining')
      AND s.attached_core_instance_id IS NOT NULL
      AND s.disconnected_at IS NULL
      AND (presence.runtime_session_id IS NOT NULL OR
           s.heartbeat_at >= clock_timestamp() - ($2::bigint * INTERVAL '1 millisecond'))
      AND s.protocol_version = 2
      AND s.runtime_contract_id = 'openlinker.runtime.v2'
      AND n.status IN ('active', 'draining')
      AND n.revoked_at IS NULL
      AND n.protocol_version = s.protocol_version
      AND n.runtime_contract_id = s.runtime_contract_id
      AND n.runtime_contract_digest = s.runtime_contract_digest
      AND n.device_certificate_serial = s.device_certificate_serial
      AND n.node_version = s.node_version
      AND n.features @> s.features
      AND s.features @> n.features
      AND (presence.runtime_session_id IS NOT NULL OR
           n.last_seen_at >= clock_timestamp() - ($2::bigint * INTERVAL '1 millisecond'))
      AND token.status = 'active_runtime'
      AND token.revoked_at IS NULL
      AND token.scopes @> ARRAY['agent:pull']::text[]
      AND (token.expires_at IS NULL OR token.expires_at > clock_timestamp())
), live_nodes AS (
    SELECT node_id, MAX(node_capacity)::int AS capacity,
           MAX(node_inflight)::int AS inflight
    FROM live_sessions
    GROUP BY node_id
)
SELECT contract.runtime_contract_id,
       contract.runtime_contract_digest,
       (SELECT COUNT(*)::int FROM live_nodes),
       (SELECT COUNT(*)::int FROM live_sessions),
       (SELECT COUNT(*)::int FROM live_sessions
        WHERE status = 'active' AND node_status = 'active'),
       COALESCE((SELECT features FROM live_sessions
        WHERE status = 'active' AND node_status = 'active'
        ORDER BY runtime_session_id LIMIT 1), ARRAY[]::text[]),
       (SELECT COUNT(*)::int FROM live_sessions
        WHERE status = 'draining' OR node_status = 'draining'),
       COALESCE((SELECT SUM(capacity)::int FROM live_nodes), 0),
       COALESCE((SELECT SUM(inflight)::int FROM live_nodes), 0),
       (SELECT COUNT(*)::int FROM runs
        WHERE agent_id = $1 AND status = 'running' AND dispatch_state = 'pending'),
       (SELECT COUNT(*)::int FROM runs
        WHERE agent_id = $1 AND status = 'running' AND dispatch_state = 'retry_wait'),
       (SELECT COUNT(*)::int FROM runs
        WHERE agent_id = $1 AND status = 'running' AND dispatch_state = 'offered'),
       (SELECT COUNT(*)::int FROM runs
        WHERE agent_id = $1 AND status = 'running' AND dispatch_state = 'executing'),
       (SELECT COUNT(*)::int FROM live_sessions WHERE transport = 'websocket'),
       (SELECT COUNT(*)::int FROM live_sessions WHERE transport = 'long_poll'),
       (SELECT MAX(transport_changed_at) FROM live_sessions),
       (SELECT transport_reason
        FROM live_sessions
        WHERE transport = 'long_poll'
        ORDER BY transport_changed_at DESC, runtime_session_id DESC
        LIMIT 1),
       (SELECT MAX(heartbeat_at) FROM live_sessions),
       (SELECT MAX(attempt.accepted_at)
        FROM run_attempts attempt WHERE attempt.agent_id = $1),
       (SELECT MAX(COALESCE(attempt.result_acknowledged_at, attempt.finished_at))
        FROM run_attempts attempt WHERE attempt.agent_id = $1)
FROM current_contract contract`

	var snapshot runtimeWorkbenchSnapshot
	err := s.pool.QueryRow(
		ctx,
		statement,
		agentID,
		coreruntime.CurrentRuntimeLivenessPolicy().SessionStaleAfter.Milliseconds(),
		presenceSessionIDs,
		presenceCoreIDs,
		presenceNodeIDs,
	).Scan(
		&snapshot.runtimeContractID,
		&snapshot.runtimeContractDigest,
		&snapshot.activeNodeCount,
		&snapshot.activeSessionCount,
		&snapshot.readySessionCount,
		&snapshot.workerFeatures,
		&snapshot.drainingSessionCount,
		&snapshot.totalCapacity,
		&snapshot.totalInflight,
		&snapshot.pendingRunCount,
		&snapshot.retryWaitRunCount,
		&snapshot.offeredRunCount,
		&snapshot.executingRunCount,
		&snapshot.websocketCount,
		&snapshot.longPollCount,
		&snapshot.transportChangedAt,
		&snapshot.latestLongPollReason,
		&snapshot.lastSessionActivityAt,
		&snapshot.lastAssignmentAt,
		&snapshot.lastResultAt,
	)
	return snapshot, err
}

func (s *Service) runtimeWorkbenchRecentRuns(
	ctx context.Context,
	userID, agentID uuid.UUID,
) ([]RuntimeWorkbenchRun, error) {
	rows, err := s.pool.Query(ctx, `
SELECT r.id, r.status, r.dispatch_state, r.attempt_count, r.max_attempts,
       r.next_attempt_at, r.source, r.started_at, r.finished_at,
       latest.id, latest.state, latest.accepted_at, latest.finished_at,
       r.error_code, r.error_message
FROM runs r
JOIN agents agent ON agent.id = r.agent_id
LEFT JOIN LATERAL (
    SELECT attempt.id,
           CASE
               WHEN attempt.finished_at IS NOT NULL THEN 'finished'
               WHEN attempt.accepted_at IS NOT NULL THEN 'executing'
               ELSE 'offered'
           END::text AS state,
           attempt.accepted_at,
           attempt.finished_at
    FROM run_attempts attempt
    WHERE attempt.run_id = r.id
    ORDER BY attempt.offer_no DESC, attempt.id DESC
    LIMIT 1
) latest ON TRUE
WHERE agent.creator_id = $1
  AND r.agent_id = $2
ORDER BY r.started_at DESC, r.id DESC
LIMIT $3`, userID, agentID, runtimeWorkbenchRecentRunLimit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]RuntimeWorkbenchRun, 0, runtimeWorkbenchRecentRunLimit)
	for rows.Next() {
		var runID uuid.UUID
		var status, dispatchState, source string
		var attemptCount, maxAttempts int32
		var nextAttemptAt, finishedAt, acceptedAt, attemptEndedAt *time.Time
		var startedAt time.Time
		var latestAttemptID *uuid.UUID
		var latestAttemptState, errorCode, errorMessage *string
		if err = rows.Scan(
			&runID, &status, &dispatchState, &attemptCount, &maxAttempts,
			&nextAttemptAt, &source, &startedAt, &finishedAt,
			&latestAttemptID, &latestAttemptState, &acceptedAt, &attemptEndedAt,
			&errorCode, &errorMessage,
		); err != nil {
			return nil, err
		}
		item := RuntimeWorkbenchRun{
			RunID:                runID.String(),
			Status:               status,
			DispatchState:        dispatchState,
			AttemptCount:         attemptCount,
			MaxAttempts:          maxAttempts,
			NextAttemptAt:        formatOptionalRuntimeTime(nextAttemptAt),
			Source:               source,
			StartedAt:            startedAt.UTC().Format(time.RFC3339),
			FinishedAt:           formatOptionalRuntimeTime(finishedAt),
			LatestAttemptState:   latestAttemptState,
			LastAssignmentAt:     formatOptionalRuntimeTime(acceptedAt),
			LatestAttemptEndedAt: formatOptionalRuntimeTime(attemptEndedAt),
			ErrorCode:            errorCode,
			ErrorMessage:         errorMessage,
			DetailURL:            "/run/" + runID.String(),
		}
		if latestAttemptID != nil {
			value := latestAttemptID.String()
			item.LatestAttemptID = &value
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func runtimeWorkbenchState(
	agent db.Agent,
	snapshot runtimeWorkbenchSnapshot,
) (connectionStatus, availability string, callable bool) {
	if !isQueuedRuntimeConnectionMode(agent.ConnectionMode) {
		return "not_applicable", "not_applicable", agent.LifecycleStatus == "active"
	}
	if agent.LifecycleStatus != "active" {
		return "offline", "disabled", false
	}
	if snapshot.readySessionCount > 0 {
		return "online", "healthy", true
	}
	if snapshot.activeSessionCount > 0 {
		return "draining", "degraded", false
	}
	return "offline", "offline", false
}

func runtimeWorkbenchDiagnostics(
	agent db.Agent,
	snapshot runtimeWorkbenchSnapshot,
	runs []RuntimeWorkbenchRun,
) []RuntimeWorkbenchDiagnostic {
	if !isQueuedRuntimeConnectionMode(agent.ConnectionMode) {
		return []RuntimeWorkbenchDiagnostic{{
			Code:            "runtime_not_applicable",
			Severity:        "info",
			Summary:         "这个 Agent 不通过 Runtime Worker 运行，请在接入设置里检查它自己的 Endpoint 或 MCP 服务。",
			TechnicalDetail: "connection_mode=" + agent.ConnectionMode + "; Runtime Worker Session inventory is not applicable.",
			NextAction:      "check_endpoint",
		}}
	}

	diagnostics := make([]RuntimeWorkbenchDiagnostic, 0, 4)
	if agent.LifecycleStatus != "active" {
		diagnostics = append(diagnostics, RuntimeWorkbenchDiagnostic{
			Code:            "agent_disabled",
			Severity:        "warning",
			Summary:         "这个 Agent 已停用，不会接收新的运行。",
			TechnicalDetail: "Agent lifecycle_status is not active; Runtime dispatch is blocked.",
			NextAction:      "enable_agent",
		})
	}
	if snapshot.activeSessionCount == 0 {
		diagnostics = append(diagnostics, RuntimeWorkbenchDiagnostic{
			Code:     "runtime_session_offline",
			Severity: "warning",
			Summary:  "Runtime Worker 还没连上。启动 Runtime Worker 后会优先使用 WebSocket；网络不合适时会自动切到长轮询。",
			TechnicalDetail: fmt.Sprintf(
				"No live supported-contract Runtime Session from lease-backed presence or the %d-second database-clock fallback window; transport_policy=ws_primary_long_poll_fallback.",
				int(coreruntime.CurrentRuntimeLivenessPolicy().SessionStaleAfter/time.Second),
			),
			NextAction: "start_runtime",
		})
	} else if snapshot.readySessionCount == 0 {
		diagnostics = append(diagnostics, RuntimeWorkbenchDiagnostic{
			Code:            "runtime_sessions_draining",
			Severity:        "warning",
			Summary:         "现有 Runtime Worker 正在排空，会完成手上的运行，但不会再接新运行。",
			TechnicalDetail: "All live Runtime Sessions or their Nodes are draining; new assignment offers are disabled.",
			NextAction:      "restore_runtime_capacity",
		})
	}
	backlog := snapshot.pendingRunCount + snapshot.retryWaitRunCount
	if backlog > 0 && snapshot.readySessionCount == 0 {
		diagnostics = append(diagnostics, RuntimeWorkbenchDiagnostic{
			Code:            "runtime_backlog_without_capacity",
			Severity:        "error",
			Summary:         "有运行在等可用的 Runtime Worker。先恢复 Node，再处理积压。",
			TechnicalDetail: "pending+retry_wait backlog exists while ready_session_count=0; transport switching does not change assignment semantics.",
			NextAction:      "restore_runtime_capacity",
		})
	}
	for _, run := range runs {
		if run.ErrorCode == nil {
			continue
		}
		switch *run.ErrorCode {
		case "RUNTIME_DISPATCH_TIMEOUT":
			diagnostics = append(diagnostics, RuntimeWorkbenchDiagnostic{
				Code:            "recent_dispatch_timeout",
				Severity:        "error",
				Summary:         "最近有运行没能及时交给 Runtime Worker。请检查连接和可用容量。",
				TechnicalDetail: "Latest Run reached RUNTIME_DISPATCH_TIMEOUT before a confirmed Runtime assignment.",
				NextAction:      "inspect_runtime_capacity",
			})
		case "RUNTIME_RETRY_EXHAUSTED":
			diagnostics = append(diagnostics, RuntimeWorkbenchDiagnostic{
				Code:            "recent_retry_exhausted",
				Severity:        "error",
				Summary:         "最近有运行重试后仍未完成，已经进入死信。",
				TechnicalDetail: "Latest Run exhausted its fenced Runtime attempt budget and entered dispatch_state=dead_letter.",
				NextAction:      "inspect_dead_letter",
			})
		}
		break
	}
	if len(diagnostics) == 0 {
		diagnostics = append(diagnostics, RuntimeWorkbenchDiagnostic{
			Code:            "runtime_ready",
			Severity:        "success",
			Summary:         "Runtime Worker 已连接，可以接收运行。默认使用 WebSocket，网络受限时由长轮询接续。",
			TechnicalDetail: "A supported-contract Runtime Session is ready; both transports share mTLS, assignment ACK, lease fencing, resume, Event/Result ACK, and persistent spool semantics.",
			NextAction:      "none",
		})
	}
	return diagnostics
}

func runtimeWorkbenchCurrentTransport(snapshot runtimeWorkbenchSnapshot) string {
	switch {
	case snapshot.websocketCount > 0 && snapshot.longPollCount > 0:
		return "mixed"
	case snapshot.websocketCount > 0:
		return string(coreruntime.RuntimeTransportWebSocket)
	case snapshot.longPollCount > 0:
		return string(coreruntime.RuntimeTransportLongPoll)
	default:
		return string(coreruntime.RuntimeTransportUnknown)
	}
}

func runtimeWorkbenchFallbackReason(currentTransport string, reason *string) *string {
	if reason == nil || (currentTransport != string(coreruntime.RuntimeTransportLongPoll) && currentTransport != "mixed") {
		return nil
	}
	switch coreruntime.RuntimeTransportReason(*reason) {
	case coreruntime.RuntimeTransportReasonWebSocketUnavailable,
		coreruntime.RuntimeTransportReasonPolicyForced:
		value := *reason
		return &value
	default:
		return nil
	}
}

func formatOptionalRuntimeTime(value *time.Time) *string {
	if value == nil {
		return nil
	}
	formatted := value.UTC().Format(time.RFC3339)
	return &formatted
}
