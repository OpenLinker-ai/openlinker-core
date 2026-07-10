package runtime

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	"github.com/OpenLinker-ai/openlinker-core/pkg/credential"
	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/endpointurl"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

// errMsgMaxLen 错误消息截断长度，避免巨大 body 灌进 DB / 响应。
const errMsgMaxLen = 500

const defaultRunEventsLimit int32 = 100
const maxRunEventsLimit int32 = 500
const maxAgentResponseEvents = 50
const maxAgentResponseBodyBytes = 4 << 20
const taskCallbackSecretByteLen = 32
const maxRunMessageContentLen = 10000
const runtimePullClaimTTL = 5 * time.Minute
const runtimePullEmptyClaimRetryAfter = 5 * time.Second
const runtimePullHeartbeatInterval = 60 * time.Second
const runtimePullMaxLongPollWait = 30 * time.Second
const runtimePullLongPollTick = 5 * time.Second
const runtimePullResponseContextTimeout = 2 * time.Second
const runtimeBestEffortWriteTimeout = 10 * time.Second
const runtimeBestEffortWriteConcurrency = 32
const runtimeTokenTouchMinInterval = 30 * time.Second
const maxA2AContextIDLen = 200
const maxConversationHistoryRuns int32 = 50
const maxConversationHistoryMessages = 120

const (
	connectionModeDirectHTTP  = "direct_http"
	connectionModeMCPServer   = "mcp_server"
	connectionModeRuntimePull = "runtime_pull"
	connectionModeRuntimeWS   = "runtime_ws"

	runtimeTokenPrefixLen = credential.PrefixLen
)

var allowedAgentResponseEventTypes = map[string]struct{}{
	"run.message.delta":  {},
	"run.status.changed": {},
	"run.artifact.delta": {},
}

// WebhookEnqueuer 触发 run 完成后向创作者推送 webhook。
//
// 用接口注入避免 runtime → webhook 的硬依赖（webhook 本身依赖 db.Run）。
// 实现见 internal/webhook.Service.EnqueueDelivery。
type WebhookEnqueuer interface {
	EnqueueDelivery(ctx context.Context, run *db.Run, agentSlug string, output map[string]interface{}) error
}

// TaskCallbackEnqueuer 触发 task callback，payload 来自 run_events。
type TaskCallbackEnqueuer interface {
	EnqueueRunEvent(ctx context.Context, event db.RunEvent) error
}

// DeliveryEnqueuer 触发 run 完成后向用户的默认投递目标发送。
//
// 同 WebhookEnqueuer 用接口注入避免 runtime → delivery 硬依赖。
// 实现见 internal/delivery.Service.EnqueueIfDefault。
type DeliveryEnqueuer interface {
	EnqueueIfDefault(ctx context.Context, run *db.Run) error
}

// Service 调用 Agent，并维护 core-owned run state, events, artifacts and availability.
//
// 关键时序约束（见 docs/13 模块 4 / docs/10 章四）：
//  1. 事务 A：INSERT runs(status=running)
//  2. 事务外：HTTP POST 创作者 endpoint（60s 超时）
//  3. 事务 B：成功 → MarkRunSuccess；失败 → MarkRunFailed
//     成功统计、availability、artifact 和事件写入不得把已完成的 run 回滚。
//  4. 事务外：异步触发 webhook 投递（不阻塞响应）
//
// HTTP 调用必须在事务外，否则会长时间占用数据库事务。
type Service struct {
	queries         *db.Queries
	requirements    runRequirementQueries
	pool            *pgxpool.Pool
	cfg             *config.Config
	httpClient      *http.Client
	webhookSvc      WebhookEnqueuer
	taskCallbackSvc TaskCallbackEnqueuer
	deliverySvc     DeliveryEnqueuer
	wsHub           *runtimeWSHub
	pullNotifier    *runtimePullNotifier
	bestEffortDBSem chan struct{}
	tokenTouchMu    sync.Mutex
	tokenTouchAt    map[uuid.UUID]time.Time
}

type runInvocation struct {
	runID                uuid.UUID
	userID               uuid.UUID
	agent                db.Agent
	req                  *RunRequest
	taskCallback         *RunTaskCallbackResponse
	delegation           *Delegation
	runtimePullAvailable bool
}

type createRunOptions struct {
	delegation                   *Delegation
	allowOfflineRuntimePullQueue bool
}

type preparedTaskCallbackSubscription struct {
	targetURL       string
	secret          string
	eventTypes      []string
	authScheme      *string
	authCredentials *string
	metadata        []byte
}

// Delegation describes an Agent-mediated child run executed within an active parent run.
type Delegation struct {
	ParentRunID   uuid.UUID
	CallerAgentID uuid.UUID
	Reason        string
}

// RuntimePullClaimOptions lets newer workers avoid tight empty polling while
// keeping the existing GET /claim behavior compatible for older workers.
type RuntimePullClaimOptions struct {
	Wait time.Duration
}

// RuntimePullRunTimeoutConfig controls how long runtime_pull runs may stay
// pending or claimed before the platform converts them to a terminal timeout.
type RuntimePullRunTimeoutConfig struct {
	DispatchTimeout time.Duration
	ResultTimeout   time.Duration
	BatchSize       int32
}

type runtimeHeartbeatOptions struct {
	asyncTokenTouch bool
}

// EndpointRunTimeoutConfig controls how long direct_http / mcp_server runs may
// stay running before the platform converts them to terminal timeout.
type EndpointRunTimeoutConfig struct {
	StaleAfter time.Duration
	BatchSize  int32
}

// SetWebhookEnqueuer 注入 webhook 触发器（main.go 启动时调用）。
//
// 用 setter 而非 NewService 参数，避免 runtime ↔ webhook 循环依赖
// （webhook 内部要 import runtime 也不行；用接口隔离）。
func (s *Service) SetWebhookEnqueuer(w WebhookEnqueuer) {
	s.webhookSvc = w
}

// SetTaskCallbackEnqueuer 注入 task callback 触发器。
func (s *Service) SetTaskCallbackEnqueuer(w TaskCallbackEnqueuer) {
	s.taskCallbackSvc = w
}

// SetDeliveryEnqueuer 注入用户侧投递触发器（main.go 启动时调用）。
func (s *Service) SetDeliveryEnqueuer(d DeliveryEnqueuer) {
	s.deliverySvc = d
}

// NewService 构造 Service。HTTP client timeout 取自 cfg.RunTimeoutSeconds（默认 60s）。
func NewService(pool *pgxpool.Pool, cfg *config.Config) *Service {
	timeout := time.Duration(cfg.RunTimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	queries := db.New(pool)
	return &Service{
		queries:      queries,
		requirements: queries,
		pool:         pool,
		cfg:          cfg,
		wsHub:        newRuntimeWSHub(),
		pullNotifier: newRuntimePullNotifier(),
		bestEffortDBSem: make(
			chan struct{},
			runtimeBestEffortWriteConcurrency,
		),
		tokenTouchAt: map[uuid.UUID]time.Time{},
		httpClient:   endpointurl.NewHTTPClient(timeout, cfg.AllowLocalHTTPEndpoints),
	}
}

func (s *Service) agentA2AContext(runID uuid.UUID, delegation *Delegation) *AgentA2AContext {
	ctx := &AgentA2AContext{
		CurrentRunID:      runID.String(),
		CallAgentEndpoint: s.callAgentEndpointURL(),
		CallAgentMethod:   "POST",
		AgentTokenType:    "ol_agent",
		AgentScopes:       []string{"agent:call"},
	}
	if delegation != nil {
		ctx.ParentRunID = delegation.ParentRunID.String()
		ctx.CallerAgentID = delegation.CallerAgentID.String()
	}
	return ctx
}

func (s *Service) agentA2AContextForRequest(runID uuid.UUID, delegation *Delegation, reqCtx *RunA2AContextRequest) *AgentA2AContext {
	base := s.agentA2AContext(runID, delegation)
	if reqCtx == nil {
		return base
	}
	base.ProtocolContextID = reqCtx.ProtocolContextID
	base.ProtocolTaskID = reqCtx.ProtocolTaskID
	if base.ProtocolTaskID == "" {
		base.ProtocolTaskID = runID.String()
	}
	base.RootContextID = reqCtx.RootContextID
	base.ParentContextID = reqCtx.ParentContextID
	base.ParentTaskID = reqCtx.ParentTaskID
	base.TraceID = reqCtx.TraceID
	base.ReferenceTaskIDs = append([]string(nil), reqCtx.ReferenceTaskIDs...)
	return base
}

func (s *Service) agentA2AContextForRun(ctx context.Context, runID uuid.UUID) *AgentA2AContext {
	base := s.agentA2AContext(runID, nil)
	delegation, err := s.queries.GetRunDelegationByChild(ctx, runID)
	if err == nil {
		base.ParentRunID = delegation.ParentRunID.String()
		base.CallerAgentID = delegation.CallerAgentID.String()
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) && !isContextErr(err) {
		log.Warn().Err(err).Str("run_id", runID.String()).Msg("runtime.agentA2AContextForRun")
	}
	mapping, err := s.queries.GetA2AContextMappingByRun(ctx, runID)
	if err == nil {
		applyA2AContextMappingToAgentContext(base, mapping)
		return base
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) && !isContextErr(err) {
		log.Warn().Err(err).Str("run_id", runID.String()).Msg("runtime.agentA2AContextForRun.mapping")
	}
	return base
}

func (s *Service) conversationContextForRun(ctx context.Context, runID uuid.UUID) *ConversationContext {
	if s == nil || s.queries == nil || s.pool == nil || runID == uuid.Nil {
		return nil
	}
	mapping, err := s.queries.GetA2AContextMappingByRun(ctx, runID)
	if err == nil {
		return s.conversationContextFromMapping(ctx, mapping)
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) && !isContextErr(err) {
		log.Warn().Err(err).Str("run_id", runID.String()).Msg("runtime.conversationContextForRun")
	}
	return nil
}

func (s *Service) conversationContextFromMapping(ctx context.Context, mapping db.A2AContextMapping) *ConversationContext {
	sessionKey := conversationSessionKey(mapping)
	conversation := &ConversationContext{
		ID:                  sessionKey,
		SessionKey:          sessionKey,
		ProtocolContextID:   mapping.ProtocolContextID,
		RootContextID:       mapping.RootContextID,
		CurrentRunID:        mapping.RunID.String(),
		CurrentProtocolTask: mapping.ProtocolTaskID,
		Source:              "core",
	}
	if s == nil || s.queries == nil || sessionKey == "" {
		return conversation
	}
	historyMappings, err := s.queries.ListA2AContextMappingsBeforeRunByRoot(ctx, db.ListA2AContextMappingsBeforeRunByRootParams{
		UserID:        mapping.UserID,
		RootContextID: mapping.RootContextID,
		CreatedAt:     mapping.CreatedAt,
		RunID:         mapping.RunID,
		Limit:         maxConversationHistoryRuns,
	})
	if err != nil {
		log.Warn().Err(err).Str("run_id", mapping.RunID.String()).Msg("runtime.conversationContextFromMapping.list")
		conversation.Truncated = true
		return conversation
	}
	if int32(len(historyMappings)) >= maxConversationHistoryRuns {
		conversation.Truncated = true
	}
	historyRunIDs := make([]uuid.UUID, 0, len(historyMappings))
	for _, historyMapping := range historyMappings {
		historyRunIDs = append(historyRunIDs, historyMapping.RunID)
	}
	messages, err := s.queries.ListRunMessagesByRuns(ctx, historyRunIDs)
	if err != nil {
		log.Warn().Err(err).Str("run_id", mapping.RunID.String()).Msg("runtime.conversationContextFromMapping.messages")
		conversation.Truncated = true
		return conversation
	}
	messagesByRunID := make(map[uuid.UUID][]db.RunMessage, len(historyRunIDs))
	for _, message := range messages {
		messagesByRunID[message.RunID] = append(messagesByRunID[message.RunID], message)
	}
	for _, historyMapping := range historyMappings {
		messages := messagesByRunID[historyMapping.RunID]
		for _, message := range messages {
			if len(conversation.HistoryBeforeCurrent) >= maxConversationHistoryMessages {
				conversation.Truncated = true
				return conversation
			}
			conversation.HistoryBeforeCurrent = append(conversation.HistoryBeforeCurrent, conversationMessageFromRunMessage(message))
		}
	}
	return conversation
}

func conversationSessionKey(mapping db.A2AContextMapping) string {
	if strings.TrimSpace(mapping.RootContextID) != "" {
		return strings.TrimSpace(mapping.RootContextID)
	}
	if strings.TrimSpace(mapping.ProtocolContextID) != "" {
		return strings.TrimSpace(mapping.ProtocolContextID)
	}
	return mapping.RunID.String()
}

func conversationMessageFromRunMessage(message db.RunMessage) ConversationMessage {
	payload := map[string]interface{}{}
	if len(message.Payload) > 0 {
		if err := json.Unmarshal(message.Payload, &payload); err != nil {
			payload = map[string]interface{}{"raw": string(message.Payload)}
		}
	}
	return ConversationMessage{
		RunID:         message.RunID.String(),
		EventSequence: message.EventSequence,
		Role:          message.Role,
		Content:       message.Content,
		Payload:       payload,
		CreatedAt:     message.CreatedAt.UTC().Format(time.RFC3339Nano),
	}
}

func (s *Service) callAgentEndpointURL() string {
	apiURL := ""
	if s.cfg != nil {
		apiURL = s.cfg.APIURL
	}
	apiBase := strings.TrimRight(strings.TrimSpace(apiURL), "/")
	if apiBase == "" {
		apiBase = "http://localhost:8080"
	}
	return apiBase + "/api/v1/agent-runtime/call-agent"
}

func agentA2AContextMap(ctx *AgentA2AContext) map[string]interface{} {
	if ctx == nil {
		return nil
	}
	value := map[string]interface{}{
		"current_run_id":      ctx.CurrentRunID,
		"call_agent_endpoint": ctx.CallAgentEndpoint,
		"call_agent_method":   ctx.CallAgentMethod,
		"agent_token_type":    ctx.AgentTokenType,
		"agent_scopes":        ctx.AgentScopes,
	}
	if ctx.ParentRunID != "" {
		value["parent_run_id"] = ctx.ParentRunID
	}
	if ctx.CallerAgentID != "" {
		value["caller_agent_id"] = ctx.CallerAgentID
	}
	if ctx.ProtocolContextID != "" {
		value["protocol_context_id"] = ctx.ProtocolContextID
	}
	if ctx.ProtocolTaskID != "" {
		value["protocol_task_id"] = ctx.ProtocolTaskID
	}
	if ctx.RootContextID != "" {
		value["root_context_id"] = ctx.RootContextID
	}
	if ctx.ParentContextID != "" {
		value["parent_context_id"] = ctx.ParentContextID
	}
	if ctx.ParentTaskID != "" {
		value["parent_task_id"] = ctx.ParentTaskID
	}
	if ctx.TraceID != "" {
		value["trace_id"] = ctx.TraceID
	}
	if len(ctx.ReferenceTaskIDs) > 0 {
		value["reference_task_ids"] = ctx.ReferenceTaskIDs
	}
	return value
}

func normalizeRunA2AContextRequest(raw *RunA2AContextRequest, delegation *Delegation, targetAgentID uuid.UUID) (*RunA2AContextRequest, error) {
	if raw == nil {
		return nil, nil
	}
	ctx := &RunA2AContextRequest{
		ProtocolContextID: trimA2AContextField(raw.ProtocolContextID),
		ProtocolTaskID:    trimA2AContextField(raw.ProtocolTaskID),
		RootContextID:     trimA2AContextField(raw.RootContextID),
		ParentContextID:   trimA2AContextField(raw.ParentContextID),
		ParentTaskID:      trimA2AContextField(raw.ParentTaskID),
		ParentRunID:       strings.TrimSpace(raw.ParentRunID),
		CallerAgentID:     strings.TrimSpace(raw.CallerAgentID),
		TargetAgentID:     strings.TrimSpace(raw.TargetAgentID),
		TraceID:           trimA2AContextField(raw.TraceID),
		ReferenceTaskIDs:  normalizeA2AReferenceTaskIDs(raw.ReferenceTaskIDs),
		Source:            strings.TrimSpace(raw.Source),
	}
	if ctx.ProtocolContextID == "" && ctx.RootContextID != "" {
		ctx.ProtocolContextID = ctx.RootContextID
	}
	if ctx.ProtocolContextID == "" {
		return nil, httpx.BadRequest("a2a_context.protocol_context_id 不能为空")
	}
	if ctx.RootContextID == "" {
		ctx.RootContextID = ctx.ProtocolContextID
	}
	if ctx.Source == "" {
		if delegation != nil {
			ctx.Source = "agent_delegation"
		} else {
			ctx.Source = "a2a_protocol"
		}
	}
	if ctx.Source != "a2a_protocol" && ctx.Source != "agent_delegation" {
		return nil, httpx.BadRequest("a2a_context.source 取值非法")
	}
	if delegation != nil {
		if ctx.ParentRunID == "" {
			ctx.ParentRunID = delegation.ParentRunID.String()
		}
		if ctx.CallerAgentID == "" {
			ctx.CallerAgentID = delegation.CallerAgentID.String()
		}
	}
	if targetAgentID != uuid.Nil && ctx.TargetAgentID == "" {
		ctx.TargetAgentID = targetAgentID.String()
	}
	for _, rawID := range []struct {
		label string
		value string
	}{
		{label: "parent_run_id", value: ctx.ParentRunID},
		{label: "caller_agent_id", value: ctx.CallerAgentID},
		{label: "target_agent_id", value: ctx.TargetAgentID},
	} {
		if rawID.value == "" {
			continue
		}
		if _, err := uuid.Parse(rawID.value); err != nil {
			return nil, httpx.BadRequest("a2a_context." + rawID.label + " 不是合法 UUID")
		}
	}
	return ctx, nil
}

func trimA2AContextField(raw string) string {
	value := strings.TrimSpace(raw)
	runes := []rune(value)
	if len(runes) > maxA2AContextIDLen {
		return string(runes[:maxA2AContextIDLen])
	}
	return value
}

func normalizeA2AReferenceTaskIDs(raw []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		item = trimA2AContextField(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}

func parseOptionalUUID(raw string) *uuid.UUID {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parsed, err := uuid.Parse(raw)
	if err != nil {
		return nil
	}
	return &parsed
}

func copyRunInput(input map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(input)+6)
	for k, v := range input {
		out[k] = v
	}
	return out
}

func attachRunA2AContextToInput(input map[string]interface{}, ctx *RunA2AContextRequest) {
	if input == nil || ctx == nil {
		return
	}
	if ctx.ProtocolContextID != "" {
		input["a2a_context_id"] = ctx.ProtocolContextID
	}
	if ctx.ProtocolTaskID != "" {
		input["a2a_task_id"] = ctx.ProtocolTaskID
	}
	if ctx.RootContextID != "" {
		input["a2a_root_context_id"] = ctx.RootContextID
	}
	if ctx.ParentContextID != "" {
		input["a2a_parent_context_id"] = ctx.ParentContextID
	}
	if ctx.ParentTaskID != "" {
		input["a2a_parent_task_id"] = ctx.ParentTaskID
	}
	if len(ctx.ReferenceTaskIDs) > 0 {
		input["a2a_reference_task_ids"] = ctx.ReferenceTaskIDs
	}
}

func runA2AContextResponseFromRequest(ctx *RunA2AContextRequest) *RunA2AContextResponse {
	if ctx == nil {
		return nil
	}
	return &RunA2AContextResponse{
		ProtocolContextID: ctx.ProtocolContextID,
		ProtocolTaskID:    ctx.ProtocolTaskID,
		RootContextID:     ctx.RootContextID,
		ParentContextID:   ctx.ParentContextID,
		ParentTaskID:      ctx.ParentTaskID,
		ParentRunID:       ctx.ParentRunID,
		CallerAgentID:     ctx.CallerAgentID,
		TargetAgentID:     ctx.TargetAgentID,
		TraceID:           ctx.TraceID,
		ReferenceTaskIDs:  append([]string(nil), ctx.ReferenceTaskIDs...),
		Source:            ctx.Source,
	}
}

func runA2AContextResponseFromMapping(mapping db.A2AContextMapping) *RunA2AContextResponse {
	resp := &RunA2AContextResponse{
		ProtocolContextID: mapping.ProtocolContextID,
		ProtocolTaskID:    mapping.ProtocolTaskID,
		RootContextID:     mapping.RootContextID,
		ParentContextID:   mapping.ParentContextID,
		ParentTaskID:      mapping.ParentTaskID,
		TraceID:           mapping.TraceID,
		ReferenceTaskIDs:  append([]string(nil), mapping.ReferenceTaskIDs...),
		Source:            mapping.Source,
	}
	if mapping.ParentRunID != nil {
		resp.ParentRunID = mapping.ParentRunID.String()
	}
	if mapping.CallerAgentID != nil {
		resp.CallerAgentID = mapping.CallerAgentID.String()
	}
	if mapping.TargetAgentID != nil {
		resp.TargetAgentID = mapping.TargetAgentID.String()
	}
	return resp
}

func applyA2AContextMappingToAgentContext(ctx *AgentA2AContext, mapping db.A2AContextMapping) {
	if ctx == nil {
		return
	}
	ctx.ProtocolContextID = mapping.ProtocolContextID
	ctx.ProtocolTaskID = mapping.ProtocolTaskID
	ctx.RootContextID = mapping.RootContextID
	ctx.ParentContextID = mapping.ParentContextID
	ctx.ParentTaskID = mapping.ParentTaskID
	ctx.TraceID = mapping.TraceID
	ctx.ReferenceTaskIDs = append([]string(nil), mapping.ReferenceTaskIDs...)
	if mapping.ParentRunID != nil && ctx.ParentRunID == "" {
		ctx.ParentRunID = mapping.ParentRunID.String()
	}
	if mapping.CallerAgentID != nil && ctx.CallerAgentID == "" {
		ctx.CallerAgentID = mapping.CallerAgentID.String()
	}
}

// Run 调用 Agent。
//
// 流程见 Service 注释。Core 不执行商业结算；财务字段仅保留为外部兼容记录。
//
// source 标记调用来源：'web' / 'mcp' / 'api'，写入 runs.source 以便 /usage 分类显示。
// 传空字符串时默认 'web'，便于旧调用方零修改。
func (s *Service) Run(ctx context.Context, userID uuid.UUID, req *RunRequest, source string) (*RunResponse, error) {
	invocation, resp, err := s.createRunningRun(ctx, userID, req, source, createRunOptions{})
	if err != nil {
		return nil, err
	}
	if s.isRuntimePull(invocation) {
		s.recordRunEventBestEffort(ctx, invocation.runID, "run.dispatch.pending", map[string]interface{}{
			"connection_mode": invocation.agent.ConnectionMode,
			"agent_id":        invocation.agent.ID.String(),
		})
		s.notifyRuntimePullRun(invocation.agent.ID)
		s.dispatchRuntimeWSRunAsync(ctx, invocation.agent)
		return resp, nil
	}
	return s.executeRun(ctx, invocation), nil
}

// StartRun 创建 running run 并在后台执行；调用方可用 GetRun/ListRunEvents/SSE 查询进度。
func (s *Service) StartRun(ctx context.Context, userID uuid.UUID, req *RunRequest, source string) (*RunResponse, error) {
	invocation, resp, err := s.createRunningRun(ctx, userID, req, source, createRunOptions{
		allowOfflineRuntimePullQueue: true,
	})
	if err != nil {
		return nil, err
	}
	if s.isRuntimePull(invocation) {
		if invocation.runtimePullAvailable {
			s.recordRunEventBestEffort(ctx, invocation.runID, "run.dispatch.pending", map[string]interface{}{
				"connection_mode": invocation.agent.ConnectionMode,
				"agent_id":        invocation.agent.ID.String(),
			})
			s.notifyRuntimePullRun(invocation.agent.ID)
			s.dispatchRuntimeWSRunAsync(ctx, invocation.agent)
		} else {
			resp.NextAction = runtimePullWaitingNextAction(resp.RunID, invocation.agent.ID)
			s.recordRunEventBestEffort(ctx, invocation.runID, "run.dispatch.waiting_runtime", map[string]interface{}{
				"connection_mode":    invocation.agent.ConnectionMode,
				"agent_id":           invocation.agent.ID.String(),
				"reason":             "runtime_offline",
				"recommended_action": "start_worker",
				"next_action":        resp.NextAction,
			})
			s.notifyRuntimePullRun(invocation.agent.ID)
		}
		return resp, nil
	}
	s.executeRunAsync(invocation)
	return resp, nil
}

// RunDelegated lets an authenticated Agent call another Agent through the platform.
// Delegated runs do not create a separate cost record until an explicit,
// user-approved settlement contract exists.
func (s *Service) RunDelegated(ctx context.Context, userID uuid.UUID, delegation Delegation, req *RunRequest) (*RunResponse, error) {
	invocation, resp, err := s.createRunningRun(ctx, userID, req, "api", createRunOptions{
		delegation: &delegation,
	})
	if err != nil {
		return nil, err
	}
	if s.isRuntimePull(invocation) {
		s.recordRunEventBestEffort(ctx, invocation.runID, "run.dispatch.pending", map[string]interface{}{
			"connection_mode": invocation.agent.ConnectionMode,
			"agent_id":        invocation.agent.ID.String(),
		})
		s.notifyRuntimePullRun(invocation.agent.ID)
		s.dispatchRuntimeWSRunAsync(ctx, invocation.agent)
		return resp, nil
	}
	return s.executeRun(ctx, invocation), nil
}

func (s *Service) isRuntimePull(invocation *runInvocation) bool {
	return invocation != nil && isQueuedRuntimeMode(invocation.agent.ConnectionMode)
}

func isQueuedRuntimeMode(mode string) bool {
	return mode == connectionModeRuntimePull || mode == connectionModeRuntimeWS
}

func (s *Service) createRunningRun(
	ctx context.Context,
	userID uuid.UUID,
	req *RunRequest,
	source string,
	opts createRunOptions,
) (*runInvocation, *RunResponse, error) {
	if source == "" {
		source = "web"
	}
	switch source {
	case "web", "mcp", "api":
	default:
		return nil, nil, httpx.BadRequest("source 取值非法")
	}
	taskCallback, err := s.prepareTaskCallbackSubscription(taskCallbackConfigFromRunRequest(req))
	if err != nil {
		return nil, nil, err
	}

	// 1. 校验 agent
	agentID, err := uuid.Parse(req.AgentID)
	if err != nil {
		return nil, nil, httpx.BadRequest("agent_id 不是合法 UUID")
	}

	agent, err := s.queries.GetAgentByID(ctx, agentID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, httpx.NotFound("Agent 不存在")
		}
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("runtime.Run: GetAgentByID")
		return nil, nil, httpx.Internal("查询 Agent 失败")
	}
	if agent.LifecycleStatus != "active" || (agent.Visibility == "private" && agent.CreatorID != userID) {
		return nil, nil, httpx.Forbidden("Agent 未公开或已下架")
	}
	if agent.ConnectionMode == "" {
		agent.ConnectionMode = connectionModeDirectHTTP
	}
	runtimePullAvailable := true
	if !isQueuedRuntimeMode(agent.ConnectionMode) {
		if err := endpointurl.Validate(agent.EndpointURL, s.cfg.AllowLocalHTTPEndpoints); err != nil {
			log.Warn().Err(err).Str("agent_id", agent.ID.String()).Msg("runtime.Run: endpoint policy rejected")
			return nil, nil, httpx.Forbidden("Agent endpoint 当前不可调用")
		}
	} else {
		available, checkErr := s.queries.HasRecentRuntimePullToken(ctx, agent.ID)
		if checkErr != nil {
			log.Error().Err(checkErr).Str("agent_id", agent.ID.String()).Msg("runtime.Run: HasRecentRuntimePullToken")
			return nil, nil, httpx.Internal("检查 Agent runtime 状态失败")
		}
		if !available && !opts.allowOfflineRuntimePullQueue {
			return nil, nil, httpx.Conflict("Agent runtime 当前离线，请稍后再试")
		}
		runtimePullAvailable = available
	}
	if agent.ConnectionMode == connectionModeMCPServer && (agent.MCPToolName == nil || strings.TrimSpace(*agent.MCPToolName) == "") {
		log.Warn().Str("agent_id", agent.ID.String()).Msg("runtime.Run: missing mcp tool")
		return nil, nil, httpx.Forbidden("Agent MCP tool 未配置")
	}
	requirementSnapshot, err := s.buildRunRequirementSnapshot(ctx, userID, agentID, req, source)
	if err != nil {
		return nil, nil, err
	}
	runA2AContext, err := normalizeRunA2AContextRequest(req.A2AContext, opts.delegation, agentID)
	if err != nil {
		return nil, nil, err
	}
	normalizedReq := *req
	normalizedReq.Input = copyRunInput(req.Input)
	normalizedReq.A2AContext = runA2AContext
	attachRunA2AContextToInput(normalizedReq.Input, runA2AContext)
	req = &normalizedReq

	// 2. Core does not perform commercial settlement. Historical financial
	// columns remain in the run schema for compatibility and are always zero.
	const cost, fee, revenue int32 = 0, 0, 0

	// 3. 序列化 input 为 JSONB
	inputJSON, err := json.Marshal(req.Input)
	if err != nil {
		return nil, nil, httpx.BadRequest("input 不是合法 JSON")
	}

	// 4. 事务 A：创建 run
	var runID uuid.UUID
	var taskCallbackResp *RunTaskCallbackResponse
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := s.queries.WithTx(tx)

		run, createErr := q.CreateRun(ctx, db.CreateRunParams{
			UserID:              userID,
			AgentID:             agentID,
			Input:               inputJSON,
			CostCents:           cost,
			PlatformFeeCents:    fee,
			CreatorRevenueCents: revenue,
			Source:              source,
		})
		if createErr != nil {
			return createErr
		}
		runID = run.ID
		if runA2AContext != nil {
			if runA2AContext.ProtocolTaskID == "" {
				runA2AContext.ProtocolTaskID = runID.String()
				attachRunA2AContextToInput(req.Input, runA2AContext)
				updatedInputJSON, marshalErr := json.Marshal(req.Input)
				if marshalErr != nil {
					return marshalErr
				}
				if _, updateErr := tx.Exec(ctx, `UPDATE runs SET input = $2 WHERE id = $1`, runID, updatedInputJSON); updateErr != nil {
					return updateErr
				}
			}
			if _, createErr = q.UpsertA2AContextMapping(ctx, db.UpsertA2AContextMappingParams{
				RunID:             runID,
				UserID:            userID,
				AgentID:           agentID,
				ProtocolContextID: runA2AContext.ProtocolContextID,
				ProtocolTaskID:    runA2AContext.ProtocolTaskID,
				RootContextID:     runA2AContext.RootContextID,
				ParentContextID:   runA2AContext.ParentContextID,
				ParentTaskID:      runA2AContext.ParentTaskID,
				ParentRunID:       parseOptionalUUID(runA2AContext.ParentRunID),
				CallerAgentID:     parseOptionalUUID(runA2AContext.CallerAgentID),
				TargetAgentID:     parseOptionalUUID(runA2AContext.TargetAgentID),
				TraceID:           runA2AContext.TraceID,
				ReferenceTaskIDs:  runA2AContext.ReferenceTaskIDs,
				Source:            runA2AContext.Source,
			}); createErr != nil {
				return createErr
			}
		}
		if taskCallback != nil {
			sub, createErr := q.CreateTaskCallbackSubscription(ctx, db.CreateTaskCallbackSubscriptionParams{
				RunID:           runID,
				OwnerUserID:     userID,
				TargetURL:       taskCallback.targetURL,
				Secret:          taskCallback.secret,
				EventTypes:      taskCallback.eventTypes,
				AuthScheme:      taskCallback.authScheme,
				AuthCredentials: taskCallback.authCredentials,
				Metadata:        taskCallback.metadata,
			})
			if createErr != nil {
				return createErr
			}
			taskCallbackResp = runTaskCallbackResponseFromSubscription(sub, taskCallback.secret)
		}
		var parentRunID *uuid.UUID
		if opts.delegation != nil {
			parentRunID = &opts.delegation.ParentRunID
			if _, createErr = q.CreateRunDelegation(ctx, db.CreateRunDelegationParams{
				ChildRunID:    runID,
				ParentRunID:   opts.delegation.ParentRunID,
				CallerAgentID: opts.delegation.CallerAgentID,
				Reason:        opts.delegation.Reason,
			}); createErr != nil {
				return createErr
			}
			if eventErr := createRunEvent(ctx, q, opts.delegation.ParentRunID, nil, "run.child.created", map[string]interface{}{
				"child_run_id":    runID.String(),
				"caller_agent_id": opts.delegation.CallerAgentID.String(),
				"target_agent_id": agentID.String(),
				"reason":          opts.delegation.Reason,
				"billing_mode":    "free_delegation",
				"a2a_context":     runA2AContextResponseFromRequest(runA2AContext),
			}); eventErr != nil {
				return eventErr
			}
		}
		payload := map[string]interface{}{
			"agent_id":   agentID.String(),
			"user_id":    userID.String(),
			"status":     "running",
			"cost_cents": cost,
		}
		if opts.delegation != nil {
			payload["caller_agent_id"] = opts.delegation.CallerAgentID.String()
			payload["billing_mode"] = "free_delegation"
		}
		if runA2AContext != nil {
			payload["a2a_context"] = runA2AContextResponseFromRequest(runA2AContext)
		}
		if eventErr := createRunEvent(ctx, q, runID, parentRunID, "run.created", payload); eventErr != nil {
			return eventErr
		}
		if eventErr := createRunEvent(ctx, q, runID, parentRunID, "run.started", runStartedEventPayload(agent, userID)); eventErr != nil {
			return eventErr
		}
		if requirementSnapshot != nil {
			evidence, createErr := q.CreateRunRequirementEvidence(ctx, requirementSnapshot.createParams(runID))
			if createErr != nil {
				return createErr
			}
			if eventErr := createRunEvent(ctx, q, runID, parentRunID, runRequirementsSnapshottedEvent, runRequirementEvidencePayload(evidence)); eventErr != nil {
				return eventErr
			}
		}
		if messageErr := createRunMessage(ctx, q, runID, nil, "user", messageContentFromMap(req.Input), req.Input); messageErr != nil {
			return messageErr
		}
		return nil
	})
	if err != nil {
		log.Error().Err(err).Str("user_id", userID.String()).Str("agent_id", agentID.String()).
			Msg("runtime.Run: pre-call tx")
		return nil, nil, httpx.Internal("创建调用记录失败")
	}

	invocation := &runInvocation{
		runID:                runID,
		userID:               userID,
		agent:                agent,
		req:                  req,
		taskCallback:         taskCallbackResp,
		delegation:           opts.delegation,
		runtimePullAvailable: runtimePullAvailable,
	}
	resp := &RunResponse{
		RunID:        runID.String(),
		Status:       "running",
		CostCents:    cost,
		Source:       source,
		A2AContext:   runA2AContextResponseFromRequest(runA2AContext),
		TaskCallback: taskCallbackResp,
	}
	if opts.delegation != nil {
		resp.ParentRunID = opts.delegation.ParentRunID.String()
		resp.CallerAgentID = opts.delegation.CallerAgentID.String()
		resp.BillingMode = "free_delegation"
	}
	s.attachRunRequirementEvidence(ctx, runID, resp)
	decorateNextAction(resp)
	return invocation, resp, nil
}

func runStartedEventPayload(agent db.Agent, userID uuid.UUID) map[string]interface{} {
	connectionMode := strings.TrimSpace(agent.ConnectionMode)
	if connectionMode == "" {
		connectionMode = connectionModeDirectHTTP
	}
	payload := map[string]interface{}{
		"agent_id":        agent.ID.String(),
		"user_id":         userID.String(),
		"status":          "running",
		"connection_mode": connectionMode,
	}
	switch connectionMode {
	case connectionModeMCPServer:
		payload["transport"] = "mcp_server"
		if host := endpointHost(agent.EndpointURL); host != "" {
			payload["endpoint_host"] = host
		}
		if agent.MCPToolName != nil && strings.TrimSpace(*agent.MCPToolName) != "" {
			payload["mcp_tool_name"] = strings.TrimSpace(*agent.MCPToolName)
		}
	case connectionModeRuntimePull:
		payload["transport"] = "runtime_pull"
	case connectionModeRuntimeWS:
		payload["transport"] = "runtime_ws"
	default:
		payload["transport"] = "http_endpoint"
		if host := endpointHost(agent.EndpointURL); host != "" {
			payload["endpoint_host"] = host
		}
	}
	return payload
}

func endpointHost(endpoint string) string {
	u, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil || u == nil || u.Host == "" {
		return ""
	}
	return u.Host
}

func taskCallbackConfigFromRunRequest(req *RunRequest) *TaskCallbackConfig {
	if req == nil {
		return nil
	}
	if req.TaskCallback != nil {
		return req.TaskCallback
	}
	if req.PushNotification != nil {
		return req.PushNotification
	}
	if req.PushNotificationAlias != nil {
		return req.PushNotificationAlias
	}
	return req.PushNotificationConfig
}

func (s *Service) prepareTaskCallbackSubscription(cfg *TaskCallbackConfig) (*preparedTaskCallbackSubscription, error) {
	if cfg == nil {
		return nil, nil
	}
	targetURL := strings.TrimSpace(cfg.URL)
	if targetURL == "" {
		return nil, httpx.BadRequest("task_callback.url 不能为空")
	}
	allowLocalHTTP := s.cfg != nil && s.cfg.AllowLocalHTTPEndpoints
	if err := endpointurl.Validate(targetURL, allowLocalHTTP); err != nil {
		return nil, httpx.BadRequest("task_callback.url 必须是 HTTPS；本地开发需开启 ALLOW_LOCAL_HTTP_ENDPOINTS 后才允许 loopback HTTP")
	}
	eventTypes, err := normalizeRunTaskCallbackEventTypes(taskCallbackEventTypesFromRunConfig(cfg))
	if err != nil {
		return nil, err
	}
	metadataMap := cfg.Metadata
	if metadataMap == nil {
		metadataMap = map[string]interface{}{}
	}
	metadata, err := json.Marshal(metadataMap)
	if err != nil {
		return nil, httpx.BadRequest("task_callback.metadata 格式错误")
	}
	secret := strings.TrimSpace(cfg.Secret)
	if secret == "" {
		generated, err := generateRunTaskCallbackSecret()
		if err != nil {
			log.Error().Err(err).Msg("runtime.prepareTaskCallbackSubscription: generate secret")
			return nil, httpx.Internal("生成 task callback secret 失败")
		}
		secret = generated
	}
	authScheme, authCredentials := callbackAuthFromRunConfig(cfg)
	return &preparedTaskCallbackSubscription{
		targetURL:       targetURL,
		secret:          secret,
		eventTypes:      eventTypes,
		authScheme:      stringPtrOrNil(authScheme),
		authCredentials: stringPtrOrNil(authCredentials),
		metadata:        metadata,
	}, nil
}

func taskCallbackEventTypesFromRunConfig(cfg *TaskCallbackConfig) []string {
	if cfg == nil {
		return nil
	}
	if len(cfg.EventTypes) > 0 {
		return cfg.EventTypes
	}
	return cfg.EventTypesAlias
}

func normalizeRunTaskCallbackEventTypes(raw []string) ([]string, error) {
	if len(raw) == 0 {
		return []string{"run.completed", "run.failed", "run.canceled"}, nil
	}
	allowed := map[string]struct{}{
		"run.created":                  {},
		"run.started":                  {},
		"run.dispatch.pending":         {},
		"run.dispatch.claimed":         {},
		"run.requirements.snapshotted": {},
		"run.message.delta":            {},
		"run.artifact.delta":           {},
		"run.status.changed":           {},
		"run.child.created":            {},
		"run.child.completed":          {},
		"run.completed":                {},
		"run.failed":                   {},
		"run.canceled":                 {},
	}
	out := make([]string, 0, len(raw))
	seen := make(map[string]struct{}, len(raw))
	for _, eventType := range raw {
		eventType = strings.TrimSpace(eventType)
		if eventType == "" {
			continue
		}
		if _, ok := allowed[eventType]; !ok {
			return nil, httpx.BadRequest("task_callback.event_types 包含不支持的事件类型")
		}
		if _, ok := seen[eventType]; ok {
			continue
		}
		seen[eventType] = struct{}{}
		out = append(out, eventType)
	}
	if len(out) == 0 {
		return nil, httpx.BadRequest("task_callback.event_types 至少包含一个事件类型")
	}
	return out, nil
}

func callbackAuthFromRunConfig(cfg *TaskCallbackConfig) (string, string) {
	if cfg == nil {
		return "", ""
	}
	if cfg.Authentication != nil {
		scheme := strings.TrimSpace(cfg.Authentication.Scheme)
		credentials := strings.TrimSpace(cfg.Authentication.Credentials)
		if scheme != "" && credentials != "" {
			return scheme, credentials
		}
	}
	if strings.TrimSpace(cfg.Token) != "" {
		return "Bearer", strings.TrimSpace(cfg.Token)
	}
	return "", ""
}

func generateRunTaskCallbackSecret() (string, error) {
	buf := make([]byte, taskCallbackSecretByteLen)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func runTaskCallbackResponseFromSubscription(sub db.TaskCallbackSubscription, secret string) *RunTaskCallbackResponse {
	resp := &RunTaskCallbackResponse{
		ID:                  sub.ID.String(),
		RunID:               sub.RunID.String(),
		TargetURL:           sub.TargetURL,
		EventTypes:          sub.EventTypes,
		AuthScheme:          stringPtrValue(sub.AuthScheme),
		Status:              sub.Status,
		ConsecutiveFailures: sub.ConsecutiveFailures,
		Secret:              secret,
		CreatedAt:           sub.CreatedAt.Format(time.RFC3339),
		UpdatedAt:           sub.UpdatedAt.Format(time.RFC3339),
	}
	return resp
}

func (s *Service) executeRunAsync(invocation *runInvocation) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), s.asyncRunTimeout())
		defer cancel()
		_ = s.executeRun(ctx, invocation)
	}()
}

func (s *Service) asyncRunTimeout() time.Duration {
	timeout := time.Duration(s.cfg.RunTimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	return timeout + 30*time.Second
}

func (s *Service) executeRun(ctx context.Context, invocation *runInvocation) *RunResponse {
	// HTTP 调用（事务外，最长 cfg.RunTimeoutSeconds）
	started := time.Now()
	output, agentEvents, agentErr, callErr := s.callAgent(
		ctx,
		&invocation.agent,
		invocation.runID,
		invocation.userID,
		invocation.req,
		invocation.delegation,
	)
	duration := clampDurationMillisToInt32(time.Since(started))

	// 处理结果
	var resp *RunResponse
	triggerExternalDelivery := invocation.delegation == nil
	if callErr != nil || agentErr != nil {
		resp = s.handleFailure(ctx, invocation.runID, invocation.agent.ID, duration, callErr, agentErr, triggerExternalDelivery)
	} else {
		resp = s.handleSuccess(
			ctx,
			invocation.runID,
			invocation.agent.ID,
			output,
			agentEvents,
			duration,
			triggerExternalDelivery,
		)
	}
	if invocation.delegation != nil {
		resp.ParentRunID = invocation.delegation.ParentRunID.String()
		resp.CallerAgentID = invocation.delegation.CallerAgentID.String()
		resp.BillingMode = "free_delegation"
		s.recordRunEventBestEffort(ctx, invocation.delegation.ParentRunID, "run.child.completed", map[string]interface{}{
			"child_run_id":    invocation.runID.String(),
			"caller_agent_id": invocation.delegation.CallerAgentID.String(),
			"target_agent_id": invocation.agent.ID.String(),
			"status":          resp.Status,
		})
		decorateNextAction(resp)
	}
	if invocation.taskCallback != nil {
		resp.TaskCallback = invocation.taskCallback
	}
	s.attachRunRequirementEvidence(ctx, invocation.runID, resp)
	return resp
}

func (s *Service) callAgent(
	ctx context.Context, agent *db.Agent, runID, userID uuid.UUID, req *RunRequest, delegation *Delegation,
) (map[string]interface{}, []AgentEvent, *AgentError, error) {
	switch agent.ConnectionMode {
	case "", connectionModeDirectHTTP:
		return s.callAgentEndpoint(ctx, agent, runID, userID, req, delegation)
	case connectionModeMCPServer:
		return s.callMCPServer(ctx, agent, runID, userID, req, delegation)
	case connectionModeRuntimePull:
		return nil, nil, nil, errors.New("runtime_pull run must be claimed by agent runtime")
	case connectionModeRuntimeWS:
		return nil, nil, nil, errors.New("runtime_ws run must be assigned over agent runtime websocket or claimed by fallback long-poll")
	default:
		return nil, nil, &AgentError{Code: "UNSUPPORTED_CONNECTION_MODE", Message: "Agent connection_mode 不支持"}, nil
	}
}

// GetRun 查单条调用详情；调用者本人和被调用 Agent 创作者可看。
func (s *Service) GetRun(ctx context.Context, userID, runID uuid.UUID) (*RunResponse, error) {
	r, err := s.queries.GetRunByID(ctx, runID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("调用记录不存在")
		}
		log.Error().Err(err).Str("run_id", runID.String()).Msg("runtime.GetRun: GetRunByID")
		return nil, httpx.Internal("查询调用记录失败")
	}
	agent, agentErr := s.queries.GetAgentByID(ctx, r.AgentID)
	if agentErr != nil && !errors.Is(agentErr, pgx.ErrNoRows) {
		log.Error().Err(agentErr).Str("agent_id", r.AgentID.String()).Msg("runtime.GetRun: GetAgentByID")
		return nil, httpx.Internal("查询调用记录失败")
	}
	if r.UserID != userID && (agentErr != nil || agent.CreatorID != userID) {
		// 不暴露存在性，统一 404
		return nil, httpx.NotFound("调用记录不存在")
	}
	resp := runToResponse(&r)
	if agentErr == nil {
		resp.AgentSlug = agent.Slug
		resp.AgentName = agent.Name
		resp.AgentConnectionMode = agent.ConnectionMode
	} else {
		s.attachRunAgentSummary(ctx, r.AgentID, resp)
	}
	s.attachRunA2AContext(ctx, runID, resp)
	s.attachRunRequirementEvidence(ctx, runID, resp)
	s.attachRunEvidenceSummary(ctx, runID, resp)
	delegation, err := s.queries.GetRunDelegationByChild(ctx, runID)
	if err == nil {
		resp.ParentRunID = delegation.ParentRunID.String()
		resp.CallerAgentID = delegation.CallerAgentID.String()
		resp.BillingMode = "free_delegation"
		decorateNextAction(resp)
		return resp, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		log.Error().Err(err).Str("run_id", runID.String()).Msg("runtime.GetRun: GetRunDelegationByChild")
		return nil, httpx.Internal("查询调用关系失败")
	}
	return resp, nil
}

func (s *Service) attachRunA2AContext(ctx context.Context, runID uuid.UUID, resp *RunResponse) {
	if resp == nil {
		return
	}
	mapping, err := s.queries.GetA2AContextMappingByRun(ctx, runID)
	if err == nil {
		resp.A2AContext = runA2AContextResponseFromMapping(mapping)
		return
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		log.Warn().Err(err).Str("run_id", runID.String()).Msg("runtime.attachRunA2AContext")
	}
}

func (s *Service) attachRunAgentSummary(ctx context.Context, agentID uuid.UUID, resp *RunResponse) {
	if resp == nil {
		return
	}
	resp.AgentID = agentID.String()
	agent, err := s.queries.GetAgentByID(ctx, agentID)
	if err != nil {
		log.Warn().Err(err).Str("agent_id", agentID.String()).Msg("runtime.attachRunAgentSummary: GetAgentByID")
		return
	}
	resp.AgentSlug = agent.Slug
	resp.AgentName = agent.Name
	resp.AgentConnectionMode = agent.ConnectionMode
}

func (s *Service) attachRunEvidenceSummary(ctx context.Context, runID uuid.UUID, resp *RunResponse) {
	if resp == nil {
		return
	}
	summary := &RunEvidenceSummary{
		Status:         resp.Status,
		CoverageStatus: "none",
		PublicSafe:     false,
		EvidenceURL:    "/run/" + runID.String(),
	}
	if resp.RequirementEvidence != nil {
		summary.CoverageStatus = resp.RequirementEvidence.CoverageStatus
		summary.MatchedSkillCount = len(resp.RequirementEvidence.MatchedSkillIDs)
		summary.MissingSkillCount = len(resp.RequirementEvidence.MissingSkillIDs) + len(resp.RequirementEvidence.MissingMCPTools)
		summary.UsedMCPToolCount = len(resp.RequirementEvidence.UsedMCPTools)
	}
	if artifacts, err := s.queries.ListRunArtifactsByRun(ctx, runID); err == nil {
		summary.ArtifactCount = len(artifacts)
		for _, artifact := range artifacts {
			if artifact.Visibility == "public_example" {
				summary.PublicSafe = true
				break
			}
		}
	} else {
		log.Warn().Err(err).Str("run_id", runID.String()).Msg("runtime.attachRunEvidenceSummary: ListRunArtifactsByRun")
	}
	if messages, err := s.queries.ListRunMessagesByRun(ctx, runID); err == nil {
		summary.MessageCount = len(messages)
	} else {
		log.Warn().Err(err).Str("run_id", runID.String()).Msg("runtime.attachRunEvidenceSummary: ListRunMessagesByRun")
	}
	resp.EvidenceSummary = summary
}

// CancelRun marks a running run as canceled. Only the run owner can cancel it.
func (s *Service) CancelRun(ctx context.Context, userID, runID uuid.UUID) (*RunResponse, error) {
	const canceledMessage = "run canceled by user"
	var canceled db.Run
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := s.queries.WithTx(tx)
		row, cancelErr := q.CancelRun(ctx, db.CancelRunParams{
			ID:           runID,
			UserID:       userID,
			ErrorMessage: canceledMessage,
		})
		if cancelErr != nil {
			return cancelErr
		}
		canceled = row
		return nil
	})
	if errors.Is(err, pgx.ErrNoRows) {
		existing, lookupErr := s.queries.GetRunByID(ctx, runID)
		if errors.Is(lookupErr, pgx.ErrNoRows) || (lookupErr == nil && existing.UserID != userID) {
			return nil, httpx.NotFound("调用记录不存在")
		}
		if lookupErr != nil {
			log.Error().Err(lookupErr).Str("run_id", runID.String()).Msg("runtime.CancelRun: GetRunByID")
			return nil, httpx.Internal("查询调用记录失败")
		}
		if existing.Status == "canceled" {
			resp := runToResponse(&existing)
			s.attachRunRequirementEvidence(ctx, runID, resp)
			return resp, nil
		}
		return nil, httpx.Conflict("run 已结束，不能取消")
	}
	if err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Str("user_id", userID.String()).Msg("runtime.CancelRun")
		return nil, httpx.Internal("取消调用失败")
	}

	duration := int32(0)
	if canceled.DurationMs != nil {
		duration = *canceled.DurationMs
	}
	s.recordRunEventBestEffort(ctx, runID, "run.canceled", map[string]interface{}{
		"status":        "canceled",
		"error_code":    "CANCELED",
		"error_message": canceledMessage,
		"duration_ms":   duration,
	})
	if s.shouldTriggerExternalDelivery(ctx, runID) {
		s.triggerWebhookByRun(runID)
		s.triggerDelivery(runID)
	}

	resp := runToResponse(&canceled)
	s.attachRunRequirementEvidence(ctx, runID, resp)
	return resp, nil
}

// ListRunEvents 查询单个 run 的事件流；仅 owner 可看。
func (s *Service) ListRunEvents(ctx context.Context, userID, runID uuid.UUID, afterSequence, limit int32) ([]RunEventResponse, error) {
	r, err := s.queries.GetRunByID(ctx, runID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("调用记录不存在")
		}
		log.Error().Err(err).Str("run_id", runID.String()).Msg("runtime.ListRunEvents: GetRunByID")
		return nil, httpx.Internal("查询调用记录失败")
	}
	if r.UserID != userID {
		return nil, httpx.NotFound("调用记录不存在")
	}

	if afterSequence < 0 {
		return nil, httpx.BadRequest("after_sequence 不能小于 0")
	}
	if limit <= 0 {
		limit = defaultRunEventsLimit
	}
	if limit > maxRunEventsLimit {
		limit = maxRunEventsLimit
	}

	events, err := s.queries.ListRunEventsByRun(ctx, db.ListRunEventsByRunParams{
		RunID:         runID,
		AfterSequence: afterSequence,
		Limit:         limit,
	})
	if err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Msg("runtime.ListRunEvents: ListRunEventsByRun")
		return nil, httpx.Internal("查询调用事件失败")
	}
	resp := make([]RunEventResponse, 0, len(events))
	for _, event := range events {
		resp = append(resp, runEventToResponse(event))
	}
	return resp, nil
}

// ListRunArtifacts returns persisted artifacts for a run. Only the run owner can read them.
func (s *Service) ListRunArtifacts(ctx context.Context, userID, runID uuid.UUID) ([]RunArtifactResponse, error) {
	r, err := s.queries.GetRunByID(ctx, runID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("调用记录不存在")
		}
		log.Error().Err(err).Str("run_id", runID.String()).Msg("runtime.ListRunArtifacts: GetRunByID")
		return nil, httpx.Internal("查询调用记录失败")
	}
	if r.UserID != userID {
		return nil, httpx.NotFound("调用记录不存在")
	}
	artifacts, err := s.queries.ListRunArtifactsByRun(ctx, runID)
	if err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Msg("runtime.ListRunArtifacts: ListRunArtifactsByRun")
		return nil, httpx.Internal("查询运行产物失败")
	}
	resp := make([]RunArtifactResponse, 0, len(artifacts))
	for _, artifact := range artifacts {
		resp = append(resp, runArtifactToResponse(artifact))
	}
	return resp, nil
}

// ListRunMessages returns stable message replay records for a run.
func (s *Service) ListRunMessages(ctx context.Context, userID, runID uuid.UUID) ([]RunMessageResponse, error) {
	r, err := s.queries.GetRunByID(ctx, runID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("调用记录不存在")
		}
		log.Error().Err(err).Str("run_id", runID.String()).Msg("runtime.ListRunMessages: GetRunByID")
		return nil, httpx.Internal("查询调用记录失败")
	}
	if r.UserID != userID {
		return nil, httpx.NotFound("调用记录不存在")
	}
	messages, err := s.queries.ListRunMessagesByRun(ctx, runID)
	if err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Msg("runtime.ListRunMessages: ListRunMessagesByRun")
		return nil, httpx.Internal("查询运行消息失败")
	}
	resp := make([]RunMessageResponse, 0, len(messages))
	for _, message := range messages {
		resp = append(resp, runMessageToResponse(message))
	}
	return resp, nil
}

// ReportRunEvent 允许 Agent endpoint 用 endpoint token 上报当前 run 的中间事件。
func (s *Service) ReportRunEvent(ctx context.Context, runID uuid.UUID, token string, req *ReportRunEventRequest) (*RunEventResponse, error) {
	if req == nil {
		return nil, httpx.BadRequest("请求体不能为空")
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return nil, httpx.Unauthorized("缺少 X-OpenLinker-Token")
	}

	r, err := s.queries.GetRunByID(ctx, runID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("调用记录不存在")
		}
		log.Error().Err(err).Str("run_id", runID.String()).Msg("runtime.ReportRunEvent: GetRunByID")
		return nil, httpx.Internal("查询调用记录失败")
	}

	agent, err := s.queries.GetAgentByID(ctx, r.AgentID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("Agent 不存在")
		}
		log.Error().Err(err).Str("agent_id", r.AgentID.String()).Msg("runtime.ReportRunEvent: GetAgentByID")
		return nil, httpx.Internal("查询 Agent 失败")
	}
	if agent.EndpointAuthHeader == nil || !constantTimeEqual(*agent.EndpointAuthHeader, token) {
		return nil, httpx.Unauthorized("Agent 事件上报 token 无效")
	}
	if r.Status != "running" {
		return nil, httpx.Conflict("run 已结束，不能继续上报事件")
	}

	eventType := strings.TrimSpace(req.EventType)
	if _, ok := allowedAgentResponseEventTypes[eventType]; !ok {
		return nil, httpx.Unprocessable("event_type 不支持")
	}

	payload := req.Payload
	if payload == nil {
		payload = map[string]interface{}{}
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return nil, httpx.BadRequest("payload 不是合法 JSON")
	}
	event, err := s.queries.CreateRunEvent(ctx, db.CreateRunEventParams{
		RunID:     runID,
		EventType: eventType,
		Payload:   payloadJSON,
	})
	if err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Str("event_type", eventType).
			Msg("runtime.ReportRunEvent: CreateRunEvent")
		return nil, httpx.Internal("记录运行事件失败")
	}
	s.triggerTaskCallbackEvent(&event)
	resp := runEventToResponse(event)
	if eventType == "run.message.delta" {
		s.recordRunMessageBestEffort(ctx, runID, &resp.Sequence, "agent", messageContentFromMap(payload), payload)
	}
	if eventType == "run.artifact.delta" {
		s.recordArtifactDeltaBestEffort(ctx, runID, &resp.Sequence, payload)
	}
	return &resp, nil
}

// ClaimRuntimePullRun lets a private / IPv4 Agent actively pull the next pending run.
// runtime_ws Agents may use this as the fallback path after WebSocket loss.
func (s *Service) ClaimRuntimePullRun(ctx context.Context, plaintextToken string, opts ...RuntimePullClaimOptions) (*RuntimePullRunResponse, error) {
	token, err := s.verifyRuntimeToken(ctx, plaintextToken, "agent:pull")
	if err != nil {
		return nil, err
	}
	return s.ClaimRuntimePullRunForToken(ctx, token, opts...)
}

// ClaimRuntimePullRunForToken claims a queued runtime run after the handler or
// WebSocket layer has already verified the runtime token and scope.
func (s *Service) ClaimRuntimePullRunForToken(ctx context.Context, token db.AgentRuntimeToken, opts ...RuntimePullClaimOptions) (*RuntimePullRunResponse, error) {
	connectionMode := strings.TrimSpace(token.ConnectionMode)
	if connectionMode == "" {
		agent, err := s.queries.GetAgentByID(ctx, token.AgentID)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("Agent 不存在")
		}
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			return nil, httpx.Internal("查询 Agent 失败")
		}
		connectionMode = agent.ConnectionMode
	}
	if !isQueuedRuntimeMode(connectionMode) {
		return nil, httpx.Conflict("Agent 不是队列型 runtime 接入模式")
	}
	claimOpts := normalizeRuntimePullClaimOptions(opts...)
	notifyC := (<-chan struct{})(nil)
	if claimOpts.Wait > 0 && s.pullNotifier != nil {
		var unsubscribe func()
		notifyC, unsubscribe = s.pullNotifier.subscribe(token.AgentID)
		defer unsubscribe()
	}
	startedWaiting := time.Now()
	for {
		resp, err := s.claimRuntimePullRunOnce(ctx, token, connectionMode)
		if err != nil || resp != nil || claimOpts.Wait <= 0 || time.Since(startedWaiting) >= claimOpts.Wait {
			return resp, err
		}
		sleepFor := runtimePullLongPollTick
		if remaining := claimOpts.Wait - time.Since(startedWaiting); remaining < sleepFor {
			sleepFor = remaining
		}
		if sleepFor <= 0 {
			return nil, nil
		}
		timer := time.NewTimer(sleepFor)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-notifyC:
			timer.Stop()
		case <-timer.C:
		}
	}
}

func normalizeRuntimePullClaimOptions(opts ...RuntimePullClaimOptions) RuntimePullClaimOptions {
	if len(opts) == 0 {
		return RuntimePullClaimOptions{}
	}
	normalized := opts[0]
	if normalized.Wait < 0 {
		normalized.Wait = 0
	}
	if normalized.Wait > runtimePullMaxLongPollWait {
		normalized.Wait = runtimePullMaxLongPollWait
	}
	return normalized
}

func (s *Service) claimRuntimePullRunOnce(ctx context.Context, token db.AgentRuntimeToken, connectionMode string) (*RuntimePullRunResponse, error) {
	run, err := s.queries.GetClaimedRuntimePullRunByToken(ctx, db.GetClaimedRuntimePullRunByTokenParams{
		AgentID:        token.AgentID,
		RuntimeTokenID: token.ID,
	})
	if err == nil {
		s.touchRuntimeTokenAsync(ctx, token.ID)
		return s.runtimePullRunResponse(ctx, token, run)
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		log.Error().Err(err).Str("agent_id", token.AgentID.String()).Msg("runtime.GetClaimedRuntimePullRunByToken")
		return nil, httpx.Internal("领取任务失败")
	}

	run, err = s.queries.ClaimRuntimePullRun(ctx, db.ClaimRuntimePullRunParams{
		AgentID:        token.AgentID,
		RuntimeTokenID: token.ID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		run, err = s.queries.ClaimStaleRuntimePullRun(ctx, db.ClaimStaleRuntimePullRunParams{
			AgentID:        token.AgentID,
			RuntimeTokenID: token.ID,
			ClaimedBefore:  time.Now().Add(-runtimePullClaimTTL),
		})
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		log.Error().Err(err).Str("agent_id", token.AgentID.String()).Msg("runtime.ClaimRuntimePullRun")
		return nil, httpx.Internal("领取任务失败")
	}
	s.touchRuntimeTokenAsync(ctx, token.ID)
	claimedPayload := map[string]interface{}{
		"agent_id":         token.AgentID.String(),
		"runtime_token_id": token.ID.String(),
	}
	if strings.TrimSpace(connectionMode) != "" {
		claimedPayload["connection_mode"] = strings.TrimSpace(connectionMode)
	}
	s.recordRunEventBestEffort(ctx, run.ID, "run.dispatch.claimed", claimedPayload)
	return s.runtimePullRunResponse(ctx, token, run)
}

func (s *Service) runtimePullRunResponse(ctx context.Context, token db.AgentRuntimeToken, run db.Run) (*RuntimePullRunResponse, error) {
	input := map[string]interface{}{}
	if len(run.Input) > 0 {
		_ = json.Unmarshal(run.Input, &input)
	}
	decorateCtx, cancel := context.WithTimeout(ctx, runtimePullResponseContextTimeout)
	defer cancel()
	return &RuntimePullRunResponse{
		RunID:          run.ID.String(),
		AgentID:        run.AgentID.String(),
		Input:          input,
		ResultEndpoint: "/api/v1/agent-runtime/runs/" + run.ID.String() + "/result",
		ResultMethod:   "POST",
		ResultRequired: true,
		Metadata: map[string]interface{}{
			"claim_ttl_seconds":                    int(runtimePullClaimTTL.Seconds()),
			"recommended_next_claim_after_seconds": 0,
			"recommended_heartbeat_after_seconds":  int(runtimePullHeartbeatInterval.Seconds()),
			"max_long_poll_wait_seconds":           int(runtimePullMaxLongPollWait.Seconds()),
			"empty_claim_retry_after_seconds":      int(runtimePullEmptyClaimRetryAfter.Seconds()),
			"result_required":                      true,
			"result_status_values":                 []string{"success", "failed", "timeout"},
			"result_timeout_seconds":               int(defaultRuntimePullResultTimeout.Seconds()),
		},
		Source:       run.Source,
		A2A:          s.agentA2AContextForRun(decorateCtx, run.ID),
		Conversation: s.conversationContextForRun(decorateCtx, run.ID),
	}, nil
}

// CompleteRuntimePullRun accepts the result of a run previously claimed by the same access token.
func (s *Service) CompleteRuntimePullRun(ctx context.Context, plaintextToken string, runID uuid.UUID, req *RuntimePullResultRequest) (*RunResponse, error) {
	token, err := s.verifyRuntimeToken(ctx, plaintextToken, "agent:pull")
	if err != nil {
		return nil, err
	}
	return s.completeRuntimePullRunForToken(ctx, token, nil, runID, req)
}

func (s *Service) completeRuntimePullRunForToken(ctx context.Context, token db.AgentRuntimeToken, agent *db.Agent, runID uuid.UUID, req *RuntimePullResultRequest) (*RunResponse, error) {
	if req == nil {
		return nil, httpx.BadRequest("请求体不能为空")
	}
	state, err := s.queries.GetRuntimePullRunState(ctx, runID)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && state.AgentID != token.AgentID) {
		return nil, httpx.NotFound("调用记录不存在")
	}
	if err != nil {
		return nil, httpx.Internal("查询调用记录失败")
	}
	if state.Status != "running" {
		return nil, httpx.Conflict("run 已结束，不能重复回传")
	}
	if state.ClaimedByRuntimeTokenID == nil || *state.ClaimedByRuntimeTokenID != token.ID {
		return nil, httpx.Conflict("run 未被当前访问令牌领取")
	}
	if agent == nil {
		agentRow, err := s.queries.GetAgentByID(ctx, token.AgentID)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("Agent 不存在")
		}
		if err != nil {
			return nil, httpx.Internal("查询 Agent 失败")
		}
		agent = &agentRow
	}
	if agent.ID != token.AgentID {
		return nil, httpx.NotFound("Agent 不存在")
	}
	if !isQueuedRuntimeMode(agent.ConnectionMode) {
		return nil, httpx.Conflict("Agent 不是队列型 runtime 接入模式")
	}

	duration := req.DurationMs
	if duration <= 0 {
		duration = clampDurationMillisToInt32(time.Since(state.StartedAt))
	}
	var resp *RunResponse
	var successOutput map[string]interface{}
	success := false
	switch req.Status {
	case "success":
		output := req.Output
		if output == nil {
			output = map[string]interface{}{}
		}
		resp = s.handleSuccess(ctx, runID, token.AgentID, output, req.Events, duration, false)
		successOutput = output
		success = true
	case "failed":
		agentErr := req.Error
		if agentErr == nil {
			agentErr = &AgentError{Code: "AGENT_REPORTED_FAILURE", Message: "Agent runtime reported failed"}
		}
		resp = s.handleFailure(ctx, runID, token.AgentID, duration, nil, agentErr, false)
	case "timeout":
		message := "Agent runtime reported timeout"
		if req.Error != nil && strings.TrimSpace(req.Error.Message) != "" {
			message = req.Error.Message
		}
		agentErr := &AgentError{Code: "TIMEOUT", Message: message}
		resp = s.handleFailure(ctx, runID, token.AgentID, duration, nil, agentErr, false)
	default:
		return nil, httpx.BadRequest("status 取值非法")
	}
	s.touchRuntimeTokenAsync(ctx, token.ID)
	postCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s.decorateDelegationCompletion(postCtx, runID, token.AgentID, resp)
	s.attachRunRequirementEvidence(postCtx, runID, resp)
	s.triggerRuntimePullCompletionDelivery(runID, token.AgentID, successOutput, success)
	return resp, nil
}

// TimeoutStaleRuntimePullRuns converts abandoned queued runtime runs into timeout
// terminal states so users and upstream callers never wait forever.
func (s *Service) TimeoutStaleRuntimePullRuns(ctx context.Context, cfg RuntimePullRunTimeoutConfig) (int32, error) {
	cfg = normalizeRuntimePullRunTimeoutConfig(cfg)
	now := time.Now()
	var staleRuns []db.ListStaleRuntimePullRunsRow

	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := s.queries.WithTx(tx)
		rows, err := q.ListStaleRuntimePullRuns(ctx, db.ListStaleRuntimePullRunsParams{
			DispatchStaleBefore: now.Add(-cfg.DispatchTimeout),
			ResultStaleBefore:   now.Add(-cfg.ResultTimeout),
			Limit:               cfg.BatchSize,
		})
		if err != nil {
			return err
		}
		staleRuns = rows
		for _, run := range rows {
			code := run.ErrorCode
			message := run.ErrorMessage
			duration := clampDurationMillisToInt32(now.Sub(run.StartedAt))
			if err := q.MarkRunFailed(ctx, db.MarkRunFailedParams{
				ID:           run.ID,
				Status:       "timeout",
				ErrorCode:    &code,
				ErrorMessage: &message,
				DurationMs:   duration,
			}); err != nil {
				return err
			}
			if _, err := q.MarkAgentAvailabilityFailure(ctx, run.AgentID); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.Error().Err(err).Msg("runtime.TimeoutStaleRuntimePullRuns")
		return 0, err
	}

	for _, run := range staleRuns {
		duration := clampDurationMillisToInt32(now.Sub(run.StartedAt))
		s.recordRunEventBestEffort(ctx, run.ID, "run.failed", map[string]interface{}{
			"status":        "timeout",
			"error_code":    run.ErrorCode,
			"error_message": run.ErrorMessage,
			"duration_ms":   duration,
		})
		resp := &RunResponse{
			RunID:      run.ID.String(),
			Status:     "timeout",
			ErrorCode:  run.ErrorCode,
			ErrorMsg:   run.ErrorMessage,
			DurationMs: duration,
		}
		s.decorateDelegationCompletion(ctx, run.ID, run.AgentID, resp)
		if s.shouldTriggerExternalDelivery(ctx, run.ID) {
			s.triggerWebhookByRun(run.ID)
			s.triggerDelivery(run.ID)
		}
	}
	return clampLenToInt32(len(staleRuns)), nil
}

// TimeoutStaleEndpointRuns converts abandoned direct_http / mcp_server runs
// into timeout terminal states. Normal endpoint calls are completed by the
// in-process goroutine; this worker is only a crash / restart / DB outage
// recovery net for runs that have exceeded the configured stale window.
func (s *Service) TimeoutStaleEndpointRuns(ctx context.Context, cfg EndpointRunTimeoutConfig) (int32, error) {
	cfg = normalizeEndpointRunTimeoutConfig(cfg)
	now := time.Now()
	var staleRuns []db.ListStaleEndpointRunsRow

	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := s.queries.WithTx(tx)
		rows, err := q.ListStaleEndpointRuns(ctx, db.ListStaleEndpointRunsParams{
			StaleBefore: now.Add(-cfg.StaleAfter),
			Limit:       cfg.BatchSize,
		})
		if err != nil {
			return err
		}
		staleRuns = rows
		for _, run := range rows {
			code := run.ErrorCode
			message := run.ErrorMessage
			duration := clampDurationMillisToInt32(now.Sub(run.StartedAt))
			if err := q.MarkRunFailed(ctx, db.MarkRunFailedParams{
				ID:           run.ID,
				Status:       "timeout",
				ErrorCode:    &code,
				ErrorMessage: &message,
				DurationMs:   duration,
			}); err != nil {
				return err
			}
			if _, err := q.MarkAgentAvailabilityFailure(ctx, run.AgentID); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.Error().Err(err).Msg("runtime.TimeoutStaleEndpointRuns")
		return 0, err
	}

	for _, run := range staleRuns {
		duration := clampDurationMillisToInt32(now.Sub(run.StartedAt))
		s.recordRunEventBestEffort(ctx, run.ID, "run.failed", map[string]interface{}{
			"status":          "timeout",
			"error_code":      run.ErrorCode,
			"error_message":   run.ErrorMessage,
			"duration_ms":     duration,
			"connection_mode": run.ConnectionMode,
		})
		resp := &RunResponse{
			RunID:      run.ID.String(),
			Status:     "timeout",
			ErrorCode:  run.ErrorCode,
			ErrorMsg:   run.ErrorMessage,
			DurationMs: duration,
		}
		s.decorateDelegationCompletion(ctx, run.ID, run.AgentID, resp)
		if s.shouldTriggerExternalDelivery(ctx, run.ID) {
			s.triggerWebhookByRun(run.ID)
			s.triggerDelivery(run.ID)
		}
	}
	return clampLenToInt32(len(staleRuns)), nil
}

func normalizeRuntimePullRunTimeoutConfig(cfg RuntimePullRunTimeoutConfig) RuntimePullRunTimeoutConfig {
	if cfg.DispatchTimeout <= 0 {
		cfg.DispatchTimeout = 2 * time.Minute
	}
	if cfg.ResultTimeout <= 0 {
		cfg.ResultTimeout = 15 * time.Minute
	}
	if cfg.ResultTimeout < runtimePullClaimTTL {
		cfg.ResultTimeout = runtimePullClaimTTL
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 50
	}
	return cfg
}

func normalizeEndpointRunTimeoutConfig(cfg EndpointRunTimeoutConfig) EndpointRunTimeoutConfig {
	if cfg.StaleAfter <= 0 {
		cfg.StaleAfter = defaultEndpointRunTimeout
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultEndpointRunBatchSize
	}
	return cfg
}

// HeartbeatAgent lets an Agent proactively mark its bound access-token owner alive.
func (s *Service) HeartbeatAgent(ctx context.Context, plaintextToken string) (*AgentHeartbeatResponse, error) {
	token, err := s.verifyRuntimeTokenAny(ctx, plaintextToken, "agent:pull", "agent:call")
	if err != nil {
		return nil, err
	}
	return s.HeartbeatAgentForToken(ctx, token)
}

// HeartbeatAgentForToken records runtime heartbeat after token validation has
// already happened at the endpoint boundary.
func (s *Service) HeartbeatAgentForToken(ctx context.Context, token db.AgentRuntimeToken) (*AgentHeartbeatResponse, error) {
	agent, err := s.queries.GetAgentByID(ctx, token.AgentID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("Agent 不存在")
	}
	if err != nil {
		log.Error().Err(err).Str("agent_id", token.AgentID.String()).Msg("runtime.HeartbeatAgent: GetAgentByID")
		return nil, httpx.Internal("查询 Agent 失败")
	}
	if agent.LifecycleStatus != "active" {
		return nil, httpx.Forbidden("Agent 未启用")
	}
	return s.heartbeatRuntimeAgentForToken(ctx, token, &agent)
}

func (s *Service) heartbeatRuntimeAgentForToken(ctx context.Context, token db.AgentRuntimeToken, agent *db.Agent) (*AgentHeartbeatResponse, error) {
	return s.heartbeatRuntimeAgentForTokenWithOptions(ctx, token, agent, runtimeHeartbeatOptions{})
}

func (s *Service) heartbeatRuntimeAgentForTokenWithOptions(ctx context.Context, token db.AgentRuntimeToken, agent *db.Agent, opts runtimeHeartbeatOptions) (*AgentHeartbeatResponse, error) {
	if agent == nil {
		return nil, httpx.NotFound("Agent 不存在")
	}
	if agent.LifecycleStatus != "active" {
		return nil, httpx.Forbidden("Agent 未启用")
	}
	snapshot, err := s.queries.MarkAgentAvailabilityHeartbeat(ctx, token.AgentID)
	if err != nil {
		log.Error().Err(err).Str("agent_id", token.AgentID.String()).Msg("runtime.HeartbeatAgent: MarkAgentAvailabilityHeartbeat")
		return nil, httpx.Internal("记录 Agent heartbeat 失败")
	}
	pendingRunCount, err := s.queries.CountClaimableRuntimePullRuns(ctx, token.AgentID)
	if err != nil {
		log.Error().Err(err).Str("agent_id", token.AgentID.String()).Msg("runtime.HeartbeatAgent: CountClaimableRuntimePullRuns")
		return nil, httpx.Internal("查询待领取任务失败")
	}
	nextClaimAfterSeconds := int32(runtimePullEmptyClaimRetryAfter.Seconds())
	claimNow := pendingRunCount > 0
	if claimNow {
		nextClaimAfterSeconds = 0
	}
	if opts.asyncTokenTouch {
		s.touchRuntimeTokenAsync(ctx, token.ID)
	} else {
		_ = s.queries.TouchAgentRuntimeToken(ctx, token.ID)
	}
	return &AgentHeartbeatResponse{
		AgentID:                          snapshot.AgentID.String(),
		AvailabilityStatus:               snapshot.AvailabilityStatus,
		LastCheckedAt:                    snapshot.LastCheckedAt,
		ConsecutiveFailures:              snapshot.ConsecutiveFailures,
		PendingRunCount:                  pendingRunCount,
		ClaimNow:                         claimNow,
		NextClaimAfterSeconds:            nextClaimAfterSeconds,
		RecommendedHeartbeatAfterSeconds: int32(runtimePullHeartbeatInterval.Seconds()),
		MaxClaimWaitSeconds:              int32(runtimePullMaxLongPollWait.Seconds()),
	}, nil
}

func (s *Service) decorateDelegationCompletion(ctx context.Context, runID, targetAgentID uuid.UUID, resp *RunResponse) {
	delegation, err := s.queries.GetRunDelegationByChild(ctx, runID)
	if errors.Is(err, pgx.ErrNoRows) {
		return
	}
	if err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Msg("runtime.decorateDelegationCompletion")
		return
	}
	resp.ParentRunID = delegation.ParentRunID.String()
	resp.CallerAgentID = delegation.CallerAgentID.String()
	resp.BillingMode = "free_delegation"
	s.recordRunEventBestEffort(ctx, delegation.ParentRunID, "run.child.completed", map[string]interface{}{
		"child_run_id":    runID.String(),
		"caller_agent_id": delegation.CallerAgentID.String(),
		"target_agent_id": targetAgentID.String(),
		"status":          resp.Status,
	})
	decorateNextAction(resp)
}

func (s *Service) verifyRuntimeToken(ctx context.Context, plaintext, requiredScope string) (db.AgentRuntimeToken, error) {
	return s.verifyRuntimeTokenAny(ctx, plaintext, requiredScope)
}

func (s *Service) ValidateRuntimeToken(ctx context.Context, plaintext string, acceptedScopes ...string) (db.AgentRuntimeToken, error) {
	return s.verifyRuntimeTokenAny(ctx, plaintext, acceptedScopes...)
}

func (s *Service) verifyRuntimeTokenAny(ctx context.Context, plaintext string, acceptedScopes ...string) (db.AgentRuntimeToken, error) {
	plaintext = strings.TrimSpace(plaintext)
	if !credential.HasAnyPrefix(plaintext, credential.AgentTokenPrefix) ||
		!credential.ValidLengthForPrefix(plaintext, credential.AgentTokenPrefix) {
		return db.AgentRuntimeToken{}, httpx.Unauthorized("访问令牌无效或已撤销")
	}
	tokens, err := s.queries.ListActiveAgentRuntimeTokensByPrefix(ctx, plaintext[:runtimeTokenPrefixLen])
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return db.AgentRuntimeToken{}, ctxErr
		}
		return db.AgentRuntimeToken{}, httpx.Unauthorized("访问令牌无效或已撤销")
	}
	for _, token := range tokens {
		if credential.VerifyTokenHash(token.TokenHash, plaintext) && hasAnyRuntimeScope(token.Scopes, acceptedScopes...) {
			return token, nil
		}
	}
	return db.AgentRuntimeToken{}, httpx.Unauthorized("访问令牌无效或已撤销")
}

func hasAnyRuntimeScope(scopes []string, accepted ...string) bool {
	for _, expected := range accepted {
		if hasRuntimeScope(scopes, expected) {
			return true
		}
	}
	return false
}

func hasRuntimeScope(scopes []string, expected string) bool {
	for _, scope := range scopes {
		if scope == expected {
			return true
		}
	}
	return false
}

// callAgentEndpoint 平台代理 HTTP 调用。
//
// 返回四元组：
//   - output: 成功时创作者返回的 output 字段
//   - events: 成功时创作者返回的中间事件
//   - agentErr: 创作者业务错误（HTTP 4xx/5xx 或 body 中 error 非空）
//   - callErr: 网络层错误（超时 / 连接失败 / 读 body 失败）
//
// 任意一个非空都视为本次调用失败。
func (s *Service) callAgentEndpoint(
	ctx context.Context, agent *db.Agent, runID, userID uuid.UUID, req *RunRequest, delegation *Delegation,
) (map[string]interface{}, []AgentEvent, *AgentError, error) {
	conversation := s.conversationContextForRun(ctx, runID)
	request := AgentRequest{
		Input:        req.Input,
		Metadata:     req.Metadata,
		RunID:        runID.String(),
		A2A:          s.agentA2AContextForRequest(runID, delegation, req.A2AContext),
		Conversation: conversation,
	}
	if delegation != nil {
		request.ParentRunID = delegation.ParentRunID.String()
		request.CallerAgentID = delegation.CallerAgentID.String()
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, agent.EndpointURL, bytes.NewReader(payload))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("User-Agent", "OpenLinker/1.0")
	httpReq.Header.Set("X-OpenLinker-Run-Id", runID.String())
	if delegation == nil {
		httpReq.Header.Set("X-OpenLinker-User-Id", userID.String())
	}
	httpReq.Header.Set("X-OpenLinker-Timestamp", fmt.Sprintf("%d", time.Now().Unix()))
	if agent.EndpointAuthHeader != nil && *agent.EndpointAuthHeader != "" {
		// 创作者注册时填的预共享 token，平台→endpoint 携带；前端永不返回。
		httpReq.Header.Set("X-OpenLinker-Token", *agent.EndpointAuthHeader)
	}

	resp, err := s.httpClient.Do(httpReq)
	if err != nil {
		return nil, nil, nil, err
	}
	defer resp.Body.Close()

	body, err := readAgentResponseBody(resp.Body)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("read body: %w", err)
	}

	output, events, agentErr, responseShape, uerr := decodeAgentEndpointResponse(body)
	if uerr != nil {
		return nil, nil, &AgentError{
			Code:    "INVALID_RESPONSE",
			Message: "agent endpoint 返回非 JSON: " + truncate(string(body), errMsgMaxLen),
		}, nil
	}

	// HTTP 状态码 >= 400 或 body.error 非空都是失败
	if resp.StatusCode >= 400 || agentErr != nil {
		if agentErr == nil {
			msg := truncate(string(body), errMsgMaxLen)
			agentErr = &AgentError{
				Code:    fmt.Sprintf("HTTP_%d", resp.StatusCode),
				Message: msg,
			}
		}
		return nil, nil, agentErr, nil
	}

	events = prependEndpointResponseEvent(events, agent.ConnectionMode, responseShape, output)
	return output, events, nil, nil
}

func decodeAgentEndpointResponse(body []byte) (map[string]interface{}, []AgentEvent, *AgentError, string, error) {
	var raw interface{}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, nil, nil, "", err
	}

	var envelope AgentResponse
	if err := json.Unmarshal(body, &envelope); err != nil {
		// Top-level arrays / strings are valid JSON, but not the canonical
		// envelope. Preserve them as output so generic HTTP endpoints are still
		// useful in Playground.
		return map[string]interface{}{"response": raw}, nil, nil, jsonValueShape(raw), nil
	}

	rawMap, ok := raw.(map[string]interface{})
	if !ok {
		return map[string]interface{}{"response": raw}, envelope.Events, envelope.Error, jsonValueShape(raw), nil
	}

	output, shape := normalizeAgentEndpointOutput(rawMap)
	return output, envelope.Events, envelope.Error, shape, nil
}

func normalizeAgentEndpointOutput(raw map[string]interface{}) (map[string]interface{}, string) {
	if raw == nil {
		return map[string]interface{}{}, "empty"
	}
	if value, ok := raw["output"]; ok {
		if value == nil {
			return map[string]interface{}{}, "output_null"
		}
		if output, ok := value.(map[string]interface{}); ok {
			return output, "output_object"
		}
		return map[string]interface{}{"output": value}, "output_value"
	}

	output := make(map[string]interface{}, len(raw))
	for key, value := range raw {
		switch key {
		case "events", "metadata", "cost_usd":
			continue
		case "error":
			if value == nil {
				continue
			}
		}
		output[key] = value
	}
	if len(output) == 0 {
		return map[string]interface{}{}, "empty_object"
	}
	return output, "top_level_object"
}

func jsonValueShape(value interface{}) string {
	switch value.(type) {
	case map[string]interface{}:
		return "top_level_object"
	case []interface{}:
		return "top_level_array"
	case string:
		return "top_level_string"
	case float64:
		return "top_level_number"
	case bool:
		return "top_level_boolean"
	case nil:
		return "top_level_null"
	default:
		return "json_value"
	}
}

func prependEndpointResponseEvent(events []AgentEvent, connectionMode, responseShape string, output map[string]interface{}) []AgentEvent {
	payload := map[string]interface{}{
		"status":          "endpoint_response_received",
		"connection_mode": connectionMode,
		"response_shape":  responseShape,
	}
	if connectionMode == "" {
		payload["connection_mode"] = connectionModeDirectHTTP
	}
	if len(output) > 0 {
		payload["output_keys"] = sortedMapKeys(output, 12)
	}
	return append([]AgentEvent{{
		EventType: "run.status.changed",
		Payload:   payload,
	}}, events...)
}

func sortedMapKeys(value map[string]interface{}, limit int) []string {
	if len(value) == 0 || limit == 0 {
		return nil
	}
	keys := make([]string, 0, len(value))
	for key := range value {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if limit > 0 && len(keys) > limit {
		keys = keys[:limit]
	}
	return keys
}

type mcpToolCallRequest struct {
	JSONRPC string            `json:"jsonrpc"`
	ID      string            `json:"id"`
	Method  string            `json:"method"`
	Params  mcpToolCallParams `json:"params"`
}

type mcpToolCallParams struct {
	Name      string                 `json:"name"`
	Arguments map[string]interface{} `json:"arguments"`
	Metadata  map[string]interface{} `json:"_meta,omitempty"`
}

type mcpToolCallResponse struct {
	Result map[string]interface{} `json:"result"`
	Error  *mcpToolCallError      `json:"error,omitempty"`
}

type mcpToolCallError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (s *Service) callMCPServer(
	ctx context.Context, agent *db.Agent, runID, userID uuid.UUID, req *RunRequest, delegation *Delegation,
) (map[string]interface{}, []AgentEvent, *AgentError, error) {
	var toolName string
	if agent.MCPToolName != nil {
		toolName = strings.TrimSpace(*agent.MCPToolName)
	}
	if toolName == "" {
		return nil, nil, &AgentError{Code: "MCP_TOOL_MISSING", Message: "Agent 未配置 MCP tool"}, nil
	}

	metadata := map[string]interface{}{
		"run_id":   runID.String(),
		"user_id":  userID.String(),
		"platform": "openlinker",
	}
	for k, v := range req.Metadata {
		metadata[k] = v
	}
	if delegation != nil {
		metadata["parent_run_id"] = delegation.ParentRunID.String()
		metadata["caller_agent_id"] = delegation.CallerAgentID.String()
	}
	metadata["a2a"] = agentA2AContextMap(s.agentA2AContextForRequest(runID, delegation, req.A2AContext))
	if conversation := s.conversationContextForRun(ctx, runID); conversation != nil {
		metadata["conversation"] = conversation
	}
	payload, err := json.Marshal(mcpToolCallRequest{
		JSONRPC: "2.0",
		ID:      runID.String(),
		Method:  "tools/call",
		Params: mcpToolCallParams{
			Name:      toolName,
			Arguments: req.Input,
			Metadata:  metadata,
		},
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("marshal mcp request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, agent.EndpointURL, bytes.NewReader(payload))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("build mcp request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("User-Agent", "OpenLinker/1.0")
	httpReq.Header.Set("X-OpenLinker-Run-Id", runID.String())
	httpReq.Header.Set("X-OpenLinker-Timestamp", fmt.Sprintf("%d", time.Now().Unix()))
	if agent.EndpointAuthHeader != nil && *agent.EndpointAuthHeader != "" {
		auth := strings.TrimSpace(*agent.EndpointAuthHeader)
		if strings.HasPrefix(strings.ToLower(auth), "bearer ") {
			httpReq.Header.Set("Authorization", auth)
		} else {
			httpReq.Header.Set("X-OpenLinker-Token", auth)
		}
	}

	resp, err := s.httpClient.Do(httpReq)
	if err != nil {
		return nil, nil, nil, err
	}
	defer resp.Body.Close()

	body, err := readAgentResponseBody(resp.Body)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("read mcp body: %w", err)
	}

	var mr mcpToolCallResponse
	if uerr := json.Unmarshal(body, &mr); uerr != nil {
		return nil, nil, &AgentError{
			Code:    "INVALID_MCP_RESPONSE",
			Message: "MCP endpoint 返回非 JSON-RPC: " + truncate(string(body), errMsgMaxLen),
		}, nil
	}
	if resp.StatusCode >= 400 || mr.Error != nil {
		if mr.Error == nil {
			return nil, nil, &AgentError{
				Code:    fmt.Sprintf("HTTP_%d", resp.StatusCode),
				Message: truncate(string(body), errMsgMaxLen),
			}, nil
		}
		return nil, nil, &AgentError{
			Code:    fmt.Sprintf("MCP_%d", mr.Error.Code),
			Message: truncate(mr.Error.Message, errMsgMaxLen),
		}, nil
	}
	return normalizeMCPResult(mr.Result), nil, nil, nil
}

func readAgentResponseBody(r io.Reader) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r, maxAgentResponseBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxAgentResponseBodyBytes {
		return nil, fmt.Errorf("response body exceeds %d bytes", maxAgentResponseBodyBytes)
	}
	return body, nil
}

func normalizeMCPResult(result map[string]interface{}) map[string]interface{} {
	if result == nil {
		return map[string]interface{}{}
	}
	if output, ok := result["output"].(map[string]interface{}); ok {
		return output
	}
	if structured, ok := result["structuredContent"].(map[string]interface{}); ok {
		return structured
	}
	return map[string]interface{}{"mcp_result": result}
}

// DryRun 让创作者侧 endpoint 跑一次「不计费、不写 runs」的探活调用。
//
// 用于 Agent 接入流程的 dry-run 步骤：使用给定输入直接命中 endpoint，
// 返回 endpoint 的输出或错误信息。runID 用随机 UUID 仅为响应头标识，
// 没有任何 DB 副作用。
//
// 返回 (output, errMsg)：errMsg 为空字符串时表示成功。
func (s *Service) DryRun(
	ctx context.Context,
	agent *db.Agent,
	input map[string]interface{},
) (map[string]interface{}, string) {
	runID := uuid.New()
	userID := uuid.New()
	output, _, agentErr, callErr := s.callAgent(ctx, agent, runID, userID, &RunRequest{
		Input: input,
	}, nil)
	if callErr != nil {
		return nil, "endpoint 调用失败: " + truncate(callErr.Error(), errMsgMaxLen)
	}
	if agentErr != nil {
		return nil, agentErr.Code + ": " + agentErr.Message
	}
	return output, ""
}

// handleSuccess 成功路径优先保证用户可见 run 状态落库。统计、
// availability 和 artifact 均不得把已完成的 run 回滚回 running。
func (s *Service) handleSuccess(
	ctx context.Context,
	runID, agentID uuid.UUID,
	output map[string]interface{},
	agentEvents []AgentEvent,
	duration int32,
	triggerExternalDelivery bool,
) *RunResponse {
	outputJSON, err := json.Marshal(output)
	if err != nil {
		// output 不可序列化属于极端情况；仍返回结果（DB 里 output 留空）。
		log.Error().Err(err).Str("run_id", runID.String()).Msg("runtime.handleSuccess: marshal output")
		outputJSON = []byte("null")
	}

	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := s.queries.WithTx(tx)
		return q.MarkRunSuccess(ctx, db.MarkRunSuccessParams{
			ID:         runID,
			Output:     outputJSON,
			DurationMs: duration,
		})
	})
	if err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).
			Msg("runtime.handleSuccess: mark success failed")
	} else {
		if artifactErr := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
			return s.createRunArtifacts(ctx, s.queries.WithTx(tx), runID, output)
		}); artifactErr != nil {
			log.Error().Err(artifactErr).Str("run_id", runID.String()).Msg("runtime.handleSuccess: create artifacts failed")
		}
		s.recordRunSuccessStatsBestEffort(ctx, runID, agentID, 0)
		s.recordAgentEventsBestEffort(ctx, runID, agentEvents)
		s.recordRunEventBestEffort(ctx, runID, "run.completed", map[string]interface{}{
			"status":      "success",
			"duration_ms": duration,
			"output":      output,
		})
	}

	if triggerExternalDelivery {
		// 委派子 run 不自动外发；最终交付由父 run 决定。
		s.triggerWebhook(runID, agentID, output)
		s.triggerDelivery(runID)
	}

	return &RunResponse{
		RunID:      runID.String(),
		Status:     "success",
		Output:     output,
		CostCents:  0,
		DurationMs: duration,
		NextAction: nextActionForSuccess(output, "", ""),
	}
}

// handleFailure 失败路径：MarkRunFailed + availability failure（一个事务）。
//
// 错误分类：
//   - context.DeadlineExceeded → 'timeout' / TIMEOUT
//   - 其他网络层错误 → 'failed' / CONNECTION_ERROR
//   - 创作者业务错误 → 'failed' / 透传 agentErr.Code
func (s *Service) handleFailure(
	ctx context.Context,
	runID, agentID uuid.UUID,
	duration int32,
	callErr error,
	agentErr *AgentError,
	triggerExternalDelivery bool,
) *RunResponse {
	errCode := "INTERNAL_ERROR"
	errMsg := "调用失败"
	runStatus := "failed"

	switch {
	case callErr != nil && (errors.Is(callErr, context.DeadlineExceeded) || isTimeoutErr(callErr)):
		errCode = "TIMEOUT"
		errMsg = "Agent endpoint 超时"
		runStatus = "timeout"
	case callErr != nil:
		errCode = "CONNECTION_ERROR"
		errMsg = truncate(callErr.Error(), errMsgMaxLen)
	case agentErr != nil:
		errCode = agentErr.Code
		errMsg = truncate(agentErr.Message, errMsgMaxLen)
		if strings.EqualFold(errCode, "TIMEOUT") {
			runStatus = "timeout"
		}
	}

	codePtr := errCode
	msgPtr := errMsg
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := s.queries.WithTx(tx)
		if e := q.MarkRunFailed(ctx, db.MarkRunFailedParams{
			ID:           runID,
			Status:       runStatus,
			ErrorCode:    &codePtr,
			ErrorMessage: &msgPtr,
			DurationMs:   duration,
		}); e != nil {
			return e
		}
		_, e := q.MarkAgentAvailabilityFailure(ctx, agentID)
		return e
	})
	if err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Msg("runtime.handleFailure: mark failed tx")
	} else {
		s.recordRunEventBestEffort(ctx, runID, "run.failed", map[string]interface{}{
			"status":        runStatus,
			"error_code":    errCode,
			"error_message": errMsg,
			"duration_ms":   duration,
		})
	}

	if triggerExternalDelivery {
		// 失败也触发 webhook / delivery，让外部系统能感知失败。
		s.triggerWebhookByRun(runID)
		s.triggerDelivery(runID)
	}

	return &RunResponse{
		RunID:      runID.String(),
		Status:     runStatus,
		ErrorCode:  errCode,
		ErrorMsg:   errMsg,
		CostCents:  0,
		DurationMs: duration,
		NextAction: nextActionForFailure(runStatus, errCode, errMsg),
	}
}

type runArtifactDraft struct {
	ArtifactType  string
	Title         string
	Content       map[string]interface{}
	Visibility    string
	MimeType      string
	FileURI       string
	FileName      string
	FileSHA256    string
	FileSizeBytes *int64
}

type runArtifactDeltaDraft struct {
	SourceArtifactID string
	ArtifactType     string
	Title            string
	Visibility       string
	MimeType         string
	FileURI          string
	FileName         string
	FileSHA256       string
	FileSizeBytes    *int64
	Append           bool
	LastChunk        bool
	Parts            []interface{}
	Payload          map[string]interface{}
}

func (s *Service) createRunArtifacts(ctx context.Context, q *db.Queries, runID uuid.UUID, output map[string]interface{}) error {
	for _, artifact := range runArtifactsFromOutput(output) {
		raw, err := json.Marshal(artifact.Content)
		if err != nil {
			return err
		}
		if _, err := q.CreateRunArtifact(ctx, db.CreateRunArtifactParams{
			RunID:         runID,
			ArtifactType:  artifact.ArtifactType,
			Title:         artifact.Title,
			Content:       raw,
			Visibility:    artifact.Visibility,
			MimeType:      stringPtrOrNil(artifact.MimeType),
			FileUri:       stringPtrOrNil(artifact.FileURI),
			FileName:      stringPtrOrNil(artifact.FileName),
			FileSha256:    stringPtrOrNil(artifact.FileSHA256),
			FileSizeBytes: artifact.FileSizeBytes,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) recordArtifactDeltaBestEffort(ctx context.Context, runID uuid.UUID, eventSequence *int32, payload map[string]interface{}) {
	if err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		return s.upsertRunArtifactDelta(ctx, s.queries.WithTx(tx), runID, eventSequence, payload)
	}); err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Msg("runtime.recordArtifactDeltaBestEffort")
	}
}

func (s *Service) upsertRunArtifactDelta(ctx context.Context, q *db.Queries, runID uuid.UUID, eventSequence *int32, payload map[string]interface{}) error {
	draft := artifactDeltaDraftFromPayload(payload)
	artifact, err := q.GetRunArtifactBySourceID(ctx, db.GetRunArtifactBySourceIDParams{
		RunID:            runID,
		SourceArtifactID: draft.SourceArtifactID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		emptyContent, marshalErr := json.Marshal(map[string]interface{}{
			"artifact_id": draft.SourceArtifactID,
			"streamed":    true,
			"complete":    false,
			"parts":       []interface{}{},
			"chunks":      []interface{}{},
		})
		if marshalErr != nil {
			return marshalErr
		}
		sourceID := draft.SourceArtifactID
		artifact, err = q.CreateRunArtifact(ctx, db.CreateRunArtifactParams{
			RunID:            runID,
			ArtifactType:     draft.ArtifactType,
			Title:            draft.Title,
			Content:          emptyContent,
			Visibility:       draft.Visibility,
			SourceArtifactID: &sourceID,
			MimeType:         stringPtrOrNil(draft.MimeType),
			FileUri:          stringPtrOrNil(draft.FileURI),
			FileName:         stringPtrOrNil(draft.FileName),
			FileSha256:       stringPtrOrNil(draft.FileSHA256),
			FileSizeBytes:    draft.FileSizeBytes,
		})
	}
	if err != nil {
		return err
	}

	partsJSON, err := json.Marshal(draft.Parts)
	if err != nil {
		return err
	}
	payloadJSON, err := json.Marshal(draft.Payload)
	if err != nil {
		return err
	}
	partsSHA := sha256Hex(partsJSON)
	payloadSHA := sha256Hex(payloadJSON)
	declaredSHA, checksumStatus := artifactChunkChecksum(draft.Payload, partsSHA)
	chunk, err := q.CreateRunArtifactChunk(ctx, db.CreateRunArtifactChunkParams{
		RunID:            runID,
		RunArtifactID:    artifact.ID,
		SourceArtifactID: draft.SourceArtifactID,
		EventSequence:    eventSequence,
		Append:           draft.Append,
		LastChunk:        draft.LastChunk,
		Parts:            partsJSON,
		Payload:          payloadJSON,
		PartsSha256:      &partsSHA,
		PayloadSha256:    &payloadSHA,
		DeclaredSha256:   stringPtrOrNil(declaredSHA),
		ChecksumStatus:   checksumStatus,
	})
	if err != nil {
		return err
	}

	content := map[string]interface{}{}
	if len(artifact.Content) > 0 {
		_ = json.Unmarshal(artifact.Content, &content)
	}
	content = mergeArtifactDeltaContent(content, draft, chunk)
	contentJSON, err := json.Marshal(content)
	if err != nil {
		return err
	}
	_, err = q.UpdateRunArtifactContent(ctx, db.UpdateRunArtifactContentParams{
		ID:            artifact.ID,
		RunID:         runID,
		ArtifactType:  draft.ArtifactType,
		Title:         draft.Title,
		Content:       contentJSON,
		Visibility:    draft.Visibility,
		MimeType:      stringPtrOrNil(draft.MimeType),
		FileUri:       stringPtrOrNil(draft.FileURI),
		FileName:      stringPtrOrNil(draft.FileName),
		FileSha256:    stringPtrOrNil(draft.FileSHA256),
		FileSizeBytes: draft.FileSizeBytes,
	})
	return err
}

func runArtifactsFromOutput(output map[string]interface{}) []runArtifactDraft {
	if output == nil {
		output = map[string]interface{}{}
	}
	if raw, ok := output["artifacts"].([]interface{}); ok && len(raw) > 0 {
		items := make([]runArtifactDraft, 0, len(raw))
		for i, item := range raw {
			if m, ok := item.(map[string]interface{}); ok {
				items = append(items, artifactDraftFromMap(m, fmt.Sprintf("Artifact %d", i+1)))
			} else {
				items = append(items, runArtifactDraft{
					ArtifactType: "data",
					Title:        fmt.Sprintf("Artifact %d", i+1),
					Content:      map[string]interface{}{"value": item},
					Visibility:   "private",
				})
			}
		}
		return items
	}
	if raw, ok := output["artifact"].(map[string]interface{}); ok {
		return []runArtifactDraft{artifactDraftFromMap(raw, "Agent 产物")}
	}
	return []runArtifactDraft{{
		ArtifactType: "json",
		Title:        "Agent 输出",
		Content:      output,
		Visibility:   "private",
	}}
}

func artifactDraftFromMap(raw map[string]interface{}, fallbackTitle string) runArtifactDraft {
	title := normalizeArtifactTitle(coalesceArtifactString(raw, "title", fallbackTitle))
	artifactType := coalesceArtifactString(raw, "artifact_type", "")
	if artifactType == "" {
		artifactType = coalesceArtifactString(raw, "type", "json")
	}
	if !validArtifactType(artifactType) {
		artifactType = "json"
	}
	visibility := coalesceArtifactString(raw, "visibility", "private")
	if !validArtifactVisibility(visibility) {
		visibility = "private"
	}
	content := map[string]interface{}{}
	if v, ok := raw["content"].(map[string]interface{}); ok {
		content = v
	} else if v, ok := raw["data"].(map[string]interface{}); ok {
		content = v
	} else {
		for k, v := range raw {
			content[k] = v
		}
	}
	meta := artifactFileMetadataFromMap(raw)
	if meta.FileURI == "" {
		meta = mergeArtifactFileMetadata(meta, artifactFileMetadataFromMap(content))
	}
	if artifactType == "file" {
		if meta.FileURI != "" {
			content["file_uri"] = meta.FileURI
		}
		if meta.FileName != "" {
			content["file_name"] = meta.FileName
		}
		if meta.MimeType != "" {
			content["mime_type"] = meta.MimeType
		}
		if meta.FileSHA256 != "" {
			content["file_sha256"] = meta.FileSHA256
		}
		if meta.FileSizeBytes != nil {
			content["file_size_bytes"] = *meta.FileSizeBytes
		}
	}
	return runArtifactDraft{
		ArtifactType:  artifactType,
		Title:         title,
		Content:       content,
		Visibility:    visibility,
		MimeType:      meta.MimeType,
		FileURI:       meta.FileURI,
		FileName:      meta.FileName,
		FileSHA256:    meta.FileSHA256,
		FileSizeBytes: meta.FileSizeBytes,
	}
}

func artifactDeltaDraftFromPayload(payload map[string]interface{}) runArtifactDeltaDraft {
	if payload == nil {
		payload = map[string]interface{}{}
	}
	sourceID := coalesceArtifactString(payload, "artifact_id", "")
	if sourceID == "" {
		sourceID = coalesceArtifactString(payload, "source_artifact_id", "")
	}
	if sourceID == "" {
		sourceID = coalesceArtifactString(payload, "id", "default")
	}
	artifactType := coalesceArtifactString(payload, "artifact_type", "")
	if artifactType == "" {
		artifactType = coalesceArtifactString(payload, "type", "data")
	}
	if !validArtifactType(artifactType) {
		artifactType = "data"
	}
	visibility := coalesceArtifactString(payload, "visibility", "private")
	if !validArtifactVisibility(visibility) {
		visibility = "private"
	}
	appendChunk := true
	if raw, ok := payload["append"].(bool); ok {
		appendChunk = raw
	}
	lastChunk := false
	if raw, ok := payload["last_chunk"].(bool); ok {
		lastChunk = raw
	}
	title := coalesceArtifactString(payload, "title", "")
	if title == "" {
		title = "Artifact " + normalizeArtifactSourceID(sourceID)
	}
	parts := artifactDeltaPartsFromPayload(payload)
	meta := artifactFileMetadataFromMap(payload)
	if meta.FileURI == "" {
		meta = mergeArtifactFileMetadata(meta, artifactFileMetadataFromParts(parts))
	}
	if artifactType == "data" && meta.FileURI != "" {
		artifactType = "file"
	}
	return runArtifactDeltaDraft{
		SourceArtifactID: normalizeArtifactSourceID(sourceID),
		ArtifactType:     artifactType,
		Title:            normalizeArtifactTitle(title),
		Visibility:       visibility,
		MimeType:         meta.MimeType,
		FileURI:          meta.FileURI,
		FileName:         meta.FileName,
		FileSHA256:       meta.FileSHA256,
		FileSizeBytes:    meta.FileSizeBytes,
		Append:           appendChunk,
		LastChunk:        lastChunk,
		Parts:            parts,
		Payload:          payload,
	}
}

func artifactDeltaPartsFromPayload(payload map[string]interface{}) []interface{} {
	if raw, ok := payload["parts"].([]interface{}); ok && len(raw) > 0 {
		return raw
	}
	for _, key := range []string{"text", "content", "message"} {
		if raw, ok := payload[key]; ok && raw != nil {
			if s, ok := raw.(string); ok {
				return []interface{}{map[string]interface{}{"type": "text", "text": s}}
			}
			return []interface{}{map[string]interface{}{"type": "data", "data": raw}}
		}
	}
	if raw, ok := payload["data"]; ok && raw != nil {
		return []interface{}{map[string]interface{}{"type": "data", "data": raw}}
	}
	return []interface{}{map[string]interface{}{"type": "data", "data": payload}}
}

func mergeArtifactDeltaContent(content map[string]interface{}, draft runArtifactDeltaDraft, chunk db.RunArtifactChunk) map[string]interface{} {
	if content == nil {
		content = map[string]interface{}{}
	}
	content["artifact_id"] = draft.SourceArtifactID
	content["streamed"] = true
	content["complete"] = draft.LastChunk
	content["last_chunk_index"] = chunk.ChunkIndex

	parts := interfaceSliceFromAny(content["parts"])
	chunks := interfaceSliceFromAny(content["chunks"])
	if !draft.Append {
		parts = []interface{}{}
		chunks = []interface{}{}
	}
	parts = append(parts, draft.Parts...)

	chunkItem := map[string]interface{}{
		"index":           chunk.ChunkIndex,
		"append":          draft.Append,
		"last_chunk":      draft.LastChunk,
		"parts":           draft.Parts,
		"checksum_status": chunk.ChecksumStatus,
	}
	if chunk.PartsSha256 != nil {
		chunkItem["parts_sha256"] = *chunk.PartsSha256
	}
	if chunk.PayloadSha256 != nil {
		chunkItem["payload_sha256"] = *chunk.PayloadSha256
	}
	if chunk.DeclaredSha256 != nil {
		chunkItem["declared_sha256"] = *chunk.DeclaredSha256
	}
	if chunk.EventSequence != nil {
		chunkItem["event_sequence"] = *chunk.EventSequence
	}
	chunks = append(chunks, chunkItem)
	content["parts"] = parts
	content["chunks"] = chunks
	if text := artifactTextFromParts(parts); text != "" {
		content["text"] = text
	}
	if draft.MimeType != "" {
		content["mime_type"] = draft.MimeType
	}
	if draft.FileURI != "" {
		content["file_uri"] = draft.FileURI
	}
	if draft.FileName != "" {
		content["file_name"] = draft.FileName
	}
	if draft.FileSHA256 != "" {
		content["file_sha256"] = draft.FileSHA256
	}
	if draft.FileSizeBytes != nil {
		content["file_size_bytes"] = *draft.FileSizeBytes
	}
	if chunk.PartsSha256 != nil {
		content["last_parts_sha256"] = *chunk.PartsSha256
	}
	content["last_checksum_status"] = chunk.ChecksumStatus
	return content
}

func interfaceSliceFromAny(raw interface{}) []interface{} {
	if raw == nil {
		return []interface{}{}
	}
	if items, ok := raw.([]interface{}); ok {
		return items
	}
	return []interface{}{raw}
}

func artifactTextFromParts(parts []interface{}) string {
	var b strings.Builder
	for _, part := range parts {
		switch v := part.(type) {
		case string:
			b.WriteString(v)
		case map[string]interface{}:
			if text, ok := v["text"].(string); ok {
				b.WriteString(text)
			}
		}
	}
	return strings.TrimSpace(b.String())
}

func artifactChunkChecksum(payload map[string]interface{}, partsSHA string) (string, string) {
	raw := firstArtifactString(payload, "parts_sha256", "partsSha256", "chunk_sha256", "chunkSha256", "chunk_parts_sha256", "chunkPartsSha256")
	if raw == "" {
		return "", "not_provided"
	}
	declared := normalizeSHA256(raw)
	if declared == "" {
		return "", "invalid"
	}
	if declared == partsSHA {
		return declared, "verified"
	}
	return declared, "mismatch"
}

func sha256Hex(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

type artifactFileMetadata struct {
	MimeType      string
	FileURI       string
	FileName      string
	FileSHA256    string
	FileSizeBytes *int64
}

func artifactFileMetadataFromMap(raw map[string]interface{}) artifactFileMetadata {
	if raw == nil {
		return artifactFileMetadata{}
	}
	meta := artifactFileMetadata{
		MimeType:   normalizeArtifactMetadataString(firstArtifactString(raw, "mime_type", "mimeType", "content_type", "contentType"), 200),
		FileURI:    normalizeArtifactMetadataString(firstArtifactString(raw, "file_uri", "fileUri", "uri", "url"), 2000),
		FileName:   normalizeArtifactMetadataString(firstArtifactString(raw, "file_name", "fileName", "name", "filename"), 500),
		FileSHA256: normalizeSHA256(firstArtifactString(raw, "file_sha256", "fileSha256", "sha256", "checksum")),
	}
	if size, ok := firstArtifactInt64(raw, "file_size_bytes", "fileSizeBytes", "size_bytes", "sizeBytes", "size"); ok {
		meta.FileSizeBytes = &size
	}
	for _, key := range []string{"file", "file_ref", "fileRef", "binary", "bytes"} {
		if nested, ok := raw[key].(map[string]interface{}); ok {
			meta = mergeArtifactFileMetadata(meta, artifactFileMetadataFromMap(nested))
		}
	}
	return meta
}

func artifactFileMetadataFromParts(parts []interface{}) artifactFileMetadata {
	var meta artifactFileMetadata
	for _, part := range parts {
		m, ok := part.(map[string]interface{})
		if !ok {
			continue
		}
		if file, ok := m["file"].(map[string]interface{}); ok {
			meta = mergeArtifactFileMetadata(meta, artifactFileMetadataFromMap(file))
		}
		meta = mergeArtifactFileMetadata(meta, artifactFileMetadataFromMap(m))
	}
	return meta
}

func mergeArtifactFileMetadata(base, next artifactFileMetadata) artifactFileMetadata {
	if base.MimeType == "" {
		base.MimeType = next.MimeType
	}
	if base.FileURI == "" {
		base.FileURI = next.FileURI
	}
	if base.FileName == "" {
		base.FileName = next.FileName
	}
	if base.FileSHA256 == "" {
		base.FileSHA256 = next.FileSHA256
	}
	if base.FileSizeBytes == nil {
		base.FileSizeBytes = next.FileSizeBytes
	}
	return base
}

func firstArtifactString(raw map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if value, ok := raw[key].(string); ok && strings.TrimSpace(value) != "" {
			return normalizeArtifactMetadataString(value, 2000)
		}
	}
	return ""
}

func firstArtifactInt64(raw map[string]interface{}, keys ...string) (int64, bool) {
	for _, key := range keys {
		switch value := raw[key].(type) {
		case int64:
			if value >= 0 {
				return value, true
			}
		case int:
			if value >= 0 {
				return int64(value), true
			}
		case int32:
			if value >= 0 {
				return int64(value), true
			}
		case float64:
			if value >= 0 {
				return int64(value), true
			}
		case float32:
			if value >= 0 {
				return int64(value), true
			}
		}
	}
	return 0, false
}

func normalizeSHA256(value string) string {
	value = strings.TrimSpace(value)
	if len(value) != 64 {
		return ""
	}
	for _, r := range value {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
			return ""
		}
	}
	return strings.ToLower(value)
}

func normalizeArtifactMetadataString(value string, max int) string {
	value = strings.TrimSpace(value)
	runes := []rune(value)
	if len(runes) > max {
		return string(runes[:max])
	}
	return value
}

func normalizeArtifactSourceID(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		value = "default"
	}
	runes := []rune(value)
	if len(runes) > 200 {
		return string(runes[:200])
	}
	return value
}

func normalizeArtifactTitle(title string) string {
	title = strings.TrimSpace(title)
	if title == "" {
		title = "Agent 产物"
	}
	runes := []rune(title)
	if len(runes) > 200 {
		return string(runes[:200])
	}
	return title
}

func coalesceArtifactString(m map[string]interface{}, key, fallback string) string {
	if v, ok := m[key].(string); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return fallback
}

func validArtifactType(v string) bool {
	switch v {
	case "json", "text", "file", "data":
		return true
	default:
		return false
	}
}

func validArtifactVisibility(v string) bool {
	switch v {
	case "private", "shared", "public_example":
		return true
	default:
		return false
	}
}

// triggerWebhook 已知 agentID + output 的快路径（成功路径用）。
//
// 不阻塞调用响应：起独立 goroutine + 独立 ctx（避免被请求 ctx 取消）。
// 拿到的 run 必须是 finished 之后再读，否则 status / finished_at 都不准。
func (s *Service) triggerWebhook(runID, agentID uuid.UUID, output map[string]interface{}) {
	if s.webhookSvc == nil {
		return
	}
	go func() {
		bgCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		run, err := s.queries.GetRunByID(bgCtx, runID)
		if err != nil {
			log.Error().Err(err).Str("run_id", runID.String()).
				Msg("runtime.triggerWebhook: GetRunByID")
			return
		}
		agent, err := s.queries.GetAgentByID(bgCtx, agentID)
		if err != nil {
			log.Error().Err(err).Str("agent_id", agentID.String()).
				Msg("runtime.triggerWebhook: GetAgentByID")
			return
		}
		if err := s.webhookSvc.EnqueueDelivery(bgCtx, &run, agent.Slug, output); err != nil {
			log.Error().Err(err).Str("run_id", runID.String()).
				Msg("runtime.triggerWebhook: EnqueueDelivery")
		}
	}()
}

// triggerWebhookByRun 失败路径用：output 不存在，agentID 由 run 中带出。
func (s *Service) triggerWebhookByRun(runID uuid.UUID) {
	if s.webhookSvc == nil {
		return
	}
	go func() {
		bgCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		run, err := s.queries.GetRunByID(bgCtx, runID)
		if err != nil {
			log.Error().Err(err).Str("run_id", runID.String()).
				Msg("runtime.triggerWebhookByRun: GetRunByID")
			return
		}
		agent, err := s.queries.GetAgentByID(bgCtx, run.AgentID)
		if err != nil {
			log.Error().Err(err).Str("agent_id", run.AgentID.String()).
				Msg("runtime.triggerWebhookByRun: GetAgentByID")
			return
		}
		// 失败 / 超时：output = nil
		if err := s.webhookSvc.EnqueueDelivery(bgCtx, &run, agent.Slug, nil); err != nil {
			log.Error().Err(err).Str("run_id", runID.String()).
				Msg("runtime.triggerWebhookByRun: EnqueueDelivery")
		}
	}()
}

func (s *Service) triggerTaskCallbackEvent(event *db.RunEvent) {
	if s.taskCallbackSvc == nil || event == nil {
		return
	}
	go func(e db.RunEvent) {
		bgCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := s.taskCallbackSvc.EnqueueRunEvent(bgCtx, e); err != nil {
			log.Error().Err(err).Str("event_id", e.ID.String()).Str("run_id", e.RunID.String()).
				Msg("runtime.triggerTaskCallbackEvent: EnqueueRunEvent")
		}
	}(*event)
}

// triggerDelivery 触发用户侧默认投递（无默认 target 时静默跳过）。
//
// 与 webhook 解耦：用户没配 webhook 但配了 delivery 时也能投。
// 仅在 run 已落库为终态后调用。
func (s *Service) triggerDelivery(runID uuid.UUID) {
	if s.deliverySvc == nil {
		return
	}
	go func() {
		bgCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		run, err := s.queries.GetRunByID(bgCtx, runID)
		if err != nil {
			log.Error().Err(err).Str("run_id", runID.String()).
				Msg("runtime.triggerDelivery: GetRunByID")
			return
		}
		if err := s.deliverySvc.EnqueueIfDefault(bgCtx, &run); err != nil {
			log.Error().Err(err).Str("run_id", runID.String()).
				Msg("runtime.triggerDelivery: EnqueueIfDefault")
		}
	}()
}

func (s *Service) shouldTriggerExternalDelivery(ctx context.Context, runID uuid.UUID) bool {
	_, err := s.queries.GetRunDelegationByChild(ctx, runID)
	if err == nil {
		return false
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		log.Warn().Err(err).Str("run_id", runID.String()).
			Msg("runtime.shouldTriggerExternalDelivery: GetRunDelegationByChild")
	}
	return true
}

func (s *Service) triggerRuntimePullCompletionDelivery(runID, agentID uuid.UUID, output map[string]interface{}, success bool) {
	go func() {
		bgCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if !s.shouldTriggerExternalDelivery(bgCtx, runID) {
			return
		}
		if success {
			s.triggerWebhook(runID, agentID, output)
		} else {
			s.triggerWebhookByRun(runID)
		}
		s.triggerDelivery(runID)
	}()
}

// runToResponse 把 db.Run 转成 RunResponse（GetRun 用）。
//
// 失败的 run 也展示原始 cost_cents=0 还是 cost_cents=原值由产品决定；
// 这里取保守做法：失败时 CostCents = 0（已退款），与同步响应一致。
func runToResponse(r *db.Run) *RunResponse {
	resp := &RunResponse{
		RunID:   r.ID.String(),
		AgentID: r.AgentID.String(),
		Status:  r.Status,
		Source:  r.Source,
	}
	if len(r.Input) > 0 {
		var in map[string]interface{}
		if err := json.Unmarshal(r.Input, &in); err == nil {
			resp.Input = in
		}
	}
	if r.DurationMs != nil {
		resp.DurationMs = *r.DurationMs
	}
	switch r.Status {
	case "success":
		resp.CostCents = r.CostCents
		if len(r.Output) > 0 {
			var out map[string]interface{}
			if err := json.Unmarshal(r.Output, &out); err == nil {
				resp.Output = out
			}
		}
	case "failed", "timeout", "canceled":
		// 已退款
		resp.CostCents = 0
		if r.ErrorCode != nil {
			resp.ErrorCode = *r.ErrorCode
		}
		if r.ErrorMessage != nil {
			resp.ErrorMsg = *r.ErrorMessage
		}
	default:
		// running（极少看到，因为同步返回；防御性兼容）
		resp.CostCents = r.CostCents
	}
	decorateNextAction(resp)
	return resp
}

func runArtifactToResponse(a db.RunArtifact) RunArtifactResponse {
	content := map[string]interface{}{}
	if len(a.Content) > 0 {
		if err := json.Unmarshal(a.Content, &content); err != nil {
			content = map[string]interface{}{"raw": string(a.Content)}
		}
	}
	return RunArtifactResponse{
		ID:               a.ID.String(),
		RunID:            a.RunID.String(),
		ArtifactType:     a.ArtifactType,
		Title:            a.Title,
		Content:          content,
		Visibility:       a.Visibility,
		SourceArtifactID: stringPtrValue(a.SourceArtifactID),
		MimeType:         stringPtrValue(a.MimeType),
		FileURI:          stringPtrValue(a.FileUri),
		FileName:         stringPtrValue(a.FileName),
		FileSHA256:       stringPtrValue(a.FileSha256),
		FileSizeBytes:    a.FileSizeBytes,
		CreatedAt:        a.CreatedAt,
	}
}

func runMessageToResponse(m db.RunMessage) RunMessageResponse {
	payload := map[string]interface{}{}
	if len(m.Payload) > 0 {
		if err := json.Unmarshal(m.Payload, &payload); err != nil {
			payload = map[string]interface{}{"raw": string(m.Payload)}
		}
	}
	return RunMessageResponse{
		ID:            m.ID.String(),
		RunID:         m.RunID.String(),
		EventSequence: m.EventSequence,
		Role:          m.Role,
		Content:       m.Content,
		Payload:       payload,
		CreatedAt:     m.CreatedAt,
	}
}

func decorateNextAction(resp *RunResponse) {
	if resp == nil {
		return
	}
	switch resp.Status {
	case "success":
		resp.NextAction = nextActionForSuccess(resp.Output, resp.ParentRunID, resp.BillingMode)
	case "failed", "timeout":
		resp.NextAction = nextActionForFailure(resp.Status, resp.ErrorCode, resp.ErrorMsg)
	case "running":
		resp.NextAction = &RunNextAction{
			Type:          "wait",
			Label:         "等待运行完成",
			Hint:          "运行仍在进行中。可以保持页面打开接收事件流，或稍后回到运行详情查看终态。",
			Href:          "/run/" + resp.RunID,
			ResourceType:  "run",
			ResourceID:    resp.RunID,
			Source:        "platform",
			RequiresHuman: false,
		}
	default:
		resp.NextAction = nil
	}
}

func runtimePullWaitingNextAction(runID string, agentID uuid.UUID) *RunNextAction {
	return &RunNextAction{
		Type:          "start_runtime_worker",
		Label:         "启动 Agent runtime",
		Hint:          "运行已进入 Agent runtime 队列，但当前没有在线 worker。请启动本地 worker 并保持 WebSocket，必要时降级到 heartbeat/claim 循环。",
		Href:          "/hub/agents/" + agentID.String() + "/onboarding",
		ResourceType:  "run",
		ResourceID:    runID,
		Source:        "agent_runtime",
		RequiresHuman: true,
		AdditionalProps: map[string]interface{}{
			"agent_id": agentID.String(),
		},
	}
}

func nextActionForSuccess(output map[string]interface{}, parentRunID, billingMode string) *RunNextAction {
	if billingMode == "free_delegation" && parentRunID != "" {
		return &RunNextAction{
			Type:          "return_to_parent",
			Label:         "返回父运行",
			Hint:          "这个子运行的结果已经回写到父运行链路，不会单独外部投递。",
			Href:          "/run/" + parentRunID,
			ResourceType:  "run",
			ResourceID:    parentRunID,
			Source:        "platform",
			RequiresHuman: false,
		}
	}
	if action, ok := nextActionFromOutput(output); ok {
		return action
	}
	return &RunNextAction{
		Type:          "review_output",
		Label:         "查看输出并投递",
		Hint:          "运行已完成。可以在本页确认结果，必要时配置投递目标或把结果写回任务详情。",
		Href:          "#delivery",
		ResourceType:  "run",
		Source:        "platform",
		RequiresHuman: true,
	}
}

func nextActionForFailure(status, code, message string) *RunNextAction {
	label := "重试或检查 Agent"
	hint := "运行失败。请检查输入、Agent endpoint 或认证配置，然后重新运行。"
	if status == "timeout" {
		label = "检查超时并重试"
		hint = "Agent 没有在超时时间内返回。请确认 endpoint 响应时间、网络连通性，或改用 runtime_ws/runtime_pull。"
	}
	props := map[string]interface{}{}
	if code != "" {
		props["error_code"] = code
	}
	if message != "" {
		props["error_message"] = message
	}
	return &RunNextAction{
		Type:            "retry",
		Label:           label,
		Hint:            hint,
		Href:            "/market",
		ResourceType:    "agent",
		Source:          "platform",
		RequiresHuman:   true,
		AdditionalProps: props,
	}
}

func nextActionFromOutput(output map[string]interface{}) (*RunNextAction, bool) {
	if output == nil {
		return nil, false
	}
	raw, ok := output["next_action"]
	if !ok {
		return nil, false
	}
	switch v := raw.(type) {
	case string:
		hint := strings.TrimSpace(v)
		if hint == "" {
			return nil, false
		}
		return &RunNextAction{
			Type:          "agent_suggested",
			Label:         "执行 Agent 建议",
			Hint:          hint,
			Source:        "agent",
			RequiresHuman: true,
		}, true
	case map[string]interface{}:
		label := stringFromMap(v, "label")
		hint := stringFromMap(v, "hint")
		if hint == "" {
			hint = stringFromMap(v, "description")
		}
		if label == "" && hint == "" {
			return nil, false
		}
		if label == "" {
			label = "执行 Agent 建议"
		}
		if hint == "" {
			hint = label
		}
		return &RunNextAction{
			Type:            coalesceString(stringFromMap(v, "type"), "agent_suggested"),
			Label:           label,
			Hint:            hint,
			Href:            stringFromMap(v, "href"),
			Method:          stringFromMap(v, "method"),
			RequiresHuman:   true,
			ResourceType:    stringFromMap(v, "resource_type"),
			ResourceID:      stringFromMap(v, "resource_id"),
			Source:          "agent",
			AdditionalProps: v,
		}, true
	default:
		return nil, false
	}
}

func stringFromMap(values map[string]interface{}, key string) string {
	raw, ok := values[key]
	if !ok || raw == nil {
		return ""
	}
	switch v := raw.(type) {
	case string:
		return strings.TrimSpace(v)
	default:
		return strings.TrimSpace(fmt.Sprint(v))
	}
}

func coalesceString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}

func stringPtrValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func stringPtrOrNil(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}

func createRunEvent(ctx context.Context, q *db.Queries, runID uuid.UUID, parentRunID *uuid.UUID, eventType string, payload map[string]interface{}) error {
	_, err := createRunEventRecord(ctx, q, runID, parentRunID, eventType, payload)
	return err
}

func createRunEventRecord(ctx context.Context, q *db.Queries, runID uuid.UUID, parentRunID *uuid.UUID, eventType string, payload map[string]interface{}) (db.RunEvent, error) {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return db.RunEvent{}, err
	}
	return q.CreateRunEvent(ctx, db.CreateRunEventParams{
		RunID:       runID,
		ParentRunID: parentRunID,
		EventType:   eventType,
		Payload:     payloadJSON,
	})
}

func createRunMessage(ctx context.Context, q *db.Queries, runID uuid.UUID, eventSequence *int32, role, content string, payload map[string]interface{}) error {
	if payload == nil {
		payload = map[string]interface{}{}
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	content = truncateRunMessageContent(strings.TrimSpace(content))
	if role == "" {
		role = "agent"
	}
	_, err = q.CreateRunMessage(ctx, db.CreateRunMessageParams{
		RunID:         runID,
		EventSequence: eventSequence,
		Role:          role,
		Content:       content,
		Payload:       payloadJSON,
	})
	return err
}

func (s *Service) recordRunEventBestEffort(ctx context.Context, runID uuid.UUID, eventType string, payload map[string]interface{}) *db.RunEvent {
	event, err := createRunEventRecord(ctx, s.queries, runID, nil, eventType, payload)
	if err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Str("event_type", eventType).
			Msg("runtime.recordRunEventBestEffort")
		return nil
	}
	s.triggerTaskCallbackEvent(&event)
	return &event
}

func (s *Service) touchRuntimeTokenAsync(ctx context.Context, tokenID uuid.UUID) {
	if !s.shouldTouchRuntimeToken(tokenID, time.Now()) {
		return
	}
	s.runBestEffortDBAsync(ctx, runtimeBestEffortWriteTimeout, func(touchCtx context.Context) {
		if err := s.queries.TouchAgentRuntimeToken(touchCtx, tokenID); err != nil {
			log.Error().Err(err).Str("token_id", tokenID.String()).Msg("runtime.touchRuntimeTokenAsync")
		}
	})
}

func (s *Service) shouldTouchRuntimeToken(tokenID uuid.UUID, now time.Time) bool {
	if s == nil || tokenID == uuid.Nil {
		return false
	}
	s.tokenTouchMu.Lock()
	defer s.tokenTouchMu.Unlock()
	if s.tokenTouchAt == nil {
		s.tokenTouchAt = map[uuid.UUID]time.Time{}
	}
	if last, ok := s.tokenTouchAt[tokenID]; ok && now.Sub(last) < runtimeTokenTouchMinInterval {
		return false
	}
	s.tokenTouchAt[tokenID] = now
	return true
}

func (s *Service) recordRunSuccessStatsBestEffort(ctx context.Context, runID, agentID uuid.UUID, revenue int32) {
	statsCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), runtimeBestEffortWriteTimeout)
	defer cancel()
	if err := pgx.BeginFunc(statsCtx, s.pool, func(tx pgx.Tx) error {
		q := s.queries.WithTx(tx)
		if e := q.IncrementAgentStats(statsCtx, db.IncrementAgentStatsParams{
			ID:           agentID,
			RevenueCents: int64(revenue),
		}); e != nil {
			return e
		}
		_, e := q.MarkAgentAvailabilitySuccess(statsCtx, agentID)
		return e
	}); err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Str("agent_id", agentID.String()).
			Msg("runtime.recordRunSuccessStatsBestEffort")
	}
}

func (s *Service) runBestEffortDBAsync(ctx context.Context, timeout time.Duration, fn func(context.Context)) {
	bgCtx := context.WithoutCancel(ctx)
	go func() {
		if s != nil && s.bestEffortDBSem != nil {
			s.bestEffortDBSem <- struct{}{}
			defer func() { <-s.bestEffortDBSem }()
		}
		opCtx, cancel := context.WithTimeout(bgCtx, timeout)
		defer cancel()
		fn(opCtx)
	}()
}

func (s *Service) recordRunMessageBestEffort(ctx context.Context, runID uuid.UUID, eventSequence *int32, role, content string, payload map[string]interface{}) {
	if err := createRunMessage(ctx, s.queries, runID, eventSequence, role, content, payload); err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Msg("runtime.recordRunMessageBestEffort")
	}
}

func (s *Service) recordAgentEventsBestEffort(ctx context.Context, runID uuid.UUID, events []AgentEvent) {
	if len(events) > maxAgentResponseEvents {
		events = events[:maxAgentResponseEvents]
	}
	for _, event := range events {
		eventType := strings.TrimSpace(event.EventType)
		if _, ok := allowedAgentResponseEventTypes[eventType]; !ok {
			log.Warn().Str("run_id", runID.String()).Str("event_type", event.EventType).
				Msg("runtime.recordAgentEventsBestEffort: unsupported event type")
			continue
		}
		payload := event.Payload
		if payload == nil {
			payload = map[string]interface{}{}
		}
		event := s.recordRunEventBestEffort(ctx, runID, eventType, payload)
		var eventSequence *int32
		if event != nil {
			eventSequence = &event.Sequence
		}
		if eventType == "run.message.delta" {
			s.recordRunMessageBestEffort(ctx, runID, eventSequence, "agent", messageContentFromMap(payload), payload)
		}
		if eventType == "run.artifact.delta" {
			s.recordArtifactDeltaBestEffort(ctx, runID, eventSequence, payload)
		}
	}
}

func messageContentFromMap(payload map[string]interface{}) string {
	if payload == nil {
		return ""
	}
	for _, key := range []string{"text", "content", "message", "summary", "query", "prompt"} {
		if raw, ok := payload[key]; ok && raw != nil {
			if s, ok := raw.(string); ok && strings.TrimSpace(s) != "" {
				return truncateRunMessageContent(strings.TrimSpace(s))
			}
		}
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	return truncateRunMessageContent(string(raw))
}

func truncateRunMessageContent(s string) string {
	runes := []rune(s)
	if len(runes) <= maxRunMessageContentLen {
		return s
	}
	return string(runes[:maxRunMessageContentLen])
}

func constantTimeEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func runEventToResponse(event db.RunEvent) RunEventResponse {
	payload := map[string]interface{}{}
	if len(event.Payload) > 0 {
		_ = json.Unmarshal(event.Payload, &payload)
	}
	resp := RunEventResponse{
		EventID:   event.ID.String(),
		RunID:     event.RunID.String(),
		Sequence:  event.Sequence,
		EventType: event.EventType,
		Payload:   payload,
		CreatedAt: event.CreatedAt,
	}
	if event.ParentRunID != nil {
		resp.ParentRunID = event.ParentRunID.String()
	}
	return resp
}

// truncate 截断超长字符串（错误消息防爆）。
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// isTimeoutErr 判断 net 层 timeout（http.Client.Timeout 触发的不一定是 context.DeadlineExceeded）。
func isTimeoutErr(err error) bool {
	type timeoutI interface{ Timeout() bool }
	var t timeoutI
	if errors.As(err, &t) {
		return t.Timeout()
	}
	return false
}

func isContextErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
