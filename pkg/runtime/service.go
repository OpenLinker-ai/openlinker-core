package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
	"golang.org/x/crypto/bcrypt"

	"github.com/kinzhi/openlinker-core/pkg/config"
	db "github.com/kinzhi/openlinker-core/pkg/db/generated"
	"github.com/kinzhi/openlinker-core/pkg/endpointurl"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

// errInsufficientBalance 内部 sentinel：事务里余额不足时用于跳过 INSERT 并回滚。
var errInsufficientBalance = errors.New("runtime: insufficient balance")

// errMsgMaxLen 错误消息截断长度，避免巨大 body 灌进 DB / 响应。
const errMsgMaxLen = 500

const defaultRunEventsLimit int32 = 100
const maxRunEventsLimit int32 = 500
const maxAgentResponseEvents = 50
const maxRunMessageContentLen = 10000
const runtimePullClaimTTL = 5 * time.Minute

const (
	connectionModeDirectHTTP  = "direct_http"
	connectionModeMCPServer   = "mcp_server"
	connectionModeRuntimePull = "runtime_pull"

	runtimeTokenPrefix     = "rt_live_"
	runtimeTokenPrefixLen  = 12
	runtimeTokenRandomSize = 32
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

// RunWebhookEnqueuer 触发 run 级别 push webhook，payload 来自 run_events。
type RunWebhookEnqueuer interface {
	EnqueueRunEvent(ctx context.Context, event db.RunEvent) error
}

// DeliveryEnqueuer 触发 run 完成后向用户的默认投递目标发送。
//
// 同 WebhookEnqueuer 用接口注入避免 runtime → delivery 硬依赖。
// 实现见 internal/delivery.Service.EnqueueIfDefault。
type DeliveryEnqueuer interface {
	EnqueueIfDefault(ctx context.Context, run *db.Run) error
}

// Service 调用 Agent，并为未来计费保留可选结算能力。
//
// 关键时序约束（见 docs/13 模块 4 / docs/10 章四）：
//  1. 事务 A：可选扣余额 + INSERT runs(status=running)
//  2. 事务外：HTTP POST 创作者 endpoint（60s 超时）
//  3. 事务 B：成功 → MarkRunSuccess + 可选 AddCreatorEarnings + IncrementAgentStats
//     失败 → MarkRunFailed + 可选 RefundUserBalance
//  4. 事务外：异步触发 webhook 投递（不阻塞响应）
//
// HTTP 调用必须在事务外，否则会长时间锁住 wallets 行。
type Service struct {
	queries       *db.Queries
	pool          *pgxpool.Pool
	cfg           *config.Config
	httpClient    *http.Client
	webhookSvc    WebhookEnqueuer
	runWebhookSvc RunWebhookEnqueuer
	deliverySvc   DeliveryEnqueuer
	walletCharger WalletCharger
}

type runInvocation struct {
	runID      uuid.UUID
	userID     uuid.UUID
	agent      db.Agent
	cost       int32
	revenue    int32
	req        *RunRequest
	settle     bool
	delegation *Delegation
}

// Delegation describes an Agent-mediated child run executed within an active parent run.
type Delegation struct {
	ParentRunID   uuid.UUID
	CallerAgentID uuid.UUID
	Reason        string
}

// SetWebhookEnqueuer 注入 webhook 触发器（main.go 启动时调用）。
//
// 用 setter 而非 NewService 参数，避免 runtime ↔ webhook 循环依赖
// （webhook 内部要 import runtime 也不行；用接口隔离）。
func (s *Service) SetWebhookEnqueuer(w WebhookEnqueuer) {
	s.webhookSvc = w
}

// SetRunWebhookEnqueuer 注入 run 级别 push webhook 触发器。
func (s *Service) SetRunWebhookEnqueuer(w RunWebhookEnqueuer) {
	s.runWebhookSvc = w
}

// SetDeliveryEnqueuer 注入用户侧投递触发器（main.go 启动时调用）。
func (s *Service) SetDeliveryEnqueuer(d DeliveryEnqueuer) {
	s.deliverySvc = d
}

// SetWalletCharger 启用未来商业化结算并注入钱包扣费/退款实现。
// 当前 Phase 1 入口不应调用本方法：未注入时运行免费，财务字段记为 0。
func (s *Service) SetWalletCharger(w WalletCharger) {
	s.walletCharger = w
}

// NewService 构造 Service。HTTP client timeout 取自 cfg.RunTimeoutSeconds（默认 60s）。
func NewService(pool *pgxpool.Pool, cfg *config.Config) *Service {
	timeout := time.Duration(cfg.RunTimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	return &Service{
		queries: db.New(pool),
		pool:    pool,
		cfg:     cfg,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

func (s *Service) agentA2AContext(runID uuid.UUID, delegation *Delegation) *AgentA2AContext {
	ctx := &AgentA2AContext{
		CurrentRunID:      runID.String(),
		CallAgentEndpoint: s.callAgentEndpointURL(),
		CallAgentMethod:   "POST",
		RuntimeTokenType:  "rt_live",
		RuntimeScopes:     []string{"agent:call"},
	}
	if delegation != nil {
		ctx.ParentRunID = delegation.ParentRunID.String()
		ctx.CallerAgentID = delegation.CallerAgentID.String()
	}
	return ctx
}

func (s *Service) agentA2AContextForRun(ctx context.Context, runID uuid.UUID) *AgentA2AContext {
	base := s.agentA2AContext(runID, nil)
	delegation, err := s.queries.GetRunDelegationByChild(ctx, runID)
	if err == nil {
		base.ParentRunID = delegation.ParentRunID.String()
		base.CallerAgentID = delegation.CallerAgentID.String()
		return base
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		log.Warn().Err(err).Str("run_id", runID.String()).Msg("runtime.agentA2AContextForRun")
	}
	return base
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
		"runtime_token_type":  ctx.RuntimeTokenType,
		"runtime_scopes":      ctx.RuntimeScopes,
	}
	if ctx.ParentRunID != "" {
		value["parent_run_id"] = ctx.ParentRunID
	}
	if ctx.CallerAgentID != "" {
		value["caller_agent_id"] = ctx.CallerAgentID
	}
	return value
}

// Run 调用 Agent。
//
// 流程见 Service 注释。只有明确注入 WalletCharger 后才执行结算。
//
// source 标记调用来源：'web' / 'mcp' / 'api'，写入 runs.source 以便 /usage 分类显示。
// 传空字符串时默认 'web'，便于旧调用方零修改。
func (s *Service) Run(ctx context.Context, userID uuid.UUID, req *RunRequest, source string) (*RunResponse, error) {
	invocation, resp, err := s.createRunningRun(ctx, userID, req, source, nil, s.walletCharger != nil)
	if err != nil {
		return nil, err
	}
	if s.isRuntimePull(invocation) {
		s.recordRunEventBestEffort(ctx, invocation.runID, "run.dispatch.pending", map[string]interface{}{
			"connection_mode": connectionModeRuntimePull,
			"agent_id":        invocation.agent.ID.String(),
		})
		return resp, nil
	}
	return s.executeRun(ctx, invocation), nil
}

// StartRun 创建 running run 并在后台执行；调用方可用 GetRun/ListRunEvents/SSE 查询进度。
func (s *Service) StartRun(ctx context.Context, userID uuid.UUID, req *RunRequest, source string) (*RunResponse, error) {
	invocation, resp, err := s.createRunningRun(ctx, userID, req, source, nil, s.walletCharger != nil)
	if err != nil {
		return nil, err
	}
	if s.isRuntimePull(invocation) {
		s.recordRunEventBestEffort(ctx, invocation.runID, "run.dispatch.pending", map[string]interface{}{
			"connection_mode": connectionModeRuntimePull,
			"agent_id":        invocation.agent.ID.String(),
		})
		return resp, nil
	}
	s.executeRunAsync(invocation)
	return resp, nil
}

// RunDelegated lets an authenticated Agent call another Agent through the platform.
// Delegated runs are free until explicit user-approved billing exists.
func (s *Service) RunDelegated(ctx context.Context, userID uuid.UUID, delegation Delegation, req *RunRequest) (*RunResponse, error) {
	invocation, resp, err := s.createRunningRun(ctx, userID, req, "api", &delegation, false)
	if err != nil {
		return nil, err
	}
	if s.isRuntimePull(invocation) {
		s.recordRunEventBestEffort(ctx, invocation.runID, "run.dispatch.pending", map[string]interface{}{
			"connection_mode": connectionModeRuntimePull,
			"agent_id":        invocation.agent.ID.String(),
		})
		return resp, nil
	}
	return s.executeRun(ctx, invocation), nil
}

func (s *Service) isRuntimePull(invocation *runInvocation) bool {
	return invocation != nil && invocation.agent.ConnectionMode == connectionModeRuntimePull
}

func (s *Service) createRunningRun(
	ctx context.Context,
	userID uuid.UUID,
	req *RunRequest,
	source string,
	delegation *Delegation,
	settle bool,
) (*runInvocation, *RunResponse, error) {
	if source == "" {
		source = "web"
	}
	switch source {
	case "web", "mcp", "api":
	default:
		return nil, nil, httpx.BadRequest("source 取值非法")
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
	if agent.LifecycleStatus != "active" || agent.Visibility == "private" {
		return nil, nil, httpx.Forbidden("Agent 未公开或已下架")
	}
	if agent.ConnectionMode == "" {
		agent.ConnectionMode = connectionModeDirectHTTP
	}
	if agent.ConnectionMode != connectionModeRuntimePull {
		if err := endpointurl.Validate(agent.EndpointURL, s.cfg.AllowLocalHTTPEndpoints); err != nil {
			log.Warn().Err(err).Str("agent_id", agent.ID.String()).Msg("runtime.Run: endpoint policy rejected")
			return nil, nil, httpx.Forbidden("Agent endpoint 当前不可调用")
		}
	}
	if agent.ConnectionMode == connectionModeMCPServer && (agent.MCPToolName == nil || strings.TrimSpace(*agent.MCPToolName) == "") {
		log.Warn().Str("agent_id", agent.ID.String()).Msg("runtime.Run: missing mcp tool")
		return nil, nil, httpx.Forbidden("Agent MCP tool 未配置")
	}
	requirementSnapshot, err := s.buildRunRequirementSnapshot(ctx, userID, agentID, req, source)
	if err != nil {
		return nil, nil, err
	}

	// 2. 计算费用：抽成 = floor(cost × rate)，creator_revenue = cost - fee
	cost := agent.PricePerCallCents
	fee := int32(float64(cost) * s.cfg.PlatformFeeRate)
	if fee < 0 {
		fee = 0
	}
	if fee > cost {
		fee = cost
	}
	revenue := cost - fee
	if !settle {
		cost = 0
		fee = 0
		revenue = 0
	}

	// 3. 序列化 input 为 JSONB
	inputJSON, err := json.Marshal(req.Input)
	if err != nil {
		return nil, nil, httpx.BadRequest("input 不是合法 JSON")
	}

	// 4. 事务 A：扣余额（如配置了 charger） + 创建 run
	var runID uuid.UUID
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := s.queries.WithTx(tx)

		if settle && s.walletCharger != nil {
			ok, chargeErr := s.walletCharger.Charge(ctx, tx, userID, int64(cost))
			if chargeErr != nil {
				return chargeErr
			}
			if !ok {
				return errInsufficientBalance
			}
		}

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
		var parentRunID *uuid.UUID
		if delegation != nil {
			parentRunID = &delegation.ParentRunID
			if _, createErr = q.CreateRunDelegation(ctx, db.CreateRunDelegationParams{
				ChildRunID:    runID,
				ParentRunID:   delegation.ParentRunID,
				CallerAgentID: delegation.CallerAgentID,
				Reason:        delegation.Reason,
			}); createErr != nil {
				return createErr
			}
			if eventErr := createRunEvent(ctx, q, delegation.ParentRunID, nil, "run.child.created", map[string]interface{}{
				"child_run_id":    runID.String(),
				"caller_agent_id": delegation.CallerAgentID.String(),
				"target_agent_id": agentID.String(),
				"reason":          delegation.Reason,
				"billing_mode":    "free_delegation",
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
		if delegation != nil {
			payload["caller_agent_id"] = delegation.CallerAgentID.String()
			payload["billing_mode"] = "free_delegation"
		}
		if eventErr := createRunEvent(ctx, q, runID, parentRunID, "run.created", payload); eventErr != nil {
			return eventErr
		}
		if eventErr := createRunEvent(ctx, q, runID, parentRunID, "run.started", map[string]interface{}{
			"agent_id": agentID.String(),
			"user_id":  userID.String(),
			"status":   "running",
		}); eventErr != nil {
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
	if errors.Is(err, errInsufficientBalance) {
		return nil, nil, httpx.PaymentRequired("余额不足，请先充值")
	}
	if err != nil {
		log.Error().Err(err).Str("user_id", userID.String()).Str("agent_id", agentID.String()).
			Msg("runtime.Run: pre-call tx")
		return nil, nil, httpx.Internal("创建调用记录失败")
	}

	invocation := &runInvocation{
		runID:      runID,
		userID:     userID,
		agent:      agent,
		cost:       cost,
		revenue:    revenue,
		req:        req,
		settle:     settle,
		delegation: delegation,
	}
	resp := &RunResponse{
		RunID:     runID.String(),
		Status:    "running",
		CostCents: cost,
		Source:    source,
	}
	if delegation != nil {
		resp.ParentRunID = delegation.ParentRunID.String()
		resp.CallerAgentID = delegation.CallerAgentID.String()
		resp.BillingMode = "free_delegation"
	}
	s.attachRunRequirementEvidence(ctx, runID, resp)
	decorateNextAction(resp)
	return invocation, resp, nil
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
	duration := int32(time.Since(started).Milliseconds())

	// 处理结果
	var resp *RunResponse
	triggerExternalDelivery := invocation.delegation == nil
	if callErr != nil || agentErr != nil {
		resp = s.handleFailure(ctx, invocation.runID, invocation.userID, invocation.agent.ID, invocation.cost, duration, callErr, agentErr, invocation.settle, triggerExternalDelivery)
	} else {
		resp = s.handleSuccess(
			ctx,
			invocation.runID,
			invocation.agent.ID,
			invocation.agent.CreatorID,
			invocation.cost,
			invocation.revenue,
			output,
			agentEvents,
			duration,
			invocation.settle,
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
	default:
		return nil, nil, &AgentError{Code: "UNSUPPORTED_CONNECTION_MODE", Message: "Agent connection_mode 不支持"}, nil
	}
}

// GetRun 查单条调用详情；仅 owner 可看。
func (s *Service) GetRun(ctx context.Context, userID, runID uuid.UUID) (*RunResponse, error) {
	r, err := s.queries.GetRunByID(ctx, runID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("调用记录不存在")
		}
		log.Error().Err(err).Str("run_id", runID.String()).Msg("runtime.GetRun: GetRunByID")
		return nil, httpx.Internal("查询调用记录失败")
	}
	if r.UserID != userID {
		// 不暴露存在性，统一 404
		return nil, httpx.NotFound("调用记录不存在")
	}
	resp := runToResponse(&r)
	s.attachRunRequirementEvidence(ctx, runID, resp)
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
		if s.walletCharger != nil && row.CostCents > 0 {
			return s.walletCharger.Refund(ctx, tx, userID, int64(row.CostCents))
		}
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
	s.triggerRunWebhookEvent(&event)
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
func (s *Service) ClaimRuntimePullRun(ctx context.Context, plaintextToken string) (*RuntimePullRunResponse, error) {
	token, err := s.verifyRuntimeToken(ctx, plaintextToken, "agent:pull")
	if err != nil {
		return nil, err
	}
	agent, err := s.queries.GetAgentByID(ctx, token.AgentID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("Agent 不存在")
	}
	if err != nil {
		return nil, httpx.Internal("查询 Agent 失败")
	}
	if agent.ConnectionMode != connectionModeRuntimePull {
		return nil, httpx.Conflict("Agent 不是 runtime_pull 接入模式")
	}
	run, err := s.queries.ClaimRuntimePullRun(ctx, db.ClaimRuntimePullRunParams{
		AgentID:        token.AgentID,
		RuntimeTokenID: token.ID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		_ = s.queries.TouchAgentRuntimeToken(ctx, token.ID)
		return nil, nil
	}
	if err != nil {
		log.Error().Err(err).Str("agent_id", token.AgentID.String()).Msg("runtime.ClaimRuntimePullRun")
		return nil, httpx.Internal("领取任务失败")
	}
	_ = s.queries.TouchAgentRuntimeToken(ctx, token.ID)
	s.recordRunEventBestEffort(ctx, run.ID, "run.dispatch.claimed", map[string]interface{}{
		"agent_id":         token.AgentID.String(),
		"runtime_token_id": token.ID.String(),
	})

	input := map[string]interface{}{}
	if len(run.Input) > 0 {
		_ = json.Unmarshal(run.Input, &input)
	}
	return &RuntimePullRunResponse{
		RunID:    run.ID.String(),
		AgentID:  run.AgentID.String(),
		Input:    input,
		Metadata: map[string]interface{}{"claim_ttl_seconds": int(runtimePullClaimTTL.Seconds())},
		Source:   run.Source,
		A2A:      s.agentA2AContextForRun(ctx, run.ID),
	}, nil
}

// CompleteRuntimePullRun accepts the result of a run previously claimed by the same Runtime Token.
func (s *Service) CompleteRuntimePullRun(ctx context.Context, plaintextToken string, runID uuid.UUID, req *RuntimePullResultRequest) (*RunResponse, error) {
	token, err := s.verifyRuntimeToken(ctx, plaintextToken, "agent:pull")
	if err != nil {
		return nil, err
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
		return nil, httpx.Conflict("run 未被当前 Runtime Token 领取")
	}
	agent, err := s.queries.GetAgentByID(ctx, token.AgentID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("Agent 不存在")
	}
	if err != nil {
		return nil, httpx.Internal("查询 Agent 失败")
	}
	if agent.ConnectionMode != connectionModeRuntimePull {
		return nil, httpx.Conflict("Agent 不是 runtime_pull 接入模式")
	}

	duration := req.DurationMs
	if duration <= 0 {
		duration = int32(time.Since(state.StartedAt).Milliseconds())
	}
	triggerExternalDelivery := s.shouldTriggerExternalDelivery(ctx, runID)
	var resp *RunResponse
	switch req.Status {
	case "success":
		output := req.Output
		if output == nil {
			output = map[string]interface{}{}
		}
		resp = s.handleSuccess(ctx, runID, token.AgentID, agent.CreatorID, state.CostCents, state.CreatorRevenueCents, output, req.Events, duration, false, triggerExternalDelivery)
	case "failed", "timeout":
		agentErr := req.Error
		if agentErr == nil {
			agentErr = &AgentError{Code: "AGENT_REPORTED_FAILURE", Message: "Agent runtime reported " + req.Status}
		}
		resp = s.handleFailure(ctx, runID, state.UserID, token.AgentID, state.CostCents, duration, nil, agentErr, false, triggerExternalDelivery)
	default:
		return nil, httpx.BadRequest("status 取值非法")
	}
	_ = s.queries.TouchAgentRuntimeToken(ctx, token.ID)
	s.decorateDelegationCompletion(ctx, runID, token.AgentID, resp)
	s.attachRunRequirementEvidence(ctx, runID, resp)
	return resp, nil
}

// HeartbeatAgent lets an Agent proactively mark its Runtime Token owner alive.
func (s *Service) HeartbeatAgent(ctx context.Context, plaintextToken string) (*AgentHeartbeatResponse, error) {
	token, err := s.verifyRuntimeTokenAny(ctx, plaintextToken, "agent:pull", "agent:call")
	if err != nil {
		return nil, err
	}
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
	snapshot, err := s.queries.MarkAgentAvailabilityHeartbeat(ctx, token.AgentID)
	if err != nil {
		log.Error().Err(err).Str("agent_id", token.AgentID.String()).Msg("runtime.HeartbeatAgent: MarkAgentAvailabilityHeartbeat")
		return nil, httpx.Internal("记录 Agent heartbeat 失败")
	}
	_ = s.queries.TouchAgentRuntimeToken(ctx, token.ID)
	return &AgentHeartbeatResponse{
		AgentID:             snapshot.AgentID.String(),
		AvailabilityStatus:  snapshot.AvailabilityStatus,
		LastCheckedAt:       snapshot.LastCheckedAt,
		ConsecutiveFailures: snapshot.ConsecutiveFailures,
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

func (s *Service) verifyRuntimeTokenAny(ctx context.Context, plaintext string, acceptedScopes ...string) (db.AgentRuntimeToken, error) {
	plaintext = strings.TrimSpace(plaintext)
	if !strings.HasPrefix(plaintext, runtimeTokenPrefix) ||
		len(plaintext) != len(runtimeTokenPrefix)+runtimeTokenRandomSize*2 {
		return db.AgentRuntimeToken{}, httpx.Unauthorized("Runtime Token 无效或已撤销")
	}
	tokens, err := s.queries.ListActiveAgentRuntimeTokensByPrefix(ctx, plaintext[:runtimeTokenPrefixLen])
	if err != nil {
		return db.AgentRuntimeToken{}, httpx.Unauthorized("Runtime Token 无效或已撤销")
	}
	for _, token := range tokens {
		if bcrypt.CompareHashAndPassword([]byte(token.TokenHash), []byte(plaintext)) == nil &&
			hasAnyRuntimeScope(token.Scopes, acceptedScopes...) {
			return token, nil
		}
	}
	return db.AgentRuntimeToken{}, httpx.Unauthorized("Runtime Token 无效或已撤销")
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
	request := AgentRequest{
		Input:    req.Input,
		Metadata: req.Metadata,
		RunID:    runID.String(),
		A2A:      s.agentA2AContext(runID, delegation),
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

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("read body: %w", err)
	}

	// 尝试解析 JSON；解析失败也视为业务级失败（创作者协议错误）
	var ar AgentResponse
	if uerr := json.Unmarshal(body, &ar); uerr != nil {
		return nil, nil, &AgentError{
			Code:    "INVALID_RESPONSE",
			Message: "agent endpoint 返回非 JSON: " + truncate(string(body), errMsgMaxLen),
		}, nil
	}

	// HTTP 状态码 >= 400 或 body.error 非空都是失败
	if resp.StatusCode >= 400 || ar.Error != nil {
		if ar.Error == nil {
			msg := truncate(string(body), errMsgMaxLen)
			ar.Error = &AgentError{
				Code:    fmt.Sprintf("HTTP_%d", resp.StatusCode),
				Message: msg,
			}
		}
		return nil, nil, ar.Error, nil
	}

	return ar.Output, ar.Events, nil, nil
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
	toolName := strings.TrimSpace("")
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
	metadata["a2a"] = agentA2AContextMap(s.agentA2AContext(runID, delegation))
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

	body, err := io.ReadAll(resp.Body)
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

// handleSuccess 成功路径：MarkRunSuccess + AddCreatorEarnings + IncrementAgentStats（一个事务）。
//
// 即使事务失败也不影响返回结果（用户已收到 output；对账系统补救）。
func (s *Service) handleSuccess(
	ctx context.Context,
	runID, agentID, creatorID uuid.UUID,
	cost, revenue int32,
	output map[string]interface{},
	agentEvents []AgentEvent,
	duration int32,
	settle bool,
	triggerExternalDelivery bool,
) *RunResponse {
	outputJSON, err := json.Marshal(output)
	if err != nil {
		// output 不可序列化属于极端情况；仍返回结果（DB 里 output 留空），不退款
		log.Error().Err(err).Str("run_id", runID.String()).Msg("runtime.handleSuccess: marshal output")
		outputJSON = []byte("null")
	}

	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := s.queries.WithTx(tx)
		if e := q.MarkRunSuccess(ctx, db.MarkRunSuccessParams{
			ID:         runID,
			Output:     outputJSON,
			DurationMs: duration,
		}); e != nil {
			return e
		}
		if settle {
			if e := q.AddCreatorEarnings(ctx, db.AddCreatorEarningsParams{
				UserID:        creatorID,
				EarningsCents: int64(revenue),
			}); e != nil {
				return e
			}
		}
		if e := q.IncrementAgentStats(ctx, db.IncrementAgentStatsParams{
			ID:           agentID,
			RevenueCents: int64(revenue),
		}); e != nil {
			return e
		}
		if _, e := q.MarkAgentAvailabilitySuccess(ctx, agentID); e != nil {
			return e
		}
		if e := s.createRunArtifacts(ctx, q, runID, output); e != nil {
			return e
		}
		return nil
	})
	if err != nil {
		// 已扣钱、已得到结果，事务失败仅影响分账与统计；记日志由对账补救
		log.Error().Err(err).Str("run_id", runID.String()).
			Str("creator_id", creatorID.String()).Int32("revenue_cents", revenue).
			Msg("runtime.handleSuccess: settle tx failed")
	} else {
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
		CostCents:  cost,
		DurationMs: duration,
		NextAction: nextActionForSuccess(output, "", ""),
	}
}

// handleFailure 失败路径：MarkRunFailed + RefundUserBalance（一个事务）。
//
// 错误分类：
//   - context.DeadlineExceeded → 'timeout' / TIMEOUT
//   - 其他网络层错误 → 'failed' / CONNECTION_ERROR
//   - 创作者业务错误 → 'failed' / 透传 agentErr.Code
func (s *Service) handleFailure(
	ctx context.Context,
	runID, userID, agentID uuid.UUID,
	cost, duration int32,
	callErr error,
	agentErr *AgentError,
	settle bool,
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
		if !settle || s.walletCharger == nil {
			_, e := q.MarkAgentAvailabilityFailure(ctx, agentID)
			return e
		}
		if e := s.walletCharger.Refund(ctx, tx, userID, int64(cost)); e != nil {
			return e
		}
		_, e := q.MarkAgentAvailabilityFailure(ctx, agentID)
		return e
	})
	if err != nil {
		// 退款失败：用户钱包未回滚，对账系统补救（极少见，DB 故障）
		log.Error().Err(err).Str("run_id", runID.String()).
			Str("user_id", userID.String()).Int32("cost_cents", cost).
			Msg("runtime.handleFailure: refund tx failed")
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
		CostCents:  0, // 失败已退款，对外口径不收钱
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

func (s *Service) triggerRunWebhookEvent(event *db.RunEvent) {
	if s.runWebhookSvc == nil || event == nil {
		return
	}
	go func(e db.RunEvent) {
		bgCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := s.runWebhookSvc.EnqueueRunEvent(bgCtx, e); err != nil {
			log.Error().Err(err).Str("event_id", e.ID.String()).Str("run_id", e.RunID.String()).
				Msg("runtime.triggerRunWebhookEvent: EnqueueRunEvent")
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

// runToResponse 把 db.Run 转成 RunResponse（GetRun 用）。
//
// 失败的 run 也展示原始 cost_cents=0 还是 cost_cents=原值由产品决定；
// 这里取保守做法：失败时 CostCents = 0（已退款），与同步响应一致。
func runToResponse(r *db.Run) *RunResponse {
	resp := &RunResponse{
		RunID:  r.ID.String(),
		Status: r.Status,
		Source: r.Source,
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
		hint = "Agent 没有在超时时间内返回。请确认 endpoint 响应时间、网络连通性或改用 runtime_pull。"
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
	s.triggerRunWebhookEvent(&event)
	return &event
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
