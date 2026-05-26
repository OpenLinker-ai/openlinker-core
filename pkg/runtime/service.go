package runtime

import (
	"bytes"
	"context"
	"crypto/subtle"
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

	"github.com/kinzhi/openlinker-core/pkg/config"
	db "github.com/kinzhi/openlinker-core/pkg/db/generated"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

// errInsufficientBalance 内部 sentinel：事务里余额不足时用于跳过 INSERT 并回滚。
var errInsufficientBalance = errors.New("runtime: insufficient balance")

// errMsgMaxLen 错误消息截断长度，避免巨大 body 灌进 DB / 响应。
const errMsgMaxLen = 500

const defaultRunEventsLimit int32 = 100
const maxRunEventsLimit int32 = 500
const maxAgentResponseEvents = 50

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

// Run 调用 Agent。
//
// 流程见 Service 注释。只有明确注入 WalletCharger 后才执行结算。
//
// source 标记调用来源：'web' / 'mcp' / 'api'，写入 runs.source 以便 /usage 分类显示。
// 传空字符串时默认 'web'，便于旧调用方零修改。
func (s *Service) Run(ctx context.Context, userID uuid.UUID, req *RunRequest, source string) (*RunResponse, error) {
	invocation, _, err := s.createRunningRun(ctx, userID, req, source, nil, s.walletCharger != nil)
	if err != nil {
		return nil, err
	}
	return s.executeRun(ctx, invocation), nil
}

// StartRun 创建 running run 并在后台执行；调用方可用 GetRun/ListRunEvents/SSE 查询进度。
func (s *Service) StartRun(ctx context.Context, userID uuid.UUID, req *RunRequest, source string) (*RunResponse, error) {
	invocation, resp, err := s.createRunningRun(ctx, userID, req, source, nil, s.walletCharger != nil)
	if err != nil {
		return nil, err
	}
	s.executeRunAsync(invocation)
	return resp, nil
}

// RunDelegated lets an authenticated Agent call another Agent through the platform.
// Delegated runs are free until explicit user-approved billing exists.
func (s *Service) RunDelegated(ctx context.Context, userID uuid.UUID, delegation Delegation, req *RunRequest) (*RunResponse, error) {
	invocation, _, err := s.createRunningRun(ctx, userID, req, "api", &delegation, false)
	if err != nil {
		return nil, err
	}
	return s.executeRun(ctx, invocation), nil
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
	output, agentEvents, agentErr, callErr := s.callAgentEndpoint(
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
	if callErr != nil || agentErr != nil {
		resp = s.handleFailure(ctx, invocation.runID, invocation.userID, invocation.cost, duration, callErr, agentErr, invocation.settle)
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
	}
	return resp
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
	delegation, err := s.queries.GetRunDelegationByChild(ctx, runID)
	if err == nil {
		resp.ParentRunID = delegation.ParentRunID.String()
		resp.CallerAgentID = delegation.CallerAgentID.String()
		resp.BillingMode = "free_delegation"
		return resp, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		log.Error().Err(err).Str("run_id", runID.String()).Msg("runtime.GetRun: GetRunDelegationByChild")
		return nil, httpx.Internal("查询调用关系失败")
	}
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
	resp := runEventToResponse(event)
	return &resp, nil
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
	output, _, agentErr, callErr := s.callAgentEndpoint(ctx, agent, runID, userID, &RunRequest{
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
		return q.IncrementAgentStats(ctx, db.IncrementAgentStatsParams{
			ID:           agentID,
			RevenueCents: int64(revenue),
		})
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

	if settle {
		// 委派子 run 不自动外发中间产物；最终交付由父 run 决定。
		s.triggerWebhook(runID, agentID, output)
		s.triggerDelivery(runID)
	}

	return &RunResponse{
		RunID:      runID.String(),
		Status:     "success",
		Output:     output,
		CostCents:  cost,
		DurationMs: duration,
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
	runID, userID uuid.UUID,
	cost, duration int32,
	callErr error,
	agentErr *AgentError,
	settle bool,
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
			return nil
		}
		return s.walletCharger.Refund(ctx, tx, userID, int64(cost))
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

	if settle {
		// 失败也触发 webhook（status='failed' / 'timeout'），让创作者侧能感知失败。
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
	case "failed", "timeout":
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
	return resp
}

func createRunEvent(ctx context.Context, q *db.Queries, runID uuid.UUID, parentRunID *uuid.UUID, eventType string, payload map[string]interface{}) error {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = q.CreateRunEvent(ctx, db.CreateRunEventParams{
		RunID:       runID,
		ParentRunID: parentRunID,
		EventType:   eventType,
		Payload:     payloadJSON,
	})
	return err
}

func (s *Service) recordRunEventBestEffort(ctx context.Context, runID uuid.UUID, eventType string, payload map[string]interface{}) {
	if err := createRunEvent(ctx, s.queries, runID, nil, eventType, payload); err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Str("event_type", eventType).
			Msg("runtime.recordRunEventBestEffort")
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
		s.recordRunEventBestEffort(ctx, runID, eventType, payload)
	}
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
