package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
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

	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/endpointurl"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

// 投递相关常量。
const (
	// maxAttempts 最大尝试次数（首次 + 2 次重试）。
	maxAttempts = 3

	// httpTimeout 单次投递 HTTP 超时（PRD 约定 15s）。
	httpTimeout = 15 * time.Second

	// responseBodyMaxLen 截断响应 body 的最大长度（防止巨大日志）。
	responseBodyMaxLen = 1024

	// errorMessageMaxLen 错误信息截断长度。
	errorMessageMaxLen = 500

	// secretByteLen 32 字节随机数 → 64 hex 字符 secret。
	secretByteLen = 32

	// defaultListLimit GET /deliveries 默认条数。
	defaultListLimit = 20
	// maxListLimit 最大条数。
	maxListLimit = 100

	// userAgent 投递时 UA。
	userAgent = "OpenLinker-Webhook/1.0"

	// eventRunCompleted run 结束事件名（成功 / 失败 / 超时 共用此 event）。
	eventRunCompleted = "run.completed"
)

// nextRetryDelay 根据已尝试次数返回下次重试间隔。
//
// attempt 是已经发生过的尝试次数：
//
//	0 → 首次失败之后 → 1 min 后重试（attempt 变成 1）
//	1 → 第 2 次失败之后 → 5 min 后重试（attempt 变成 2）
//	2 → 第 3 次失败之后 → 0（不再重试，标记 final failed）
func nextRetryDelay(attempt int) time.Duration {
	switch attempt {
	case 0:
		return 1 * time.Minute
	case 1:
		return 5 * time.Minute
	case 2:
		return 30 * time.Minute
	default:
		return 0
	}
}

// Service webhook 业务逻辑层。
//
// 关键约束（见 docs/13 子轮 2.1）：
//   - 投递不阻塞 /run 响应（runtime 用 goroutine 调 EnqueueDelivery）
//   - HTTP 超时 15s
//   - 失败按 1min / 5min / 30min 三次重试，第 3 次失败终态 failed
//   - 签名 HMAC-SHA256，header X-OpenLinker-Signature: sha256=<hex>
type Service struct {
	queries        webhookQueries
	pool           *pgxpool.Pool
	httpClient     *http.Client
	allowLocalHTTP bool
}

type webhookQueries interface {
	SetAgentWebhook(context.Context, db.SetAgentWebhookParams) (int64, error)
	ClearAgentWebhook(context.Context, db.ClearAgentWebhookParams) (int64, error)
	GetAgentWebhookConfig(context.Context, uuid.UUID) (db.GetAgentWebhookConfigRow, error)
	CreateWebhookDelivery(context.Context, db.CreateWebhookDeliveryParams) (db.WebhookDelivery, error)
	GetWebhookDeliveryByID(context.Context, uuid.UUID) (db.GetWebhookDeliveryRow, error)
	MarkDeliverySuccess(context.Context, db.MarkDeliverySuccessParams) error
	MarkDeliveryFailedRetry(context.Context, db.MarkDeliveryFailedRetryParams) error
	MarkDeliveryFailedFinal(context.Context, db.MarkDeliveryFailedFinalParams) error
	ListDeliveriesByAgent(context.Context, db.ListDeliveriesByAgentParams) ([]db.WebhookDelivery, error)
	GetRunByID(context.Context, uuid.UUID) (db.Run, error)
	CreateTaskCallbackSubscription(context.Context, db.CreateTaskCallbackSubscriptionParams) (db.TaskCallbackSubscription, error)
	GetLatestRunEventForTypes(context.Context, db.GetLatestRunEventForTypesParams) (db.RunEvent, error)
	ListTaskCallbackSubscriptionsByRun(context.Context, db.ListTaskCallbackSubscriptionsByRunParams) ([]db.TaskCallbackSubscription, error)
	ListTaskCallbackSubscriptionsByOwner(context.Context, db.ListTaskCallbackSubscriptionsByOwnerParams) ([]db.TaskCallbackSubscription, error)
	BatchUpdateTaskCallbackSubscriptionsForOwner(context.Context, db.BatchUpdateTaskCallbackSubscriptionsForOwnerParams) ([]db.TaskCallbackSubscription, error)
	UpdateTaskCallbackSubscriptionStatusForOwner(context.Context, db.UpdateTaskCallbackSubscriptionStatusForOwnerParams) (db.TaskCallbackSubscription, error)
	DeleteTaskCallbackSubscriptionForOwner(context.Context, db.DeleteTaskCallbackSubscriptionForOwnerParams) (int64, error)
	ListActiveTaskCallbackSubscriptionsForEvent(context.Context, db.ListActiveTaskCallbackSubscriptionsForEventParams) ([]db.TaskCallbackSubscription, error)
	CreateTaskCallbackDelivery(context.Context, db.CreateTaskCallbackDeliveryParams) (db.TaskCallbackDelivery, error)
	GetTaskCallbackDeliveryByID(context.Context, uuid.UUID) (db.GetTaskCallbackDeliveryByIDRow, error)
	ListTaskCallbackDeliveriesByRun(context.Context, db.ListTaskCallbackDeliveriesByRunParams) ([]db.ListTaskCallbackDeliveriesByRunRow, error)
	MarkTaskCallbackDeliverySuccess(context.Context, db.MarkTaskCallbackDeliverySuccessParams) error
	MarkTaskCallbackDeliveryFailedRetry(context.Context, db.MarkTaskCallbackDeliveryFailedRetryParams) error
	MarkTaskCallbackDeliveryFailedFinal(context.Context, db.MarkTaskCallbackDeliveryFailedFinalParams) error
	IncrementTaskCallbackSubscriptionFailure(context.Context, uuid.UUID) error
	ResetTaskCallbackSubscriptionFailures(context.Context, uuid.UUID) error
	ListPendingTaskCallbackDeliveries(context.Context) ([]db.TaskCallbackDelivery, error)
}

// NewService 构造 Service。
func NewService(pool *pgxpool.Pool, cfg ...*config.Config) *Service {
	s := &Service{
		queries: db.New(pool),
		pool:    pool,
		httpClient: &http.Client{
			Timeout: httpTimeout,
		},
	}
	if len(cfg) > 0 && cfg[0] != nil {
		s.allowLocalHTTP = cfg[0].AllowLocalHTTPEndpoints
	}
	return s
}

// SetWebhook 创作者设置 webhook_url，生成新 secret 并返回（仅本次返回）。
//
// 校验：
//  1. agent 必须存在
//  2. agent.creator_id == userID（防越权）
//  3. URL 必须 https（schema CHECK 兜底，但前置返回友好错误）
func (s *Service) SetWebhook(ctx context.Context, agentID, userID uuid.UUID, url string) (*SetWebhookResponse, error) {
	if !strings.HasPrefix(url, "https://") {
		return nil, httpx.BadRequest("webhook_url 必须以 https:// 开头")
	}
	if len(url) > 500 {
		return nil, httpx.BadRequest("webhook_url 过长")
	}

	// 先检查 agent 归属（避免泄露存在性差异）
	agent, err := s.queries.GetAgentWebhookConfig(ctx, agentID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("Agent 不存在")
		}
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("webhook.SetWebhook: GetAgentWebhookConfig")
		return nil, httpx.Internal("查询 Agent 失败")
	}
	if agent.CreatorID != userID {
		return nil, httpx.NotFound("Agent 不存在")
	}

	secret, err := generateSecret()
	if err != nil {
		log.Error().Err(err).Msg("webhook.SetWebhook: generateSecret")
		return nil, httpx.Internal("生成 secret 失败")
	}

	urlPtr := url
	secretPtr := secret
	rows, err := s.queries.SetAgentWebhook(ctx, db.SetAgentWebhookParams{
		ID:            agentID,
		WebhookURL:    &urlPtr,
		WebhookSecret: &secretPtr,
		CreatorID:     userID,
	})
	if err != nil {
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("webhook.SetWebhook: SetAgentWebhook")
		return nil, httpx.Internal("保存 webhook 失败")
	}
	if rows == 0 {
		// 极少：刚才确认归属、这里却 0 行（并发被删等）。统一 404
		return nil, httpx.NotFound("Agent 不存在")
	}

	return &SetWebhookResponse{
		URL:    url,
		Secret: secret,
	}, nil
}

// ClearWebhook 清除 webhook 配置（url 与 secret 都置 NULL）。
func (s *Service) ClearWebhook(ctx context.Context, agentID, userID uuid.UUID) error {
	rows, err := s.queries.ClearAgentWebhook(ctx, db.ClearAgentWebhookParams{
		ID:        agentID,
		CreatorID: userID,
	})
	if err != nil {
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("webhook.ClearWebhook")
		return httpx.Internal("清除 webhook 失败")
	}
	if rows == 0 {
		return httpx.NotFound("Agent 不存在")
	}
	return nil
}

// RotateSecret 重新生成 secret，保留原 url。
//
// agent 必须已配 webhook_url；否则 404。
func (s *Service) RotateSecret(ctx context.Context, agentID, userID uuid.UUID) (*SetWebhookResponse, error) {
	agent, err := s.queries.GetAgentWebhookConfig(ctx, agentID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("Agent 不存在")
		}
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("webhook.RotateSecret: GetAgentWebhookConfig")
		return nil, httpx.Internal("查询 Agent 失败")
	}
	if agent.CreatorID != userID {
		return nil, httpx.NotFound("Agent 不存在")
	}
	if agent.WebhookURL == nil || *agent.WebhookURL == "" {
		return nil, httpx.BadRequest("尚未配置 webhook_url")
	}

	secret, err := generateSecret()
	if err != nil {
		log.Error().Err(err).Msg("webhook.RotateSecret: generateSecret")
		return nil, httpx.Internal("生成 secret 失败")
	}

	url := *agent.WebhookURL
	urlPtr := url
	secretPtr := secret
	rows, err := s.queries.SetAgentWebhook(ctx, db.SetAgentWebhookParams{
		ID:            agentID,
		WebhookURL:    &urlPtr,
		WebhookSecret: &secretPtr,
		CreatorID:     userID,
	})
	if err != nil {
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("webhook.RotateSecret: SetAgentWebhook")
		return nil, httpx.Internal("更新 secret 失败")
	}
	if rows == 0 {
		return nil, httpx.NotFound("Agent 不存在")
	}

	return &SetWebhookResponse{URL: url, Secret: secret}, nil
}

// EnqueueDelivery handles the legacy Agent webhook queue.
//
// 流程：
//  1. 查 legacy Agent webhook URL：NULL → 直接 return（不投递）
//  2. 构造 payload（event=run.completed）
//  3. INSERT webhook_deliveries (status=pending, next_retry_at=NOW())
//  4. 立即异步触发第一次投递（不等待）
func (s *Service) EnqueueDelivery(ctx context.Context, run *db.Run, agentSlug string, output map[string]interface{}) error {
	cfg, err := s.queries.GetAgentWebhookConfig(ctx, run.AgentID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil // agent 不存在不抛错（极端：刚被删）
		}
		return fmt.Errorf("get agent webhook config: %w", err)
	}
	if cfg.WebhookURL == nil || *cfg.WebhookURL == "" {
		return nil // 未配置 webhook，跳过
	}

	// 构造 payload
	payload := buildPayload(run, agentSlug, output)
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	delivery, err := s.queries.CreateWebhookDelivery(ctx, db.CreateWebhookDeliveryParams{
		AgentID: run.AgentID,
		RunID:   run.ID,
		URL:     *cfg.WebhookURL,
		Payload: payloadBytes,
	})
	if err != nil {
		return fmt.Errorf("create webhook delivery: %w", err)
	}

	// 立即第一次投递（独立 goroutine 与新 ctx，避免被父 ctx 提前取消）
	go func(deliveryID uuid.UUID) {
		bgCtx, cancel := context.WithTimeout(context.Background(), httpTimeout+5*time.Second)
		defer cancel()
		if err := s.AttemptDelivery(bgCtx, deliveryID); err != nil {
			log.Error().Err(err).Str("delivery_id", deliveryID.String()).
				Msg("webhook.EnqueueDelivery: first attempt failed")
		}
	}(delivery.ID)

	return nil
}

// AttemptDelivery 单次投递。返回 nil 表示已正确处理 DB 状态（成功 / 已写重试时间 / 已 final）。
//
// 流程：
//  1. 取 delivery（含 secret）
//  2. 若已 success/failed 则跳过（worker 并发兜底）
//  3. 若 secret 为 NULL（agent 已 ClearWebhook），直接 final failed
//  4. HTTP POST 带签名
//  5. 根据 HTTP 结果决定 success / retry / final fail
func (s *Service) AttemptDelivery(ctx context.Context, deliveryID uuid.UUID) error {
	row, err := s.queries.GetWebhookDeliveryByID(ctx, deliveryID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("get delivery: %w", err)
	}

	// 已终态（worker 与立即投递并发兜底）
	if row.Status != "pending" {
		return nil
	}

	// secret 在 attempt 期间被清除：标记 final failed
	if row.WebhookSecret == nil || *row.WebhookSecret == "" {
		errMsg := "webhook secret 已被清除"
		return s.queries.MarkDeliveryFailedFinal(ctx, db.MarkDeliveryFailedFinalParams{
			ID:           deliveryID,
			ErrorMessage: &errMsg,
		})
	}

	// 发请求
	statusCode, respBody, attemptErr := s.doDeliver(ctx, row.URL, *row.WebhookSecret, deliveryID, row.Payload)

	// 成功（2xx）
	if attemptErr == nil && statusCode >= 200 && statusCode < 300 {
		statusPtr := int32(statusCode)
		bodyPtr := truncate(respBody, responseBodyMaxLen)
		return s.queries.MarkDeliverySuccess(ctx, db.MarkDeliverySuccessParams{
			ID:             deliveryID,
			ResponseStatus: &statusPtr,
			ResponseBody:   &bodyPtr,
		})
	}

	// 失败：分支决定 retry 还是 final
	var (
		statusPtr *int32
		bodyPtr   *string
		errMsg    string
	)
	if statusCode > 0 {
		v := int32(statusCode)
		statusPtr = &v
		b := truncate(respBody, responseBodyMaxLen)
		bodyPtr = &b
	}
	if attemptErr != nil {
		errMsg = truncate(attemptErr.Error(), errorMessageMaxLen)
	} else {
		errMsg = fmt.Sprintf("HTTP %d", statusCode)
	}
	errMsgPtr := errMsg

	// row.AttemptCount 是当前已尝试次数（本次投递前的值）
	currAttempt := int(row.AttemptCount) // 0 / 1 / 2
	delay := nextRetryDelay(currAttempt)
	// 如果已 attempt >= maxAttempts - 1，本次失败后视为 final
	if currAttempt+1 >= maxAttempts || delay == 0 {
		return s.queries.MarkDeliveryFailedFinal(ctx, db.MarkDeliveryFailedFinalParams{
			ID:             deliveryID,
			ResponseStatus: statusPtr,
			ResponseBody:   bodyPtr,
			ErrorMessage:   &errMsgPtr,
		})
	}

	nextAt := time.Now().Add(delay)
	return s.queries.MarkDeliveryFailedRetry(ctx, db.MarkDeliveryFailedRetryParams{
		ID:             deliveryID,
		ResponseStatus: statusPtr,
		ResponseBody:   bodyPtr,
		ErrorMessage:   &errMsgPtr,
		NextRetryAt:    nextAt,
	})
}

// ListDeliveries 创作者查看 agent 投递历史。
func (s *Service) ListDeliveries(ctx context.Context, agentID, userID uuid.UUID, limit int) ([]DeliveryListItem, error) {
	if limit <= 0 || limit > maxListLimit {
		limit = defaultListLimit
	}

	// 校验归属
	cfg, err := s.queries.GetAgentWebhookConfig(ctx, agentID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("Agent 不存在")
		}
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("webhook.ListDeliveries: GetAgentWebhookConfig")
		return nil, httpx.Internal("查询 Agent 失败")
	}
	if cfg.CreatorID != userID {
		return nil, httpx.NotFound("Agent 不存在")
	}

	rows, err := s.queries.ListDeliveriesByAgent(ctx, db.ListDeliveriesByAgentParams{
		AgentID: agentID,
		Limit:   int32(limit),
	})
	if err != nil {
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("webhook.ListDeliveries")
		return nil, httpx.Internal("查询投递历史失败")
	}
	out := make([]DeliveryListItem, 0, len(rows))
	for _, r := range rows {
		out = append(out, toDeliveryListItem(r))
	}
	return out, nil
}

// CreateTaskCallbackSubscription registers a signed caller-owned callback for one run.
func (s *Service) CreateTaskCallbackSubscription(ctx context.Context, runID, userID uuid.UUID, req *CreateTaskCallbackRequest) (*TaskCallbackSubscriptionResponse, error) {
	if req == nil {
		return nil, httpx.BadRequest("请求体不能为空")
	}
	targetURL := strings.TrimSpace(req.URL)
	if err := endpointurl.Validate(targetURL, s.allowLocalHTTP); err != nil {
		return nil, httpx.BadRequest("target_url 必须是 HTTPS；本地开发需开启 ALLOW_LOCAL_HTTP_ENDPOINTS 后才允许 loopback HTTP")
	}
	eventTypes := normalizeTaskCallbackEventTypes(req.EventTypes)
	authScheme, authCredentials := normalizeCallbackAuth(req.AuthScheme, req.AuthCredentials)
	metadataMap := req.Metadata
	if metadataMap == nil {
		metadataMap = map[string]interface{}{}
	}
	metadata, err := json.Marshal(metadataMap)
	if err != nil {
		return nil, httpx.BadRequest("metadata 格式错误")
	}

	run, err := s.queries.GetRunByID(ctx, runID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("调用记录不存在")
		}
		log.Error().Err(err).Str("run_id", runID.String()).Msg("webhook.CreateTaskCallbackSubscription: GetRunByID")
		return nil, httpx.Internal("查询调用记录失败")
	}
	if run.UserID != userID {
		return nil, httpx.NotFound("调用记录不存在")
	}

	secret, err := generateSecret()
	if err != nil {
		return nil, httpx.Internal("生成 task callback secret 失败")
	}
	sub, err := s.queries.CreateTaskCallbackSubscription(ctx, db.CreateTaskCallbackSubscriptionParams{
		RunID:           runID,
		OwnerUserID:     userID,
		TargetURL:       targetURL,
		Secret:          secret,
		EventTypes:      eventTypes,
		AuthScheme:      authScheme,
		AuthCredentials: authCredentials,
		Metadata:        metadata,
	})
	if err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Msg("webhook.CreateTaskCallbackSubscription: insert")
		return nil, httpx.Internal("创建 task callback 失败")
	}

	// If matching events already exist, enqueue the latest one immediately so late subscribers can catch up.
	if event, err := s.queries.GetLatestRunEventForTypes(ctx, db.GetLatestRunEventForTypesParams{
		RunID:      runID,
		EventTypes: eventTypes,
	}); err == nil {
		_ = s.enqueueTaskCallbackDelivery(ctx, sub, event)
	} else if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		log.Error().Err(err).Str("run_id", runID.String()).Msg("webhook.CreateTaskCallbackSubscription: GetLatestRunEventForTypes")
	}

	resp := taskCallbackSubscriptionToResponse(sub)
	resp.Secret = secret
	return &resp, nil
}

// ListTaskCallbackSubscriptions returns active/non-deleted task callbacks.
func (s *Service) ListTaskCallbackSubscriptions(ctx context.Context, runID, userID uuid.UUID) ([]TaskCallbackSubscriptionResponse, error) {
	if err := s.ensureRunOwner(ctx, runID, userID); err != nil {
		return nil, err
	}
	rows, err := s.queries.ListTaskCallbackSubscriptionsByRun(ctx, db.ListTaskCallbackSubscriptionsByRunParams{
		RunID:       runID,
		OwnerUserID: userID,
	})
	if err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Msg("webhook.ListTaskCallbackSubscriptions")
		return nil, httpx.Internal("查询 task callback 失败")
	}
	items := make([]TaskCallbackSubscriptionResponse, 0, len(rows))
	for _, row := range rows {
		items = append(items, taskCallbackSubscriptionToResponse(row))
	}
	return items, nil
}

func (s *Service) ListTaskCallbackDeliveries(ctx context.Context, runID, userID uuid.UUID, limit int) ([]TaskCallbackDeliveryResponse, error) {
	if err := s.ensureRunOwner(ctx, runID, userID); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > maxListLimit {
		limit = defaultListLimit
	}
	rows, err := s.queries.ListTaskCallbackDeliveriesByRun(ctx, db.ListTaskCallbackDeliveriesByRunParams{
		RunID:       runID,
		OwnerUserID: userID,
		Limit:       int32(limit),
	})
	if err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Msg("webhook.ListTaskCallbackDeliveries")
		return nil, httpx.Internal("查询 task callback 投递记录失败")
	}
	items := make([]TaskCallbackDeliveryResponse, 0, len(rows))
	for _, row := range rows {
		items = append(items, taskCallbackDeliveryToResponse(row))
	}
	return items, nil
}

func (s *Service) ListTaskCallbackSubscriptionsForOwner(ctx context.Context, userID uuid.UUID, status string, limit int) ([]TaskCallbackSubscriptionResponse, error) {
	status = strings.TrimSpace(status)
	if status != "" && status != "active" && status != "paused" && status != "failed" {
		return nil, httpx.BadRequest("status 只能是 active、paused 或 failed")
	}
	if limit <= 0 || limit > maxListLimit {
		limit = defaultListLimit
	}
	rows, err := s.queries.ListTaskCallbackSubscriptionsByOwner(ctx, db.ListTaskCallbackSubscriptionsByOwnerParams{
		OwnerUserID: userID,
		Status:      status,
		Limit:       int32(limit),
	})
	if err != nil {
		log.Error().Err(err).Str("user_id", userID.String()).Str("status", status).Msg("webhook.ListTaskCallbackSubscriptionsForOwner")
		return nil, httpx.Internal("查询 task callback 失败")
	}
	items := make([]TaskCallbackSubscriptionResponse, 0, len(rows))
	for _, row := range rows {
		items = append(items, taskCallbackSubscriptionToResponse(row))
	}
	return items, nil
}

func (s *Service) BatchManageTaskCallbackSubscriptions(ctx context.Context, userID uuid.UUID, req *BatchTaskCallbackSubscriptionsRequest) (*BatchTaskCallbackSubscriptionsResponse, error) {
	if req == nil {
		return nil, httpx.BadRequest("请求体不能为空")
	}
	status, err := batchActionToTaskCallbackStatus(req.Action)
	if err != nil {
		return nil, err
	}
	ids, err := parseTaskCallbackSubscriptionIDs(req.SubscriptionIDs)
	if err != nil {
		return nil, err
	}
	rows, err := s.queries.BatchUpdateTaskCallbackSubscriptionsForOwner(ctx, db.BatchUpdateTaskCallbackSubscriptionsForOwnerParams{
		OwnerUserID: userID,
		IDs:         ids,
		Status:      status,
	})
	if err != nil {
		log.Error().Err(err).Str("user_id", userID.String()).Str("action", req.Action).Msg("webhook.BatchManageTaskCallbackSubscriptions")
		return nil, httpx.Internal("批量更新 task callback 失败")
	}
	items := make([]TaskCallbackSubscriptionResponse, 0, len(rows))
	for _, row := range rows {
		items = append(items, taskCallbackSubscriptionToResponse(row))
	}
	return &BatchTaskCallbackSubscriptionsResponse{
		Action:       strings.TrimSpace(req.Action),
		UpdatedCount: len(items),
		Items:        items,
	}, nil
}

// UpdateTaskCallbackSubscriptionStatus pauses or resumes a task callback.
func (s *Service) UpdateTaskCallbackSubscriptionStatus(ctx context.Context, runID, subscriptionID, userID uuid.UUID, status string) (*TaskCallbackSubscriptionResponse, error) {
	if status != "active" && status != "paused" {
		return nil, httpx.BadRequest("status 只能是 active 或 paused")
	}
	if err := s.ensureRunOwner(ctx, runID, userID); err != nil {
		return nil, err
	}
	sub, err := s.queries.UpdateTaskCallbackSubscriptionStatusForOwner(ctx, db.UpdateTaskCallbackSubscriptionStatusForOwnerParams{
		ID:          subscriptionID,
		RunID:       runID,
		OwnerUserID: userID,
		Status:      status,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("task callback 不存在")
		}
		log.Error().Err(err).Str("run_id", runID.String()).Str("subscription_id", subscriptionID.String()).Str("status", status).
			Msg("webhook.UpdateTaskCallbackSubscriptionStatus")
		return nil, httpx.Internal("更新 task callback 状态失败")
	}
	resp := taskCallbackSubscriptionToResponse(sub)
	return &resp, nil
}

// DeleteTaskCallbackSubscription soft-deletes a task callback.
func (s *Service) DeleteTaskCallbackSubscription(ctx context.Context, runID, subscriptionID, userID uuid.UUID) error {
	if err := s.ensureRunOwner(ctx, runID, userID); err != nil {
		return err
	}
	affected, err := s.queries.DeleteTaskCallbackSubscriptionForOwner(ctx, db.DeleteTaskCallbackSubscriptionForOwnerParams{
		ID:          subscriptionID,
		RunID:       runID,
		OwnerUserID: userID,
	})
	if err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Str("subscription_id", subscriptionID.String()).Msg("webhook.DeleteTaskCallbackSubscription")
		return httpx.Internal("删除 task callback 失败")
	}
	if affected == 0 {
		return httpx.NotFound("task callback 不存在")
	}
	return nil
}

// EnqueueRunEvent creates deliveries for all subscriptions interested in this run_event.
func (s *Service) EnqueueRunEvent(ctx context.Context, event db.RunEvent) error {
	subs, err := s.queries.ListActiveTaskCallbackSubscriptionsForEvent(ctx, db.ListActiveTaskCallbackSubscriptionsForEventParams{
		RunID:     event.RunID,
		EventType: event.EventType,
	})
	if err != nil {
		return fmt.Errorf("list task callback subscriptions: %w", err)
	}
	for _, sub := range subs {
		if err := s.enqueueTaskCallbackDelivery(ctx, sub, event); err != nil {
			log.Error().Err(err).Str("subscription_id", sub.ID.String()).Str("event_id", event.ID.String()).
				Msg("webhook.EnqueueRunEvent: enqueue delivery")
		}
	}
	return nil
}

func (s *Service) enqueueTaskCallbackDelivery(ctx context.Context, sub db.TaskCallbackSubscription, event db.RunEvent) error {
	payload := taskCallbackPayload(sub, event)
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	delivery, err := s.queries.CreateTaskCallbackDelivery(ctx, db.CreateTaskCallbackDeliveryParams{
		SubscriptionID: sub.ID,
		RunEventID:     event.ID,
		Payload:        payloadBytes,
	})
	if err != nil {
		return err
	}
	go func(deliveryID uuid.UUID) {
		bgCtx, cancel := context.WithTimeout(context.Background(), httpTimeout+5*time.Second)
		defer cancel()
		if err := s.AttemptTaskCallbackDelivery(bgCtx, deliveryID); err != nil {
			log.Error().Err(err).Str("delivery_id", deliveryID.String()).
				Msg("webhook.enqueueTaskCallbackDelivery: first attempt failed")
		}
	}(delivery.ID)
	return nil
}

// AttemptTaskCallbackDelivery performs one task callback delivery attempt.
func (s *Service) AttemptTaskCallbackDelivery(ctx context.Context, deliveryID uuid.UUID) error {
	row, err := s.queries.GetTaskCallbackDeliveryByID(ctx, deliveryID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("get task callback delivery: %w", err)
	}
	if row.Status != "pending" {
		return nil
	}

	statusCode, respBody, attemptErr := s.doDeliverWithEvent(ctx, row.TargetURL, row.Secret, deliveryID, row.Payload, row.EventType, row.AuthScheme, row.AuthCredentials)
	if attemptErr == nil && statusCode >= 200 && statusCode < 300 {
		statusPtr := int32(statusCode)
		bodyPtr := truncate(respBody, responseBodyMaxLen)
		if err := s.queries.MarkTaskCallbackDeliverySuccess(ctx, db.MarkTaskCallbackDeliverySuccessParams{
			ID:             deliveryID,
			ResponseStatus: &statusPtr,
			ResponseBody:   &bodyPtr,
		}); err != nil {
			return err
		}
		return s.queries.ResetTaskCallbackSubscriptionFailures(ctx, row.SubscriptionID)
	}

	var (
		statusPtr *int32
		bodyPtr   *string
		errMsg    string
	)
	if statusCode > 0 {
		v := int32(statusCode)
		statusPtr = &v
		b := truncate(respBody, responseBodyMaxLen)
		bodyPtr = &b
	}
	if attemptErr != nil {
		errMsg = truncate(attemptErr.Error(), errorMessageMaxLen)
	} else {
		errMsg = fmt.Sprintf("HTTP %d", statusCode)
	}
	errMsgPtr := errMsg
	currAttempt := int(row.AttemptCount)
	delay := nextRetryDelay(currAttempt)
	if currAttempt+1 >= maxAttempts || delay == 0 {
		if err := s.queries.MarkTaskCallbackDeliveryFailedFinal(ctx, db.MarkTaskCallbackDeliveryFailedFinalParams{
			ID:             deliveryID,
			ResponseStatus: statusPtr,
			ResponseBody:   bodyPtr,
			ErrorMessage:   &errMsgPtr,
		}); err != nil {
			return err
		}
		return s.queries.IncrementTaskCallbackSubscriptionFailure(ctx, row.SubscriptionID)
	}
	nextAt := time.Now().Add(delay)
	return s.queries.MarkTaskCallbackDeliveryFailedRetry(ctx, db.MarkTaskCallbackDeliveryFailedRetryParams{
		ID:             deliveryID,
		ResponseStatus: statusPtr,
		ResponseBody:   bodyPtr,
		ErrorMessage:   &errMsgPtr,
		NextRetryAt:    nextAt,
	})
}

// processPending worker 内部用：扫一批应重试的投递并逐个执行。
func (s *Service) processPending(ctx context.Context) {
	rows, err := s.queries.ListPendingTaskCallbackDeliveries(ctx)
	if err != nil {
		log.Error().Err(err).Msg("webhook.processPending: ListPendingTaskCallbackDeliveries")
		return
	}
	for _, d := range rows {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if err := s.AttemptTaskCallbackDelivery(ctx, d.ID); err != nil {
			log.Error().Err(err).Str("delivery_id", d.ID.String()).
				Msg("webhook.processPending: AttemptTaskCallbackDelivery")
		}
	}
}

// doDeliver 真正发起 HTTP POST。
//
// 返回：HTTP status code（0 表示连不上）、响应 body（截断前）、网络层错误。
func (s *Service) doDeliver(
	ctx context.Context, url, secret string, deliveryID uuid.UUID, payload []byte,
) (int, string, error) {
	return s.doDeliverWithEvent(ctx, url, secret, deliveryID, payload, eventRunCompleted, nil, nil)
}

func (s *Service) doDeliverWithEvent(
	ctx context.Context, url, secret string, deliveryID uuid.UUID, payload []byte, eventType string, authScheme, authCredentials *string,
) (int, string, error) {
	signature := signPayload(payload, secret)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return 0, "", fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("X-OpenLinker-Signature", "sha256="+signature)
	req.Header.Set("X-OpenLinker-Event", eventType)
	req.Header.Set("X-OpenLinker-Delivery", deliveryID.String())
	req.Header.Set("X-OpenLinker-Timestamp", fmt.Sprintf("%d", time.Now().Unix()))
	if authScheme != nil && authCredentials != nil {
		scheme := strings.TrimSpace(*authScheme)
		credentials := strings.TrimSpace(*authCredentials)
		if scheme != "" && credentials != "" {
			req.Header.Set("Authorization", scheme+" "+credentials)
		}
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()

	// 限制读取量，防止巨大 body
	body, _ := io.ReadAll(io.LimitReader(resp.Body, int64(responseBodyMaxLen)*4))
	return resp.StatusCode, string(body), nil
}

func (s *Service) ensureRunOwner(ctx context.Context, runID, userID uuid.UUID) error {
	run, err := s.queries.GetRunByID(ctx, runID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return httpx.NotFound("调用记录不存在")
		}
		log.Error().Err(err).Str("run_id", runID.String()).Msg("webhook.ensureRunOwner: GetRunByID")
		return httpx.Internal("查询调用记录失败")
	}
	if run.UserID != userID {
		return httpx.NotFound("调用记录不存在")
	}
	return nil
}

// generateSecret 生成 32 字节随机数 → 64 hex 字符。
func generateSecret() (string, error) {
	b := make([]byte, secretByteLen)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// signPayload 计算 HMAC-SHA256(payload, secret) 的 hex。
func signPayload(payload []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

// buildPayload 把 run + agent + output 组装成投递 body。
func buildPayload(run *db.Run, agentSlug string, output map[string]interface{}) WebhookPayload {
	finishedAt := time.Now().UTC()
	if run.FinishedAt != nil {
		finishedAt = run.FinishedAt.UTC()
	}

	// duration：DB 中可能为 nil（极端：未写入），这里兜个 0
	var duration int32
	if run.DurationMs != nil {
		duration = *run.DurationMs
	}

	// 失败时 cost 已退款，对外口径与 RunResponse 一致（见 runtime/service.go runToResponse）
	var costCents int32
	switch run.Status {
	case "success":
		costCents = run.CostCents
	default:
		costCents = 0
	}

	// input/output 反序列化（input 必有；output 仅 success 必有）
	var input map[string]interface{}
	if len(run.Input) > 0 {
		_ = json.Unmarshal(run.Input, &input)
	}
	if input == nil {
		input = map[string]interface{}{}
	}

	p := WebhookPayload{
		Event:      eventRunCompleted,
		RunID:      run.ID.String(),
		AgentID:    run.AgentID.String(),
		AgentSlug:  agentSlug,
		UserID:     run.UserID.String(),
		Status:     run.Status,
		Input:      input,
		Output:     output,
		CostCents:  costCents,
		DurationMs: duration,
		StartedAt:  run.StartedAt.UTC().Format(time.RFC3339),
		FinishedAt: finishedAt.Format(time.RFC3339),
	}
	if run.ErrorCode != nil {
		p.ErrorCode = *run.ErrorCode
	}
	if run.ErrorMessage != nil {
		p.ErrorMessage = *run.ErrorMessage
	}
	return p
}

func normalizeTaskCallbackEventTypes(raw []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		item = strings.TrimSpace(item)
		switch item {
		case "run.created",
			"run.started",
			"run.dispatch.pending",
			"run.dispatch.claimed",
			"run.requirements.snapshotted",
			"run.message.delta",
			"run.artifact.delta",
			"run.status.changed",
			"run.child.created",
			"run.child.completed",
			"run.completed",
			"run.failed",
			"run.canceled":
			if _, ok := seen[item]; !ok {
				seen[item] = struct{}{}
				out = append(out, item)
			}
		}
	}
	if len(out) == 0 {
		return []string{"run.completed", "run.failed", "run.canceled"}
	}
	return out
}

func normalizeCallbackAuth(scheme, credentials string) (*string, *string) {
	scheme = strings.TrimSpace(scheme)
	credentials = strings.TrimSpace(credentials)
	if scheme == "" || credentials == "" {
		return nil, nil
	}
	return &scheme, &credentials
}

func batchActionToTaskCallbackStatus(action string) (string, error) {
	switch strings.TrimSpace(action) {
	case "pause":
		return "paused", nil
	case "resume":
		return "active", nil
	case "delete":
		return "deleted", nil
	default:
		return "", httpx.BadRequest("action 只能是 pause、resume 或 delete")
	}
}

func parseTaskCallbackSubscriptionIDs(raw []string) ([]uuid.UUID, error) {
	if len(raw) == 0 {
		return nil, httpx.BadRequest("subscription_ids 不能为空")
	}
	if len(raw) > 50 {
		return nil, httpx.BadRequest("subscription_ids 最多 50 个")
	}
	seen := map[uuid.UUID]struct{}{}
	out := make([]uuid.UUID, 0, len(raw))
	for _, item := range raw {
		id, err := uuid.Parse(strings.TrimSpace(item))
		if err != nil {
			return nil, httpx.BadRequest("subscription_ids 包含非法 uuid")
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out, nil
}

func taskCallbackPayload(sub db.TaskCallbackSubscription, event db.RunEvent) TaskCallbackPayload {
	payload := map[string]interface{}{}
	if len(event.Payload) > 0 {
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			payload = map[string]interface{}{"raw": string(event.Payload)}
		}
	}
	out := TaskCallbackPayload{
		EventID:        event.ID.String(),
		RunID:          event.RunID.String(),
		EventType:      event.EventType,
		Sequence:       event.Sequence,
		Payload:        payload,
		SubscriptionID: sub.ID.String(),
		CreatedAt:      event.CreatedAt.UTC().Format(time.RFC3339),
	}
	if event.ParentRunID != nil {
		out.ParentRunID = event.ParentRunID.String()
	}
	return out
}

func taskCallbackSubscriptionToResponse(sub db.TaskCallbackSubscription) TaskCallbackSubscriptionResponse {
	resp := TaskCallbackSubscriptionResponse{
		ID:                  sub.ID.String(),
		RunID:               sub.RunID.String(),
		TargetURL:           sub.TargetURL,
		EventTypes:          append([]string{}, sub.EventTypes...),
		Status:              sub.Status,
		ConsecutiveFailures: sub.ConsecutiveFailures,
		CreatedAt:           sub.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:           sub.UpdatedAt.UTC().Format(time.RFC3339),
	}
	if sub.AuthScheme != nil {
		resp.AuthScheme = *sub.AuthScheme
	}
	return resp
}

func taskCallbackDeliveryToResponse(row db.ListTaskCallbackDeliveriesByRunRow) TaskCallbackDeliveryResponse {
	item := TaskCallbackDeliveryResponse{
		ID:             row.ID.String(),
		SubscriptionID: row.SubscriptionID.String(),
		RunEventID:     row.RunEventID.String(),
		EventType:      row.EventType,
		TargetURL:      row.TargetURL,
		Status:         row.Status,
		ResponseStatus: row.ResponseStatus,
		ErrorMessage:   row.ErrorMessage,
		AttemptCount:   row.AttemptCount,
		CreatedAt:      row.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:      row.UpdatedAt.UTC().Format(time.RFC3339),
	}
	if row.NextRetryAt != nil {
		t := row.NextRetryAt.UTC().Format(time.RFC3339)
		item.NextRetryAt = &t
	}
	if row.DeliveredAt != nil {
		t := row.DeliveredAt.UTC().Format(time.RFC3339)
		item.DeliveredAt = &t
	}
	return item
}

// toDeliveryListItem db.WebhookDelivery → API DTO。
func toDeliveryListItem(d db.WebhookDelivery) DeliveryListItem {
	item := DeliveryListItem{
		ID:             d.ID.String(),
		RunID:          d.RunID.String(),
		URL:            d.URL,
		Status:         d.Status,
		ResponseStatus: d.ResponseStatus,
		ErrorMessage:   d.ErrorMessage,
		AttemptCount:   d.AttemptCount,
		CreatedAt:      d.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:      d.UpdatedAt.UTC().Format(time.RFC3339),
	}
	if d.NextRetryAt != nil {
		t := d.NextRetryAt.UTC().Format(time.RFC3339)
		item.NextRetryAt = &t
	}
	return item
}

// truncate 截断字符串到 n 字符内（防爆 DB / 日志）。
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
