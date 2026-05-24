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

	db "github.com/kinzhi/openlinker-core/pkg/db/generated"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
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
	queries    *db.Queries
	pool       *pgxpool.Pool
	httpClient *http.Client
}

// NewService 构造 Service。
func NewService(pool *pgxpool.Pool) *Service {
	return &Service{
		queries: db.New(pool),
		pool:    pool,
		httpClient: &http.Client{
			Timeout: httpTimeout,
		},
	}
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

// EnqueueDelivery 在 run 完成后触发投递（runtime 调用，必须在 goroutine 中）。
//
// 流程：
//  1. 查 agent.webhook_url：NULL → 直接 return（不投递）
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

// processPending worker 内部用：扫一批应重试的投递并逐个执行。
func (s *Service) processPending(ctx context.Context) {
	rows, err := s.queries.ListPendingDeliveries(ctx)
	if err != nil {
		log.Error().Err(err).Msg("webhook.processPending: ListPendingDeliveries")
		return
	}
	for _, d := range rows {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if err := s.AttemptDelivery(ctx, d.ID); err != nil {
			log.Error().Err(err).Str("delivery_id", d.ID.String()).
				Msg("webhook.processPending: AttemptDelivery")
		}
	}
}

// doDeliver 真正发起 HTTP POST。
//
// 返回：HTTP status code（0 表示连不上）、响应 body（截断前）、网络层错误。
func (s *Service) doDeliver(
	ctx context.Context, url, secret string, deliveryID uuid.UUID, payload []byte,
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
	req.Header.Set("X-OpenLinker-Event", eventRunCompleted)
	req.Header.Set("X-OpenLinker-Delivery", deliveryID.String())
	req.Header.Set("X-OpenLinker-Timestamp", fmt.Sprintf("%d", time.Now().Unix()))

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()

	// 限制读取量，防止巨大 body
	body, _ := io.ReadAll(io.LimitReader(resp.Body, int64(responseBodyMaxLen)*4))
	return resp.StatusCode, string(body), nil
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
