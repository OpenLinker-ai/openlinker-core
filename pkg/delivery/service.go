package delivery

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

	"github.com/kinzhi/openlinker-core/pkg/config"
	db "github.com/kinzhi/openlinker-core/pkg/db/generated"
	"github.com/kinzhi/openlinker-core/pkg/endpointurl"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

const (
	// maxAttempts 首次 + 2 次重试。
	maxAttempts        = 3
	httpTimeout        = 15 * time.Second
	responseBodyMaxLen = 1024
	errorMessageMaxLen = 500
	secretByteLen      = 32

	// maxTargetsPerUser 防止滥用（同 api_keys 的应用层限制）。
	maxTargetsPerUser = 10

	userAgent         = "OpenLinker-Delivery/1.0"
	eventRunCompleted = "run.completed"

	targetTypeWebhook = "webhook"
	targetTypeSlack   = "slack"
)

// nextRetryDelay 1min / 5min / 30min 退避。
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

// Service Output Delivery 业务层。
//
// CRUD delivery_targets + 创建/重试 run_deliveries。
// HMAC 签名（webhook）/ Slack incoming webhook（slack）。
type Service struct {
	queries        *db.Queries
	pool           *pgxpool.Pool
	httpClient     *http.Client
	allowLocalHTTP bool
}

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

// CreateTarget 用户新增投递目标。
//
// 当 IsDefault=true 时先清掉原 default 避免唯一索引冲突。
func (s *Service) CreateTarget(ctx context.Context, userID uuid.UUID, req *CreateTargetRequest) (*TargetResponse, error) {
	if err := endpointurl.Validate(req.URL, s.allowLocalHTTP); err != nil {
		return nil, httpx.BadRequest("url 必须是 HTTPS；本地开发需开启 ALLOW_LOCAL_HTTP_ENDPOINTS 后才允许 loopback HTTP")
	}

	// 应用层限制：每用户最多 10 个 target
	existing, err := s.queries.ListDeliveryTargetsByUser(ctx, userID)
	if err != nil {
		log.Error().Err(err).Msg("delivery.CreateTarget: ListDeliveryTargetsByUser")
		return nil, httpx.Internal("查询目标列表失败")
	}
	if len(existing) >= maxTargetsPerUser {
		return nil, httpx.BadRequest(fmt.Sprintf("每个账号最多 %d 个投递目标", maxTargetsPerUser))
	}

	secret, err := generateSecret()
	if err != nil {
		log.Error().Err(err).Msg("delivery.CreateTarget: generateSecret")
		return nil, httpx.Internal("生成 secret 失败")
	}

	cfg, err := json.Marshal(map[string]string{"url": req.URL})
	if err != nil {
		return nil, httpx.Internal("序列化 config 失败")
	}

	// is_default=true 时先清原 default
	if req.IsDefault {
		if err := s.queries.ClearDefaultDeliveryTarget(ctx, userID); err != nil {
			log.Error().Err(err).Msg("delivery.CreateTarget: ClearDefaultDeliveryTarget")
			return nil, httpx.Internal("清除原默认目标失败")
		}
	}

	t, err := s.queries.CreateDeliveryTarget(ctx, db.CreateDeliveryTargetParams{
		UserID:    userID,
		Name:      req.Name,
		Type:      req.Type,
		Config:    cfg,
		Secret:    secret,
		IsDefault: req.IsDefault,
	})
	if err != nil {
		log.Error().Err(err).Msg("delivery.CreateTarget: CreateDeliveryTarget")
		return nil, httpx.Internal("创建投递目标失败")
	}
	resp := toTargetResponse(t, true)
	return &resp, nil
}

// ListTargets 列出当前用户所有 target（不返回 secret）。
func (s *Service) ListTargets(ctx context.Context, userID uuid.UUID) ([]TargetResponse, error) {
	rows, err := s.queries.ListDeliveryTargetsByUser(ctx, userID)
	if err != nil {
		log.Error().Err(err).Msg("delivery.ListTargets")
		return nil, httpx.Internal("查询投递目标失败")
	}
	out := make([]TargetResponse, 0, len(rows))
	for _, t := range rows {
		out = append(out, toTargetResponse(t, false))
	}
	return out, nil
}

// DeleteTarget 按 id 删除（限定 user_id）。
func (s *Service) DeleteTarget(ctx context.Context, targetID, userID uuid.UUID) error {
	rows, err := s.queries.DeleteDeliveryTarget(ctx, db.DeleteDeliveryTargetParams{
		ID:     targetID,
		UserID: userID,
	})
	if err != nil {
		log.Error().Err(err).Msg("delivery.DeleteTarget")
		return httpx.Internal("删除投递目标失败")
	}
	if rows == 0 {
		return httpx.NotFound("投递目标不存在")
	}
	return nil
}

// SetDefault 将指定 target 设为默认（清掉原 default）。
func (s *Service) SetDefault(ctx context.Context, targetID, userID uuid.UUID) error {
	if err := s.queries.ClearDefaultDeliveryTarget(ctx, userID); err != nil {
		log.Error().Err(err).Msg("delivery.SetDefault: ClearDefaultDeliveryTarget")
		return httpx.Internal("清除原默认目标失败")
	}
	rows, err := s.queries.SetDeliveryTargetDefault(ctx, db.SetDeliveryTargetDefaultParams{
		ID:     targetID,
		UserID: userID,
	})
	if err != nil {
		log.Error().Err(err).Msg("delivery.SetDefault: SetDeliveryTargetDefault")
		return httpx.Internal("更新默认目标失败")
	}
	if rows == 0 {
		return httpx.NotFound("投递目标不存在")
	}
	return nil
}

// DeliverRun 手动触发投递（用户 /run/:id 点投递按钮）。
//
// run 必须属于当前用户、且终态（success / failed / timeout）。
// targetID 为空时取用户 is_default target；无 default 返回 400。
func (s *Service) DeliverRun(ctx context.Context, userID, runID uuid.UUID, targetID *uuid.UUID) (*DeliveryItem, error) {
	run, err := s.queries.GetRunByID(ctx, runID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("Run 不存在")
		}
		log.Error().Err(err).Str("run_id", runID.String()).Msg("delivery.DeliverRun: GetRunByID")
		return nil, httpx.Internal("查询 Run 失败")
	}
	if run.UserID != userID {
		return nil, httpx.NotFound("Run 不存在")
	}
	if run.Status == "running" {
		return nil, httpx.BadRequest("Run 仍在执行中")
	}

	// 选 target
	var target db.DeliveryTarget
	if targetID != nil {
		t, err := s.queries.GetDeliveryTargetByID(ctx, *targetID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, httpx.NotFound("投递目标不存在")
			}
			return nil, httpx.Internal("查询投递目标失败")
		}
		if t.UserID != userID {
			return nil, httpx.NotFound("投递目标不存在")
		}
		target = t
	} else {
		t, err := s.queries.GetDefaultDeliveryTarget(ctx, userID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, httpx.BadRequest("尚未设置默认投递目标")
			}
			return nil, httpx.Internal("查询默认目标失败")
		}
		target = t
	}

	delivery, err := s.enqueue(ctx, userID, &run, target)
	if err != nil {
		return nil, err
	}
	go s.attemptInBackground(delivery.ID)
	item := toDeliveryItem(delivery)
	return &item, nil
}

// EnqueueIfDefault Run 终态后由 runtime 异步调用：若用户有 default target，自动投递。
//
// 无 default → 静默跳过（不报错）。这是 setter 注入的接口实现。
func (s *Service) EnqueueIfDefault(ctx context.Context, run *db.Run) error {
	target, err := s.queries.GetDefaultDeliveryTarget(ctx, run.UserID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("get default delivery target: %w", err)
	}
	delivery, err := s.enqueue(ctx, run.UserID, run, target)
	if err != nil {
		return err
	}
	go s.attemptInBackground(delivery.ID)
	return nil
}

// ListByRun 查询某 run 的投递历史。
func (s *Service) ListByRun(ctx context.Context, runID, userID uuid.UUID) ([]DeliveryItem, error) {
	run, err := s.queries.GetRunByID(ctx, runID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("Run 不存在")
		}
		return nil, httpx.Internal("查询 Run 失败")
	}
	if run.UserID != userID {
		return nil, httpx.NotFound("Run 不存在")
	}
	rows, err := s.queries.ListRunDeliveriesByRun(ctx, runID)
	if err != nil {
		log.Error().Err(err).Msg("delivery.ListByRun")
		return nil, httpx.Internal("查询投递历史失败")
	}
	out := make([]DeliveryItem, 0, len(rows))
	for _, r := range rows {
		out = append(out, toDeliveryItem(r))
	}
	return out, nil
}

// RetryDelivery 手动重试 failed 投递（reset 后 worker 会重新拾起）。
func (s *Service) RetryDelivery(ctx context.Context, deliveryID, userID uuid.UUID) error {
	rows, err := s.queries.ResetRunDeliveryForRetry(ctx, db.ResetRunDeliveryForRetryParams{
		ID:     deliveryID,
		UserID: userID,
	})
	if err != nil {
		log.Error().Err(err).Msg("delivery.RetryDelivery")
		return httpx.Internal("重试投递失败")
	}
	if rows == 0 {
		return httpx.NotFound("投递记录不存在或不是 failed 状态")
	}
	go s.attemptInBackground(deliveryID)
	return nil
}

// enqueue 内部：构造 payload + 写 run_deliveries（INSERT pending）。
func (s *Service) enqueue(ctx context.Context, userID uuid.UUID, run *db.Run, target db.DeliveryTarget) (db.RunDelivery, error) {
	agent, err := s.queries.GetAgentByID(ctx, run.AgentID)
	if err != nil {
		log.Error().Err(err).Str("agent_id", run.AgentID.String()).Msg("delivery.enqueue: GetAgentByID")
		return db.RunDelivery{}, httpx.Internal("查询 Agent 失败")
	}
	payload := buildPayload(run, agent.Slug, agent.Name)
	body, err := json.Marshal(payload)
	if err != nil {
		return db.RunDelivery{}, httpx.Internal("序列化投递 payload 失败")
	}

	var targetURL string
	var cfg map[string]string
	if err := json.Unmarshal(target.Config, &cfg); err == nil {
		targetURL = cfg["url"]
	}
	if targetURL == "" {
		return db.RunDelivery{}, httpx.BadRequest("投递目标 url 为空")
	}

	d, err := s.queries.CreateRunDelivery(ctx, db.CreateRunDeliveryParams{
		RunID:      run.ID,
		TargetID:   target.ID,
		UserID:     userID,
		TargetType: target.Type,
		TargetURL:  targetURL,
		Payload:    body,
	})
	if err != nil {
		log.Error().Err(err).Msg("delivery.enqueue: CreateRunDelivery")
		return db.RunDelivery{}, httpx.Internal("创建投递记录失败")
	}
	return d, nil
}

// attemptInBackground 在独立 goroutine 与新 ctx 中尝试投递（不阻塞调用方）。
func (s *Service) attemptInBackground(deliveryID uuid.UUID) {
	bgCtx, cancel := context.WithTimeout(context.Background(), httpTimeout+5*time.Second)
	defer cancel()
	if err := s.AttemptDelivery(bgCtx, deliveryID); err != nil {
		log.Error().Err(err).Str("delivery_id", deliveryID.String()).
			Msg("delivery: first attempt failed")
	}
}

// AttemptDelivery 单次投递。
func (s *Service) AttemptDelivery(ctx context.Context, deliveryID uuid.UUID) error {
	row, err := s.queries.GetRunDeliveryByID(ctx, deliveryID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("get run delivery: %w", err)
	}
	if row.Status != "pending" {
		return nil
	}

	// target 已被删除：终态 failed
	if row.TargetSecret == nil {
		errMsg := "投递目标已被删除"
		return s.queries.MarkRunDeliveryFailedFinal(ctx, db.MarkRunDeliveryFailedFinalParams{
			ID:           deliveryID,
			ErrorMessage: &errMsg,
		})
	}

	statusCode, respBody, attemptErr := s.doDeliver(ctx, row.TargetType, row.TargetURL, *row.TargetSecret, deliveryID, row.Payload)

	if attemptErr == nil && statusCode >= 200 && statusCode < 300 {
		statusPtr := int32(statusCode)
		bodyPtr := truncate(respBody, responseBodyMaxLen)
		return s.queries.MarkRunDeliverySuccess(ctx, db.MarkRunDeliverySuccessParams{
			ID:             deliveryID,
			ResponseStatus: &statusPtr,
			ResponseBody:   &bodyPtr,
		})
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
		return s.queries.MarkRunDeliveryFailedFinal(ctx, db.MarkRunDeliveryFailedFinalParams{
			ID:             deliveryID,
			ResponseStatus: statusPtr,
			ResponseBody:   bodyPtr,
			ErrorMessage:   &errMsgPtr,
		})
	}

	nextAt := time.Now().Add(delay)
	return s.queries.MarkRunDeliveryFailedRetry(ctx, db.MarkRunDeliveryFailedRetryParams{
		ID:             deliveryID,
		ResponseStatus: statusPtr,
		ResponseBody:   bodyPtr,
		ErrorMessage:   &errMsgPtr,
		NextRetryAt:    nextAt,
	})
}

// doDeliver 根据 type 走不同的投递格式。
//
// webhook：原始 JSON payload + HMAC 签名 header。
// slack：把 payload 转成 text，调 Slack incoming webhook（无签名，URL 本身是 secret）。
func (s *Service) doDeliver(
	ctx context.Context, targetType, url, secret string, deliveryID uuid.UUID, payload []byte,
) (int, string, error) {
	switch targetType {
	case targetTypeSlack:
		return s.deliverSlack(ctx, url, deliveryID, payload)
	default:
		return s.deliverWebhook(ctx, url, secret, deliveryID, payload)
	}
}

func (s *Service) deliverWebhook(
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
	return doRequest(s.httpClient, req)
}

// deliverSlack 把 OpenLinker 投递 payload 重新打包成 Slack 文本，POST 到 incoming webhook。
func (s *Service) deliverSlack(
	ctx context.Context, url string, _ uuid.UUID, payload []byte,
) (int, string, error) {
	var p DeliveryPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return 0, "", fmt.Errorf("unmarshal payload: %w", err)
	}
	text := buildSlackText(p)
	body, err := json.Marshal(map[string]string{"text": text})
	if err != nil {
		return 0, "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, "", fmt.Errorf("build slack request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", userAgent)
	return doRequest(s.httpClient, req)
}

func doRequest(c *http.Client, req *http.Request) (int, string, error) {
	resp, err := c.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, int64(responseBodyMaxLen)*4))
	return resp.StatusCode, string(body), nil
}

// processPending worker 内部用。
func (s *Service) processPending(ctx context.Context) {
	rows, err := s.queries.ListPendingRunDeliveries(ctx)
	if err != nil {
		log.Error().Err(err).Msg("delivery.processPending: ListPendingRunDeliveries")
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
				Msg("delivery.processPending: AttemptDelivery")
		}
	}
}

func buildPayload(run *db.Run, agentSlug, agentName string) DeliveryPayload {
	finishedAt := time.Now().UTC()
	if run.FinishedAt != nil {
		finishedAt = run.FinishedAt.UTC()
	}
	var duration int32
	if run.DurationMs != nil {
		duration = *run.DurationMs
	}
	var costCents int32
	if run.Status == "success" {
		costCents = run.CostCents
	}

	var input map[string]interface{}
	if len(run.Input) > 0 {
		_ = json.Unmarshal(run.Input, &input)
	}
	if input == nil {
		input = map[string]interface{}{}
	}
	var output map[string]interface{}
	if len(run.Output) > 0 {
		_ = json.Unmarshal(run.Output, &output)
	}

	return DeliveryPayload{
		Event:      eventRunCompleted,
		RunID:      run.ID.String(),
		AgentID:    run.AgentID.String(),
		AgentSlug:  agentSlug,
		AgentName:  agentName,
		Status:     run.Status,
		Input:      input,
		Output:     output,
		CostCents:  costCents,
		DurationMs: duration,
		StartedAt:  run.StartedAt.UTC().Format(time.RFC3339),
		FinishedAt: finishedAt.Format(time.RFC3339),
	}
}

func buildSlackText(p DeliveryPayload) string {
	var b strings.Builder
	fmt.Fprintf(&b, "*OpenLinker Run* `%s`\n", p.AgentName)
	fmt.Fprintf(&b, "Status: *%s* · %dms · $%.3f\n", p.Status, p.DurationMs, float64(p.CostCents)/100)
	if len(p.Output) > 0 {
		raw, _ := json.MarshalIndent(p.Output, "", "  ")
		out := truncate(string(raw), 1500)
		fmt.Fprintf(&b, "```\n%s\n```", out)
	}
	return b.String()
}

func generateSecret() (string, error) {
	b := make([]byte, secretByteLen)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func signPayload(payload []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
