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

	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/endpointurl"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
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
	eventRunFailed    = "run.failed"
	eventRunCanceled  = "run.canceled"

	targetTypeWebhook = "webhook"
	targetTypeSlack   = "slack"

	defaultDeliveryHistoryLimit = 50
	maxDeliveryHistoryLimit     = 100
)

var defaultDeliveryEventTypes = []string{eventRunCompleted, eventRunFailed, eventRunCanceled}

type deliveryTargetConfig struct {
	URL        string   `json:"url"`
	EventTypes []string `json:"event_types,omitempty"`
}

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
	queries        deliveryQueries
	txRunner       deliveryTxRunner
	pool           *pgxpool.Pool
	httpClient     *http.Client
	allowLocalHTTP bool
}

type deliveryTxRunner interface {
	runInTx(context.Context, func(deliveryQueries) error) error
}

type pgxDeliveryTxRunner struct {
	pool *pgxpool.Pool
}

func (r pgxDeliveryTxRunner) runInTx(ctx context.Context, fn func(deliveryQueries) error) error {
	return pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		return fn(db.New(tx))
	})
}

type deliveryQueries interface {
	ListDeliveryTargetsByUser(context.Context, uuid.UUID) ([]db.DeliveryTarget, error)
	ClearDefaultDeliveryTarget(context.Context, uuid.UUID) error
	CreateDeliveryTarget(context.Context, db.CreateDeliveryTargetParams) (db.DeliveryTarget, error)
	DeleteDeliveryTarget(context.Context, db.DeleteDeliveryTargetParams) (int64, error)
	SetDeliveryTargetDefault(context.Context, db.SetDeliveryTargetDefaultParams) (int64, error)
	UpdateDeliveryTargetConfig(context.Context, db.UpdateDeliveryTargetConfigParams) (db.DeliveryTarget, error)
	GetRunByID(context.Context, uuid.UUID) (db.Run, error)
	GetDeliveryTargetByID(context.Context, uuid.UUID) (db.DeliveryTarget, error)
	GetDefaultDeliveryTarget(context.Context, uuid.UUID) (db.DeliveryTarget, error)
	GetAgentByID(context.Context, uuid.UUID) (db.Agent, error)
	CreateRunDelivery(context.Context, db.CreateRunDeliveryParams) (db.RunDelivery, error)
	ListRunDeliveriesByRun(context.Context, uuid.UUID) ([]db.RunDelivery, error)
	ListRunDeliveriesByUser(context.Context, db.ListRunDeliveriesByUserParams) ([]db.RunDelivery, error)
	ResetRunDeliveryForRetry(context.Context, db.ResetRunDeliveryForRetryParams) (int64, error)
	GetRunDeliveryByID(context.Context, uuid.UUID) (db.GetRunDeliveryRow, error)
	MarkRunDeliverySuccess(context.Context, db.MarkRunDeliverySuccessParams) error
	MarkRunDeliveryFailedRetry(context.Context, db.MarkRunDeliveryFailedRetryParams) error
	MarkRunDeliveryFailedFinal(context.Context, db.MarkRunDeliveryFailedFinalParams) error
	ListPendingRunDeliveries(context.Context) ([]db.RunDelivery, error)
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
	if pool != nil {
		s.txRunner = pgxDeliveryTxRunner{pool: pool}
	}
	return s
}

// CreateTarget 用户新增投递目标。
//
// 当 IsDefault=true 时先清掉原 default 避免唯一索引冲突。
func (s *Service) CreateTarget(ctx context.Context, userID uuid.UUID, req *CreateTargetRequest) (*TargetResponse, error) {
	targetURL := strings.TrimSpace(req.URL)
	if err := endpointurl.Validate(targetURL, s.allowLocalHTTP); err != nil {
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

	cfg, err := json.Marshal(deliveryTargetConfig{
		URL:        targetURL,
		EventTypes: normalizeDeliveryEventTypes(req.EventTypes),
	})
	if err != nil {
		return nil, httpx.Internal("序列化 config 失败")
	}

	var t db.DeliveryTarget
	create := func(q deliveryQueries) error {
		if req.IsDefault {
			if err := q.ClearDefaultDeliveryTarget(ctx, userID); err != nil {
				return fmt.Errorf("clear default: %w", err)
			}
		}
		var createErr error
		t, createErr = q.CreateDeliveryTarget(ctx, db.CreateDeliveryTargetParams{
			UserID:    userID,
			Name:      req.Name,
			Type:      req.Type,
			Config:    cfg,
			Secret:    secret,
			IsDefault: req.IsDefault,
		})
		if createErr != nil {
			return fmt.Errorf("create target: %w", createErr)
		}
		return nil
	}

	if req.IsDefault {
		err = s.runInTx(ctx, create)
	} else {
		err = create(s.queries)
	}
	if err != nil {
		log.Error().Err(err).Msg("delivery.CreateTarget")
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
	var rows int64
	err := s.runInTx(ctx, func(q deliveryQueries) error {
		if err := q.ClearDefaultDeliveryTarget(ctx, userID); err != nil {
			return fmt.Errorf("clear default: %w", err)
		}
		var setErr error
		rows, setErr = q.SetDeliveryTargetDefault(ctx, db.SetDeliveryTargetDefaultParams{
			ID:     targetID,
			UserID: userID,
		})
		if setErr != nil {
			return fmt.Errorf("set default: %w", setErr)
		}
		return nil
	})
	if err != nil {
		log.Error().Err(err).Msg("delivery.SetDefault")
		return httpx.Internal("更新默认目标失败")
	}
	if rows == 0 {
		return httpx.NotFound("投递目标不存在")
	}
	return nil
}

func (s *Service) runInTx(ctx context.Context, fn func(deliveryQueries) error) error {
	if s.txRunner == nil {
		return fn(s.queries)
	}
	return s.txRunner.runInTx(ctx, fn)
}

func (s *Service) UpdateTarget(ctx context.Context, targetID, userID uuid.UUID, req *UpdateTargetRequest) (*TargetResponse, error) {
	if req == nil {
		return nil, httpx.BadRequest("请求体不能为空")
	}
	if len(req.EventTypes) == 0 {
		return nil, httpx.BadRequest("event_types 至少选择一个")
	}
	target, err := s.queries.GetDeliveryTargetByID(ctx, targetID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("投递目标不存在")
		}
		return nil, httpx.Internal("查询投递目标失败")
	}
	if target.UserID != userID {
		return nil, httpx.NotFound("投递目标不存在")
	}
	cfg := parseTargetConfig(target.Config)
	cfg.EventTypes = normalizeDeliveryEventTypes(req.EventTypes)
	body, err := json.Marshal(cfg)
	if err != nil {
		return nil, httpx.Internal("序列化 config 失败")
	}
	updated, err := s.queries.UpdateDeliveryTargetConfig(ctx, db.UpdateDeliveryTargetConfigParams{
		ID:     targetID,
		UserID: userID,
		Config: body,
	})
	if err != nil {
		log.Error().Err(err).Str("target_id", targetID.String()).Msg("delivery.UpdateTarget")
		return nil, httpx.Internal("更新投递目标失败")
	}
	resp := toTargetResponse(updated, false)
	return &resp, nil
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

	delivery, err := s.enqueue(ctx, userID, &run, target, deliveryEventForRunStatus(run.Status))
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
	eventType := deliveryEventForRunStatus(run.Status)
	if !targetAllowsDeliveryEvent(target, eventType) {
		return nil
	}
	delivery, err := s.enqueue(ctx, run.UserID, run, target, eventType)
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

// List 查询当前用户的外部投递历史，可选按 Agent / Run / 状态过滤。
func (s *Service) List(ctx context.Context, userID uuid.UUID, filter DeliveryListFilter) ([]DeliveryItem, error) {
	limit := filter.Limit
	if limit <= 0 {
		limit = defaultDeliveryHistoryLimit
	}
	if limit > maxDeliveryHistoryLimit {
		limit = maxDeliveryHistoryLimit
	}

	var agentID uuid.UUID
	hasAgentID := false
	if filter.AgentID != nil && strings.TrimSpace(*filter.AgentID) != "" {
		id, err := uuid.Parse(strings.TrimSpace(*filter.AgentID))
		if err != nil {
			return nil, httpx.BadRequest("agent_id 不是合法 uuid")
		}
		agentID = id
		hasAgentID = true
	}

	var runID uuid.UUID
	hasRunID := false
	if filter.RunID != nil && strings.TrimSpace(*filter.RunID) != "" {
		id, err := uuid.Parse(strings.TrimSpace(*filter.RunID))
		if err != nil {
			return nil, httpx.BadRequest("run_id 不是合法 uuid")
		}
		runID = id
		hasRunID = true
	}

	status := strings.TrimSpace(filter.Status)
	if status != "" && status != "pending" && status != "success" && status != "failed" {
		return nil, httpx.BadRequest("status 只能是 pending、success 或 failed")
	}

	rows, err := s.queries.ListRunDeliveriesByUser(ctx, db.ListRunDeliveriesByUserParams{
		UserID:     userID,
		HasAgentID: hasAgentID,
		AgentID:    agentID,
		HasRunID:   hasRunID,
		RunID:      runID,
		Status:     status,
		Limit:      limit,
	})
	if err != nil {
		log.Error().Err(err).Msg("delivery.List")
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
func (s *Service) enqueue(ctx context.Context, userID uuid.UUID, run *db.Run, target db.DeliveryTarget, eventType string) (db.RunDelivery, error) {
	agent, err := s.queries.GetAgentByID(ctx, run.AgentID)
	if err != nil {
		log.Error().Err(err).Str("agent_id", run.AgentID.String()).Msg("delivery.enqueue: GetAgentByID")
		return db.RunDelivery{}, httpx.Internal("查询 Agent 失败")
	}
	payload := buildPayload(run, agent.Slug, agent.Name, eventType)
	body, err := json.Marshal(payload)
	if err != nil {
		return db.RunDelivery{}, httpx.Internal("序列化投递 payload 失败")
	}

	targetURL := parseTargetConfig(target.Config).URL
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
	eventType := deliveryEventFromPayload(payload)
	switch targetType {
	case targetTypeSlack:
		return s.deliverSlack(ctx, url, deliveryID, payload)
	default:
		return s.deliverWebhook(ctx, url, secret, deliveryID, payload, eventType)
	}
}

func (s *Service) deliverWebhook(
	ctx context.Context, url, secret string, deliveryID uuid.UUID, payload []byte, eventType string,
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

func buildPayload(run *db.Run, agentSlug, agentName, eventType string) DeliveryPayload {
	if eventType == "" {
		eventType = deliveryEventForRunStatus(run.Status)
	}
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
		Event:      eventType,
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

func parseTargetConfig(raw []byte) deliveryTargetConfig {
	cfg := deliveryTargetConfig{}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &cfg)
	}
	if cfg.URL == "" {
		var legacy map[string]string
		if err := json.Unmarshal(raw, &legacy); err == nil {
			cfg.URL = legacy["url"]
		}
	}
	cfg.URL = strings.TrimSpace(cfg.URL)
	cfg.EventTypes = normalizeDeliveryEventTypes(cfg.EventTypes)
	return cfg
}

func normalizeDeliveryEventTypes(events []string) []string {
	if len(events) == 0 {
		return append([]string(nil), defaultDeliveryEventTypes...)
	}
	seen := make(map[string]bool, len(events))
	out := make([]string, 0, len(events))
	for _, event := range events {
		event = strings.TrimSpace(event)
		if !isDeliveryEventType(event) || seen[event] {
			continue
		}
		seen[event] = true
		out = append(out, event)
	}
	if len(out) == 0 {
		return append([]string(nil), defaultDeliveryEventTypes...)
	}
	return out
}

func isDeliveryEventType(event string) bool {
	switch event {
	case eventRunCompleted, eventRunFailed, eventRunCanceled:
		return true
	default:
		return false
	}
}

func deliveryEventForRunStatus(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "success", "completed", "succeeded":
		return eventRunCompleted
	case "canceled", "cancelled":
		return eventRunCanceled
	default:
		return eventRunFailed
	}
}

func targetAllowsDeliveryEvent(target db.DeliveryTarget, eventType string) bool {
	cfg := parseTargetConfig(target.Config)
	for _, allowed := range cfg.EventTypes {
		if allowed == eventType {
			return true
		}
	}
	return false
}

func deliveryEventFromPayload(payload []byte) string {
	var body struct {
		Event string `json:"event"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		return eventRunCompleted
	}
	if isDeliveryEventType(body.Event) {
		return body.Event
	}
	return eventRunCompleted
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
