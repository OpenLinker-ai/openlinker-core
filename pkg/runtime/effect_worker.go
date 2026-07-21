package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/eventwake"
)

const (
	RunEffectTypeAgentWebhook     = "run.agent_webhook"
	RunEffectTypeTaskCallback     = "run.task_callback"
	RunEffectTypeDefaultDelivery  = "run.default_delivery"
	RunEffectTypeParentCompletion = "run.parent_completion"
)

// RunEffectAttemptResult is returned by one downstream delivery attempt. A
// handler must perform at most one external HTTP request for a claimed Effect.
// ErrorCode and SafeMessage are persisted, so they must never contain target
// URLs, credentials, request/response payloads, or caller-controlled text.
type RunEffectAttemptResult struct {
	Succeeded   bool
	Retryable   bool
	ErrorCode   string
	SafeMessage string
	Err         error
}

func RunEffectAttemptSucceeded() RunEffectAttemptResult {
	return RunEffectAttemptResult{Succeeded: true}
}

func RetryableRunEffectAttempt(code, safeMessage string, err error) RunEffectAttemptResult {
	return RunEffectAttemptResult{
		Retryable:   true,
		ErrorCode:   normalizeRunEffectErrorCode(code),
		SafeMessage: sanitizeRunEffectSafeMessage(safeMessage),
		Err:         err,
	}
}

func PermanentRunEffectFailure(code, safeMessage string, err error) RunEffectAttemptResult {
	return RunEffectAttemptResult{
		ErrorCode:   normalizeRunEffectErrorCode(code),
		SafeMessage: sanitizeRunEffectSafeMessage(safeMessage),
		Err:         err,
	}
}

func (r RunEffectAttemptResult) persistedError() string {
	code := normalizeRunEffectErrorCode(r.ErrorCode)
	message := sanitizeRunEffectSafeMessage(r.SafeMessage)
	if message == "" {
		return code
	}
	return code + ": " + message
}

func normalizeRunEffectErrorCode(code string) string {
	code = strings.ToUpper(strings.TrimSpace(code))
	if code == "" {
		return "EFFECT_DELIVERY_FAILED"
	}
	var b strings.Builder
	for _, r := range code {
		switch {
		case r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
		if b.Len() >= 80 {
			break
		}
	}
	if b.Len() == 0 {
		return "EFFECT_DELIVERY_FAILED"
	}
	return b.String()
}

func sanitizeRunEffectSafeMessage(message string) string {
	message = strings.TrimSpace(message)
	runes := []rune(message)
	if len(runes) > 400 {
		message = string(runes[:400])
	}
	return message
}

// WebhookRunEffectHandler owns Agent webhook and terminal task callback
// materialization. Downstream delivery IDs must equal effect.ID.
type WebhookRunEffectHandler interface {
	AttemptAgentWebhookEffect(context.Context, db.RunEffectOutbox) RunEffectAttemptResult
	AttemptTaskCallbackEffect(context.Context, db.RunEffectOutbox) RunEffectAttemptResult
	ResetWebhookEffectDelivery(context.Context, db.RunEffectOutbox) error
	EnqueueRunEventDurable(context.Context, db.RunEvent) error
}

// DeliveryRunEffectHandler owns automatic delivery-target Effects. Manual
// user-triggered deliveries remain on delivery.Service's legacy queue.
type DeliveryRunEffectHandler interface {
	AttemptDefaultDeliveryEffect(context.Context, db.RunEffectOutbox) RunEffectAttemptResult
	ResetDefaultDeliveryEffect(context.Context, db.RunEffectOutbox) error
}

type runEffectHandlers struct {
	webhook  WebhookRunEffectHandler
	delivery DeliveryRunEffectHandler
}

func (h runEffectHandlers) attempt(ctx context.Context, effect db.RunEffectOutbox) RunEffectAttemptResult {
	switch effect.EffectType {
	case RunEffectTypeAgentWebhook:
		if h.webhook == nil {
			return RetryableRunEffectAttempt("HANDLER_UNAVAILABLE", "webhook effect handler unavailable", nil)
		}
		return h.webhook.AttemptAgentWebhookEffect(ctx, effect)
	case RunEffectTypeTaskCallback:
		if h.webhook == nil {
			return RetryableRunEffectAttempt("HANDLER_UNAVAILABLE", "task callback effect handler unavailable", nil)
		}
		return h.webhook.AttemptTaskCallbackEffect(ctx, effect)
	case RunEffectTypeDefaultDelivery:
		if h.delivery == nil {
			return RetryableRunEffectAttempt("HANDLER_UNAVAILABLE", "delivery effect handler unavailable", nil)
		}
		return h.delivery.AttemptDefaultDeliveryEffect(ctx, effect)
	default:
		return PermanentRunEffectFailure("EFFECT_TYPE_UNSUPPORTED", "unsupported run effect type", nil)
	}
}

func (h runEffectHandlers) reset(ctx context.Context, effect db.RunEffectOutbox) error {
	switch effect.EffectType {
	case RunEffectTypeAgentWebhook, RunEffectTypeTaskCallback:
		if h.webhook == nil {
			return errors.New("webhook effect handler unavailable")
		}
		return h.webhook.ResetWebhookEffectDelivery(ctx, effect)
	case RunEffectTypeDefaultDelivery:
		if h.delivery == nil {
			return errors.New("delivery effect handler unavailable")
		}
		return h.delivery.ResetDefaultDeliveryEffect(ctx, effect)
	case RunEffectTypeParentCompletion:
		return nil
	default:
		return errors.New("unsupported run effect type")
	}
}

// deterministicRunEffectChildEventID creates an immutable parent Event ID from
// the stable Effect ID. google/uuid NewSHA1 emits an RFC 4122 version-5 UUID.
func deterministicRunEffectChildEventID(effectID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(effectID, []byte("run.child.completed"))
}

const (
	defaultRunEffectWorkerInterval      = time.Second
	defaultRunEffectWorkerLeaseDuration = 30 * time.Second
	defaultRunEffectWorkerBatchSize     = int32(16)
	runEffectWorkerWakeTopic            = "work.run_effect.available"
)

type RunEffectWorkerConfig struct {
	Interval      time.Duration
	LeaseDuration time.Duration
	BatchSize     int32
	Observer      WorkerObserver
}

func normalizeRunEffectWorkerConfig(cfg RunEffectWorkerConfig) RunEffectWorkerConfig {
	if cfg.Interval <= 0 {
		cfg.Interval = defaultRunEffectWorkerInterval
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = defaultRunEffectWorkerLeaseDuration
	}
	if cfg.LeaseDuration < 20*time.Second {
		cfg.LeaseDuration = 20 * time.Second
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultRunEffectWorkerBatchSize
	}
	if cfg.BatchSize > 100 {
		cfg.BatchSize = 100
	}
	return cfg
}

type runEffectStore interface {
	ClaimRunEffects(context.Context, db.ClaimRunEffectsParams) ([]db.RunEffectOutbox, error)
	DeadLetterExpiredRunEffectsAtLimit(context.Context) ([]db.RunEffectOutbox, error)
	DeadLetterRunEffect(context.Context, db.DeadLetterRunEffectParams) (db.RunEffectOutbox, error)
	MarkRunEffectSucceeded(context.Context, db.MarkRunEffectSucceededParams) (db.RunEffectOutbox, error)
	RetryOrDeadLetterRunEffect(context.Context, db.RetryOrDeadLetterRunEffectParams) (db.RunEffectOutbox, error)
	GetRunEffectByID(context.Context, uuid.UUID) (db.RunEffectOutbox, error)
	ReplayRunEffect(context.Context, db.ReplayRunEffectParams) (db.RunEffectOutbox, error)

	GetRunDelegationByChild(context.Context, uuid.UUID) (db.RunDelegation, error)
	GetRunByID(context.Context, uuid.UUID) (db.Run, error)
	CreateRunEffectParentEvent(context.Context, db.CreateRunEffectParentEventParams) (db.RunEvent, error)
}

type runEffectNextDueStore interface {
	NextRunEffectDue(context.Context) (db.NextRunEffectDueRow, error)
}

type RunEffectWorker struct {
	queries  runEffectStore
	handlers runEffectHandlers
}

func NewRunEffectWorker(
	queries runEffectStore,
	webhook WebhookRunEffectHandler,
	delivery DeliveryRunEffectHandler,
) *RunEffectWorker {
	return &RunEffectWorker{
		queries: queries,
		handlers: runEffectHandlers{
			webhook:  webhook,
			delivery: delivery,
		},
	}
}

func (w *RunEffectWorker) SetHandlers(
	webhook WebhookRunEffectHandler,
	delivery DeliveryRunEffectHandler,
) {
	if w == nil {
		return
	}
	w.handlers = runEffectHandlers{webhook: webhook, delivery: delivery}
}

func (w *RunEffectWorker) ProcessOnce(
	ctx context.Context,
	cfg RunEffectWorkerConfig,
) (int, error) {
	if w == nil || w.queries == nil {
		return 0, errors.New("run effect worker is not configured")
	}
	cfg = normalizeRunEffectWorkerConfig(cfg)
	expired, err := w.queries.DeadLetterExpiredRunEffectsAtLimit(ctx)
	if err != nil {
		return 0, fmt.Errorf("dead-letter expired run effects: %w", err)
	}
	for _, effect := range expired {
		log.Warn().
			Str("effect_id", effect.ID.String()).
			Str("effect_type", effect.EffectType).
			Int32("attempt_count", effect.AttemptCount).
			Msg("runtime effect exhausted after processing lease expiry")
	}

	// LeaseOwner is a claim-generation token, not a long-lived Core instance
	// ID. A new UUID per batch fences late writes even when the same process
	// reacquires an expired Effect.
	leaseOwner := uuid.New()
	effects, err := w.queries.ClaimRunEffects(ctx, db.ClaimRunEffectsParams{
		LeaseOwner:      leaseOwner,
		LeaseDurationMs: cfg.LeaseDuration.Milliseconds(),
		Limit:           cfg.BatchSize,
	})
	if err != nil {
		return 0, fmt.Errorf("claim run effects: %w", err)
	}

	errs := make(chan error, len(effects))
	var wait sync.WaitGroup
	wait.Add(len(effects))
	for _, effect := range effects {
		effect := effect
		go func() {
			defer wait.Done()
			if effectErr := w.processClaimed(ctx, leaseOwner, effect); effectErr != nil {
				errs <- effectErr
			}
		}()
	}
	wait.Wait()
	close(errs)
	var combined error
	for effectErr := range errs {
		combined = errors.Join(combined, effectErr)
	}
	return len(effects), combined
}

func (w *RunEffectWorker) processClaimed(
	ctx context.Context,
	leaseOwner uuid.UUID,
	effect db.RunEffectOutbox,
) error {
	if effect.Status != "processing" || effect.LeaseOwner == nil ||
		*effect.LeaseOwner != leaseOwner || effect.AttemptCount <= 0 {
		return fmt.Errorf("claimed effect %s has invalid lease state", effect.ID)
	}
	var result RunEffectAttemptResult
	if effect.EffectType == RunEffectTypeParentCompletion {
		result = w.attemptParentCompletion(ctx, effect)
	} else {
		result = w.handlers.attempt(ctx, effect)
	}
	if result.Succeeded {
		_, err := w.queries.MarkRunEffectSucceeded(ctx, db.MarkRunEffectSucceededParams{
			ID: effect.ID, LeaseOwner: leaseOwner, AttemptCount: effect.AttemptCount,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("mark effect %s succeeded: %w", effect.ID, err)
		}
		return nil
	}

	lastError := result.persistedError()
	if !result.Retryable {
		_, err := w.queries.DeadLetterRunEffect(ctx, db.DeadLetterRunEffectParams{
			ID: effect.ID, LeaseOwner: leaseOwner, AttemptCount: effect.AttemptCount,
			LastError: lastError,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("dead-letter effect %s: %w", effect.ID, err)
		}
		return nil
	}

	_, err := w.queries.RetryOrDeadLetterRunEffect(ctx, db.RetryOrDeadLetterRunEffectParams{
		ID: effect.ID, LeaseOwner: leaseOwner, AttemptCount: effect.AttemptCount,
		RetryAfterMs: runEffectRetryDelay(effect.AttemptCount).Milliseconds(),
		LastError:    lastError,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("schedule effect %s retry: %w", effect.ID, err)
	}
	return nil
}

func runEffectRetryDelay(attemptCount int32) time.Duration {
	switch attemptCount {
	case 1:
		return time.Minute
	case 2:
		return 5 * time.Minute
	default:
		return 30 * time.Minute
	}
}

func (w *RunEffectWorker) attemptParentCompletion(
	ctx context.Context,
	effect db.RunEffectOutbox,
) RunEffectAttemptResult {
	parentRunID, err := parentRunIDFromEffect(effect)
	if err != nil {
		return PermanentRunEffectFailure(
			"EFFECT_TARGET_INVALID", "parent completion target is invalid", err,
		)
	}
	delegation, err := w.queries.GetRunDelegationByChild(ctx, effect.RunID)
	if errors.Is(err, pgx.ErrNoRows) {
		return PermanentRunEffectFailure(
			"DELEGATION_NOT_FOUND", "child Run delegation is unavailable", err,
		)
	}
	if err != nil {
		return RetryableRunEffectAttempt(
			"DELEGATION_READ_FAILED", "cannot read child Run delegation", err,
		)
	}
	if delegation.ParentRunID != parentRunID {
		return PermanentRunEffectFailure(
			"EFFECT_TARGET_MISMATCH", "parent completion target does not match delegation", nil,
		)
	}
	child, err := w.queries.GetRunByID(ctx, effect.RunID)
	if errors.Is(err, pgx.ErrNoRows) {
		return PermanentRunEffectFailure(
			"CHILD_RUN_NOT_FOUND", "child Run is unavailable", err,
		)
	}
	if err != nil {
		return RetryableRunEffectAttempt(
			"CHILD_RUN_READ_FAILED", "cannot read child Run", err,
		)
	}
	payload, err := json.Marshal(map[string]any{
		"child_run_id":    effect.RunID.String(),
		"caller_agent_id": delegation.CallerAgentID.String(),
		"target_agent_id": child.AgentID.String(),
		"status":          child.Status,
	})
	if err != nil {
		return PermanentRunEffectFailure(
			"PARENT_EVENT_PAYLOAD_INVALID", "parent completion payload cannot be encoded", err,
		)
	}
	eventID := deterministicRunEffectChildEventID(effect.ID)
	event, err := w.queries.CreateRunEffectParentEvent(ctx, db.CreateRunEffectParentEventParams{
		ID: eventID, ParentRunID: parentRunID, Payload: payload,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return PermanentRunEffectFailure(
			"PARENT_EVENT_ID_CONFLICT", "parent completion event identity conflicts", err,
		)
	}
	if err != nil {
		return RetryableRunEffectAttempt(
			"PARENT_EVENT_WRITE_FAILED", "cannot append parent completion event", err,
		)
	}
	if event.ID != eventID || event.RunID != parentRunID ||
		event.EventType != "run.child.completed" || !jsonBytesEqual(event.Payload, payload) {
		return PermanentRunEffectFailure(
			"PARENT_EVENT_ID_CONFLICT", "parent completion event identity conflicts", nil,
		)
	}
	// Existing parent Run subscribers continue to receive run.child.completed.
	// EnqueueRunEvent is idempotent on (subscription_id, run_event_id).
	if w.handlers.webhook == nil {
		return RetryableRunEffectAttempt(
			"HANDLER_UNAVAILABLE", "parent callback handler unavailable", nil,
		)
	}
	if err := w.handlers.webhook.EnqueueRunEventDurable(ctx, event); err != nil {
		return RetryableRunEffectAttempt(
			"PARENT_CALLBACK_ENQUEUE_FAILED", "cannot enqueue parent completion callbacks", err,
		)
	}
	return RunEffectAttemptSucceeded()
}

func parentRunIDFromEffect(effect db.RunEffectOutbox) (uuid.UUID, error) {
	const prefix = "parent_run:"
	var metadata map[string]any
	if len(effect.Metadata) > 0 {
		_ = json.Unmarshal(effect.Metadata, &metadata)
	}
	metadataValue, _ := metadata["parent_run_id"].(string)
	metadataValue = strings.TrimSpace(metadataValue)
	keyValue := ""
	if strings.HasPrefix(effect.TargetKey, prefix) {
		keyValue = strings.TrimSpace(strings.TrimPrefix(effect.TargetKey, prefix))
	}
	value := metadataValue
	if value == "" {
		value = keyValue
	}
	id, err := uuid.Parse(value)
	if err != nil {
		return uuid.Nil, err
	}
	if metadataValue != "" && keyValue != "" && metadataValue != keyValue {
		return uuid.Nil, errors.New("parent target key and metadata disagree")
	}
	return id, nil
}

func jsonBytesEqual(left, right []byte) bool {
	var leftValue any
	var rightValue any
	if json.Unmarshal(left, &leftValue) != nil || json.Unmarshal(right, &rightValue) != nil {
		return false
	}
	leftCanonical, leftErr := CanonicalizeRFC8785(leftValue)
	rightCanonical, rightErr := CanonicalizeRFC8785(rightValue)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftCanonical, rightCanonical)
}

func (w *RunEffectWorker) Replay(
	ctx context.Context,
	effectID uuid.UUID,
	actorType string,
	actorID *uuid.UUID,
	reason string,
) (*db.RunEffectOutbox, error) {
	if w == nil || w.queries == nil {
		return nil, errors.New("run effect worker is not configured")
	}
	effect, err := w.queries.GetRunEffectByID(ctx, effectID)
	if err != nil {
		return nil, err
	}
	if effect.Status != "dead_letter" {
		return nil, errors.New("run effect is not dead-lettered")
	}
	// Reset downstream first. If the process stops before ReplayRunEffect, the
	// outbox remains dead-lettered and legacy workers ignore the linked row.
	if err := w.handlers.reset(ctx, effect); err != nil {
		return nil, err
	}
	replayed, err := w.queries.ReplayRunEffect(ctx, db.ReplayRunEffectParams{
		ID: effectID, ActorType: actorType, ActorID: actorID, Reason: reason,
	})
	if err != nil {
		return nil, err
	}
	return &replayed, nil
}

func StartRunEffectWorker(
	ctx context.Context,
	svc *Service,
	cfg RunEffectWorkerConfig,
) {
	if svc == nil || svc.effectWorker == nil {
		return
	}
	cfg = normalizeRunEffectWorkerConfig(cfg)
	log.Info().
		Dur("interval", cfg.Interval).
		Dur("lease_duration", cfg.LeaseDuration).
		Int32("batch_size", cfg.BatchSize).
		Msg("runtime: run effect worker started")
	defer log.Info().Msg("runtime: run effect worker stopped")

	runEffectWorkerTick(ctx, svc.effectWorker, cfg)
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runEffectWorkerTick(ctx, svc.effectWorker, cfg)
		}
	}
}

// StartRunEffectWorkerWithWake preserves the legacy worker as the degraded
// path. While LISTEN is healthy, a topic notification, the earliest durable
// due timestamp, or the 60-second reconciliation pass is the only reason to
// query/claim Effects.
func StartRunEffectWorkerWithWake(
	ctx context.Context,
	svc *Service,
	cfg RunEffectWorkerConfig,
	source eventwake.TopicSource,
) {
	if svc == nil || svc.effectWorker == nil {
		return
	}
	cfg = normalizeRunEffectWorkerConfig(cfg)
	worker := svc.effectWorker
	if source == nil {
		StartRunEffectWorker(ctx, svc, cfg)
		return
	}
	if _, ok := worker.queries.(runEffectNextDueStore); !ok {
		StartRunEffectWorker(ctx, svc, cfg)
		return
	}

	log.Info().
		Dur("fallback_interval", cfg.Interval).
		Dur("lease_duration", cfg.LeaseDuration).
		Int32("batch_size", cfg.BatchSize).
		Msg("runtime: event-driven run effect worker started")
	defer log.Info().Msg("runtime: event-driven run effect worker stopped")

	for ctx.Err() == nil {
		if !eventWakeSourceHealthy(source) {
			runEffectWorkerPass(ctx, worker, cfg, "degraded_poll")
			if !waitWorkerFallbackInterval(ctx, cfg.Interval) {
				return
			}
			continue
		}

		subscription, err := source.SubscribeTopic(runEffectWorkerWakeTopic)
		if err != nil {
			runEffectWorkerPass(ctx, worker, cfg, "degraded_poll")
			if !waitWorkerFallbackInterval(ctx, cfg.Interval) {
				return
			}
			continue
		}
		if !eventWakeSourceHealthy(source) {
			subscription.Close()
			continue
		}
		err = eventwake.RunScheduler(ctx, subscription, eventwake.SchedulerConfig{
			ReconcileInterval: time.Minute,
			ErrorRetry:        cfg.Interval,
			HealthCheck:       cfg.Interval,
			Healthy:           func() bool { return eventWakeSourceHealthy(source) },
		}, func(runCtx context.Context, reason string) (eventwake.SchedulerResult, error) {
			if passErr := runEffectWorkerPass(runCtx, worker, cfg, reason); passErr != nil {
				return eventwake.SchedulerResult{}, passErr
			}
			return worker.nextDueSchedule(runCtx)
		})
		subscription.Close()
		if ctx.Err() != nil {
			return
		}
		if err != nil && !errors.Is(err, eventwake.ErrWakeSourceDegraded) {
			log.Warn().Err(err).Msg("runtime: run effect event scheduler stopped; polling fallback enabled")
		}
	}
}

func runEffectWorkerTick(
	ctx context.Context,
	worker *RunEffectWorker,
	cfg RunEffectWorkerConfig,
) {
	_ = runEffectWorkerPass(ctx, worker, cfg, "tick")
}

func runEffectWorkerPass(
	ctx context.Context,
	worker *RunEffectWorker,
	cfg RunEffectWorkerConfig,
	reason string,
) error {
	observeWorker(cfg.Observer, "runtime.run_effect.claim", reason, int(cfg.BatchSize))
	processed, err := worker.ProcessOnce(ctx, cfg)
	if err != nil {
		log.Warn().Err(err).Msg("runtime: run effect worker tick failed")
		return err
	}
	if processed > 0 {
		log.Debug().Int("processed", processed).Msg("runtime: run effects processed")
	}
	return nil
}

func (w *RunEffectWorker) nextDueSchedule(
	ctx context.Context,
) (eventwake.SchedulerResult, error) {
	store, ok := w.queries.(runEffectNextDueStore)
	if !ok {
		return eventwake.SchedulerResult{}, errors.New("run effect next-due query is not configured")
	}
	next, err := store.NextRunEffectDue(ctx)
	if err != nil {
		return eventwake.SchedulerResult{}, fmt.Errorf("read next run effect due time: %w", err)
	}
	return databaseDueSchedule(next.NextDueAt, next.DatabaseNow), nil
}
