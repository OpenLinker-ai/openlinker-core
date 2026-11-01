package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	runtimepkg "github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

func (s *Service) AttemptAgentWebhookEffect(
	ctx context.Context,
	effect db.RunEffectOutbox,
) runtimepkg.RunEffectAttemptResult {
	if effect.EffectType != runtimepkg.RunEffectTypeAgentWebhook {
		return runtimepkg.PermanentRunEffectFailure(
			"EFFECT_TYPE_MISMATCH", "effect is not an Agent webhook", nil,
		)
	}
	leaseOwner, leaseErr := claimedEffectLeaseOwner(effect)
	if leaseErr != nil {
		return runtimepkg.PermanentRunEffectFailure(
			"EFFECT_LEASE_INVALID", "effect delivery lease is invalid", leaseErr,
		)
	}
	q, err := s.runEffectQueries()
	if err != nil {
		return runtimepkg.RetryableRunEffectAttempt(
			"EFFECT_STORE_UNAVAILABLE", "effect delivery store unavailable", err,
		)
	}

	row, err := q.GetWebhookDeliveryByID(ctx, effect.ID)
	if errors.Is(err, pgx.ErrNoRows) {
		row, err = s.materializeAgentWebhookEffect(ctx, q, effect)
	}
	if err != nil {
		return classifyWebhookEffectMaterializationError(err)
	}
	if state := validateWebhookEffectDelivery(effect, row.WebhookDelivery); !state.Succeeded || state.ErrorCode != "" {
		return state
	}
	return s.attemptAgentWebhookEffectDelivery(ctx, q, effect, leaseOwner, row)
}

func (s *Service) materializeAgentWebhookEffect(
	ctx context.Context,
	q webhookEffectQueries,
	effect db.RunEffectOutbox,
) (db.GetWebhookDeliveryRow, error) {
	targetAgentID, err := effectTargetUUID(effect, "agent:", "agent_id")
	if err != nil {
		return db.GetWebhookDeliveryRow{}, permanentEffectHandlerError{
			code: "EFFECT_TARGET_INVALID", safeMessage: "Agent webhook target is invalid", cause: err,
		}
	}
	run, err := q.GetRunByID(ctx, effect.RunID)
	if err != nil {
		return db.GetWebhookDeliveryRow{}, err
	}
	if run.AgentID != targetAgentID {
		return db.GetWebhookDeliveryRow{}, permanentEffectHandlerError{
			code: "EFFECT_TARGET_MISMATCH", safeMessage: "Agent webhook target does not match Run", cause: nil,
		}
	}
	cfg, err := q.GetAgentWebhookConfig(ctx, targetAgentID)
	if err != nil {
		return db.GetWebhookDeliveryRow{}, err
	}
	if cfg.WebhookURL == nil || strings.TrimSpace(*cfg.WebhookURL) == "" {
		return db.GetWebhookDeliveryRow{}, permanentEffectHandlerError{
			code: "WEBHOOK_TARGET_UNAVAILABLE", safeMessage: "Agent webhook is no longer configured", cause: nil,
		}
	}

	payload, err := json.Marshal(buildPayload(&run, cfg.Slug, runOutput(&run)))
	if err != nil {
		return db.GetWebhookDeliveryRow{}, permanentEffectHandlerError{
			code: "WEBHOOK_PAYLOAD_INVALID", safeMessage: "Agent webhook payload cannot be encoded", cause: err,
		}
	}
	delivery, err := q.CreateWebhookEffectDelivery(ctx, db.CreateWebhookEffectDeliveryParams{
		ID:      effect.ID,
		AgentID: targetAgentID,
		RunID:   effect.RunID,
		URL:     strings.TrimSpace(*cfg.WebhookURL),
		Payload: payload,
	})
	if err != nil {
		return db.GetWebhookDeliveryRow{}, err
	}
	if delivery.ID != effect.ID || delivery.EffectOutboxID == nil || *delivery.EffectOutboxID != effect.ID {
		return db.GetWebhookDeliveryRow{}, permanentEffectHandlerError{
			code: "DELIVERY_ID_CONFLICT", safeMessage: "Agent webhook delivery identity conflicts", cause: nil,
		}
	}
	return q.GetWebhookDeliveryByID(ctx, effect.ID)
}

func (s *Service) attemptAgentWebhookEffectDelivery(
	ctx context.Context,
	q webhookEffectQueries,
	effect db.RunEffectOutbox,
	leaseOwner uuid.UUID,
	row db.GetWebhookDeliveryRow,
) runtimepkg.RunEffectAttemptResult {
	if row.Status == "success" {
		return runtimepkg.RunEffectAttemptSucceeded()
	}
	if row.Status == "failed" {
		return runtimepkg.PermanentRunEffectFailure(
			"DELIVERY_ALREADY_FAILED", "Agent webhook delivery is already failed", nil,
		)
	}
	if row.Status != "pending" {
		return runtimepkg.PermanentRunEffectFailure(
			"DELIVERY_STATE_INVALID", "Agent webhook delivery state is invalid", nil,
		)
	}
	if row.WebhookSecret == nil || *row.WebhookSecret == "" {
		message := "webhook secret is unavailable"
		rows, err := q.MarkWebhookEffectDeliveryFailed(ctx, db.MarkWebhookEffectDeliveryFailedParams{
			ID:           effect.ID,
			ErrorMessage: &message,
			AttemptCount: effect.AttemptCount,
			LeaseOwner:   leaseOwner,
		})
		if err != nil {
			return runtimepkg.RetryableRunEffectAttempt(
				"DELIVERY_STATE_WRITE_FAILED", "cannot record Agent webhook failure", err,
			)
		}
		if rows == 0 {
			return resolveSupersededWebhookAttempt(ctx, q, effect)
		}
		return runtimepkg.PermanentRunEffectFailure(
			"WEBHOOK_SECRET_UNAVAILABLE", "Agent webhook credential is unavailable", nil,
		)
	}

	statusCode, responseBody, attemptErr := s.doDeliver(
		ctx, row.URL, *row.WebhookSecret, effect.ID, row.Payload,
	)
	statusPtr, bodyPtr, errorMessage := effectHTTPOutcome(statusCode, responseBody, attemptErr)
	if attemptErr == nil && statusCode >= http.StatusOK && statusCode < http.StatusMultipleChoices {
		rows, err := q.MarkWebhookEffectDeliverySuccess(ctx, db.MarkWebhookEffectDeliverySuccessParams{
			ID:             effect.ID,
			ResponseStatus: statusPtr,
			ResponseBody:   bodyPtr,
			AttemptCount:   effect.AttemptCount,
			LeaseOwner:     leaseOwner,
		})
		if err != nil {
			return runtimepkg.RetryableRunEffectAttempt(
				"DELIVERY_STATE_WRITE_FAILED", "cannot record Agent webhook success", err,
			)
		}
		if rows == 0 {
			return resolveSupersededWebhookAttempt(ctx, q, effect)
		}
		return runtimepkg.RunEffectAttemptSucceeded()
	}

	finalAttempt := effect.AttemptCount >= effect.MaxAttempts
	var (
		rows    int64
		markErr error
	)
	if finalAttempt {
		rows, markErr = q.MarkWebhookEffectDeliveryFailed(ctx, db.MarkWebhookEffectDeliveryFailedParams{
			ID:             effect.ID,
			ResponseStatus: statusPtr,
			ResponseBody:   bodyPtr,
			ErrorMessage:   errorMessage,
			AttemptCount:   effect.AttemptCount,
			LeaseOwner:     leaseOwner,
		})
	} else {
		rows, markErr = q.MarkWebhookEffectDeliveryRetry(ctx, db.MarkWebhookEffectDeliveryRetryParams{
			ID:             effect.ID,
			ResponseStatus: statusPtr,
			ResponseBody:   bodyPtr,
			ErrorMessage:   errorMessage,
			AttemptCount:   effect.AttemptCount,
			LeaseOwner:     leaseOwner,
		})
	}
	if markErr != nil {
		return runtimepkg.RetryableRunEffectAttempt(
			"DELIVERY_STATE_WRITE_FAILED", "cannot record Agent webhook attempt", markErr,
		)
	}
	if rows == 0 {
		return resolveSupersededWebhookAttempt(ctx, q, effect)
	}
	return retryableHTTPResult(statusCode, attemptErr, "Agent webhook delivery failed")
}

func (s *Service) AttemptTaskCallbackEffect(
	ctx context.Context,
	effect db.RunEffectOutbox,
) runtimepkg.RunEffectAttemptResult {
	if effect.EffectType != runtimepkg.RunEffectTypeTaskCallback {
		return runtimepkg.PermanentRunEffectFailure(
			"EFFECT_TYPE_MISMATCH", "effect is not a task callback", nil,
		)
	}
	leaseOwner, leaseErr := claimedEffectLeaseOwner(effect)
	if leaseErr != nil {
		return runtimepkg.PermanentRunEffectFailure(
			"EFFECT_LEASE_INVALID", "effect delivery lease is invalid", leaseErr,
		)
	}
	q, err := s.runEffectQueries()
	if err != nil {
		return runtimepkg.RetryableRunEffectAttempt(
			"EFFECT_STORE_UNAVAILABLE", "effect delivery store unavailable", err,
		)
	}
	row, err := q.GetTaskCallbackDeliveryByID(ctx, effect.ID)
	if errors.Is(err, pgx.ErrNoRows) {
		row, err = s.materializeTaskCallbackEffect(ctx, q, effect)
	}
	if err != nil {
		return classifyWebhookEffectMaterializationError(err)
	}
	if state := validateTaskCallbackEffectDelivery(effect, row.TaskCallbackDelivery); !state.Succeeded || state.ErrorCode != "" {
		return state
	}
	return s.attemptTaskCallbackEffectDelivery(ctx, q, effect, leaseOwner, row)
}

func (s *Service) materializeTaskCallbackEffect(
	ctx context.Context,
	q webhookEffectQueries,
	effect db.RunEffectOutbox,
) (db.GetTaskCallbackDeliveryByIDRow, error) {
	subscriptionID, err := effectTargetUUID(effect, "subscription:", "subscription_id")
	if err != nil {
		return db.GetTaskCallbackDeliveryByIDRow{}, permanentEffectHandlerError{
			code: "EFFECT_TARGET_INVALID", safeMessage: "task callback target is invalid", cause: err,
		}
	}
	subscription, err := q.GetTaskCallbackSubscriptionByID(ctx, subscriptionID)
	if err != nil {
		return db.GetTaskCallbackDeliveryByIDRow{}, err
	}
	if subscription.RunID != effect.RunID {
		return db.GetTaskCallbackDeliveryByIDRow{}, permanentEffectHandlerError{
			code: "EFFECT_TARGET_MISMATCH", safeMessage: "task callback target does not match Run", cause: nil,
		}
	}
	event, err := q.GetTaskCallbackEffectEvent(ctx, db.GetTaskCallbackEffectEventParams{
		ID: effect.TerminalEventID, RunID: effect.RunID,
	})
	if err != nil {
		return db.GetTaskCallbackDeliveryByIDRow{}, err
	}
	projected, err := projectedTaskCallbackPayload(ctx, q, subscription, event)
	if err != nil {
		return db.GetTaskCallbackDeliveryByIDRow{}, err
	}
	payload, err := json.Marshal(projected)
	if err != nil {
		return db.GetTaskCallbackDeliveryByIDRow{}, permanentEffectHandlerError{
			code: "CALLBACK_PAYLOAD_INVALID", safeMessage: "task callback payload cannot be encoded", cause: err,
		}
	}
	delivery, err := q.CreateTaskCallbackEffectDelivery(ctx, db.CreateTaskCallbackEffectDeliveryParams{
		ID:             effect.ID,
		SubscriptionID: subscriptionID,
		RunEventID:     effect.TerminalEventID,
		Payload:        payload,
	})
	if err != nil {
		return db.GetTaskCallbackDeliveryByIDRow{}, err
	}
	if delivery.ID != effect.ID || delivery.EffectOutboxID == nil || *delivery.EffectOutboxID != effect.ID {
		return db.GetTaskCallbackDeliveryByIDRow{}, permanentEffectHandlerError{
			code: "DELIVERY_ID_CONFLICT", safeMessage: "task callback delivery identity conflicts", cause: nil,
		}
	}
	return q.GetTaskCallbackDeliveryByID(ctx, effect.ID)
}

func (s *Service) attemptTaskCallbackEffectDelivery(
	ctx context.Context,
	q webhookEffectQueries,
	effect db.RunEffectOutbox,
	leaseOwner uuid.UUID,
	row db.GetTaskCallbackDeliveryByIDRow,
) runtimepkg.RunEffectAttemptResult {
	if row.Status == "success" {
		return runtimepkg.RunEffectAttemptSucceeded()
	}
	if row.Status == "failed" {
		return runtimepkg.PermanentRunEffectFailure(
			"DELIVERY_ALREADY_FAILED", "task callback delivery is already failed", nil,
		)
	}
	if row.Status != "pending" {
		return runtimepkg.PermanentRunEffectFailure(
			"DELIVERY_STATE_INVALID", "task callback delivery state is invalid", nil,
		)
	}

	statusCode, responseBody, attemptErr := s.doDeliverWithEvent(
		ctx,
		row.TargetURL,
		row.Secret,
		effect.ID,
		row.Payload,
		row.EventType,
		row.AuthScheme,
		row.AuthCredentials,
	)
	statusPtr, bodyPtr, errorMessage := effectHTTPOutcome(statusCode, responseBody, attemptErr)
	if attemptErr == nil && statusCode >= http.StatusOK && statusCode < http.StatusMultipleChoices {
		rows, err := q.MarkTaskCallbackEffectDeliverySuccess(ctx, db.MarkTaskCallbackEffectDeliverySuccessParams{
			ID:             effect.ID,
			ResponseStatus: statusPtr,
			ResponseBody:   bodyPtr,
			AttemptCount:   effect.AttemptCount,
			LeaseOwner:     leaseOwner,
		})
		if err != nil {
			return runtimepkg.RetryableRunEffectAttempt(
				"DELIVERY_STATE_WRITE_FAILED", "cannot record task callback success", err,
			)
		}
		if rows == 0 {
			return resolveSupersededTaskCallbackAttempt(ctx, q, effect)
		}
		return runtimepkg.RunEffectAttemptSucceeded()
	}

	finalAttempt := effect.AttemptCount >= effect.MaxAttempts
	var (
		rows    int64
		markErr error
	)
	if finalAttempt {
		rows, markErr = q.MarkTaskCallbackEffectDeliveryFailed(ctx, db.MarkTaskCallbackEffectDeliveryFailedParams{
			ID:             effect.ID,
			ResponseStatus: statusPtr,
			ResponseBody:   bodyPtr,
			ErrorMessage:   errorMessage,
			AttemptCount:   effect.AttemptCount,
			LeaseOwner:     leaseOwner,
		})
	} else {
		rows, markErr = q.MarkTaskCallbackEffectDeliveryRetry(ctx, db.MarkTaskCallbackEffectDeliveryRetryParams{
			ID:             effect.ID,
			ResponseStatus: statusPtr,
			ResponseBody:   bodyPtr,
			ErrorMessage:   errorMessage,
			AttemptCount:   effect.AttemptCount,
			LeaseOwner:     leaseOwner,
		})
	}
	if markErr != nil {
		return runtimepkg.RetryableRunEffectAttempt(
			"DELIVERY_STATE_WRITE_FAILED", "cannot record task callback attempt", markErr,
		)
	}
	if rows == 0 {
		return resolveSupersededTaskCallbackAttempt(ctx, q, effect)
	}
	return retryableHTTPResult(statusCode, attemptErr, "task callback delivery failed")
}

func (s *Service) ResetWebhookEffectDelivery(ctx context.Context, effect db.RunEffectOutbox) error {
	q, err := s.runEffectQueries()
	if err != nil {
		return err
	}
	switch effect.EffectType {
	case runtimepkg.RunEffectTypeAgentWebhook:
		var rows int64
		rows, err = q.ResetWebhookEffectDelivery(ctx, effect.ID)
		if err == nil && rows == 0 {
			err = reconcileWebhookEffectReset(ctx, q, effect)
		}
	case runtimepkg.RunEffectTypeTaskCallback:
		var rows int64
		rows, err = q.ResetTaskCallbackEffectDelivery(ctx, effect.ID)
		if err == nil && rows == 0 {
			err = reconcileTaskCallbackEffectReset(ctx, q, effect)
		}
	default:
		return fmt.Errorf("unsupported webhook effect type %q", effect.EffectType)
	}
	return err
}

// A zero-row reset is ambiguous: the downstream row may not have been
// materialized, a previous replay may already have reset it, or its identity
// may conflict. Only the first two states are safe to replay.
func reconcileWebhookEffectReset(
	ctx context.Context,
	q webhookEffectQueries,
	effect db.RunEffectOutbox,
) error {
	row, err := q.GetWebhookDeliveryByID(ctx, effect.ID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read Agent webhook delivery after reset: %w", err)
	}
	if state := validateWebhookEffectDelivery(effect, row.WebhookDelivery); state.ErrorCode != "" {
		return errors.New(state.SafeMessage)
	}
	switch row.Status {
	case "pending", "success":
		return nil
	case "failed":
		return errors.New("Agent webhook delivery remains failed after replay reset")
	default:
		return fmt.Errorf("Agent webhook delivery has invalid replay state %q", row.Status)
	}
}

func reconcileTaskCallbackEffectReset(
	ctx context.Context,
	q webhookEffectQueries,
	effect db.RunEffectOutbox,
) error {
	row, err := q.GetTaskCallbackDeliveryByID(ctx, effect.ID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read task callback delivery after reset: %w", err)
	}
	if state := validateTaskCallbackEffectDelivery(effect, row.TaskCallbackDelivery); state.ErrorCode != "" {
		return errors.New(state.SafeMessage)
	}
	switch row.Status {
	case "pending", "success":
		return nil
	case "failed":
		return errors.New("task callback delivery remains failed after replay reset")
	default:
		return fmt.Errorf("task callback delivery has invalid replay state %q", row.Status)
	}
}

func (s *Service) runEffectQueries() (webhookEffectQueries, error) {
	if s == nil {
		return nil, errors.New("webhook service is nil")
	}
	if s.effectQueries != nil {
		return s.effectQueries, nil
	}
	if q, ok := s.queries.(webhookEffectQueries); ok {
		return q, nil
	}
	return nil, errors.New("webhook effect queries are not configured")
}

type permanentEffectHandlerError struct {
	code        string
	safeMessage string
	cause       error
}

func (e permanentEffectHandlerError) Error() string {
	if e.cause != nil {
		return e.cause.Error()
	}
	return e.safeMessage
}

func classifyWebhookEffectMaterializationError(err error) runtimepkg.RunEffectAttemptResult {
	var permanent permanentEffectHandlerError
	if errors.As(err, &permanent) {
		return runtimepkg.PermanentRunEffectFailure(
			permanent.code, permanent.safeMessage, permanent.cause,
		)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return runtimepkg.PermanentRunEffectFailure(
			"EFFECT_TARGET_UNAVAILABLE", "effect target or delivery identity is unavailable", err,
		)
	}
	return runtimepkg.RetryableRunEffectAttempt(
		"EFFECT_MATERIALIZATION_FAILED", "effect delivery cannot be materialized", err,
	)
}

func validateWebhookEffectDelivery(
	effect db.RunEffectOutbox,
	delivery db.WebhookDelivery,
) runtimepkg.RunEffectAttemptResult {
	if delivery.ID != effect.ID || delivery.RunID != effect.RunID ||
		delivery.EffectOutboxID == nil || *delivery.EffectOutboxID != effect.ID {
		return runtimepkg.PermanentRunEffectFailure(
			"DELIVERY_ID_CONFLICT", "Agent webhook delivery identity conflicts", nil,
		)
	}
	if delivery.Status == "success" {
		return runtimepkg.RunEffectAttemptSucceeded()
	}
	return runtimepkg.RunEffectAttemptResult{Succeeded: true}
}

func validateTaskCallbackEffectDelivery(
	effect db.RunEffectOutbox,
	delivery db.TaskCallbackDelivery,
) runtimepkg.RunEffectAttemptResult {
	if delivery.ID != effect.ID || delivery.RunEventID != effect.TerminalEventID ||
		delivery.EffectOutboxID == nil || *delivery.EffectOutboxID != effect.ID {
		return runtimepkg.PermanentRunEffectFailure(
			"DELIVERY_ID_CONFLICT", "task callback delivery identity conflicts", nil,
		)
	}
	if delivery.Status == "success" {
		return runtimepkg.RunEffectAttemptSucceeded()
	}
	return runtimepkg.RunEffectAttemptResult{Succeeded: true}
}

func resolveSupersededWebhookAttempt(
	ctx context.Context,
	q webhookEffectQueries,
	effect db.RunEffectOutbox,
) runtimepkg.RunEffectAttemptResult {
	row, err := q.GetWebhookDeliveryByID(ctx, effect.ID)
	if err != nil {
		return runtimepkg.RetryableRunEffectAttempt(
			"DELIVERY_STATE_READ_FAILED", "cannot resolve Agent webhook attempt state", err,
		)
	}
	if row.Status == "success" {
		return runtimepkg.RunEffectAttemptSucceeded()
	}
	if row.Status == "failed" {
		return runtimepkg.PermanentRunEffectFailure(
			"DELIVERY_ALREADY_FAILED", "Agent webhook delivery is already failed", nil,
		)
	}
	return runtimepkg.RetryableRunEffectAttempt(
		"DELIVERY_ATTEMPT_SUPERSEDED", "Agent webhook attempt was superseded", nil,
	)
}

func resolveSupersededTaskCallbackAttempt(
	ctx context.Context,
	q webhookEffectQueries,
	effect db.RunEffectOutbox,
) runtimepkg.RunEffectAttemptResult {
	row, err := q.GetTaskCallbackDeliveryByID(ctx, effect.ID)
	if err != nil {
		return runtimepkg.RetryableRunEffectAttempt(
			"DELIVERY_STATE_READ_FAILED", "cannot resolve task callback attempt state", err,
		)
	}
	if row.Status == "success" {
		return runtimepkg.RunEffectAttemptSucceeded()
	}
	if row.Status == "failed" {
		return runtimepkg.PermanentRunEffectFailure(
			"DELIVERY_ALREADY_FAILED", "task callback delivery is already failed", nil,
		)
	}
	return runtimepkg.RetryableRunEffectAttempt(
		"DELIVERY_ATTEMPT_SUPERSEDED", "task callback attempt was superseded", nil,
	)
}

func effectTargetUUID(effect db.RunEffectOutbox, prefix, metadataKey string) (uuid.UUID, error) {
	var metadata map[string]any
	if len(effect.Metadata) > 0 {
		_ = json.Unmarshal(effect.Metadata, &metadata)
	}
	var metadataValue string
	if raw, ok := metadata[metadataKey].(string); ok {
		metadataValue = strings.TrimSpace(raw)
	}
	keyValue := strings.TrimSpace(strings.TrimPrefix(effect.TargetKey, prefix))
	if !strings.HasPrefix(effect.TargetKey, prefix) {
		keyValue = ""
	}
	value := metadataValue
	if value == "" {
		value = keyValue
	}
	id, err := uuid.Parse(value)
	if err != nil {
		return uuid.Nil, err
	}
	if keyValue != "" && metadataValue != "" && keyValue != metadataValue {
		return uuid.Nil, errors.New("target key and metadata disagree")
	}
	return id, nil
}

func runOutput(run *db.Run) map[string]interface{} {
	if run == nil || run.Status != "success" || len(run.Output) == 0 {
		return nil
	}
	var output map[string]interface{}
	if err := json.Unmarshal(run.Output, &output); err != nil {
		return nil
	}
	return output
}

func effectHTTPOutcome(
	statusCode int,
	responseBody string,
	attemptErr error,
) (*int32, *string, *string) {
	statusPtr := responseStatusPtr(statusCode)
	var bodyPtr *string
	if statusCode > 0 {
		body := truncate(responseBody, responseBodyMaxLen)
		bodyPtr = &body
	}
	var message string
	if attemptErr != nil {
		message = "network delivery failed"
	} else {
		message = fmt.Sprintf("HTTP %d", statusCode)
	}
	return statusPtr, bodyPtr, &message
}

func retryableHTTPResult(
	statusCode int,
	attemptErr error,
	safeMessage string,
) runtimepkg.RunEffectAttemptResult {
	code := "NETWORK_ERROR"
	if statusCode > 0 {
		code = fmt.Sprintf("HTTP_%d", statusCode)
	}
	return runtimepkg.RetryableRunEffectAttempt(code, safeMessage, attemptErr)
}

func claimedEffectLeaseOwner(effect db.RunEffectOutbox) (uuid.UUID, error) {
	if effect.Status != "processing" || effect.LeaseOwner == nil || *effect.LeaseOwner == uuid.Nil ||
		effect.AttemptCount <= 0 {
		return uuid.Nil, errors.New("effect is not held by a processing lease")
	}
	return *effect.LeaseOwner, nil
}
