package delivery

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

func (s *Service) AttemptDefaultDeliveryEffect(
	ctx context.Context,
	effect db.RunEffectOutbox,
) runtimepkg.RunEffectAttemptResult {
	if effect.EffectType != runtimepkg.RunEffectTypeDefaultDelivery {
		return runtimepkg.PermanentRunEffectFailure(
			"EFFECT_TYPE_MISMATCH", "effect is not a default delivery", nil,
		)
	}
	leaseOwner, leaseErr := claimedDeliveryEffectLeaseOwner(effect)
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
	row, err := q.GetRunDeliveryByID(ctx, effect.ID)
	if errors.Is(err, pgx.ErrNoRows) {
		row, err = s.materializeDefaultDeliveryEffect(ctx, q, effect)
	}
	if err != nil {
		return classifyDeliveryEffectMaterializationError(err)
	}
	if row.ID != effect.ID || row.RunID != effect.RunID ||
		row.EffectOutboxID == nil || *row.EffectOutboxID != effect.ID {
		return runtimepkg.PermanentRunEffectFailure(
			"DELIVERY_ID_CONFLICT", "default delivery identity conflicts", nil,
		)
	}
	return s.attemptDefaultEffectDelivery(ctx, q, effect, leaseOwner, row)
}

func (s *Service) materializeDefaultDeliveryEffect(
	ctx context.Context,
	q deliveryEffectQueries,
	effect db.RunEffectOutbox,
) (db.GetRunDeliveryRow, error) {
	targetID, err := deliveryEffectTargetUUID(effect)
	if err != nil {
		return db.GetRunDeliveryRow{}, permanentDeliveryEffectError{
			code: "EFFECT_TARGET_INVALID", safeMessage: "default delivery target is invalid", cause: err,
		}
	}
	run, err := q.GetRunByID(ctx, effect.RunID)
	if err != nil {
		return db.GetRunDeliveryRow{}, err
	}
	target, err := q.GetDeliveryTargetByID(ctx, targetID)
	if err != nil {
		return db.GetRunDeliveryRow{}, err
	}
	if target.UserID != run.UserID {
		return db.GetRunDeliveryRow{}, permanentDeliveryEffectError{
			code: "EFFECT_TARGET_MISMATCH", safeMessage: "default delivery target does not match Run owner", cause: nil,
		}
	}
	agent, err := q.GetAgentByID(ctx, run.AgentID)
	if err != nil {
		return db.GetRunDeliveryRow{}, err
	}
	targetURL := parseTargetConfig(target.Config).URL
	if targetURL == "" {
		return db.GetRunDeliveryRow{}, permanentDeliveryEffectError{
			code: "DELIVERY_TARGET_UNAVAILABLE", safeMessage: "default delivery target has no endpoint", cause: nil,
		}
	}
	payload, err := json.Marshal(projectDeliveryPayload(buildPayload(
		&run,
		agent.Slug,
		agent.Name,
		deliveryEventForRunStatus(run.Status),
	), run.UserID, agent))
	if err != nil {
		return db.GetRunDeliveryRow{}, permanentDeliveryEffectError{
			code: "DELIVERY_PAYLOAD_INVALID", safeMessage: "default delivery payload cannot be encoded", cause: err,
		}
	}
	delivery, err := q.CreateRunEffectDelivery(ctx, db.CreateRunEffectDeliveryParams{
		ID:         effect.ID,
		RunID:      effect.RunID,
		TargetID:   target.ID,
		UserID:     run.UserID,
		TargetType: target.Type,
		TargetURL:  targetURL,
		Payload:    payload,
	})
	if err != nil {
		return db.GetRunDeliveryRow{}, err
	}
	if delivery.ID != effect.ID || delivery.EffectOutboxID == nil || *delivery.EffectOutboxID != effect.ID {
		return db.GetRunDeliveryRow{}, permanentDeliveryEffectError{
			code: "DELIVERY_ID_CONFLICT", safeMessage: "default delivery identity conflicts", cause: nil,
		}
	}
	return q.GetRunDeliveryByID(ctx, effect.ID)
}

func (s *Service) attemptDefaultEffectDelivery(
	ctx context.Context,
	q deliveryEffectQueries,
	effect db.RunEffectOutbox,
	leaseOwner uuid.UUID,
	row db.GetRunDeliveryRow,
) runtimepkg.RunEffectAttemptResult {
	if row.Status == "success" {
		return runtimepkg.RunEffectAttemptSucceeded()
	}
	if row.Status == "failed" {
		return runtimepkg.PermanentRunEffectFailure(
			"DELIVERY_ALREADY_FAILED", "default delivery is already failed", nil,
		)
	}
	if row.Status != "pending" {
		return runtimepkg.PermanentRunEffectFailure(
			"DELIVERY_STATE_INVALID", "default delivery state is invalid", nil,
		)
	}
	if row.TargetSecret == nil {
		message := "delivery target is unavailable"
		rows, err := q.MarkRunEffectDeliveryFailed(ctx, db.MarkRunEffectDeliveryFailedParams{
			ID:           effect.ID,
			ErrorMessage: &message,
			AttemptCount: effect.AttemptCount,
			LeaseOwner:   leaseOwner,
		})
		if err != nil {
			return runtimepkg.RetryableRunEffectAttempt(
				"DELIVERY_STATE_WRITE_FAILED", "cannot record missing delivery target", err,
			)
		}
		if rows == 0 {
			return resolveSupersededDefaultDeliveryAttempt(ctx, q, effect)
		}
		return runtimepkg.PermanentRunEffectFailure(
			"DELIVERY_TARGET_UNAVAILABLE", "default delivery target is unavailable", nil,
		)
	}

	statusCode, responseBody, attemptErr := s.doDeliver(
		ctx,
		row.TargetType,
		row.TargetURL,
		*row.TargetSecret,
		effect.ID,
		row.Payload,
	)
	statusPtr, bodyPtr, errorMessage := deliveryEffectHTTPOutcome(statusCode, responseBody, attemptErr)
	if attemptErr == nil && statusCode >= http.StatusOK && statusCode < http.StatusMultipleChoices {
		rows, err := q.MarkRunEffectDeliverySuccess(ctx, db.MarkRunEffectDeliverySuccessParams{
			ID:             effect.ID,
			ResponseStatus: statusPtr,
			ResponseBody:   bodyPtr,
			AttemptCount:   effect.AttemptCount,
			LeaseOwner:     leaseOwner,
		})
		if err != nil {
			return runtimepkg.RetryableRunEffectAttempt(
				"DELIVERY_STATE_WRITE_FAILED", "cannot record default delivery success", err,
			)
		}
		if rows == 0 {
			return resolveSupersededDefaultDeliveryAttempt(ctx, q, effect)
		}
		return runtimepkg.RunEffectAttemptSucceeded()
	}

	finalAttempt := effect.AttemptCount >= effect.MaxAttempts
	var (
		rows    int64
		markErr error
	)
	if finalAttempt {
		rows, markErr = q.MarkRunEffectDeliveryFailed(ctx, db.MarkRunEffectDeliveryFailedParams{
			ID:             effect.ID,
			ResponseStatus: statusPtr,
			ResponseBody:   bodyPtr,
			ErrorMessage:   errorMessage,
			AttemptCount:   effect.AttemptCount,
			LeaseOwner:     leaseOwner,
		})
	} else {
		rows, markErr = q.MarkRunEffectDeliveryRetry(ctx, db.MarkRunEffectDeliveryRetryParams{
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
			"DELIVERY_STATE_WRITE_FAILED", "cannot record default delivery attempt", markErr,
		)
	}
	if rows == 0 {
		return resolveSupersededDefaultDeliveryAttempt(ctx, q, effect)
	}
	return retryableDeliveryHTTPResult(statusCode, attemptErr)
}

func (s *Service) ResetDefaultDeliveryEffect(ctx context.Context, effect db.RunEffectOutbox) error {
	if effect.EffectType != runtimepkg.RunEffectTypeDefaultDelivery {
		return fmt.Errorf("unsupported delivery effect type %q", effect.EffectType)
	}
	q, err := s.runEffectQueries()
	if err != nil {
		return err
	}
	rows, err := q.ResetRunEffectDelivery(ctx, effect.ID)
	if err != nil || rows > 0 {
		return err
	}
	row, err := q.GetRunDeliveryByID(ctx, effect.ID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read default delivery after reset: %w", err)
	}
	if row.ID != effect.ID || row.RunID != effect.RunID ||
		row.EffectOutboxID == nil || *row.EffectOutboxID != effect.ID {
		return errors.New("default delivery identity conflicts during replay reset")
	}
	switch row.Status {
	case "pending", "success":
		return nil
	case "failed":
		return errors.New("default delivery remains failed after replay reset")
	default:
		return fmt.Errorf("default delivery has invalid replay state %q", row.Status)
	}
}

func (s *Service) runEffectQueries() (deliveryEffectQueries, error) {
	if s == nil {
		return nil, errors.New("delivery service is nil")
	}
	if s.effectQueries != nil {
		return s.effectQueries, nil
	}
	if q, ok := s.queries.(deliveryEffectQueries); ok {
		return q, nil
	}
	return nil, errors.New("delivery effect queries are not configured")
}

type permanentDeliveryEffectError struct {
	code        string
	safeMessage string
	cause       error
}

func (e permanentDeliveryEffectError) Error() string {
	if e.cause != nil {
		return e.cause.Error()
	}
	return e.safeMessage
}

func classifyDeliveryEffectMaterializationError(err error) runtimepkg.RunEffectAttemptResult {
	var permanent permanentDeliveryEffectError
	if errors.As(err, &permanent) {
		return runtimepkg.PermanentRunEffectFailure(
			permanent.code, permanent.safeMessage, permanent.cause,
		)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return runtimepkg.PermanentRunEffectFailure(
			"EFFECT_TARGET_UNAVAILABLE", "default delivery target or identity is unavailable", err,
		)
	}
	return runtimepkg.RetryableRunEffectAttempt(
		"EFFECT_MATERIALIZATION_FAILED", "default delivery cannot be materialized", err,
	)
}

func deliveryEffectTargetUUID(effect db.RunEffectOutbox) (uuid.UUID, error) {
	const prefix = "delivery_target:"
	var metadata map[string]any
	if len(effect.Metadata) > 0 {
		_ = json.Unmarshal(effect.Metadata, &metadata)
	}
	metadataValue, _ := metadata["delivery_target_id"].(string)
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
		return uuid.Nil, errors.New("target key and metadata disagree")
	}
	return id, nil
}

func deliveryEffectHTTPOutcome(
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
	message := "network delivery failed"
	if attemptErr == nil {
		message = fmt.Sprintf("HTTP %d", statusCode)
	}
	return statusPtr, bodyPtr, &message
}

func retryableDeliveryHTTPResult(
	statusCode int,
	attemptErr error,
) runtimepkg.RunEffectAttemptResult {
	code := "NETWORK_ERROR"
	if statusCode > 0 {
		code = fmt.Sprintf("HTTP_%d", statusCode)
	}
	return runtimepkg.RetryableRunEffectAttempt(
		code, "default delivery failed", attemptErr,
	)
}

func resolveSupersededDefaultDeliveryAttempt(
	ctx context.Context,
	q deliveryEffectQueries,
	effect db.RunEffectOutbox,
) runtimepkg.RunEffectAttemptResult {
	row, err := q.GetRunDeliveryByID(ctx, effect.ID)
	if err != nil {
		return runtimepkg.RetryableRunEffectAttempt(
			"DELIVERY_STATE_READ_FAILED", "cannot resolve default delivery attempt state", err,
		)
	}
	if row.Status == "success" {
		return runtimepkg.RunEffectAttemptSucceeded()
	}
	if row.Status == "failed" {
		return runtimepkg.PermanentRunEffectFailure(
			"DELIVERY_ALREADY_FAILED", "default delivery is already failed", nil,
		)
	}
	return runtimepkg.RetryableRunEffectAttempt(
		"DELIVERY_ATTEMPT_SUPERSEDED", "default delivery attempt was superseded", nil,
	)
}

func claimedDeliveryEffectLeaseOwner(effect db.RunEffectOutbox) (uuid.UUID, error) {
	if effect.Status != "processing" || effect.LeaseOwner == nil || *effect.LeaseOwner == uuid.Nil ||
		effect.AttemptCount <= 0 {
		return uuid.Nil, errors.New("effect is not held by a processing lease")
	}
	return *effect.LeaseOwner, nil
}
