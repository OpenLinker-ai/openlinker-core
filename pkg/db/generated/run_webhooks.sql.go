// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/run_webhooks.sql）。

package db

import (
	"context"
	"time"

	"github.com/google/uuid"
)

func scanRunWebhookSubscription(row interface {
	Scan(dest ...any) error
}, s *RunWebhookSubscription) error {
	return row.Scan(
		&s.ID,
		&s.RunID,
		&s.OwnerUserID,
		&s.CallerAgentID,
		&s.TargetURL,
		&s.Secret,
		&s.EventTypes,
		&s.PushAuthScheme,
		&s.PushAuthCredentials,
		&s.PushMetadata,
		&s.Status,
		&s.ConsecutiveFailures,
		&s.CreatedAt,
		&s.UpdatedAt,
		&s.DeletedAt,
	)
}

const createRunWebhookSubscription = `-- name: CreateRunWebhookSubscription :one
INSERT INTO run_webhook_subscriptions (
    run_id, owner_user_id, caller_agent_id, target_url, secret, event_types,
    push_auth_scheme, push_auth_credentials, push_metadata
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, COALESCE($9, '{}'::jsonb)
)
RETURNING id, run_id, owner_user_id, caller_agent_id, target_url, secret,
          event_types, push_auth_scheme, push_auth_credentials, push_metadata,
          status, consecutive_failures, created_at, updated_at, deleted_at`

type CreateRunWebhookSubscriptionParams struct {
	RunID               uuid.UUID  `db:"run_id" json:"run_id"`
	OwnerUserID         uuid.UUID  `db:"owner_user_id" json:"owner_user_id"`
	CallerAgentID       *uuid.UUID `db:"caller_agent_id" json:"caller_agent_id"`
	TargetURL           string     `db:"target_url" json:"target_url"`
	Secret              string     `db:"secret" json:"secret"`
	EventTypes          []string   `db:"event_types" json:"event_types"`
	PushAuthScheme      *string    `db:"push_auth_scheme" json:"push_auth_scheme"`
	PushAuthCredentials *string    `db:"push_auth_credentials" json:"push_auth_credentials"`
	PushMetadata        []byte     `db:"push_metadata" json:"push_metadata"`
}

func (q *Queries) CreateRunWebhookSubscription(ctx context.Context, arg CreateRunWebhookSubscriptionParams) (RunWebhookSubscription, error) {
	row := q.db.QueryRow(ctx, createRunWebhookSubscription,
		arg.RunID,
		arg.OwnerUserID,
		arg.CallerAgentID,
		arg.TargetURL,
		arg.Secret,
		arg.EventTypes,
		arg.PushAuthScheme,
		arg.PushAuthCredentials,
		arg.PushMetadata,
	)
	var s RunWebhookSubscription
	err := scanRunWebhookSubscription(row, &s)
	return s, err
}

const listRunWebhookSubscriptionsByRun = `-- name: ListRunWebhookSubscriptionsByRun :many
SELECT id, run_id, owner_user_id, caller_agent_id, target_url, secret,
       event_types, push_auth_scheme, push_auth_credentials, push_metadata,
       status, consecutive_failures, created_at, updated_at, deleted_at
FROM run_webhook_subscriptions
WHERE run_id = $1
  AND owner_user_id = $2
  AND status <> 'deleted'
ORDER BY created_at DESC`

type ListRunWebhookSubscriptionsByRunParams struct {
	RunID       uuid.UUID `db:"run_id" json:"run_id"`
	OwnerUserID uuid.UUID `db:"owner_user_id" json:"owner_user_id"`
}

func (q *Queries) ListRunWebhookSubscriptionsByRun(ctx context.Context, arg ListRunWebhookSubscriptionsByRunParams) ([]RunWebhookSubscription, error) {
	rows, err := q.db.Query(ctx, listRunWebhookSubscriptionsByRun, arg.RunID, arg.OwnerUserID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []RunWebhookSubscription
	for rows.Next() {
		var s RunWebhookSubscription
		if err := scanRunWebhookSubscription(rows, &s); err != nil {
			return nil, err
		}
		items = append(items, s)
	}
	return items, rows.Err()
}

const listRunWebhookSubscriptionsByOwner = `-- name: ListRunWebhookSubscriptionsByOwner :many
SELECT id, run_id, owner_user_id, caller_agent_id, target_url, secret,
       event_types, push_auth_scheme, push_auth_credentials, push_metadata,
       status, consecutive_failures, created_at, updated_at, deleted_at
FROM run_webhook_subscriptions
WHERE owner_user_id = $1
  AND status <> 'deleted'
  AND ($2::text = '' OR status = $2)
ORDER BY updated_at DESC, created_at DESC
LIMIT $3`

type ListRunWebhookSubscriptionsByOwnerParams struct {
	OwnerUserID uuid.UUID `db:"owner_user_id" json:"owner_user_id"`
	Status      string    `db:"status" json:"status"`
	Limit       int32     `db:"limit" json:"limit"`
}

func (q *Queries) ListRunWebhookSubscriptionsByOwner(ctx context.Context, arg ListRunWebhookSubscriptionsByOwnerParams) ([]RunWebhookSubscription, error) {
	rows, err := q.db.Query(ctx, listRunWebhookSubscriptionsByOwner, arg.OwnerUserID, arg.Status, arg.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []RunWebhookSubscription
	for rows.Next() {
		var s RunWebhookSubscription
		if err := scanRunWebhookSubscription(rows, &s); err != nil {
			return nil, err
		}
		items = append(items, s)
	}
	return items, rows.Err()
}

const deleteRunWebhookSubscriptionForOwner = `-- name: DeleteRunWebhookSubscriptionForOwner :execrows
UPDATE run_webhook_subscriptions
SET status = 'deleted',
    deleted_at = NOW()
WHERE id = $1
  AND run_id = $2
  AND owner_user_id = $3
  AND status <> 'deleted'`

type DeleteRunWebhookSubscriptionForOwnerParams struct {
	ID          uuid.UUID `db:"id" json:"id"`
	RunID       uuid.UUID `db:"run_id" json:"run_id"`
	OwnerUserID uuid.UUID `db:"owner_user_id" json:"owner_user_id"`
}

func (q *Queries) DeleteRunWebhookSubscriptionForOwner(ctx context.Context, arg DeleteRunWebhookSubscriptionForOwnerParams) (int64, error) {
	tag, err := q.db.Exec(ctx, deleteRunWebhookSubscriptionForOwner, arg.ID, arg.RunID, arg.OwnerUserID)
	return tag.RowsAffected(), err
}

const updateRunWebhookSubscriptionStatusForOwner = `-- name: UpdateRunWebhookSubscriptionStatusForOwner :one
UPDATE run_webhook_subscriptions
SET status = $4,
    consecutive_failures = CASE WHEN $4 = 'active' THEN 0 ELSE consecutive_failures END,
    updated_at = NOW()
WHERE id = $1
  AND run_id = $2
  AND owner_user_id = $3
  AND status <> 'deleted'
RETURNING id, run_id, owner_user_id, caller_agent_id, target_url, secret,
          event_types, push_auth_scheme, push_auth_credentials, push_metadata,
          status, consecutive_failures, created_at, updated_at, deleted_at`

type UpdateRunWebhookSubscriptionStatusForOwnerParams struct {
	ID          uuid.UUID `db:"id" json:"id"`
	RunID       uuid.UUID `db:"run_id" json:"run_id"`
	OwnerUserID uuid.UUID `db:"owner_user_id" json:"owner_user_id"`
	Status      string    `db:"status" json:"status"`
}

func (q *Queries) UpdateRunWebhookSubscriptionStatusForOwner(ctx context.Context, arg UpdateRunWebhookSubscriptionStatusForOwnerParams) (RunWebhookSubscription, error) {
	row := q.db.QueryRow(ctx, updateRunWebhookSubscriptionStatusForOwner, arg.ID, arg.RunID, arg.OwnerUserID, arg.Status)
	var s RunWebhookSubscription
	err := scanRunWebhookSubscription(row, &s)
	return s, err
}

const batchUpdateRunWebhookSubscriptionsForOwner = `-- name: BatchUpdateRunWebhookSubscriptionsForOwner :many
UPDATE run_webhook_subscriptions
SET status = $3,
    consecutive_failures = CASE WHEN $3 = 'active' THEN 0 ELSE consecutive_failures END,
    deleted_at = CASE WHEN $3 = 'deleted' THEN NOW() ELSE deleted_at END,
    updated_at = NOW()
WHERE owner_user_id = $1
  AND id = ANY($2::uuid[])
  AND status <> 'deleted'
RETURNING id, run_id, owner_user_id, caller_agent_id, target_url, secret,
          event_types, push_auth_scheme, push_auth_credentials, push_metadata,
          status, consecutive_failures, created_at, updated_at, deleted_at`

type BatchUpdateRunWebhookSubscriptionsForOwnerParams struct {
	OwnerUserID uuid.UUID   `db:"owner_user_id" json:"owner_user_id"`
	IDs         []uuid.UUID `db:"ids" json:"ids"`
	Status      string      `db:"status" json:"status"`
}

func (q *Queries) BatchUpdateRunWebhookSubscriptionsForOwner(ctx context.Context, arg BatchUpdateRunWebhookSubscriptionsForOwnerParams) ([]RunWebhookSubscription, error) {
	rows, err := q.db.Query(ctx, batchUpdateRunWebhookSubscriptionsForOwner, arg.OwnerUserID, arg.IDs, arg.Status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []RunWebhookSubscription
	for rows.Next() {
		var s RunWebhookSubscription
		if err := scanRunWebhookSubscription(rows, &s); err != nil {
			return nil, err
		}
		items = append(items, s)
	}
	return items, rows.Err()
}

const listActiveRunWebhookSubscriptionsForEvent = `-- name: ListActiveRunWebhookSubscriptionsForEvent :many
SELECT id, run_id, owner_user_id, caller_agent_id, target_url, secret,
       event_types, push_auth_scheme, push_auth_credentials, push_metadata,
       status, consecutive_failures, created_at, updated_at, deleted_at
FROM run_webhook_subscriptions
WHERE run_id = $1
  AND status = 'active'
  AND $2 = ANY(event_types)
ORDER BY created_at ASC`

type ListActiveRunWebhookSubscriptionsForEventParams struct {
	RunID     uuid.UUID `db:"run_id" json:"run_id"`
	EventType string    `db:"event_type" json:"event_type"`
}

func (q *Queries) ListActiveRunWebhookSubscriptionsForEvent(ctx context.Context, arg ListActiveRunWebhookSubscriptionsForEventParams) ([]RunWebhookSubscription, error) {
	rows, err := q.db.Query(ctx, listActiveRunWebhookSubscriptionsForEvent, arg.RunID, arg.EventType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []RunWebhookSubscription
	for rows.Next() {
		var s RunWebhookSubscription
		if err := scanRunWebhookSubscription(rows, &s); err != nil {
			return nil, err
		}
		items = append(items, s)
	}
	return items, rows.Err()
}

func scanRunWebhookDelivery(row interface {
	Scan(dest ...any) error
}, d *RunWebhookDelivery) error {
	return row.Scan(
		&d.ID,
		&d.SubscriptionID,
		&d.RunEventID,
		&d.Payload,
		&d.Status,
		&d.ResponseStatus,
		&d.ResponseBody,
		&d.ErrorMessage,
		&d.AttemptCount,
		&d.NextRetryAt,
		&d.DeliveredAt,
		&d.CreatedAt,
		&d.UpdatedAt,
	)
}

const createRunWebhookDelivery = `-- name: CreateRunWebhookDelivery :one
INSERT INTO run_webhook_deliveries (
    subscription_id, run_event_id, payload, status, attempt_count, next_retry_at
) VALUES (
    $1, $2, $3, 'pending', 0, NOW()
)
ON CONFLICT (subscription_id, run_event_id) DO UPDATE
SET payload = EXCLUDED.payload
RETURNING id, subscription_id, run_event_id, payload, status,
          response_status, response_body, error_message,
          attempt_count, next_retry_at, delivered_at, created_at, updated_at`

type CreateRunWebhookDeliveryParams struct {
	SubscriptionID uuid.UUID `db:"subscription_id" json:"subscription_id"`
	RunEventID     uuid.UUID `db:"run_event_id" json:"run_event_id"`
	Payload        []byte    `db:"payload" json:"payload"`
}

func (q *Queries) CreateRunWebhookDelivery(ctx context.Context, arg CreateRunWebhookDeliveryParams) (RunWebhookDelivery, error) {
	row := q.db.QueryRow(ctx, createRunWebhookDelivery, arg.SubscriptionID, arg.RunEventID, arg.Payload)
	var d RunWebhookDelivery
	err := scanRunWebhookDelivery(row, &d)
	return d, err
}

const getRunWebhookDeliveryByID = `-- name: GetRunWebhookDeliveryByID :one
SELECT d.id, d.subscription_id, d.run_event_id, d.payload, d.status,
       d.response_status, d.response_body, d.error_message,
       d.attempt_count, d.next_retry_at, d.delivered_at, d.created_at, d.updated_at,
       s.target_url, s.secret, s.push_auth_scheme, s.push_auth_credentials, e.event_type
FROM run_webhook_deliveries d
JOIN run_webhook_subscriptions s ON s.id = d.subscription_id
JOIN run_events e ON e.id = d.run_event_id
WHERE d.id = $1`

type GetRunWebhookDeliveryByIDRow struct {
	RunWebhookDelivery
	TargetURL           string  `db:"target_url" json:"target_url"`
	Secret              string  `db:"secret" json:"-"`
	PushAuthScheme      *string `db:"push_auth_scheme" json:"push_auth_scheme,omitempty"`
	PushAuthCredentials *string `db:"push_auth_credentials" json:"-"`
	EventType           string  `db:"event_type" json:"event_type"`
}

func (q *Queries) GetRunWebhookDeliveryByID(ctx context.Context, id uuid.UUID) (GetRunWebhookDeliveryByIDRow, error) {
	row := q.db.QueryRow(ctx, getRunWebhookDeliveryByID, id)
	var r GetRunWebhookDeliveryByIDRow
	err := row.Scan(
		&r.ID,
		&r.SubscriptionID,
		&r.RunEventID,
		&r.Payload,
		&r.Status,
		&r.ResponseStatus,
		&r.ResponseBody,
		&r.ErrorMessage,
		&r.AttemptCount,
		&r.NextRetryAt,
		&r.DeliveredAt,
		&r.CreatedAt,
		&r.UpdatedAt,
		&r.TargetURL,
		&r.Secret,
		&r.PushAuthScheme,
		&r.PushAuthCredentials,
		&r.EventType,
	)
	return r, err
}

const markRunWebhookDeliverySuccess = `-- name: MarkRunWebhookDeliverySuccess :exec
UPDATE run_webhook_deliveries
SET status = 'success',
    response_status = $2,
    response_body = $3,
    error_message = NULL,
    attempt_count = attempt_count + 1,
    next_retry_at = NULL,
    delivered_at = NOW(),
    updated_at = NOW()
WHERE id = $1`

type MarkRunWebhookDeliverySuccessParams struct {
	ID             uuid.UUID `db:"id" json:"id"`
	ResponseStatus *int32    `db:"response_status" json:"response_status"`
	ResponseBody   *string   `db:"response_body" json:"response_body"`
}

func (q *Queries) MarkRunWebhookDeliverySuccess(ctx context.Context, arg MarkRunWebhookDeliverySuccessParams) error {
	_, err := q.db.Exec(ctx, markRunWebhookDeliverySuccess, arg.ID, arg.ResponseStatus, arg.ResponseBody)
	return err
}

const markRunWebhookDeliveryFailedRetry = `-- name: MarkRunWebhookDeliveryFailedRetry :exec
UPDATE run_webhook_deliveries
SET status = 'pending',
    response_status = $2,
    response_body = $3,
    error_message = $4,
    attempt_count = attempt_count + 1,
    next_retry_at = $5,
    updated_at = NOW()
WHERE id = $1`

type MarkRunWebhookDeliveryFailedRetryParams struct {
	ID             uuid.UUID `db:"id" json:"id"`
	ResponseStatus *int32    `db:"response_status" json:"response_status"`
	ResponseBody   *string   `db:"response_body" json:"response_body"`
	ErrorMessage   *string   `db:"error_message" json:"error_message"`
	NextRetryAt    time.Time `db:"next_retry_at" json:"next_retry_at"`
}

func (q *Queries) MarkRunWebhookDeliveryFailedRetry(ctx context.Context, arg MarkRunWebhookDeliveryFailedRetryParams) error {
	_, err := q.db.Exec(ctx, markRunWebhookDeliveryFailedRetry, arg.ID, arg.ResponseStatus, arg.ResponseBody, arg.ErrorMessage, arg.NextRetryAt)
	return err
}

const markRunWebhookDeliveryFailedFinal = `-- name: MarkRunWebhookDeliveryFailedFinal :exec
UPDATE run_webhook_deliveries
SET status = 'failed',
    response_status = $2,
    response_body = $3,
    error_message = $4,
    attempt_count = attempt_count + 1,
    next_retry_at = NULL,
    updated_at = NOW()
WHERE id = $1`

type MarkRunWebhookDeliveryFailedFinalParams struct {
	ID             uuid.UUID `db:"id" json:"id"`
	ResponseStatus *int32    `db:"response_status" json:"response_status"`
	ResponseBody   *string   `db:"response_body" json:"response_body"`
	ErrorMessage   *string   `db:"error_message" json:"error_message"`
}

func (q *Queries) MarkRunWebhookDeliveryFailedFinal(ctx context.Context, arg MarkRunWebhookDeliveryFailedFinalParams) error {
	_, err := q.db.Exec(ctx, markRunWebhookDeliveryFailedFinal, arg.ID, arg.ResponseStatus, arg.ResponseBody, arg.ErrorMessage)
	return err
}

const incrementRunWebhookSubscriptionFailure = `-- name: IncrementRunWebhookSubscriptionFailure :exec
UPDATE run_webhook_subscriptions
SET consecutive_failures = consecutive_failures + 1,
    status = CASE
        WHEN consecutive_failures + 1 >= 3 THEN 'failed'
        ELSE status
    END,
    updated_at = NOW()
WHERE id = $1`

func (q *Queries) IncrementRunWebhookSubscriptionFailure(ctx context.Context, id uuid.UUID) error {
	_, err := q.db.Exec(ctx, incrementRunWebhookSubscriptionFailure, id)
	return err
}

const resetRunWebhookSubscriptionFailures = `-- name: ResetRunWebhookSubscriptionFailures :exec
UPDATE run_webhook_subscriptions
SET consecutive_failures = 0,
    updated_at = NOW()
WHERE id = $1`

func (q *Queries) ResetRunWebhookSubscriptionFailures(ctx context.Context, id uuid.UUID) error {
	_, err := q.db.Exec(ctx, resetRunWebhookSubscriptionFailures, id)
	return err
}

const listPendingRunWebhookDeliveries = `-- name: ListPendingRunWebhookDeliveries :many
SELECT id, subscription_id, run_event_id, payload, status,
       response_status, response_body, error_message,
       attempt_count, next_retry_at, delivered_at, created_at, updated_at
FROM run_webhook_deliveries
WHERE status = 'pending'
  AND next_retry_at IS NOT NULL
  AND next_retry_at <= NOW()
ORDER BY next_retry_at ASC
LIMIT 50`

func (q *Queries) ListPendingRunWebhookDeliveries(ctx context.Context) ([]RunWebhookDelivery, error) {
	rows, err := q.db.Query(ctx, listPendingRunWebhookDeliveries)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []RunWebhookDelivery
	for rows.Next() {
		var d RunWebhookDelivery
		if err := scanRunWebhookDelivery(rows, &d); err != nil {
			return nil, err
		}
		items = append(items, d)
	}
	return items, rows.Err()
}

const getLatestRunEventForTypes = `-- name: GetLatestRunEventForTypes :one
SELECT id, run_id, parent_run_id, sequence, event_type, payload, created_at
FROM run_events
WHERE run_id = $1
  AND event_type = ANY($2::text[])
ORDER BY sequence DESC
LIMIT 1`

type GetLatestRunEventForTypesParams struct {
	RunID      uuid.UUID `db:"run_id" json:"run_id"`
	EventTypes []string  `db:"event_types" json:"event_types"`
}

func (q *Queries) GetLatestRunEventForTypes(ctx context.Context, arg GetLatestRunEventForTypesParams) (RunEvent, error) {
	row := q.db.QueryRow(ctx, getLatestRunEventForTypes, arg.RunID, arg.EventTypes)
	var e RunEvent
	err := scanRunEvent(row, &e)
	return e, err
}
