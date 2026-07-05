// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/task_callbacks.sql）。

package db

import (
	"context"
	"time"

	"github.com/google/uuid"
)

func scanTaskCallbackSubscription(row interface {
	Scan(dest ...any) error
}, s *TaskCallbackSubscription) error {
	return row.Scan(
		&s.ID,
		&s.RunID,
		&s.OwnerUserID,
		&s.CallerAgentID,
		&s.TargetURL,
		&s.Secret,
		&s.EventTypes,
		&s.AuthScheme,
		&s.AuthCredentials,
		&s.Metadata,
		&s.Status,
		&s.ConsecutiveFailures,
		&s.CreatedAt,
		&s.UpdatedAt,
		&s.DeletedAt,
	)
}

const createTaskCallbackSubscription = `-- name: CreateTaskCallbackSubscription :one
INSERT INTO task_callback_subscriptions (
    run_id, owner_user_id, caller_agent_id, target_url, secret, event_types,
    auth_scheme, auth_credentials, metadata
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, COALESCE($9, '{}'::jsonb)
)
RETURNING id, run_id, owner_user_id, caller_agent_id, target_url, secret,
          event_types, auth_scheme, auth_credentials, metadata,
          status, consecutive_failures, created_at, updated_at, deleted_at`

type CreateTaskCallbackSubscriptionParams struct {
	RunID           uuid.UUID  `db:"run_id" json:"run_id"`
	OwnerUserID     uuid.UUID  `db:"owner_user_id" json:"owner_user_id"`
	CallerAgentID   *uuid.UUID `db:"caller_agent_id" json:"caller_agent_id"`
	TargetURL       string     `db:"target_url" json:"target_url"`
	Secret          string     `db:"secret" json:"secret"`
	EventTypes      []string   `db:"event_types" json:"event_types"`
	AuthScheme      *string    `db:"auth_scheme" json:"auth_scheme"`
	AuthCredentials *string    `db:"auth_credentials" json:"auth_credentials"`
	Metadata        []byte     `db:"metadata" json:"metadata"`
}

func (q *Queries) CreateTaskCallbackSubscription(ctx context.Context, arg CreateTaskCallbackSubscriptionParams) (TaskCallbackSubscription, error) {
	row := q.db.QueryRow(ctx, createTaskCallbackSubscription,
		arg.RunID,
		arg.OwnerUserID,
		arg.CallerAgentID,
		arg.TargetURL,
		arg.Secret,
		arg.EventTypes,
		arg.AuthScheme,
		arg.AuthCredentials,
		arg.Metadata,
	)
	var s TaskCallbackSubscription
	err := scanTaskCallbackSubscription(row, &s)
	return s, err
}

const listTaskCallbackSubscriptionsByRun = `-- name: ListTaskCallbackSubscriptionsByRun :many
SELECT id, run_id, owner_user_id, caller_agent_id, target_url, secret,
       event_types, auth_scheme, auth_credentials, metadata,
       status, consecutive_failures, created_at, updated_at, deleted_at
FROM task_callback_subscriptions
WHERE run_id = $1
  AND owner_user_id = $2
  AND status <> 'deleted'
ORDER BY created_at DESC`

type ListTaskCallbackSubscriptionsByRunParams struct {
	RunID       uuid.UUID `db:"run_id" json:"run_id"`
	OwnerUserID uuid.UUID `db:"owner_user_id" json:"owner_user_id"`
}

func (q *Queries) ListTaskCallbackSubscriptionsByRun(ctx context.Context, arg ListTaskCallbackSubscriptionsByRunParams) ([]TaskCallbackSubscription, error) {
	rows, err := q.db.Query(ctx, listTaskCallbackSubscriptionsByRun, arg.RunID, arg.OwnerUserID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []TaskCallbackSubscription
	for rows.Next() {
		var s TaskCallbackSubscription
		if err := scanTaskCallbackSubscription(rows, &s); err != nil {
			return nil, err
		}
		items = append(items, s)
	}
	return items, rows.Err()
}

const listTaskCallbackSubscriptionsByOwner = `-- name: ListTaskCallbackSubscriptionsByOwner :many
SELECT id, run_id, owner_user_id, caller_agent_id, target_url, secret,
       event_types, auth_scheme, auth_credentials, metadata,
       status, consecutive_failures, created_at, updated_at, deleted_at
FROM task_callback_subscriptions
WHERE owner_user_id = $1
  AND status <> 'deleted'
  AND ($2::text = '' OR status = $2)
ORDER BY updated_at DESC, created_at DESC
LIMIT $3`

type ListTaskCallbackSubscriptionsByOwnerParams struct {
	OwnerUserID uuid.UUID `db:"owner_user_id" json:"owner_user_id"`
	Status      string    `db:"status" json:"status"`
	Limit       int32     `db:"limit" json:"limit"`
}

func (q *Queries) ListTaskCallbackSubscriptionsByOwner(ctx context.Context, arg ListTaskCallbackSubscriptionsByOwnerParams) ([]TaskCallbackSubscription, error) {
	rows, err := q.db.Query(ctx, listTaskCallbackSubscriptionsByOwner, arg.OwnerUserID, arg.Status, arg.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []TaskCallbackSubscription
	for rows.Next() {
		var s TaskCallbackSubscription
		if err := scanTaskCallbackSubscription(rows, &s); err != nil {
			return nil, err
		}
		items = append(items, s)
	}
	return items, rows.Err()
}

const deleteTaskCallbackSubscriptionForOwner = `-- name: DeleteTaskCallbackSubscriptionForOwner :execrows
UPDATE task_callback_subscriptions
SET status = 'deleted',
    deleted_at = NOW()
WHERE id = $1
  AND run_id = $2
  AND owner_user_id = $3
  AND status <> 'deleted'`

type DeleteTaskCallbackSubscriptionForOwnerParams struct {
	ID          uuid.UUID `db:"id" json:"id"`
	RunID       uuid.UUID `db:"run_id" json:"run_id"`
	OwnerUserID uuid.UUID `db:"owner_user_id" json:"owner_user_id"`
}

func (q *Queries) DeleteTaskCallbackSubscriptionForOwner(ctx context.Context, arg DeleteTaskCallbackSubscriptionForOwnerParams) (int64, error) {
	tag, err := q.db.Exec(ctx, deleteTaskCallbackSubscriptionForOwner, arg.ID, arg.RunID, arg.OwnerUserID)
	return tag.RowsAffected(), err
}

const updateTaskCallbackSubscriptionStatusForOwner = `-- name: UpdateTaskCallbackSubscriptionStatusForOwner :one
UPDATE task_callback_subscriptions
SET status = $4,
    consecutive_failures = CASE WHEN $4 = 'active' THEN 0 ELSE consecutive_failures END,
    updated_at = NOW()
WHERE id = $1
  AND run_id = $2
  AND owner_user_id = $3
  AND status <> 'deleted'
RETURNING id, run_id, owner_user_id, caller_agent_id, target_url, secret,
          event_types, auth_scheme, auth_credentials, metadata,
          status, consecutive_failures, created_at, updated_at, deleted_at`

type UpdateTaskCallbackSubscriptionStatusForOwnerParams struct {
	ID          uuid.UUID `db:"id" json:"id"`
	RunID       uuid.UUID `db:"run_id" json:"run_id"`
	OwnerUserID uuid.UUID `db:"owner_user_id" json:"owner_user_id"`
	Status      string    `db:"status" json:"status"`
}

func (q *Queries) UpdateTaskCallbackSubscriptionStatusForOwner(ctx context.Context, arg UpdateTaskCallbackSubscriptionStatusForOwnerParams) (TaskCallbackSubscription, error) {
	row := q.db.QueryRow(ctx, updateTaskCallbackSubscriptionStatusForOwner, arg.ID, arg.RunID, arg.OwnerUserID, arg.Status)
	var s TaskCallbackSubscription
	err := scanTaskCallbackSubscription(row, &s)
	return s, err
}

const batchUpdateTaskCallbackSubscriptionsForOwner = `-- name: BatchUpdateTaskCallbackSubscriptionsForOwner :many
UPDATE task_callback_subscriptions
SET status = $3,
    consecutive_failures = CASE WHEN $3 = 'active' THEN 0 ELSE consecutive_failures END,
    deleted_at = CASE WHEN $3 = 'deleted' THEN NOW() ELSE deleted_at END,
    updated_at = NOW()
WHERE owner_user_id = $1
  AND id = ANY($2::uuid[])
  AND status <> 'deleted'
RETURNING id, run_id, owner_user_id, caller_agent_id, target_url, secret,
          event_types, auth_scheme, auth_credentials, metadata,
          status, consecutive_failures, created_at, updated_at, deleted_at`

type BatchUpdateTaskCallbackSubscriptionsForOwnerParams struct {
	OwnerUserID uuid.UUID   `db:"owner_user_id" json:"owner_user_id"`
	IDs         []uuid.UUID `db:"ids" json:"ids"`
	Status      string      `db:"status" json:"status"`
}

func (q *Queries) BatchUpdateTaskCallbackSubscriptionsForOwner(ctx context.Context, arg BatchUpdateTaskCallbackSubscriptionsForOwnerParams) ([]TaskCallbackSubscription, error) {
	rows, err := q.db.Query(ctx, batchUpdateTaskCallbackSubscriptionsForOwner, arg.OwnerUserID, arg.IDs, arg.Status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []TaskCallbackSubscription
	for rows.Next() {
		var s TaskCallbackSubscription
		if err := scanTaskCallbackSubscription(rows, &s); err != nil {
			return nil, err
		}
		items = append(items, s)
	}
	return items, rows.Err()
}

const listActiveTaskCallbackSubscriptionsForEvent = `-- name: ListActiveTaskCallbackSubscriptionsForEvent :many
SELECT id, run_id, owner_user_id, caller_agent_id, target_url, secret,
       event_types, auth_scheme, auth_credentials, metadata,
       status, consecutive_failures, created_at, updated_at, deleted_at
FROM task_callback_subscriptions
WHERE run_id = $1
  AND status = 'active'
  AND $2 = ANY(event_types)
ORDER BY created_at ASC`

type ListActiveTaskCallbackSubscriptionsForEventParams struct {
	RunID     uuid.UUID `db:"run_id" json:"run_id"`
	EventType string    `db:"event_type" json:"event_type"`
}

func (q *Queries) ListActiveTaskCallbackSubscriptionsForEvent(ctx context.Context, arg ListActiveTaskCallbackSubscriptionsForEventParams) ([]TaskCallbackSubscription, error) {
	rows, err := q.db.Query(ctx, listActiveTaskCallbackSubscriptionsForEvent, arg.RunID, arg.EventType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []TaskCallbackSubscription
	for rows.Next() {
		var s TaskCallbackSubscription
		if err := scanTaskCallbackSubscription(rows, &s); err != nil {
			return nil, err
		}
		items = append(items, s)
	}
	return items, rows.Err()
}

func scanTaskCallbackDelivery(row interface {
	Scan(dest ...any) error
}, d *TaskCallbackDelivery) error {
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

const createTaskCallbackDelivery = `-- name: CreateTaskCallbackDelivery :one
INSERT INTO task_callback_deliveries (
    subscription_id, run_event_id, payload, status, attempt_count, next_retry_at
) VALUES (
    $1, $2, $3, 'pending', 0, NOW()
)
ON CONFLICT (subscription_id, run_event_id) DO UPDATE
SET payload = EXCLUDED.payload
RETURNING id, subscription_id, run_event_id, payload, status,
          response_status, response_body, error_message,
          attempt_count, next_retry_at, delivered_at, created_at, updated_at`

type CreateTaskCallbackDeliveryParams struct {
	SubscriptionID uuid.UUID `db:"subscription_id" json:"subscription_id"`
	RunEventID     uuid.UUID `db:"run_event_id" json:"run_event_id"`
	Payload        []byte    `db:"payload" json:"payload"`
}

func (q *Queries) CreateTaskCallbackDelivery(ctx context.Context, arg CreateTaskCallbackDeliveryParams) (TaskCallbackDelivery, error) {
	row := q.db.QueryRow(ctx, createTaskCallbackDelivery, arg.SubscriptionID, arg.RunEventID, arg.Payload)
	var d TaskCallbackDelivery
	err := scanTaskCallbackDelivery(row, &d)
	return d, err
}

const getTaskCallbackDeliveryByID = `-- name: GetTaskCallbackDeliveryByID :one
SELECT d.id, d.subscription_id, d.run_event_id, d.payload, d.status,
       d.response_status, d.response_body, d.error_message,
       d.attempt_count, d.next_retry_at, d.delivered_at, d.created_at, d.updated_at,
       s.target_url, s.secret, s.auth_scheme, s.auth_credentials, e.event_type
FROM task_callback_deliveries d
JOIN task_callback_subscriptions s ON s.id = d.subscription_id
JOIN run_events e ON e.id = d.run_event_id
WHERE d.id = $1`

type GetTaskCallbackDeliveryByIDRow struct {
	TaskCallbackDelivery
	TargetURL       string  `db:"target_url" json:"target_url"`
	Secret          string  `db:"secret" json:"-"`
	AuthScheme      *string `db:"auth_scheme" json:"auth_scheme,omitempty"`
	AuthCredentials *string `db:"auth_credentials" json:"-"`
	EventType       string  `db:"event_type" json:"event_type"`
}

func (q *Queries) GetTaskCallbackDeliveryByID(ctx context.Context, id uuid.UUID) (GetTaskCallbackDeliveryByIDRow, error) {
	row := q.db.QueryRow(ctx, getTaskCallbackDeliveryByID, id)
	var r GetTaskCallbackDeliveryByIDRow
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
		&r.AuthScheme,
		&r.AuthCredentials,
		&r.EventType,
	)
	return r, err
}

const listTaskCallbackDeliveriesByRun = `-- name: ListTaskCallbackDeliveriesByRun :many
SELECT d.id, d.subscription_id, d.run_event_id, d.payload, d.status,
       d.response_status, d.response_body, d.error_message,
       d.attempt_count, d.next_retry_at, d.delivered_at, d.created_at, d.updated_at,
       s.target_url, e.event_type
FROM task_callback_deliveries d
JOIN task_callback_subscriptions s ON s.id = d.subscription_id
JOIN run_events e ON e.id = d.run_event_id
WHERE s.run_id = $1
  AND s.owner_user_id = $2
ORDER BY d.created_at DESC
LIMIT $3`

type ListTaskCallbackDeliveriesByRunParams struct {
	RunID       uuid.UUID `db:"run_id" json:"run_id"`
	OwnerUserID uuid.UUID `db:"owner_user_id" json:"owner_user_id"`
	Limit       int32     `db:"limit" json:"limit"`
}

type ListTaskCallbackDeliveriesByRunRow struct {
	TaskCallbackDelivery
	TargetURL string `db:"target_url" json:"target_url"`
	EventType string `db:"event_type" json:"event_type"`
}

func (q *Queries) ListTaskCallbackDeliveriesByRun(ctx context.Context, arg ListTaskCallbackDeliveriesByRunParams) ([]ListTaskCallbackDeliveriesByRunRow, error) {
	rows, err := q.db.Query(ctx, listTaskCallbackDeliveriesByRun, arg.RunID, arg.OwnerUserID, arg.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []ListTaskCallbackDeliveriesByRunRow
	for rows.Next() {
		var r ListTaskCallbackDeliveriesByRunRow
		if err := rows.Scan(
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
			&r.EventType,
		); err != nil {
			return nil, err
		}
		items = append(items, r)
	}
	return items, rows.Err()
}

const markTaskCallbackDeliverySuccess = `-- name: MarkTaskCallbackDeliverySuccess :exec
UPDATE task_callback_deliveries
SET status = 'success',
    response_status = $2,
    response_body = $3,
    error_message = NULL,
    attempt_count = attempt_count + 1,
    next_retry_at = NULL,
    delivered_at = NOW(),
    updated_at = NOW()
WHERE id = $1`

type MarkTaskCallbackDeliverySuccessParams struct {
	ID             uuid.UUID `db:"id" json:"id"`
	ResponseStatus *int32    `db:"response_status" json:"response_status"`
	ResponseBody   *string   `db:"response_body" json:"response_body"`
}

func (q *Queries) MarkTaskCallbackDeliverySuccess(ctx context.Context, arg MarkTaskCallbackDeliverySuccessParams) error {
	_, err := q.db.Exec(ctx, markTaskCallbackDeliverySuccess, arg.ID, arg.ResponseStatus, arg.ResponseBody)
	return err
}

const markTaskCallbackDeliveryFailedRetry = `-- name: MarkTaskCallbackDeliveryFailedRetry :exec
UPDATE task_callback_deliveries
SET status = 'pending',
    response_status = $2,
    response_body = $3,
    error_message = $4,
    attempt_count = attempt_count + 1,
    next_retry_at = $5,
    updated_at = NOW()
WHERE id = $1`

type MarkTaskCallbackDeliveryFailedRetryParams struct {
	ID             uuid.UUID `db:"id" json:"id"`
	ResponseStatus *int32    `db:"response_status" json:"response_status"`
	ResponseBody   *string   `db:"response_body" json:"response_body"`
	ErrorMessage   *string   `db:"error_message" json:"error_message"`
	NextRetryAt    time.Time `db:"next_retry_at" json:"next_retry_at"`
}

func (q *Queries) MarkTaskCallbackDeliveryFailedRetry(ctx context.Context, arg MarkTaskCallbackDeliveryFailedRetryParams) error {
	_, err := q.db.Exec(ctx, markTaskCallbackDeliveryFailedRetry, arg.ID, arg.ResponseStatus, arg.ResponseBody, arg.ErrorMessage, arg.NextRetryAt)
	return err
}

const markTaskCallbackDeliveryFailedFinal = `-- name: MarkTaskCallbackDeliveryFailedFinal :exec
UPDATE task_callback_deliveries
SET status = 'failed',
    response_status = $2,
    response_body = $3,
    error_message = $4,
    attempt_count = attempt_count + 1,
    next_retry_at = NULL,
    updated_at = NOW()
WHERE id = $1`

type MarkTaskCallbackDeliveryFailedFinalParams struct {
	ID             uuid.UUID `db:"id" json:"id"`
	ResponseStatus *int32    `db:"response_status" json:"response_status"`
	ResponseBody   *string   `db:"response_body" json:"response_body"`
	ErrorMessage   *string   `db:"error_message" json:"error_message"`
}

func (q *Queries) MarkTaskCallbackDeliveryFailedFinal(ctx context.Context, arg MarkTaskCallbackDeliveryFailedFinalParams) error {
	_, err := q.db.Exec(ctx, markTaskCallbackDeliveryFailedFinal, arg.ID, arg.ResponseStatus, arg.ResponseBody, arg.ErrorMessage)
	return err
}

const incrementTaskCallbackSubscriptionFailure = `-- name: IncrementTaskCallbackSubscriptionFailure :exec
UPDATE task_callback_subscriptions
SET consecutive_failures = consecutive_failures + 1,
    status = CASE
        WHEN consecutive_failures + 1 >= 3 THEN 'failed'
        ELSE status
    END,
    updated_at = NOW()
WHERE id = $1`

func (q *Queries) IncrementTaskCallbackSubscriptionFailure(ctx context.Context, id uuid.UUID) error {
	_, err := q.db.Exec(ctx, incrementTaskCallbackSubscriptionFailure, id)
	return err
}

const resetTaskCallbackSubscriptionFailures = `-- name: ResetTaskCallbackSubscriptionFailures :exec
UPDATE task_callback_subscriptions
SET consecutive_failures = 0,
    updated_at = NOW()
WHERE id = $1`

func (q *Queries) ResetTaskCallbackSubscriptionFailures(ctx context.Context, id uuid.UUID) error {
	_, err := q.db.Exec(ctx, resetTaskCallbackSubscriptionFailures, id)
	return err
}

const listPendingTaskCallbackDeliveries = `-- name: ListPendingTaskCallbackDeliveries :many
SELECT id, subscription_id, run_event_id, payload, status,
       response_status, response_body, error_message,
       attempt_count, next_retry_at, delivered_at, created_at, updated_at
FROM task_callback_deliveries
WHERE status = 'pending'
  AND next_retry_at IS NOT NULL
  AND next_retry_at <= NOW()
ORDER BY next_retry_at ASC
LIMIT 50`

func (q *Queries) ListPendingTaskCallbackDeliveries(ctx context.Context) ([]TaskCallbackDelivery, error) {
	rows, err := q.db.Query(ctx, listPendingTaskCallbackDeliveries)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []TaskCallbackDelivery
	for rows.Next() {
		var d TaskCallbackDelivery
		if err := scanTaskCallbackDelivery(rows, &d); err != nil {
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
