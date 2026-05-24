// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/delivery.sql）。
//
// Phase 2 §7：用户侧 Output Delivery。
// 风格参考 webhooks.sql.go：const + 方法，逐字段 Scan。

package db

import (
	"context"
	"time"

	"github.com/google/uuid"
)

func scanDeliveryTarget(row interface {
	Scan(dest ...any) error
}, t *DeliveryTarget) error {
	return row.Scan(
		&t.ID,
		&t.UserID,
		&t.Name,
		&t.Type,
		&t.Config,
		&t.Secret,
		&t.IsDefault,
		&t.CreatedAt,
		&t.UpdatedAt,
	)
}

func scanRunDelivery(row interface {
	Scan(dest ...any) error
}, d *RunDelivery) error {
	return row.Scan(
		&d.ID,
		&d.RunID,
		&d.TargetID,
		&d.UserID,
		&d.TargetType,
		&d.TargetURL,
		&d.Payload,
		&d.Status,
		&d.ResponseStatus,
		&d.ResponseBody,
		&d.ErrorMessage,
		&d.AttemptCount,
		&d.NextRetryAt,
		&d.CreatedAt,
		&d.UpdatedAt,
	)
}

const createDeliveryTarget = `-- name: CreateDeliveryTarget :one
INSERT INTO delivery_targets (user_id, name, type, config, secret, is_default)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING id, user_id, name, type, config, secret, is_default, created_at, updated_at`

type CreateDeliveryTargetParams struct {
	UserID    uuid.UUID `db:"user_id" json:"user_id"`
	Name      string    `db:"name" json:"name"`
	Type      string    `db:"type" json:"type"`
	Config    []byte    `db:"config" json:"config"`
	Secret    string    `db:"secret" json:"secret"`
	IsDefault bool      `db:"is_default" json:"is_default"`
}

func (q *Queries) CreateDeliveryTarget(ctx context.Context, arg CreateDeliveryTargetParams) (DeliveryTarget, error) {
	row := q.db.QueryRow(ctx, createDeliveryTarget,
		arg.UserID, arg.Name, arg.Type, arg.Config, arg.Secret, arg.IsDefault,
	)
	var t DeliveryTarget
	err := scanDeliveryTarget(row, &t)
	return t, err
}

const listDeliveryTargetsByUser = `-- name: ListDeliveryTargetsByUser :many
SELECT id, user_id, name, type, config, secret, is_default, created_at, updated_at
FROM delivery_targets
WHERE user_id = $1
ORDER BY is_default DESC, created_at DESC`

func (q *Queries) ListDeliveryTargetsByUser(ctx context.Context, userID uuid.UUID) ([]DeliveryTarget, error) {
	rows, err := q.db.Query(ctx, listDeliveryTargetsByUser, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []DeliveryTarget
	for rows.Next() {
		var t DeliveryTarget
		if err := scanDeliveryTarget(rows, &t); err != nil {
			return nil, err
		}
		items = append(items, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getDeliveryTargetByID = `-- name: GetDeliveryTargetByID :one
SELECT id, user_id, name, type, config, secret, is_default, created_at, updated_at
FROM delivery_targets
WHERE id = $1`

func (q *Queries) GetDeliveryTargetByID(ctx context.Context, id uuid.UUID) (DeliveryTarget, error) {
	row := q.db.QueryRow(ctx, getDeliveryTargetByID, id)
	var t DeliveryTarget
	err := scanDeliveryTarget(row, &t)
	return t, err
}

const getDefaultDeliveryTarget = `-- name: GetDefaultDeliveryTarget :one
SELECT id, user_id, name, type, config, secret, is_default, created_at, updated_at
FROM delivery_targets
WHERE user_id = $1 AND is_default = TRUE
LIMIT 1`

func (q *Queries) GetDefaultDeliveryTarget(ctx context.Context, userID uuid.UUID) (DeliveryTarget, error) {
	row := q.db.QueryRow(ctx, getDefaultDeliveryTarget, userID)
	var t DeliveryTarget
	err := scanDeliveryTarget(row, &t)
	return t, err
}

const deleteDeliveryTarget = `-- name: DeleteDeliveryTarget :execrows
DELETE FROM delivery_targets WHERE id = $1 AND user_id = $2`

type DeleteDeliveryTargetParams struct {
	ID     uuid.UUID `db:"id" json:"id"`
	UserID uuid.UUID `db:"user_id" json:"user_id"`
}

func (q *Queries) DeleteDeliveryTarget(ctx context.Context, arg DeleteDeliveryTargetParams) (int64, error) {
	tag, err := q.db.Exec(ctx, deleteDeliveryTarget, arg.ID, arg.UserID)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

const clearDefaultDeliveryTarget = `-- name: ClearDefaultDeliveryTarget :exec
UPDATE delivery_targets
SET is_default = FALSE, updated_at = NOW()
WHERE user_id = $1 AND is_default = TRUE`

func (q *Queries) ClearDefaultDeliveryTarget(ctx context.Context, userID uuid.UUID) error {
	_, err := q.db.Exec(ctx, clearDefaultDeliveryTarget, userID)
	return err
}

const setDeliveryTargetDefault = `-- name: SetDeliveryTargetDefault :execrows
UPDATE delivery_targets
SET is_default = TRUE, updated_at = NOW()
WHERE id = $1 AND user_id = $2`

type SetDeliveryTargetDefaultParams struct {
	ID     uuid.UUID `db:"id" json:"id"`
	UserID uuid.UUID `db:"user_id" json:"user_id"`
}

func (q *Queries) SetDeliveryTargetDefault(ctx context.Context, arg SetDeliveryTargetDefaultParams) (int64, error) {
	tag, err := q.db.Exec(ctx, setDeliveryTargetDefault, arg.ID, arg.UserID)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

const createRunDelivery = `-- name: CreateRunDelivery :one
INSERT INTO run_deliveries (
    run_id, target_id, user_id, target_type, target_url, payload,
    status, attempt_count, next_retry_at
) VALUES (
    $1, $2, $3, $4, $5, $6, 'pending', 0, NOW()
)
RETURNING id, run_id, target_id, user_id, target_type, target_url, payload,
          status, response_status, response_body, error_message,
          attempt_count, next_retry_at, created_at, updated_at`

type CreateRunDeliveryParams struct {
	RunID      uuid.UUID `db:"run_id" json:"run_id"`
	TargetID   uuid.UUID `db:"target_id" json:"target_id"`
	UserID     uuid.UUID `db:"user_id" json:"user_id"`
	TargetType string    `db:"target_type" json:"target_type"`
	TargetURL  string    `db:"target_url" json:"target_url"`
	Payload    []byte    `db:"payload" json:"payload"`
}

func (q *Queries) CreateRunDelivery(ctx context.Context, arg CreateRunDeliveryParams) (RunDelivery, error) {
	row := q.db.QueryRow(ctx, createRunDelivery,
		arg.RunID, arg.TargetID, arg.UserID, arg.TargetType, arg.TargetURL, arg.Payload,
	)
	var d RunDelivery
	err := scanRunDelivery(row, &d)
	return d, err
}

const getRunDeliveryByID = `-- name: GetRunDeliveryByID :one
SELECT d.id, d.run_id, d.target_id, d.user_id, d.target_type, d.target_url, d.payload,
       d.status, d.response_status, d.response_body, d.error_message,
       d.attempt_count, d.next_retry_at, d.created_at, d.updated_at,
       t.secret AS target_secret,
       t.config AS target_config
FROM run_deliveries d
LEFT JOIN delivery_targets t ON t.id = d.target_id
WHERE d.id = $1`

// GetRunDeliveryRow JOIN 结果：投递记录 + 当前 target 的 secret / config。
//
// 投递时 target 已被删时 TargetSecret / TargetConfig 为 NULL，worker 会标 final failed。
type GetRunDeliveryRow struct {
	RunDelivery
	TargetSecret *string `db:"target_secret" json:"-"`
	TargetConfig []byte  `db:"target_config" json:"-"`
}

func (q *Queries) GetRunDeliveryByID(ctx context.Context, id uuid.UUID) (GetRunDeliveryRow, error) {
	row := q.db.QueryRow(ctx, getRunDeliveryByID, id)
	var r GetRunDeliveryRow
	err := row.Scan(
		&r.ID,
		&r.RunID,
		&r.TargetID,
		&r.UserID,
		&r.TargetType,
		&r.TargetURL,
		&r.Payload,
		&r.Status,
		&r.ResponseStatus,
		&r.ResponseBody,
		&r.ErrorMessage,
		&r.AttemptCount,
		&r.NextRetryAt,
		&r.CreatedAt,
		&r.UpdatedAt,
		&r.TargetSecret,
		&r.TargetConfig,
	)
	return r, err
}

const markRunDeliverySuccess = `-- name: MarkRunDeliverySuccess :exec
UPDATE run_deliveries
SET status = 'success',
    response_status = $2,
    response_body = $3,
    error_message = NULL,
    attempt_count = attempt_count + 1,
    next_retry_at = NULL,
    updated_at = NOW()
WHERE id = $1`

type MarkRunDeliverySuccessParams struct {
	ID             uuid.UUID `db:"id" json:"id"`
	ResponseStatus *int32    `db:"response_status" json:"response_status"`
	ResponseBody   *string   `db:"response_body" json:"response_body"`
}

func (q *Queries) MarkRunDeliverySuccess(ctx context.Context, arg MarkRunDeliverySuccessParams) error {
	_, err := q.db.Exec(ctx, markRunDeliverySuccess, arg.ID, arg.ResponseStatus, arg.ResponseBody)
	return err
}

const markRunDeliveryFailedRetry = `-- name: MarkRunDeliveryFailedRetry :exec
UPDATE run_deliveries
SET status = 'pending',
    response_status = $2,
    response_body = $3,
    error_message = $4,
    attempt_count = attempt_count + 1,
    next_retry_at = $5,
    updated_at = NOW()
WHERE id = $1`

type MarkRunDeliveryFailedRetryParams struct {
	ID             uuid.UUID `db:"id" json:"id"`
	ResponseStatus *int32    `db:"response_status" json:"response_status"`
	ResponseBody   *string   `db:"response_body" json:"response_body"`
	ErrorMessage   *string   `db:"error_message" json:"error_message"`
	NextRetryAt    time.Time `db:"next_retry_at" json:"next_retry_at"`
}

func (q *Queries) MarkRunDeliveryFailedRetry(ctx context.Context, arg MarkRunDeliveryFailedRetryParams) error {
	_, err := q.db.Exec(ctx, markRunDeliveryFailedRetry,
		arg.ID, arg.ResponseStatus, arg.ResponseBody, arg.ErrorMessage, arg.NextRetryAt,
	)
	return err
}

const markRunDeliveryFailedFinal = `-- name: MarkRunDeliveryFailedFinal :exec
UPDATE run_deliveries
SET status = 'failed',
    response_status = $2,
    response_body = $3,
    error_message = $4,
    attempt_count = attempt_count + 1,
    next_retry_at = NULL,
    updated_at = NOW()
WHERE id = $1`

type MarkRunDeliveryFailedFinalParams struct {
	ID             uuid.UUID `db:"id" json:"id"`
	ResponseStatus *int32    `db:"response_status" json:"response_status"`
	ResponseBody   *string   `db:"response_body" json:"response_body"`
	ErrorMessage   *string   `db:"error_message" json:"error_message"`
}

func (q *Queries) MarkRunDeliveryFailedFinal(ctx context.Context, arg MarkRunDeliveryFailedFinalParams) error {
	_, err := q.db.Exec(ctx, markRunDeliveryFailedFinal,
		arg.ID, arg.ResponseStatus, arg.ResponseBody, arg.ErrorMessage,
	)
	return err
}

const listPendingRunDeliveries = `-- name: ListPendingRunDeliveries :many
SELECT id, run_id, target_id, user_id, target_type, target_url, payload,
       status, response_status, response_body, error_message,
       attempt_count, next_retry_at, created_at, updated_at
FROM run_deliveries
WHERE status = 'pending' AND next_retry_at IS NOT NULL AND next_retry_at <= NOW()
ORDER BY next_retry_at ASC
LIMIT 50`

func (q *Queries) ListPendingRunDeliveries(ctx context.Context) ([]RunDelivery, error) {
	rows, err := q.db.Query(ctx, listPendingRunDeliveries)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []RunDelivery
	for rows.Next() {
		var d RunDelivery
		if err := scanRunDelivery(rows, &d); err != nil {
			return nil, err
		}
		items = append(items, d)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const listRunDeliveriesByRun = `-- name: ListRunDeliveriesByRun :many
SELECT id, run_id, target_id, user_id, target_type, target_url, payload,
       status, response_status, response_body, error_message,
       attempt_count, next_retry_at, created_at, updated_at
FROM run_deliveries
WHERE run_id = $1
ORDER BY created_at DESC`

func (q *Queries) ListRunDeliveriesByRun(ctx context.Context, runID uuid.UUID) ([]RunDelivery, error) {
	rows, err := q.db.Query(ctx, listRunDeliveriesByRun, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []RunDelivery
	for rows.Next() {
		var d RunDelivery
		if err := scanRunDelivery(rows, &d); err != nil {
			return nil, err
		}
		items = append(items, d)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const resetRunDeliveryForRetry = `-- name: ResetRunDeliveryForRetry :execrows
UPDATE run_deliveries
SET status = 'pending',
    next_retry_at = NOW(),
    updated_at = NOW()
WHERE id = $1 AND user_id = $2 AND status = 'failed'`

type ResetRunDeliveryForRetryParams struct {
	ID     uuid.UUID `db:"id" json:"id"`
	UserID uuid.UUID `db:"user_id" json:"user_id"`
}

func (q *Queries) ResetRunDeliveryForRetry(ctx context.Context, arg ResetRunDeliveryForRetryParams) (int64, error) {
	tag, err := q.db.Exec(ctx, resetRunDeliveryForRetry, arg.ID, arg.UserID)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
