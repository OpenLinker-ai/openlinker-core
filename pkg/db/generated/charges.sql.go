// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/charges.sql）。

package db

import (
	"context"

	"github.com/google/uuid"
)

const createCharge = `-- name: CreateCharge :one
INSERT INTO charges (
    user_id, amount_cents, currency, stripe_payment_intent_id, status
) VALUES (
    $1, $2, $3, $4, $5
)
RETURNING id, user_id, amount_cents, currency, stripe_payment_intent_id,
          status, failure_message, created_at, succeeded_at`

// CreateChargeParams 入参。
type CreateChargeParams struct {
	UserID                uuid.UUID `db:"user_id" json:"user_id"`
	AmountCents           int32     `db:"amount_cents" json:"amount_cents"`
	Currency              string    `db:"currency" json:"currency"`
	StripePaymentIntentID *string   `db:"stripe_payment_intent_id" json:"stripe_payment_intent_id"`
	Status                string    `db:"status" json:"status"`
}

// CreateCharge 写入一条充值记录（一般 status='pending'）。
func (q *Queries) CreateCharge(ctx context.Context, arg CreateChargeParams) (Charge, error) {
	row := q.db.QueryRow(ctx, createCharge,
		arg.UserID,
		arg.AmountCents,
		arg.Currency,
		arg.StripePaymentIntentID,
		arg.Status,
	)
	var ch Charge
	err := row.Scan(
		&ch.ID,
		&ch.UserID,
		&ch.AmountCents,
		&ch.Currency,
		&ch.StripePaymentIntentID,
		&ch.Status,
		&ch.FailureMessage,
		&ch.CreatedAt,
		&ch.SucceededAt,
	)
	return ch, err
}

const getChargeByPaymentIntentID = `-- name: GetChargeByPaymentIntentID :one
SELECT id, user_id, amount_cents, currency, stripe_payment_intent_id,
       status, failure_message, created_at, succeeded_at
FROM charges
WHERE stripe_payment_intent_id = $1`

// GetChargeByPaymentIntentID 按 Stripe PI id 查 charge（webhook 入账用）。
func (q *Queries) GetChargeByPaymentIntentID(ctx context.Context, paymentIntentID string) (Charge, error) {
	row := q.db.QueryRow(ctx, getChargeByPaymentIntentID, paymentIntentID)
	var ch Charge
	err := row.Scan(
		&ch.ID,
		&ch.UserID,
		&ch.AmountCents,
		&ch.Currency,
		&ch.StripePaymentIntentID,
		&ch.Status,
		&ch.FailureMessage,
		&ch.CreatedAt,
		&ch.SucceededAt,
	)
	return ch, err
}

const markChargeSucceeded = `-- name: MarkChargeSucceeded :exec
UPDATE charges
SET status = 'succeeded',
    succeeded_at = NOW()
WHERE stripe_payment_intent_id = $1 AND status = 'pending'`

// MarkChargeSucceeded 把 charge 置为已成功。
// 返回受影响行数：0 表示已经入过账（重复 webhook），调用方应跳过钱包加钱。
func (q *Queries) MarkChargeSucceeded(ctx context.Context, paymentIntentID string) (int64, error) {
	tag, err := q.db.Exec(ctx, markChargeSucceeded, paymentIntentID)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

const markChargeFailed = `-- name: MarkChargeFailed :exec
UPDATE charges
SET status = 'failed',
    failure_message = $2
WHERE stripe_payment_intent_id = $1 AND status = 'pending'`

// MarkChargeFailedParams 入参。
type MarkChargeFailedParams struct {
	StripePaymentIntentID string `db:"stripe_payment_intent_id" json:"stripe_payment_intent_id"`
	FailureMessage        string `db:"failure_message" json:"failure_message"`
}

// MarkChargeFailed 标记充值失败（写失败原因）。
func (q *Queries) MarkChargeFailed(ctx context.Context, arg MarkChargeFailedParams) error {
	msg := arg.FailureMessage
	var msgPtr *string
	if msg != "" {
		msgPtr = &msg
	}
	_, err := q.db.Exec(ctx, markChargeFailed, arg.StripePaymentIntentID, msgPtr)
	return err
}

const listChargesByUser = `-- name: ListChargesByUser :many
SELECT id, user_id, amount_cents, currency, stripe_payment_intent_id,
       status, failure_message, created_at, succeeded_at
FROM charges
WHERE user_id = $1
ORDER BY created_at DESC
LIMIT $2`

// ListChargesByUserParams 入参。
type ListChargesByUserParams struct {
	UserID uuid.UUID `db:"user_id" json:"user_id"`
	Limit  int32     `db:"limit" json:"limit"`
}

// ListChargesByUser 用户充值历史（按时间倒序）。
func (q *Queries) ListChargesByUser(ctx context.Context, arg ListChargesByUserParams) ([]Charge, error) {
	rows, err := q.db.Query(ctx, listChargesByUser, arg.UserID, arg.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []Charge
	for rows.Next() {
		var ch Charge
		if err := rows.Scan(
			&ch.ID,
			&ch.UserID,
			&ch.AmountCents,
			&ch.Currency,
			&ch.StripePaymentIntentID,
			&ch.Status,
			&ch.FailureMessage,
			&ch.CreatedAt,
			&ch.SucceededAt,
		); err != nil {
			return nil, err
		}
		items = append(items, ch)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
