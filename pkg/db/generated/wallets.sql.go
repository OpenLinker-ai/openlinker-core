// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/wallets.sql）。

package db

import (
	"context"

	"github.com/google/uuid"
)

const getWalletByUserID = `-- name: GetWalletByUserID :one
SELECT user_id, balance_cents, earnings_cents, total_charged_cents,
       total_spent_cents, total_earned_cents, total_withdrawn_cents, updated_at
FROM wallets WHERE user_id = $1`

// GetWalletByUserID 读取钱包（不加锁）。
func (q *Queries) GetWalletByUserID(ctx context.Context, userID uuid.UUID) (Wallet, error) {
	row := q.db.QueryRow(ctx, getWalletByUserID, userID)
	var w Wallet
	err := row.Scan(
		&w.UserID,
		&w.BalanceCents,
		&w.EarningsCents,
		&w.TotalChargedCents,
		&w.TotalSpentCents,
		&w.TotalEarnedCents,
		&w.TotalWithdrawnCents,
		&w.UpdatedAt,
	)
	return w, err
}

const getWalletForUpdate = `-- name: GetWalletForUpdate :one
SELECT user_id, balance_cents, earnings_cents, total_charged_cents,
       total_spent_cents, total_earned_cents, total_withdrawn_cents, updated_at
FROM wallets WHERE user_id = $1 FOR UPDATE`

// GetWalletForUpdate 在事务中锁定钱包行（FOR UPDATE）。
// 仅当 q 来自 WithTx(tx) 时才有意义。
func (q *Queries) GetWalletForUpdate(ctx context.Context, userID uuid.UUID) (Wallet, error) {
	row := q.db.QueryRow(ctx, getWalletForUpdate, userID)
	var w Wallet
	err := row.Scan(
		&w.UserID,
		&w.BalanceCents,
		&w.EarningsCents,
		&w.TotalChargedCents,
		&w.TotalSpentCents,
		&w.TotalEarnedCents,
		&w.TotalWithdrawnCents,
		&w.UpdatedAt,
	)
	return w, err
}

const addWalletBalance = `-- name: AddWalletBalance :exec
UPDATE wallets
SET balance_cents = balance_cents + $2,
    total_charged_cents = total_charged_cents + $2
WHERE user_id = $1`

// AddWalletBalanceParams 入参。
type AddWalletBalanceParams struct {
	UserID      uuid.UUID `db:"user_id" json:"user_id"`
	AmountCents int64     `db:"amount_cents" json:"amount_cents"`
}

// AddWalletBalance 充值入账：原子性增加 balance + total_charged。
func (q *Queries) AddWalletBalance(ctx context.Context, arg AddWalletBalanceParams) error {
	_, err := q.db.Exec(ctx, addWalletBalance, arg.UserID, arg.AmountCents)
	return err
}

const subtractWalletEarnings = `-- name: SubtractWalletEarnings :exec
UPDATE wallets
SET earnings_cents = earnings_cents - $2
WHERE user_id = $1 AND earnings_cents >= $2`

// SubtractWalletEarningsParams 入参。
type SubtractWalletEarningsParams struct {
	UserID      uuid.UUID `db:"user_id" json:"user_id"`
	AmountCents int64     `db:"amount_cents" json:"amount_cents"`
}

// SubtractWalletEarnings 扣减创作者 earnings_cents（用于提现申请）。
// 返回的 CommandTag 行数 == 0 表示余额不足。
func (q *Queries) SubtractWalletEarnings(ctx context.Context, arg SubtractWalletEarningsParams) error {
	_, err := q.db.Exec(ctx, subtractWalletEarnings, arg.UserID, arg.AmountCents)
	return err
}

// SubtractWalletEarningsRows 与 SubtractWalletEarnings 相同语义，
// 但额外返回受影响行数，方便 service 层判断 earnings 是否足够。
func (q *Queries) SubtractWalletEarningsRows(ctx context.Context, arg SubtractWalletEarningsParams) (int64, error) {
	tag, err := q.db.Exec(ctx, subtractWalletEarnings, arg.UserID, arg.AmountCents)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

const addWalletEarnings = `-- name: AddWalletEarnings :exec
UPDATE wallets
SET earnings_cents = earnings_cents + $2
WHERE user_id = $1`

// AddWalletEarningsParams 入参。
type AddWalletEarningsParams struct {
	UserID      uuid.UUID `db:"user_id" json:"user_id"`
	AmountCents int64     `db:"amount_cents" json:"amount_cents"`
}

// AddWalletEarnings 退还 earnings（管理员拒绝提现时调用）。
func (q *Queries) AddWalletEarnings(ctx context.Context, arg AddWalletEarningsParams) error {
	_, err := q.db.Exec(ctx, addWalletEarnings, arg.UserID, arg.AmountCents)
	return err
}

const addWalletWithdrawn = `-- name: AddWalletWithdrawn :exec
UPDATE wallets
SET total_withdrawn_cents = total_withdrawn_cents + $2
WHERE user_id = $1`

// AddWalletWithdrawnParams 入参。
type AddWalletWithdrawnParams struct {
	UserID      uuid.UUID `db:"user_id" json:"user_id"`
	AmountCents int64     `db:"amount_cents" json:"amount_cents"`
}

// AddWalletWithdrawn 提现完成时累计 total_withdrawn_cents。
func (q *Queries) AddWalletWithdrawn(ctx context.Context, arg AddWalletWithdrawnParams) error {
	_, err := q.db.Exec(ctx, addWalletWithdrawn, arg.UserID, arg.AmountCents)
	return err
}

const subtractWalletBalance = `-- name: SubtractWalletBalance :execrows
UPDATE wallets
SET balance_cents = balance_cents - $2,
    total_spent_cents = total_spent_cents + $2
WHERE user_id = $1 AND balance_cents >= $2`

// SubtractWalletBalanceParams 入参（模块 4 调用扣款用）。
type SubtractWalletBalanceParams struct {
	UserID       uuid.UUID `db:"user_id" json:"user_id"`
	BalanceCents int64     `db:"balance_cents" json:"balance_cents"`
}

// SubtractWalletBalance 调用时扣余额。
// 返回受影响行数 == 0 表示余额不足（WHERE 子句中 balance_cents >= $2 守卫不会更新）。
// 必须在事务中调用，与 CreateRun 一起原子执行。
func (q *Queries) SubtractWalletBalance(ctx context.Context, arg SubtractWalletBalanceParams) (int64, error) {
	tag, err := q.db.Exec(ctx, subtractWalletBalance, arg.UserID, arg.BalanceCents)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

const addCreatorEarnings = `-- name: AddCreatorEarnings :exec
UPDATE wallets
SET earnings_cents = earnings_cents + $2,
    total_earned_cents = total_earned_cents + $2
WHERE user_id = $1`

// AddCreatorEarningsParams 入参（模块 4 抽成入账用）。
type AddCreatorEarningsParams struct {
	UserID        uuid.UUID `db:"user_id" json:"user_id"`
	EarningsCents int64     `db:"earnings_cents" json:"earnings_cents"`
}

// AddCreatorEarnings 调用成功后给创作者钱包入账。
// total_earned 累计创作者赚到的总额（含已提现部分）。
func (q *Queries) AddCreatorEarnings(ctx context.Context, arg AddCreatorEarningsParams) error {
	_, err := q.db.Exec(ctx, addCreatorEarnings, arg.UserID, arg.EarningsCents)
	return err
}

const refundUserBalance = `-- name: RefundUserBalance :exec
UPDATE wallets
SET balance_cents = balance_cents + $2,
    total_spent_cents = total_spent_cents - $2
WHERE user_id = $1`

// RefundUserBalanceParams 入参（模块 4 失败退款用）。
type RefundUserBalanceParams struct {
	UserID       uuid.UUID `db:"user_id" json:"user_id"`
	BalanceCents int64     `db:"balance_cents" json:"balance_cents"`
}

// RefundUserBalance 调用失败退款：回滚 balance 与 total_spent。
func (q *Queries) RefundUserBalance(ctx context.Context, arg RefundUserBalanceParams) error {
	_, err := q.db.Exec(ctx, refundUserBalance, arg.UserID, arg.BalanceCents)
	return err
}
