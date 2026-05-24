// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/withdrawals.sql）。

package db

import (
	"context"

	"github.com/google/uuid"
)

const createWithdrawal = `-- name: CreateWithdrawal :one
INSERT INTO withdrawals (
    creator_id, amount_cents, status
) VALUES (
    $1, $2, 'pending'
)
RETURNING id, creator_id, amount_cents, status, notes, created_at, paid_at`

// CreateWithdrawalParams 入参。
type CreateWithdrawalParams struct {
	CreatorID   uuid.UUID `db:"creator_id" json:"creator_id"`
	AmountCents int32     `db:"amount_cents" json:"amount_cents"`
}

// CreateWithdrawal 创建提现申请（status='pending'）。
func (q *Queries) CreateWithdrawal(ctx context.Context, arg CreateWithdrawalParams) (Withdrawal, error) {
	row := q.db.QueryRow(ctx, createWithdrawal, arg.CreatorID, arg.AmountCents)
	var w Withdrawal
	err := row.Scan(
		&w.ID,
		&w.CreatorID,
		&w.AmountCents,
		&w.Status,
		&w.Notes,
		&w.CreatedAt,
		&w.PaidAt,
	)
	return w, err
}

const getWithdrawalByID = `-- name: GetWithdrawalByID :one
SELECT id, creator_id, amount_cents, status, notes, created_at, paid_at
FROM withdrawals
WHERE id = $1`

// GetWithdrawalByID 按 id 查询。
func (q *Queries) GetWithdrawalByID(ctx context.Context, id uuid.UUID) (Withdrawal, error) {
	row := q.db.QueryRow(ctx, getWithdrawalByID, id)
	var w Withdrawal
	err := row.Scan(
		&w.ID,
		&w.CreatorID,
		&w.AmountCents,
		&w.Status,
		&w.Notes,
		&w.CreatedAt,
		&w.PaidAt,
	)
	return w, err
}

const listWithdrawalsByCreator = `-- name: ListWithdrawalsByCreator :many
SELECT id, creator_id, amount_cents, status, notes, created_at, paid_at
FROM withdrawals
WHERE creator_id = $1
ORDER BY created_at DESC`

// ListWithdrawalsByCreator 创作者提现历史（倒序）。
func (q *Queries) ListWithdrawalsByCreator(ctx context.Context, creatorID uuid.UUID) ([]Withdrawal, error) {
	rows, err := q.db.Query(ctx, listWithdrawalsByCreator, creatorID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []Withdrawal
	for rows.Next() {
		var w Withdrawal
		if err := rows.Scan(
			&w.ID,
			&w.CreatorID,
			&w.AmountCents,
			&w.Status,
			&w.Notes,
			&w.CreatedAt,
			&w.PaidAt,
		); err != nil {
			return nil, err
		}
		items = append(items, w)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const listPendingWithdrawals = `-- name: ListPendingWithdrawals :many
SELECT id, creator_id, amount_cents, status, notes, created_at, paid_at
FROM withdrawals
WHERE status = 'pending'
ORDER BY created_at ASC`

// ListPendingWithdrawals 管理员视角：所有待处理提现（先到先处理）。
func (q *Queries) ListPendingWithdrawals(ctx context.Context) ([]Withdrawal, error) {
	rows, err := q.db.Query(ctx, listPendingWithdrawals)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []Withdrawal
	for rows.Next() {
		var w Withdrawal
		if err := rows.Scan(
			&w.ID,
			&w.CreatorID,
			&w.AmountCents,
			&w.Status,
			&w.Notes,
			&w.CreatedAt,
			&w.PaidAt,
		); err != nil {
			return nil, err
		}
		items = append(items, w)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const markWithdrawalPaid = `-- name: MarkWithdrawalPaid :exec
UPDATE withdrawals
SET status = 'paid',
    paid_at = NOW(),
    notes = $2
WHERE id = $1 AND status = 'pending'`

// MarkWithdrawalPaidParams 入参。
type MarkWithdrawalPaidParams struct {
	ID    uuid.UUID `db:"id" json:"id"`
	Notes *string   `db:"notes" json:"notes"`
}

// MarkWithdrawalPaid 标记提现已支付。
// 返回受影响行数：0 表示状态非 pending（已被处理）。
func (q *Queries) MarkWithdrawalPaid(ctx context.Context, arg MarkWithdrawalPaidParams) (int64, error) {
	tag, err := q.db.Exec(ctx, markWithdrawalPaid, arg.ID, arg.Notes)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

const markWithdrawalRejected = `-- name: MarkWithdrawalRejected :exec
UPDATE withdrawals
SET status = 'rejected',
    notes = $2
WHERE id = $1 AND status = 'pending'`

// MarkWithdrawalRejectedParams 入参。
type MarkWithdrawalRejectedParams struct {
	ID    uuid.UUID `db:"id" json:"id"`
	Notes *string   `db:"notes" json:"notes"`
}

// MarkWithdrawalRejected 标记提现已拒绝。
// 返回受影响行数：0 表示状态非 pending（已被处理）。
// 退还 earnings 由 service 在事务内单独调用 AddWalletEarnings。
func (q *Queries) MarkWithdrawalRejected(ctx context.Context, arg MarkWithdrawalRejectedParams) (int64, error) {
	tag, err := q.db.Exec(ctx, markWithdrawalRejected, arg.ID, arg.Notes)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
