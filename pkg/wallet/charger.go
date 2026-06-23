package wallet

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Charger implements runtime.WalletCharger against the cloud wallet tables.
//
// It intentionally receives the caller's pgx.Tx so charging, run creation,
// success settlement and refunds stay atomic with runtime state changes.
type Charger struct{}

func NewCharger() *Charger {
	return &Charger{}
}

type txExecutor interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func (c *Charger) Charge(ctx context.Context, tx pgx.Tx, userID uuid.UUID, amountCents int64) (bool, error) {
	return charge(ctx, tx, userID, amountCents)
}

func (c *Charger) CreditCreator(ctx context.Context, tx pgx.Tx, creatorID uuid.UUID, amountCents int64) error {
	_, err := tx.Exec(ctx, `
UPDATE wallets
SET earnings_cents = earnings_cents + $2,
    total_earned_cents = total_earned_cents + $2
WHERE user_id = $1
`, creatorID, amountCents)
	return err
}

func (c *Charger) Refund(ctx context.Context, tx pgx.Tx, userID uuid.UUID, amountCents int64) error {
	return refund(ctx, tx, userID, amountCents)
}

func charge(ctx context.Context, exec txExecutor, userID uuid.UUID, amountCents int64) (bool, error) {
	tag, err := exec.Exec(ctx, `
UPDATE wallets
SET balance_cents = balance_cents - $2,
    total_spent_cents = total_spent_cents + $2
WHERE user_id = $1 AND balance_cents >= $2
`, userID, amountCents)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func refund(ctx context.Context, exec txExecutor, userID uuid.UUID, amountCents int64) error {
	_, err := exec.Exec(ctx, `
UPDATE wallets
SET balance_cents = balance_cents + $2,
    total_spent_cents = GREATEST(total_spent_cents - $2, 0)
WHERE user_id = $1
`, userID, amountCents)
	return err
}
