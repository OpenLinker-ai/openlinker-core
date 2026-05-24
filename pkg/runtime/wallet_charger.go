package runtime

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// WalletCharger 钱包扣费 / 退款接口。
//
// 实现在 cloud 侧(internal/wallet.Charger),通过 SetWalletCharger 注入。
// 之所以走接口:wallets 表归 cloud,core 单独部署时没有钱包系统,
// charger == nil 直接跳过扣费,只写 runs.cost_cents 做账面记录。
//
// 接口签名带 pgx.Tx:SubtractWalletBalance 必须与 CreateRun 共享事务,
// 否则余额不足时 runs 行可能已插入但事务回滚不一致。退款同理。
type WalletCharger interface {
	// Charge 事务内扣余额。返回 (true, nil) = 已扣;(false, nil) = 余额不足;(_, err) = DB 错误。
	Charge(ctx context.Context, tx pgx.Tx, userID uuid.UUID, amountCents int64) (bool, error)
	// Refund 事务内退款(run 失败 / 超时路径)。
	Refund(ctx context.Context, tx pgx.Tx, userID uuid.UUID, amountCents int64) error
}
