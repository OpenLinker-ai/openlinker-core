-- name: GetWalletByUserID :one
-- 读取用户钱包（不加锁）
SELECT user_id, balance_cents, earnings_cents, total_charged_cents,
       total_spent_cents, total_earned_cents, total_withdrawn_cents, updated_at
FROM wallets WHERE user_id = $1;

-- name: GetWalletForUpdate :one
-- 在事务中锁定钱包行（FOR UPDATE）。仅可在事务里调用。
SELECT user_id, balance_cents, earnings_cents, total_charged_cents,
       total_spent_cents, total_earned_cents, total_withdrawn_cents, updated_at
FROM wallets WHERE user_id = $1 FOR UPDATE;

-- name: AddWalletBalance :exec
-- 充值入账：原子性增加 balance + total_charged。
UPDATE wallets
SET balance_cents = balance_cents + $2,
    total_charged_cents = total_charged_cents + $2
WHERE user_id = $1;

-- name: SubtractWalletEarnings :exec
-- 创作者申请提现时扣减 earnings_cents。
-- 受 wallets_earnings_nonneg CHECK 约束保护，不足会失败。
UPDATE wallets
SET earnings_cents = earnings_cents - $2
WHERE user_id = $1 AND earnings_cents >= $2;

-- name: AddWalletEarnings :exec
-- 退还 earnings（管理员拒绝提现时回填）。
UPDATE wallets
SET earnings_cents = earnings_cents + $2
WHERE user_id = $1;

-- name: AddWalletWithdrawn :exec
-- 提现 paid 时累计 total_withdrawn_cents。
UPDATE wallets
SET total_withdrawn_cents = total_withdrawn_cents + $2
WHERE user_id = $1;

-- name: SubtractWalletBalance :execrows
-- 调用时扣余额（事务内）
-- 返回受影响行数：0 = 余额不足（schema CHECK balance_cents >= 0 阻止负值更新）
UPDATE wallets
SET balance_cents = balance_cents - $2,
    total_spent_cents = total_spent_cents + $2
WHERE user_id = $1 AND balance_cents >= $2;

-- name: AddCreatorEarnings :exec
-- 调用成功后抽成入账：creator earnings_cents 增加
UPDATE wallets
SET earnings_cents = earnings_cents + $2,
    total_earned_cents = total_earned_cents + $2
WHERE user_id = $1;

-- name: RefundUserBalance :exec
-- 调用失败退款：余额回滚，total_spent 同时回滚
UPDATE wallets
SET balance_cents = balance_cents + $2,
    total_spent_cents = total_spent_cents - $2
WHERE user_id = $1;
