-- name: CreateCharge :one
-- 创建充值记录（status = 'pending'）。
INSERT INTO charges (
    user_id, amount_cents, currency, stripe_payment_intent_id, status
) VALUES (
    $1, $2, $3, $4, $5
)
RETURNING id, user_id, amount_cents, currency, stripe_payment_intent_id,
          status, failure_message, created_at, succeeded_at;

-- name: GetChargeByPaymentIntentID :one
-- 按 Stripe payment_intent_id 查询充值记录（webhook 用）。
SELECT id, user_id, amount_cents, currency, stripe_payment_intent_id,
       status, failure_message, created_at, succeeded_at
FROM charges
WHERE stripe_payment_intent_id = $1;

-- name: MarkChargeSucceeded :exec
-- 充值成功入账：仅当当前 status = 'pending' 才更新（防止重复入账）。
-- 配合 stripe_payment_intent_id UNIQUE 与上层事务，实现幂等。
UPDATE charges
SET status = 'succeeded',
    succeeded_at = NOW()
WHERE stripe_payment_intent_id = $1 AND status = 'pending';

-- name: MarkChargeFailed :exec
-- 充值失败：记录失败原因。
UPDATE charges
SET status = 'failed',
    failure_message = $2
WHERE stripe_payment_intent_id = $1 AND status = 'pending';

-- name: ListChargesByUser :many
-- 用户充值历史（按时间倒序，limit 控制条数）。
SELECT id, user_id, amount_cents, currency, stripe_payment_intent_id,
       status, failure_message, created_at, succeeded_at
FROM charges
WHERE user_id = $1
ORDER BY created_at DESC
LIMIT $2;
