-- name: CreateWithdrawal :one
-- 创建提现申请（status = 'pending'）。
INSERT INTO withdrawals (
    creator_id, amount_cents, status
) VALUES (
    $1, $2, 'pending'
)
RETURNING id, creator_id, amount_cents, status, notes, created_at, paid_at;

-- name: GetWithdrawalByID :one
-- 按 id 查询提现记录。
SELECT id, creator_id, amount_cents, status, notes, created_at, paid_at
FROM withdrawals
WHERE id = $1;

-- name: ListWithdrawalsByCreator :many
-- 创作者的提现历史（按时间倒序）。
SELECT id, creator_id, amount_cents, status, notes, created_at, paid_at
FROM withdrawals
WHERE creator_id = $1
ORDER BY created_at DESC;

-- name: ListPendingWithdrawals :many
-- 管理员视角：所有 pending 提现（按时间正序方便先到先处理）。
SELECT id, creator_id, amount_cents, status, notes, created_at, paid_at
FROM withdrawals
WHERE status = 'pending'
ORDER BY created_at ASC;

-- name: MarkWithdrawalPaid :exec
-- 管理员标记已支付：仅当当前 status = 'pending'。
UPDATE withdrawals
SET status = 'paid',
    paid_at = NOW(),
    notes = $2
WHERE id = $1 AND status = 'pending';

-- name: MarkWithdrawalRejected :exec
-- 管理员拒绝：仅当当前 status = 'pending'。
-- 退还 earnings 由 service 层在事务中完成（AddWalletEarnings）。
UPDATE withdrawals
SET status = 'rejected',
    notes = $2
WHERE id = $1 AND status = 'pending';
