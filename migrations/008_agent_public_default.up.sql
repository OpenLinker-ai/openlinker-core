-- Agent 注册默认立即公开；pending/rejected 保留给人工处理或历史兼容。
ALTER TABLE agents ALTER COLUMN status SET DEFAULT 'approved';

UPDATE agents
SET status = 'approved',
    approved_at = COALESCE(approved_at, NOW()),
    rejection_reason = NULL,
    updated_at = NOW()
WHERE status = 'pending';
