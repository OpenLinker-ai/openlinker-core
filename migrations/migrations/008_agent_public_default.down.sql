-- 回退默认值；不回滚已公开的数据，避免误下架。
ALTER TABLE agents ALTER COLUMN status SET DEFAULT 'pending';
