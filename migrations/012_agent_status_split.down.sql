-- 012_agent_status_split.down.sql
-- 还原到 008 之后的单字段 status 模型。

BEGIN;

DROP INDEX IF EXISTS idx_agents_pending_certification;
DROP INDEX IF EXISTS idx_agents_market_listing;
DROP INDEX IF EXISTS idx_agents_lifecycle;

ALTER TABLE agents
    ADD COLUMN status TEXT NOT NULL DEFAULT 'approved',
    ADD COLUMN approved_at TIMESTAMPTZ;

UPDATE agents
SET status = CASE
        WHEN lifecycle_status = 'disabled' THEN 'disabled'
        WHEN certification_status = 'rejected' THEN 'rejected'
        WHEN certification_status = 'pending' THEN 'pending'
        ELSE 'approved'
    END,
    approved_at = certified_at;

ALTER TABLE agents
    DROP CONSTRAINT IF EXISTS agents_lifecycle_status_valid,
    DROP CONSTRAINT IF EXISTS agents_visibility_valid,
    DROP CONSTRAINT IF EXISTS agents_certification_status_valid,
    ADD CONSTRAINT agents_status_valid
        CHECK (status IN ('pending', 'approved', 'rejected', 'disabled'));

CREATE INDEX idx_agents_status ON agents (status, created_at DESC);
CREATE INDEX idx_agents_approved ON agents (created_at DESC) WHERE status = 'approved';

ALTER TABLE agents
    DROP COLUMN lifecycle_status,
    DROP COLUMN visibility,
    DROP COLUMN certification_status,
    DROP COLUMN certified_at;

COMMIT;
