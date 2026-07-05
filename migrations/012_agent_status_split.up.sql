-- 012_agent_status_split.up.sql
-- Phase 2 缺口 2：把 agents.status 单字段拆成三维度（docs/29 §三）。
--
--   lifecycle_status   active / disabled                  平台运转开关
--   visibility         public / unlisted / private        市场可见度（与 agent_call_policies 正交）
--   certification_status unreviewed/pending/certified/rejected  推荐 / 认证 / 榜单
--
-- approved_at → certified_at（语义对齐认证而非"过审"）。
-- rejection_reason 保留：现在专指 certification rejection 原因。
--
-- 数据回填映射（沿用 008_agent_public_default 后的口径，即所有新建默认 approved）：
--   approved → lifecycle=active, visibility=public, cert=unreviewed
--   pending  → lifecycle=active, visibility=public, cert=pending
--   rejected → lifecycle=active, visibility=public, cert=rejected
--   disabled → lifecycle=disabled, visibility=public, cert=unreviewed

BEGIN;

ALTER TABLE agents
    ADD COLUMN lifecycle_status TEXT NOT NULL DEFAULT 'active',
    ADD COLUMN visibility TEXT NOT NULL DEFAULT 'public',
    ADD COLUMN certification_status TEXT NOT NULL DEFAULT 'unreviewed',
    ADD COLUMN certified_at TIMESTAMPTZ;

UPDATE agents
SET lifecycle_status = CASE WHEN status = 'disabled' THEN 'disabled' ELSE 'active' END,
    visibility = 'public',
    certification_status = CASE
        WHEN status = 'pending'  THEN 'pending'
        WHEN status = 'rejected' THEN 'rejected'
        ELSE 'unreviewed'
    END,
    certified_at = NULL;

-- 把旧 approved_at 当作 certified_at 的迁移源仅在 approved 的情况下保留；
-- pending/rejected/disabled 没有"被认证"语义，certified_at 应为 NULL。
UPDATE agents
SET certified_at = approved_at
WHERE status = 'approved' AND approved_at IS NOT NULL;

ALTER TABLE agents
    DROP CONSTRAINT IF EXISTS agents_status_valid,
    ADD CONSTRAINT agents_lifecycle_status_valid
        CHECK (lifecycle_status IN ('active', 'disabled')),
    ADD CONSTRAINT agents_visibility_valid
        CHECK (visibility IN ('public', 'unlisted', 'private')),
    ADD CONSTRAINT agents_certification_status_valid
        CHECK (certification_status IN ('unreviewed', 'pending', 'certified', 'rejected'));

DROP INDEX IF EXISTS idx_agents_status;
DROP INDEX IF EXISTS idx_agents_approved;

ALTER TABLE agents
    DROP COLUMN status,
    DROP COLUMN approved_at;

CREATE INDEX idx_agents_lifecycle ON agents (lifecycle_status, created_at DESC);
-- 市场查询主路径：visibility=public AND lifecycle_status=active。
CREATE INDEX idx_agents_market_listing
    ON agents (created_at DESC)
    WHERE visibility = 'public' AND lifecycle_status = 'active';
CREATE INDEX idx_agents_pending_certification
    ON agents (created_at DESC)
    WHERE certification_status = 'pending';

COMMIT;
