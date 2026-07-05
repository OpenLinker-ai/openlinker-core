-- 001_init.up.sql
-- openlinker-core 基线: users + agents + runs + updated_at 触发器
-- 钱包/充值/提现/api_keys 等商业化表归 openlinker-cloud/migrations/001_cloud_init.up.sql
-- 关联 docs/10-phase1-architecture.md 章三, docs/26 §7

BEGIN;

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ──────────────────────────────────────────────────────
-- 1. 用户
-- ──────────────────────────────────────────────────────
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email TEXT NOT NULL,
    password_hash TEXT,
    oauth_provider TEXT,
    oauth_id TEXT,
    display_name TEXT NOT NULL,
    avatar_url TEXT,
    is_creator BOOLEAN NOT NULL DEFAULT FALSE,
    creator_verified BOOLEAN NOT NULL DEFAULT FALSE,
    is_admin BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ,
    CONSTRAINT users_email_unique UNIQUE (email),
    CONSTRAINT users_oauth_unique UNIQUE (oauth_provider, oauth_id),
    CONSTRAINT users_must_have_credential CHECK (
        password_hash IS NOT NULL OR
        (oauth_provider IS NOT NULL AND oauth_id IS NOT NULL)
    )
);

CREATE INDEX idx_users_email ON users (email) WHERE deleted_at IS NULL;
CREATE INDEX idx_users_creator ON users (is_creator) WHERE is_creator = TRUE AND deleted_at IS NULL;

-- ──────────────────────────────────────────────────────
-- 2. Agent
-- ──────────────────────────────────────────────────────
CREATE TABLE agents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    creator_id UUID NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    slug TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    endpoint_url TEXT NOT NULL,
    endpoint_auth_header TEXT,
    price_per_call_cents INTEGER NOT NULL,
    tags TEXT[] NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'pending',
    rejection_reason TEXT,
    approved_at TIMESTAMPTZ,
    total_calls INTEGER NOT NULL DEFAULT 0,
    total_revenue_cents BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT agents_slug_unique UNIQUE (slug),
    CONSTRAINT agents_slug_format CHECK (slug ~ '^[a-z0-9][a-z0-9-]*[a-z0-9]$'),
    CONSTRAINT agents_status_valid CHECK (status IN ('pending', 'approved', 'rejected', 'disabled')),
    CONSTRAINT agents_price_positive CHECK (price_per_call_cents > 0 AND price_per_call_cents <= 1000000),
    CONSTRAINT agents_endpoint_https CHECK (endpoint_url LIKE 'https://%')
);

CREATE INDEX idx_agents_creator ON agents (creator_id);
CREATE INDEX idx_agents_status ON agents (status, created_at DESC);
CREATE INDEX idx_agents_approved ON agents (created_at DESC) WHERE status = 'approved';
CREATE INDEX idx_agents_tags ON agents USING GIN (tags);

-- ──────────────────────────────────────────────────────
-- 3. 调用记录
-- ──────────────────────────────────────────────────────
CREATE TABLE runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    agent_id UUID NOT NULL REFERENCES agents(id) ON DELETE RESTRICT,
    input JSONB NOT NULL,
    output JSONB,
    status TEXT NOT NULL DEFAULT 'running',
    error_code TEXT,
    error_message TEXT,
    cost_cents INTEGER NOT NULL,
    platform_fee_cents INTEGER NOT NULL,
    creator_revenue_cents INTEGER NOT NULL,
    duration_ms INTEGER,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    CONSTRAINT runs_status_valid CHECK (status IN ('running', 'success', 'failed', 'timeout')),
    CONSTRAINT runs_cost_nonneg CHECK (cost_cents >= 0),
    CONSTRAINT runs_fee_consistent CHECK (cost_cents = platform_fee_cents + creator_revenue_cents)
);

CREATE INDEX idx_runs_user_time ON runs (user_id, started_at DESC);
CREATE INDEX idx_runs_agent_time ON runs (agent_id, started_at DESC);
CREATE INDEX idx_runs_status ON runs (status, started_at DESC);

-- ──────────────────────────────────────────────────────
-- updated_at 自动维护
-- ──────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION trigger_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER users_set_updated_at BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();
CREATE TRIGGER agents_set_updated_at BEFORE UPDATE ON agents
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

COMMIT;
