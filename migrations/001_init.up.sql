-- 001_init.up.sql
-- OpenLinker Phase 1 数据库初始化
-- 关联 docs/10-phase1-architecture.md 章三

BEGIN;

-- 启用扩展
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
-- 4. 钱包
-- ──────────────────────────────────────────────────────
CREATE TABLE wallets (
    user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    balance_cents BIGINT NOT NULL DEFAULT 0,
    earnings_cents BIGINT NOT NULL DEFAULT 0,
    total_charged_cents BIGINT NOT NULL DEFAULT 0,
    total_spent_cents BIGINT NOT NULL DEFAULT 0,
    total_earned_cents BIGINT NOT NULL DEFAULT 0,
    total_withdrawn_cents BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT wallets_balance_nonneg CHECK (balance_cents >= 0),
    CONSTRAINT wallets_earnings_nonneg CHECK (earnings_cents >= 0)
);

-- ──────────────────────────────────────────────────────
-- 5. 充值记录
-- ──────────────────────────────────────────────────────
CREATE TABLE charges (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    amount_cents INTEGER NOT NULL,
    currency TEXT NOT NULL DEFAULT 'usd',
    stripe_payment_intent_id TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    failure_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    succeeded_at TIMESTAMPTZ,
    CONSTRAINT charges_amount_positive CHECK (amount_cents > 0),
    CONSTRAINT charges_status_valid CHECK (status IN ('pending', 'succeeded', 'failed', 'cancelled')),
    CONSTRAINT charges_pi_unique UNIQUE (stripe_payment_intent_id)
);

CREATE INDEX idx_charges_user_time ON charges (user_id, created_at DESC);

-- ──────────────────────────────────────────────────────
-- 6. 提现记录
-- ──────────────────────────────────────────────────────
CREATE TABLE withdrawals (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    creator_id UUID NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    amount_cents INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    paid_at TIMESTAMPTZ,
    CONSTRAINT withdrawals_amount_positive CHECK (amount_cents > 0),
    CONSTRAINT withdrawals_min_amount CHECK (amount_cents >= 5000),
    CONSTRAINT withdrawals_status_valid CHECK (status IN ('pending', 'paid', 'rejected'))
);

CREATE INDEX idx_withdrawals_creator ON withdrawals (creator_id, created_at DESC);
CREATE INDEX idx_withdrawals_status ON withdrawals (status, created_at);

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
CREATE TRIGGER wallets_set_updated_at BEFORE UPDATE ON wallets
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

COMMIT;
