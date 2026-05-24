-- 005_agent_capabilities.up.sql
-- 模块 A：Agent capabilities + examples + dry-run
-- 关联 docs/25-page-flow-backend-slices.md §4

BEGIN;

-- ──────────────────────────────────────────────────────
-- 14. agent_capabilities：当前版本的能力声明（input / output JSON Schema）
-- ──────────────────────────────────────────────────────
-- 与 agents 1:1（unique agent_id）。version 用于审计；publish 时递增。
CREATE TABLE agent_capabilities (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id UUID NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    input_schema JSONB NOT NULL,
    output_schema JSONB NOT NULL,
    summary TEXT NOT NULL DEFAULT '',
    version INTEGER NOT NULL DEFAULT 1,
    published_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT agent_capabilities_agent_unique UNIQUE (agent_id),
    CONSTRAINT agent_capabilities_version_positive CHECK (version >= 1)
);

CREATE INDEX idx_agent_capabilities_agent ON agent_capabilities (agent_id);

-- ──────────────────────────────────────────────────────
-- 15. agent_examples：input / output 示例（detail 页展示、benchmark / dry-run 取首条）
-- ──────────────────────────────────────────────────────
CREATE TABLE agent_examples (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id UUID NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    title TEXT NOT NULL,
    input_json JSONB NOT NULL,
    expected_output_json JSONB,
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT agent_examples_title_len CHECK (char_length(title) BETWEEN 1 AND 120)
);

CREATE INDEX idx_agent_examples_agent ON agent_examples (agent_id, sort_order, created_at);

-- ──────────────────────────────────────────────────────
-- 16. agent_onboarding_status：接入完成度状态机
-- ──────────────────────────────────────────────────────
-- 与 agents 1:1。endpoint_set 由 agents 表写入后立刻置 TRUE；其余三项随能力声明 / 示例 / dry-run 推进。
-- dry_run_last_result：'pending' / 'pass' / 'fail'；fail 时 error 写入 dry_run_error。
CREATE TABLE agent_onboarding_status (
    agent_id UUID PRIMARY KEY REFERENCES agents(id) ON DELETE CASCADE,
    endpoint_set BOOLEAN NOT NULL DEFAULT FALSE,
    capabilities_set BOOLEAN NOT NULL DEFAULT FALSE,
    examples_set BOOLEAN NOT NULL DEFAULT FALSE,
    dry_run_passed BOOLEAN NOT NULL DEFAULT FALSE,
    dry_run_last_result TEXT NOT NULL DEFAULT 'pending',
    dry_run_error TEXT,
    dry_run_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT agent_onboarding_dry_run_result_valid
        CHECK (dry_run_last_result IN ('pending', 'pass', 'fail'))
);

-- ──────────────────────────────────────────────────────
-- 回填：所有现有 agents 默认 endpoint_set=TRUE（agents.endpoint_url 已是 NOT NULL）。
-- 让历史 Agent 立刻看到合理的接入进度状态。
-- ──────────────────────────────────────────────────────
INSERT INTO agent_onboarding_status (agent_id, endpoint_set)
SELECT id, TRUE FROM agents
ON CONFLICT (agent_id) DO NOTHING;

COMMIT;
