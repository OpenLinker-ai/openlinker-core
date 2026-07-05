-- 056_skill_proposals.up.sql
-- 用户提交缺失 Skill / 导入 Agent Skill 声明后的平台审核提案。

BEGIN;

CREATE TABLE skill_proposals (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    agent_id UUID REFERENCES agents(id) ON DELETE SET NULL,
    proposed_skill_id TEXT NOT NULL,
    category TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'manual',
    status TEXT NOT NULL DEFAULT 'pending',
    matched_skill_id TEXT REFERENCES skills(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT skill_proposals_owner_skill_unique UNIQUE (owner_user_id, proposed_skill_id),
    CONSTRAINT skill_proposals_id_format CHECK (proposed_skill_id ~ '^[a-z][a-z0-9]*(?:[/_-][a-z0-9]+)*$'),
    CONSTRAINT skill_proposals_category_format CHECK (category ~ '^[a-z][a-z0-9_-]{1,79}$'),
    CONSTRAINT skill_proposals_name_len CHECK (char_length(name) BETWEEN 1 AND 120),
    CONSTRAINT skill_proposals_description_len CHECK (char_length(description) BETWEEN 4 AND 1000),
    CONSTRAINT skill_proposals_source_valid CHECK (source IN ('manual', 'imported_text', 'imported_json')),
    CONSTRAINT skill_proposals_status_valid CHECK (status IN ('pending', 'merged', 'rejected'))
);

CREATE INDEX idx_skill_proposals_owner ON skill_proposals (owner_user_id, updated_at DESC);
CREATE INDEX idx_skill_proposals_status ON skill_proposals (status, updated_at DESC);
CREATE INDEX idx_skill_proposals_matched_skill ON skill_proposals (matched_skill_id);

CREATE TRIGGER skill_proposals_set_updated_at BEFORE UPDATE ON skill_proposals
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

COMMIT;
