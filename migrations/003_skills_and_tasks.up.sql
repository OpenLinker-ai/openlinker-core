-- 003_skills_and_tasks.up.sql
-- 子轮 2.3 + 2.4：Skill 注册表 + 任务驱动 A 形态
-- 关联 docs/09-mvp-development-roadmap.md 章三

BEGIN;

-- ──────────────────────────────────────────────────────
-- 10. skills 注册表（平台维护，30 个核心 skill）
-- ──────────────────────────────────────────────────────
CREATE TABLE skills (
    id TEXT PRIMARY KEY,                    -- "content/translation"、"dev/code-review" 等
    category TEXT NOT NULL,                 -- "content" / "dev" / "data" / "media" / "ops" / "ai"
    name TEXT NOT NULL,                     -- 中文名："翻译"、"代码审查"
    description TEXT NOT NULL,              -- 一句话描述（用于 LLM 解析时的语义匹配）
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT skills_id_format CHECK (id ~ '^[a-z]+/[a-z0-9-]+$')
);

CREATE INDEX idx_skills_category ON skills (category, sort_order);

-- ──────────────────────────────────────────────────────
-- 11. agent_skills 关联（Agent ↔ Skills，N:M）
-- ──────────────────────────────────────────────────────
CREATE TABLE agent_skills (
    agent_id UUID NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    skill_id TEXT NOT NULL REFERENCES skills(id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (agent_id, skill_id)
);

CREATE INDEX idx_agent_skills_skill ON agent_skills (skill_id, agent_id);

-- ──────────────────────────────────────────────────────
-- 12. task_queries（任务驱动 A 形态：自然语言 → 推荐）
-- ──────────────────────────────────────────────────────
CREATE TABLE task_queries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    query TEXT NOT NULL,                    -- 用户原始描述
    parsed_skills TEXT[] NOT NULL DEFAULT '{}',  -- LLM 解析出的 skill_id 列表
    recommended_agent_ids UUID[] NOT NULL DEFAULT '{}',  -- 推荐的 Agent 顺序
    chosen_agent_id UUID REFERENCES agents(id) ON DELETE SET NULL,  -- 用户最终选了哪个
    chosen_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT task_queries_query_len CHECK (char_length(query) BETWEEN 4 AND 500)
);

CREATE INDEX idx_task_queries_user ON task_queries (user_id, created_at DESC);

-- ──────────────────────────────────────────────────────
-- 13. 内置 30 个 skill（6 分类 × 5 个）
-- ──────────────────────────────────────────────────────
INSERT INTO skills (id, category, name, description, sort_order) VALUES
    -- content（内容创作 / 文本处理）
    ('content/translation',     'content', '翻译',          '中英互译、多语言翻译、术语库本地化',                   1),
    ('content/summarization',   'content', '摘要',          '长文压缩、要点提取、会议纪要生成',                     2),
    ('content/copywriting',     'content', '文案',          '营销文案、社媒推文、产品描述、广告创意',               3),
    ('content/proofreading',    'content', '校对润色',      '错别字、语法、风格统一、SEO 优化',                     4),
    ('content/structured-data', 'content', '结构化抽取',    '从非结构化文本中抽取人名/地名/日期/金额/字段',         5),

    -- dev（研发工程）
    ('dev/code-review',         'dev',     '代码审查',      'PR 评审、风格检查、潜在 bug 提示',                     1),
    ('dev/code-generation',     'dev',     '代码生成',      '按 spec 生成函数 / 测试 / 脚手架',                     2),
    ('dev/code-explanation',    'dev',     '代码解释',      '解读 legacy 代码、生成文档、画时序图',                 3),
    ('dev/test-generation',     'dev',     '测试生成',      '单测 / 集成测试 / mock 数据生成',                      4),
    ('dev/devops-ci',           'dev',     'CI/CD',         'GitHub Actions、流水线、部署脚本',                     5),

    -- data（数据 / 分析）
    ('data/sql-query',          'data',    'SQL 查询',      '自然语言转 SQL、慢查询优化、schema 解读',              1),
    ('data/data-cleaning',      'data',    '数据清洗',      '去重 / 补全 / 类型转换 / 异常值检测',                  2),
    ('data/analysis',           'data',    '数据分析',      '统计 / 趋势 / 同比环比、生成洞察文字',                 3),
    ('data/visualization',      'data',    '可视化',        '生成 chart 配置 / 仪表盘 spec / Mermaid 图',           4),
    ('data/forecasting',        'data',    '预测',          '时序预测、销量 / 流量 / 库存预估',                     5),

    -- media（图像 / 音视频）
    ('media/image-generate',    'media',   '图像生成',      '文生图、产品图、海报、avatar',                         1),
    ('media/image-edit',        'media',   '图像编辑',      '抠图 / 换背景 / 修复 / 风格化 / 超分',                 2),
    ('media/audio-transcribe',  'media',   '语音转写',      '中英多语言 ASR、字幕生成、说话人分离',                 3),
    ('media/audio-generate',    'media',   '语音合成',      '多角色 TTS、克隆音色、语调 / 情绪控制',                4),
    ('media/video-process',     'media',   '视频处理',      '剪辑 / 字幕 / 搬运 / 切片 / 摘要',                     5),

    -- ops（业务流程 / 运维）
    ('ops/document-generate',   'ops',     '文档生成',      'PDF / Word / 报告 / 合同 / 简历',                      1),
    ('ops/email-process',       'ops',     '邮件处理',      '分类 / 自动回复 / 内容抽取 / 抄送决策',                2),
    ('ops/scheduling',          'ops',     '日程安排',      '会议时间协调、提醒、跨时区',                           3),
    ('ops/web-scraping',        'ops',     '网页抓取',      '抓取站点 / API / 监控 / 价格追踪',                     4),
    ('ops/notification',        'ops',     '通知推送',      '微信 / Slack / 邮件 / 短信投递',                       5),

    -- ai（AI 工程 / 多 Agent）
    ('ai/rag',                  'ai',      'RAG 检索',      '知识库问答、语义检索、文档级 QA',                      1),
    ('ai/agent-orchestration',  'ai',      'Agent 编排',    '多步 Agent 协作 / 工具链调度',                         2),
    ('ai/finetune',             'ai',      '模型微调',      'LoRA / SFT / 数据准备 / 评测',                         3),
    ('ai/prompt-engineering',   'ai',      'Prompt 工程',   '迭代 prompt、A/B、few-shot 设计',                      4),
    ('ai/safety-eval',          'ai',      '安全评测',      '幻觉检测、jailbreak 测试、合规审查',                   5);

COMMIT;
