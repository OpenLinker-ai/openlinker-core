-- 006_skill_benchmark.up.sql
-- 模块 B：Skill Benchmark
-- 关联 docs/25-page-flow-backend-slices.md §5
--
-- 三张表：
--   skill_test_cases             — 平台维护的测试用例（每个 skill 3-5 条）
--   agent_skill_benchmark_runs   — 单次执行记录（agent × skill × case → score）
--   agent_skill_scores           — 聚合（agent × skill → verified/pending/failed + 平均分）

BEGIN;

-- ──────────────────────────────────────────────────────
-- 17. skill_test_cases：平台维护测试用例
-- ──────────────────────────────────────────────────────
-- input_json 是喂给 Agent endpoint 的输入；
-- judge_prompt 是 LLM-as-judge 评分时的 user prompt 模板（{output} 占位由 service 替换）；
-- 暂不存"标准答案"——评分完全由 judge 决定。
CREATE TABLE skill_test_cases (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    skill_id TEXT NOT NULL REFERENCES skills(id) ON DELETE CASCADE,
    title TEXT NOT NULL,
    input_json JSONB NOT NULL,
    judge_prompt TEXT NOT NULL,
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT skill_test_cases_title_len CHECK (char_length(title) BETWEEN 1 AND 200)
);

CREATE INDEX idx_skill_test_cases_skill ON skill_test_cases (skill_id, sort_order);

-- ──────────────────────────────────────────────────────
-- 18. agent_skill_benchmark_runs：单条执行记录
-- ──────────────────────────────────────────────────────
-- status: 'pending' / 'success' / 'failed'
--   failed = endpoint 调用 / judge 任一环节出错（score 为 NULL）
--   success = endpoint 返回 + judge 给出 0-100 分（score 非空）
-- batch_id 把同一次"跑某 skill"的多条 case 关联起来，方便 UI 聚合。
CREATE TABLE agent_skill_benchmark_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id UUID NOT NULL,
    agent_id UUID NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    skill_id TEXT NOT NULL REFERENCES skills(id) ON DELETE CASCADE,
    test_case_id UUID NOT NULL REFERENCES skill_test_cases(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'pending',
    score INTEGER,
    raw_output JSONB,
    judge_reasoning TEXT,
    error_message TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    CONSTRAINT agent_skill_benchmark_status_valid
        CHECK (status IN ('pending', 'success', 'failed')),
    CONSTRAINT agent_skill_benchmark_score_range
        CHECK (score IS NULL OR (score >= 0 AND score <= 100))
);

CREATE INDEX idx_benchmark_runs_agent_skill
    ON agent_skill_benchmark_runs (agent_id, skill_id, started_at DESC);
CREATE INDEX idx_benchmark_runs_batch
    ON agent_skill_benchmark_runs (batch_id);

-- ──────────────────────────────────────────────────────
-- 19. agent_skill_scores：聚合视图
-- ──────────────────────────────────────────────────────
-- status: 'pending' / 'verified' / 'failed'
--   verified = 最近一批 benchmark 平均分 >= verified_threshold (默认 75)
--   failed = 跑完了但平均分不达标，或所有 case 都执行失败
--   pending = 还没跑过，或正在跑
CREATE TABLE agent_skill_scores (
    agent_id UUID NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    skill_id TEXT NOT NULL REFERENCES skills(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'pending',
    average_score INTEGER,
    pass_count INTEGER NOT NULL DEFAULT 0,
    total_count INTEGER NOT NULL DEFAULT 0,
    last_batch_id UUID,
    verified_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (agent_id, skill_id),
    CONSTRAINT agent_skill_scores_status_valid
        CHECK (status IN ('pending', 'verified', 'failed')),
    CONSTRAINT agent_skill_scores_avg_range
        CHECK (average_score IS NULL OR (average_score >= 0 AND average_score <= 100))
);

CREATE INDEX idx_agent_skill_scores_skill
    ON agent_skill_scores (skill_id, status, average_score DESC);

-- ──────────────────────────────────────────────────────
-- 20. seed：5 个高价值 skill 的测试用例（每个 3 条）
-- ──────────────────────────────────────────────────────
-- 仅 seed 5 个 skill，其余 25 个先空着；创作者跑这些 skill 才能拿 verified。
-- judge_prompt 模板里 {output} 由 service 层用实际 Agent 输出替换。
INSERT INTO skill_test_cases (skill_id, title, input_json, judge_prompt, sort_order) VALUES
    -- content/translation
    ('content/translation', '中译英 · 商务邮件',
     '{"query":"请把下面这段商务邮件翻成英文，保持正式语气：\n您好，我们已收到贵司的报价单，正在内部评估，预计本周内给出答复。"}'::jsonb,
     '请评估下面的英文翻译是否准确、自然、符合商务邮件语境。给 0-100 分（90+ 母语级；75-89 准确达意；60-74 基本正确有小瑕疵；< 60 有明显错误）。\n\n译文：\n{output}\n\n输出 JSON：{"score": 数字, "reason": "一句话点评"}',
     1),
    ('content/translation', '英译中 · 技术文档',
     '{"query":"Translate the following to Chinese: A Goroutine is a lightweight thread of execution managed by the Go runtime, multiplexed onto multiple OS threads."}'::jsonb,
     '请评估下面的中文翻译是否准确传达原文技术含义且术语得当。给 0-100 分。\n\n译文：\n{output}\n\n输出 JSON：{"score": 数字, "reason": "一句话点评"}',
     2),
    ('content/translation', '多语言混合 · 保留专有名词',
     '{"query":"Translate to Chinese, keep brand names in English: Apple released the new MacBook Pro with M5 chip yesterday, alongside iOS 19 beta."}'::jsonb,
     '请评估翻译是否准确，是否按要求保留品牌名英文。给 0-100 分。\n\n译文：\n{output}\n\n输出 JSON：{"score": 数字, "reason": "一句话点评"}',
     3),

    -- content/summarization
    ('content/summarization', '长文压缩 · 新闻',
     '{"query":"用 2-3 句话总结这段新闻：\n本周三，美联储宣布将基准利率维持在 5.25%-5.50% 不变，这是连续第 6 次按兵不动。鲍威尔在新闻发布会上表示，通胀降温的进展放缓，距离 2% 目标仍有距离；同时强调当前数据不支持立即降息。市场反应方面，10 年期美债收益率上行 8bp 至 4.6%，标普 500 收跌 0.4%。"}'::jsonb,
     '请评估摘要是否准确、信息完整、长度适当（2-3 句）。给 0-100 分。\n\n摘要：\n{output}\n\n输出 JSON：{"score": 数字, "reason": "一句话点评"}',
     1),
    ('content/summarization', '要点提取 · 会议纪要',
     '{"query":"从下面对话提取 3-5 条决议要点（项目符号格式）：\nA：下周二的 demo 我们决定用方案 B，理由是稳定性更好。\nB：好，那我去对接客户确认时间。\nA：另外预算超了 15%，需要找 CFO 走特批。\nC：我去联系 CFO 王总，目标周五前拿到批复。\nB：UI 改稿明天给到设计组。"}'::jsonb,
     '请评估要点是否覆盖关键决议、责任人清晰、格式规范。给 0-100 分。\n\n要点：\n{output}\n\n输出 JSON：{"score": 数字, "reason": "一句话点评"}',
     2),
    ('content/summarization', '一句话标题',
     '{"query":"给下面的产品介绍写一句不超过 20 字的标题：\nOpenLinker 是一个让 AI Agent 互联互通的市场和协议层。开发者可以把自己写的 Agent 注册上来公开售卖，调用方既能通过网页直接试用，也能通过 MCP 让 Claude/Cursor 等工具一键接入。"}'::jsonb,
     '请评估标题是否抓住核心卖点、是否在 20 字内。给 0-100 分。\n\n标题：\n{output}\n\n输出 JSON：{"score": 数字, "reason": "一句话点评"}',
     3),

    -- dev/code-review
    ('dev/code-review', '审查 SQL 注入风险',
     '{"query":"请审查这段 Go 代码并指出问题：\nfunc getUser(db *sql.DB, name string) (*User, error) {\n    q := fmt.Sprintf(\"SELECT * FROM users WHERE name = ''%s''\", name)\n    row := db.QueryRow(q)\n    var u User\n    if err := row.Scan(&u.ID, &u.Name); err != nil {\n        return nil, err\n    }\n    return &u, nil\n}"}'::jsonb,
     '请评估审查是否准确指出 SQL 注入问题并给出修复建议（参数化查询）。给 0-100 分。\n\n审查意见：\n{output}\n\n输出 JSON：{"score": 数字, "reason": "一句话点评"}',
     1),
    ('dev/code-review', '审查并发风险',
     '{"query":"审查下面代码：\nvar counter int\nfunc inc() { counter++ }\nfunc main() {\n    var wg sync.WaitGroup\n    for i := 0; i < 1000; i++ {\n        wg.Add(1)\n        go func() { defer wg.Done(); inc() }()\n    }\n    wg.Wait()\n    fmt.Println(counter)\n}"}'::jsonb,
     '请评估是否准确指出 data race 并给出修复（mutex / atomic）。给 0-100 分。\n\n审查意见：\n{output}\n\n输出 JSON：{"score": 数字, "reason": "一句话点评"}',
     2),
    ('dev/code-review', '审查错误处理',
     '{"query":"审查：\nfunc fetch(url string) string {\n    resp, _ := http.Get(url)\n    body, _ := io.ReadAll(resp.Body)\n    return string(body)\n}"}'::jsonb,
     '请评估是否指出忽略 error、未关闭 Body、未检查 nil resp 等问题。给 0-100 分。\n\n审查意见：\n{output}\n\n输出 JSON：{"score": 数字, "reason": "一句话点评"}',
     3),

    -- data/sql-query
    ('data/sql-query', '自然语言转 SQL · 简单聚合',
     '{"query":"表 orders(id, user_id, amount_cents, created_at)。请写 SQL 查询本月总销售额（cents），按 user_id 分组，取 top 10。"}'::jsonb,
     '请评估 SQL 是否正确：日期过滤、SUM 聚合、GROUP BY、ORDER BY DESC LIMIT 10。给 0-100 分。\n\nSQL：\n{output}\n\n输出 JSON：{"score": 数字, "reason": "一句话点评"}',
     1),
    ('data/sql-query', '解读 schema',
     '{"query":"解读这段建表：\nCREATE TABLE runs (\n  id UUID PRIMARY KEY,\n  user_id UUID REFERENCES users(id),\n  agent_id UUID REFERENCES agents(id),\n  status TEXT CHECK (status IN (''running'',''success'',''failed'')),\n  cost_cents INT,\n  created_at TIMESTAMPTZ DEFAULT NOW()\n);"}'::jsonb,
     '请评估解读是否覆盖主键、外键、status 取值、计费字段含义。给 0-100 分。\n\n解读：\n{output}\n\n输出 JSON：{"score": 数字, "reason": "一句话点评"}',
     2),
    ('data/sql-query', '慢查询优化建议',
     '{"query":"下面查询很慢，给出 2-3 条优化建议：\nSELECT a.name, COUNT(r.id) FROM agents a\nLEFT JOIN runs r ON r.agent_id = a.id\nWHERE r.created_at > NOW() - INTERVAL ''30 days''\nGROUP BY a.name ORDER BY COUNT(r.id) DESC;"}'::jsonb,
     '请评估优化建议（索引、JOIN 顺序、分区、谓词下推等）是否切中要害。给 0-100 分。\n\n建议：\n{output}\n\n输出 JSON：{"score": 数字, "reason": "一句话点评"}',
     3),

    -- ops/email-process
    ('ops/email-process', '邮件分类',
     '{"query":"把下面邮件归到一个类别（销售线索/客户支持/账单/垃圾邮件/其他）并给一句理由：\n主题：关于贵司 SaaS 产品的 API 限流策略\n正文：你好，我司在评估接入贵司 API，想咨询免费版每分钟调用上限，以及付费后是否能突破。"}'::jsonb,
     '请评估分类是否准确（应为"销售线索"或"客户支持"）。给 0-100 分。\n\n输出：\n{output}\n\n输出 JSON：{"score": 数字, "reason": "一句话点评"}',
     1),
    ('ops/email-process', '邮件抽取关键信息',
     '{"query":"从邮件中抽取：发件人、发票号、金额、币种，以 JSON 返回：\nFrom: billing@aws.com\nDear customer, your invoice INV-2026-04-887 totaling $1,234.56 USD is now available."}'::jsonb,
     '请评估抽取的 JSON 字段是否完整准确（4 个字段）。给 0-100 分。\n\n输出：\n{output}\n\n输出 JSON：{"score": 数字, "reason": "一句话点评"}',
     2),
    ('ops/email-process', '邮件自动回复',
     '{"query":"客户发来：贵司产品 X 是否支持 SSO?\n请生成一段简短礼貌的自动回复（中文，3-5 句）。"}'::jsonb,
     '请评估回复是否礼貌、信息明确（哪怕说不确定也要给后续路径）。给 0-100 分。\n\n回复：\n{output}\n\n输出 JSON：{"score": 数字, "reason": "一句话点评"}',
     3);

COMMIT;
