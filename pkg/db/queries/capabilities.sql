-- capabilities.sql
--
-- 模块 A（Phase 2 §4）：Agent capabilities + examples + dry-run。
-- 表结构见 migrations/086_current_schema_init.up.sql。
--
-- 设计要点：
--   - capabilities 与 agent 1:1（unique agent_id）。Upsert 时 version += 1。
--   - examples 多条，sort_order 升序排列；删除限定 owner。
--   - onboarding_status 与 agent 1:1。新建 agent 时由 agent service 显式 ensure 行存在；
--     这里只 UPDATE，不在每条 query 里 INSERT-ON-CONFLICT，避免行尺寸放大。
--   - 所有"创作者写"query 都带 creator_id 校验，越权返回 0 行 / NoRows。
--   - 公开读查询（详情页用）由 agent slug 走 join，不暴露 owner 字段。

-- ──────────────────────────────────────────────────────
-- agent_capabilities
-- ──────────────────────────────────────────────────────

-- name: UpsertAgentCapability :one
-- 创作者首次声明 / 后续更新能力声明。无论是新增还是更新，version 都自增。
-- 参数顺序：$1 agent_id, $2 creator_id, $3 input_schema, $4 output_schema, $5 summary。
-- 约束：creator_id 必须匹配 agents 表中的 creator_id（否则返回 NoRows）。
INSERT INTO agent_capabilities (
    agent_id, input_schema, output_schema, summary, version, published_at, updated_at
)
SELECT a.id, $3, $4, $5, 1, NOW(), NOW()
FROM agents a
WHERE a.id = $1 AND a.creator_id = $2
ON CONFLICT (agent_id) DO UPDATE
SET input_schema = EXCLUDED.input_schema,
    output_schema = EXCLUDED.output_schema,
    summary = EXCLUDED.summary,
    version = agent_capabilities.version + 1,
    published_at = NOW(),
    updated_at = NOW()
RETURNING id, agent_id, input_schema, output_schema, summary,
          version, published_at, updated_at;

-- name: GetAgentCapabilityByAgentID :one
-- 通用：按 agent_id 取（公开读、benchmark / dry-run 内部用）。
SELECT id, agent_id, input_schema, output_schema, summary,
       version, published_at, updated_at
FROM agent_capabilities
WHERE agent_id = $1;

-- name: GetAgentCapabilityBySlug :one
-- 公开详情页：按 slug 取（仅已公开 Agent 暴露）。
SELECT c.id, c.agent_id, c.input_schema, c.output_schema, c.summary,
       c.version, c.published_at, c.updated_at
FROM agent_capabilities c
JOIN agents a ON a.id = c.agent_id
WHERE a.slug = $1
  AND a.visibility IN ('public', 'unlisted')
  AND a.lifecycle_status = 'active';

-- ──────────────────────────────────────────────────────
-- agent_examples
-- ──────────────────────────────────────────────────────

-- name: CreateAgentExample :one
-- 创作者新增 example。owner 校验：JOIN agents 限定 creator_id。
-- 参数顺序：$1 agent_id, $2 creator_id, $3 title, $4 input_json, $5 expected_output_json, $6 sort_order。
INSERT INTO agent_examples (
    agent_id, title, input_json, expected_output_json, sort_order
)
SELECT a.id, $3, $4, $5, $6
FROM agents a
WHERE a.id = $1 AND a.creator_id = $2
RETURNING id, agent_id, title, input_json, expected_output_json,
          sort_order, created_at, updated_at;

-- name: ListAgentExamplesByAgentID :many
-- 按 agent_id 列出示例，按 sort_order 升序、created_at 升序。
SELECT id, agent_id, title, input_json, expected_output_json,
       sort_order, created_at, updated_at
FROM agent_examples
WHERE agent_id = $1
ORDER BY sort_order, created_at;

-- name: ListAgentExamplesBySlug :many
-- 公开详情页：按 slug 列出（仅已公开 Agent）。
SELECT e.id, e.agent_id, e.title, e.input_json, e.expected_output_json,
       e.sort_order, e.created_at, e.updated_at
FROM agent_examples e
JOIN agents a ON a.id = e.agent_id
WHERE a.slug = $1
  AND a.visibility IN ('public', 'unlisted')
  AND a.lifecycle_status = 'active'
ORDER BY e.sort_order, e.created_at;

-- name: DeleteAgentExampleForOwner :execrows
-- 创作者删除 example。需 join agents 校验 owner，返回受影响行数（0 表示越权）。
DELETE FROM agent_examples e
USING agents a
WHERE e.id = $1
  AND e.agent_id = $2
  AND e.agent_id = a.id
  AND a.creator_id = $3;

-- name: CountAgentExamplesByAgentID :one
-- examples 数量；service 层判断是否需要把 onboarding.examples_set 切回 FALSE。
SELECT COUNT(*)::int FROM agent_examples WHERE agent_id = $1;

-- name: GetFirstExampleInputByAgentID :one
-- dry-run / benchmark 取第一条 example 的 input。
SELECT input_json
FROM agent_examples
WHERE agent_id = $1
ORDER BY sort_order, created_at
LIMIT 1;

-- ──────────────────────────────────────────────────────
-- agent_onboarding_status
-- ──────────────────────────────────────────────────────

-- name: EnsureOnboardingStatus :exec
-- agent.CreateAgent 之后立刻调用，确保 status 行存在；endpoint_set 默认置 TRUE。
INSERT INTO agent_onboarding_status (agent_id, endpoint_set)
VALUES ($1, TRUE)
ON CONFLICT (agent_id) DO NOTHING;

-- name: GetOnboardingStatusForOwner :one
-- 创作者查询接入完成度（用 owner 校验，越权返回 NoRows）。
SELECT s.agent_id, s.endpoint_set, s.capabilities_set, s.examples_set,
       s.dry_run_passed, s.dry_run_last_result, s.dry_run_error, s.dry_run_at,
       s.updated_at
FROM agent_onboarding_status s
JOIN agents a ON a.id = s.agent_id
WHERE s.agent_id = $1 AND a.creator_id = $2;

-- name: MarkCapabilitiesSet :execrows
-- UpsertCapability 成功后调用。
UPDATE agent_onboarding_status
SET capabilities_set = TRUE, updated_at = NOW()
WHERE agent_id = $1;

-- name: MarkExamplesSet :execrows
-- 第一条 example 创建后 examples_set=TRUE；删除最后一条时由 service 层切回 FALSE。
-- 参数：$1 agent_id, $2 examples_set。
UPDATE agent_onboarding_status
SET examples_set = $2, updated_at = NOW()
WHERE agent_id = $1;

-- name: UpdateDryRunResult :execrows
-- dry-run 执行后写入结果。result = 'pass' 时同时把 dry_run_passed 置 TRUE。
-- 参数：$1 agent_id, $2 result ('pass'|'fail'), $3 error (nullable)。
UPDATE agent_onboarding_status
SET dry_run_passed = ($2 = 'pass'),
    dry_run_last_result = $2,
    dry_run_error = $3,
    dry_run_at = NOW(),
    updated_at = NOW()
WHERE agent_id = $1;
