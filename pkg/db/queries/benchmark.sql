-- benchmark.sql
--
-- 模块 B（Phase 2 §5）：Skill Benchmark
-- 表结构见 migrations/086_current_schema_init.up.sql。
--
-- 设计要点：
--   - 创作者触发：只允许 agent_skills 已声明的 skill；service 层做关联校验。
--   - 一次 benchmark 一个 batch_id；多条 case 共享 batch，便于 UI 聚合。
--   - score 写入后由 service 层按 verified_threshold (75) 汇总到 agent_skill_scores。
--   - 公开读：详情页 / 列表页只看 agent_skill_scores（不暴露 raw_output / judge_reasoning）。

-- ──────────────────────────────────────────────────────
-- skill_test_cases
-- ──────────────────────────────────────────────────────

-- name: ListTestCasesBySkill :many
-- benchmark 执行时按 skill 取全部测试用例。
SELECT id, skill_id, title, input_json, judge_prompt, sort_order, created_at
FROM skill_test_cases
WHERE skill_id = $1
ORDER BY sort_order, created_at;

-- name: CountTestCasesBySkill :one
-- 校验某 skill 是否已 seed 测试用例（0 表示该 skill 暂不支持 benchmark）。
SELECT COUNT(*)::int FROM skill_test_cases WHERE skill_id = $1;

-- ──────────────────────────────────────────────────────
-- agent_skill_benchmark_runs
-- ──────────────────────────────────────────────────────

-- name: CreateBenchmarkRun :one
-- 单条 case 执行前插入 pending 行；service 层在 endpoint + judge 后 update。
-- 参数：$1 batch_id, $2 agent_id, $3 skill_id, $4 test_case_id。
INSERT INTO agent_skill_benchmark_runs (
    batch_id, agent_id, skill_id, test_case_id, status, started_at
) VALUES ($1, $2, $3, $4, 'pending', NOW())
RETURNING id, batch_id, agent_id, skill_id, test_case_id, status,
          score, raw_output, judge_reasoning, error_message,
          started_at, finished_at;

-- name: MarkBenchmarkRunSuccess :execrows
-- judge 成功打分后写入 score + raw_output + 推理。
-- 参数：$1 id, $2 score, $3 raw_output, $4 judge_reasoning。
UPDATE agent_skill_benchmark_runs
SET status = 'success',
    score = $2,
    raw_output = $3,
    judge_reasoning = $4,
    finished_at = NOW()
WHERE id = $1;

-- name: MarkBenchmarkRunFailed :execrows
-- endpoint / judge 任一环节失败时写入。
-- 参数：$1 id, $2 error_message。
UPDATE agent_skill_benchmark_runs
SET status = 'failed',
    error_message = $2,
    finished_at = NOW()
WHERE id = $1;

-- name: ListBenchmarkRunsByBatch :many
-- 详情页：列出某 batch 的全部 case 结果。
SELECT r.id, r.batch_id, r.agent_id, r.skill_id, r.test_case_id, r.status,
       r.score, r.raw_output, r.judge_reasoning, r.error_message,
       r.started_at, r.finished_at,
       c.title AS test_case_title
FROM agent_skill_benchmark_runs r
JOIN skill_test_cases c ON c.id = r.test_case_id
WHERE r.batch_id = $1
ORDER BY c.sort_order, c.created_at;

-- name: ListLatestBatchesByAgent :many
-- 创作者中心：列出某 agent 最近 N 次 benchmark batch 概览（按 skill 维度聚合）。
SELECT DISTINCT ON (r.batch_id, r.skill_id)
       r.batch_id, r.skill_id, r.started_at,
       COUNT(*) FILTER (WHERE r.status = 'success') OVER (PARTITION BY r.batch_id) AS success_count,
       COUNT(*) OVER (PARTITION BY r.batch_id) AS total_count
FROM agent_skill_benchmark_runs r
WHERE r.agent_id = $1
ORDER BY r.batch_id, r.skill_id, r.started_at DESC
LIMIT $2;

-- name: ListBenchmarkBatchSummariesByAgent :many
-- 公开 GET /agents/:id/benchmarks：每个 batch 一行，含 skill_id、成功/总数、平均分（仅 success）、起止时间。
SELECT b.batch_id, b.skill_id,
       MIN(b.started_at) AS started_at,
       MAX(b.finished_at) AS finished_at,
       COUNT(*)::int AS total_count,
       COUNT(*) FILTER (WHERE b.status = 'success')::int AS success_count,
       (AVG(b.score) FILTER (WHERE b.status = 'success'))::int AS average_score
FROM agent_skill_benchmark_runs b
WHERE b.agent_id = $1
GROUP BY b.batch_id, b.skill_id
ORDER BY MIN(b.started_at) DESC
LIMIT $2;

-- name: GetAgentVerifiedSkillStats :one
-- 公开市场详情页用：某 Agent 的 verified skill 数 + 最近一次 batch_id。
SELECT
    COUNT(*) FILTER (WHERE s.status = 'verified')::int AS verified_count,
    (SELECT latest.last_batch_id FROM agent_skill_scores latest
     WHERE latest.agent_id = $1 AND latest.last_batch_id IS NOT NULL
     ORDER BY latest.updated_at DESC LIMIT 1) AS latest_batch_id
FROM agent_skill_scores s
WHERE s.agent_id = $1;

-- ──────────────────────────────────────────────────────
-- agent_skill_scores
-- ──────────────────────────────────────────────────────

-- name: UpsertAgentSkillScore :one
-- 一次 benchmark 跑完后 service 层调用。
-- 参数：$1 agent_id, $2 skill_id, $3 status, $4 average_score,
--       $5 pass_count, $6 total_count, $7 last_batch_id, $8 verified_at。
INSERT INTO agent_skill_scores (
    agent_id, skill_id, status, average_score,
    pass_count, total_count, last_batch_id, verified_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
ON CONFLICT ON CONSTRAINT agent_skill_scores_pkey DO UPDATE
SET status = EXCLUDED.status,
    average_score = EXCLUDED.average_score,
    pass_count = EXCLUDED.pass_count,
    total_count = EXCLUDED.total_count,
    last_batch_id = EXCLUDED.last_batch_id,
    verified_at = COALESCE(EXCLUDED.verified_at, agent_skill_scores.verified_at),
    updated_at = NOW()
RETURNING agent_skill_scores.agent_id, agent_skill_scores.skill_id,
          agent_skill_scores.status, agent_skill_scores.average_score,
          agent_skill_scores.pass_count, agent_skill_scores.total_count,
          agent_skill_scores.last_batch_id, agent_skill_scores.verified_at,
          agent_skill_scores.updated_at;

-- name: GetAgentSkillScore :one
-- 单条查询（service 内部用）。
SELECT agent_id, skill_id, status, average_score,
       pass_count, total_count, last_batch_id, verified_at, updated_at
FROM agent_skill_scores
WHERE agent_id = $1 AND skill_id = $2;

-- name: ListAgentSkillScoresByAgent :many
-- 创作者中心 / 公开详情页：列出某 agent 全部 skill 的评分。
SELECT s.agent_id, s.skill_id, s.status, s.average_score,
       s.pass_count, s.total_count, s.last_batch_id, s.verified_at, s.updated_at
FROM agent_skill_scores s
WHERE s.agent_id = $1
ORDER BY s.status DESC, s.average_score DESC NULLS LAST, s.skill_id;

-- name: ListAgentSkillScoresBySlug :many
-- 公开详情页：按 agent.slug 查（仅已公开 Agent）。
SELECT s.agent_id, s.skill_id, s.status, s.average_score,
       s.pass_count, s.total_count, s.last_batch_id, s.verified_at, s.updated_at
FROM agent_skill_scores s
JOIN agents a ON a.id = s.agent_id
WHERE a.slug = $1
  AND a.visibility IN ('public', 'unlisted')
  AND a.lifecycle_status = 'active'
ORDER BY s.status DESC, s.average_score DESC NULLS LAST, s.skill_id;

-- name: ListTopAgentsBySkill :many
-- /skills 列表页：某 skill 下 verified Agent 的 top-N，按平均分降序。
SELECT s.agent_id, s.average_score, s.verified_at,
       a.slug, a.name, a.description, a.tags, a.price_per_call_cents, a.total_calls
FROM agent_skill_scores s
JOIN agents a ON a.id = s.agent_id
WHERE s.skill_id = $1
  AND s.status = 'verified'
  AND a.visibility = 'public' AND a.lifecycle_status = 'active'
ORDER BY s.average_score DESC NULLS LAST, a.total_calls DESC, a.id
LIMIT $2;

-- name: ListAgentsBySkillsWithVerified :many
-- RecommendAgentsBySkills 加 verified/availability 加权。Runtime Worker 必须有
-- PostgreSQL 证明的 current-contract ready Session；周期在线性由 Redis Session
-- lease 与 Runtime reaper 收敛，已被替代的数据库 heartbeat 只用于排序。Agent Token
-- 的使用时间不是在线性证据。Direct/MCP Agent 仍需 healthy 或成功运行证据。
-- 排序：命中 skill 数 desc → 可用性 → 最近 Session/成功证据 → verified 数 desc → total_calls desc。
-- 同时返回 verified_count 让上层决定排序权重。
SELECT a.id AS agent_id,
       COUNT(DISTINCT ag.skill_id)::int AS match_count,
       COUNT(DISTINCT s.skill_id) FILTER (WHERE s.status = 'verified')::int AS verified_count,
       a.total_calls
FROM agent_skills ag
JOIN agents a ON a.id = ag.agent_id
LEFT JOIN agent_availability_snapshots av ON av.agent_id = a.id
LEFT JOIN LATERAL (
    SELECT MAX(session.heartbeat_at) AS last_runtime_session_at
    FROM runtime_sessions session
    JOIN runtime_nodes node ON node.node_id = session.node_id
    JOIN agent_tokens credential
      ON credential.id = session.credential_id
     AND credential.agent_id = session.agent_id
    JOIN runtime_wire_contracts wire
      ON wire.runtime_contract_id = session.runtime_contract_id
     AND wire.runtime_contract_digest = session.runtime_contract_digest
     AND wire.support_tier IN ('current', 'previous')
    WHERE session.agent_id = a.id
      AND session.status = 'active'
      AND session.attached_core_instance_id IS NOT NULL
      AND session.disconnected_at IS NULL
      AND session.protocol_version = 2
      AND session.runtime_contract_id = 'openlinker.runtime.v2'
      AND session.features @> ARRAY[
          'lease_fence', 'assignment_confirm', 'renew', 'resume',
          'event_ack', 'result_ack', 'cancel', 'persistent_spool'
      ]::text[]
      AND node.status = 'active'
      AND node.revoked_at IS NULL
      AND node.protocol_version = session.protocol_version
      AND node.runtime_contract_id = session.runtime_contract_id
      AND node.runtime_contract_digest = session.runtime_contract_digest
      AND node.device_certificate_serial = session.device_certificate_serial
      AND node.node_version = session.node_version
      AND node.features @> session.features
      AND session.features @> node.features
      AND credential.status = 'active_runtime'
      AND credential.revoked_at IS NULL
      AND credential.scopes @> ARRAY['agent:pull']::text[]
      AND (credential.expires_at IS NULL OR credential.expires_at > clock_timestamp())
      AND EXISTS (
          SELECT 1
          FROM runtime_session_attachments attachment
          WHERE attachment.runtime_session_id = session.runtime_session_id
            AND attachment.core_instance_id = session.attached_core_instance_id
            AND attachment.detached_at IS NULL
      )
) runtime_ready ON TRUE
LEFT JOIN agent_skill_scores s
       ON s.agent_id = ag.agent_id
      AND s.skill_id = ag.skill_id
      AND s.skill_id = ANY($1::text[])
WHERE ag.skill_id = ANY($1::text[])
  AND a.visibility = 'public' AND a.lifecycle_status = 'active'
  AND NOT EXISTS (
      SELECT 1
      FROM unnest(a.tags) AS tag
      WHERE lower(tag) IN ('internal', 'test', 'testing', 'validation')
         OR tag IN ('内部', '测试', '验收')
  )
  AND COALESCE(av.availability_status, 'unknown') <> 'unreachable'
  AND (
      (
          a.connection_mode = 'runtime'
          AND runtime_ready.last_runtime_session_at IS NOT NULL
      )
      OR (
          a.connection_mode <> 'runtime'
          AND (
              COALESCE(av.availability_status, 'unknown') = 'healthy'
              OR av.last_successful_run_at IS NOT NULL
          )
      )
  )
GROUP BY a.id, a.total_calls, av.availability_status, av.last_successful_run_at, runtime_ready.last_runtime_session_at
ORDER BY match_count DESC,
    CASE COALESCE(av.availability_status, 'unknown')
    WHEN 'healthy' THEN 0
    WHEN 'unknown' THEN 1
    WHEN 'degraded' THEN 2
    ELSE 3
END ASC,
    GREATEST(
        COALESCE(av.last_successful_run_at, TIMESTAMPTZ 'epoch'),
        COALESCE(runtime_ready.last_runtime_session_at, TIMESTAMPTZ 'epoch')
    ) DESC,
    verified_count DESC, a.total_calls DESC, a.id;
