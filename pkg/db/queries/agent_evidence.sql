-- agent_evidence.sql
--
-- Agent 就绪证据随连接目标失效。证据只对它被采集时的连接目标
-- （connection_mode + endpoint_url + mcp_tool_name，及调用方式 endpoint_auth_header、
-- endpoint_auth_header_name、mcp_transport）
-- 成立；创作者改连接目标时，UpdateAgent 在同一事务内把下列派生快照恢复到
-- 未验证状态。原始历史（runs、agent_skill_benchmark_runs、告警）不删除。
--
--   callable / 可用性巡检：agent_availability_snapshots
--   dry-run / 健康检查：agent_onboarding_status.dry_run_*
--   certified：agents.certification_status / certified_at
--   verified：agent_skill_scores（Skill Benchmark 汇总）

-- name: LockAgentEvidenceTarget :one
-- 证据写入前确认 Agent 仍指向被探测的连接目标；FOR SHARE 持有到写入提交，
-- 与 UpdateAgent 的行锁互斥，连接目标变更不能插在确认与写入之间。
-- $5 = TRUE 时同时要求调用凭据、认证头名称与 MCP 传输方式未变（可用性 / dry-run 证据）；Benchmark
-- 只比较连接目标。目标已变化时返回 0 行，调用方丢弃结果。
SELECT id
FROM agents
WHERE id = $1
  AND connection_mode = $2
  AND endpoint_url = $3
  AND mcp_tool_name IS NOT DISTINCT FROM $4::text
  AND (NOT $5::boolean OR (
      endpoint_auth_header IS NOT DISTINCT FROM $6::text
      AND mcp_transport = COALESCE(NULLIF($7::text, ''), 'legacy_json')
      AND endpoint_auth_header_name IS NOT DISTINCT FROM $8::text
  ))
FOR SHARE;

-- name: LockAgentConnectionModeForEvidence :one
-- Runtime Agent 的连接目标就是它自己的 Runtime 队列（endpoint_url 只是展示值，
-- Runtime Worker 不使用 endpoint_auth_header），因此只比较 connection_mode。
SELECT id
FROM agents
WHERE id = $1 AND connection_mode = $2
FOR SHARE;

-- name: ResetAgentAvailabilityEvidence :execrows
-- 可用性快照回到 unknown，并清空 last_checked_at，让巡检下一轮立即重新探测新目标。
UPDATE agent_availability_snapshots
SET availability_status = 'unknown',
    last_successful_run_at = NULL,
    last_failed_run_at = NULL,
    last_checked_at = NULL,
    consecutive_failures = 0,
    updated_at = NOW()
WHERE agent_id = $1;

-- name: ResetAgentDryRunEvidence :execrows
-- dry-run / 健康检查结果回到 pending；capability / examples 声明保留。
UPDATE agent_onboarding_status
SET dry_run_passed = FALSE,
    dry_run_last_result = 'pending',
    dry_run_error = NULL,
    dry_run_at = NULL,
    updated_at = NOW()
WHERE agent_id = $1;

-- name: ReopenAgentCertificationAfterTargetChange :execrows
-- 已认证 Agent 改连接目标：认证回到 pending 重新进入运营审核队列，
-- certified_at 清空，徽章立即撤下。其他认证状态不变。
UPDATE agents
SET certification_status = 'pending',
    certified_at = NULL,
    rejection_reason = NULL,
    updated_at = NOW()
WHERE id = $1 AND certification_status = 'certified';

-- name: ResetAgentSkillScoresAfterTargetChange :execrows
-- Skill Benchmark 汇总回到 pending（与发起新 benchmark 时的预置状态一致），
-- verified_at 一并清空；逐用例结果仍保留在 agent_skill_benchmark_runs。
UPDATE agent_skill_scores
SET status = 'pending',
    average_score = NULL,
    pass_count = 0,
    last_batch_id = NULL,
    verified_at = NULL,
    updated_at = NOW()
WHERE agent_id = $1;
