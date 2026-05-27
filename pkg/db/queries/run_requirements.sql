-- run_requirements.sql
--
-- 任务要求与运行证据：把 task_queries 的 Skill/MCP 要求快照到 run。

-- name: CreateRunRequirementEvidence :one
INSERT INTO run_requirement_evidence (
    run_id, task_id, agent_id, user_id,
    required_skill_ids, required_mcp_tools,
    agent_skill_ids, matched_skill_ids, missing_skill_ids,
    used_mcp_tools, missing_mcp_tools,
    coverage_status, evidence_source
) VALUES (
    $1, $2, $3, $4,
    $5, $6,
    $7, $8, $9,
    $10, $11,
    $12, $13
)
RETURNING run_id, task_id, agent_id, user_id,
          required_skill_ids, required_mcp_tools,
          agent_skill_ids, matched_skill_ids, missing_skill_ids,
          used_mcp_tools, missing_mcp_tools,
          coverage_status, evidence_source, created_at;

-- name: GetRunRequirementEvidenceByRun :one
SELECT run_id, task_id, agent_id, user_id,
       required_skill_ids, required_mcp_tools,
       agent_skill_ids, matched_skill_ids, missing_skill_ids,
       used_mcp_tools, missing_mcp_tools,
       coverage_status, evidence_source, created_at
FROM run_requirement_evidence
WHERE run_id = $1;
