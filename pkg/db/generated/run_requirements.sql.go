// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/run_requirements.sql）。

package db

import (
	"context"

	"github.com/google/uuid"
)

func scanRunRequirementEvidence(row interface {
	Scan(dest ...any) error
}, r *RunRequirementEvidence) error {
	return row.Scan(
		&r.RunID,
		&r.TaskID,
		&r.AgentID,
		&r.UserID,
		&r.RequiredSkillIDs,
		&r.RequiredMCPTools,
		&r.AgentSkillIDs,
		&r.MatchedSkillIDs,
		&r.MissingSkillIDs,
		&r.UsedMCPTools,
		&r.MissingMCPTools,
		&r.CoverageStatus,
		&r.EvidenceSource,
		&r.CreatedAt,
	)
}

const createRunRequirementEvidence = `-- name: CreateRunRequirementEvidence :one
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
          coverage_status, evidence_source, created_at`

type CreateRunRequirementEvidenceParams struct {
	RunID            uuid.UUID `db:"run_id" json:"run_id"`
	TaskID           uuid.UUID `db:"task_id" json:"task_id"`
	AgentID          uuid.UUID `db:"agent_id" json:"agent_id"`
	UserID           uuid.UUID `db:"user_id" json:"user_id"`
	RequiredSkillIDs []string  `db:"required_skill_ids" json:"required_skill_ids"`
	RequiredMCPTools []string  `db:"required_mcp_tools" json:"required_mcp_tools"`
	AgentSkillIDs    []string  `db:"agent_skill_ids" json:"agent_skill_ids"`
	MatchedSkillIDs  []string  `db:"matched_skill_ids" json:"matched_skill_ids"`
	MissingSkillIDs  []string  `db:"missing_skill_ids" json:"missing_skill_ids"`
	UsedMCPTools     []string  `db:"used_mcp_tools" json:"used_mcp_tools"`
	MissingMCPTools  []string  `db:"missing_mcp_tools" json:"missing_mcp_tools"`
	CoverageStatus   string    `db:"coverage_status" json:"coverage_status"`
	EvidenceSource   string    `db:"evidence_source" json:"evidence_source"`
}

func (q *Queries) CreateRunRequirementEvidence(ctx context.Context, arg CreateRunRequirementEvidenceParams) (RunRequirementEvidence, error) {
	row := q.db.QueryRow(ctx, createRunRequirementEvidence,
		arg.RunID,
		arg.TaskID,
		arg.AgentID,
		arg.UserID,
		arg.RequiredSkillIDs,
		arg.RequiredMCPTools,
		arg.AgentSkillIDs,
		arg.MatchedSkillIDs,
		arg.MissingSkillIDs,
		arg.UsedMCPTools,
		arg.MissingMCPTools,
		arg.CoverageStatus,
		arg.EvidenceSource,
	)
	var evidence RunRequirementEvidence
	err := scanRunRequirementEvidence(row, &evidence)
	return evidence, err
}

const getRunRequirementEvidenceByRun = `-- name: GetRunRequirementEvidenceByRun :one
SELECT run_id, task_id, agent_id, user_id,
       required_skill_ids, required_mcp_tools,
       agent_skill_ids, matched_skill_ids, missing_skill_ids,
       used_mcp_tools, missing_mcp_tools,
       coverage_status, evidence_source, created_at
FROM run_requirement_evidence
WHERE run_id = $1`

func (q *Queries) GetRunRequirementEvidenceByRun(ctx context.Context, runID uuid.UUID) (RunRequirementEvidence, error) {
	row := q.db.QueryRow(ctx, getRunRequirementEvidenceByRun, runID)
	var evidence RunRequirementEvidence
	err := scanRunRequirementEvidence(row, &evidence)
	return evidence, err
}
