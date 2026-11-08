// Hand-maintained SQL query implementation.
// sqlc comparison output is isolated under .sqlc/generated; review it manually before any migration of this runtime package.
//
// Agent 就绪证据随连接目标失效（见 pkg/db/queries/agent_evidence.sql）。

package db

import (
	"context"

	"github.com/google/uuid"
)

const lockAgentEvidenceTarget = `-- name: LockAgentEvidenceTarget :one
SELECT id
FROM agents
WHERE id = $1
  AND connection_mode = $2
  AND endpoint_url = $3
  AND mcp_tool_name IS NOT DISTINCT FROM $4::text
  AND (NOT $5::boolean OR endpoint_auth_header IS NOT DISTINCT FROM $6::text)
FOR SHARE`

type LockAgentEvidenceTargetParams struct {
	ID                 uuid.UUID `db:"id" json:"id"`
	ConnectionMode     string    `db:"connection_mode" json:"connection_mode"`
	EndpointURL        string    `db:"endpoint_url" json:"endpoint_url"`
	MCPToolName        *string   `db:"mcp_tool_name" json:"mcp_tool_name"`
	CompareAuthHeader  bool      `db:"compare_auth_header" json:"compare_auth_header"`
	EndpointAuthHeader *string   `db:"endpoint_auth_header" json:"endpoint_auth_header"`
}

// LockAgentEvidenceTarget returns pgx.ErrNoRows when the Agent no longer
// points at the probed connection target. It must run inside the transaction
// that writes the dependent evidence.
func (q *Queries) LockAgentEvidenceTarget(ctx context.Context, arg LockAgentEvidenceTargetParams) (uuid.UUID, error) {
	row := q.db.QueryRow(ctx, lockAgentEvidenceTarget,
		arg.ID,
		arg.ConnectionMode,
		arg.EndpointURL,
		arg.MCPToolName,
		arg.CompareAuthHeader,
		arg.EndpointAuthHeader,
	)
	var id uuid.UUID
	err := row.Scan(&id)
	return id, err
}

const resetAgentAvailabilityEvidence = `-- name: ResetAgentAvailabilityEvidence :execrows
UPDATE agent_availability_snapshots
SET availability_status = 'unknown',
    last_successful_run_at = NULL,
    last_failed_run_at = NULL,
    last_checked_at = NULL,
    consecutive_failures = 0,
    updated_at = NOW()
WHERE agent_id = $1`

func (q *Queries) ResetAgentAvailabilityEvidence(ctx context.Context, agentID uuid.UUID) (int64, error) {
	tag, err := q.db.Exec(ctx, resetAgentAvailabilityEvidence, agentID)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

const resetAgentDryRunEvidence = `-- name: ResetAgentDryRunEvidence :execrows
UPDATE agent_onboarding_status
SET dry_run_passed = FALSE,
    dry_run_last_result = 'pending',
    dry_run_error = NULL,
    dry_run_at = NULL,
    updated_at = NOW()
WHERE agent_id = $1`

func (q *Queries) ResetAgentDryRunEvidence(ctx context.Context, agentID uuid.UUID) (int64, error) {
	tag, err := q.db.Exec(ctx, resetAgentDryRunEvidence, agentID)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

const reopenAgentCertificationAfterTargetChange = `-- name: ReopenAgentCertificationAfterTargetChange :execrows
UPDATE agents
SET certification_status = 'pending',
    certified_at = NULL,
    rejection_reason = NULL,
    updated_at = NOW()
WHERE id = $1 AND certification_status = 'certified'`

func (q *Queries) ReopenAgentCertificationAfterTargetChange(ctx context.Context, agentID uuid.UUID) (int64, error) {
	tag, err := q.db.Exec(ctx, reopenAgentCertificationAfterTargetChange, agentID)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

const resetAgentSkillScoresAfterTargetChange = `-- name: ResetAgentSkillScoresAfterTargetChange :execrows
UPDATE agent_skill_scores
SET status = 'pending',
    average_score = NULL,
    pass_count = 0,
    last_batch_id = NULL,
    verified_at = NULL,
    updated_at = NOW()
WHERE agent_id = $1`

func (q *Queries) ResetAgentSkillScoresAfterTargetChange(ctx context.Context, agentID uuid.UUID) (int64, error) {
	tag, err := q.db.Exec(ctx, resetAgentSkillScoresAfterTargetChange, agentID)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
