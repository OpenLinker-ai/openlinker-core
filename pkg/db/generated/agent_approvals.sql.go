// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/agent_approvals.sql）。

package db

import (
	"context"

	"github.com/google/uuid"
	"time"
)

// scanAgentApproval 统一扫描列顺序。
func scanAgentApproval(row interface {
	Scan(dest ...any) error
}, a *AgentActionApprovalRequest) error {
	return row.Scan(
		&a.ID,
		&a.AgentID,
		&a.RequestedByUserID,
		&a.RequestedByTokenID,
		&a.Action,
		&a.PayloadJSON,
		&a.Status,
		&a.ApprovalURLSlug,
		&a.ExpiresAt,
		&a.DecidedAt,
		&a.DecidedByUserID,
		&a.DecisionNote,
		&a.CreatedAt,
	)
}

const createAgentApproval = `-- name: CreateAgentApproval :one
INSERT INTO agent_action_approval_requests (
    agent_id, requested_by_user_id, requested_by_token_id,
    action, payload_json, approval_url_slug, expires_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7
)
RETURNING id, agent_id, requested_by_user_id, requested_by_token_id,
          action, payload_json, status, approval_url_slug,
          expires_at, decided_at, decided_by_user_id, decision_note,
          created_at`

type CreateAgentApprovalParams struct {
	AgentID            uuid.UUID  `db:"agent_id" json:"agent_id"`
	RequestedByUserID  *uuid.UUID `db:"requested_by_user_id" json:"requested_by_user_id"`
	RequestedByTokenID *uuid.UUID `db:"requested_by_token_id" json:"requested_by_token_id"`
	Action             string     `db:"action" json:"action"`
	PayloadJSON        []byte     `db:"payload_json" json:"payload_json"`
	ApprovalURLSlug    string     `db:"approval_url_slug" json:"approval_url_slug"`
	ExpiresAt          time.Time  `db:"expires_at" json:"expires_at"`
}

func (q *Queries) CreateAgentApproval(ctx context.Context, arg CreateAgentApprovalParams) (AgentActionApprovalRequest, error) {
	row := q.db.QueryRow(ctx, createAgentApproval,
		arg.AgentID, arg.RequestedByUserID, arg.RequestedByTokenID,
		arg.Action, arg.PayloadJSON, arg.ApprovalURLSlug, arg.ExpiresAt)
	var a AgentActionApprovalRequest
	err := scanAgentApproval(row, &a)
	return a, err
}

const getAgentApprovalForCreator = `-- name: GetAgentApprovalForCreator :one
SELECT a.id, a.agent_id, a.requested_by_user_id, a.requested_by_token_id,
       a.action, a.payload_json, a.status, a.approval_url_slug,
       a.expires_at, a.decided_at, a.decided_by_user_id, a.decision_note,
       a.created_at
FROM agent_action_approval_requests a
JOIN agents ag ON ag.id = a.agent_id
WHERE a.id = $1 AND ag.creator_id = $2`

type GetAgentApprovalForCreatorParams struct {
	ID        uuid.UUID `db:"id" json:"id"`
	CreatorID uuid.UUID `db:"creator_id" json:"creator_id"`
}

func (q *Queries) GetAgentApprovalForCreator(ctx context.Context, arg GetAgentApprovalForCreatorParams) (AgentActionApprovalRequest, error) {
	row := q.db.QueryRow(ctx, getAgentApprovalForCreator, arg.ID, arg.CreatorID)
	var a AgentActionApprovalRequest
	err := scanAgentApproval(row, &a)
	return a, err
}

const listAgentApprovalsForCreator = `-- name: ListAgentApprovalsForCreator :many
SELECT a.id, a.agent_id, a.requested_by_user_id, a.requested_by_token_id,
       a.action, a.payload_json, a.status, a.approval_url_slug,
       a.expires_at, a.decided_at, a.decided_by_user_id, a.decision_note,
       a.created_at
FROM agent_action_approval_requests a
JOIN agents ag ON ag.id = a.agent_id
WHERE ag.creator_id = $1
ORDER BY a.created_at DESC`

func (q *Queries) ListAgentApprovalsForCreator(ctx context.Context, creatorID uuid.UUID) ([]AgentActionApprovalRequest, error) {
	rows, err := q.db.Query(ctx, listAgentApprovalsForCreator, creatorID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []AgentActionApprovalRequest
	for rows.Next() {
		var a AgentActionApprovalRequest
		if err := scanAgentApproval(rows, &a); err != nil {
			return nil, err
		}
		items = append(items, a)
	}
	return items, rows.Err()
}

const confirmAgentApproval = `-- name: ConfirmAgentApproval :execrows
UPDATE agent_action_approval_requests a
SET status = 'confirmed',
    decided_at = NOW(),
    decided_by_user_id = $2,
    decision_note = $3
FROM agents ag
WHERE a.id = $1
  AND a.agent_id = ag.id
  AND ag.creator_id = $2
  AND a.status = 'pending'
  AND a.expires_at > NOW()`

type ConfirmAgentApprovalParams struct {
	ID           uuid.UUID `db:"id" json:"id"`
	CreatorID    uuid.UUID `db:"creator_id" json:"creator_id"`
	DecisionNote *string   `db:"decision_note" json:"decision_note"`
}

func (q *Queries) ConfirmAgentApproval(ctx context.Context, arg ConfirmAgentApprovalParams) (int64, error) {
	tag, err := q.db.Exec(ctx, confirmAgentApproval, arg.ID, arg.CreatorID, arg.DecisionNote)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

const rejectAgentApproval = `-- name: RejectAgentApproval :execrows
UPDATE agent_action_approval_requests a
SET status = 'rejected',
    decided_at = NOW(),
    decided_by_user_id = $2,
    decision_note = $3
FROM agents ag
WHERE a.id = $1
  AND a.agent_id = ag.id
  AND ag.creator_id = $2
  AND a.status = 'pending'
  AND a.expires_at > NOW()`

type RejectAgentApprovalParams = ConfirmAgentApprovalParams

func (q *Queries) RejectAgentApproval(ctx context.Context, arg RejectAgentApprovalParams) (int64, error) {
	tag, err := q.db.Exec(ctx, rejectAgentApproval, arg.ID, arg.CreatorID, arg.DecisionNote)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

const expireAgentApprovals = `-- name: ExpireAgentApprovals :execrows
UPDATE agent_action_approval_requests
SET status = 'expired'
WHERE status = 'pending' AND expires_at <= NOW()`

func (q *Queries) ExpireAgentApprovals(ctx context.Context) (int64, error) {
	tag, err := q.db.Exec(ctx, expireAgentApprovals)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
