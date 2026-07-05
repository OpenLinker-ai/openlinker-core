-- agent_approvals.sql
-- Phase 2 缺口 2：高风险动作审批 CRUD（docs/29 §3.4）。

-- name: CreateAgentApproval :one
INSERT INTO agent_action_approval_requests (
    agent_id, requested_by_user_id, requested_by_token_id,
    action, payload_json, approval_url_slug, expires_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7
)
RETURNING id, agent_id, requested_by_user_id, requested_by_token_id,
          action, payload_json, status, approval_url_slug,
          expires_at, decided_at, decided_by_user_id, decision_note,
          created_at;

-- name: GetAgentApprovalForCreator :one
SELECT a.id, a.agent_id, a.requested_by_user_id, a.requested_by_token_id,
       a.action, a.payload_json, a.status, a.approval_url_slug,
       a.expires_at, a.decided_at, a.decided_by_user_id, a.decision_note,
       a.created_at
FROM agent_action_approval_requests a
JOIN agents ag ON ag.id = a.agent_id
WHERE a.id = $1 AND ag.creator_id = $2;

-- name: ListAgentApprovalsForCreator :many
SELECT a.id, a.agent_id, a.requested_by_user_id, a.requested_by_token_id,
       a.action, a.payload_json, a.status, a.approval_url_slug,
       a.expires_at, a.decided_at, a.decided_by_user_id, a.decision_note,
       a.created_at
FROM agent_action_approval_requests a
JOIN agents ag ON ag.id = a.agent_id
WHERE ag.creator_id = $1
ORDER BY a.created_at DESC;

-- name: ConfirmAgentApproval :execrows
-- 仅 status='pending' 且未过期 + 归属 creator 时生效。
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
  AND a.expires_at > NOW();

-- name: RejectAgentApproval :execrows
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
  AND a.expires_at > NOW();

-- name: ExpireAgentApprovals :execrows
-- 后台清理：把超过 expires_at 的 pending 标记为 expired。
UPDATE agent_action_approval_requests
SET status = 'expired'
WHERE status = 'pending' AND expires_at <= NOW();
