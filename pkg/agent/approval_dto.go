package agent

// CreateApprovalRequest 创作者侧手动发起一条审批记录（用于 UI 模拟 / 测试）。
//
// 后续 Agent Runtime Token 触发高风险动作时，由 runtime 自动写入，
// 这里的接口仅保留给前端 / E2E 触发样例使用。
type CreateApprovalRequest struct {
	AgentID          string                 `json:"agent_id" validate:"required,uuid"`
	Action           string                 `json:"action" validate:"required,min=1,max=80"`
	Payload          map[string]interface{} `json:"payload"`
	ExpiresInMinutes int32                  `json:"expires_in_minutes" validate:"omitempty,min=5,max=1440"`
}

// ApprovalResponse 单条审批记录。
type ApprovalResponse struct {
	ID                 string                 `json:"id"`
	AgentID            string                 `json:"agent_id"`
	RequestedByUserID  *string                `json:"requested_by_user_id,omitempty"`
	RequestedByTokenID *string                `json:"requested_by_token_id,omitempty"`
	Action             string                 `json:"action"`
	Payload            map[string]interface{} `json:"payload"`
	Status             string                 `json:"status"`
	ApprovalURL        string                 `json:"approval_url"`
	ApprovalURLSlug    string                 `json:"approval_url_slug"`
	ExpiresAt          string                 `json:"expires_at"`
	DecidedAt          *string                `json:"decided_at,omitempty"`
	DecidedByUserID    *string                `json:"decided_by_user_id,omitempty"`
	DecisionNote       *string                `json:"decision_note,omitempty"`
	CreatedAt          string                 `json:"created_at"`
}

// ApprovalDecisionRequest 创作者侧确认 / 拒绝审批的请求体。
type ApprovalDecisionRequest struct {
	Note string `json:"note" validate:"omitempty,max=500"`
}
