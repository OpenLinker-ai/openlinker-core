package workflow

import "github.com/google/uuid"

// CreateWorkflowRequest creates a sequential workflow made of Agent nodes.
type CreateWorkflowRequest struct {
	Name        string                   `json:"name" validate:"required,min=1,max=120"`
	Description string                   `json:"description,omitempty" validate:"omitempty,max=500"`
	Nodes       []WorkflowNodeRequest    `json:"nodes" validate:"required,min=1,max=10,dive"`
	Edges       []map[string]interface{} `json:"edges,omitempty" validate:"omitempty,max=20"`
}

type WorkflowNodeRequest struct {
	Key     string                 `json:"key" validate:"required,min=1,max=80"`
	Title   string                 `json:"title,omitempty" validate:"omitempty,max=160"`
	AgentID uuid.UUID              `json:"agent_id" validate:"required"`
	Config  map[string]interface{} `json:"config,omitempty"`
}

type RunWorkflowRequest struct {
	Input       map[string]interface{} `json:"input,omitempty"`
	MaxAttempts int32                  `json:"max_attempts,omitempty" validate:"omitempty,min=1,max=10"`
}

type RerunWorkflowStepRequest struct {
	NodeKey string `json:"node_key" validate:"required,min=1,max=80"`
}

type WorkflowResponse struct {
	ID          string                   `json:"id"`
	Name        string                   `json:"name"`
	Description string                   `json:"description"`
	Status      string                   `json:"status"`
	Nodes       []WorkflowNodeResponse   `json:"nodes"`
	Edges       []map[string]interface{} `json:"edges"`
	CreatedAt   string                   `json:"created_at"`
	UpdatedAt   string                   `json:"updated_at"`
}

type WorkflowListResponse struct {
	Items []WorkflowResponse `json:"items"`
	Total int32              `json:"total"`
}

type WorkflowNodeResponse struct {
	ID       string                 `json:"id"`
	Key      string                 `json:"key"`
	Type     string                 `json:"type"`
	AgentID  string                 `json:"agent_id"`
	Title    string                 `json:"title"`
	Config   map[string]interface{} `json:"config"`
	Position int32                  `json:"position"`
}

type WorkflowRunResponse struct {
	ID              string                    `json:"id"`
	WorkflowID      string                    `json:"workflow_id"`
	Status          string                    `json:"status"`
	Input           map[string]interface{}    `json:"input"`
	Output          map[string]interface{}    `json:"output,omitempty"`
	Error           string                    `json:"error_message,omitempty"`
	Steps           []WorkflowRunStepResponse `json:"steps"`
	AttemptCount    int32                     `json:"attempt_count"`
	MaxAttempts     int32                     `json:"max_attempts"`
	NextRetryAt     string                    `json:"next_retry_at,omitempty"`
	ClaimedAt       string                    `json:"claimed_at,omitempty"`
	LastWorkerError string                    `json:"last_worker_error,omitempty"`
	StartedAt       string                    `json:"started_at"`
	FinishedAt      string                    `json:"finished_at,omitempty"`
	CreatedAt       string                    `json:"created_at"`
	UpdatedAt       string                    `json:"updated_at"`
}

type WorkflowRunListResponse struct {
	Items []WorkflowRunResponse `json:"items"`
	Total int32                 `json:"total"`
}

type WorkflowStepRerunResponse struct {
	SourceRunID    string                        `json:"source_run_id"`
	RerunRunID     string                        `json:"rerun_run_id"`
	NodeKey        string                        `json:"node_key"`
	ReusedNodeKeys []string                      `json:"reused_node_keys"`
	RerunNodeKeys  []string                      `json:"rerun_node_keys"`
	Run            WorkflowRunResponse           `json:"run"`
	Comparison     WorkflowRunComparisonResponse `json:"comparison"`
}

type WorkflowRunComparisonResponse struct {
	BaseRunID       string                           `json:"base_run_id"`
	CandidateRunID  string                           `json:"candidate_run_id"`
	WorkflowID      string                           `json:"workflow_id"`
	StatusChanged   bool                             `json:"status_changed"`
	OutputChanged   bool                             `json:"output_changed"`
	ChangedNodeKeys []string                         `json:"changed_node_keys"`
	Steps           []WorkflowRunStepCompareResponse `json:"steps"`
}

type WorkflowRunStepCompareResponse struct {
	NodeKey         string `json:"node_key"`
	BaseStatus      string `json:"base_status,omitempty"`
	CandidateStatus string `json:"candidate_status,omitempty"`
	BaseRunID       string `json:"base_run_id,omitempty"`
	CandidateRunID  string `json:"candidate_run_id,omitempty"`
	StatusChanged   bool   `json:"status_changed"`
	RunChanged      bool   `json:"run_changed"`
	OutputChanged   bool   `json:"output_changed"`
	ErrorChanged    bool   `json:"error_changed"`
	Changed         bool   `json:"changed"`
}

type WorkflowRunStepResponse struct {
	ID         string                 `json:"id"`
	NodeID     string                 `json:"node_id"`
	NodeKey    string                 `json:"node_key"`
	AgentID    string                 `json:"agent_id"`
	RunID      string                 `json:"run_id,omitempty"`
	Status     string                 `json:"status"`
	Input      map[string]interface{} `json:"input"`
	Output     map[string]interface{} `json:"output,omitempty"`
	Error      string                 `json:"error_message,omitempty"`
	Sequence   int32                  `json:"sequence"`
	StartedAt  string                 `json:"started_at"`
	FinishedAt string                 `json:"finished_at,omitempty"`
}
