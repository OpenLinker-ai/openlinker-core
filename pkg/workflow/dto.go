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
	Input map[string]interface{} `json:"input,omitempty"`
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
	ID         string                    `json:"id"`
	WorkflowID string                    `json:"workflow_id"`
	Status     string                    `json:"status"`
	Input      map[string]interface{}    `json:"input"`
	Output     map[string]interface{}    `json:"output,omitempty"`
	Error      string                    `json:"error_message,omitempty"`
	Steps      []WorkflowRunStepResponse `json:"steps"`
	StartedAt  string                    `json:"started_at"`
	FinishedAt string                    `json:"finished_at,omitempty"`
	CreatedAt  string                    `json:"created_at"`
	UpdatedAt  string                    `json:"updated_at"`
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
