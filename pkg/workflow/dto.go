package workflow

import "github.com/google/uuid"

// CreateWorkflowRequest creates a DAG of Agent nodes. Omitted/null edges use
// the sequential default; an explicit empty array makes all nodes independent.
type CreateWorkflowRequest struct {
	Name        string `json:"name" validate:"required,min=1,max=120"`
	Description string `json:"description,omitempty" validate:"omitempty,max=500"`
	// The configured limits (WORKFLOW_MAX_NODES/EDGES, default 30/60) are
	// enforced by the service; these tags are the absolute ceiling.
	Nodes []WorkflowNodeRequest    `json:"nodes" validate:"required,min=1,max=100,dive"`
	Edges []map[string]interface{} `json:"edges" validate:"omitempty,max=200"`
	// Semantics "v2" interprets node policies (config.policy), join modes
	// (config.join), conditional edges (when), input mapping v2 and
	// output_mapping. Default "v1" keeps the original interpretation.
	Semantics     string                 `json:"semantics,omitempty" validate:"omitempty,oneof=v1 v2"`
	OutputMapping map[string]interface{} `json:"output_mapping,omitempty"`
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
	// Version runs a specific definition version (queued runs only); the
	// default is the current version.
	Version int32 `json:"version,omitempty" validate:"omitempty,min=1"`
}

// ExternalExecutionTargetValidation is the narrow Core result consumed by an
// external execution caller. It deliberately carries no workflow definition
// or endpoint details.
type ExternalExecutionTargetValidation struct {
	TargetName        string `json:"target_name"`
	Executable        bool   `json:"executable"`
	UnavailableReason string `json:"unavailable_reason,omitempty"`
	ContractHash      string `json:"contract_hash,omitempty"`
	// TargetVersionID is the definition version the hash describes.
	TargetVersionID string `json:"target_version_id,omitempty"`
}

type RerunWorkflowStepRequest struct {
	NodeKey string `json:"node_key" validate:"required,min=1,max=80"`
	// AcknowledgeUnconfirmedChildren: the owner accepts that children of the
	// source run may still be executing in their backends.
	AcknowledgeUnconfirmedChildren bool `json:"acknowledge_unconfirmed_children,omitempty"`
}

type RetryWorkflowRunRequest struct {
	AcknowledgeUnconfirmedChildren bool `json:"acknowledge_unconfirmed_children,omitempty"`
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
	// Version is the immutable definition version these nodes and edges
	// belong to; send it back as If-Match when updating.
	Version       int32                  `json:"version,omitempty"`
	VersionID     string                 `json:"version_id,omitempty"`
	Semantics     string                 `json:"semantics,omitempty"`
	OutputMapping map[string]interface{} `json:"output_mapping,omitempty"`
}

// UpdateWorkflowRequest replaces the definition with a new version.
type UpdateWorkflowRequest = CreateWorkflowRequest

type DuplicateWorkflowRequest struct {
	Name string `json:"name,omitempty" validate:"omitempty,min=1,max=120"`
}

type WorkflowVersionSummary struct {
	ID               string `json:"id"`
	Version          int32  `json:"version"`
	Semantics        string `json:"semantics"`
	Name             string `json:"name"`
	DefinitionDigest string `json:"definition_digest"`
	NodeCount        int32  `json:"node_count"`
	Current          bool   `json:"current"`
	CreatedAt        string `json:"created_at"`
}

type WorkflowVersionListResponse struct {
	Items          []WorkflowVersionSummary `json:"items"`
	CurrentVersion int32                    `json:"current_version"`
}

type WorkflowListResponse struct {
	Items        []WorkflowResponse `json:"items"`
	Total        int32              `json:"total"`
	Page         int32              `json:"page"`
	Size         int32              `json:"size"`
	Query        string             `json:"query,omitempty"`
	Sort         string             `json:"sort"`
	StatusFilter string             `json:"status_filter,omitempty"`
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
	// HasUnconfirmedChildren: some child Run of this run could not be shown
	// to have stopped in its backend.
	HasUnconfirmedChildren bool `json:"has_unconfirmed_children,omitempty"`
	// UnconfirmedChildrenAcknowledgedBy: the owner who created this run by
	// Retry/Rerun accepting unconfirmed children of the source run.
	UnconfirmedChildrenAcknowledgedBy string `json:"unconfirmed_children_acknowledged_by,omitempty"`
	// WorkflowVersion is the definition version this run executes.
	WorkflowVersion int32 `json:"workflow_version,omitempty"`
	// Revision increases with every change to the run, its nodes, attempts
	// and stop records; event streams use it as the event id.
	Revision int64 `json:"revision,omitempty"`
	// Nodes are the logical node states (absent for history without them).
	Nodes []WorkflowRunNodeResponse `json:"nodes,omitempty"`
}

type WorkflowRunNodeResponse struct {
	NodeKey          string `json:"node_key"`
	State            string `json:"state"`
	EffectiveAttempt int32  `json:"effective_attempt,omitempty"`
	SkipReason       string `json:"skip_reason,omitempty"`
}

type WorkflowRunListResponse struct {
	Items        []WorkflowRunResponse `json:"items"`
	Total        int32                 `json:"total"`
	Page         int32                 `json:"page"`
	Size         int32                 `json:"size"`
	Query        string                `json:"query,omitempty"`
	Sort         string                `json:"sort"`
	StatusFilter string                `json:"status_filter,omitempty"`
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
	Attempt    int32                  `json:"attempt"`
	// Effective marks the node's latest listed attempt, the one readers use.
	Effective bool   `json:"effective"`
	Source    string `json:"source"`
}
