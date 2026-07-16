package externalexecution

import (
	"encoding/json"

	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

const (
	TargetTypeAgent    = "agent"
	TargetTypeWorkflow = "workflow"
)

type TargetValidationRequest struct {
	TargetType  string          `json:"target_type"`
	TargetID    string          `json:"target_id"`
	InputSchema json.RawMessage `json:"input_schema,omitempty"`
}

type TargetValidationResponse struct {
	TargetType        string `json:"target_type"`
	TargetID          string `json:"target_id"`
	TargetName        string `json:"target_name"`
	Executable        bool   `json:"executable"`
	UnavailableReason string `json:"unavailable_reason,omitempty"`
	ContractHash      string `json:"contract_hash,omitempty"`
}

type ExecutionRequest struct {
	ExternalRequestID    string                 `json:"external_request_id"`
	TargetType           string                 `json:"target_type"`
	TargetID             string                 `json:"target_id"`
	Input                map[string]interface{} `json:"input"`
	Metadata             map[string]interface{} `json:"metadata,omitempty"`
	TraceID              string                 `json:"trace_id"`
	ExpectedContractHash string                 `json:"expected_contract_hash"`
	InputSchema          json.RawMessage        `json:"input_schema"`
}

type ExecutionStartResponse struct {
	ExecutionID string `json:"execution_id"`
	Status      string `json:"status"`
}

type ExecutionStatusResponse struct {
	ExternalRequestID string                         `json:"external_request_id"`
	ExecutionID       string                         `json:"execution_id,omitempty"`
	TargetType        string                         `json:"target_type,omitempty"`
	Status            string                         `json:"status"`
	Output            map[string]interface{}         `json:"output,omitempty"`
	Artifacts         []runtime.RunArtifactResponse  `json:"artifacts"`
	ErrorCode         string                         `json:"error_code,omitempty"`
	ErrorMessage      string                         `json:"error_message,omitempty"`
	StartedAt         string                         `json:"started_at,omitempty"`
	FinishedAt        string                         `json:"finished_at,omitempty"`
	Cancellation      *ExecutionCancellationResponse `json:"cancellation,omitempty"`
}

type ExecutionCancelRequest struct {
	ReasonCode string `json:"reason_code"`
}

type ExecutionCancellationResponse struct {
	CancellationID string `json:"cancellation_id"`
	State          string `json:"state"`
	ReasonCode     string `json:"reason_code"`
	ExecutionKind  string `json:"execution_kind,omitempty"`
	ExecutionID    string `json:"execution_id,omitempty"`
	RequestedAt    string `json:"requested_at"`
	AppliedAt      string `json:"applied_at,omitempty"`
	FinishedAt     string `json:"finished_at,omitempty"`
}

type SafeExecutionError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
