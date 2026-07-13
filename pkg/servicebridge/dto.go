package servicebridge

import (
	"encoding/json"

	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

const (
	TargetTypeAgent    = "agent"
	TargetTypeWorkflow = "workflow"
)

type TargetValidationRequest struct {
	SellerUserID string          `json:"seller_user_id"`
	TargetType   string          `json:"target_type"`
	TargetID     string          `json:"target_id"`
	InputSchema  json.RawMessage `json:"input_schema,omitempty"`
}

type TargetValidationResponse struct {
	TargetType        string `json:"target_type"`
	TargetID          string `json:"target_id"`
	TargetName        string `json:"target_name"`
	Executable        bool   `json:"executable"`
	UnavailableReason string `json:"unavailable_reason,omitempty"`
}

type ExecutionRequest struct {
	ExternalOrderID string                 `json:"external_order_id"`
	BuyerUserID     string                 `json:"buyer_user_id"`
	SellerUserID    string                 `json:"seller_user_id"`
	TargetType      string                 `json:"target_type"`
	TargetID        string                 `json:"target_id"`
	Input           map[string]interface{} `json:"input"`
	TraceID         string                 `json:"trace_id"`
}

type ExecutionStartResponse struct {
	ExecutionID string `json:"execution_id"`
	Status      string `json:"status"`
}

type ExecutionStatusResponse struct {
	ExternalOrderID string                        `json:"external_order_id"`
	ExecutionID     string                        `json:"execution_id,omitempty"`
	TargetType      string                        `json:"target_type"`
	Status          string                        `json:"status"`
	Output          map[string]interface{}        `json:"output,omitempty"`
	Artifacts       []runtime.RunArtifactResponse `json:"artifacts"`
	ErrorCode       string                        `json:"error_code,omitempty"`
	ErrorMessage    string                        `json:"error_message,omitempty"`
	StartedAt       string                        `json:"started_at,omitempty"`
	FinishedAt      string                        `json:"finished_at,omitempty"`
}

type SafeExecutionError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
