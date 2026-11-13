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
	// TargetVersionID (workflow targets only) validates one definition
	// version instead of the current one.
	TargetVersionID string `json:"target_version_id,omitempty"`
}

type TargetValidationResponse struct {
	TargetType        string `json:"target_type"`
	TargetID          string `json:"target_id"`
	TargetName        string `json:"target_name"`
	Executable        bool   `json:"executable"`
	UnavailableReason string `json:"unavailable_reason,omitempty"`
	// UnavailableDetail is only set for input_schema_incompatible. It names
	// the first capability property and rule the bounded checker could not
	// prove, so the target owner can fix the listing or the capability.
	UnavailableDetail *TargetUnavailableDetail `json:"unavailable_detail,omitempty"`
	ContractHash      string                   `json:"contract_hash,omitempty"`
	// TargetVersionID is the workflow definition version the contract hash
	// describes; pass it as target_version_id to execute exactly that version.
	TargetVersionID string `json:"target_version_id,omitempty"`
}

// TargetUnavailableDetail explains an input-schema incompatibility. Rule is
// one of the SchemaRule* constants. CapabilityValue echoes the target
// capability's own constraint value (for example 4000 for max_length); it
// never contains caller input.
type TargetUnavailableDetail struct {
	Property        string          `json:"property,omitempty"`
	Rule            string          `json:"rule"`
	CapabilityValue json.RawMessage `json:"capability_value,omitempty"`
}

// Input-schema incompatibility rules reported in TargetUnavailableDetail.
const (
	SchemaRuleCapabilityMissing    = "capability_missing"
	SchemaRuleSchemaInvalid        = "schema_invalid"
	SchemaRuleType                 = "type"
	SchemaRuleRequired             = "required"
	SchemaRuleAdditionalProperties = "additional_properties"
	SchemaRuleUnsupportedKeyword   = "unsupported_keyword"
	SchemaRuleFormat               = "format"
	SchemaRuleEnum                 = "enum"
	SchemaRuleMinLength            = "min_length"
	SchemaRuleMaxLength            = "max_length"
	SchemaRuleMinimum              = "minimum"
	SchemaRuleMaximum              = "maximum"
	SchemaRuleExclusiveMinimum     = "exclusive_minimum"
	SchemaRuleExclusiveMaximum     = "exclusive_maximum"
	SchemaRuleMultipleOf           = "multiple_of"
	SchemaRulePattern              = "pattern"
	SchemaRuleConst                = "const"
)

// Bounded failure categories reported in ExecutionStatusResponse. They are
// derived from Core's own run error codes and never from Agent output.
const (
	FailureCategoryAgentError       = "agent_error"
	FailureCategoryAgentUnreachable = "agent_unreachable"
	FailureCategoryTimeout          = "timeout"
	FailureCategoryOutputInvalid    = "output_invalid"
	FailureCategoryInputRejected    = "input_rejected"
	FailureCategoryPolicyRejected   = "policy_rejected"
	FailureCategoryRetryExhausted   = "retry_exhausted"
	FailureCategoryResultUnknown    = "result_unknown"
	FailureCategoryCanceled         = "canceled"
	FailureCategoryPlatformError    = "platform_error"
	FailureCategoryUnknown          = "unknown"
)

type ExecutionRequest struct {
	ExternalRequestID    string                 `json:"external_request_id"`
	TargetType           string                 `json:"target_type"`
	TargetID             string                 `json:"target_id"`
	Input                map[string]interface{} `json:"input"`
	Metadata             map[string]interface{} `json:"metadata,omitempty"`
	TraceID              string                 `json:"trace_id"`
	ExpectedContractHash string                 `json:"expected_contract_hash"`
	InputSchema          json.RawMessage        `json:"input_schema"`
	// TargetVersionID (workflow targets only) binds the execution to one
	// definition version. Without it the version current at launch runs and
	// must still match expected_contract_hash.
	TargetVersionID string `json:"target_version_id,omitempty"`
}

type ExecutionStartResponse struct {
	ExecutionID string `json:"execution_id"`
	Status      string `json:"status"`
}

type ExecutionStatusResponse struct {
	ExternalRequestID string                        `json:"external_request_id"`
	ExecutionID       string                        `json:"execution_id,omitempty"`
	TargetType        string                        `json:"target_type,omitempty"`
	Status            string                        `json:"status"`
	Output            map[string]interface{}        `json:"output,omitempty"`
	Artifacts         []runtime.RunArtifactResponse `json:"artifacts"`
	ErrorCode         string                        `json:"error_code,omitempty"`
	ErrorMessage      string                        `json:"error_message,omitempty"`
	// FailureCategory is set only when Status is failed. It is one of the
	// FailureCategory* constants; ErrorCode keeps its existing values.
	FailureCategory string                         `json:"failure_category,omitempty"`
	StartedAt       string                         `json:"started_at,omitempty"`
	FinishedAt      string                         `json:"finished_at,omitempty"`
	Cancellation    *ExecutionCancellationResponse `json:"cancellation,omitempty"`
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
