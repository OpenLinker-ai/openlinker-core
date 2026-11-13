package externalexecution

import (
	"context"
	"errors"
	"regexp"
	"sort"
	"strings"

	"github.com/google/uuid"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/workflow"
)

// failure_category for a failed External Execution is derived only from Core
// run statuses and an explicit allowlist of run error codes. It never carries
// free text, Agent output or an unlisted code; ErrorCode/ErrorMessage keep
// their existing safe values.
//
// Run status (Runtime GetRun):
//
//	timeout                         -> timeout
//	canceled                        -> canceled
//	failed                          -> by runs.error_code below
//	any other status                -> unknown (EXECUTION_STATUS_UNKNOWN)
//
// runs.error_code of a failed run:
//
//	RUNTIME_RETRY_EXHAUSTED                          -> retry_exhausted
//	ENDPOINT_RESULT_UNKNOWN                          -> result_unknown
//	RUN_DEADLINE_EXCEEDED, RUNTIME_DISPATCH_TIMEOUT,
//	ATTEMPT_DEADLINE_EXCEEDED                        -> timeout
//	CANCELED, RUN_CANCELED                           -> canceled
//	AGENT_ERROR, MCP_TOOL_FAILED, MCP_TOOL_MISSING,
//	HANDLER_ERROR, HANDLER_FAILED, HANDLER_PANIC,
//	HANDLER_CANCELED, HANDLER_CANCELLED,
//	AGENT_WORKER_ERROR, AGENT_RUNTIME_ERROR,
//	AGENT_RUNTIME_PANIC                              -> agent_error
//	HTTP_4xx, HTTP_5xx (except 502/503/504)          -> agent_error
//	HTTP_502, HTTP_503, HTTP_504                     -> agent_unreachable
//	MCP_<JSON-RPC code> (except -32602)              -> agent_error
//	MCP_-32602, RUN_INPUT_SCHEMA_MISMATCH            -> input_rejected
//	INVALID_RESPONSE, INVALID_MCP_RESPONSE,
//	HANDLER_EVENT_INVALID, RESULT_INVALID,
//	RESULT_TOO_LARGE, RESULT_STATUS_INVALID,
//	AGENT_RUNTIME_INVALID_RESULT                     -> output_invalid
//	MCP_TOOL_ACCESS_REVOKED                          -> policy_rejected
//	CORE_EVENT_PERSIST_FAILED,
//	UNSUPPORTED_CONNECTION_MODE,
//	ASSIGNMENT_INPUT_INVALID,
//	ASSIGNMENT_METADATA_INVALID,
//	ASSIGNMENT_AUTHORITY_INVALID                     -> platform_error
//	any other code (including free-form Agent codes) -> unknown
//
// Sources: Core's direct HTTP/MCP executor (pkg/runtime service.go and
// core_executor.go), the result finalizer and deadline/lease reconciler
// (finalizer.go, reconciler.go), Runtime cancellation, and the codes the Go,
// TypeScript and Python SDK Runtime Workers report themselves. Direct HTTP
// Agents and Runtime Workers may also report their own codes; those are
// passed through to runs.error_code unchanged and stay unknown here unless
// they are one of the listed values.
//
// Workflow runs: canceled -> canceled; failed -> the category of the first
// failed step (by sequence) whose child run ended failed, timeout or
// canceled; no such step -> unknown; any other status -> unknown.
//
// Cancellation evidence: an unconfirmed External Execution cancellation
// (CANCEL_UNCONFIRMED) -> result_unknown.
var runErrorCodeFailureCategories = map[string]string{
	"RUNTIME_RETRY_EXHAUSTED": FailureCategoryRetryExhausted,
	"ENDPOINT_RESULT_UNKNOWN": FailureCategoryResultUnknown,

	"RUN_DEADLINE_EXCEEDED":     FailureCategoryTimeout,
	"RUNTIME_DISPATCH_TIMEOUT":  FailureCategoryTimeout,
	"ATTEMPT_DEADLINE_EXCEEDED": FailureCategoryTimeout,

	"CANCELED":     FailureCategoryCanceled,
	"RUN_CANCELED": FailureCategoryCanceled,

	"AGENT_ERROR":         FailureCategoryAgentError,
	"MCP_TOOL_FAILED":     FailureCategoryAgentError,
	"MCP_TOOL_MISSING":    FailureCategoryAgentError,
	"HANDLER_ERROR":       FailureCategoryAgentError,
	"HANDLER_FAILED":      FailureCategoryAgentError,
	"HANDLER_PANIC":       FailureCategoryAgentError,
	"HANDLER_CANCELED":    FailureCategoryAgentError,
	"HANDLER_CANCELLED":   FailureCategoryAgentError,
	"AGENT_WORKER_ERROR":  FailureCategoryAgentError,
	"AGENT_RUNTIME_ERROR": FailureCategoryAgentError,
	"AGENT_RUNTIME_PANIC": FailureCategoryAgentError,

	"HTTP_502": FailureCategoryAgentUnreachable,
	"HTTP_503": FailureCategoryAgentUnreachable,
	"HTTP_504": FailureCategoryAgentUnreachable,

	"MCP_-32602":                FailureCategoryInputRejected,
	"RUN_INPUT_SCHEMA_MISMATCH": FailureCategoryInputRejected,

	"INVALID_RESPONSE":             FailureCategoryOutputInvalid,
	"INVALID_MCP_RESPONSE":         FailureCategoryOutputInvalid,
	"HANDLER_EVENT_INVALID":        FailureCategoryOutputInvalid,
	"RESULT_INVALID":               FailureCategoryOutputInvalid,
	"RESULT_TOO_LARGE":             FailureCategoryOutputInvalid,
	"RESULT_STATUS_INVALID":        FailureCategoryOutputInvalid,
	"AGENT_RUNTIME_INVALID_RESULT": FailureCategoryOutputInvalid,

	"MCP_TOOL_ACCESS_REVOKED": FailureCategoryPolicyRejected,

	"CORE_EVENT_PERSIST_FAILED":    FailureCategoryPlatformError,
	"UNSUPPORTED_CONNECTION_MODE":  FailureCategoryPlatformError,
	"ASSIGNMENT_INPUT_INVALID":     FailureCategoryPlatformError,
	"ASSIGNMENT_METADATA_INVALID":  FailureCategoryPlatformError,
	"ASSIGNMENT_AUTHORITY_INVALID": FailureCategoryPlatformError,
}

var (
	// HTTP_<status> is produced by Core's direct HTTP/MCP executor for a
	// status >= 400 without an Agent error envelope.
	runHTTPStatusErrorCodePattern = regexp.MustCompile(`^HTTP_[45][0-9]{2}$`)
	// MCP_<code> carries the JSON-RPC error code of an MCP tool call.
	runMCPErrorCodePattern = regexp.MustCompile(`^MCP_-?[0-9]{1,10}$`)
)

func runErrorCodeFailureCategory(code string) string {
	if category, ok := runErrorCodeFailureCategories[code]; ok {
		return category
	}
	if runHTTPStatusErrorCodePattern.MatchString(code) || runMCPErrorCodePattern.MatchString(code) {
		return FailureCategoryAgentError
	}
	return FailureCategoryUnknown
}

// runFailureCategory maps a Core run's status and error code.
func runFailureCategory(status, errorCode string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "timeout":
		return FailureCategoryTimeout
	case "canceled":
		return FailureCategoryCanceled
	case "failed":
		return runErrorCodeFailureCategory(strings.TrimSpace(errorCode))
	default:
		return FailureCategoryUnknown
	}
}

func runStatusIsTerminalFailure(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "failed", "timeout", "canceled":
		return true
	default:
		return false
	}
}

// workflowFailureCategory derives a failed workflow run's category from its
// steps' child runs. Steps of one level run concurrently and a failure stops
// its siblings, which then also fail without a terminal child run; the first
// failed step whose child run ended failed, timeout or canceled is taken as the
// cause. Child runs are read as the actor, like the artifact aggregation. A
// child that is not visible is skipped; any other read error is returned and
// the status response falls back to unknown instead of failing.
func (s *Service) workflowFailureCategory(ctx context.Context, actorUserID uuid.UUID, run *workflow.WorkflowRunResponse) (string, error) {
	switch strings.ToLower(strings.TrimSpace(run.Status)) {
	case "canceled":
		return FailureCategoryCanceled, nil
	case "failed":
	default:
		return FailureCategoryUnknown, nil
	}
	// A node whose effective attempt hit its deadline is the cause: its
	// child stop is recorded as unknown, not as a failed child run.
	for _, node := range run.Nodes {
		if strings.ToLower(strings.TrimSpace(node.State)) == "timeout" {
			return FailureCategoryTimeout, nil
		}
	}
	steps := append([]workflow.WorkflowRunStepResponse(nil), run.Steps...)
	sort.SliceStable(steps, func(i, j int) bool { return steps[i].Sequence < steps[j].Sequence })
	if len(run.Nodes) == 0 {
		for _, step := range steps {
			if strings.ToLower(strings.TrimSpace(step.Status)) == "timeout" {
				return FailureCategoryTimeout, nil
			}
		}
	}
	for _, step := range steps {
		if strings.ToLower(strings.TrimSpace(step.Status)) != "failed" {
			continue
		}
		childID, err := uuid.Parse(strings.TrimSpace(step.RunID))
		if err != nil || childID == uuid.Nil {
			continue
		}
		child, err := s.runtime.GetRun(ctx, actorUserID, childID)
		if err != nil {
			var httpErr *httpx.HTTPError
			if errors.As(err, &httpErr) && httpErr.Status < 500 {
				continue
			}
			return "", err
		}
		if child == nil || !runStatusIsTerminalFailure(child.Status) {
			continue
		}
		return runFailureCategory(child.Status, child.ErrorCode), nil
	}
	return FailureCategoryUnknown, nil
}
