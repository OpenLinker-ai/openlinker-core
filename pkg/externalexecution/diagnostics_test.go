package externalexecution

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/OpenLinker-ai/openlinker-core/pkg/workflow"
)

func TestValidateTargetReturnsSchemaIncompatibilityDetailToOwnerOnly(t *testing.T) {
	ownerID, targetID := uuid.New(), uuid.New()
	agents := callableAgent(targetID)
	agents.onboarding = &agent.OnboardingResponse{Capability: &agent.CapabilityResponse{
		Version: 1,
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"prompt": map[string]interface{}{"type": "string", "maxLength": 4000.0},
			},
			"required": []interface{}{"prompt"},
		},
		OutputSchema: map[string]interface{}{"type": "object"},
	}}
	svc := newTestService(agents, ownerID)
	stricter := json.RawMessage(`{"type":"object","properties":{"prompt":{"type":"string","maxLength":2000}},"required":["prompt"]}`)
	bounded, err := svc.ValidateTarget(context.Background(), testPrincipal(ownerID), &TargetValidationRequest{
		TargetType: TargetTypeAgent, TargetID: targetID.String(), InputSchema: stricter,
	})
	if err != nil || !bounded.Executable || bounded.UnavailableDetail != nil || bounded.ContractHash == "" {
		t.Fatalf("stricter bounded listing ValidateTarget() = %#v, %v", bounded, err)
	}

	listing := json.RawMessage(`{"type":"object","properties":{"prompt":{"type":"string","maxLength":5000}},"required":["prompt"]}`)
	resp, err := svc.ValidateTarget(context.Background(), testPrincipal(ownerID), &TargetValidationRequest{
		TargetType: TargetTypeAgent, TargetID: targetID.String(), InputSchema: listing,
	})
	if err != nil || resp.Executable || resp.UnavailableReason != "input_schema_incompatible" || resp.ContractHash != "" {
		t.Fatalf("ValidateTarget() = %#v, %v", resp, err)
	}
	requireDetail(t, resp.UnavailableDetail, "prompt", SchemaRuleMaxLength, `4000`)
	encoded, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"unavailable_detail":{"property":"prompt","rule":"max_length","capability_value":4000}`) {
		t.Fatalf("encoded response = %s", encoded)
	}

	other, err := svc.ValidateTarget(context.Background(), testPrincipal(uuid.New()), &TargetValidationRequest{
		TargetType: TargetTypeAgent, TargetID: targetID.String(), InputSchema: listing,
	})
	if err != nil || other.UnavailableReason != "not_found" || other.UnavailableDetail != nil {
		t.Fatalf("non-owner ValidateTarget() = %#v, %v", other, err)
	}

	agents.onboarding.Capability.InputSchema["properties"] = map[string]interface{}{"prompt": map[string]interface{}{"type": "string"}}
	compatible, err := svc.ValidateTarget(context.Background(), testPrincipal(ownerID), &TargetValidationRequest{
		TargetType: TargetTypeAgent, TargetID: targetID.String(), InputSchema: listing,
	})
	if err != nil || !compatible.Executable || compatible.UnavailableDetail != nil {
		t.Fatalf("compatible ValidateTarget() = %#v, %v", compatible, err)
	}
	encoded, _ = json.Marshal(compatible)
	if strings.Contains(string(encoded), "unavailable_detail") {
		t.Fatalf("compatible response carries a detail: %s", encoded)
	}

	agents.onboarding = &agent.OnboardingResponse{}
	missing, err := svc.ValidateTarget(context.Background(), testPrincipal(ownerID), &TargetValidationRequest{
		TargetType: TargetTypeAgent, TargetID: targetID.String(), InputSchema: listing,
	})
	if err != nil || missing.UnavailableReason != "input_schema_incompatible" {
		t.Fatalf("missing capability ValidateTarget() = %#v, %v", missing, err)
	}
	requireDetail(t, missing.UnavailableDetail, "", SchemaRuleCapabilityMissing, "")
}

func TestStartExecutionSchemaIncompatibilityDoesNotLeakDetail(t *testing.T) {
	requestID, actorID, ownerID, targetID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	agents := callableAgent(targetID)
	agents.onboarding = &agent.OnboardingResponse{Capability: &agent.CapabilityResponse{
		Version: 1,
		InputSchema: map[string]interface{}{"type": "object", "properties": map[string]interface{}{
			"prompt": map[string]interface{}{"type": "string"},
		}},
		OutputSchema: map[string]interface{}{"type": "object"},
	}}
	runtimeSvc := &fakeRuntimeService{}
	svc := NewService(agents, runtimeSvc, &fakeWorkflowService{}, newMemoryStore(ownerID))
	listing := json.RawMessage(`{"type":"object","properties":{"prompt":{"type":"string"}}}`)
	validation, err := svc.ValidateTarget(context.Background(), testPrincipal(ownerID), &TargetValidationRequest{
		TargetType: TargetTypeAgent, TargetID: targetID.String(), InputSchema: listing,
	})
	if err != nil || !validation.Executable {
		t.Fatalf("ValidateTarget() = %#v, %v", validation, err)
	}
	// The owner tightens the capability without changing its contract-relevant
	// version; the start-time re-check must reject with the existing code only.
	agents.onboarding.Capability.InputSchema["properties"] = map[string]interface{}{
		"prompt": map[string]interface{}{"type": "string", "pattern": "^secret-owner-rule$"},
	}
	_, err = svc.StartExecution(context.Background(), testPrincipal(actorID), &ExecutionRequest{
		ExternalRequestID: requestID.String(), TargetType: TargetTypeAgent, TargetID: targetID.String(),
		Input: map[string]interface{}{"prompt": "hi"}, TraceID: "trace-detail",
		ExpectedContractHash: validation.ContractHash, InputSchema: listing,
	})
	assertHTTPCode(t, err, httpx.ErrorCode("TARGET_CONTRACT_CHANGED"))
	var httpErr *httpx.HTTPError
	if !errors.As(err, &httpErr) || httpErr.Details != nil || strings.Contains(httpErr.Message, "secret-owner-rule") {
		t.Fatalf("start rejection leaked capability detail: %#v", err)
	}
	if runtimeSvc.startCalls != 0 {
		t.Fatalf("StartRun calls = %d, want 0", runtimeSvc.startCalls)
	}
}

func TestRunErrorCodeFailureCategoryMapping(t *testing.T) {
	known := map[string]struct{}{
		FailureCategoryAgentError: {}, FailureCategoryAgentUnreachable: {}, FailureCategoryTimeout: {},
		FailureCategoryOutputInvalid: {}, FailureCategoryInputRejected: {}, FailureCategoryPolicyRejected: {},
		FailureCategoryRetryExhausted: {}, FailureCategoryResultUnknown: {}, FailureCategoryCanceled: {},
		FailureCategoryPlatformError: {}, FailureCategoryUnknown: {},
	}
	for code, category := range runErrorCodeFailureCategories {
		if _, ok := known[category]; !ok {
			t.Fatalf("%s maps to unknown category constant %q", code, category)
		}
	}
	cases := []struct {
		status, code, want string
	}{
		{"failed", "RUNTIME_RETRY_EXHAUSTED", FailureCategoryRetryExhausted},
		{"failed", "ENDPOINT_RESULT_UNKNOWN", FailureCategoryResultUnknown},
		{"failed", "CORE_EVENT_PERSIST_FAILED", FailureCategoryPlatformError},
		{"failed", "UNSUPPORTED_CONNECTION_MODE", FailureCategoryPlatformError},
		{"failed", "ASSIGNMENT_AUTHORITY_INVALID", FailureCategoryPlatformError},
		{"failed", "AGENT_ERROR", FailureCategoryAgentError},
		{"failed", "HANDLER_ERROR", FailureCategoryAgentError},
		{"failed", "HANDLER_PANIC", FailureCategoryAgentError},
		{"failed", "HANDLER_FAILED", FailureCategoryAgentError},
		{"failed", "HANDLER_CANCELLED", FailureCategoryAgentError},
		{"failed", "AGENT_RUNTIME_ERROR", FailureCategoryAgentError},
		{"failed", "MCP_TOOL_FAILED", FailureCategoryAgentError},
		{"failed", "HTTP_404", FailureCategoryAgentError},
		{"failed", "HTTP_500", FailureCategoryAgentError},
		{"failed", "HTTP_502", FailureCategoryAgentUnreachable},
		{"failed", "HTTP_503", FailureCategoryAgentUnreachable},
		{"failed", "HTTP_504", FailureCategoryAgentUnreachable},
		{"failed", "HTTP_302", FailureCategoryUnknown},
		{"failed", "HTTP_5000", FailureCategoryUnknown},
		{"failed", "MCP_-32601", FailureCategoryAgentError},
		{"failed", "MCP_-32602", FailureCategoryInputRejected},
		{"failed", "MCP_abc", FailureCategoryUnknown},
		{"failed", "RUN_INPUT_SCHEMA_MISMATCH", FailureCategoryInputRejected},
		{"failed", "INVALID_RESPONSE", FailureCategoryOutputInvalid},
		{"failed", "INVALID_MCP_RESPONSE", FailureCategoryOutputInvalid},
		{"failed", "RESULT_TOO_LARGE", FailureCategoryOutputInvalid},
		{"failed", "AGENT_RUNTIME_INVALID_RESULT", FailureCategoryOutputInvalid},
		{"failed", "MCP_TOOL_ACCESS_REVOKED", FailureCategoryPolicyRejected},
		{"failed", "ATTEMPT_DEADLINE_EXCEEDED", FailureCategoryTimeout},
		{"failed", "RUN_CANCELED", FailureCategoryCanceled},
		{"failed", " HANDLER_PANIC ", FailureCategoryAgentError},
		{"failed", "handler_panic", FailureCategoryUnknown},
		{"failed", "Agent said: database password is hunter2", FailureCategoryUnknown},
		{"failed", "", FailureCategoryUnknown},
		{"timeout", "RUN_DEADLINE_EXCEEDED", FailureCategoryTimeout},
		{"timeout", "RUNTIME_DISPATCH_TIMEOUT", FailureCategoryTimeout},
		{"canceled", "CANCELED", FailureCategoryCanceled},
		{"Canceled", "", FailureCategoryCanceled},
		{"exploded", "AGENT_ERROR", FailureCategoryUnknown},
	}
	for _, tc := range cases {
		if got := runFailureCategory(tc.status, tc.code); got != tc.want {
			t.Errorf("runFailureCategory(%q, %q) = %q, want %q", tc.status, tc.code, got, tc.want)
		}
	}
}

func attachedRunService(t *testing.T, run *runtime.RunResponse) (*Service, *fakeRuntimeService, uuid.UUID, uuid.UUID) {
	t.Helper()
	requestID, actorID, targetID, runID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	kind := "run"
	store := newMemoryStore()
	store.records[executionStoreKey("openlinker-cloud", requestID)] = ExecutionRecord{
		CallerServiceID: "openlinker-cloud", ExternalRequestID: requestID, ActorUserID: actorID,
		TargetType: TargetTypeAgent, TargetID: targetID, StartState: startStateAttached,
		ExecutionKind: &kind, ExecutionID: &runID,
	}
	run.RunID = runID.String()
	runtimeSvc := &fakeRuntimeService{getResponse: run}
	return NewService(callableAgent(targetID), runtimeSvc, &fakeWorkflowService{}, store), runtimeSvc, requestID, actorID
}

func TestGetExecutionReportsRunFailureCategory(t *testing.T) {
	cases := []struct {
		status, code         string
		wantStatus, wantCode string
		wantCategory         string
	}{
		{"failed", "HANDLER_PANIC", "failed", "EXECUTION_FAILED", FailureCategoryAgentError},
		{"failed", "RUNTIME_RETRY_EXHAUSTED", "failed", "EXECUTION_FAILED", FailureCategoryRetryExhausted},
		{"failed", "ENDPOINT_RESULT_UNKNOWN", "failed", "EXECUTION_FAILED", FailureCategoryResultUnknown},
		{"failed", "AGENT_CUSTOM_SECRET_CODE", "failed", "EXECUTION_FAILED", FailureCategoryUnknown},
		{"timeout", "RUN_DEADLINE_EXCEEDED", "failed", "EXECUTION_TIMEOUT", FailureCategoryTimeout},
		{"canceled", "CANCELED", "failed", "EXECUTION_CANCELED", FailureCategoryCanceled},
		{"exploded", "AGENT_ERROR", "failed", "EXECUTION_STATUS_UNKNOWN", FailureCategoryUnknown},
		{"success", "", "succeeded", "", ""},
		{"running", "", "running", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.status+"/"+tc.code, func(t *testing.T) {
			now := time.Now().UTC()
			svc, _, requestID, actorID := attachedRunService(t, &runtime.RunResponse{
				Status: tc.status, ErrorCode: tc.code, ErrorMsg: "agent private text " + tc.code, StartedAt: now,
			})
			resp, err := svc.GetExecution(context.Background(), testPrincipal(actorID), requestID.String())
			if err != nil || resp.Status != tc.wantStatus || resp.ErrorCode != tc.wantCode || resp.FailureCategory != tc.wantCategory {
				t.Fatalf("GetExecution() = %#v, %v", resp, err)
			}
			if strings.Contains(resp.ErrorMessage, "agent private text") {
				t.Fatalf("unsafe error message %q", resp.ErrorMessage)
			}
			encoded, _ := json.Marshal(resp)
			if tc.wantCategory == "" && strings.Contains(string(encoded), "failure_category") {
				t.Fatalf("non-failed status carries failure_category: %s", encoded)
			}
		})
	}
}

func attachedWorkflowService(t *testing.T, run *workflow.WorkflowRunResponse, runtimeSvc *fakeRuntimeService) (*Service, ExecutionRecord, uuid.UUID) {
	t.Helper()
	requestID, actorID, targetID, workflowRunID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	kind := "workflow_run"
	record := ExecutionRecord{
		CallerServiceID: "openlinker-cloud", ExternalRequestID: requestID, ActorUserID: actorID,
		TargetType: TargetTypeWorkflow, TargetID: targetID, StartState: startStateAttached,
		ExecutionKind: &kind, ExecutionID: &workflowRunID,
	}
	store := newMemoryStore()
	store.records[executionStoreKey("openlinker-cloud", requestID)] = record
	run.ID = workflowRunID.String()
	return NewService(callableAgent(uuid.New()), runtimeSvc, &fakeWorkflowService{getResponse: run}, store), record, actorID
}

func TestGetWorkflowExecutionReportsFirstFailedChildCategory(t *testing.T) {
	succeeded, casualty, notVisible, failed, timedOut := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	runtimeSvc := &fakeRuntimeService{
		runs: map[uuid.UUID]*runtime.RunResponse{
			succeeded: {Status: "success"},
			casualty:  {Status: "running"},
			failed:    {Status: "failed", ErrorCode: "HANDLER_PANIC", ErrorMsg: "agent private text"},
			timedOut:  {Status: "timeout", ErrorCode: "RUN_DEADLINE_EXCEEDED"},
		},
		runErrs: map[uuid.UUID]error{notVisible: httpx.NotFound("调用记录不存在")},
	}
	run := &workflow.WorkflowRunResponse{Status: "failed", Error: "workflow private text", Steps: []workflow.WorkflowRunStepResponse{
		{Sequence: 4, Status: "failed", RunID: timedOut.String()},
		{Sequence: 3, Status: "failed", RunID: failed.String()},
		{Sequence: 0, Status: "success", RunID: succeeded.String()},
		{Sequence: 1, Status: "failed", RunID: casualty.String()},
		{Sequence: 1, Status: "failed"},
		{Sequence: 2, Status: "failed", RunID: notVisible.String()},
	}}
	svc, record, actorID := attachedWorkflowService(t, run, runtimeSvc)

	resp, err := svc.GetExecution(context.Background(), testPrincipal(actorID), record.ExternalRequestID.String())
	if err != nil || resp.Status != "failed" || resp.ErrorCode != "WORKFLOW_EXECUTION_FAILED" ||
		resp.ErrorMessage != "Workflow execution failed." || resp.FailureCategory != FailureCategoryAgentError {
		t.Fatalf("GetExecution() = %#v, %v", resp, err)
	}

	// A Start replay reports only the status and must not read child runs.
	runtimeSvc.getCalls = 0
	start, complete, err := svc.responseForReservedRecord(context.Background(), record)
	if err != nil || !complete || start.Status != "failed" || runtimeSvc.getCalls != 0 {
		t.Fatalf("Start replay = %#v, %v, %v; child reads = %d", start, complete, err, runtimeSvc.getCalls)
	}

	run.Steps = []workflow.WorkflowRunStepResponse{{Sequence: 0, Status: "failed"}, {Sequence: 1, Status: "failed", RunID: casualty.String()}}
	resp, err = svc.GetExecution(context.Background(), testPrincipal(actorID), record.ExternalRequestID.String())
	if err != nil || resp.FailureCategory != FailureCategoryUnknown {
		t.Fatalf("no terminal child = %#v, %v", resp, err)
	}

	run.Steps = []workflow.WorkflowRunStepResponse{{Sequence: 0, Status: "failed", RunID: timedOut.String()}}
	resp, err = svc.GetExecution(context.Background(), testPrincipal(actorID), record.ExternalRequestID.String())
	if err != nil || resp.FailureCategory != FailureCategoryTimeout {
		t.Fatalf("timed-out child = %#v, %v", resp, err)
	}

	broken := uuid.New()
	runtimeSvc.runErrs[broken] = httpx.Internal("查询调用记录失败")
	run.Steps = []workflow.WorkflowRunStepResponse{{Sequence: 0, Status: "failed", RunID: broken.String()}}
	if resp, err = svc.GetExecution(context.Background(), testPrincipal(actorID), record.ExternalRequestID.String()); err == nil {
		t.Fatalf("transient child read error must not record a category: %#v", resp)
	}

	runtimeSvc.getCalls = 0
	run.Status = "canceled"
	resp, err = svc.GetExecution(context.Background(), testPrincipal(actorID), record.ExternalRequestID.String())
	if err != nil || resp.Status != "failed" || resp.ErrorCode != "EXECUTION_CANCELED" || resp.FailureCategory != FailureCategoryCanceled || runtimeSvc.getCalls != 0 {
		t.Fatalf("canceled workflow = %#v, %v (child reads %d)", resp, err, runtimeSvc.getCalls)
	}

	run.Status = "exploded"
	resp, err = svc.GetExecution(context.Background(), testPrincipal(actorID), record.ExternalRequestID.String())
	if err != nil || resp.ErrorCode != "EXECUTION_STATUS_UNKNOWN" || resp.FailureCategory != FailureCategoryUnknown {
		t.Fatalf("unknown workflow status = %#v, %v", resp, err)
	}

	run.Status = "success"
	resp, err = svc.GetExecution(context.Background(), testPrincipal(actorID), record.ExternalRequestID.String())
	if err != nil || resp.Status != "succeeded" || resp.FailureCategory != "" {
		t.Fatalf("succeeded workflow = %#v, %v", resp, err)
	}
}

func TestCancellationStatusFailureCategory(t *testing.T) {
	cases := []struct {
		state, wantStatus, wantCode, wantCategory string
	}{
		{"unconfirmed", "failed", "CANCEL_UNCONFIRMED", FailureCategoryResultUnknown},
		{"stopped", "canceled", "", ""},
		{"requested", "canceling", "", ""},
		{"stopping", "canceling", "", ""},
		{"not_applied", "failed", "EXECUTION_FAILED", FailureCategoryAgentError},
	}
	for _, tc := range cases {
		t.Run(tc.state, func(t *testing.T) {
			svc, _, requestID, _ := attachedRunService(t, &runtime.RunResponse{
				Status: "failed", ErrorCode: "HANDLER_ERROR", StartedAt: time.Now(),
			})
			record := svc.store.(*memoryStore).records[executionStoreKey("openlinker-cloud", requestID)]
			resp, err := svc.cancellationStatusResponse(context.Background(), &record, CancellationRecord{
				ID: uuid.New(), ExternalRequestID: requestID, State: tc.state, ReasonCode: "CALLER_REQUESTED", RequestedAt: time.Now(),
			})
			if err != nil || resp.Status != tc.wantStatus || resp.ErrorCode != tc.wantCode || resp.FailureCategory != tc.wantCategory {
				t.Fatalf("cancellationStatusResponse(%s) = %#v, %v", tc.state, resp, err)
			}
		})
	}

	svc, _, requestID, _ := attachedRunService(t, &runtime.RunResponse{Status: "running", StartedAt: time.Now()})
	resp, err := svc.cancellationStatusResponse(context.Background(), nil, CancellationRecord{
		ID: uuid.New(), ExternalRequestID: requestID, State: "unconfirmed", ReasonCode: "CALLER_REQUESTED", RequestedAt: time.Now(),
	})
	if err != nil || resp.Status != "failed" || resp.FailureCategory != FailureCategoryResultUnknown {
		t.Fatalf("unattached unconfirmed cancellation = %#v, %v", resp, err)
	}
}
