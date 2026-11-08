package externalexecution

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

// boundedListingFixture is an executable Agent whose capability caps prompt at
// 4000 characters, listed with a stricter 10-character bound.
func boundedListingFixture(t *testing.T) (*Service, *fakeRuntimeService, *memoryStore, uuid.UUID, json.RawMessage, string) {
	t.Helper()
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
	runID := uuid.New()
	runtimeSvc := &fakeRuntimeService{
		startResponse: &runtime.RunResponse{RunID: runID.String(), Status: "running", StartedAt: time.Now()},
		getResponse:   &runtime.RunResponse{RunID: runID.String(), Status: "running", StartedAt: time.Now()},
	}
	store := newMemoryStore(ownerID)
	svc := NewService(agents, runtimeSvc, &fakeWorkflowService{}, store)
	listing := json.RawMessage(`{"type":"object","properties":{"prompt":{"type":"string","maxLength":10}},"required":["prompt"]}`)
	validation, err := svc.ValidateTarget(context.Background(), testPrincipal(ownerID), &TargetValidationRequest{
		TargetType: TargetTypeAgent, TargetID: targetID.String(), InputSchema: listing,
	})
	if err != nil || !validation.Executable || validation.UnavailableDetail != nil {
		t.Fatalf("bounded listing must now be executable: %#v, %v", validation, err)
	}
	return svc, runtimeSvc, store, targetID, listing, validation.ContractHash
}

func boundedRequest(targetID uuid.UUID, listing json.RawMessage, contractHash, prompt string) *ExecutionRequest {
	return &ExecutionRequest{
		ExternalRequestID: uuid.NewString(), TargetType: TargetTypeAgent, TargetID: targetID.String(),
		Input: map[string]interface{}{"prompt": prompt}, TraceID: "trace-bounded",
		ExpectedContractHash: contractHash, InputSchema: listing,
	}
}

func TestStartExecutionEnforcesListingBoundsBeforeReserving(t *testing.T) {
	svc, runtimeSvc, store, targetID, listing, contractHash := boundedListingFixture(t)
	principal := testPrincipal(uuid.New())

	started, err := svc.StartExecution(context.Background(), principal, boundedRequest(targetID, listing, contractHash, "héllo wörl"))
	if err != nil || started.Status != "running" || runtimeSvc.startCalls != 1 {
		t.Fatalf("in-bound start = %#v, %v (start calls %d)", started, err, runtimeSvc.startCalls)
	}

	violating := boundedRequest(targetID, listing, contractHash, "eleven-runes")
	_, err = svc.StartExecution(context.Background(), principal, violating)
	requireInputInvalid(t, err, executionInputSourceInputSchema, "prompt", SchemaRuleMaxLength)
	if strings.Contains(err.Error(), "eleven-runes") {
		t.Fatalf("rejection echoes input: %v", err)
	}
	requestID := uuid.MustParse(violating.ExternalRequestID)
	if _, reserved := store.records[executionStoreKey(principal.CallerServiceID, requestID)]; reserved {
		t.Fatal("a rejected input must not reserve the external_request_id")
	}
	if _, keyErr := store.GetKey(context.Background(), principal.CallerServiceID, requestID); keyErr == nil {
		t.Fatal("a rejected input must not create an identity key")
	}
	if runtimeSvc.startCalls != 1 {
		t.Fatalf("Runtime start calls = %d, want 1", runtimeSvc.startCalls)
	}

	// The same request with a fixed input is a new, valid request.
	violating.Input = map[string]interface{}{"prompt": "ok"}
	if _, err := svc.StartExecution(context.Background(), principal, violating); err != nil {
		t.Fatalf("corrected start = %v", err)
	}
}

func TestStartExecutionInputEnforcementKeepsExistingRecords(t *testing.T) {
	svc, runtimeSvc, store, targetID, listing, contractHash := boundedListingFixture(t)
	principal := testPrincipal(uuid.New())
	ownerID := store.defaultOwner

	// An execution attached before enforcement existed still replays.
	attachedReq := boundedRequest(targetID, listing, contractHash, "longer than ten runes")
	attached := currentExecutionRecord(t, principal, attachedReq)
	runID, kind := uuid.MustParse(runtimeSvc.getResponse.RunID), "run"
	attached.StartState, attached.ExecutionID, attached.ExecutionKind = startStateAttached, &runID, &kind
	attached.AuthorizedTargetOwnerID = &ownerID
	store.records[executionStoreKey(principal.CallerServiceID, attached.ExternalRequestID)] = attached
	replay, err := svc.StartExecution(context.Background(), principal, attachedReq)
	if err != nil || replay.ExecutionID != runID.String() {
		t.Fatalf("attached replay = %#v, %v", replay, err)
	}

	// A record reserved before enforcement but never authorized is checked
	// before authorization and stays untouched.
	pendingReq := boundedRequest(targetID, listing, contractHash, "longer than ten runes")
	pending := currentExecutionRecord(t, principal, pendingReq)
	store.records[executionStoreKey(principal.CallerServiceID, pending.ExternalRequestID)] = pending
	_, err = svc.StartExecution(context.Background(), principal, pendingReq)
	requireInputInvalid(t, err, executionInputSourceInputSchema, "prompt", SchemaRuleMaxLength)
	if stored := store.records[executionStoreKey(principal.CallerServiceID, pending.ExternalRequestID)]; stored.StartState != startStatePending || stored.StartToken != nil {
		t.Fatalf("pending record changed: %#v", stored)
	}

	// A semantically different reuse of an existing id is still a conflict.
	reuse := *attachedReq
	reuse.Input = map[string]interface{}{"prompt": "also way too long"}
	_, err = svc.StartExecution(context.Background(), principal, &reuse)
	assertHTTPStatus(t, err, http.StatusConflict)
	if runtimeSvc.startCalls != 0 {
		t.Fatalf("Runtime start calls = %d, want 0", runtimeSvc.startCalls)
	}
}

func TestStartExecutionInputRejectionKeepsIdentityAnswers(t *testing.T) {
	svc, _, store, targetID, listing, contractHash := boundedListingFixture(t)
	principal := testPrincipal(uuid.New())
	state := memoryCancellationState(store)

	canceled := boundedRequest(targetID, listing, contractHash, "longer than ten runes")
	canceledID := uuid.MustParse(canceled.ExternalRequestID)
	key := executionStoreKey(principal.CallerServiceID, canceledID)
	state.keys[key] = ExecutionKey{CallerServiceID: principal.CallerServiceID, ExternalRequestID: canceledID, ActorUserID: principal.ActorUserID}
	state.cancellations[key] = CancellationRecord{ID: uuid.New(), CallerServiceID: principal.CallerServiceID, ExternalRequestID: canceledID, ActorUserID: principal.ActorUserID, State: "stopped", ReasonCode: "CALLER_REQUESTED", RequestedAt: time.Now()}
	_, err := svc.StartExecution(context.Background(), principal, canceled)
	assertHTTPCode(t, err, httpx.ErrorCode("EXTERNAL_EXECUTION_CANCELED"))

	foreign := boundedRequest(targetID, listing, contractHash, "longer than ten runes")
	foreignID := uuid.MustParse(foreign.ExternalRequestID)
	state.keys[executionStoreKey(principal.CallerServiceID, foreignID)] = ExecutionKey{CallerServiceID: principal.CallerServiceID, ExternalRequestID: foreignID, ActorUserID: uuid.New()}
	_, err = svc.StartExecution(context.Background(), principal, foreign)
	assertHTTPStatus(t, err, http.StatusConflict)
}

// Runtime rejects input that fails the capability before it creates a Run.
// That must reach the caller as a final 4xx, not a retried 503.
func TestStartExecutionRuntimeInputRejectionIsTerminal(t *testing.T) {
	svc, runtimeSvc, store, targetID, listing, contractHash := boundedListingFixture(t)
	principal := testPrincipal(uuid.New())
	runtimeSvc.startErr = &httpx.HTTPError{
		Status: http.StatusUnprocessableEntity, Code: httpx.ErrorCode("RUN_INPUT_SCHEMA_MISMATCH"), Message: "input 不匹配 Agent input_schema",
		Details: map[string]interface{}{"path": "input.buyer-secret-key", "reason": "additional_property"},
	}
	req := boundedRequest(targetID, listing, contractHash, "ok")
	req.Input["buyer-secret-key"] = "x"

	_, err := svc.StartExecution(context.Background(), principal, req)
	requireInputInvalid(t, err, executionInputSourceCapability, "", SchemaRuleAdditionalProperties)
	if strings.Contains(err.Error(), "buyer-secret") {
		t.Fatalf("rejection echoes input: %v", err)
	}
	recordKey := executionStoreKey(principal.CallerServiceID, uuid.MustParse(req.ExternalRequestID))
	if stored := store.records[recordKey]; stored.StartState != startStateLaunching || stored.ExecutionID != nil || runtimeSvc.startCalls != 1 {
		t.Fatalf("after rejection record=%#v start calls=%d", stored, runtimeSvc.startCalls)
	}

	// Once the launch lease lapses a retry re-launches and gets the same
	// final answer instead of an internal error.
	stored := store.records[recordKey]
	expired := time.Now().Add(-time.Second)
	stored.StartLeaseUntil = &expired
	store.records[recordKey] = stored
	_, err = svc.StartExecution(context.Background(), principal, req)
	requireInputInvalid(t, err, executionInputSourceCapability, "", SchemaRuleAdditionalProperties)
	if runtimeSvc.startCalls != 2 {
		t.Fatalf("Runtime start calls = %d, want a single re-launch", runtimeSvc.startCalls)
	}

	// Other Runtime failures stay transient.
	runtimeSvc.startErr = httpx.Internal("db down")
	other := boundedRequest(targetID, listing, contractHash, "ok")
	_, err = svc.StartExecution(context.Background(), principal, other)
	assertHTTPStatus(t, err, http.StatusServiceUnavailable)
}
