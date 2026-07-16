package externalexecution

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	"github.com/OpenLinker-ai/openlinker-core/pkg/executioncontract"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/OpenLinker-ai/openlinker-core/pkg/workflow"
)

func TestValidateTargetRequiresPublicCallableOwnedAgent(t *testing.T) {
	ownerID := uuid.New()
	targetID := uuid.New()
	agents := &fakeAgentService{response: &agent.AgentResponse{
		ID: targetID.String(), Name: "Resume editor", LifecycleStatus: "active", Visibility: "public",
		Readiness: &agent.Readiness{Callable: true},
	}}
	svc := newTestService(agents, ownerID)

	resp, err := svc.ValidateTarget(context.Background(), testPrincipal(ownerID), &TargetValidationRequest{
		TargetType: TargetTypeAgent, TargetID: targetID.String(),
	})
	if err != nil || !resp.Executable || resp.TargetName != "Resume editor" {
		t.Fatalf("ValidateTarget() = %#v, %v", resp, err)
	}
	resp, err = svc.ValidateTarget(context.Background(), testPrincipal(uuid.New()), &TargetValidationRequest{
		TargetType: TargetTypeAgent, TargetID: targetID.String(),
	})
	if err != nil || resp.Executable || resp.UnavailableReason != "not_found" || resp.TargetName != "" {
		t.Fatalf("non-owner ValidateTarget() = %#v, %v", resp, err)
	}

	agents.response.Visibility = "private"
	resp, err = svc.ValidateTarget(context.Background(), testPrincipal(ownerID), &TargetValidationRequest{
		TargetType: TargetTypeAgent, TargetID: targetID.String(),
	})
	if err != nil || resp.Executable || resp.UnavailableReason != "not_public" {
		t.Fatalf("private ValidateTarget() = %#v, %v", resp, err)
	}
	if resp.ContractHash != "" {
		t.Fatalf("private target leaked contract hash %q", resp.ContractHash)
	}

	agents.err = httpx.NotFound("secret owner detail")
	resp, err = svc.ValidateTarget(context.Background(), testPrincipal(ownerID), &TargetValidationRequest{
		TargetType: TargetTypeAgent, TargetID: targetID.String(),
	})
	if err != nil || resp.Executable || resp.UnavailableReason != "not_found" {
		t.Fatalf("unowned ValidateTarget() = %#v, %v", resp, err)
	}
}

func TestValidateTargetChecksListingInputAgainstAgentCapability(t *testing.T) {
	ownerID, targetID := uuid.New(), uuid.New()
	agents := callableAgent(targetID)
	agents.onboarding = &agent.OnboardingResponse{Capability: &agent.CapabilityResponse{InputSchema: map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"topic": map[string]interface{}{"type": "string"},
		},
		"required":             []interface{}{"topic"},
		"additionalProperties": false,
	}}}
	svc := newTestService(agents, ownerID)

	resp, err := svc.ValidateTarget(context.Background(), testPrincipal(ownerID), &TargetValidationRequest{
		TargetType: TargetTypeAgent, TargetID: targetID.String(),
		InputSchema: json.RawMessage(`{"type":"object","properties":{"topic":{"type":"string"}},"required":["topic"]}`),
	})
	if err != nil || !resp.Executable {
		t.Fatalf("compatible ValidateTarget() = %#v, %v", resp, err)
	}
	if !executioncontract.Valid(resp.ContractHash) {
		t.Fatalf("compatible target contract hash = %q", resp.ContractHash)
	}

	resp, err = svc.ValidateTarget(context.Background(), testPrincipal(ownerID), &TargetValidationRequest{
		TargetType: TargetTypeAgent, TargetID: targetID.String(),
		InputSchema: json.RawMessage(`{"type":"object","properties":{"topic":{"type":"number"}},"required":["topic"]}`),
	})
	if err != nil || resp.Executable || resp.UnavailableReason != "input_schema_incompatible" {
		t.Fatalf("incompatible ValidateTarget() = %#v, %v", resp, err)
	}
	if resp.ContractHash != "" {
		t.Fatalf("incompatible target leaked contract hash %q", resp.ContractHash)
	}
}

func TestStartExecutionIsIdempotentAndRejectsSemanticReuse(t *testing.T) {
	requestID, actorID, ownerID, targetID, runID := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	agents := callableAgent(targetID)
	runtimeSvc := &fakeRuntimeService{
		startResponse: &runtime.RunResponse{RunID: runID.String(), Status: "running", StartedAt: time.Now()},
		getResponse:   &runtime.RunResponse{RunID: runID.String(), Status: "running", StartedAt: time.Now()},
	}
	svc := NewService(agents, runtimeSvc, &fakeWorkflowService{}, newMemoryStore(ownerID))
	inputSchema := json.RawMessage(`{"type":"object","properties":{}}`)
	validation, err := svc.ValidateTarget(context.Background(), testPrincipal(ownerID), &TargetValidationRequest{
		TargetType: TargetTypeAgent, TargetID: targetID.String(), InputSchema: inputSchema,
	})
	if err != nil || !executioncontract.Valid(validation.ContractHash) {
		t.Fatalf("ValidateTarget contract = %#v, %v", validation, err)
	}
	req := &ExecutionRequest{
		ExternalRequestID: requestID.String(), TargetType: TargetTypeAgent, TargetID: targetID.String(), Input: map[string]interface{}{"topic": "Go"}, TraceID: "trace-1",
		Metadata:             map[string]interface{}{"external_order_id": requestID.String(), "seller_user_id": ownerID.String()},
		ExpectedContractHash: validation.ContractHash, InputSchema: inputSchema,
	}

	first, err := svc.StartExecution(context.Background(), testPrincipal(actorID), req)
	if err != nil || first.ExecutionID != runID.String() || first.Status != "running" {
		t.Fatalf("first StartExecution() = %#v, %v", first, err)
	}
	second, err := svc.StartExecution(context.Background(), testPrincipal(actorID), req)
	if err != nil || second.ExecutionID != runID.String() {
		t.Fatalf("second StartExecution() = %#v, %v", second, err)
	}
	if runtimeSvc.startCalls != 1 {
		t.Fatalf("StartRun calls = %d, want 1", runtimeSvc.startCalls)
	}
	if len(runtimeSvc.startActors) != 1 || runtimeSvc.startActors[0] != actorID {
		t.Fatalf("StartRun actors = %v, want signed actor %s", runtimeSvc.startActors, actorID)
	}
	if got := runtimeSvc.startRequests[0].Metadata["caller_service_id"]; got != "openlinker-cloud" {
		t.Fatalf("caller_service_id metadata = %#v", got)
	}
	if got := runtimeSvc.startRequests[0].Metadata["external_order_id"]; got != requestID.String() {
		t.Fatalf("forwarded metadata external_order_id = %#v", got)
	}
	if got := runtimeSvc.startRequests[0].Metadata["seller_user_id"]; got != ownerID.String() {
		t.Fatalf("forwarded metadata seller_user_id = %#v", got)
	}
	if _, leaked := runtimeSvc.startRequests[0].Metadata["target_owner_user_id"]; leaked {
		t.Fatal("transient target owner must not be persisted in runtime metadata")
	}

	mutated := *req
	mutated.Input = map[string]interface{}{"topic": "Rust"}
	_, err = svc.StartExecution(context.Background(), testPrincipal(actorID), &mutated)
	assertHTTPStatus(t, err, 409)

	mutated = *req
	mutated.ExpectedContractHash = "hct:v1:" + strings.Repeat("b", 64)
	_, err = svc.StartExecution(context.Background(), testPrincipal(actorID), &mutated)
	assertHTTPStatus(t, err, 409)

	mutated = *req
	mutated.InputSchema = json.RawMessage(`{"type":"object","properties":{"topic":{"type":"string"}}}`)
	_, err = svc.StartExecution(context.Background(), testPrincipal(actorID), &mutated)
	assertHTTPStatus(t, err, 409)

	mutated = *req
	mutated.Metadata = map[string]interface{}{"external_order_id": requestID.String(), "seller_user_id": uuid.NewString()}
	_, err = svc.StartExecution(context.Background(), testPrincipal(actorID), &mutated)
	assertHTTPStatus(t, err, 409)

	_, err = svc.StartExecution(context.Background(), testPrincipal(uuid.New()), req)
	assertHTTPStatus(t, err, 409)

	otherCaller := &Principal{CallerServiceID: "other-service", ActorUserID: actorID}
	_, err = svc.StartExecution(context.Background(), otherCaller, req)
	if err != nil {
		t.Fatalf("same external request id from another verified caller must be isolated: %v", err)
	}
	if runtimeSvc.startCalls != 2 {
		t.Fatalf("StartRun calls after caller isolation = %d, want 2", runtimeSvc.startCalls)
	}
	if runtimeSvc.startRequests[0].IdempotencyKey == runtimeSvc.startRequests[1].IdempotencyKey {
		t.Fatalf("runtime idempotency keys must be caller-isolated: %q", runtimeSvc.startRequests[0].IdempotencyKey)
	}
}

func TestStartExecutionRejectsProtectedAndUnboundedMetadataBeforeMutation(t *testing.T) {
	actorID, targetID := uuid.New(), uuid.New()
	base := &ExecutionRequest{
		ExternalRequestID: uuid.NewString(), TargetType: TargetTypeAgent, TargetID: targetID.String(),
		Input: map[string]interface{}{}, TraceID: "trace-metadata",
		ExpectedContractHash: "hct:v1:" + strings.Repeat("a", 64),
		InputSchema:          json.RawMessage(`{"type":"object"}`),
	}
	runtimeSvc := &fakeRuntimeService{}
	svc := NewService(callableAgent(targetID), runtimeSvc, &fakeWorkflowService{}, newMemoryStore(uuid.New()))

	for _, protected := range []string{"external_request_id", "caller_service_id", "trace_id"} {
		req := *base
		req.Metadata = map[string]interface{}{protected: "spoofed"}
		_, err := svc.StartExecution(context.Background(), testPrincipal(actorID), &req)
		assertHTTPStatus(t, err, http.StatusBadRequest)
	}

	req := *base
	req.Metadata = map[string]interface{}{"payload": strings.Repeat("x", maxExternalMetadataBytes)}
	_, err := svc.StartExecution(context.Background(), testPrincipal(actorID), &req)
	assertHTTPStatus(t, err, http.StatusBadRequest)

	if runtimeSvc.startCalls != 0 {
		t.Fatalf("invalid metadata started %d runs", runtimeSvc.startCalls)
	}
}

func TestNormalizeExternalMetadataEnforcesCanonicalBounds(t *testing.T) {
	maxKey := "a" + strings.Repeat("b", maxExternalMetadataKey-1)
	if _, err := normalizeExternalMetadata(map[string]interface{}{maxKey: "ok"}); err != nil {
		t.Fatalf("64-byte metadata key rejected: %v", err)
	}
	for name, metadata := range map[string]map[string]interface{}{
		"key too long":   {maxKey + "c": "bad"},
		"control":        {"bad\nkey": "bad"},
		"reserved shape": {"__proto__": "bad"},
		"invalid I-JSON": {"value": math.Inf(1)},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := normalizeExternalMetadata(metadata)
			assertHTTPStatus(t, err, http.StatusBadRequest)
		})
	}

	tooMany := make(map[string]interface{}, maxExternalMetadataKeys+1)
	for i := 0; i <= maxExternalMetadataKeys; i++ {
		tooMany["k"+strconv.Itoa(i)] = i
	}
	_, err := normalizeExternalMetadata(tooMany)
	assertHTTPStatus(t, err, http.StatusBadRequest)

	canonicalObjectOverhead := len(`{"a":""}`)
	exact := map[string]interface{}{"a": strings.Repeat("x", maxExternalMetadataBytes-canonicalObjectOverhead)}
	if _, err := normalizeExternalMetadata(exact); err != nil {
		t.Fatalf("exact metadata byte limit rejected: %v", err)
	}
	exact["a"] = exact["a"].(string) + "x"
	_, err = normalizeExternalMetadata(exact)
	assertHTTPStatus(t, err, http.StatusBadRequest)
}

func TestStartExecutionReturnsAttachedReplayAfterTargetDeletion(t *testing.T) {
	requestID, actorID, ownerID, targetID, runID := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	store := newMemoryStore(ownerID)
	runtimeSvc := &fakeRuntimeService{
		startResponse: &runtime.RunResponse{RunID: runID.String(), Status: "running", StartedAt: time.Now()},
		getResponse:   &runtime.RunResponse{RunID: runID.String(), Status: "running", StartedAt: time.Now()},
	}
	svc := NewService(callableAgent(targetID), runtimeSvc, &fakeWorkflowService{}, store)
	validation, err := svc.ValidateTarget(context.Background(), testPrincipal(ownerID), &TargetValidationRequest{
		TargetType: TargetTypeAgent, TargetID: targetID.String(), InputSchema: json.RawMessage(`{"type":"object","properties":{}}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	req := &ExecutionRequest{
		ExternalRequestID: requestID.String(), TargetType: TargetTypeAgent, TargetID: targetID.String(), Input: map[string]interface{}{"topic": "Go"},
		TraceID: "trace-deleted-target", ExpectedContractHash: validation.ContractHash, InputSchema: json.RawMessage(`{"type":"object","properties":{}}`),
	}
	if _, err := svc.StartExecution(context.Background(), testPrincipal(actorID), req); err != nil {
		t.Fatal(err)
	}
	resolvesBeforeReplay := store.resolveCalls
	store.defaultOwner = uuid.Nil
	replayed, err := svc.StartExecution(context.Background(), testPrincipal(actorID), req)
	if err != nil || replayed.ExecutionID != runID.String() || replayed.Status != "running" {
		t.Fatalf("deleted-target replay = %#v, %v", replayed, err)
	}
	if store.resolveCalls != resolvesBeforeReplay || runtimeSvc.startCalls != 1 {
		t.Fatalf("replay resolved target %d extra times or started %d runs", store.resolveCalls-resolvesBeforeReplay, runtimeSvc.startCalls)
	}
}

func TestStartExecutionRecoversUnattachedRunBeforeDeletedTargetLookup(t *testing.T) {
	requestID, actorID, targetID, runID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	req := &ExecutionRequest{
		ExternalRequestID: requestID.String(), TargetType: TargetTypeAgent, TargetID: targetID.String(),
		Input: map[string]interface{}{"topic": "Go"}, Metadata: map[string]interface{}{"order_ref": "stable"},
		TraceID: "trace-unattached-run", ExpectedContractHash: "hct:v1:" + strings.Repeat("a", 64),
		InputSchema: json.RawMessage(`{"type":"object"}`),
	}
	store := newMemoryStore()
	store.defaultOwner = uuid.Nil
	store.records[executionStoreKey("openlinker-cloud", requestID)] = currentExecutionRecord(t, testPrincipal(actorID), req)
	runtimeSvc := &fakeRuntimeService{
		lookupResponse: &runtime.RunResponse{RunID: runID.String(), Status: "running", StartedAt: time.Now()},
		lookupFound:    true,
		getResponse:    &runtime.RunResponse{RunID: runID.String(), Status: "running", StartedAt: time.Now()},
	}
	svc := NewService(&fakeAgentService{err: httpx.NotFound("deleted")}, runtimeSvc, &fakeWorkflowService{}, store)

	response, err := svc.StartExecution(context.Background(), testPrincipal(actorID), req)
	if err != nil || response.ExecutionID != runID.String() || response.Status != "running" {
		t.Fatalf("unattached Run recovery = %#v, %v", response, err)
	}
	if runtimeSvc.lookupCalls != 1 || runtimeSvc.startCalls != 0 || store.resolveCalls != 0 {
		t.Fatalf("lookup/start/target resolves = %d/%d/%d, want 1/0/0", runtimeSvc.lookupCalls, runtimeSvc.startCalls, store.resolveCalls)
	}
	if got := runtimeSvc.lookupRequests[0].Metadata["external_request_id"]; got != requestID.String() {
		t.Fatalf("recovery lookup metadata = %#v", runtimeSvc.lookupRequests[0].Metadata)
	}
}

func TestStartExecutionRecoversUnattachedWorkflowBeforeContractValidation(t *testing.T) {
	requestID, actorID, workflowID, workflowRunID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	req := &ExecutionRequest{
		ExternalRequestID: requestID.String(), TargetType: TargetTypeWorkflow, TargetID: workflowID.String(),
		Input: map[string]interface{}{"topic": "Go"}, TraceID: "trace-unattached-workflow",
		ExpectedContractHash: "hct:v1:" + strings.Repeat("b", 64), InputSchema: json.RawMessage(`{"type":"object"}`),
	}
	store := newMemoryStore()
	store.defaultOwner = uuid.Nil
	store.records[executionStoreKey("openlinker-cloud", requestID)] = currentExecutionRecord(t, testPrincipal(actorID), req)
	workflows := &fakeWorkflowService{
		lookup:      &workflow.WorkflowRunResponse{ID: workflowRunID.String(), WorkflowID: workflowID.String(), Status: "running"},
		lookupFound: true,
		getResponse: &workflow.WorkflowRunResponse{ID: workflowRunID.String(), WorkflowID: workflowID.String(), Status: "running"},
		validation:  &WorkflowTargetValidation{Executable: false, UnavailableReason: "not_found"},
	}
	svc := NewService(callableAgent(uuid.New()), &fakeRuntimeService{}, workflows, store)

	response, err := svc.StartExecution(context.Background(), testPrincipal(actorID), req)
	if err != nil || response.ExecutionID != workflowRunID.String() || response.Status != "running" {
		t.Fatalf("unattached Workflow recovery = %#v, %v", response, err)
	}
	if workflows.lookupCalls != 1 || workflows.startCalls != 0 || store.resolveCalls != 0 {
		t.Fatalf("lookup/start/target resolves = %d/%d/%d, want 1/0/0", workflows.lookupCalls, workflows.startCalls, store.resolveCalls)
	}
}

func TestStartExecutionSerializesMutableValidationAndReplaysWinner(t *testing.T) {
	requestID, actorID, ownerID, targetID, runID := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	agents := callableAgent(targetID)
	store := newMemoryStore(ownerID)
	runtimeSvc := &fakeRuntimeService{
		startResponse: &runtime.RunResponse{RunID: runID.String(), Status: "running", StartedAt: time.Now()},
		getResponse:   &runtime.RunResponse{RunID: runID.String(), Status: "running", StartedAt: time.Now()},
	}
	svc := NewService(agents, runtimeSvc, &fakeWorkflowService{}, store)
	schema := json.RawMessage(`{"type":"object"}`)
	validation, err := svc.ValidateTarget(context.Background(), testPrincipal(ownerID), &TargetValidationRequest{
		TargetType: TargetTypeAgent, TargetID: targetID.String(), InputSchema: schema,
	})
	if err != nil || !validation.Executable {
		t.Fatalf("ValidateTarget() = %#v, %v", validation, err)
	}
	req := &ExecutionRequest{
		ExternalRequestID: requestID.String(), TargetType: TargetTypeAgent, TargetID: targetID.String(),
		Input: map[string]interface{}{"topic": "serialized"}, TraceID: "trace-serialized-starter",
		ExpectedContractHash: validation.ContractHash, InputSchema: schema,
	}
	agents.blockStarted = make(chan struct{})
	agents.blockRelease = make(chan struct{})
	type startResult struct {
		response *ExecutionStartResponse
		err      error
	}
	firstResult := make(chan startResult, 1)
	go func() {
		response, startErr := svc.StartExecution(context.Background(), testPrincipal(actorID), req)
		firstResult <- startResult{response: response, err: startErr}
	}()
	<-agents.blockStarted

	secondResponse, secondErr := svc.StartExecution(context.Background(), testPrincipal(actorID), req)
	if secondResponse != nil {
		t.Fatalf("concurrent starter response = %#v", secondResponse)
	}
	assertHTTPStatus(t, secondErr, http.StatusServiceUnavailable)
	if runtimeSvc.startCalls != 0 {
		t.Fatalf("concurrent loser started %d Runs before authorization", runtimeSvc.startCalls)
	}
	close(agents.blockRelease)
	first := <-firstResult
	if first.err != nil || first.response == nil || first.response.ExecutionID != runID.String() {
		t.Fatalf("winning starter = %#v, %v", first.response, first.err)
	}

	replayed, err := svc.StartExecution(context.Background(), testPrincipal(actorID), req)
	if err != nil || replayed.ExecutionID != runID.String() {
		t.Fatalf("winner replay = %#v, %v", replayed, err)
	}
	if runtimeSvc.startCalls != 1 || store.claimCalls != 2 {
		t.Fatalf("Run starts / starter claims = %d / %d, want 1 / 2", runtimeSvc.startCalls, store.claimCalls)
	}
}

func TestStartExecutionReplaysDurableRejectionWithoutMutableLookup(t *testing.T) {
	requestID, actorID, targetID := uuid.New(), uuid.New(), uuid.New()
	store := newMemoryStore()
	store.defaultOwner = uuid.Nil
	runtimeSvc := &fakeRuntimeService{}
	svc := NewService(callableAgent(targetID), runtimeSvc, &fakeWorkflowService{}, store)
	req := &ExecutionRequest{
		ExternalRequestID: requestID.String(), TargetType: TargetTypeAgent, TargetID: targetID.String(),
		Input: map[string]interface{}{}, TraceID: "trace-durable-rejection",
		ExpectedContractHash: "hct:v1:" + strings.Repeat("a", 64), InputSchema: json.RawMessage(`{"type":"object"}`),
	}

	_, err := svc.StartExecution(context.Background(), testPrincipal(actorID), req)
	assertHTTPCode(t, err, httpx.ErrorCode("TARGET_UNAVAILABLE"))
	store.defaultOwner = uuid.New()
	_, err = svc.StartExecution(context.Background(), testPrincipal(actorID), req)
	assertHTTPCode(t, err, httpx.ErrorCode("TARGET_UNAVAILABLE"))
	if store.resolveCalls != 1 || runtimeSvc.lookupCalls != 1 || runtimeSvc.startCalls != 0 {
		t.Fatalf("durable rejection resolve/lookup/start = %d/%d/%d", store.resolveCalls, runtimeSvc.lookupCalls, runtimeSvc.startCalls)
	}
}

func TestStartExecutionSerializesRecoveryInsideStarterClaim(t *testing.T) {
	requestID, actorID, targetID, runID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	request := &ExecutionRequest{
		ExternalRequestID: requestID.String(), TargetType: TargetTypeAgent, TargetID: targetID.String(),
		Input: map[string]interface{}{}, TraceID: "trace-recovery-rejection-race",
		ExpectedContractHash: "hct:v1:" + strings.Repeat("a", 64), InputSchema: json.RawMessage(`{"type":"object"}`),
	}
	principal := testPrincipal(actorID)
	store := newMemoryStore()
	store.defaultOwner = uuid.Nil
	store.records[executionStoreKey(principal.CallerServiceID, requestID)] = currentExecutionRecord(t, principal, request)
	lookupStarted := make(chan struct{})
	releaseLookup := make(chan struct{})
	runtimeSvc := &fakeRuntimeService{
		getResponse: &runtime.RunResponse{RunID: runID.String(), Status: "running", StartedAt: time.Now()},
		lookupFunc: func(call int) (*runtime.RunResponse, bool, error) {
			if call == 1 {
				close(lookupStarted)
				<-releaseLookup
				return &runtime.RunResponse{RunID: runID.String(), Status: "running", StartedAt: time.Now()}, true, nil
			}
			return nil, false, nil
		},
	}
	svc := NewService(callableAgent(targetID), runtimeSvc, &fakeWorkflowService{}, store)

	type startResult struct {
		response *ExecutionStartResponse
		err      error
	}
	slowResult := make(chan startResult, 1)
	go func() {
		response, err := svc.StartExecution(context.Background(), principal, request)
		slowResult <- startResult{response: response, err: err}
	}()
	<-lookupStarted

	fastResponse, fastErr := svc.StartExecution(context.Background(), principal, request)
	if fastResponse != nil {
		t.Fatalf("contending starter response = %#v", fastResponse)
	}
	assertHTTPStatus(t, fastErr, http.StatusServiceUnavailable)
	close(releaseLookup)
	slow := <-slowResult
	if slow.err != nil || slow.response == nil || slow.response.ExecutionID != runID.String() {
		t.Fatalf("slow recovery response = %#v, %v", slow.response, slow.err)
	}

	record := store.records[executionStoreKey(principal.CallerServiceID, requestID)]
	if record.StartState != startStateAuthorized || record.ExecutionID == nil || *record.ExecutionID != runID ||
		record.RejectionCode != nil || runtimeSvc.lookupCalls != 1 || runtimeSvc.startCalls != 0 {
		t.Fatalf("serialized recovery state: record=%#v lookup_calls=%d start_calls=%d", record, runtimeSvc.lookupCalls, runtimeSvc.startCalls)
	}
}

func TestStartExecutionPersistsValidationHTTPRejection(t *testing.T) {
	requestID, actorID, ownerID, targetID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	agents := callableAgent(targetID)
	agents.onboardingErr = httpx.NotFound("target retired during validation")
	store := newMemoryStore(ownerID)
	runtimeSvc := &fakeRuntimeService{}
	svc := NewService(agents, runtimeSvc, &fakeWorkflowService{}, store)
	req := &ExecutionRequest{
		ExternalRequestID: requestID.String(), TargetType: TargetTypeAgent, TargetID: targetID.String(), Input: map[string]interface{}{},
		TraceID: "trace-validation-rejection", ExpectedContractHash: "hct:v1:" + strings.Repeat("d", 64), InputSchema: json.RawMessage(`{"type":"object"}`),
	}

	_, err := svc.StartExecution(context.Background(), testPrincipal(actorID), req)
	assertHTTPCode(t, err, httpx.ErrorCode("TARGET_UNAVAILABLE"))
	agents.onboardingErr = nil
	_, err = svc.StartExecution(context.Background(), testPrincipal(actorID), req)
	assertHTTPCode(t, err, httpx.ErrorCode("TARGET_UNAVAILABLE"))
	if store.resolveCalls != 1 || runtimeSvc.startCalls != 0 {
		t.Fatalf("validation rejection resolve/start = %d/%d", store.resolveCalls, runtimeSvc.startCalls)
	}
}

func TestStartExecutionExpiredEvaluatorCannotLateStart(t *testing.T) {
	requestID, actorID, ownerID, targetID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	currentTime := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	store := newMemoryStore(ownerID)
	store.now = func() time.Time { return currentTime }
	agents := callableAgent(targetID)
	runtimeSvc := &fakeRuntimeService{}
	svc := NewService(agents, runtimeSvc, &fakeWorkflowService{}, store)
	schema := json.RawMessage(`{"type":"object"}`)
	validation, err := svc.ValidateTarget(context.Background(), testPrincipal(ownerID), &TargetValidationRequest{
		TargetType: TargetTypeAgent, TargetID: targetID.String(), InputSchema: schema,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := &ExecutionRequest{
		ExternalRequestID: requestID.String(), TargetType: TargetTypeAgent, TargetID: targetID.String(), Input: map[string]interface{}{},
		TraceID: "trace-expired-evaluator", ExpectedContractHash: validation.ContractHash, InputSchema: schema,
	}
	replacementToken := uuid.New()
	agents.onGet = func() {
		currentTime = currentTime.Add(startEvaluationLease + time.Second)
		_, claimed, claimErr := store.ClaimStartEvaluation(context.Background(), "openlinker-cloud", requestID, replacementToken, startEvaluationLease)
		if claimErr != nil || !claimed {
			t.Errorf("replacement claim = %v, %v", claimed, claimErr)
		}
	}

	_, err = svc.StartExecution(context.Background(), testPrincipal(actorID), request)
	assertHTTPStatus(t, err, http.StatusServiceUnavailable)
	if runtimeSvc.startCalls != 0 {
		t.Fatalf("expired evaluator started %d Runs", runtimeSvc.startCalls)
	}
	record := store.records[executionStoreKey("openlinker-cloud", requestID)]
	if record.StartState != startStateEvaluating || record.StartToken == nil || *record.StartToken != replacementToken {
		t.Fatalf("replacement evaluation state = %#v", record)
	}
}

func TestStartExecutionAuthorizedRetrySkipsExternalMutableValidation(t *testing.T) {
	requestID, actorID, ownerID, targetID, runID := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	req := &ExecutionRequest{
		ExternalRequestID: requestID.String(), TargetType: TargetTypeAgent, TargetID: targetID.String(), Input: map[string]interface{}{},
		TraceID: "trace-authorized-retry", ExpectedContractHash: "hct:v1:" + strings.Repeat("c", 64), InputSchema: json.RawMessage(`{"type":"object"}`),
	}
	principal := testPrincipal(actorID)
	record := currentExecutionRecord(t, principal, req)
	record.StartState = startStateAuthorized
	record.AuthorizedTargetOwnerID = &ownerID
	store := newMemoryStore()
	store.defaultOwner = uuid.Nil
	store.records[executionStoreKey("openlinker-cloud", requestID)] = record
	runtimeSvc := &fakeRuntimeService{
		startResponse: &runtime.RunResponse{RunID: runID.String(), Status: "running", StartedAt: time.Now()},
		getResponse:   &runtime.RunResponse{RunID: runID.String(), Status: "running", StartedAt: time.Now()},
	}
	svc := NewService(&fakeAgentService{err: httpx.NotFound("target retired")}, runtimeSvc, &fakeWorkflowService{}, store)

	response, err := svc.StartExecution(context.Background(), principal, req)
	if err != nil || response.ExecutionID != runID.String() {
		t.Fatalf("authorized retry = %#v, %v", response, err)
	}
	if store.resolveCalls != 0 || runtimeSvc.startCalls != 1 {
		t.Fatalf("authorized retry resolve/start = %d/%d", store.resolveCalls, runtimeSvc.startCalls)
	}
}

func TestStartExecutionAuthorizedDownstreamErrorRemainsTransient(t *testing.T) {
	requestID, actorID, ownerID, targetID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	req := &ExecutionRequest{
		ExternalRequestID: requestID.String(), TargetType: TargetTypeAgent, TargetID: targetID.String(), Input: map[string]interface{}{},
		TraceID: "trace-authorized-transient", ExpectedContractHash: "hct:v1:" + strings.Repeat("e", 64), InputSchema: json.RawMessage(`{"type":"object"}`),
	}
	principal := testPrincipal(actorID)
	record := currentExecutionRecord(t, principal, req)
	record.StartState = startStateAuthorized
	record.AuthorizedTargetOwnerID = &ownerID
	store := newMemoryStore()
	store.records[executionStoreKey("openlinker-cloud", requestID)] = record
	runtimeSvc := &fakeRuntimeService{}
	svc := NewService(&fakeAgentService{err: httpx.NotFound("target retired")}, runtimeSvc, &fakeWorkflowService{}, store)

	_, err := svc.StartExecution(context.Background(), principal, req)
	assertHTTPStatus(t, err, http.StatusServiceUnavailable)
	stored := store.records[executionStoreKey("openlinker-cloud", requestID)]
	if stored.StartState != startStateLaunching || stored.RejectionCode != nil || runtimeSvc.startCalls != 1 || store.resolveCalls != 0 {
		t.Fatalf("authorized transient state/start/resolve = %#v/%d/%d", stored, runtimeSvc.startCalls, store.resolveCalls)
	}
}

func TestStartExecutionAuthorizedRecoveryCollisionRemainsTransient(t *testing.T) {
	requestID, actorID, ownerID, targetID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	request := &ExecutionRequest{
		ExternalRequestID: requestID.String(), TargetType: TargetTypeAgent, TargetID: targetID.String(), Input: map[string]interface{}{},
		TraceID: "trace-authorized-recovery-collision", ExpectedContractHash: "hct:v1:" + strings.Repeat("f", 64),
		InputSchema: json.RawMessage(`{"type":"object"}`),
	}
	principal := testPrincipal(actorID)
	record := currentExecutionRecord(t, principal, request)
	record.StartState = startStateAuthorized
	record.AuthorizedTargetOwnerID = &ownerID
	store := newMemoryStore()
	store.records[executionStoreKey(principal.CallerServiceID, requestID)] = record
	runtimeSvc := &fakeRuntimeService{lookupErr: httpx.Conflict("creation identity collision")}
	svc := NewService(callableAgent(targetID), runtimeSvc, &fakeWorkflowService{}, store)

	response, err := svc.StartExecution(context.Background(), principal, request)
	if response != nil {
		t.Fatalf("authorized collision response = %#v", response)
	}
	assertHTTPStatus(t, err, http.StatusServiceUnavailable)
	stored := store.records[executionStoreKey(principal.CallerServiceID, requestID)]
	if stored.StartState != startStateAuthorized || stored.ExecutionID != nil || stored.RejectionCode != nil || runtimeSvc.startCalls != 0 {
		t.Fatalf("authorized collision became terminal: record=%#v start_calls=%d", stored, runtimeSvc.startCalls)
	}
}

func TestStartExecutionReplaysAttachedLegacyFingerprintWithoutRestart(t *testing.T) {
	requestID, actorID, targetID, runID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	kind := "run"
	contractHash := "hct:v1:" + strings.Repeat("a", 64)
	store := newMemoryStore()
	store.defaultOwner = uuid.Nil
	store.records[executionStoreKey("openlinker-cloud", requestID)] = ExecutionRecord{
		CallerServiceID: "openlinker-cloud", ExternalRequestID: requestID, RequestFingerprintVersion: legacyRequestFingerprintVersion,
		ActorUserID: actorID, TargetType: TargetTypeAgent, TargetID: targetID,
		InputFingerprint: []byte("legacy-fingerprint-cannot-be-recomputed"), ExpectedContractHash: &contractHash,
		TraceID: "trace-legacy", ExecutionKind: &kind, ExecutionID: &runID,
	}
	runtimeSvc := &fakeRuntimeService{getResponse: &runtime.RunResponse{RunID: runID.String(), Status: "running", StartedAt: time.Now()}}
	svc := NewService(callableAgent(targetID), runtimeSvc, &fakeWorkflowService{}, store)
	replayed, err := svc.StartExecution(context.Background(), testPrincipal(actorID), &ExecutionRequest{
		ExternalRequestID: requestID.String(), TargetType: TargetTypeAgent, TargetID: targetID.String(),
		Input: map[string]interface{}{"input": "not retained by the legacy record"}, TraceID: "trace-legacy",
		ExpectedContractHash: contractHash, InputSchema: json.RawMessage(`{"type":"object","properties":{"choice":{"type":"string","enum":["b","a"]}}}`),
	})
	if err != nil || replayed.ExecutionID != runID.String() {
		t.Fatalf("legacy replay = %#v, %v", replayed, err)
	}
	if runtimeSvc.startCalls != 0 || store.resolveCalls != 0 {
		t.Fatalf("legacy replay started %d runs or resolved target %d times", runtimeSvc.startCalls, store.resolveCalls)
	}
}

func TestStartExecutionKeepsLegacyRuntimeIdentityUntilAttach(t *testing.T) {
	requestID, actorID, ownerID, targetID, runID := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	store := newMemoryStore(ownerID)
	runtimeSvc := &fakeRuntimeService{
		startResponse: &runtime.RunResponse{RunID: runID.String(), Status: "running", StartedAt: time.Now()},
		getResponse:   &runtime.RunResponse{RunID: runID.String(), Status: "running", StartedAt: time.Now()},
	}
	svc := NewService(callableAgent(targetID), runtimeSvc, &fakeWorkflowService{}, store)
	validation, err := svc.ValidateTarget(context.Background(), testPrincipal(ownerID), &TargetValidationRequest{
		TargetType: TargetTypeAgent, TargetID: targetID.String(), InputSchema: json.RawMessage(`{"type":"object","properties":{}}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	contractHash := validation.ContractHash
	store.resolveCalls = 0
	store.records[executionStoreKey("openlinker-cloud", requestID)] = ExecutionRecord{
		CallerServiceID: "openlinker-cloud", ExternalRequestID: requestID, RequestFingerprintVersion: legacyRequestFingerprintVersion,
		ActorUserID: actorID, TargetType: TargetTypeAgent, TargetID: targetID,
		ExpectedContractHash: &contractHash, TraceID: "trace-incomplete-legacy",
		DownstreamReplayIdentity: legacyReplayIdentity(t, requestID, ownerID, "trace-incomplete-legacy"),
	}
	response, err := svc.StartExecution(context.Background(), testPrincipal(actorID), &ExecutionRequest{
		ExternalRequestID: requestID.String(), TargetType: TargetTypeAgent, TargetID: targetID.String(), Input: map[string]interface{}{},
		Metadata: map[string]interface{}{"external_order_id": requestID.String(), "seller_user_id": ownerID.String()},
		TraceID:  "trace-incomplete-legacy", ExpectedContractHash: contractHash, InputSchema: json.RawMessage(`{"type":"object","properties":{}}`),
	})
	if err != nil || response.ExecutionID != runID.String() {
		t.Fatalf("legacy start = %#v, %v", response, err)
	}
	attached := store.records[executionStoreKey("openlinker-cloud", requestID)]
	if attached.RequestFingerprintVersion != legacyRequestFingerprintVersion || runtimeSvc.lookupCalls != 1 || runtimeSvc.startCalls != 1 || store.resolveCalls != 1 {
		t.Fatalf("legacy record/version/lookup/start/resolve = %d/%d/%d/%d", attached.RequestFingerprintVersion, runtimeSvc.lookupCalls, runtimeSvc.startCalls, store.resolveCalls)
	}
	started := runtimeSvc.startRequests[0]
	if started.IdempotencyKey != "hosted-service-order/"+requestID.String() ||
		started.CreationProtocol != "hosted" || started.CreationMethod != "service-order.execute" {
		t.Fatalf("legacy Runtime identity = %#v", started)
	}
}

func TestStartExecutionLegacyRuntimeCollisionDoesNotAttachOrResolveTarget(t *testing.T) {
	requestID, actorID, ownerID, targetID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	contractHash := "hct:v1:" + strings.Repeat("a", 64)
	store := newMemoryStore()
	store.defaultOwner = uuid.Nil
	store.records[executionStoreKey("openlinker-cloud", requestID)] = ExecutionRecord{
		CallerServiceID: "openlinker-cloud", ExternalRequestID: requestID, RequestFingerprintVersion: legacyRequestFingerprintVersion,
		ActorUserID: actorID, TargetType: TargetTypeAgent, TargetID: targetID, ExpectedContractHash: &contractHash,
		TraceID: "trace-legacy-collision", DownstreamReplayIdentity: legacyReplayIdentity(t, requestID, ownerID, "trace-legacy-collision"),
	}
	runtimeSvc := &fakeRuntimeService{lookupErr: httpx.Conflict("idempotency key reused")}
	svc := NewService(callableAgent(targetID), runtimeSvc, &fakeWorkflowService{}, store)
	request := &ExecutionRequest{
		ExternalRequestID: requestID.String(), TargetType: TargetTypeAgent, TargetID: targetID.String(),
		Input:    map[string]interface{}{"different": "input"},
		Metadata: map[string]interface{}{"external_order_id": requestID.String(), "seller_user_id": ownerID.String()},
		TraceID:  "trace-legacy-collision", ExpectedContractHash: contractHash, InputSchema: json.RawMessage(`{"type":"object"}`),
	}
	_, err := svc.StartExecution(context.Background(), testPrincipal(actorID), request)
	assertHTTPCode(t, err, httpx.ErrorCode("DOWNSTREAM_IDENTITY_CONFLICT"))
	_, err = svc.StartExecution(context.Background(), testPrincipal(actorID), request)
	assertHTTPCode(t, err, httpx.ErrorCode("DOWNSTREAM_IDENTITY_CONFLICT"))
	record := store.records[executionStoreKey("openlinker-cloud", requestID)]
	if runtimeSvc.lookupCalls != 1 || runtimeSvc.startCalls != 0 || store.resolveCalls != 0 || record.ExecutionID != nil ||
		record.StartState != startStateRejected || record.RejectionCode == nil || *record.RejectionCode != "DOWNSTREAM_IDENTITY_CONFLICT" {
		t.Fatalf("collision lookup/start/resolve/state = %d/%d/%d/%#v", runtimeSvc.lookupCalls, runtimeSvc.startCalls, store.resolveCalls, record)
	}
}

func TestStartExecutionRejectsMalformedLegacyRuntimeReplayIdentity(t *testing.T) {
	requestID, actorID, ownerID, targetID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	contractHash := "hct:v1:" + strings.Repeat("a", 64)
	req := &ExecutionRequest{
		ExternalRequestID: requestID.String(), TargetType: TargetTypeAgent, TargetID: targetID.String(),
		Input:    map[string]interface{}{},
		Metadata: map[string]interface{}{"external_order_id": requestID.String(), "seller_user_id": ownerID.String()},
		TraceID:  "trace-malformed-legacy", ExpectedContractHash: contractHash, InputSchema: json.RawMessage(`{"type":"object"}`),
	}
	tests := []struct {
		name       string
		mutate     func(map[string]interface{})
		wantStatus int
	}{
		{name: "json null kind", mutate: func(envelope map[string]interface{}) { envelope["kind"] = nil }, wantStatus: http.StatusInternalServerError},
		{name: "missing metadata", mutate: func(envelope map[string]interface{}) { delete(envelope, "metadata") }, wantStatus: http.StatusInternalServerError},
		{name: "extra field", mutate: func(envelope map[string]interface{}) { envelope["unexpected"] = true }, wantStatus: http.StatusInternalServerError},
		{name: "non canonical protocol", mutate: func(envelope map[string]interface{}) { envelope["creation_protocol"] = "Hosted" }, wantStatus: http.StatusInternalServerError},
		{name: "metadata mismatch", mutate: func(envelope map[string]interface{}) {
			envelope["metadata"].(map[string]interface{})["trace_id"] = "different"
		}, wantStatus: http.StatusConflict},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var envelope map[string]interface{}
			if err := json.Unmarshal(legacyReplayIdentity(t, requestID, ownerID, req.TraceID), &envelope); err != nil {
				t.Fatal(err)
			}
			tt.mutate(envelope)
			encoded, err := json.Marshal(envelope)
			if err != nil {
				t.Fatal(err)
			}
			store := newMemoryStore(ownerID)
			store.records[executionStoreKey("openlinker-cloud", requestID)] = ExecutionRecord{
				CallerServiceID: "openlinker-cloud", ExternalRequestID: requestID, RequestFingerprintVersion: legacyRequestFingerprintVersion,
				ActorUserID: actorID, TargetType: TargetTypeAgent, TargetID: targetID, ExpectedContractHash: &contractHash,
				TraceID: req.TraceID, DownstreamReplayIdentity: encoded,
			}
			runtimeSvc := &fakeRuntimeService{}
			svc := NewService(callableAgent(targetID), runtimeSvc, &fakeWorkflowService{}, store)
			_, err = svc.StartExecution(context.Background(), testPrincipal(actorID), req)
			assertHTTPStatus(t, err, tt.wantStatus)
			if runtimeSvc.lookupCalls != 0 || runtimeSvc.startCalls != 0 || store.resolveCalls != 0 {
				t.Fatalf("malformed envelope lookup/start/resolve = %d/%d/%d", runtimeSvc.lookupCalls, runtimeSvc.startCalls, store.resolveCalls)
			}
		})
	}
}

func TestStartExecutionPromotesLegacyWorkflowOnlyAfterRecoveryMiss(t *testing.T) {
	requestID, actorID, ownerID, workflowID, workflowRunID := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	workflows := &fakeWorkflowService{
		start:       &workflow.WorkflowRunResponse{ID: workflowRunID.String(), WorkflowID: workflowID.String(), Status: "running"},
		getResponse: &workflow.WorkflowRunResponse{ID: workflowRunID.String(), WorkflowID: workflowID.String(), Status: "running"},
	}
	store := newMemoryStore(ownerID)
	svc := NewService(callableAgent(uuid.New()), &fakeRuntimeService{}, workflows, store)
	validation, err := svc.ValidateTarget(context.Background(), testPrincipal(ownerID), &TargetValidationRequest{
		TargetType: TargetTypeWorkflow, TargetID: workflowID.String(), InputSchema: json.RawMessage(`{"type":"object"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	contractHash := validation.ContractHash
	store.resolveCalls = 0
	store.records[executionStoreKey("openlinker-cloud", requestID)] = ExecutionRecord{
		CallerServiceID: "openlinker-cloud", ExternalRequestID: requestID, RequestFingerprintVersion: legacyRequestFingerprintVersion,
		ActorUserID: actorID, TargetType: TargetTypeWorkflow, TargetID: workflowID,
		ExpectedContractHash: &contractHash, TraceID: "trace-legacy-workflow",
	}
	response, err := svc.StartExecution(context.Background(), testPrincipal(actorID), &ExecutionRequest{
		ExternalRequestID: requestID.String(), TargetType: TargetTypeWorkflow, TargetID: workflowID.String(), Input: map[string]interface{}{},
		TraceID: "trace-legacy-workflow", ExpectedContractHash: contractHash, InputSchema: json.RawMessage(`{"type":"object"}`),
	})
	if err != nil || response.ExecutionID != workflowRunID.String() {
		t.Fatalf("legacy Workflow promotion = %#v, %v", response, err)
	}
	record := store.records[executionStoreKey("openlinker-cloud", requestID)]
	if record.RequestFingerprintVersion != currentRequestFingerprintVersion || workflows.lookupCalls != 0 || workflows.startCalls != 1 || store.resolveCalls != 1 {
		t.Fatalf("version/lookup/start/resolve = %d/%d/%d/%d", record.RequestFingerprintVersion, workflows.lookupCalls, workflows.startCalls, store.resolveCalls)
	}
}

func TestStartExecutionRejectsMissingAndChangedContractBeforeRun(t *testing.T) {
	requestID, actorID, ownerID, targetID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	agents := callableAgent(targetID)
	agents.onboarding = &agent.OnboardingResponse{Capability: &agent.CapabilityResponse{
		Version: 1, InputSchema: map[string]interface{}{"type": "object", "properties": map[string]interface{}{}},
		OutputSchema: map[string]interface{}{"type": "object"},
	}}
	runtimeSvc := &fakeRuntimeService{}
	svc := NewService(agents, runtimeSvc, &fakeWorkflowService{}, newMemoryStore(ownerID))
	schema := json.RawMessage(`{"type":"object","properties":{}}`)
	validation, err := svc.ValidateTarget(context.Background(), testPrincipal(ownerID), &TargetValidationRequest{
		TargetType: TargetTypeAgent, TargetID: targetID.String(), InputSchema: schema,
	})
	if err != nil || !validation.Executable {
		t.Fatalf("ValidateTarget() = %#v, %v", validation, err)
	}
	base := &ExecutionRequest{ExternalRequestID: requestID.String(), TargetType: TargetTypeAgent, TargetID: targetID.String(), Input: map[string]interface{}{}, TraceID: "trace-contract", InputSchema: schema}
	_, err = svc.StartExecution(context.Background(), testPrincipal(actorID), base)
	assertHTTPCode(t, err, httpx.ErrorCode("EXTERNAL_EXECUTION_CONTRACT_REQUIRED"))

	base.ExpectedContractHash = validation.ContractHash
	agents.onboarding.Capability.Version++
	_, err = svc.StartExecution(context.Background(), testPrincipal(actorID), base)
	assertHTTPCode(t, err, httpx.ErrorCode("TARGET_CONTRACT_CHANGED"))
	if runtimeSvc.startCalls != 0 {
		t.Fatalf("StartRun calls = %d, want 0", runtimeSvc.startCalls)
	}
}

func TestStartExecutionRejectsFrozenSchemaDriftBeforeRun(t *testing.T) {
	requestID, actorID, ownerID, targetID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	agents := callableAgent(targetID)
	agents.onboarding = &agent.OnboardingResponse{Capability: &agent.CapabilityResponse{
		Version:      1,
		InputSchema:  map[string]interface{}{"type": "object", "properties": map[string]interface{}{"topic": map[string]interface{}{"type": "string"}}, "required": []interface{}{"topic"}, "additionalProperties": false},
		OutputSchema: map[string]interface{}{"type": "object"},
	}}
	runtimeSvc := &fakeRuntimeService{}
	svc := NewService(agents, runtimeSvc, &fakeWorkflowService{}, newMemoryStore(ownerID))
	compatible := json.RawMessage(`{"type":"object","properties":{"topic":{"type":"string"}},"required":["topic"]}`)
	validation, err := svc.ValidateTarget(context.Background(), testPrincipal(ownerID), &TargetValidationRequest{TargetType: TargetTypeAgent, TargetID: targetID.String(), InputSchema: compatible})
	if err != nil || !validation.Executable {
		t.Fatalf("ValidateTarget() = %#v, %v", validation, err)
	}
	_, err = svc.StartExecution(context.Background(), testPrincipal(actorID), &ExecutionRequest{
		ExternalRequestID: requestID.String(), TargetType: TargetTypeAgent, TargetID: targetID.String(), Input: map[string]interface{}{"topic": 42}, TraceID: "trace-schema",
		ExpectedContractHash: validation.ContractHash,
		InputSchema:          json.RawMessage(`{"type":"object","properties":{"topic":{"type":"number"}},"required":["topic"]}`),
	})
	assertHTTPCode(t, err, httpx.ErrorCode("TARGET_CONTRACT_CHANGED"))
	if runtimeSvc.startCalls != 0 {
		t.Fatalf("StartRun calls = %d, want 0", runtimeSvc.startCalls)
	}
}

func TestStartExecutionRejectsWorkflowContractChangeBeforeRun(t *testing.T) {
	requestID, actorID, ownerID, workflowID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	initialHash, err := executioncontract.WorkflowHash(executioncontract.Workflow{ID: workflowID.String()})
	if err != nil {
		t.Fatal(err)
	}
	changedHash, err := executioncontract.WorkflowHash(executioncontract.Workflow{ID: workflowID.String(), Edges: []map[string]interface{}{{"from": "a", "to": "b"}}})
	if err != nil {
		t.Fatal(err)
	}
	workflows := &fakeWorkflowService{validation: &WorkflowTargetValidation{
		Executable: true, TargetName: "Workflow", ContractHash: initialHash,
	}}
	svc := NewService(callableAgent(uuid.New()), &fakeRuntimeService{}, workflows, newMemoryStore(ownerID))
	validation, err := svc.ValidateTarget(context.Background(), testPrincipal(ownerID), &TargetValidationRequest{
		TargetType: TargetTypeWorkflow, TargetID: workflowID.String(), InputSchema: json.RawMessage(`{"type":"object","properties":{}}`),
	})
	if err != nil || validation.ContractHash != initialHash {
		t.Fatalf("ValidateTarget() = %#v, %v", validation, err)
	}
	workflows.validation = &WorkflowTargetValidation{Executable: true, TargetName: "Workflow", ContractHash: changedHash}
	_, err = svc.StartExecution(context.Background(), testPrincipal(actorID), &ExecutionRequest{
		ExternalRequestID: requestID.String(), TargetType: TargetTypeWorkflow, TargetID: workflowID.String(), Input: map[string]interface{}{}, TraceID: "trace-workflow-contract",
		ExpectedContractHash: initialHash, InputSchema: json.RawMessage(`{"type":"object","properties":{}}`),
	})
	assertHTTPCode(t, err, httpx.ErrorCode("TARGET_CONTRACT_CHANGED"))
	if workflows.startCalls != 0 {
		t.Fatalf("StartExternalWorkflowRun calls = %d, want 0", workflows.startCalls)
	}
}

func TestGetExecutionReturnsOutputArtifactsAndSafeFailure(t *testing.T) {
	requestID, actorID, targetID, runID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	kind := "run"
	store := newMemoryStore()
	store.records[executionStoreKey("openlinker-cloud", requestID)] = ExecutionRecord{
		CallerServiceID: "openlinker-cloud", ExternalRequestID: requestID, ActorUserID: actorID,
		TargetType: TargetTypeAgent, TargetID: targetID, ExecutionKind: &kind, ExecutionID: &runID,
	}
	now := time.Now().UTC()
	runtimeSvc := &fakeRuntimeService{
		getResponse: &runtime.RunResponse{
			RunID: runID.String(), Status: "success", Output: map[string]interface{}{"answer": "done"}, StartedAt: now, FinishedAt: &now,
		},
		artifacts: map[uuid.UUID][]runtime.RunArtifactResponse{
			runID: {{ID: uuid.NewString(), RunID: runID.String(), ArtifactType: "text", Title: "Report"}},
		},
	}
	svc := NewService(callableAgent(targetID), runtimeSvc, &fakeWorkflowService{}, store)

	resp, err := svc.GetExecution(context.Background(), testPrincipal(actorID), requestID.String())
	if err != nil || resp.Status != "succeeded" || resp.Output["answer"] != "done" || len(resp.Artifacts) != 1 || resp.FinishedAt == "" {
		t.Fatalf("GetExecution success = %#v, %v", resp, err)
	}
	_, err = svc.GetExecution(context.Background(), testPrincipal(uuid.New()), requestID.String())
	assertHTTPStatus(t, err, http.StatusNotFound)
	_, err = svc.GetExecution(context.Background(), &Principal{CallerServiceID: "other-service", ActorUserID: actorID}, requestID.String())
	assertHTTPStatus(t, err, http.StatusNotFound)

	runtimeSvc.getResponse.Status = "failed"
	runtimeSvc.getResponse.ErrorCode = "ENDPOINT_SECRET"
	runtimeSvc.getResponse.ErrorMsg = "dial tcp 10.0.0.1 with bearer secret"
	resp, err = svc.GetExecution(context.Background(), testPrincipal(actorID), requestID.String())
	if err != nil || resp.Status != "failed" || resp.ErrorCode != "EXECUTION_FAILED" {
		t.Fatalf("GetExecution failed = %#v, %v", resp, err)
	}
	if resp.ErrorMessage == runtimeSvc.getResponse.ErrorMsg || resp.Output != nil {
		t.Fatalf("unsafe failure response = %#v", resp)
	}
}

func TestGetExecutionReplaysDurableStartRejection(t *testing.T) {
	requestID, actorID, targetID := uuid.New(), uuid.New(), uuid.New()
	rejectionCode := "TARGET_CONTRACT_CHANGED"
	store := newMemoryStore()
	store.records[executionStoreKey("openlinker-cloud", requestID)] = ExecutionRecord{
		CallerServiceID: "openlinker-cloud", ExternalRequestID: requestID, ActorUserID: actorID,
		TargetType: TargetTypeAgent, TargetID: targetID, StartState: startStateRejected,
		RejectionCode: &rejectionCode,
	}
	svc := NewService(callableAgent(targetID), &fakeRuntimeService{}, &fakeWorkflowService{}, store)

	_, err := svc.GetExecution(context.Background(), testPrincipal(actorID), requestID.String())
	assertHTTPCode(t, err, httpx.ErrorCode("TARGET_CONTRACT_CHANGED"))
}

func TestAttachExecutionReplaysAuthoritativeConcurrentAttachment(t *testing.T) {
	requestID, actorID, targetID := uuid.New(), uuid.New(), uuid.New()
	authoritativeID, losingID := uuid.New(), uuid.New()
	kind := "run"
	store := newMemoryStore()
	store.records[executionStoreKey("openlinker-cloud", requestID)] = ExecutionRecord{
		CallerServiceID: "openlinker-cloud", ExternalRequestID: requestID, ActorUserID: actorID,
		TargetType: TargetTypeAgent, TargetID: targetID, StartState: startStateAuthorized,
		ExecutionKind: &kind, ExecutionID: &authoritativeID,
	}
	runtimeSvc := &fakeRuntimeService{getResponse: &runtime.RunResponse{
		RunID: authoritativeID.String(), Status: "running", StartedAt: time.Now(),
	}}
	svc := NewService(callableAgent(targetID), runtimeSvc, &fakeWorkflowService{}, store)

	response, err := svc.attachExecution(context.Background(), parsedExecution{
		callerServiceID: "openlinker-cloud", externalRequestID: requestID, actorUserID: actorID,
	}, kind, losingID)
	if err != nil || response == nil || response.ExecutionID != authoritativeID.String() || response.Status != "running" {
		t.Fatalf("authoritative attachment replay = %#v, %v", response, err)
	}
	record := store.records[executionStoreKey("openlinker-cloud", requestID)]
	if record.ExecutionID == nil || *record.ExecutionID != authoritativeID {
		t.Fatalf("losing attachment overwrote Core truth: %#v", record)
	}
}

func TestGetWorkflowExecutionAggregatesChildArtifacts(t *testing.T) {
	requestID, actorID, targetID := uuid.New(), uuid.New(), uuid.New()
	childA, childB := uuid.New(), uuid.New()
	kind := "workflow_run"
	store := newMemoryStore()
	store.records[executionStoreKey("openlinker-cloud", requestID)] = ExecutionRecord{
		CallerServiceID: "openlinker-cloud", ExternalRequestID: requestID, ActorUserID: actorID,
		TargetType: TargetTypeWorkflow, TargetID: targetID, ExecutionKind: &kind, ExecutionID: &requestID,
	}
	workflows := &fakeWorkflowService{getResponse: &workflow.WorkflowRunResponse{
		ID: requestID.String(), WorkflowID: targetID.String(), Status: "success",
		Output: map[string]interface{}{"complete": true}, CreatedAt: "2026-07-13T00:00:00Z", UpdatedAt: "2026-07-13T00:01:00Z",
		Steps: []workflow.WorkflowRunStepResponse{{RunID: childA.String()}, {RunID: childB.String()}},
	}}
	sharedID := uuid.NewString()
	runtimeSvc := &fakeRuntimeService{artifacts: map[uuid.UUID][]runtime.RunArtifactResponse{
		childA: {{ID: sharedID, RunID: childA.String(), Title: "A"}},
		childB: {{ID: sharedID, RunID: childB.String(), Title: "duplicate"}, {ID: uuid.NewString(), RunID: childB.String(), Title: "B"}},
	}}
	svc := NewService(callableAgent(uuid.New()), runtimeSvc, workflows, store)

	resp, err := svc.GetExecution(context.Background(), testPrincipal(actorID), requestID.String())
	if err != nil || resp.Status != "succeeded" || len(resp.Artifacts) != 2 || resp.Output["complete"] != true {
		t.Fatalf("GetExecution workflow = %#v, %v", resp, err)
	}
}

func newTestService(agents agentService, owners ...uuid.UUID) *Service {
	return NewService(agents, &fakeRuntimeService{}, &fakeWorkflowService{}, newMemoryStore(owners...))
}

func callableAgent(targetID uuid.UUID) *fakeAgentService {
	return &fakeAgentService{response: &agent.AgentResponse{
		ID: targetID.String(), Name: "Callable", LifecycleStatus: "active", Visibility: "public",
		Readiness: &agent.Readiness{Callable: true},
	}}
}

type fakeAgentService struct {
	response      *agent.AgentResponse
	err           error
	onboarding    *agent.OnboardingResponse
	onboardingErr error
	blockStarted  chan struct{}
	blockRelease  chan struct{}
	blockOnce     sync.Once
	onGet         func()
	onGetOnce     sync.Once
}

func (f *fakeAgentService) GetMyAgent(context.Context, uuid.UUID, uuid.UUID) (*agent.AgentResponse, error) {
	if f.blockStarted != nil && f.blockRelease != nil {
		f.blockOnce.Do(func() { close(f.blockStarted) })
		<-f.blockRelease
	}
	if f.onGet != nil {
		f.onGetOnce.Do(f.onGet)
	}
	return f.response, f.err
}

func (f *fakeAgentService) GetAgentOnboarding(context.Context, uuid.UUID, uuid.UUID) (*agent.OnboardingResponse, error) {
	if f.onboardingErr != nil {
		return nil, f.onboardingErr
	}
	if f.onboarding == nil {
		return &agent.OnboardingResponse{Capability: &agent.CapabilityResponse{InputSchema: map[string]interface{}{
			"type": "object", "properties": map[string]interface{}{},
		}}}, nil
	}
	return f.onboarding, nil
}

type fakeRuntimeService struct {
	lookupMu       sync.Mutex
	startResponse  *runtime.RunResponse
	lookupResponse *runtime.RunResponse
	lookupFound    bool
	lookupErr      error
	lookupFunc     func(int) (*runtime.RunResponse, bool, error)
	getResponse    *runtime.RunResponse
	artifacts      map[uuid.UUID][]runtime.RunArtifactResponse
	startCalls     int
	lookupCalls    int
	startActors    []uuid.UUID
	startRequests  []*runtime.RunRequest
	lookupRequests []*runtime.RunRequest
	cancelResponse *runtime.RunResponse
	cancelErr      error
	cancelEvidence runtime.RunCancellationEvidence
	evidenceErr    error
}

func (f *fakeRuntimeService) LookupRunByCreationRequest(_ context.Context, _ uuid.UUID, request *runtime.RunRequest, _ string) (*runtime.RunResponse, bool, error) {
	f.lookupMu.Lock()
	f.lookupCalls++
	call := f.lookupCalls
	f.lookupRequests = append(f.lookupRequests, request)
	lookupFunc := f.lookupFunc
	response, found, err := f.lookupResponse, f.lookupFound, f.lookupErr
	f.lookupMu.Unlock()
	if lookupFunc != nil {
		return lookupFunc(call)
	}
	return response, found, err
}

func (f *fakeRuntimeService) LookupRunByCreationIdentity(context.Context, uuid.UUID, []byte, []byte) (*runtime.RunResponse, bool, error) {
	f.lookupMu.Lock()
	f.lookupCalls++
	response, found, err := f.lookupResponse, f.lookupFound, f.lookupErr
	f.lookupMu.Unlock()
	return response, found, err
}

func (f *fakeRuntimeService) StartRun(_ context.Context, actorID uuid.UUID, request *runtime.RunRequest, _ string) (*runtime.RunResponse, error) {
	f.startCalls++
	f.startActors = append(f.startActors, actorID)
	f.startRequests = append(f.startRequests, request)
	if f.startResponse == nil {
		return nil, errors.New("unexpected StartRun")
	}
	return f.startResponse, nil
}

func (f *fakeRuntimeService) StartExternalRun(ctx context.Context, actorID uuid.UUID, request *runtime.RunRequest, source string, _ runtime.ExternalExecutionLaunchFence) (*runtime.RunResponse, error) {
	return f.StartRun(ctx, actorID, request, source)
}

func (f *fakeRuntimeService) CancelRun(context.Context, uuid.UUID, uuid.UUID) (*runtime.RunResponse, error) {
	if f.cancelErr != nil {
		return f.cancelResponse, f.cancelErr
	}
	if f.cancelResponse != nil {
		return f.cancelResponse, nil
	}
	if f.getResponse != nil {
		return f.getResponse, nil
	}
	return &runtime.RunResponse{Status: "canceled"}, nil
}

func (f *fakeRuntimeService) GetRunCancellationEvidence(context.Context, uuid.UUID, uuid.UUID) (runtime.RunCancellationEvidence, error) {
	if f.evidenceErr != nil {
		return runtime.RunCancellationEvidence{}, f.evidenceErr
	}
	if f.cancelEvidence.CancellationID != uuid.Nil || f.cancelEvidence.State != "" {
		return f.cancelEvidence, nil
	}
	return runtime.RunCancellationEvidence{CancellationID: uuid.New(), State: "stopped"}, nil
}

func (f *fakeRuntimeService) GetRun(context.Context, uuid.UUID, uuid.UUID) (*runtime.RunResponse, error) {
	if f.getResponse == nil {
		return nil, errors.New("unexpected GetRun")
	}
	return f.getResponse, nil
}

func (f *fakeRuntimeService) ListRunArtifacts(_ context.Context, _ uuid.UUID, runID uuid.UUID) ([]runtime.RunArtifactResponse, error) {
	return f.artifacts[runID], nil
}

type fakeWorkflowService struct {
	validation  *WorkflowTargetValidation
	start       *workflow.WorkflowRunResponse
	lookup      *workflow.WorkflowRunResponse
	lookupFound bool
	lookupErr   error
	getResponse *workflow.WorkflowRunResponse
	startCalls  int
	lookupCalls int
}

func (f *fakeWorkflowService) ValidateExternalExecutionTarget(context.Context, uuid.UUID, uuid.UUID) (*WorkflowTargetValidation, error) {
	if f.validation == nil {
		hash, _ := executioncontract.AgentHash(executioncontract.Agent{ID: "workflow-fixture", ConnectionMode: "runtime"})
		return &WorkflowTargetValidation{Executable: true, TargetName: "Workflow", ContractHash: hash}, nil
	}
	return f.validation, nil
}

func (f *fakeWorkflowService) LookupExternalExecutionWorkflowRun(context.Context, string, uuid.UUID, uuid.UUID, uuid.UUID, map[string]interface{}) (*workflow.WorkflowRunResponse, bool, error) {
	f.lookupCalls++
	return f.lookup, f.lookupFound, f.lookupErr
}

func (f *fakeWorkflowService) LookupExternalExecutionWorkflowRunByIdentity(context.Context, string, uuid.UUID, uuid.UUID, uuid.UUID) (*workflow.WorkflowRunResponse, bool, error) {
	f.lookupCalls++
	return f.lookup, f.lookupFound, f.lookupErr
}

func (f *fakeWorkflowService) StartExternalWorkflowRun(context.Context, string, uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID, map[string]interface{}) (*workflow.WorkflowRunResponse, error) {
	f.startCalls++
	if f.start == nil {
		return nil, errors.New("unexpected StartExternalWorkflowRun")
	}
	return f.start, nil
}

func (f *fakeWorkflowService) StartExternalExecutionWorkflowRunWithFence(_ context.Context, _, _, _ uuid.UUID, _ map[string]interface{}, _ workflow.ExternalExecutionLaunchFence) (*workflow.WorkflowRunResponse, error) {
	f.startCalls++
	if f.start == nil {
		return nil, errors.New("unexpected StartExternalExecutionWorkflowRunWithFence")
	}
	return f.start, nil
}

func (f *fakeWorkflowService) CancelExternalWorkflowRun(context.Context, uuid.UUID, uuid.UUID, string) (*workflow.WorkflowRunResponse, workflow.CancellationEvidence, error) {
	resp := f.getResponse
	if resp == nil {
		resp = &workflow.WorkflowRunResponse{Status: "canceled"}
	}
	return resp, workflow.CancellationEvidence{CancellationID: uuid.New(), State: "stopped"}, nil
}

func (f *fakeWorkflowService) GetWorkflowCancellationEvidence(context.Context, uuid.UUID, uuid.UUID) (workflow.CancellationEvidence, error) {
	return workflow.CancellationEvidence{CancellationID: uuid.New(), State: "stopped"}, nil
}

func (f *fakeWorkflowService) GetWorkflowRun(context.Context, uuid.UUID, uuid.UUID) (*workflow.WorkflowRunResponse, error) {
	if f.getResponse == nil {
		return nil, errors.New("unexpected GetWorkflowRun")
	}
	return f.getResponse, nil
}

type memoryStore struct {
	mu           sync.Mutex
	records      map[string]ExecutionRecord
	defaultOwner uuid.UUID
	resolveCalls int
	getCalls     int
	resolveHook  func()
	claimCalls   int
	now          func() time.Time
}

func newMemoryStore(owners ...uuid.UUID) *memoryStore {
	ownerID := uuid.New()
	if len(owners) > 0 && owners[0] != uuid.Nil {
		ownerID = owners[0]
	}
	return &memoryStore{records: map[string]ExecutionRecord{}, defaultOwner: ownerID, now: time.Now}
}

func (m *memoryStore) ResolveTargetOwner(context.Context, string, uuid.UUID) (uuid.UUID, error) {
	m.mu.Lock()
	m.resolveCalls++
	ownerID := m.defaultOwner
	hook := m.resolveHook
	m.resolveHook = nil
	m.mu.Unlock()
	if hook != nil {
		hook()
	}
	if ownerID == uuid.Nil {
		return uuid.Nil, pgx.ErrNoRows
	}
	return ownerID, nil
}

func (m *memoryStore) Reserve(_ context.Context, record ExecutionRecord) (ExecutionRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := executionStoreKey(record.CallerServiceID, record.ExternalRequestID)
	if existingKey, ok := memoryCancellationState(m).keys[key]; ok && existingKey.ActorUserID != record.ActorUserID {
		return ExecutionRecord{}, ErrExecutionIdentityConflict
	}
	if _, canceled := memoryCancellationState(m).cancellations[key]; canceled {
		return ExecutionRecord{}, ErrExecutionCanceled
	}
	if existing, ok := m.records[key]; ok {
		return memoryExecutionRecordDefaults(existing), nil
	}
	record.StartState = startStatePending
	record.InputFingerprint = append([]byte(nil), record.InputFingerprint...)
	record.InputSchemaFingerprint = append([]byte(nil), record.InputSchemaFingerprint...)
	record.DownstreamReplayIdentity = append([]byte(nil), record.DownstreamReplayIdentity...)
	if record.ExpectedContractHash != nil {
		value := *record.ExpectedContractHash
		record.ExpectedContractHash = &value
	}
	m.records[key] = record
	return record, nil
}

func (m *memoryStore) PromoteLegacyReservation(_ context.Context, replacement ExecutionRecord) (ExecutionRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := executionStoreKey(replacement.CallerServiceID, replacement.ExternalRequestID)
	record, ok := m.records[key]
	if !ok {
		return ExecutionRecord{}, pgx.ErrNoRows
	}
	if record.RequestFingerprintVersion == legacyRequestFingerprintVersion &&
		record.ExecutionID == nil && record.ExecutionKind == nil &&
		record.ActorUserID == replacement.ActorUserID && record.TargetType == replacement.TargetType &&
		record.TargetID == replacement.TargetID && record.TraceID == replacement.TraceID {
		replacement.StartState = record.StartState
		replacement.StartToken = record.StartToken
		replacement.StartLeaseUntil = record.StartLeaseUntil
		replacement.AuthorizedTargetOwnerID = record.AuthorizedTargetOwnerID
		replacement.RejectionCode = record.RejectionCode
		replacement.InputFingerprint = append([]byte(nil), replacement.InputFingerprint...)
		replacement.InputSchemaFingerprint = append([]byte(nil), replacement.InputSchemaFingerprint...)
		m.records[key] = replacement
		return replacement, nil
	}
	return record, nil
}

func (m *memoryStore) Get(_ context.Context, callerServiceID string, id uuid.UUID) (ExecutionRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getCalls++
	record, ok := m.records[executionStoreKey(callerServiceID, id)]
	if !ok {
		return ExecutionRecord{}, pgx.ErrNoRows
	}
	record = memoryExecutionRecordDefaults(record)
	m.records[executionStoreKey(callerServiceID, id)] = record
	return record, nil
}

func (m *memoryStore) ClaimStartEvaluation(
	_ context.Context,
	callerServiceID string,
	id, token uuid.UUID,
	lease time.Duration,
) (ExecutionRecord, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.claimCalls++
	key := executionStoreKey(callerServiceID, id)
	record, ok := m.records[key]
	if !ok {
		return ExecutionRecord{}, false, pgx.ErrNoRows
	}
	record = memoryExecutionRecordDefaults(record)
	now := m.now()
	claimable := record.StartState == startStatePending ||
		(record.StartState == startStateEvaluating && record.StartLeaseUntil != nil && !record.StartLeaseUntil.After(now))
	if record.ExecutionID != nil || !claimable {
		m.records[key] = record
		return record, false, nil
	}
	leaseUntil := now.Add(lease)
	record.StartState = startStateEvaluating
	record.StartToken = &token
	record.StartLeaseUntil = &leaseUntil
	record.AuthorizedTargetOwnerID = nil
	record.RejectionCode = nil
	m.records[key] = record
	return record, true, nil
}

func (m *memoryStore) AuthorizeStart(
	_ context.Context,
	callerServiceID string,
	id, token, targetOwnerID uuid.UUID,
) (ExecutionRecord, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := executionStoreKey(callerServiceID, id)
	record, ok := m.records[key]
	if !ok {
		return ExecutionRecord{}, false, pgx.ErrNoRows
	}
	record = memoryExecutionRecordDefaults(record)
	if record.ExecutionID == nil && record.StartState == startStateEvaluating && record.StartToken != nil &&
		*record.StartToken == token && record.StartLeaseUntil != nil && record.StartLeaseUntil.After(m.now()) {
		record.StartState = startStateAuthorized
		record.StartToken = nil
		record.StartLeaseUntil = nil
		record.AuthorizedTargetOwnerID = &targetOwnerID
		record.RejectionCode = nil
		m.records[key] = record
		return record, true, nil
	}
	m.records[key] = record
	return record, false, nil
}

func (m *memoryStore) RejectStart(
	_ context.Context,
	callerServiceID string,
	id, token uuid.UUID,
	rejectionCode string,
) (ExecutionRecord, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := executionStoreKey(callerServiceID, id)
	record, ok := m.records[key]
	if !ok {
		return ExecutionRecord{}, false, pgx.ErrNoRows
	}
	record = memoryExecutionRecordDefaults(record)
	if record.ExecutionID == nil && record.StartState == startStateEvaluating && record.StartToken != nil &&
		*record.StartToken == token && record.StartLeaseUntil != nil && record.StartLeaseUntil.After(m.now()) {
		record.StartState = startStateRejected
		record.StartToken = nil
		record.StartLeaseUntil = nil
		record.AuthorizedTargetOwnerID = nil
		record.RejectionCode = &rejectionCode
		m.records[key] = record
		return record, true, nil
	}
	m.records[key] = record
	return record, false, nil
}

func (m *memoryStore) ReleaseStartEvaluation(_ context.Context, callerServiceID string, id, token uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := executionStoreKey(callerServiceID, id)
	record, ok := m.records[key]
	if !ok {
		return pgx.ErrNoRows
	}
	record = memoryExecutionRecordDefaults(record)
	if record.ExecutionID == nil && record.StartState == startStateEvaluating && record.StartToken != nil && *record.StartToken == token {
		record.StartState = startStatePending
		record.StartToken = nil
		record.StartLeaseUntil = nil
		record.AuthorizedTargetOwnerID = nil
		record.RejectionCode = nil
		m.records[key] = record
	}
	return nil
}

func (m *memoryStore) Attach(_ context.Context, callerServiceID string, id uuid.UUID, kind string, executionID uuid.UUID) (ExecutionRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := executionStoreKey(callerServiceID, id)
	record, ok := m.records[key]
	if !ok {
		return ExecutionRecord{}, pgx.ErrNoRows
	}
	record = memoryExecutionRecordDefaults(record)
	attachable := record.StartState == startStatePending || record.StartState == startStateEvaluating || record.StartState == startStateAuthorized
	if record.ExecutionID == nil && attachable {
		record.ExecutionKind = &kind
		record.ExecutionID = &executionID
		record.StartState = startStateAuthorized
		record.StartToken = nil
		record.StartLeaseUntil = nil
		record.RejectionCode = nil
		m.records[key] = record
	}
	return record, nil
}

func memoryExecutionRecordDefaults(record ExecutionRecord) ExecutionRecord {
	if record.StartState == "" {
		if record.ExecutionID != nil {
			record.StartState = startStateAuthorized
		} else {
			record.StartState = startStatePending
		}
	}
	return record
}

func executionStoreKey(callerServiceID string, id uuid.UUID) string {
	return callerServiceID + "\x00" + id.String()
}

func testPrincipal(actorID uuid.UUID) *Principal {
	return &Principal{CallerServiceID: "openlinker-cloud", ActorUserID: actorID}
}

func currentExecutionRecord(t *testing.T, principal *Principal, request *ExecutionRequest) ExecutionRecord {
	t.Helper()
	parsed, err := parseExecutionRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	parsed.callerServiceID = principal.CallerServiceID
	parsed.actorUserID = principal.ActorUserID
	fingerprint, err := executionRequestFingerprint(parsed)
	if err != nil {
		t.Fatal(err)
	}
	contractHash := parsed.expectedContractHash
	return ExecutionRecord{
		CallerServiceID: principal.CallerServiceID, ExternalRequestID: parsed.externalRequestID,
		RequestFingerprintVersion: currentRequestFingerprintVersion, ActorUserID: principal.ActorUserID,
		TargetType: parsed.targetType, TargetID: parsed.targetID, InputFingerprint: fingerprint[:],
		ExpectedContractHash: &contractHash, InputSchemaFingerprint: parsed.inputSchemaFingerprint[:], TraceID: parsed.traceID,
	}
}

func legacyReplayIdentity(t *testing.T, requestID, ownerID uuid.UUID, traceID string) []byte {
	t.Helper()
	encoded, err := json.Marshal(map[string]interface{}{
		"version":           1,
		"kind":              "run",
		"source":            "api",
		"idempotency_key":   "hosted-service-order/" + requestID.String(),
		"creation_protocol": "hosted",
		"creation_method":   "service-order.execute",
		"metadata": map[string]interface{}{
			"external_order_id": requestID.String(),
			"seller_user_id":    ownerID.String(),
			"trace_id":          traceID,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func assertHTTPStatus(t *testing.T, err error, status int) {
	t.Helper()
	var he *httpx.HTTPError
	if !errors.As(err, &he) || he.Status != status {
		t.Fatalf("error = %#v, want HTTP %d", err, status)
	}
}

func assertHTTPCode(t *testing.T, err error, code httpx.ErrorCode) {
	t.Helper()
	var he *httpx.HTTPError
	if !errors.As(err, &he) || he.Code != code {
		t.Fatalf("error = %#v, want code %s", err, code)
	}
}
