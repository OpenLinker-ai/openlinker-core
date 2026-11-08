package externalexecution

import (
	"context"
	"encoding/json"
	"errors"
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
// The request is then rejected durably under the launch token: every later
// Start and GetExecution returns the same terminal answer and nothing ever
// launches, even after the capability becomes permissive.
func TestStartExecutionRuntimeInputRejectionIsDurable(t *testing.T) {
	svc, runtimeSvc, store, targetID, listing, contractHash := boundedListingFixture(t)
	principal := testPrincipal(uuid.New())
	runtimeSvc.startErr = &httpx.HTTPError{
		Status: http.StatusUnprocessableEntity, Code: httpx.ErrorCode("RUN_INPUT_SCHEMA_MISMATCH"), Message: "input 不匹配 Agent input_schema",
		Details: map[string]interface{}{"path": "input.buyer-secret-key", "reason": "additional_property"},
	}
	req := boundedRequest(targetID, listing, contractHash, "ok")
	req.Input["buyer-secret-key"] = "x"
	recordKey := executionStoreKey(principal.CallerServiceID, uuid.MustParse(req.ExternalRequestID))

	requireTerminal := func(step string, err error) {
		t.Helper()
		assertHTTPCode(t, err, httpx.ErrorCode("TARGET_CONTRACT_CHANGED"))
		var httpErr *httpx.HTTPError
		if errors.As(err, &httpErr) && (httpErr.Details != nil || strings.Contains(httpErr.Message, "buyer-secret")) {
			t.Fatalf("%s: rejection leaks Runtime detail: %#v", step, httpErr)
		}
		stored := store.records[recordKey]
		if stored.StartState != startStateRejected || stored.RejectionCode == nil || *stored.RejectionCode != "TARGET_CONTRACT_CHANGED" ||
			stored.ExecutionID != nil || stored.StartToken != nil || stored.AuthorizedTargetOwnerID != nil {
			t.Fatalf("%s: record = %#v", step, stored)
		}
		if runtimeSvc.startCalls != 1 {
			t.Fatalf("%s: Runtime start calls = %d, want the single original launch", step, runtimeSvc.startCalls)
		}
	}

	_, err := svc.StartExecution(context.Background(), principal, req)
	requireTerminal("first start", err)
	_, err = svc.StartExecution(context.Background(), principal, req)
	requireTerminal("immediate retry", err)

	// No lease remains to expire, and a permissive capability changes nothing.
	runtimeSvc.startErr = nil
	store.now = func() time.Time { return time.Now().Add(time.Hour) }
	_, err = svc.StartExecution(context.Background(), principal, req)
	requireTerminal("retry after the lease and a permissive capability", err)

	_, err = svc.GetExecution(context.Background(), principal, req.ExternalRequestID)
	requireTerminal("GetExecution", err)
}

func TestStartExecutionRuntimeInputRejectionRespectsLaunchFence(t *testing.T) {
	svc, runtimeSvc, store, targetID, listing, contractHash := boundedListingFixture(t)
	principal := testPrincipal(uuid.New())
	runtimeSvc.startErr = &httpx.HTTPError{Status: http.StatusUnprocessableEntity, Code: httpx.ErrorCode("RUN_INPUT_SCHEMA_MISMATCH")}
	req := boundedRequest(targetID, listing, contractHash, "ok")
	requestID := uuid.MustParse(req.ExternalRequestID)
	recordKey := executionStoreKey(principal.CallerServiceID, requestID)

	// A cancellation that arrived during the launch keeps its own outcome.
	runtimeSvc.startHook = func() {
		store.mu.Lock()
		defer store.mu.Unlock()
		memoryCancellationState(store).cancellations[recordKey] = CancellationRecord{
			ID: uuid.New(), CallerServiceID: principal.CallerServiceID, ExternalRequestID: requestID, ActorUserID: principal.ActorUserID,
			State: "requested", ReasonCode: "CALLER_REQUESTED", RequestedAt: time.Now(),
		}
	}
	_, err := svc.StartExecution(context.Background(), principal, req)
	assertHTTPStatus(t, err, http.StatusServiceUnavailable)
	if stored := store.records[recordKey]; stored.StartState != startStateLaunching || stored.RejectionCode != nil {
		t.Fatalf("canceled launch was rejected: %#v", stored)
	}

	// A stale launch token never overwrites the current launch.
	stored := store.records[recordKey]
	other := uuid.New()
	stored.StartToken = &other
	store.records[recordKey] = stored
	if _, rejected, err := store.RejectLaunch(context.Background(), principal.CallerServiceID, requestID, uuid.New(), "TARGET_CONTRACT_CHANGED"); err != nil || rejected {
		t.Fatalf("stale token rejected the launch: %v, %v", rejected, err)
	}
}

// A reservation from before input_schema enforcement that already has a
// committed Run is recovered first; its input is only checked on a miss.
func TestStartExecutionRecoversCommittedWorkBeforeInputEnforcement(t *testing.T) {
	requestID, actorID, ownerID, targetID, runID := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	listing := json.RawMessage(`{"type":"object","properties":{"channel":{"type":"string","enum":["email","web"]}}}`)
	store := newMemoryStore(ownerID)
	runtimeSvc := &fakeRuntimeService{
		lookupResponse: &runtime.RunResponse{RunID: runID.String(), Status: "running", StartedAt: time.Now()},
		lookupFound:    true,
		getResponse:    &runtime.RunResponse{RunID: runID.String(), Status: "running", StartedAt: time.Now()},
	}
	svc := NewService(callableAgent(targetID), runtimeSvc, &fakeWorkflowService{}, store)
	contractHash := "hct:v1:" + strings.Repeat("a", 64)
	store.records[executionStoreKey("openlinker-cloud", requestID)] = ExecutionRecord{
		CallerServiceID: "openlinker-cloud", ExternalRequestID: requestID, RequestFingerprintVersion: legacyRequestFingerprintVersion,
		ActorUserID: actorID, TargetType: TargetTypeAgent, TargetID: targetID,
		ExpectedContractHash: &contractHash, TraceID: "trace-legacy-recovery",
		DownstreamReplayIdentity: legacyReplayIdentity(t, requestID, ownerID, "trace-legacy-recovery"),
	}
	req := &ExecutionRequest{
		ExternalRequestID: requestID.String(), TargetType: TargetTypeAgent, TargetID: targetID.String(),
		Input:    map[string]interface{}{"channel": ""},
		Metadata: map[string]interface{}{"external_order_id": requestID.String(), "seller_user_id": ownerID.String()},
		TraceID:  "trace-legacy-recovery", ExpectedContractHash: contractHash, InputSchema: listing,
	}
	if err := validateExternalExecutionInput(req.Input, decodeJSONObject(t, string(listing))); err == nil {
		t.Fatal("fixture input must violate the listing")
	}

	response, err := svc.StartExecution(context.Background(), testPrincipal(actorID), req)
	if err != nil || response.ExecutionID != runID.String() {
		t.Fatalf("legacy recovery = %#v, %v", response, err)
	}
	attached := store.records[executionStoreKey("openlinker-cloud", requestID)]
	if attached.ExecutionID == nil || *attached.ExecutionID != runID || runtimeSvc.startCalls != 0 {
		t.Fatalf("committed work was not recovered: record=%#v start calls=%d", attached, runtimeSvc.startCalls)
	}

	// Without committed work the same reservation is rejected and stays pending.
	missID := uuid.New()
	store.records[executionStoreKey("openlinker-cloud", missID)] = ExecutionRecord{
		CallerServiceID: "openlinker-cloud", ExternalRequestID: missID, RequestFingerprintVersion: legacyRequestFingerprintVersion,
		ActorUserID: actorID, TargetType: TargetTypeAgent, TargetID: targetID,
		ExpectedContractHash: &contractHash, TraceID: "trace-legacy-recovery",
		DownstreamReplayIdentity: legacyReplayIdentity(t, missID, ownerID, "trace-legacy-recovery"),
	}
	runtimeSvc.lookupFound = false
	miss := *req
	miss.ExternalRequestID = missID.String()
	miss.Metadata = map[string]interface{}{"external_order_id": missID.String(), "seller_user_id": ownerID.String()}
	_, err = svc.StartExecution(context.Background(), testPrincipal(actorID), &miss)
	requireInputInvalid(t, err, executionInputSourceInputSchema, "channel", SchemaRuleEnum)
	if stored := store.records[executionStoreKey("openlinker-cloud", missID)]; stored.StartState != startStatePending || stored.StartToken != nil || runtimeSvc.startCalls != 0 {
		t.Fatalf("rejected legacy reservation changed: %#v (start calls %d)", stored, runtimeSvc.startCalls)
	}
}

// Authorized and expired-launching reservations from before input_schema
// enforcement are checked after their recovery misses; committed work is
// still recovered first.
func TestStartExecutionChecksAuthorizedAndExpiredLaunchingReservations(t *testing.T) {
	svc, runtimeSvc, store, targetID, listing, contractHash := boundedListingFixture(t)
	principal := testPrincipal(uuid.New())
	ownerID := store.defaultOwner
	expired := time.Now().Add(-time.Second)
	token := uuid.New()
	seed := func(state, prompt string) (*ExecutionRequest, string) {
		t.Helper()
		req := boundedRequest(targetID, listing, contractHash, prompt)
		record := currentExecutionRecord(t, principal, req)
		record.StartState = state
		record.AuthorizedTargetOwnerID = &ownerID
		if state == startStateLaunching {
			record.StartToken, record.StartLeaseUntil = &token, &expired
		}
		key := executionStoreKey(principal.CallerServiceID, record.ExternalRequestID)
		store.records[key] = record
		return req, key
	}
	tooLong := "longer than ten runes"

	for _, state := range []string{startStateAuthorized, startStateLaunching} {
		t.Run(state, func(t *testing.T) {
			startCalls := runtimeSvc.startCalls
			req, key := seed(state, tooLong)
			before := store.records[key]
			_, err := svc.StartExecution(context.Background(), principal, req)
			requireInputInvalid(t, err, executionInputSourceInputSchema, "prompt", SchemaRuleMaxLength)
			if after := store.records[key]; after.StartState != before.StartState || after.ExecutionID != nil || runtimeSvc.startCalls != startCalls {
				t.Fatalf("rejected %s reservation changed: %#v (start calls %d)", state, after, runtimeSvc.startCalls)
			}
		})
	}

	// Committed work is recovered before the input is checked.
	recoveredRun := uuid.New()
	runtimeSvc.lookupResponse = &runtime.RunResponse{RunID: recoveredRun.String(), Status: "running", StartedAt: time.Now()}
	runtimeSvc.lookupFound = true
	authorizedReq, _ := seed(startStateAuthorized, tooLong)
	if response, err := svc.StartExecution(context.Background(), principal, authorizedReq); err != nil || response.ExecutionID != recoveredRun.String() {
		t.Fatalf("authorized recovery = %#v, %v", response, err)
	}
	launchingReq, launchingKey := seed(startStateLaunching, tooLong)
	launching := store.records[launchingKey]
	launching.DownstreamKeyHash, launching.DownstreamFingerprint = make([]byte, 32), make([]byte, 32)
	store.records[launchingKey] = launching
	if response, err := svc.StartExecution(context.Background(), principal, launchingReq); err != nil || response.ExecutionID != recoveredRun.String() {
		t.Fatalf("launching recovery = %#v, %v", response, err)
	}
}

// Outside the capability-rejection path, an expired launch keeps main's
// behaviour: Core does not reclaim it and answers 500, so a generic Runtime
// failure never turns into new work on a later retry.
func TestStartExecutionExpiredLaunchAfterGenericFailureKeepsMainBehaviour(t *testing.T) {
	svc, runtimeSvc, store, targetID, listing, contractHash := boundedListingFixture(t)
	principal := testPrincipal(uuid.New())
	runtimeSvc.startErr = httpx.Internal("runtime unavailable")
	req := boundedRequest(targetID, listing, contractHash, "ok")
	key := executionStoreKey(principal.CallerServiceID, uuid.MustParse(req.ExternalRequestID))

	_, err := svc.StartExecution(context.Background(), principal, req)
	assertHTTPStatus(t, err, http.StatusServiceUnavailable)
	if stored := store.records[key]; stored.StartState != startStateLaunching || runtimeSvc.startCalls != 1 {
		t.Fatalf("after generic failure record=%#v start calls=%d", stored, runtimeSvc.startCalls)
	}
	_, err = svc.StartExecution(context.Background(), principal, req)
	assertHTTPStatus(t, err, http.StatusServiceUnavailable)

	runtimeSvc.startErr = nil
	stored := store.records[key]
	expired := time.Now().Add(-time.Second)
	stored.StartLeaseUntil = &expired
	store.records[key] = stored
	_, err = svc.StartExecution(context.Background(), principal, req)
	assertHTTPStatusAndCode(t, err, http.StatusInternalServerError, httpx.CodeInternal)
	if after := store.records[key]; after.StartState != startStateLaunching || after.ExecutionID != nil || runtimeSvc.startCalls != 1 {
		t.Fatalf("expired generic launch created work: %#v (start calls %d)", after, runtimeSvc.startCalls)
	}
}
