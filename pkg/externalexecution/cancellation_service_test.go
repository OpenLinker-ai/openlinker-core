package externalexecution

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

func TestCancelBeforeReservationCreatesStoppedTombstoneAndBlocksStart(t *testing.T) {
	actorID, requestID, targetID := uuid.New(), uuid.New(), uuid.New()
	store := newMemoryStore(uuid.New())
	svc := NewService(callableAgent(targetID), &fakeRuntimeService{}, &fakeWorkflowService{}, store)
	principal := testPrincipal(actorID)

	first, err := svc.CancelExecution(context.Background(), principal, requestID.String(), &ExecutionCancelRequest{
		ReasonCode: "CALLER_REQUESTED",
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != "canceled" || first.ExecutionID != "" || first.TargetType != "" ||
		first.Cancellation == nil || first.Cancellation.State != "stopped" ||
		first.Cancellation.ReasonCode != "CALLER_REQUESTED" {
		t.Fatalf("cancel-before-start response = %#v", first)
	}
	firstID := first.Cancellation.CancellationID

	replayed, err := svc.CancelExecution(context.Background(), principal, requestID.String(), &ExecutionCancelRequest{
		ReasonCode: "DEADLINE_EXCEEDED",
	})
	if err != nil {
		t.Fatal(err)
	}
	if replayed.Cancellation.CancellationID != firstID ||
		replayed.Cancellation.ReasonCode != "CALLER_REQUESTED" {
		t.Fatalf("first-writer evidence changed: %#v", replayed.Cancellation)
	}

	_, err = svc.StartExecution(context.Background(), principal, &ExecutionRequest{
		ExternalRequestID: requestID.String(), TargetType: TargetTypeAgent,
		TargetID: targetID.String(), Input: map[string]interface{}{}, TraceID: "trace-canceled",
		ExpectedContractHash: "hct:v1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		InputSchema:          json.RawMessage(`{"type":"object"}`),
	})
	assertHTTPCode(t, err, "EXTERNAL_EXECUTION_CANCELED")
}

func TestCancelAttachedRuntimeRequiresStoppedEvidence(t *testing.T) {
	actorID, ownerID, requestID, targetID, runID := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	runtimeSvc := &fakeRuntimeService{
		startResponse:  &runtime.RunResponse{RunID: runID.String(), Status: "running", StartedAt: time.Now()},
		getResponse:    &runtime.RunResponse{RunID: runID.String(), Status: "running", StartedAt: time.Now()},
		cancelEvidence: runtime.RunCancellationEvidence{CancellationID: uuid.New(), State: "stopped"},
	}
	svc := NewService(callableAgent(targetID), runtimeSvc, &fakeWorkflowService{}, newMemoryStore(ownerID))
	principal := testPrincipal(actorID)
	validation, err := svc.ValidateTarget(context.Background(), testPrincipal(ownerID), &TargetValidationRequest{
		TargetType: TargetTypeAgent, TargetID: targetID.String(),
		InputSchema: json.RawMessage(`{"type":"object","properties":{}}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = svc.StartExecution(context.Background(), principal, &ExecutionRequest{
		ExternalRequestID: requestID.String(), TargetType: TargetTypeAgent,
		TargetID: targetID.String(), Input: map[string]interface{}{}, TraceID: "trace-attached-cancel",
		ExpectedContractHash: validation.ContractHash,
		InputSchema:          json.RawMessage(`{"type":"object","properties":{}}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := svc.CancelExecution(context.Background(), principal, requestID.String(), &ExecutionCancelRequest{
		ReasonCode: "CALLER_REQUESTED",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != "canceled" || resp.ExecutionID != runID.String() ||
		resp.Cancellation == nil || resp.Cancellation.State != "stopped" {
		t.Fatalf("attached cancel response = %#v", resp)
	}
}

func TestCancelUnconfirmedNeverClaimsCanceled(t *testing.T) {
	actorID, requestID, runID := uuid.New(), uuid.New(), uuid.New()
	store := newMemoryStore(uuid.New())
	kind := "run"
	store.records[executionStoreKey("openlinker-cloud", requestID)] = ExecutionRecord{
		CallerServiceID: "openlinker-cloud", ExternalRequestID: requestID, ActorUserID: actorID,
		TargetType: TargetTypeAgent, TargetID: uuid.New(), StartState: startStateAttached,
		ExecutionKind: &kind, ExecutionID: &runID,
	}
	runtimeSvc := &fakeRuntimeService{
		getResponse: &runtime.RunResponse{RunID: runID.String(), Status: "canceled", StartedAt: time.Now()},
		cancelEvidence: runtime.RunCancellationEvidence{
			CancellationID: uuid.New(), State: "unconfirmed", ErrorCode: "CANCEL_UNCONFIRMED",
		},
	}
	svc := NewService(&fakeAgentService{}, runtimeSvc, &fakeWorkflowService{}, store)
	resp, err := svc.CancelExecution(context.Background(), testPrincipal(actorID), requestID.String(), &ExecutionCancelRequest{
		ReasonCode: "DEADLINE_EXCEEDED",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != "failed" || resp.ErrorCode != "CANCEL_UNCONFIRMED" ||
		resp.Cancellation == nil || resp.Cancellation.State != "unconfirmed" {
		t.Fatalf("unconfirmed response = %#v", resp)
	}
}

func TestNotAppliedCancellationFailsClosedWhenTerminalReadFails(t *testing.T) {
	actorID, requestID, runID := uuid.New(), uuid.New(), uuid.New()
	kind := "run"
	now := time.Now().UTC()
	svc := NewService(&fakeAgentService{}, &fakeRuntimeService{}, &fakeWorkflowService{}, newMemoryStore(uuid.New()))
	record := &ExecutionRecord{
		CallerServiceID: "openlinker-cloud", ExternalRequestID: requestID,
		ActorUserID: actorID, TargetType: TargetTypeAgent, TargetID: uuid.New(),
		StartState: startStateAttached, ExecutionKind: &kind, ExecutionID: &runID,
	}
	cancellation := CancellationRecord{
		ID: uuid.New(), CallerServiceID: "openlinker-cloud",
		ExternalRequestID: requestID, ActorUserID: actorID,
		ReasonCode: "CALLER_REQUESTED", State: "not_applied",
		RequestedAt: now, AppliedAt: &now, FinishedAt: &now, UpdatedAt: now,
	}

	resp, err := svc.cancellationStatusResponse(context.Background(), record, cancellation)
	if resp != nil {
		t.Fatalf("not_applied response fabricated from failed terminal read: %#v", resp)
	}
	var httpErr *httpx.HTTPError
	if !errors.As(err, &httpErr) || httpErr.Code != httpx.CodeServiceUnavailable {
		t.Fatalf("not_applied terminal read error = %v", err)
	}
}
