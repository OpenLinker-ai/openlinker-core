package servicebridge

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/OpenLinker-ai/openlinker-core/pkg/workflow"
)

func TestValidateTargetRequiresPublicCallableOwnedAgent(t *testing.T) {
	sellerID := uuid.New()
	targetID := uuid.New()
	agents := &fakeAgentService{response: &agent.AgentResponse{
		ID: targetID.String(), Name: "Resume editor", LifecycleStatus: "active", Visibility: "public",
		Readiness: &agent.Readiness{Callable: true},
	}}
	svc := newTestService(agents)

	resp, err := svc.ValidateTarget(context.Background(), &TargetValidationRequest{
		SellerUserID: sellerID.String(), TargetType: TargetTypeAgent, TargetID: targetID.String(),
	})
	if err != nil || !resp.Executable || resp.TargetName != "Resume editor" {
		t.Fatalf("ValidateTarget() = %#v, %v", resp, err)
	}

	agents.response.Visibility = "private"
	resp, err = svc.ValidateTarget(context.Background(), &TargetValidationRequest{
		SellerUserID: sellerID.String(), TargetType: TargetTypeAgent, TargetID: targetID.String(),
	})
	if err != nil || resp.Executable || resp.UnavailableReason != "not_public" {
		t.Fatalf("private ValidateTarget() = %#v, %v", resp, err)
	}

	agents.err = httpx.NotFound("secret owner detail")
	resp, err = svc.ValidateTarget(context.Background(), &TargetValidationRequest{
		SellerUserID: sellerID.String(), TargetType: TargetTypeAgent, TargetID: targetID.String(),
	})
	if err != nil || resp.Executable || resp.UnavailableReason != "not_found" {
		t.Fatalf("unowned ValidateTarget() = %#v, %v", resp, err)
	}
}

func TestValidateTargetChecksListingInputAgainstAgentCapability(t *testing.T) {
	sellerID, targetID := uuid.New(), uuid.New()
	agents := callableAgent(targetID)
	agents.onboarding = &agent.OnboardingResponse{Capability: &agent.CapabilityResponse{InputSchema: map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"topic": map[string]interface{}{"type": "string"},
		},
		"required":             []interface{}{"topic"},
		"additionalProperties": false,
	}}}
	svc := newTestService(agents)

	resp, err := svc.ValidateTarget(context.Background(), &TargetValidationRequest{
		SellerUserID: sellerID.String(), TargetType: TargetTypeAgent, TargetID: targetID.String(),
		InputSchema: json.RawMessage(`[{"key":"topic","type":"text","required":true}]`),
	})
	if err != nil || !resp.Executable {
		t.Fatalf("compatible ValidateTarget() = %#v, %v", resp, err)
	}

	resp, err = svc.ValidateTarget(context.Background(), &TargetValidationRequest{
		SellerUserID: sellerID.String(), TargetType: TargetTypeAgent, TargetID: targetID.String(),
		InputSchema: json.RawMessage(`[{"key":"topic","type":"number","required":true}]`),
	})
	if err != nil || resp.Executable || resp.UnavailableReason != "input_schema_incompatible" {
		t.Fatalf("incompatible ValidateTarget() = %#v, %v", resp, err)
	}
}

func TestStartExecutionIsIdempotentAndRejectsSemanticReuse(t *testing.T) {
	orderID, buyerID, sellerID, targetID, runID := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	agents := callableAgent(targetID)
	runtimeSvc := &fakeRuntimeService{
		startResponse: &runtime.RunResponse{RunID: runID.String(), Status: "running", StartedAt: time.Now()},
		getResponse:   &runtime.RunResponse{RunID: runID.String(), Status: "running", StartedAt: time.Now()},
	}
	svc := NewService(agents, runtimeSvc, &fakeWorkflowService{}, newMemoryStore())
	req := &ExecutionRequest{
		ExternalOrderID: orderID.String(), BuyerUserID: buyerID.String(), SellerUserID: sellerID.String(),
		TargetType: TargetTypeAgent, TargetID: targetID.String(), Input: map[string]interface{}{"topic": "Go"}, TraceID: "trace-1",
	}

	first, err := svc.StartExecution(context.Background(), req)
	if err != nil || first.ExecutionID != runID.String() || first.Status != "running" {
		t.Fatalf("first StartExecution() = %#v, %v", first, err)
	}
	second, err := svc.StartExecution(context.Background(), req)
	if err != nil || second.ExecutionID != runID.String() {
		t.Fatalf("second StartExecution() = %#v, %v", second, err)
	}
	if runtimeSvc.startCalls != 1 {
		t.Fatalf("StartRun calls = %d, want 1", runtimeSvc.startCalls)
	}

	mutated := *req
	mutated.Input = map[string]interface{}{"topic": "Rust"}
	_, err = svc.StartExecution(context.Background(), &mutated)
	assertHTTPStatus(t, err, 409)
}

func TestGetExecutionReturnsOutputArtifactsAndSafeFailure(t *testing.T) {
	orderID, buyerID, sellerID, targetID, runID := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	kind := "run"
	store := newMemoryStore()
	store.records[orderID] = ExecutionRecord{
		ExternalOrderID: orderID, BuyerUserID: buyerID, SellerUserID: sellerID,
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

	resp, err := svc.GetExecution(context.Background(), orderID.String())
	if err != nil || resp.Status != "succeeded" || resp.Output["answer"] != "done" || len(resp.Artifacts) != 1 || resp.FinishedAt == "" {
		t.Fatalf("GetExecution success = %#v, %v", resp, err)
	}

	runtimeSvc.getResponse.Status = "failed"
	runtimeSvc.getResponse.ErrorCode = "ENDPOINT_SECRET"
	runtimeSvc.getResponse.ErrorMsg = "dial tcp 10.0.0.1 with bearer secret"
	resp, err = svc.GetExecution(context.Background(), orderID.String())
	if err != nil || resp.Status != "failed" || resp.ErrorCode != "EXECUTION_FAILED" {
		t.Fatalf("GetExecution failed = %#v, %v", resp, err)
	}
	if resp.ErrorMessage == runtimeSvc.getResponse.ErrorMsg || resp.Output != nil {
		t.Fatalf("unsafe failure response = %#v", resp)
	}
}

func TestGetWorkflowExecutionAggregatesChildArtifacts(t *testing.T) {
	orderID, buyerID, sellerID, targetID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	childA, childB := uuid.New(), uuid.New()
	kind := "workflow_run"
	store := newMemoryStore()
	store.records[orderID] = ExecutionRecord{
		ExternalOrderID: orderID, BuyerUserID: buyerID, SellerUserID: sellerID,
		TargetType: TargetTypeWorkflow, TargetID: targetID, ExecutionKind: &kind, ExecutionID: &orderID,
	}
	workflows := &fakeWorkflowService{getResponse: &workflow.WorkflowRunResponse{
		ID: orderID.String(), WorkflowID: targetID.String(), Status: "success",
		Output: map[string]interface{}{"complete": true}, CreatedAt: "2026-07-13T00:00:00Z", UpdatedAt: "2026-07-13T00:01:00Z",
		Steps: []workflow.WorkflowRunStepResponse{{RunID: childA.String()}, {RunID: childB.String()}},
	}}
	sharedID := uuid.NewString()
	runtimeSvc := &fakeRuntimeService{artifacts: map[uuid.UUID][]runtime.RunArtifactResponse{
		childA: {{ID: sharedID, RunID: childA.String(), Title: "A"}},
		childB: {{ID: sharedID, RunID: childB.String(), Title: "duplicate"}, {ID: uuid.NewString(), RunID: childB.String(), Title: "B"}},
	}}
	svc := NewService(callableAgent(uuid.New()), runtimeSvc, workflows, store)

	resp, err := svc.GetExecution(context.Background(), orderID.String())
	if err != nil || resp.Status != "succeeded" || len(resp.Artifacts) != 2 || resp.Output["complete"] != true {
		t.Fatalf("GetExecution workflow = %#v, %v", resp, err)
	}
}

func newTestService(agents agentService) *Service {
	return NewService(agents, &fakeRuntimeService{}, &fakeWorkflowService{}, newMemoryStore())
}

func callableAgent(targetID uuid.UUID) *fakeAgentService {
	return &fakeAgentService{response: &agent.AgentResponse{
		ID: targetID.String(), Name: "Callable", LifecycleStatus: "active", Visibility: "public",
		Readiness: &agent.Readiness{Callable: true},
	}}
}

type fakeAgentService struct {
	response   *agent.AgentResponse
	err        error
	onboarding *agent.OnboardingResponse
}

func (f *fakeAgentService) GetMyAgent(context.Context, uuid.UUID, uuid.UUID) (*agent.AgentResponse, error) {
	return f.response, f.err
}

func (f *fakeAgentService) GetAgentOnboarding(context.Context, uuid.UUID, uuid.UUID) (*agent.OnboardingResponse, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.onboarding == nil {
		return &agent.OnboardingResponse{Capability: &agent.CapabilityResponse{InputSchema: map[string]interface{}{
			"type": "object", "properties": map[string]interface{}{},
		}}}, nil
	}
	return f.onboarding, nil
}

type fakeRuntimeService struct {
	startResponse *runtime.RunResponse
	getResponse   *runtime.RunResponse
	artifacts     map[uuid.UUID][]runtime.RunArtifactResponse
	startCalls    int
}

func (f *fakeRuntimeService) StartRun(context.Context, uuid.UUID, *runtime.RunRequest, string) (*runtime.RunResponse, error) {
	f.startCalls++
	if f.startResponse == nil {
		return nil, errors.New("unexpected StartRun")
	}
	return f.startResponse, nil
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
	validation  *workflow.HostedTargetValidation
	start       *workflow.WorkflowRunResponse
	getResponse *workflow.WorkflowRunResponse
}

func (f *fakeWorkflowService) ValidateHostedExecutionTarget(context.Context, uuid.UUID, uuid.UUID) (*workflow.HostedTargetValidation, error) {
	if f.validation == nil {
		return &workflow.HostedTargetValidation{Executable: true, TargetName: "Workflow"}, nil
	}
	return f.validation, nil
}

func (f *fakeWorkflowService) StartHostedWorkflowRun(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID, map[string]interface{}) (*workflow.WorkflowRunResponse, error) {
	if f.start == nil {
		return nil, errors.New("unexpected StartHostedWorkflowRun")
	}
	return f.start, nil
}

func (f *fakeWorkflowService) GetWorkflowRun(context.Context, uuid.UUID, uuid.UUID) (*workflow.WorkflowRunResponse, error) {
	if f.getResponse == nil {
		return nil, errors.New("unexpected GetWorkflowRun")
	}
	return f.getResponse, nil
}

type memoryStore struct {
	mu      sync.Mutex
	records map[uuid.UUID]ExecutionRecord
}

func newMemoryStore() *memoryStore {
	return &memoryStore{records: map[uuid.UUID]ExecutionRecord{}}
}

func (m *memoryStore) Reserve(_ context.Context, record ExecutionRecord) (ExecutionRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.records[record.ExternalOrderID]; ok {
		return existing, nil
	}
	record.InputFingerprint = append([]byte(nil), record.InputFingerprint...)
	m.records[record.ExternalOrderID] = record
	return record, nil
}

func (m *memoryStore) Get(_ context.Context, id uuid.UUID) (ExecutionRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record, ok := m.records[id]
	if !ok {
		return ExecutionRecord{}, pgx.ErrNoRows
	}
	return record, nil
}

func (m *memoryStore) Attach(_ context.Context, id uuid.UUID, kind string, executionID uuid.UUID) (ExecutionRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record, ok := m.records[id]
	if !ok {
		return ExecutionRecord{}, pgx.ErrNoRows
	}
	if record.ExecutionID == nil {
		record.ExecutionKind = &kind
		record.ExecutionID = &executionID
		m.records[id] = record
	}
	return record, nil
}

func assertHTTPStatus(t *testing.T, err error, status int) {
	t.Helper()
	var he *httpx.HTTPError
	if !errors.As(err, &he) || he.Status != status {
		t.Fatalf("error = %#v, want HTTP %d", err, status)
	}
}
