package servicebridge

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/OpenLinker-ai/openlinker-core/pkg/workflow"
)

type agentService interface {
	GetMyAgent(context.Context, uuid.UUID, uuid.UUID) (*agent.AgentResponse, error)
	GetAgentOnboarding(context.Context, uuid.UUID, uuid.UUID) (*agent.OnboardingResponse, error)
}

type runtimeService interface {
	StartRun(context.Context, uuid.UUID, *runtime.RunRequest, string) (*runtime.RunResponse, error)
	GetRun(context.Context, uuid.UUID, uuid.UUID) (*runtime.RunResponse, error)
	ListRunArtifacts(context.Context, uuid.UUID, uuid.UUID) ([]runtime.RunArtifactResponse, error)
}

type workflowService interface {
	ValidateHostedExecutionTarget(context.Context, uuid.UUID, uuid.UUID) (*workflow.HostedTargetValidation, error)
	StartHostedWorkflowRun(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID, map[string]interface{}) (*workflow.WorkflowRunResponse, error)
	GetWorkflowRun(context.Context, uuid.UUID, uuid.UUID) (*workflow.WorkflowRunResponse, error)
}

type Service struct {
	agents    agentService
	runtime   runtimeService
	workflows workflowService
	store     Store
}

func NewService(agents agentService, runtimeSvc runtimeService, workflows workflowService, store Store) *Service {
	return &Service{agents: agents, runtime: runtimeSvc, workflows: workflows, store: store}
}

func (s *Service) ValidateTarget(ctx context.Context, req *TargetValidationRequest) (*TargetValidationResponse, error) {
	if req == nil {
		return nil, httpx.BadRequest("请求体不能为空")
	}
	sellerID, targetID, targetType, err := parseTargetIdentity(req.SellerUserID, req.TargetID, req.TargetType)
	if err != nil {
		return nil, err
	}
	inputSchema, err := normalizeHostedInputSchema(req.InputSchema)
	if err != nil {
		return nil, err
	}
	resp, err := s.validateTarget(ctx, sellerID, targetType, targetID)
	if err != nil || !resp.Executable || targetType != TargetTypeAgent || inputSchema == nil {
		return resp, err
	}
	onboarding, err := s.agents.GetAgentOnboarding(ctx, targetID, sellerID)
	if err != nil {
		return nil, err
	}
	if onboarding.Capability == nil || !hostedInputSchemaCompatible(inputSchema, onboarding.Capability.InputSchema) {
		resp.Executable = false
		resp.UnavailableReason = "input_schema_incompatible"
	}
	return resp, nil
}

func (s *Service) validateTarget(ctx context.Context, sellerID uuid.UUID, targetType string, targetID uuid.UUID) (*TargetValidationResponse, error) {
	resp := &TargetValidationResponse{TargetType: targetType, TargetID: targetID.String()}
	switch targetType {
	case TargetTypeAgent:
		target, err := s.agents.GetMyAgent(ctx, targetID, sellerID)
		if err != nil {
			var he *httpx.HTTPError
			if errors.As(err, &he) && he.Status < 500 {
				resp.UnavailableReason = "not_found"
				return resp, nil
			}
			return nil, err
		}
		resp.TargetName = target.Name
		if target.LifecycleStatus != "active" {
			resp.UnavailableReason = "not_active"
			return resp, nil
		}
		if target.Visibility != "public" {
			resp.UnavailableReason = "not_public"
			return resp, nil
		}
		if target.Readiness == nil || !target.Readiness.Callable {
			resp.UnavailableReason = "not_callable"
			return resp, nil
		}
		resp.Executable = true
		return resp, nil
	case TargetTypeWorkflow:
		target, err := s.workflows.ValidateHostedExecutionTarget(ctx, sellerID, targetID)
		if err != nil {
			return nil, err
		}
		resp.TargetName = target.TargetName
		resp.Executable = target.Executable
		resp.UnavailableReason = target.UnavailableReason
		return resp, nil
	default:
		return nil, httpx.BadRequest("target_type 必须是 agent 或 workflow")
	}
}

func (s *Service) StartExecution(ctx context.Context, req *ExecutionRequest) (*ExecutionStartResponse, error) {
	parsed, fingerprint, err := parseExecutionRequest(req)
	if err != nil {
		return nil, err
	}
	record, err := s.store.Reserve(ctx, ExecutionRecord{
		ExternalOrderID:  parsed.externalOrderID,
		BuyerUserID:      parsed.buyerUserID,
		SellerUserID:     parsed.sellerUserID,
		TargetType:       parsed.targetType,
		TargetID:         parsed.targetID,
		InputFingerprint: fingerprint[:],
		TraceID:          parsed.traceID,
	})
	if err != nil {
		return nil, httpx.Internal("保存 Hosted 执行幂等记录失败")
	}
	if !executionRecordMatches(record, parsed, fingerprint) {
		return nil, httpx.Conflict("external_order_id 已用于不同的执行请求")
	}
	if record.ExecutionID != nil {
		status, err := s.getExecutionStatus(ctx, record)
		if err != nil {
			return nil, err
		}
		return &ExecutionStartResponse{ExecutionID: record.ExecutionID.String(), Status: status.Status}, nil
	}

	validation, err := s.validateTarget(ctx, parsed.sellerUserID, parsed.targetType, parsed.targetID)
	if err != nil {
		return nil, err
	}
	if !validation.Executable {
		return nil, httpx.Conflict("该服务目标当前不可执行")
	}

	var executionID uuid.UUID
	var executionKind string
	switch parsed.targetType {
	case TargetTypeAgent:
		started, err := s.runtime.StartRun(ctx, parsed.buyerUserID, &runtime.RunRequest{
			AgentID:        parsed.targetID.String(),
			Input:          parsed.input,
			IdempotencyKey: "hosted-service-order/" + parsed.externalOrderID.String(),
			Metadata: map[string]interface{}{
				"external_order_id": parsed.externalOrderID.String(),
				"seller_user_id":    parsed.sellerUserID.String(),
				"trace_id":          parsed.traceID,
			},
			CreationProtocol: "hosted",
			CreationMethod:   "service-order.execute",
		}, "api")
		if err != nil {
			return nil, err
		}
		executionID, err = uuid.Parse(started.RunID)
		if err != nil {
			return nil, httpx.Internal("Runtime 返回了无效 execution_id")
		}
		executionKind = "run"
	case TargetTypeWorkflow:
		started, err := s.workflows.StartHostedWorkflowRun(ctx, parsed.sellerUserID, parsed.buyerUserID, parsed.targetID, parsed.externalOrderID, parsed.input)
		if err != nil {
			return nil, err
		}
		executionID, err = uuid.Parse(started.ID)
		if err != nil {
			return nil, httpx.Internal("Workflow 返回了无效 execution_id")
		}
		executionKind = "workflow_run"
	}

	record, err = s.store.Attach(ctx, parsed.externalOrderID, executionKind, executionID)
	if err != nil {
		return nil, httpx.Internal("保存 Hosted execution_id 失败")
	}
	if record.ExecutionID == nil || *record.ExecutionID != executionID || record.ExecutionKind == nil || *record.ExecutionKind != executionKind {
		return nil, httpx.Conflict("external_order_id 已关联其他执行")
	}
	status, err := s.getExecutionStatus(ctx, record)
	if err != nil {
		return nil, err
	}
	return &ExecutionStartResponse{ExecutionID: executionID.String(), Status: status.Status}, nil
}

func (s *Service) GetExecution(ctx context.Context, externalOrderID string) (*ExecutionStatusResponse, error) {
	id, err := uuid.Parse(strings.TrimSpace(externalOrderID))
	if err != nil || id == uuid.Nil {
		return nil, httpx.BadRequest("external_order_id 不是合法 uuid")
	}
	record, err := s.store.Get(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("Hosted 执行不存在")
	}
	if err != nil {
		return nil, httpx.Internal("查询 Hosted 执行失败")
	}
	return s.getExecutionStatus(ctx, record)
}

func (s *Service) getExecutionStatus(ctx context.Context, record ExecutionRecord) (*ExecutionStatusResponse, error) {
	resp := &ExecutionStatusResponse{
		ExternalOrderID: record.ExternalOrderID.String(),
		TargetType:      record.TargetType,
		Status:          "pending",
		Artifacts:       []runtime.RunArtifactResponse{},
	}
	if record.ExecutionID == nil || record.ExecutionKind == nil {
		return resp, nil
	}
	resp.ExecutionID = record.ExecutionID.String()
	switch *record.ExecutionKind {
	case "run":
		run, err := s.runtime.GetRun(ctx, record.BuyerUserID, *record.ExecutionID)
		if err != nil {
			return nil, err
		}
		status, safeError := normalizeRuntimeStatus(run.Status)
		resp.Status = status
		setSafeExecutionError(resp, safeError)
		if resp.Status == "succeeded" {
			resp.Output = run.Output
		}
		artifacts, err := s.runtime.ListRunArtifacts(ctx, record.BuyerUserID, *record.ExecutionID)
		if err != nil {
			return nil, err
		}
		if artifacts == nil {
			artifacts = []runtime.RunArtifactResponse{}
		}
		resp.Artifacts = artifacts
		resp.StartedAt, resp.FinishedAt = runtimeTimestamps(run)
		return resp, nil
	case "workflow_run":
		run, err := s.workflows.GetWorkflowRun(ctx, record.BuyerUserID, *record.ExecutionID)
		if err != nil {
			return nil, err
		}
		status, safeError := normalizeWorkflowStatus(run.Status)
		resp.Status = status
		setSafeExecutionError(resp, safeError)
		if resp.Status == "succeeded" {
			resp.Output = run.Output
		}
		resp.StartedAt = run.StartedAt
		resp.FinishedAt = run.FinishedAt
		seen := map[string]struct{}{}
		for _, step := range run.Steps {
			childID, err := uuid.Parse(step.RunID)
			if err != nil || childID == uuid.Nil {
				continue
			}
			artifacts, err := s.runtime.ListRunArtifacts(ctx, record.BuyerUserID, childID)
			if err != nil {
				return nil, err
			}
			for _, artifact := range artifacts {
				if _, ok := seen[artifact.ID]; ok {
					continue
				}
				seen[artifact.ID] = struct{}{}
				resp.Artifacts = append(resp.Artifacts, artifact)
			}
		}
		return resp, nil
	default:
		return nil, httpx.Internal("Hosted 执行类型无效")
	}
}

type parsedExecution struct {
	externalOrderID uuid.UUID
	buyerUserID     uuid.UUID
	sellerUserID    uuid.UUID
	targetType      string
	targetID        uuid.UUID
	input           map[string]interface{}
	traceID         string
}

func parseExecutionRequest(req *ExecutionRequest) (parsedExecution, [sha256.Size]byte, error) {
	if req == nil {
		return parsedExecution{}, [sha256.Size]byte{}, httpx.BadRequest("请求体不能为空")
	}
	externalOrderID, err := uuid.Parse(strings.TrimSpace(req.ExternalOrderID))
	if err != nil || externalOrderID == uuid.Nil {
		return parsedExecution{}, [sha256.Size]byte{}, httpx.BadRequest("external_order_id 不是合法 uuid")
	}
	buyerID, err := uuid.Parse(strings.TrimSpace(req.BuyerUserID))
	if err != nil || buyerID == uuid.Nil {
		return parsedExecution{}, [sha256.Size]byte{}, httpx.BadRequest("buyer_user_id 不是合法 uuid")
	}
	sellerID, targetID, targetType, err := parseTargetIdentity(req.SellerUserID, req.TargetID, req.TargetType)
	if err != nil {
		return parsedExecution{}, [sha256.Size]byte{}, err
	}
	traceID := strings.TrimSpace(req.TraceID)
	if traceID == "" || len(traceID) > 200 {
		return parsedExecution{}, [sha256.Size]byte{}, httpx.BadRequest("trace_id 长度必须为 1 到 200")
	}
	input := req.Input
	if input == nil {
		input = map[string]interface{}{}
	}
	parsed := parsedExecution{
		externalOrderID: externalOrderID, buyerUserID: buyerID, sellerUserID: sellerID,
		targetType: targetType, targetID: targetID, input: input, traceID: traceID,
	}
	canonical, err := runtime.CanonicalizeRFC8785(map[string]interface{}{
		"buyer_user_id": buyerID.String(), "seller_user_id": sellerID.String(),
		"target_type": targetType, "target_id": targetID.String(),
		"input": input, "trace_id": traceID,
	})
	if err != nil {
		return parsedExecution{}, [sha256.Size]byte{}, httpx.BadRequest("input 不是合法 I-JSON")
	}
	return parsed, sha256.Sum256(canonical), nil
}

func parseTargetIdentity(sellerRaw, targetRaw, targetTypeRaw string) (uuid.UUID, uuid.UUID, string, error) {
	sellerID, err := uuid.Parse(strings.TrimSpace(sellerRaw))
	if err != nil || sellerID == uuid.Nil {
		return uuid.Nil, uuid.Nil, "", httpx.BadRequest("seller_user_id 不是合法 uuid")
	}
	targetID, err := uuid.Parse(strings.TrimSpace(targetRaw))
	if err != nil || targetID == uuid.Nil {
		return uuid.Nil, uuid.Nil, "", httpx.BadRequest("target_id 不是合法 uuid")
	}
	targetType := strings.ToLower(strings.TrimSpace(targetTypeRaw))
	if targetType != TargetTypeAgent && targetType != TargetTypeWorkflow {
		return uuid.Nil, uuid.Nil, "", httpx.BadRequest("target_type 必须是 agent 或 workflow")
	}
	return sellerID, targetID, targetType, nil
}

func normalizeHostedInputSchema(raw json.RawMessage) (map[string]interface{}, error) {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil
	}
	var value interface{}
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, httpx.BadRequest("input_schema 不是合法 JSON")
	}
	switch typed := value.(type) {
	case map[string]interface{}:
		return typed, nil
	case []interface{}:
		return controlledFieldsToJSONSchema(typed)
	default:
		return nil, httpx.BadRequest("input_schema 必须是 JSON object 或 array")
	}
}

func executionRecordMatches(record ExecutionRecord, parsed parsedExecution, fingerprint [sha256.Size]byte) bool {
	return record.ExternalOrderID == parsed.externalOrderID &&
		record.BuyerUserID == parsed.buyerUserID &&
		record.SellerUserID == parsed.sellerUserID &&
		record.TargetType == parsed.targetType &&
		record.TargetID == parsed.targetID &&
		bytes.Equal(record.InputFingerprint, fingerprint[:]) &&
		record.TraceID == parsed.traceID
}

func normalizeRuntimeStatus(status string) (string, *SafeExecutionError) {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "pending":
		return "pending", nil
	case "running":
		return "running", nil
	case "success":
		return "succeeded", nil
	case "timeout":
		return "failed", &SafeExecutionError{Code: "EXECUTION_TIMEOUT", Message: "Execution timed out."}
	case "canceled":
		return "failed", &SafeExecutionError{Code: "EXECUTION_CANCELED", Message: "Execution was canceled."}
	case "failed":
		return "failed", &SafeExecutionError{Code: "EXECUTION_FAILED", Message: "Execution failed."}
	default:
		return "failed", &SafeExecutionError{Code: "EXECUTION_STATUS_UNKNOWN", Message: "Execution ended with an unknown status."}
	}
}

func normalizeWorkflowStatus(status string) (string, *SafeExecutionError) {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "pending":
		return "pending", nil
	case "running", "paused":
		return "running", nil
	case "success":
		return "succeeded", nil
	case "canceled":
		return "failed", &SafeExecutionError{Code: "EXECUTION_CANCELED", Message: "Execution was canceled."}
	case "failed":
		return "failed", &SafeExecutionError{Code: "WORKFLOW_EXECUTION_FAILED", Message: "Workflow execution failed."}
	default:
		return "failed", &SafeExecutionError{Code: "EXECUTION_STATUS_UNKNOWN", Message: "Execution ended with an unknown status."}
	}
}

func setSafeExecutionError(resp *ExecutionStatusResponse, safeError *SafeExecutionError) {
	if resp == nil || safeError == nil {
		return
	}
	resp.ErrorCode = safeError.Code
	resp.ErrorMessage = safeError.Message
}

func runtimeTimestamps(run *runtime.RunResponse) (string, string) {
	if run == nil {
		return "", ""
	}
	started := ""
	if !run.StartedAt.IsZero() {
		started = run.StartedAt.UTC().Format(time.RFC3339Nano)
	}
	finished := ""
	if run.FinishedAt != nil {
		finished = run.FinishedAt.UTC().Format(time.RFC3339Nano)
	}
	return started, finished
}
