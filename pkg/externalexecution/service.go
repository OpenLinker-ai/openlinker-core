package externalexecution

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	"github.com/OpenLinker-ai/openlinker-core/pkg/executioncontract"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/OpenLinker-ai/openlinker-core/pkg/workflow"
)

type agentService interface {
	GetMyAgent(context.Context, uuid.UUID, uuid.UUID) (*agent.AgentResponse, error)
	GetAgentOnboarding(context.Context, uuid.UUID, uuid.UUID) (*agent.OnboardingResponse, error)
}

type runtimeService interface {
	LookupRunByCreationRequest(context.Context, uuid.UUID, *runtime.RunRequest, string) (*runtime.RunResponse, bool, error)
	LookupRunByCreationIdentity(context.Context, uuid.UUID, []byte, []byte) (*runtime.RunResponse, bool, error)
	StartRun(context.Context, uuid.UUID, *runtime.RunRequest, string) (*runtime.RunResponse, error)
	StartExternalRun(context.Context, uuid.UUID, *runtime.RunRequest, string, runtime.ExternalExecutionLaunchFence) (*runtime.RunResponse, error)
	GetRun(context.Context, uuid.UUID, uuid.UUID) (*runtime.RunResponse, error)
	ListRunArtifacts(context.Context, uuid.UUID, uuid.UUID) ([]runtime.RunArtifactResponse, error)
	CancelRun(context.Context, uuid.UUID, uuid.UUID) (*runtime.RunResponse, error)
	GetRunCancellationEvidence(context.Context, uuid.UUID, uuid.UUID) (runtime.RunCancellationEvidence, error)
}

type WorkflowTargetValidation struct {
	TargetName        string
	Executable        bool
	UnavailableReason string
	ContractHash      string
}

type workflowService interface {
	ValidateExternalExecutionTarget(context.Context, uuid.UUID, uuid.UUID) (*WorkflowTargetValidation, error)
	LookupExternalExecutionWorkflowRun(context.Context, string, uuid.UUID, uuid.UUID, uuid.UUID, map[string]interface{}) (*workflow.WorkflowRunResponse, bool, error)
	LookupExternalExecutionWorkflowRunByIdentity(context.Context, string, uuid.UUID, uuid.UUID, uuid.UUID) (*workflow.WorkflowRunResponse, bool, error)
	StartExternalWorkflowRun(context.Context, string, uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID, map[string]interface{}) (*workflow.WorkflowRunResponse, error)
	StartExternalExecutionWorkflowRunWithFence(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, map[string]interface{}, workflow.ExternalExecutionLaunchFence) (*workflow.WorkflowRunResponse, error)
	GetWorkflowRun(context.Context, uuid.UUID, uuid.UUID) (*workflow.WorkflowRunResponse, error)
	CancelExternalWorkflowRun(context.Context, uuid.UUID, uuid.UUID, string) (*workflow.WorkflowRunResponse, workflow.CancellationEvidence, error)
	GetWorkflowCancellationEvidence(context.Context, uuid.UUID, uuid.UUID) (workflow.CancellationEvidence, error)
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

func (s *Service) ValidateTarget(ctx context.Context, principal *Principal, req *TargetValidationRequest) (*TargetValidationResponse, error) {
	if err := validatePrincipal(principal); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, httpx.BadRequest("请求体不能为空")
	}
	targetID, targetType, err := parseTargetIdentity(req.TargetID, req.TargetType)
	if err != nil {
		return nil, err
	}
	inputSchema, err := normalizeExternalInputSchema(req.InputSchema)
	if err != nil {
		return nil, err
	}
	targetOwnerID, err := s.store.ResolveTargetOwner(ctx, targetType, targetID)
	if errors.Is(err, pgx.ErrNoRows) {
		return &TargetValidationResponse{
			TargetType: targetType, TargetID: targetID.String(), UnavailableReason: "not_found",
		}, nil
	}
	if err != nil {
		return nil, httpx.Internal("查询外部执行目标失败")
	}
	if targetOwnerID != principal.ActorUserID {
		return &TargetValidationResponse{
			TargetType: targetType, TargetID: targetID.String(), UnavailableReason: "not_found",
		}, nil
	}
	return s.validateTarget(ctx, targetOwnerID, targetType, targetID, inputSchema)
}

func (s *Service) validateTarget(ctx context.Context, targetOwnerID uuid.UUID, targetType string, targetID uuid.UUID, inputSchema map[string]interface{}) (*TargetValidationResponse, error) {
	resp := &TargetValidationResponse{TargetType: targetType, TargetID: targetID.String()}
	switch targetType {
	case TargetTypeAgent:
		target, err := s.agents.GetMyAgent(ctx, targetID, targetOwnerID)
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
		onboarding, err := s.agents.GetAgentOnboarding(ctx, targetID, targetOwnerID)
		if err != nil {
			return nil, err
		}
		if onboarding.Capability == nil || (inputSchema != nil && !externalInputSchemaCompatible(inputSchema, onboarding.Capability.InputSchema)) {
			resp.UnavailableReason = "input_schema_incompatible"
			return resp, nil
		}
		connectionMode := strings.TrimSpace(target.ConnectionMode)
		if connectionMode == "" {
			connectionMode = "direct_http"
		}
		mcpToolName := ""
		if target.MCPToolName != nil {
			mcpToolName = *target.MCPToolName
		}
		contractHash, err := executioncontract.AgentHash(executioncontract.Agent{
			ID: target.ID, ConnectionMode: connectionMode, EndpointURL: target.EndpointURL,
			MCPToolName: mcpToolName, CapabilityVersion: onboarding.Capability.Version,
			InputSchema: onboarding.Capability.InputSchema, OutputSchema: onboarding.Capability.OutputSchema,
		})
		if err != nil {
			return nil, httpx.Internal("计算 Agent contract 失败")
		}
		resp.Executable = true
		resp.ContractHash = contractHash
		return resp, nil
	case TargetTypeWorkflow:
		target, err := s.workflows.ValidateExternalExecutionTarget(ctx, targetOwnerID, targetID)
		if err != nil {
			return nil, err
		}
		resp.TargetName = target.TargetName
		resp.Executable = target.Executable
		resp.UnavailableReason = target.UnavailableReason
		resp.ContractHash = target.ContractHash
		if resp.Executable && !executioncontract.Valid(resp.ContractHash) {
			return nil, httpx.Internal("Workflow 返回了无效 execution contract")
		}
		return resp, nil
	default:
		return nil, httpx.BadRequest("target_type 必须是 agent 或 workflow")
	}
}

func (s *Service) StartExecution(ctx context.Context, principal *Principal, req *ExecutionRequest) (*ExecutionStartResponse, error) {
	if err := validatePrincipal(principal); err != nil {
		return nil, err
	}
	parsed, err := parseExecutionRequest(req)
	if err != nil {
		return nil, err
	}
	parsed.callerServiceID = principal.CallerServiceID
	parsed.actorUserID = principal.ActorUserID
	fingerprint, err := executionRequestFingerprint(parsed)
	if err != nil {
		return nil, err
	}

	// Reserve the immutable identity before consulting mutable target state. An
	// attached record is authoritative; an unattached record first performs a
	// read-only downstream lookup and then enters the durable starter state
	// machine. This prevents concurrent callers from disagreeing about a target
	// while one of them is already authorized to create the execution.
	record, err := s.store.Get(ctx, parsed.callerServiceID, parsed.externalRequestID)
	if errors.Is(err, pgx.ErrNoRows) {
		record, err = s.store.Reserve(ctx, ExecutionRecord{
			CallerServiceID:           parsed.callerServiceID,
			ExternalRequestID:         parsed.externalRequestID,
			RequestFingerprintVersion: currentRequestFingerprintVersion,
			ActorUserID:               parsed.actorUserID,
			TargetType:                parsed.targetType,
			TargetID:                  parsed.targetID,
			InputFingerprint:          fingerprint[:],
			ExpectedContractHash:      &parsed.expectedContractHash,
			InputSchemaFingerprint:    parsed.inputSchemaFingerprint[:],
			TraceID:                   parsed.traceID,
		})
		if err != nil {
			if errors.Is(err, ErrExecutionCanceled) {
				return nil, externalExecutionCanceledError()
			}
			if errors.Is(err, ErrExecutionIdentityConflict) {
				return nil, httpx.Conflict("external_request_id 已绑定其他 actor")
			}
			return nil, httpx.Internal("保存外部执行幂等记录失败")
		}
	} else if err != nil {
		return nil, httpx.Internal("查询外部执行幂等记录失败")
	}
	if !executionRecordMatches(record, parsed, fingerprint) {
		return nil, httpx.Conflict("external_request_id 已用于不同的执行请求")
	}
	if rejectionErr := executionStartRejection(record); rejectionErr != nil {
		return nil, rejectionErr
	}
	if response, complete, replayErr := s.responseForReservedRecord(ctx, record); complete || replayErr != nil {
		return response, replayErr
	}
	switch record.StartState {
	case startStateAuthorized:
		response, recovered, recoveryErr := s.lookupAndAttachReservedExecution(ctx, record, parsed)
		if recoveryErr != nil {
			return nil, httpx.ServiceUnavailable("外部执行启动状态正在确认，请重试")
		}
		if recovered {
			return response, nil
		}
	case startStateLaunching:
		response, recovered, recoveryErr := s.lookupAndAttachLaunchingExecution(ctx, record, parsed)
		if recoveryErr != nil {
			return nil, recoveryErr
		}
		if recovered {
			return response, nil
		}
		if record.StartLeaseUntil != nil && record.StartLeaseUntil.After(time.Now()) {
			return nil, httpx.ServiceUnavailable("外部执行正在启动，请重试")
		}
	case startStatePending, startStateEvaluating:
	default:
		return nil, httpx.Internal("外部执行启动状态无效")
	}

	if record.StartState != startStateAuthorized {
		var response *ExecutionStartResponse
		record, response, err = s.ensureExecutionStartAuthorized(ctx, record, parsed, fingerprint)
		if err != nil || response != nil {
			return response, err
		}
	}
	if record.AuthorizedTargetOwnerID == nil || *record.AuthorizedTargetOwnerID == uuid.Nil {
		return nil, httpx.Internal("外部执行授权缺少目标所有者快照")
	}
	parsed.targetOwnerUserID = *record.AuthorizedTargetOwnerID
	launchToken := uuid.New()
	launchRecord, claimed, err := s.store.ClaimLaunch(
		ctx, parsed.callerServiceID, parsed.externalRequestID, launchToken, startLaunchLease,
	)
	if err != nil {
		return nil, httpx.Internal("竞争外部执行 launch fence 失败")
	}
	if !claimed {
		if rejectionErr := executionStartRejection(launchRecord); rejectionErr != nil {
			return nil, rejectionErr
		}
		response, recovered, recoveryErr := s.lookupAndAttachLaunchingExecution(ctx, launchRecord, parsed)
		if recoveryErr != nil {
			return nil, recoveryErr
		}
		if recovered {
			return response, nil
		}
		return nil, httpx.ServiceUnavailable("外部执行 launch fence 正在确认，请重试")
	}
	record = launchRecord

	var executionID uuid.UUID
	var executionKind string
	switch parsed.targetType {
	case TargetTypeAgent:
		runRequest, source, requestErr := runtimeCreationRequest(record, parsed)
		if requestErr != nil {
			return nil, requestErr
		}
		started, err := s.runtime.StartExternalRun(ctx, parsed.actorUserID, runRequest, source, runtime.ExternalExecutionLaunchFence{
			CallerServiceID: parsed.callerServiceID, ExternalRequestID: parsed.externalRequestID,
			ActorUserID: parsed.actorUserID, LaunchToken: launchToken,
		})
		if err != nil {
			if errors.Is(err, runtime.ErrExternalExecutionLaunchFenceRejected) {
				return nil, s.launchFenceFailure(ctx, parsed)
			}
			return nil, httpx.ServiceUnavailable("Runtime 启动状态正在确认，请重试")
		}
		executionID, err = uuid.Parse(started.RunID)
		if err != nil {
			return nil, httpx.Internal("Runtime 返回了无效 execution_id")
		}
		executionKind = "run"
	case TargetTypeWorkflow:
		started, err := s.workflows.StartExternalExecutionWorkflowRunWithFence(
			ctx, parsed.targetOwnerUserID, parsed.actorUserID, parsed.targetID, parsed.input,
			workflow.ExternalExecutionLaunchFence{
				CallerServiceID: parsed.callerServiceID, ExternalRequestID: parsed.externalRequestID,
				ActorUserID: parsed.actorUserID, LaunchToken: launchToken,
			},
		)
		if err != nil {
			if errors.Is(err, workflow.ErrExternalWorkflowLaunchFenceRejected) {
				return nil, s.launchFenceFailure(ctx, parsed)
			}
			return nil, httpx.ServiceUnavailable("Workflow 启动状态正在确认，请重试")
		}
		executionID, err = uuid.Parse(started.ID)
		if err != nil {
			return nil, httpx.Internal("Workflow 返回了无效 execution_id")
		}
		executionKind = "workflow_run"
	}

	return s.attachLaunchedExecution(ctx, parsed, launchToken, executionKind, executionID)
}

const startEvaluationLease = 30 * time.Second
const startLaunchLease = 30 * time.Second

const (
	startStatePending    = "pending"
	startStateEvaluating = "evaluating"
	startStateAuthorized = "authorized"
	startStateLaunching  = "launching"
	startStateAttached   = "attached"
	startStateRejected   = "rejected"
	startStateCanceled   = "canceled"
)

func (s *Service) ensureExecutionStartAuthorized(
	ctx context.Context,
	record ExecutionRecord,
	parsed parsedExecution,
	fingerprint [sha256.Size]byte,
) (ExecutionRecord, *ExecutionStartResponse, error) {
	switch record.StartState {
	case startStateAuthorized:
		return record, nil, nil
	case startStateRejected:
		return ExecutionRecord{}, nil, executionStartRejection(record)
	case startStatePending, startStateEvaluating:
	default:
		return ExecutionRecord{}, nil, httpx.Internal("外部执行启动状态无效")
	}

	token := uuid.New()
	claimedRecord, claimed, err := s.store.ClaimStartEvaluation(
		ctx, parsed.callerServiceID, parsed.externalRequestID, token, startEvaluationLease,
	)
	if err != nil {
		return ExecutionRecord{}, nil, httpx.Internal("竞争外部执行启动权失败")
	}
	if !executionRecordMatches(claimedRecord, parsed, fingerprint) {
		return ExecutionRecord{}, nil, httpx.Conflict("external_request_id 已用于不同的执行请求")
	}
	if response, complete, replayErr := s.responseForReservedRecord(ctx, claimedRecord); complete || replayErr != nil {
		return ExecutionRecord{}, response, replayErr
	}
	if !claimed {
		if claimedRecord.StartState == startStateRejected {
			return ExecutionRecord{}, nil, executionStartRejection(claimedRecord)
		}
		return ExecutionRecord{}, nil, httpx.ServiceUnavailable("外部执行请求正在处理中，请重试")
	}
	if claimedRecord.StartState != startStateEvaluating || claimedRecord.StartToken == nil || *claimedRecord.StartToken != token {
		return ExecutionRecord{}, nil, httpx.Internal("外部执行启动权状态无效")
	}

	claimActive := true
	defer func() {
		if !claimActive {
			return
		}
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.store.ReleaseStartEvaluation(releaseCtx, parsed.callerServiceID, parsed.externalRequestID, token)
	}()

	response, recovered, recoveryErr := s.lookupAndAttachReservedExecution(ctx, claimedRecord, parsed)
	if recoveryErr != nil {
		if isExecutionErrorCode(recoveryErr, httpx.ErrorCode("TARGET_UNAVAILABLE")) ||
			isExecutionErrorCode(recoveryErr, httpx.ErrorCode("TARGET_CONTRACT_CHANGED")) ||
			isExecutionErrorCode(recoveryErr, httpx.ErrorCode("DOWNSTREAM_IDENTITY_CONFLICT")) {
			return ExecutionRecord{}, nil, recoveryErr
		}
		if errors.Is(recoveryErr, errDownstreamIdentityConflict) {
			rejected, rejectedResponse, rejectErr := s.rejectStartEvaluation(
				ctx, parsed, fingerprint, token, "DOWNSTREAM_IDENTITY_CONFLICT",
			)
			if rejected {
				claimActive = false
			}
			return ExecutionRecord{}, rejectedResponse, rejectErr
		}
		if isExecutionErrorCode(recoveryErr, httpx.CodeConflict) {
			return ExecutionRecord{}, nil, recoveryErr
		}
		var recoveryHTTPError *httpx.HTTPError
		if errors.As(recoveryErr, &recoveryHTTPError) && recoveryHTTPError.Status >= 500 {
			return ExecutionRecord{}, nil, recoveryErr
		}
		return ExecutionRecord{}, nil, httpx.ServiceUnavailable("下游执行状态正在确认，请重试")
	}
	if recovered {
		claimActive = false
		return ExecutionRecord{}, response, nil
	}

	if claimedRecord.RequestFingerprintVersion == legacyRequestFingerprintVersion &&
		claimedRecord.TargetType != TargetTypeAgent && claimedRecord.ExecutionID == nil && claimedRecord.ExecutionKind == nil {
		claimedRecord, err = s.store.PromoteLegacyReservation(ctx, ExecutionRecord{
			CallerServiceID:           parsed.callerServiceID,
			ExternalRequestID:         parsed.externalRequestID,
			RequestFingerprintVersion: currentRequestFingerprintVersion,
			ActorUserID:               parsed.actorUserID,
			TargetType:                parsed.targetType,
			TargetID:                  parsed.targetID,
			InputFingerprint:          fingerprint[:],
			ExpectedContractHash:      &parsed.expectedContractHash,
			InputSchemaFingerprint:    parsed.inputSchemaFingerprint[:],
			TraceID:                   parsed.traceID,
		})
		if err != nil {
			return ExecutionRecord{}, nil, httpx.Internal("升级外部执行幂等记录失败")
		}
	}
	if !executionRecordMatches(claimedRecord, parsed, fingerprint) {
		return ExecutionRecord{}, nil, httpx.Conflict("external_request_id 已用于不同的执行请求")
	}
	if response, complete, replayErr := s.responseForReservedRecord(ctx, claimedRecord); complete || replayErr != nil {
		return ExecutionRecord{}, response, replayErr
	}
	if rejectionErr := executionStartRejection(claimedRecord); rejectionErr != nil {
		return ExecutionRecord{}, nil, rejectionErr
	}

	targetOwnerID, err := s.resolveTargetOwner(ctx, parsed)
	if err != nil {
		if isExecutionErrorCode(err, httpx.ErrorCode("TARGET_UNAVAILABLE")) {
			rejected, response, rejectErr := s.rejectStartEvaluation(ctx, parsed, fingerprint, token, "TARGET_UNAVAILABLE")
			if rejected {
				claimActive = false
			}
			return ExecutionRecord{}, response, rejectErr
		}
		return ExecutionRecord{}, nil, err
	}
	validation, err := s.validateTarget(ctx, targetOwnerID, parsed.targetType, parsed.targetID, parsed.inputSchema)
	if err != nil {
		var validationHTTPError *httpx.HTTPError
		if errors.As(err, &validationHTTPError) && validationHTTPError.Status >= 400 && validationHTTPError.Status < 500 {
			rejected, response, rejectErr := s.rejectStartEvaluation(ctx, parsed, fingerprint, token, "TARGET_UNAVAILABLE")
			if rejected {
				claimActive = false
			}
			return ExecutionRecord{}, response, rejectErr
		}
		return ExecutionRecord{}, nil, err
	}
	rejectionCode := ""
	if !validation.Executable {
		if validation.UnavailableReason == "input_schema_incompatible" {
			rejectionCode = "TARGET_CONTRACT_CHANGED"
		} else {
			rejectionCode = "TARGET_UNAVAILABLE"
		}
	} else if subtle.ConstantTimeCompare([]byte(validation.ContractHash), []byte(parsed.expectedContractHash)) != 1 {
		rejectionCode = "TARGET_CONTRACT_CHANGED"
	}
	if rejectionCode != "" {
		rejected, response, rejectErr := s.rejectStartEvaluation(ctx, parsed, fingerprint, token, rejectionCode)
		if rejected {
			claimActive = false
		}
		return ExecutionRecord{}, response, rejectErr
	}

	authorizedRecord, authorized, err := s.store.AuthorizeStart(
		ctx, parsed.callerServiceID, parsed.externalRequestID, token, targetOwnerID,
	)
	if err != nil {
		return ExecutionRecord{}, nil, httpx.Internal("保存外部执行启动授权失败")
	}
	if !executionRecordMatches(authorizedRecord, parsed, fingerprint) {
		return ExecutionRecord{}, nil, httpx.Conflict("external_request_id 已用于不同的执行请求")
	}
	if !authorized {
		if response, complete, replayErr := s.responseForReservedRecord(ctx, authorizedRecord); complete || replayErr != nil {
			return ExecutionRecord{}, response, replayErr
		}
		if authorizedRecord.StartState == startStateRejected {
			return ExecutionRecord{}, nil, executionStartRejection(authorizedRecord)
		}
		return ExecutionRecord{}, nil, httpx.ServiceUnavailable("外部执行启动授权已过期，请重试")
	}
	claimActive = false
	return authorizedRecord, nil, nil
}

func (s *Service) rejectStartEvaluation(
	ctx context.Context,
	parsed parsedExecution,
	fingerprint [sha256.Size]byte,
	token uuid.UUID,
	rejectionCode string,
) (bool, *ExecutionStartResponse, error) {
	rejectedRecord, rejected, err := s.store.RejectStart(
		ctx, parsed.callerServiceID, parsed.externalRequestID, token, rejectionCode,
	)
	if err != nil {
		return false, nil, httpx.Internal("保存外部执行拒绝状态失败")
	}
	if !executionRecordMatches(rejectedRecord, parsed, fingerprint) {
		return false, nil, httpx.Conflict("external_request_id 已用于不同的执行请求")
	}
	if !rejected {
		if response, complete, replayErr := s.responseForReservedRecord(ctx, rejectedRecord); complete || replayErr != nil {
			return false, response, replayErr
		}
		if rejectedRecord.StartState == startStateRejected {
			return false, nil, executionStartRejection(rejectedRecord)
		}
		return false, nil, httpx.ServiceUnavailable("外部执行启动决策已过期，请重试")
	}
	return true, nil, executionStartRejection(rejectedRecord)
}

func executionStartRejection(record ExecutionRecord) error {
	if record.StartState == startStateCanceled {
		return externalExecutionCanceledError()
	}
	if record.StartState != startStateRejected {
		return nil
	}
	if record.RejectionCode == nil {
		return httpx.Internal("外部执行拒绝状态无效")
	}
	switch *record.RejectionCode {
	case "TARGET_UNAVAILABLE":
		return targetUnavailableError()
	case "TARGET_CONTRACT_CHANGED":
		return targetContractChangedError()
	case "DOWNSTREAM_IDENTITY_CONFLICT":
		return downstreamIdentityConflictError()
	default:
		return httpx.Internal("外部执行拒绝原因无效")
	}
}

func externalExecutionCanceledError() error {
	return httpx.NewError(
		http.StatusConflict,
		httpx.ErrorCode("EXTERNAL_EXECUTION_CANCELED"),
		"external execution 已取消",
	)
}

func isExecutionErrorCode(err error, code httpx.ErrorCode) bool {
	var httpErr *httpx.HTTPError
	return errors.As(err, &httpErr) && httpErr.Code == code
}

func (s *Service) resolveTargetOwner(ctx context.Context, parsed parsedExecution) (uuid.UUID, error) {
	ownerID, err := s.store.ResolveTargetOwner(ctx, parsed.targetType, parsed.targetID)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, targetUnavailableError()
	}
	if err != nil {
		return uuid.Nil, httpx.Internal("查询外部执行目标失败")
	}
	return ownerID, nil
}

func (s *Service) lookupAndAttachReservedExecution(
	ctx context.Context,
	record ExecutionRecord,
	parsed parsedExecution,
) (*ExecutionStartResponse, bool, error) {
	switch record.TargetType {
	case TargetTypeAgent:
		runRequest, source, err := runtimeCreationRequest(record, parsed)
		if err != nil {
			return nil, false, err
		}
		existing, found, err := s.runtime.LookupRunByCreationRequest(ctx, parsed.actorUserID, runRequest, source)
		if isExecutionErrorCode(err, httpx.CodeConflict) {
			return nil, false, errDownstreamIdentityConflict
		}
		if err != nil || !found {
			return nil, false, err
		}
		executionID, err := uuid.Parse(strings.TrimSpace(existing.RunID))
		if err != nil || executionID == uuid.Nil {
			return nil, false, httpx.Internal("Runtime 返回了无效 execution_id")
		}
		response, err := s.attachExecution(ctx, parsed, "run", executionID)
		return response, true, err
	case TargetTypeWorkflow:
		if record.RequestFingerprintVersion != currentRequestFingerprintVersion {
			return nil, false, nil
		}
		existing, found, err := s.workflows.LookupExternalExecutionWorkflowRun(
			ctx, parsed.callerServiceID, parsed.actorUserID, parsed.targetID, parsed.externalRequestID, parsed.input,
		)
		if isExecutionErrorCode(err, httpx.CodeConflict) {
			return nil, false, errDownstreamIdentityConflict
		}
		if err != nil || !found {
			return nil, false, err
		}
		executionID, err := uuid.Parse(strings.TrimSpace(existing.ID))
		if err != nil || executionID == uuid.Nil {
			return nil, false, httpx.Internal("Workflow 返回了无效 execution_id")
		}
		response, err := s.attachExecution(ctx, parsed, "workflow_run", executionID)
		return response, true, err
	default:
		return nil, false, httpx.Internal("外部执行类型无效")
	}
}

func (s *Service) lookupAndAttachLaunchingExecution(
	ctx context.Context,
	record ExecutionRecord,
	parsed parsedExecution,
) (*ExecutionStartResponse, bool, error) {
	if record.StartState != startStateLaunching || record.StartToken == nil || *record.StartToken == uuid.Nil {
		return nil, false, nil
	}
	var executionID uuid.UUID
	var executionKind string
	switch record.TargetType {
	case TargetTypeAgent:
		if len(record.DownstreamKeyHash) == 0 && len(record.DownstreamFingerprint) == 0 {
			return nil, false, nil
		}
		if len(record.DownstreamKeyHash) != sha256.Size || len(record.DownstreamFingerprint) != sha256.Size {
			return nil, false, httpx.Internal("外部执行下游恢复身份无效")
		}
		existing, found, err := s.runtime.LookupRunByCreationIdentity(
			ctx, parsed.actorUserID, record.DownstreamKeyHash, record.DownstreamFingerprint,
		)
		if err != nil || !found {
			return nil, false, err
		}
		executionID, err = uuid.Parse(strings.TrimSpace(existing.RunID))
		if err != nil || executionID == uuid.Nil {
			return nil, false, httpx.Internal("Runtime 返回了无效 execution_id")
		}
		executionKind = "run"
	case TargetTypeWorkflow:
		existing, found, err := s.workflows.LookupExternalExecutionWorkflowRunByIdentity(
			ctx, parsed.callerServiceID, parsed.actorUserID, parsed.targetID, parsed.externalRequestID,
		)
		if err != nil || !found {
			return nil, false, err
		}
		executionID, err = uuid.Parse(strings.TrimSpace(existing.ID))
		if err != nil || executionID == uuid.Nil {
			return nil, false, httpx.Internal("Workflow 返回了无效 execution_id")
		}
		executionKind = "workflow_run"
	default:
		return nil, false, httpx.Internal("外部执行类型无效")
	}
	response, err := s.attachLaunchedExecution(
		ctx, parsed, *record.StartToken, executionKind, executionID,
	)
	return response, err == nil, err
}

var errDownstreamIdentityConflict = errors.New("downstream execution identity conflict")

func (s *Service) attachExecution(
	ctx context.Context,
	parsed parsedExecution,
	executionKind string,
	executionID uuid.UUID,
) (*ExecutionStartResponse, error) {
	record, err := s.store.Attach(ctx, parsed.callerServiceID, parsed.externalRequestID, executionKind, executionID)
	if err != nil {
		return nil, httpx.Internal("保存外部 execution_id 失败")
	}
	if rejectionErr := executionStartRejection(record); rejectionErr != nil {
		return nil, rejectionErr
	}
	if response, complete, replayErr := s.responseForReservedRecord(ctx, record); complete || replayErr != nil {
		return response, replayErr
	}
	return nil, httpx.ServiceUnavailable("外部执行关联状态正在确认，请重试")
}

func (s *Service) attachLaunchedExecution(
	ctx context.Context,
	parsed parsedExecution,
	launchToken uuid.UUID,
	executionKind string,
	executionID uuid.UUID,
) (*ExecutionStartResponse, error) {
	record, attached, err := s.store.AttachLaunched(
		ctx, parsed.callerServiceID, parsed.externalRequestID,
		launchToken, executionKind, executionID,
	)
	if err != nil {
		return nil, httpx.Internal("保存外部 execution_id 失败")
	}
	if rejectionErr := executionStartRejection(record); rejectionErr != nil {
		return nil, rejectionErr
	}
	if !attached {
		if response, complete, replayErr := s.responseForReservedRecord(ctx, record); complete || replayErr != nil {
			return response, replayErr
		}
		return nil, httpx.ServiceUnavailable("外部执行关联状态正在确认，请重试")
	}
	if response, complete, replayErr := s.responseForReservedRecord(ctx, record); complete || replayErr != nil {
		return response, replayErr
	}
	return nil, httpx.Internal("外部执行关联缺少 execution attachment")
}

func (s *Service) launchFenceFailure(ctx context.Context, parsed parsedExecution) error {
	record, err := s.store.Get(ctx, parsed.callerServiceID, parsed.externalRequestID)
	if err == nil && record.StartState == startStateCanceled {
		return externalExecutionCanceledError()
	}
	return httpx.ServiceUnavailable("外部执行 launch fence 已变化，请重试")
}

func (s *Service) responseForReservedRecord(ctx context.Context, record ExecutionRecord) (*ExecutionStartResponse, bool, error) {
	attachmentComplete := record.ExecutionID != nil && record.ExecutionKind != nil
	attachmentEmpty := record.ExecutionID == nil && record.ExecutionKind == nil
	if !attachmentComplete && !attachmentEmpty {
		return nil, true, httpx.Internal("外部执行幂等记录包含不完整的 execution attachment")
	}
	if !attachmentComplete {
		return nil, false, nil
	}
	status, err := s.getExecutionStatus(ctx, record)
	if err != nil {
		return nil, true, httpx.ServiceUnavailable("外部执行状态正在确认，请重试")
	}
	return &ExecutionStartResponse{ExecutionID: record.ExecutionID.String(), Status: status.Status}, true, nil
}

func (s *Service) GetExecution(ctx context.Context, principal *Principal, externalRequestID string) (*ExecutionStatusResponse, error) {
	if err := validatePrincipal(principal); err != nil {
		return nil, err
	}
	id, err := uuid.Parse(strings.TrimSpace(externalRequestID))
	if err != nil || id == uuid.Nil {
		return nil, httpx.BadRequest("external_request_id 不是合法 uuid")
	}
	record, err := s.store.Get(ctx, principal.CallerServiceID, id)
	if errors.Is(err, pgx.ErrNoRows) {
		key, keyErr := s.store.GetKey(ctx, principal.CallerServiceID, id)
		if errors.Is(keyErr, pgx.ErrNoRows) || (keyErr == nil && key.ActorUserID != principal.ActorUserID) {
			return nil, httpx.NotFound("外部执行不存在")
		}
		if keyErr != nil {
			return nil, httpx.Internal("查询外部执行 identity 失败")
		}
		cancellation, cancelErr := s.store.GetCancellation(ctx, principal.CallerServiceID, id)
		if errors.Is(cancelErr, pgx.ErrNoRows) {
			return nil, httpx.NotFound("外部执行不存在")
		}
		if cancelErr != nil {
			return nil, httpx.Internal("查询外部执行取消 evidence 失败")
		}
		return s.cancellationStatusResponse(ctx, nil, cancellation)
	}
	if err != nil {
		return nil, httpx.Internal("查询外部执行失败")
	}
	if record.ActorUserID != principal.ActorUserID {
		return nil, httpx.NotFound("外部执行不存在")
	}
	if cancellation, cancelErr := s.store.GetCancellation(ctx, principal.CallerServiceID, id); cancelErr == nil {
		return s.cancellationStatusResponse(ctx, &record, cancellation)
	} else if !errors.Is(cancelErr, pgx.ErrNoRows) {
		return nil, httpx.Internal("查询外部执行取消 evidence 失败")
	}
	return s.getExecutionStatus(ctx, record)
}

func (s *Service) getExecutionStatus(ctx context.Context, record ExecutionRecord) (*ExecutionStatusResponse, error) {
	if rejectionErr := executionStartRejection(record); rejectionErr != nil {
		return nil, rejectionErr
	}
	resp := &ExecutionStatusResponse{
		ExternalRequestID: record.ExternalRequestID.String(),
		TargetType:        record.TargetType,
		Status:            "pending",
		Artifacts:         []runtime.RunArtifactResponse{},
	}
	if record.ExecutionID == nil || record.ExecutionKind == nil {
		return resp, nil
	}
	resp.ExecutionID = record.ExecutionID.String()
	switch *record.ExecutionKind {
	case "run":
		run, err := s.runtime.GetRun(ctx, record.ActorUserID, *record.ExecutionID)
		if err != nil {
			return nil, err
		}
		status, safeError := normalizeRuntimeStatus(run.Status)
		resp.Status = status
		setSafeExecutionError(resp, safeError)
		if resp.Status == "succeeded" {
			resp.Output = run.Output
		}
		artifacts, err := s.runtime.ListRunArtifacts(ctx, record.ActorUserID, *record.ExecutionID)
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
		run, err := s.workflows.GetWorkflowRun(ctx, record.ActorUserID, *record.ExecutionID)
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
			artifacts, err := s.runtime.ListRunArtifacts(ctx, record.ActorUserID, childID)
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
		return nil, httpx.Internal("外部执行类型无效")
	}
}

type parsedExecution struct {
	callerServiceID        string
	externalRequestID      uuid.UUID
	actorUserID            uuid.UUID
	targetOwnerUserID      uuid.UUID
	targetType             string
	targetID               uuid.UUID
	input                  map[string]interface{}
	metadata               map[string]interface{}
	traceID                string
	expectedContractHash   string
	inputSchema            map[string]interface{}
	inputSchemaFingerprint [sha256.Size]byte
}

type downstreamReplayIdentity struct {
	Version          int                    `json:"version"`
	Kind             string                 `json:"kind"`
	Source           string                 `json:"source"`
	IdempotencyKey   string                 `json:"idempotency_key"`
	CreationProtocol string                 `json:"creation_protocol"`
	CreationMethod   string                 `json:"creation_method"`
	Metadata         map[string]interface{} `json:"metadata"`
}

var downstreamReplayIdentityFields = map[string]struct{}{
	"version": {}, "kind": {}, "source": {}, "idempotency_key": {},
	"creation_protocol": {}, "creation_method": {}, "metadata": {},
}

func runtimeCreationRequest(record ExecutionRecord, parsed parsedExecution) (*runtime.RunRequest, string, error) {
	switch record.RequestFingerprintVersion {
	case currentRequestFingerprintVersion:
		metadata := make(map[string]interface{}, len(parsed.metadata)+3)
		for key, value := range parsed.metadata {
			metadata[key] = value
		}
		metadata["external_request_id"] = parsed.externalRequestID.String()
		metadata["caller_service_id"] = parsed.callerServiceID
		metadata["trace_id"] = parsed.traceID
		return &runtime.RunRequest{
			AgentID:          parsed.targetID.String(),
			Input:            parsed.input,
			IdempotencyKey:   externalExecutionIdempotencyKey(parsed.callerServiceID, parsed.externalRequestID),
			Metadata:         metadata,
			CreationProtocol: "external_execution",
			CreationMethod:   "external-execution.start",
		}, "api", nil
	case legacyRequestFingerprintVersion:
		return legacyRuntimeCreationRequest(record, parsed)
	default:
		return nil, "", httpx.Internal("外部执行幂等记录版本无效")
	}
}

func legacyRuntimeCreationRequest(record ExecutionRecord, parsed parsedExecution) (*runtime.RunRequest, string, error) {
	if len(record.DownstreamReplayIdentity) == 0 {
		return nil, "", httpx.Internal("外部执行下游恢复身份缺失")
	}
	var rawFields map[string]json.RawMessage
	if err := json.Unmarshal(record.DownstreamReplayIdentity, &rawFields); err != nil || len(rawFields) != len(downstreamReplayIdentityFields) {
		return nil, "", httpx.Internal("外部执行下游恢复身份无效")
	}
	for field := range downstreamReplayIdentityFields {
		if _, ok := rawFields[field]; !ok {
			return nil, "", httpx.Internal("外部执行下游恢复身份无效")
		}
	}
	var identity downstreamReplayIdentity
	if err := json.Unmarshal(record.DownstreamReplayIdentity, &identity); err != nil ||
		identity.Version != 1 || identity.Kind != "run" || identity.Source != "api" ||
		identity.CreationProtocol == "" || identity.CreationProtocol != strings.TrimSpace(identity.CreationProtocol) ||
		len(identity.CreationProtocol) > 80 || identity.CreationProtocol != strings.ToLower(identity.CreationProtocol) ||
		identity.CreationMethod == "" || identity.CreationMethod != strings.TrimSpace(identity.CreationMethod) ||
		len(identity.CreationMethod) > 120 || identity.CreationMethod != strings.ToLower(identity.CreationMethod) ||
		identity.Metadata == nil {
		return nil, "", httpx.Internal("外部执行下游恢复身份无效")
	}
	if _, err := runtime.HashIdempotencyKey(identity.IdempotencyKey); err != nil {
		return nil, "", httpx.Internal("外部执行下游恢复身份无效")
	}
	storedMetadata, err := runtime.CanonicalizeRFC8785(identity.Metadata)
	if err != nil {
		return nil, "", httpx.Internal("外部执行下游恢复身份无效")
	}
	expectedMetadata := make(map[string]interface{}, len(parsed.metadata)+1)
	for key, value := range parsed.metadata {
		expectedMetadata[key] = value
	}
	expectedMetadata["trace_id"] = parsed.traceID
	canonicalExpected, err := runtime.CanonicalizeRFC8785(expectedMetadata)
	if err != nil {
		return nil, "", httpx.BadRequest("metadata 不是合法 I-JSON")
	}
	if !bytes.Equal(storedMetadata, canonicalExpected) {
		return nil, "", httpx.Conflict("external_request_id 已用于不同的执行请求")
	}
	return &runtime.RunRequest{
		AgentID:          parsed.targetID.String(),
		Input:            parsed.input,
		Metadata:         identity.Metadata,
		IdempotencyKey:   identity.IdempotencyKey,
		CreationProtocol: identity.CreationProtocol,
		CreationMethod:   identity.CreationMethod,
	}, identity.Source, nil
}

func validatePrincipal(principal *Principal) error {
	if principal == nil || principal.ActorUserID == uuid.Nil {
		return httpx.Unauthorized("外部执行代理身份无效")
	}
	callerServiceID := strings.TrimSpace(principal.CallerServiceID)
	if callerServiceID == "" || len(callerServiceID) > 200 || callerServiceID != principal.CallerServiceID {
		return httpx.Unauthorized("外部执行调用服务身份无效")
	}
	return nil
}

func parseExecutionRequest(req *ExecutionRequest) (parsedExecution, error) {
	if req == nil {
		return parsedExecution{}, httpx.BadRequest("请求体不能为空")
	}
	externalRequestID, err := uuid.Parse(strings.TrimSpace(req.ExternalRequestID))
	if err != nil || externalRequestID == uuid.Nil {
		return parsedExecution{}, httpx.BadRequest("external_request_id 不是合法 uuid")
	}
	targetID, targetType, err := parseTargetIdentity(req.TargetID, req.TargetType)
	if err != nil {
		return parsedExecution{}, err
	}
	traceID := strings.TrimSpace(req.TraceID)
	if traceID == "" || len(traceID) > 200 {
		return parsedExecution{}, httpx.BadRequest("trace_id 长度必须为 1 到 200")
	}
	input := req.Input
	if input == nil {
		input = map[string]interface{}{}
	}
	metadata, err := normalizeExternalMetadata(req.Metadata)
	if err != nil {
		return parsedExecution{}, err
	}
	expectedContractHash := strings.TrimSpace(req.ExpectedContractHash)
	if !executioncontract.Valid(expectedContractHash) {
		return parsedExecution{}, httpx.NewError(http.StatusUnprocessableEntity, httpx.ErrorCode("EXTERNAL_EXECUTION_CONTRACT_REQUIRED"), "expected_contract_hash 缺失或格式无效")
	}
	inputSchema, err := normalizeExternalInputSchema(req.InputSchema)
	if err != nil || inputSchema == nil {
		if err != nil {
			return parsedExecution{}, err
		}
		return parsedExecution{}, httpx.NewError(http.StatusUnprocessableEntity, httpx.ErrorCode("EXTERNAL_EXECUTION_CONTRACT_REQUIRED"), "input_schema 缺失")
	}
	canonicalSchema, err := runtime.CanonicalizeRFC8785(inputSchema)
	if err != nil {
		return parsedExecution{}, httpx.BadRequest("input_schema 不是合法 I-JSON")
	}
	schemaFingerprint := sha256.Sum256(canonicalSchema)
	return parsedExecution{
		externalRequestID: externalRequestID,
		targetType:        targetType, targetID: targetID, input: input, metadata: metadata, traceID: traceID,
		expectedContractHash: expectedContractHash, inputSchema: inputSchema, inputSchemaFingerprint: schemaFingerprint,
	}, nil
}

func executionRequestFingerprint(parsed parsedExecution) ([sha256.Size]byte, error) {
	canonical, err := runtime.CanonicalizeRFC8785(map[string]interface{}{
		"caller_service_id": parsed.callerServiceID, "actor_user_id": parsed.actorUserID.String(),
		"target_type": parsed.targetType, "target_id": parsed.targetID.String(),
		"input": parsed.input, "metadata": parsed.metadata, "trace_id": parsed.traceID, "expected_contract_hash": parsed.expectedContractHash,
		"input_schema": parsed.inputSchema,
	})
	if err != nil {
		return [sha256.Size]byte{}, httpx.BadRequest("input 不是合法 I-JSON")
	}
	return sha256.Sum256(canonical), nil
}

const (
	maxExternalMetadataKeys  = 32
	maxExternalMetadataKey   = 64
	maxExternalMetadataBytes = 16 * 1024
)

var externalMetadataKeyPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.-]{0,63}$`)

var protectedExternalMetadataKeys = map[string]struct{}{
	"external_request_id": {},
	"caller_service_id":   {},
	"trace_id":            {},
}

func normalizeExternalMetadata(input map[string]interface{}) (map[string]interface{}, error) {
	if len(input) > maxExternalMetadataKeys {
		return nil, httpx.BadRequest("metadata 字段数量不能超过 32")
	}
	metadata := make(map[string]interface{}, len(input))
	for key, value := range input {
		if len(key) > maxExternalMetadataKey || !externalMetadataKeyPattern.MatchString(key) {
			return nil, httpx.BadRequest("metadata key 必须匹配 [A-Za-z][A-Za-z0-9_.-]{0,63}")
		}
		if _, protected := protectedExternalMetadataKeys[key]; protected {
			return nil, httpx.BadRequest("metadata 不能覆盖 Core 保留字段")
		}
		metadata[key] = value
	}
	canonical, err := runtime.CanonicalizeRFC8785(metadata)
	if err != nil {
		return nil, httpx.BadRequest("metadata 不是合法 I-JSON")
	}
	if len(canonical) > maxExternalMetadataBytes {
		return nil, httpx.BadRequest("metadata 不能超过 16384 canonical JSON bytes")
	}
	return metadata, nil
}

func externalExecutionIdempotencyKey(callerServiceID string, externalRequestID uuid.UUID) string {
	digest := sha256.Sum256([]byte(callerServiceID))
	return "external-execution/" + hex.EncodeToString(digest[:]) + "/" + externalRequestID.String()
}

func parseTargetIdentity(targetRaw, targetTypeRaw string) (uuid.UUID, string, error) {
	targetID, err := uuid.Parse(strings.TrimSpace(targetRaw))
	if err != nil || targetID == uuid.Nil {
		return uuid.Nil, "", httpx.BadRequest("target_id 不是合法 uuid")
	}
	targetType := strings.ToLower(strings.TrimSpace(targetTypeRaw))
	if targetType != TargetTypeAgent && targetType != TargetTypeWorkflow {
		return uuid.Nil, "", httpx.BadRequest("target_type 必须是 agent 或 workflow")
	}
	return targetID, targetType, nil
}

func normalizeExternalInputSchema(raw json.RawMessage) (map[string]interface{}, error) {
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
	default:
		return nil, httpx.BadRequest("input_schema 必须是 JSON Schema object")
	}
}

const (
	legacyRequestFingerprintVersion  int16 = 1
	currentRequestFingerprintVersion int16 = 2
)

func executionRecordMatches(record ExecutionRecord, parsed parsedExecution, fingerprint [sha256.Size]byte) bool {
	baseMatches := record.ExternalRequestID == parsed.externalRequestID &&
		record.CallerServiceID == parsed.callerServiceID &&
		record.ActorUserID == parsed.actorUserID &&
		record.TargetType == parsed.targetType &&
		record.TargetID == parsed.targetID &&
		record.TraceID == parsed.traceID
	if !baseMatches {
		return false
	}
	switch record.RequestFingerprintVersion {
	case legacyRequestFingerprintVersion:
		// Version 1 fingerprints included the former caller's target-owner field
		// and a controlled-schema enum whose order was not deterministic. The raw
		// input is not retained, so those hashes cannot be recomputed reliably.
		// Attached rows can only replay their durable execution. Migration 074
		// gives unattached Agent rows an exact downstream Runtime identity; those
		// rows are never promoted. An unattached Workflow row may be promoted only
		// after its legacy deterministic recovery path has missed.
		return record.ExpectedContractHash == nil || *record.ExpectedContractHash == parsed.expectedContractHash
	case currentRequestFingerprintVersion:
		return bytes.Equal(record.InputFingerprint, fingerprint[:]) &&
			record.ExpectedContractHash != nil && *record.ExpectedContractHash == parsed.expectedContractHash &&
			bytes.Equal(record.InputSchemaFingerprint, parsed.inputSchemaFingerprint[:])
	default:
		return false
	}
}

func targetContractChangedError() *httpx.HTTPError {
	return httpx.NewError(http.StatusConflict, httpx.ErrorCode("TARGET_CONTRACT_CHANGED"), "外部执行目标契约已变化")
}

func targetUnavailableError() *httpx.HTTPError {
	return httpx.NewError(http.StatusConflict, httpx.ErrorCode("TARGET_UNAVAILABLE"), "外部执行目标当前不可执行")
}

func downstreamIdentityConflictError() *httpx.HTTPError {
	return httpx.NewError(
		http.StatusConflict,
		httpx.ErrorCode("DOWNSTREAM_IDENTITY_CONFLICT"),
		"下游执行身份已用于其他请求",
	)
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
