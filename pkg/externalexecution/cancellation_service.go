package externalexecution

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

func (s *Service) CancelExecution(
	ctx context.Context,
	principal *Principal,
	externalRequestID string,
	req *ExecutionCancelRequest,
) (*ExecutionStatusResponse, error) {
	if err := validatePrincipal(principal); err != nil {
		return nil, err
	}
	requestID, err := uuid.Parse(strings.TrimSpace(externalRequestID))
	if err != nil || requestID == uuid.Nil {
		return nil, httpx.BadRequest("external_request_id 不是合法 uuid")
	}
	if req == nil || (req.ReasonCode != "CALLER_REQUESTED" && req.ReasonCode != "DEADLINE_EXCEEDED") {
		return nil, httpx.BadRequest("reason_code 无效")
	}
	execution, cancellation, err := s.store.RequestCancel(
		ctx, principal.CallerServiceID, requestID, principal.ActorUserID, req.ReasonCode,
	)
	if errors.Is(err, ErrExecutionIdentityConflict) {
		return nil, httpx.NotFound("外部执行不存在")
	}
	if err != nil {
		return nil, httpx.Internal("保存外部执行取消意图失败")
	}
	if cancellation.State == "requested" || cancellation.State == "stopping" {
		execution, cancellation, err = s.reconcileExternalCancellation(
			ctx, principal, requestID, execution, cancellation,
		)
		if err != nil {
			return nil, err
		}
	}
	return s.cancellationStatusResponse(ctx, execution, cancellation)
}

func (s *Service) reconcileExternalCancellation(
	ctx context.Context,
	principal *Principal,
	requestID uuid.UUID,
	execution *ExecutionRecord,
	cancellation CancellationRecord,
) (*ExecutionRecord, CancellationRecord, error) {
	if execution == nil {
		return nil, cancellation, httpx.ServiceUnavailable("外部执行 launch 状态正在确认")
	}
	if execution.ExecutionID == nil || execution.ExecutionKind == nil {
		recoveredKind, recoveredID, found, err := s.recoverCanceledLaunch(ctx, principal, *execution)
		if err != nil {
			return execution, cancellation, httpx.ServiceUnavailable("外部执行 launch 状态正在确认")
		}
		if !found {
			if execution.TargetType == TargetTypeWorkflow && len(execution.DownstreamKeyHash) == 0 {
				execution, cancellation, err = s.store.AdvanceCancellation(
					ctx, principal.CallerServiceID, requestID, "stopped",
				)
				return execution, cancellation, err
			}
			// Agent identity hashes are committed in the same transaction as
			// the Run. A hash with no matching Run is inconsistent, not proof of
			// no execution, so remain requested and fail closed.
			return execution, cancellation, httpx.ServiceUnavailable("下游执行恢复身份冲突")
		}
		attached, attachedOK, err := s.store.AttachCanceledRecovery(
			ctx, principal.CallerServiceID, requestID, recoveredKind, recoveredID,
		)
		if err != nil || !attachedOK {
			return execution, cancellation, httpx.ServiceUnavailable("外部执行关联状态正在确认")
		}
		execution = &attached
	}

	switch *execution.ExecutionKind {
	case "run":
		return s.cancelRuntimeExecution(ctx, principal, requestID, execution, cancellation)
	case "workflow_run":
		return s.cancelWorkflowExecution(ctx, principal, requestID, execution, cancellation)
	default:
		return execution, cancellation, httpx.Internal("外部执行类型无效")
	}
}

func (s *Service) recoverCanceledLaunch(
	ctx context.Context,
	principal *Principal,
	execution ExecutionRecord,
) (string, uuid.UUID, bool, error) {
	switch execution.TargetType {
	case TargetTypeAgent:
		if len(execution.DownstreamKeyHash) != 32 || len(execution.DownstreamFingerprint) != 32 {
			return "", uuid.Nil, false, nil
		}
		run, found, err := s.runtime.LookupRunByCreationIdentity(
			ctx, principal.ActorUserID, execution.DownstreamKeyHash, execution.DownstreamFingerprint,
		)
		if err != nil || !found {
			return "", uuid.Nil, found, err
		}
		id, err := uuid.Parse(strings.TrimSpace(run.RunID))
		return "run", id, err == nil && id != uuid.Nil, err
	case TargetTypeWorkflow:
		run, found, err := s.workflows.LookupExternalExecutionWorkflowRunByIdentity(
			ctx, principal.CallerServiceID, principal.ActorUserID,
			execution.TargetID, execution.ExternalRequestID,
		)
		if err != nil || !found {
			return "", uuid.Nil, found, err
		}
		id, err := uuid.Parse(strings.TrimSpace(run.ID))
		return "workflow_run", id, err == nil && id != uuid.Nil, err
	default:
		return "", uuid.Nil, false, httpx.Internal("外部执行类型无效")
	}
}

func (s *Service) cancelRuntimeExecution(
	ctx context.Context,
	principal *Principal,
	requestID uuid.UUID,
	execution *ExecutionRecord,
	cancellation CancellationRecord,
) (*ExecutionRecord, CancellationRecord, error) {
	_, cancelErr := s.runtime.CancelRun(ctx, principal.ActorUserID, *execution.ExecutionID)
	if cancelErr != nil {
		run, getErr := s.runtime.GetRun(ctx, principal.ActorUserID, *execution.ExecutionID)
		if getErr != nil {
			return execution, cancellation, httpx.ServiceUnavailable("Runtime 取消状态正在确认")
		}
		status, _ := normalizeRuntimeStatus(run.Status)
		if status == "succeeded" || (status == "failed" && strings.ToLower(strings.TrimSpace(run.Status)) != "canceled") {
			return s.advanceExternalCancellation(ctx, principal.CallerServiceID, requestID, "not_applied")
		}
	}
	evidence, err := s.runtime.GetRunCancellationEvidence(ctx, principal.ActorUserID, *execution.ExecutionID)
	if err != nil {
		return execution, cancellation, httpx.ServiceUnavailable("Runtime stop evidence 正在确认")
	}
	next := "stopping"
	if evidence.State == "stopped" {
		next = "stopped"
	} else if evidence.State == "unconfirmed" {
		next = "unconfirmed"
	}
	return s.advanceExternalCancellation(ctx, principal.CallerServiceID, requestID, next)
}

func (s *Service) cancelWorkflowExecution(
	ctx context.Context,
	principal *Principal,
	requestID uuid.UUID,
	execution *ExecutionRecord,
	cancellation CancellationRecord,
) (*ExecutionRecord, CancellationRecord, error) {
	run, evidence, err := s.workflows.CancelExternalWorkflowRun(
		ctx, principal.ActorUserID, *execution.ExecutionID, cancellation.ReasonCode,
	)
	if err != nil {
		if run != nil {
			status, _ := normalizeWorkflowStatus(run.Status)
			if status == "succeeded" || (status == "failed" && strings.ToLower(strings.TrimSpace(run.Status)) != "canceled") {
				return s.advanceExternalCancellation(ctx, principal.CallerServiceID, requestID, "not_applied")
			}
		}
		return execution, cancellation, httpx.ServiceUnavailable("Workflow 取消状态正在确认")
	}
	next := "stopping"
	if evidence.State == "stopped" {
		next = "stopped"
	} else if evidence.State == "unconfirmed" {
		next = "unconfirmed"
	} else if evidence.State == "not_applied" {
		next = "not_applied"
	}
	return s.advanceExternalCancellation(ctx, principal.CallerServiceID, requestID, next)
}

func (s *Service) advanceExternalCancellation(
	ctx context.Context,
	callerServiceID string,
	requestID uuid.UUID,
	next string,
) (*ExecutionRecord, CancellationRecord, error) {
	execution, cancellation, err := s.store.AdvanceCancellation(ctx, callerServiceID, requestID, next)
	if err != nil {
		return execution, cancellation, httpx.Internal("更新外部执行取消 evidence 失败")
	}
	return execution, cancellation, nil
}

func (s *Service) cancellationStatusResponse(
	ctx context.Context,
	execution *ExecutionRecord,
	cancellation CancellationRecord,
) (*ExecutionStatusResponse, error) {
	resp := &ExecutionStatusResponse{
		ExternalRequestID: cancellation.ExternalRequestID.String(),
		Status:            "canceling",
		Artifacts:         []runtime.RunArtifactResponse{},
		Cancellation:      cancellationToResponse(cancellation),
	}
	terminalStatusLoaded := false
	if execution != nil {
		resp.TargetType = execution.TargetType
		if execution.ExecutionID != nil && execution.ExecutionKind != nil {
			statusRecord := *execution
			// The canceled starter state is a Start fence, not the downstream
			// status. Cancellation owns the response semantics here.
			statusRecord.StartState = startStateAttached
			base, err := s.getExecutionStatus(ctx, statusRecord)
			if err == nil {
				resp = base
				resp.Cancellation = cancellationToResponse(cancellation)
				terminalStatusLoaded = true
			}
		}
	}
	switch cancellation.State {
	case "stopped":
		resp.Status = "canceled"
		resp.ErrorCode, resp.ErrorMessage = "", ""
	case "unconfirmed":
		resp.Status = "failed"
		resp.ErrorCode = "CANCEL_UNCONFIRMED"
		resp.ErrorMessage = "Execution cancellation could not be confirmed."
	case "not_applied":
		// not_applied proves only that cancellation lost to a downstream
		// terminal; it does not snapshot that terminal's output/error. Never
		// fabricate "canceling" when the authoritative read is unavailable.
		if !terminalStatusLoaded || (resp.Status != "succeeded" && resp.Status != "failed") {
			return nil, httpx.ServiceUnavailable("下游终态正在确认")
		}
	case "requested", "stopping":
		resp.Status = "canceling"
		resp.ErrorCode, resp.ErrorMessage = "", ""
	default:
		return nil, httpx.Internal("外部执行取消 evidence 无效")
	}
	if resp.Artifacts == nil {
		resp.Artifacts = []runtime.RunArtifactResponse{}
	}
	return resp, nil
}

func cancellationToResponse(record CancellationRecord) *ExecutionCancellationResponse {
	resp := &ExecutionCancellationResponse{
		CancellationID: record.ID.String(), State: record.State, ReasonCode: record.ReasonCode,
		RequestedAt: record.RequestedAt.UTC().Format(time.RFC3339Nano),
	}
	if record.ExecutionKindSnapshot != nil {
		resp.ExecutionKind = *record.ExecutionKindSnapshot
	}
	if record.ExecutionIDSnapshot != nil {
		resp.ExecutionID = record.ExecutionIDSnapshot.String()
	}
	if record.AppliedAt != nil {
		resp.AppliedAt = record.AppliedAt.UTC().Format(time.RFC3339Nano)
	}
	if record.FinishedAt != nil {
		resp.FinishedAt = record.FinishedAt.UTC().Format(time.RFC3339Nano)
	}
	return resp
}

// ReconcilePendingCancellations is the maintenance-only mutation path. GET
// never calls it and therefore remains strictly read-only.
func (s *Service) ReconcilePendingCancellations(ctx context.Context, limit int) (int, error) {
	records, err := s.store.ListPendingCancellations(ctx, limit)
	if err != nil {
		return 0, err
	}
	completed := 0
	for _, cancellation := range records {
		key, err := s.store.GetKey(ctx, cancellation.CallerServiceID, cancellation.ExternalRequestID)
		if err != nil {
			return completed, err
		}
		execution, err := s.store.Get(ctx, cancellation.CallerServiceID, cancellation.ExternalRequestID)
		if errors.Is(err, pgx.ErrNoRows) {
			executionPtr, _, advanceErr := s.advanceExternalCancellation(
				ctx, cancellation.CallerServiceID, cancellation.ExternalRequestID, "stopped",
			)
			_ = executionPtr
			if advanceErr != nil {
				return completed, advanceErr
			}
			completed++
			continue
		}
		if err != nil {
			return completed, err
		}
		principal := &Principal{CallerServiceID: key.CallerServiceID, ActorUserID: key.ActorUserID}
		_, updated, reconcileErr := s.reconcileExternalCancellation(
			ctx, principal, cancellation.ExternalRequestID, &execution, cancellation,
		)
		if reconcileErr != nil {
			continue
		}
		if updated.State != "requested" && updated.State != "stopping" {
			completed++
		}
	}
	return completed, nil
}
