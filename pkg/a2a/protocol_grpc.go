package a2a

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	a2apb "github.com/OpenLinker-ai/openlinker-core/pkg/a2a/pb"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/google/uuid"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const a2aGRPCPollInterval = time.Second

type GRPCServer struct {
	a2apb.UnimplementedA2AServiceServer

	svc          service
	cardProvider AgentCardProvider
	auth         GRPCAuthenticator
	pollInterval time.Duration
}

func NewGRPCServer(svc service, cardProvider AgentCardProvider, auth GRPCAuthenticator) *GRPCServer {
	return &GRPCServer{
		svc:          svc,
		cardProvider: cardProvider,
		auth:         auth,
		pollInterval: a2aGRPCPollInterval,
	}
}

func (s *GRPCServer) SendMessage(ctx context.Context, req *a2apb.SendMessageRequest) (*a2apb.SendMessageResponse, error) {
	authInfo, err := s.authenticate(ctx, "agents:run")
	if err != nil {
		return nil, err
	}
	slug, err := tenantSlug(req.GetTenant())
	if err != nil {
		return nil, grpcErrorFromA2A(err)
	}
	params := messageSendParamsFromProto(req)
	attachProtocolServiceMetadata(params, a2aServiceParameters{Version: a2aProtocolVersionCurrent})
	task, err := s.svc.SendProtocolMessage(ctx, authInfo.UserID, slug, params)
	if err != nil {
		return nil, grpcErrorFromA2A(err)
	}
	return protoSendMessageResponse(task), nil
}

func (s *GRPCServer) SendStreamingMessage(req *a2apb.SendMessageRequest, stream a2apb.A2AService_SendStreamingMessageServer) error {
	ctx := stream.Context()
	authInfo, err := s.authenticate(ctx, "agents:run")
	if err != nil {
		return err
	}
	slug, err := tenantSlug(req.GetTenant())
	if err != nil {
		return grpcErrorFromA2A(err)
	}
	params := messageSendParamsFromProto(req)
	attachProtocolServiceMetadata(params, a2aServiceParameters{Version: a2aProtocolVersionCurrent})
	task, err := s.svc.StartProtocolMessage(ctx, authInfo.UserID, slug, params)
	if err != nil {
		return grpcErrorFromA2A(err)
	}
	return s.streamTask(ctx, stream.Send, authInfo.UserID, slug, task.ID, task)
}

func (s *GRPCServer) GetTask(ctx context.Context, req *a2apb.GetTaskRequest) (*a2apb.Task, error) {
	authInfo, err := s.authenticate(ctx, "runs:read")
	if err != nil {
		return nil, err
	}
	slug, err := tenantSlug(req.GetTenant())
	if err != nil {
		return nil, grpcErrorFromA2A(err)
	}
	var historyLength *int
	if req.HistoryLength != nil {
		value := int(req.GetHistoryLength())
		historyLength = &value
	}
	task, err := s.svc.GetProtocolTask(ctx, authInfo.UserID, slug, req.GetId(), historyLength)
	if err != nil {
		return nil, grpcErrorFromA2A(err)
	}
	return taskToProto(task), nil
}

func (s *GRPCServer) ListTasks(ctx context.Context, req *a2apb.ListTasksRequest) (*a2apb.ListTasksResponse, error) {
	authInfo, err := s.authenticate(ctx, "runs:read")
	if err != nil {
		return nil, err
	}
	slug, err := tenantSlug(req.GetTenant())
	if err != nil {
		return nil, grpcErrorFromA2A(err)
	}
	params := &A2ATaskListParams{
		ContextID:            req.GetContextId(),
		Status:               taskStateFromProto(req.GetStatus()),
		PageToken:            req.GetPageToken(),
		StatusTimestampAfter: timestampToRFC3339(req.GetStatusTimestampAfter()),
	}
	if req.PageSize != nil {
		value := int(req.GetPageSize())
		params.PageSize = &value
	}
	if req.HistoryLength != nil {
		value := int(req.GetHistoryLength())
		params.HistoryLength = &value
	}
	if req.IncludeArtifacts != nil {
		value := req.GetIncludeArtifacts()
		params.IncludeArtifacts = &value
	}
	resp, err := s.svc.ListProtocolTasks(ctx, authInfo.UserID, slug, params)
	if err != nil {
		return nil, grpcErrorFromA2A(err)
	}
	tasks := make([]*a2apb.Task, 0, len(resp.Tasks))
	for i := range resp.Tasks {
		tasks = append(tasks, taskToProto(&resp.Tasks[i]))
	}
	return &a2apb.ListTasksResponse{
		Tasks:         tasks,
		NextPageToken: resp.NextPageToken,
		PageSize:      resp.PageSize,
		TotalSize:     resp.TotalSize,
	}, nil
}

func (s *GRPCServer) CancelTask(ctx context.Context, req *a2apb.CancelTaskRequest) (*a2apb.Task, error) {
	authInfo, err := s.authenticate(ctx, "agents:run")
	if err != nil {
		return nil, err
	}
	slug, err := tenantSlug(req.GetTenant())
	if err != nil {
		return nil, grpcErrorFromA2A(err)
	}
	task, err := s.svc.CancelProtocolTask(ctx, authInfo.UserID, slug, req.GetId())
	if err != nil {
		return nil, grpcErrorFromA2A(err)
	}
	return taskToProto(task), nil
}

func (s *GRPCServer) SubscribeToTask(req *a2apb.SubscribeToTaskRequest, stream a2apb.A2AService_SubscribeToTaskServer) error {
	ctx := stream.Context()
	authInfo, err := s.authenticate(ctx, "runs:read")
	if err != nil {
		return err
	}
	slug, err := tenantSlug(req.GetTenant())
	if err != nil {
		return grpcErrorFromA2A(err)
	}
	task, err := s.svc.GetProtocolTask(ctx, authInfo.UserID, slug, req.GetId(), nil)
	if err != nil {
		return grpcErrorFromA2A(err)
	}
	if isTerminalA2ATaskState(task.Status.State) {
		return grpcErrorFromA2A(a2aUnsupportedOperation("终态任务不能建立 SubscribeToTask stream"))
	}
	return s.streamTask(ctx, stream.Send, authInfo.UserID, slug, req.GetId(), task)
}

func (s *GRPCServer) CreateTaskPushNotificationConfig(ctx context.Context, req *a2apb.TaskPushNotificationConfig) (*a2apb.TaskPushNotificationConfig, error) {
	authInfo, err := s.authenticate(ctx, "runs:read")
	if err != nil {
		return nil, err
	}
	slug, err := tenantSlug(req.GetTenant())
	if err != nil {
		return nil, grpcErrorFromA2A(err)
	}
	cfg := pushConfigFromProto(req)
	if cfg == nil {
		cfg = &A2APushNotificationConfig{}
	}
	resp, err := s.svc.SetPushNotificationConfig(ctx, authInfo.UserID, slug, &A2ATaskPushConfigParams{
		ID:                     req.GetId(),
		TaskID:                 req.GetTaskId(),
		PushNotificationConfig: *cfg,
	})
	if err != nil {
		return nil, grpcErrorFromA2A(err)
	}
	resp.Tenant = slug
	return taskPushConfigToProto(resp), nil
}

func (s *GRPCServer) GetTaskPushNotificationConfig(ctx context.Context, req *a2apb.GetTaskPushNotificationConfigRequest) (*a2apb.TaskPushNotificationConfig, error) {
	authInfo, err := s.authenticate(ctx, "runs:read")
	if err != nil {
		return nil, err
	}
	slug, err := tenantSlug(req.GetTenant())
	if err != nil {
		return nil, grpcErrorFromA2A(err)
	}
	resp, err := s.svc.GetPushNotificationConfig(ctx, authInfo.UserID, slug, &A2ATaskPushConfigParams{
		TaskID:                   req.GetTaskId(),
		PushNotificationConfigID: req.GetId(),
	})
	if err != nil {
		return nil, grpcErrorFromA2A(err)
	}
	resp.Tenant = slug
	return taskPushConfigToProto(resp), nil
}

func (s *GRPCServer) ListTaskPushNotificationConfigs(ctx context.Context, req *a2apb.ListTaskPushNotificationConfigsRequest) (*a2apb.ListTaskPushNotificationConfigsResponse, error) {
	authInfo, err := s.authenticate(ctx, "runs:read")
	if err != nil {
		return nil, err
	}
	slug, err := tenantSlug(req.GetTenant())
	if err != nil {
		return nil, grpcErrorFromA2A(err)
	}
	pageSize := int(req.GetPageSize())
	resp, err := s.svc.ListPushNotificationConfigs(ctx, authInfo.UserID, slug, &A2ATaskPushConfigParams{
		TaskID:    req.GetTaskId(),
		PageSize:  &pageSize,
		PageToken: req.GetPageToken(),
	})
	if err != nil {
		return nil, grpcErrorFromA2A(err)
	}
	items := resp.Configs
	if len(items) == 0 {
		items = resp.Items
	}
	configs := make([]*a2apb.TaskPushNotificationConfig, 0, len(items))
	for i := range items {
		items[i].Tenant = slug
		configs = append(configs, taskPushConfigToProto(&items[i]))
	}
	return &a2apb.ListTaskPushNotificationConfigsResponse{
		Configs:       configs,
		NextPageToken: resp.NextPageToken,
	}, nil
}

func (s *GRPCServer) GetExtendedAgentCard(ctx context.Context, req *a2apb.GetExtendedAgentCardRequest) (*a2apb.AgentCard, error) {
	if _, err := s.authenticate(ctx, "runs:read"); err != nil {
		return nil, err
	}
	if s.cardProvider == nil {
		return nil, grpcErrorFromA2A(a2aProtocolError(a2aErrorExtendedAgentCardNotConfigured, http.StatusBadRequest, "A2A Extended Agent Card 服务未配置", nil))
	}
	slug, err := tenantSlug(req.GetTenant())
	if err != nil {
		return nil, grpcErrorFromA2A(err)
	}
	card, err := s.cardProvider.GetExtendedAgentCardBySlug(ctx, slug)
	if err != nil {
		return nil, grpcErrorFromA2A(err)
	}
	return agentCardToProto(card), nil
}

func (s *GRPCServer) DeleteTaskPushNotificationConfig(ctx context.Context, req *a2apb.DeleteTaskPushNotificationConfigRequest) (*emptypb.Empty, error) {
	authInfo, err := s.authenticate(ctx, "runs:read")
	if err != nil {
		return nil, err
	}
	slug, err := tenantSlug(req.GetTenant())
	if err != nil {
		return nil, grpcErrorFromA2A(err)
	}
	if err := s.svc.DeletePushNotificationConfig(ctx, authInfo.UserID, slug, &A2ATaskPushConfigParams{
		TaskID:                   req.GetTaskId(),
		PushNotificationConfigID: req.GetId(),
	}); err != nil {
		return nil, grpcErrorFromA2A(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *GRPCServer) authenticate(ctx context.Context, scope string) (*GRPCAuthInfo, error) {
	if err := requireGRPCA2AVersion(ctx); err != nil {
		return nil, grpcErrorFromA2A(err)
	}
	if s.auth == nil {
		return nil, status.Error(codes.Unauthenticated, "A2A gRPC 鉴权未启用")
	}
	info, err := s.auth.AuthenticateA2AGRPC(ctx)
	if err != nil {
		return nil, grpcErrorFromA2A(err)
	}
	if !grpcAuthHasScope(info, scope) {
		return nil, grpcErrorFromA2A(httpx.Forbidden("访问令牌缺少 scope: " + scope))
	}
	return info, nil
}

func requireGRPCA2AVersion(ctx context.Context) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil
	}
	raw := ""
	for _, value := range md.Get("a2a-version") {
		if strings.TrimSpace(value) != "" {
			raw = strings.TrimSpace(value)
			break
		}
	}
	if raw == "" {
		return nil
	}
	normalized := normalizeA2AVersion(raw)
	for _, supported := range a2aSupportedProtocolVersions {
		if normalized == supported {
			return nil
		}
	}
	return a2aVersionNotSupported(raw)
}

func (s *GRPCServer) streamTask(ctx context.Context, send func(*a2apb.StreamResponse) error, userID uuid.UUID, slug string, taskID string, initial *A2ATask) error {
	maxStateOrder := -1
	if initial != nil {
		if initial.ResponseMessage != nil {
			return send(protoStreamResponseFromEvent(initial.ResponseMessage))
		}
		maxStateOrder = a2aTaskStateOrder(initial.Status.State)
		if err := send(protoStreamResponseFromTask(initial)); err != nil {
			return err
		}
		if isTerminalA2ATaskState(initial.Status.State) {
			return nil
		}
	}
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()
	var afterSequence int32
	for {
		events, terminal, nextSequence, err := s.svc.ListProtocolTaskEvents(ctx, userID, slug, taskID, afterSequence)
		if err != nil {
			return grpcErrorFromA2A(err)
		}
		for _, event := range events {
			eventStateOrder := a2aStreamItemStateOrder(event)
			if eventStateOrder >= 0 && maxStateOrder >= 0 && eventStateOrder < maxStateOrder {
				continue
			}
			resp := protoStreamResponseFromEvent(event)
			if resp == nil {
				continue
			}
			if err := send(resp); err != nil {
				return err
			}
			if eventStateOrder > maxStateOrder {
				maxStateOrder = eventStateOrder
			}
		}
		afterSequence = nextSequence
		if terminal {
			return nil
		}
		select {
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		case <-ticker.C:
		}
	}
}

func tenantSlug(raw string) (string, error) {
	slug := strings.TrimSpace(raw)
	if slug == "" {
		return "", httpx.BadRequest("A2A gRPC 请求缺少 tenant；请使用 Agent Card supportedInterfaces[].tenant")
	}
	return slug, nil
}

func timestampToRFC3339(ts *timestamppb.Timestamp) string {
	if ts == nil {
		return ""
	}
	return ts.AsTime().UTC().Format(time.RFC3339Nano)
}

func grpcErrorFromA2A(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	var he *httpx.HTTPError
	if !errors.As(err, &he) {
		return status.Error(codes.Internal, "internal error")
	}
	reason := string(he.Code)
	grpcCode := grpcCodeFromHTTPError(he)
	if errorType := a2aErrorTypeFromHTTPError(he); errorType != "" {
		reason = a2aErrorReason(errorType)
		grpcCode = grpcCodeFromA2AErrorType(errorType)
	}
	metadata := map[string]string{"http_status": strconv.Itoa(he.Status)}
	for key, value := range grpcErrorMetadata(he.Details) {
		metadata[key] = value
	}
	st := status.New(grpcCode, he.Message)
	withDetails, detailErr := st.WithDetails(&errdetails.ErrorInfo{
		Reason:   reason,
		Domain:   "a2a-protocol.org",
		Metadata: metadata,
	})
	if detailErr != nil {
		return st.Err()
	}
	return withDetails.Err()
}

func grpcCodeFromHTTPError(err *httpx.HTTPError) codes.Code {
	switch err.Status {
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		return codes.InvalidArgument
	case http.StatusUnauthorized:
		return codes.Unauthenticated
	case http.StatusForbidden:
		return codes.PermissionDenied
	case http.StatusNotFound:
		return codes.NotFound
	case http.StatusConflict:
		return codes.FailedPrecondition
	case http.StatusTooManyRequests:
		return codes.ResourceExhausted
	case http.StatusServiceUnavailable:
		return codes.Unavailable
	default:
		return codes.Internal
	}
}

func grpcCodeFromA2AErrorType(errorType string) codes.Code {
	switch errorType {
	case a2aErrorTaskNotFound:
		return codes.NotFound
	case a2aErrorTaskNotCancelable, a2aErrorExtendedAgentCardNotConfigured, a2aErrorExtensionSupportRequired:
		return codes.FailedPrecondition
	case a2aErrorPushNotificationNotSupported, a2aErrorUnsupportedOperation:
		return codes.Unimplemented
	case a2aErrorContentTypeNotSupported:
		return codes.InvalidArgument
	case a2aErrorVersionNotSupported:
		return codes.Unimplemented
	case a2aErrorInvalidAgentResponse:
		return codes.Internal
	default:
		return codes.Internal
	}
}

func grpcErrorMetadata(details interface{}) map[string]string {
	out := map[string]string{}
	items, ok := details.([]map[string]interface{})
	if !ok || len(items) == 0 {
		return out
	}
	if rawMetadata, ok := items[0]["metadata"].(map[string]string); ok {
		for key, value := range rawMetadata {
			out[key] = value
		}
		return out
	}
	if metadata, ok := items[0]["metadata"].(map[string]interface{}); ok {
		for key, value := range metadata {
			out[key] = strings.TrimSpace(valueString(value))
		}
	}
	return out
}

func valueString(value interface{}) string {
	switch typed := value.(type) {
	case string:
		return typed
	default:
		return fmt.Sprint(value)
	}
}
