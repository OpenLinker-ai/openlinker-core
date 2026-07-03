package a2a

import (
	"context"
	"testing"
	"time"

	a2apb "github.com/OpenLinker-ai/openlinker-core/pkg/a2a/pb"
	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type staticGRPCAuth struct {
	info *GRPCAuthInfo
	err  error
}

func (a staticGRPCAuth) AuthenticateA2AGRPC(context.Context) (*GRPCAuthInfo, error) {
	if a.err != nil {
		return nil, a.err
	}
	return a.info, nil
}

type recordingA2AStream struct {
	ctx      context.Context
	messages []*a2apb.StreamResponse
}

func (s *recordingA2AStream) Send(resp *a2apb.StreamResponse) error {
	s.messages = append(s.messages, resp)
	return nil
}

func (s *recordingA2AStream) SetHeader(metadata.MD) error  { return nil }
func (s *recordingA2AStream) SendHeader(metadata.MD) error { return nil }
func (s *recordingA2AStream) SetTrailer(metadata.MD)       {}
func (s *recordingA2AStream) Context() context.Context {
	if s.ctx != nil {
		return s.ctx
	}
	return context.Background()
}
func (s *recordingA2AStream) SendMsg(interface{}) error { return nil }
func (s *recordingA2AStream) RecvMsg(interface{}) error { return nil }

type fakeGRPCAgentCardProvider struct {
	card *agent.AgentCardResponse
}

func (p fakeGRPCAgentCardProvider) GetAgentCardBySlug(context.Context, string) (*agent.AgentCardResponse, error) {
	return p.card, nil
}

func (p fakeGRPCAgentCardProvider) GetExtendedAgentCardBySlug(context.Context, string) (*agent.AgentCardResponse, error) {
	return p.card, nil
}

func TestGRPCServerSendMessageUsesTenantAndProtocolMetadata(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.NewString()
	svc := newFakeA2AService(taskID)
	srv := NewGRPCServer(svc, nil, staticGRPCAuth{info: &GRPCAuthInfo{UserID: userID, AuthMethod: "jwt"}})
	meta, err := structpb.NewStruct(map[string]interface{}{"trace_id": "trace-1"})
	require.NoError(t, err)

	resp, err := srv.SendMessage(context.Background(), &a2apb.SendMessageRequest{
		Tenant: "agent-slug",
		Message: &a2apb.Message{
			MessageId: "msg-1",
			Role:      a2apb.Role_ROLE_USER,
			Parts: []*a2apb.Part{{
				Content:   &a2apb.Part_Text{Text: "hello"},
				MediaType: "text/plain",
			}},
			Metadata: meta,
		},
		Configuration: &a2apb.SendMessageConfiguration{
			AcceptedOutputModes: []string{"text/plain"},
			ReturnImmediately:   true,
			TaskPushNotificationConfig: &a2apb.TaskPushNotificationConfig{
				Url:   "https://callback.example/a2a",
				Token: "callback-token",
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, taskID, resp.GetTask().GetId())
	require.True(t, svc.called("message/send"))
	require.Equal(t, userID, svc.userID)
	require.Equal(t, "agent-slug", svc.slug)
	require.Equal(t, "hello", svc.sendParams.Message.Parts[0]["text"])
	require.Equal(t, "trace-1", svc.sendParams.Message.Metadata["trace_id"])
	require.Equal(t, "1.0", svc.sendParams.Metadata["a2a_protocol_version"])
	require.NotNil(t, svc.sendParams.Configuration.ReturnImmediately)
	require.True(t, *svc.sendParams.Configuration.ReturnImmediately)
	require.Equal(t, "https://callback.example/a2a", svc.sendParams.Configuration.PushNotificationConfig.URL)
}

func TestGRPCServerTaskMethodsUseExistingProtocolService(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.NewString()
	svc := newFakeA2AService(taskID)
	srv := NewGRPCServer(svc, nil, staticGRPCAuth{info: &GRPCAuthInfo{UserID: userID, AuthMethod: "jwt"}})

	historyLength := int32(2)
	task, err := srv.GetTask(context.Background(), &a2apb.GetTaskRequest{Tenant: "agent", Id: taskID, HistoryLength: &historyLength})
	require.NoError(t, err)
	require.Equal(t, taskID, task.GetId())
	require.Equal(t, 2, *svc.historyLength)

	pageSize := int32(7)
	includeArtifacts := true
	statusAfter := timestamppb.New(time.Date(2026, 6, 30, 1, 2, 3, 0, time.UTC))
	listResp, err := srv.ListTasks(context.Background(), &a2apb.ListTasksRequest{
		Tenant:               "agent",
		ContextId:            "ctx-1",
		Status:               a2apb.TaskState_TASK_STATE_COMPLETED,
		PageSize:             &pageSize,
		PageToken:            "page-token",
		HistoryLength:        &historyLength,
		StatusTimestampAfter: statusAfter,
		IncludeArtifacts:     &includeArtifacts,
	})
	require.NoError(t, err)
	require.Len(t, listResp.GetTasks(), 1)
	require.Equal(t, "completed", svc.listParams.Status)
	require.Equal(t, "ctx-1", svc.listParams.ContextID)
	require.Equal(t, 7, *svc.listParams.PageSize)
	require.True(t, *svc.listParams.IncludeArtifacts)

	cancelResp, err := srv.CancelTask(context.Background(), &a2apb.CancelTaskRequest{Tenant: "agent", Id: taskID})
	require.NoError(t, err)
	require.Equal(t, a2apb.TaskState_TASK_STATE_CANCELED, cancelResp.GetStatus().GetState())
}

func TestGRPCServerStreamingSendsInitialTaskAndEvents(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.NewString()
	svc := newFakeA2AService(taskID)
	srv := NewGRPCServer(svc, nil, staticGRPCAuth{info: &GRPCAuthInfo{UserID: userID, AuthMethod: "jwt"}})
	stream := &recordingA2AStream{ctx: context.Background()}

	err := srv.SendStreamingMessage(&a2apb.SendMessageRequest{
		Tenant: "agent",
		Message: &a2apb.Message{
			Role:  a2apb.Role_ROLE_USER,
			Parts: []*a2apb.Part{{Content: &a2apb.Part_Text{Text: "stream me"}}},
		},
	}, stream)
	require.NoError(t, err)
	require.True(t, svc.called("message/stream"))
	require.True(t, svc.called("events"))
	require.Len(t, stream.messages, 2)
	require.Equal(t, taskID, stream.messages[0].GetTask().GetId())
	require.Equal(t, a2apb.TaskState_TASK_STATE_COMPLETED, stream.messages[1].GetStatusUpdate().GetStatus().GetState())
}

func TestGRPCServerPushAndExtendedCardMethods(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.NewString()
	svc := newFakeA2AService(taskID)
	cardProvider := fakeGRPCAgentCardProvider{card: &agent.AgentCardResponse{
		Name:        "Card Agent",
		Description: "Extended card",
		Version:     "v1",
		Provider:    agent.AgentCardProvider{Organization: "OpenLinker"},
		SupportedInterfaces: []agent.AgentCardInterface{{
			URL:             "https://grpc.example/a2a",
			ProtocolBinding: "GRPC",
			Tenant:          "agent",
			ProtocolVersion: "1.0",
		}},
		Capabilities: agent.AgentCardCapabilities{Streaming: true, PushNotifications: true, ExtendedAgentCard: true},
		Skills:       []agent.AgentCardSkill{{ID: "skill-1", Name: "Skill", Description: "Does work"}},
	}}
	srv := NewGRPCServer(svc, cardProvider, staticGRPCAuth{info: &GRPCAuthInfo{UserID: userID, AuthMethod: "jwt"}})

	created, err := srv.CreateTaskPushNotificationConfig(context.Background(), &a2apb.TaskPushNotificationConfig{
		Tenant: "agent",
		TaskId: taskID,
		Url:    "https://callback.example/a2a",
		Token:  "push-token",
	})
	require.NoError(t, err)
	require.Equal(t, "agent", created.GetTenant())
	require.Equal(t, taskID, svc.pushParams.TaskID)
	require.Equal(t, "https://callback.example/a2a", svc.pushParams.PushNotificationConfig.URL)
	require.Equal(t, "push-token", svc.pushParams.PushNotificationConfig.Token)

	_, err = srv.GetTaskPushNotificationConfig(context.Background(), &a2apb.GetTaskPushNotificationConfigRequest{Tenant: "agent", TaskId: taskID, Id: "cfg-1"})
	require.NoError(t, err)
	require.Equal(t, "cfg-1", svc.pushParams.PushNotificationConfigID)

	listed, err := srv.ListTaskPushNotificationConfigs(context.Background(), &a2apb.ListTaskPushNotificationConfigsRequest{Tenant: "agent", TaskId: taskID, PageSize: 10})
	require.NoError(t, err)
	require.Len(t, listed.GetConfigs(), 1)
	require.Equal(t, "agent", listed.GetConfigs()[0].GetTenant())

	_, err = srv.DeleteTaskPushNotificationConfig(context.Background(), &a2apb.DeleteTaskPushNotificationConfigRequest{Tenant: "agent", TaskId: taskID, Id: "cfg-1"})
	require.NoError(t, err)
	require.True(t, svc.called("push/delete"))

	card, err := srv.GetExtendedAgentCard(context.Background(), &a2apb.GetExtendedAgentCardRequest{Tenant: "agent"})
	require.NoError(t, err)
	require.Equal(t, "Card Agent", card.GetName())
	require.Equal(t, "agent", card.GetSupportedInterfaces()[0].GetTenant())
}

func TestGRPCServerErrorsMapToGRPCStatus(t *testing.T) {
	userID := uuid.New()
	srv := NewGRPCServer(newFakeA2AService(uuid.NewString()), nil, staticGRPCAuth{info: &GRPCAuthInfo{UserID: userID, AuthMethod: "user_token", Scopes: []string{"runs:read"}}})
	_, err := srv.SendMessage(context.Background(), &a2apb.SendMessageRequest{Tenant: "agent"})
	require.Error(t, err)
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	srv = NewGRPCServer(newFakeA2AService(uuid.NewString()), nil, staticGRPCAuth{info: &GRPCAuthInfo{UserID: userID, AuthMethod: "jwt"}})
	_, err = srv.GetTask(context.Background(), &a2apb.GetTaskRequest{Id: uuid.NewString()})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}
