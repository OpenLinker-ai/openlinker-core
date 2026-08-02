package a2a

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	a2apb "github.com/OpenLinker-ai/openlinker-core/pkg/a2a/pb"
	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	"github.com/OpenLinker-ai/openlinker-core/pkg/auth"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fakeGRPCUserStatusChecker struct {
	err          error
	userID       uuid.UUID
	tokenVersion int64
}

type rejectingGRPCUserTokenVerifier struct{}

func (rejectingGRPCUserTokenVerifier) Verify(context.Context, string) (uuid.UUID, []string, error) {
	return uuid.Nil, nil, errors.New("revoked")
}

func TestBearerGRPCAuthenticatorNamesUserTokenFailures(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"authorization", "Bearer ol_user_invalid",
	))

	for _, tc := range []struct {
		name     string
		verifier auth.ApiKeyVerifier
		want     string
	}{
		{name: "verifier missing", want: "User Token 鉴权未启用"},
		{name: "verifier rejects", verifier: rejectingGRPCUserTokenVerifier{}, want: "User Token 无效或已撤销"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewBearerGRPCAuthenticator("unused", tc.verifier).AuthenticateA2AGRPC(ctx)
			var httpErr *httpx.HTTPError
			require.True(t, errors.As(err, &httpErr))
			require.Equal(t, tc.want, httpErr.Message)
		})
	}
}

func (c *fakeGRPCUserStatusChecker) EnsureUserEnabled(_ context.Context, userID uuid.UUID) error {
	c.userID = userID
	return c.err
}

func (c *fakeGRPCUserStatusChecker) EnsureJWTUserVersion(_ context.Context, userID uuid.UUID, tokenVersion int64) error {
	c.userID = userID
	c.tokenVersion = tokenVersion
	return c.err
}

func TestBearerGRPCAuthenticatorRejectsDisabledJWTUser(t *testing.T) {
	const secret = "grpc-user-status-secret-32-chars"
	userID := uuid.New()
	token, err := auth.GenerateToken(userID.String(), secret, time.Hour)
	require.NoError(t, err)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"authorization", "Bearer "+token,
	))
	checker := &fakeGRPCUserStatusChecker{err: httpx.Unauthorized("账号已禁用")}

	authenticator, err := NewBearerGRPCAuthenticatorWithUserStatus(secret, nil, checker)
	require.NoError(t, err)
	_, err = authenticator.AuthenticateA2AGRPC(ctx)
	var httpErr *httpx.HTTPError
	require.True(t, errors.As(err, &httpErr))
	require.Equal(t, httpx.CodeUnauthorized, httpErr.Code)
	require.Equal(t, userID, checker.userID)

	checker.err = nil
	authenticator, err = NewBearerGRPCAuthenticatorWithUserStatus(secret, nil, checker)
	require.NoError(t, err)
	info, err := authenticator.AuthenticateA2AGRPC(ctx)
	require.NoError(t, err)
	require.Equal(t, userID, info.UserID)
	require.Equal(t, auth.AuthMethodJWT, info.AuthMethod)
}

func TestBearerGRPCAuthenticatorRejectsRevokedJWTVersion(t *testing.T) {
	const secret = "grpc-revoked-version-secret-32-chars"
	userID := uuid.New()
	token, err := auth.GenerateTokenWithVersion(userID.String(), secret, time.Hour, 4)
	require.NoError(t, err)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"authorization", "Bearer "+token,
	))
	checker := &fakeGRPCUserStatusChecker{err: httpx.Unauthorized("token 无效或已过期")}

	authenticator, err := NewBearerGRPCAuthenticatorWithUserStatus(secret, nil, checker)
	require.NoError(t, err)
	_, err = authenticator.AuthenticateA2AGRPC(ctx)
	var httpErr *httpx.HTTPError
	require.True(t, errors.As(err, &httpErr))
	require.Equal(t, httpx.CodeUnauthorized, httpErr.Code)
	require.Equal(t, userID, checker.userID)
	require.Equal(t, int64(4), checker.tokenVersion)
}

func TestBearerGRPCAuthenticatorRejectsMissingAndTypedNilStatusChecker(t *testing.T) {
	if _, err := NewBearerGRPCAuthenticatorWithUserStatus("secret", nil, nil); err == nil {
		t.Fatal("nil checker should fail construction")
	}
	var typedNil *fakeGRPCUserStatusChecker
	if _, err := NewBearerGRPCAuthenticatorWithUserStatus("secret", nil, typedNil); err == nil {
		t.Fatal("typed-nil checker should fail construction")
	}
}

func TestBearerGRPCCompatibilityAuthenticatorRejectsJWTWithoutStatusAuthority(t *testing.T) {
	const secret = "grpc-no-status-secret-32-chars"
	token, err := auth.GenerateToken(uuid.NewString(), secret, time.Hour)
	require.NoError(t, err)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"authorization", "Bearer "+token,
	))

	_, err = NewBearerGRPCAuthenticator(secret, nil).AuthenticateA2AGRPC(ctx)
	var httpErr *httpx.HTTPError
	require.True(t, errors.As(err, &httpErr))
	require.Equal(t, httpx.CodeUnauthorized, httpErr.Code)
}

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

func TestGRPCServerSpecificAgentGrantCannotCrossTenant(t *testing.T) {
	userID := uuid.New()
	agentA := uuid.New()
	agentB := uuid.New()
	principal := &auth.AuthPrincipal{
		UserID: userID, AuthMethod: auth.AuthMethodUserToken,
		Grants: []auth.Grant{{
			Permission: "agents:run", ResourceType: "agent", ResourceID: &agentA,
			Constraints: json.RawMessage(`{}`),
		}},
	}
	info := &GRPCAuthInfo{UserID: userID, AuthMethod: auth.AuthMethodUserToken, Principal: principal}
	req := &a2apb.SendMessageRequest{
		Tenant:  "agent",
		Message: &a2apb.Message{MessageId: "m1", Role: a2apb.Role_ROLE_USER},
	}

	allowedCard := &agent.AgentCardResponse{OpenLinker: agent.AgentCardOpenLinkerExt{AgentID: agentA.String()}}
	allowed := NewGRPCServer(newFakeA2AService(uuid.NewString()), fakeGRPCAgentCardProvider{card: allowedCard}, staticGRPCAuth{info: info})
	if _, err := allowed.SendMessage(context.Background(), req); err != nil {
		t.Fatalf("specific Agent grant should allow matching tenant: %v", err)
	}

	deniedCard := &agent.AgentCardResponse{OpenLinker: agent.AgentCardOpenLinkerExt{AgentID: agentB.String()}}
	denied := NewGRPCServer(newFakeA2AService(uuid.NewString()), fakeGRPCAgentCardProvider{card: deniedCard}, staticGRPCAuth{info: info})
	_, err := denied.SendMessage(context.Background(), req)
	require.Error(t, err)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
}
