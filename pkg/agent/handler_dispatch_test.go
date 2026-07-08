package agent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

func TestAgentHandlerDispatchesServiceSuccess(t *testing.T) {
	userID := uuid.New()
	agentID := uuid.New()
	exampleID := uuid.New()
	alertID := uuid.New()
	mock := &mockAgentService{}
	h := NewHandler(mock)

	c, rec := newAgentDispatchContext(agentDispatchRequest{
		method: http.MethodGet,
		target: "/agents/check-slug?slug=demo-agent",
	})
	requireNoDispatchError(t, h.CheckSlug(c))
	requireDispatchStatus(t, rec, http.StatusOK)
	checkSlug := decodeAgentDispatchJSON[SlugCheckResponse](t, rec)
	if checkSlug.Slug != "demo-agent" || !checkSlug.Available {
		t.Fatalf("CheckSlug response = %#v", checkSlug)
	}

	c, rec = newAgentDispatchContext(agentDispatchRequest{
		method: http.MethodPost,
		target: "/me/become-creator",
		userID: userID.String(),
	})
	requireNoDispatchError(t, h.BecomeCreator(c))
	requireDispatchStatus(t, rec, http.StatusOK)

	c, rec = newAgentDispatchContext(agentDispatchRequest{
		method: http.MethodPost,
		target: "/creator/agents",
		userID: userID.String(),
		body:   `{"slug":"demo-agent","name":"Demo Agent","description":"demo","endpoint_url":"https://example.com/a","tags":["ai"],"skill_ids":["data/sql-query"],"visibility":"public"}`,
	})
	requireNoDispatchError(t, h.CreateAgent(c))
	requireDispatchStatus(t, rec, http.StatusCreated)
	if !reflect.DeepEqual(mock.lastCreateAgent.SkillIDs, []string{"data/sql-query"}) {
		t.Fatalf("CreateAgent skill_ids = %#v", mock.lastCreateAgent.SkillIDs)
	}

	c, rec = newAgentDispatchContext(agentDispatchRequest{
		method: http.MethodGet,
		target: "/creator/agents",
		userID: userID.String(),
	})
	requireNoDispatchError(t, h.ListMyAgents(c))
	requireDispatchStatus(t, rec, http.StatusOK)
	defaultPageBody := decodeAgentDispatchJSON[AgentListResponse](t, rec)
	if defaultPageBody.Limit != 25 || defaultPageBody.Offset != 0 {
		t.Fatalf("ListMyAgents default pagination = %#v", defaultPageBody)
	}
	if mock.lastListOptions.Status != "active" {
		t.Fatalf("ListMyAgents default status = %q, want active", mock.lastListOptions.Status)
	}

	c, rec = newAgentDispatchContext(agentDispatchRequest{
		method: http.MethodGet,
		target: "/creator/agents?limit=50",
		userID: userID.String(),
	})
	requireNoDispatchError(t, h.ListMyAgents(c))
	requireDispatchStatus(t, rec, http.StatusOK)
	pageOnlyBody := decodeAgentDispatchJSON[AgentListResponse](t, rec)
	if pageOnlyBody.Limit != 50 || mock.lastListOptions.Status != "active" {
		t.Fatalf("ListMyAgents paged default = body %#v options %#v", pageOnlyBody, mock.lastListOptions)
	}

	c, rec = newAgentDispatchContext(agentDispatchRequest{
		method: http.MethodGet,
		target: "/creator/agents?limit=10&offset=20&q=demo&status=active&visibility=public&certification_status=certified&sort_by=name&skill_ids=data/sql-query,dev/code-review",
		userID: userID.String(),
	})
	requireNoDispatchError(t, h.ListMyAgents(c))
	requireDispatchStatus(t, rec, http.StatusOK)
	pageBody := decodeAgentDispatchJSON[AgentListResponse](t, rec)
	if pageBody.Limit != 10 || pageBody.Offset != 20 {
		t.Fatalf("ListMyAgentsPage pagination = %#v", pageBody)
	}
	if mock.lastListOptions.Query != "demo" || mock.lastListOptions.Status != "active" || mock.lastListOptions.SortBy != "name" {
		t.Fatalf("ListMyAgentsPage options = %#v", mock.lastListOptions)
	}
	if !reflect.DeepEqual(mock.lastListOptions.SkillIDs, []string{"data/sql-query", "dev/code-review"}) {
		t.Fatalf("ListMyAgentsPage skill_ids = %#v", mock.lastListOptions.SkillIDs)
	}

	c, rec = newAgentDispatchContext(agentDispatchRequest{
		method: http.MethodGet,
		target: "/creator/agents/" + agentID.String(),
		userID: userID.String(),
		params: map[string]string{"id": agentID.String()},
	})
	requireNoDispatchError(t, h.GetMyAgent(c))
	requireDispatchStatus(t, rec, http.StatusOK)

	c, rec = newAgentDispatchContext(agentDispatchRequest{
		method: http.MethodPatch,
		target: "/creator/agents/" + agentID.String(),
		userID: userID.String(),
		params: map[string]string{"id": agentID.String()},
		body:   `{"name":"Updated Agent","description":"updated","endpoint_url":"https://example.com/b","tags":["ops"],"visibility":"unlisted"}`,
	})
	requireNoDispatchError(t, h.UpdateAgent(c))
	requireDispatchStatus(t, rec, http.StatusOK)

	c, rec = newAgentDispatchContext(agentDispatchRequest{
		method: http.MethodPatch,
		target: "/creator/agents/" + agentID.String() + "/visibility",
		userID: userID.String(),
		params: map[string]string{"id": agentID.String()},
		body:   `{"visibility":"private"}`,
	})
	requireNoDispatchError(t, h.UpdateVisibility(c))
	requireDispatchStatus(t, rec, http.StatusOK)

	c, rec = newAgentDispatchContext(agentDispatchRequest{
		method: http.MethodDelete,
		target: "/creator/agents/" + agentID.String(),
		userID: userID.String(),
		params: map[string]string{"id": agentID.String()},
	})
	requireNoDispatchError(t, h.DisableAgent(c))
	requireDispatchStatus(t, rec, http.StatusOK)

	c, rec = newAgentDispatchContext(agentDispatchRequest{
		method: http.MethodGet,
		target: "/creator/agents/" + agentID.String() + "/onboarding",
		userID: userID.String(),
		params: map[string]string{"id": agentID.String()},
	})
	requireNoDispatchError(t, h.GetAgentOnboarding(c))
	requireDispatchStatus(t, rec, http.StatusOK)

	c, rec = newAgentDispatchContext(agentDispatchRequest{
		method: http.MethodPut,
		target: "/creator/agents/" + agentID.String() + "/capabilities",
		userID: userID.String(),
		params: map[string]string{"id": agentID.String()},
		body:   `{"input_schema":{"type":"object"},"output_schema":{"type":"object"},"summary":"ready"}`,
	})
	requireNoDispatchError(t, h.UpsertCapability(c))
	requireDispatchStatus(t, rec, http.StatusOK)

	c, rec = newAgentDispatchContext(agentDispatchRequest{
		method: http.MethodPost,
		target: "/creator/agents/" + agentID.String() + "/examples",
		userID: userID.String(),
		params: map[string]string{"id": agentID.String()},
		body:   `{"title":"sample","input_json":{"q":"hello"},"expected_output_json":{"answer":"hi"},"sort_order":2}`,
	})
	requireNoDispatchError(t, h.CreateExample(c))
	requireDispatchStatus(t, rec, http.StatusCreated)

	c, rec = newAgentDispatchContext(agentDispatchRequest{
		method: http.MethodDelete,
		target: "/creator/agents/" + agentID.String() + "/examples/" + exampleID.String(),
		userID: userID.String(),
		params: map[string]string{"id": agentID.String(), "exampleID": exampleID.String()},
	})
	requireNoDispatchError(t, h.DeleteExample(c))
	requireDispatchStatus(t, rec, http.StatusOK)

	c, rec = newAgentDispatchContext(agentDispatchRequest{
		method: http.MethodPost,
		target: "/creator/agents/" + agentID.String() + "/dry-run",
		userID: userID.String(),
		params: map[string]string{"id": agentID.String()},
	})
	requireNoDispatchError(t, h.RunDryRun(c))
	requireDispatchStatus(t, rec, http.StatusOK)

	c, rec = newAgentDispatchContext(agentDispatchRequest{
		method: http.MethodPost,
		target: "/creator/agents/" + agentID.String() + "/health-check",
		userID: userID.String(),
		params: map[string]string{"id": agentID.String()},
	})
	requireNoDispatchError(t, h.RunHealthCheck(c))
	requireDispatchStatus(t, rec, http.StatusOK)

	c, rec = newAgentDispatchContext(agentDispatchRequest{
		method: http.MethodGet,
		target: "/creator/availability-alerts?limit=7",
		userID: userID.String(),
	})
	requireNoDispatchError(t, h.ListAvailabilityAlerts(c))
	requireDispatchStatus(t, rec, http.StatusOK)
	if mock.lastAlertLimit != 7 {
		t.Fatalf("ListAvailabilityAlerts limit = %d, want 7", mock.lastAlertLimit)
	}

	c, rec = newAgentDispatchContext(agentDispatchRequest{
		method: http.MethodPost,
		target: "/creator/availability-alerts/" + alertID.String() + "/read",
		userID: userID.String(),
		params: map[string]string{"alertID": alertID.String()},
	})
	requireNoDispatchError(t, h.MarkAvailabilityAlertRead(c))
	requireDispatchStatus(t, rec, http.StatusOK)

	c, rec = newAgentDispatchContext(agentDispatchRequest{
		method: http.MethodGet,
		target: "/admin/agents/pending",
	})
	requireNoDispatchError(t, h.ListPendingAgents(c))
	requireDispatchStatus(t, rec, http.StatusOK)
	pendingBody := decodeAgentDispatchJSON[map[string][]AgentResponse](t, rec)
	if pendingBody["items"] == nil {
		t.Fatalf("ListPendingAgents should normalize nil items to an empty slice: %#v", pendingBody)
	}

	c, rec = newAgentDispatchContext(agentDispatchRequest{
		method: http.MethodPost,
		target: "/creator/agents/" + agentID.String() + "/request-certification",
		userID: userID.String(),
		params: map[string]string{"id": agentID.String()},
	})
	requireNoDispatchError(t, h.RequestCertification(c))
	requireDispatchStatus(t, rec, http.StatusOK)

	c, rec = newAgentDispatchContext(agentDispatchRequest{
		method: http.MethodPost,
		target: "/admin/agents/" + agentID.String() + "/certify",
		params: map[string]string{"id": agentID.String()},
	})
	requireNoDispatchError(t, h.CertifyAgent(c))
	requireDispatchStatus(t, rec, http.StatusOK)

	c, rec = newAgentDispatchContext(agentDispatchRequest{
		method: http.MethodPost,
		target: "/admin/agents/" + agentID.String() + "/reject-certification",
		params: map[string]string{"id": agentID.String()},
		body:   `{"reason":"missing required proof"}`,
	})
	requireNoDispatchError(t, h.RejectCertification(c))
	requireDispatchStatus(t, rec, http.StatusOK)

	wantCalls := []string{
		"CheckSlug",
		"BecomeCreator",
		"CreateAgent",
		"ListMyAgentsPage",
		"ListMyAgentsPage",
		"ListMyAgentsPage",
		"GetMyAgent",
		"UpdateAgent",
		"SetVisibility",
		"DisableAgent",
		"GetAgentOnboarding",
		"UpsertCapability",
		"CreateExample",
		"DeleteExample",
		"RunDryRun",
		"RunDryRun",
		"ListAvailabilityAlerts",
		"MarkAvailabilityAlertRead",
		"ListPendingForAdmin",
		"RequestCertification",
		"CertifyAgent",
		"RejectCertification",
	}
	if !reflect.DeepEqual(mock.calls, wantCalls) {
		t.Fatalf("calls = %#v, want %#v", mock.calls, wantCalls)
	}
	if mock.lastUserID != userID || mock.lastAgentID != agentID || mock.lastExampleID != exampleID || mock.lastAlertID != alertID {
		t.Fatalf("ids were not dispatched correctly: user=%s agent=%s example=%s alert=%s", mock.lastUserID, mock.lastAgentID, mock.lastExampleID, mock.lastAlertID)
	}
	if mock.lastCreateAgent == nil || mock.lastCreateAgent.Slug != "demo-agent" || mock.lastCreateAgent.Tags[0] != "ai" {
		t.Fatalf("CreateAgent request not dispatched: %#v", mock.lastCreateAgent)
	}
	if mock.lastVisibility != "private" || mock.lastRejectReason != "missing required proof" {
		t.Fatalf("visibility/reject reason = %q/%q", mock.lastVisibility, mock.lastRejectReason)
	}
}

func TestRegistrationApprovalAndMetricHandlersDispatchServices(t *testing.T) {
	userID := uuid.New()
	agentID := uuid.New()
	tokenID := uuid.New()
	approvalID := uuid.New()
	regSvc := &mockRegistrationService{}
	reg := NewRegistrationHandler(regSvc)

	c, rec := newAgentDispatchContext(agentDispatchRequest{
		method: http.MethodPost,
		target: "/creator/agent-tokens",
		userID: userID.String(),
		body:   `{"name":"local dev","expires_in_minutes":30}`,
	})
	requireNoDispatchError(t, reg.CreateAgentToken(c))
	requireDispatchStatus(t, rec, http.StatusCreated)

	c, rec = newAgentDispatchContext(agentDispatchRequest{
		method: http.MethodGet,
		target: "/creator/agent-tokens?limit=5&offset=10&sort_by=name&sort_dir=asc",
		userID: userID.String(),
	})
	requireNoDispatchError(t, reg.ListAgentTokens(c))
	requireDispatchStatus(t, rec, http.StatusOK)
	if regSvc.lastListOpts.Limit != 5 || regSvc.lastListOpts.Offset != 10 || regSvc.lastListOpts.SortBy != "name" || regSvc.lastListOpts.SortDir != "asc" {
		t.Fatalf("list opts = %#v", regSvc.lastListOpts)
	}

	c, rec = newAgentDispatchContext(agentDispatchRequest{
		method: http.MethodDelete,
		target: "/creator/agent-tokens/" + tokenID.String(),
		userID: userID.String(),
		params: map[string]string{"id": tokenID.String()},
	})
	requireNoDispatchError(t, reg.RevokeAgentToken(c))
	requireDispatchStatus(t, rec, http.StatusNoContent)

	bearer := strings.Repeat("t", 24)
	c, rec = newAgentDispatchContext(agentDispatchRequest{
		method:  http.MethodPost,
		target:  "/agent-registration/agents",
		headers: map[string]string{echo.HeaderAuthorization: "Bearer " + bearer},
		body:    `{"name":"Bootstrap Agent","endpoint_url":"https://example.com/boot","ability_tags":["ai"]}`,
	})
	requireNoDispatchError(t, reg.RegisterAgentViaToken(c))
	requireDispatchStatus(t, rec, http.StatusCreated)
	if regSvc.lastRegisterReq == nil || regSvc.lastRegisterReq.AgentToken != bearer {
		t.Fatalf("agent token was not pulled from Authorization header: %#v", regSvc.lastRegisterReq)
	}
	if !reflect.DeepEqual(regSvc.lastRegisterReq.Tags, []string{"ai"}) || regSvc.lastRegisterReq.Visibility != "public" {
		t.Fatalf("registration defaults were not normalized before dispatch: %#v", regSvc.lastRegisterReq)
	}

	approvalSvc := &mockApprovalService{}
	approval := NewApprovalHandler(approvalSvc)
	c, rec = newAgentDispatchContext(agentDispatchRequest{
		method: http.MethodPost,
		target: "/creator/approvals",
		userID: userID.String(),
		body:   `{"agent_id":"` + agentID.String() + `","action":"set-visibility-public","payload":{"visibility":"public"}}`,
	})
	requireNoDispatchError(t, approval.CreateApproval(c))
	requireDispatchStatus(t, rec, http.StatusCreated)

	c, rec = newAgentDispatchContext(agentDispatchRequest{
		method: http.MethodGet,
		target: "/creator/approvals",
		userID: userID.String(),
	})
	requireNoDispatchError(t, approval.ListApprovals(c))
	requireDispatchStatus(t, rec, http.StatusOK)

	c, rec = newAgentDispatchContext(agentDispatchRequest{
		method: http.MethodGet,
		target: "/creator/approvals/" + approvalID.String(),
		userID: userID.String(),
		params: map[string]string{"id": approvalID.String()},
	})
	requireNoDispatchError(t, approval.GetApproval(c))
	requireDispatchStatus(t, rec, http.StatusOK)

	c, rec = newAgentDispatchContext(agentDispatchRequest{
		method: http.MethodPost,
		target: "/creator/approvals/" + approvalID.String() + "/confirm",
		userID: userID.String(),
		params: map[string]string{"id": approvalID.String()},
		body:   `{"note":"approved"}`,
	})
	requireNoDispatchError(t, approval.ConfirmApproval(c))
	requireDispatchStatus(t, rec, http.StatusOK)

	c, rec = newAgentDispatchContext(agentDispatchRequest{
		method: http.MethodPost,
		target: "/creator/approvals/" + approvalID.String() + "/reject",
		userID: userID.String(),
		params: map[string]string{"id": approvalID.String()},
		body:   `{"note":"rejected"}`,
	})
	requireNoDispatchError(t, approval.RejectApproval(c))
	requireDispatchStatus(t, rec, http.StatusOK)

	wantApprovalCalls := []string{"CreateApproval", "ListApprovals", "GetApproval", "ConfirmApproval", "RejectApproval"}
	if !reflect.DeepEqual(approvalSvc.calls, wantApprovalCalls) || approvalSvc.lastNote != "rejected" {
		t.Fatalf("approval calls/note = %#v/%q", approvalSvc.calls, approvalSvc.lastNote)
	}

	metricSvc := &mockMetricService{}
	metric := NewMetricHandler(metricSvc)
	c, rec = newAgentDispatchContext(agentDispatchRequest{
		method: http.MethodGet,
		target: "/agents/" + agentID.String() + "/metrics",
		params: map[string]string{"id": agentID.String()},
	})
	requireNoDispatchError(t, metric.GetMetrics(c))
	requireDispatchStatus(t, rec, http.StatusOK)
	if metricSvc.lastAgentID != agentID {
		t.Fatalf("metric agent id = %s, want %s", metricSvc.lastAgentID, agentID)
	}
}

func TestAgentHandlersPropagateServiceErrors(t *testing.T) {
	sentinel := errors.New("service stopped")
	userID := uuid.New().String()
	agentID := uuid.New().String()
	exampleID := uuid.New().String()
	alertID := uuid.New().String()

	for _, tc := range []struct {
		name   string
		method func(echo.Context) error
		req    agentDispatchRequest
	}{
		{
			name:   "check slug",
			method: NewHandler(&mockAgentService{err: sentinel}).CheckSlug,
			req:    agentDispatchRequest{method: http.MethodGet, target: "/?slug=demo-agent"},
		},
		{
			name:   "create agent",
			method: NewHandler(&mockAgentService{err: sentinel}).CreateAgent,
			req: agentDispatchRequest{
				method: http.MethodPost,
				target: "/creator/agents",
				userID: userID,
				body:   `{"slug":"demo-agent","name":"Demo Agent","endpoint_url":"https://example.com/a","tags":["ai"]}`,
			},
		},
		{
			name:   "delete example",
			method: NewHandler(&mockAgentService{err: sentinel}).DeleteExample,
			req: agentDispatchRequest{
				method: http.MethodDelete,
				target: "/" + agentID + "/examples/" + exampleID,
				userID: userID,
				params: map[string]string{"id": agentID, "exampleID": exampleID},
			},
		},
		{
			name:   "mark alert",
			method: NewHandler(&mockAgentService{err: sentinel}).MarkAvailabilityAlertRead,
			req: agentDispatchRequest{
				method: http.MethodPost,
				target: "/alerts/" + alertID + "/read",
				userID: userID,
				params: map[string]string{"alertID": alertID},
			},
		},
		{
			name:   "reject certification",
			method: NewHandler(&mockAgentService{err: sentinel}).RejectCertification,
			req: agentDispatchRequest{
				method: http.MethodPost,
				target: "/" + agentID + "/reject-certification",
				params: map[string]string{"id": agentID},
				body:   `{"reason":"missing required proof"}`,
			},
		},
		{
			name:   "registration",
			method: NewRegistrationHandler(&mockRegistrationService{err: sentinel}).RegisterAgentViaToken,
			req: agentDispatchRequest{
				method: http.MethodPost,
				target: "/agent-registration/agents",
				body:   `{"agent_token":"` + strings.Repeat("t", 24) + `","name":"Bootstrap Agent","endpoint_url":"https://example.com/boot","tags":["ai"]}`,
			},
		},
		{
			name:   "approval",
			method: NewApprovalHandler(&mockApprovalService{err: sentinel}).ConfirmApproval,
			req: agentDispatchRequest{
				method: http.MethodPost,
				target: "/" + agentID + "/confirm",
				userID: userID,
				params: map[string]string{"id": agentID},
				body:   `{"note":"approved"}`,
			},
		},
		{
			name:   "metric",
			method: NewMetricHandler(&mockMetricService{err: sentinel}).GetMetrics,
			req: agentDispatchRequest{
				method: http.MethodGet,
				target: "/" + agentID + "/metrics",
				params: map[string]string{"id": agentID},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := newAgentDispatchContext(tc.req)
			if err := tc.method(c); !errors.Is(err, sentinel) {
				t.Fatalf("error = %v, want sentinel", err)
			}
		})
	}
}

type mockAgentService struct {
	err              error
	calls            []string
	lastSlug         string
	lastUserID       uuid.UUID
	lastAgentID      uuid.UUID
	lastExampleID    uuid.UUID
	lastAlertID      uuid.UUID
	lastAlertLimit   int32
	lastVisibility   string
	lastRejectReason string
	lastCreateAgent  *CreateAgentRequest
	lastListOptions  AgentListOptions
}

func (m *mockAgentService) record(call string) {
	m.calls = append(m.calls, call)
}

func (m *mockAgentService) CheckSlug(_ context.Context, slug string) (*SlugCheckResponse, error) {
	m.record("CheckSlug")
	m.lastSlug = slug
	if m.err != nil {
		return nil, m.err
	}
	return &SlugCheckResponse{Slug: slug, Available: true}, nil
}

func (m *mockAgentService) BecomeCreator(_ context.Context, userID uuid.UUID) error {
	m.record("BecomeCreator")
	m.lastUserID = userID
	return m.err
}

func (m *mockAgentService) CreateAgent(_ context.Context, userID uuid.UUID, req *CreateAgentRequest) (*AgentResponse, error) {
	m.record("CreateAgent")
	m.lastUserID = userID
	m.lastCreateAgent = req
	if m.err != nil {
		return nil, m.err
	}
	return &AgentResponse{ID: uuid.NewString(), Slug: req.Slug, Name: req.Name, Tags: req.Tags}, nil
}

func (m *mockAgentService) ListMyAgents(_ context.Context, userID uuid.UUID) ([]AgentResponse, error) {
	m.record("ListMyAgents")
	m.lastUserID = userID
	return nil, m.err
}

func (m *mockAgentService) ListMyAgentsPage(_ context.Context, userID uuid.UUID, opts AgentListOptions) (*AgentListResponse, error) {
	m.record("ListMyAgentsPage")
	m.lastUserID = userID
	m.lastListOptions = opts
	if m.err != nil {
		return nil, m.err
	}
	return &AgentListResponse{Items: []AgentResponse{}, Total: 42, Limit: opts.Limit, Offset: opts.Offset}, nil
}

func (m *mockAgentService) GetMyAgent(_ context.Context, agentID, userID uuid.UUID) (*AgentResponse, error) {
	m.record("GetMyAgent")
	m.lastAgentID = agentID
	m.lastUserID = userID
	if m.err != nil {
		return nil, m.err
	}
	return &AgentResponse{ID: agentID.String(), Slug: "demo-agent", Name: "Demo Agent"}, nil
}

func (m *mockAgentService) UpdateAgent(_ context.Context, agentID, userID uuid.UUID, req *UpdateAgentRequest) (*AgentResponse, error) {
	m.record("UpdateAgent")
	m.lastAgentID = agentID
	m.lastUserID = userID
	if m.err != nil {
		return nil, m.err
	}
	return &AgentResponse{ID: agentID.String(), Name: req.Name, Tags: req.Tags}, nil
}

func (m *mockAgentService) SetVisibility(_ context.Context, agentID, userID uuid.UUID, visibility string) (*AgentResponse, error) {
	m.record("SetVisibility")
	m.lastAgentID = agentID
	m.lastUserID = userID
	m.lastVisibility = visibility
	if m.err != nil {
		return nil, m.err
	}
	return &AgentResponse{ID: agentID.String(), Visibility: visibility}, nil
}

func (m *mockAgentService) DisableAgent(_ context.Context, agentID, userID uuid.UUID) error {
	m.record("DisableAgent")
	m.lastAgentID = agentID
	m.lastUserID = userID
	return m.err
}

func (m *mockAgentService) GetAgentOnboarding(_ context.Context, agentID, userID uuid.UUID) (*OnboardingResponse, error) {
	m.record("GetAgentOnboarding")
	m.lastAgentID = agentID
	m.lastUserID = userID
	if m.err != nil {
		return nil, m.err
	}
	return &OnboardingResponse{Status: OnboardingStatusResponse{AgentID: agentID.String()}}, nil
}

func (m *mockAgentService) UpsertCapability(_ context.Context, agentID, userID uuid.UUID, req *UpsertCapabilityRequest) (*CapabilityResponse, error) {
	m.record("UpsertCapability")
	m.lastAgentID = agentID
	m.lastUserID = userID
	if m.err != nil {
		return nil, m.err
	}
	return &CapabilityResponse{AgentID: agentID.String(), InputSchema: req.InputSchema, OutputSchema: req.OutputSchema}, nil
}

func (m *mockAgentService) CreateExample(_ context.Context, agentID, userID uuid.UUID, req *CreateExampleRequest) (*ExampleResponse, error) {
	m.record("CreateExample")
	m.lastAgentID = agentID
	m.lastUserID = userID
	if m.err != nil {
		return nil, m.err
	}
	return &ExampleResponse{ID: uuid.NewString(), AgentID: agentID.String(), Title: req.Title, InputJSON: req.InputJSON}, nil
}

func (m *mockAgentService) DeleteExample(_ context.Context, agentID, exampleID, userID uuid.UUID) error {
	m.record("DeleteExample")
	m.lastAgentID = agentID
	m.lastExampleID = exampleID
	m.lastUserID = userID
	return m.err
}

func (m *mockAgentService) RunDryRun(_ context.Context, agentID, userID uuid.UUID) (*DryRunResponse, error) {
	m.record("RunDryRun")
	m.lastAgentID = agentID
	m.lastUserID = userID
	if m.err != nil {
		return nil, m.err
	}
	return &DryRunResponse{Result: "pass", Output: map[string]interface{}{"ok": true}}, nil
}

func (m *mockAgentService) ListAvailabilityAlerts(_ context.Context, userID uuid.UUID, limit int32) (*AvailabilityAlertListResponse, error) {
	m.record("ListAvailabilityAlerts")
	m.lastUserID = userID
	m.lastAlertLimit = limit
	if m.err != nil {
		return nil, m.err
	}
	return &AvailabilityAlertListResponse{Items: []AvailabilityAlertResponse{}, Total: 0, Unread: 0}, nil
}

func (m *mockAgentService) MarkAvailabilityAlertRead(_ context.Context, userID, alertID uuid.UUID) (*AvailabilityAlertResponse, error) {
	m.record("MarkAvailabilityAlertRead")
	m.lastUserID = userID
	m.lastAlertID = alertID
	if m.err != nil {
		return nil, m.err
	}
	return &AvailabilityAlertResponse{ID: alertID.String(), Type: "availability"}, nil
}

func (m *mockAgentService) ListPendingForAdmin(context.Context) ([]AgentResponse, error) {
	m.record("ListPendingForAdmin")
	return nil, m.err
}

func (m *mockAgentService) RequestCertification(_ context.Context, agentID, userID uuid.UUID) error {
	m.record("RequestCertification")
	m.lastAgentID = agentID
	m.lastUserID = userID
	return m.err
}

func (m *mockAgentService) CertifyAgent(_ context.Context, agentID uuid.UUID) error {
	m.record("CertifyAgent")
	m.lastAgentID = agentID
	return m.err
}

func (m *mockAgentService) RejectCertification(_ context.Context, agentID uuid.UUID, reason string) error {
	m.record("RejectCertification")
	m.lastAgentID = agentID
	m.lastRejectReason = reason
	return m.err
}

type mockRegistrationService struct {
	err             error
	calls           []string
	lastUserID      uuid.UUID
	lastTokenID     uuid.UUID
	lastListOpts    ListAgentTokensOptions
	lastRegisterReq *RegisterAgentViaTokenRequest
}

func (m *mockRegistrationService) record(call string) {
	m.calls = append(m.calls, call)
}

func (m *mockRegistrationService) CreateAgentToken(_ context.Context, userID uuid.UUID, req *CreateAgentTokenRequest) (*AgentTokenResponse, error) {
	m.record("CreateAgentToken")
	m.lastUserID = userID
	if m.err != nil {
		return nil, m.err
	}
	return &AgentTokenResponse{ID: uuid.NewString(), Name: req.Name, Prefix: "ol_agent_test", Status: "pending_registration"}, nil
}

func (m *mockRegistrationService) ListAgentTokens(_ context.Context, userID uuid.UUID, _ *uuid.UUID, opts ListAgentTokensOptions) (*AgentTokenListResponse, error) {
	m.record("ListAgentTokens")
	m.lastUserID = userID
	m.lastListOpts = opts
	if m.err != nil {
		return nil, m.err
	}
	return &AgentTokenListResponse{
		Items:   []AgentTokenResponse{{ID: uuid.NewString(), Name: "local", Prefix: "ol_agent_test", Status: "pending_registration"}},
		Total:   1,
		Limit:   opts.Limit,
		Offset:  opts.Offset,
		SortBy:  opts.SortBy,
		SortDir: opts.SortDir,
	}, nil
}

func (m *mockRegistrationService) RevokeAgentToken(_ context.Context, userID, tokenID uuid.UUID) error {
	m.record("RevokeAgentToken")
	m.lastUserID = userID
	m.lastTokenID = tokenID
	return m.err
}

func (m *mockRegistrationService) RegisterAgentViaToken(_ context.Context, req *RegisterAgentViaTokenRequest) (*RegisterAgentViaTokenResponse, error) {
	m.record("RegisterAgentViaToken")
	m.lastRegisterReq = req
	if m.err != nil {
		return nil, m.err
	}
	return &RegisterAgentViaTokenResponse{
		Agent: AgentResponse{
			ID:         uuid.NewString(),
			Slug:       "bootstrap-agent",
			Name:       req.Name,
			Tags:       req.Tags,
			Visibility: req.Visibility,
		},
		AgentToken: AgentTokenResponse{ID: uuid.NewString(), Prefix: "ol_agent_test", Status: "active_runtime"},
	}, nil
}

type mockApprovalService struct {
	err        error
	calls      []string
	lastUserID uuid.UUID
	lastID     uuid.UUID
	lastNote   string
}

func (m *mockApprovalService) record(call string) {
	m.calls = append(m.calls, call)
}

func (m *mockApprovalService) CreateApproval(_ context.Context, userID uuid.UUID, req *CreateApprovalRequest) (*ApprovalResponse, error) {
	m.record("CreateApproval")
	m.lastUserID = userID
	if m.err != nil {
		return nil, m.err
	}
	return &ApprovalResponse{ID: uuid.NewString(), AgentID: req.AgentID, Action: req.Action, Status: "pending"}, nil
}

func (m *mockApprovalService) ListApprovals(_ context.Context, userID uuid.UUID) ([]ApprovalResponse, error) {
	m.record("ListApprovals")
	m.lastUserID = userID
	if m.err != nil {
		return nil, m.err
	}
	return []ApprovalResponse{{ID: uuid.NewString(), Status: "pending"}}, nil
}

func (m *mockApprovalService) GetApproval(_ context.Context, userID, id uuid.UUID) (*ApprovalResponse, error) {
	m.record("GetApproval")
	m.lastUserID = userID
	m.lastID = id
	if m.err != nil {
		return nil, m.err
	}
	return &ApprovalResponse{ID: id.String(), Status: "pending"}, nil
}

func (m *mockApprovalService) ConfirmApproval(_ context.Context, userID, id uuid.UUID, note string) error {
	m.record("ConfirmApproval")
	m.lastUserID = userID
	m.lastID = id
	m.lastNote = note
	return m.err
}

func (m *mockApprovalService) RejectApproval(_ context.Context, userID, id uuid.UUID, note string) error {
	m.record("RejectApproval")
	m.lastUserID = userID
	m.lastID = id
	m.lastNote = note
	return m.err
}

type mockMetricService struct {
	err         error
	lastAgentID uuid.UUID
}

func (m *mockMetricService) GetSnapshots(_ context.Context, agentID uuid.UUID) (*MetricSnapshotsResponse, error) {
	m.lastAgentID = agentID
	if m.err != nil {
		return nil, m.err
	}
	return &MetricSnapshotsResponse{AgentID: agentID.String(), Items: []MetricSnapshot{}}, nil
}

type agentDispatchRequest struct {
	method  string
	target  string
	body    string
	userID  string
	params  map[string]string
	headers map[string]string
}

func newAgentDispatchContext(reqSpec agentDispatchRequest) (echo.Context, *httptest.ResponseRecorder) {
	method := reqSpec.method
	if method == "" {
		method = http.MethodGet
	}
	req := httptest.NewRequest(method, reqSpec.target, strings.NewReader(reqSpec.body))
	if reqSpec.body != "" {
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	}
	for name, value := range reqSpec.headers {
		req.Header.Set(name, value)
	}
	rec := httptest.NewRecorder()
	c := echo.New().NewContext(req, rec)
	if reqSpec.userID != "" {
		c.Set(string(httpx.CtxKeyUserID), reqSpec.userID)
	}
	if len(reqSpec.params) > 0 {
		names := make([]string, 0, len(reqSpec.params))
		values := make([]string, 0, len(reqSpec.params))
		for name, value := range reqSpec.params {
			names = append(names, name)
			values = append(values, value)
		}
		c.SetParamNames(names...)
		c.SetParamValues(values...)
	}
	return c, rec
}

func decodeAgentDispatchJSON[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var out T
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode response %q: %v", rec.Body.String(), err)
	}
	return out
}

func requireNoDispatchError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func requireDispatchStatus(t *testing.T, rec *httptest.ResponseRecorder, want int) {
	t.Helper()
	if rec.Code != want {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, want, rec.Body.String())
	}
}
