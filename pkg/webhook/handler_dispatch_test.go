package webhook

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

func TestWebhookHandlerDispatchesServiceSuccess(t *testing.T) {
	userID := uuid.New()
	agentID := uuid.New()
	runID := uuid.New()
	callbackID := uuid.New()
	createdAt := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC).Format(time.RFC3339)
	subscription := TaskCallbackSubscriptionResponse{
		ID:                  callbackID.String(),
		RunID:               runID.String(),
		TargetURL:           "https://client.example/push",
		EventTypes:          []string{"run.completed"},
		AuthScheme:          "Bearer",
		Status:              "active",
		ConsecutiveFailures: 0,
		Secret:              "secret",
		CreatedAt:           createdAt,
		UpdatedAt:           createdAt,
	}

	t.Run("set", func(t *testing.T) {
		mock := &mockWebhookService{setResp: &SetWebhookResponse{URL: "https://client.example/hook", Secret: "secret"}}
		c, rec := newWebhookRecorderContext(
			http.MethodPost,
			"/creator/agents/"+agentID.String()+"/webhook",
			`{"webhook_url":"https://client.example/hook"}`,
			userID.String(),
			map[string]string{"id": agentID.String()},
		)

		if err := NewHandler(mock).Set(c); err != nil {
			t.Fatalf("Set error = %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
		}
		if mock.setAgentID != agentID || mock.setUserID != userID || mock.setURL != "https://client.example/hook" {
			t.Fatalf("captured set = agent %s user %s url %q", mock.setAgentID, mock.setUserID, mock.setURL)
		}
		var body SetWebhookResponse
		decodeWebhookJSON(t, rec, &body)
		if body.URL == "" || body.Secret == "" {
			t.Fatalf("body = %#v", body)
		}
	})

	t.Run("clear", func(t *testing.T) {
		mock := &mockWebhookService{}
		c, rec := newWebhookRecorderContext(http.MethodDelete, "/creator/agents/"+agentID.String()+"/webhook", "", userID.String(), map[string]string{"id": agentID.String()})

		if err := NewHandler(mock).Clear(c); err != nil {
			t.Fatalf("Clear error = %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
		}
		if mock.clearAgentID != agentID || mock.clearUserID != userID {
			t.Fatalf("captured clear = agent %s user %s", mock.clearAgentID, mock.clearUserID)
		}
		assertWebhookStatusBody(t, rec, "cleared")
	})

	t.Run("rotate", func(t *testing.T) {
		mock := &mockWebhookService{rotateResp: &SetWebhookResponse{URL: "https://client.example/hook", Secret: "new-secret"}}
		c, rec := newWebhookRecorderContext(http.MethodPost, "/creator/agents/"+agentID.String()+"/webhook/rotate", "", userID.String(), map[string]string{"id": agentID.String()})

		if err := NewHandler(mock).Rotate(c); err != nil {
			t.Fatalf("Rotate error = %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
		}
		if mock.rotateAgentID != agentID || mock.rotateUserID != userID {
			t.Fatalf("captured rotate = agent %s user %s", mock.rotateAgentID, mock.rotateUserID)
		}
	})

	t.Run("list deliveries nil becomes empty", func(t *testing.T) {
		mock := &mockWebhookService{}
		c, rec := newWebhookRecorderContext(http.MethodGet, "/creator/agents/"+agentID.String()+"/webhook/deliveries?limit=7", "", userID.String(), map[string]string{"id": agentID.String()})

		if err := NewHandler(mock).ListDeliveries(c); err != nil {
			t.Fatalf("ListDeliveries error = %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
		}
		if mock.listDeliveriesAgentID != agentID || mock.listDeliveriesUserID != userID || mock.listDeliveriesLimit != 7 {
			t.Fatalf("captured list deliveries = agent %s user %s limit %d", mock.listDeliveriesAgentID, mock.listDeliveriesUserID, mock.listDeliveriesLimit)
		}
		var body struct {
			Items []DeliveryListItem `json:"items"`
		}
		decodeWebhookJSON(t, rec, &body)
		if body.Items == nil || len(body.Items) != 0 {
			t.Fatalf("body = %#v", body)
		}
	})

	t.Run("create task callback", func(t *testing.T) {
		mock := &mockWebhookService{createTaskCallbackResp: &subscription}
		c, rec := newWebhookRecorderContext(
			http.MethodPost,
			"/runs/"+runID.String()+"/task-callbacks",
			`{"target_url":"https://client.example/push","event_types":["run.completed"],"auth_scheme":"Bearer","auth_credentials":"token"}`,
			userID.String(),
			map[string]string{"id": runID.String()},
		)

		if err := NewHandler(mock).CreateTaskCallback(c); err != nil {
			t.Fatalf("CreateTaskCallback error = %v", err)
		}
		if rec.Code != http.StatusCreated {
			t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
		}
		if mock.createTaskCallbackRunID != runID || mock.createTaskCallbackUserID != userID || mock.createTaskCallbackReq == nil || mock.createTaskCallbackReq.URL == "" {
			t.Fatalf("captured create task callback = run %s user %s req %#v", mock.createTaskCallbackRunID, mock.createTaskCallbackUserID, mock.createTaskCallbackReq)
		}
		var body TaskCallbackSubscriptionResponse
		decodeWebhookJSON(t, rec, &body)
		if body.ID != callbackID.String() || body.Secret == "" {
			t.Fatalf("body = %#v", body)
		}
	})

	t.Run("list task callbacks nil becomes empty", func(t *testing.T) {
		mock := &mockWebhookService{}
		c, rec := newWebhookRecorderContext(http.MethodGet, "/runs/"+runID.String()+"/task-callbacks", "", userID.String(), map[string]string{"id": runID.String()})

		if err := NewHandler(mock).ListTaskCallbacks(c); err != nil {
			t.Fatalf("ListTaskCallbacks error = %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
		}
		if mock.listTaskCallbacksRunID != runID || mock.listTaskCallbacksUserID != userID {
			t.Fatalf("captured list task callbacks = run %s user %s", mock.listTaskCallbacksRunID, mock.listTaskCallbacksUserID)
		}
		var body struct {
			Items []TaskCallbackSubscriptionResponse `json:"items"`
		}
		decodeWebhookJSON(t, rec, &body)
		if body.Items == nil || len(body.Items) != 0 {
			t.Fatalf("body = %#v", body)
		}
	})

	t.Run("managed list", func(t *testing.T) {
		mock := &mockWebhookService{listOwnerResp: []TaskCallbackSubscriptionResponse{subscription}}
		c, rec := newWebhookRecorderContext(http.MethodGet, "/task-callbacks?status=active&limit=5", "", userID.String(), nil)

		if err := NewHandler(mock).ListManagedTaskCallbacks(c); err != nil {
			t.Fatalf("ListManagedTaskCallbacks error = %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
		}
		if mock.listOwnerUserID != userID || mock.listOwnerStatus != "active" || mock.listOwnerLimit != 5 {
			t.Fatalf("captured owner list = user %s status %q limit %d", mock.listOwnerUserID, mock.listOwnerStatus, mock.listOwnerLimit)
		}
		var body TaskCallbackSubscriptionListResponse
		decodeWebhookJSON(t, rec, &body)
		if len(body.Items) != 1 || body.Items[0].ID != callbackID.String() {
			t.Fatalf("body = %#v", body)
		}
	})

	t.Run("batch manage", func(t *testing.T) {
		mock := &mockWebhookService{batchResp: &BatchTaskCallbackSubscriptionsResponse{
			Action:       "pause",
			UpdatedCount: 1,
			Items:        []TaskCallbackSubscriptionResponse{subscription},
		}}
		c, rec := newWebhookRecorderContext(
			http.MethodPost,
			"/task-callbacks/batch",
			`{"subscription_ids":["`+callbackID.String()+`"],"action":"pause"}`,
			userID.String(),
			nil,
		)

		if err := NewHandler(mock).BatchManageTaskCallbacks(c); err != nil {
			t.Fatalf("BatchManageTaskCallbacks error = %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
		}
		if mock.batchUserID != userID || mock.batchReq == nil || mock.batchReq.Action != "pause" {
			t.Fatalf("captured batch = user %s req %#v", mock.batchUserID, mock.batchReq)
		}
	})

	t.Run("pause resume delete", func(t *testing.T) {
		mock := &mockWebhookService{updateStatusResp: &subscription}
		pauseCtx, pauseRec := newWebhookRecorderContext(
			http.MethodPost,
			"/runs/"+runID.String()+"/task-callbacks/"+callbackID.String()+"/pause",
			"",
			userID.String(),
			map[string]string{"id": runID.String(), "callbackID": callbackID.String()},
		)
		if err := NewHandler(mock).PauseTaskCallback(pauseCtx); err != nil {
			t.Fatalf("PauseTaskCallback error = %v", err)
		}
		if pauseRec.Code != http.StatusOK || mock.updateStatus != "paused" {
			t.Fatalf("pause code=%d status=%q body=%s", pauseRec.Code, mock.updateStatus, pauseRec.Body.String())
		}

		resumeCtx, resumeRec := newWebhookRecorderContext(
			http.MethodPost,
			"/runs/"+runID.String()+"/task-callbacks/"+callbackID.String()+"/resume",
			"",
			userID.String(),
			map[string]string{"id": runID.String(), "callbackID": callbackID.String()},
		)
		if err := NewHandler(mock).ResumeTaskCallback(resumeCtx); err != nil {
			t.Fatalf("ResumeTaskCallback error = %v", err)
		}
		if resumeRec.Code != http.StatusOK || mock.updateStatus != "active" {
			t.Fatalf("resume code=%d status=%q body=%s", resumeRec.Code, mock.updateStatus, resumeRec.Body.String())
		}

		deleteCtx, deleteRec := newWebhookRecorderContext(
			http.MethodDelete,
			"/runs/"+runID.String()+"/task-callbacks/"+callbackID.String(),
			"",
			userID.String(),
			map[string]string{"id": runID.String(), "callbackID": callbackID.String()},
		)
		if err := NewHandler(mock).DeleteTaskCallback(deleteCtx); err != nil {
			t.Fatalf("DeleteTaskCallback error = %v", err)
		}
		if deleteRec.Code != http.StatusOK || mock.deleteCallbackRunID != runID || mock.deleteCallbackID != callbackID || mock.deleteCallbackUserID != userID {
			t.Fatalf("delete code=%d run=%s webhook=%s user=%s", deleteRec.Code, mock.deleteCallbackRunID, mock.deleteCallbackID, mock.deleteCallbackUserID)
		}
		assertWebhookStatusBody(t, deleteRec, "deleted")
	})

}

func TestWebhookHandlerPropagatesServiceErrors(t *testing.T) {
	userID := uuid.New()
	agentID := uuid.New()
	runID := uuid.New()
	callbackID := uuid.New()

	tests := []struct {
		name string
		call func(*Handler, echo.Context) error
		mock *mockWebhookService
		ctx  echo.Context
		want int
	}{
		{
			name: "set",
			call: (*Handler).Set,
			mock: &mockWebhookService{setErr: httpx.BadRequest("bad url")},
			ctx:  mustWebhookContext(http.MethodPost, "/creator/agents/"+agentID.String()+"/webhook", `{"webhook_url":"https://client.example/hook"}`, userID.String(), map[string]string{"id": agentID.String()}),
			want: http.StatusBadRequest,
		},
		{
			name: "clear",
			call: (*Handler).Clear,
			mock: &mockWebhookService{clearErr: httpx.NotFound("missing")},
			ctx:  mustWebhookContext(http.MethodDelete, "/creator/agents/"+agentID.String()+"/webhook", "", userID.String(), map[string]string{"id": agentID.String()}),
			want: http.StatusNotFound,
		},
		{
			name: "rotate",
			call: (*Handler).Rotate,
			mock: &mockWebhookService{rotateErr: httpx.Internal("rotate failed")},
			ctx:  mustWebhookContext(http.MethodPost, "/creator/agents/"+agentID.String()+"/webhook/rotate", "", userID.String(), map[string]string{"id": agentID.String()}),
			want: http.StatusInternalServerError,
		},
		{
			name: "list deliveries",
			call: (*Handler).ListDeliveries,
			mock: &mockWebhookService{listDeliveriesErr: httpx.NotFound("missing")},
			ctx:  mustWebhookContext(http.MethodGet, "/creator/agents/"+agentID.String()+"/webhook/deliveries", "", userID.String(), map[string]string{"id": agentID.String()}),
			want: http.StatusNotFound,
		},
		{
			name: "create task callback",
			call: (*Handler).CreateTaskCallback,
			mock: &mockWebhookService{createTaskCallbackErr: httpx.Conflict("duplicate")},
			ctx:  mustWebhookContext(http.MethodPost, "/runs/"+runID.String()+"/task-callbacks", `{"target_url":"https://client.example/push"}`, userID.String(), map[string]string{"id": runID.String()}),
			want: http.StatusConflict,
		},
		{
			name: "list task callbacks",
			call: (*Handler).ListTaskCallbacks,
			mock: &mockWebhookService{listTaskCallbacksErr: httpx.NotFound("missing")},
			ctx:  mustWebhookContext(http.MethodGet, "/runs/"+runID.String()+"/task-callbacks", "", userID.String(), map[string]string{"id": runID.String()}),
			want: http.StatusNotFound,
		},
		{
			name: "managed list",
			call: (*Handler).ListManagedTaskCallbacks,
			mock: &mockWebhookService{listOwnerErr: httpx.Internal("list failed")},
			ctx:  mustWebhookContext(http.MethodGet, "/task-callbacks", "", userID.String(), nil),
			want: http.StatusInternalServerError,
		},
		{
			name: "batch",
			call: (*Handler).BatchManageTaskCallbacks,
			mock: &mockWebhookService{batchErr: httpx.BadRequest("bad batch")},
			ctx:  mustWebhookContext(http.MethodPost, "/task-callbacks/batch", `{"subscription_ids":["`+callbackID.String()+`"],"action":"pause"}`, userID.String(), nil),
			want: http.StatusBadRequest,
		},
		{
			name: "delete",
			call: (*Handler).DeleteTaskCallback,
			mock: &mockWebhookService{deleteCallbackErr: httpx.NotFound("missing")},
			ctx:  mustWebhookContext(http.MethodDelete, "/runs/"+runID.String()+"/task-callbacks/"+callbackID.String(), "", userID.String(), map[string]string{"id": runID.String(), "callbackID": callbackID.String()}),
			want: http.StatusNotFound,
		},
		{
			name: "pause",
			call: (*Handler).PauseTaskCallback,
			mock: &mockWebhookService{updateStatusErr: httpx.NotFound("missing")},
			ctx:  mustWebhookContext(http.MethodPost, "/runs/"+runID.String()+"/task-callbacks/"+callbackID.String()+"/pause", "", userID.String(), map[string]string{"id": runID.String(), "callbackID": callbackID.String()}),
			want: http.StatusNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requireWebhookHTTPStatus(t, tt.call(NewHandler(tt.mock), tt.ctx), tt.want)
		})
	}
}

type mockWebhookService struct {
	setAgentID uuid.UUID
	setUserID  uuid.UUID
	setURL     string
	setResp    *SetWebhookResponse
	setErr     error

	clearAgentID uuid.UUID
	clearUserID  uuid.UUID
	clearErr     error

	rotateAgentID uuid.UUID
	rotateUserID  uuid.UUID
	rotateResp    *SetWebhookResponse
	rotateErr     error

	listDeliveriesAgentID uuid.UUID
	listDeliveriesUserID  uuid.UUID
	listDeliveriesLimit   int
	listDeliveriesResp    []DeliveryListItem
	listDeliveriesErr     error

	createTaskCallbackRunID  uuid.UUID
	createTaskCallbackUserID uuid.UUID
	createTaskCallbackReq    *CreateTaskCallbackRequest
	createTaskCallbackResp   *TaskCallbackSubscriptionResponse
	createTaskCallbackErr    error

	listTaskCallbacksRunID  uuid.UUID
	listTaskCallbacksUserID uuid.UUID
	listTaskCallbacksResp   []TaskCallbackSubscriptionResponse
	listTaskCallbacksErr    error

	listOwnerUserID uuid.UUID
	listOwnerStatus string
	listOwnerLimit  int
	listOwnerResp   []TaskCallbackSubscriptionResponse
	listOwnerErr    error

	batchUserID uuid.UUID
	batchReq    *BatchTaskCallbackSubscriptionsRequest
	batchResp   *BatchTaskCallbackSubscriptionsResponse
	batchErr    error

	deleteCallbackRunID  uuid.UUID
	deleteCallbackID     uuid.UUID
	deleteCallbackUserID uuid.UUID
	deleteCallbackErr    error

	updateRunID      uuid.UUID
	updateCallbackID uuid.UUID
	updateUserID     uuid.UUID
	updateStatus     string
	updateStatusResp *TaskCallbackSubscriptionResponse
	updateStatusErr  error
}

func (m *mockWebhookService) SetWebhook(_ context.Context, agentID, userID uuid.UUID, url string) (*SetWebhookResponse, error) {
	m.setAgentID = agentID
	m.setUserID = userID
	m.setURL = url
	return m.setResp, m.setErr
}

func (m *mockWebhookService) ClearWebhook(_ context.Context, agentID, userID uuid.UUID) error {
	m.clearAgentID = agentID
	m.clearUserID = userID
	return m.clearErr
}

func (m *mockWebhookService) RotateSecret(_ context.Context, agentID, userID uuid.UUID) (*SetWebhookResponse, error) {
	m.rotateAgentID = agentID
	m.rotateUserID = userID
	return m.rotateResp, m.rotateErr
}

func (m *mockWebhookService) ListDeliveries(_ context.Context, agentID, userID uuid.UUID, limit int) ([]DeliveryListItem, error) {
	m.listDeliveriesAgentID = agentID
	m.listDeliveriesUserID = userID
	m.listDeliveriesLimit = limit
	return m.listDeliveriesResp, m.listDeliveriesErr
}

func (m *mockWebhookService) CreateTaskCallbackSubscription(_ context.Context, runID, userID uuid.UUID, req *CreateTaskCallbackRequest) (*TaskCallbackSubscriptionResponse, error) {
	m.createTaskCallbackRunID = runID
	m.createTaskCallbackUserID = userID
	m.createTaskCallbackReq = req
	return m.createTaskCallbackResp, m.createTaskCallbackErr
}

func (m *mockWebhookService) ListTaskCallbackSubscriptions(_ context.Context, runID, userID uuid.UUID) ([]TaskCallbackSubscriptionResponse, error) {
	m.listTaskCallbacksRunID = runID
	m.listTaskCallbacksUserID = userID
	return m.listTaskCallbacksResp, m.listTaskCallbacksErr
}

func (m *mockWebhookService) ListTaskCallbackSubscriptionsForOwner(_ context.Context, userID uuid.UUID, status string, limit int) ([]TaskCallbackSubscriptionResponse, error) {
	m.listOwnerUserID = userID
	m.listOwnerStatus = status
	m.listOwnerLimit = limit
	return m.listOwnerResp, m.listOwnerErr
}

func (m *mockWebhookService) BatchManageTaskCallbackSubscriptions(_ context.Context, userID uuid.UUID, req *BatchTaskCallbackSubscriptionsRequest) (*BatchTaskCallbackSubscriptionsResponse, error) {
	m.batchUserID = userID
	m.batchReq = req
	return m.batchResp, m.batchErr
}

func (m *mockWebhookService) DeleteTaskCallbackSubscription(_ context.Context, runID, callbackID, userID uuid.UUID) error {
	m.deleteCallbackRunID = runID
	m.deleteCallbackID = callbackID
	m.deleteCallbackUserID = userID
	return m.deleteCallbackErr
}

func (m *mockWebhookService) UpdateTaskCallbackSubscriptionStatus(_ context.Context, runID, callbackID, userID uuid.UUID, status string) (*TaskCallbackSubscriptionResponse, error) {
	m.updateRunID = runID
	m.updateCallbackID = callbackID
	m.updateUserID = userID
	m.updateStatus = status
	return m.updateStatusResp, m.updateStatusErr
}

func newWebhookRecorderContext(method, target, body, userID string, params map[string]string) (echo.Context, *httptest.ResponseRecorder) {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	if body != "" {
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	}
	rec := httptest.NewRecorder()
	c := echo.New().NewContext(req, rec)
	if userID != "" {
		c.Set(string(httpx.CtxKeyUserID), userID)
	}
	if len(params) > 0 {
		names := make([]string, 0, len(params))
		values := make([]string, 0, len(params))
		for name, value := range params {
			names = append(names, name)
			values = append(values, value)
		}
		c.SetParamNames(names...)
		c.SetParamValues(values...)
	}
	return c, rec
}

func mustWebhookContext(method, target, body, userID string, params map[string]string) echo.Context {
	c, _ := newWebhookRecorderContext(method, target, body, userID, params)
	return c
}

func decodeWebhookJSON(t *testing.T, rec *httptest.ResponseRecorder, out any) {
	t.Helper()
	if err := json.Unmarshal(rec.Body.Bytes(), out); err != nil {
		t.Fatalf("decode json: %v body=%s", err, rec.Body.String())
	}
}

func assertWebhookStatusBody(t *testing.T, rec *httptest.ResponseRecorder, want string) {
	t.Helper()
	var body map[string]string
	decodeWebhookJSON(t, rec, &body)
	if body["status"] != want {
		t.Fatalf("status body = %#v, want %q", body, want)
	}
}
