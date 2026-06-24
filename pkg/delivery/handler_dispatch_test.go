package delivery

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

func TestDeliveryHandlerDispatchesServiceSuccess(t *testing.T) {
	userID := uuid.New()
	targetID := uuid.New()
	runID := uuid.New()
	deliveryID := uuid.New()
	createdAt := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC).Format(time.RFC3339)

	t.Run("create target", func(t *testing.T) {
		mock := &mockDeliveryService{createTargetResp: &TargetResponse{
			ID:        targetID.String(),
			Name:      "Webhook",
			Type:      targetTypeWebhook,
			URL:       "https://example.com/hook",
			Secret:    "secret",
			IsDefault: true,
			CreatedAt: createdAt,
		}}
		c, rec := newDeliveryRecorderContext(
			http.MethodPost,
			"/delivery-targets",
			`{"name":"Webhook","type":"webhook","url":"https://example.com/hook","is_default":true}`,
			userID.String(),
			nil,
		)

		if err := NewHandler(mock).CreateTarget(c); err != nil {
			t.Fatalf("CreateTarget error = %v", err)
		}
		if rec.Code != http.StatusCreated {
			t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
		}
		if mock.createTargetUserID != userID || mock.createTargetReq == nil || mock.createTargetReq.URL != "https://example.com/hook" || !mock.createTargetReq.IsDefault {
			t.Fatalf("captured create target = user %s req %#v", mock.createTargetUserID, mock.createTargetReq)
		}
		var body TargetResponse
		decodeDeliveryJSON(t, rec, &body)
		if body.ID != targetID.String() || body.Secret == "" {
			t.Fatalf("body = %#v", body)
		}
	})

	t.Run("list targets", func(t *testing.T) {
		mock := &mockDeliveryService{listTargetsResp: []TargetResponse{{
			ID:        targetID.String(),
			Name:      "Webhook",
			Type:      targetTypeWebhook,
			URL:       "https://example.com/hook",
			CreatedAt: createdAt,
		}}}
		c, rec := newDeliveryRecorderContext(http.MethodGet, "/delivery-targets", "", userID.String(), nil)

		if err := NewHandler(mock).ListTargets(c); err != nil {
			t.Fatalf("ListTargets error = %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
		}
		if mock.listTargetsUserID != userID {
			t.Fatalf("captured list targets user = %s", mock.listTargetsUserID)
		}
		var body struct {
			Items []TargetResponse `json:"items"`
		}
		decodeDeliveryJSON(t, rec, &body)
		if len(body.Items) != 1 || body.Items[0].ID != targetID.String() {
			t.Fatalf("body = %#v", body)
		}
	})

	t.Run("delete target", func(t *testing.T) {
		mock := &mockDeliveryService{}
		c, rec := newDeliveryRecorderContext(http.MethodDelete, "/delivery-targets/"+targetID.String(), "", userID.String(), map[string]string{"id": targetID.String()})

		if err := NewHandler(mock).DeleteTarget(c); err != nil {
			t.Fatalf("DeleteTarget error = %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
		}
		if mock.deleteTargetID != targetID || mock.deleteTargetUserID != userID {
			t.Fatalf("captured delete target = target %s user %s", mock.deleteTargetID, mock.deleteTargetUserID)
		}
		assertStatusBody(t, rec, "deleted")
	})

	t.Run("set default", func(t *testing.T) {
		mock := &mockDeliveryService{}
		c, rec := newDeliveryRecorderContext(http.MethodPost, "/delivery-targets/"+targetID.String()+"/default", "", userID.String(), map[string]string{"id": targetID.String()})

		if err := NewHandler(mock).SetDefault(c); err != nil {
			t.Fatalf("SetDefault error = %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
		}
		if mock.setDefaultTargetID != targetID || mock.setDefaultUserID != userID {
			t.Fatalf("captured set default = target %s user %s", mock.setDefaultTargetID, mock.setDefaultUserID)
		}
		assertStatusBody(t, rec, "default_set")
	})

	t.Run("deliver run with explicit target", func(t *testing.T) {
		mock := &mockDeliveryService{deliverRunResp: &DeliveryItem{
			ID:           deliveryID.String(),
			RunID:        runID.String(),
			TargetID:     targetID.String(),
			TargetType:   targetTypeWebhook,
			TargetURL:    "https://example.com/hook",
			Status:       "pending",
			AttemptCount: 0,
			CreatedAt:    createdAt,
			UpdatedAt:    createdAt,
		}}
		c, rec := newDeliveryRecorderContext(
			http.MethodPost,
			"/runs/"+runID.String()+"/deliver",
			`{"target_id":"`+targetID.String()+`"}`,
			userID.String(),
			map[string]string{"id": runID.String()},
		)

		if err := NewHandler(mock).DeliverRun(c); err != nil {
			t.Fatalf("DeliverRun error = %v", err)
		}
		if rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
		}
		if mock.deliverRunUserID != userID || mock.deliverRunID != runID || mock.deliverTargetID == nil || *mock.deliverTargetID != targetID {
			t.Fatalf("captured deliver = user %s run %s target %#v", mock.deliverRunUserID, mock.deliverRunID, mock.deliverTargetID)
		}
		var body DeliveryItem
		decodeDeliveryJSON(t, rec, &body)
		if body.ID != deliveryID.String() || body.Status != "pending" {
			t.Fatalf("body = %#v", body)
		}
	})

	t.Run("deliver run with default target", func(t *testing.T) {
		mock := &mockDeliveryService{deliverRunResp: &DeliveryItem{ID: deliveryID.String(), Status: "pending"}}
		c, rec := newDeliveryRecorderContext(http.MethodPost, "/runs/"+runID.String()+"/deliver", "", userID.String(), map[string]string{"id": runID.String()})

		if err := NewHandler(mock).DeliverRun(c); err != nil {
			t.Fatalf("DeliverRun default error = %v", err)
		}
		if rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
		}
		if mock.deliverTargetID != nil {
			t.Fatalf("default target should pass nil target id, got %#v", mock.deliverTargetID)
		}
	})

	t.Run("list deliveries nil becomes empty", func(t *testing.T) {
		mock := &mockDeliveryService{}
		c, rec := newDeliveryRecorderContext(http.MethodGet, "/runs/"+runID.String()+"/deliveries", "", userID.String(), map[string]string{"id": runID.String()})

		if err := NewHandler(mock).ListDeliveries(c); err != nil {
			t.Fatalf("ListDeliveries error = %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
		}
		if mock.listByRunID != runID || mock.listByRunUserID != userID {
			t.Fatalf("captured list deliveries = run %s user %s", mock.listByRunID, mock.listByRunUserID)
		}
		var body struct {
			Items []DeliveryItem `json:"items"`
		}
		decodeDeliveryJSON(t, rec, &body)
		if body.Items == nil || len(body.Items) != 0 {
			t.Fatalf("body = %#v", body)
		}
	})

	t.Run("list all deliveries filters", func(t *testing.T) {
		mock := &mockDeliveryService{listResp: []DeliveryItem{{
			ID:           deliveryID.String(),
			RunID:        runID.String(),
			TargetID:     targetID.String(),
			TargetType:   targetTypeSlack,
			TargetURL:    "https://hooks.slack.com/services/demo",
			Status:       "failed",
			AttemptCount: 3,
			CreatedAt:    createdAt,
			UpdatedAt:    createdAt,
		}}}
		c, rec := newDeliveryRecorderContext(
			http.MethodGet,
			"/deliveries?agent_id="+targetID.String()+"&run_id="+runID.String()+"&status=failed&limit=7",
			"",
			userID.String(),
			nil,
		)

		if err := NewHandler(mock).ListAllDeliveries(c); err != nil {
			t.Fatalf("ListAllDeliveries error = %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
		}
		if mock.listUserID != userID || mock.listFilter.AgentID == nil || *mock.listFilter.AgentID != targetID.String() || mock.listFilter.RunID == nil || *mock.listFilter.RunID != runID.String() || mock.listFilter.Status != "failed" || mock.listFilter.Limit != 7 {
			t.Fatalf("captured list = user %s filter %#v", mock.listUserID, mock.listFilter)
		}
		var body struct {
			Items []DeliveryItem `json:"items"`
		}
		decodeDeliveryJSON(t, rec, &body)
		if len(body.Items) != 1 || body.Items[0].ID != deliveryID.String() {
			t.Fatalf("body = %#v", body)
		}
	})

	t.Run("retry delivery", func(t *testing.T) {
		mock := &mockDeliveryService{}
		c, rec := newDeliveryRecorderContext(http.MethodPost, "/deliveries/"+deliveryID.String()+"/retry", "", userID.String(), map[string]string{"id": deliveryID.String()})

		if err := NewHandler(mock).RetryDelivery(c); err != nil {
			t.Fatalf("RetryDelivery error = %v", err)
		}
		if rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
		}
		if mock.retryDeliveryID != deliveryID || mock.retryDeliveryUserID != userID {
			t.Fatalf("captured retry = delivery %s user %s", mock.retryDeliveryID, mock.retryDeliveryUserID)
		}
		assertStatusBody(t, rec, "retry_enqueued")
	})
}

func TestDeliveryHandlerPropagatesServiceErrors(t *testing.T) {
	userID := uuid.New()
	targetID := uuid.New()
	runID := uuid.New()
	deliveryID := uuid.New()

	tests := []struct {
		name string
		call func(*Handler, echo.Context) error
		mock *mockDeliveryService
		ctx  echo.Context
		want int
	}{
		{
			name: "create target",
			call: (*Handler).CreateTarget,
			mock: &mockDeliveryService{createTargetErr: httpx.Conflict("duplicate")},
			ctx:  mustDeliveryContext(http.MethodPost, "/delivery-targets", `{"name":"Webhook","type":"webhook","url":"https://example.com/hook"}`, userID.String(), nil),
			want: http.StatusConflict,
		},
		{
			name: "list targets",
			call: (*Handler).ListTargets,
			mock: &mockDeliveryService{listTargetsErr: httpx.Internal("list failed")},
			ctx:  mustDeliveryContext(http.MethodGet, "/delivery-targets", "", userID.String(), nil),
			want: http.StatusInternalServerError,
		},
		{
			name: "delete target",
			call: (*Handler).DeleteTarget,
			mock: &mockDeliveryService{deleteTargetErr: httpx.NotFound("missing")},
			ctx:  mustDeliveryContext(http.MethodDelete, "/delivery-targets/"+targetID.String(), "", userID.String(), map[string]string{"id": targetID.String()}),
			want: http.StatusNotFound,
		},
		{
			name: "set default",
			call: (*Handler).SetDefault,
			mock: &mockDeliveryService{setDefaultErr: httpx.NotFound("missing")},
			ctx:  mustDeliveryContext(http.MethodPost, "/delivery-targets/"+targetID.String()+"/default", "", userID.String(), map[string]string{"id": targetID.String()}),
			want: http.StatusNotFound,
		},
		{
			name: "deliver run",
			call: (*Handler).DeliverRun,
			mock: &mockDeliveryService{deliverRunErr: httpx.BadRequest("not ready")},
			ctx:  mustDeliveryContext(http.MethodPost, "/runs/"+runID.String()+"/deliver", "", userID.String(), map[string]string{"id": runID.String()}),
			want: http.StatusBadRequest,
		},
		{
			name: "list deliveries",
			call: (*Handler).ListDeliveries,
			mock: &mockDeliveryService{listByRunErr: httpx.NotFound("missing")},
			ctx:  mustDeliveryContext(http.MethodGet, "/runs/"+runID.String()+"/deliveries", "", userID.String(), map[string]string{"id": runID.String()}),
			want: http.StatusNotFound,
		},
		{
			name: "list all deliveries",
			call: (*Handler).ListAllDeliveries,
			mock: &mockDeliveryService{listErr: httpx.Internal("list failed")},
			ctx:  mustDeliveryContext(http.MethodGet, "/deliveries", "", userID.String(), nil),
			want: http.StatusInternalServerError,
		},
		{
			name: "retry delivery",
			call: (*Handler).RetryDelivery,
			mock: &mockDeliveryService{retryDeliveryErr: httpx.NotFound("missing")},
			ctx:  mustDeliveryContext(http.MethodPost, "/deliveries/"+deliveryID.String()+"/retry", "", userID.String(), map[string]string{"id": deliveryID.String()}),
			want: http.StatusNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requireDeliveryHTTPStatus(t, tt.call(NewHandler(tt.mock), tt.ctx), tt.want)
		})
	}
}

type mockDeliveryService struct {
	createTargetUserID uuid.UUID
	createTargetReq    *CreateTargetRequest
	createTargetResp   *TargetResponse
	createTargetErr    error

	listTargetsUserID uuid.UUID
	listTargetsResp   []TargetResponse
	listTargetsErr    error

	deleteTargetID     uuid.UUID
	deleteTargetUserID uuid.UUID
	deleteTargetErr    error

	setDefaultTargetID uuid.UUID
	setDefaultUserID   uuid.UUID
	setDefaultErr      error

	deliverRunUserID uuid.UUID
	deliverRunID     uuid.UUID
	deliverTargetID  *uuid.UUID
	deliverRunResp   *DeliveryItem
	deliverRunErr    error

	listByRunID     uuid.UUID
	listByRunUserID uuid.UUID
	listByRunResp   []DeliveryItem
	listByRunErr    error

	listUserID uuid.UUID
	listFilter DeliveryListFilter
	listResp   []DeliveryItem
	listErr    error

	retryDeliveryID     uuid.UUID
	retryDeliveryUserID uuid.UUID
	retryDeliveryErr    error
}

func (m *mockDeliveryService) CreateTarget(_ context.Context, userID uuid.UUID, req *CreateTargetRequest) (*TargetResponse, error) {
	m.createTargetUserID = userID
	m.createTargetReq = req
	return m.createTargetResp, m.createTargetErr
}

func (m *mockDeliveryService) ListTargets(_ context.Context, userID uuid.UUID) ([]TargetResponse, error) {
	m.listTargetsUserID = userID
	return m.listTargetsResp, m.listTargetsErr
}

func (m *mockDeliveryService) DeleteTarget(_ context.Context, targetID, userID uuid.UUID) error {
	m.deleteTargetID = targetID
	m.deleteTargetUserID = userID
	return m.deleteTargetErr
}

func (m *mockDeliveryService) SetDefault(_ context.Context, targetID, userID uuid.UUID) error {
	m.setDefaultTargetID = targetID
	m.setDefaultUserID = userID
	return m.setDefaultErr
}

func (m *mockDeliveryService) DeliverRun(_ context.Context, userID, runID uuid.UUID, targetID *uuid.UUID) (*DeliveryItem, error) {
	m.deliverRunUserID = userID
	m.deliverRunID = runID
	m.deliverTargetID = targetID
	return m.deliverRunResp, m.deliverRunErr
}

func (m *mockDeliveryService) ListByRun(_ context.Context, runID, userID uuid.UUID) ([]DeliveryItem, error) {
	m.listByRunID = runID
	m.listByRunUserID = userID
	return m.listByRunResp, m.listByRunErr
}

func (m *mockDeliveryService) List(_ context.Context, userID uuid.UUID, filter DeliveryListFilter) ([]DeliveryItem, error) {
	m.listUserID = userID
	m.listFilter = filter
	return m.listResp, m.listErr
}

func (m *mockDeliveryService) RetryDelivery(_ context.Context, deliveryID, userID uuid.UUID) error {
	m.retryDeliveryID = deliveryID
	m.retryDeliveryUserID = userID
	return m.retryDeliveryErr
}

func newDeliveryRecorderContext(method, target, body, userID string, params map[string]string) (echo.Context, *httptest.ResponseRecorder) {
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

func mustDeliveryContext(method, target, body, userID string, params map[string]string) echo.Context {
	c, _ := newDeliveryRecorderContext(method, target, body, userID, params)
	return c
}

func decodeDeliveryJSON(t *testing.T, rec *httptest.ResponseRecorder, out any) {
	t.Helper()
	if err := json.Unmarshal(rec.Body.Bytes(), out); err != nil {
		t.Fatalf("decode json: %v body=%s", err, rec.Body.String())
	}
}

func assertStatusBody(t *testing.T, rec *httptest.ResponseRecorder, want string) {
	t.Helper()
	var body map[string]string
	decodeDeliveryJSON(t, rec, &body)
	if body["status"] != want {
		t.Fatalf("status body = %#v, want %q", body, want)
	}
}
