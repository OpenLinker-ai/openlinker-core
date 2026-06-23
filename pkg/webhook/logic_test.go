package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

func TestWebhookPureHelpers(t *testing.T) {
	cfg := &config.Config{AllowLocalHTTPEndpoints: true}
	svc := NewService(nil, cfg)
	if svc == nil || svc.pool != nil || svc.httpClient == nil || !svc.allowLocalHTTP {
		t.Fatalf("NewService did not preserve optional config: %+v", svc)
	}
	if withoutCfg := NewService(nil); withoutCfg == nil || withoutCfg.allowLocalHTTP {
		t.Fatalf("NewService without config should keep local HTTP disabled: %+v", withoutCfg)
	}
	handler := NewHandler(nil, cfg)
	if handler == nil || handler.validator == nil || handler.cfg != cfg {
		t.Fatalf("NewHandler did not preserve optional config: %+v", handler)
	}

	for _, tc := range []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 0, want: time.Minute},
		{attempt: 1, want: 5 * time.Minute},
		{attempt: 2, want: 30 * time.Minute},
		{attempt: 3, want: 0},
		{attempt: -1, want: 0},
	} {
		if got := nextRetryDelay(tc.attempt); got != tc.want {
			t.Fatalf("nextRetryDelay(%d) = %v, want %v", tc.attempt, got, tc.want)
		}
	}

	secret, err := generateSecret()
	if err != nil {
		t.Fatalf("generateSecret error: %v", err)
	}
	if len(secret) != secretByteLen*2 {
		t.Fatalf("secret length = %d", len(secret))
	}
	if _, err := hex.DecodeString(secret); err != nil {
		t.Fatalf("secret should be hex: %v", err)
	}

	payload := []byte(`{"event":"run.completed"}`)
	mac := hmac.New(sha256.New, []byte("secret"))
	mac.Write(payload)
	if got := signPayload(payload, "secret"); got != hex.EncodeToString(mac.Sum(nil)) {
		t.Fatalf("signPayload = %q", got)
	}

	if got := truncate("abcdef", 3); got != "abc" {
		t.Fatalf("truncate short = %q", got)
	}
	if got := truncate("abc", 3); got != "abc" {
		t.Fatalf("truncate exact = %q", got)
	}
}

func TestWebhookPayloadHelpers(t *testing.T) {
	started := time.Date(2026, 6, 20, 8, 0, 0, 0, time.UTC)
	finished := started.Add(4 * time.Second)
	duration := int32(4000)
	run := &db.Run{
		ID:         uuid.New(),
		UserID:     uuid.New(),
		AgentID:    uuid.New(),
		Input:      []byte(`{"prompt":"ship"}`),
		Status:     "success",
		Output:     []byte(`{"ignored":"service output arg wins"}`),
		CostCents:  250,
		DurationMs: &duration,
		StartedAt:  started,
		FinishedAt: &finished,
	}
	got := buildPayload(run, "runner", map[string]interface{}{"answer": "done"})
	if got.Event != eventRunCompleted || got.RunID != run.ID.String() || got.UserID != run.UserID.String() {
		t.Fatalf("unexpected identifiers: %+v", got)
	}
	if got.AgentSlug != "runner" || got.Status != "success" || got.CostCents != 250 || got.DurationMs != 4000 {
		t.Fatalf("unexpected payload basics: %+v", got)
	}
	if got.Input["prompt"] != "ship" || got.Output["answer"] != "done" {
		t.Fatalf("unexpected input/output: %+v / %+v", got.Input, got.Output)
	}
	if got.StartedAt != started.Format(time.RFC3339) || got.FinishedAt != finished.Format(time.RFC3339) {
		t.Fatalf("unexpected timestamps: %+v", got)
	}

	code := "E_TIMEOUT"
	message := "agent timed out"
	failedRun := &db.Run{
		ID:           uuid.New(),
		UserID:       uuid.New(),
		AgentID:      uuid.New(),
		Input:        []byte(`not-json`),
		Status:       "timeout",
		ErrorCode:    &code,
		ErrorMessage: &message,
		CostCents:    999,
		StartedAt:    started,
	}
	failed := buildPayload(failedRun, "runner", nil)
	if failed.CostCents != 0 || len(failed.Input) != 0 || failed.ErrorCode != code || failed.ErrorMessage != message {
		t.Fatalf("unexpected failed payload: %+v", failed)
	}
	if failed.FinishedAt == "" {
		t.Fatalf("failed payload should synthesize finished_at")
	}
}

func TestRunWebhookNormalizationAndPayloads(t *testing.T) {
	gotEvents := normalizeRunWebhookEventTypes([]string{
		" run.completed ",
		"run.completed",
		"unknown",
		"run.message.delta",
	})
	if !reflect.DeepEqual(gotEvents, []string{"run.completed", "run.message.delta"}) {
		t.Fatalf("normalizeRunWebhookEventTypes = %#v", gotEvents)
	}
	defaultEvents := normalizeRunWebhookEventTypes([]string{"unknown", " "})
	if !reflect.DeepEqual(defaultEvents, []string{"run.completed", "run.failed", "run.canceled"}) {
		t.Fatalf("default events = %#v", defaultEvents)
	}

	scheme, creds := normalizePushAuth(" Bearer ", " token ")
	if scheme == nil || creds == nil || *scheme != "Bearer" || *creds != "token" {
		t.Fatalf("normalizePushAuth = %#v %#v", scheme, creds)
	}
	if scheme, creds := normalizePushAuth("Bearer", " "); scheme != nil || creds != nil {
		t.Fatalf("empty credentials should omit auth: %#v %#v", scheme, creds)
	}
	if scheme, creds := normalizePushAuth(" ", "token"); scheme != nil || creds != nil {
		t.Fatalf("empty scheme should omit auth: %#v %#v", scheme, creds)
	}

	for _, tc := range []struct {
		action string
		want   string
	}{
		{action: " pause ", want: "paused"},
		{action: "resume", want: "active"},
		{action: "delete", want: "deleted"},
	} {
		if got, err := batchActionToRunWebhookStatus(tc.action); err != nil || got != tc.want {
			t.Fatalf("batchActionToRunWebhookStatus(%q) = %q %v", tc.action, got, err)
		}
	}
	if _, err := batchActionToRunWebhookStatus("stop"); err == nil {
		t.Fatalf("invalid batch action should fail")
	}

	idA := uuid.New()
	idB := uuid.New()
	ids, err := parseRunWebhookSubscriptionIDs([]string{idA.String(), " " + idA.String() + " ", idB.String()})
	if err != nil || !reflect.DeepEqual(ids, []uuid.UUID{idA, idB}) {
		t.Fatalf("parse ids = %#v %v", ids, err)
	}
	if _, err := parseRunWebhookSubscriptionIDs(nil); err == nil {
		t.Fatalf("empty ids should fail")
	}
	if _, err := parseRunWebhookSubscriptionIDs([]string{"bad"}); err == nil {
		t.Fatalf("bad uuid should fail")
	}
	tooMany := make([]string, 51)
	for i := range tooMany {
		tooMany[i] = uuid.NewString()
	}
	if _, err := parseRunWebhookSubscriptionIDs(tooMany); err == nil {
		t.Fatalf("too many ids should fail")
	}

	now := time.Date(2026, 6, 20, 9, 0, 0, 0, time.UTC)
	parentID := uuid.New()
	sub := db.RunWebhookSubscription{
		ID:                  uuid.New(),
		RunID:               uuid.New(),
		TargetURL:           "https://client.example/a2a/push",
		EventTypes:          []string{"run.completed"},
		PushAuthScheme:      stringPtr("Bearer"),
		Status:              "active",
		ConsecutiveFailures: 2,
		CreatedAt:           now,
		UpdatedAt:           now.Add(time.Second),
	}
	resp := runWebhookSubscriptionToResponse(sub)
	if resp.ID != sub.ID.String() || resp.PushAuthScheme != "Bearer" || resp.ConsecutiveFailures != 2 {
		t.Fatalf("subscription response = %+v", resp)
	}
	resp.EventTypes[0] = "mutated"
	if sub.EventTypes[0] != "run.completed" {
		t.Fatalf("response should copy event types")
	}
	sub.PushAuthScheme = nil
	noAuthResp := runWebhookSubscriptionToResponse(sub)
	if noAuthResp.PushAuthScheme != "" {
		t.Fatalf("nil push auth scheme should serialize empty: %+v", noAuthResp)
	}

	event := db.RunEvent{
		ID:          uuid.New(),
		RunID:       sub.RunID,
		ParentRunID: &parentID,
		Sequence:    7,
		EventType:   "run.artifact.delta",
		Payload:     []byte(`{"path":"report.json"}`),
		CreatedAt:   now,
	}
	pushPayload := runWebhookPayload(sub, event)
	if pushPayload.EventID != event.ID.String() || pushPayload.ParentRunID != parentID.String() || pushPayload.Payload["path"] != "report.json" {
		t.Fatalf("runWebhookPayload = %+v", pushPayload)
	}
	event.Payload = []byte(`not-json`)
	rawPayload := runWebhookPayload(sub, event)
	if rawPayload.Payload["raw"] != "not-json" {
		t.Fatalf("invalid payload should be preserved as raw: %+v", rawPayload.Payload)
	}
	event.ParentRunID = nil
	event.Payload = nil
	emptyPayload := runWebhookPayload(sub, event)
	if emptyPayload.ParentRunID != "" || len(emptyPayload.Payload) != 0 {
		t.Fatalf("nil parent/payload should stay empty: %+v", emptyPayload)
	}

	status := int32(202)
	errMsg := "accepted"
	nextRetry := now.Add(time.Minute)
	item := toDeliveryListItem(db.WebhookDelivery{
		ID:             uuid.New(),
		RunID:          uuid.New(),
		URL:            "https://client.example/hook",
		Status:         "pending",
		ResponseStatus: &status,
		ErrorMessage:   &errMsg,
		AttemptCount:   1,
		NextRetryAt:    &nextRetry,
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	if item.NextRetryAt == nil || *item.NextRetryAt != nextRetry.Format(time.RFC3339) || item.ResponseStatus == nil || *item.ResponseStatus != status {
		t.Fatalf("delivery list item = %+v", item)
	}
	minimalItem := toDeliveryListItem(db.WebhookDelivery{ID: uuid.New(), RunID: uuid.New(), CreatedAt: now, UpdatedAt: now})
	if minimalItem.NextRetryAt != nil || minimalItem.ResponseStatus != nil || minimalItem.CreatedAt != now.Format(time.RFC3339) {
		t.Fatalf("minimal delivery list item = %+v", minimalItem)
	}
}

func TestDoDeliverWithEventAddsCompatibilityHeaders(t *testing.T) {
	payload := []byte(`{"event_type":"run.completed"}`)
	deliveryID := uuid.New()
	secret := "push-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s", r.Method)
		}
		if r.Header.Get("User-Agent") != userAgent || r.Header.Get("Accept") != "application/json" || r.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("missing HTTP compatibility headers: %#v", r.Header)
		}
		if r.Header.Get("X-OpenLinker-Event") != "run.completed" || r.Header.Get("X-OpenLinker-Delivery") != deliveryID.String() {
			t.Fatalf("missing push headers: %#v", r.Header)
		}
		if _, err := strconv.ParseInt(r.Header.Get("X-OpenLinker-Timestamp"), 10, 64); err != nil {
			t.Fatalf("timestamp header = %q", r.Header.Get("X-OpenLinker-Timestamp"))
		}
		if r.Header.Get("X-OpenLinker-Signature") != "sha256="+signPayload(payload, secret) {
			t.Fatalf("signature = %q", r.Header.Get("X-OpenLinker-Signature"))
		}
		if r.Header.Get("Authorization") != "Bearer token" {
			t.Fatalf("authorization = %q", r.Header.Get("Authorization"))
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("accepted"))
	}))
	defer server.Close()

	svc := &Service{httpClient: server.Client()}
	status, body, err := svc.doDeliverWithEvent(context.Background(), server.URL, secret, deliveryID, payload, "run.completed", stringPtr(" Bearer "), stringPtr(" token "))
	if err != nil || status != http.StatusAccepted || body != "accepted" {
		t.Fatalf("doDeliverWithEvent = %d %q %v", status, body, err)
	}
	if _, _, err := svc.doDeliverWithEvent(context.Background(), "://bad", secret, deliveryID, payload, "run.completed", nil, nil); err == nil {
		t.Fatalf("invalid URL should fail before HTTP")
	}
}

func TestDoDeliverWrapperLimitsBodyAndWorkerStopsOnCancel(t *testing.T) {
	payload := []byte(`{"event":"run.completed"}`)
	deliveryID := uuid.New()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-OpenLinker-Event") != eventRunCompleted {
			t.Fatalf("wrapper event header = %q", r.Header.Get("X-OpenLinker-Event"))
		}
		if r.Header.Get("Authorization") != "" {
			t.Fatalf("doDeliver should not set auth header: %q", r.Header.Get("Authorization"))
		}
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(strings.Repeat("x", responseBodyMaxLen*5)))
	}))
	defer server.Close()

	svc := &Service{httpClient: server.Client()}
	status, body, err := svc.doDeliver(context.Background(), server.URL, "secret", deliveryID, payload)
	if err != nil || status != http.StatusConflict {
		t.Fatalf("doDeliver = %d %q %v", status, body, err)
	}
	if len(body) != responseBodyMaxLen*4 {
		t.Fatalf("response body should be capped at %d, got %d", responseBodyMaxLen*4, len(body))
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() {
		StartWorker(ctx, &Service{})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("StartWorker should stop promptly when context is already canceled")
	}
}

func TestWebhookHandlerValidationAndRoutes(t *testing.T) {
	h := NewHandler(&Service{})
	userID := uuid.NewString()
	id := uuid.NewString()
	webhookID := uuid.NewString()

	for _, tc := range []struct {
		name   string
		method func(echo.Context) error
		req    *webhookHandlerRequest
		want   int
	}{
		{name: "set missing user", method: h.Set, req: &webhookHandlerRequest{method: http.MethodPost, target: "/"}, want: http.StatusUnauthorized},
		{name: "set invalid id", method: h.Set, req: &webhookHandlerRequest{method: http.MethodPost, target: "/", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "set invalid json", method: h.Set, req: &webhookHandlerRequest{method: http.MethodPost, target: "/", userID: userID, params: map[string]string{"id": id}, body: "{"}, want: http.StatusBadRequest},
		{name: "set validation", method: h.Set, req: &webhookHandlerRequest{method: http.MethodPost, target: "/", userID: userID, params: map[string]string{"id": id}, body: `{}`}, want: http.StatusUnprocessableEntity},
		{name: "clear missing user", method: h.Clear, req: &webhookHandlerRequest{method: http.MethodDelete, target: "/", params: map[string]string{"id": id}}, want: http.StatusUnauthorized},
		{name: "rotate invalid id", method: h.Rotate, req: &webhookHandlerRequest{method: http.MethodPost, target: "/", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "list deliveries bad limit", method: h.ListDeliveries, req: &webhookHandlerRequest{method: http.MethodGet, target: "/?limit=bad", userID: userID, params: map[string]string{"id": id}}, want: http.StatusBadRequest},
		{name: "create run webhook validation", method: h.CreateRunWebhook, req: &webhookHandlerRequest{method: http.MethodPost, target: "/", userID: userID, params: map[string]string{"id": id}, body: `{}`}, want: http.StatusUnprocessableEntity},
		{name: "list run webhooks invalid id", method: h.ListRunWebhooks, req: &webhookHandlerRequest{method: http.MethodGet, target: "/", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "managed list missing user", method: h.ListManagedRunWebhooks, req: &webhookHandlerRequest{method: http.MethodGet, target: "/"}, want: http.StatusUnauthorized},
		{name: "managed list bad limit", method: h.ListManagedRunWebhooks, req: &webhookHandlerRequest{method: http.MethodGet, target: "/?limit=bad", userID: userID}, want: http.StatusBadRequest},
		{name: "batch invalid json", method: h.BatchManageRunWebhooks, req: &webhookHandlerRequest{method: http.MethodPost, target: "/", userID: userID, body: "{"}, want: http.StatusBadRequest},
		{name: "batch validation", method: h.BatchManageRunWebhooks, req: &webhookHandlerRequest{method: http.MethodPost, target: "/", userID: userID, body: `{}`}, want: http.StatusUnprocessableEntity},
		{name: "delete bad run id", method: h.DeleteRunWebhook, req: &webhookHandlerRequest{method: http.MethodDelete, target: "/", userID: userID, params: map[string]string{"id": "bad", "webhookID": webhookID}}, want: http.StatusBadRequest},
		{name: "delete bad webhook id", method: h.DeleteRunWebhook, req: &webhookHandlerRequest{method: http.MethodDelete, target: "/", userID: userID, params: map[string]string{"id": id, "webhookID": "bad"}}, want: http.StatusBadRequest},
		{name: "pause bad run id", method: h.PauseRunWebhook, req: &webhookHandlerRequest{method: http.MethodPost, target: "/", userID: userID, params: map[string]string{"id": "bad", "webhookID": webhookID}}, want: http.StatusBadRequest},
		{name: "pause bad webhook id", method: h.PauseRunWebhook, req: &webhookHandlerRequest{method: http.MethodPost, target: "/", userID: userID, params: map[string]string{"id": id, "webhookID": "bad"}}, want: http.StatusBadRequest},
		{name: "resume bad webhook id", method: h.ResumeRunWebhook, req: &webhookHandlerRequest{method: http.MethodPost, target: "/", userID: userID, params: map[string]string{"id": id, "webhookID": "bad"}}, want: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newWebhookTestContext(tc.req)
			requireWebhookHTTPStatus(t, tc.method(c), tc.want)
		})
	}

	c := newWebhookTestContext(&webhookHandlerRequest{method: http.MethodGet, target: "/", userID: userID})
	if got, err := userIDFromCtx(c); err != nil || got.String() != userID {
		t.Fatalf("userIDFromCtx valid = %s %v", got, err)
	}
	c = newWebhookTestContext(&webhookHandlerRequest{method: http.MethodGet, target: "/", userID: "bad"})
	requireWebhookHTTPStatus(t, userIDFromCtxOnly(c), http.StatusUnauthorized)
	c = newWebhookTestContext(&webhookHandlerRequest{method: http.MethodGet, target: "/", params: map[string]string{"id": id}})
	if got, err := pathID(c); err != nil || got.String() != id {
		t.Fatalf("pathID valid = %s %v", got, err)
	}

	e := echo.New()
	api := e.Group("/api/v1")
	noop := func(next echo.HandlerFunc) echo.HandlerFunc { return next }
	h.RegisterProtected(api, noop)
	routes := map[string]bool{}
	for _, route := range e.Routes() {
		routes[route.Method+" "+route.Path] = true
	}
	for _, route := range []string{
		"POST /api/v1/creator/agents/:id/webhook",
		"DELETE /api/v1/creator/agents/:id/webhook",
		"POST /api/v1/creator/agents/:id/webhook/rotate",
		"GET /api/v1/creator/agents/:id/webhook/deliveries",
		"POST /api/v1/runs/:id/webhooks",
		"GET /api/v1/runs/:id/webhooks",
		"POST /api/v1/runs/:id/webhooks/:webhookID/pause",
		"POST /api/v1/runs/:id/webhooks/:webhookID/resume",
		"DELETE /api/v1/runs/:id/webhooks/:webhookID",
		"GET /api/v1/run-webhooks",
		"POST /api/v1/run-webhooks/batch",
	} {
		if !routes[route] {
			t.Fatalf("missing route %s", route)
		}
	}
}

type webhookHandlerRequest struct {
	method  string
	target  string
	body    string
	userID  string
	params  map[string]string
	headers map[string]string
}

func newWebhookTestContext(spec *webhookHandlerRequest) echo.Context {
	method := spec.method
	if method == "" {
		method = http.MethodGet
	}
	req := httptest.NewRequest(method, spec.target, strings.NewReader(spec.body))
	if spec.body != "" {
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	}
	for key, value := range spec.headers {
		req.Header.Set(key, value)
	}
	rec := httptest.NewRecorder()
	c := echo.New().NewContext(req, rec)
	if spec.userID != "" {
		c.Set(string(httpx.CtxKeyUserID), spec.userID)
	}
	if len(spec.params) > 0 {
		names := make([]string, 0, len(spec.params))
		values := make([]string, 0, len(spec.params))
		for name, value := range spec.params {
			names = append(names, name)
			values = append(values, value)
		}
		c.SetParamNames(names...)
		c.SetParamValues(values...)
	}
	return c
}

func requireWebhookHTTPStatus(t *testing.T, err error, want int) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected HTTP error %d, got nil", want)
	}
	var he *httpx.HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("expected *httpx.HTTPError, got %T (%v)", err, err)
	}
	if he.Status != want {
		t.Fatalf("HTTP status = %d (%s), want %d", he.Status, he.Message, want)
	}
}

func userIDFromCtxOnly(c echo.Context) error {
	_, err := userIDFromCtx(c)
	return err
}

func stringPtr(v string) *string {
	return &v
}
