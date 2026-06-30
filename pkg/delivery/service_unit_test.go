package delivery

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

func TestNextRetryDelay(t *testing.T) {
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 0, want: time.Minute},
		{attempt: 1, want: 5 * time.Minute},
		{attempt: 2, want: 30 * time.Minute},
		{attempt: 3, want: 0},
	}
	for _, tt := range tests {
		if got := nextRetryDelay(tt.attempt); got != tt.want {
			t.Fatalf("nextRetryDelay(%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

func TestBuildPayloadNormalizesRunData(t *testing.T) {
	started := time.Date(2026, 6, 19, 8, 0, 0, 0, time.UTC)
	finished := started.Add(3 * time.Second)
	duration := int32(3000)
	run := &db.Run{
		ID:         uuid.New(),
		AgentID:    uuid.New(),
		Input:      []byte(`{"prompt":"ship it"}`),
		Output:     []byte(`{"answer":"done"}`),
		Status:     "success",
		CostCents:  125,
		DurationMs: &duration,
		StartedAt:  started,
		FinishedAt: &finished,
	}

	got := buildPayload(run, "shipper", "Shipping Agent", eventRunCompleted)
	if got.Event != eventRunCompleted || got.RunID != run.ID.String() || got.AgentID != run.AgentID.String() {
		t.Fatalf("unexpected identifiers: %+v", got)
	}
	if got.AgentSlug != "shipper" || got.AgentName != "Shipping Agent" || got.Status != "success" {
		t.Fatalf("unexpected agent/status: %+v", got)
	}
	if got.Input["prompt"] != "ship it" || got.Output["answer"] != "done" {
		t.Fatalf("unexpected input/output: %+v / %+v", got.Input, got.Output)
	}
	if got.CostCents != 125 || got.DurationMs != 3000 {
		t.Fatalf("unexpected accounting: %+v", got)
	}
	if got.StartedAt != started.Format(time.RFC3339) || got.FinishedAt != finished.Format(time.RFC3339) {
		t.Fatalf("unexpected timestamps: %+v", got)
	}
}

func TestBuildPayloadDefaultsInvalidJSONAndFailedCost(t *testing.T) {
	started := time.Date(2026, 6, 19, 8, 0, 0, 0, time.UTC)
	run := &db.Run{
		ID:        uuid.New(),
		AgentID:   uuid.New(),
		Input:     []byte(`not-json`),
		Output:    []byte(`not-json`),
		Status:    "failed",
		CostCents: 999,
		StartedAt: started,
	}

	got := buildPayload(run, "agent", "Agent", eventRunFailed)
	if len(got.Input) != 0 {
		t.Fatalf("invalid input JSON should become empty object: %+v", got.Input)
	}
	if got.Output != nil {
		t.Fatalf("invalid output JSON should stay omitted: %+v", got.Output)
	}
	if got.CostCents != 0 {
		t.Fatalf("failed run cost = %d, want 0", got.CostCents)
	}
	if got.FinishedAt == "" {
		t.Fatalf("FinishedAt should be populated even without run.FinishedAt")
	}
}

func TestSignPayloadAndGenerateSecret(t *testing.T) {
	payload := []byte(`{"run_id":"run_1"}`)
	secret := "secret"
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)
	want := hex.EncodeToString(mac.Sum(nil))
	if got := signPayload(payload, secret); got != want {
		t.Fatalf("signature = %s, want %s", got, want)
	}

	generated, err := generateSecret()
	if err != nil {
		t.Fatalf("generateSecret returned error: %v", err)
	}
	if len(generated) != secretByteLen*2 {
		t.Fatalf("secret length = %d", len(generated))
	}
	if _, err := hex.DecodeString(generated); err != nil {
		t.Fatalf("secret should be hex: %v", err)
	}
}

func TestDeliveryTransforms(t *testing.T) {
	now := time.Date(2026, 6, 19, 8, 0, 0, 0, time.UTC)
	targetID := uuid.New()
	runID := uuid.New()
	target := db.DeliveryTarget{
		ID:        targetID,
		Name:      "Webhook",
		Type:      targetTypeWebhook,
		Config:    []byte(`{"url":"https://example.com/hook","event_types":["run.completed","run.failed"]}`),
		Secret:    "delivery-secret",
		IsDefault: true,
		CreatedAt: now,
	}
	targetResp := toTargetResponse(target, true)
	if targetResp.ID != targetID.String() || targetResp.URL != "https://example.com/hook" || targetResp.Secret != "delivery-secret" || len(targetResp.EventTypes) != 2 {
		t.Fatalf("unexpected target response: %+v", targetResp)
	}

	status := int32(500)
	errMsg := "temporarily down"
	nextRetry := now.Add(time.Minute)
	item := toDeliveryItem(db.RunDelivery{
		ID:             uuid.New(),
		RunID:          runID,
		TargetID:       targetID,
		TargetType:     targetTypeWebhook,
		TargetURL:      "https://example.com/hook",
		Status:         "failed",
		ResponseStatus: &status,
		ErrorMessage:   &errMsg,
		AttemptCount:   2,
		NextRetryAt:    &nextRetry,
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	if item.RunID != runID.String() || item.TargetID != targetID.String() || item.NextRetryAt == nil {
		t.Fatalf("unexpected delivery item: %+v", item)
	}
	if *item.NextRetryAt != nextRetry.Format(time.RFC3339) {
		t.Fatalf("next_retry_at = %s", *item.NextRetryAt)
	}
}

func TestDeliverWebhookSendsSignedJSON(t *testing.T) {
	deliveryID := uuid.New()
	payload := []byte(`{"event":"run.completed"}`)
	secret := "webhook-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s", r.Method)
		}
		if r.Header.Get("User-Agent") != userAgent || r.Header.Get("X-OpenLinker-Event") != eventRunCompleted {
			t.Fatalf("missing delivery headers: %#v", r.Header)
		}
		if r.Header.Get("X-OpenLinker-Delivery") != deliveryID.String() {
			t.Fatalf("delivery id header = %q", r.Header.Get("X-OpenLinker-Delivery"))
		}
		if r.Header.Get("X-OpenLinker-Signature") != "sha256="+signPayload(payload, secret) {
			t.Fatalf("signature header = %q", r.Header.Get("X-OpenLinker-Signature"))
		}
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if body["event"] != eventRunCompleted {
			t.Fatalf("event = %q", body["event"])
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("accepted"))
	}))
	defer server.Close()

	svc := &Service{httpClient: server.Client()}
	status, body, err := svc.deliverWebhook(context.Background(), server.URL, secret, deliveryID, payload, eventRunCompleted)
	if err != nil {
		t.Fatalf("deliverWebhook error: %v", err)
	}
	if status != http.StatusAccepted || body != "accepted" {
		t.Fatalf("status/body = %d/%q", status, body)
	}
}

func TestDeliverSlackFormatsPayload(t *testing.T) {
	payload, err := json.Marshal(DeliveryPayload{
		AgentName:  "Slack Agent",
		Status:     "success",
		CostCents:  123,
		DurationMs: 456,
		Output:     map[string]interface{}{"summary": "done"},
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("User-Agent") != userAgent || r.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("missing Slack headers: %#v", r.Header)
		}
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		text := body["text"]
		if !strings.Contains(text, "Slack Agent") || !strings.Contains(text, "$1.230") || !strings.Contains(text, "summary") {
			t.Fatalf("unexpected Slack text: %q", text)
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	svc := &Service{httpClient: server.Client()}
	status, body, err := svc.deliverSlack(context.Background(), server.URL, uuid.New(), payload)
	if err != nil {
		t.Fatalf("deliverSlack error: %v", err)
	}
	if status != http.StatusOK || body != "ok" {
		t.Fatalf("status/body = %d/%q", status, body)
	}
}

func TestDeliverSlackRejectsInvalidPayloadAndTruncate(t *testing.T) {
	svc := &Service{httpClient: http.DefaultClient}
	if _, _, err := svc.deliverSlack(context.Background(), "http://example.invalid", uuid.New(), []byte(`not-json`)); err == nil {
		t.Fatalf("invalid Slack payload should fail before HTTP")
	}
	if got := truncate("abcdef", 3); got != "abc" {
		t.Fatalf("truncate = %q", got)
	}
	if got := truncate("abc", 3); got != "abc" {
		t.Fatalf("truncate exact = %q", got)
	}
}

func TestDeliveryServiceTargetCRUDAndHistory(t *testing.T) {
	userID := uuid.New()
	targetID := uuid.New()
	runID := uuid.New()
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	target := db.DeliveryTarget{
		ID:        targetID,
		UserID:    userID,
		Name:      "Webhook",
		Type:      targetTypeWebhook,
		Config:    []byte(`{"url":"https://example.com/hook"}`),
		Secret:    "secret",
		IsDefault: true,
		CreatedAt: now,
		UpdatedAt: now,
	}

	queries := &fakeDeliveryQueries{
		targets:      []db.DeliveryTarget{target},
		createTarget: target,
		deleteRows:   1,
		defaultRows:  1,
		target:       target,
		updateTarget: target,
		run:          db.Run{ID: runID, UserID: userID, Status: "success"},
		deliveries: []db.RunDelivery{
			{ID: uuid.New(), RunID: runID, TargetID: targetID, UserID: userID, TargetType: targetTypeWebhook, TargetURL: "https://example.com/hook", Status: "success", CreatedAt: now, UpdatedAt: now},
		},
	}
	txRunner := &fakeDeliveryTxRunner{q: queries}
	svc := &Service{queries: queries, txRunner: txRunner}

	created, err := svc.CreateTarget(context.Background(), userID, &CreateTargetRequest{
		Name:       "Webhook",
		Type:       targetTypeWebhook,
		URL:        "https://example.com/hook",
		EventTypes: []string{eventRunCompleted, eventRunCanceled},
		IsDefault:  true,
	})
	if err != nil {
		t.Fatalf("CreateTarget error = %v", err)
	}
	if created.ID != targetID.String() || created.Secret != "secret" || !queries.clearDefaultCalled {
		t.Fatalf("created target/clear default = %#v/%v", created, queries.clearDefaultCalled)
	}
	if txRunner.calls != 1 {
		t.Fatalf("default CreateTarget should use transaction, calls=%d", txRunner.calls)
	}
	if queries.createTargetArg.UserID != userID || queries.createTargetArg.Name != "Webhook" || queries.createTargetArg.Type != targetTypeWebhook || queries.createTargetArg.Secret == "" {
		t.Fatalf("create target arg = %#v", queries.createTargetArg)
	}
	var createdCfg deliveryTargetConfig
	if err := json.Unmarshal(queries.createTargetArg.Config, &createdCfg); err != nil || len(createdCfg.EventTypes) != 2 || createdCfg.EventTypes[1] != eventRunCanceled {
		t.Fatalf("create target config = %#v %v", createdCfg, err)
	}

	listed, err := svc.ListTargets(context.Background(), userID)
	if err != nil {
		t.Fatalf("ListTargets error = %v", err)
	}
	if len(listed) != 1 || listed[0].Secret != "" || listed[0].URL != "https://example.com/hook" {
		t.Fatalf("listed targets = %#v", listed)
	}

	if err := svc.DeleteTarget(context.Background(), targetID, userID); err != nil {
		t.Fatalf("DeleteTarget error = %v", err)
	}
	if queries.deleteArg.ID != targetID || queries.deleteArg.UserID != userID {
		t.Fatalf("delete arg = %#v", queries.deleteArg)
	}

	if err := svc.SetDefault(context.Background(), targetID, userID); err != nil {
		t.Fatalf("SetDefault error = %v", err)
	}
	if txRunner.calls != 2 {
		t.Fatalf("SetDefault should use transaction, calls=%d", txRunner.calls)
	}
	if queries.defaultArg.ID != targetID || queries.defaultArg.UserID != userID {
		t.Fatalf("default arg = %#v", queries.defaultArg)
	}

	updated, err := svc.UpdateTarget(context.Background(), targetID, userID, &UpdateTargetRequest{
		EventTypes: []string{eventRunFailed},
	})
	if err != nil {
		t.Fatalf("UpdateTarget error = %v", err)
	}
	if len(updated.EventTypes) != 1 || updated.EventTypes[0] != eventRunFailed {
		t.Fatalf("updated target = %#v", updated)
	}
	var updateCfg deliveryTargetConfig
	if err := json.Unmarshal(queries.updateTargetArg.Config, &updateCfg); err != nil || updateCfg.URL != "https://example.com/hook" || len(updateCfg.EventTypes) != 1 || updateCfg.EventTypes[0] != eventRunFailed {
		t.Fatalf("update target config = %#v %v", updateCfg, err)
	}

	history, err := svc.ListByRun(context.Background(), runID, userID)
	if err != nil {
		t.Fatalf("ListByRun error = %v", err)
	}
	if len(history) != 1 || history[0].RunID != runID.String() {
		t.Fatalf("history = %#v", history)
	}

	agentID := uuid.New()
	agentIDString := agentID.String()
	listedHistory, err := svc.List(context.Background(), userID, DeliveryListFilter{
		AgentID: &agentIDString,
		Status:  "success",
		Limit:   25,
	})
	if err != nil {
		t.Fatalf("List error = %v", err)
	}
	if len(listedHistory) != 1 || listedHistory[0].RunID != runID.String() {
		t.Fatalf("listed history = %#v", listedHistory)
	}
	if queries.listByUserArg.UserID != userID || !queries.listByUserArg.HasAgentID || queries.listByUserArg.AgentID != agentID || queries.listByUserArg.Status != "success" || queries.listByUserArg.Limit != 25 {
		t.Fatalf("list by user arg = %#v", queries.listByUserArg)
	}
}

func TestDeliveryServiceEnqueueAndAttemptDelivery(t *testing.T) {
	userID := uuid.New()
	runID := uuid.New()
	agentID := uuid.New()
	targetID := uuid.New()
	deliveryID := uuid.New()
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	run := db.Run{
		ID:        runID,
		UserID:    userID,
		AgentID:   agentID,
		Input:     []byte(`{"prompt":"hello"}`),
		Output:    []byte(`{"answer":"world"}`),
		Status:    "success",
		CostCents: 25,
		StartedAt: now,
	}
	target := db.DeliveryTarget{
		ID:     targetID,
		UserID: userID,
		Type:   targetTypeWebhook,
		Config: []byte(`{"url":"https://example.com/hook"}`),
		Secret: "secret",
	}
	queries := &fakeDeliveryQueries{
		agent: db.Agent{ID: agentID, Slug: "helper", Name: "Helper"},
		createDelivery: db.RunDelivery{
			ID:         deliveryID,
			RunID:      runID,
			TargetID:   targetID,
			UserID:     userID,
			TargetType: targetTypeWebhook,
			TargetURL:  "https://example.com/hook",
			Status:     "pending",
			CreatedAt:  now,
			UpdatedAt:  now,
		},
	}
	svc := &Service{queries: queries}
	enqueued, err := svc.enqueue(context.Background(), userID, &run, target, eventRunCompleted)
	if err != nil {
		t.Fatalf("enqueue error = %v", err)
	}
	if enqueued.ID != deliveryID || queries.createDeliveryArg.RunID != runID || queries.createDeliveryArg.TargetID != targetID {
		t.Fatalf("enqueue/create arg = %#v/%#v", enqueued, queries.createDeliveryArg)
	}
	var payload DeliveryPayload
	if err := json.Unmarshal(queries.createDeliveryArg.Payload, &payload); err != nil {
		t.Fatalf("payload decode: %v", err)
	}
	if payload.AgentSlug != "helper" || payload.Input["prompt"] != "hello" || payload.Output["answer"] != "world" {
		t.Fatalf("payload = %#v", payload)
	}

	secret := "webhook-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-OpenLinker-Signature") != "sha256="+signPayload([]byte(`{"event":"run.completed"}`), secret) {
			t.Fatalf("signature = %q", r.Header.Get("X-OpenLinker-Signature"))
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("accepted"))
	}))
	defer server.Close()

	successQueries := &fakeDeliveryQueries{
		runDeliveryRow: db.GetRunDeliveryRow{
			RunDelivery: db.RunDelivery{
				ID:         deliveryID,
				TargetType: targetTypeWebhook,
				TargetURL:  server.URL,
				Payload:    []byte(`{"event":"run.completed"}`),
				Status:     "pending",
			},
			TargetSecret: &secret,
		},
	}
	svc = &Service{queries: successQueries, httpClient: server.Client()}
	if err := svc.AttemptDelivery(context.Background(), deliveryID); err != nil {
		t.Fatalf("AttemptDelivery success error = %v", err)
	}
	if successQueries.successArg.ID != deliveryID || successQueries.successArg.ResponseStatus == nil || *successQueries.successArg.ResponseStatus != http.StatusAccepted {
		t.Fatalf("success arg = %#v", successQueries.successArg)
	}
	if successQueries.successArg.ResponseBody == nil || *successQueries.successArg.ResponseBody != "accepted" {
		t.Fatalf("success body = %#v", successQueries.successArg.ResponseBody)
	}

	failingServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("temporary"))
	}))
	defer failingServer.Close()
	retryQueries := &fakeDeliveryQueries{
		runDeliveryRow: db.GetRunDeliveryRow{
			RunDelivery: db.RunDelivery{
				ID:           deliveryID,
				TargetType:   targetTypeWebhook,
				TargetURL:    failingServer.URL,
				Payload:      []byte(`{"event":"run.completed"}`),
				Status:       "pending",
				AttemptCount: 0,
			},
			TargetSecret: &secret,
		},
	}
	svc = &Service{queries: retryQueries, httpClient: failingServer.Client()}
	if err := svc.AttemptDelivery(context.Background(), deliveryID); err != nil {
		t.Fatalf("AttemptDelivery retry error = %v", err)
	}
	if retryQueries.retryArg.ID != deliveryID || retryQueries.retryArg.ResponseStatus == nil || *retryQueries.retryArg.ResponseStatus != http.StatusInternalServerError {
		t.Fatalf("retry arg = %#v", retryQueries.retryArg)
	}
	if retryQueries.retryArg.ErrorMessage == nil || *retryQueries.retryArg.ErrorMessage != "HTTP 500" {
		t.Fatalf("retry error message = %#v", retryQueries.retryArg.ErrorMessage)
	}
	if retryQueries.retryArg.NextRetryAt.IsZero() {
		t.Fatalf("retry next_retry_at was not set")
	}

	deletedTargetQueries := &fakeDeliveryQueries{
		runDeliveryRow: db.GetRunDeliveryRow{
			RunDelivery: db.RunDelivery{ID: deliveryID, Status: "pending"},
		},
	}
	svc = &Service{queries: deletedTargetQueries, httpClient: http.DefaultClient}
	if err := svc.AttemptDelivery(context.Background(), deliveryID); err != nil {
		t.Fatalf("AttemptDelivery deleted target error = %v", err)
	}
	if deletedTargetQueries.finalArg.ErrorMessage == nil || *deletedTargetQueries.finalArg.ErrorMessage != "投递目标已被删除" {
		t.Fatalf("deleted target final arg = %#v", deletedTargetQueries.finalArg)
	}
}

func TestDeliveryServiceDeliverRunSelectsExplicitAndDefaultTargets(t *testing.T) {
	userID := uuid.New()
	runID := uuid.New()
	agentID := uuid.New()
	explicitTargetID := uuid.New()
	defaultTargetID := uuid.New()
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	run := db.Run{
		ID:        runID,
		UserID:    userID,
		AgentID:   agentID,
		Status:    "success",
		StartedAt: now,
	}
	agent := db.Agent{ID: agentID, Slug: "delivery-agent", Name: "Delivery Agent"}

	explicitQueries := &fakeDeliveryQueries{
		run:   run,
		agent: agent,
		target: db.DeliveryTarget{
			ID:     explicitTargetID,
			UserID: userID,
			Type:   targetTypeWebhook,
			Config: []byte(`{"url":"https://example.com/explicit"}`),
			Secret: "secret",
		},
		createDelivery: db.RunDelivery{
			ID:         uuid.New(),
			RunID:      runID,
			TargetID:   explicitTargetID,
			UserID:     userID,
			TargetType: targetTypeWebhook,
			TargetURL:  "https://example.com/explicit",
			Status:     "pending",
			CreatedAt:  now,
			UpdatedAt:  now,
		},
	}
	explicitItem, err := (&Service{queries: explicitQueries}).DeliverRun(context.Background(), userID, runID, &explicitTargetID)
	if err != nil {
		t.Fatalf("DeliverRun explicit target error = %v", err)
	}
	if explicitItem.TargetID != explicitTargetID.String() || explicitQueries.createDeliveryArg.TargetURL != "https://example.com/explicit" {
		t.Fatalf("explicit delivery item/arg = %#v/%#v", explicitItem, explicitQueries.createDeliveryArg)
	}

	defaultQueries := &fakeDeliveryQueries{
		run:   run,
		agent: agent,
		defaultTarget: db.DeliveryTarget{
			ID:     defaultTargetID,
			UserID: userID,
			Type:   targetTypeSlack,
			Config: []byte(`{"url":"https://hooks.slack.com/services/T000/B000/secret"}`),
			Secret: "unused",
		},
		createDelivery: db.RunDelivery{
			ID:         uuid.New(),
			RunID:      runID,
			TargetID:   defaultTargetID,
			UserID:     userID,
			TargetType: targetTypeSlack,
			TargetURL:  "https://hooks.slack.com/services/T000/B000/secret",
			Status:     "pending",
			CreatedAt:  now,
			UpdatedAt:  now,
		},
	}
	defaultItem, err := (&Service{queries: defaultQueries}).DeliverRun(context.Background(), userID, runID, nil)
	if err != nil {
		t.Fatalf("DeliverRun default target error = %v", err)
	}
	if defaultItem.TargetID != defaultTargetID.String() || defaultQueries.createDeliveryArg.TargetType != targetTypeSlack {
		t.Fatalf("default delivery item/arg = %#v/%#v", defaultItem, defaultQueries.createDeliveryArg)
	}

	wrongOwner := &fakeDeliveryQueries{
		run: run,
		target: db.DeliveryTarget{
			ID:     explicitTargetID,
			UserID: uuid.New(),
			Config: []byte(`{"url":"https://example.com/foreign"}`),
		},
	}
	_, err = (&Service{queries: wrongOwner}).DeliverRun(context.Background(), userID, runID, &explicitTargetID)
	requireDeliveryHTTPStatus(t, err, http.StatusNotFound)
}

func TestDeliveryServiceAutoEnqueueRetryAndProcessPending(t *testing.T) {
	userID := uuid.New()
	runID := uuid.New()
	agentID := uuid.New()
	targetID := uuid.New()
	deliveryID := uuid.New()
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	run := &db.Run{
		ID:        runID,
		UserID:    userID,
		AgentID:   agentID,
		Status:    "success",
		StartedAt: now,
	}
	target := db.DeliveryTarget{
		ID:     targetID,
		UserID: userID,
		Type:   targetTypeWebhook,
		Config: []byte(`{"url":"https://example.com/default"}`),
		Secret: "secret",
	}
	agent := db.Agent{ID: agentID, Slug: "delivery-agent", Name: "Delivery Agent"}

	noDefault := &fakeDeliveryQueries{defaultGetErr: pgx.ErrNoRows}
	if err := (&Service{queries: noDefault}).EnqueueIfDefault(context.Background(), run); err != nil {
		t.Fatalf("EnqueueIfDefault without default should skip: %v", err)
	}
	if noDefault.createDeliveryArg.RunID != uuid.Nil {
		t.Fatalf("EnqueueIfDefault without default created delivery = %#v", noDefault.createDeliveryArg)
	}

	autoQueries := &fakeDeliveryQueries{
		defaultTarget: target,
		agent:         agent,
		createDelivery: db.RunDelivery{
			ID:         deliveryID,
			RunID:      runID,
			TargetID:   targetID,
			UserID:     userID,
			TargetType: targetTypeWebhook,
			TargetURL:  "https://example.com/default",
			Status:     "pending",
			CreatedAt:  now,
			UpdatedAt:  now,
		},
	}
	if err := (&Service{queries: autoQueries}).EnqueueIfDefault(context.Background(), run); err != nil {
		t.Fatalf("EnqueueIfDefault error = %v", err)
	}
	if autoQueries.createDeliveryArg.RunID != runID || autoQueries.createDeliveryArg.TargetID != targetID {
		t.Fatalf("EnqueueIfDefault create arg = %#v", autoQueries.createDeliveryArg)
	}

	filteredRun := *run
	filteredRun.Status = "failed"
	filteredQueries := &fakeDeliveryQueries{
		defaultTarget: db.DeliveryTarget{
			ID:     targetID,
			UserID: userID,
			Type:   targetTypeWebhook,
			Config: []byte(`{"url":"https://example.com/default","event_types":["run.completed"]}`),
			Secret: "secret",
		},
		agent: agent,
	}
	if err := (&Service{queries: filteredQueries}).EnqueueIfDefault(context.Background(), &filteredRun); err != nil {
		t.Fatalf("EnqueueIfDefault filtered event error = %v", err)
	}
	if filteredQueries.createDeliveryArg.RunID != uuid.Nil {
		t.Fatalf("filtered event should not enqueue delivery = %#v", filteredQueries.createDeliveryArg)
	}

	retryQueries := &fakeDeliveryQueries{resetRows: 1}
	if err := (&Service{queries: retryQueries}).RetryDelivery(context.Background(), deliveryID, userID); err != nil {
		t.Fatalf("RetryDelivery success error = %v", err)
	}
	if retryQueries.resetArg.ID != deliveryID || retryQueries.resetArg.UserID != userID {
		t.Fatalf("RetryDelivery reset arg = %#v", retryQueries.resetArg)
	}

	secret := "webhook-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("processed"))
	}))
	defer server.Close()
	pendingQueries := &fakeDeliveryQueries{
		pending: []db.RunDelivery{{ID: deliveryID}},
		runDeliveryRow: db.GetRunDeliveryRow{
			RunDelivery: db.RunDelivery{
				ID:         deliveryID,
				TargetType: targetTypeWebhook,
				TargetURL:  server.URL,
				Payload:    []byte(`{"event":"run.completed"}`),
				Status:     "pending",
			},
			TargetSecret: &secret,
		},
	}
	(&Service{queries: pendingQueries, httpClient: server.Client()}).processPending(context.Background())
	if pendingQueries.successArg.ID != deliveryID {
		t.Fatalf("processPending success arg = %#v", pendingQueries.successArg)
	}
}

func TestDeliveryServiceErrors(t *testing.T) {
	userID := uuid.New()
	runID := uuid.New()
	targetID := uuid.New()
	deliveryID := uuid.New()
	sentinel := errors.New("database stopped")

	for _, tc := range []struct {
		name string
		call func(*Service) error
		q    *fakeDeliveryQueries
		want int
	}{
		{
			name: "create target list error",
			call: func(s *Service) error {
				_, err := s.CreateTarget(context.Background(), userID, &CreateTargetRequest{Name: "Webhook", Type: targetTypeWebhook, URL: "https://example.com/hook"})
				return err
			},
			q:    &fakeDeliveryQueries{listTargetsErr: sentinel},
			want: http.StatusInternalServerError,
		},
		{
			name: "create target limit",
			call: func(s *Service) error {
				_, err := s.CreateTarget(context.Background(), userID, &CreateTargetRequest{Name: "Webhook", Type: targetTypeWebhook, URL: "https://example.com/hook"})
				return err
			},
			q:    &fakeDeliveryQueries{targets: make([]db.DeliveryTarget, maxTargetsPerUser)},
			want: http.StatusBadRequest,
		},
		{
			name: "delete missing target",
			call: func(s *Service) error {
				return s.DeleteTarget(context.Background(), targetID, userID)
			},
			q:    &fakeDeliveryQueries{},
			want: http.StatusNotFound,
		},
		{
			name: "default update missing target",
			call: func(s *Service) error {
				return s.SetDefault(context.Background(), targetID, userID)
			},
			q:    &fakeDeliveryQueries{},
			want: http.StatusNotFound,
		},
		{
			name: "deliver missing run",
			call: func(s *Service) error {
				_, err := s.DeliverRun(context.Background(), userID, runID, nil)
				return err
			},
			q:    &fakeDeliveryQueries{runErr: pgx.ErrNoRows},
			want: http.StatusNotFound,
		},
		{
			name: "deliver running run",
			call: func(s *Service) error {
				_, err := s.DeliverRun(context.Background(), userID, runID, nil)
				return err
			},
			q:    &fakeDeliveryQueries{run: db.Run{ID: runID, UserID: userID, Status: "running"}},
			want: http.StatusBadRequest,
		},
		{
			name: "deliver default target missing",
			call: func(s *Service) error {
				_, err := s.DeliverRun(context.Background(), userID, runID, nil)
				return err
			},
			q:    &fakeDeliveryQueries{run: db.Run{ID: runID, UserID: userID, Status: "success"}, defaultErr: pgx.ErrNoRows},
			want: http.StatusBadRequest,
		},
		{
			name: "list by run wrong owner",
			call: func(s *Service) error {
				_, err := s.ListByRun(context.Background(), runID, userID)
				return err
			},
			q:    &fakeDeliveryQueries{run: db.Run{ID: runID, UserID: uuid.New(), Status: "success"}},
			want: http.StatusNotFound,
		},
		{
			name: "retry missing delivery",
			call: func(s *Service) error {
				return s.RetryDelivery(context.Background(), deliveryID, userID)
			},
			q:    &fakeDeliveryQueries{},
			want: http.StatusNotFound,
		},
		{
			name: "enqueue missing agent",
			call: func(s *Service) error {
				_, err := s.enqueue(context.Background(), userID, &db.Run{ID: runID, AgentID: uuid.New()}, db.DeliveryTarget{ID: targetID, Config: []byte(`{"url":"https://example.com/hook"}`)}, eventRunCompleted)
				return err
			},
			q:    &fakeDeliveryQueries{agentErr: sentinel},
			want: http.StatusInternalServerError,
		},
		{
			name: "enqueue empty target url",
			call: func(s *Service) error {
				_, err := s.enqueue(context.Background(), userID, &db.Run{ID: runID, AgentID: uuid.New()}, db.DeliveryTarget{ID: targetID, Config: []byte(`{}`)}, eventRunCompleted)
				return err
			},
			q:    &fakeDeliveryQueries{agent: db.Agent{ID: uuid.New(), Slug: "helper", Name: "Helper"}},
			want: http.StatusBadRequest,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requireDeliveryHTTPStatus(t, tc.call(&Service{queries: tc.q, httpClient: http.DefaultClient}), tc.want)
		})
	}
}

type fakeDeliveryQueries struct {
	targets        []db.DeliveryTarget
	listTargetsErr error

	clearDefaultCalled bool
	clearDefaultErr    error

	createTargetArg db.CreateDeliveryTargetParams
	createTarget    db.DeliveryTarget
	createTargetErr error

	deleteArg  db.DeleteDeliveryTargetParams
	deleteRows int64
	deleteErr  error

	defaultArg  db.SetDeliveryTargetDefaultParams
	defaultRows int64
	defaultErr  error

	updateTargetArg db.UpdateDeliveryTargetConfigParams
	updateTarget    db.DeliveryTarget
	updateTargetErr error

	run    db.Run
	runErr error

	target    db.DeliveryTarget
	targetErr error

	defaultTarget db.DeliveryTarget
	defaultGetErr error

	agent    db.Agent
	agentErr error

	createDeliveryArg db.CreateRunDeliveryParams
	createDelivery    db.RunDelivery
	createDeliveryErr error

	deliveries    []db.RunDelivery
	deliveriesErr error
	listByUserArg db.ListRunDeliveriesByUserParams

	resetArg  db.ResetRunDeliveryForRetryParams
	resetRows int64
	resetErr  error

	runDeliveryRow db.GetRunDeliveryRow
	runDeliveryErr error

	successArg db.MarkRunDeliverySuccessParams
	successErr error

	retryArg db.MarkRunDeliveryFailedRetryParams
	retryErr error

	finalArg db.MarkRunDeliveryFailedFinalParams
	finalErr error

	pending    []db.RunDelivery
	pendingErr error
}

type fakeDeliveryTxRunner struct {
	q     deliveryQueries
	calls int
	err   error
}

func (r *fakeDeliveryTxRunner) runInTx(ctx context.Context, fn func(deliveryQueries) error) error {
	r.calls++
	if r.err != nil {
		return r.err
	}
	return fn(r.q)
}

func (q *fakeDeliveryQueries) ListDeliveryTargetsByUser(context.Context, uuid.UUID) ([]db.DeliveryTarget, error) {
	return q.targets, q.listTargetsErr
}

func (q *fakeDeliveryQueries) ClearDefaultDeliveryTarget(context.Context, uuid.UUID) error {
	q.clearDefaultCalled = true
	return q.clearDefaultErr
}

func (q *fakeDeliveryQueries) CreateDeliveryTarget(_ context.Context, arg db.CreateDeliveryTargetParams) (db.DeliveryTarget, error) {
	q.createTargetArg = arg
	if q.createTarget.ID == uuid.Nil {
		q.createTarget = db.DeliveryTarget{
			ID:        uuid.New(),
			UserID:    arg.UserID,
			Name:      arg.Name,
			Type:      arg.Type,
			Config:    arg.Config,
			Secret:    arg.Secret,
			IsDefault: arg.IsDefault,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
	}
	return q.createTarget, q.createTargetErr
}

func (q *fakeDeliveryQueries) DeleteDeliveryTarget(_ context.Context, arg db.DeleteDeliveryTargetParams) (int64, error) {
	q.deleteArg = arg
	return q.deleteRows, q.deleteErr
}

func (q *fakeDeliveryQueries) SetDeliveryTargetDefault(_ context.Context, arg db.SetDeliveryTargetDefaultParams) (int64, error) {
	q.defaultArg = arg
	return q.defaultRows, q.defaultErr
}

func (q *fakeDeliveryQueries) UpdateDeliveryTargetConfig(_ context.Context, arg db.UpdateDeliveryTargetConfigParams) (db.DeliveryTarget, error) {
	q.updateTargetArg = arg
	if q.updateTarget.ID == uuid.Nil {
		q.updateTarget = db.DeliveryTarget{
			ID:        arg.ID,
			UserID:    arg.UserID,
			Name:      "Webhook",
			Type:      targetTypeWebhook,
			Config:    arg.Config,
			Secret:    "secret",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
	}
	q.updateTarget.Config = arg.Config
	return q.updateTarget, q.updateTargetErr
}

func (q *fakeDeliveryQueries) GetRunByID(context.Context, uuid.UUID) (db.Run, error) {
	return q.run, q.runErr
}

func (q *fakeDeliveryQueries) GetDeliveryTargetByID(context.Context, uuid.UUID) (db.DeliveryTarget, error) {
	return q.target, q.targetErr
}

func (q *fakeDeliveryQueries) GetDefaultDeliveryTarget(context.Context, uuid.UUID) (db.DeliveryTarget, error) {
	if q.defaultGetErr != nil {
		return db.DeliveryTarget{}, q.defaultGetErr
	}
	return q.defaultTarget, q.defaultErr
}

func (q *fakeDeliveryQueries) GetAgentByID(context.Context, uuid.UUID) (db.Agent, error) {
	return q.agent, q.agentErr
}

func (q *fakeDeliveryQueries) CreateRunDelivery(_ context.Context, arg db.CreateRunDeliveryParams) (db.RunDelivery, error) {
	q.createDeliveryArg = arg
	if q.createDelivery.ID == uuid.Nil {
		q.createDelivery = db.RunDelivery{
			ID:         uuid.New(),
			RunID:      arg.RunID,
			TargetID:   arg.TargetID,
			UserID:     arg.UserID,
			TargetType: arg.TargetType,
			TargetURL:  arg.TargetURL,
			Payload:    arg.Payload,
			Status:     "pending",
			CreatedAt:  time.Now(),
			UpdatedAt:  time.Now(),
		}
	}
	return q.createDelivery, q.createDeliveryErr
}

func (q *fakeDeliveryQueries) ListRunDeliveriesByRun(context.Context, uuid.UUID) ([]db.RunDelivery, error) {
	return q.deliveries, q.deliveriesErr
}

func (q *fakeDeliveryQueries) ListRunDeliveriesByUser(_ context.Context, arg db.ListRunDeliveriesByUserParams) ([]db.RunDelivery, error) {
	q.listByUserArg = arg
	return q.deliveries, q.deliveriesErr
}

func (q *fakeDeliveryQueries) ResetRunDeliveryForRetry(_ context.Context, arg db.ResetRunDeliveryForRetryParams) (int64, error) {
	q.resetArg = arg
	return q.resetRows, q.resetErr
}

func (q *fakeDeliveryQueries) GetRunDeliveryByID(context.Context, uuid.UUID) (db.GetRunDeliveryRow, error) {
	return q.runDeliveryRow, q.runDeliveryErr
}

func (q *fakeDeliveryQueries) MarkRunDeliverySuccess(_ context.Context, arg db.MarkRunDeliverySuccessParams) error {
	q.successArg = arg
	return q.successErr
}

func (q *fakeDeliveryQueries) MarkRunDeliveryFailedRetry(_ context.Context, arg db.MarkRunDeliveryFailedRetryParams) error {
	q.retryArg = arg
	return q.retryErr
}

func (q *fakeDeliveryQueries) MarkRunDeliveryFailedFinal(_ context.Context, arg db.MarkRunDeliveryFailedFinalParams) error {
	q.finalArg = arg
	return q.finalErr
}

func (q *fakeDeliveryQueries) ListPendingRunDeliveries(context.Context) ([]db.RunDelivery, error) {
	return q.pending, q.pendingErr
}
