package delivery

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	db "github.com/kinzhi/openlinker-core/pkg/db/generated"
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

	got := buildPayload(run, "shipper", "Shipping Agent")
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

	got := buildPayload(run, "agent", "Agent")
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
		Config:    []byte(`{"url":"https://example.com/hook"}`),
		Secret:    "delivery-secret",
		IsDefault: true,
		CreatedAt: now,
	}
	targetResp := toTargetResponse(target, true)
	if targetResp.ID != targetID.String() || targetResp.URL != "https://example.com/hook" || targetResp.Secret != "delivery-secret" {
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
	status, body, err := svc.deliverWebhook(context.Background(), server.URL, secret, deliveryID, payload)
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
