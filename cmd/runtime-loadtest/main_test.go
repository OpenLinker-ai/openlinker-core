package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestAPIClientDoWithHeadersSkipsContextCancellationMetric(t *testing.T) {
	m := &metrics{}
	api := &apiClient{
		root: "http://127.0.0.1:1",
		client: &http.Client{
			Timeout: time.Second,
		},
	}
	ctx, cancel := context.WithCancel(withMetrics(context.Background(), m))
	cancel()

	if _, _, err := api.doWithHeaders(ctx, "runtime-claim", http.MethodGet, "/agent-runtime/runs/claim", nil, "", nil); err == nil {
		t.Fatal("expected context cancellation error")
	}
	if got := len(m.httpOps); got != 0 {
		t.Fatalf("http metric count = %d, want 0", got)
	}
}

func TestWebSocketDialErrorIncludesResponseDetails(t *testing.T) {
	err := websocketDialError(errors.New("websocket: bad handshake"), &http.Response{
		Status: "429 Too Many Requests",
		Header: http.Header{
			"Content-Type": []string{"application/json"},
			"Retry-After":  []string{"5"},
		},
		Body: io.NopCloser(strings.NewReader(`{"code":"RATE_LIMITED","message":"slow down"}`)),
	})
	got := err.Error()
	for _, want := range []string{
		"websocket: bad handshake",
		"status=429 Too Many Requests",
		"retry_after=5",
		"content_type=application/json",
		`body={"code":"RATE_LIMITED","message":"slow down"}`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("websocketDialError = %q, missing %q", got, want)
		}
	}
}

func TestEnsureAccountUsesVerificationCodeAndLoginAfterRegister(t *testing.T) {
	var loginCalls int
	var sawVerification bool
	var sawRegister bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth/login":
			loginCalls++
			if loginCalls == 1 {
				http.Error(w, `{"error":{"code":"UNAUTHORIZED"}}`, http.StatusUnauthorized)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"email": "perf@example.local",
				"jwt":   "jwt-token",
				"user":  map[string]any{"id": "user-1"},
			})
		case "/auth/verification-code":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode verification request: %v", err)
			}
			if body["email"] != "perf@example.local" || body["purpose"] != "register" {
				t.Fatalf("verification body = %#v", body)
			}
			sawVerification = true
			_ = json.NewEncoder(w).Encode(map[string]any{"debug_code": "123456"})
		case "/auth/register":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode register request: %v", err)
			}
			if body["password_confirm"] != "password-123" || body["verification_code"] != "123456" {
				t.Fatalf("register body = %#v", body)
			}
			sawRegister = true
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"user_id":      "user-1",
				"email":        "perf@example.local",
				"display_name": "Perf User",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	got, err := ensureAccount(context.Background(), &apiClient{
		root:   server.URL,
		client: server.Client(),
	}, "perf@example.local", "password-123", "Perf User", &metrics{})
	if err != nil {
		t.Fatalf("ensureAccount error = %v", err)
	}
	if got.JWT != "jwt-token" {
		t.Fatalf("jwt = %q, want jwt-token", got.JWT)
	}
	if !sawVerification || !sawRegister {
		t.Fatalf("saw verification=%v register=%v", sawVerification, sawRegister)
	}
	if loginCalls != 2 {
		t.Fatalf("login calls = %d, want 2", loginCalls)
	}
}

func TestWaitForWSHeartbeatAcksReturnsWhenCountsMatch(t *testing.T) {
	m := &metrics{}
	m.c.wsHeartbeats.Store(2)
	go func() {
		time.Sleep(20 * time.Millisecond)
		m.c.wsHeartbeatAcks.Store(2)
	}()

	if err := waitForWSHeartbeatAcks(context.Background(), m, time.Second); err != nil {
		t.Fatalf("waitForWSHeartbeatAcks error = %v", err)
	}
}

func TestWaitForWSHeartbeatAcksTimesOutWhenCountsMismatch(t *testing.T) {
	m := &metrics{}
	m.c.wsHeartbeats.Store(2)
	m.c.wsHeartbeatAcks.Store(1)

	if err := waitForWSHeartbeatAcks(context.Background(), m, 20*time.Millisecond); err == nil {
		t.Fatal("expected heartbeat ack wait timeout")
	}
}

func TestEffectivePullClaimWaitLeavesRequestTimeoutBuffer(t *testing.T) {
	got := effectivePullClaimWait(config{
		PullClaimWait:  19 * time.Second,
		RequestTimeout: 20 * time.Second,
	})
	if got != 5*time.Second {
		t.Fatalf("effectivePullClaimWait = %s, want 5s", got)
	}
}

func TestConfigValidateDerivesSetupConcurrency(t *testing.T) {
	cfg := config{
		APIRoot:          "http://127.0.0.1:8080/api/v1",
		Mode:             "runtime_ws",
		Users:            1,
		Agents:           1,
		WorkersPerAgent:  1,
		Runs:             1,
		RunConcurrency:   1,
		SetupConcurrency: 16,
		Timeout:          time.Second,
		RequestTimeout:   time.Second,
	}

	if err := cfg.validate(); err != nil {
		t.Fatalf("validate error = %v", err)
	}
	if cfg.SetupUserConcurrency != 8 {
		t.Fatalf("setup user concurrency = %d, want 8", cfg.SetupUserConcurrency)
	}
	if cfg.SetupAgentConcurrency != 16 {
		t.Fatalf("setup agent concurrency = %d, want 16", cfg.SetupAgentConcurrency)
	}
}

func TestRunSetupJobsProcessesAllIndexes(t *testing.T) {
	var seen atomic.Int32
	err := runSetupJobs(context.Background(), 4, 11, func(ctx context.Context, index int) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		seen.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("runSetupJobs error = %v", err)
	}
	if got := seen.Load(); got != 11 {
		t.Fatalf("processed jobs = %d, want 11", got)
	}
}

func TestRunSetupJobsCancelsOnFirstError(t *testing.T) {
	want := errors.New("setup failed")
	err := runSetupJobs(context.Background(), 3, 20, func(ctx context.Context, index int) error {
		if index == 3 {
			return want
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	})
	if !errors.Is(err, want) {
		t.Fatalf("runSetupJobs error = %v, want %v", err, want)
	}
}

func TestSubmitRunsDoesNotDeadlockWhenErrorsExceedConcurrency(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, `{"error":"create failed"}`, http.StatusInternalServerError)
	}))
	defer server.Close()

	creator := &account{
		Email: "perf@example.local",
		JWT:   "jwt-token",
	}
	creator.User.ID = "user-1"
	done := make(chan error, 1)
	go func() {
		done <- submitRuns(
			context.Background(),
			&apiClient{root: server.URL, client: server.Client()},
			config{},
			[]agentRef{{ID: "agent-1", Creator: creator}},
			newRunTracker(),
			&metrics{},
			"measured",
			8,
			2,
		)
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("submitRuns error = nil, want create-run error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("submitRuns deadlocked after create-run errors exceeded concurrency")
	}
	if got := requests.Load(); got != 8 {
		t.Fatalf("requests = %d, want 8", got)
	}
}

func TestMeasuredTimelineBucketsMeasuredPhaseRuns(t *testing.T) {
	start := time.Date(2026, 7, 5, 8, 0, 0, 0, time.UTC)
	records := []*runRecord{
		{
			ClientID:    "run-1",
			Phase:       "measured",
			SubmittedAt: start.Add(100 * time.Millisecond),
			CreatedAt:   start.Add(150 * time.Millisecond),
			AssignedAt:  start.Add(300 * time.Millisecond),
			CompletedAt: start.Add(900 * time.Millisecond),
		},
		{
			ClientID:    "run-2",
			Phase:       "measured",
			SubmittedAt: start.Add(1100 * time.Millisecond),
			CreatedAt:   start.Add(1200 * time.Millisecond),
			AssignedAt:  start.Add(1500 * time.Millisecond),
			CompletedAt: start.Add(1900 * time.Millisecond),
		},
		{
			ClientID:    "history-1",
			Phase:       "history",
			SubmittedAt: start.Add(100 * time.Millisecond),
			CreatedAt:   start.Add(150 * time.Millisecond),
			AssignedAt:  start.Add(300 * time.Millisecond),
			CompletedAt: start.Add(900 * time.Millisecond),
		},
	}

	timeline := measuredTimeline(records[:2], start, start.Add(2*time.Second))
	if got := timeline["bucket_ms"]; got != 1000.0 {
		t.Fatalf("bucket_ms = %v, want 1000", got)
	}
	buckets, ok := timeline["buckets"].([]map[string]any)
	if !ok {
		t.Fatalf("buckets type = %T", timeline["buckets"])
	}
	if len(buckets) != 2 {
		t.Fatalf("bucket count = %d, want 2", len(buckets))
	}
	if got := buckets[0]["submitted"]; got != 1 {
		t.Fatalf("bucket0 submitted = %v, want 1", got)
	}
	if got := buckets[0]["completed"]; got != 1 {
		t.Fatalf("bucket0 completed = %v, want 1", got)
	}
	if got := buckets[1]["submitted"]; got != 1 {
		t.Fatalf("bucket1 submitted = %v, want 1", got)
	}
	if got := buckets[1]["completed"]; got != 1 {
		t.Fatalf("bucket1 completed = %v, want 1", got)
	}
	stats, ok := buckets[0]["completion_ms"].(map[string]any)
	if !ok {
		t.Fatalf("completion_ms type = %T", buckets[0]["completion_ms"])
	}
	if got := stats["p95"]; got != 800.0 {
		t.Fatalf("bucket0 completion p95 = %v, want 800", got)
	}
	if got := buckets[0]["complete_rps"]; got != 1.0 {
		t.Fatalf("bucket0 complete_rps = %v, want 1", got)
	}
}

func TestTimelineBucketSizeCapsLongRuns(t *testing.T) {
	size := timelineBucketSize(2 * time.Hour)
	if size < time.Second {
		t.Fatalf("bucket size = %s, want >= 1s", size)
	}
	if buckets := int((2*time.Hour + size - 1) / size); buckets > 240 {
		t.Fatalf("bucket count = %d, want <= 240", buckets)
	}
}

func TestMeasuredTimelineCountsCreateFailuresWithoutCreatedAt(t *testing.T) {
	start := time.Date(2026, 7, 5, 8, 0, 0, 0, time.UTC)
	timeline := measuredTimeline([]*runRecord{
		{
			ClientID:    "run-1",
			Phase:       "measured",
			SubmittedAt: start.Add(100 * time.Millisecond),
			CreateErr:   "create timeout",
		},
	}, start, start.Add(time.Second))

	buckets := timeline["buckets"].([]map[string]any)
	if got := buckets[0]["failed"]; got != 1 {
		t.Fatalf("bucket failed = %v, want 1", got)
	}
}

func TestWSReadyReportBuildsLatencyStatsAndTimeline(t *testing.T) {
	start := time.Date(2026, 7, 5, 8, 0, 0, 0, time.UTC)
	m := &metrics{startedAt: start}
	m.startWSReadyWindow(start)
	m.recordWSReady(start.Add(100 * time.Millisecond))
	m.recordWSReady(start.Add(1500 * time.Millisecond))

	durations, timeline := wsReadyReport(m)
	stats := summaryStats(durations)
	if got := stats["count"]; got != 2 {
		t.Fatalf("ready count = %v, want 2", got)
	}
	if got := stats["max"]; got != 1500.0 {
		t.Fatalf("ready max = %v, want 1500", got)
	}
	buckets := timeline["buckets"].([]map[string]any)
	if len(buckets) != 2 {
		t.Fatalf("ready bucket count = %d, want 2", len(buckets))
	}
	if got := buckets[1]["cumulative"]; got != 2 {
		t.Fatalf("ready final cumulative = %v, want 2", got)
	}
}
