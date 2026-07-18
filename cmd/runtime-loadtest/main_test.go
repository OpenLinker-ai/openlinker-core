package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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

	if _, _, err := api.doWithHeaders(ctx, "runtime-v2-claim", http.MethodPost, "/agent-runtime/runs/claim", nil, "", nil); err == nil {
		t.Fatal("expected context cancellation error")
	}
	if got := len(m.httpOps); got != 0 {
		t.Fatalf("http metric count = %d, want 0", got)
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

func TestSetupAccountsUsesAuthAndCoreAPIRoots(t *testing.T) {
	var authCalls atomic.Int32
	var coreCalls atomic.Int32
	authServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/auth/login" {
			http.NotFound(w, r)
			return
		}
		authCalls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"email": "openlinker-perf-routing-u000@example.local",
			"jwt":   "jwt-token",
			"user":  map[string]any{"id": "user-1"},
		})
	}))
	defer authServer.Close()
	coreServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/me/become-creator" {
			http.NotFound(w, r)
			return
		}
		if got := r.Header.Get("Authorization"); got != "Bearer jwt-token" {
			t.Errorf("authorization = %q", got)
		}
		coreCalls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer coreServer.Close()

	cfg := config{Users: 1, SetupUserConcurrency: 1}
	accounts, err := setupAccounts(
		context.Background(),
		&apiClient{root: authServer.URL, client: authServer.Client()},
		&apiClient{root: coreServer.URL, client: coreServer.Client()},
		cfg,
		"routing",
		&metrics{},
	)
	if err != nil {
		t.Fatalf("setupAccounts error = %v", err)
	}
	if len(accounts) != 1 || accounts[0].JWT != "jwt-token" {
		t.Fatalf("accounts = %#v", accounts)
	}
	if got := authCalls.Load(); got != 1 {
		t.Fatalf("auth calls = %d, want 1", got)
	}
	if got := coreCalls.Load(); got != 1 {
		t.Fatalf("core calls = %d, want 1", got)
	}
}

func TestSetupAgentsReusesRedeemedTokenWithinWorkerLimit(t *testing.T) {
	for _, workers := range []int{1, 10} {
		t.Run(fmt.Sprintf("workers_%d", workers), func(t *testing.T) {
			var tokenCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/creator/agent-tokens":
					call := tokenCalls.Add(1)
					_ = json.NewEncoder(w).Encode(map[string]any{
						"plaintext_token": fmt.Sprintf("runtime-token-%02d", call),
					})
				case "/agent-registration/agents":
					if got := r.Header.Get("Authorization"); got != "Bearer runtime-token-01" {
						t.Errorf("registration authorization = %q", got)
					}
					w.WriteHeader(http.StatusCreated)
					_ = json.NewEncoder(w).Encode(map[string]any{
						"agent": map[string]any{"id": "agent-1", "slug": "perf-agent-1"},
					})
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()

			creator := account{JWT: "creator-jwt"}
			creator.User.ID = "user-1"
			agents, err := setupAgents(
				context.Background(),
				&apiClient{root: server.URL, client: server.Client()},
				config{Agents: 1, WorkersPerAgent: workers, SetupAgentConcurrency: 1},
				"token-count",
				[]account{creator},
				&metrics{},
			)
			if err != nil {
				t.Fatalf("setupAgents error = %v", err)
			}
			if got := tokenCalls.Load(); got != int32(workers) {
				t.Fatalf("token calls = %d, want %d", got, workers)
			}
			if got := len(agents[0].RuntimeKeys); got != workers {
				t.Fatalf("runtime key count = %d, want %d", got, workers)
			}
			if got := agents[0].RuntimeKeys[0]; got != "runtime-token-01" {
				t.Fatalf("first runtime key = %q, want redeemed registration token", got)
			}
		})
	}
}

func TestConfigValidateDerivesSetupConcurrency(t *testing.T) {
	directory := t.TempDir()
	cert := filepath.Join(directory, "node.crt")
	key := filepath.Join(directory, "node.key")
	ca := filepath.Join(directory, "server-ca.crt")
	for _, path := range []string{cert, key, ca} {
		if err := os.WriteFile(path, []byte("test"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	cfg := config{
		APIRoot: "http://127.0.0.1:8080/api/v1", RuntimeURL: "https://runtime.example.test",
		Transport: transportAuto, Scenarios: []string{"baseline"},
		NodeID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", NodeVersion: runtimeLoadtestNodeVersion,
		MTLSCertFile: cert, MTLSKeyFile: key, MTLSCAFile: ca, StateDir: directory,
		Users: 1, Agents: 1, WorkersPerAgent: 1, Runs: 1,
		RunConcurrency: 1, SetupConcurrency: 16,
		Timeout: time.Second, RequestTimeout: time.Second, ReadyTimeout: time.Second,
		PullWait: time.Second, CommandWait: time.Second,
		HeartbeatInterval: time.Second, WSProbeInterval: time.Second,
		CancelConcurrency: 1,
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
	if cfg.AuthAPIRoot != cfg.APIRoot {
		t.Fatalf("auth API root = %q, want Core API root %q", cfg.AuthAPIRoot, cfg.APIRoot)
	}
}

func TestConfigValidateDefaultsSlowConnectionCapacityProfile(t *testing.T) {
	directory := t.TempDir()
	cert := filepath.Join(directory, "node.crt")
	key := filepath.Join(directory, "node.key")
	ca := filepath.Join(directory, "server-ca.crt")
	for _, path := range []string{cert, key, ca} {
		if err := os.WriteFile(path, []byte("test"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	cfg := config{
		APIRoot: "http://127.0.0.1:8080/api/v1", RuntimeURL: "https://runtime.example.test",
		Transport: transportWS, Scenarios: []string{"ws-only"}, ConnectionCapacity: true,
		NodeID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", NodeVersion: runtimeLoadtestNodeVersion,
		MTLSCertFile: cert, MTLSKeyFile: key, MTLSCAFile: ca, StateDir: directory,
		Users: 1, Agents: 3, WorkersPerAgent: 10, Runs: 1,
		RunConcurrency: 1, SetupConcurrency: 1,
		Timeout: 10 * time.Minute, RequestTimeout: time.Second, ReadyTimeout: time.Second,
		PullWait: time.Second, CommandWait: time.Second,
		HeartbeatInterval: time.Second, WSProbeInterval: time.Second,
		CancelConcurrency: 1,
	}

	if err := cfg.validate(); err != nil {
		t.Fatalf("validate error = %v", err)
	}
	if cfg.ConnectionStepSize != 25 {
		t.Fatalf("connection step size = %d, want 25", cfg.ConnectionStepSize)
	}
	if cfg.ConnectionStepHold != 30*time.Second {
		t.Fatalf("connection step hold = %s, want 30s", cfg.ConnectionStepHold)
	}
	if cfg.ConnectStagger != 500*time.Millisecond {
		t.Fatalf("connect stagger = %s, want 500ms", cfg.ConnectStagger)
	}
	if cfg.HoldAfter != 5*time.Minute {
		t.Fatalf("final hold = %s, want 5m", cfg.HoldAfter)
	}
}

func TestRecommendedConnectionCapacityKeepsTwentyPercentHeadroom(t *testing.T) {
	if got := recommendedConnectionCapacity(359, 25); got != 275 {
		t.Fatalf("recommended capacity = %d, want 275", got)
	}
	if got := minimumStableConnections(101); got != 100 {
		t.Fatalf("minimum stable connections = %d, want 100", got)
	}
}

func TestObserveConnectionCapacityStageUsesCoreHealthAndRetention(t *testing.T) {
	var healthCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/healthz", "/readyz":
			healthCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	api := &apiClient{root: server.URL + "/api/v1", client: server.Client()}

	stableMetrics := &metrics{}
	stableMetrics.c.workersConnected.Store(100)
	stableStage := connectionCapacityStage{}
	if err := observeConnectionCapacityStage(
		context.Background(), 30*time.Millisecond, 100, api, stableMetrics, &stableStage,
	); err != nil {
		t.Fatalf("stable stage error = %v", err)
	}
	if stableStage.HealthSamples < 2 || healthCalls.Load() < 4 {
		t.Fatalf("health samples = %d calls = %d", stableStage.HealthSamples, healthCalls.Load())
	}

	unstableMetrics := &metrics{}
	unstableMetrics.c.workersConnected.Store(98)
	unstableStage := connectionCapacityStage{}
	if err := observeConnectionCapacityStage(
		context.Background(), 50*time.Millisecond, 100, api, unstableMetrics, &unstableStage,
	); err == nil {
		t.Fatal("unstable stage was accepted")
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
		var body struct {
			Input map[string]any `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode run request: %v", err)
		} else if got, want := r.Header.Get("Idempotency-Key"), body.Input["client_task_id"]; got == "" || got != want {
			t.Errorf("Idempotency-Key = %q, client_task_id = %v", got, want)
		}
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

func TestSubmitRunsAcceptsCreatedRun(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"run_id": "run-1"})
	}))
	defer server.Close()

	creator := &account{JWT: "jwt-token"}
	creator.User.ID = "user-1"
	err := submitRuns(
		context.Background(),
		&apiClient{root: server.URL, client: server.Client()},
		config{},
		[]agentRef{{ID: "agent-1", Creator: creator}},
		newRunTracker(),
		&metrics{},
		"measured",
		1,
		1,
	)
	if err != nil {
		t.Fatalf("submitRuns error = %v", err)
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

func TestPhaseReportsIncludeTimestampsAndDurations(t *testing.T) {
	start := time.Date(2026, 7, 5, 8, 0, 0, 0, time.UTC)
	phases := phaseTimestamps{
		setupAccountsStart: start,
		setupAccountsEnd:   start.Add(2 * time.Second),
		setupAgentsStart:   start.Add(2 * time.Second),
		setupAgentsEnd:     start.Add(7 * time.Second),
		workersStart:       start.Add(8 * time.Second),
		workersReady:       start.Add(11 * time.Second),
		measuredStart:      start.Add(12 * time.Second),
		measuredEnd:        start.Add(72 * time.Second),
		holdStart:          start.Add(73 * time.Second),
		holdEnd:            start.Add(193 * time.Second),
	}

	timestamps := phaseTimestampReport(phases)
	if got := timestamps["setup_accounts_started_at"]; got != "2026-07-05T08:00:00Z" {
		t.Fatalf("setup_accounts_started_at = %v", got)
	}
	if _, ok := timestamps["history_started_at"]; ok {
		t.Fatalf("history_started_at should be omitted when zero")
	}

	durations := phaseDurationReport(phases)
	if got := durations["setup_accounts_ms"]; got != 2000.0 {
		t.Fatalf("setup_accounts_ms = %v, want 2000", got)
	}
	if got := durations["setup_agents_ms"]; got != 5000.0 {
		t.Fatalf("setup_agents_ms = %v, want 5000", got)
	}
	if got := durations["pre_measured_ms"]; got != 12000.0 {
		t.Fatalf("pre_measured_ms = %v, want 12000", got)
	}
	if _, ok := durations["history_ms"]; ok {
		t.Fatalf("history_ms should be omitted when zero")
	}
}
