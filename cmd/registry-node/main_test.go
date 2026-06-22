package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRunCycleClaimsLocalAgentAndCompletesProxyRun(t *testing.T) {
	const (
		nodeSecret     = "rn_live_test"
		localAgentID   = "local-agent-1"
		proxyRunID     = "proxy-run-1"
		cloudRunID     = "cloud-run-1"
		cloudListingID = "cloud-listing-1"
		registryNodeID = "registry-node-1"
	)

	localAgentCalled := false
	localAgent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, cloudRunID, r.Header.Get("X-OpenLinker-Run-Id"))
		require.Equal(t, proxyRunID, r.Header.Get("X-OpenLinker-Proxy-Run-Id"))

		var req agentRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		require.Equal(t, cloudRunID, req.RunID)
		require.Equal(t, "hello", req.Input["text"])
		require.Equal(t, proxyRunID, req.Metadata["proxy_run_id"])
		require.Equal(t, localAgentID, req.Metadata["local_agent_id"])
		localAgentCalled = true

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"output":{"summary":"local agent done","answer":"42"}}`))
	}))
	defer localAgent.Close()

	claimCount := 0
	var completed completeProxyRunRequest
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer "+nodeSecret, r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/registry-node/heartbeat":
			require.Equal(t, http.MethodPost, r.Method)
			_, _ = w.Write([]byte(`{"node_id":"registry-node-1","heartbeat_status":"healthy","linked_listing_count":1,"pending_run_count":1}`))
		case "/api/v1/registry-node/metadata-sync":
			require.Equal(t, http.MethodPost, r.Method)
			_, _ = w.Write([]byte(`{"registry_node_id":"registry-node-1","synced_listing_count":1}`))
		case "/api/v1/proxy/runs/claim":
			require.Equal(t, http.MethodGet, r.Method)
			if claimCount > 0 {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			claimCount++
			_ = json.NewEncoder(w).Encode(proxyRun{
				ID:             proxyRunID,
				CloudRunID:     cloudRunID,
				CloudListingID: cloudListingID,
				RegistryNodeID: registryNodeID,
				LocalAgentID:   localAgentID,
				Status:         "claimed",
				PayloadPolicy:  "metadata_only",
				Input:          map[string]any{"text": "hello"},
			})
		case "/api/v1/proxy/runs/proxy-run-1/result":
			require.Equal(t, http.MethodPost, r.Method)
			require.NoError(t, json.NewDecoder(r.Body).Decode(&completed))
			_ = json.NewEncoder(w).Encode(proxyRun{
				ID:             proxyRunID,
				CloudRunID:     cloudRunID,
				CloudListingID: cloudListingID,
				RegistryNodeID: registryNodeID,
				LocalAgentID:   localAgentID,
				Status:         completed.Status,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer api.Close()

	cfg := config{
		APIBase:      api.URL + "/api/v1",
		NodeSecret:   nodeSecret,
		Endpoints:    endpointMap{localAgentID: localAgent.URL},
		HTTPTimeout:  2 * time.Second,
		Interval:     time.Second,
		SyncMetadata: true,
	}
	d := newDaemon(cfg, log.New(testWriter{t}, "", 0))
	require.NoError(t, d.runCycle(context.Background(), true))
	require.True(t, localAgentCalled)
	require.Equal(t, 1, claimCount)
	require.Equal(t, "success", completed.Status)
	require.Equal(t, "local agent done", completed.OutputSummary)
	require.Equal(t, "42", completed.Output["answer"])
}

func TestInvokeLocalAgentFailsWhenMappingMissing(t *testing.T) {
	d := newDaemon(config{
		APIBase:     "http://127.0.0.1:1/api/v1",
		NodeSecret:  "rn_live_test",
		Endpoints:   endpointMap{},
		HTTPTimeout: time.Second,
	}, log.New(testWriter{t}, "", 0))
	result := d.invokeLocalAgent(context.Background(), &proxyRun{
		ID:           "proxy-run-1",
		CloudRunID:   "cloud-run-1",
		LocalAgentID: "unknown-agent",
		Input:        map[string]any{"text": "hello"},
	})
	require.Equal(t, "failed", result.Status)
	require.Equal(t, "LOCAL_AGENT_ENDPOINT_NOT_CONFIGURED", result.ErrorCode)
}

func TestEndpointMapParsesListsAndValidatesURLs(t *testing.T) {
	endpoints := endpointMap{}
	require.NoError(t, endpoints.add(""))
	require.NoError(t, endpoints.add(" agent-a = http://127.0.0.1:18081/run "))
	require.NoError(t, endpoints.addList("agent-b=https://example.com/run,agent-c=http://example.net/a2a"))

	require.Equal(t, "http://127.0.0.1:18081/run", endpoints["agent-a"])
	require.Equal(t, "https://example.com/run", endpoints["agent-b"])
	require.Equal(t, "http://example.net/a2a", endpoints["agent-c"])
	rendered := endpoints.String()
	require.Contains(t, rendered, "agent-a=http://127.0.0.1:18081/run")
	require.Contains(t, rendered, "agent-b=https://example.com/run")

	for _, raw := range []string{
		"missing-equals",
		"=http://example.com",
		"agent-only=",
		"agent=://bad-url",
		"agent=ftp://example.com/run",
	} {
		t.Run(raw, func(t *testing.T) {
			require.Error(t, endpointMap{}.add(raw))
		})
	}
}

func TestParseConfigMergesEnvFlagsAndValidates(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-api", "https://api.example/root/",
		"-secret", " flag-secret ",
		"-agent-endpoint", "flag-agent=https://flag.example/run",
		"-once",
		"-interval", "2s",
		"-timeout", "3s",
		"-sync-metadata=false",
	}, map[string]string{
		"OPENLINKER_API_ROOT":        "https://env.example/api/v1",
		"OPENLINKER_NODE_SECRET":     "env-secret",
		"OPENLINKER_AGENT_ENDPOINTS": "env-agent=http://env.example/run",
	})
	require.NoError(t, err)
	require.Equal(t, "https://api.example/root/api/v1", cfg.APIBase)
	require.Equal(t, "flag-secret", cfg.NodeSecret)
	require.True(t, cfg.Once)
	require.False(t, cfg.SyncMetadata)
	require.Equal(t, 2*time.Second, cfg.Interval)
	require.Equal(t, 3*time.Second, cfg.HTTPTimeout)
	require.Equal(t, "http://env.example/run", cfg.Endpoints["env-agent"])
	require.Equal(t, "https://flag.example/run", cfg.Endpoints["flag-agent"])

	tests := []struct {
		name string
		args []string
		env  map[string]string
		want string
	}{
		{
			name: "missing secret",
			env:  map[string]string{"OPENLINKER_AGENT_ENDPOINTS": "agent=http://example.com/run"},
			want: "missing -secret",
		},
		{
			name: "missing endpoint",
			env:  map[string]string{"OPENLINKER_NODE_SECRET": "rn_live_test"},
			want: "missing -agent-endpoint",
		},
		{
			name: "bad env endpoint",
			env: map[string]string{
				"OPENLINKER_NODE_SECRET":     "rn_live_test",
				"OPENLINKER_AGENT_ENDPOINTS": "bad-endpoint",
			},
			want: "agent endpoint must be",
		},
		{
			name: "bad flag endpoint",
			args: []string{"-agent-endpoint", "agent=ftp://example.com/run"},
			env:  map[string]string{"OPENLINKER_NODE_SECRET": "rn_live_test"},
			want: "only supports http/https",
		},
		{
			name: "bad interval",
			args: []string{"-interval", "0s"},
			env: map[string]string{
				"OPENLINKER_NODE_SECRET":     "rn_live_test",
				"OPENLINKER_AGENT_ENDPOINTS": "agent=http://example.com/run",
			},
			want: "-interval must be positive",
		},
		{
			name: "bad timeout",
			args: []string{"-timeout", "0s"},
			env: map[string]string{
				"OPENLINKER_NODE_SECRET":     "rn_live_test",
				"OPENLINKER_AGENT_ENDPOINTS": "agent=http://example.com/run",
			},
			want: "-timeout must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseConfig(tt.args, tt.env)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestCloudClientClaimHandlesNoContentAndErrors(t *testing.T) {
	claimCount := 0
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer rn_live_test", r.Header.Get("Authorization"))
		require.Equal(t, "/api/v1/proxy/runs/claim", r.URL.Path)
		claimCount++
		if claimCount == 1 {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		http.Error(w, "try later", http.StatusTeapot)
	}))
	defer api.Close()

	client := &cloudClient{base: api.URL + "/api/v1", secret: "rn_live_test", httpClient: api.Client()}
	run, ok, err := client.claim(context.Background())
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, run)

	run, ok, err = client.claim(context.Background())
	require.Error(t, err)
	require.False(t, ok)
	require.Nil(t, run)
	require.Contains(t, err.Error(), "returned 418")
	require.Contains(t, err.Error(), "try later")
}

func TestInvokeLocalAgentResponseBoundaries(t *testing.T) {
	tests := []struct {
		name          string
		status        int
		body          string
		wantStatus    string
		wantCode      string
		wantMessage   string
		wantRetryable bool
		wantSummary   string
	}{
		{
			name:          "retryable server error with structured body",
			status:        http.StatusBadGateway,
			body:          `{"error":{"code":"UPSTREAM_BUSY","message":"try again"}}`,
			wantStatus:    "failed",
			wantCode:      "UPSTREAM_BUSY",
			wantMessage:   "try again",
			wantRetryable: true,
		},
		{
			name:        "client error with plain body",
			status:      http.StatusBadRequest,
			body:        "bad input",
			wantStatus:  "failed",
			wantCode:    "LOCAL_AGENT_HTTP_400",
			wantMessage: "bad input",
		},
		{
			name:        "non json success response",
			status:      http.StatusOK,
			body:        "not-json",
			wantStatus:  "failed",
			wantCode:    "INVALID_LOCAL_AGENT_RESPONSE",
			wantMessage: "non-JSON response",
		},
		{
			name:        "agent error without explicit code",
			status:      http.StatusOK,
			body:        `{"error":{"message":"denied"}}`,
			wantStatus:  "failed",
			wantCode:    "LOCAL_AGENT_ERROR",
			wantMessage: "denied",
		},
		{
			name:        "missing output",
			status:      http.StatusOK,
			body:        `{}`,
			wantStatus:  "failed",
			wantCode:    "LOCAL_AGENT_OUTPUT_MISSING",
			wantMessage: "must include an output object",
		},
		{
			name:        "success summary",
			status:      http.StatusOK,
			body:        `{"output":{"message":" hello from local agent "}}`,
			wantStatus:  "success",
			wantSummary: "hello from local agent",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			localAgent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "cloud-run-1", r.Header.Get("X-OpenLinker-Run-Id"))
				require.Equal(t, "proxy-run-1", r.Header.Get("X-OpenLinker-Proxy-Run-Id"))
				require.Equal(t, "registry-node-1", r.Header.Get("X-OpenLinker-Registry-Node-Id"))

				var req agentRequest
				require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
				require.Equal(t, "cloud-run-1", req.RunID)
				require.Equal(t, "hello", req.Input["text"])
				require.Equal(t, "metadata_only", req.Metadata["payload_policy"])

				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer localAgent.Close()

			d := newDaemon(config{
				APIBase:     "http://127.0.0.1:1/api/v1",
				NodeSecret:  "rn_live_test",
				Endpoints:   endpointMap{"local-agent-1": localAgent.URL},
				HTTPTimeout: time.Second,
			}, log.New(testWriter{t}, "", 0))
			result := d.invokeLocalAgent(context.Background(), &proxyRun{
				ID:             "proxy-run-1",
				CloudRunID:     "cloud-run-1",
				CloudListingID: "listing-1",
				RegistryNodeID: "registry-node-1",
				LocalAgentID:   "local-agent-1",
				PayloadPolicy:  "metadata_only",
				Input:          map[string]any{"text": "hello"},
			})

			require.Equal(t, tt.wantStatus, result.Status)
			require.Equal(t, tt.wantRetryable, result.Retryable)
			if tt.wantStatus == "success" {
				require.Equal(t, tt.wantSummary, result.OutputSummary)
				require.Equal(t, " hello from local agent ", result.Output["message"])
				return
			}
			require.Equal(t, tt.wantCode, result.ErrorCode)
			require.Contains(t, result.ErrorMessage, tt.wantMessage)
		})
	}
}

func TestResultHelpersSummarizeAndTruncate(t *testing.T) {
	failed := failedResult("", "")
	require.Equal(t, "failed", failed.Status)
	require.Equal(t, "LOCAL_AGENT_ERROR", failed.ErrorCode)
	require.Equal(t, "LOCAL_AGENT_ERROR", failed.ErrorMessage)

	retryable := retryableFailedResult("TEMPORARY", "try later")
	require.True(t, retryable.Retryable)
	require.Equal(t, "TEMPORARY", retryable.ErrorCode)

	fromBody := failedResultFromBody("LOCAL_AGENT_HTTP_400", []byte(`{"error":{"message":"bad payload"}}`))
	require.Equal(t, "LOCAL_AGENT_HTTP_400", fromBody.ErrorCode)
	require.Equal(t, "bad payload", fromBody.ErrorMessage)

	require.Equal(t, "summary wins", summarizeOutput(map[string]any{
		"summary": " summary wins ",
		"answer":  "answer loses",
	}))
	require.Contains(t, summarizeOutput(map[string]any{"nested": map[string]any{"ok": true}}), `"nested"`)
	require.Equal(t, "", truncate("hello", 0))
	require.Equal(t, "he", truncate("hello", 2))
	require.Equal(t, "hello", truncate("hello", 5))
	require.True(t, strings.HasSuffix(truncate("hello world", 8), "..."))
}

type testWriter struct {
	t *testing.T
}

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", p)
	return len(p), nil
}
