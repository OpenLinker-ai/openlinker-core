package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
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

type testWriter struct {
	t *testing.T
}

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", p)
	return len(p), nil
}
