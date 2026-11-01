package registry_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/OpenLinker-ai/openlinker-core/pkg/registry"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const proxyEchoToken = "seller-endpoint-token"

func proxyEchoOutput() map[string]any {
	return map[string]any{
		"origin":  "203.0.113.7",
		"headers": map[string]any{"X-Forwarded-For": []any{"203.0.113.7"}, "X-Openlinker-Token": []any{proxyEchoToken}},
		"note":    "signed with " + proxyEchoToken,
		"total":   float64(3),
		"artifacts": []any{map[string]any{
			"id": "a1", "title": "echo " + proxyEchoToken, "artifact_type": "data",
			"content": map[string]any{"headers": map[string]any{"X-Openlinker-Token": proxyEchoToken}, "rows": float64(3)},
		}},
	}
}

func assertNoProxyEcho(t *testing.T, value any) {
	t.Helper()
	raw, err := json.Marshal(value)
	require.NoError(t, err)
	assert.NotContains(t, string(raw), proxyEchoToken)
	assert.NotContains(t, string(raw), "203.0.113.7")
	assert.NotContains(t, string(raw), "X-Forwarded-For")
}

func TestProxyRunReadsAreFilteredForRequester(t *testing.T) {
	pool := setupRegistryBridgeDB(t)
	svc := newLocalRegistryService(pool)
	ctx := context.Background()
	owner := insertRegistryOwner(t, pool)
	requester := insertRegistryOwner(t, pool)
	node, err := svc.CreateNode(ctx, owner, &registry.CreateNodeRequest{NodeName: "Echo Bridge", NodeType: "bridge_proxy"})
	require.NoError(t, err)
	summary, err := json.Marshal(proxyEchoOutput())
	require.NoError(t, err)

	complete := func(viewer uuid.UUID, policy string, req registry.CompleteProxyRunRequest) (uuid.UUID, *registry.CreateProxyRunRequest) {
		t.Helper()
		agent := insertRegistryAgent(t, pool, owner)
		_, err := pool.Exec(ctx, `UPDATE agents SET endpoint_auth_header=$2 WHERE id=$1`, agent, proxyEchoToken)
		require.NoError(t, err)
		listing, err := svc.CreateRegistryListing(ctx, owner, &registry.CreateRegistryListingRequest{RegistryNodeID: node.ID, AgentID: agent.String(), PayloadPolicy: policy})
		require.NoError(t, err)
		create := &registry.CreateProxyRunRequest{RegistryListingID: listing.RegistryListingID, IdempotencyKey: "echo-" + uuid.NewString(), Input: map[string]any{"q": "x"}}
		created, err := svc.CreateProxyRun(ctx, viewer, create)
		require.NoError(t, err)
		claimed, err := svc.ClaimProxyRun(ctx, node.NodeSecret)
		require.NoError(t, err)
		require.Equal(t, created.ID, claimed.ID)
		id := uuid.MustParse(created.ID)
		_, err = svc.CompleteProxyRun(ctx, node.NodeSecret, id, &req)
		require.NoError(t, err)
		return id, create
	}

	t.Run("full payload", func(t *testing.T) {
		id, create := complete(requester, "store_full_payload", registry.CompleteProxyRunRequest{Status: "success", Output: proxyEchoOutput(), OutputSummary: string(summary)})
		got, err := svc.GetProxyRun(ctx, requester, id)
		require.NoError(t, err)
		assertNoProxyEcho(t, got)
		assert.True(t, got.OutputRedacted)
		assert.Equal(t, float64(3), got.Output["total"])
		assert.Equal(t, "signed with [redacted]", got.Output["note"])

		replay, err := svc.CreateProxyRun(ctx, requester, create)
		require.NoError(t, err)
		require.Equal(t, id.String(), replay.ID)
		assertNoProxyEcho(t, replay)
		assert.True(t, replay.OutputRedacted)

		artifacts, err := svc.ListProxyRunArtifacts(ctx, requester, id)
		require.NoError(t, err)
		require.Len(t, artifacts, 1)
		assertNoProxyEcho(t, artifacts)
		assert.True(t, artifacts[0].OutputRedacted)
		assert.Equal(t, float64(3), artifacts[0].Content["rows"])

		var stored string
		require.NoError(t, pool.QueryRow(ctx, `SELECT output::text FROM proxy_runs WHERE id=$1`, id).Scan(&stored))
		assert.Contains(t, stored, proxyEchoToken, "stored output is not rewritten")
	})
	t.Run("metadata only still hides artifact title and error message", func(t *testing.T) {
		id, _ := complete(requester, "metadata_only", registry.CompleteProxyRunRequest{Status: "success", Output: proxyEchoOutput()})
		artifacts, err := svc.ListProxyRunArtifacts(ctx, requester, id)
		require.NoError(t, err)
		require.Len(t, artifacts, 1)
		assert.Equal(t, "echo [redacted]", artifacts[0].Title)

		failed, _ := complete(requester, "metadata_only", registry.CompleteProxyRunRequest{Status: "failed", ErrorMessage: "rejected " + proxyEchoToken})
		got, err := svc.GetProxyRun(ctx, requester, failed)
		require.NoError(t, err)
		assert.Equal(t, "rejected [redacted]", got.ErrorMessage)
		assert.True(t, got.OutputRedacted)
	})
	t.Run("summary policy", func(t *testing.T) {
		id, _ := complete(requester, "store_run_summary", registry.CompleteProxyRunRequest{Status: "success", Output: proxyEchoOutput(), OutputSummary: string(summary)})
		got, err := svc.GetProxyRun(ctx, requester, id)
		require.NoError(t, err)
		assertNoProxyEcho(t, got)
		assert.True(t, got.OutputRedacted)
	})
	t.Run("creator as requester reads the original", func(t *testing.T) {
		id, _ := complete(owner, "store_full_payload", registry.CompleteProxyRunRequest{Status: "success", Output: proxyEchoOutput(), OutputSummary: "signed with " + proxyEchoToken})
		got, err := svc.GetProxyRun(ctx, owner, id)
		require.NoError(t, err)
		assert.False(t, got.OutputRedacted)
		assert.Equal(t, "signed with "+proxyEchoToken, got.Output["note"])
		assert.Equal(t, "signed with "+proxyEchoToken, got.OutputSummary)
		artifacts, err := svc.ListProxyRunArtifacts(ctx, owner, id)
		require.NoError(t, err)
		require.Len(t, artifacts, 1)
		assert.Contains(t, artifacts[0].Title, proxyEchoToken)
	})
	t.Run("business output is unchanged", func(t *testing.T) {
		id, _ := complete(requester, "store_full_payload", registry.CompleteProxyRunRequest{Status: "success", Output: map[string]any{"origin": "shanghai", "total": float64(3)}, OutputSummary: "3 rows"})
		got, err := svc.GetProxyRun(ctx, requester, id)
		require.NoError(t, err)
		assert.False(t, got.OutputRedacted)
		assert.Equal(t, "shanghai", got.Output["origin"])
		assert.Equal(t, "3 rows", got.OutputSummary)
	})
	t.Run("node keeps its own result", func(t *testing.T) {
		agent := insertRegistryAgent(t, pool, owner)
		listing, err := svc.CreateRegistryListing(ctx, owner, &registry.CreateRegistryListingRequest{RegistryNodeID: node.ID, AgentID: agent.String(), PayloadPolicy: "store_full_payload"})
		require.NoError(t, err)
		created, err := svc.CreateProxyRun(ctx, requester, &registry.CreateProxyRunRequest{RegistryListingID: listing.RegistryListingID, IdempotencyKey: "node-" + uuid.NewString(), Input: map[string]any{"q": "x"}})
		require.NoError(t, err)
		_, err = svc.ClaimProxyRun(ctx, node.NodeSecret)
		require.NoError(t, err)
		completed, err := svc.CompleteProxyRun(ctx, node.NodeSecret, uuid.MustParse(created.ID), &registry.CompleteProxyRunRequest{Status: "success", Output: proxyEchoOutput()})
		require.NoError(t, err)
		assert.False(t, completed.OutputRedacted)
		assert.Equal(t, "signed with "+proxyEchoToken, completed.Output["note"])
	})
}

func TestRemoteProxyRunRelayIsFiltered(t *testing.T) {
	for name, remoteFlag := range map[string]bool{"remote did not filter": false, "remote already filtered": true} {
		t.Run(name, func(t *testing.T) {
			remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusCreated)
				output := map[string]any{"total": float64(3)}
				if !remoteFlag {
					output = proxyEchoOutput()
				}
				_ = json.NewEncoder(w).Encode(registry.ProxyRunResponse{ID: uuid.NewString(), RegistryListingID: uuid.NewString(), Status: "success", Output: output, OutputRedacted: remoteFlag})
			}))
			defer remote.Close()
			resp, err := newLocalRegistryService(nil).CreateRemoteProxyRun(context.Background(), uuid.Nil, &registry.CreateRemoteProxyRunRequest{
				RemoteAPIBaseURL: remote.URL, RemoteBearerToken: "remote-token-123", RemoteRegistryListingID: uuid.NewString(),
				IdempotencyKey: "cross-route-123", Input: map[string]any{"q": "x"},
			})
			require.NoError(t, err)
			assert.True(t, resp.RemoteRun.OutputRedacted)
			raw, err := json.Marshal(resp.RemoteRun)
			require.NoError(t, err)
			assert.NotContains(t, string(raw), "203.0.113.7")
			assert.False(t, strings.Contains(string(raw), "X-Openlinker-Token"))
		})
	}
}
