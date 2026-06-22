package registry_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kinzhi/openlinker-core/pkg/httpx"
	"github.com/kinzhi/openlinker-core/pkg/registry"
)

const truncateRegistryBridgeTables = "TRUNCATE proxy_runs, registry_peers, registry_federation_invites, cloud_listing_links, registry_nodes, agent_skills, agents, wallets, users RESTART IDENTITY CASCADE"
const registryTestDBTimeout = 30 * time.Second

func setupRegistryBridgeDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL 未设置，跳过 registry 集成测试")
	}
	ctx, cancel := context.WithTimeout(context.Background(), registryTestDBTimeout)
	defer cancel()
	cfg, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	cfg.ConnConfig.RuntimeParams["lock_timeout"] = "5s"
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = registryTestDBTimeout.String()

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	require.NoError(t, pool.Ping(ctx))
	_, err = pool.Exec(ctx, truncateRegistryBridgeTables)
	require.NoError(t, err)
	t.Cleanup(func() {
		// setupRegistryBridgeDB truncates before each test. Closing here is
		// enough and avoids doubling suite time with a second TRUNCATE.
		pool.Close()
	})
	return pool
}

func insertRegistryOwner(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO users (id, email, password_hash, display_name, is_creator, creator_verified)
		 VALUES ($1, $2, 'x', 'Registry Owner', TRUE, TRUE)`,
		id, "registry-"+id.String()[:8]+"@example.com")
	require.NoError(t, err)
	return id
}

func insertRegistryAgent(t *testing.T, pool *pgxpool.Pool, ownerID uuid.UUID) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO agents (
			id, creator_id, slug, name, description, endpoint_url,
			price_per_call_cents, tags, lifecycle_status, visibility, certification_status
		) VALUES (
			$1, $2, $3, 'Bridge Agent', 'private node agent', 'http://127.0.0.1:18081',
			0, ARRAY['bridge'], 'active', 'private', 'unreviewed'
		)`,
		id, ownerID, "bridge-agent-"+id.String()[:8])
	require.NoError(t, err)
	return id
}

func requireHTTPStatus(t *testing.T, err error, status int) {
	t.Helper()
	var httpErr *httpx.HTTPError
	require.ErrorAs(t, err, &httpErr)
	assert.Equal(t, status, httpErr.Status)
}

func TestRegistryNodeHeartbeatAndCloudListing(t *testing.T) {
	pool := setupRegistryBridgeDB(t)
	svc := registry.NewService(pool)
	ctx := context.Background()

	ownerID := insertRegistryOwner(t, pool)
	agentID := insertRegistryAgent(t, pool, ownerID)

	node, err := svc.CreateNode(ctx, ownerID, &registry.CreateNodeRequest{
		NodeName: "Local Bridge",
		NodeType: "bridge_proxy",
		BaseURL:  "http://127.0.0.1:3000",
	})
	require.NoError(t, err)
	require.NotEmpty(t, node.ID)
	require.NotEmpty(t, node.NodeSecret)
	assert.Equal(t, "unknown", node.HeartbeatStatus)
	assert.Contains(t, node.Scopes, "heartbeat")
	assert.Contains(t, node.Scopes, "proxy:pull")
	assert.Contains(t, node.Scopes, "proxy:result")

	heartbeat, err := svc.Heartbeat(ctx, node.NodeSecret)
	require.NoError(t, err)
	assert.Equal(t, node.ID, heartbeat.NodeID)
	assert.Equal(t, "healthy", heartbeat.HeartbeatStatus)
	assert.Equal(t, int32(0), heartbeat.LinkedListingCount)

	listing, err := svc.CreateCloudListing(ctx, ownerID, &registry.CreateCloudListingRequest{
		RegistryNodeID: node.ID,
		AgentID:        agentID.String(),
		RoutingMode:    "pull_proxy",
		PayloadPolicy:  "metadata_only",
	})
	require.NoError(t, err)
	assert.Equal(t, node.ID, listing.RegistryNodeID)
	assert.Equal(t, agentID.String(), listing.AgentID)
	assert.Equal(t, "Bridge Agent", listing.AgentName)
	assert.Equal(t, "private node agent", listing.AgentDescription)
	assert.ElementsMatch(t, []string{"bridge"}, listing.AgentTags)
	assert.Equal(t, "unknown", listing.AvailabilityStatus)
	assert.NotEmpty(t, listing.MetadataSyncedAt)
	assert.Equal(t, "linked", listing.SyncStatus)
	assert.Equal(t, "pull_proxy", listing.RoutingMode)
	assert.Equal(t, "metadata_only", listing.PayloadPolicy)
	nodeID, err := uuid.Parse(node.ID)
	require.NoError(t, err)
	cloudListingID, err := uuid.Parse(listing.CloudListingID)
	require.NoError(t, err)

	heartbeat, err = svc.Heartbeat(ctx, node.NodeSecret)
	require.NoError(t, err)
	assert.Equal(t, int32(1), heartbeat.LinkedListingCount)
	assert.Equal(t, int32(0), heartbeat.PendingRunCount)

	nodes, err := svc.ListNodes(ctx, ownerID)
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	assert.Empty(t, nodes[0].NodeSecret, "node secret must only be returned on create")
	assert.Equal(t, "healthy", nodes[0].HeartbeatStatus)

	listings, err := svc.ListCloudListings(ctx, ownerID)
	require.NoError(t, err)
	require.Len(t, listings, 1)
	assert.Equal(t, listing.CloudListingID, listings[0].CloudListingID)
	assert.Equal(t, "Bridge Agent", listings[0].AgentName)
	assert.Equal(t, "private node agent", listings[0].AgentDescription)
	assert.Equal(t, "unknown", listings[0].AvailabilityStatus)

	_, err = pool.Exec(ctx,
		`UPDATE agents SET name='Bridge Agent Synced', description='synced description', tags=ARRAY['bridge','synced'] WHERE id=$1`,
		agentID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`INSERT INTO agent_availability_snapshots (agent_id, availability_status, last_checked_at, updated_at)
		 VALUES ($1, 'healthy', NOW(), NOW())
		 ON CONFLICT (agent_id) DO UPDATE
		 SET availability_status='healthy', last_checked_at=NOW(), updated_at=NOW()`,
		agentID)
	require.NoError(t, err)

	listings, err = svc.ListCloudListings(ctx, ownerID)
	require.NoError(t, err)
	require.Len(t, listings, 1)
	assert.Equal(t, "Bridge Agent", listings[0].AgentName, "listing uses the last synced snapshot before metadata sync")

	synced, err := svc.SyncCloudListingMetadata(ctx, ownerID, cloudListingID)
	require.NoError(t, err)
	assert.Equal(t, "Bridge Agent Synced", synced.AgentName)
	assert.Equal(t, "synced description", synced.AgentDescription)
	assert.ElementsMatch(t, []string{"bridge", "synced"}, synced.AgentTags)
	assert.Equal(t, "healthy", synced.AvailabilityStatus)
	assert.NotEmpty(t, synced.MetadataSyncedAt)

	_, err = pool.Exec(ctx,
		`UPDATE agents SET name='Bridge Agent Node Synced', description='node synced description', tags=ARRAY['bridge','node'] WHERE id=$1`,
		agentID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`UPDATE agent_availability_snapshots SET availability_status='degraded', last_checked_at=NOW(), updated_at=NOW() WHERE agent_id=$1`,
		agentID)
	require.NoError(t, err)
	nodeSync, err := svc.SyncNodeMetadata(ctx, node.NodeSecret)
	require.NoError(t, err)
	assert.Equal(t, node.ID, nodeSync.RegistryNodeID)
	assert.Equal(t, int32(1), nodeSync.SyncedListingCount)

	listings, err = svc.ListCloudListings(ctx, ownerID)
	require.NoError(t, err)
	require.Len(t, listings, 1)
	assert.Equal(t, "Bridge Agent Node Synced", listings[0].AgentName)
	assert.Equal(t, "node synced description", listings[0].AgentDescription)
	assert.Equal(t, "degraded", listings[0].AvailabilityStatus)

	idempotencyKey := "proxy-test-" + uuid.NewString()
	proxyRun, err := svc.CreateProxyRun(ctx, ownerID, &registry.CreateProxyRunRequest{
		CloudListingID: listing.CloudListingID,
		IdempotencyKey: idempotencyKey,
		Input: map[string]any{
			"task": "run SQL and summarize",
		},
		InputSummary: "run SQL",
	})
	require.NoError(t, err)
	require.NotEmpty(t, proxyRun.ID)
	assert.Equal(t, "pending", proxyRun.Status)
	assert.Equal(t, listing.CloudListingID, proxyRun.CloudListingID)
	assert.Equal(t, node.ID, proxyRun.RegistryNodeID)
	assert.Equal(t, agentID.String(), proxyRun.LocalAgentID)
	assert.Equal(t, "metadata_only", proxyRun.PayloadPolicy)
	assert.Nil(t, proxyRun.Input)
	assert.Empty(t, proxyRun.InputSummary)

	heartbeat, err = svc.Heartbeat(ctx, node.NodeSecret)
	require.NoError(t, err)
	assert.Equal(t, int32(1), heartbeat.PendingRunCount)

	again, err := svc.CreateProxyRun(ctx, ownerID, &registry.CreateProxyRunRequest{
		CloudListingID: listing.CloudListingID,
		IdempotencyKey: idempotencyKey,
		Input: map[string]any{
			"task": "duplicate should not create another row",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, proxyRun.ID, again.ID)

	claimed, err := svc.ClaimProxyRun(ctx, node.NodeSecret)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, proxyRun.ID, claimed.ID)
	assert.Equal(t, "claimed", claimed.Status)
	assert.Equal(t, "run SQL and summarize", claimed.Input["task"])
	assert.Equal(t, int32(1), claimed.AttemptCount)
	runID, err := uuid.Parse(proxyRun.ID)
	require.NoError(t, err)
	var nodeInputStillStored bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT node_input IS NOT NULL FROM proxy_runs WHERE id=$1`, runID).Scan(&nodeInputStillStored))
	assert.True(t, nodeInputStillStored)

	heartbeat, err = svc.Heartbeat(ctx, node.NodeSecret)
	require.NoError(t, err)
	assert.Equal(t, int32(0), heartbeat.PendingRunCount)

	artifactBody := []byte("order_id,total\n1,42\n")
	artifactSum := sha256.Sum256(artifactBody)
	artifactSHA := hex.EncodeToString(artifactSum[:])
	artifactServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/artifacts/orders.csv" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/csv")
		_, _ = w.Write(artifactBody)
	}))
	defer artifactServer.Close()

	completed, err := svc.CompleteProxyRun(ctx, node.NodeSecret, runID, &registry.CompleteProxyRunRequest{
		Status: "success",
		Output: map[string]any{
			"summary": "SQL 查询完成，发现订单数为 42",
			"artifacts": []map[string]any{{
				"id":              "orders-csv",
				"title":           "订单查询结果",
				"artifact_type":   "file",
				"file_uri":        artifactServer.URL + "/artifacts/orders.csv",
				"file_name":       "orders.csv",
				"mime_type":       "text/csv",
				"file_sha256":     artifactSHA,
				"file_size_bytes": len(artifactBody),
				"content": map[string]any{
					"rows": 42,
				},
			}},
		},
		OutputSummary: "SQL 查询完成",
	})
	require.NoError(t, err)
	assert.Equal(t, "success", completed.Status)
	assert.Empty(t, completed.OutputSummary)
	assert.Nil(t, completed.Output)
	assert.NotEmpty(t, completed.FinishedAt)
	require.NoError(t, pool.QueryRow(ctx, `SELECT node_input IS NOT NULL FROM proxy_runs WHERE id=$1`, runID).Scan(&nodeInputStillStored))
	assert.False(t, nodeInputStillStored)

	fetched, err := svc.GetProxyRun(ctx, ownerID, runID)
	require.NoError(t, err)
	assert.Equal(t, "success", fetched.Status)
	assert.Empty(t, fetched.OutputSummary)
	assert.Nil(t, fetched.Output)

	artifacts, err := svc.ListProxyRunArtifacts(ctx, ownerID, runID)
	require.NoError(t, err)
	require.Len(t, artifacts, 1)
	assert.Equal(t, "orders-csv", artifacts[0].SourceArtifactID)
	assert.Equal(t, "file", artifacts[0].ArtifactType)
	assert.Equal(t, "订单查询结果", artifacts[0].Title)
	assert.Equal(t, artifactServer.URL+"/artifacts/orders.csv", artifacts[0].FileURI)
	assert.Equal(t, "orders.csv", artifacts[0].FileName)
	assert.Equal(t, "text/csv", artifacts[0].MimeType)
	assert.Equal(t, artifactSHA, artifacts[0].FileSHA256)
	require.NotNil(t, artifacts[0].FileSizeBytes)
	assert.Equal(t, int64(len(artifactBody)), *artifacts[0].FileSizeBytes)
	assert.Nil(t, artifacts[0].Content, "metadata_only keeps artifact file metadata but not artifact content")
	artifactID, err := uuid.Parse(artifacts[0].ID)
	require.NoError(t, err)
	downloaded, err := svc.DownloadProxyRunArtifact(ctx, ownerID, runID, artifactID)
	require.NoError(t, err)
	assert.Equal(t, "orders.csv", downloaded.FileName)
	assert.Equal(t, "text/csv", downloaded.ContentType)
	assert.Equal(t, artifactSHA, downloaded.SHA256)
	assert.Equal(t, artifactBody, downloaded.Body)

	emptyClaim, err := svc.ClaimProxyRun(ctx, node.NodeSecret)
	require.NoError(t, err)
	assert.Nil(t, emptyClaim)

	paused, err := svc.UpdateCloudListingStatus(ctx, ownerID, cloudListingID, &registry.UpdateCloudListingStatusRequest{
		SyncStatus: "paused",
	})
	require.NoError(t, err)
	assert.Equal(t, "paused", paused.SyncStatus)

	_, err = svc.CreateProxyRun(ctx, ownerID, &registry.CreateProxyRunRequest{
		CloudListingID: listing.CloudListingID,
		IdempotencyKey: "paused-" + uuid.NewString(),
	})
	requireHTTPStatus(t, err, http.StatusNotFound)

	resumed, err := svc.UpdateCloudListingStatus(ctx, ownerID, cloudListingID, &registry.UpdateCloudListingStatusRequest{
		SyncStatus: "linked",
	})
	require.NoError(t, err)
	assert.Equal(t, "linked", resumed.SyncStatus)

	rotated, err := svc.RotateNodeSecret(ctx, ownerID, nodeID)
	require.NoError(t, err)
	require.NotEmpty(t, rotated.NodeSecret)
	assert.NotEqual(t, node.NodeSecret, rotated.NodeSecret)
	assert.NotEqual(t, node.SecretPrefix, rotated.SecretPrefix)

	_, err = svc.Heartbeat(ctx, node.NodeSecret)
	requireHTTPStatus(t, err, http.StatusUnauthorized)
	heartbeat, err = svc.Heartbeat(ctx, rotated.NodeSecret)
	require.NoError(t, err)
	assert.Equal(t, node.ID, heartbeat.NodeID)

	timeoutRun, err := svc.CreateProxyRun(ctx, ownerID, &registry.CreateProxyRunRequest{
		CloudListingID: listing.CloudListingID,
		IdempotencyKey: "timeout-" + uuid.NewString(),
		InputSummary:   "will timeout",
	})
	require.NoError(t, err)
	claimedTimeout, err := svc.ClaimProxyRun(ctx, rotated.NodeSecret)
	require.NoError(t, err)
	require.NotNil(t, claimedTimeout)
	assert.Equal(t, timeoutRun.ID, claimedTimeout.ID)
	timeoutRunID, err := uuid.Parse(timeoutRun.ID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `UPDATE proxy_runs SET claimed_at = NOW() - INTERVAL '1 minute' WHERE id=$1`, timeoutRunID)
	require.NoError(t, err)

	expired, err := svc.ExpireStaleProxyRuns(ctx, 0)
	require.NoError(t, err)
	assert.Equal(t, int32(1), expired)
	fetchedTimeout, err := svc.GetProxyRun(ctx, ownerID, timeoutRunID)
	require.NoError(t, err)
	assert.Equal(t, "timeout", fetchedTimeout.Status)
	assert.Equal(t, "PROXY_RUN_TIMEOUT", fetchedTimeout.ErrorCode)
	assert.NotEmpty(t, fetchedTimeout.FinishedAt)

	_, err = svc.CompleteProxyRun(ctx, rotated.NodeSecret, timeoutRunID, &registry.CompleteProxyRunRequest{
		Status: "success",
	})
	requireHTTPStatus(t, err, http.StatusNotFound)

	revoked, err := svc.RevokeNode(ctx, ownerID, nodeID)
	require.NoError(t, err)
	assert.Equal(t, "revoked", revoked.HeartbeatStatus)
	assert.NotEmpty(t, revoked.RevokedAt)

	_, err = svc.Heartbeat(ctx, rotated.NodeSecret)
	requireHTTPStatus(t, err, http.StatusUnauthorized)
	_, err = svc.RotateNodeSecret(ctx, ownerID, nodeID)
	requireHTTPStatus(t, err, http.StatusNotFound)
	_, err = svc.UpdateCloudListingStatus(ctx, ownerID, cloudListingID, &registry.UpdateCloudListingStatusRequest{
		SyncStatus: "linked",
	})
	requireHTTPStatus(t, err, http.StatusNotFound)
	_, err = svc.CreateProxyRun(ctx, ownerID, &registry.CreateProxyRunRequest{
		CloudListingID: listing.CloudListingID,
		IdempotencyKey: "revoked-" + uuid.NewString(),
	})
	requireHTTPStatus(t, err, http.StatusNotFound)

	listings, err = svc.ListCloudListings(ctx, ownerID)
	require.NoError(t, err)
	require.Len(t, listings, 1)
	assert.Equal(t, "paused", listings[0].SyncStatus)
}

func TestStartProxyRunWorkerStopsWhenCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	registry.StartProxyRunWorker(ctx, nil, registry.ProxyRunWorkerConfig{})
}

func TestStartProxyRunWorkerExpiresStaleProxyRuns(t *testing.T) {
	pool := setupRegistryBridgeDB(t)
	svc := registry.NewService(pool)
	ctx := context.Background()

	ownerID := insertRegistryOwner(t, pool)
	agentID := insertRegistryAgent(t, pool, ownerID)
	node, err := svc.CreateNode(ctx, ownerID, &registry.CreateNodeRequest{
		NodeName: "Worker Bridge",
		NodeType: "bridge_proxy",
		BaseURL:  "http://127.0.0.1:3000",
	})
	require.NoError(t, err)
	listing, err := svc.CreateCloudListing(ctx, ownerID, &registry.CreateCloudListingRequest{
		RegistryNodeID: node.ID,
		AgentID:        agentID.String(),
		RoutingMode:    "pull_proxy",
		PayloadPolicy:  "store_full_payload",
	})
	require.NoError(t, err)
	proxyRun, err := svc.CreateProxyRun(ctx, ownerID, &registry.CreateProxyRunRequest{
		CloudListingID: listing.CloudListingID,
		IdempotencyKey: "worker-timeout-" + uuid.NewString(),
		InputSummary:   "worker should expire this run",
	})
	require.NoError(t, err)
	claimed, err := svc.ClaimProxyRun(ctx, node.NodeSecret)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, proxyRun.ID, claimed.ID)
	runID := uuid.MustParse(proxyRun.ID)
	_, err = pool.Exec(ctx, `UPDATE proxy_runs SET claimed_at = NOW() - INTERVAL '1 minute' WHERE id=$1`, runID)
	require.NoError(t, err)

	workerCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		registry.StartProxyRunWorker(workerCtx, svc, registry.ProxyRunWorkerConfig{
			Interval: 5 * time.Millisecond,
			Timeout:  time.Millisecond,
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("proxy run worker did not stop after cancellation")
		}
	})

	require.Eventually(t, func() bool {
		fetched, err := svc.GetProxyRun(ctx, ownerID, runID)
		return err == nil && fetched.Status == "timeout" && fetched.ErrorCode == "PROXY_RUN_TIMEOUT"
	}, time.Second, 10*time.Millisecond)
	cancel()
	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
}

func TestCreateRemoteProxyRunRoutesToRemoteRegistryAPI(t *testing.T) {
	remoteListingID := uuid.New()
	remoteRunID := uuid.New()
	var capturedAuth string
	var capturedBody struct {
		CloudListingID string         `json:"cloud_listing_id"`
		IdempotencyKey string         `json:"idempotency_key"`
		Input          map[string]any `json:"input"`
		InputSummary   string         `json:"input_summary"`
	}
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/api/v1/proxy/runs", r.URL.Path)
		capturedAuth = r.Header.Get("Authorization")
		require.NoError(t, json.NewDecoder(r.Body).Decode(&capturedBody))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(registry.ProxyRunResponse{
			ID:                 remoteRunID.String(),
			CloudRunID:         uuid.NewString(),
			CloudListingLinkID: uuid.NewString(),
			CloudListingID:     remoteListingID.String(),
			RegistryNodeID:     uuid.NewString(),
			LocalAgentID:       uuid.NewString(),
			RequestingUserID:   uuid.NewString(),
			IdempotencyKey:     "cross-route-123",
			Status:             "pending",
			PayloadPolicy:      "metadata_only",
			AttemptCount:       0,
			MaxAttempts:        3,
			CreatedAt:          time.Now().UTC().Format(time.RFC3339),
			UpdatedAt:          time.Now().UTC().Format(time.RFC3339),
		}))
	}))
	defer remote.Close()

	svc := registry.NewService(nil)
	resp, err := svc.CreateRemoteProxyRun(context.Background(), uuid.Nil, &registry.CreateRemoteProxyRunRequest{
		RemoteAPIBaseURL:     remote.URL,
		RemoteBearerToken:    "remote-token-123",
		RemoteCloudListingID: remoteListingID.String(),
		IdempotencyKey:       "cross-route-123",
		Input:                map[string]any{"task": "route cross registry"},
		InputSummary:         "cross registry route",
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, remote.URL+"/api/v1", resp.RemoteAPIBaseURL)
	assert.Equal(t, "explicit", resp.RouteMode)
	assert.Empty(t, resp.RegistryPeerID)
	assert.Equal(t, remoteRunID.String(), resp.RemoteRun.ID)
	assert.Equal(t, "Bearer remote-token-123", capturedAuth)
	assert.Equal(t, remoteListingID.String(), capturedBody.CloudListingID)
	assert.Equal(t, "cross-route-123", capturedBody.IdempotencyKey)
	assert.Equal(t, "route cross registry", capturedBody.Input["task"])
	assert.Equal(t, "cross registry route", capturedBody.InputSummary)
}

func TestRegistryPeerRoutesRemoteProxyRunWithoutRepeatingCredentials(t *testing.T) {
	pool := setupRegistryBridgeDB(t)
	svc := registry.NewService(pool)
	ctx := context.Background()

	ownerID := insertRegistryOwner(t, pool)
	remoteListingID := uuid.New()
	remoteRunID := uuid.New()
	var capturedAuth string
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/api/v1/proxy/runs", r.URL.Path)
		capturedAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(registry.ProxyRunResponse{
			ID:                 remoteRunID.String(),
			CloudRunID:         uuid.NewString(),
			CloudListingLinkID: uuid.NewString(),
			CloudListingID:     remoteListingID.String(),
			RegistryNodeID:     uuid.NewString(),
			LocalAgentID:       uuid.NewString(),
			RequestingUserID:   ownerID.String(),
			IdempotencyKey:     "peer-route-123",
			Status:             "pending",
			PayloadPolicy:      "metadata_only",
			AttemptCount:       0,
			MaxAttempts:        3,
			CreatedAt:          time.Now().UTC().Format(time.RFC3339),
			UpdatedAt:          time.Now().UTC().Format(time.RFC3339),
		}))
	}))
	defer remote.Close()

	peer, err := svc.CreateRegistryPeer(ctx, ownerID, &registry.CreateRegistryPeerRequest{
		Name:        "Remote Registry",
		APIBaseURL:  remote.URL,
		BearerToken: "peer-token-123",
	})
	require.NoError(t, err)
	require.NotEmpty(t, peer.ID)
	assert.Equal(t, remote.URL+"/api/v1", peer.APIBaseURL)
	assert.Empty(t, peer.LastUsedAt)
	assert.NotContains(t, peer.CredentialHint, "peer-token-123")

	resp, err := svc.CreateRemoteProxyRun(ctx, ownerID, &registry.CreateRemoteProxyRunRequest{
		RegistryPeerID:       peer.ID,
		RemoteCloudListingID: remoteListingID.String(),
		IdempotencyKey:       "peer-route-123",
		Input:                map[string]any{"task": "route via trusted registry directory"},
	})
	require.NoError(t, err)
	assert.Equal(t, "registry_peer", resp.RouteMode)
	assert.Equal(t, peer.ID, resp.RegistryPeerID)
	assert.Equal(t, remoteRunID.String(), resp.RemoteRun.ID)
	assert.Equal(t, "Bearer peer-token-123", capturedAuth)

	autoResp, err := svc.CreateRemoteProxyRun(ctx, ownerID, &registry.CreateRemoteProxyRunRequest{
		RemoteCloudListingID: remoteListingID.String(),
		IdempotencyKey:       "peer-route-auto",
		Input:                map[string]any{"task": "auto route via the only active registry peer"},
	})
	require.NoError(t, err)
	assert.Equal(t, "registry_peer_auto", autoResp.RouteMode)
	assert.Equal(t, peer.ID, autoResp.RegistryPeerID)
	assert.Equal(t, remote.URL+"/api/v1", autoResp.RemoteAPIBaseURL)
	assert.Equal(t, "Bearer peer-token-123", capturedAuth)

	peers, err := svc.ListRegistryPeers(ctx, ownerID)
	require.NoError(t, err)
	require.Len(t, peers, 1)
	assert.NotEmpty(t, peers[0].LastUsedAt)

	_, err = svc.CreateRegistryPeer(ctx, ownerID, &registry.CreateRegistryPeerRequest{
		Name:        "Backup Remote Registry",
		APIBaseURL:  remote.URL,
		BearerToken: "peer-token-456",
	})
	require.NoError(t, err)
	_, err = svc.CreateRemoteProxyRun(ctx, ownerID, &registry.CreateRemoteProxyRunRequest{
		RemoteCloudListingID: remoteListingID.String(),
		IdempotencyKey:       "peer-route-ambiguous",
	})
	requireHTTPStatus(t, err, http.StatusConflict)

	err = svc.DeleteRegistryPeer(ctx, ownerID, uuid.MustParse(peer.ID))
	require.NoError(t, err)
	peers, err = svc.ListRegistryPeers(ctx, ownerID)
	require.NoError(t, err)
	require.Len(t, peers, 1)
}

func TestRegistryNodePeerAndRemoteRouteBoundaries(t *testing.T) {
	pool := setupRegistryBridgeDB(t)
	svc := registry.NewService(pool)
	ctx := context.Background()

	ownerID := insertRegistryOwner(t, pool)
	otherOwnerID := insertRegistryOwner(t, pool)
	agentID := insertRegistryAgent(t, pool, ownerID)

	_, err := svc.CreateNode(ctx, ownerID, &registry.CreateNodeRequest{
		NodeName: "X",
	})
	requireHTTPStatus(t, err, http.StatusUnprocessableEntity)
	_, err = svc.CreateNode(ctx, ownerID, &registry.CreateNodeRequest{
		NodeName: "Bad Type",
		NodeType: "invalid",
	})
	requireHTTPStatus(t, err, http.StatusUnprocessableEntity)
	_, err = svc.CreateNode(ctx, ownerID, &registry.CreateNodeRequest{
		NodeName: "Bad URL",
		BaseURL:  "ftp://node.example",
	})
	requireHTTPStatus(t, err, http.StatusUnprocessableEntity)
	_, err = svc.CreateNode(ctx, ownerID, &registry.CreateNodeRequest{
		NodeName: "Bad Scope",
		Scopes:   []string{"unknown"},
	})
	requireHTTPStatus(t, err, http.StatusUnprocessableEntity)

	limitedNode, err := svc.CreateNode(ctx, ownerID, &registry.CreateNodeRequest{
		NodeName: "Heartbeat Only",
		NodeType: "bridge_proxy",
		Scopes:   []string{"heartbeat"},
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"heartbeat"}, limitedNode.Scopes)

	heartbeat, err := svc.Heartbeat(ctx, limitedNode.NodeSecret)
	require.NoError(t, err)
	assert.Equal(t, limitedNode.ID, heartbeat.NodeID)
	_, err = svc.SyncNodeMetadata(ctx, limitedNode.NodeSecret)
	requireHTTPStatus(t, err, http.StatusUnauthorized)
	_, err = svc.ClaimProxyRun(ctx, limitedNode.NodeSecret)
	requireHTTPStatus(t, err, http.StatusUnauthorized)

	node, err := svc.CreateNode(ctx, ownerID, &registry.CreateNodeRequest{
		NodeName: "Boundary Bridge",
		NodeType: "bridge_proxy",
	})
	require.NoError(t, err)
	nodeID := uuid.MustParse(node.ID)

	_, err = svc.CreateCloudListing(ctx, ownerID, &registry.CreateCloudListingRequest{
		RegistryNodeID: "not-a-uuid",
		AgentID:        agentID.String(),
	})
	requireHTTPStatus(t, err, http.StatusBadRequest)
	_, err = svc.CreateCloudListing(ctx, ownerID, &registry.CreateCloudListingRequest{
		RegistryNodeID: node.ID,
		AgentID:        "not-a-uuid",
	})
	requireHTTPStatus(t, err, http.StatusBadRequest)
	_, err = svc.CreateCloudListing(ctx, otherOwnerID, &registry.CreateCloudListingRequest{
		RegistryNodeID: node.ID,
		AgentID:        agentID.String(),
	})
	requireHTTPStatus(t, err, http.StatusNotFound)

	listing, err := svc.CreateCloudListing(ctx, ownerID, &registry.CreateCloudListingRequest{
		RegistryNodeID: node.ID,
		AgentID:        agentID.String(),
		PayloadPolicy:  "metadata_only",
	})
	require.NoError(t, err)
	listingID := uuid.MustParse(listing.CloudListingID)

	_, err = svc.SyncCloudListingMetadata(ctx, otherOwnerID, listingID)
	requireHTTPStatus(t, err, http.StatusNotFound)
	_, err = svc.UpdateCloudListingStatus(ctx, otherOwnerID, listingID, &registry.UpdateCloudListingStatusRequest{
		SyncStatus: "paused",
	})
	requireHTTPStatus(t, err, http.StatusNotFound)
	_, err = svc.UpdateCloudListingStatus(ctx, ownerID, listingID, &registry.UpdateCloudListingStatusRequest{
		SyncStatus: "archived",
	})
	requireHTTPStatus(t, err, http.StatusUnprocessableEntity)

	peers, err := svc.ListRegistryPeers(ctx, ownerID)
	require.NoError(t, err)
	assert.Empty(t, peers)
	_, err = svc.CreateRegistryPeer(ctx, ownerID, &registry.CreateRegistryPeerRequest{
		Name:        "X",
		APIBaseURL:  "https://remote.example",
		BearerToken: "short",
	})
	requireHTTPStatus(t, err, http.StatusUnprocessableEntity)
	_, err = svc.CreateRegistryPeer(ctx, ownerID, &registry.CreateRegistryPeerRequest{
		Name:        "Bad URL Peer",
		APIBaseURL:  "ftp://remote.example",
		BearerToken: "peer-token-123",
	})
	requireHTTPStatus(t, err, http.StatusUnprocessableEntity)
	_, err = svc.CreateRegistryPeer(ctx, ownerID, &registry.CreateRegistryPeerRequest{
		Name:          "Bad Status Peer",
		APIBaseURL:    "https://remote.example",
		BearerToken:   "peer-token-123",
		InitialStatus: "disabled",
	})
	requireHTTPStatus(t, err, http.StatusUnprocessableEntity)
	pausedPeer, err := svc.CreateRegistryPeer(ctx, ownerID, &registry.CreateRegistryPeerRequest{
		Name:          "Paused Peer",
		APIBaseURL:    "https://remote.example",
		BearerToken:   "paused-token-123",
		InitialStatus: "paused",
	})
	require.NoError(t, err)
	assert.Equal(t, "paused", pausedPeer.Status)
	_, err = svc.CreateRemoteProxyRun(ctx, ownerID, &registry.CreateRemoteProxyRunRequest{
		RegistryPeerID:       pausedPeer.ID,
		RemoteCloudListingID: uuid.NewString(),
		IdempotencyKey:       "remote-paused-peer",
	})
	requireHTTPStatus(t, err, http.StatusNotFound)
	_, err = svc.CreateRemoteProxyRun(ctx, ownerID, &registry.CreateRemoteProxyRunRequest{
		RegistryPeerID:       pausedPeer.ID,
		RemoteAPIBaseURL:     "https://remote.example",
		RemoteBearerToken:    "explicit-token-123",
		RemoteCloudListingID: uuid.NewString(),
		IdempotencyKey:       "remote-peer-mixed",
	})
	requireHTTPStatus(t, err, http.StatusUnprocessableEntity)
	_, err = svc.CreateRemoteProxyRun(ctx, ownerID, &registry.CreateRemoteProxyRunRequest{
		RemoteCloudListingID: uuid.NewString(),
		IdempotencyKey:       "remote-no-active-peer",
	})
	requireHTTPStatus(t, err, http.StatusUnprocessableEntity)

	err = svc.DeleteRegistryPeer(ctx, otherOwnerID, uuid.MustParse(pausedPeer.ID))
	requireHTTPStatus(t, err, http.StatusNotFound)
	require.NoError(t, svc.DeleteRegistryPeer(ctx, ownerID, uuid.MustParse(pausedPeer.ID)))
	err = svc.DeleteRegistryPeer(ctx, ownerID, uuid.MustParse(pausedPeer.ID))
	requireHTTPStatus(t, err, http.StatusNotFound)

	_, err = svc.CreateRegistryFederationInvite(ctx, ownerID, &registry.CreateRegistryFederationInviteRequest{
		Name:             "X",
		APIBaseURL:       "https://peer.example",
		BearerToken:      "federation-peer-token-123",
		ExpiresInSeconds: 120,
	})
	requireHTTPStatus(t, err, http.StatusUnprocessableEntity)
	_, err = svc.CreateRegistryFederationInvite(ctx, ownerID, &registry.CreateRegistryFederationInviteRequest{
		Name:             "Bad Federation URL",
		APIBaseURL:       "ftp://peer.example",
		BearerToken:      "federation-peer-token-123",
		ExpiresInSeconds: 120,
	})
	requireHTTPStatus(t, err, http.StatusUnprocessableEntity)
	_, err = svc.CreateRegistryFederationInvite(ctx, ownerID, &registry.CreateRegistryFederationInviteRequest{
		Name:             "Short Federation Token",
		APIBaseURL:       "https://peer.example",
		BearerToken:      "short",
		ExpiresInSeconds: 120,
	})
	requireHTTPStatus(t, err, http.StatusUnprocessableEntity)
	_, err = svc.CreateRegistryFederationInvite(ctx, ownerID, &registry.CreateRegistryFederationInviteRequest{
		Name:             "Short Federation TTL",
		APIBaseURL:       "https://peer.example",
		BearerToken:      "federation-peer-token-123",
		ExpiresInSeconds: 30,
	})
	requireHTTPStatus(t, err, http.StatusUnprocessableEntity)
	_, err = svc.ConsumeRegistryFederationInvite(ctx, &registry.ConsumeRegistryFederationInviteRequest{
		FederationToken: "bad-token",
	})
	requireHTTPStatus(t, err, http.StatusUnauthorized)
	expiringInvite, err := svc.CreateRegistryFederationInvite(ctx, ownerID, &registry.CreateRegistryFederationInviteRequest{
		Name:             "Expiring Federation",
		APIBaseURL:       "https://peer.example",
		BearerToken:      "federation-peer-token-123",
		ExpiresInSeconds: 60,
	})
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `UPDATE registry_federation_invites SET expires_at = NOW() - INTERVAL '1 second' WHERE id = $1`, expiringInvite.ID)
	require.NoError(t, err)
	_, err = svc.ConsumeRegistryFederationInvite(ctx, &registry.ConsumeRegistryFederationInviteRequest{
		FederationToken: expiringInvite.FederationToken,
	})
	requireHTTPStatus(t, err, http.StatusUnauthorized)

	_, err = svc.RevokeNode(ctx, otherOwnerID, nodeID)
	requireHTTPStatus(t, err, http.StatusNotFound)
	_, err = svc.RotateNodeSecret(ctx, otherOwnerID, nodeID)
	requireHTTPStatus(t, err, http.StatusNotFound)
	revoked, err := svc.RevokeNode(ctx, ownerID, nodeID)
	require.NoError(t, err)
	assert.Equal(t, "revoked", revoked.HeartbeatStatus)
	require.NotEmpty(t, revoked.RevokedAt)
	_, err = svc.Heartbeat(ctx, node.NodeSecret)
	requireHTTPStatus(t, err, http.StatusUnauthorized)
	_, err = svc.CreateCloudListing(ctx, ownerID, &registry.CreateCloudListingRequest{
		RegistryNodeID: node.ID,
		AgentID:        agentID.String(),
	})
	requireHTTPStatus(t, err, http.StatusConflict)
}

func TestCreateRemoteProxyRunHandlesRemoteFailures(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
		want    int
	}{
		{
			name: "non_2xx_status_maps_to_bad_gateway",
			handler: func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodPost, r.Method)
				assert.Equal(t, "/api/v1/proxy/runs", r.URL.Path)
				assert.Equal(t, "Bearer explicit-token-123", r.Header.Get("Authorization"))
				http.Error(w, "remote queue saturated", http.StatusTooManyRequests)
			},
			want: http.StatusBadGateway,
		},
		{
			name: "invalid_json_maps_to_service_unavailable",
			handler: func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodPost, r.Method)
				assert.Equal(t, "/api/v1/proxy/runs", r.URL.Path)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte("{"))
			},
			want: http.StatusServiceUnavailable,
		},
		{
			name: "missing_remote_identifiers_maps_to_service_unavailable",
			handler: func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodPost, r.Method)
				assert.Equal(t, "/api/v1/proxy/runs", r.URL.Path)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusCreated)
				require.NoError(t, json.NewEncoder(w).Encode(registry.ProxyRunResponse{
					Status: "pending",
				}))
			},
			want: http.StatusServiceUnavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			remote := httptest.NewServer(tt.handler)
			defer remote.Close()

			svc := registry.NewService(nil)
			_, err := svc.CreateRemoteProxyRun(context.Background(), uuid.Nil, &registry.CreateRemoteProxyRunRequest{
				RemoteAPIBaseURL:     remote.URL,
				RemoteBearerToken:    "explicit-token-123",
				RemoteCloudListingID: uuid.NewString(),
				IdempotencyKey:       "remote-failure-" + tt.name,
				Input:                map[string]any{"task": "remote failure"},
			})
			requireHTTPStatus(t, err, tt.want)
		})
	}
}

func TestRegistryFederationInviteExchangeCreatesPeer(t *testing.T) {
	pool := setupRegistryBridgeDB(t)
	svc := registry.NewService(pool)
	ctx := context.Background()

	ownerID := insertRegistryOwner(t, pool)
	var capturedExchangeToken string
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/api/v1/registry-peers/federation-invitations/exchange", r.URL.Path)
		assert.Empty(t, r.Header.Get("Authorization"))

		var req registry.ConsumeRegistryFederationInviteRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		capturedExchangeToken = req.FederationToken

		material, err := svc.ConsumeRegistryFederationInvite(ctx, &req)
		if err != nil {
			http.Error(w, "federation exchange failed", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(material))
	}))
	defer remote.Close()

	directInvite, err := svc.CreateRegistryFederationInvite(ctx, ownerID, &registry.CreateRegistryFederationInviteRequest{
		Name:             "Direct Federation",
		APIBaseURL:       remote.URL,
		BearerToken:      "direct-peer-token-123",
		ExpiresInSeconds: 120,
	})
	require.NoError(t, err)
	require.NotEmpty(t, directInvite.FederationToken)
	assert.Equal(t, "active", directInvite.Status)
	assert.Equal(t, remote.URL+"/api/v1", directInvite.APIBaseURL)
	assert.Equal(t, remote.URL+"/api/v1/registry-peers/federation-invitations/exchange", directInvite.ExchangeURL)
	assert.NotContains(t, directInvite.CredentialHint, "direct-peer-token-123")

	material, err := svc.ConsumeRegistryFederationInvite(ctx, &registry.ConsumeRegistryFederationInviteRequest{
		FederationToken: directInvite.FederationToken,
	})
	require.NoError(t, err)
	assert.Equal(t, "Direct Federation", material.Name)
	assert.Equal(t, remote.URL+"/api/v1", material.APIBaseURL)
	assert.Equal(t, "direct-peer-token-123", material.BearerToken)
	assert.Equal(t, directInvite.CredentialHint, material.CredentialHint)

	_, err = svc.ConsumeRegistryFederationInvite(ctx, &registry.ConsumeRegistryFederationInviteRequest{
		FederationToken: directInvite.FederationToken,
	})
	requireHTTPStatus(t, err, http.StatusUnauthorized)

	exchangeInvite, err := svc.CreateRegistryFederationInvite(ctx, ownerID, &registry.CreateRegistryFederationInviteRequest{
		Name:             "Remote Registry",
		APIBaseURL:       remote.URL,
		BearerToken:      "exchange-peer-token-123",
		ExpiresInSeconds: 120,
	})
	require.NoError(t, err)
	exchanged, err := svc.ExchangeRegistryFederationInvite(ctx, ownerID, &registry.ExchangeRegistryFederationInviteRequest{
		ExchangeURL:     exchangeInvite.ExchangeURL,
		FederationToken: exchangeInvite.FederationToken,
		Name:            "Federated Peer",
		InitialStatus:   "active",
	})
	require.NoError(t, err)
	assert.Equal(t, exchangeInvite.FederationToken, capturedExchangeToken)
	assert.Equal(t, exchangeInvite.ExchangeURL, exchanged.ExchangeURL)
	assert.Equal(t, exchangeInvite.CredentialHint, exchanged.RemoteCredentialHint)
	assert.Equal(t, "Federated Peer", exchanged.Peer.Name)
	assert.Equal(t, remote.URL+"/api/v1", exchanged.Peer.APIBaseURL)
	assert.Equal(t, "active", exchanged.Peer.Status)
	assert.NotContains(t, exchanged.Peer.CredentialHint, "exchange-peer-token-123")

	peers, err := svc.ListRegistryPeers(ctx, ownerID)
	require.NoError(t, err)
	require.Len(t, peers, 1)
	assert.Equal(t, exchanged.Peer.ID, peers[0].ID)
}

func TestProxyRunPayloadPolicies(t *testing.T) {
	pool := setupRegistryBridgeDB(t)
	svc := registry.NewService(pool)
	ctx := context.Background()

	ownerID := insertRegistryOwner(t, pool)
	node, err := svc.CreateNode(ctx, ownerID, &registry.CreateNodeRequest{
		NodeName: "Policy Bridge",
		NodeType: "bridge_proxy",
	})
	require.NoError(t, err)

	summaryAgentID := insertRegistryAgent(t, pool, ownerID)
	summaryListing, err := svc.CreateCloudListing(ctx, ownerID, &registry.CreateCloudListingRequest{
		RegistryNodeID: node.ID,
		AgentID:        summaryAgentID.String(),
		PayloadPolicy:  "store_run_summary",
	})
	require.NoError(t, err)

	summaryRun, err := svc.CreateProxyRun(ctx, ownerID, &registry.CreateProxyRunRequest{
		CloudListingID: summaryListing.CloudListingID,
		IdempotencyKey: "summary-" + uuid.NewString(),
		Input: map[string]any{
			"prompt": "contains private customer payload",
			"secret": "do-not-store",
		},
		InputSummary: "private payload task",
	})
	require.NoError(t, err)
	assert.Equal(t, "store_run_summary", summaryRun.PayloadPolicy)
	assert.Nil(t, summaryRun.Input)
	assert.Equal(t, "private payload task", summaryRun.InputSummary)

	claimedSummary, err := svc.ClaimProxyRun(ctx, node.NodeSecret)
	require.NoError(t, err)
	require.NotNil(t, claimedSummary)
	assert.Equal(t, "contains private customer payload", claimedSummary.Input["prompt"])
	assert.Equal(t, "do-not-store", claimedSummary.Input["secret"])

	summaryRunID, err := uuid.Parse(summaryRun.ID)
	require.NoError(t, err)
	completedSummary, err := svc.CompleteProxyRun(ctx, node.NodeSecret, summaryRunID, &registry.CompleteProxyRunRequest{
		Status: "success",
		Output: map[string]any{
			"raw": "private result body",
		},
		OutputSummary: "private result summarized",
	})
	require.NoError(t, err)
	assert.Nil(t, completedSummary.Output)
	assert.Equal(t, "private result summarized", completedSummary.OutputSummary)

	fetchedSummary, err := svc.GetProxyRun(ctx, ownerID, summaryRunID)
	require.NoError(t, err)
	assert.Nil(t, fetchedSummary.Input)
	assert.Equal(t, "private payload task", fetchedSummary.InputSummary)
	assert.Nil(t, fetchedSummary.Output)
	assert.Equal(t, "private result summarized", fetchedSummary.OutputSummary)

	fullAgentID := insertRegistryAgent(t, pool, ownerID)
	fullListing, err := svc.CreateCloudListing(ctx, ownerID, &registry.CreateCloudListingRequest{
		RegistryNodeID:       node.ID,
		AgentID:              fullAgentID.String(),
		PayloadPolicy:        "store_full_payload",
		PayloadRedactionKeys: []string{"secret", "apiKey"},
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"secret", "apiKey"}, fullListing.PayloadRedactionKeys)

	fullRun, err := svc.CreateProxyRun(ctx, ownerID, &registry.CreateProxyRunRequest{
		CloudListingID: fullListing.CloudListingID,
		IdempotencyKey: "full-" + uuid.NewString(),
		Input: map[string]any{
			"prompt": "store this payload",
			"secret": "do not persist",
			"nested": map[string]any{
				"apiKey": "nested secret",
			},
		},
		InputSummary: "full payload task",
	})
	require.NoError(t, err)
	assert.Equal(t, "store_full_payload", fullRun.PayloadPolicy)
	assert.Equal(t, "store this payload", fullRun.Input["prompt"])
	assert.Equal(t, "[redacted]", fullRun.Input["secret"])
	require.IsType(t, map[string]any{}, fullRun.Input["nested"])
	assert.Equal(t, "[redacted]", fullRun.Input["nested"].(map[string]any)["apiKey"])
	assert.Equal(t, "full payload task", fullRun.InputSummary)

	claimedFull, err := svc.ClaimProxyRun(ctx, node.NodeSecret)
	require.NoError(t, err)
	require.NotNil(t, claimedFull)
	assert.Equal(t, fullRun.ID, claimedFull.ID)
	assert.Equal(t, "store this payload", claimedFull.Input["prompt"])
	assert.Equal(t, "do not persist", claimedFull.Input["secret"])
	require.IsType(t, map[string]any{}, claimedFull.Input["nested"])
	assert.Equal(t, "nested secret", claimedFull.Input["nested"].(map[string]any)["apiKey"])

	fullRunID, err := uuid.Parse(fullRun.ID)
	require.NoError(t, err)
	completedFull, err := svc.CompleteProxyRun(ctx, node.NodeSecret, fullRunID, &registry.CompleteProxyRunRequest{
		Status: "success",
		Output: map[string]any{
			"raw":    "stored result body",
			"secret": "output secret",
			"nested": map[string]any{
				"apiKey": "output nested secret",
			},
			"artifacts": []map[string]any{{
				"id":            "full-json",
				"title":         "Full JSON Result",
				"artifact_type": "data",
				"content": map[string]any{
					"summary": "stored artifact content",
				},
			}},
		},
		OutputSummary: "full result summary",
	})
	require.NoError(t, err)
	assert.Equal(t, "stored result body", completedFull.Output["raw"])
	assert.Equal(t, "[redacted]", completedFull.Output["secret"])
	require.IsType(t, map[string]any{}, completedFull.Output["nested"])
	assert.Equal(t, "[redacted]", completedFull.Output["nested"].(map[string]any)["apiKey"])
	assert.Equal(t, "full result summary", completedFull.OutputSummary)
	fullArtifacts, err := svc.ListProxyRunArtifacts(ctx, ownerID, fullRunID)
	require.NoError(t, err)
	require.Len(t, fullArtifacts, 1)
	assert.Equal(t, "full-json", fullArtifacts[0].SourceArtifactID)
	assert.Equal(t, "data", fullArtifacts[0].ArtifactType)
	assert.Equal(t, "stored artifact content", fullArtifacts[0].Content["summary"])
}

func TestProxyRunRoutesAcrossMultipleHealthyNodesByLoad(t *testing.T) {
	pool := setupRegistryBridgeDB(t)
	svc := registry.NewService(pool)
	ctx := context.Background()

	ownerID := insertRegistryOwner(t, pool)
	agentID := insertRegistryAgent(t, pool, ownerID)
	nodeA, err := svc.CreateNode(ctx, ownerID, &registry.CreateNodeRequest{
		NodeName: "Bridge A",
		NodeType: "bridge_proxy",
	})
	require.NoError(t, err)
	nodeB, err := svc.CreateNode(ctx, ownerID, &registry.CreateNodeRequest{
		NodeName: "Bridge B",
		NodeType: "bridge_proxy",
	})
	require.NoError(t, err)
	_, err = svc.Heartbeat(ctx, nodeB.NodeSecret)
	require.NoError(t, err)
	_, err = svc.Heartbeat(ctx, nodeA.NodeSecret)
	require.NoError(t, err)

	listingA, err := svc.CreateCloudListing(ctx, ownerID, &registry.CreateCloudListingRequest{
		RegistryNodeID: nodeA.ID,
		AgentID:        agentID.String(),
		PayloadPolicy:  "store_run_summary",
	})
	require.NoError(t, err)
	listingB, err := svc.CreateCloudListing(ctx, ownerID, &registry.CreateCloudListingRequest{
		CloudListingID: listingA.CloudListingID,
		RegistryNodeID: nodeB.ID,
		AgentID:        agentID.String(),
		PayloadPolicy:  "store_run_summary",
	})
	require.NoError(t, err)
	assert.Equal(t, listingA.CloudListingID, listingB.CloudListingID)
	assert.NotEqual(t, listingA.ID, listingB.ID)

	first, err := svc.CreateProxyRun(ctx, ownerID, &registry.CreateProxyRunRequest{
		CloudListingID: listingA.CloudListingID,
		IdempotencyKey: "multi-" + uuid.NewString(),
		Input: map[string]any{
			"task": "first run keeps its node busy",
		},
		InputSummary: "first",
	})
	require.NoError(t, err)
	assert.Equal(t, "pending", first.Status)

	second, err := svc.CreateProxyRun(ctx, ownerID, &registry.CreateProxyRunRequest{
		CloudListingID: listingA.CloudListingID,
		IdempotencyKey: "multi-" + uuid.NewString(),
		Input: map[string]any{
			"task": "second run should route away from loaded node",
		},
		InputSummary: "second",
	})
	require.NoError(t, err)
	assert.Equal(t, "pending", second.Status)
	assert.NotEqual(t, first.RegistryNodeID, second.RegistryNodeID)
	assert.Equal(t, first.CloudListingID, second.CloudListingID)

	again, err := svc.CreateProxyRun(ctx, ownerID, &registry.CreateProxyRunRequest{
		CloudListingID: listingA.CloudListingID,
		IdempotencyKey: first.IdempotencyKey,
		Input: map[string]any{
			"task": "idempotent duplicate should keep the original node",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, first.ID, again.ID)
	assert.Equal(t, first.RegistryNodeID, again.RegistryNodeID)

	claimByNode := map[string]*registry.ProxyRunResponse{}
	for _, node := range []*registry.RegistryNodeResponse{nodeA, nodeB} {
		claimed, err := svc.ClaimProxyRun(ctx, node.NodeSecret)
		require.NoError(t, err)
		require.NotNil(t, claimed)
		claimByNode[node.ID] = claimed
	}
	assert.Equal(t, first.ID, claimByNode[first.RegistryNodeID].ID)
	assert.Equal(t, second.ID, claimByNode[second.RegistryNodeID].ID)
}

func TestProxyRunRetryableFailureRequeues(t *testing.T) {
	pool := setupRegistryBridgeDB(t)
	svc := registry.NewService(pool)
	ctx := context.Background()

	ownerID := insertRegistryOwner(t, pool)
	agentID := insertRegistryAgent(t, pool, ownerID)
	node, err := svc.CreateNode(ctx, ownerID, &registry.CreateNodeRequest{
		NodeName: "Retry Bridge",
		NodeType: "bridge_proxy",
	})
	require.NoError(t, err)
	listing, err := svc.CreateCloudListing(ctx, ownerID, &registry.CreateCloudListingRequest{
		RegistryNodeID: node.ID,
		AgentID:        agentID.String(),
		PayloadPolicy:  "metadata_only",
	})
	require.NoError(t, err)

	proxyRun, err := svc.CreateProxyRun(ctx, ownerID, &registry.CreateProxyRunRequest{
		CloudListingID: listing.CloudListingID,
		IdempotencyKey: "retry-" + uuid.NewString(),
		Input: map[string]any{
			"task": "retry me once",
		},
	})
	require.NoError(t, err)

	firstClaim, err := svc.ClaimProxyRun(ctx, node.NodeSecret)
	require.NoError(t, err)
	require.NotNil(t, firstClaim)
	assert.Equal(t, proxyRun.ID, firstClaim.ID)
	assert.Equal(t, "retry me once", firstClaim.Input["task"])
	assert.Equal(t, int32(1), firstClaim.AttemptCount)

	runID, err := uuid.Parse(proxyRun.ID)
	require.NoError(t, err)
	requeued, err := svc.CompleteProxyRun(ctx, node.NodeSecret, runID, &registry.CompleteProxyRunRequest{
		Status:       "failed",
		ErrorCode:    "LOCAL_AGENT_UNREACHABLE",
		ErrorMessage: "temporary network failure",
		Retryable:    true,
	})
	require.NoError(t, err)
	assert.Equal(t, "pending", requeued.Status)
	assert.Equal(t, int32(1), requeued.AttemptCount)
	assert.Empty(t, requeued.FinishedAt)
	assert.Equal(t, "LOCAL_AGENT_UNREACHABLE", requeued.ErrorCode)

	secondClaim, err := svc.ClaimProxyRun(ctx, node.NodeSecret)
	require.NoError(t, err)
	require.NotNil(t, secondClaim)
	assert.Equal(t, proxyRun.ID, secondClaim.ID)
	assert.Equal(t, "retry me once", secondClaim.Input["task"])
	assert.Equal(t, int32(2), secondClaim.AttemptCount)

	completed, err := svc.CompleteProxyRun(ctx, node.NodeSecret, runID, &registry.CompleteProxyRunRequest{
		Status: "success",
		Output: map[string]any{
			"summary": "completed after retry",
		},
		OutputSummary: "completed after retry",
	})
	require.NoError(t, err)
	assert.Equal(t, "success", completed.Status)
	assert.Equal(t, int32(2), completed.AttemptCount)
	assert.Empty(t, completed.ErrorCode)
	assert.Nil(t, completed.Output)

	fetched, err := svc.GetProxyRun(ctx, ownerID, runID)
	require.NoError(t, err)
	assert.Equal(t, "success", fetched.Status)
	assert.Equal(t, int32(2), fetched.AttemptCount)
	assert.Nil(t, fetched.Input)
	assert.Nil(t, fetched.Output)
	var nodeInputStillStored bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT node_input IS NOT NULL FROM proxy_runs WHERE id=$1`, runID).Scan(&nodeInputStillStored))
	assert.False(t, nodeInputStillStored)
}
