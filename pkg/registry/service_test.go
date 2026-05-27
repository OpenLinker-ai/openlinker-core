package registry_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kinzhi/openlinker-core/pkg/registry"
)

const truncateRegistryBridgeTables = "TRUNCATE proxy_runs, cloud_listing_links, registry_nodes, agent_skills, agents, wallets, users RESTART IDENTITY CASCADE"

func setupRegistryBridgeDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL 未设置，跳过 registry 集成测试")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	require.NoError(t, pool.Ping(ctx))
	_, err = pool.Exec(ctx, truncateRegistryBridgeTables)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanCancel()
		_, _ = pool.Exec(cleanCtx, truncateRegistryBridgeTables)
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
	assert.Equal(t, "linked", listing.SyncStatus)
	assert.Equal(t, "pull_proxy", listing.RoutingMode)
	assert.Equal(t, "metadata_only", listing.PayloadPolicy)

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
	assert.Equal(t, "run SQL and summarize", proxyRun.Input["task"])

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

	heartbeat, err = svc.Heartbeat(ctx, node.NodeSecret)
	require.NoError(t, err)
	assert.Equal(t, int32(0), heartbeat.PendingRunCount)

	runID, err := uuid.Parse(proxyRun.ID)
	require.NoError(t, err)
	completed, err := svc.CompleteProxyRun(ctx, node.NodeSecret, runID, &registry.CompleteProxyRunRequest{
		Status: "success",
		Output: map[string]any{
			"summary": "SQL 查询完成，发现订单数为 42",
		},
		OutputSummary: "SQL 查询完成",
	})
	require.NoError(t, err)
	assert.Equal(t, "success", completed.Status)
	assert.Equal(t, "SQL 查询完成", completed.OutputSummary)
	assert.Equal(t, "SQL 查询完成，发现订单数为 42", completed.Output["summary"])
	assert.NotEmpty(t, completed.FinishedAt)

	fetched, err := svc.GetProxyRun(ctx, ownerID, runID)
	require.NoError(t, err)
	assert.Equal(t, "success", fetched.Status)
	assert.Equal(t, "SQL 查询完成", fetched.OutputSummary)

	emptyClaim, err := svc.ClaimProxyRun(ctx, node.NodeSecret)
	require.NoError(t, err)
	assert.Nil(t, emptyClaim)
}
