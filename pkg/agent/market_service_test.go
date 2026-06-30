// Package agent_test - MarketService 集成测试。
//
// 这些测试需要真实 Postgres，通过环境变量 TEST_DATABASE_URL 提供。
// 未设置则 t.Skip()。
//
// 实际 API（与 internal/agent/market_service.go 对齐）：
//
//	type MarketService struct{ ... }
//	func NewMarketService(pool *pgxpool.Pool) *MarketService
//	func (s *MarketService) ListMarket(ctx context.Context, tags []string, keyword string, page, size int32) (*MarketListResponse, error)
//	func (s *MarketService) GetBySlug(ctx context.Context, slug string) (*AgentDetailResponse, error)
//
//	type MarketListItem struct {
//	    ID, Slug, Name, Description string
//	    PricePerCallCents int32
//	    Tags              []string
//	    TotalCalls        int32
//	    Creator           CreatorMini  // {DisplayName}
//	}
//	type MarketListResponse struct { Items []MarketListItem; Total, Page, Size int32 }
//	type AgentDetailResponse struct {
//	    ID, Slug, Name, Description, EndpointURL string
//	    PricePerCallCents int32
//	    Tags              []string
//	    TotalCalls        int32
//	    Creator           CreatorMini
//	    CreatedAt         string
//	    ApprovedAt        *string
//	    // 永远不返回 EndpointAuthHeader
//	}
//
// 错误：service 层返回 *httpx.HTTPError，未找到 -> 404 NotFound。
package agent_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

// assertHTTPStatus 用 errors.As 断言 *httpx.HTTPError 的 status。
func assertHTTPStatus(t *testing.T, err error, want int) {
	t.Helper()
	require.Error(t, err)
	var he *httpx.HTTPError
	require.True(t, errors.As(err, &he), "expected *httpx.HTTPError, got %T (%v)", err, err)
	assert.Equal(t, want, he.Status)
}

func TestAgentCardOpenLinkerExtSerializesReadinessAvailabilityAndRuntime(t *testing.T) {
	ext := agent.AgentCardOpenLinkerExt{
		AgentID:            uuid.NewString(),
		Slug:               "native-a2a",
		AvailabilityStatus: "healthy",
		Readiness: agent.Readiness{
			Listed:             true,
			Discoverable:       true,
			Callable:           true,
			AvailabilityStatus: "healthy",
		},
		Availability: agent.Availability{
			Status: "healthy",
			Label:  "可用",
			Hint:   "最近一次真实调用成功，当前可用性良好。",
		},
		Runtime: agent.AgentCardRuntimeExt{
			Adapter:        "openlinker_a2a_proxy",
			ConnectionMode: "runtime_pull",
			OnlineSignal:   "runtime_pull_heartbeat_claim_result",
			TaskLifecycle:  "openlinker_run_task_lifecycle",
		},
	}

	raw, err := json.Marshal(ext)
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Contains(t, decoded, "readiness")
	require.Contains(t, decoded, "availability")
	require.Contains(t, decoded, "runtime")

	runtimeContract := decoded["runtime"].(map[string]any)
	assert.Equal(t, "openlinker_a2a_proxy", runtimeContract["adapter"])
	assert.Equal(t, "runtime_pull", runtimeContract["connection_mode"])
	assert.Equal(t, "runtime_pull_heartbeat_claim_result", runtimeContract["online_signal"])
	assert.Equal(t, "openlinker_run_task_lifecycle", runtimeContract["task_lifecycle"])
}

// ─────────────────────────────────────────────────────────
// ListMarket
// ─────────────────────────────────────────────────────────

func TestListMarket_HappyPath(t *testing.T) {
	pool := setupTestDB(t)
	svc := agent.NewMarketService(pool)
	creatorID, _ := setupTestData(t, pool)

	for i := 0; i < 5; i++ {
		createApprovedAgent(t, pool, creatorID, slugN("happy", i))
	}

	resp, err := svc.ListMarket(context.Background(), nil, "", 1, 12)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Len(t, resp.Items, 5, "should return all 5 approved agents")
	assert.Equal(t, int32(5), resp.Total, "total should be 5")
	for _, item := range resp.Items {
		assert.True(t, item.Readiness.Listed)
		assert.True(t, item.Readiness.Discoverable)
		assert.False(t, item.Readiness.Callable, "no run evidence yet")
		assert.False(t, item.Readiness.Verified, "no benchmark evidence yet")
		assert.False(t, item.Readiness.Certified)
		assert.False(t, item.Readiness.PaidEnabled)
		assert.Equal(t, "/api/v1/agents/"+item.Slug+"/agent-card.json", item.Readiness.AgentCardURL)
		assert.Equal(t, item.Availability.Status, item.Readiness.AvailabilityStatus)
	}
}

func TestListMarket_SortsByAvailability(t *testing.T) {
	pool := setupTestDB(t)
	svc := agent.NewMarketService(pool)
	creatorID, _ := setupTestData(t, pool)
	ctx := context.Background()

	createApprovedAgent(t, pool, creatorID, "sort-unreachable")
	createApprovedAgent(t, pool, creatorID, "sort-healthy")
	createApprovedAgent(t, pool, creatorID, "sort-unknown")

	statusBySlug := map[string]string{
		"sort-unreachable": "unreachable",
		"sort-healthy":     "healthy",
	}
	for slug, status := range statusBySlug {
		var agentID uuid.UUID
		require.NoError(t, pool.QueryRow(ctx, `SELECT id FROM agents WHERE slug=$1`, slug).Scan(&agentID))
		_, err := pool.Exec(ctx,
			`INSERT INTO agent_availability_snapshots (
				agent_id, availability_status, last_checked_at, consecutive_failures
			) VALUES ($1, $2, NOW(), CASE WHEN $2 = 'unreachable' THEN 3 ELSE 0 END)`,
			agentID,
			status,
		)
		require.NoError(t, err)
	}

	resp, err := svc.ListMarket(ctx, nil, "", 1, 12)
	require.NoError(t, err)
	require.Len(t, resp.Items, 3)
	assert.Equal(t, "sort-healthy", resp.Items[0].Slug)
	assert.Equal(t, "sort-unreachable", resp.Items[2].Slug)
}

func TestListMarket_CallableOnlyFiltersUncallable(t *testing.T) {
	pool := setupTestDB(t)
	svc := agent.NewMarketService(pool)
	creatorID, _ := setupTestData(t, pool)
	ctx := context.Background()

	healthyID := createApprovedAgent(t, pool, creatorID, "callable-healthy")
	createApprovedAgent(t, pool, creatorID, "callable-unknown")
	unreachableID := createApprovedAgent(t, pool, creatorID, "callable-unreachable")
	_, err := pool.Exec(ctx,
		`INSERT INTO agent_availability_snapshots (
			agent_id, availability_status, last_checked_at, consecutive_failures
		) VALUES
			($1, 'healthy', NOW(), 0),
			($2, 'unreachable', NOW(), 3)`,
		healthyID,
		unreachableID,
	)
	require.NoError(t, err)

	resp, err := svc.ListMarket(ctx, nil, "", 1, 12, true)
	require.NoError(t, err)
	require.Len(t, resp.Items, 1)
	assert.Equal(t, "callable-healthy", resp.Items[0].Slug)
	assert.True(t, resp.Items[0].Readiness.Callable)
	assert.Equal(t, int32(1), resp.Total)
}

func TestListMarket_FiltersInternalValidationAndTestAgents(t *testing.T) {
	pool := setupTestDB(t)
	svc := agent.NewMarketService(pool)
	creatorID, _ := setupTestData(t, pool)
	ctx := context.Background()

	publicID := createApprovedAgent(t, pool, creatorID, "public-callable")
	internalID := createApprovedAgent(t, pool, creatorID, "internal-callable", WithTags([]string{"internal"}))
	validationID := createApprovedAgent(t, pool, creatorID, "validation-callable", WithTags([]string{"validation"}))
	testID := createApprovedAgent(t, pool, creatorID, "test-callable", WithTags([]string{"测试"}))

	_, err := pool.Exec(ctx,
		`INSERT INTO agent_availability_snapshots (
			agent_id, availability_status, last_checked_at, consecutive_failures
		) VALUES
			($1, 'healthy', NOW(), 0),
			($2, 'healthy', NOW(), 0),
			($3, 'healthy', NOW(), 0),
			($4, 'healthy', NOW(), 0)`,
		publicID,
		internalID,
		validationID,
		testID,
	)
	require.NoError(t, err)

	resp, err := svc.ListMarket(ctx, nil, "", 1, 12, true)
	require.NoError(t, err)
	require.Len(t, resp.Items, 1)
	assert.Equal(t, "public-callable", resp.Items[0].Slug)
	assert.Equal(t, int32(1), resp.Total)

	resp, err = svc.ListMarket(ctx, []string{"validation"}, "", 1, 12)
	require.NoError(t, err)
	assert.Empty(t, resp.Items, "reserved validation/test/internal tags must not leak through tag search")
	assert.Equal(t, int32(0), resp.Total)
}

func TestListMarket_RuntimePullWithoutRecentWorkerShownUnreachable(t *testing.T) {
	pool := setupTestDB(t)
	svc := agent.NewMarketService(pool)
	creatorID, _ := setupTestData(t, pool)
	ctx := context.Background()

	agentID := createApprovedAgent(t, pool, creatorID, "runtime-offline")
	_, err := pool.Exec(ctx,
		`UPDATE agents
		 SET connection_mode='runtime_pull',
		     endpoint_url=$2
		 WHERE id=$1`,
		agentID,
		"openlinker-runtime-pull://runtime-offline",
	)
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`INSERT INTO agent_availability_snapshots (
			agent_id, availability_status, last_successful_run_at, last_checked_at, consecutive_failures
		) VALUES ($1, 'healthy', NOW(), NOW(), 0)`,
		agentID,
	)
	require.NoError(t, err)

	resp, err := svc.ListMarket(ctx, nil, "runtime-offline", 1, 12)
	require.NoError(t, err)
	require.Len(t, resp.Items, 1)
	assert.Equal(t, "unreachable", resp.Items[0].Availability.Status)
	assert.Contains(t, resp.Items[0].Availability.Hint, "运行时心跳")
	assert.False(t, resp.Items[0].Readiness.Callable)

	createApprovedAgent(t, pool, creatorID, "runtime-direct-fallback")
	resp, err = svc.ListMarket(ctx, nil, "", 1, 12)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(resp.Items), 2)
	assert.Equal(t, "runtime-direct-fallback", resp.Items[0].Slug)
	assert.Equal(t, "runtime-offline", resp.Items[len(resp.Items)-1].Slug)

	_, err = pool.Exec(ctx,
		`INSERT INTO agent_runtime_tokens (
			agent_id, created_by_user_id, name, prefix, token_hash, scopes, last_used_at
		) VALUES ($1, $2, 'worker', 'rt_live_aabbccdd', 'hash', ARRAY['agent:pull']::text[], NOW())`,
		agentID,
		creatorID,
	)
	require.NoError(t, err)

	detail, err := svc.GetBySlug(ctx, "runtime-offline")
	require.NoError(t, err)
	assert.Equal(t, "healthy", detail.Availability.Status)
	assert.True(t, detail.Readiness.Callable)

	resp, err = svc.ListMarket(ctx, nil, "", 1, 12)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(resp.Items), 2)
	assert.Equal(t, "runtime-offline", resp.Items[0].Slug)
}

func TestListMarket_FilterByTags(t *testing.T) {
	pool := setupTestDB(t)
	svc := agent.NewMarketService(pool)
	creatorID, _ := setupTestData(t, pool)

	createApprovedAgent(t, pool, creatorID, "ag-fin", WithTags([]string{"finance"}))
	createApprovedAgent(t, pool, creatorID, "ag-data", WithTags([]string{"data"}))
	createApprovedAgent(t, pool, creatorID, "ag-both", WithTags([]string{"finance", "data"}))

	ctx := context.Background()

	// tags=["finance"] 应返回 ag-fin + ag-both = 2
	resp1, err := svc.ListMarket(ctx, []string{"finance"}, "", 1, 12)
	require.NoError(t, err)
	assert.Equal(t, int32(2), resp1.Total, "tags=[finance] OR matches 2 agents")
	assert.Len(t, resp1.Items, 2)

	// tags=["finance","data"] OR 关系：返回所有含 finance 或 data 的 = 3
	resp2, err := svc.ListMarket(ctx, []string{"finance", "data"}, "", 1, 12)
	require.NoError(t, err)
	assert.Equal(t, int32(3), resp2.Total, "tags=[finance,data] OR matches all 3")
	assert.Len(t, resp2.Items, 3)
}

func TestListMarket_KeywordSearch(t *testing.T) {
	pool := setupTestDB(t)
	svc := agent.NewMarketService(pool)
	creatorID, _ := setupTestData(t, pool)

	createApprovedAgent(t, pool, creatorID, "fin-review",
		WithName("财务审阅 Agent"), WithDescription("帮你审阅账目"))
	createApprovedAgent(t, pool, creatorID, "code-review",
		WithName("代码审查"), WithDescription("review your code"))

	ctx := context.Background()

	// 关键词"财务"只命中第一个
	resp1, err := svc.ListMarket(ctx, nil, "财务", 1, 12)
	require.NoError(t, err)
	assert.Equal(t, int32(1), resp1.Total)
	require.Len(t, resp1.Items, 1)
	assert.Contains(t, resp1.Items[0].Name, "财务")

	// 关键词"审"两个 name 都含
	resp2, err := svc.ListMarket(ctx, nil, "审", 1, 12)
	require.NoError(t, err)
	assert.Equal(t, int32(2), resp2.Total, "keyword '审' should match both '审阅' and '审查'")
	assert.Len(t, resp2.Items, 2)
}

func TestListMarket_FiltersOutDisabled(t *testing.T) {
	// Phase 2 缺口 2 后语义：市场过滤只看 visibility=public + lifecycle=active。
	// pending / rejected 这些 certification_status 仍出现在市场（只是不挂认证徽章）；
	// 只有 disabled 不出现。private/unlisted 也通过 visibility 过滤掉。
	pool := setupTestDB(t)
	svc := agent.NewMarketService(pool)
	creatorID, _ := setupTestData(t, pool)

	createApprovedAgent(t, pool, creatorID, "ag-approved", WithStatus("approved"))
	createApprovedAgent(t, pool, creatorID, "ag-pending", WithStatus("pending"))
	createApprovedAgent(t, pool, creatorID, "ag-rejected", WithStatus("rejected"))
	createApprovedAgent(t, pool, creatorID, "ag-disabled", WithStatus("disabled"))

	resp, err := svc.ListMarket(context.Background(), nil, "", 1, 12)
	require.NoError(t, err)
	assert.Equal(t, int32(3), resp.Total, "disabled 不进市场；approved/pending/rejected 都进")
	slugs := make([]string, 0, len(resp.Items))
	for _, it := range resp.Items {
		slugs = append(slugs, it.Slug)
	}
	assert.ElementsMatch(t, []string{"ag-approved", "ag-pending", "ag-rejected"}, slugs)
}

func TestListMarket_Pagination(t *testing.T) {
	pool := setupTestDB(t)
	svc := agent.NewMarketService(pool)
	creatorID, _ := setupTestData(t, pool)

	for i := 0; i < 25; i++ {
		createApprovedAgent(t, pool, creatorID, slugN("page", i))
	}

	ctx := context.Background()

	// page=1 size=12 -> 12 items, total=25
	resp1, err := svc.ListMarket(ctx, nil, "", 1, 12)
	require.NoError(t, err)
	assert.Len(t, resp1.Items, 12, "page=1 size=12 should yield 12")
	assert.Equal(t, int32(25), resp1.Total)

	// page=2 size=12 -> 12 items
	resp2, err := svc.ListMarket(ctx, nil, "", 2, 12)
	require.NoError(t, err)
	assert.Len(t, resp2.Items, 12, "page=2 size=12 should yield 12")
	assert.Equal(t, int32(25), resp2.Total)

	// page=3 size=12 -> 1 item（25 - 24 = 1）
	resp3, err := svc.ListMarket(ctx, nil, "", 3, 12)
	require.NoError(t, err)
	assert.Len(t, resp3.Items, 1, "page=3 size=12 should yield 1 (25-24)")
	assert.Equal(t, int32(25), resp3.Total)

	// 各 page 之间无重复 slug
	seen := map[string]bool{}
	for _, all := range [][]agent.MarketListItem{resp1.Items, resp2.Items, resp3.Items} {
		for _, it := range all {
			require.False(t, seen[it.Slug], "duplicate slug across pages: %s", it.Slug)
			seen[it.Slug] = true
		}
	}
	assert.Equal(t, 25, len(seen), "all 25 unique slugs should be covered")
}

func TestListMarket_ClampSize(t *testing.T) {
	pool := setupTestDB(t)
	svc := agent.NewMarketService(pool)
	creatorID, _ := setupTestData(t, pool)

	// 插 60 个，请求 size=100 应被 clamp 到 50
	for i := 0; i < 60; i++ {
		createApprovedAgent(t, pool, creatorID, slugN("clamp", i))
	}

	resp, err := svc.ListMarket(context.Background(), nil, "", 1, 100)
	require.NoError(t, err)
	assert.LessOrEqual(t, len(resp.Items), 50, "size>50 should be clamped to 50")
	assert.Equal(t, int32(60), resp.Total, "total still reflects all 60")
	// 实现把 size clamp 后回填进 response.Size
	assert.LessOrEqual(t, resp.Size, int32(50), "response.Size should reflect clamped value")
}

func TestListMarket_EmptyResult(t *testing.T) {
	pool := setupTestDB(t)
	svc := agent.NewMarketService(pool)
	_, _ = setupTestData(t, pool)
	// 不插任何 agent

	resp, err := svc.ListMarket(context.Background(), nil, "", 1, 12)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, int32(0), resp.Total)
	// items 应是 [] 而非 nil（确保 JSON 序列化为 [] 而不是 null）
	assert.NotNil(t, resp.Items, "items must be empty slice, not nil")
	assert.Len(t, resp.Items, 0)

	// 进一步：序列化后应为 "[]"
	raw, err := json.Marshal(resp.Items)
	require.NoError(t, err)
	assert.Equal(t, "[]", string(raw),
		"items must marshal to '[]' not 'null'")
}

// ─────────────────────────────────────────────────────────
// GetBySlug
// ─────────────────────────────────────────────────────────

func TestGetBySlug_HappyPath(t *testing.T) {
	pool := setupTestDB(t)
	svc := agent.NewMarketService(pool)
	creatorID, _ := setupTestData(t, pool)

	createApprovedAgent(t, pool, creatorID, "fin-review",
		WithName("Financial Review"),
		WithTags([]string{"finance"}),
		WithPrice(500),
	)
	var agentID uuid.UUID
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT id FROM agents WHERE slug=$1`, "fin-review").Scan(&agentID))
	_, err := pool.Exec(context.Background(),
		`INSERT INTO agent_skills (agent_id, skill_id)
		 VALUES ($1, 'data/sql-query'), ($1, 'data/analysis')`,
		agentID)
	require.NoError(t, err)
	_, err = pool.Exec(context.Background(),
		`INSERT INTO agent_availability_snapshots (
			agent_id, availability_status, last_successful_run_at, last_checked_at, consecutive_failures
		) VALUES ($1, 'healthy', NOW(), NOW(), 0)`,
		agentID)
	require.NoError(t, err)

	detail, err := svc.GetBySlug(context.Background(), "fin-review")
	require.NoError(t, err)
	require.NotNil(t, detail)
	assert.Equal(t, "fin-review", detail.Slug)
	assert.Equal(t, "Financial Review", detail.Name)
	assert.Equal(t, "Test Creator", detail.Creator.DisplayName, "creator.display_name must be joined")
	assert.NotEmpty(t, detail.EndpointURL, "detail must include endpoint_url")
	require.Len(t, detail.Skills, 2)
	assert.Equal(t, "data/sql-query", detail.Skills[0].ID)
	assert.Equal(t, "SQL 查询", detail.Skills[0].Name)
	assert.Equal(t, "healthy", detail.Availability.Status)
	assert.Equal(t, "可用", detail.Availability.Label)
	assert.Equal(t, int32(0), detail.Availability.ConsecutiveFailures)
	require.NotNil(t, detail.Availability.LastSuccessfulRunAt)
	assert.True(t, detail.Readiness.Listed)
	assert.True(t, detail.Readiness.Discoverable)
	assert.True(t, detail.Readiness.Callable)
	assert.False(t, detail.Readiness.Verified)
	assert.False(t, detail.Readiness.Certified)
	assert.False(t, detail.Readiness.PaidEnabled)
	assert.Equal(t, "/api/v1/agents/fin-review/agent-card.json", detail.Readiness.AgentCardURL)
	assert.Equal(t, "/api/v1/a2a/agents/fin-review", detail.Readiness.A2AEndpoint)
	assert.Equal(t, detail.Availability.LastSuccessfulRunAt, detail.Readiness.LastSuccessfulRunAt)
	assert.Equal(t, "healthy", detail.Readiness.AvailabilityStatus)
}

func TestGetBySlug_ReadinessReflectsCertificationBenchmarkAndUnlisted(t *testing.T) {
	pool := setupTestDB(t)
	svc := agent.NewMarketService(pool)
	creatorID, _ := setupTestData(t, pool)
	ctx := context.Background()

	agentID := createApprovedAgent(t, pool, creatorID, "ready-agent", WithStatus("certified"))
	_, err := pool.Exec(ctx,
		`UPDATE agents SET visibility='unlisted' WHERE id=$1`,
		agentID,
	)
	require.NoError(t, err)
	batchID := uuid.New()
	_, err = pool.Exec(ctx,
		`INSERT INTO agent_skill_scores (
			agent_id, skill_id, status, average_score, pass_count, total_count, last_batch_id, verified_at
		) VALUES ($1, 'data/sql-query', 'verified', 92, 3, 3, $2, NOW())`,
		agentID,
		batchID,
	)
	require.NoError(t, err)

	detail, err := svc.GetBySlug(ctx, "ready-agent")
	require.NoError(t, err)
	assert.False(t, detail.Readiness.Listed, "unlisted agents are not public market listings")
	assert.True(t, detail.Readiness.Discoverable, "unlisted agents remain discoverable by direct slug/card")
	assert.True(t, detail.Readiness.Verified)
	assert.True(t, detail.Readiness.Certified)
	assert.False(t, detail.Readiness.PaidEnabled)
	assert.Equal(t, int32(1), detail.Readiness.VerifiedSkillCount)
	require.NotNil(t, detail.Readiness.LatestBenchmarkBatchID)
	assert.Equal(t, batchID.String(), *detail.Readiness.LatestBenchmarkBatchID)
	assert.Equal(t, "payments are not enabled in the current release", detail.Readiness.Explanation["paid_enabled"])
}

func TestGetAgentCardBySlug_HappyPath(t *testing.T) {
	pool := setupTestDB(t)
	svc := agent.NewMarketService(pool)
	creatorID, _ := setupTestData(t, pool)

	createApprovedAgent(t, pool, creatorID, "card-agent",
		WithName("Card Agent"),
		WithDescription("Machine-readable card test"),
		WithTags([]string{"data", "analysis"}),
	)
	var agentID uuid.UUID
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT id FROM agents WHERE slug=$1`, "card-agent").Scan(&agentID))
	_, err := pool.Exec(context.Background(),
		`INSERT INTO agent_skills (agent_id, skill_id)
		 VALUES ($1, 'data/sql-query')`,
		agentID)
	require.NoError(t, err)

	card, err := svc.GetAgentCardBySlug(context.Background(), "card-agent")
	require.NoError(t, err)
	require.NotNil(t, card)
	assert.Equal(t, "Card Agent", card.Name)
	assert.Equal(t, "/api/v1/a2a/agents/card-agent", card.URL)
	assert.Equal(t, "1.0", card.ProtocolVersion)
	assert.Equal(t, []string{"0.3", "1.0"}, card.ProtocolVersions)
	require.Len(t, card.SupportedInterfaces, 3)
	assert.Equal(t, "JSONRPC", card.SupportedInterfaces[0].ProtocolBinding)
	assert.Equal(t, "1.0", card.SupportedInterfaces[0].ProtocolVersion)
	for _, item := range card.SupportedInterfaces {
		assert.NotEqual(t, "gRPC", item.ProtocolBinding)
	}
	assert.True(t, card.Capabilities.Streaming)
	assert.True(t, card.Capabilities.PushNotifications)
	assert.True(t, card.Capabilities.PushNotificationsLegacy)
	require.Len(t, card.Capabilities.Extensions, 2)
	for _, ext := range card.Capabilities.Extensions {
		assert.False(t, ext.Required)
	}
	assert.Equal(t, []string{"application/json", "text/plain"}, card.DefaultInputModesCurrent)
	assert.Equal(t, []string{"Bearer"}, card.Authentication.Schemes)
	assert.Contains(t, card.Authentication.Scopes, "agents:run")
	require.Len(t, card.Skills, 1)
	assert.Equal(t, "data/sql-query", card.Skills[0].ID)
	assert.Equal(t, "card-agent", card.OpenLinker.Slug)
	assert.Equal(t, agentID.String(), card.OpenLinker.AgentID)
	assert.Equal(t, "/api/v1/a2a/agents/card-agent", card.OpenLinker.InvocationEndpoint)
	assert.Equal(t, "/api/v1/a2a/agents/card-agent/message:stream", card.OpenLinker.StreamEndpoint)
	assert.Equal(t, "/api/v1/a2a/agents/card-agent/tasks/{task_id}", card.OpenLinker.TaskLookupEndpoint)
	assert.Equal(t, "/api/v1/a2a/agents/card-agent/tasks/{task_id}:subscribe", card.OpenLinker.TaskSubscribeEndpoint)
	assert.Equal(t, []string{"data/sql-query"}, card.OpenLinker.SkillIDs)
	assert.Equal(t, "unknown", card.OpenLinker.AvailabilityStatus)
}

func TestGetBySlug_NotFound(t *testing.T) {
	pool := setupTestDB(t)
	svc := agent.NewMarketService(pool)
	_, _ = setupTestData(t, pool)

	_, err := svc.GetBySlug(context.Background(), "no-such-slug")
	assertHTTPStatus(t, err, http.StatusNotFound)
}

func TestGetBySlugForOwner_ReturnsPrivateOwnedAgent(t *testing.T) {
	pool := setupTestDB(t)
	svc := agent.NewMarketService(pool)
	creatorID, _ := setupTestData(t, pool)
	otherCreatorID := insertCreatorUser(t, pool, "Other Creator")
	ctx := context.Background()

	agentID := createApprovedAgent(t, pool, creatorID, "owner-private")
	_, err := pool.Exec(ctx, `UPDATE agents SET visibility='private' WHERE id=$1`, agentID)
	require.NoError(t, err)

	_, err = svc.GetBySlug(ctx, "owner-private")
	assertHTTPStatus(t, err, http.StatusNotFound)

	detail, err := svc.GetBySlugForOwner(ctx, "owner-private", creatorID)
	require.NoError(t, err)
	require.NotNil(t, detail)
	assert.Equal(t, "owner-private", detail.Slug)
	assert.Equal(t, "private", detail.Visibility)

	_, err = svc.GetBySlugForOwner(ctx, "owner-private", otherCreatorID)
	assertHTTPStatus(t, err, http.StatusNotFound)
}

func TestGetBySlug_DisabledNotReturned(t *testing.T) {
	// Phase 2 缺口 2 后：pending/rejected 仍能按 slug 访问（与市场列表一致），
	// 详情页拒绝的是 disabled (lifecycle) 与 private (visibility)。unlisted 凭直链可访问。
	pool := setupTestDB(t)
	svc := agent.NewMarketService(pool)
	creatorID, _ := setupTestData(t, pool)

	createApprovedAgent(t, pool, creatorID, "ag-disabled", WithStatus("disabled"))

	_, err := svc.GetBySlug(context.Background(), "ag-disabled")
	assertHTTPStatus(t, err, http.StatusNotFound)
}

// TestGetBySlug_DoesNotExposeAuthHeader 确认 endpoint_auth_header 永不在响应里。
//
// 用 JSON 序列化双保险：
//  1. 序列化 detail 为 JSON 检查不含 "endpoint_auth_header"
//  2. 也检查不含原始密钥字符串
func TestGetBySlug_DoesNotExposeAuthHeader(t *testing.T) {
	pool := setupTestDB(t)
	svc := agent.NewMarketService(pool)
	creatorID, _ := setupTestData(t, pool)

	const secretHeader = "Bearer super-secret-token-9876543"
	createApprovedAgent(t, pool, creatorID, "with-auth",
		WithAuthHeader(secretHeader),
	)

	detail, err := svc.GetBySlug(context.Background(), "with-auth")
	require.NoError(t, err)
	require.NotNil(t, detail)

	raw, err := json.Marshal(detail)
	require.NoError(t, err)
	jsonStr := string(raw)

	assert.NotContains(t, jsonStr, "endpoint_auth_header",
		"response JSON must NOT contain 'endpoint_auth_header' field; got: %s", jsonStr)
	assert.NotContains(t, jsonStr, secretHeader,
		"response JSON must NOT contain raw secret value; got: %s", jsonStr)
	assert.False(t, strings.Contains(strings.ToLower(jsonStr), "auth_header"),
		"response JSON must NOT contain any auth_header substring; got: %s", jsonStr)
}

// ─────────────────────────────────────────────────────────
// 小工具
// ─────────────────────────────────────────────────────────

// slugN 生成形如 "<prefix>-<n>" 的合法 slug。
func slugN(prefix string, i int) string {
	const digits = "0123456789"
	if i == 0 {
		return prefix + "-0"
	}
	var rev []byte
	for n := i; n > 0; n /= 10 {
		rev = append(rev, digits[n%10])
	}
	// 反转
	for l, r := 0, len(rev)-1; l < r; l, r = l+1, r-1 {
		rev[l], rev[r] = rev[r], rev[l]
	}
	return prefix + "-" + string(rev)
}
