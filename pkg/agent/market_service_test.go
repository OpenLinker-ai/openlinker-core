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

	"github.com/kinzhi/openlinker-core/pkg/agent"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

// assertHTTPStatus 用 errors.As 断言 *httpx.HTTPError 的 status。
func assertHTTPStatus(t *testing.T, err error, want int) {
	t.Helper()
	require.Error(t, err)
	var he *httpx.HTTPError
	require.True(t, errors.As(err, &he), "expected *httpx.HTTPError, got %T (%v)", err, err)
	assert.Equal(t, want, he.Status)
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
}

func TestGetBySlug_NotFound(t *testing.T) {
	pool := setupTestDB(t)
	svc := agent.NewMarketService(pool)
	_, _ = setupTestData(t, pool)

	_, err := svc.GetBySlug(context.Background(), "no-such-slug")
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
