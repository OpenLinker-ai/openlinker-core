// Package agent_test - MarketHandler HTTP 集成测试。
//
// 与 service_test 共用 testhelpers_test.go 的 setup。
//
// 期望 API（与 internal/agent/market_handler.go 对齐）：
//
//	type MarketHandler struct{ ... }
//	func NewMarketHandler(svc *MarketService) *MarketHandler
//	func (h *MarketHandler) Register(api *echo.Group)
//
//	路由（公开，无需 JWT）：
//	  GET /agents              -> 200 ListMarketResponse  query: tags, q, page, size
//	  GET /agents/:slug        -> 200 AgentDetailResponse
//
// query 解析约定：
//   - tags=finance,data        -> service.Tags = ["finance","data"]
//   - q=审计                    -> service.Keyword = "审计"
//   - page=2&size=10           -> service.Page=2, Size=10
//   - 默认 page=1, size=12
//   - page<1 应被 clamp 到 1（推荐）；或返回 400（实现可选）
package agent_test

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kinzhi/openlinker-core/pkg/agent"
	"github.com/kinzhi/openlinker-core/pkg/auth"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

// setupTestServer 启动 echo + MarketHandler，返回测试 server + pool（已 setup）。
func setupTestServer(t *testing.T) (*httptest.Server, *pgxpool.Pool) {
	t.Helper()
	pool := setupTestDB(t)

	svc := agent.NewMarketService(pool)
	h := agent.NewMarketHandler(svc)

	e := echo.New()
	e.HTTPErrorHandler = func(err error, c echo.Context) {
		if c.Response().Committed {
			return
		}
		_ = httpx.SendError(c, err)
	}

	api := e.Group("/api/v1")
	h.Register(api)

	srv := httptest.NewServer(e)
	t.Cleanup(func() { srv.Close() })
	return srv, pool
}

func setupProtectedMarketTestServer(t *testing.T) (*httptest.Server, *pgxpool.Pool) {
	t.Helper()
	pool := setupTestDB(t)

	svc := agent.NewMarketService(pool)
	h := agent.NewMarketHandler(svc)

	e := echo.New()
	e.HTTPErrorHandler = func(err error, c echo.Context) {
		if c.Response().Committed {
			return
		}
		_ = httpx.SendError(c, err)
	}

	api := e.Group("/api/v1")
	jwtMW := auth.JWTMiddleware(testHandlerSecret)
	h.Register(api)
	h.RegisterProtected(api, jwtMW)

	srv := httptest.NewServer(e)
	t.Cleanup(func() { srv.Close() })
	return srv, pool
}

// getJSON 简化 GET 请求，返回 (*http.Response, body bytes)。
func getJSON(t *testing.T, srvURL, path string, headers map[string]string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, srvURL+path, nil)
	require.NoError(t, err)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp, body
}

// listResponseBody 与 ListMarketResponse 字段保持兼容。
type listResponseBody struct {
	Items []map[string]any `json:"items"`
	Total int32            `json:"total"`
	Page  int              `json:"page,omitempty"`
	Size  int              `json:"size,omitempty"`
}

// ─────────────────────────────────────────────────────────
// GET /agents
// ─────────────────────────────────────────────────────────

func TestGetListMarket_HappyPath(t *testing.T) {
	srv, pool := setupTestServer(t)
	creatorID, _ := setupTestData(t, pool)

	for i := 0; i < 3; i++ {
		createApprovedAgent(t, pool, creatorID, slugN("h-happy", i))
	}

	resp, raw := getJSON(t, srv.URL, "/api/v1/agents", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode, "body=%s", string(raw))

	var out listResponseBody
	require.NoError(t, json.Unmarshal(raw, &out), "raw=%s", string(raw))
	assert.Equal(t, int32(3), out.Total)
	assert.Len(t, out.Items, 3)
}

func TestGetListMarket_QueryParams(t *testing.T) {
	srv, pool := setupTestServer(t)
	creatorID, _ := setupTestData(t, pool)

	createApprovedAgent(t, pool, creatorID, "h-fin",
		WithName("审计 Agent"),
		WithTags([]string{"finance"}))
	createApprovedAgent(t, pool, creatorID, "h-data",
		WithName("数据 Agent"),
		WithTags([]string{"data"}))
	createApprovedAgent(t, pool, creatorID, "h-both",
		WithName("综合 Agent"),
		WithTags([]string{"finance", "data"}))

	// tags=finance,data -> 3 个 (OR)
	resp1, raw1 := getJSON(t, srv.URL, "/api/v1/agents?tags=finance,data", nil)
	assert.Equal(t, http.StatusOK, resp1.StatusCode, "body=%s", string(raw1))
	var out1 listResponseBody
	require.NoError(t, json.Unmarshal(raw1, &out1))
	assert.Equal(t, int32(3), out1.Total, "tags=finance,data should match all 3")

	// q=审计 -> 1 个
	resp2, raw2 := getJSON(t, srv.URL, "/api/v1/agents?q="+url.QueryEscape("审计"), nil)
	assert.Equal(t, http.StatusOK, resp2.StatusCode, "body=%s", string(raw2))
	var out2 listResponseBody
	require.NoError(t, json.Unmarshal(raw2, &out2))
	assert.Equal(t, int32(1), out2.Total, "q=审计 should match 1")

	// page=2 size=10：先插更多达 25 个再测
	for i := 0; i < 22; i++ {
		createApprovedAgent(t, pool, creatorID, slugN("h-page", i))
	}
	resp3, raw3 := getJSON(t, srv.URL, "/api/v1/agents?page=2&size=10", nil)
	assert.Equal(t, http.StatusOK, resp3.StatusCode, "body=%s", string(raw3))
	var out3 listResponseBody
	require.NoError(t, json.Unmarshal(raw3, &out3))
	// 共 25 个 (3 + 22)，page=2 size=10 -> items 10
	assert.Equal(t, int32(25), out3.Total, "total should be 25")
	assert.Len(t, out3.Items, 10, "page=2 size=10 should have 10 items")
}

func TestGetListMarket_DefaultPagination(t *testing.T) {
	srv, pool := setupTestServer(t)
	creatorID, _ := setupTestData(t, pool)

	// 插 15 个，验证默认 size=12（应只返 12 个）
	for i := 0; i < 15; i++ {
		createApprovedAgent(t, pool, creatorID, slugN("h-def", i))
	}

	resp, raw := getJSON(t, srv.URL, "/api/v1/agents", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode, "body=%s", string(raw))

	var out listResponseBody
	require.NoError(t, json.Unmarshal(raw, &out))
	assert.Equal(t, int32(15), out.Total)
	assert.Len(t, out.Items, 12, "default size should be 12")
}

func TestGetListMarket_BadPage(t *testing.T) {
	srv, pool := setupTestServer(t)
	creatorID, _ := setupTestData(t, pool)
	createApprovedAgent(t, pool, creatorID, "h-bad-page")

	// page=-1：clamp 到 1 (200) 或返回 400 都可接受。
	resp, raw := getJSON(t, srv.URL, "/api/v1/agents?page=-1", nil)
	if resp.StatusCode == http.StatusOK {
		// 实现选择 clamp，应仍能正常返回
		var out listResponseBody
		require.NoError(t, json.Unmarshal(raw, &out), "body=%s", string(raw))
		assert.GreaterOrEqual(t, out.Total, int32(1))
	} else {
		// 实现选择拒绝，必须 4xx
		assert.GreaterOrEqual(t, resp.StatusCode, 400, "body=%s", string(raw))
		assert.Less(t, resp.StatusCode, 500, "body=%s", string(raw))
	}
}

// ─────────────────────────────────────────────────────────
// GET /agents/:slug
// ─────────────────────────────────────────────────────────

func TestGetBySlug_HandlerHappyPath(t *testing.T) {
	srv, pool := setupTestServer(t)
	creatorID, _ := setupTestData(t, pool)

	createApprovedAgent(t, pool, creatorID, "finance-review",
		WithName("Financial Review"),
		WithTags([]string{"finance"}))
	var agentID uuid.UUID
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT id FROM agents WHERE slug=$1`, "finance-review").Scan(&agentID))
	_, err := pool.Exec(context.Background(),
		`INSERT INTO agent_skills (agent_id, skill_id) VALUES ($1, 'data/sql-query')`,
		agentID)
	require.NoError(t, err)

	resp, raw := getJSON(t, srv.URL, "/api/v1/agents/finance-review", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode, "body=%s", string(raw))

	var detail map[string]any
	require.NoError(t, json.Unmarshal(raw, &detail), "raw=%s", string(raw))
	assert.Equal(t, "finance-review", detail["slug"])
	assert.Equal(t, "Financial Review", detail["name"])
	// 详情应含 endpoint_url 但不含 endpoint_auth_header
	assert.Contains(t, detail, "endpoint_url", "detail must include endpoint_url")
	skills, ok := detail["skills"].([]any)
	require.True(t, ok, "skills must be an array; body=%s", string(raw))
	require.Len(t, skills, 1)
	assert.NotContains(t, detail, "endpoint_auth_header",
		"detail must NOT include endpoint_auth_header; full body=%s", string(raw))
	// raw 字符串也不应含敏感字段名
	assert.False(t, strings.Contains(string(raw), "endpoint_auth_header"),
		"response body must NOT contain 'endpoint_auth_header' substring")
}

func TestGetBySlug_HandlerNotFound(t *testing.T) {
	srv, _ := setupTestServer(t)

	resp, raw := getJSON(t, srv.URL, "/api/v1/agents/no-such-slug-xx", nil)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode, "body=%s", string(raw))
}

func TestGetBySlugForOwner_HandlerReturnsPrivateOwnedAgent(t *testing.T) {
	srv, pool := setupProtectedMarketTestServer(t)
	creatorID, _ := setupTestData(t, pool)
	otherCreatorID := insertCreatorUser(t, pool, "Other Creator")
	ctx := context.Background()

	agentID := createApprovedAgent(t, pool, creatorID, "handler-owner-private")
	_, err := pool.Exec(ctx, `UPDATE agents SET visibility='private' WHERE id=$1`, agentID)
	require.NoError(t, err)

	publicResp, publicRaw := getJSON(t, srv.URL, "/api/v1/agents/handler-owner-private", nil)
	assert.Equal(t, http.StatusNotFound, publicResp.StatusCode, "body=%s", string(publicRaw))

	ownerResp, ownerRaw := getJSON(t, srv.URL,
		"/api/v1/creator/agents/by-slug/handler-owner-private",
		map[string]string{"Authorization": signJWT(t, creatorID)},
	)
	assert.Equal(t, http.StatusOK, ownerResp.StatusCode, "body=%s", string(ownerRaw))
	assert.NotContains(t, string(ownerRaw), "endpoint_auth_header")

	var detail map[string]any
	require.NoError(t, json.Unmarshal(ownerRaw, &detail), "raw=%s", string(ownerRaw))
	assert.Equal(t, "handler-owner-private", detail["slug"])
	assert.Equal(t, "private", detail["visibility"])

	otherResp, otherRaw := getJSON(t, srv.URL,
		"/api/v1/creator/agents/by-slug/handler-owner-private",
		map[string]string{"Authorization": signJWT(t, otherCreatorID)},
	)
	assert.Equal(t, http.StatusNotFound, otherResp.StatusCode, "body=%s", string(otherRaw))
}

func TestGetAgentCard_HandlerHappyPath(t *testing.T) {
	t.Setenv("AGENT_CARD_SIGNING_SEED", "agent-card-test-signing-seed")
	srv, pool := setupTestServer(t)
	creatorID, _ := setupTestData(t, pool)

	createApprovedAgent(t, pool, creatorID, "agent-card",
		WithName("Agent Card"),
		WithTags([]string{"finance"}))

	resp, raw := getJSON(t, srv.URL, "/api/v1/agents/agent-card/agent-card.json", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode, "body=%s", string(raw))
	assert.Equal(t, "public, max-age=300", resp.Header.Get("Cache-Control"))

	var card map[string]any
	require.NoError(t, json.Unmarshal(raw, &card), "raw=%s", string(raw))
	assert.Equal(t, "Agent Card", card["name"])
	assert.Equal(t, "/api/v1/a2a/agents/agent-card", card["url"])
	assert.Equal(t, "1.0", card["protocolVersion"])
	require.Contains(t, card, "supportedInterfaces")
	capabilities, ok := card["capabilities"].(map[string]any)
	require.True(t, ok, "capabilities must exist; body=%s", string(raw))
	assert.Equal(t, true, capabilities["streaming"])
	assert.Equal(t, true, capabilities["pushNotifications"])
	assert.Equal(t, true, capabilities["push_notifications"])
	assert.Equal(t, true, capabilities["extendedAgentCard"])
	openlinker, ok := card["openlinker"].(map[string]any)
	require.True(t, ok, "openlinker extension must exist; body=%s", string(raw))
	assert.Equal(t, "agent-card", openlinker["slug"])
	assert.Equal(t, "public", openlinker["card_variant"])
	assert.Equal(t, "/api/v1/agents/agent-card/agent-card.extended.json", openlinker["extended_card_endpoint"])
	assert.Equal(t, "/api/v1/a2a/agents/agent-card", openlinker["invocation_endpoint"])
	assert.Equal(t, "/api/v1/a2a/agents/agent-card/message:stream", openlinker["stream_endpoint"])
	assert.Equal(t, "/api/v1/a2a/agents/agent-card/tasks/{task_id}", openlinker["task_lookup_endpoint"])
	assert.Equal(t, "/api/v1/a2a/agents/agent-card/tasks/{task_id}:subscribe", openlinker["task_subscribe_endpoint"])
	readiness, ok := openlinker["readiness"].(map[string]any)
	require.True(t, ok, "openlinker.readiness must exist; body=%s", string(raw))
	assert.Equal(t, true, readiness["listed"])
	assert.Equal(t, true, readiness["discoverable"])
	assert.Equal(t, false, readiness["callable"])
	assert.Equal(t, "unknown", readiness["availability_status"])
	availability, ok := openlinker["availability"].(map[string]any)
	require.True(t, ok, "openlinker.availability must exist; body=%s", string(raw))
	assert.Equal(t, "unknown", availability["status"])
	assert.Equal(t, "Agent 已注册，但还没有成功运行或失败记录。首次调用后会更新可用性。", availability["hint"])
	runtimeContract, ok := openlinker["runtime"].(map[string]any)
	require.True(t, ok, "openlinker.runtime must exist; body=%s", string(raw))
	assert.Equal(t, "openlinker_a2a_proxy", runtimeContract["adapter"])
	assert.Equal(t, "direct_http", runtimeContract["connection_mode"])
	assert.Equal(t, "direct_endpoint_probe_and_run_result", runtimeContract["online_signal"])
	assert.Equal(t, "openlinker_run_task_lifecycle", runtimeContract["task_lifecycle"])
	verifyAgentCardSignature(t, raw)
	assert.NotContains(t, string(raw), "endpoint_auth_header")
}

func TestGetExtendedAgentCard_HandlerHappyPath(t *testing.T) {
	t.Setenv("AGENT_CARD_SIGNING_SEED", "agent-card-test-signing-seed")
	srv, pool := setupTestServer(t)
	creatorID, _ := setupTestData(t, pool)

	createApprovedAgent(t, pool, creatorID, "agent-card-extended",
		WithName("Extended Agent Card"),
		WithTags([]string{"finance"}))

	resp, raw := getJSON(t, srv.URL, "/api/v1/agents/agent-card-extended/agent-card.extended.json", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode, "body=%s", string(raw))
	assert.Equal(t, "public, max-age=300", resp.Header.Get("Cache-Control"))

	var card map[string]any
	require.NoError(t, json.Unmarshal(raw, &card), "raw=%s", string(raw))
	openlinker, ok := card["openlinker"].(map[string]any)
	require.True(t, ok, "openlinker extension must exist; body=%s", string(raw))
	assert.Equal(t, "extended", openlinker["card_variant"])
	assert.Contains(t, card, "signature")
	verifyAgentCardSignature(t, raw)
	assert.NotContains(t, string(raw), "endpoint_auth_header")
}

func verifyAgentCardSignature(t *testing.T, raw []byte) {
	t.Helper()
	var card agent.AgentCardResponse
	require.NoError(t, json.Unmarshal(raw, &card), "raw=%s", string(raw))
	require.NotNil(t, card.Signature, "signed Agent Card must include signature")
	assert.Equal(t, "Ed25519", card.Signature.Algorithm)
	publicKey, err := base64.RawURLEncoding.DecodeString(card.Signature.PublicKey)
	require.NoError(t, err)
	signature, err := base64.RawURLEncoding.DecodeString(card.Signature.Signature)
	require.NoError(t, err)
	signed := card.Signature
	card.Signature = nil
	payload, err := json.Marshal(card)
	require.NoError(t, err)
	digest := sha256.Sum256(payload)
	assert.Equal(t, "sha256-"+base64.RawURLEncoding.EncodeToString(digest[:]), signed.PayloadDigest)
	assert.True(t, ed25519.Verify(ed25519.PublicKey(publicKey), payload, signature))
}

// TestGetBySlug_NoAuthRequired 验证市场端点完全公开 —— 不带 Authorization 仍 200。
func TestGetBySlug_NoAuthRequired(t *testing.T) {
	srv, pool := setupTestServer(t)
	creatorID, _ := setupTestData(t, pool)
	createApprovedAgent(t, pool, creatorID, "public-agent")

	// 完全不带任何 header
	resp, raw := getJSON(t, srv.URL, "/api/v1/agents/public-agent", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode,
		"market endpoint must be public; body=%s", string(raw))

	// 即使带个无意义的 Authorization 也应 200（公开端点不校验）
	resp2, raw2 := getJSON(t, srv.URL, "/api/v1/agents/public-agent",
		map[string]string{"Authorization": "Bearer garbage"})
	assert.Equal(t, http.StatusOK, resp2.StatusCode,
		"public endpoint must ignore Authorization; body=%s", string(raw2))
}
