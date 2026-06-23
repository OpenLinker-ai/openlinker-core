// Package agent_test - HTTP handler 集成测试。
//
// 与 service_test.go 一样，需要 TEST_DATABASE_URL，否则 t.Skip()。
//
// 实际 API（与 internal/agent/handler.go 对齐）：
//
//	func NewHandler(svc *Service, cfg ...*config.Config) *Handler
//	func (h *Handler) Register(api *echo.Group)
//	    GET    /agents/check-slug?slug=xxx
//	func (h *Handler) RegisterProtected(api *echo.Group, jwtMW echo.MiddlewareFunc)
//	    POST   /me/become-creator
//	    POST   /creator/agents
//	    GET    /creator/agents
//	    GET    /creator/agents/:id
//	    PATCH  /creator/agents/:id
//	    DELETE /creator/agents/:id
//	func (h *Handler) RegisterAdmin(api *echo.Group, jwtMW, adminMW echo.MiddlewareFunc)
//	    GET    /admin/agents/pending
//	    POST   /admin/agents/:id/approve
//	    POST   /admin/agents/:id/reject
//
// 用 echo.ServeHTTP + httptest.NewRecorder 直接驱动，不开 socket（任务要求）。
package agent_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	"github.com/OpenLinker-ai/openlinker-core/pkg/auth"
	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	dbgen "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

const testHandlerSecret = "test-secret-32-chars-aaaaaaaaaaaa"

// setupHandlerTest 启动 echo 实例 + 挂载 agent handler，返回 echo + pool。
// 复用 testhelpers_test.go 的 setupTestDB。
func setupHandlerTest(t *testing.T) (*echo.Echo, *pgxpool.Pool) {
	t.Helper()
	pool := setupTestDB(t) // 已含 t.Skip + Cleanup

	cfg := &config.Config{
		JWTSecret:       testHandlerSecret,
		JWTExpireHours:  1,
		PlatformFeeRate: 0.25,
	}
	svc := agent.NewService(pool, cfg)
	h := agent.NewHandler(svc, cfg)

	e := echo.New()
	e.HTTPErrorHandler = func(err error, c echo.Context) {
		if c.Response().Committed {
			return
		}
		_ = httpx.SendError(c, err)
	}

	api := e.Group("/api/v1")
	jwtMW := auth.JWTMiddleware(testHandlerSecret)
	adminMW := newTestAdminMiddleware(pool)

	mountAgentRoutes(h, api, jwtMW, adminMW)

	return e, pool
}

// mountAgentRoutes 把 agent handler 挂到 echo group。
// 与 internal/agent/handler.go 暴露的方法名对齐。
func mountAgentRoutes(h *agent.Handler, api *echo.Group, jwtMW, adminMW echo.MiddlewareFunc) {
	h.Register(api)                      // GET /agents/check-slug
	h.RegisterProtected(api, jwtMW)      // /me/become-creator + /creator/*
	h.RegisterAdmin(api, jwtMW, adminMW) // /admin/*
}

// newTestAdminMiddleware 仿照 internal/admin/middleware.go，直接用 pool 查
// users.is_admin。独立实现避免引入 internal/admin 包导致循环依赖。
func newTestAdminMiddleware(pool *pgxpool.Pool) echo.MiddlewareFunc {
	q := dbgen.New(pool)
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			idStr := httpx.UserIDFrom(c)
			if idStr == "" {
				return httpx.Unauthorized("")
			}
			uid, err := uuid.Parse(idStr)
			if err != nil {
				return httpx.Unauthorized("token 无效")
			}
			user, err := q.GetUserByID(c.Request().Context(), uid)
			if err != nil {
				return httpx.Unauthorized("用户不存在")
			}
			if !user.IsAdmin {
				return httpx.Forbidden("需要管理员权限")
			}
			c.Set(string(httpx.CtxKeyAdmin), true)
			return next(c)
		}
	}
}

// insertUserHandler 直接 INSERT 用户 + wallet，可指定是否 creator / admin。
func insertUserHandler(t *testing.T, pool *pgxpool.Pool, isCreator, isAdmin bool) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	uid := uuid.New()
	pwHash := "x"
	email := "h-" + uid.String()[:8] + "@example.com"
	_, err := pool.Exec(ctx,
		`INSERT INTO users (id, email, password_hash, display_name, is_creator, creator_verified, is_admin)
		 VALUES ($1, $2, $3, $4, $5, $5, $6)`,
		uid, email, pwHash, "H Tester", isCreator, isAdmin)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO wallets (user_id) VALUES ($1)`, uid)
	require.NoError(t, err)
	return uid
}

// signJWT 帮测试构造 Authorization Bearer。
func signJWT(t *testing.T, userID uuid.UUID) string {
	t.Helper()
	tok, err := auth.GenerateToken(userID.String(), testHandlerSecret, 1*time.Hour)
	require.NoError(t, err)
	return "Bearer " + tok
}

// doRequest 以 NewRecorder 方式驱动 echo，不开 socket。
func doRequest(t *testing.T, e *echo.Echo, method, target string, body any, headers map[string]string) (*httptest.ResponseRecorder, []byte) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		require.NoError(t, json.NewEncoder(&buf).Encode(body))
	}
	req := httptest.NewRequest(method, target, &buf)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	raw, _ := io.ReadAll(rec.Body)
	return rec, raw
}

// validBody 返回一个合法的 CreateAgent JSON body（map 形式，避免依赖 dto 内部 tag）。
func validBody(slug string) map[string]any {
	return map[string]any{
		"slug":                 slug,
		"name":                 "HTTP Test Agent",
		"description":          "An agent created via HTTP test.",
		"endpoint_url":         "https://example.com/agent/" + slug,
		"endpoint_auth_header": "Bearer secret",
		"price_per_call_cents": 500,
		"tags":                 []string{"http", "test"},
	}
}

// agentRespBody 解 agent 响应（仅断言关心的字段，多余字段被忽略）。
type agentRespBody struct {
	ID                string   `json:"id"`
	Slug              string   `json:"slug"`
	Name              string   `json:"name"`
	Status            string   `json:"status"`
	PricePerCallCents int32    `json:"price_per_call_cents"`
	Tags              []string `json:"tags"`
}

// ────────────────────────────────────────────────────────────
// POST /creator/agents
// ────────────────────────────────────────────────────────────

func TestPostCreateAgent_HappyPath(t *testing.T) {
	e, pool := setupHandlerTest(t)
	uid := insertUserHandler(t, pool, true, false)

	rec, raw := doRequest(t, e, http.MethodPost, "/api/v1/creator/agents",
		validBody(freshSlug("h-create")), map[string]string{
			"Authorization": signJWT(t, uid),
		})
	assert.Equal(t, http.StatusCreated, rec.Code, "body=%s", string(raw))

	var out agentRespBody
	require.NoError(t, json.Unmarshal(raw, &out))
	assert.NotEmpty(t, out.ID)
	assert.Equal(t, "approved", out.Status)
	assert.Equal(t, int32(500), out.PricePerCallCents)
}

func TestPostCreateAgent_NoAuth(t *testing.T) {
	e, _ := setupHandlerTest(t)

	rec, _ := doRequest(t, e, http.MethodPost, "/api/v1/creator/agents",
		validBody(freshSlug("h-noauth")), nil)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestPostCreateAgent_NotCreator(t *testing.T) {
	e, pool := setupHandlerTest(t)
	uid := insertUserHandler(t, pool, false, false) // is_creator=false

	rec, raw := doRequest(t, e, http.MethodPost, "/api/v1/creator/agents",
		validBody(freshSlug("h-not-creator")), map[string]string{
			"Authorization": signJWT(t, uid),
		})
	assert.Equal(t, http.StatusForbidden, rec.Code, "body=%s", string(raw))
}

func TestPostCreateAgent_BadSlug(t *testing.T) {
	e, pool := setupHandlerTest(t)
	uid := insertUserHandler(t, pool, true, false)

	body := validBody("placeholder")
	body["slug"] = "Invalid Slug" // 含空格 + 大写
	rec, raw := doRequest(t, e, http.MethodPost, "/api/v1/creator/agents",
		body, map[string]string{
			"Authorization": signJWT(t, uid),
		})
	// 422 (validator) 或 400 (service regex) 或 409 (DB CHECK) 都可
	assert.Contains(t,
		[]int{http.StatusUnprocessableEntity, http.StatusBadRequest, http.StatusConflict},
		rec.Code, "body=%s", string(raw))
}

func TestPostCreateAgent_DuplicateSlug(t *testing.T) {
	e, pool := setupHandlerTest(t)
	uid := insertUserHandler(t, pool, true, false)
	tok := signJWT(t, uid)
	slug := freshSlug("h-dup")

	rec, _ := doRequest(t, e, http.MethodPost, "/api/v1/creator/agents",
		validBody(slug), map[string]string{"Authorization": tok})
	require.Equal(t, http.StatusCreated, rec.Code)

	rec2, _ := doRequest(t, e, http.MethodPost, "/api/v1/creator/agents",
		validBody(slug), map[string]string{"Authorization": tok})
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

// ────────────────────────────────────────────────────────────
// GET /agents/check-slug
// ────────────────────────────────────────────────────────────

func TestGetCheckSlug_Available(t *testing.T) {
	e, _ := setupHandlerTest(t)

	slug := freshSlug("h-avail")
	rec, raw := doRequest(t, e, http.MethodGet,
		"/api/v1/agents/check-slug?slug="+slug, nil, nil)
	assert.Equal(t, http.StatusOK, rec.Code, "body=%s", string(raw))

	var out struct {
		Slug      string `json:"slug"`
		Available bool   `json:"available"`
	}
	require.NoError(t, json.Unmarshal(raw, &out))
	assert.True(t, out.Available)
}

func TestGetCheckSlug_Taken(t *testing.T) {
	e, pool := setupHandlerTest(t)
	uid := insertUserHandler(t, pool, true, false)
	tok := signJWT(t, uid)
	slug := freshSlug("h-taken")

	createRec, _ := doRequest(t, e, http.MethodPost, "/api/v1/creator/agents",
		validBody(slug), map[string]string{"Authorization": tok})
	require.Equal(t, http.StatusCreated, createRec.Code)

	rec, raw := doRequest(t, e, http.MethodGet,
		"/api/v1/agents/check-slug?slug="+slug, nil, nil)
	assert.Equal(t, http.StatusOK, rec.Code, "body=%s", string(raw))

	var out struct {
		Slug      string `json:"slug"`
		Available bool   `json:"available"`
	}
	require.NoError(t, json.Unmarshal(raw, &out))
	assert.False(t, out.Available)
}

// ────────────────────────────────────────────────────────────
// PATCH /creator/agents/:id
// ────────────────────────────────────────────────────────────

func TestPatchUpdateAgent(t *testing.T) {
	e, pool := setupHandlerTest(t)
	uid := insertUserHandler(t, pool, true, false)
	tok := signJWT(t, uid)

	createRec, createRaw := doRequest(t, e, http.MethodPost, "/api/v1/creator/agents",
		validBody(freshSlug("h-patch")), map[string]string{"Authorization": tok})
	require.Equal(t, http.StatusCreated, createRec.Code, "body=%s", string(createRaw))
	var created agentRespBody
	require.NoError(t, json.Unmarshal(createRaw, &created))

	updBody := map[string]any{
		"name":                 "Patched Name",
		"description":          "Patched description text.",
		"endpoint_url":         "https://example.com/v2",
		"endpoint_auth_header": "Bearer new",
		"price_per_call_cents": 777,
		"tags":                 []string{"patched"},
	}
	rec, raw := doRequest(t, e, http.MethodPatch,
		"/api/v1/creator/agents/"+created.ID, updBody,
		map[string]string{"Authorization": tok})
	assert.Equal(t, http.StatusOK, rec.Code, "body=%s", string(raw))

	var updated agentRespBody
	require.NoError(t, json.Unmarshal(raw, &updated))
	assert.Equal(t, "Patched Name", updated.Name)
	assert.Equal(t, int32(777), updated.PricePerCallCents)
}

// ────────────────────────────────────────────────────────────
// DELETE /creator/agents/:id (Disable)
// ────────────────────────────────────────────────────────────

func TestDeleteAgent(t *testing.T) {
	e, pool := setupHandlerTest(t)
	uid := insertUserHandler(t, pool, true, false)
	tok := signJWT(t, uid)

	createRec, createRaw := doRequest(t, e, http.MethodPost, "/api/v1/creator/agents",
		validBody(freshSlug("h-del")), map[string]string{"Authorization": tok})
	require.Equal(t, http.StatusCreated, createRec.Code)
	var created agentRespBody
	require.NoError(t, json.Unmarshal(createRaw, &created))

	rec, raw := doRequest(t, e, http.MethodDelete,
		"/api/v1/creator/agents/"+created.ID, nil,
		map[string]string{"Authorization": tok})
	// 204 No Content 优先，200 也可接受
	assert.Contains(t, []int{http.StatusNoContent, http.StatusOK}, rec.Code,
		"body=%s", string(raw))

	// 直接 DB 校验：DisableAgent 现在改 lifecycle_status='disabled'。
	var lifecycle string
	require.NoError(t,
		pool.QueryRow(context.Background(),
			`SELECT lifecycle_status FROM agents WHERE id=$1`, created.ID).Scan(&lifecycle))
	assert.Equal(t, "disabled", lifecycle)
}

// ────────────────────────────────────────────────────────────
// GET /creator/agents
// ────────────────────────────────────────────────────────────

func TestGetListMyAgents(t *testing.T) {
	e, pool := setupHandlerTest(t)
	uid := insertUserHandler(t, pool, true, false)
	tok := signJWT(t, uid)

	for i := 0; i < 2; i++ {
		rec, raw := doRequest(t, e, http.MethodPost, "/api/v1/creator/agents",
			validBody(freshSlug("h-list")), map[string]string{"Authorization": tok})
		require.Equal(t, http.StatusCreated, rec.Code, "body=%s", string(raw))
	}

	rec, raw := doRequest(t, e, http.MethodGet,
		"/api/v1/creator/agents", nil, map[string]string{"Authorization": tok})
	assert.Equal(t, http.StatusOK, rec.Code, "body=%s", string(raw))

	// 接口可能返回 {"items":[...]} 或裸数组 —— 两种都允许
	var asObj struct {
		Items []agentRespBody `json:"items"`
	}
	if err := json.Unmarshal(raw, &asObj); err == nil && asObj.Items != nil {
		assert.Len(t, asObj.Items, 2)
		return
	}
	var asArr []agentRespBody
	require.NoError(t, json.Unmarshal(raw, &asArr))
	assert.Len(t, asArr, 2)
}

// ────────────────────────────────────────────────────────────
// POST /admin/agents/:id/certify   (Phase 2 缺口 2 后)
// ────────────────────────────────────────────────────────────

func TestPostCertifyAgent_AsRegularUser(t *testing.T) {
	e, pool := setupHandlerTest(t)

	creator := insertUserHandler(t, pool, true, false)
	creatorTok := signJWT(t, creator)
	createRec, createRaw := doRequest(t, e, http.MethodPost, "/api/v1/creator/agents",
		validBody(freshSlug("h-certify-1")), map[string]string{"Authorization": creatorTok})
	require.Equal(t, http.StatusCreated, createRec.Code)
	var created agentRespBody
	require.NoError(t, json.Unmarshal(createRaw, &created))

	// 普通用户尝试 certify -> 403
	regular := insertUserHandler(t, pool, false, false)
	rec, raw := doRequest(t, e, http.MethodPost,
		"/api/v1/admin/agents/"+created.ID+"/certify", nil,
		map[string]string{"Authorization": signJWT(t, regular)})
	assert.Equal(t, http.StatusForbidden, rec.Code, "body=%s", string(raw))
}

func TestPostCertifyAgent_AsAdmin(t *testing.T) {
	e, pool := setupHandlerTest(t)

	creator := insertUserHandler(t, pool, true, false)
	creatorTok := signJWT(t, creator)
	createRec, createRaw := doRequest(t, e, http.MethodPost, "/api/v1/creator/agents",
		validBody(freshSlug("h-certify-2")), map[string]string{"Authorization": creatorTok})
	require.Equal(t, http.StatusCreated, createRec.Code)
	var created agentRespBody
	require.NoError(t, json.Unmarshal(createRaw, &created))
	// 创作者发起认证申请 → certification_status='pending'
	reqRec, reqRaw := doRequest(t, e, http.MethodPost,
		"/api/v1/creator/agents/"+created.ID+"/request-certification", nil,
		map[string]string{"Authorization": creatorTok})
	require.Equal(t, http.StatusOK, reqRec.Code, "body=%s", string(reqRaw))

	admin := insertUserHandler(t, pool, false, true)
	rec, raw := doRequest(t, e, http.MethodPost,
		"/api/v1/admin/agents/"+created.ID+"/certify", nil,
		map[string]string{"Authorization": signJWT(t, admin)})
	assert.Equal(t, http.StatusOK, rec.Code, "body=%s", string(raw))

	var certStatus string
	require.NoError(t,
		pool.QueryRow(context.Background(),
			`SELECT certification_status FROM agents WHERE id=$1`, created.ID).Scan(&certStatus))
	assert.Equal(t, "certified", certStatus)
}

// ────────────────────────────────────────────────────────────
// POST /admin/agents/:id/reject-certification
// ────────────────────────────────────────────────────────────

func TestPostRejectCertification_MissingReason(t *testing.T) {
	e, pool := setupHandlerTest(t)

	creator := insertUserHandler(t, pool, true, false)
	creatorTok := signJWT(t, creator)
	createRec, createRaw := doRequest(t, e, http.MethodPost, "/api/v1/creator/agents",
		validBody(freshSlug("h-reject-cert-1")), map[string]string{"Authorization": creatorTok})
	require.Equal(t, http.StatusCreated, createRec.Code)
	var created agentRespBody
	require.NoError(t, json.Unmarshal(createRaw, &created))
	reqRec, _ := doRequest(t, e, http.MethodPost,
		"/api/v1/creator/agents/"+created.ID+"/request-certification", nil,
		map[string]string{"Authorization": creatorTok})
	require.Equal(t, http.StatusOK, reqRec.Code)

	admin := insertUserHandler(t, pool, false, true)
	rec, raw := doRequest(t, e, http.MethodPost,
		"/api/v1/admin/agents/"+created.ID+"/reject-certification",
		map[string]any{},
		map[string]string{"Authorization": signJWT(t, admin)})
	assert.Contains(t,
		[]int{http.StatusUnprocessableEntity, http.StatusBadRequest},
		rec.Code, "missing reason should be 4xx; body=%s", string(raw))
}

func TestPostRejectCertification_AsAdmin(t *testing.T) {
	e, pool := setupHandlerTest(t)

	creator := insertUserHandler(t, pool, true, false)
	creatorTok := signJWT(t, creator)
	createRec, createRaw := doRequest(t, e, http.MethodPost, "/api/v1/creator/agents",
		validBody(freshSlug("h-reject-cert-2")), map[string]string{"Authorization": creatorTok})
	require.Equal(t, http.StatusCreated, createRec.Code)
	var created agentRespBody
	require.NoError(t, json.Unmarshal(createRaw, &created))
	reqRec, _ := doRequest(t, e, http.MethodPost,
		"/api/v1/creator/agents/"+created.ID+"/request-certification", nil,
		map[string]string{"Authorization": creatorTok})
	require.Equal(t, http.StatusOK, reqRec.Code)

	admin := insertUserHandler(t, pool, false, true)
	body := map[string]any{"reason": "endpoint not reachable"}
	rec, raw := doRequest(t, e, http.MethodPost,
		"/api/v1/admin/agents/"+created.ID+"/reject-certification", body,
		map[string]string{"Authorization": signJWT(t, admin)})
	assert.Equal(t, http.StatusOK, rec.Code, "body=%s", string(raw))

	var certStatus string
	var reason *string
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT certification_status, rejection_reason FROM agents WHERE id=$1`, created.ID).
		Scan(&certStatus, &reason))
	assert.Equal(t, "rejected", certStatus)
	require.NotNil(t, reason)
	assert.Contains(t, *reason, "endpoint not reachable")
}

// ────────────────────────────────────────────────────────────
// POST /me/become-creator
// ────────────────────────────────────────────────────────────

func TestPostBecomeCreator(t *testing.T) {
	e, pool := setupHandlerTest(t)

	uid := insertUserHandler(t, pool, false, false) // is_creator=false
	rec, raw := doRequest(t, e, http.MethodPost,
		"/api/v1/me/become-creator", nil, map[string]string{
			"Authorization": signJWT(t, uid),
		})
	assert.Contains(t, []int{http.StatusOK, http.StatusNoContent}, rec.Code,
		"body=%s", string(raw))

	var v bool
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT is_creator FROM users WHERE id=$1`, uid).Scan(&v))
	assert.True(t, v, "is_creator must be TRUE after become")
}
