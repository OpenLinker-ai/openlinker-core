// Package agent_test - Service 层集成测试（subagent-2b 写）。
//
// 这些测试需要真实 Postgres，通过环境变量 TEST_DATABASE_URL 提供。
// 本地开发可通过 docker-compose 起 Postgres，然后：
//
//	export TEST_DATABASE_URL="postgres://postgres:postgres@localhost:5432/openlinker_test?sslmode=disable"
//	go test -tags agent_impl ./internal/agent/... -race -v
//
// 若 TEST_DATABASE_URL 未设置，所有 service 集成测试都会 t.Skip()。
//
// 期望 API（subagent-2a 在 internal/agent/{dto,service}.go 实现）：
//
//	type CreateAgentRequest struct {
//	    Slug, Name, Description, EndpointURL string
//	    EndpointAuthHeader string
//	    PricePerCallCents  int32
//	    Tags               []string
//	}
//	type UpdateAgentRequest struct {
//	    Name, Description, EndpointURL, EndpointAuthHeader string
//	    PricePerCallCents int32
//	    Tags              []string
//	}
//	type AgentResponse struct {
//	    ID, CreatorID, Slug, Name, Description, EndpointURL, Status string
//	    PricePerCallCents int32
//	    Tags []string
//	    RejectionReason *string
//	    ApprovedAt *string
//	    CreatedAt, UpdatedAt string
//	}
//	type SlugCheckResponse struct { Slug string; Available bool }
//	type RejectRequest struct { Reason string }
//
//	func NewService(pool *pgxpool.Pool, cfg *config.Config) *Service
//	func (s *Service) CreateAgent(ctx, creatorID uuid.UUID, req *CreateAgentRequest) (*AgentResponse, error)
//	func (s *Service) UpdateAgent(ctx, agentID, creatorID uuid.UUID, req *UpdateAgentRequest) (*AgentResponse, error)
//	func (s *Service) DisableAgent(ctx, agentID, creatorID uuid.UUID) error
//	func (s *Service) ListMyAgents(ctx, creatorID uuid.UUID) ([]AgentResponse, error)
//	func (s *Service) CheckSlug(ctx, slug string) (*SlugCheckResponse, error)
//	func (s *Service) BecomeCreator(ctx, userID uuid.UUID) error
//	func (s *Service) ListPendingForAdmin(ctx) ([]AgentResponse, error)
//	func (s *Service) ApproveAgent(ctx, agentID uuid.UUID) error
//	func (s *Service) RejectAgent(ctx, agentID uuid.UUID, reason string) error
//
// 错误约定：返回 *httpx.HTTPError，按 HTTP 语义区分：
//   - 非 creator 创建 -> Forbidden (403)
//   - slug 重复 -> Conflict (409)
//   - slug 格式错误 -> BadRequest 或 Unprocessable
//   - 不属于自己的 agent -> NotFound (404)（不暴露存在性）
//   - 状态不允许的操作（reject 已 approved） -> Conflict 或 BadRequest
//
// 共享 helper（在 testhelpers_test.go）：
//   - truncateAll / setupTestDB / skipIfNoDB
//   - insertCreatorUser / setupTestData
//   - createApprovedAgent + WithName/WithStatus/WithTags ...
//
// 共享 assertHTTPStatus 在 market_service_test.go。
package agent_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kinzhi/openlinker-core/pkg/agent"
	"github.com/kinzhi/openlinker-core/pkg/config"
	db "github.com/kinzhi/openlinker-core/pkg/db/generated"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

// newTestService 构造 Service。
func newTestService(t *testing.T, pool *pgxpool.Pool) *agent.Service {
	t.Helper()
	cfg := &config.Config{
		PlatformFeeRate: 0.25,
	}
	return agent.NewService(pool, cfg)
}

// insertNonCreatorUser 直接 INSERT 一个非 creator 用户（绕过 auth）。
// is_creator=FALSE。返回 user_id。
func insertNonCreatorUser(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	uid := uuid.New()
	email := "agent-nc-" + uid.String()[:8] + "@example.com"
	_, err := pool.Exec(ctx,
		`INSERT INTO users (id, email, password_hash, display_name, is_creator)
		 VALUES ($1, $2, $3, $4, FALSE)`,
		uid, email, "x", "Non Creator")
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO wallets (user_id) VALUES ($1)`, uid)
	require.NoError(t, err)
	return uid
}

// insertCreatorWithWallet wraps setupTestData 的 creator-only 形式。
func insertCreatorWithWallet(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	uid, _ := setupTestData(t, pool)
	return uid
}

// freshSlug 生成一个稳定合法的 slug。
func freshSlug(prefix string) string {
	suffix := strings.ReplaceAll(uuid.NewString()[:8], "_", "-")
	return strings.ToLower(prefix + "-" + suffix)
}

// validCreateReq 构造一个完整合法的 CreateAgentRequest。
func validCreateReq(slug string) *agent.CreateAgentRequest {
	return &agent.CreateAgentRequest{
		Slug:               slug,
		Name:               "Test Agent",
		Description:        "An agent for unit tests.",
		EndpointURL:        "https://example.com/agent/" + slug,
		EndpointAuthHeader: "Bearer secret",
		PricePerCallCents:  500,
		Tags:               []string{"test", "unit"},
	}
}

type mockDryRunner struct {
	output map[string]interface{}
	errMsg string
}

func (m mockDryRunner) DryRun(_ context.Context, _ *db.Agent, _ map[string]interface{}) (map[string]interface{}, string) {
	return m.output, m.errMsg
}

// assertHTTPStatusIn 接受多种允许的状态码（如 400 或 422）。
func assertHTTPStatusIn(t *testing.T, err error, allowed ...int) {
	t.Helper()
	require.Error(t, err)
	var he *httpx.HTTPError
	require.True(t, errors.As(err, &he), "expected *httpx.HTTPError, got %T (%v)", err, err)
	for _, s := range allowed {
		if he.Status == s {
			return
		}
	}
	t.Fatalf("status %d not in allowed %v (msg=%s)", he.Status, allowed, he.Message)
}

func createDryRunReadyAgent(t *testing.T, svc *agent.Service, creatorID uuid.UUID, slug string) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	created, err := svc.CreateAgent(ctx, creatorID, validCreateReq(slug))
	require.NoError(t, err)
	agentID := uuid.MustParse(created.ID)
	_, err = svc.UpsertCapability(ctx, agentID, creatorID, &agent.UpsertCapabilityRequest{
		InputSchema: map[string]interface{}{
			"type":       "object",
			"properties": map[string]interface{}{"query": map[string]interface{}{"type": "string"}},
			"required":   []interface{}{"query"},
		},
		OutputSchema: map[string]interface{}{
			"type":       "object",
			"properties": map[string]interface{}{"result": map[string]interface{}{"type": "string"}},
			"required":   []interface{}{"result"},
		},
		Summary: "dry run ready",
	})
	require.NoError(t, err)
	_, err = svc.CreateExample(ctx, agentID, creatorID, &agent.CreateExampleRequest{
		Title:              "health example",
		InputJSON:          map[string]interface{}{"query": "ping"},
		ExpectedOutputJSON: map[string]interface{}{"result": "pong"},
	})
	require.NoError(t, err)
	return agentID
}

// readAgentStatus 直接 SQL 读 agent 表派生 status（与 toAgentResponse 的 deriveLegacyStatus 同口径）
// 以及 certified_at / rejection_reason。Phase 2 缺口 2 后保留旧签名以兼容已有用例。
func readAgentStatus(t *testing.T, pool *pgxpool.Pool, agentID uuid.UUID) (status string, certifiedAt *time.Time, rejectionReason *string) {
	t.Helper()
	err := pool.QueryRow(context.Background(),
		`SELECT (CASE
		            WHEN lifecycle_status='disabled' THEN 'disabled'
		            WHEN certification_status='pending' THEN 'pending'
		            WHEN certification_status='rejected' THEN 'rejected'
		            ELSE 'approved'
		        END)::text, certified_at, rejection_reason
		 FROM agents WHERE id=$1`, agentID).
		Scan(&status, &certifiedAt, &rejectionReason)
	require.NoError(t, err)
	return
}

// forceAgentStatus 直接 SQL 改写 agent 三维状态（为了测各种状态过渡）。
// 接受旧 status 文案；映射如下：
//
//	approved → lifecycle=active, cert=unreviewed   （不写 certified_at）
//	pending  → lifecycle=active, cert=pending
//	rejected → lifecycle=active, cert=rejected
//	disabled → lifecycle=disabled
//	certified→ lifecycle=active, cert=certified, certified_at=NOW()
func forceAgentStatus(t *testing.T, pool *pgxpool.Pool, agentID uuid.UUID, status string) {
	t.Helper()
	ctx := context.Background()
	switch status {
	case "approved":
		_, err := pool.Exec(ctx,
			`UPDATE agents SET lifecycle_status='active', certification_status='unreviewed', rejection_reason=NULL WHERE id=$1`, agentID)
		require.NoError(t, err)
	case "pending":
		_, err := pool.Exec(ctx,
			`UPDATE agents SET lifecycle_status='active', certification_status='pending', rejection_reason=NULL WHERE id=$1`, agentID)
		require.NoError(t, err)
	case "rejected":
		_, err := pool.Exec(ctx,
			`UPDATE agents SET lifecycle_status='active', certification_status='rejected', rejection_reason='forced rejection' WHERE id=$1`, agentID)
		require.NoError(t, err)
	case "disabled":
		_, err := pool.Exec(ctx,
			`UPDATE agents SET lifecycle_status='disabled' WHERE id=$1`, agentID)
		require.NoError(t, err)
	case "certified":
		_, err := pool.Exec(ctx,
			`UPDATE agents SET lifecycle_status='active', certification_status='certified', certified_at=NOW(), rejection_reason=NULL WHERE id=$1`, agentID)
		require.NoError(t, err)
	default:
		t.Fatalf("forceAgentStatus: unknown legacy status %q", status)
	}
}

// readUserIsCreator 读用户 is_creator 标志。
func readUserIsCreator(t *testing.T, pool *pgxpool.Pool, userID uuid.UUID) bool {
	t.Helper()
	var v bool
	err := pool.QueryRow(context.Background(),
		`SELECT is_creator FROM users WHERE id=$1`, userID).Scan(&v)
	require.NoError(t, err)
	return v
}

// ────────────────────────────────────────────────────────────
// CreateAgent
// ────────────────────────────────────────────────────────────

func TestCreateAgent_HappyPath(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreatorWithWallet(t, pool)

	req := validCreateReq(freshSlug("create-happy"))
	resp, err := svc.CreateAgent(ctx, uid, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, req.Slug, resp.Slug)
	assert.Equal(t, req.Name, resp.Name)
	assert.Equal(t, "approved", resp.Status, "new agent should be public immediately (derived)")
	assert.Equal(t, "active", resp.LifecycleStatus)
	assert.Equal(t, "public", resp.Visibility)
	assert.Equal(t, "unreviewed", resp.CertificationStatus)
	assert.Equal(t, req.PricePerCallCents, resp.PricePerCallCents)
	assert.ElementsMatch(t, req.Tags, resp.Tags)

	// DB 里确实有这条 agent
	var count int
	err = pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM agents WHERE creator_id=$1 AND slug=$2`,
		uid, req.Slug).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestCreateAgent_NotCreator(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertNonCreatorUser(t, pool) // is_creator=FALSE

	req := validCreateReq(freshSlug("not-creator"))
	_, err := svc.CreateAgent(ctx, uid, req)
	assertHTTPStatus(t, err, http.StatusForbidden)
}

func TestCreateAgent_DuplicateSlug(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreatorWithWallet(t, pool)
	slug := freshSlug("dup")

	_, err := svc.CreateAgent(ctx, uid, validCreateReq(slug))
	require.NoError(t, err)

	// 第二次相同 slug -> Conflict
	_, err = svc.CreateAgent(ctx, uid, validCreateReq(slug))
	assertHTTPStatus(t, err, http.StatusConflict)
}

func TestCreateAgent_InvalidSlug(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreatorWithWallet(t, pool)

	cases := []struct {
		name string
		slug string
	}{
		{"with space", "Invalid Slug"},
		{"upper case", "InvalidSlug"},
		{"leading dash", "-invalid"},
		{"trailing dash", "invalid-"},
		{"underscore", "invalid_slug"},
		{"empty", ""},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			req := validCreateReq("placeholder")
			req.Slug = tc.slug
			_, err := svc.CreateAgent(ctx, uid, req)
			// 校验失败 -> 400 / 422 / 409 都可（DB CHECK 兜底也算 Conflict）
			assertHTTPStatusIn(t, err,
				http.StatusBadRequest,
				http.StatusUnprocessableEntity,
				http.StatusConflict)
		})
	}
}

// ────────────────────────────────────────────────────────────
// UpdateAgent
// ────────────────────────────────────────────────────────────

func TestUpdateAgent_HappyPath(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreatorWithWallet(t, pool)
	created, err := svc.CreateAgent(ctx, uid, validCreateReq(freshSlug("upd")))
	require.NoError(t, err)

	agentID, err := uuid.Parse(created.ID)
	require.NoError(t, err)

	upd := &agent.UpdateAgentRequest{
		Name:               "Updated Name",
		Description:        "New description text.",
		EndpointURL:        "https://example.com/v2",
		EndpointAuthHeader: "Bearer new-secret",
		PricePerCallCents:  999,
		Tags:               []string{"updated"},
	}
	resp, err := svc.UpdateAgent(ctx, agentID, uid, upd)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "Updated Name", resp.Name)
	assert.Equal(t, int32(999), resp.PricePerCallCents)
	assert.ElementsMatch(t, []string{"updated"}, resp.Tags)
}

func TestSetVisibility_HappyPath(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreatorWithWallet(t, pool)
	created, err := svc.CreateAgent(ctx, uid, validCreateReq(freshSlug("visibility")))
	require.NoError(t, err)
	agentID, err := uuid.Parse(created.ID)
	require.NoError(t, err)

	resp, err := svc.SetVisibility(ctx, agentID, uid, "private")
	require.NoError(t, err)
	require.Equal(t, "private", resp.Visibility)
	require.Equal(t, "active", resp.LifecycleStatus)
}

func TestUpdateAgent_ApprovedEditable(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreatorWithWallet(t, pool)
	created, err := svc.CreateAgent(ctx, uid, validCreateReq(freshSlug("upd-approved")))
	require.NoError(t, err)
	agentID, _ := uuid.Parse(created.ID)

	upd := &agent.UpdateAgentRequest{
		Name: "Updated approved agent", Description: "Updated public agent.",
		EndpointURL: "https://example.com/x", PricePerCallCents: 1,
	}
	resp, err := svc.UpdateAgent(ctx, agentID, uid, upd)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "approved", resp.Status)
	assert.Equal(t, "Updated approved agent", resp.Name)
}

func TestUpdateAgent_NotOwner(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	a := insertCreatorWithWallet(t, pool)
	b := insertCreatorWithWallet(t, pool)

	created, err := svc.CreateAgent(ctx, a, validCreateReq(freshSlug("not-owner")))
	require.NoError(t, err)
	agentID, _ := uuid.Parse(created.ID)

	upd := &agent.UpdateAgentRequest{
		Name: "B trying to edit A", Description: "x",
		EndpointURL: "https://example.com/x", PricePerCallCents: 1,
	}
	_, err = svc.UpdateAgent(ctx, agentID, b, upd)
	// 不暴露存在性 -> 404
	assertHTTPStatus(t, err, http.StatusNotFound)
}

// ────────────────────────────────────────────────────────────
// DisableAgent
// ────────────────────────────────────────────────────────────

func TestDisableAgent(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreatorWithWallet(t, pool)
	created, err := svc.CreateAgent(ctx, uid, validCreateReq(freshSlug("disable")))
	require.NoError(t, err)
	agentID, _ := uuid.Parse(created.ID)

	// 先 approved 再 disable，更接近真实流程
	forceAgentStatus(t, pool, agentID, "approved")

	require.NoError(t, svc.DisableAgent(ctx, agentID, uid))

	status, _, _ := readAgentStatus(t, pool, agentID)
	assert.Equal(t, "disabled", status)
}

// ────────────────────────────────────────────────────────────
// ListMyAgents
// ────────────────────────────────────────────────────────────

func TestListMyAgents(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreatorWithWallet(t, pool)

	// 插 3 个，故意拉开 created_at
	for i := 0; i < 3; i++ {
		_, err := svc.CreateAgent(ctx, uid, validCreateReq(freshSlug("list-mine")))
		require.NoError(t, err)
		// 微小 sleep 保证 created_at 严格递增
		time.Sleep(10 * time.Millisecond)
	}

	got, err := svc.ListMyAgents(ctx, uid)
	require.NoError(t, err)
	require.Len(t, got, 3)

	// created_at DESC：first.CreatedAt >= second.CreatedAt
	t1, err := time.Parse(time.RFC3339Nano, got[0].CreatedAt)
	if err != nil {
		t1, err = time.Parse(time.RFC3339, got[0].CreatedAt)
	}
	require.NoError(t, err, "CreatedAt 应是 RFC3339")
	t2, err := time.Parse(time.RFC3339Nano, got[1].CreatedAt)
	if err != nil {
		t2, err = time.Parse(time.RFC3339, got[1].CreatedAt)
	}
	require.NoError(t, err)
	assert.False(t, t1.Before(t2), "ListMyAgents must be ordered by created_at DESC")
}

func TestListMyAgents_Empty(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreatorWithWallet(t, pool)
	got, err := svc.ListMyAgents(ctx, uid)
	require.NoError(t, err)
	assert.Empty(t, got, "no agents -> empty slice (not error)")
}

func TestListMyAgents_IncludesWebhookURL(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreatorWithWallet(t, pool)
	created, err := svc.CreateAgent(ctx, uid, validCreateReq(freshSlug("webhook-url")))
	require.NoError(t, err)
	agentID, err := uuid.Parse(created.ID)
	require.NoError(t, err)

	const webhookURL = "https://example.com/openlinker/webhook"
	_, err = pool.Exec(ctx,
		`UPDATE agents SET webhook_url=$2 WHERE id=$1`,
		agentID, webhookURL)
	require.NoError(t, err)

	got, err := svc.ListMyAgents(ctx, uid)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NotNil(t, got[0].WebhookURL)
	assert.Equal(t, webhookURL, *got[0].WebhookURL)
}

func TestGetMyAgentOnboardingAndDeleteExample(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	ownerID := insertCreatorWithWallet(t, pool)
	otherCreatorID := insertCreatorWithWallet(t, pool)
	slug := freshSlug("onboarding")
	created, err := svc.CreateAgent(ctx, ownerID, validCreateReq(slug))
	require.NoError(t, err)
	agentID := uuid.MustParse(created.ID)

	owned, err := svc.GetMyAgent(ctx, agentID, ownerID)
	require.NoError(t, err)
	assert.Equal(t, slug, owned.Slug)
	assert.Equal(t, "direct_http", owned.ConnectionMode)
	assert.Equal(t, "public", owned.Visibility)

	_, err = svc.GetMyAgent(ctx, agentID, otherCreatorID)
	assertHTTPStatus(t, err, http.StatusNotFound)
	_, err = svc.GetAgentOnboarding(ctx, agentID, otherCreatorID)
	assertHTTPStatus(t, err, http.StatusNotFound)

	initial, err := svc.GetAgentOnboarding(ctx, agentID, ownerID)
	require.NoError(t, err)
	assert.Equal(t, agentID.String(), initial.Status.AgentID)
	assert.True(t, initial.Status.EndpointSet)
	assert.False(t, initial.Status.CapabilitiesSet)
	assert.False(t, initial.Status.ExamplesSet)
	assert.Nil(t, initial.Capability)
	assert.Empty(t, initial.Examples)
	assert.Equal(t, "unknown", initial.Availability.Status)

	inputSchema := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"query": map[string]interface{}{"type": "string"},
		},
		"required": []interface{}{"query"},
	}
	outputSchema := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"result": map[string]interface{}{"type": "string"},
		},
		"required": []interface{}{"result"},
	}
	capability, err := svc.UpsertCapability(ctx, agentID, ownerID, &agent.UpsertCapabilityRequest{
		InputSchema:  inputSchema,
		OutputSchema: outputSchema,
		Summary:      "single query to single result",
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), capability.Version)

	example, err := svc.CreateExample(ctx, agentID, ownerID, &agent.CreateExampleRequest{
		Title:              "happy path",
		InputJSON:          map[string]interface{}{"query": "ping"},
		ExpectedOutputJSON: map[string]interface{}{"result": "pong"},
		SortOrder:          7,
	})
	require.NoError(t, err)

	onboarding, err := svc.GetAgentOnboarding(ctx, agentID, ownerID)
	require.NoError(t, err)
	assert.True(t, onboarding.Status.CapabilitiesSet)
	assert.True(t, onboarding.Status.ExamplesSet)
	require.NotNil(t, onboarding.Capability)
	assert.Equal(t, "single query to single result", onboarding.Capability.Summary)
	require.Len(t, onboarding.Examples, 1)
	assert.Equal(t, example.ID, onboarding.Examples[0].ID)
	assert.Equal(t, int32(7), onboarding.Examples[0].SortOrder)
	assert.Equal(t, "ping", onboarding.Examples[0].InputJSON["query"])
	assert.Equal(t, "pong", onboarding.Examples[0].ExpectedOutputJSON["result"])

	exampleID := uuid.MustParse(example.ID)
	err = svc.DeleteExample(ctx, agentID, exampleID, otherCreatorID)
	assertHTTPStatus(t, err, http.StatusNotFound)

	require.NoError(t, svc.DeleteExample(ctx, agentID, exampleID, ownerID))
	afterDelete, err := svc.GetAgentOnboarding(ctx, agentID, ownerID)
	require.NoError(t, err)
	assert.True(t, afterDelete.Status.CapabilitiesSet)
	assert.False(t, afterDelete.Status.ExamplesSet)
	assert.Empty(t, afterDelete.Examples)
	require.NotNil(t, afterDelete.Capability)
	assert.Equal(t, capability.ID, afterDelete.Capability.ID)
}

// ────────────────────────────────────────────────────────────
// CheckSlug
// ────────────────────────────────────────────────────────────

func TestCheckSlug_Available(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	resp, err := svc.CheckSlug(ctx, freshSlug("avail"))
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Available, "fresh slug must be available")
}

func TestCheckSlug_Taken(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreatorWithWallet(t, pool)
	slug := freshSlug("taken")
	_, err := svc.CreateAgent(ctx, uid, validCreateReq(slug))
	require.NoError(t, err)

	resp, err := svc.CheckSlug(ctx, slug)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, slug, resp.Slug)
	assert.False(t, resp.Available, "existing slug must be reported taken")
}

// ────────────────────────────────────────────────────────────
// BecomeCreator
// ────────────────────────────────────────────────────────────

func TestBecomeCreator(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertNonCreatorUser(t, pool) // 起始：is_creator=false
	require.False(t, readUserIsCreator(t, pool, uid))

	require.NoError(t, svc.BecomeCreator(ctx, uid))
	assert.True(t, readUserIsCreator(t, pool, uid),
		"after BecomeCreator, users.is_creator must be TRUE")

	// 幂等：再调一次仍然成功，不报错
	require.NoError(t, svc.BecomeCreator(ctx, uid))
	assert.True(t, readUserIsCreator(t, pool, uid))
}

// ────────────────────────────────────────────────────────────
// Approve / Reject
// ────────────────────────────────────────────────────────────

// Phase 2 缺口 2 后：原 ApproveAgent/RejectAgent 改名为 CertifyAgent/RejectCertification，
// 仅对 certification_status='pending' 生效（创作者先 RequestCertification）。
//
// 旧测试里 "approved" 的语义不再对应 certification 字段——新建即 lifecycle=active +
// cert=unreviewed，所以测 happy-path 时要先 RequestCertification → pending。

func TestCertifyAgent_HappyPath(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreatorWithWallet(t, pool)
	created, err := svc.CreateAgent(ctx, uid, validCreateReq(freshSlug("certify")))
	require.NoError(t, err)
	agentID, _ := uuid.Parse(created.ID)

	require.NoError(t, svc.RequestCertification(ctx, agentID, uid))
	require.NoError(t, svc.CertifyAgent(ctx, agentID))

	derived, certifiedAt, _ := readAgentStatus(t, pool, agentID)
	assert.Equal(t, "approved", derived, "certified 在派生 status 中表现为 approved")
	require.NotNil(t, certifiedAt, "certified_at 必须写入")
	assert.WithinDuration(t, time.Now(), *certifiedAt, 5*time.Second)
}

func TestCertifyAgent_RequiresPending(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreatorWithWallet(t, pool)
	created, err := svc.CreateAgent(ctx, uid, validCreateReq(freshSlug("certify-direct")))
	require.NoError(t, err)
	agentID, _ := uuid.Parse(created.ID)

	// 未 request-certification 直接 certify → 409
	err = svc.CertifyAgent(ctx, agentID)
	assertHTTPStatusIn(t, err, http.StatusConflict, http.StatusBadRequest)
}

func TestRequestCertification_NotRepeatable(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreatorWithWallet(t, pool)
	created, err := svc.CreateAgent(ctx, uid, validCreateReq(freshSlug("request-cert-twice")))
	require.NoError(t, err)
	agentID, _ := uuid.Parse(created.ID)

	require.NoError(t, svc.RequestCertification(ctx, agentID, uid))
	// 第二次再申请：已 pending → 409
	err = svc.RequestCertification(ctx, agentID, uid)
	assertHTTPStatusIn(t, err, http.StatusConflict, http.StatusBadRequest)
}

func TestRejectCertification_HappyPath(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreatorWithWallet(t, pool)
	created, err := svc.CreateAgent(ctx, uid, validCreateReq(freshSlug("reject-cert")))
	require.NoError(t, err)
	agentID, _ := uuid.Parse(created.ID)
	require.NoError(t, svc.RequestCertification(ctx, agentID, uid))

	reason := "endpoint not reachable"
	require.NoError(t, svc.RejectCertification(ctx, agentID, reason))

	derived, _, rejReason := readAgentStatus(t, pool, agentID)
	assert.Equal(t, "rejected", derived)
	require.NotNil(t, rejReason)
	assert.Equal(t, reason, *rejReason)
}

func TestRejectCertification_NonPending(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreatorWithWallet(t, pool)
	created, err := svc.CreateAgent(ctx, uid, validCreateReq(freshSlug("reject-cert-nonpending")))
	require.NoError(t, err)
	agentID, _ := uuid.Parse(created.ID)

	// 没有先 request-certification，状态是 unreviewed → 拒绝失败
	err = svc.RejectCertification(ctx, agentID, "too late")
	assertHTTPStatusIn(t, err, http.StatusConflict, http.StatusBadRequest)

	derived, _, _ := readAgentStatus(t, pool, agentID)
	assert.Equal(t, "approved", derived, "未申请认证的 Agent 不应被状态改写")
}

func TestRejectCertification_EmptyReason(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreatorWithWallet(t, pool)
	created, err := svc.CreateAgent(ctx, uid, validCreateReq(freshSlug("reject-cert-no-reason")))
	require.NoError(t, err)
	agentID, _ := uuid.Parse(created.ID)
	require.NoError(t, svc.RequestCertification(ctx, agentID, uid))

	err = svc.RejectCertification(ctx, agentID, "")
	assertHTTPStatusIn(t, err, http.StatusBadRequest, http.StatusUnprocessableEntity)
}

func TestRunDryRunMarksAvailabilityHealthy(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	svc.SetDryRunner(mockDryRunner{output: map[string]interface{}{"result": "pong"}})
	ctx := context.Background()

	uid := insertCreatorWithWallet(t, pool)
	agentID := createDryRunReadyAgent(t, svc, uid, freshSlug("dryrun-health"))

	resp, err := svc.RunDryRun(ctx, agentID, uid)
	require.NoError(t, err)
	assert.Equal(t, "pass", resp.Result)
	assert.Equal(t, "healthy", resp.Availability.Status)
	assert.Empty(t, resp.RepairHints)

	var status string
	var failures int32
	err = pool.QueryRow(ctx,
		`SELECT availability_status, consecutive_failures
		 FROM agent_availability_snapshots WHERE agent_id=$1`, agentID).
		Scan(&status, &failures)
	require.NoError(t, err)
	assert.Equal(t, "healthy", status)
	assert.Equal(t, int32(0), failures)
}

func TestRunDryRunFailureReturnsRepairHintsAndMarksDegraded(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	svc.SetDryRunner(mockDryRunner{errMsg: "endpoint 调用失败: connection refused"})
	ctx := context.Background()

	uid := insertCreatorWithWallet(t, pool)
	agentID := createDryRunReadyAgent(t, svc, uid, freshSlug("dryrun-fail"))

	resp, err := svc.RunDryRun(ctx, agentID, uid)
	require.NoError(t, err)
	assert.Equal(t, "fail", resp.Result)
	assert.Equal(t, "degraded", resp.Availability.Status)
	assert.NotEmpty(t, resp.RepairHints)

	var status string
	var failures int32
	err = pool.QueryRow(ctx,
		`SELECT availability_status, consecutive_failures
		 FROM agent_availability_snapshots WHERE agent_id=$1`, agentID).
		Scan(&status, &failures)
	require.NoError(t, err)
	assert.Equal(t, "degraded", status)
	assert.Equal(t, int32(1), failures)
}

func TestRunDueAvailabilityChecksCreatesUnreadAlert(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	svc.SetDryRunner(mockDryRunner{errMsg: "endpoint 调用失败: connection refused"})
	ctx := context.Background()

	uid := insertCreatorWithWallet(t, pool)
	agentID := createDryRunReadyAgent(t, svc, uid, freshSlug("monitor-fail"))

	resp, err := svc.RunDueAvailabilityChecks(ctx, 10, 0)
	require.NoError(t, err)
	assert.Equal(t, int32(1), resp.Checked)
	assert.Equal(t, int32(1), resp.Failed)
	require.Len(t, resp.Alerts, 1)
	assert.Equal(t, agentID.String(), resp.Alerts[0].AgentID)
	assert.Equal(t, "availability_failed", resp.Alerts[0].Type)
	assert.Equal(t, "degraded", resp.Alerts[0].AvailabilityStatus)
	assert.NotEmpty(t, resp.Alerts[0].RepairHints)

	alerts, err := svc.ListAvailabilityAlerts(ctx, uid, 20)
	require.NoError(t, err)
	assert.Equal(t, int32(1), alerts.Total)
	assert.Equal(t, int32(1), alerts.Unread)
	require.Len(t, alerts.Items, 1)
	assert.Equal(t, resp.Alerts[0].ID, alerts.Items[0].ID)
	assert.Equal(t, "availability_failed", alerts.Items[0].Type)

	alertID := uuid.MustParse(alerts.Items[0].ID)
	_, err = svc.MarkAvailabilityAlertRead(ctx, uid, alertID)
	require.NoError(t, err)
	alerts, err = svc.ListAvailabilityAlerts(ctx, uid, 20)
	require.NoError(t, err)
	assert.Equal(t, int32(0), alerts.Unread)
}

func TestRunDueAvailabilityChecksCreatesRecoveryAlert(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreatorWithWallet(t, pool)
	agentID := createDryRunReadyAgent(t, svc, uid, freshSlug("monitor-recover"))

	svc.SetDryRunner(mockDryRunner{errMsg: "endpoint 调用失败: timeout"})
	_, err := svc.RunDueAvailabilityChecks(ctx, 10, 0)
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`UPDATE agent_availability_snapshots
		 SET last_checked_at = NOW() - INTERVAL '1 hour'
		 WHERE agent_id = $1`, agentID)
	require.NoError(t, err)

	svc.SetDryRunner(mockDryRunner{output: map[string]interface{}{"result": "pong"}})
	resp, err := svc.RunDueAvailabilityChecks(ctx, 10, 0)
	require.NoError(t, err)
	assert.Equal(t, int32(1), resp.Checked)
	assert.Equal(t, int32(1), resp.Passed)
	require.Len(t, resp.Alerts, 1)
	assert.Equal(t, "availability_recovered", resp.Alerts[0].Type)
	assert.Equal(t, "healthy", resp.Alerts[0].AvailabilityStatus)

	alerts, err := svc.ListAvailabilityAlerts(ctx, uid, 20)
	require.NoError(t, err)
	assert.Equal(t, int32(2), alerts.Total)
	assert.Contains(t, []string{alerts.Items[0].Type, alerts.Items[1].Type}, "availability_recovered")
}

// ────────────────────────────────────────────────────────────
// ListPendingForAdmin
// ────────────────────────────────────────────────────────────

func TestListPendingForAdmin(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreatorWithWallet(t, pool)

	// 3 pending + 1 approved（pending 只用于人工处理队列，不是默认发布状态）
	for i := 0; i < 3; i++ {
		created, err := svc.CreateAgent(ctx, uid, validCreateReq(freshSlug("pend")))
		require.NoError(t, err)
		agentID, _ := uuid.Parse(created.ID)
		forceAgentStatus(t, pool, agentID, "pending")
	}
	approvedResp, err := svc.CreateAgent(ctx, uid, validCreateReq(freshSlug("pend-approved")))
	require.NoError(t, err)
	approvedID, _ := uuid.Parse(approvedResp.ID)
	forceAgentStatus(t, pool, approvedID, "approved")

	got, err := svc.ListPendingForAdmin(ctx)
	require.NoError(t, err)
	assert.Len(t, got, 3, "should return only pending agents")

	for _, a := range got {
		assert.Equal(t, "pending", a.Status, "all returned agents must be pending")
		assert.NotEmpty(t, a.ID)
		// admin 视图应包含 creator 信息（dto.AgentResponse.Creator）
		if a.Creator != nil {
			assert.NotEmpty(t, a.Creator.ID, "admin list should include creator info")
		}
	}
}
