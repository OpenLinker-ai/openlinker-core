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

// readAgentStatus 直接 SQL 读 agent 表 status / approved_at / rejection_reason。
func readAgentStatus(t *testing.T, pool *pgxpool.Pool, agentID uuid.UUID) (status string, approvedAt *time.Time, rejectionReason *string) {
	t.Helper()
	err := pool.QueryRow(context.Background(),
		`SELECT status, approved_at, rejection_reason FROM agents WHERE id=$1`, agentID).
		Scan(&status, &approvedAt, &rejectionReason)
	require.NoError(t, err)
	return
}

// forceAgentStatus 直接 SQL 改写 agent 状态（为了测各种状态过渡）。
func forceAgentStatus(t *testing.T, pool *pgxpool.Pool, agentID uuid.UUID, status string) {
	t.Helper()
	switch status {
	case "approved":
		_, err := pool.Exec(context.Background(),
			`UPDATE agents SET status='approved', approved_at=COALESCE(approved_at, NOW()), rejection_reason=NULL WHERE id=$1`, agentID)
		require.NoError(t, err)
	case "pending":
		_, err := pool.Exec(context.Background(),
			`UPDATE agents SET status='pending', approved_at=NULL, rejection_reason=NULL WHERE id=$1`, agentID)
		require.NoError(t, err)
	case "rejected":
		_, err := pool.Exec(context.Background(),
			`UPDATE agents SET status='rejected', approved_at=NULL, rejection_reason='forced rejection' WHERE id=$1`, agentID)
		require.NoError(t, err)
	case "disabled":
		_, err := pool.Exec(context.Background(),
			`UPDATE agents SET status='disabled', approved_at=COALESCE(approved_at, NOW()) WHERE id=$1`, agentID)
		require.NoError(t, err)
	default:
		_, err := pool.Exec(context.Background(),
			`UPDATE agents SET status=$2 WHERE id=$1`, agentID, status)
		require.NoError(t, err)
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
	assert.Equal(t, "approved", resp.Status, "new agent should be public immediately")
	assert.NotNil(t, resp.ApprovedAt, "approved_at should be set when public immediately")
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

func TestApproveAgent(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreatorWithWallet(t, pool)
	created, err := svc.CreateAgent(ctx, uid, validCreateReq(freshSlug("approve")))
	require.NoError(t, err)
	agentID, _ := uuid.Parse(created.ID)
	forceAgentStatus(t, pool, agentID, "pending")

	require.NoError(t, svc.ApproveAgent(ctx, agentID))

	status, approvedAt, _ := readAgentStatus(t, pool, agentID)
	assert.Equal(t, "approved", status)
	require.NotNil(t, approvedAt, "approved_at must be set")
	assert.WithinDuration(t, time.Now(), *approvedAt, 5*time.Second)
}

func TestApproveAgent_AlreadyApproved(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreatorWithWallet(t, pool)
	created, err := svc.CreateAgent(ctx, uid, validCreateReq(freshSlug("approve-twice")))
	require.NoError(t, err)
	agentID, _ := uuid.Parse(created.ID)

	status, firstApprovedAt, _ := readAgentStatus(t, pool, agentID)
	assert.Equal(t, "approved", status)
	require.NotNil(t, firstApprovedAt)

	// 已公开 Agent 不应重复刷新 approved_at。
	err = svc.ApproveAgent(ctx, agentID)
	assertHTTPStatusIn(t, err, http.StatusConflict, http.StatusBadRequest)
	_, secondApprovedAt, _ := readAgentStatus(t, pool, agentID)
	require.NotNil(t, secondApprovedAt)
	assert.Equal(t, firstApprovedAt.Unix(), secondApprovedAt.Unix(),
		"approved_at must NOT be updated on idempotent re-approve")
}

func TestRejectAgent(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreatorWithWallet(t, pool)
	created, err := svc.CreateAgent(ctx, uid, validCreateReq(freshSlug("reject")))
	require.NoError(t, err)
	agentID, _ := uuid.Parse(created.ID)
	forceAgentStatus(t, pool, agentID, "pending")

	reason := "endpoint not reachable"
	require.NoError(t, svc.RejectAgent(ctx, agentID, reason))

	status, _, rejReason := readAgentStatus(t, pool, agentID)
	assert.Equal(t, "rejected", status)
	require.NotNil(t, rejReason)
	assert.Equal(t, reason, *rejReason)
}

func TestRejectAgent_NonPending(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreatorWithWallet(t, pool)
	created, err := svc.CreateAgent(ctx, uid, validCreateReq(freshSlug("reject-approved")))
	require.NoError(t, err)
	agentID, _ := uuid.Parse(created.ID)

	// 强制 approved
	forceAgentStatus(t, pool, agentID, "approved")

	err = svc.RejectAgent(ctx, agentID, "too late")
	// approved 之后再 reject -> 4xx 错误
	assertHTTPStatusIn(t, err,
		http.StatusBadRequest,
		http.StatusConflict,
		http.StatusUnprocessableEntity)

	// status 不应改变
	status, _, _ := readAgentStatus(t, pool, agentID)
	assert.Equal(t, "approved", status, "rejected attempt must NOT mutate approved status")
}

func TestRejectAgent_EmptyReason(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreatorWithWallet(t, pool)
	created, err := svc.CreateAgent(ctx, uid, validCreateReq(freshSlug("reject-no-reason")))
	require.NoError(t, err)
	agentID, _ := uuid.Parse(created.ID)

	err = svc.RejectAgent(ctx, agentID, "")
	assertHTTPStatusIn(t, err,
		http.StatusBadRequest,
		http.StatusUnprocessableEntity)
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
