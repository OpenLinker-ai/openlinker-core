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
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

// newTestService 构造 Service。
func newTestService(t *testing.T, pool *pgxpool.Pool) *agent.Service {
	t.Helper()
	cfg := &config.Config{}
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
	return uid
}

// insertCreator wraps setupTestData 的 creator-only 形式。
func insertCreator(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
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

func validObjectSchema(requiredField string) map[string]interface{} {
	return map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			requiredField: map[string]interface{}{"type": "string"},
		},
		"required": []interface{}{requiredField},
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

func readAgentEndpointAuthHeader(t *testing.T, pool *pgxpool.Pool, agentID uuid.UUID) *string {
	t.Helper()
	var header *string
	err := pool.QueryRow(context.Background(),
		`SELECT endpoint_auth_header FROM agents WHERE id=$1`, agentID).
		Scan(&header)
	require.NoError(t, err)
	return header
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

	uid := insertCreator(t, pool)

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

func TestCreateAgent_BindsDeclaredSkillIDs(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreator(t, pool)
	req := validCreateReq(freshSlug("create-skills"))
	req.SkillIDs = []string{" data/sql-query ", "content/translation", "data/sql-query"}

	resp, err := svc.CreateAgent(ctx, uid, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.ElementsMatch(t, []string{"data/sql-query", "content/translation"}, resp.SkillIDs)

	agentID, err := uuid.Parse(resp.ID)
	require.NoError(t, err)
	var skillIDs []string
	err = pool.QueryRow(ctx,
		`SELECT COALESCE(ARRAY_AGG(skill_id ORDER BY skill_id), ARRAY[]::text[]) FROM agent_skills WHERE agent_id=$1`,
		agentID).Scan(&skillIDs)
	require.NoError(t, err)
	assert.Equal(t, []string{"content/translation", "data/sql-query"}, skillIDs)
}

func TestCreateAgent_RejectsUnknownSkillWithoutCreatingAgent(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreator(t, pool)
	slug := freshSlug("bad-skill")
	req := validCreateReq(slug)
	req.SkillIDs = []string{"missing/skill"}

	_, err := svc.CreateAgent(ctx, uid, req)
	assertHTTPStatus(t, err, http.StatusBadRequest)

	var count int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM agents WHERE slug=$1`, slug).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
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

	uid := insertCreator(t, pool)
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

	uid := insertCreator(t, pool)

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

	uid := insertCreator(t, pool)
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

	header := readAgentEndpointAuthHeader(t, pool, agentID)
	require.NotNil(t, header)
	assert.Equal(t, "Bearer new-secret", *header)

	resp, err = svc.UpdateAgent(ctx, agentID, uid, &agent.UpdateAgentRequest{
		Name:              "Updated Name Again",
		Description:       "Credentials should be preserved when omitted.",
		EndpointURL:       "https://example.com/v3",
		PricePerCallCents: 111,
		Tags:              []string{"preserve"},
	})
	require.NoError(t, err)
	assert.Equal(t, "Updated Name Again", resp.Name)
	header = readAgentEndpointAuthHeader(t, pool, agentID)
	require.NotNil(t, header)
	assert.Equal(t, "Bearer new-secret", *header)

	resp, err = svc.UpdateAgent(ctx, agentID, uid, &agent.UpdateAgentRequest{
		Name:              "Updated Without Auth",
		Description:       "Credentials should be cleared only when explicitly requested.",
		EndpointURL:       "https://example.com/v4",
		PricePerCallCents: 112,
		Tags:              []string{"cleared"},
		ClearEndpointAuth: true,
	})
	require.NoError(t, err)
	assert.Equal(t, "Updated Without Auth", resp.Name)
	assert.Nil(t, readAgentEndpointAuthHeader(t, pool, agentID))
}

func TestSetVisibility_HappyPath(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreator(t, pool)
	created, err := svc.CreateAgent(ctx, uid, validCreateReq(freshSlug("visibility")))
	require.NoError(t, err)
	agentID, err := uuid.Parse(created.ID)
	require.NoError(t, err)

	resp, err := svc.SetVisibility(ctx, agentID, uid, "private")
	require.NoError(t, err)
	require.Equal(t, "private", resp.Visibility)
	require.Equal(t, "active", resp.LifecycleStatus)
}

func TestBrowserInteractionPolicyCanBeOwnerStagedBeforeFirstSession(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	ownerID := insertCreator(t, pool)
	req := validCreateReq(freshSlug("initial-browser-policy"))
	req.Visibility = "private"
	req.ConnectionMode = "runtime"
	created, err := svc.CreateAgent(ctx, ownerID, req)
	require.NoError(t, err)
	agentID := uuid.MustParse(created.ID)

	initial, err := svc.GetBrowserInteractionPolicy(ctx, agentID, ownerID)
	require.NoError(t, err)
	require.Equal(t, "restricted", initial.InteractionPolicy)
	require.Equal(t, int64(1), initial.InteractionPolicyGeneration)
	require.Empty(t, initial.BrowserMutationOrigins)

	staged, err := svc.UpdateBrowserInteractionPolicy(
		ctx,
		agentID,
		ownerID,
		&agent.UpdateBrowserInteractionPolicyRequest{
			InteractionPolicy:      " full ",
			BrowserMutationOrigins: []string{"https://EXAMPLE.com:443"},
		},
	)
	require.NoError(t, err)
	require.Equal(t, "full", staged.InteractionPolicy)
	require.Equal(t, int64(1), staged.InteractionPolicyGeneration)
	require.Equal(t, []string{"https://example.com"}, staged.BrowserMutationOrigins)

	var profileCount, intentCount int
	var browserDeclared bool
	require.NoError(t, pool.QueryRow(ctx, `
SELECT
    (SELECT count(*) FROM runtime_agent_execution_profiles WHERE agent_id = $1),
    (SELECT count(*) FROM runtime_agent_browser_policy_intents WHERE agent_id = $1),
    (SELECT browser_execution_profile FROM agents WHERE id = $1)`, agentID).
		Scan(&profileCount, &intentCount, &browserDeclared))
	require.Zero(t, profileCount)
	require.Equal(t, 1, intentCount)
	require.True(t, browserDeclared)

	_, err = svc.SetVisibility(ctx, agentID, ownerID, "public")
	assertHTTPStatus(t, err, http.StatusConflict)
	_, err = svc.UpdateAgent(ctx, agentID, ownerID, &agent.UpdateAgentRequest{
		Name:              "Declared Browser Agent",
		Description:       "Must retain the Runtime connection boundary.",
		EndpointURL:       "https://example.com/direct",
		PricePerCallCents: 0,
		Tags:              []string{"browser"},
		Visibility:        "private",
		ConnectionMode:    "direct_http",
		ClearEndpointAuth: true,
	})
	assertHTTPStatus(t, err, http.StatusConflict)
}

func TestUpdateAgentNodeVisibilityAndDisabledBoundaries(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	ownerID := insertCreator(t, pool)
	otherCreatorID := insertCreator(t, pool)
	created, err := svc.CreateAgent(ctx, ownerID, validCreateReq(freshSlug("agent-node-update")))
	require.NoError(t, err)
	agentID := uuid.MustParse(created.ID)

	updated, err := svc.UpdateAgent(ctx, agentID, ownerID, &agent.UpdateAgentRequest{
		Name:               "Runtime Worker",
		Description:        "Updated to the transport-neutral Runtime Worker mode.",
		EndpointURL:        "https://example.com/ignored-for-agent-node",
		EndpointAuthHeader: "  Bearer node-secret  ",
		PricePerCallCents:  321,
		Tags:               []string{" Runtime ", "WS"},
		Visibility:         "unlisted",
		ConnectionMode:     "runtime",
	})
	require.NoError(t, err)
	assert.Equal(t, "runtime", updated.ConnectionMode)
	assert.Equal(t, "openlinker-runtime://"+created.Slug, updated.EndpointURL)
	assert.Equal(t, "unlisted", updated.Visibility)
	assert.ElementsMatch(t, []string{"runtime", "ws"}, updated.Tags)
	assert.Nil(t, updated.MCPToolName)

	_, err = svc.SetVisibility(ctx, agentID, ownerID, "invalid")
	assertHTTPStatus(t, err, http.StatusUnprocessableEntity)

	err = svc.DisableAgent(ctx, agentID, otherCreatorID)
	assertHTTPStatus(t, err, http.StatusNotFound)

	require.NoError(t, svc.DisableAgent(ctx, agentID, ownerID))
	_, err = svc.UpdateAgent(ctx, agentID, ownerID, &agent.UpdateAgentRequest{
		Name:              "Cannot edit disabled",
		Description:       "disabled",
		EndpointURL:       "https://example.com/disabled",
		PricePerCallCents: 1,
		Tags:              []string{"disabled"},
	})
	assertHTTPStatus(t, err, http.StatusForbidden)

	_, err = svc.SetVisibility(ctx, agentID, ownerID, "private")
	assertHTTPStatus(t, err, http.StatusForbidden)
}

// waitForBlockedBackend waits until some backend of the test database is
// queued on a heavyweight lock, i.e. the concurrent writer under test is
// parked behind the transaction the test holds open.
func waitForBlockedBackend(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	require.Eventually(t, func() bool {
		var waiting int
		err := pool.QueryRow(context.Background(), `
SELECT COUNT(*) FROM pg_stat_activity
WHERE datname = current_database() AND state = 'active' AND wait_event_type = 'Lock'`).Scan(&waiting)
		return err == nil && waiting > 0
	}, 30*time.Second, 10*time.Millisecond)
}

// updateAgentRacingWith starts UpdateAgent while another transaction holds an
// uncommitted settings change, commits that change once UpdateAgent is queued
// behind it, and returns UpdateAgent's result.
func updateAgentRacingWith(
	t *testing.T,
	pool *pgxpool.Pool,
	svc *agent.Service,
	agentID, creatorID uuid.UUID,
	concurrentSQL string,
	req *agent.UpdateAgentRequest,
) (*agent.AgentResponse, error) {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, concurrentSQL, agentID)
	require.NoError(t, err)

	type result struct {
		resp *agent.AgentResponse
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := svc.UpdateAgent(ctx, agentID, creatorID, req)
		done <- result{resp: resp, err: err}
	}()
	waitForBlockedBackend(t, pool)
	require.NoError(t, tx.Commit(ctx))
	select {
	case got := <-done:
		return got.resp, got.err
	case <-time.After(30 * time.Second):
		t.Fatal("UpdateAgent did not finish after the concurrent change committed")
		return nil, nil
	}
}

func TestUpdateAgentKeepsConcurrentVisibilityChangeWhenOmitted(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	ownerID := insertCreator(t, pool)
	created, err := svc.CreateAgent(ctx, ownerID, validCreateReq(freshSlug("update-visibility-race")))
	require.NoError(t, err)
	require.Equal(t, "public", created.Visibility)
	agentID := uuid.MustParse(created.ID)

	updated, err := updateAgentRacingWith(t, pool, svc, agentID, ownerID,
		`UPDATE agents SET visibility = 'private' WHERE id = $1`,
		&agent.UpdateAgentRequest{
			Name:              "Renamed While Hidden",
			Description:       "settings save without visibility",
			PricePerCallCents: 700,
			Tags:              []string{"settings"},
		})
	require.NoError(t, err)
	assert.Equal(t, "Renamed While Hidden", updated.Name)
	assert.Equal(t, int32(700), updated.PricePerCallCents)
	assert.Equal(t, "private", updated.Visibility, "omitted visibility must not revert a concurrent change")

	var visibility, name, endpoint string
	var authHeader *string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT visibility, name, endpoint_url, endpoint_auth_header FROM agents WHERE id = $1`, agentID).
		Scan(&visibility, &name, &endpoint, &authHeader))
	assert.Equal(t, "private", visibility)
	assert.Equal(t, "Renamed While Hidden", name)
	assert.Equal(t, created.EndpointURL, endpoint)
	require.NotNil(t, authHeader, "omitted auth header keeps carrying over")
	assert.Equal(t, "Bearer secret", *authHeader)
}

func TestUpdateAgentKeepsConcurrentConnectionChangeWhenOmitted(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	ownerID := insertCreator(t, pool)
	created, err := svc.CreateAgent(ctx, ownerID, validCreateReq(freshSlug("update-connection-race")))
	require.NoError(t, err)
	require.Equal(t, "direct_http", created.ConnectionMode)
	agentID := uuid.MustParse(created.ID)

	updated, err := updateAgentRacingWith(t, pool, svc, agentID, ownerID,
		`UPDATE agents
		 SET connection_mode = 'mcp_server', mcp_tool_name = 'search',
		     endpoint_url = 'https://example.com/mcp'
		 WHERE id = $1`,
		&agent.UpdateAgentRequest{
			Name:              "Moved Endpoint",
			Description:       "endpoint change without connection mode",
			EndpointURL:       "https://example.com/mcp-v2",
			PricePerCallCents: 500,
			Tags:              []string{"settings"},
		})
	require.NoError(t, err)
	assert.Equal(t, "mcp_server", updated.ConnectionMode, "omitted connection_mode must not revert a concurrent change")
	require.NotNil(t, updated.MCPToolName)
	assert.Equal(t, "search", *updated.MCPToolName)
	assert.Equal(t, "https://example.com/mcp-v2", updated.EndpointURL)
}

func TestUpdateAgent_ApprovedEditable(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreator(t, pool)
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

	a := insertCreator(t, pool)
	b := insertCreator(t, pool)

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

func TestServiceBoundaryValidationBranches(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	ownerID := insertCreator(t, pool)
	otherCreatorID := insertCreator(t, pool)

	_, err := svc.CreateAgent(ctx, uuid.New(), validCreateReq(freshSlug("missing-user")))
	assertHTTPStatus(t, err, http.StatusNotFound)

	missingTool := validCreateReq(freshSlug("mcp-missing-tool"))
	missingTool.ConnectionMode = "mcp_server"
	missingTool.MCPToolName = ""
	missingTool.EndpointURL = "https://example.com/mcp"
	_, err = svc.CreateAgent(ctx, ownerID, missingTool)
	assertHTTPStatus(t, err, http.StatusUnprocessableEntity)

	localDirect := validCreateReq(freshSlug("local-rejected"))
	localDirect.EndpointURL = "http://localhost:3000/agent"
	_, err = svc.CreateAgent(ctx, ownerID, localDirect)
	assertHTTPStatus(t, err, http.StatusUnprocessableEntity)

	mcpReq := validCreateReq(freshSlug("mcp-preserve-tool"))
	mcpReq.ConnectionMode = "mcp_server"
	mcpReq.EndpointURL = "https://example.com/mcp"
	mcpReq.MCPToolName = "lookup"
	mcpCreated, err := svc.CreateAgent(ctx, ownerID, mcpReq)
	require.NoError(t, err)
	mcpAgentID := uuid.MustParse(mcpCreated.ID)

	updated, err := svc.UpdateAgent(ctx, mcpAgentID, ownerID, &agent.UpdateAgentRequest{
		Name:              "MCP Updated",
		Description:       "preserves tool name when connection mode is unchanged",
		PricePerCallCents: 42,
		Tags:              []string{"mcp"},
	})
	require.NoError(t, err)
	require.NotNil(t, updated.MCPToolName)
	assert.Equal(t, "lookup", *updated.MCPToolName)
	assert.Equal(t, "https://example.com/mcp", updated.EndpointURL)

	_, err = svc.UpdateAgent(ctx, mcpAgentID, ownerID, &agent.UpdateAgentRequest{
		Name:              "Bad Local Update",
		Description:       "local endpoint is not allowed for direct mode by default",
		ConnectionMode:    "direct_http",
		EndpointURL:       "http://localhost:3000/agent",
		PricePerCallCents: 42,
		Tags:              []string{"bad"},
	})
	assertHTTPStatus(t, err, http.StatusUnprocessableEntity)

	_, err = svc.SetVisibility(ctx, uuid.New(), ownerID, "public")
	assertHTTPStatus(t, err, http.StatusNotFound)

	_, err = svc.UpsertCapability(ctx, mcpAgentID, ownerID, nil)
	assertHTTPStatus(t, err, http.StatusUnprocessableEntity)

	_, err = svc.UpsertCapability(ctx, mcpAgentID, ownerID, &agent.UpsertCapabilityRequest{
		InputSchema:  validObjectSchema("query"),
		OutputSchema: validObjectSchema("result"),
		Summary:      strings.Repeat("x", 1001),
	})
	assertHTTPStatus(t, err, http.StatusUnprocessableEntity)

	_, err = svc.UpsertCapability(ctx, mcpAgentID, otherCreatorID, &agent.UpsertCapabilityRequest{
		InputSchema:  validObjectSchema("query"),
		OutputSchema: validObjectSchema("result"),
		Summary:      "not owner",
	})
	assertHTTPStatus(t, err, http.StatusNotFound)

	_, err = svc.CreateExample(ctx, mcpAgentID, ownerID, nil)
	assertHTTPStatus(t, err, http.StatusUnprocessableEntity)
	_, err = svc.CreateExample(ctx, mcpAgentID, ownerID, &agent.CreateExampleRequest{
		Title:     " ",
		InputJSON: map[string]interface{}{"query": "ping"},
	})
	assertHTTPStatus(t, err, http.StatusUnprocessableEntity)
	_, err = svc.CreateExample(ctx, mcpAgentID, otherCreatorID, &agent.CreateExampleRequest{
		Title:     "not owner",
		InputJSON: map[string]interface{}{"query": "ping"},
	})
	assertHTTPStatus(t, err, http.StatusNotFound)

	err = svc.DeleteExample(ctx, mcpAgentID, uuid.New(), ownerID)
	assertHTTPStatus(t, err, http.StatusNotFound)

	svc.SetDryRunner(mockDryRunner{output: map[string]interface{}{"result": "pong"}})
	_, err = svc.RunDryRun(ctx, mcpAgentID, otherCreatorID)
	assertHTTPStatus(t, err, http.StatusNotFound)

	_, err = svc.UpsertCapability(ctx, mcpAgentID, ownerID, &agent.UpsertCapabilityRequest{
		InputSchema:  validObjectSchema("query"),
		OutputSchema: validObjectSchema("result"),
		Summary:      "valid capability",
	})
	require.NoError(t, err)
	example, err := svc.CreateExample(ctx, mcpAgentID, ownerID, &agent.CreateExampleRequest{
		Title:              "valid then corrupt",
		InputJSON:          map[string]interface{}{"query": "ping"},
		ExpectedOutputJSON: map[string]interface{}{"result": "pong"},
	})
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`UPDATE agent_examples SET input_json = '[]'::jsonb WHERE id = $1`,
		uuid.MustParse(example.ID))
	require.NoError(t, err)
	_, err = svc.RunDryRun(ctx, mcpAgentID, ownerID)
	assertHTTPStatus(t, err, http.StatusUnprocessableEntity)
}

// ────────────────────────────────────────────────────────────
// DisableAgent
// ────────────────────────────────────────────────────────────

func TestDisableAgent(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreator(t, pool)
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

	uid := insertCreator(t, pool)

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

	uid := insertCreator(t, pool)
	got, err := svc.ListMyAgents(ctx, uid)
	require.NoError(t, err)
	assert.Empty(t, got, "no agents -> empty slice (not error)")
}

func TestListMyAgentsPageStatusActiveFiltersDisabled(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreator(t, pool)
	activeAgent, err := svc.CreateAgent(ctx, uid, validCreateReq(freshSlug("active-page")))
	require.NoError(t, err)
	disabledAgent, err := svc.CreateAgent(ctx, uid, validCreateReq(freshSlug("disabled-page")))
	require.NoError(t, err)
	require.NoError(t, svc.DisableAgent(ctx, uuid.MustParse(disabledAgent.ID), uid))

	page, err := svc.ListMyAgentsPage(ctx, uid, agent.AgentListOptions{
		Status: "active",
		Limit:  10,
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, page.Total)
	require.Len(t, page.Items, 1)
	require.Equal(t, activeAgent.ID, page.Items[0].ID)
	require.Equal(t, "active", page.Items[0].LifecycleStatus)
	require.EqualValues(t, 1, page.Counts.Total)
}

func TestListMyAgentsPageDefaultsToNewestFirst(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreator(t, pool)
	olderAgent, err := svc.CreateAgent(ctx, uid, validCreateReq(freshSlug("older-page")))
	require.NoError(t, err)
	olderAgentID := uuid.MustParse(olderAgent.ID)
	insertLegacyTerminalRun(t, pool, uid, olderAgentID, "success", 12, 10, 2, 8, time.Now())

	time.Sleep(10 * time.Millisecond)
	newerAgent, err := svc.CreateAgent(ctx, uid, validCreateReq(freshSlug("newer-page")))
	require.NoError(t, err)

	defaultPage, err := svc.ListMyAgentsPage(ctx, uid, agent.AgentListOptions{
		Status: "active",
		Limit:  1,
	})
	require.NoError(t, err)
	require.EqualValues(t, 2, defaultPage.Total)
	require.Len(t, defaultPage.Items, 1)
	require.Equal(t, newerAgent.ID, defaultPage.Items[0].ID)

	callsPage, err := svc.ListMyAgentsPage(ctx, uid, agent.AgentListOptions{
		Status: "active",
		SortBy: "calls_this_month",
		Limit:  1,
	})
	require.NoError(t, err)
	require.Len(t, callsPage.Items, 1)
	require.Equal(t, olderAgent.ID, callsPage.Items[0].ID)
}

func TestListMyAgentsPageFiltersBySkillIDsBeforePagination(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreator(t, pool)
	_, err := svc.CreateAgent(ctx, uid, validCreateReq(freshSlug("unmatched-skill-page")))
	require.NoError(t, err)
	matchedAgent, err := svc.CreateAgent(ctx, uid, validCreateReq(freshSlug("matched-skill-page")))
	require.NoError(t, err)
	matchedAgentID := uuid.MustParse(matchedAgent.ID)
	_, err = pool.Exec(ctx,
		`INSERT INTO agent_skills (agent_id, skill_id) VALUES ($1, $2)`,
		matchedAgentID, "data/sql-query")
	require.NoError(t, err)

	page, err := svc.ListMyAgentsPage(ctx, uid, agent.AgentListOptions{
		Status:   "active",
		SkillIDs: []string{"DATA/SQL-QUERY", "data/sql-query"},
		Limit:    1,
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, page.Total)
	require.Len(t, page.Items, 1)
	require.Equal(t, matchedAgent.ID, page.Items[0].ID)
	require.Equal(t, []string{"data/sql-query"}, page.Items[0].SkillIDs)
}

func TestListMyAgents_IncludesDeclaredSkillIDs(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreator(t, pool)
	created, err := svc.CreateAgent(ctx, uid, validCreateReq(freshSlug("list-skill")))
	require.NoError(t, err)
	agentID, err := uuid.Parse(created.ID)
	require.NoError(t, err)

	_, err = pool.Exec(ctx,
		`INSERT INTO agent_skills (agent_id, skill_id) VALUES ($1, $2)`,
		agentID, "data/sql-query")
	require.NoError(t, err)

	got, err := svc.ListMyAgents(ctx, uid)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, []string{"data/sql-query"}, got[0].SkillIDs)
}

func TestListMyAgents_DoesNotExposeWebhookURL(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreator(t, pool)
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
	raw, err := json.Marshal(got[0])
	require.NoError(t, err)
	assert.NotContains(t, string(raw), "webhook_url")
	assert.NotContains(t, string(raw), webhookURL)
}

func TestGetMyAgentOnboardingAndDeleteExample(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	ownerID := insertCreator(t, pool)
	otherCreatorID := insertCreator(t, pool)
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

func TestCreateExampleRejectsSchemaMismatchesAndLimit(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	ownerID := insertCreator(t, pool)
	created, err := svc.CreateAgent(ctx, ownerID, validCreateReq(freshSlug("example-limits")))
	require.NoError(t, err)
	agentID := uuid.MustParse(created.ID)

	_, err = svc.UpsertCapability(ctx, agentID, ownerID, &agent.UpsertCapabilityRequest{
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"query": map[string]interface{}{"type": "string"},
			},
			"required": []interface{}{"query"},
		},
		OutputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"result": map[string]interface{}{"type": "string"},
			},
			"required": []interface{}{"result"},
		},
		Summary: "schema guarded examples",
	})
	require.NoError(t, err)

	_, err = svc.CreateExample(ctx, agentID, ownerID, &agent.CreateExampleRequest{
		Title:              "bad input type",
		InputJSON:          map[string]interface{}{"query": float64(123)},
		ExpectedOutputJSON: map[string]interface{}{"result": "ok"},
	})
	assertHTTPStatus(t, err, http.StatusUnprocessableEntity)

	_, err = svc.CreateExample(ctx, agentID, ownerID, &agent.CreateExampleRequest{
		Title:              "bad output type",
		InputJSON:          map[string]interface{}{"query": "ping"},
		ExpectedOutputJSON: map[string]interface{}{"result": float64(123)},
	})
	assertHTTPStatus(t, err, http.StatusUnprocessableEntity)

	for i := 0; i < 20; i++ {
		_, err = svc.CreateExample(ctx, agentID, ownerID, &agent.CreateExampleRequest{
			Title:              "valid example " + uuid.NewString()[:8],
			InputJSON:          map[string]interface{}{"query": "ping"},
			ExpectedOutputJSON: map[string]interface{}{"result": "pong"},
			SortOrder:          int32(i),
		})
		require.NoError(t, err)
	}

	_, err = svc.CreateExample(ctx, agentID, ownerID, &agent.CreateExampleRequest{
		Title:              "too many examples",
		InputJSON:          map[string]interface{}{"query": "ping"},
		ExpectedOutputJSON: map[string]interface{}{"result": "pong"},
	})
	assertHTTPStatus(t, err, http.StatusBadRequest)
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

	uid := insertCreator(t, pool)
	slug := freshSlug("taken")
	_, err := svc.CreateAgent(ctx, uid, validCreateReq(slug))
	require.NoError(t, err)

	resp, err := svc.CheckSlug(ctx, slug)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, slug, resp.Slug)
	assert.False(t, resp.Available, "existing slug must be reported taken")
}

func TestCheckSlug_InvalidReturnsUnavailableWithoutDB(t *testing.T) {
	svc := agent.NewService(nil, &config.Config{})

	resp, err := svc.CheckSlug(context.Background(), " Bad Slug ")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "Bad Slug", resp.Slug)
	assert.False(t, resp.Available)
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

func TestBecomeCreator_NotFound(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)

	err := svc.BecomeCreator(context.Background(), uuid.New())
	assertHTTPStatus(t, err, http.StatusNotFound)
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

	uid := insertCreator(t, pool)
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

	uid := insertCreator(t, pool)
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

	uid := insertCreator(t, pool)
	created, err := svc.CreateAgent(ctx, uid, validCreateReq(freshSlug("request-cert-twice")))
	require.NoError(t, err)
	agentID, _ := uuid.Parse(created.ID)

	require.NoError(t, svc.RequestCertification(ctx, agentID, uid))
	// 第二次再申请：已 pending → 409
	err = svc.RequestCertification(ctx, agentID, uid)
	assertHTTPStatusIn(t, err, http.StatusConflict, http.StatusBadRequest)
}

func TestCertificationBoundaryNotFoundAndOwnerIsolation(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	ownerID := insertCreator(t, pool)
	otherOwnerID := insertCreatorUser(t, pool, "Other Creator")
	created, err := svc.CreateAgent(ctx, ownerID, validCreateReq(freshSlug("cert-boundary")))
	require.NoError(t, err)
	agentID := uuid.MustParse(created.ID)

	err = svc.RequestCertification(ctx, agentID, otherOwnerID)
	assertHTTPStatus(t, err, http.StatusNotFound)
	err = svc.RequestCertification(ctx, uuid.New(), ownerID)
	assertHTTPStatus(t, err, http.StatusNotFound)
	err = svc.CertifyAgent(ctx, uuid.New())
	assertHTTPStatus(t, err, http.StatusNotFound)
	err = svc.RejectCertification(ctx, uuid.New(), "missing")
	assertHTTPStatus(t, err, http.StatusNotFound)
}

func TestRejectCertification_HappyPath(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreator(t, pool)
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

	uid := insertCreator(t, pool)
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

	uid := insertCreator(t, pool)
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

	uid := insertCreator(t, pool)
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

	uid := insertCreator(t, pool)
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

func TestRunDryRunPreconditionsAndOutputSchemaFailure(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	ownerID := insertCreator(t, pool)
	created, err := svc.CreateAgent(ctx, ownerID, validCreateReq(freshSlug("dryrun-preconditions")))
	require.NoError(t, err)
	agentID := uuid.MustParse(created.ID)

	_, err = svc.RunDryRun(ctx, agentID, ownerID)
	assertHTTPStatus(t, err, http.StatusServiceUnavailable)

	svc.SetDryRunner(mockDryRunner{output: map[string]interface{}{"result": float64(123)}})
	_, err = svc.RunDryRun(ctx, agentID, ownerID)
	assertHTTPStatus(t, err, http.StatusUnprocessableEntity)

	_, err = svc.CreateExample(ctx, agentID, ownerID, &agent.CreateExampleRequest{
		Title:     "input before capability",
		InputJSON: map[string]interface{}{"query": "ping"},
	})
	require.NoError(t, err)
	_, err = svc.RunDryRun(ctx, agentID, ownerID)
	assertHTTPStatus(t, err, http.StatusUnprocessableEntity)

	_, err = svc.UpsertCapability(ctx, agentID, ownerID, &agent.UpsertCapabilityRequest{
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"query": map[string]interface{}{"type": "string"},
			},
			"required": []interface{}{"query"},
		},
		OutputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"result": map[string]interface{}{"type": "string"},
			},
			"required": []interface{}{"result"},
		},
		Summary: "dry run output validation",
	})
	require.NoError(t, err)

	resp, err := svc.RunDryRun(ctx, agentID, ownerID)
	require.NoError(t, err)
	assert.Equal(t, "fail", resp.Result)
	require.NotNil(t, resp.Error)
	assert.Contains(t, *resp.Error, "output 不匹配 output_schema")
	assert.Contains(t, strings.Join(resp.RepairHints, "\n"), "input_schema / output_schema")
	assert.Equal(t, "degraded", resp.Availability.Status)
}

func TestRunDueAvailabilityChecksCreatesUnreadAlert(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	svc.SetDryRunner(mockDryRunner{errMsg: "endpoint 调用失败: connection refused"})
	ctx := context.Background()

	uid := insertCreator(t, pool)
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

	uid := insertCreator(t, pool)
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

func TestStartAvailabilityMonitorRunsInitialTick(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	svc.SetDryRunner(mockDryRunner{errMsg: "endpoint 调用失败: timeout"})
	ctx := context.Background()

	uid := insertCreator(t, pool)
	agentID := createDryRunReadyAgent(t, svc, uid, freshSlug("monitor-start"))

	monitorCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	agent.StartAvailabilityMonitor(monitorCtx, svc, agent.AvailabilityMonitorConfig{
		InitialDelay: time.Millisecond,
		Interval:     time.Hour,
		BatchSize:    1,
	})

	require.Eventually(t, func() bool {
		alerts, err := svc.ListAvailabilityAlerts(ctx, uid, 10)
		return err == nil &&
			alerts.Total == 1 &&
			alerts.Unread == 1 &&
			len(alerts.Items) == 1 &&
			alerts.Items[0].AgentID == agentID.String() &&
			alerts.Items[0].Type == "availability_failed"
	}, 2*time.Second, 20*time.Millisecond)
}

func TestRunDueAvailabilityChecksEscalatesAndIsolatesAlerts(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	_, err := svc.RunDueAvailabilityChecks(ctx, 0, -1)
	assertHTTPStatus(t, err, http.StatusServiceUnavailable)

	uid := insertCreator(t, pool)
	otherUID := insertCreatorUser(t, pool, "Other Creator")
	agentID := createDryRunReadyAgent(t, svc, uid, freshSlug("monitor-critical"))
	svc.SetDryRunner(mockDryRunner{errMsg: "endpoint 调用失败: timeout"})

	for i := 0; i < 3; i++ {
		resp, err := svc.RunDueAvailabilityChecks(ctx, 0, -1)
		require.NoError(t, err)
		assert.Equal(t, int32(1), resp.Checked)
		assert.Equal(t, int32(1), resp.Failed)
		require.Len(t, resp.Alerts, 1)

		_, err = pool.Exec(ctx,
			`UPDATE agent_availability_snapshots
			 SET last_checked_at = NOW() - INTERVAL '1 hour'
			 WHERE agent_id = $1`, agentID)
		require.NoError(t, err)
	}

	alerts, err := svc.ListAvailabilityAlerts(ctx, uid, 0)
	require.NoError(t, err)
	assert.Equal(t, int32(1), alerts.Total)
	assert.Equal(t, int32(1), alerts.Unread)
	require.Len(t, alerts.Items, 1)
	assert.Equal(t, "availability_failed", alerts.Items[0].Type)
	assert.Equal(t, "critical", alerts.Items[0].Severity)
	assert.Equal(t, "unreachable", alerts.Items[0].AvailabilityStatus)
	assert.Equal(t, int32(3), alerts.Items[0].ConsecutiveFailures)
	assert.NotNil(t, alerts.Items[0].LastError)
	assert.Contains(t, strings.Join(alerts.Items[0].RepairHints, "\n"), "首包响应")

	otherAlerts, err := svc.ListAvailabilityAlerts(ctx, otherUID, 200)
	require.NoError(t, err)
	assert.Equal(t, int32(0), otherAlerts.Total)
	assert.Empty(t, otherAlerts.Items)

	alertID := uuid.MustParse(alerts.Items[0].ID)
	_, err = svc.MarkAvailabilityAlertRead(ctx, otherUID, alertID)
	assertHTTPStatus(t, err, http.StatusNotFound)
	read, err := svc.MarkAvailabilityAlertRead(ctx, uid, alertID)
	require.NoError(t, err)
	assert.NotNil(t, read.ReadAt)
}

// scriptedDryRunner counts probes per Agent and can run a hook mid-probe.
type scriptedDryRunner struct {
	mu     sync.Mutex
	calls  map[uuid.UUID]int
	hook   func(ctx context.Context, agentID uuid.UUID)
	output map[string]interface{}
	errMsg string
}

func newScriptedDryRunner(errMsg string) *scriptedDryRunner {
	return &scriptedDryRunner{
		calls:  map[uuid.UUID]int{},
		output: map[string]interface{}{"result": "pong"},
		errMsg: errMsg,
	}
}

func (r *scriptedDryRunner) DryRun(ctx context.Context, a *db.Agent, _ map[string]interface{}) (map[string]interface{}, string) {
	r.mu.Lock()
	r.calls[a.ID]++
	hook := r.hook
	r.mu.Unlock()
	if hook != nil {
		hook(ctx, a.ID)
	}
	if r.errMsg != "" {
		return nil, r.errMsg
	}
	return r.output, ""
}

func (r *scriptedDryRunner) callsFor(agentID uuid.UUID) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls[agentID]
}

func TestRunDueAvailabilityChecksProbesEachAgentOnceAcrossReplicas(t *testing.T) {
	pool := setupTestDB(t)
	ctx := context.Background()
	const staleSeconds = 3600

	setup := newTestService(t, pool)
	setup.SetDryRunner(mockDryRunner{output: map[string]interface{}{"result": "pong"}})
	uid := insertCreator(t, pool)
	first := createDryRunReadyAgent(t, setup, uid, freshSlug("replica-first"))
	second := createDryRunReadyAgent(t, setup, uid, freshSlug("replica-second"))

	// Replica B stalls inside its probe of the first Agent while replica A runs
	// a whole batch, then resumes and reaches the second Agent after A has
	// already checked it.
	replicaB := newTestService(t, pool)
	runnerB := newScriptedDryRunner("endpoint 调用失败: timeout")
	entered, resume := make(chan struct{}), make(chan struct{})
	var resumeOnce sync.Once
	resumeB := func() { resumeOnce.Do(func() { close(resume) }) }
	t.Cleanup(resumeB)
	runnerB.hook = func(_ context.Context, agentID uuid.UUID) {
		if agentID == first {
			close(entered)
			<-resume
		}
	}
	replicaB.SetDryRunner(runnerB)
	type batch struct {
		resp *agent.AvailabilityCheckBatchResponse
		err  error
	}
	doneB := make(chan batch, 1)
	go func() {
		resp, err := replicaB.RunDueAvailabilityChecks(ctx, 10, staleSeconds)
		doneB <- batch{resp: resp, err: err}
	}()
	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("replica B never started probing the first Agent")
	}

	replicaA := newTestService(t, pool)
	runnerA := newScriptedDryRunner("endpoint 调用失败: timeout")
	replicaA.SetDryRunner(runnerA)
	respA, err := replicaA.RunDueAvailabilityChecks(ctx, 10, staleSeconds)
	require.NoError(t, err)
	assert.Equal(t, int32(1), respA.Checked, "replica A must skip the Agent replica B is probing")
	assert.Equal(t, 0, runnerA.callsFor(first))
	assert.Equal(t, 1, runnerA.callsFor(second))

	resumeB()
	var gotB batch
	select {
	case gotB = <-doneB:
	case <-time.After(30 * time.Second):
		t.Fatal("replica B batch did not finish")
	}
	require.NoError(t, gotB.err)
	assert.Equal(t, int32(1), gotB.resp.Checked, "replica B must not re-probe an Agent checked in this window")
	assert.Equal(t, 1, runnerB.callsFor(first))
	assert.Equal(t, 0, runnerB.callsFor(second))

	for _, agentID := range []uuid.UUID{first, second} {
		var failures int32
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT consecutive_failures FROM agent_availability_snapshots WHERE agent_id = $1`, agentID).
			Scan(&failures))
		assert.Equal(t, int32(1), failures, "one failed probe per staleness window")
	}

	// A single replica keeps re-checking once the window has passed.
	_, err = pool.Exec(ctx,
		`UPDATE agent_availability_snapshots SET last_checked_at = NOW() - INTERVAL '2 hours'`)
	require.NoError(t, err)
	respA, err = replicaA.RunDueAvailabilityChecks(ctx, 10, staleSeconds)
	require.NoError(t, err)
	assert.Equal(t, int32(2), respA.Checked)
}

func TestRunDueAvailabilityChecksDiscardsResultWhenAgentChangesDuringProbe(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreator(t, pool)
	svc.SetDryRunner(mockDryRunner{output: map[string]interface{}{"result": "pong"}})
	agentID := createDryRunReadyAgent(t, svc, uid, freshSlug("monitor-edit-race"))

	runner := newScriptedDryRunner("")
	runner.hook = func(hookCtx context.Context, id uuid.UUID) {
		_, err := pool.Exec(hookCtx,
			`UPDATE agents SET endpoint_url = 'https://example.com/agent/moved' WHERE id = $1`, id)
		require.NoError(t, err)
	}
	svc.SetDryRunner(runner)
	resp, err := svc.RunDueAvailabilityChecks(ctx, 10, 0)
	require.NoError(t, err)
	assert.Equal(t, 1, runner.callsFor(agentID))
	assert.Equal(t, int32(0), resp.Checked, "a probe of the previous endpoint must be discarded")
	assert.Empty(t, resp.Alerts)

	var snapshots int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM agent_availability_snapshots WHERE agent_id = $1`, agentID).Scan(&snapshots))
	assert.Equal(t, 0, snapshots, "no stale healthy snapshot after an endpoint change")
	var dryRunResult string
	var dryRunAt *time.Time
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT dry_run_last_result, dry_run_at FROM agent_onboarding_status WHERE agent_id = $1`, agentID).
		Scan(&dryRunResult, &dryRunAt))
	assert.Equal(t, "pending", dryRunResult)
	assert.Nil(t, dryRunAt)

	// The Agent stays due and the next pass probes the current endpoint.
	runner.hook = nil
	resp, err = svc.RunDueAvailabilityChecks(ctx, 10, 0)
	require.NoError(t, err)
	assert.Equal(t, int32(1), resp.Checked)
	assert.Equal(t, int32(1), resp.Passed)
	var status string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT availability_status FROM agent_availability_snapshots WHERE agent_id = $1`, agentID).Scan(&status))
	assert.Equal(t, "healthy", status)

	// The creator-triggered dry-run keeps writing its result unconditionally.
	runner.hook = func(hookCtx context.Context, id uuid.UUID) {
		_, err := pool.Exec(hookCtx, `UPDATE agents SET description = 'edited' WHERE id = $1`, id)
		require.NoError(t, err)
	}
	runner.errMsg = "endpoint 调用失败: connection refused"
	dryRun, err := svc.RunDryRun(ctx, agentID, uid)
	require.NoError(t, err)
	assert.Equal(t, "degraded", dryRun.Availability.Status)
}

// terminateAvailabilityLockHolder terminates the backend holding the Agent's
// monitor check lock and waits until it has exited.
func terminateAvailabilityLockHolder(t *testing.T, ctx context.Context, pool *pgxpool.Pool, agentID uuid.UUID) {
	t.Helper()
	var lockPID int32
	require.NoError(t, pool.QueryRow(ctx, `
SELECT l.pid
FROM pg_locks l
CROSS JOIN (SELECT hashtextextended('agent-availability-check:' || $1::text, 0) AS key) k
WHERE l.locktype = 'advisory' AND l.granted AND l.objsubid = 1
  AND l.classid::bigint = (k.key >> 32) & 4294967295
  AND l.objid::bigint = k.key & 4294967295`, agentID).Scan(&lockPID))
	var signaled bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, lockPID).Scan(&signaled))
	require.True(t, signaled)
	require.Eventually(t, func() bool {
		var gone bool
		err := pool.QueryRow(ctx,
			`SELECT NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1)`, lockPID).Scan(&gone)
		return err == nil && gone
	}, 30*time.Second, 10*time.Millisecond)
}

// assertNoStoredAvailabilityResult checks that a monitor result left no trace.
func assertNoStoredAvailabilityResult(t *testing.T, pool *pgxpool.Pool, agentID uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	var snapshots, alerts int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM agent_availability_snapshots WHERE agent_id = $1`, agentID).Scan(&snapshots))
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM agent_availability_alerts WHERE agent_id = $1`, agentID).Scan(&alerts))
	assert.Zero(t, snapshots, "no availability snapshot may be stored")
	assert.Zero(t, alerts, "no availability alert may be sent")
	var dryRunResult string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT dry_run_last_result FROM agent_onboarding_status WHERE agent_id = $1`, agentID).Scan(&dryRunResult))
	assert.Equal(t, "pending", dryRunResult, "no onboarding dry-run result may be stored")
}

func TestRunDueAvailabilityChecksDiscardsResultWhenLockSessionDiesBeforeCommit(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreator(t, pool)
	agentID := createDryRunReadyAgent(t, svc, uid, freshSlug("monitor-lock-commit"))
	svc.SetDryRunner(newScriptedDryRunner("endpoint 调用失败: timeout"))
	hookCalls := 0
	svc.SetAvailabilityResultBeforeCommitHook(func(hookCtx context.Context, id uuid.UUID) {
		// The guard and the writes already ran; the lock session dies before commit.
		hookCalls++
		terminateAvailabilityLockHolder(t, hookCtx, pool, id)
	})

	resp, err := svc.RunDueAvailabilityChecks(ctx, 10, 0)
	require.NoError(t, err)
	assert.Equal(t, 1, hookCalls)
	assert.Equal(t, int32(0), resp.Checked)
	assert.Empty(t, resp.Alerts)
	assertNoStoredAvailabilityResult(t, pool, agentID)

	svc.SetAvailabilityResultBeforeCommitHook(nil)
	resp, err = svc.RunDueAvailabilityChecks(ctx, 10, 0)
	require.NoError(t, err)
	assert.Equal(t, int32(1), resp.Checked)
	assert.Equal(t, int32(1), resp.Failed)
	require.Len(t, resp.Alerts, 1)
}

// newShortLockTimeoutService returns a Service on its own pool whose sessions
// give up on row locks after 200ms.
func newShortLockTimeoutService(t *testing.T) *agent.Service {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(skipIfNoDB(t))
	require.NoError(t, err)
	cfg.ConnConfig.RuntimeParams["lock_timeout"] = "200ms"
	shortLockPool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(shortLockPool.Close)
	return agent.NewService(shortLockPool, &config.Config{})
}

func TestRunDueAvailabilityChecksSendsNothingWhenGuardTimesOut(t *testing.T) {
	pool := setupTestDB(t)
	ctx := context.Background()
	svc := newShortLockTimeoutService(t)

	uid := insertCreator(t, pool)
	agentID := createDryRunReadyAgent(t, newTestService(t, pool), uid, freshSlug("monitor-guard-timeout"))

	// While the probe runs, an endpoint edit takes the Agent row and has not
	// committed when the monitor tries to store its result.
	var editor pgx.Tx
	runner := newScriptedDryRunner("endpoint 调用失败: connection refused")
	runner.hook = func(hookCtx context.Context, id uuid.UUID) {
		var err error
		editor, err = pool.Begin(hookCtx)
		require.NoError(t, err)
		_, err = editor.Exec(hookCtx, `SELECT 1 FROM agents WHERE id = $1 FOR UPDATE`, id)
		require.NoError(t, err)
	}
	svc.SetDryRunner(runner)

	resp, err := svc.RunDueAvailabilityChecks(ctx, 10, 0)
	require.NotNil(t, editor)
	require.NoError(t, editor.Rollback(ctx))
	require.NoError(t, err)
	assert.Equal(t, 1, runner.callsFor(agentID))
	assert.Equal(t, int32(0), resp.Checked, "an unvalidated result must not be counted")
	assert.Empty(t, resp.Alerts, "an unvalidated result must not alert")
	assertNoStoredAvailabilityResult(t, pool, agentID)
}

func TestRunDueAvailabilityChecksSendsNothingWhenOnboardingWriteFails(t *testing.T) {
	pool := setupTestDB(t)
	ctx := context.Background()
	svc := newShortLockTimeoutService(t)

	uid := insertCreator(t, pool)
	agentID := createDryRunReadyAgent(t, newTestService(t, pool), uid, freshSlug("monitor-onboarding-blocked"))

	// While the probe runs, another transaction holds the onboarding row, so
	// the monitor's dry-run result write times out inside the guarded write.
	var blocker pgx.Tx
	runner := newScriptedDryRunner("endpoint 调用失败: connection refused")
	runner.hook = func(hookCtx context.Context, id uuid.UUID) {
		var err error
		blocker, err = pool.Begin(hookCtx)
		require.NoError(t, err)
		_, err = blocker.Exec(hookCtx, `SELECT 1 FROM agent_onboarding_status WHERE agent_id = $1 FOR UPDATE`, id)
		require.NoError(t, err)
	}
	svc.SetDryRunner(runner)

	resp, err := svc.RunDueAvailabilityChecks(ctx, 10, 0)
	require.NotNil(t, blocker)
	require.NoError(t, blocker.Rollback(ctx))
	require.NoError(t, err)
	assert.Equal(t, 1, runner.callsFor(agentID))
	assert.Equal(t, int32(0), resp.Checked, "a partially stored result must not be counted")
	assert.Empty(t, resp.Alerts, "a partially stored result must not alert")
	assertNoStoredAvailabilityResult(t, pool, agentID)
}

func TestRunDueAvailabilityChecksDiscardsResultWhenCheckLockIsLost(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreator(t, pool)
	agentID := createDryRunReadyAgent(t, svc, uid, freshSlug("monitor-lock-lost"))

	runner := newScriptedDryRunner("")
	runner.hook = func(hookCtx context.Context, id uuid.UUID) {
		// Another replica could take the lock now, so this probe must not be stored.
		terminateAvailabilityLockHolder(t, hookCtx, pool, id)
	}
	svc.SetDryRunner(runner)

	resp, err := svc.RunDueAvailabilityChecks(ctx, 10, 0)
	require.NoError(t, err)
	assert.Equal(t, 1, runner.callsFor(agentID))
	assert.Equal(t, int32(0), resp.Checked, "a probe whose check lock was lost must be discarded")
	assert.Empty(t, resp.Alerts)
	assertNoStoredAvailabilityResult(t, pool, agentID)

	// The next batch opens a fresh lock session and stores its result.
	runner.hook = nil
	resp, err = svc.RunDueAvailabilityChecks(ctx, 10, 0)
	require.NoError(t, err)
	assert.Equal(t, int32(1), resp.Checked)
	assert.Equal(t, int32(1), resp.Passed)
}

func TestRunDueAvailabilityChecksGuardsPreflightFailures(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreator(t, pool)
	probed := createDryRunReadyAgent(t, svc, uid, freshSlug("preflight-probed"))
	edited := createDryRunReadyAgent(t, svc, uid, freshSlug("preflight-edited"))
	mismatched := createDryRunReadyAgent(t, svc, uid, freshSlug("preflight-mismatch"))

	// An unchanged Agent whose example no longer matches its schema still gets
	// a stored failure and an alert.
	_, err := pool.Exec(ctx, `
UPDATE agent_capabilities
SET input_schema = '{"type":"object","properties":{"other":{"type":"string"}},"required":["other"]}'::jsonb
WHERE agent_id = $1`, mismatched)
	require.NoError(t, err)

	// While the first Agent is probed, the second one is edited and loses its
	// examples, so its later preflight fails against a row the batch did not list.
	runner := newScriptedDryRunner("")
	runner.hook = func(hookCtx context.Context, id uuid.UUID) {
		if id != probed {
			return
		}
		_, err := pool.Exec(hookCtx, `DELETE FROM agent_examples WHERE agent_id = $1`, edited)
		require.NoError(t, err)
		_, err = pool.Exec(hookCtx, `UPDATE agents SET description = 'edited mid batch' WHERE id = $1`, edited)
		require.NoError(t, err)
	}
	svc.SetDryRunner(runner)

	resp, err := svc.RunDueAvailabilityChecks(ctx, 10, 0)
	require.NoError(t, err)
	assert.Equal(t, int32(2), resp.Checked)
	assert.Equal(t, int32(1), resp.Passed)
	assert.Equal(t, int32(1), resp.Failed)
	require.Len(t, resp.Alerts, 1)
	assert.Equal(t, mismatched.String(), resp.Alerts[0].AgentID)

	var snapshots, alerts int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM agent_availability_snapshots WHERE agent_id = $1`, edited).Scan(&snapshots))
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM agent_availability_alerts WHERE agent_id = $1`, edited).Scan(&alerts))
	assert.Zero(t, snapshots, "a superseded preflight failure must not be stored")
	assert.Zero(t, alerts, "a superseded preflight failure must not alert")

	var status string
	var failures int32
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT availability_status, consecutive_failures FROM agent_availability_snapshots WHERE agent_id = $1`,
		mismatched).Scan(&status, &failures))
	assert.Equal(t, "degraded", status)
	assert.Equal(t, int32(1), failures)
}

// ────────────────────────────────────────────────────────────
// ListPendingForAdmin
// ────────────────────────────────────────────────────────────

func TestListPendingForAdmin(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	uid := insertCreator(t, pool)

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
