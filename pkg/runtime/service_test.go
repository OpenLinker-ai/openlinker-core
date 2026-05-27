// Package runtime_test - Service 层集成测试。
//
// 需要 TEST_DATABASE_URL 和 mock HTTP endpoint（见 mock_endpoint_test.go）。
// 未设置 TEST_DATABASE_URL 则全部 t.Skip()。
//
// 期望 API（来自任务规格 + docs/13 + internal/runtime/dto.go）：
//
//	type Service struct{ ... }
//	func NewService(pool *pgxpool.Pool, cfg *config.Config) *Service
//	func (s *Service) Run(ctx context.Context, userID uuid.UUID, req *RunRequest) (*RunResponse, error)
//	func (s *Service) GetRun(ctx context.Context, userID, runID uuid.UUID) (*RunResponse, error)
//
//	type RunRequest struct {
//	    AgentID  string                 `json:"agent_id" validate:"required,uuid"`
//	    Input    map[string]interface{} `json:"input" validate:"required"`
//	    Metadata map[string]interface{} `json:"metadata,omitempty"`
//	}
//
//	type RunResponse struct {
//	    RunID      string                 `json:"run_id"`
//	    Status     string                 `json:"status"`        // success / failed / timeout
//	    Output     map[string]interface{} `json:"output,omitempty"`
//	    ErrorCode  string                 `json:"error_code,omitempty"`
//	    ErrorMsg   string                 `json:"error_message,omitempty"`
//	    CostCents  int32                  `json:"cost_cents"`
//	    DurationMs int32                  `json:"duration_ms"`
//	}
//
// 错误：service 层返回 *httpx.HTTPError，
//   - agent 不存在 -> 404
//   - agent 已下架或 private -> 403；认证状态不阻塞公开调用
//   - 当前免费阶段不要求余额
//
// 平台抽成：cfg.PlatformFeeRate=0.25 → 平台 25%, creator 75%, floor 取整。
//
// 内部 endpoint 默认通过 https；本文件用 httptest.NewTLSServer，并通过
// export_for_test.go 注入 server.Client() 信任自签证书。

package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kinzhi/openlinker-core/pkg/config"
	db "github.com/kinzhi/openlinker-core/pkg/db/generated"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
	"github.com/kinzhi/openlinker-core/pkg/runtime"
)

const truncateAll = "TRUNCATE run_requirement_evidence, run_artifact_chunks, run_artifacts, run_messages, run_events, run_webhook_deliveries, run_webhook_subscriptions, wallets, runs, charges, withdrawals, task_queries, agent_skills, agents, users RESTART IDENTITY CASCADE"

// ────────────────────────────────────────────────────────────
// DB / Service setup
// ────────────────────────────────────────────────────────────

// setupTestDB 拿到 pool 并清理表。无 TEST_DATABASE_URL 则 skip。
func setupTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL 未设置，跳过 runtime 集成测试")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	require.NoError(t, pool.Ping(ctx))
	_, err = pool.Exec(ctx, truncateAll)
	require.NoError(t, err)
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = pool.Exec(c, truncateAll)
		pool.Close()
	})
	return pool
}

// newTestConfig 构造 runtime 测试用的 *config.Config。
//
// 关键字段：
//   - PlatformFeeRate=0.25：平台抽 25%
//   - RunTimeoutSeconds=2：测试用短 timeout，避免单测跑太久
func newTestConfig() *config.Config {
	return &config.Config{
		PlatformFeeRate:   0.25,
		RunTimeoutSeconds: 2,
	}
}

// newTestService 构造 Service。
func newTestService(t *testing.T, pool *pgxpool.Pool) *runtime.Service {
	t.Helper()
	return runtime.NewService(pool, newTestConfig())
}

// ────────────────────────────────────────────────────────────
// 数据工厂：user / creator / agent / wallet
// ────────────────────────────────────────────────────────────

// insertUserWithBalance 创建一个普通 user + wallet，余额 = balanceCents。
func insertUserWithBalance(t *testing.T, pool *pgxpool.Pool, balanceCents int64) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	uid := uuid.New()
	email := "rt-u-" + uid.String()[:8] + "@example.com"
	_, err := pool.Exec(ctx,
		`INSERT INTO users (id, email, password_hash, display_name) VALUES ($1, $2, $3, $4)`,
		uid, email, "x", "Runtime User")
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`INSERT INTO wallets (user_id, balance_cents, total_charged_cents) VALUES ($1, $2, $2)`,
		uid, balanceCents)
	require.NoError(t, err)
	return uid
}

// insertCreator 创建 creator（is_creator=TRUE）+ wallet。
func insertCreator(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	uid := uuid.New()
	email := "rt-c-" + uid.String()[:8] + "@example.com"
	_, err := pool.Exec(ctx,
		`INSERT INTO users (id, email, password_hash, display_name, is_creator, creator_verified)
		 VALUES ($1, $2, $3, $4, TRUE, TRUE)`,
		uid, email, "x", "Runtime Creator")
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO wallets (user_id) VALUES ($1)`, uid)
	require.NoError(t, err)
	return uid
}

// insertAgent 直接 SQL 插一个 agent，把旧 status 文案翻译成新三维字段。
//
//	approved → lifecycle=active, visibility=public, cert=unreviewed
//	disabled → lifecycle=disabled
//	pending  → lifecycle=active, visibility=public, cert=pending
//	rejected → lifecycle=active, visibility=public, cert=rejected
func insertAgent(t *testing.T, pool *pgxpool.Pool, creatorID uuid.UUID, endpoint string, priceCents int32, status string) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	id := uuid.New()
	slug := "rt-a-" + id.String()[:8]
	lifecycle := "active"
	cert := "unreviewed"
	switch status {
	case "approved":
		// defaults
	case "disabled":
		lifecycle = "disabled"
	case "pending":
		cert = "pending"
	case "rejected":
		cert = "rejected"
	default:
		require.Failf(t, "insertAgent unknown legacy status", "%q", status)
	}
	_, err := pool.Exec(ctx,
		`INSERT INTO agents (
			id, creator_id, slug, name, description, endpoint_url, endpoint_auth_header,
			price_per_call_cents, tags, lifecycle_status, visibility, certification_status
		) VALUES ($1, $2, $3, $4, $5, $6, NULL, $7, '{}', $8, 'public', $9)`,
		id, creatorID, slug, "Runtime Agent", "test agent", endpoint, priceCents, lifecycle, cert)
	require.NoError(t, err, "insert agent")
	return id
}

func setAgentEndpointToken(t *testing.T, pool *pgxpool.Pool, agentID uuid.UUID, token string) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`UPDATE agents SET endpoint_auth_header=$2 WHERE id=$1`,
		agentID, token)
	require.NoError(t, err)
}

func insertRunningRun(t *testing.T, pool *pgxpool.Pool, userID, agentID uuid.UUID) uuid.UUID {
	t.Helper()
	runID := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO runs (
			id, user_id, agent_id, input, status,
			cost_cents, platform_fee_cents, creator_revenue_cents
		) VALUES ($1, $2, $3, '{}'::jsonb, 'running', 10, 2, 8)`,
		runID, userID, agentID)
	require.NoError(t, err)
	return runID
}

// readWallet 读 wallet 主要字段。
type walletRow struct {
	BalanceCents      int64
	EarningsCents     int64
	TotalChargedCents int64
	TotalSpentCents   int64
	TotalEarnedCents  int64
}

func readWallet(t *testing.T, pool *pgxpool.Pool, userID uuid.UUID) walletRow {
	t.Helper()
	var w walletRow
	err := pool.QueryRow(context.Background(),
		`SELECT balance_cents, earnings_cents, total_charged_cents, total_spent_cents, total_earned_cents
		 FROM wallets WHERE user_id=$1`, userID).
		Scan(&w.BalanceCents, &w.EarningsCents, &w.TotalChargedCents, &w.TotalSpentCents, &w.TotalEarnedCents)
	require.NoError(t, err)
	return w
}

type runRow struct {
	Status              string
	CostCents           int32
	PlatformFeeCents    int32
	CreatorRevenueCents int32
	ErrorCode           *string
}

type agentAvailabilityRow struct {
	Status              string
	ConsecutiveFailures int32
	LastSuccessfulRunAt *time.Time
	LastFailedRunAt     *time.Time
	LastCheckedAt       *time.Time
}

func readAgentAvailability(t *testing.T, pool *pgxpool.Pool, agentID uuid.UUID) agentAvailabilityRow {
	t.Helper()
	var a agentAvailabilityRow
	err := pool.QueryRow(context.Background(),
		`SELECT availability_status, consecutive_failures,
		        last_successful_run_at, last_failed_run_at, last_checked_at
		 FROM agent_availability_snapshots WHERE agent_id=$1`, agentID).
		Scan(&a.Status, &a.ConsecutiveFailures, &a.LastSuccessfulRunAt, &a.LastFailedRunAt, &a.LastCheckedAt)
	require.NoError(t, err)
	return a
}

// readRun 读一条 runs 记录的主要字段。
func readRun(t *testing.T, pool *pgxpool.Pool, runID uuid.UUID) runRow {
	t.Helper()
	var r runRow
	err := pool.QueryRow(context.Background(),
		`SELECT status, cost_cents, platform_fee_cents, creator_revenue_cents, error_code
		 FROM runs WHERE id=$1`, runID).
		Scan(&r.Status, &r.CostCents, &r.PlatformFeeCents, &r.CreatorRevenueCents, &r.ErrorCode)
	require.NoError(t, err)
	return r
}

type runEventRow struct {
	Sequence  int32
	EventType string
	Payload   map[string]any
}

// readRunEvents 读 run_events 的事件类型和 payload，按 sequence 正序。
func readRunEvents(t *testing.T, pool *pgxpool.Pool, runID uuid.UUID) []runEventRow {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT sequence, event_type, payload FROM run_events WHERE run_id=$1 ORDER BY sequence ASC`,
		runID)
	require.NoError(t, err)
	defer rows.Close()

	var events []runEventRow
	for rows.Next() {
		var event runEventRow
		var raw []byte
		require.NoError(t, rows.Scan(&event.Sequence, &event.EventType, &raw))
		require.NoError(t, json.Unmarshal(raw, &event.Payload))
		events = append(events, event)
	}
	require.NoError(t, rows.Err())
	return events
}

// countRunsForUser 数 user 的 run 总数。
func countRunsForUser(t *testing.T, pool *pgxpool.Pool, userID uuid.UUID) int {
	t.Helper()
	var n int
	err := pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM runs WHERE user_id=$1`, userID).Scan(&n)
	require.NoError(t, err)
	return n
}

// readAgentStats 读 agents.total_calls / total_revenue_cents。
func readAgentStats(t *testing.T, pool *pgxpool.Pool, agentID uuid.UUID) (totalCalls int32, totalRevenue int64) {
	t.Helper()
	err := pool.QueryRow(context.Background(),
		`SELECT total_calls, total_revenue_cents FROM agents WHERE id=$1`, agentID).
		Scan(&totalCalls, &totalRevenue)
	require.NoError(t, err)
	return
}

// assertHTTPStatus 把 error 当 *httpx.HTTPError 取 status 断言。
func assertHTTPStatus(t *testing.T, err error, want int) {
	t.Helper()
	require.Error(t, err)
	var he *httpx.HTTPError
	require.True(t, errors.As(err, &he), "expected *httpx.HTTPError, got %T (%v)", err, err)
	assert.Equal(t, want, he.Status, "code=%s msg=%s", he.Code, he.Message)
}

// assertNoLostMoney 简化对账：所有 wallet 余额非负，runs 表 fee+revenue=cost。
//
// Phase 1 保守版：
//   - 所有 wallet.balance_cents >= 0
//   - 所有 wallet.earnings_cents >= 0
//   - runs.platform_fee + runs.creator_revenue = runs.cost (DB CHECK 已保证，再次断言以防意外绕过)
func assertNoLostMoney(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()

	var negBalances, negEarnings int
	err := pool.QueryRow(ctx,
		`SELECT
		   COUNT(*) FILTER (WHERE balance_cents < 0),
		   COUNT(*) FILTER (WHERE earnings_cents < 0)
		 FROM wallets`).Scan(&negBalances, &negEarnings)
	require.NoError(t, err)
	assert.Equal(t, 0, negBalances, "wallet balance must never go negative")
	assert.Equal(t, 0, negEarnings, "wallet earnings must never go negative")

	var inconsistent int
	err = pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM runs
		 WHERE cost_cents <> platform_fee_cents + creator_revenue_cents`).Scan(&inconsistent)
	require.NoError(t, err)
	assert.Equal(t, 0, inconsistent, "runs.cost must equal platform_fee + creator_revenue")
}

// startMockEndpointForService 启动 https mock 并把 server.Client() 注入 svc，
// 以便服务跳过自签 CA 验证。返回 mock server URL。
func startMockEndpointForService(t *testing.T, svc *runtime.Service, handler http.HandlerFunc) string {
	t.Helper()
	server := startMockEndpoint(t, handler)
	// server.Client() 自动信任 server 的自签证书，复用它作为 service 的 http.Client。
	// 保留 cfg 的 timeout（server.Client() 默认无 timeout，会让 timeout 测试不可靠）。
	c := server.Client()
	c.Timeout = time.Duration(newTestConfig().RunTimeoutSeconds) * time.Second
	svc.SetHTTPClient(c)
	return server.URL
}

// makeRunReq 便捷构造 *runtime.RunRequest。
func makeRunReq(agentID uuid.UUID, input map[string]interface{}) *runtime.RunRequest {
	if input == nil {
		input = map[string]interface{}{}
	}
	return &runtime.RunRequest{
		AgentID: agentID.String(),
		Input:   input,
	}
}

// mustParseUUID 测试快捷：把 RunResponse.RunID（string）解析回 uuid.UUID。
func mustParseUUID(t *testing.T, s string) uuid.UUID {
	t.Helper()
	id, err := uuid.Parse(s)
	require.NoError(t, err, "parse uuid %q", s)
	return id
}

// ────────────────────────────────────────────────────────────
// Run - HappyPath
// ────────────────────────────────────────────────────────────

func TestRun_HappyPath(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000) // $10
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"text":"ok"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved") // $0.10

	resp, err := svc.Run(ctx, userID, makeRunReq(agentID, map[string]any{"q": "hello"}), "")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "success", resp.Status)
	assert.Equal(t, int32(0), resp.CostCents)

	user := readWallet(t, pool, userID)
	creator := readWallet(t, pool, creatorID)
	assert.Equal(t, int64(1000), user.BalanceCents, "Phase 1 does not deduct balance")
	assert.Equal(t, int64(0), user.TotalSpentCents)
	assert.Equal(t, int64(0), creator.EarningsCents)
	assert.Equal(t, int64(0), creator.TotalEarnedCents)

	assert.Equal(t, 1, countRunsForUser(t, pool, userID))
	totalCalls, totalRevenue := readAgentStats(t, pool, agentID)
	assert.Equal(t, int32(1), totalCalls)
	assert.Equal(t, int64(0), totalRevenue)
	availability := readAgentAvailability(t, pool, agentID)
	assert.Equal(t, "healthy", availability.Status)
	assert.Equal(t, int32(0), availability.ConsecutiveFailures)
	require.NotNil(t, availability.LastSuccessfulRunAt)

	run := readRun(t, pool, mustParseUUID(t, resp.RunID))
	assert.Equal(t, "success", run.Status)
	assert.Equal(t, int32(0), run.CostCents)
	assert.Equal(t, int32(0), run.PlatformFeeCents)
	assert.Equal(t, int32(0), run.CreatorRevenueCents)

	events := readRunEvents(t, pool, mustParseUUID(t, resp.RunID))
	require.Len(t, events, 3)
	assert.Equal(t, int32(1), events[0].Sequence)
	assert.Equal(t, "run.created", events[0].EventType)
	assert.Equal(t, "running", events[0].Payload["status"])
	assert.Equal(t, "run.started", events[1].EventType)
	assert.Equal(t, int32(3), events[2].Sequence)
	assert.Equal(t, "run.completed", events[2].EventType)
	assert.Equal(t, "success", events[2].Payload["status"])

	artifacts, err := svc.ListRunArtifacts(ctx, userID, mustParseUUID(t, resp.RunID))
	require.NoError(t, err)
	require.Len(t, artifacts, 1)
	assert.Equal(t, "Agent 输出", artifacts[0].Title)
	assert.Equal(t, "json", artifacts[0].ArtifactType)
	assert.Equal(t, "private", artifacts[0].Visibility)
	assert.Equal(t, "ok", artifacts[0].Content["text"])

	assertNoLostMoney(t, pool)
}

func TestRun_WithTaskRequirementEvidence(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"summary":"done"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")
	_, err := pool.Exec(ctx,
		`INSERT INTO agent_skills (agent_id, skill_id) VALUES ($1, 'data/sql-query')`,
		agentID)
	require.NoError(t, err)

	var taskID uuid.UUID
	err = pool.QueryRow(ctx,
		`INSERT INTO task_queries (user_id, query, parsed_skills, mcp_tools, recommended_agent_ids)
		 VALUES ($1, '做 SQL 查询和数据分析', ARRAY['data/sql-query','data/analysis']::text[], ARRAY['run_agent']::text[], ARRAY[$2]::uuid[])
		 RETURNING id`,
		userID, agentID).Scan(&taskID)
	require.NoError(t, err)

	req := makeRunReq(agentID, map[string]any{"q": "SELECT count(*) FROM orders"})
	req.Metadata = map[string]any{"task_id": taskID.String()}
	resp, err := svc.Run(ctx, userID, req, "mcp")
	require.NoError(t, err)
	require.Equal(t, "success", resp.Status)
	require.NotNil(t, resp.RequirementEvidence)
	assert.Equal(t, taskID.String(), resp.RequirementEvidence.TaskID)
	assert.Equal(t, "partial", resp.RequirementEvidence.CoverageStatus)
	assert.Equal(t, []string{"data/sql-query"}, resp.RequirementEvidence.MatchedSkillIDs)
	assert.Equal(t, []string{"data/analysis"}, resp.RequirementEvidence.MissingSkillIDs)
	assert.Equal(t, []string{"run_agent"}, resp.RequirementEvidence.UsedMCPTools)
	assert.Empty(t, resp.RequirementEvidence.MissingMCPTools)

	got, err := svc.GetRun(ctx, userID, mustParseUUID(t, resp.RunID))
	require.NoError(t, err)
	require.NotNil(t, got.RequirementEvidence)
	assert.Equal(t, resp.RequirementEvidence.CoverageStatus, got.RequirementEvidence.CoverageStatus)

	events := readRunEvents(t, pool, mustParseUUID(t, resp.RunID))
	require.Len(t, events, 4)
	assert.Equal(t, "run.requirements.snapshotted", events[2].EventType)
	assert.Equal(t, taskID.String(), events[2].Payload["task_id"])
	assert.Equal(t, "partial", events[2].Payload["coverage_status"])
	assert.Equal(t, "run.completed", events[3].EventType)
}

func TestRun_PersistsDeclaredArtifacts(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{
		"output":{
			"summary":"ok",
			"artifacts":[
				{"title":"结论摘要","artifact_type":"json","content":{"summary":"ok"},"visibility":"shared"},
				{"title":"原始数据","type":"data","data":{"rows":3}}
			]
		}
	}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")

	resp, err := svc.Run(ctx, userID, makeRunReq(agentID, map[string]any{"q": "artifact"}), "")
	require.NoError(t, err)
	require.Equal(t, "success", resp.Status)

	artifacts, err := svc.ListRunArtifacts(ctx, userID, mustParseUUID(t, resp.RunID))
	require.NoError(t, err)
	require.Len(t, artifacts, 2)
	assert.Equal(t, "结论摘要", artifacts[0].Title)
	assert.Equal(t, "shared", artifacts[0].Visibility)
	assert.Equal(t, "ok", artifacts[0].Content["summary"])
	assert.Equal(t, "原始数据", artifacts[1].Title)
	assert.Equal(t, "data", artifacts[1].ArtifactType)
	assert.Equal(t, float64(3), artifacts[1].Content["rows"])
}

func TestRun_NextActionFromAgentOutput(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{
		"output":{
			"text":"ok",
			"next_action":"请确认摘要，并把结果投递到默认 webhook。"
		}
	}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")

	resp, err := svc.Run(ctx, userID, makeRunReq(agentID, map[string]any{"q": "hello"}), "")
	require.NoError(t, err)
	require.NotNil(t, resp.NextAction)
	assert.Equal(t, "agent_suggested", resp.NextAction.Type)
	assert.Equal(t, "执行 Agent 建议", resp.NextAction.Label)
	assert.Equal(t, "请确认摘要，并把结果投递到默认 webhook。", resp.NextAction.Hint)
	assert.Equal(t, "agent", resp.NextAction.Source)

	got, err := svc.GetRun(ctx, userID, mustParseUUID(t, resp.RunID))
	require.NoError(t, err)
	require.NotNil(t, got.NextAction)
	assert.Equal(t, resp.NextAction.Hint, got.NextAction.Hint)
}

func TestRun_RecordsAgentReturnedEventsBeforeCompleted(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{
		"events": [
			{"event_type":"run.message.delta","payload":{"text":"draft ready"}},
			{"event_type":"run.artifact.delta","payload":{"artifact_id":"art_1","title":"Draft artifact","artifact_type":"text","append":true,"last_chunk":true,"parts":[{"type":"text","text":"chunk one"}]}}
		],
		"output":{"text":"ok"}
	}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")

	resp, err := svc.Run(ctx, userID, makeRunReq(agentID, map[string]any{"q": "hello"}), "")
	require.NoError(t, err)
	require.Equal(t, "success", resp.Status)

	events := readRunEvents(t, pool, mustParseUUID(t, resp.RunID))
	require.Len(t, events, 5)
	assert.Equal(t, "run.created", events[0].EventType)
	assert.Equal(t, "run.started", events[1].EventType)
	assert.Equal(t, "run.message.delta", events[2].EventType)
	assert.Equal(t, "draft ready", events[2].Payload["text"])
	assert.Equal(t, "run.artifact.delta", events[3].EventType)
	assert.Equal(t, "art_1", events[3].Payload["artifact_id"])
	assert.Equal(t, "run.completed", events[4].EventType)

	artifacts, err := svc.ListRunArtifacts(ctx, userID, mustParseUUID(t, resp.RunID))
	require.NoError(t, err)
	require.Len(t, artifacts, 2)
	var streamed *runtime.RunArtifactResponse
	for i := range artifacts {
		if artifacts[i].SourceArtifactID == "art_1" {
			streamed = &artifacts[i]
			break
		}
	}
	require.NotNil(t, streamed)
	assert.Equal(t, "text", streamed.ArtifactType)
	assert.Equal(t, "Draft artifact", streamed.Title)
	assert.Equal(t, "chunk one", streamed.Content["text"])
	assert.Equal(t, true, streamed.Content["complete"])

	chunks, err := db.New(pool).ListRunArtifactChunksByRun(ctx, mustParseUUID(t, resp.RunID))
	require.NoError(t, err)
	require.Len(t, chunks, 1)
	assert.Equal(t, "art_1", chunks[0].SourceArtifactID)
	assert.Equal(t, int32(0), chunks[0].ChunkIndex)
	require.NotNil(t, chunks[0].EventSequence)
	assert.Equal(t, int32(4), *chunks[0].EventSequence)

	messages, err := svc.ListRunMessages(ctx, userID, mustParseUUID(t, resp.RunID))
	require.NoError(t, err)
	require.Len(t, messages, 2)
	assert.Equal(t, "user", messages[0].Role)
	assert.Contains(t, messages[0].Content, "hello")
	assert.Equal(t, "agent", messages[1].Role)
	assert.Equal(t, "draft ready", messages[1].Content)

	assertNoLostMoney(t, pool)
}

func TestStartRun_ReturnsRunningAndCompletesInBackground(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	called := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseEndpoint := func() {
		releaseOnce.Do(func() { close(release) })
	}
	defer releaseEndpoint()

	userID := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, func(w http.ResponseWriter, r *http.Request) {
		select {
		case called <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"output":{"text":"async ok"}}`))
	})
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")

	resp, err := svc.StartRun(ctx, userID, makeRunReq(agentID, map[string]any{"q": "hello"}), "")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "running", resp.Status)
	assert.NotEmpty(t, resp.RunID)
	assert.Equal(t, int32(0), resp.CostCents)

	runID := mustParseUUID(t, resp.RunID)
	running, err := svc.GetRun(ctx, userID, runID)
	require.NoError(t, err)
	assert.Equal(t, "running", running.Status)

	events := readRunEvents(t, pool, runID)
	require.Len(t, events, 2)
	assert.Equal(t, "run.created", events[0].EventType)
	assert.Equal(t, "run.started", events[1].EventType)

	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("async endpoint was not called")
	}
	releaseEndpoint()

	require.Eventually(t, func() bool {
		got, getErr := svc.GetRun(ctx, userID, runID)
		return getErr == nil && got.Status == "success"
	}, 3*time.Second, 20*time.Millisecond)

	completed, err := svc.GetRun(ctx, userID, runID)
	require.NoError(t, err)
	assert.Equal(t, "success", completed.Status)
	assert.Equal(t, "async ok", completed.Output["text"])

	events = readRunEvents(t, pool, runID)
	require.Len(t, events, 3)
	assert.Equal(t, "run.completed", events[2].EventType)

	assertNoLostMoney(t, pool)
}

// ────────────────────────────────────────────────────────────
// Run - 错误路径
// ────────────────────────────────────────────────────────────

func TestRun_FreePhaseAllowsZeroBalance(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1) // $0.01
	creatorID := insertCreator(t, pool)
	called, getCount := mockEndpointCounting(mockEndpointReturning(http.StatusOK, `{"output":{"text":"ok"}}`))
	endpoint := startMockEndpointForService(t, svc, called)
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")

	resp, err := svc.Run(ctx, userID, makeRunReq(agentID, nil), "")
	require.NoError(t, err)
	assert.Equal(t, "success", resp.Status)

	assert.Equal(t, 1, countRunsForUser(t, pool, userID))
	w := readWallet(t, pool, userID)
	assert.Equal(t, int64(1), w.BalanceCents)
	assert.Equal(t, int64(1), getCount(), "free run must call endpoint without balance")

	assertNoLostMoney(t, pool)
}

func TestRun_AgentNotFound(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000)
	_, err := svc.Run(ctx, userID, makeRunReq(uuid.New(), nil), "")
	assertHTTPStatus(t, err, http.StatusNotFound)
}

func TestRun_CertificationPendingStillCallable(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"text":"ok"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "pending")

	resp, err := svc.Run(ctx, userID, makeRunReq(agentID, nil), "")
	require.NoError(t, err)
	assert.Equal(t, "success", resp.Status)
}

// ────────────────────────────────────────────────────────────
// Run - endpoint 失败 → 退款
// ────────────────────────────────────────────────────────────

func TestRun_EndpointReturns500(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusInternalServerError, `{"err":"boom"}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")

	resp, err := svc.Run(ctx, userID, makeRunReq(agentID, nil), "")
	require.NoError(t, err, "endpoint failure 应该是逻辑失败而不是 HTTP error")
	require.NotNil(t, resp)
	assert.Equal(t, "failed", resp.Status)

	// 退款 → user 回到 1000，creator 收益 = 0
	user := readWallet(t, pool, userID)
	creator := readWallet(t, pool, creatorID)
	assert.Equal(t, int64(1000), user.BalanceCents, "user balance 必须退款")
	assert.Equal(t, int64(0), creator.EarningsCents)

	run := readRun(t, pool, mustParseUUID(t, resp.RunID))
	assert.Equal(t, "failed", run.Status)
	require.NotNil(t, run.ErrorCode)
	assert.NotEmpty(t, *run.ErrorCode)
	availability := readAgentAvailability(t, pool, agentID)
	assert.Equal(t, "degraded", availability.Status)
	assert.Equal(t, int32(1), availability.ConsecutiveFailures)
	require.NotNil(t, availability.LastFailedRunAt)

	events := readRunEvents(t, pool, mustParseUUID(t, resp.RunID))
	require.Len(t, events, 3)
	assert.Equal(t, "run.failed", events[2].EventType)
	assert.Equal(t, "failed", events[2].Payload["status"])

	assertNoLostMoney(t, pool)
}

func TestRun_ConsecutiveFailuresMarkAgentUnreachable(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc,
		mockEndpointReturning(http.StatusInternalServerError, `{"error":{"code":"BOOM","message":"down"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")

	for i := 0; i < 3; i++ {
		resp, err := svc.Run(ctx, userID, makeRunReq(agentID, map[string]any{"try": i}), "")
		require.NoError(t, err)
		assert.Equal(t, "failed", resp.Status)
	}

	availability := readAgentAvailability(t, pool, agentID)
	assert.Equal(t, "unreachable", availability.Status)
	assert.Equal(t, int32(3), availability.ConsecutiveFailures)

	assertNoLostMoney(t, pool)
}

func TestRun_EndpointReturnsErrorJSON(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc,
		mockEndpointReturning(http.StatusOK, `{"error":{"code":"BAD_INPUT","message":"x"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")

	resp, err := svc.Run(ctx, userID, makeRunReq(agentID, nil), "")
	require.NoError(t, err)
	assert.Equal(t, "failed", resp.Status)

	user := readWallet(t, pool, userID)
	assert.Equal(t, int64(1000), user.BalanceCents, "退款")

	run := readRun(t, pool, mustParseUUID(t, resp.RunID))
	require.NotNil(t, run.ErrorCode)
	assert.Equal(t, "BAD_INPUT", *run.ErrorCode)

	assertNoLostMoney(t, pool)
}

func TestRun_EndpointTimeout(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	// cfg.RunTimeoutSeconds=2，让 endpoint sleep 5s 触发 timeout
	endpoint := startMockEndpointForService(t, svc, mockEndpointTimeout(5*time.Second))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")

	resp, err := svc.Run(ctx, userID, makeRunReq(agentID, nil), "")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "timeout", resp.Status)

	user := readWallet(t, pool, userID)
	assert.Equal(t, int64(1000), user.BalanceCents, "timeout 也要退款")

	run := readRun(t, pool, mustParseUUID(t, resp.RunID))
	require.NotNil(t, run.ErrorCode)
	assert.Equal(t, "TIMEOUT", *run.ErrorCode)

	assertNoLostMoney(t, pool)
}

func TestRun_EndpointReturnsInvalidJSON(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("not json"))
	})
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")

	resp, err := svc.Run(ctx, userID, makeRunReq(agentID, nil), "")
	require.NoError(t, err)
	assert.Equal(t, "failed", resp.Status)

	user := readWallet(t, pool, userID)
	assert.Equal(t, int64(1000), user.BalanceCents, "退款")

	run := readRun(t, pool, mustParseUUID(t, resp.RunID))
	require.NotNil(t, run.ErrorCode)
	assert.Equal(t, "INVALID_RESPONSE", *run.ErrorCode)

	assertNoLostMoney(t, pool)
}

// ────────────────────────────────────────────────────────────
// Run - 免费阶段财务字段
// ────────────────────────────────────────────────────────────

func TestRun_FreePhaseRecordsNoSettlement(t *testing.T) {
	cases := []struct {
		name  string
		price int32
	}{
		{"100c", 100},
		{"10c", 10},
		{"4c", 4},
		{"1c", 1},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			pool := setupTestDB(t)
			svc := newTestService(t, pool)
			ctx := context.Background()

			userID := insertUserWithBalance(t, pool, 100000)
			creatorID := insertCreator(t, pool)
			endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"text":"ok"}}`))
			agentID := insertAgent(t, pool, creatorID, endpoint, tc.price, "approved")

			resp, err := svc.Run(ctx, userID, makeRunReq(agentID, nil), "")
			require.NoError(t, err)
			require.Equal(t, "success", resp.Status)

			run := readRun(t, pool, mustParseUUID(t, resp.RunID))
			assert.Equal(t, int32(0), run.CostCents)
			assert.Equal(t, int32(0), run.PlatformFeeCents)
			assert.Equal(t, int32(0), run.CreatorRevenueCents)

			creator := readWallet(t, pool, creatorID)
			assert.Equal(t, int64(0), creator.EarningsCents)

			assertNoLostMoney(t, pool)
		})
	}
}

// ────────────────────────────────────────────────────────────
// Run - 并发安全
// ────────────────────────────────────────────────────────────

func TestRun_ConcurrentFreeCallsDoNotTouchBalance(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)

	userID := insertUserWithBalance(t, pool, 100) // $1
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"text":"ok"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 100, "approved") // $1 each

	const N = 10
	var (
		wg              sync.WaitGroup
		successes       int64
		paymentRequired int64
		otherErrs       int64
	)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := svc.Run(ctx, userID, makeRunReq(agentID, nil), "")
			if err != nil {
				var he *httpx.HTTPError
				if errors.As(err, &he) && he.Status == http.StatusPaymentRequired {
					atomic.AddInt64(&paymentRequired, 1)
					return
				}
				atomic.AddInt64(&otherErrs, 1)
				return
			}
			if resp.Status == "success" {
				atomic.AddInt64(&successes, 1)
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, int64(0), atomic.LoadInt64(&otherErrs), "no unexpected errors")
	assert.Equal(t, int64(N), atomic.LoadInt64(&successes), "all free runs should succeed")
	assert.Equal(t, int64(0), atomic.LoadInt64(&paymentRequired))

	user := readWallet(t, pool, userID)
	assert.GreaterOrEqual(t, user.BalanceCents, int64(0), "balance never negative")
	assert.Equal(t, int64(100), user.BalanceCents, "free runs leave balance untouched")

	assertNoLostMoney(t, pool)
}

// ────────────────────────────────────────────────────────────
// GetRun
// ────────────────────────────────────────────────────────────

func TestGetRun_HappyPath(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"text":"ok"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")

	created, err := svc.Run(ctx, userID, makeRunReq(agentID, map[string]any{"q": "hi"}), "")
	require.NoError(t, err)

	got, err := svc.GetRun(ctx, userID, mustParseUUID(t, created.RunID))
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, created.RunID, got.RunID)
	// AgentID 不在 RunResponse 中，跳过此断言（DTO 设计简化）
	assert.Equal(t, created.Status, got.Status)
	assert.Equal(t, created.CostCents, got.CostCents)
}

func TestListRunEvents_HappyPath(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"text":"ok"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")

	created, err := svc.Run(ctx, userID, makeRunReq(agentID, nil), "")
	require.NoError(t, err)
	runID := mustParseUUID(t, created.RunID)

	events, err := svc.ListRunEvents(ctx, userID, runID, 1, 10)
	require.NoError(t, err)
	require.Len(t, events, 2)
	assert.Equal(t, int32(2), events[0].Sequence)
	assert.Equal(t, "run.started", events[0].EventType)
	assert.Equal(t, "run.completed", events[1].EventType)
}

func TestReportRunEvent_HappyPath(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"text":"ok"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")
	setAgentEndpointToken(t, pool, agentID, "agent-secret")
	runID := insertRunningRun(t, pool, userID, agentID)

	event, err := svc.ReportRunEvent(ctx, runID, "agent-secret", &runtime.ReportRunEventRequest{
		EventType: "run.message.delta",
		Payload:   map[string]interface{}{"text": "working"},
	})
	require.NoError(t, err)
	require.NotNil(t, event)
	assert.Equal(t, int32(1), event.Sequence)
	assert.Equal(t, "run.message.delta", event.EventType)
	assert.Equal(t, "working", event.Payload["text"])

	events := readRunEvents(t, pool, runID)
	require.Len(t, events, 1)
	assert.Equal(t, "run.message.delta", events[0].EventType)
	assert.Equal(t, "working", events[0].Payload["text"])

	messages, err := svc.ListRunMessages(ctx, userID, runID)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	assert.Equal(t, "agent", messages[0].Role)
	assert.Equal(t, "working", messages[0].Content)
	require.NotNil(t, messages[0].EventSequence)
	assert.Equal(t, int32(1), *messages[0].EventSequence)
}

func TestReportRunEvent_PersistsArtifactDelta(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"text":"ok"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")
	setAgentEndpointToken(t, pool, agentID, "agent-secret")
	runID := insertRunningRun(t, pool, userID, agentID)

	event, err := svc.ReportRunEvent(ctx, runID, "agent-secret", &runtime.ReportRunEventRequest{
		EventType: "run.artifact.delta",
		Payload: map[string]interface{}{
			"artifact_id":   "report",
			"title":         "Report Stream",
			"artifact_type": "text",
			"append":        true,
			"last_chunk":    false,
			"parts": []interface{}{
				map[string]interface{}{"type": "text", "text": "hello "},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, event)
	assert.Equal(t, int32(1), event.Sequence)

	_, err = svc.ReportRunEvent(ctx, runID, "agent-secret", &runtime.ReportRunEventRequest{
		EventType: "run.artifact.delta",
		Payload: map[string]interface{}{
			"artifact_id":   "report",
			"title":         "Report Stream",
			"artifact_type": "text",
			"append":        true,
			"last_chunk":    true,
			"parts": []interface{}{
				map[string]interface{}{"type": "text", "text": "world"},
			},
		},
	})
	require.NoError(t, err)

	artifacts, err := svc.ListRunArtifacts(ctx, userID, runID)
	require.NoError(t, err)
	require.Len(t, artifacts, 1)
	assert.Equal(t, "report", artifacts[0].SourceArtifactID)
	assert.Equal(t, "Report Stream", artifacts[0].Title)
	assert.Equal(t, "text", artifacts[0].ArtifactType)
	assert.Equal(t, "hello world", artifacts[0].Content["text"])
	assert.Equal(t, true, artifacts[0].Content["complete"])
	assert.Equal(t, float64(1), artifacts[0].Content["last_chunk_index"])

	chunks, err := db.New(pool).ListRunArtifactChunksByRun(ctx, runID)
	require.NoError(t, err)
	require.Len(t, chunks, 2)
	assert.Equal(t, int32(0), chunks[0].ChunkIndex)
	assert.Equal(t, int32(1), chunks[1].ChunkIndex)
	require.NotNil(t, chunks[1].EventSequence)
	assert.Equal(t, int32(2), *chunks[1].EventSequence)
	assert.True(t, chunks[1].LastChunk)
}

func TestReportRunEvent_RejectsInvalidToken(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"text":"ok"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")
	setAgentEndpointToken(t, pool, agentID, "agent-secret")
	runID := insertRunningRun(t, pool, userID, agentID)

	_, err := svc.ReportRunEvent(ctx, runID, "wrong-secret", &runtime.ReportRunEventRequest{
		EventType: "run.message.delta",
		Payload:   map[string]interface{}{"text": "working"},
	})
	assertHTTPStatus(t, err, http.StatusUnauthorized)
	assert.Empty(t, readRunEvents(t, pool, runID))
}

func TestReportRunEvent_RejectsFinishedRun(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"text":"ok"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")
	setAgentEndpointToken(t, pool, agentID, "agent-secret")
	runID := insertRunningRun(t, pool, userID, agentID)
	_, err := pool.Exec(ctx, `UPDATE runs SET status='success', finished_at=NOW() WHERE id=$1`, runID)
	require.NoError(t, err)

	_, err = svc.ReportRunEvent(ctx, runID, "agent-secret", &runtime.ReportRunEventRequest{
		EventType: "run.message.delta",
		Payload:   map[string]interface{}{"text": "too late"},
	})
	assertHTTPStatus(t, err, http.StatusConflict)
	assert.Empty(t, readRunEvents(t, pool, runID))
}

func TestReportRunEvent_RejectsUnsupportedEventType(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"text":"ok"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")
	setAgentEndpointToken(t, pool, agentID, "agent-secret")
	runID := insertRunningRun(t, pool, userID, agentID)

	_, err := svc.ReportRunEvent(ctx, runID, "agent-secret", &runtime.ReportRunEventRequest{
		EventType: "run.debug.raw",
		Payload:   map[string]interface{}{"text": "debug"},
	})
	assertHTTPStatus(t, err, http.StatusUnprocessableEntity)
	assert.Empty(t, readRunEvents(t, pool, runID))
}

func TestGetRun_NotOwner(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userA := insertUserWithBalance(t, pool, 1000)
	userB := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"text":"ok"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")

	created, err := svc.Run(ctx, userA, makeRunReq(agentID, nil), "")
	require.NoError(t, err)

	// userB 不是 owner，必须 404（不暴露存在性）
	_, err = svc.GetRun(ctx, userB, mustParseUUID(t, created.RunID))
	assertHTTPStatus(t, err, http.StatusNotFound)
}

func TestListRunEvents_NotOwner(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userA := insertUserWithBalance(t, pool, 1000)
	userB := insertUserWithBalance(t, pool, 1000)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"text":"ok"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")

	created, err := svc.Run(ctx, userA, makeRunReq(agentID, nil), "")
	require.NoError(t, err)

	_, err = svc.ListRunEvents(ctx, userB, mustParseUUID(t, created.RunID), 0, 10)
	assertHTTPStatus(t, err, http.StatusNotFound)
}

// ensureMockServerSilenced 让 httptest 的默认 ErrorLog 别打扰测试输出。
// 仅在测试包初始化阶段设置一次。预留 hook，不强制依赖。
//
// nolint:unused — go test 在没有 -v 时也会显示这种，作为预留 hook。
var _ = func() error { return nil }
