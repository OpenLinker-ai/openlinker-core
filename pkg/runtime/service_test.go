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
//   - agent 已下架 -> 403；private 仅 creator owner 可自测调用；认证状态不阻塞公开调用
//   - Core runtime 不依赖商业结算服务
//
// Core runtime does not perform commercial settlement; financial run fields stay 0.
//
// 内部 endpoint 默认通过 https；本文件用 httptest.NewTLSServer，并通过
// export_for_test.go 注入 server.Client() 信任自签证书。

package runtime_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

const truncateAll = "TRUNCATE run_requirement_evidence, run_artifact_chunks, run_artifacts, run_messages, run_events, task_callback_deliveries, task_callback_subscriptions, runs, task_queries, agent_skills, agents, users RESTART IDENTITY CASCADE"

const testDBOpTimeout = 30 * time.Second

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
	ctx, cancel := context.WithTimeout(context.Background(), testDBOpTimeout)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	require.NoError(t, pool.Ping(ctx))

	truncateCtx, truncateCancel := context.WithTimeout(context.Background(), testDBOpTimeout)
	defer truncateCancel()
	_, err = pool.Exec(truncateCtx, truncateAll)
	require.NoError(t, err)
	setRuntimeClusterMode(t, pool, "normal")
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), testDBOpTimeout)
		defer cancel()
		_, _ = pool.Exec(c, truncateAll)
		_, _ = pool.Exec(c, `
UPDATE runtime_cluster_control
SET mode = 'hard_maintenance', expected_replicas = 1,
    hard_maintenance_at = clock_timestamp(), reopened_at = NULL,
    version = version + 1, updated_at = clock_timestamp()
WHERE singleton_id = 1`)
		pool.Close()
	})
	return pool
}

func setRuntimeClusterMode(t *testing.T, pool *pgxpool.Pool, mode string) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
UPDATE runtime_cluster_control
SET mode = $1, expected_replicas = 1,
    drain_started_at = CASE WHEN $1 = 'draining' THEN clock_timestamp() ELSE NULL END,
    drain_deadline_at = NULL,
    hard_maintenance_at = CASE WHEN $1 = 'hard_maintenance' THEN clock_timestamp() ELSE hard_maintenance_at END,
    reopened_at = CASE WHEN $1 = 'normal' THEN clock_timestamp() ELSE NULL END,
    version = version + 1, updated_at = clock_timestamp()
WHERE singleton_id = 1`, mode)
	require.NoError(t, err)
}

// newTestConfig 构造 runtime 测试用的 *config.Config。
//
// 关键字段：
//   - RunTimeoutSeconds=2：测试用短 timeout，避免单测跑太久
//   - AllowLocalHTTPEndpoints=true：允许 httptest loopback endpoint
func newTestConfig() *config.Config {
	return &config.Config{
		RunTimeoutSeconds:       2,
		AllowLocalHTTPEndpoints: true,
	}
}

// newTestService 构造 Service。
func newTestService(t *testing.T, pool *pgxpool.Pool) *runtime.Service {
	t.Helper()
	svc := runtime.NewService(pool, newTestConfig())
	svc.ConfigureCoreRuntime(uuid.New())
	return svc
}

// ────────────────────────────────────────────────────────────
// 数据工厂：user / creator / agent
// ────────────────────────────────────────────────────────────

// insertRuntimeUser 创建一个普通 user。Core 测试不创建 cloud-owned wallet 行。
func insertRuntimeUser(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	uid := uuid.New()
	email := "rt-u-" + uid.String()[:8] + "@example.com"
	_, err := pool.Exec(ctx,
		`INSERT INTO users (id, email, password_hash, display_name) VALUES ($1, $2, $3, $4)`,
		uid, email, "x", "Runtime User")
	require.NoError(t, err)
	return uid
}

// insertCreator 创建 creator（is_creator=TRUE）。
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
	idempotencyKeyHash := sha256.Sum256(append([]byte("key:"), runID[:]...))
	idempotencyFingerprint := sha256.Sum256(append([]byte("request:"), runID[:]...))
	_, err := pool.Exec(context.Background(),
		`INSERT INTO runs (
			id, user_id, agent_id, input, status,
			cost_cents, platform_fee_cents, creator_revenue_cents,
			idempotency_key_hash, idempotency_fingerprint, request_metadata,
			connection_mode_snapshot, endpoint_idempotency_snapshot,
			max_offer_count, max_attempts, dispatch_deadline_at, run_deadline_at
		) VALUES (
			$1, $2, $3, '{}'::jsonb, 'running', 10, 2, 8,
			$4, $5, '{}'::jsonb, 'direct_http', TRUE,
			1, 1, clock_timestamp() + INTERVAL '5 minutes',
			clock_timestamp() + INTERVAL '10 minutes'
		)`,
		runID, userID, agentID, idempotencyKeyHash[:], idempotencyFingerprint[:])
	require.NoError(t, err)
	return runID
}

func TestGetRunIncludesEvidenceSummary(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"summary":"ok"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 0, "approved")
	taskID := uuid.New()

	_, err := pool.Exec(context.Background(),
		`INSERT INTO task_queries (id, user_id, query, parsed_skills, mcp_tools, recommended_agent_ids)
		 VALUES ($1, $2, '分析客服对话', '{content/summarization,content/structured-data}', '{run_agent}', $3)`,
		taskID, userID, []uuid.UUID{agentID})
	require.NoError(t, err)
	created, err := svc.Run(
		context.Background(), userID,
		makeRunReq(agentID, map[string]any{"query": "x"}), "web",
	)
	require.NoError(t, err)
	require.Equal(t, "success", created.Status)
	runID := mustParseUUID(t, created.RunID)
	_, err = pool.Exec(context.Background(),
		`INSERT INTO run_requirement_evidence (
				run_id, task_id, agent_id, user_id, required_skill_ids, required_mcp_tools,
				agent_skill_ids, matched_skill_ids, missing_skill_ids, used_mcp_tools,
				missing_mcp_tools, coverage_status, evidence_source
			) VALUES (
				$1, $2, $3, $4,
				'{content/summarization,content/structured-data}', '{run_agent}',
				'{content/summarization,content/structured-data}',
				'{content/summarization,content/structured-data}', '{}', '{run_agent}',
				'{}', 'covered', 'web'
			)`,
		runID, taskID, agentID, userID)
	require.NoError(t, err)
	_, err = pool.Exec(context.Background(),
		`UPDATE run_artifacts SET visibility = 'public_example' WHERE run_id = $1`,
		runID)
	require.NoError(t, err)

	resp, err := svc.GetRun(context.Background(), userID, runID)
	require.NoError(t, err)
	require.NotNil(t, resp.EvidenceSummary)
	assert.Equal(t, "success", resp.EvidenceSummary.Status)
	assert.Equal(t, "covered", resp.EvidenceSummary.CoverageStatus)
	assert.Equal(t, 2, resp.EvidenceSummary.MatchedSkillCount)
	assert.Equal(t, 0, resp.EvidenceSummary.MissingSkillCount)
	assert.Equal(t, 1, resp.EvidenceSummary.UsedMCPToolCount)
	assert.Equal(t, 1, resp.EvidenceSummary.ArtifactCount)
	assert.Equal(t, 1, resp.EvidenceSummary.MessageCount)
	assert.True(t, resp.EvidenceSummary.PublicSafe)
	assert.Equal(t, "/run/"+runID.String(), resp.EvidenceSummary.EvidenceURL)
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
	Sequence    int32
	EventType   string
	ParentRunID *uuid.UUID
	Payload     map[string]any
}

type recordedWebhookDelivery struct {
	RunID     uuid.UUID
	AgentSlug string
	Output    map[string]interface{}
}

type recordingRuntimeWebhookEnqueuer struct {
	deliveries chan recordedWebhookDelivery
	err        error
}

func (r *recordingRuntimeWebhookEnqueuer) EnqueueDelivery(_ context.Context, run *db.Run, agentSlug string, output map[string]interface{}) error {
	if run != nil {
		r.deliveries <- recordedWebhookDelivery{RunID: run.ID, AgentSlug: agentSlug, Output: output}
	}
	return r.err
}

type recordingRuntimeDeliveryEnqueuer struct {
	runs chan uuid.UUID
	err  error
}

func (r *recordingRuntimeDeliveryEnqueuer) EnqueueIfDefault(_ context.Context, run *db.Run) error {
	if run != nil {
		r.runs <- run.ID
	}
	return r.err
}

func waitRecordedWebhookDelivery(t *testing.T, ch <-chan recordedWebhookDelivery) recordedWebhookDelivery {
	t.Helper()
	select {
	case got := <-ch:
		return got
	case <-time.After(3 * time.Second):
		t.Fatal("webhook delivery was not enqueued")
		return recordedWebhookDelivery{}
	}
}

func waitRecordedDelivery(t *testing.T, ch <-chan uuid.UUID) uuid.UUID {
	t.Helper()
	select {
	case got := <-ch:
		return got
	case <-time.After(3 * time.Second):
		t.Fatal("delivery was not enqueued")
		return uuid.Nil
	}
}

// readRunEvents 读 run_events 的事件类型和 payload，按 sequence 正序。
func readRunEvents(t *testing.T, pool *pgxpool.Pool, runID uuid.UUID) []runEventRow {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT sequence, event_type, parent_run_id, payload FROM run_events WHERE run_id=$1 ORDER BY sequence ASC`,
		runID)
	require.NoError(t, err)
	defer rows.Close()

	var events []runEventRow
	for rows.Next() {
		var event runEventRow
		var raw []byte
		require.NoError(t, rows.Scan(&event.Sequence, &event.EventType, &event.ParentRunID, &raw))
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

// assertRunAccountingConsistent 简化对账：runs 表 fee+revenue=cost。
// DB CHECK 已保证一次，这里再次断言以防测试绕过约束。
func assertRunAccountingConsistent(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()

	var inconsistent int
	err := pool.QueryRow(ctx,
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
		AgentID:        agentID.String(),
		Input:          input,
		IdempotencyKey: "test/" + uuid.NewString(),
	}
}

type recordingRuntimeTaskCallbackEnqueuer struct {
	events chan db.RunEvent
}

func (r *recordingRuntimeTaskCallbackEnqueuer) EnqueueRunEvent(_ context.Context, event db.RunEvent) error {
	r.events <- event
	return nil
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

	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"text":"ok"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved") // $0.10

	resp, err := svc.Run(ctx, userID, makeRunReq(agentID, map[string]any{"q": "hello"}), "")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "success", resp.Status)
	assert.Equal(t, int32(0), resp.CostCents)

	assert.Equal(t, 1, countRunsForUser(t, pool, userID))
	totalCalls, totalRevenue := readAgentStats(t, pool, agentID)
	assert.Equal(t, int32(1), totalCalls)
	assert.Equal(t, int64(0), totalRevenue)
	availability := readAgentAvailability(t, pool, agentID)
	assert.Equal(t, "healthy", availability.Status)
	assert.Equal(t, int32(0), availability.ConsecutiveFailures)
	require.NotNil(t, availability.LastSuccessfulRunAt)

	runID := mustParseUUID(t, resp.RunID)
	run := readRun(t, pool, runID)
	assert.Equal(t, "success", run.Status)
	assert.Equal(t, int32(0), run.CostCents)
	assert.Equal(t, int32(0), run.PlatformFeeCents)
	assert.Equal(t, int32(0), run.CreatorRevenueCents)
	var executorType, outcome string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT executor_type, outcome FROM run_attempts WHERE run_id = $1`, runID).Scan(
		&executorType, &outcome,
	))
	assert.Equal(t, "core_http", executorType)
	assert.Equal(t, "success", outcome)

	events := readRunEvents(t, pool, mustParseUUID(t, resp.RunID))
	require.Len(t, events, 4)
	assert.Equal(t, int32(1), events[0].Sequence)
	assert.Equal(t, "run.created", events[0].EventType)
	assert.Equal(t, "running", events[0].Payload["status"])
	assert.Equal(t, "run.started", events[1].EventType)
	assert.Equal(t, "direct_http", events[1].Payload["connection_mode"])
	assert.Equal(t, "http_endpoint", events[1].Payload["transport"])
	assert.NotEmpty(t, events[1].Payload["endpoint_host"])
	assert.Equal(t, int32(3), events[2].Sequence)
	assert.Equal(t, "run.status.changed", events[2].EventType)
	assert.Equal(t, "endpoint_response_received", events[2].Payload["status"])
	assert.Equal(t, "run.completed", events[3].EventType)
	assert.Equal(t, "success", events[3].Payload["status"])

	artifacts, err := svc.ListRunArtifacts(ctx, userID, mustParseUUID(t, resp.RunID))
	require.NoError(t, err)
	require.Len(t, artifacts, 1)
	assert.Equal(t, "Agent 输出", artifacts[0].Title)
	assert.Equal(t, "json", artifacts[0].ArtifactType)
	assert.Equal(t, "private", artifacts[0].Visibility)
	assert.Equal(t, "ok", artifacts[0].Content["text"])

	assertRunAccountingConsistent(t, pool)
}

func TestRun_MCPServerUsesCoreAttemptAndFinalizer(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, func(w http.ResponseWriter, r *http.Request) {
		var request map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		require.Equal(t, "tools/call", request["method"])
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":{"structuredContent":{"answer":"mcp ok"}}}`))
	})
	agentID := insertAgent(t, pool, creatorID, endpoint, 0, "approved")
	_, err := pool.Exec(ctx, `
		UPDATE agents
		SET connection_mode = 'mcp_server', mcp_tool_name = 'analyze'
		WHERE id = $1`, agentID)
	require.NoError(t, err)

	resp, err := svc.Run(ctx, userID, makeRunReq(agentID, map[string]any{"q": "review"}), "api")
	require.NoError(t, err)
	require.Equal(t, "success", resp.Status)
	require.Equal(t, "mcp ok", resp.Output["answer"])

	runID := mustParseUUID(t, resp.RunID)
	var executorType, outcome string
	var finished bool
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT executor_type, outcome, finished_at IS NOT NULL
		FROM run_attempts
		WHERE run_id = $1`, runID).Scan(&executorType, &outcome, &finished))
	require.Equal(t, "core_mcp", executorType)
	require.Equal(t, "success", outcome)
	require.True(t, finished)

	events := readRunEvents(t, pool, runID)
	require.NotEmpty(t, events)
	require.Equal(t, "run.completed", events[len(events)-1].EventType)
}

func TestRun_DirectHTTPCommitsAttemptBeforeExternalIO(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()
	type observation struct {
		executorType  string
		dispatchState string
		accepted      bool
		err           error
	}
	observed := make(chan observation, 1)
	endpoint := startMockEndpointForService(t, svc, func(w http.ResponseWriter, r *http.Request) {
		runID, err := uuid.Parse(r.Header.Get("X-OpenLinker-Run-Id"))
		if err != nil {
			observed <- observation{err: err}
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var got observation
		got.err = pool.QueryRow(r.Context(), `
			SELECT a.executor_type, run.dispatch_state, a.accepted_at IS NOT NULL
			FROM runs run
			JOIN run_attempts a ON a.run_id = run.id AND a.id = run.active_attempt_id
			WHERE run.id = $1`, runID).Scan(&got.executorType, &got.dispatchState, &got.accepted)
		observed <- got
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"output":{"ok":true}}`))
	})

	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, creatorID, endpoint, 0, "approved")
	resp, err := svc.Run(ctx, userID, makeRunReq(agentID, map[string]any{"q": "proof"}), "api")
	require.NoError(t, err)
	require.Equal(t, "success", resp.Status)
	got := <-observed
	require.NoError(t, got.err)
	require.Equal(t, "core_http", got.executorType)
	require.Equal(t, "executing", got.dispatchState)
	require.True(t, got.accepted)
}

func TestRun_AgentNodeReadySessionClaimsDurableOffer(t *testing.T) {
	pool := setupTestDB(t)
	ctx := context.Background()
	coreID := uuid.New()
	svc := runtime.NewService(pool, newTestConfig())
	svc.ConfigureCoreRuntime(coreID)

	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, creatorID, "openlinker-runtime://ready-session", 0, "approved")
	_, err := pool.Exec(ctx, `UPDATE agents SET connection_mode = 'runtime' WHERE id = $1`, agentID)
	require.NoError(t, err)

	tokenID, nodeID, sessionID, attachmentID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	workerID := uuid.NewString()
	certificateSerial := hex.EncodeToString(nodeID[:])
	thumbprintDigest := sha256.Sum256(nodeID[:])
	thumbprint := hex.EncodeToString(thumbprintDigest[:])
	prefix := "ol_agent_" + hex.EncodeToString(tokenID[:6])
	fixtureTx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = fixtureTx.Rollback(ctx) }()
	_, err = fixtureTx.Exec(ctx, `
		INSERT INTO agent_tokens (
			id, agent_id, creator_user_id, name, prefix, token_hash,
			scopes, status, redeemed_at
		) VALUES ($1, $2, $3, 'Runtime Worker ready Session', $4, $5,
			ARRAY['agent:call', 'agent:pull']::text[], 'active_runtime', clock_timestamp())`,
		tokenID, agentID, creatorID, prefix, "hash-"+tokenID.String())
	require.NoError(t, err)
	_, err = fixtureTx.Exec(ctx, `
		INSERT INTO runtime_nodes (
			node_id, display_name, device_certificate_serial,
			device_public_key_thumbprint, node_version, protocol_version,
			runtime_contract_id, runtime_contract_digest, features,
			capacity, last_seen_at
		) VALUES ($1, 'Runtime Worker ready Session', $2, $3, 'test-v2', 2,
			'openlinker.runtime.v2', $4, $5, 1, clock_timestamp())`,
		nodeID, certificateSerial, thumbprint, runtime.RuntimeContractDigest,
		runtime.RuntimeRequiredFeatures())
	require.NoError(t, err)
	_, err = fixtureTx.Exec(ctx, `
		INSERT INTO runtime_sessions (
			runtime_session_id, node_id, agent_id, credential_id, worker_id,
			session_epoch, device_certificate_serial, node_version,
			protocol_version, runtime_contract_id, runtime_contract_digest,
			features, capacity, attached_core_instance_id
		) VALUES ($1, $2, $3, $4, $5, 1, $6, 'test-v2', 2,
			'openlinker.runtime.v2', $7, $8, 1, $9)`,
		sessionID, nodeID, agentID, tokenID, workerID, certificateSerial,
		runtime.RuntimeContractDigest, runtime.RuntimeRequiredFeatures(), coreID)
	require.NoError(t, err)
	_, err = fixtureTx.Exec(ctx, `
		INSERT INTO runtime_session_attachments (
			id, runtime_session_id, core_instance_id, attachment_kind
		) VALUES ($1, $2, $3, 'connected')`, attachmentID, sessionID, coreID)
	require.NoError(t, err)
	require.NoError(t, fixtureTx.Commit(ctx))

	created, err := svc.Run(ctx, userID, makeRunReq(agentID, map[string]any{"task": "claim me"}), "api")
	require.NoError(t, err)
	require.Equal(t, "running", created.Status)
	require.Equal(t, "runtime", created.AgentConnectionMode)

	signer, err := runtime.NewRuntimeInvocationSignerWithPrevious(
		"test-current", "runtime-v2-integration-signing-secret-32-bytes", "", "",
	)
	require.NoError(t, err)
	leases := runtime.NewRuntimeLeaseService(pool, coreID, signer, runtime.DefaultRuntimeLeaseConfig())
	assigned, err := leases.ClaimOffer(ctx, runtime.RuntimeSessionPrincipal{
		RuntimeSessionID:                sessionID,
		NodeID:                          nodeID,
		AgentID:                         agentID,
		CredentialID:                    tokenID,
		WorkerID:                        workerID,
		SessionEpoch:                    1,
		RuntimeContractDigest:           runtime.RuntimeContractDigest,
		CoreInstanceID:                  coreID,
		AttachmentID:                    attachmentID,
		DeviceCertificateSerial:         certificateSerial,
		DevicePublicKeyThumbprintSHA256: thumbprint,
		Status:                          "active",
	})
	require.NoError(t, err)
	require.NotNil(t, assigned)
	require.Equal(t, mustParseUUID(t, created.RunID), assigned.AttemptIdentity.RunID)
	require.Equal(t, sessionID, assigned.AttemptIdentity.RuntimeSessionID)

	var dispatchState, executorType string
	var offerCount int32
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT r.dispatch_state, r.offer_count, a.executor_type
		FROM runs r
		JOIN run_attempts a ON a.id = r.active_attempt_id
		WHERE r.id = $1`, assigned.AttemptIdentity.RunID).Scan(&dispatchState, &offerCount, &executorType))
	require.Equal(t, "offered", dispatchState)
	require.Equal(t, int32(1), offerCount)
	require.Equal(t, "runtime", executorType)

	var accepted, unfinished bool
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT accepted_at IS NOT NULL, finished_at IS NULL
		FROM run_attempts WHERE id = $1`, assigned.AttemptIdentity.AttemptID).Scan(&accepted, &unfinished))
	require.False(t, accepted, "ClaimOffer must leave a durable, unaccepted offer")
	require.True(t, unfinished)

	_, err = svc.DrainRuntimeNode(ctx, nodeID)
	require.NoError(t, err)
	// Zero the advisory counters so the durable unaccepted-offer row is the
	// only activation fence exercised here.
	_, err = pool.Exec(ctx, `UPDATE runtime_sessions SET inflight = 0 WHERE runtime_session_id = $1`, sessionID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `UPDATE runtime_nodes SET inflight = 0 WHERE node_id = $1`, nodeID)
	require.NoError(t, err)
	_, err = svc.ActivateRuntimeNode(ctx, nodeID)
	var activationErr *httpx.HTTPError
	require.ErrorAs(t, err, &activationErr)
	require.Equal(t, httpx.ErrorCode("RUNTIME_NODE_NOT_QUIESCENT"), activationErr.Code)
}

func TestRun_CreatesCallerOwnedTaskCallbackSubscription(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"text":"callback ok"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")
	req := makeRunReq(agentID, map[string]any{"q": "callback"})
	req.PushNotificationConfig = &runtime.TaskCallbackConfig{
		URL:        "https://caller.example.com/a2a/events",
		Token:      "caller-token",
		Secret:     "caller-secret",
		EventTypes: []string{"run.completed", "run.failed"},
		Metadata:   map[string]interface{}{"client": "integration"},
	}

	resp, err := svc.Run(ctx, userID, req, "api")
	require.NoError(t, err)
	require.Equal(t, "success", resp.Status)
	require.NotNil(t, resp.TaskCallback)
	assert.Equal(t, resp.RunID, resp.TaskCallback.RunID)
	assert.Equal(t, "https://caller.example.com/a2a/events", resp.TaskCallback.TargetURL)
	assert.Equal(t, []string{"run.completed", "run.failed"}, resp.TaskCallback.EventTypes)
	assert.Equal(t, "Bearer", resp.TaskCallback.AuthScheme)
	assert.Equal(t, "caller-secret", resp.TaskCallback.Secret)

	runID := mustParseUUID(t, resp.RunID)
	subs, err := db.New(pool).ListTaskCallbackSubscriptionsByRun(ctx, db.ListTaskCallbackSubscriptionsByRunParams{
		RunID:       runID,
		OwnerUserID: userID,
	})
	require.NoError(t, err)
	require.Len(t, subs, 1)
	assert.Equal(t, userID, subs[0].OwnerUserID)
	assert.Nil(t, subs[0].CallerAgentID)
	assert.Equal(t, "active", subs[0].Status)
	require.NotNil(t, subs[0].AuthScheme)
	require.NotNil(t, subs[0].AuthCredentials)
	assert.Equal(t, "Bearer", *subs[0].AuthScheme)
	assert.Equal(t, "caller-token", *subs[0].AuthCredentials)
	assert.Equal(t, "caller-secret", subs[0].Secret)
	var metadata map[string]any
	require.NoError(t, json.Unmarshal(subs[0].Metadata, &metadata))
	assert.Equal(t, "integration", metadata["client"])

	effects, err := db.New(pool).ListRunEffectsByRun(ctx, runID)
	require.NoError(t, err)
	require.Condition(t, func() bool {
		for _, effect := range effects {
			if effect.EffectType == runtime.RunEffectTypeTaskCallback {
				return true
			}
		}
		return false
	}, "terminal finalization must enqueue a durable task callback effect")
}

func TestRun_WithTaskRequirementEvidence(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"summary":"done"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")
	_, err := pool.Exec(ctx,
		`INSERT INTO agent_skills (agent_id, skill_id) VALUES ($1, 'data/sql-query')`,
		agentID)
	require.NoError(t, err)

	var taskID uuid.UUID
	err = pool.QueryRow(ctx,
		`INSERT INTO task_queries (user_id, query, parsed_skills, mcp_tools, recommended_agent_ids, chosen_agent_id, chosen_at)
		 VALUES ($1, '做 SQL 查询和数据分析', ARRAY['data/sql-query','data/analysis']::text[], ARRAY['run_agent']::text[], ARRAY[$2]::uuid[], $2, NOW())
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
	var completionRunID uuid.UUID
	var completionSummary string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT completion_run_id, completion_summary FROM task_queries WHERE id = $1`, taskID,
	).Scan(&completionRunID, &completionSummary))
	require.Equal(t, mustParseUUID(t, resp.RunID), completionRunID)
	require.Equal(t, "done", completionSummary)

	got, err := svc.GetRun(ctx, userID, mustParseUUID(t, resp.RunID))
	require.NoError(t, err)
	require.NotNil(t, got.RequirementEvidence)
	assert.Equal(t, resp.RequirementEvidence.CoverageStatus, got.RequirementEvidence.CoverageStatus)

	events := readRunEvents(t, pool, mustParseUUID(t, resp.RunID))
	require.Len(t, events, 5)
	assert.Equal(t, "run.requirements.snapshotted", events[1].EventType)
	assert.Equal(t, taskID.String(), events[1].Payload["task_id"])
	assert.Equal(t, "partial", events[1].Payload["coverage_status"])
	assert.Equal(t, "run.started", events[2].EventType)
	assert.Equal(t, "run.status.changed", events[3].EventType)
	assert.Equal(t, "run.completed", events[4].EventType)
}

func TestRun_PersistsDeclaredArtifacts(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertRuntimeUser(t, pool)
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
	byTitle := map[string]runtime.RunArtifactResponse{}
	for _, artifact := range artifacts {
		byTitle[artifact.Title] = artifact
	}
	summary := byTitle["结论摘要"]
	assert.Equal(t, "shared", summary.Visibility)
	assert.Equal(t, "ok", summary.Content["summary"])
	rawData := byTitle["原始数据"]
	assert.Equal(t, "data", rawData.ArtifactType)
	assert.Equal(t, float64(3), rawData.Content["rows"])
}

func TestRun_PersistsFileArtifactMetadata(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{
		"output":{
			"summary":"file ready",
			"artifacts":[
				{
					"title":"报告 CSV",
					"artifact_type":"file",
					"file_uri":"https://files.example/report.csv",
					"file_name":"report.csv",
					"mime_type":"text/csv",
					"file_sha256":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
					"file_size_bytes":128,
					"content":{"rows":3}
				}
			]
		}
	}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")

	resp, err := svc.Run(ctx, userID, makeRunReq(agentID, map[string]any{"q": "file artifact"}), "")
	require.NoError(t, err)
	require.Equal(t, "success", resp.Status)

	artifacts, err := svc.ListRunArtifacts(ctx, userID, mustParseUUID(t, resp.RunID))
	require.NoError(t, err)
	require.Len(t, artifacts, 1)
	assert.Equal(t, "file", artifacts[0].ArtifactType)
	assert.Equal(t, "text/csv", artifacts[0].MimeType)
	assert.Equal(t, "https://files.example/report.csv", artifacts[0].FileURI)
	assert.Equal(t, "report.csv", artifacts[0].FileName)
	assert.Equal(t, "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", artifacts[0].FileSHA256)
	require.NotNil(t, artifacts[0].FileSizeBytes)
	assert.Equal(t, int64(128), *artifacts[0].FileSizeBytes)
	assert.Equal(t, float64(3), artifacts[0].Content["rows"])
}

func TestRun_NextActionFromAgentOutput(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertRuntimeUser(t, pool)
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

	userID := insertRuntimeUser(t, pool)
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
	require.Len(t, events, 6)
	assert.Equal(t, "run.created", events[0].EventType)
	assert.Equal(t, "run.started", events[1].EventType)
	assert.Equal(t, "run.status.changed", events[2].EventType)
	assert.Equal(t, "endpoint_response_received", events[2].Payload["status"])
	assert.Equal(t, "run.message.delta", events[3].EventType)
	assert.Equal(t, "draft ready", events[3].Payload["text"])
	assert.Equal(t, "run.artifact.delta", events[4].EventType)
	assert.Equal(t, "art_1", events[4].Payload["artifact_id"])
	assert.Equal(t, "run.completed", events[5].EventType)

	artifacts, err := svc.ListRunArtifacts(ctx, userID, mustParseUUID(t, resp.RunID))
	require.NoError(t, err)
	require.Len(t, artifacts, 1)
	var streamed *runtime.RunArtifactResponse
	for i := range artifacts {
		if artifacts[i].SourceArtifactID == "art_1" {
			streamed = &artifacts[i]
			break
		}
	}
	require.NotNil(t, streamed)
	assert.NotEqual(t, "Agent 输出", streamed.Title)
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
	assert.Equal(t, int32(5), *chunks[0].EventSequence)

	messages, err := svc.ListRunMessages(ctx, userID, mustParseUUID(t, resp.RunID))
	require.NoError(t, err)
	require.Len(t, messages, 2)
	assert.Equal(t, "user", messages[0].Role)
	assert.Contains(t, messages[0].Content, "hello")
	assert.Equal(t, "agent", messages[1].Role)
	assert.Equal(t, "draft ready", messages[1].Content)

	assertRunAccountingConsistent(t, pool)
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

	userID := insertRuntimeUser(t, pool)
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

	require.Eventually(t, func() bool {
		return len(readRunEvents(t, pool, runID)) >= 2
	}, time.Second, 10*time.Millisecond)
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

	require.Eventually(t, func() bool {
		got := readRunEvents(t, pool, runID)
		return len(got) >= 4 && got[3].EventType == "run.completed"
	}, 3*time.Second, 20*time.Millisecond)
	events = readRunEvents(t, pool, runID)
	require.Len(t, events, 4)
	assert.Equal(t, "run.status.changed", events[2].EventType)
	assert.Equal(t, "run.completed", events[3].EventType)

	assertRunAccountingConsistent(t, pool)
}

// ────────────────────────────────────────────────────────────
// Run - 错误路径
// ────────────────────────────────────────────────────────────

func TestRun_FreePhaseDoesNotRequireWalletBalance(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	called, getCount := mockEndpointCounting(mockEndpointReturning(http.StatusOK, `{"output":{"text":"ok"}}`))
	endpoint := startMockEndpointForService(t, svc, called)
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")

	resp, err := svc.Run(ctx, userID, makeRunReq(agentID, nil), "")
	require.NoError(t, err)
	assert.Equal(t, "success", resp.Status)

	assert.Equal(t, 1, countRunsForUser(t, pool, userID))
	assert.Equal(t, int64(1), getCount(), "free run must call endpoint without wallet balance")

	assertRunAccountingConsistent(t, pool)
}

func TestRun_AgentNotFound(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertRuntimeUser(t, pool)
	_, err := svc.Run(ctx, userID, makeRunReq(uuid.New(), nil), "")
	assertHTTPStatus(t, err, http.StatusNotFound)
}

func TestRun_CertificationPendingStillCallable(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"text":"ok"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "pending")

	resp, err := svc.Run(ctx, userID, makeRunReq(agentID, nil), "")
	require.NoError(t, err)
	assert.Equal(t, "success", resp.Status)
}

func TestRun_PrivateAgentCallableByOwnerOnly(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	creatorID := insertCreator(t, pool)
	otherUserID := insertRuntimeUser(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"text":"owner ok"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")
	_, err := pool.Exec(ctx, `UPDATE agents SET visibility='private' WHERE id=$1`, agentID)
	require.NoError(t, err)

	_, err = svc.Run(ctx, otherUserID, makeRunReq(agentID, nil), "")
	assertHTTPStatus(t, err, http.StatusForbidden)

	resp, err := svc.Run(ctx, creatorID, makeRunReq(agentID, nil), "")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "success", resp.Status)
	assert.Equal(t, "owner ok", resp.Output["text"])
}

// ────────────────────────────────────────────────────────────
// Run - endpoint 失败
// ────────────────────────────────────────────────────────────

func TestRun_EndpointReturns500(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusInternalServerError, `{"err":"boom"}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")

	resp, err := svc.Run(ctx, userID, makeRunReq(agentID, nil), "")
	require.NoError(t, err, "endpoint failure 应该是逻辑失败而不是 HTTP error")
	require.NotNil(t, resp)
	assert.Equal(t, "failed", resp.Status)

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

	assertRunAccountingConsistent(t, pool)
}

func TestRun_ConsecutiveFailuresMarkAgentUnreachable(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertRuntimeUser(t, pool)
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

	assertRunAccountingConsistent(t, pool)
}

func TestRun_EndpointReturnsErrorJSON(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc,
		mockEndpointReturning(http.StatusOK, `{"error":{"code":"BAD_INPUT","message":"x"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")

	resp, err := svc.Run(ctx, userID, makeRunReq(agentID, nil), "")
	require.NoError(t, err)
	assert.Equal(t, "failed", resp.Status)

	run := readRun(t, pool, mustParseUUID(t, resp.RunID))
	require.NotNil(t, run.ErrorCode)
	assert.Equal(t, "BAD_INPUT", *run.ErrorCode)

	assertRunAccountingConsistent(t, pool)
}

func TestRun_EndpointTimeout(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	// cfg.RunTimeoutSeconds=2，让 endpoint sleep 5s 触发 timeout
	endpoint := startMockEndpointForService(t, svc, mockEndpointTimeout(5*time.Second))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")

	resp, err := svc.Run(ctx, userID, makeRunReq(agentID, nil), "")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "failed", resp.Status)

	runID := mustParseUUID(t, resp.RunID)
	run := readRun(t, pool, runID)
	require.NotNil(t, run.ErrorCode)
	assert.Equal(t, "ENDPOINT_RESULT_UNKNOWN", *run.ErrorCode)
	var maxAttempts int32
	require.NoError(t, pool.QueryRow(ctx, `SELECT max_attempts FROM runs WHERE id = $1`, runID).Scan(&maxAttempts))
	assert.Equal(t, int32(1), maxAttempts)
	var attempts int
	var outcome, attemptErrorCode string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)::int, max(outcome), max(error_code)
		FROM run_attempts WHERE run_id = $1`, runID).Scan(
		&attempts, &outcome, &attemptErrorCode,
	))
	assert.Equal(t, 1, attempts)
	assert.Equal(t, "non_retryable_failure", outcome)
	assert.Equal(t, "ENDPOINT_RESULT_UNKNOWN", attemptErrorCode)

	assertRunAccountingConsistent(t, pool)
}

func TestRun_EndpointReturnsInvalidJSON(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertRuntimeUser(t, pool)
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

	run := readRun(t, pool, mustParseUUID(t, resp.RunID))
	require.NotNil(t, run.ErrorCode)
	assert.Equal(t, "INVALID_RESPONSE", *run.ErrorCode)

	assertRunAccountingConsistent(t, pool)
}

// ────────────────────────────────────────────────────────────
// Run - 无商业结算时的财务字段
// ────────────────────────────────────────────────────────────

func TestRun_RecordsNoSettlementWithoutSettlementService(t *testing.T) {
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

			userID := insertRuntimeUser(t, pool)
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

			assertRunAccountingConsistent(t, pool)
		})
	}
}

// ────────────────────────────────────────────────────────────
// Run - 并发安全
// ────────────────────────────────────────────────────────────

func TestRun_ConcurrentCallsDoNotRequireSettlement(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)

	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"text":"ok"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 100, "approved") // $1 each

	const N = 10
	var (
		wg        sync.WaitGroup
		successes int64
		otherErrs int64
	)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := svc.Run(ctx, userID, makeRunReq(agentID, nil), "")
			if err != nil {
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

	assertRunAccountingConsistent(t, pool)
}

// ────────────────────────────────────────────────────────────
// GetRun
// ────────────────────────────────────────────────────────────

func TestGetRun_HappyPath(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertRuntimeUser(t, pool)
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

	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"text":"ok"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")

	created, err := svc.Run(ctx, userID, makeRunReq(agentID, nil), "")
	require.NoError(t, err)
	runID := mustParseUUID(t, created.RunID)

	events, err := svc.ListRunEvents(ctx, userID, runID, 1, 10)
	require.NoError(t, err)
	require.Len(t, events, 3)
	assert.Equal(t, int32(2), events[0].Sequence)
	assert.Equal(t, "run.started", events[0].EventType)
	assert.Equal(t, "direct_http", events[0].Payload["connection_mode"])
	assert.Equal(t, "run.status.changed", events[1].EventType)
	assert.Equal(t, "run.completed", events[2].EventType)
}

func TestCancelRun_MarksRunningRunCanceledAndEmitsEvent(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"ok":true}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")
	runID := insertRunningRun(t, pool, userID, agentID)

	resp, err := svc.CancelRun(ctx, userID, runID)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, runID.String(), resp.RunID)
	assert.Equal(t, "canceled", resp.Status)
	assert.Equal(t, "CANCELED", resp.ErrorCode)
	assert.Equal(t, "run canceled by user", resp.ErrorMsg)
	assert.GreaterOrEqual(t, resp.DurationMs, int32(0))

	stored := readRun(t, pool, runID)
	assert.Equal(t, "canceled", stored.Status)
	require.NotNil(t, stored.ErrorCode)
	assert.Equal(t, "CANCELED", *stored.ErrorCode)

	again, err := svc.CancelRun(ctx, userID, runID)
	require.NoError(t, err)
	assert.Equal(t, "canceled", again.Status)
	assert.Equal(t, runID.String(), again.RunID)

	events := readRunEvents(t, pool, runID)
	require.Len(t, events, 1)
	assert.Equal(t, "run.canceled", events[0].EventType)
	assert.Equal(t, "canceled", events[0].Payload["status"])
	assert.Equal(t, "CANCELED", events[0].Payload["error_code"])
}

func TestCancelRun_RejectsNonOwnerMissingAndFinishedRuns(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertRuntimeUser(t, pool)
	otherUserID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, creatorID, "https://example.com/agent", 10, "approved")

	runID := insertRunningRun(t, pool, userID, agentID)
	_, err := svc.CancelRun(ctx, otherUserID, runID)
	assertHTTPStatus(t, err, http.StatusNotFound)

	_, err = svc.CancelRun(ctx, userID, uuid.New())
	assertHTTPStatus(t, err, http.StatusNotFound)

	finished, err := svc.Run(ctx, userID, makeRunReq(agentID, map[string]any{"done": true}), "api")
	require.NoError(t, err)
	finishedRunID := mustParseUUID(t, finished.RunID)
	_, err = svc.CancelRun(ctx, userID, finishedRunID)
	assertHTTPStatus(t, err, http.StatusConflict)
}

func sha256String(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func TestGetRun_NotOwner(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userA := insertRuntimeUser(t, pool)
	userB := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"text":"ok"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")

	created, err := svc.Run(ctx, userA, makeRunReq(agentID, nil), "")
	require.NoError(t, err)

	// userB 不是 owner，必须 404（不暴露存在性）
	_, err = svc.GetRun(ctx, userB, mustParseUUID(t, created.RunID))
	assertHTTPStatus(t, err, http.StatusNotFound)

	got, err := svc.GetRun(ctx, creatorID, mustParseUUID(t, created.RunID))
	require.NoError(t, err)
	require.Equal(t, created.RunID, got.RunID)
	require.Equal(t, agentID.String(), got.AgentID)
}

func TestGetRun_Missing(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userID := insertRuntimeUser(t, pool)
	_, err := svc.GetRun(ctx, userID, uuid.New())
	assertHTTPStatus(t, err, http.StatusNotFound)
}

func TestListRunEvents_NotOwner(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userA := insertRuntimeUser(t, pool)
	userB := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"text":"ok"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")

	created, err := svc.Run(ctx, userA, makeRunReq(agentID, nil), "")
	require.NoError(t, err)

	_, err = svc.ListRunEvents(ctx, userB, mustParseUUID(t, created.RunID), 0, 10)
	assertHTTPStatus(t, err, http.StatusNotFound)
}

func TestRunReadEndpointsRejectMissingInvalidAndNonOwner(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()

	userA := insertRuntimeUser(t, pool)
	userB := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, creatorID, "https://example.com/agent", 10, "approved")
	runID := insertRunningRun(t, pool, userA, agentID)
	missingRunID := uuid.New()

	_, err := svc.ListRunEvents(ctx, userA, missingRunID, 0, 10)
	assertHTTPStatus(t, err, http.StatusNotFound)
	_, err = svc.ListRunEvents(ctx, userA, runID, -1, 10)
	assertHTTPStatus(t, err, http.StatusBadRequest)
	events, err := svc.ListRunEvents(ctx, userA, runID, 0, 9999)
	require.NoError(t, err)
	assert.Empty(t, events)

	_, err = svc.ListRunArtifacts(ctx, userA, missingRunID)
	assertHTTPStatus(t, err, http.StatusNotFound)
	_, err = svc.ListRunArtifacts(ctx, userB, runID)
	assertHTTPStatus(t, err, http.StatusNotFound)
	artifacts, err := svc.ListRunArtifacts(ctx, userA, runID)
	require.NoError(t, err)
	assert.Empty(t, artifacts)

	_, err = svc.ListRunMessages(ctx, userA, missingRunID)
	assertHTTPStatus(t, err, http.StatusNotFound)
	_, err = svc.ListRunMessages(ctx, userB, runID)
	assertHTTPStatus(t, err, http.StatusNotFound)
	messages, err := svc.ListRunMessages(ctx, userA, runID)
	require.NoError(t, err)
	assert.Empty(t, messages)
}
