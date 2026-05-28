// Package agent_test - 共享测试辅助（market + 注册/公开状态 共用）。
//
// 这些 helper 由 subagent-3a (市场查询) 与 subagent-2b (Agent 注册/公开状态)
// 的 _test.go 共同使用，集中放在本文件里避免重复定义。
//
// 设计要点：
//   - 包名 `agent_test`（外部黑盒测试），与 internal/agent 的真实代码隔离
//   - 通过 TEST_DATABASE_URL 提供真实 Postgres，未设置则 t.Skip()
//   - 每次 setupTestDB 都会 TRUNCATE 关键表来保证数据隔离
package agent_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// truncateAll 测试间数据隔离：从依赖最多的表先删，CASCADE 兜底。
// users CASCADE 会带走 cloud 侧 wallets/charges/withdrawals/api_keys（若已应用 cloud migrations）；
// 这里只列 core 自带的表，避免运行 core 测试时强制依赖 cloud schema。
const truncateAll = "TRUNCATE runs, agents, users RESTART IDENTITY CASCADE"

const testDBOpTimeout = 30 * time.Second
const agentTestAdvisoryLockID int64 = 270017

// skipIfNoDB 检查 TEST_DATABASE_URL 环境变量；未设置则 skip 当前 test。
// 返回 dsn 字符串，调用方可以用它再连一次（少见）。
func skipIfNoDB(t *testing.T) string {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skipf("TEST_DATABASE_URL 未设置，跳过 agent 集成测试")
	}
	return url
}

// setupTestDB 拿到 pool，并清理表保证测试隔离。t.Cleanup 注册 close。
func setupTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := skipIfNoDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), testDBOpTimeout)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err, "connect test db")
	require.NoError(t, pool.Ping(ctx), "ping test db")

	_, err = pool.Exec(ctx, `SELECT pg_advisory_lock($1)`, agentTestAdvisoryLockID)
	require.NoError(t, err, "lock agent test db")

	t.Cleanup(func() {
		clean, cancel := context.WithTimeout(context.Background(), testDBOpTimeout)
		defer cancel()
		_, _ = pool.Exec(clean, truncateAll)
		_, _ = pool.Exec(clean, `SELECT pg_advisory_unlock($1)`, agentTestAdvisoryLockID)
		pool.Close()
	})

	truncateCtx, truncateCancel := context.WithTimeout(context.Background(), testDBOpTimeout)
	defer truncateCancel()
	_, err = pool.Exec(truncateCtx, truncateAll)
	require.NoError(t, err, "truncate test tables")
	return pool
}

// ─────────────────────────────────────────────────────────
// User / Agent 工厂
// ─────────────────────────────────────────────────────────

// insertCreatorUser 直接 INSERT 一个 creator 用户（绕过 auth）。返回 user_id。
// is_creator=TRUE，creator_verified=TRUE。
func insertCreatorUser(t *testing.T, pool *pgxpool.Pool, displayName string) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	uid := uuid.New()
	email := "agent-c-" + uid.String()[:8] + "@example.com"
	_, err := pool.Exec(ctx,
		`INSERT INTO users (id, email, password_hash, display_name, is_creator, creator_verified)
		 VALUES ($1, $2, $3, $4, TRUE, TRUE)`,
		uid, email, "x", displayName)
	require.NoError(t, err, "insert creator user")
	return uid
}

// setupTestData 创建一个 creator + wallet，返回 creatorID 和 cleanup 函数。
//
// cleanup 仅作为约定预留：因为 setupTestDB 已经在 t.Cleanup 注册了 TRUNCATE，
// 单测里通常不必显式调 cleanup。返回 cleanup 主要让调用方可读性更好。
func setupTestData(t *testing.T, pool *pgxpool.Pool) (uuid.UUID, func()) {
	t.Helper()
	creatorID := insertCreatorUser(t, pool, "Test Creator")
	// wallets 在 cloud migrations 里，core 测试 DB 不一定有；best-effort 插入。
	ctx := context.Background()
	_, _ = pool.Exec(ctx, `INSERT INTO wallets (user_id) VALUES ($1)`, creatorID)

	cleanup := func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = pool.Exec(c, `DELETE FROM agents WHERE creator_id = $1`, creatorID)
		_, _ = pool.Exec(c, `DELETE FROM wallets WHERE user_id = $1`, creatorID)
		_, _ = pool.Exec(c, `DELETE FROM users WHERE id = $1`, creatorID)
	}
	return creatorID, cleanup
}

// ─────────────────────────────────────────────────────────
// AgentOpt - 函数选项模式构造测试 agent
// ─────────────────────────────────────────────────────────

// createAgentArgs 是构造测试 agent 时的所有可调字段。默认值见 createApprovedAgent。
type createAgentArgs struct {
	name              string
	description       string
	endpointURL       string
	endpointAuthHdr   *string
	pricePerCallCents int32
	tags              []string
	status            string // 默认 'approved'，可改为 pending/rejected/disabled 测过滤
}

// AgentOpt 函数选项。
type AgentOpt func(*createAgentArgs)

// WithName 覆盖默认 name。
func WithName(n string) AgentOpt {
	return func(a *createAgentArgs) { a.name = n }
}

// WithDescription 覆盖默认 description。
func WithDescription(d string) AgentOpt {
	return func(a *createAgentArgs) { a.description = d }
}

// WithTags 覆盖默认 tags（默认空数组）。
func WithTags(tags []string) AgentOpt {
	return func(a *createAgentArgs) { a.tags = tags }
}

// WithStatus 覆盖默认 status（默认 'approved'）。用于测过滤。
func WithStatus(s string) AgentOpt {
	return func(a *createAgentArgs) { a.status = s }
}

// WithAuthHeader 设置 endpoint_auth_header（默认 nil）。
func WithAuthHeader(h string) AgentOpt {
	return func(a *createAgentArgs) {
		hh := h
		a.endpointAuthHdr = &hh
	}
}

// WithPrice 覆盖默认 price_per_call_cents。
func WithPrice(cents int32) AgentOpt {
	return func(a *createAgentArgs) { a.pricePerCallCents = cents }
}

// WithEndpointURL 覆盖默认 endpoint_url（必须以 https:// 开头，由 DB 约束）。
func WithEndpointURL(u string) AgentOpt {
	return func(a *createAgentArgs) { a.endpointURL = u }
}

// createApprovedAgent 直接 SQL INSERT 一个 agent，默认 status='approved' 并设
// approved_at=NOW()。返回 agent ID（uuid）。
//
// status 默认 'approved'：若用 WithStatus 改成 pending/rejected/disabled，
// approved_at 会保留为 NULL（pending/rejected）或 NOW()（disabled，因为之前
// 通过过审）。这不影响市场查询测试，因为筛选条件只看 status。
func createApprovedAgent(t *testing.T, pool *pgxpool.Pool, creatorID uuid.UUID, slug string, opts ...AgentOpt) uuid.UUID {
	t.Helper()
	args := createAgentArgs{
		name:              "Test Agent " + slug,
		description:       "An agent for testing.",
		endpointURL:       "https://example.com/agent/" + slug,
		endpointAuthHdr:   nil,
		pricePerCallCents: 100,
		tags:              []string{},
		status:            "approved",
	}
	for _, o := range opts {
		o(&args)
	}
	ctx := context.Background()
	id := uuid.New()

	// 把旧 status 文案翻译成新三维字段，保持已有测试不必改写。
	lifecycle := "active"
	cert := "unreviewed"
	var certifiedAt *time.Time
	var rejectionReason *string
	switch args.status {
	case "approved":
		// 新建 Agent 即公开但未认证；certified_at 留 NULL
	case "pending":
		cert = "pending"
	case "rejected":
		cert = "rejected"
		r := "forced rejection"
		rejectionReason = &r
	case "disabled":
		lifecycle = "disabled"
	case "certified":
		cert = "certified"
		now := time.Now()
		certifiedAt = &now
	default:
		require.Failf(t, "createApprovedAgent unknown legacy status", "%q", args.status)
	}

	_, err := pool.Exec(ctx,
		`INSERT INTO agents (
			id, creator_id, slug, name, description, endpoint_url, endpoint_auth_header,
			price_per_call_cents, tags, lifecycle_status, visibility,
			certification_status, certified_at, rejection_reason
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'public', $11, $12, $13
		)`,
		id, creatorID, slug, args.name, args.description, args.endpointURL,
		args.endpointAuthHdr, args.pricePerCallCents, args.tags,
		lifecycle, cert, certifiedAt, rejectionReason,
	)
	require.NoError(t, err, "insert agent slug=%s status=%s", slug, args.status)
	return id
}
