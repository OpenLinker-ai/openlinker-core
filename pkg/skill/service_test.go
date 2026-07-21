package skill_test

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/OpenLinker-ai/openlinker-core/pkg/skill"
)

const truncateSkillTables = "TRUNCATE runtime_session_attachments, runtime_sessions, runtime_nodes, webhook_deliveries, runs, task_queries, agent_tokens, agent_availability_snapshots, agent_skills, agents, users RESTART IDENTITY CASCADE"

func setupSkillTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL 未设置，跳过 skill 集成测试")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	require.NoError(t, pool.Ping(ctx))
	_, err = pool.Exec(ctx, truncateSkillTables)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, _ = pool.Exec(cleanCtx, truncateSkillTables)
		pool.Close()
	})
	return pool
}

func insertSkillCreator(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO users (id, email, password_hash, display_name, is_creator, creator_verified)
		 VALUES ($1, $2, 'x', 'Skill Creator', TRUE, TRUE)`,
		id, "skill-c-"+id.String()[:8]+"@example.com")
	require.NoError(t, err)
	return id
}

func insertSkillAgent(t *testing.T, pool *pgxpool.Pool, creatorID uuid.UUID, slug, status string, totalCalls int32) uuid.UUID {
	t.Helper()
	id := uuid.New()
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
		require.Failf(t, "insertSkillAgent unknown legacy status", "%q", status)
	}
	_, err := pool.Exec(context.Background(),
		`INSERT INTO agents (
			id, creator_id, slug, name, description, endpoint_url, price_per_call_cents,
			tags, lifecycle_status, visibility, certification_status, total_calls
		) VALUES ($1, $2, $3, $4, 'Skill test agent', $5, 100, '{data}', $6, 'public', $7, $8)`,
		id, creatorID, slug, "Skill Agent "+slug, "https://example.com/agent/"+slug, lifecycle, cert, totalCalls)
	require.NoError(t, err)
	return id
}

func insertSkillRuntimePullAgent(t *testing.T, pool *pgxpool.Pool, creatorID uuid.UUID, slug string, totalCalls int32, lastUsedAt *time.Time) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO agents (
			id, creator_id, slug, name, description, endpoint_url, price_per_call_cents,
			tags, lifecycle_status, visibility, certification_status, total_calls, connection_mode
		) VALUES ($1, $2, $3, $4, 'Skill test runtime agent', $5, 100, '{data}', 'active', 'public', 'unreviewed', $6, 'runtime')`,
		id, creatorID, slug, "Skill Runtime "+slug, "openlinker-runtime://"+slug, totalCalls)
	require.NoError(t, err)
	_, err = pool.Exec(context.Background(),
		`INSERT INTO agent_tokens (
			id, agent_id, creator_user_id, name, prefix, token_hash, scopes, last_used_at, status, redeemed_at
		) VALUES ($1, $2, $3, 'Agent Token', $4, 'hash', ARRAY['agent:pull']::text[], $5, 'active_runtime', NOW())`,
		uuid.New(), id, creatorID, "ol_agent_"+uuid.NewString()[:8], lastUsedAt)
	require.NoError(t, err)
	return id
}

func insertSkillRuntimeSession(t *testing.T, pool *pgxpool.Pool, agentID uuid.UUID, heartbeatAt time.Time) {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()

	var credentialID uuid.UUID
	require.NoError(t, tx.QueryRow(ctx, `
SELECT id FROM agent_tokens
WHERE agent_id = $1 AND status = 'active_runtime'
ORDER BY created_at DESC LIMIT 1`, agentID).Scan(&credentialID))
	nodeID := uuid.New()
	sessionID := uuid.New()
	coreID := uuid.New()
	serial := strings.ReplaceAll(nodeID.String(), "-", "")
	_, err = tx.Exec(ctx, `
INSERT INTO runtime_nodes (
    node_id, display_name, device_certificate_serial,
    device_public_key_thumbprint, node_version, protocol_version,
    runtime_contract_id, runtime_contract_digest, features,
    capacity, inflight, status, last_seen_at
) VALUES ($1, 'Skill Runtime Node', $2, $3, 'skill-v2', 2,
          $4, $5, $6, 1, 0, 'active', $7)`,
		nodeID, serial, serial+serial, runtime.RuntimeContractID,
		runtime.RuntimeContractDigest, runtime.RuntimeRequiredFeatures(), heartbeatAt)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `
INSERT INTO runtime_sessions (
    runtime_session_id, node_id, agent_id, credential_id, worker_id,
    session_epoch, device_certificate_serial, node_version,
    protocol_version, runtime_contract_id, runtime_contract_digest,
    features, capacity, inflight, status, attached_core_instance_id,
    heartbeat_at
) VALUES ($1, $2, $3, $4, 'skill-worker', 1, $5, 'skill-v2',
          2, $6, $7, $8, 1, 0, 'active', $9, $10)`,
		sessionID, nodeID, agentID, credentialID, serial,
		runtime.RuntimeContractID, runtime.RuntimeContractDigest,
		runtime.RuntimeRequiredFeatures(), coreID, heartbeatAt)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `
INSERT INTO runtime_session_attachments (
    runtime_session_id, core_instance_id, attachment_kind
) VALUES ($1, $2, 'connected')`, sessionID, coreID)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))
}

func markSkillAgentAvailability(t *testing.T, pool *pgxpool.Pool, agentID uuid.UUID, status string) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO agent_availability_snapshots (
			agent_id, availability_status, last_checked_at, updated_at
		) VALUES ($1, $2, NOW(), NOW())
		ON CONFLICT (agent_id) DO UPDATE
		SET availability_status = EXCLUDED.availability_status,
		    last_checked_at = EXCLUDED.last_checked_at,
		    updated_at = EXCLUDED.updated_at`,
		agentID, status)
	require.NoError(t, err)
}

func assertSkillHTTPStatus(t *testing.T, err error, want int) {
	t.Helper()
	require.Error(t, err)
	var he *httpx.HTTPError
	require.True(t, errors.As(err, &he), "expected *httpx.HTTPError, got %T (%v)", err, err)
	assert.Equal(t, want, he.Status)
}

func TestSetListAndRecommendAgentSkills(t *testing.T) {
	pool := setupSkillTestDB(t)
	svc := skill.NewService(pool)
	creatorID := insertSkillCreator(t, pool)
	best := insertSkillAgent(t, pool, creatorID, "skill-best-"+uuid.NewString()[:8], "approved", 5)
	second := insertSkillAgent(t, pool, creatorID, "skill-second-"+uuid.NewString()[:8], "approved", 20)
	testingAgent := insertSkillAgent(t, pool, creatorID, "skill-testing-"+uuid.NewString()[:8], "approved", 500)
	// docs/29 缺口 2 后语义：certification_status='pending' 仍进推荐池；
	// 只有 disabled 会被过滤。换成 disabled 才能保留这个负面用例。
	pending := insertSkillAgent(t, pool, creatorID, "skill-disabled-"+uuid.NewString()[:8], "disabled", 100)
	ctx := context.Background()
	markSkillAgentAvailability(t, pool, best, "healthy")
	markSkillAgentAvailability(t, pool, second, "healthy")
	markSkillAgentAvailability(t, pool, testingAgent, "healthy")
	_, err := pool.Exec(ctx, `UPDATE agents SET tags = ARRAY['testing']::text[] WHERE id = $1`, testingAgent)
	require.NoError(t, err)

	require.NoError(t, svc.SetAgentSkills(ctx, best, []string{
		" data/sql-query ",
		"data/analysis",
		"data/sql-query",
		"",
	}))
	require.NoError(t, svc.SetAgentSkills(ctx, second, []string{"data/sql-query"}))
	require.NoError(t, svc.SetAgentSkills(ctx, testingAgent, []string{"data/sql-query", "data/analysis"}))
	require.NoError(t, svc.SetAgentSkills(ctx, pending, []string{"data/sql-query", "data/analysis"}))

	items, err := svc.ListForAgent(ctx, best)
	require.NoError(t, err)
	require.Len(t, items, 2)
	assert.Equal(t, "data/sql-query", items[0].ID)
	assert.Equal(t, "data/analysis", items[1].ID)

	matches, err := svc.RecommendAgentsBySkills(ctx, []string{"data/sql-query", "data/analysis"}, 10)
	require.NoError(t, err)
	require.Len(t, matches, 2)
	assert.Equal(t, best, matches[0].AgentID)
	assert.Equal(t, int32(2), matches[0].MatchCount)
	assert.Equal(t, second, matches[1].AgentID)
	assert.Equal(t, int32(1), matches[1].MatchCount)
	recommendedIDs := []uuid.UUID{matches[0].AgentID, matches[1].AgentID}
	assert.NotContains(t, recommendedIDs, testingAgent)

	limited, err := svc.RecommendAgentsBySkills(ctx, []string{"data/sql-query", "data/analysis"}, 1)
	require.NoError(t, err)
	require.Len(t, limited, 1)
	assert.Equal(t, best, limited[0].AgentID)
}

func TestRecommendAgentsBySkillsUsesDurableRuntimeSessionsAfterDatabaseHeartbeatReplacement(t *testing.T) {
	pool := setupSkillTestDB(t)
	svc := skill.NewService(pool)
	creatorID := insertSkillCreator(t, pool)
	ctx := context.Background()
	recentTokenUse := time.Now().Add(-time.Minute)

	readyRuntime := insertSkillRuntimePullAgent(t, pool, creatorID, "skill-runtime-ready-"+uuid.NewString()[:8], 100, nil)
	tokenOnlyRuntime := insertSkillRuntimePullAgent(t, pool, creatorID, "skill-runtime-token-only-"+uuid.NewString()[:8], 1000, &recentTokenUse)
	leaseBackedRuntime := insertSkillRuntimePullAgent(t, pool, creatorID, "skill-runtime-lease-backed-"+uuid.NewString()[:8], 500, &recentTokenUse)
	unreachableRuntime := insertSkillRuntimePullAgent(t, pool, creatorID, "skill-runtime-down-"+uuid.NewString()[:8], 2000, nil)
	betterDirect := insertSkillAgent(t, pool, creatorID, "skill-direct-better-"+uuid.NewString()[:8], "approved", 1)
	direct := insertSkillAgent(t, pool, creatorID, "skill-direct-"+uuid.NewString()[:8], "approved", 1)
	markSkillAgentAvailability(t, pool, unreachableRuntime, "unreachable")
	markSkillAgentAvailability(t, pool, betterDirect, "healthy")
	markSkillAgentAvailability(t, pool, direct, "healthy")
	insertSkillRuntimeSession(t, pool, readyRuntime, time.Now())
	insertSkillRuntimeSession(t, pool, leaseBackedRuntime, time.Now().Add(-time.Minute))
	insertSkillRuntimeSession(t, pool, unreachableRuntime, time.Now())

	require.NoError(t, svc.SetAgentSkills(ctx, betterDirect, []string{"data/sql-query", "data/analysis"}))
	require.NoError(t, svc.SetAgentSkills(ctx, readyRuntime, []string{"data/sql-query"}))
	require.NoError(t, svc.SetAgentSkills(ctx, tokenOnlyRuntime, []string{"data/sql-query"}))
	require.NoError(t, svc.SetAgentSkills(ctx, leaseBackedRuntime, []string{"data/sql-query"}))
	require.NoError(t, svc.SetAgentSkills(ctx, unreachableRuntime, []string{"data/sql-query"}))
	require.NoError(t, svc.SetAgentSkills(ctx, direct, []string{"data/sql-query"}))

	matches, err := svc.RecommendAgentsBySkills(ctx, []string{"data/sql-query", "data/analysis"}, 10)
	require.NoError(t, err)
	require.Len(t, matches, 4)
	assert.Equal(t, betterDirect, matches[0].AgentID)
	assert.Equal(t, int32(2), matches[0].MatchCount)
	assert.Equal(t, direct, matches[1].AgentID)
	assert.Equal(t, readyRuntime, matches[2].AgentID, "a current ready Session must qualify even when Agent Token last_used_at is NULL")
	assert.Equal(t, leaseBackedRuntime, matches[3].AgentID, "Redis lease and reaper own periodic liveness after database heartbeat replacement")
	recommendedIDs := []uuid.UUID{matches[0].AgentID, matches[1].AgentID, matches[2].AgentID, matches[3].AgentID}
	assert.NotContains(t, recommendedIDs, tokenOnlyRuntime, "recent Agent Token use is not online evidence")
	assert.NotContains(t, recommendedIDs, unreachableRuntime)
}

func TestRecommendAgentsBySkillsFiltersUncallableDirectAgents(t *testing.T) {
	pool := setupSkillTestDB(t)
	svc := skill.NewService(pool)
	creatorID := insertSkillCreator(t, pool)
	ctx := context.Background()

	callable := insertSkillAgent(t, pool, creatorID, "skill-callable-"+uuid.NewString()[:8], "approved", 10)
	unknown := insertSkillAgent(t, pool, creatorID, "skill-unknown-"+uuid.NewString()[:8], "approved", 100)
	unreachable := insertSkillAgent(t, pool, creatorID, "skill-unreachable-"+uuid.NewString()[:8], "approved", 1000)
	markSkillAgentAvailability(t, pool, callable, "healthy")
	markSkillAgentAvailability(t, pool, unreachable, "unreachable")

	require.NoError(t, svc.SetAgentSkills(ctx, callable, []string{"data/sql-query"}))
	require.NoError(t, svc.SetAgentSkills(ctx, unknown, []string{"data/sql-query"}))
	require.NoError(t, svc.SetAgentSkills(ctx, unreachable, []string{"data/sql-query"}))

	matches, err := svc.RecommendAgentsBySkills(ctx, []string{"data/sql-query"}, 10)
	require.NoError(t, err)
	require.Len(t, matches, 1)
	assert.Equal(t, callable, matches[0].AgentID)
}

func TestSetAgentSkillsValidation(t *testing.T) {
	pool := setupSkillTestDB(t)
	svc := skill.NewService(pool)
	creatorID := insertSkillCreator(t, pool)
	agentID := insertSkillAgent(t, pool, creatorID, "skill-validation-"+uuid.NewString()[:8], "approved", 0)
	ctx := context.Background()

	err := svc.SetAgentSkills(ctx, agentID, []string{"missing/not-real"})
	assertSkillHTTPStatus(t, err, http.StatusBadRequest)

	err = svc.SetAgentSkills(ctx, agentID, []string{
		"content/translation",
		"content/summarization",
		"content/copywriting",
		"content/proofreading",
		"content/structured-data",
		"data/sql-query",
	})
	assertSkillHTTPStatus(t, err, http.StatusBadRequest)
}

func TestListAllSkills(t *testing.T) {
	pool := setupSkillTestDB(t)
	items, err := skill.NewService(pool).ListAll(context.Background())
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(items), 30)
	assert.Equal(t, "ai/rag", items[0].ID)
}
