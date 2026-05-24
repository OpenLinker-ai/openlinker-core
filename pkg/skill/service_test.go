package skill_test

import (
	"context"
	"errors"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kinzhi/openlinker-core/pkg/skill"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

const truncateSkillTables = "TRUNCATE webhook_deliveries, api_keys, wallets, runs, charges, withdrawals, task_queries, agent_skills, agents, users RESTART IDENTITY CASCADE"

func setupSkillTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL 未设置，跳过 skill 集成测试")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	require.NoError(t, pool.Ping(ctx))
	_, err = pool.Exec(ctx, truncateSkillTables)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
	var approvedAt *time.Time
	if status == "approved" || status == "disabled" {
		now := time.Now()
		approvedAt = &now
	}
	_, err := pool.Exec(context.Background(),
		`INSERT INTO agents (
			id, creator_id, slug, name, description, endpoint_url, price_per_call_cents,
			tags, status, approved_at, total_calls
		) VALUES ($1, $2, $3, $4, 'Skill test agent', $5, 100, '{data}', $6, $7, $8)`,
		id, creatorID, slug, "Skill Agent "+slug, "https://example.com/agent/"+slug, status, approvedAt, totalCalls)
	require.NoError(t, err)
	return id
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
	pending := insertSkillAgent(t, pool, creatorID, "skill-pending-"+uuid.NewString()[:8], "pending", 100)
	ctx := context.Background()

	require.NoError(t, svc.SetAgentSkills(ctx, best, []string{
		" data/sql-query ",
		"data/analysis",
		"data/sql-query",
		"",
	}))
	require.NoError(t, svc.SetAgentSkills(ctx, second, []string{"data/sql-query"}))
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

	limited, err := svc.RecommendAgentsBySkills(ctx, []string{"data/sql-query", "data/analysis"}, 1)
	require.NoError(t, err)
	require.Len(t, limited, 1)
	assert.Equal(t, best, limited[0].AgentID)
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
