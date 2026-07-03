package skill

import (
	"context"
	"errors"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

const truncateBenchmarkSkillTables = "TRUNCATE agent_skill_benchmark_runs, agent_skill_scores, webhook_deliveries, user_tokens, wallets, runs, charges, withdrawals, task_queries, agent_tokens, agent_availability_snapshots, agent_skills, agents, users RESTART IDENTITY CASCADE"

func setupBenchmarkSkillDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL 未设置，跳过 skill benchmark 集成测试")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	require.NoError(t, pool.Ping(ctx))
	_, err = pool.Exec(ctx, truncateBenchmarkSkillTables)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, _ = pool.Exec(cleanCtx, truncateBenchmarkSkillTables)
		pool.Close()
	})
	return pool
}

func insertBenchmarkSkillCreator(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO users (id, email, password_hash, display_name, is_creator, creator_verified)
		 VALUES ($1, $2, 'x', 'Benchmark Creator', TRUE, TRUE)`,
		id, "bench-skill-"+id.String()[:8]+"@example.com")
	require.NoError(t, err)
	return id
}

func insertBenchmarkSkillAgent(t *testing.T, pool *pgxpool.Pool, creatorID uuid.UUID, slug string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO agents (
			id, creator_id, slug, name, description, endpoint_url, price_per_call_cents,
			tags, lifecycle_status, visibility, certification_status, total_calls
		) VALUES (
			$1, $2, $3, 'Benchmark Skill Agent', 'translation benchmark agent',
			'https://example.com/agent/benchmark', 100, ARRAY['content'],
			'active', 'public', 'unreviewed', 42
		)`,
		id, creatorID, slug)
	require.NoError(t, err)
	return id
}

func TestBenchmarkServiceRunBenchmarkClosedLoop(t *testing.T) {
	pool := setupBenchmarkSkillDB(t)
	parent := NewService(pool)
	benchLLM := &fakeBenchmarkLLM{response: `{"score":82,"reason":"output matched expected quality"}`}
	bench := NewBenchmarkService(parent, fakeEndpointRunner{}, benchLLM)
	ctx := context.Background()

	creatorID := insertBenchmarkSkillCreator(t, pool)
	slug := "bench-skill-" + uuid.NewString()[:8]
	agentID := insertBenchmarkSkillAgent(t, pool, creatorID, slug)
	require.NoError(t, parent.SetAgentSkills(ctx, agentID, []string{"content/translation"}))

	resp, err := bench.RunBenchmark(ctx, agentID, creatorID, "content/translation")
	require.NoError(t, err)
	require.Equal(t, "content/translation", resp.SkillID)
	require.Equal(t, "running", resp.Status)
	require.NotEmpty(t, resp.BatchID)
	batchID, err := uuid.Parse(resp.BatchID)
	require.NoError(t, err)

	var scores []SkillScoreItem
	require.Eventually(t, func() bool {
		var err error
		scores, err = bench.ListAgentScores(ctx, agentID)
		if err != nil || len(scores) != 1 || scores[0].LastBatchID == nil {
			return false
		}
		return scores[0].Status == BenchmarkStatusVerified && *scores[0].LastBatchID == resp.BatchID
	}, 10*time.Second, 50*time.Millisecond)
	require.Equal(t, int32(82), *scores[0].AverageScore)
	require.Equal(t, scores[0].TotalCount, scores[0].PassCount)

	require.NoError(t, bench.assertOwner(ctx, agentID, creatorID))
	requireBenchmarkSkillHTTPStatus(t, bench.assertOwner(ctx, agentID, uuid.New()), http.StatusNotFound)

	detail, err := bench.GetBatchDetail(ctx, agentID, creatorID, batchID)
	require.NoError(t, err)
	require.Equal(t, BenchmarkStatusVerified, detail.Status)
	require.Equal(t, int32(82), *detail.AverageScore)
	require.Len(t, detail.Items, int(scores[0].TotalCount))
	require.Equal(t, "success", detail.Items[0].Status)
	require.NotNil(t, detail.Items[0].JudgeReasoning)

	slugScores, err := bench.ListAgentScoresBySlug(ctx, slug)
	require.NoError(t, err)
	require.Len(t, slugScores, 1)
	require.Equal(t, BenchmarkStatusVerified, slugScores[0].Status)

	publicSummaries, err := bench.ListBatchSummariesPublic(ctx, agentID, 0)
	require.NoError(t, err)
	require.Len(t, publicSummaries, 1)
	require.Equal(t, resp.BatchID, publicSummaries[0].BatchID)
	require.Equal(t, "content/translation", publicSummaries[0].SkillID)

	publicDetail, err := bench.GetBatchDetailPublic(ctx, agentID, batchID)
	require.NoError(t, err)
	require.Equal(t, BenchmarkStatusVerified, publicDetail.Status)
	require.Len(t, publicDetail.Items, int(scores[0].TotalCount))
	require.Nil(t, publicDetail.Items[0].JudgeReasoning)

	topAgents, err := bench.ListTopAgents(ctx, "content/translation", 0)
	require.NoError(t, err)
	require.NotEmpty(t, topAgents)
	require.Equal(t, agentID.String(), topAgents[0].AgentID)
	require.Equal(t, slug, topAgents[0].Slug)
	require.Equal(t, int32(82), *topAgents[0].AverageScore)
}

func requireBenchmarkSkillHTTPStatus(t *testing.T, err error, status int) {
	t.Helper()
	require.Error(t, err)
	var httpErr *httpx.HTTPError
	require.Truef(t, errors.As(err, &httpErr), "expected *httpx.HTTPError, got %T (%v)", err, err)
	require.Equal(t, status, httpErr.Status)
}
