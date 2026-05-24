package task_test

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

	"github.com/kinzhi/openlinker-core/pkg/task"
	db "github.com/kinzhi/openlinker-core/pkg/db/generated"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

const truncateTaskTables = "TRUNCATE webhook_deliveries, wallets, runs, charges, withdrawals, task_queries, agent_skills, agents, users RESTART IDENTITY CASCADE"

type fakeSkillRecommender struct {
	skills      []db.Skill
	matches     []task.AgentMatch
	gotSkillIDs []string
}

func (f *fakeSkillRecommender) ListAll(context.Context) ([]db.Skill, error) {
	return append([]db.Skill{}, f.skills...), nil
}

func (f *fakeSkillRecommender) RecommendAgentsBySkills(_ context.Context, skillIDs []string, limit int) ([]task.AgentMatch, error) {
	f.gotSkillIDs = append([]string{}, skillIDs...)
	out := append([]task.AgentMatch{}, f.matches...)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func setupTaskTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL 未设置，跳过 task 集成测试")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	require.NoError(t, pool.Ping(ctx))
	_, err = pool.Exec(ctx, truncateTaskTables)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = pool.Exec(cleanCtx, truncateTaskTables)
		pool.Close()
	})
	return pool
}

func testSkills() []db.Skill {
	now := time.Now()
	return []db.Skill{
		{
			ID:          "data/sql-query",
			Category:    "data",
			Name:        "SQL 查询",
			Description: "自然语言转 SQL、慢查询优化、schema 解读",
			SortOrder:   1,
			CreatedAt:   now,
		},
		{
			ID:          "data/analysis",
			Category:    "data",
			Name:        "数据分析",
			Description: "统计、趋势、同比环比、生成洞察文字",
			SortOrder:   2,
			CreatedAt:   now,
		},
	}
}

func insertTaskUser(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO users (id, email, password_hash, display_name)
		 VALUES ($1, $2, 'x', 'Task User')`,
		id, "task-u-"+id.String()[:8]+"@example.com")
	require.NoError(t, err)
	return id
}

func insertTaskCreator(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO users (id, email, password_hash, display_name, is_creator, creator_verified)
		 VALUES ($1, $2, 'x', 'Task Creator', TRUE, TRUE)`,
		id, "task-c-"+id.String()[:8]+"@example.com")
	require.NoError(t, err)
	return id
}

func insertTaskAgent(t *testing.T, pool *pgxpool.Pool, creatorID uuid.UUID, slug, status string) uuid.UUID {
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
			tags, status, approved_at
		) VALUES ($1, $2, $3, $4, 'Task test agent', $5, 100, '{data}', $6, $7)`,
		id, creatorID, slug, "Task Agent "+slug, "https://example.com/agent/"+slug, status, approvedAt)
	require.NoError(t, err)
	return id
}

func assertTaskHTTPStatus(t *testing.T, err error, want int) {
	t.Helper()
	require.Error(t, err)
	var he *httpx.HTTPError
	require.True(t, errors.As(err, &he), "expected *httpx.HTTPError, got %T (%v)", err, err)
	assert.Equal(t, want, he.Status)
}

func TestRecommendPersistsAndDetailRoundTrip(t *testing.T) {
	pool := setupTaskTestDB(t)
	userID := insertTaskUser(t, pool)
	creatorID := insertTaskCreator(t, pool)
	firstAgent := insertTaskAgent(t, pool, creatorID, "task-first-"+uuid.NewString()[:8], "approved")
	secondAgent := insertTaskAgent(t, pool, creatorID, "task-second-"+uuid.NewString()[:8], "approved")

	fake := &fakeSkillRecommender{
		skills: testSkills(),
		matches: []task.AgentMatch{
			{AgentID: firstAgent, MatchCount: 2},
			{AgentID: secondAgent, MatchCount: 1},
		},
	}
	svc := task.NewService(pool, nil, fake)

	resp, err := svc.Recommend(context.Background(), userID, "请帮我做 SQL 查询和数据分析")
	require.NoError(t, err)
	require.NotEqual(t, uuid.Nil, resp.TaskID)
	assert.NotEmpty(t, fake.gotSkillIDs)
	require.Len(t, resp.Recommendations, 2)
	assert.Equal(t, firstAgent.String(), resp.Recommendations[0].Agent.ID)
	assert.Equal(t, float32(1), resp.Recommendations[0].MatchScore)
	assert.Equal(t, secondAgent.String(), resp.Recommendations[1].Agent.ID)
	assert.Contains(t, resp.Recommendations[0].Why, "SQL 查询")

	var stored []uuid.UUID
	err = pool.QueryRow(context.Background(),
		`SELECT recommended_agent_ids FROM task_queries WHERE id=$1`, resp.TaskID).Scan(&stored)
	require.NoError(t, err)
	assert.Equal(t, []uuid.UUID{firstAgent, secondAgent}, stored)

	detail, err := svc.GetByID(context.Background(), resp.TaskID, userID)
	require.NoError(t, err)
	require.Len(t, detail.Recommendations, 2)
	assert.Equal(t, firstAgent.String(), detail.Recommendations[0].Agent.ID)
	assert.Equal(t, secondAgent.String(), detail.Recommendations[1].Agent.ID)

	require.NoError(t, svc.Choose(context.Background(), resp.TaskID, userID, secondAgent))
	history, err := svc.ListMine(context.Background(), userID, 20)
	require.NoError(t, err)
	require.Len(t, history, 1)
	require.NotNil(t, history[0].ChosenAgentID)
	assert.Equal(t, secondAgent.String(), *history[0].ChosenAgentID)
}

func TestChooseRejectsUnrecommendedAndWrongOwner(t *testing.T) {
	pool := setupTaskTestDB(t)
	userID := insertTaskUser(t, pool)
	otherUserID := insertTaskUser(t, pool)
	creatorID := insertTaskCreator(t, pool)
	recommended := insertTaskAgent(t, pool, creatorID, "task-recommended-"+uuid.NewString()[:8], "approved")
	notRecommended := insertTaskAgent(t, pool, creatorID, "task-not-rec-"+uuid.NewString()[:8], "approved")

	var taskID uuid.UUID
	err := pool.QueryRow(context.Background(),
		`INSERT INTO task_queries (user_id, query, parsed_skills, recommended_agent_ids)
		 VALUES ($1, '做 SQL 查询', '{data/sql-query}', $2)
		 RETURNING id`,
		userID, []uuid.UUID{recommended}).Scan(&taskID)
	require.NoError(t, err)

	err = task.NewService(pool, nil, &fakeSkillRecommender{skills: testSkills()}).
		Choose(context.Background(), taskID, userID, notRecommended)
	assertTaskHTTPStatus(t, err, http.StatusBadRequest)

	err = task.NewService(pool, nil, &fakeSkillRecommender{skills: testSkills()}).
		Choose(context.Background(), taskID, otherUserID, recommended)
	assertTaskHTTPStatus(t, err, http.StatusNotFound)
}

func TestGetByIDSkipsDisabledHistoricalRecommendation(t *testing.T) {
	pool := setupTaskTestDB(t)
	userID := insertTaskUser(t, pool)
	creatorID := insertTaskCreator(t, pool)
	approvedAgent := insertTaskAgent(t, pool, creatorID, "task-live-"+uuid.NewString()[:8], "approved")
	disabledAgent := insertTaskAgent(t, pool, creatorID, "task-disabled-"+uuid.NewString()[:8], "disabled")

	var taskID uuid.UUID
	err := pool.QueryRow(context.Background(),
		`INSERT INTO task_queries (user_id, query, parsed_skills, recommended_agent_ids)
		 VALUES ($1, '做 SQL 查询', '{data/sql-query}', $2)
		 RETURNING id`,
		userID, []uuid.UUID{disabledAgent, approvedAgent}).Scan(&taskID)
	require.NoError(t, err)

	detail, err := task.NewService(pool, nil, &fakeSkillRecommender{skills: testSkills()}).
		GetByID(context.Background(), taskID, userID)
	require.NoError(t, err)
	require.Len(t, detail.Recommendations, 1)
	assert.Equal(t, approvedAgent.String(), detail.Recommendations[0].Agent.ID)
}
