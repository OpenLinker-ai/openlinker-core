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

	db "github.com/kinzhi/openlinker-core/pkg/db/generated"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
	"github.com/kinzhi/openlinker-core/pkg/runtime"
	"github.com/kinzhi/openlinker-core/pkg/task"
)

const truncateTaskTables = "TRUNCATE webhook_deliveries, wallets, runs, charges, withdrawals, task_queries, agent_skills, agents, users RESTART IDENTITY CASCADE"

type fakeSkillRecommender struct {
	skills      []db.Skill
	matches     []task.AgentMatch
	gotSkillIDs []string
}

type fakeRuntimeStarter struct {
	gotUserID uuid.UUID
	gotReq    *runtime.RunRequest
	resp      *runtime.RunResponse
}

func (f *fakeRuntimeStarter) StartRun(_ context.Context, userID uuid.UUID, req *runtime.RunRequest, source string) (*runtime.RunResponse, error) {
	f.gotUserID = userID
	f.gotReq = req
	if f.resp != nil {
		return f.resp, nil
	}
	return &runtime.RunResponse{RunID: uuid.NewString(), Status: "running", Source: source}, nil
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
		require.Failf(t, "insertTaskAgent unknown legacy status", "%q", status)
	}
	_, err := pool.Exec(context.Background(),
		`INSERT INTO agents (
			id, creator_id, slug, name, description, endpoint_url, price_per_call_cents,
			tags, lifecycle_status, visibility, certification_status
		) VALUES ($1, $2, $3, $4, 'Task test agent', $5, 100, '{data}', $6, 'public', $7)`,
		id, creatorID, slug, "Task Agent "+slug, "https://example.com/agent/"+slug, lifecycle, cert)
	require.NoError(t, err)
	return id
}

func insertTaskAgentSkills(t *testing.T, pool *pgxpool.Pool, agentID uuid.UUID, skillIDs ...string) {
	t.Helper()
	for _, skillID := range skillIDs {
		_, err := pool.Exec(context.Background(),
			`INSERT INTO agent_skills (agent_id, skill_id) VALUES ($1, $2)`,
			agentID, skillID)
		require.NoError(t, err)
	}
}

func insertSuccessfulTaskRun(t *testing.T, pool *pgxpool.Pool, userID, agentID uuid.UUID, summary string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO runs (
			id, user_id, agent_id, input, output, status,
			cost_cents, platform_fee_cents, creator_revenue_cents,
			duration_ms, source, finished_at
		) VALUES (
			$1, $2, $3, '{"text":"task"}'::jsonb, $4::jsonb, 'success',
			0, 0, 0, 12, 'web', NOW()
		)`,
		id, userID, agentID, `{"summary": "`+summary+`"}`)
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
	insertTaskAgentSkills(t, pool, firstAgent, "data/sql-query", "data/analysis")
	insertTaskAgentSkills(t, pool, secondAgent, "data/sql-query")

	fake := &fakeSkillRecommender{
		skills: testSkills(),
		matches: []task.AgentMatch{
			{AgentID: firstAgent, MatchCount: 2},
			{AgentID: secondAgent, MatchCount: 1},
		},
	}
	svc := task.NewService(pool, nil, fake)

	resp, err := svc.Recommend(context.Background(), userID, &task.RecommendRequest{
		Query:    "请帮我做 SQL 查询和数据分析",
		SkillIDs: []string{"data/sql-query"},
		MCPTools: []string{"create_task", "run_agent"},
	})
	require.NoError(t, err)
	require.NotEqual(t, uuid.Nil, resp.TaskID)
	assert.Equal(t, []string{"data/sql-query", "data/analysis"}, fake.gotSkillIDs)
	require.Len(t, resp.ParsedSkillRefs, 2)
	assert.Equal(t, "SQL 查询", resp.ParsedSkillRefs[0].Name)
	assert.Equal(t, []string{"create_task", "run_agent"}, resp.MCPTools)
	require.Len(t, resp.MCPToolRefs, 2)
	assert.Equal(t, "create_task", resp.MCPToolRefs[0].Name)
	require.Len(t, resp.Recommendations, 2)
	assert.Equal(t, firstAgent.String(), resp.Recommendations[0].Agent.ID)
	assert.Equal(t, float32(1), resp.Recommendations[0].MatchScore)
	assert.Equal(t, []string{"data"}, resp.Recommendations[0].Agent.Tags)
	require.Len(t, resp.Recommendations[0].MatchedSkills, 2)
	assert.Equal(t, "data/sql-query", resp.Recommendations[0].MatchedSkills[0].ID)
	assert.Equal(t, secondAgent.String(), resp.Recommendations[1].Agent.ID)
	require.Len(t, resp.Recommendations[1].MatchedSkills, 1)
	assert.Equal(t, "data/sql-query", resp.Recommendations[1].MatchedSkills[0].ID)
	assert.Contains(t, resp.Recommendations[0].Why, "SQL 查询")

	var stored []uuid.UUID
	var storedMCP []string
	err = pool.QueryRow(context.Background(),
		`SELECT recommended_agent_ids, mcp_tools FROM task_queries WHERE id=$1`, resp.TaskID).Scan(&stored, &storedMCP)
	require.NoError(t, err)
	assert.Equal(t, []uuid.UUID{firstAgent, secondAgent}, stored)
	assert.Equal(t, []string{"create_task", "run_agent"}, storedMCP)

	detail, err := svc.GetByID(context.Background(), resp.TaskID, userID)
	require.NoError(t, err)
	require.Len(t, detail.ParsedSkillRefs, 2)
	assert.Equal(t, []string{"create_task", "run_agent"}, detail.MCPTools)
	require.Len(t, detail.MCPToolRefs, 2)
	require.Len(t, detail.Recommendations, 2)
	assert.Equal(t, firstAgent.String(), detail.Recommendations[0].Agent.ID)
	require.Len(t, detail.Recommendations[0].MatchedSkills, 2)
	assert.Equal(t, secondAgent.String(), detail.Recommendations[1].Agent.ID)
	require.Len(t, detail.Recommendations[1].MatchedSkills, 1)

	board, err := svc.ListBoard(context.Background(), 20)
	require.NoError(t, err)
	require.Len(t, board, 1)
	assert.Equal(t, resp.TaskID.String(), board[0].ID)
	assert.Equal(t, "open", board[0].Status)
	assert.Equal(t, 2, board[0].RecommendedAgentCount)
	require.Len(t, board[0].ParsedSkillRefs, 2)
	assert.Equal(t, "数据分析", board[0].ParsedSkillRefs[1].Name)
	assert.Equal(t, []string{"create_task", "run_agent"}, board[0].MCPTools)
	require.Len(t, board[0].MCPToolRefs, 2)

	require.NoError(t, svc.Choose(context.Background(), resp.TaskID, userID, secondAgent))
	history, err := svc.ListMine(context.Background(), userID, 20)
	require.NoError(t, err)
	require.Len(t, history, 1)
	require.NotNil(t, history[0].ChosenAgentID)
	assert.Equal(t, secondAgent.String(), *history[0].ChosenAgentID)
	assert.Equal(t, "matched", history[0].Status)
	assert.Equal(t, []string{"create_task", "run_agent"}, history[0].MCPTools)
}

func TestTaskBoardClaimAndCompleteRoundTrip(t *testing.T) {
	pool := setupTaskTestDB(t)
	ownerID := insertTaskUser(t, pool)
	creatorID := insertTaskCreator(t, pool)
	agentID := insertTaskAgent(t, pool, creatorID, "task-worker-"+uuid.NewString()[:8], "approved")
	insertTaskAgentSkills(t, pool, agentID, "data/sql-query")

	var taskID uuid.UUID
	err := pool.QueryRow(context.Background(),
		`INSERT INTO task_queries (user_id, query, parsed_skills, recommended_agent_ids)
		 VALUES ($1, '帮我做 SQL 统计分析', '{data/sql-query}', '{}')
		 RETURNING id`,
		ownerID).Scan(&taskID)
	require.NoError(t, err)

	svc := task.NewService(pool, nil, &fakeSkillRecommender{skills: testSkills()})
	claimed, err := svc.Claim(context.Background(), taskID, creatorID, agentID)
	require.NoError(t, err)
	assert.Equal(t, taskID.String(), claimed.TaskID)
	assert.Equal(t, "in_progress", claimed.Status)
	require.NotNil(t, claimed.ClaimedAt)

	board, err := svc.ListBoard(context.Background(), 20)
	require.NoError(t, err)
	require.Len(t, board, 1)
	assert.Equal(t, "in_progress", board[0].Status)
	require.NotNil(t, board[0].ClaimedAgentID)
	assert.Equal(t, agentID.String(), *board[0].ClaimedAgentID)

	runID := insertSuccessfulTaskRun(t, pool, creatorID, agentID, "分析完成")
	completed, err := svc.Complete(context.Background(), taskID, creatorID, &task.CompleteRequest{
		AgentID:       agentID,
		RunID:         runID,
		ResultSummary: "分析完成",
		ResultArtifact: map[string]interface{}{
			"summary": "分析完成",
			"rows":    3,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "completed", completed.Status)
	assert.Equal(t, "submitted", completed.DeliveryStatus)
	assert.Equal(t, "private", completed.DeliveryVisibility)
	require.NotNil(t, completed.CompletionRunID)
	assert.Equal(t, runID.String(), *completed.CompletionRunID)

	detail, err := svc.GetByID(context.Background(), taskID, ownerID)
	require.NoError(t, err)
	assert.Equal(t, "completed", detail.Status)
	assert.Equal(t, "submitted", detail.DeliveryStatus)
	assert.Equal(t, "private", detail.DeliveryVisibility)
	require.NotNil(t, detail.CompletionSummary)
	assert.Equal(t, "分析完成", *detail.CompletionSummary)
	require.NotNil(t, detail.DeliveryArtifact)
	assert.Equal(t, "分析完成", detail.DeliveryArtifact["summary"])

	_, err = svc.Claim(context.Background(), taskID, creatorID, agentID)
	assertTaskHTTPStatus(t, err, http.StatusConflict)

	revision, err := svc.RequestRevision(context.Background(), taskID, ownerID, &task.RevisionRequest{
		Note: "请补充样本量和 SQL 口径",
	})
	require.NoError(t, err)
	assert.Equal(t, "revision_requested", revision.Status)
	assert.Equal(t, "revision_requested", revision.DeliveryStatus)
	require.NotNil(t, revision.RevisionNote)
	assert.Equal(t, "请补充样本量和 SQL 口径", *revision.RevisionNote)

	runner := &fakeRuntimeStarter{}
	svc.SetRunStarter(runner)
	revisionRun, err := svc.RunTask(context.Background(), taskID, creatorID, &task.RunTaskRequest{
		AgentID: agentID,
		Input: map[string]interface{}{
			"text": "按返修要求补充样本量和 SQL 口径",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "revision_requested", revisionRun.Status)
	require.NotNil(t, runner.gotReq)
	assert.Equal(t, taskID.String(), runner.gotReq.Metadata["task_id"])
	assert.Equal(t, "按返修要求补充样本量和 SQL 口径", runner.gotReq.Input["text"])

	secondRunID := insertSuccessfulTaskRun(t, pool, creatorID, agentID, "补充完成")
	resubmitted, err := svc.Complete(context.Background(), taskID, creatorID, &task.CompleteRequest{
		AgentID:       agentID,
		RunID:         secondRunID,
		ResultSummary: "补充完成",
	})
	require.NoError(t, err)
	assert.Equal(t, "completed", resubmitted.Status)
	assert.Equal(t, "submitted", resubmitted.DeliveryStatus)
	assert.Nil(t, resubmitted.RevisionNote)

	accepted, err := svc.AcceptDelivery(context.Background(), taskID, ownerID)
	require.NoError(t, err)
	assert.Equal(t, "accepted", accepted.Status)
	assert.Equal(t, "accepted", accepted.DeliveryStatus)
	require.NotNil(t, accepted.AcceptedAt)
}

func TestRunTaskUsesSelectedAgentAndTaskInput(t *testing.T) {
	pool := setupTaskTestDB(t)
	userID := insertTaskUser(t, pool)
	creatorID := insertTaskCreator(t, pool)
	agentID := insertTaskAgent(t, pool, creatorID, "task-run-"+uuid.NewString()[:8], "approved")

	var taskID uuid.UUID
	err := pool.QueryRow(context.Background(),
		`INSERT INTO task_queries (
			user_id, query, parsed_skills, recommended_agent_ids, chosen_agent_id, chosen_at
		) VALUES (
			$1, '做 SQL 查询', '{data/sql-query}', $2, $3, NOW()
		) RETURNING id`,
		userID, []uuid.UUID{agentID}, agentID).Scan(&taskID)
	require.NoError(t, err)

	runner := &fakeRuntimeStarter{}
	svc := task.NewService(pool, nil, &fakeSkillRecommender{skills: testSkills()})
	svc.SetRunStarter(runner)
	resp, err := svc.RunTask(context.Background(), taskID, userID, &task.RunTaskRequest{
		AgentID: agentID,
	})
	require.NoError(t, err)
	assert.Equal(t, taskID.String(), resp.TaskID)
	assert.Equal(t, "matched", resp.Status)
	require.NotNil(t, runner.gotReq)
	assert.Equal(t, agentID.String(), runner.gotReq.AgentID)
	assert.Equal(t, "做 SQL 查询", runner.gotReq.Input["text"])
	assert.Equal(t, taskID.String(), runner.gotReq.Metadata["task_id"])
	assert.Equal(t, userID, runner.gotUserID)
}

func TestRecommendRejectsUnknownAssociations(t *testing.T) {
	pool := setupTaskTestDB(t)
	userID := insertTaskUser(t, pool)
	svc := task.NewService(pool, nil, &fakeSkillRecommender{skills: testSkills()})

	_, err := svc.Recommend(context.Background(), userID, &task.RecommendRequest{
		Query:    "请帮我做 SQL 查询和数据分析",
		SkillIDs: []string{"missing/skill"},
	})
	assertTaskHTTPStatus(t, err, http.StatusUnprocessableEntity)

	_, err = svc.Recommend(context.Background(), userID, &task.RecommendRequest{
		Query:    "请帮我做 SQL 查询和数据分析",
		MCPTools: []string{"unknown_tool"},
	})
	assertTaskHTTPStatus(t, err, http.StatusUnprocessableEntity)
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
