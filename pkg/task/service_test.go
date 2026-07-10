package task_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/OpenLinker-ai/openlinker-core/pkg/task"
)

const truncateTaskTables = "TRUNCATE webhook_deliveries, runs, task_queries, agent_skills, agents, users RESTART IDENTITY CASCADE"

type fakeSkillRecommender struct {
	skills      []db.Skill
	matches     []task.AgentMatch
	gotSkillIDs []string
	listErr     error
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
	if f.listErr != nil {
		return nil, f.listErr
	}
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
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	require.NoError(t, pool.Ping(ctx))
	_, err = pool.Exec(ctx, truncateTaskTables)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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
			ID:          "content/summarization",
			Category:    "content",
			Name:        "摘要",
			Description: "长文压缩、要点提取、会议纪要生成",
			SortOrder:   1,
			CreatedAt:   now,
		},
		{
			ID:          "content/structured-data",
			Category:    "content",
			Name:        "结构化抽取",
			Description: "从非结构化文本中抽取字段",
			SortOrder:   2,
			CreatedAt:   now,
		},
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
		{
			ID:          "dev/code-review",
			Category:    "dev",
			Name:        "代码审查",
			Description: "PR 评审、风格检查、潜在 bug 提示",
			SortOrder:   1,
			CreatedAt:   now,
		},
		{
			ID:          "ops/web-scraping",
			Category:    "ops",
			Name:        "网页抓取",
			Description: "抓取站点 / API / 监控 / 价格追踪",
			SortOrder:   1,
			CreatedAt:   now,
		},
		{
			ID:          "ops/document-generate",
			Category:    "ops",
			Name:        "文档生成",
			Description: "PDF / Word / 报告 / 合同 / 简历",
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

func decodeTaskHandlerJSON(t *testing.T, rec *httptest.ResponseRecorder, out any) {
	t.Helper()
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), out))
}

func newTaskHandlerContext(e *echo.Echo, method, path, body string, userID, taskID uuid.UUID) (echo.Context, *httptest.ResponseRecorder) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	}
	c := e.NewContext(req, rec)
	if userID != uuid.Nil {
		c.Set(string(httpx.CtxKeyUserID), userID.String())
	}
	if taskID != uuid.Nil {
		c.SetParamNames("id")
		c.SetParamValues(taskID.String())
	}
	return c, rec
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
	assert.Equal(t, "private", resp.Visibility)
	require.GreaterOrEqual(t, len(fake.gotSkillIDs), 2)
	assert.Equal(t, []string{"data/sql-query", "data/analysis"}, fake.gotSkillIDs[:2])
	require.Len(t, resp.ParsedSkillRefs, len(fake.gotSkillIDs))
	assert.Equal(t, "SQL 查询", resp.ParsedSkillRefs[0].Name)
	assert.Equal(t, []string{"create_task", "run_agent"}, resp.MCPTools)
	require.Len(t, resp.MCPToolRefs, 2)
	assert.Equal(t, "create_task", resp.MCPToolRefs[0].Name)
	require.Len(t, resp.Recommendations, 2)
	assert.Equal(t, firstAgent.String(), resp.Recommendations[0].Agent.ID)
	assert.InDelta(t, float32(2)/float32(len(fake.gotSkillIDs)), resp.Recommendations[0].MatchScore, 0.001)
	assert.Equal(t, []string{"data"}, resp.Recommendations[0].Agent.Tags)
	require.Len(t, resp.Recommendations[0].MatchedSkills, 2)
	assert.Equal(t, "data/sql-query", resp.Recommendations[0].MatchedSkills[0].ID)
	assert.Equal(t, "匹配 SQL 查询 + 数据分析", resp.Recommendations[0].Why)
	assert.Equal(t, secondAgent.String(), resp.Recommendations[1].Agent.ID)
	require.Len(t, resp.Recommendations[1].MatchedSkills, 1)
	assert.Equal(t, "data/sql-query", resp.Recommendations[1].MatchedSkills[0].ID)
	assert.Equal(t, "匹配 SQL 查询", resp.Recommendations[1].Why)
	assert.NotContains(t, resp.Recommendations[1].Why, "数据分析")

	var stored []uuid.UUID
	var storedMCP []string
	err = pool.QueryRow(context.Background(),
		`SELECT recommended_agent_ids, mcp_tools FROM task_queries WHERE id=$1`, resp.TaskID).Scan(&stored, &storedMCP)
	require.NoError(t, err)
	assert.Equal(t, []uuid.UUID{firstAgent, secondAgent}, stored)
	assert.Equal(t, []string{"create_task", "run_agent"}, storedMCP)

	detail, err := svc.GetByID(context.Background(), resp.TaskID, userID)
	require.NoError(t, err)
	require.Len(t, detail.ParsedSkillRefs, len(fake.gotSkillIDs))
	assert.Equal(t, []string{"create_task", "run_agent"}, detail.MCPTools)
	require.Len(t, detail.MCPToolRefs, 2)
	require.Len(t, detail.Recommendations, 2)
	assert.Equal(t, firstAgent.String(), detail.Recommendations[0].Agent.ID)
	require.Len(t, detail.Recommendations[0].MatchedSkills, 2)
	assert.Equal(t, "匹配 SQL 查询 + 数据分析", detail.Recommendations[0].Why)
	assert.Equal(t, "匹配 SQL 查询", detail.Recommendations[1].Why)
	assert.Equal(t, secondAgent.String(), detail.Recommendations[1].Agent.ID)
	require.Len(t, detail.Recommendations[1].MatchedSkills, 1)

	board, err := svc.ListBoard(context.Background(), 20)
	require.NoError(t, err)
	require.Empty(t, board, "recommend should create a private draft, not a public board task")

	published, err := svc.Publish(context.Background(), resp.TaskID, userID, &task.PublishRequest{
		PublicSummary: "公开 SQL 数据分析任务",
	})
	require.NoError(t, err)
	assert.Equal(t, "public", published.Visibility)
	require.NotNil(t, published.PublicSummary)
	assert.Equal(t, "公开 SQL 数据分析任务", *published.PublicSummary)

	board, err = svc.ListBoard(context.Background(), 20)
	require.NoError(t, err)
	require.Len(t, board, 1)
	assert.Equal(t, resp.TaskID.String(), board[0].ID)
	assert.Equal(t, "公开 SQL 数据分析任务", board[0].Query)
	assert.Equal(t, "公开 SQL 数据分析任务", board[0].PublicSummary)
	assert.Equal(t, "open", board[0].Status)
	assert.Equal(t, 2, board[0].RecommendedAgentCount)
	require.Len(t, board[0].ParsedSkillRefs, len(fake.gotSkillIDs))
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

func TestRecommendPersistsPendingExplicitSkill(t *testing.T) {
	pool := setupTaskTestDB(t)
	userID := insertTaskUser(t, pool)
	fake := &fakeSkillRecommender{skills: testSkills()}
	svc := task.NewService(pool, nil, fake)

	resp, err := svc.Recommend(context.Background(), userID, &task.RecommendRequest{
		Query:    "I need an Agent for a missing capability",
		SkillIDs: []string{"ai/custom-capability"},
		MCPTools: []string{"create_task", "run_agent"},
	})
	require.NoError(t, err)
	require.NotEqual(t, uuid.Nil, resp.TaskID)
	assert.Equal(t, []string{"ai/custom-capability"}, resp.ParsedSkills)
	require.Len(t, resp.ParsedSkillRefs, 1)
	assert.Equal(t, "ai/custom-capability", resp.ParsedSkillRefs[0].ID)
	assert.Equal(t, "ai/custom-capability", resp.ParsedSkillRefs[0].Name)
	assert.Empty(t, resp.Recommendations)
	require.NotNil(t, resp.NextAction)
	assert.Equal(t, "publish_task", resp.NextAction.Type)
	assert.Equal(t, "no_public_agent", resp.NextAction.ReasonCode)

	var parsed []string
	var recommended []uuid.UUID
	err = pool.QueryRow(context.Background(),
		`SELECT parsed_skills, recommended_agent_ids FROM task_queries WHERE id=$1`,
		resp.TaskID,
	).Scan(&parsed, &recommended)
	require.NoError(t, err)
	assert.Equal(t, []string{"ai/custom-capability"}, parsed)
	assert.Empty(t, recommended)

	detail, err := svc.GetByID(context.Background(), resp.TaskID, userID)
	require.NoError(t, err)
	assert.Equal(t, []string{"ai/custom-capability"}, detail.ParsedSkills)
	require.Len(t, detail.ParsedSkillRefs, 1)
	assert.Equal(t, "ai/custom-capability", detail.ParsedSkillRefs[0].Name)
	assert.Empty(t, detail.Recommendations)
}

func TestRecommendPendingExplicitSkillLimitsRecommendationScope(t *testing.T) {
	pool := setupTaskTestDB(t)
	userID := insertTaskUser(t, pool)
	fake := &fakeSkillRecommender{skills: testSkills()}
	svc := task.NewService(pool, nil, fake)

	resp, err := svc.Recommend(context.Background(), userID, &task.RecommendRequest{
		Query:    "请帮我做 SQL 查询和数据分析，但是需要一个当前目录没有的新能力",
		SkillIDs: []string{"ai/custom-capability"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"ai/custom-capability"}, fake.gotSkillIDs)
	assert.Contains(t, resp.ParsedSkills, "ai/custom-capability")
	assert.Contains(t, resp.ParsedSkills, "data/sql-query")
	assert.Contains(t, resp.ParsedSkills, "data/analysis")
	assert.Empty(t, resp.Recommendations)
	require.NotNil(t, resp.NextAction)
	assert.Equal(t, "publish_task", resp.NextAction.Type)
}

func TestRecommendPreferredAgentSlugRanksFirst(t *testing.T) {
	pool := setupTaskTestDB(t)
	userID := insertTaskUser(t, pool)
	creatorID := insertTaskCreator(t, pool)
	firstSlug := "task-auto-" + uuid.NewString()[:8]
	preferredSlug := "task-preferred-" + uuid.NewString()[:8]
	firstAgent := insertTaskAgent(t, pool, creatorID, firstSlug, "approved")
	preferredAgent := insertTaskAgent(t, pool, creatorID, preferredSlug, "approved")
	insertTaskAgentSkills(t, pool, firstAgent, "dev/code-review")
	insertTaskAgentSkills(t, pool, preferredAgent, "dev/code-review")

	fake := &fakeSkillRecommender{
		skills: testSkills(),
		matches: []task.AgentMatch{
			{AgentID: firstAgent, MatchCount: 1},
			{AgentID: preferredAgent, MatchCount: 1},
		},
	}
	svc := task.NewService(pool, nil, fake)

	resp, err := svc.Recommend(context.Background(), userID, &task.RecommendRequest{
		Query:      "请帮我审查这段代码有没有明显问题",
		SkillIDs:   []string{"dev/code-review"},
		AgentSlugs: []string{preferredSlug},
	})
	require.NoError(t, err)
	require.Len(t, resp.Recommendations, 2)
	assert.Equal(t, preferredAgent.String(), resp.Recommendations[0].Agent.ID)
	assert.Equal(t, preferredSlug, resp.Recommendations[0].Agent.Slug)
	assert.Equal(t, firstAgent.String(), resp.Recommendations[1].Agent.ID)
	require.Len(t, resp.Recommendations[0].MatchedSkills, 1)
	assert.Equal(t, "dev/code-review", resp.Recommendations[0].MatchedSkills[0].ID)

	var stored []uuid.UUID
	err = pool.QueryRow(context.Background(),
		`SELECT recommended_agent_ids FROM task_queries WHERE id=$1`, resp.TaskID).Scan(&stored)
	require.NoError(t, err)
	assert.Equal(t, []uuid.UUID{preferredAgent, firstAgent}, stored)
}

func TestTaskTemplatesAndTemplateIDDriveRecommendationSkills(t *testing.T) {
	pool := setupTaskTestDB(t)
	userID := insertTaskUser(t, pool)
	creatorID := insertTaskCreator(t, pool)
	agentID := insertTaskAgent(t, pool, creatorID, "task-support-"+uuid.NewString()[:8], "approved")
	insertTaskAgentSkills(t, pool, agentID, "content/summarization", "content/structured-data")

	fake := &fakeSkillRecommender{
		skills:  testSkills(),
		matches: []task.AgentMatch{{AgentID: agentID, MatchCount: 2}},
	}
	svc := task.NewService(pool, nil, fake)

	templates, err := svc.ListTaskTemplates(context.Background())
	require.NoError(t, err)
	require.Len(t, templates, 5)
	assert.Equal(t, "support-review", templates[0].ID)
	assert.Equal(t, "private", templates[0].DefaultVisibility)
	assert.Equal(t, []string{"content/summarization", "content/structured-data"}, templates[0].RequiredSkillIDs)
	assert.Equal(t, []string{"create_task", "run_agent", "get_run"}, templates[0].RequiredMCPTools)
	require.Len(t, templates[0].RequiredSkillRefs, 2)
	require.Len(t, templates[0].RequiredMCPToolRefs, 3)
	assert.Equal(t, "摘要", templates[0].RequiredSkillRefs[0].Name)
	assert.Equal(t, "create_task", templates[0].RequiredMCPToolRefs[0].Name)

	resp, err := svc.Recommend(context.Background(), userID, &task.RecommendRequest{
		TemplateID: "support-review",
		Query:      "请复盘这段客服对话，输出问题分类和下一步动作",
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(fake.gotSkillIDs), 2)
	assert.Equal(t, []string{"content/summarization", "content/structured-data"}, fake.gotSkillIDs[:2])
	require.Len(t, resp.Recommendations, 1)
	assert.Equal(t, agentID.String(), resp.Recommendations[0].Agent.ID)
	require.Len(t, resp.ParsedSkillRefs, len(fake.gotSkillIDs))
	assert.Equal(t, "结构化抽取", resp.ParsedSkillRefs[1].Name)
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
	_, err = svc.Claim(context.Background(), taskID, creatorID, agentID)
	assertTaskHTTPStatus(t, err, http.StatusConflict)

	published, err := svc.Publish(context.Background(), taskID, ownerID, &task.PublishRequest{
		PublicSummary: "SQL 统计分析公开任务",
	})
	require.NoError(t, err)
	assert.Equal(t, "public", published.Visibility)

	claimed, err := svc.Claim(context.Background(), taskID, creatorID, agentID)
	require.NoError(t, err)
	assert.Equal(t, taskID.String(), claimed.TaskID)
	assert.Equal(t, "in_progress", claimed.Status)
	assert.Equal(t, "SQL 统计分析公开任务", claimed.Query)
	require.NotNil(t, claimed.ClaimedAt)

	board, err := svc.ListBoard(context.Background(), 20)
	require.NoError(t, err)
	require.Len(t, board, 1)
	assert.Equal(t, "in_progress", board[0].Status)
	require.NotNil(t, board[0].ClaimedAgentID)
	assert.Equal(t, agentID.String(), *board[0].ClaimedAgentID)

	boardPage, err := svc.ListBoardPage(context.Background(), "SQL", "in_progress", "data/sql-query", "", "published_desc", 1, 10)
	require.NoError(t, err)
	require.Len(t, boardPage.Items, 1)
	assert.Equal(t, int32(1), boardPage.Total)
	assert.Equal(t, "in_progress", boardPage.StatusFilter)
	assert.Equal(t, "data/sql-query", boardPage.SkillFilter)
	assert.Equal(t, []string{"data/sql-query"}, boardPage.SkillIDsFilter)

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

	unpublished, err := svc.Unpublish(context.Background(), taskID, ownerID)
	require.NoError(t, err)
	assert.Equal(t, "private", unpublished.Visibility)
	assert.Equal(t, "accepted", unpublished.Status)
	assert.Nil(t, unpublished.PublicSummary)
	assert.Nil(t, unpublished.PublishedAt)
	require.NotNil(t, unpublished.AcceptedAt)

	boardAfterUnpublish, err := svc.ListBoard(context.Background(), 20)
	require.NoError(t, err)
	require.Empty(t, boardAfterUnpublish)

	again, err := svc.Unpublish(context.Background(), taskID, ownerID)
	require.NoError(t, err)
	assert.Equal(t, "private", again.Visibility)
}

func TestRecommendWithoutMatchesReturnsPrivateDraftNextAction(t *testing.T) {
	pool := setupTaskTestDB(t)
	userID := insertTaskUser(t, pool)
	svc := task.NewService(pool, nil, &fakeSkillRecommender{skills: testSkills()})

	resp, err := svc.Recommend(context.Background(), userID, &task.RecommendRequest{
		Query:    "请帮我做 SQL 查询和数据分析",
		SkillIDs: []string{"data/sql-query"},
	})
	require.NoError(t, err)
	require.NotEqual(t, uuid.Nil, resp.TaskID)
	assert.Equal(t, "private", resp.Visibility)
	require.Empty(t, resp.Recommendations)
	require.NotNil(t, resp.NextAction)
	assert.Equal(t, "publish_task", resp.NextAction.Type)
	assert.Contains(t, resp.NextAction.Href, resp.TaskID.String())

	board, err := svc.ListBoard(context.Background(), 20)
	require.NoError(t, err)
	require.Empty(t, board)
}

func TestPublicTaskBoundariesAndCatalogFallback(t *testing.T) {
	pool := setupTaskTestDB(t)
	ownerID := insertTaskUser(t, pool)
	otherUserID := insertTaskUser(t, pool)
	creatorID := insertTaskCreator(t, pool)
	agentID := insertTaskAgent(t, pool, creatorID, "task-public-"+uuid.NewString()[:8], "approved")
	insertTaskAgentSkills(t, pool, agentID, "data/sql-query")

	longQuery := strings.TrimSpace(strings.Repeat("SQL task boundary ", 20))
	var taskID uuid.UUID
	err := pool.QueryRow(context.Background(),
		`INSERT INTO task_queries (user_id, query, parsed_skills, mcp_tools, recommended_agent_ids)
		 VALUES ($1, $2, $3, $4, $5)
		 RETURNING id`,
		ownerID, longQuery, []string{"missing/skill", "data/sql-query"}, []string{"run_agent", "create_task"}, []uuid.UUID{agentID}).Scan(&taskID)
	require.NoError(t, err)

	svc := task.NewService(pool, nil, &fakeSkillRecommender{listErr: errors.New("catalog offline")})
	published, err := svc.Publish(context.Background(), taskID, ownerID, nil)
	require.NoError(t, err)
	require.NotNil(t, published.PublicSummary)
	assert.Equal(t, longQuery, published.Query)
	assert.Len(t, []rune(*published.PublicSummary), 240)
	assert.Equal(t, longQuery[:240], *published.PublicSummary)
	require.Len(t, published.ParsedSkillRefs, 2)
	assert.Equal(t, "missing/skill", published.ParsedSkillRefs[0].Name)
	assert.Equal(t, "data/sql-query", published.ParsedSkillRefs[1].Name)
	require.Len(t, published.Recommendations, 1)
	assert.Equal(t, agentID.String(), published.Recommendations[0].Agent.ID)

	republished, err := svc.Publish(context.Background(), taskID, ownerID, &task.PublishRequest{
		PublicSummary: "do not replace an already public task",
	})
	require.NoError(t, err)
	require.NotNil(t, republished.PublicSummary)
	assert.Equal(t, *published.PublicSummary, *republished.PublicSummary)

	_, err = svc.Publish(context.Background(), taskID, otherUserID, &task.PublishRequest{PublicSummary: "wrong owner"})
	assertTaskHTTPStatus(t, err, http.StatusNotFound)

	_, err = svc.GetByID(context.Background(), taskID, otherUserID)
	assertTaskHTTPStatus(t, err, http.StatusNotFound)
	_, err = svc.GetByID(context.Background(), uuid.New(), ownerID)
	assertTaskHTTPStatus(t, err, http.StatusNotFound)

	board, err := svc.ListBoard(context.Background(), 0)
	require.NoError(t, err)
	require.Len(t, board, 1)
	assert.Equal(t, taskID.String(), board[0].ID)
	assert.Equal(t, *published.PublicSummary, board[0].Query)
	assert.Equal(t, "open", board[0].Status)
	assert.Equal(t, "pending", board[0].DeliveryStatus)
	require.Len(t, board[0].ParsedSkillRefs, 2)
	assert.Equal(t, "missing/skill", board[0].ParsedSkillRefs[0].Name)
	assert.Equal(t, "data/sql-query", board[0].ParsedSkillRefs[1].Name)
	require.Len(t, board[0].MCPToolRefs, 2)
	assert.Equal(t, "run_agent", board[0].MCPToolRefs[0].Name)

	boardPage, err := svc.ListBoardPage(context.Background(), "SQL", "open", "data/sql-query", "run_agent", "recommended_desc", 1, 10)
	require.NoError(t, err)
	require.Len(t, boardPage.Items, 1)
	assert.Equal(t, int32(1), boardPage.Total)
	assert.Equal(t, "recommended_desc", boardPage.Sort)
	assert.Equal(t, "run_agent", boardPage.MCPFilter)
	assert.Equal(t, []string{"data/sql-query"}, boardPage.SkillIDsFilter)

	multiSkillBoardPage, err := svc.ListBoardPage(context.Background(), "SQL", "open", "missing/other,data/sql-query", "", "published_desc", 1, 10)
	require.NoError(t, err)
	require.Len(t, multiSkillBoardPage.Items, 1)
	assert.Equal(t, int32(1), multiSkillBoardPage.Total)
	assert.Equal(t, "missing/other,data/sql-query", multiSkillBoardPage.SkillFilter)
	assert.Equal(t, []string{"missing/other", "data/sql-query"}, multiSkillBoardPage.SkillIDsFilter)

	privateQuerySearch, err := svc.ListBoardPage(context.Background(), "SQL task boundary", "", "", "", "", 1, 10)
	require.NoError(t, err)
	require.Empty(t, privateQuerySearch.Items)
	assert.Equal(t, int32(0), privateQuerySearch.Total)

	history, err := svc.ListMine(context.Background(), ownerID, 0)
	require.NoError(t, err)
	require.NotEmpty(t, history)
	assert.Equal(t, taskID.String(), history[0].ID)
	assert.Equal(t, "public", history[0].Visibility)

	var completedTaskID uuid.UUID
	err = pool.QueryRow(context.Background(),
		`INSERT INTO task_queries (
			user_id, query, parsed_skills, recommended_agent_ids,
			claimed_agent_id, claimed_by_user_id, claimed_at,
			completed_at, completion_summary, delivery_status
		) VALUES (
			$1, 'completed task cannot publish', '{}', $2,
			$3, $4, NOW(),
			NOW(), 'done', 'submitted'
		) RETURNING id`,
		ownerID, []uuid.UUID{agentID}, agentID, creatorID).Scan(&completedTaskID)
	require.NoError(t, err)

	_, err = svc.Publish(context.Background(), completedTaskID, ownerID, &task.PublishRequest{
		PublicSummary: "completed task cannot publish",
	})
	assertTaskHTTPStatus(t, err, http.StatusConflict)
}

func TestTaskHandlersListBoardAndMineSuccess(t *testing.T) {
	pool := setupTaskTestDB(t)
	ownerID := insertTaskUser(t, pool)
	otherID := insertTaskUser(t, pool)
	creatorID := insertTaskCreator(t, pool)
	agentID := insertTaskAgent(t, pool, creatorID, "task-handler-"+uuid.NewString()[:8], "approved")
	insertTaskAgentSkills(t, pool, agentID, "data/sql-query")

	ctx := context.Background()
	publicSummary := "公开任务广场摘要"
	_, err := pool.Exec(ctx,
		`INSERT INTO task_queries (
			user_id, query, parsed_skills, mcp_tools, recommended_agent_ids,
			visibility, public_summary, published_at
		) VALUES ($1, 'private board source', '{data/sql-query}', '{run_agent}', $2, 'public', $3, NOW())`,
		ownerID, []uuid.UUID{agentID}, publicSummary)
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`INSERT INTO task_queries (
			user_id, query, parsed_skills, mcp_tools, recommended_agent_ids
		) VALUES ($1, 'owner private task', '{data/sql-query}', '{create_task}', $2)`,
		ownerID, []uuid.UUID{agentID})
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`INSERT INTO task_queries (
			user_id, query, parsed_skills, mcp_tools, recommended_agent_ids
		) VALUES ($1, 'other private task', '{data/sql-query}', '{}', $2)`,
		otherID, []uuid.UUID{agentID})
	require.NoError(t, err)

	svc := task.NewService(pool, nil, &fakeSkillRecommender{skills: testSkills()})
	h := task.NewHandler(svc)
	e := echo.New()

	boardRec := httptest.NewRecorder()
	boardCtx := e.NewContext(httptest.NewRequest(http.MethodGet, "/api/v1/tasks/board?limit=99", nil), boardRec)
	require.NoError(t, h.ListBoard(boardCtx))
	require.Equal(t, http.StatusOK, boardRec.Code)
	var boardBody struct {
		Items []task.PublicTaskItem `json:"items"`
		Total int32                 `json:"total"`
		Page  int32                 `json:"page"`
		Size  int32                 `json:"size"`
	}
	decodeTaskHandlerJSON(t, boardRec, &boardBody)
	require.Len(t, boardBody.Items, 1)
	assert.Equal(t, int32(1), boardBody.Total)
	assert.Equal(t, int32(1), boardBody.Page)
	assert.Equal(t, int32(50), boardBody.Size)
	assert.Equal(t, publicSummary, boardBody.Items[0].Query)
	assert.Equal(t, "open", boardBody.Items[0].Status)
	require.Len(t, boardBody.Items[0].ParsedSkillRefs, 1)
	assert.Equal(t, "SQL 查询", boardBody.Items[0].ParsedSkillRefs[0].Name)

	privateSearchRec := httptest.NewRecorder()
	privateSearchCtx := e.NewContext(httptest.NewRequest(http.MethodGet, "/api/v1/tasks/board?q=private+board+source", nil), privateSearchRec)
	require.NoError(t, h.ListBoard(privateSearchCtx))
	require.Equal(t, http.StatusOK, privateSearchRec.Code)
	var privateSearchBody struct {
		Items []task.PublicTaskItem `json:"items"`
		Total int32                 `json:"total"`
	}
	decodeTaskHandlerJSON(t, privateSearchRec, &privateSearchBody)
	require.Empty(t, privateSearchBody.Items)
	assert.Equal(t, int32(0), privateSearchBody.Total)

	mineRec := httptest.NewRecorder()
	mineCtx := e.NewContext(httptest.NewRequest(http.MethodGet, "/api/v1/tasks/me?limit=99", nil), mineRec)
	mineCtx.Set(string(httpx.CtxKeyUserID), ownerID.String())
	require.NoError(t, h.ListMine(mineCtx))
	require.Equal(t, http.StatusOK, mineRec.Code)
	var mineBody struct {
		Items []task.HistoryItem `json:"items"`
	}
	decodeTaskHandlerJSON(t, mineRec, &mineBody)
	require.Len(t, mineBody.Items, 2)
	queries := map[string]bool{}
	for _, item := range mineBody.Items {
		queries[item.Query] = true
		assert.NotEqual(t, "other private task", item.Query)
	}
	assert.True(t, queries["private board source"])
	assert.True(t, queries["owner private task"])
}

func TestTaskHandlersWorkLifecycleSuccess(t *testing.T) {
	pool := setupTaskTestDB(t)
	ownerID := insertTaskUser(t, pool)
	creatorID := insertTaskCreator(t, pool)
	agentID := insertTaskAgent(t, pool, creatorID, "task-handler-flow-"+uuid.NewString()[:8], "approved")
	insertTaskAgentSkills(t, pool, agentID, "data/sql-query")

	var taskID uuid.UUID
	err := pool.QueryRow(context.Background(),
		`INSERT INTO task_queries (user_id, query, parsed_skills, mcp_tools, recommended_agent_ids)
		 VALUES ($1, '请分析订单 SQL 趋势', '{data/sql-query}', '{run_agent}', $2)
		 RETURNING id`,
		ownerID, []uuid.UUID{agentID}).Scan(&taskID)
	require.NoError(t, err)

	runner := &fakeRuntimeStarter{}
	svc := task.NewService(pool, nil, &fakeSkillRecommender{skills: testSkills()})
	svc.SetRunStarter(runner)
	h := task.NewHandler(svc)
	e := echo.New()

	c, rec := newTaskHandlerContext(e, http.MethodPost, "/api/v1/tasks/"+taskID.String()+"/publish",
		`{"public_summary":"公开 handler 工作流任务"}`, ownerID, taskID)
	require.NoError(t, h.Publish(c))
	require.Equal(t, http.StatusOK, rec.Code)
	var published task.DetailResponse
	decodeTaskHandlerJSON(t, rec, &published)
	assert.Equal(t, "public", published.Visibility)
	require.NotNil(t, published.PublicSummary)
	assert.Equal(t, "公开 handler 工作流任务", *published.PublicSummary)

	c, rec = newTaskHandlerContext(e, http.MethodPost, "/api/v1/tasks/"+taskID.String()+"/claim",
		`{"agent_id":"`+agentID.String()+`"}`, creatorID, taskID)
	require.NoError(t, h.Claim(c))
	require.Equal(t, http.StatusOK, rec.Code)
	var claimed task.WorkResponse
	decodeTaskHandlerJSON(t, rec, &claimed)
	assert.Equal(t, "in_progress", claimed.Status)
	assert.Equal(t, agentID.String(), claimed.AgentID)

	c, rec = newTaskHandlerContext(e, http.MethodPost, "/api/v1/tasks/"+taskID.String()+"/run",
		`{"agent_id":"`+agentID.String()+`","input":{"text":"按公开任务执行 SQL 分析"}}`, creatorID, taskID)
	require.NoError(t, h.Run(c))
	require.Equal(t, http.StatusAccepted, rec.Code)
	var runResp task.RunTaskResponse
	decodeTaskHandlerJSON(t, rec, &runResp)
	assert.Equal(t, taskID.String(), runResp.TaskID)
	assert.Equal(t, "in_progress", runResp.Status)
	require.NotNil(t, runner.gotReq)
	assert.Equal(t, "按公开任务执行 SQL 分析", runner.gotReq.Input["text"])

	firstRunID := insertSuccessfulTaskRun(t, pool, creatorID, agentID, "handler 分析完成")
	c, rec = newTaskHandlerContext(e, http.MethodPost, "/api/v1/tasks/"+taskID.String()+"/complete",
		`{"agent_id":"`+agentID.String()+`","run_id":"`+firstRunID.String()+`","result_summary":"handler 分析完成","delivery_visibility":"shared","result_artifact":{"summary":"handler 分析完成"}}`,
		creatorID, taskID)
	require.NoError(t, h.Complete(c))
	require.Equal(t, http.StatusOK, rec.Code)
	var completed task.WorkResponse
	decodeTaskHandlerJSON(t, rec, &completed)
	assert.Equal(t, "completed", completed.Status)
	assert.Equal(t, "submitted", completed.DeliveryStatus)
	assert.Equal(t, "shared", completed.DeliveryVisibility)

	c, rec = newTaskHandlerContext(e, http.MethodPost, "/api/v1/tasks/"+taskID.String()+"/revision",
		`{"note":"请补充 SQL 口径"}`, ownerID, taskID)
	require.NoError(t, h.RequestRevision(c))
	require.Equal(t, http.StatusOK, rec.Code)
	var revision task.WorkResponse
	decodeTaskHandlerJSON(t, rec, &revision)
	assert.Equal(t, "revision_requested", revision.Status)
	require.NotNil(t, revision.RevisionNote)
	assert.Equal(t, "请补充 SQL 口径", *revision.RevisionNote)

	secondRunID := insertSuccessfulTaskRun(t, pool, creatorID, agentID, "handler 补充完成")
	c, rec = newTaskHandlerContext(e, http.MethodPost, "/api/v1/tasks/"+taskID.String()+"/complete",
		`{"agent_id":"`+agentID.String()+`","run_id":"`+secondRunID.String()+`","result_summary":"handler 补充完成"}`,
		creatorID, taskID)
	require.NoError(t, h.Complete(c))
	require.Equal(t, http.StatusOK, rec.Code)

	c, rec = newTaskHandlerContext(e, http.MethodPost, "/api/v1/tasks/"+taskID.String()+"/accept", "", ownerID, taskID)
	require.NoError(t, h.Accept(c))
	require.Equal(t, http.StatusOK, rec.Code)
	var accepted task.WorkResponse
	decodeTaskHandlerJSON(t, rec, &accepted)
	assert.Equal(t, "accepted", accepted.Status)
	assert.Equal(t, "accepted", accepted.DeliveryStatus)

	c, rec = newTaskHandlerContext(e, http.MethodPost, "/api/v1/tasks/"+taskID.String()+"/unpublish", "", ownerID, taskID)
	require.NoError(t, h.Unpublish(c))
	require.Equal(t, http.StatusOK, rec.Code)
	var unpublished task.DetailResponse
	decodeTaskHandlerJSON(t, rec, &unpublished)
	assert.Equal(t, "private", unpublished.Visibility)
	assert.Equal(t, "accepted", unpublished.Status)

	boardRec := httptest.NewRecorder()
	boardCtx := e.NewContext(httptest.NewRequest(http.MethodGet, "/api/v1/tasks/board?limit=50", nil), boardRec)
	require.NoError(t, h.ListBoard(boardCtx))
	require.Equal(t, http.StatusOK, boardRec.Code)
	var boardBody struct {
		Items []task.PublicTaskItem `json:"items"`
		Total int32                 `json:"total"`
	}
	decodeTaskHandlerJSON(t, boardRec, &boardBody)
	require.Empty(t, boardBody.Items)
	assert.Equal(t, int32(0), boardBody.Total)

	c, rec = newTaskHandlerContext(e, http.MethodGet, "/api/v1/tasks/"+taskID.String(), "", ownerID, taskID)
	require.NoError(t, h.GetByID(c))
	require.Equal(t, http.StatusOK, rec.Code)
	var detail task.DetailResponse
	decodeTaskHandlerJSON(t, rec, &detail)
	assert.Equal(t, "accepted", detail.Status)
	assert.Equal(t, "accepted", detail.DeliveryStatus)
	require.NotNil(t, detail.CompletionSummary)
	assert.Equal(t, "handler 补充完成", *detail.CompletionSummary)
}

func TestRunTaskUsesSelectedAgentAndTaskInput(t *testing.T) {
	pool := setupTaskTestDB(t)
	userID := insertTaskUser(t, pool)
	creatorID := insertTaskCreator(t, pool)
	agentID := insertTaskAgent(t, pool, creatorID, "task-run-"+uuid.NewString()[:8], "approved")

	var taskID uuid.UUID
	err := pool.QueryRow(context.Background(),
		`INSERT INTO task_queries (
			user_id, query, parsed_skills, mcp_tools, recommended_agent_ids, chosen_agent_id, chosen_at
		) VALUES (
			$1, '做 SQL 查询', '{data/sql-query}', '{run_agent}', $2, $3, NOW()
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
	assert.Equal(t, []string{"run_agent"}, runner.gotReq.Metadata["used_mcp_tools"])
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
