package mcp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/OpenLinker-ai/openlinker-core/pkg/task"
)

const truncateMCPServiceTables = "TRUNCATE run_requirement_evidence, run_artifact_chunks, run_artifacts, run_messages, run_events, task_callback_deliveries, runs, task_queries, agent_tokens, agent_availability_snapshots, agent_skills, agents, users RESTART IDENTITY CASCADE"

func TestServiceBridgesMarketRuntimeAndRunReads(t *testing.T) {
	pool := setupMCPServiceTestDB(t)
	ctx := context.Background()

	var received runtime.AgentRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		require.NoError(t, json.NewDecoder(r.Body).Decode(&received))
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(map[string]interface{}{
			"output": map[string]interface{}{
				"answer": "mcp runtime ok",
			},
		}))
	}))
	t.Cleanup(server.Close)

	userID := insertMCPUser(t, pool, "mcp-user", false)
	creatorID := insertMCPUser(t, pool, "mcp-creator", true)
	agentID := insertMCPAgent(t, pool, creatorID, "mcp-bridge-agent", "MCP Bridge Agent", server.URL, []string{"data", "sql"})
	insertMCPAgentSkills(t, pool, agentID, "data/sql-query")

	runtimeSvc := runtime.NewService(pool, &config.Config{RunTimeoutSeconds: 5, AllowLocalHTTPEndpoints: true})
	runtimeSvc.ConfigureCoreRuntime(uuid.New())
	svc := NewService(
		agent.NewMarketService(pool),
		runtimeSvc,
		nil,
	)

	defaultSearch, err := svc.SearchAgents(ctx, &SearchAgentsRequest{})
	require.NoError(t, err)
	require.Equal(t, int32(10), defaultSearch.Size)
	require.Equal(t, int32(1), defaultSearch.Total)

	filtered, err := svc.SearchAgents(ctx, &SearchAgentsRequest{Query: "bridge", Tags: []string{"sql"}, SkillIDs: []string{"data/sql-query"}, Limit: 99})
	require.NoError(t, err)
	require.Equal(t, int32(50), filtered.Size)
	require.Len(t, filtered.Items, 1)
	require.Equal(t, "mcp-bridge-agent", filtered.Items[0].Slug)
	require.Len(t, filtered.Items[0].Skills, 1)
	require.Equal(t, "data/sql-query", filtered.Items[0].Skills[0].ID)

	detail, err := svc.GetAgent(ctx, &GetAgentRequest{Slug: "mcp-bridge-agent"})
	require.NoError(t, err)
	require.Equal(t, agentID.String(), detail.ID)
	require.Equal(t, "MCP Bridge Agent", detail.Name)

	run, err := svc.RunAgent(ctx, userID, &RunAgentRequest{
		AgentID:        agentID.String(),
		Input:          map[string]interface{}{"prompt": "hello from mcp"},
		IdempotencyKey: "mcp-service-bridge-1",
		Metadata: map[string]interface{}{
			"trace":          "mcp-service-test",
			"used_mcp_tools": []interface{}{"search_agents"},
		},
	})
	require.NoError(t, err)
	require.Equal(t, "success", run.Status)
	require.Equal(t, "mcp runtime ok", run.Output["answer"])
	require.Equal(t, "hello from mcp", received.Input["prompt"])
	require.Equal(t, "mcp-service-test", received.Metadata["trace"])
	require.Equal(t, []interface{}{"search_agents", "run_agent"}, received.Metadata["used_mcp_tools"])

	runID := uuid.MustParse(run.RunID)
	gotRun, err := svc.GetRun(ctx, userID, runID)
	require.NoError(t, err)
	require.Equal(t, run.RunID, gotRun.RunID)
	require.Equal(t, "success", gotRun.Status)
	require.Equal(t, "mcp", gotRun.Source)
	require.Equal(t, "mcp runtime ok", gotRun.Output["answer"])
}

func TestServiceRunAgentPropagatesStableCreationIdentity(t *testing.T) {
	runner := &fakeMCPRuntimeRunner{
		response: &runtime.RunResponse{RunID: uuid.NewString(), Status: "success"},
	}
	svc := &Service{runtime: runner}
	userID := uuid.New()

	resp, err := svc.RunAgent(context.Background(), userID, &RunAgentRequest{
		AgentID:        uuid.NewString(),
		Input:          map[string]interface{}{"text": "hello"},
		Metadata:       map[string]interface{}{"trace": "kept"},
		IdempotencyKey: "mcp-call-2026-07-11-1",
	})
	require.NoError(t, err)
	require.Same(t, runner.response, resp)
	require.Equal(t, userID, runner.userID)
	require.Equal(t, "mcp", runner.source)
	require.NotNil(t, runner.request)
	require.Equal(t, "mcp-call-2026-07-11-1", runner.request.IdempotencyKey)
	require.Equal(t, "mcp", runner.request.CreationProtocol)
	require.Equal(t, "run_agent", runner.request.CreationMethod)
	require.Equal(t, "kept", runner.request.Metadata["trace"])
	require.Equal(t, []string{"run_agent"}, runner.request.Metadata["used_mcp_tools"])
}

func TestServiceRunAgentRejectsUnsafeIdempotencyKeyBeforeRuntime(t *testing.T) {
	runner := &fakeMCPRuntimeRunner{}
	svc := &Service{runtime: runner}

	_, err := svc.RunAgent(context.Background(), uuid.New(), &RunAgentRequest{
		AgentID:        uuid.NewString(),
		Input:          map[string]interface{}{"text": "hello"},
		IdempotencyKey: "unsafe\nkey",
	})
	require.Error(t, err)
	require.Nil(t, runner.request)

	var httpErr *httpx.HTTPError
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, http.StatusUnprocessableEntity, httpErr.Status)
	require.Equal(t, httpx.ErrorCode(runtime.IdempotencyErrorKeyInvalid), httpErr.Code)
	require.NotContains(t, httpErr.Message, "unsafe")
}

type fakeMCPRuntimeRunner struct {
	userID   uuid.UUID
	request  *runtime.RunRequest
	source   string
	response *runtime.RunResponse
}

func (f *fakeMCPRuntimeRunner) Run(_ context.Context, userID uuid.UUID, req *runtime.RunRequest, source string) (*runtime.RunResponse, error) {
	f.userID = userID
	f.request = req
	f.source = source
	return f.response, nil
}

func (f *fakeMCPRuntimeRunner) GetRun(context.Context, uuid.UUID, uuid.UUID) (*runtime.RunResponse, error) {
	return f.response, nil
}

func TestServiceCreateTaskBridgesRecommendations(t *testing.T) {
	pool := setupMCPServiceTestDB(t)
	ctx := context.Background()

	userID := insertMCPUser(t, pool, "mcp-task-user", false)
	creatorID := insertMCPUser(t, pool, "mcp-task-creator", true)
	agentID := insertMCPAgent(t, pool, creatorID, "mcp-task-agent", "MCP Task Agent", "https://example.com/task-agent", []string{"data"})
	insertMCPAgentSkills(t, pool, agentID, "data/sql-query")

	skills := []db.Skill{{
		ID:          "data/sql-query",
		Category:    "data",
		Name:        "SQL 查询",
		Description: "SQL 查询和数据分析",
		SortOrder:   1,
		CreatedAt:   time.Now(),
	}}
	recommender := &fakeMCPSkillRecommender{
		skills:  skills,
		matches: []task.AgentMatch{{AgentID: agentID, MatchCount: 1}},
	}
	taskSvc := task.NewService(pool, nil, recommender)
	svc := NewService(nil, nil, taskSvc)

	resp, err := svc.CreateTask(ctx, userID, &CreateTaskRequest{
		Query:    "please run a SQL query over customer data",
		SkillIDs: []string{"data/sql-query"},
		MCPTools: []string{"search_agents", "run_agent"},
	})
	require.NoError(t, err)
	require.NotEqual(t, uuid.Nil, resp.TaskID)
	require.Equal(t, []string{"data/sql-query"}, recommender.gotSkillIDs)
	require.Equal(t, []string{"data/sql-query"}, resp.ParsedSkills)
	require.Equal(t, []string{"search_agents", "run_agent"}, resp.MCPTools)
	require.Len(t, resp.Recommendations, 1)
	require.Equal(t, agentID.String(), resp.Recommendations[0].Agent.ID)
	require.Equal(t, "mcp-task-agent", resp.Recommendations[0].Agent.Slug)
	require.Len(t, resp.Recommendations[0].MatchedSkills, 1)
	require.Equal(t, "data/sql-query", resp.Recommendations[0].MatchedSkills[0].ID)

	var storedTools []string
	err = pool.QueryRow(ctx, `SELECT mcp_tools FROM task_queries WHERE id=$1`, resp.TaskID).Scan(&storedTools)
	require.NoError(t, err)
	require.Equal(t, []string{"search_agents", "run_agent"}, storedTools)
}

func setupMCPServiceTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL 未设置，跳过 mcp service 集成测试")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	require.NoError(t, pool.Ping(ctx))
	_, err = pool.Exec(ctx, truncateMCPServiceTables)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
UPDATE runtime_cluster_control
SET mode = 'normal', expected_replicas = 1, reopened_at = clock_timestamp(),
    version = version + 1, updated_at = clock_timestamp()
WHERE singleton_id = 1`)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanCancel()
		_, _ = pool.Exec(cleanCtx, truncateMCPServiceTables)
		_, _ = pool.Exec(cleanCtx, `
UPDATE runtime_cluster_control
SET mode = 'hard_maintenance', expected_replicas = 1,
    hard_maintenance_at = clock_timestamp(), reopened_at = NULL,
    version = version + 1, updated_at = clock_timestamp()
WHERE singleton_id = 1`)
		pool.Close()
	})
	return pool
}

func insertMCPUser(t *testing.T, pool *pgxpool.Pool, prefix string, creator bool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO users (id, email, password_hash, display_name, is_creator, creator_verified)
		 VALUES ($1, $2, 'x', $3, $4, $4)`,
		id,
		prefix+"-"+id.String()[:8]+"@example.com",
		prefix,
		creator,
	)
	require.NoError(t, err)
	return id
}

func insertMCPAgent(t *testing.T, pool *pgxpool.Pool, creatorID uuid.UUID, slug, name, endpoint string, tags []string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO agents (
			id, creator_id, slug, name, description, endpoint_url, endpoint_auth_header,
			price_per_call_cents, tags, lifecycle_status, visibility, certification_status
		) VALUES ($1, $2, $3, $4, 'MCP service test agent', $5, NULL, 0, $6, 'active', 'public', 'unreviewed')`,
		id,
		creatorID,
		slug,
		name,
		endpoint,
		tags,
	)
	require.NoError(t, err)
	return id
}

func insertMCPAgentSkills(t *testing.T, pool *pgxpool.Pool, agentID uuid.UUID, skillIDs ...string) {
	t.Helper()
	for _, skillID := range skillIDs {
		_, err := pool.Exec(context.Background(),
			`INSERT INTO agent_skills (agent_id, skill_id) VALUES ($1, $2)`,
			agentID,
			skillID,
		)
		require.NoError(t, err)
	}
}

type fakeMCPSkillRecommender struct {
	skills      []db.Skill
	matches     []task.AgentMatch
	gotSkillIDs []string
}

func (f *fakeMCPSkillRecommender) ListAll(context.Context) ([]db.Skill, error) {
	return append([]db.Skill{}, f.skills...), nil
}

func (f *fakeMCPSkillRecommender) RecommendAgentsBySkills(_ context.Context, skillIDs []string, limit int) ([]task.AgentMatch, error) {
	f.gotSkillIDs = append([]string{}, skillIDs...)
	out := append([]task.AgentMatch{}, f.matches...)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
