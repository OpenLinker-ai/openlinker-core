package workflow_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/kinzhi/openlinker-core/pkg/config"
	runtimemod "github.com/kinzhi/openlinker-core/pkg/runtime"
	"github.com/kinzhi/openlinker-core/pkg/workflow"
)

func TestWorkflowRunExecutesAgentNodesAndPersistsChildRuns(t *testing.T) {
	pool := setupWorkflowTestDB(t)

	var mu sync.Mutex
	var inputs []map[string]interface{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req runtimemod.AgentRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))

		mu.Lock()
		inputs = append(inputs, req.Input)
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(map[string]interface{}{
			"output": map[string]interface{}{
				"step":          req.Input["node_key"],
				"previous_node": req.Input["previous_node"],
				"summary":       fmt.Sprintf("completed %v", req.Input["node_key"]),
			},
			"events": []map[string]interface{}{
				{
					"event_type": "run.message.delta",
					"payload": map[string]interface{}{
						"text": fmt.Sprintf("workflow node %v done", req.Input["node_key"]),
					},
				},
			},
		})
		require.NoError(t, err)
	}))
	t.Cleanup(server.Close)

	userID := insertWorkflowUser(t, pool, "wf-user")
	creatorID := insertWorkflowUser(t, pool, "wf-creator")
	agentID := insertWorkflowAgent(t, pool, creatorID, server.URL)

	runtimeSvc := runtimemod.NewService(pool, &config.Config{
		PlatformFeeRate:         0,
		RunTimeoutSeconds:       5,
		AllowLocalHTTPEndpoints: true,
	})
	svc := workflow.NewService(pool, runtimeSvc)

	created, err := svc.CreateWorkflow(context.Background(), userID, &workflow.CreateWorkflowRequest{
		Name:        "Two step workflow",
		Description: "executes two real agent runs",
		Nodes: []workflow.WorkflowNodeRequest{
			{Key: "extract", Title: "Extract", AgentID: agentID},
			{Key: "summarize", Title: "Summarize", AgentID: agentID},
		},
	})
	require.NoError(t, err)
	require.Len(t, created.Nodes, 2)

	run, err := svc.RunWorkflow(context.Background(), userID, uuid.MustParse(created.ID), &workflow.RunWorkflowRequest{
		Input: map[string]interface{}{"topic": "workflow validation"},
	})
	require.NoError(t, err)
	require.Equal(t, "success", run.Status)
	require.Len(t, run.Steps, 2)
	require.NotEmpty(t, run.Steps[0].RunID)
	require.NotEmpty(t, run.Steps[1].RunID)
	require.Equal(t, "extract", run.Steps[0].Output["step"])
	require.Equal(t, "summarize", run.Steps[1].Output["step"])
	require.Equal(t, "extract", run.Steps[1].Input["previous_node"])
	require.Equal(t, "summarize", run.Output["step"])

	mu.Lock()
	require.Len(t, inputs, 2)
	require.Equal(t, "extract", inputs[0]["node_key"])
	require.Equal(t, "summarize", inputs[1]["node_key"])
	require.Equal(t, "extract", inputs[1]["previous_node"])
	mu.Unlock()

	reloaded, err := svc.GetWorkflowRun(context.Background(), userID, uuid.MustParse(run.ID))
	require.NoError(t, err)
	require.Equal(t, "success", reloaded.Status)
	require.Len(t, reloaded.Steps, 2)

	listed, err := svc.ListWorkflows(context.Background(), userID, 20)
	require.NoError(t, err)
	require.Len(t, listed.Items, 1)
	require.Equal(t, int32(1), listed.Total)
	require.Equal(t, created.ID, listed.Items[0].ID)
	require.Len(t, listed.Items[0].Nodes, 2)

	var runCount int
	err = pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM runs WHERE user_id = $1 AND agent_id = $2`, userID, agentID).
		Scan(&runCount)
	require.NoError(t, err)
	require.Equal(t, 2, runCount)
}

func setupWorkflowTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL 未设置，跳过 workflow 集成测试")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	require.NoError(t, pool.Ping(ctx))
	truncateWorkflowTables(t, pool)
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = pool.Exec(c, `TRUNCATE workflows, runs, agents, users RESTART IDENTITY CASCADE`)
		pool.Close()
	})
	return pool
}

func truncateWorkflowTables(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`TRUNCATE workflows, runs, agents, users RESTART IDENTITY CASCADE`)
	require.NoError(t, err)
}

func insertWorkflowUser(t *testing.T, pool *pgxpool.Pool, prefix string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO users (id, email, password_hash, display_name, is_creator, creator_verified)
		 VALUES ($1, $2, $3, $4, TRUE, TRUE)`,
		id, prefix+"-"+id.String()[:8]+"@example.com", "x", prefix)
	require.NoError(t, err)
	return id
}

func insertWorkflowAgent(t *testing.T, pool *pgxpool.Pool, creatorID uuid.UUID, endpoint string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO agents (
			id, creator_id, slug, name, description, endpoint_url, endpoint_auth_header,
			price_per_call_cents, tags, lifecycle_status, visibility, certification_status
		) VALUES ($1, $2, $3, $4, $5, $6, NULL, 0, '{}', 'active', 'public', 'unreviewed')`,
		id, creatorID, "wf-agent-"+id.String()[:8], "Workflow Agent", "test workflow agent", endpoint)
	require.NoError(t, err)
	return id
}
