package workflow_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	runtimemod "github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/OpenLinker-ai/openlinker-core/pkg/workflow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestWorkflowSavedEmptyEdgesExecuteConcurrently(t *testing.T) {
	pool := setupWorkflowTestDB(t)
	var arrived atomic.Int32
	both := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if arrived.Add(1) == 2 {
			close(both)
		}
		select {
		case <-both:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"output": map[string]any{"overlap": 2}})
		case <-time.After(2 * time.Second):
			http.Error(w, "independent sibling never started", http.StatusGatewayTimeout)
		}
	}))
	t.Cleanup(server.Close)
	user := insertWorkflowUser(t, pool, "parallel-owner")
	agent := insertWorkflowAgent(t, pool, user, server.URL)
	runtimeSvc := runtimemod.NewService(pool, newWorkflowRuntimeTestConfig(5, true))
	runtimeSvc.ConfigureCoreRuntime(uuid.New())
	svc := workflow.NewService(pool, runtimeSvc)
	created, err := svc.CreateWorkflow(context.Background(), user, &workflow.CreateWorkflowRequest{
		Name: "Independent nodes", Nodes: []workflow.WorkflowNodeRequest{{Key: "a", AgentID: agent}, {Key: "b", AgentID: agent}},
		Edges: []map[string]interface{}{},
	})
	require.NoError(t, err)
	require.NotNil(t, created.Edges)
	require.Empty(t, created.Edges)
	run, err := svc.RunWorkflow(context.Background(), user, uuid.MustParse(created.ID), &workflow.RunWorkflowRequest{})
	require.NoError(t, err)
	require.Equal(t, "success", run.Status)
	require.Len(t, run.Steps, 2)
	for _, step := range run.Steps {
		require.Equal(t, "success", step.Status)
		require.Nil(t, step.Input["previous_node"])
	}
}
