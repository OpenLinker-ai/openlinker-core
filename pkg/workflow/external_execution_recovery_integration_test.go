package workflow_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	runtimemod "github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/OpenLinker-ai/openlinker-core/pkg/workflow"
)

const externalExecutionMaxAttempts int32 = 3

func TestResolveExternalExecutionWorkflowRunSurvivesTargetRetirement(t *testing.T) {
	pool := setupWorkflowTestDB(t)
	targetOwnerID := insertWorkflowUser(t, pool, "external-recovery-owner")
	actorUserID := insertWorkflowUser(t, pool, "external-recovery-actor")
	replacementOwnerID := insertWorkflowUser(t, pool, "external-recovery-replacement")
	agentID := insertWorkflowAgent(t, pool, targetOwnerID, "https://agent.example/run")

	runtimeSvc := runtimemod.NewService(pool, newWorkflowRuntimeTestConfig(5, false))
	runtimeSvc.ConfigureCoreRuntime(uuid.New())
	svc := workflow.NewService(pool, runtimeSvc)
	created, err := svc.CreateWorkflow(context.Background(), targetOwnerID, &workflow.CreateWorkflowRequest{
		Name:  "External recovery target",
		Nodes: []workflow.WorkflowNodeRequest{{Key: "run", AgentID: agentID}},
	})
	require.NoError(t, err)
	workflowID := uuid.MustParse(created.ID)
	externalRequestID := uuid.New()
	input := map[string]any{"topic": "committed workflow"}

	committed, err := svc.StartExternalExecutionWorkflowRun(
		context.Background(),
		"openlinker-cloud",
		targetOwnerID,
		actorUserID,
		workflowID,
		externalRequestID,
		input,
	)
	require.NoError(t, err)
	require.Equal(t, "pending", committed.Status)

	// Reassignment plus archival makes the original owner/target lookup return
	// not_found, while the immutable execution row remains recoverable.
	_, err = pool.Exec(context.Background(), `
		UPDATE workflows
		SET user_id = $2, status = 'archived'
		WHERE id = $1`, workflowID, replacementOwnerID)
	require.NoError(t, err)
	validation, err := svc.ValidateExternalExecutionTarget(context.Background(), targetOwnerID, workflowID)
	require.NoError(t, err)
	require.False(t, validation.Executable)
	require.Equal(t, "not_found", validation.UnavailableReason)

	startReplay, err := svc.StartExternalExecutionWorkflowRun(
		context.Background(),
		"openlinker-cloud",
		targetOwnerID,
		actorUserID,
		workflowID,
		externalRequestID,
		map[string]any{"topic": "committed workflow"},
	)
	require.NoError(t, err)
	require.Equal(t, committed.ID, startReplay.ID)

	replayed, found, err := svc.LookupExternalExecutionWorkflowRun(
		context.Background(),
		"openlinker-cloud",
		actorUserID,
		workflowID,
		externalRequestID,
		map[string]any{"topic": "committed workflow"},
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, committed.ID, replayed.ID)
	require.Equal(t, committed.WorkflowID, replayed.WorkflowID)
	require.Equal(t, committed.Input, replayed.Input)

	tests := []struct {
		name       string
		actorID    uuid.UUID
		workflowID uuid.UUID
		input      map[string]any
	}{
		{name: "actor", actorID: uuid.New(), workflowID: workflowID, input: input},
		{name: "workflow", actorID: actorUserID, workflowID: uuid.New(), input: input},
		{name: "input", actorID: actorUserID, workflowID: workflowID, input: map[string]any{"topic": "different"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, found, err := svc.LookupExternalExecutionWorkflowRun(
				context.Background(),
				"openlinker-cloud",
				tt.actorID,
				tt.workflowID,
				externalRequestID,
				tt.input,
			)
			require.Nil(t, resp)
			require.False(t, found)
			var httpErr *httpx.HTTPError
			require.ErrorAs(t, err, &httpErr)
			require.Equal(t, http.StatusConflict, httpErr.Status)
		})
	}

	_, err = pool.Exec(context.Background(), `UPDATE workflow_runs SET max_attempts = $2 WHERE id = $1`, uuid.MustParse(committed.ID), externalExecutionMaxAttempts+1)
	require.NoError(t, err)
	resp, found, err := svc.LookupExternalExecutionWorkflowRun(
		context.Background(),
		"openlinker-cloud",
		actorUserID,
		workflowID,
		externalRequestID,
		input,
	)
	require.Nil(t, resp)
	require.False(t, found)
	var httpErr *httpx.HTTPError
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, http.StatusConflict, httpErr.Status)
}

func TestResolveExternalExecutionWorkflowRunMissingIdentityHasNoSideEffects(t *testing.T) {
	pool := setupWorkflowTestDB(t)
	svc := workflow.NewService(pool, nil)
	actorUserID := insertWorkflowUser(t, pool, "external-recovery-missing")

	var runsBefore int
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM workflow_runs`).Scan(&runsBefore))
	resp, found, err := svc.LookupExternalExecutionWorkflowRun(
		context.Background(),
		"openlinker-cloud",
		actorUserID,
		uuid.New(), // deliberately absent from the mutable Workflow registry
		uuid.New(),
		map[string]any{"topic": "missing"},
	)
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, resp)

	var runsAfter int
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM workflow_runs`).Scan(&runsAfter))
	require.Equal(t, runsBefore, runsAfter, "a recovery miss must not create a workflow run")
}
