package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	"github.com/OpenLinker-ai/openlinker-core/pkg/externalexecution"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/OpenLinker-ai/openlinker-core/pkg/task"
	"github.com/OpenLinker-ai/openlinker-core/pkg/workflow"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// Hosted service orders, workflow steps and Task runs cannot carry a typed
// conversation, yet native Runtime hosts refuse work without a trusted one.
// Each request mirrors its production entrypoint's protocol, method and source.
func TestEntrypointsWithoutConversationReceiveCoreOwnedContext(t *testing.T) {
	entrypoints := []struct {
		protocol, method, source string
		metadata                 map[string]any
	}{
		{"external_execution", "external-execution.start", "api", map[string]any{"trace_id": "service-order-trace"}},
		{"hosted", "service-order.execute", "api", map[string]any{"trace_id": "legacy-service-order-trace"}},
		{"workflow", "node.run", "api", map[string]any{"workflow_node_key": "step-1"}},
		{"task", "run", "web", nil},
	}
	for _, entry := range entrypoints {
		t.Run(entry.protocol, func(t *testing.T) {
			pool, svc, leases, principal, target := dryRunReadyFixture(t)
			ctx := context.Background()
			_, err := pool.Exec(ctx, `INSERT INTO agent_capabilities(agent_id,input_schema,output_schema)
VALUES($1,'{"type":"object","required":["text"],"properties":{"text":{"type":"string"}},"additionalProperties":false}'::jsonb,'{"type":"object"}'::jsonb)`, target.ID)
			require.NoError(t, err)
			if entry.protocol == "task" {
				taskID := uuid.New()
				_, err = pool.Exec(ctx, `INSERT INTO task_queries (id,user_id,query,parsed_skills,mcp_tools,recommended_agent_ids,chosen_agent_id,chosen_at)
VALUES ($1,$2,'synthetic task',ARRAY[]::text[],ARRAY[]::text[],$3,$4,clock_timestamp())`, taskID, target.CreatorID, []uuid.UUID{target.ID}, target.ID)
				require.NoError(t, err)
				entry.metadata = map[string]any{"task_id": taskID.String()}
			}
			input := map[string]any{"text": "synthetic " + entry.protocol + " invocation"}
			start := func(key string) string {
				t.Helper()
				metadata := map[string]any{
					"conversation":                  map[string]any{"source": "core", "session_key": "forged-session", "current_run_id": "forged-run"},
					"_openlinker_runtime_authority": map[string]any{"source": "core", "principal_scope_id": "forged-principal"},
				}
				for k, v := range entry.metadata {
					metadata[k] = v
				}
				run, err := svc.StartRun(ctx, target.CreatorID, &runtime.RunRequest{
					AgentID: target.ID.String(), Input: input, Metadata: metadata, IdempotencyKey: key,
					CreationProtocol: entry.protocol, CreationMethod: entry.method,
				}, entry.source)
				require.NoError(t, err)
				return run.RunID
			}
			first := start(entry.protocol + "-first")
			require.Equal(t, first, start(entry.protocol+"-first"), "retries must reuse the committed context")
			second := start(entry.protocol + "-second")
			require.NotEqual(t, first, second)
			for _, id := range []string{first, second} {
				var raw, persistedInput []byte
				var contextID string
				var sameTransaction bool
				require.NoError(t, pool.QueryRow(ctx, `SELECT r.request_metadata, m.protocol_context_id, r.xmin=m.xmin, r.input
FROM runs r JOIN a2a_context_mappings m ON m.run_id=r.id WHERE r.id=$1`, id).Scan(&raw, &contextID, &sameTransaction, &persistedInput))
				require.JSONEq(t, `{"text":"synthetic `+entry.protocol+` invocation"}`, string(persistedInput))
				require.Equal(t, "ctx-"+id, contextID)
				require.True(t, sameTransaction, "context must commit atomically with immutable Run creation")
				var stored map[string]any
				require.NoError(t, json.Unmarshal(raw, &stored))
				conversation := stored["conversation"].(map[string]any)
				require.Equal(t, "core", conversation["source"])
				require.Equal(t, "ctx-"+id, conversation["session_key"])
				require.Equal(t, id, conversation["current_run_id"])
				require.NotContains(t, conversation, "history_before_current", "independent runs must not share history")
				require.NotContains(t, string(raw), "forged-")
			}
			assignment, err := leases.ClaimOffer(ctx, principal)
			require.NoError(t, err)
			require.NotNil(t, assignment)
			id := assignment.AttemptIdentity.RunID.String()
			require.Contains(t, []string{first, second}, id)
			require.Equal(t, input, assignment.Input, "implicit context must not rewrite application input")
			conversation := assignment.Metadata["conversation"].(map[string]any)
			require.Equal(t, "core", conversation["source"])
			require.Equal(t, "ctx-"+id, conversation["session_key"])
			require.Equal(t, id, conversation["current_run_id"])
			authority := assignment.Metadata["_openlinker_runtime_authority"].(map[string]any)
			require.Equal(t, "core", authority["source"])
			require.Regexp(t, `^ps1_[A-Za-z0-9_-]{43}$`, authority["principal_scope_id"])
		})
	}
}

type agentTargetsOnlyWorkflows struct{}

var errWorkflowOutOfScope = errors.New("workflow targets are outside this test")

func (agentTargetsOnlyWorkflows) ValidateExternalExecutionTarget(context.Context, uuid.UUID, uuid.UUID) (*externalexecution.WorkflowTargetValidation, error) {
	return nil, errWorkflowOutOfScope
}

func (agentTargetsOnlyWorkflows) LookupExternalExecutionWorkflowRun(context.Context, string, uuid.UUID, uuid.UUID, uuid.UUID, map[string]interface{}) (*workflow.WorkflowRunResponse, bool, error) {
	return nil, false, errWorkflowOutOfScope
}

func (agentTargetsOnlyWorkflows) LookupExternalExecutionWorkflowRunByIdentity(context.Context, string, uuid.UUID, uuid.UUID, uuid.UUID) (*workflow.WorkflowRunResponse, bool, error) {
	return nil, false, errWorkflowOutOfScope
}

func (agentTargetsOnlyWorkflows) StartExternalWorkflowRun(context.Context, string, uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID, map[string]interface{}) (*workflow.WorkflowRunResponse, error) {
	return nil, errWorkflowOutOfScope
}

func (agentTargetsOnlyWorkflows) StartExternalExecutionWorkflowRunWithFence(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, map[string]interface{}, workflow.ExternalExecutionLaunchFence) (*workflow.WorkflowRunResponse, error) {
	return nil, errWorkflowOutOfScope
}

func (agentTargetsOnlyWorkflows) GetWorkflowRun(context.Context, uuid.UUID, uuid.UUID) (*workflow.WorkflowRunResponse, error) {
	return nil, errWorkflowOutOfScope
}

func (agentTargetsOnlyWorkflows) CancelExternalWorkflowRun(context.Context, uuid.UUID, uuid.UUID, string) (*workflow.WorkflowRunResponse, workflow.CancellationEvidence, error) {
	return nil, workflow.CancellationEvidence{}, errWorkflowOutOfScope
}

func (agentTargetsOnlyWorkflows) GetWorkflowCancellationEvidence(context.Context, uuid.UUID, uuid.UUID) (workflow.CancellationEvidence, error) {
	return workflow.CancellationEvidence{}, errWorkflowOutOfScope
}

// requireIndependentCoreConversation checks the committed mapping and the
// assignment a native host receives for one Run.
func requireIndependentCoreConversation(t *testing.T, pool *pgxpool.Pool, runID string, input map[string]any) {
	t.Helper()
	var raw, persistedInput []byte
	var contextID, rootID string
	var sameTransaction bool
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT r.request_metadata, m.protocol_context_id, m.root_context_id, r.xmin=m.xmin, r.input
FROM runs r JOIN a2a_context_mappings m ON m.run_id=r.id WHERE r.id=$1`, runID).Scan(&raw, &contextID, &rootID, &sameTransaction, &persistedInput))
	expectedInput, err := json.Marshal(input)
	require.NoError(t, err)
	require.JSONEq(t, string(expectedInput), string(persistedInput))
	require.Equal(t, "ctx-"+runID, contextID)
	require.Equal(t, "ctx-"+runID, rootID, "independent Runs must not join another conversation")
	require.True(t, sameTransaction, "context must commit atomically with immutable Run creation")
	var stored map[string]any
	require.NoError(t, json.Unmarshal(raw, &stored))
	conversation := stored["conversation"].(map[string]any)
	require.Equal(t, "core", conversation["source"])
	require.Equal(t, runID, conversation["current_run_id"])
	require.NotContains(t, conversation, "history_before_current")
}

func TestHostedServiceOrderRuntimeRunReceivesCoreOwnedContext(t *testing.T) {
	pool, svc, leases, principal, target := dryRunReadyFixture(t)
	ctx := context.Background()
	_, err := pool.Exec(ctx, `UPDATE agents SET visibility='public' WHERE id=$1`, target.ID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO agent_capabilities(agent_id,input_schema,output_schema)
VALUES($1,'{"type":"object","required":["text"],"properties":{"text":{"type":"string"}},"additionalProperties":true}'::jsonb,'{"type":"object"}'::jsonb)`, target.ID)
	require.NoError(t, err)
	external := externalexecution.NewService(
		agent.NewService(pool, newTestConfig()), svc, agentTargetsOnlyWorkflows{}, externalexecution.NewSQLStore(pool),
	)
	schema := json.RawMessage(`{"type":"object","required":["text"],"properties":{"text":{"type":"string"}},"additionalProperties":false}`)
	validation, err := external.ValidateTarget(ctx, &externalexecution.Principal{CallerServiceID: "openlinker-cloud", ActorUserID: target.CreatorID},
		&externalexecution.TargetValidationRequest{TargetType: "agent", TargetID: target.ID.String(), InputSchema: schema})
	require.NoError(t, err)
	require.True(t, validation.Executable, validation.UnavailableReason)

	// Service orders run as the buyer, a different user from the seller.
	buyer := insertRuntimeUser(t, pool)
	order := func(requestID string) *externalexecution.ExecutionStartResponse {
		t.Helper()
		started, err := external.StartExecution(ctx, &externalexecution.Principal{CallerServiceID: "openlinker-cloud", ActorUserID: buyer},
			&externalexecution.ExecutionRequest{
				ExternalRequestID: requestID, TargetType: "agent", TargetID: target.ID.String(),
				Input:    map[string]any{"text": "service order " + requestID[:8]},
				Metadata: map[string]any{"conversation": map[string]any{"source": "core", "session_key": "forged-session", "current_run_id": "forged-run"}},
				TraceID:  "trace-" + requestID[:8], ExpectedContractHash: validation.ContractHash, InputSchema: schema,
			})
		require.NoError(t, err)
		return started
	}
	firstID, secondID := uuid.NewString(), uuid.NewString()
	first := order(firstID)
	require.Equal(t, first.ExecutionID, order(firstID).ExecutionID, "a repeated order start must reuse the committed Run and context")
	second := order(secondID)
	require.NotEqual(t, first.ExecutionID, second.ExecutionID)
	var runIDs []string
	for _, item := range []struct {
		execution *externalexecution.ExecutionStartResponse
		requestID string
	}{{first, firstID}, {second, secondID}} {
		var runID string
		require.NoError(t, pool.QueryRow(ctx, `SELECT execution_id::text FROM external_executions
WHERE caller_service_id='openlinker-cloud' AND external_request_id=$1 AND execution_kind='run'`, item.requestID).Scan(&runID))
		require.Equal(t, item.execution.ExecutionID, runID)
		requireIndependentCoreConversation(t, pool, runID, map[string]any{"text": "service order " + item.requestID[:8]})
		runIDs = append(runIDs, runID)
	}
	var mappingUser uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `SELECT user_id FROM a2a_context_mappings WHERE run_id=$1`, runIDs[0]).Scan(&mappingUser))
	require.Equal(t, buyer, mappingUser, "the conversation belongs to the buyer who created the Run")

	assignment, err := leases.ClaimOffer(ctx, principal)
	require.NoError(t, err)
	require.NotNil(t, assignment)
	id := assignment.AttemptIdentity.RunID.String()
	require.Contains(t, runIDs, id)
	conversation := assignment.Metadata["conversation"].(map[string]any)
	require.Equal(t, "core", conversation["source"])
	require.Equal(t, "ctx-"+id, conversation["session_key"])
	require.Equal(t, id, conversation["current_run_id"])
	authority := assignment.Metadata["_openlinker_runtime_authority"].(map[string]any)
	require.Equal(t, "core", authority["source"])
	require.Regexp(t, `^ps1_[A-Za-z0-9_-]{43}$`, authority["principal_scope_id"])
}

func TestPrivateTaskRunsReceiveIndependentCoreOwnedContexts(t *testing.T) {
	pool, svc, _, _, target := dryRunReadyFixture(t)
	ctx := context.Background()
	taskID := uuid.New()
	_, err := pool.Exec(ctx, `INSERT INTO task_queries (id,user_id,query,parsed_skills,mcp_tools,recommended_agent_ids,chosen_agent_id,chosen_at)
VALUES ($1,$2,'synthetic task',ARRAY[]::text[],ARRAY[]::text[],$3,$4,clock_timestamp())`, taskID, target.CreatorID, []uuid.UUID{target.ID}, target.ID)
	require.NoError(t, err)
	tasks := task.NewService(pool, nil, nil)
	tasks.SetRunStarter(svc)
	run := func(key string) string {
		t.Helper()
		resp, err := tasks.RunTask(ctx, taskID, target.CreatorID, &task.RunTaskRequest{
			AgentID: target.ID, Input: map[string]any{"text": "same task"}, IdempotencyKey: key,
		})
		require.NoError(t, err)
		return resp.Run.RunID
	}
	first := run("task-first")
	require.Equal(t, first, run("task-first"))
	second := run("task-second")
	require.NotEqual(t, first, second, "two executions of one task are separate Runs")
	for _, id := range []string{first, second} {
		requireIndependentCoreConversation(t, pool, id, map[string]any{"text": "same task"})
	}
}
