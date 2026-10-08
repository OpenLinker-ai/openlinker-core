package runtime_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	"github.com/OpenLinker-ai/openlinker-core/pkg/auth"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/mcp"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/OpenLinker-ai/openlinker-core/pkg/usertoken"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

// Exercise both production MCP surfaces, their real credential middleware and
// the assignment consumed by SDK Workers. Native adapters require this trusted
// conversation even when the caller is not using the A2A wire protocol.
func TestMCPRuntimeCreatesTrustedConversation(t *testing.T) {
	for _, path := range []string{"/mcp", "/mcp/agents/"} {
		for _, tool := range []string{"run_agent", "start_agent_run"} {
			t.Run(path+tool, func(t *testing.T) {
				pool, svc, leases, principal, target := dryRunReadyFixture(t)
				ctx := context.Background()
				_, err := pool.Exec(ctx, `INSERT INTO agent_capabilities(agent_id,input_schema,output_schema)
VALUES($1,'{"type":"object","required":["task"],"properties":{"task":{"type":"string"},"a2a_context_id":{"type":"string"},"a2a_task_id":{"type":"string"}}}'::jsonb,'{"type":"object"}'::jsonb)`, target.ID)
				require.NoError(t, err)
				tokens := usertoken.NewService(pool)
				token, err := tokens.Create(ctx, target.CreatorID, &usertoken.CreateRequest{
					Name: "MCP native context", Scopes: []string{"agents:run", "runs:read"},
				})
				require.NoError(t, err)
				e := echo.New()
				e.HTTPErrorHandler = func(err error, c echo.Context) { _ = httpx.SendError(c, err) }
				handler := mcp.NewHandler(mcp.NewService(agent.NewMarketService(pool), svc, nil))
				handler.SetAgentScopeResolver(mcp.NewDirectory(pool))
				handler.Register(e.Group("/api/v1"), auth.HybridAuthMiddlewareWithUserStatus(
					newTestConfig().JWTSecret, tokens, auth.NewDBUserStatusChecker(pool),
				))
				endpoint := "/api/v1" + path
				if path != "/mcp" {
					endpoint += target.ID.String()
				}
				input := map[string]any{"task": "synthetic native invocation"}
				metadata := map[string]any{
					"conversation":                  map[string]any{"source": "core", "session_key": "forged-session", "current_run_id": "forged-run"},
					"_openlinker_runtime_authority": map[string]any{"source": "core", "principal_scope_id": "forged-principal"},
				}
				call := func(key string) string {
					t.Helper()
					raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": map[string]any{
						"name": tool, "arguments": map[string]any{"agent_id": target.ID.String(), "input": input, "metadata": metadata, "idempotency_key": key},
					}})
					require.NoError(t, err)
					req := httptest.NewRequest(http.MethodPost, endpoint, bytes.NewReader(raw))
					req.Header.Set("Content-Type", "application/json")
					req.Header.Set("Authorization", "Bearer "+token.PlaintextToken)
					rec := httptest.NewRecorder()
					e.ServeHTTP(rec, req)
					require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
					var response mcpRPCResponse
					require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
					require.Nil(t, response.Error, rec.Body.String())
					require.False(t, response.Result.IsError, rec.Body.String())
					return content(response)["run_id"].(string)
				}
				first := call("native-context-first")
				require.Equal(t, first, call("native-context-first"), "retries must reuse the committed context")
				second := call("native-context-second")
				require.NotEqual(t, first, second)
				for _, id := range []string{first, second} {
					var raw, persistedInput []byte
					var contextID string
					var sameTransaction bool
					require.NoError(t, pool.QueryRow(ctx, `SELECT r.request_metadata, m.protocol_context_id, r.xmin=m.xmin, r.input
FROM runs r JOIN a2a_context_mappings m ON m.run_id=r.id WHERE r.id=$1`, id).Scan(&raw, &contextID, &sameTransaction, &persistedInput))
					require.JSONEq(t, `{"task":"synthetic native invocation"}`, string(persistedInput))
					require.Equal(t, "ctx-"+id, contextID)
					require.True(t, sameTransaction, "context must commit atomically with immutable Run creation")
					var stored map[string]any
					require.NoError(t, json.Unmarshal(raw, &stored))
					conversation := stored["conversation"].(map[string]any)
					require.Equal(t, "core", conversation["source"])
					require.Equal(t, "ctx-"+id, conversation["session_key"])
					require.Equal(t, id, conversation["current_run_id"])
					require.NotContains(t, conversation, "history_before_current", "independent calls must not reuse history")
					require.NotContains(t, string(raw), "forged-")
				}
				assignment, err := leases.ClaimOffer(ctx, principal)
				require.NoError(t, err)
				require.NotNil(t, assignment)
				id := assignment.AttemptIdentity.RunID.String()
				require.Contains(t, []string{first, second}, id)
				require.Equal(t, input, assignment.Input, "implicit MCP context must not rewrite application input")
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
}

func TestMCPRuntimeReplaysPreContextIdentity(t *testing.T) {
	pool, svc, _, _, target := dryRunReadyFixture(t)
	ctx := context.Background()
	request := &runtime.RunRequest{AgentID: target.ID.String(), Input: map[string]any{"task": "legacy MCP"}, Metadata: map[string]any{}, IdempotencyKey: "pre-context-mcp", CreationProtocol: "mcp", CreationMethod: "run_agent"}
	key, err := runtime.HashIdempotencyKey(request.IdempotencyKey)
	require.NoError(t, err)
	fingerprint, err := runtime.FingerprintRunCreation(runtime.RunFingerprintInput{
		Target: runtime.RunFingerprintTarget{AgentID: target.ID.String()}, Input: request.Input, Metadata: request.Metadata,
		Source: runtime.RunFingerprintSource{Protocol: "mcp", Method: "run_agent"}, Options: map[string]any{},
	})
	require.NoError(t, err)
	legacy := uuid.New()
	_, err = pool.Exec(ctx, `INSERT INTO runs(id,user_id,agent_id,input,status,source,idempotency_key_hash,idempotency_fingerprint,
connection_mode_snapshot,dispatch_state,cost_cents,platform_fee_cents,creator_revenue_cents,dispatch_deadline_at,run_deadline_at)
VALUES($1,$2,$3,$4::jsonb,'running','mcp',$5,$6,'runtime','pending',0,0,0,clock_timestamp()+interval '5 minutes',clock_timestamp()+interval '10 minutes')`,
		legacy, target.CreatorID, target.ID, `{"task":"legacy MCP"}`, key[:], fingerprint[:])
	require.NoError(t, err)
	replayed, err := svc.StartRun(ctx, target.CreatorID, request, "mcp")
	require.NoError(t, err)
	require.True(t, replayed.Replayed)
	require.Equal(t, legacy.String(), replayed.RunID)
	var mappings int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM a2a_context_mappings WHERE run_id=$1`, legacy).Scan(&mappings))
	require.Zero(t, mappings, "old committed calls must not be rewritten or dispatched again")
}

func TestMCPConversationDefaultsPreserveOtherEntrypoints(t *testing.T) {
	for _, kind := range []string{"direct MCP", "REST without context", "explicit context"} {
		t.Run(kind, func(t *testing.T) {
			pool, svc, _, _, target := dryRunReadyFixture(t)
			ctx := context.Background()
			_, err := pool.Exec(ctx, `INSERT INTO agent_capabilities(agent_id,input_schema,output_schema)
VALUES($1,'{"type":"object","properties":{"task":{"type":"string"},"a2a_context_id":{"type":"string"},"a2a_task_id":{"type":"string"}}}'::jsonb,'{"type":"object"}'::jsonb)`, target.ID)
			require.NoError(t, err)
			req := &runtime.RunRequest{AgentID: target.ID.String(), Input: map[string]any{"task": "unchanged input"}, IdempotencyKey: "context-boundary", CreationProtocol: "mcp", CreationMethod: "run_agent"}
			source := "mcp"
			switch kind {
			case "direct MCP":
				endpoint := startMockEndpointForService(t, svc, func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"output":{"answer":"synthetic"}}`))
				})
				_, err := pool.Exec(ctx, `UPDATE agents SET connection_mode='direct_http',endpoint_url=$2 WHERE id=$1`, target.ID, endpoint)
				require.NoError(t, err)
			case "REST without context":
				req.CreationProtocol, req.CreationMethod, source = "rest", "runs.create", "web"
			case "explicit context":
				req.A2AContext = &runtime.RunA2AContextRequest{ProtocolContextID: "explicit-conversation", ProtocolTaskID: "explicit-task"}
			}
			call := svc.StartRun
			if kind == "direct MCP" {
				call = svc.Run
			}
			run, err := call(ctx, target.CreatorID, req, source)
			require.NoError(t, err)
			var mappings int
			require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM a2a_context_mappings WHERE run_id=$1`, run.RunID).Scan(&mappings))
			if kind == "explicit context" {
				require.Equal(t, 1, mappings)
				var contextID, taskID string
				require.NoError(t, pool.QueryRow(ctx, `SELECT protocol_context_id,protocol_task_id FROM a2a_context_mappings WHERE run_id=$1`, run.RunID).Scan(&contextID, &taskID))
				require.Equal(t, "explicit-conversation", contextID)
				require.Equal(t, "explicit-task", taskID)
				var inputJSON []byte
				require.NoError(t, pool.QueryRow(ctx, `SELECT input FROM runs WHERE id=$1`, run.RunID).Scan(&inputJSON))
				var persistedInput map[string]any
				require.NoError(t, json.Unmarshal(inputJSON, &persistedInput))
				require.Equal(t, "explicit-conversation", persistedInput["a2a_context_id"])
				require.Equal(t, "explicit-task", persistedInput["a2a_task_id"])
			} else {
				require.Zero(t, mappings)
			}
		})
	}
}

func TestMCPRuntimeConcurrentContextCreation(t *testing.T) {
	pool, svc, _, _, target := dryRunReadyFixture(t)
	ctx := context.Background()
	mcpSvc := mcp.NewService(agent.NewMarketService(pool), svc, nil)
	request := &mcp.RunAgentRequest{AgentID: target.ID.String(), Input: map[string]any{"task": "concurrent MCP"}, IdempotencyKey: "same-concurrent-key"}
	type result struct {
		run *runtime.RunResponse
		err error
	}
	start, results := make(chan struct{}), make(chan result, 2)
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			run, err := mcpSvc.StartAgentRun(ctx, target.CreatorID, request)
			results <- result{run, err}
		}()
	}
	close(start)
	first, second := <-results, <-results
	require.NoError(t, first.err)
	require.NoError(t, second.err)
	require.Equal(t, first.run.RunID, second.run.RunID)
	var runs, mappings int
	var contextID string
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM runs WHERE agent_id=$1`, target.ID).Scan(&runs))
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*),min(protocol_context_id) FROM a2a_context_mappings WHERE agent_id=$1`, target.ID).Scan(&mappings, &contextID))
	require.Equal(t, 1, runs)
	require.Equal(t, 1, mappings)
	require.Equal(t, "ctx-"+first.run.RunID, contextID)
}
