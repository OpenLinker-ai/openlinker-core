package runtime_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	"github.com/OpenLinker-ai/openlinker-core/pkg/auth"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/mcp"
	"github.com/OpenLinker-ai/openlinker-core/pkg/usertoken"
)

type mcpRPCResponse struct {
	Result struct {
		Tools []struct {
			Name        string         `json:"name"`
			Description string         `json:"description"`
			InputSchema map[string]any `json:"inputSchema"`
		} `json:"tools"`
		Instructions      string `json:"instructions"`
		IsError           bool   `json:"isError"`
		StructuredContent any    `json:"structuredContent"`
	} `json:"result"`
	Error *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func content(response mcpRPCResponse) map[string]any {
	out, _ := response.Result.StructuredContent.(map[string]any)
	return out
}

// The scoped MCP endpoint is exercised through the real User Token middleware,
// MCP handler, runtime Service and a synthetic upstream mcp_server.
func TestAgentScopedMCPEndpoint(t *testing.T) {
	pool := setupTestDB(t)
	ctx := context.Background()
	svc := newTestService(t, pool)
	var upstreamCalls atomic.Int32
	endpoint := startMockEndpointForService(t, svc, func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		var call struct {
			Method string `json:"method"`
			Params struct {
				Name      string         `json:"name"`
				Arguments map[string]any `json:"arguments"`
			} `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&call)
		w.Header().Set("Content-Type", "application/json")
		raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "result": map[string]any{"structuredContent": map[string]any{"tool": call.Params.Name, "echo": call.Params.Arguments}}})
		_, _ = w.Write(raw)
	})
	cfg := newTestConfig()
	creator, caller := insertCreator(t, pool), insertRuntimeUser(t, pool)
	newMCPAgent := func(visibility, schema string) (uuid.UUID, string) {
		id := insertAgent(t, pool, creator, endpoint, 0, "approved")
		var slug string
		require.NoError(t, pool.QueryRow(ctx, `UPDATE agents SET connection_mode='mcp_server',mcp_tool_name='analyze',visibility=$2 WHERE id=$1 RETURNING slug`, id, visibility).Scan(&slug))
		if schema != "" {
			_, err := pool.Exec(ctx, `INSERT INTO agent_capabilities(agent_id,input_schema,output_schema) VALUES($1,$2::jsonb,'{"type":"object"}'::jsonb)`, id, schema)
			require.NoError(t, err)
		}
		return id, slug
	}
	agentA, slugA := newMCPAgent("public", `{"type":"object","required":["text"],"properties":{"text":{"type":"string","description":"PUBLIC-SCHEMA-MARKER"}}}`)
	agentB, _ := newMCPAgent("public", "")
	privateAgent, _ := newMCPAgent("private", `{"type":"object","properties":{"secret":{"type":"string","description":"PRIVATE-SCHEMA-MARKER"}}}`)
	directAgent := insertAgent(t, pool, creator, endpoint, 0, "approved")

	tokens := usertoken.NewService(pool)
	tokenIDs := map[string]string{}
	issue := func(user uuid.UUID, scopes ...string) string {
		token, err := tokens.Create(ctx, user, &usertoken.CreateRequest{Name: "mcp-" + uuid.NewString()[:8], Scopes: scopes})
		require.NoError(t, err)
		tokenIDs[token.PlaintextToken] = token.ID
		return token.PlaintextToken
	}
	// Narrow grants in place: Create only accepts resource IDs the user owns.
	restrict := func(plaintext, permission string, resource uuid.UUID) {
		tag, err := pool.Exec(ctx, `UPDATE user_token_core_grants SET resource_id=$3 WHERE token_id=$1 AND permission=$2`, tokenIDs[plaintext], permission, resource)
		require.NoError(t, err)
		require.EqualValues(t, 1, tag.RowsAffected())
	}
	all := []string{"agents:read", "agents:run", "runs:read", "runs:cancel"}
	callerToken, ownerToken := issue(caller, all...), issue(creator, all...)
	narrowOwner := issue(creator, all...)
	restrict(narrowOwner, "agents:run", agentB)
	_, err := pool.Exec(ctx, `DELETE FROM user_token_core_grants WHERE token_id=$1 AND permission='agents:read'`, tokenIDs[narrowOwner])
	require.NoError(t, err)
	narrowCaller := issue(caller, all...)
	restrict(narrowCaller, "agents:run", agentB)
	jwt, err := auth.GenerateTokenWithVersion(caller.String(), cfg.JWTSecret, time.Hour, 0)
	require.NoError(t, err)

	e := echo.New()
	e.HTTPErrorHandler = func(err error, c echo.Context) { _ = httpx.SendError(c, err) }
	api := e.Group("/api/v1")
	market := agent.NewMarketService(pool)
	agent.NewMarketHandler(market).Register(api)
	handler := mcp.NewHandler(mcp.NewService(market, svc, nil))
	directory := mcp.NewDirectory(pool)
	handler.SetAgentScopeResolver(directory)
	handler.Register(api, auth.HybridAuthMiddlewareWithUserStatus(cfg.JWTSecret, tokens, auth.NewDBUserStatusChecker(pool)))
	mcp.NewCatalogHandler(directory).Register(api)

	send := func(method, path, bearer, accept string, body any) *httptest.ResponseRecorder {
		t.Helper()
		var reader *bytes.Reader
		if body == nil {
			reader = bytes.NewReader(nil)
		} else {
			raw, _ := json.Marshal(body)
			reader = bytes.NewReader(raw)
		}
		req := httptest.NewRequest(method, "/api/v1"+path, reader)
		req.Header.Set("Content-Type", "application/json")
		if bearer != "" {
			req.Header.Set("Authorization", "Bearer "+bearer)
		}
		if accept != "" {
			req.Header.Set("Accept", accept)
		}
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		return rec
	}
	rpc := func(agentID uuid.UUID, bearer, method string, params any, wantStatus int) (mcpRPCResponse, string) {
		t.Helper()
		rec := send(http.MethodPost, "/mcp/agents/"+agentID.String(), bearer, "", map[string]any{"jsonrpc": "2.0", "id": 7, "method": method, "params": params})
		require.Equal(t, wantStatus, rec.Code, rec.Body.String())
		var out mcpRPCResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		return out, rec.Body.String()
	}
	tool := func(agentID uuid.UUID, bearer, name string, args map[string]any) (mcpRPCResponse, string) {
		t.Helper()
		return rpc(agentID, bearer, "tools/call", map[string]any{"name": name, "arguments": args}, http.StatusOK)
	}

	// Authentication comes first: no credential, a browser JWT, and bad paths.
	require.Equal(t, http.StatusUnauthorized, send(http.MethodPost, "/mcp/agents/"+agentA.String(), "", "", map[string]any{"jsonrpc": "2.0", "id": 1, "method": "tools/list"}).Code)
	_, body := rpc(agentA, jwt, "tools/list", nil, http.StatusForbidden)
	require.NotContains(t, body, "PUBLIC-SCHEMA-MARKER")
	_, body = rpc(privateAgent, jwt, "initialize", nil, http.StatusForbidden)
	require.NotContains(t, body, "PRIVATE-SCHEMA-MARKER")
	require.Equal(t, http.StatusNotFound, send(http.MethodPost, "/mcp/agents/not-a-uuid", callerToken, "", map[string]any{"jsonrpc": "2.0", "id": 1, "method": "tools/list"}).Code)
	rpc(uuid.New(), callerToken, "tools/list", nil, http.StatusNotFound)
	rpc(directAgent, callerToken, "tools/list", nil, http.StatusOK)

	// GET describes the endpoint without reading the Agent; SSE is not offered.
	require.Equal(t, http.StatusMethodNotAllowed, send(http.MethodGet, "/mcp/agents/"+privateAgent.String(), callerToken, "text/event-stream", nil).Code)
	info := send(http.MethodGet, "/mcp/agents/"+privateAgent.String(), callerToken, "application/json", nil)
	require.Equal(t, http.StatusOK, info.Code)
	require.NotContains(t, info.Body.String(), "PRIVATE-SCHEMA-MARKER")

	// tools/list is the fixed six-tool set; input stays nested under `input`.
	listed, _ := rpc(agentA, callerToken, "tools/list", nil, http.StatusOK)
	names := []string{}
	for _, item := range listed.Result.Tools {
		names = append(names, item.Name)
	}
	require.Equal(t, []string{"run_agent", "start_agent_run", "get_run", "list_run_events", "list_run_artifacts", "cancel_run"}, names)
	runSchema := listed.Result.Tools[0].InputSchema
	require.Equal(t, []any{"input", "idempotency_key"}, runSchema["required"])
	inputSchema := runSchema["properties"].(map[string]any)["input"].(map[string]any)
	require.Equal(t, []any{"text"}, inputSchema["required"])
	require.Contains(t, listed.Result.Tools[0].Description, slugA)
	initialized, _ := rpc(agentA, callerToken, "initialize", nil, http.StatusOK)
	require.Contains(t, initialized.Result.Instructions, slugA)

	// Private Agents: hidden from others and from owner tokens narrowed elsewhere.
	_, body = rpc(privateAgent, callerToken, "tools/list", nil, http.StatusNotFound)
	require.NotContains(t, body, "PRIVATE-SCHEMA-MARKER")
	_, body = rpc(privateAgent, narrowOwner, "tools/list", nil, http.StatusNotFound)
	require.NotContains(t, body, "PRIVATE-SCHEMA-MARKER")
	require.Equal(t, http.StatusNotFound, send(http.MethodPost, "/mcp/agents/"+privateAgent.String(), callerToken, "", map[string]any{"jsonrpc": "2.0", "method": "notifications/initialized"}).Code)
	_, body = rpc(privateAgent, ownerToken, "tools/list", nil, http.StatusOK)
	require.Contains(t, body, "PRIVATE-SCHEMA-MARKER")

	// run_agent injects the endpoint Agent; a different explicit ID is rejected.
	upstreamBefore := upstreamCalls.Load()
	mismatch, _ := tool(agentA, callerToken, "run_agent", map[string]any{"agent_id": agentB.String(), "input": map[string]any{"text": "x"}, "idempotency_key": "mismatch"})
	require.NotNil(t, mismatch.Error)
	require.Equal(t, -32602, mismatch.Error.Code)
	require.Equal(t, upstreamBefore, upstreamCalls.Load())
	unknown, _ := tool(agentA, callerToken, "search_agents", map[string]any{})
	require.Equal(t, -32602, unknown.Error.Code)
	ran, _ := tool(agentA, callerToken, "run_agent", map[string]any{"input": map[string]any{"text": "hello", "nested": map[string]any{"k": "v"}}, "idempotency_key": "scoped-a-1"})
	require.False(t, ran.Result.IsError, ran.Result.StructuredContent)
	runA := content(ran)["run_id"].(string)
	require.Equal(t, agentA.String(), content(ran)["agent_id"])
	output := content(ran)["output"].(map[string]any)
	require.Equal(t, "analyze", output["tool"])
	require.Equal(t, map[string]any{"text": "hello", "nested": map[string]any{"k": "v"}}, output["echo"])
	explicit, _ := tool(agentA, callerToken, "start_agent_run", map[string]any{"agent_id": agentA.String(), "input": map[string]any{"text": "again"}, "idempotency_key": "scoped-a-2"})
	require.False(t, explicit.Result.IsError, explicit.Result.StructuredContent)
	var source string
	require.NoError(t, pool.QueryRow(ctx, `SELECT source FROM runs WHERE id=$1`, runA).Scan(&source))
	require.Equal(t, "mcp", source)

	// agents:run narrowed to B cannot run A through A's endpoint, but can run B.
	denied, _ := tool(agentA, narrowCaller, "run_agent", map[string]any{"input": map[string]any{"text": "x"}, "idempotency_key": "narrow-a"})
	require.True(t, denied.Result.IsError)
	allowed, _ := tool(agentB, narrowCaller, "run_agent", map[string]any{"input": map[string]any{}, "idempotency_key": "narrow-b"})
	require.False(t, allowed.Result.IsError, allowed.Result.StructuredContent)
	runB := content(allowed)["run_id"].(string)

	// Run tools: ordinary authorization first, then the run must belong to this Agent.
	got, _ := tool(agentA, callerToken, "get_run", map[string]any{"run_id": runA})
	require.False(t, got.Result.IsError)
	artifacts, _ := tool(agentA, callerToken, "list_run_artifacts", map[string]any{"run_id": runA})
	require.False(t, artifacts.Result.IsError)
	items, ok := content(artifacts)["items"].([]any)
	require.True(t, ok, "MCP structuredContent must be an object with an items array")
	require.NotEmpty(t, items)
	for _, name := range []string{"get_run", "list_run_events", "list_run_artifacts", "cancel_run"} {
		crossed, body := tool(agentA, callerToken, name, map[string]any{"run_id": runB})
		require.True(t, crossed.Result.IsError, name)
		require.Contains(t, body, "NOT_FOUND", name)
		inScope, _ := tool(agentB, callerToken, name, map[string]any{"run_id": runB})
		if name != "cancel_run" {
			require.False(t, inScope.Result.IsError, name)
		}
	}
	running := insertRunningRun(t, pool, caller, agentB)
	cancelled, _ := tool(agentA, callerToken, "cancel_run", map[string]any{"run_id": running.String()})
	require.True(t, cancelled.Result.IsError)
	var status string
	require.NoError(t, pool.QueryRow(ctx, `SELECT status FROM runs WHERE id=$1`, running).Scan(&status))
	require.Equal(t, "running", status)
	otherUser := issue(insertRuntimeUser(t, pool), all...)
	foreign, _ := tool(agentA, otherUser, "get_run", map[string]any{"run_id": runA})
	require.True(t, foreign.Result.IsError)

	// A run-scoped token reads only its own run, even on the matching endpoint.
	runScoped := issue(caller, "runs:read")
	restrict(runScoped, "runs:read", uuid.MustParse(runA))
	own, _ := tool(agentA, runScoped, "get_run", map[string]any{"run_id": runA})
	require.False(t, own.Result.IsError)
	other, body := tool(agentA, runScoped, "get_run", map[string]any{"run_id": content(explicit)["run_id"]})
	require.True(t, other.Result.IsError)
	require.Contains(t, body, "PERMISSION_DENIED")

	// Platform MCP is unchanged: nine tools, unrestricted dispatch.
	platform := send(http.MethodPost, "/mcp", callerToken, "", map[string]any{"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
	var platformList mcpRPCResponse
	require.NoError(t, json.Unmarshal(platform.Body.Bytes(), &platformList))
	require.Len(t, platformList.Result.Tools, 9)
	platformRun := send(http.MethodPost, "/mcp", callerToken, "", map[string]any{"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": map[string]any{"name": "get_run", "arguments": map[string]any{"run_id": runB}}})
	require.NoError(t, json.Unmarshal(platformRun.Body.Bytes(), &platformList))
	require.False(t, platformList.Result.IsError)

	// Public detail and get_agent hide the upstream address; the owner view keeps it.
	detail := send(http.MethodGet, "/agents/"+slugA, "", "", nil)
	require.Equal(t, http.StatusOK, detail.Code)
	require.NotContains(t, detail.Body.String(), strings.TrimPrefix(endpoint, "https://"))
	require.Contains(t, detail.Body.String(), `"connection_mode":"mcp_server"`)
	agentTool := send(http.MethodPost, "/mcp", callerToken, "", map[string]any{"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": map[string]any{"name": "get_agent", "arguments": map[string]any{"slug": slugA}}})
	require.NotContains(t, agentTool.Body.String(), strings.TrimPrefix(endpoint, "https://"))
	ownerView, err := market.GetBySlugForOwner(ctx, slugA, creator)
	require.NoError(t, err)
	require.Equal(t, endpoint, ownerView.EndpointURL)

	// The catalog lists only active public mcp_server Agents, filtered before paging.
	_, err = pool.Exec(ctx, `UPDATE agents SET name='Catalog Searchable 100%' WHERE id=$1`, agentB)
	require.NoError(t, err)
	catalog := send(http.MethodGet, "/mcp-services?size=1", "", "", nil)
	require.Equal(t, http.StatusOK, catalog.Code)
	require.Equal(t, "no-store", catalog.Header().Get("Cache-Control"))
	var services mcp.ServiceListResponse
	require.NoError(t, json.Unmarshal(catalog.Body.Bytes(), &services))
	require.EqualValues(t, 2, services.Total)
	require.Len(t, services.Items, 1)
	require.Equal(t, 1, services.Size)
	require.NotContains(t, catalog.Body.String(), endpoint)
	require.NoError(t, json.Unmarshal(send(http.MethodGet, "/mcp-services?q=100%25", "", "", nil).Body.Bytes(), &services))
	require.EqualValues(t, 1, services.Total)
	require.Equal(t, agentB.String(), services.Items[0].ID)
	require.Equal(t, "mcp_server", services.Items[0].ConnectionMode)
	require.Equal(t, "analyze", services.Items[0].MCPToolName)
	_, err = pool.Exec(ctx, `UPDATE agents SET lifecycle_status='disabled' WHERE id=$1`, agentB)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(send(http.MethodGet, "/mcp-services", "", "", nil).Body.Bytes(), &services))
	require.EqualValues(t, 1, services.Total)
	require.Equal(t, agentA.String(), services.Items[0].ID)
	rpc(agentB, callerToken, "tools/list", nil, http.StatusNotFound)
}
