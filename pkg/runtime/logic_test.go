package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

func TestRuntimeAuthScopeAndParsingHelpers(t *testing.T) {
	e := echo.New()
	c := e.NewContext(httptest.NewRequest(http.MethodGet, "/api/v1/runs/id", nil), httptest.NewRecorder())

	_, err := userIDFromCtx(c)
	var httpErr *httpx.HTTPError
	require.True(t, errors.As(err, &httpErr))
	require.Equal(t, http.StatusUnauthorized, httpErr.Status)

	c.Set(string(httpx.CtxKeyUserID), "not-a-uuid")
	_, err = userIDFromCtx(c)
	require.True(t, errors.As(err, &httpErr))
	require.Equal(t, "token 无效", httpErr.Message)

	uid := uuid.New()
	c.Set(string(httpx.CtxKeyUserID), uid.String())
	got, err := userIDFromCtx(c)
	require.NoError(t, err)
	require.Equal(t, uid, got)

	c.Set(string(httpx.CtxKeyAuthMethod), "jwt")
	require.Equal(t, "web", sourceFromCtx(c))
	require.NoError(t, requireAPIKeyScope(c, "agents:run"))
	c.Set(string(httpx.CtxKeyAuthMethod), "unknown")
	require.Equal(t, "web", sourceFromCtx(c))

	c.Set(string(httpx.CtxKeyAuthMethod), "user_token")
	c.Set(string(httpx.CtxKeyAuthScopes), []string{"runs:read"})
	require.Equal(t, "api", sourceFromCtx(c))
	err = requireAPIKeyScope(c, "agents:run")
	require.True(t, errors.As(err, &httpErr))
	require.Equal(t, http.StatusForbidden, httpErr.Status)

	token, err := runtimeBearerToken(" Bearer  ol_user_test  ")
	require.NoError(t, err)
	require.Equal(t, "ol_user_test", token)
	token, err = runtimeBearerToken("bearer rt_lower")
	require.NoError(t, err)
	require.Equal(t, "rt_lower", token)
	_, err = runtimeBearerToken("Basic abc")
	require.True(t, errors.As(err, &httpErr))
	require.Equal(t, http.StatusUnauthorized, httpErr.Status)
	require.Equal(t, "缺少 Agent Token", httpErr.Message)

	_, err = (&Service{}).ValidateRuntimeToken(context.Background(), "not-an-agent-token", "agent:pull")
	require.True(t, errors.As(err, &httpErr))
	require.Equal(t, http.StatusUnauthorized, httpErr.Status)
	require.Equal(t, "Agent Token 无效或已撤销", httpErr.Message)

	n, err := parseOptionalInt32("")
	require.NoError(t, err)
	require.Equal(t, int32(0), n)
	n, err = parseOptionalInt32("42")
	require.NoError(t, err)
	require.Equal(t, int32(42), n)
	_, err = parseOptionalInt32("bad")
	require.Error(t, err)
	_, err = parseOptionalInt32("2147483648")
	require.Error(t, err)

	req := httptest.NewRequest(http.MethodGet, "/runs/id/stream?after_sequence=7", nil)
	c = e.NewContext(req, httptest.NewRecorder())
	n, err = afterSequenceFromSSE(c)
	require.NoError(t, err)
	require.Equal(t, int32(7), n)
	req = httptest.NewRequest(http.MethodGet, "/runs/id/stream", nil)
	req.Header.Set("Last-Event-ID", "9")
	c = e.NewContext(req, httptest.NewRecorder())
	n, err = afterSequenceFromSSE(c)
	require.NoError(t, err)
	require.Equal(t, int32(9), n)
	req = httptest.NewRequest(http.MethodGet, "/runs/id/stream", nil)
	c = e.NewContext(req, httptest.NewRecorder())
	n, err = afterSequenceFromSSE(c)
	require.NoError(t, err)
	require.Equal(t, int32(0), n)
}

func TestRunStartedEventPayloadIncludesConnectionDetails(t *testing.T) {
	userID := uuid.New()
	toolName := "search_docs"

	tests := []struct {
		name          string
		agent         db.Agent
		wantMode      string
		wantTransport string
		wantHost      string
		wantTool      string
	}{
		{
			name: "direct http",
			agent: db.Agent{
				ID:             uuid.New(),
				ConnectionMode: connectionModeDirectHTTP,
				EndpointURL:    "https://agent.example.com/run",
			},
			wantMode:      connectionModeDirectHTTP,
			wantTransport: "http_endpoint",
			wantHost:      "agent.example.com",
		},
		{
			name: "mcp",
			agent: db.Agent{
				ID:             uuid.New(),
				ConnectionMode: connectionModeMCPServer,
				EndpointURL:    "https://mcp.example.com/sse",
				MCPToolName:    &toolName,
			},
			wantMode:      connectionModeMCPServer,
			wantTransport: "mcp_server",
			wantHost:      "mcp.example.com",
			wantTool:      toolName,
		},
		{
			name: "runtime ws",
			agent: db.Agent{
				ID:             uuid.New(),
				ConnectionMode: connectionModeRuntime,
			},
			wantMode:      connectionModeRuntime,
			wantTransport: "runtime",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := runStartedEventPayload(tt.agent, userID)
			require.Equal(t, tt.agent.ID.String(), got["agent_id"])
			require.Equal(t, userID.String(), got["user_id"])
			require.Equal(t, "running", got["status"])
			require.Equal(t, tt.wantMode, got["connection_mode"])
			require.Equal(t, tt.wantTransport, got["transport"])
			if tt.wantHost != "" {
				require.Equal(t, tt.wantHost, got["endpoint_host"])
			}
			if tt.wantTool != "" {
				require.Equal(t, tt.wantTool, got["mcp_tool_name"])
			}
		})
	}
}

func TestRuntimeScopeHelpers(t *testing.T) {
	require.True(t, hasRuntimeScope([]string{"agent:pull"}, "agent:pull"))
	require.False(t, hasRuntimeScope([]string{"agent:call"}, "agent:pull"))
	require.True(t, hasAnyRuntimeScope([]string{"agent:call"}, "agent:pull", "agent:call"))
	require.False(t, hasAnyRuntimeScope([]string{"runs:read"}, "agent:pull", "agent:call"))
}

func TestRuntimeMCPAndLowLevelHelpers(t *testing.T) {
	require.Equal(t, map[string]interface{}{}, normalizeMCPResult(nil))
	require.Equal(t, map[string]interface{}{"answer": 1}, normalizeMCPResult(map[string]interface{}{"output": map[string]interface{}{"answer": 1}}))
	require.Equal(t, map[string]interface{}{"summary": "done"}, normalizeMCPResult(map[string]interface{}{"structuredContent": map[string]interface{}{"summary": "done"}}))
	require.Equal(t, map[string]interface{}{"mcp_result": map[string]interface{}{"content": []interface{}{"raw"}}}, normalizeMCPResult(map[string]interface{}{"content": []interface{}{"raw"}}))

	require.Equal(t, "abc", truncate("abcdef", 3))
	require.Equal(t, "ab", truncate("ab", 3))
	require.True(t, isTimeoutErr(fakeTimeoutErr{timeout: true}))
	require.False(t, isTimeoutErr(fakeTimeoutErr{timeout: false}))
	require.False(t, isTimeoutErr(errors.New("plain error")))
}

func TestRuntimeCallAgentDispatchAndDryRun(t *testing.T) {
	runID := uuid.New()
	userID := uuid.New()
	parentRunID := uuid.New()
	callerAgentID := uuid.New()
	token := "shared-secret"
	var gotDirect AgentRequest
	directClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		require.Equal(t, "shared-secret", r.Header.Get("X-OpenLinker-Token"))
		require.NotEmpty(t, r.Header.Get("X-OpenLinker-Run-Id"))
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotDirect))
		return testHTTPResponse(http.StatusOK, `{"output":{"answer":"direct-ok"},"events":[{"event_type":"run.message.delta","payload":{"text":"working"}}]}`), nil
	})}

	svc := NewService(nil, &config.Config{APIURL: "https://api.example.com"})
	svc.SetHTTPClient(directClient)
	agent := &db.Agent{
		ID:                 uuid.New(),
		EndpointURL:        "https://agent.example/run",
		EndpointAuthHeader: &token,
		ConnectionMode:     connectionModeDirectHTTP,
	}
	output, events, agentErr, callErr := svc.callAgent(context.Background(), agent, runID, userID, &RunRequest{
		Input:    map[string]interface{}{"question": "status"},
		Metadata: map[string]interface{}{"trace_id": "trace-1"},
	}, &Delegation{ParentRunID: parentRunID, CallerAgentID: callerAgentID})
	require.NoError(t, callErr)
	require.Nil(t, agentErr)
	require.Equal(t, "direct-ok", output["answer"])
	require.Len(t, events, 2)
	require.Equal(t, "run.status.changed", events[0].EventType)
	require.Equal(t, parentRunID.String(), gotDirect.ParentRunID)
	require.Equal(t, callerAgentID.String(), gotDirect.CallerAgentID)
	require.NotNil(t, gotDirect.A2A)
	require.Equal(t, runID.String(), gotDirect.A2A.CurrentRunID)

	dryOutput, dryErr := svc.DryRun(context.Background(), agent, map[string]interface{}{"ping": true})
	require.Empty(t, dryErr)
	require.Equal(t, "direct-ok", dryOutput["answer"])

	agent.EndpointURL = "://bad-url"
	dryOutput, dryErr = svc.DryRun(context.Background(), agent, map[string]interface{}{"ping": true})
	require.Nil(t, dryOutput)
	require.Contains(t, dryErr, "endpoint 调用失败")

	svc.SetHTTPClient(&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return testHTTPResponse(http.StatusBadGateway, `{"error":{"code":"UPSTREAM_BAD","message":"upstream refused"}}`), nil
	})})
	agent.EndpointURL = "https://agent.example/error"
	dryOutput, dryErr = svc.DryRun(context.Background(), agent, map[string]interface{}{"ping": true})
	require.Nil(t, dryOutput)
	require.Equal(t, "UPSTREAM_BAD: upstream refused", dryErr)

	agent.ConnectionMode = connectionModeRuntime
	_, _, agentErr, callErr = svc.callAgent(context.Background(), agent, runID, userID, &RunRequest{}, nil)
	require.Error(t, callErr)
	require.Nil(t, agentErr)
	require.Contains(t, callErr.Error(), "runtime")

	agent.ConnectionMode = connectionModeRuntime
	_, _, agentErr, callErr = svc.callAgent(context.Background(), agent, runID, userID, &RunRequest{}, nil)
	require.Error(t, callErr)
	require.Nil(t, agentErr)
	require.Contains(t, callErr.Error(), "runtime")

	agent.ConnectionMode = "unsupported"
	_, _, agentErr, callErr = svc.callAgent(context.Background(), agent, runID, userID, &RunRequest{}, nil)
	require.NoError(t, callErr)
	require.NotNil(t, agentErr)
	require.Equal(t, "UNSUPPORTED_CONNECTION_MODE", agentErr.Code)
}

func TestRuntimeMCPServerProtocolEdges(t *testing.T) {
	runID := uuid.New()
	userID := uuid.New()
	toolName := "lookup"
	bearer := "Bearer mcp-token"
	var got mcpToolCallRequest
	mcpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		require.Equal(t, bearer, r.Header.Get("Authorization"))
		require.Empty(t, r.Header.Get("X-OpenLinker-Token"))
		require.Equal(t, runID.String(), r.Header.Get("X-OpenLinker-Run-Id"))
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		return testHTTPResponse(http.StatusOK, `{"result":{"structuredContent":{"answer":"mcp-ok"}}}`), nil
	})}

	svc := NewService(nil, &config.Config{APIURL: "https://api.example.com"})
	svc.SetHTTPClient(mcpClient)
	agent := &db.Agent{
		ID:                 uuid.New(),
		EndpointURL:        "https://mcp.example/rpc",
		EndpointAuthHeader: &bearer,
		ConnectionMode:     connectionModeMCPServer,
		MCPToolName:        &toolName,
	}
	output, events, agentErr, callErr := svc.callAgent(context.Background(), agent, runID, userID, &RunRequest{
		Input:    map[string]interface{}{"city": "shanghai"},
		Metadata: map[string]interface{}{"trace_id": "trace-mcp"},
	}, &Delegation{ParentRunID: uuid.New(), CallerAgentID: uuid.New()})
	require.NoError(t, callErr)
	require.Nil(t, agentErr)
	require.Nil(t, events)
	require.Equal(t, "mcp-ok", output["answer"])
	require.Equal(t, "2.0", got.JSONRPC)
	require.Equal(t, "tools/call", got.Method)
	require.Equal(t, toolName, got.Params.Name)
	require.Equal(t, "shanghai", got.Params.Arguments["city"])
	require.Equal(t, "trace-mcp", got.Params.Metadata["trace_id"])
	require.NotNil(t, got.Params.Metadata["a2a"])

	agent.MCPToolName = nil
	_, _, agentErr, callErr = svc.callAgent(context.Background(), agent, runID, userID, &RunRequest{}, nil)
	require.NoError(t, callErr)
	require.NotNil(t, agentErr)
	require.Equal(t, "MCP_TOOL_MISSING", agentErr.Code)

	svc.SetHTTPClient(&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return testHTTPResponse(http.StatusOK, `{"error":{"code":-32001,"message":"tool failed"}}`), nil
	})})
	agent.MCPToolName = &toolName
	agent.EndpointURL = "https://mcp.example/error"
	_, _, agentErr, callErr = svc.callAgent(context.Background(), agent, runID, userID, &RunRequest{}, nil)
	require.NoError(t, callErr)
	require.NotNil(t, agentErr)
	require.Equal(t, "MCP_-32001", agentErr.Code)

	svc.SetHTTPClient(&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return testHTTPResponse(http.StatusServiceUnavailable, `not-json`), nil
	})})
	agent.EndpointURL = "https://mcp.example/invalid"
	_, _, agentErr, callErr = svc.callAgent(context.Background(), agent, runID, userID, &RunRequest{}, nil)
	require.NoError(t, callErr)
	require.NotNil(t, agentErr)
	require.Equal(t, "INVALID_MCP_RESPONSE", agentErr.Code)

	plainAuth := "mcp-secret"
	svc.SetHTTPClient(&http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		require.Empty(t, r.Header.Get("Authorization"))
		require.Equal(t, plainAuth, r.Header.Get("X-OpenLinker-Token"))
		return testHTTPResponse(http.StatusBadGateway, `{"result":{"output":{"ignored":true}}}`), nil
	})})
	agent.EndpointAuthHeader = &plainAuth
	agent.EndpointURL = "https://mcp.example/http-error"
	_, _, agentErr, callErr = svc.callAgent(context.Background(), agent, runID, userID, &RunRequest{}, nil)
	require.NoError(t, callErr)
	require.NotNil(t, agentErr)
	require.Equal(t, "HTTP_502", agentErr.Code)
}

func TestA2AContextAndRequirementEvidenceHelpers(t *testing.T) {
	runID := uuid.New()
	parentRunID := uuid.New()
	callerAgentID := uuid.New()
	taskID := uuid.New()
	agentID := uuid.New()
	userID := uuid.New()
	svc := NewService(nil, &config.Config{APIURL: " https://api.example.com/ "})

	ctx := svc.agentA2AContext(runID, &Delegation{ParentRunID: parentRunID, CallerAgentID: callerAgentID})
	require.Equal(t, runID.String(), ctx.CurrentRunID)
	require.Equal(t, parentRunID.String(), ctx.ParentRunID)
	require.Equal(t, callerAgentID.String(), ctx.CallerAgentID)

	m := agentA2AContextMap(ctx)
	require.Equal(t, runID.String(), m["current_run_id"])
	require.Equal(t, parentRunID.String(), m["parent_run_id"])
	require.Nil(t, agentA2AContextMap(nil))

	id, ok, err := taskIDFromRunMetadata(nil)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, uuid.Nil, id)
	id, ok, err = taskIDFromRunMetadata(map[string]interface{}{"task_id": " "})
	require.NoError(t, err)
	require.False(t, ok)
	id, ok, err = taskIDFromRunMetadata(map[string]interface{}{"task_id": taskID.String()})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, taskID, id)
	_, _, err = taskIDFromRunMetadata(map[string]interface{}{"task_id": "bad"})
	require.Error(t, err)

	require.Equal(t, []string{"a", "b"}, uniqueStrings([]string{" a ", "", "b", "a"}))
	require.Equal(t, []string{"run_agent", "get_run"}, normalizeUsedMCPTools(map[string]interface{}{"used_mcp_tools": []interface{}{"run_agent", "get_run", "run_agent"}}, "web"))
	require.Equal(t, []string{"x", "run_agent"}, normalizeUsedMCPTools(map[string]interface{}{"used_mcp_tools": "x"}, "mcp"))
	require.Nil(t, stringListFromMetadata(nil))
	require.Equal(t, []string{"a", "b"}, stringListFromMetadata([]string{"a", "b"}))
	require.Equal(t, []string{" a ", "2", "true"}, stringListFromMetadata([]interface{}{" a ", 2, true}))
	require.Nil(t, stringListFromMetadata(" "))
	require.Equal(t, []string{"1"}, stringListFromMetadata(1))
	matched, missing := splitCoverage([]string{"a", "b"}, []string{"b", "c"})
	require.Equal(t, []string{"b"}, matched)
	require.Equal(t, []string{"a"}, missing)
	require.Equal(t, "no_requirements", coverageStatus(nil, nil, nil, nil, nil, nil))
	require.Equal(t, "covered", coverageStatus([]string{"a"}, []string{"run_agent"}, []string{"a"}, nil, []string{"run_agent"}, nil))
	require.Equal(t, "partial", coverageStatus([]string{"a", "b"}, nil, []string{"a"}, []string{"b"}, nil, nil))
	require.Equal(t, "missing_requirements", coverageStatus([]string{"a"}, nil, nil, []string{"a"}, nil, nil))

	chosenAgent := agentID
	task := db.TaskQuery{UserID: userID, ChosenAgentID: &chosenAgent}
	require.NoError(t, requireTaskRunAssociation(task, userID, agentID))
	require.Error(t, requireTaskRunAssociation(task, uuid.New(), agentID))
	otherAgent := uuid.New()
	require.Error(t, requireTaskRunAssociation(task, userID, otherAgent))
	task = db.TaskQuery{UserID: userID, RecommendedAgentIDs: []uuid.UUID{agentID}}
	require.Error(t, requireTaskRunAssociation(task, userID, agentID))
	require.True(t, uuidInList(agentID, []uuid.UUID{agentID}))
	require.False(t, uuidInList(agentID, []uuid.UUID{otherAgent}))

	snapshot := &runRequirementSnapshot{
		TaskID:           taskID,
		AgentID:          agentID,
		UserID:           userID,
		RequiredSkills:   []string{"data/sql"},
		RequiredMCPTools: []string{"run_agent"},
		AgentSkills:      []string{"data/sql", "data/analysis"},
		MatchedSkills:    []string{"data/sql"},
		UsedMCPTools:     []string{"run_agent"},
		CoverageStatus:   "covered",
		EvidenceSource:   "mcp",
	}
	params := snapshot.createParams(runID)
	require.Equal(t, runID, params.RunID)
	require.Equal(t, []string{"data/sql"}, params.RequiredSkillIDs)

	created := time.Date(2026, 6, 20, 12, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	evidence := db.RunRequirementEvidence{
		RunID:            runID,
		TaskID:           taskID,
		AgentID:          agentID,
		UserID:           userID,
		RequiredSkillIDs: []string{"data/sql"},
		RequiredMCPTools: []string{"run_agent"},
		AgentSkillIDs:    []string{"data/sql"},
		MatchedSkillIDs:  []string{"data/sql"},
		UsedMCPTools:     []string{"run_agent"},
		CoverageStatus:   "covered",
		EvidenceSource:   "mcp",
		CreatedAt:        created,
	}
	payload := runRequirementEvidencePayload(evidence)
	require.Equal(t, taskID.String(), payload["task_id"])
	require.Equal(t, "2026-06-20T04:00:00Z", payload["evidence_created_at"])
	resp := runRequirementEvidenceToResponse(evidence)
	require.Equal(t, runID.String(), resp.RunID)
	resp.RequiredSkillIDs[0] = "changed"
	require.Equal(t, []string{"data/sql"}, evidence.RequiredSkillIDs)
}

func TestRuntimeA2AQueryErrorEdge(t *testing.T) {
	ctx := context.Background()
	runID := uuid.New()

	svc := &Service{queries: db.New(&runtimeFakeDBTX{rows: []runtimeFakeRow{{err: errors.New("delegation store down")}}})}
	a2a := svc.agentA2AContextForRun(ctx, runID)
	require.Equal(t, runID.String(), a2a.CurrentRunID)
	require.Empty(t, a2a.ParentRunID)
	require.Empty(t, a2a.CallerAgentID)
}

func TestRuntimeRequirementSnapshotQueries(t *testing.T) {
	ctx := context.Background()
	userID := uuid.New()
	agentID := uuid.New()
	taskID := uuid.New()
	q := &fakeRunRequirementQueries{
		task: db.TaskQuery{
			ID:                  taskID,
			UserID:              userID,
			ChosenAgentID:       &agentID,
			ParsedSkills:        []string{"data/sql", "content/summary", "data/sql"},
			MCPTools:            []string{"run_agent", "files/export", "run_agent"},
			RecommendedAgentIDs: []uuid.UUID{agentID},
		},
		skills: []db.Skill{
			{ID: "data/sql"},
			{ID: "data/analysis"},
			{ID: "data/sql"},
		},
	}
	svc := &Service{requirements: q}
	req := &RunRequest{Metadata: map[string]interface{}{
		"task_id":        taskID.String(),
		"used_mcp_tools": []interface{}{"files/export", "files/export"},
	}}

	snapshot, err := svc.buildRunRequirementSnapshot(ctx, userID, agentID, req, "mcp")
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	require.Equal(t, taskID, q.taskID)
	require.Equal(t, agentID, q.agentID)
	require.Equal(t, []string{"data/sql", "content/summary"}, snapshot.RequiredSkills)
	require.Equal(t, []string{"run_agent", "files/export"}, snapshot.RequiredMCPTools)
	require.Equal(t, []string{"data/sql", "data/analysis"}, snapshot.AgentSkills)
	require.Equal(t, []string{"data/sql"}, snapshot.MatchedSkills)
	require.Equal(t, []string{"content/summary"}, snapshot.MissingSkills)
	require.Equal(t, []string{"files/export", "run_agent"}, snapshot.UsedMCPTools)
	require.Empty(t, snapshot.MissingMCPTools)
	require.Equal(t, "partial", snapshot.CoverageStatus)
	require.Equal(t, "mcp", snapshot.EvidenceSource)

	withoutTask, err := (&Service{requirements: &fakeRunRequirementQueries{}}).buildRunRequirementSnapshot(ctx, userID, agentID, &RunRequest{}, "web")
	require.NoError(t, err)
	require.Nil(t, withoutTask)

	_, err = (&Service{requirements: &fakeRunRequirementQueries{taskErr: pgx.ErrNoRows}}).buildRunRequirementSnapshot(ctx, userID, agentID, req, "web")
	var httpErr *httpx.HTTPError
	require.True(t, errors.As(err, &httpErr))
	require.Equal(t, http.StatusNotFound, httpErr.Status)

	_, err = (&Service{requirements: &fakeRunRequirementQueries{taskErr: errors.New("task store down")}}).buildRunRequirementSnapshot(ctx, userID, agentID, req, "web")
	require.True(t, errors.As(err, &httpErr))
	require.Equal(t, http.StatusInternalServerError, httpErr.Status)

	_, err = (&Service{requirements: &fakeRunRequirementQueries{task: q.task, skillsErr: errors.New("skill store down")}}).buildRunRequirementSnapshot(ctx, userID, agentID, req, "web")
	require.True(t, errors.As(err, &httpErr))
	require.Equal(t, http.StatusInternalServerError, httpErr.Status)

	require.Nil(t, (&Service{}).requirementQueries())
}

func TestRuntimeAttachRequirementEvidenceFromQueries(t *testing.T) {
	ctx := context.Background()
	runID := uuid.New()
	taskID := uuid.New()
	agentID := uuid.New()
	userID := uuid.New()
	created := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	q := &fakeRunRequirementQueries{
		evidence: db.RunRequirementEvidence{
			RunID:            runID,
			TaskID:           taskID,
			AgentID:          agentID,
			UserID:           userID,
			RequiredSkillIDs: []string{"data/sql"},
			MatchedSkillIDs:  []string{"data/sql"},
			CoverageStatus:   "covered",
			EvidenceSource:   "api",
			CreatedAt:        created,
		},
	}
	resp := &RunResponse{}

	(&Service{requirements: q}).attachRunRequirementEvidence(ctx, runID, resp)
	require.Equal(t, runID, q.runID)
	require.NotNil(t, resp.RequirementEvidence)
	require.Equal(t, taskID.String(), resp.RequirementEvidence.TaskID)
	require.Equal(t, "covered", resp.RequirementEvidence.CoverageStatus)

	missing := &RunResponse{}
	(&Service{requirements: &fakeRunRequirementQueries{evidenceErr: pgx.ErrNoRows}}).attachRunRequirementEvidence(ctx, runID, missing)
	require.Nil(t, missing.RequirementEvidence)
	errored := &RunResponse{}
	(&Service{requirements: &fakeRunRequirementQueries{evidenceErr: errors.New("evidence store down")}}).attachRunRequirementEvidence(ctx, runID, errored)
	require.Nil(t, errored.RequirementEvidence)
	(&Service{requirements: q}).attachRunRequirementEvidence(ctx, runID, nil)
}

func TestRuntimeServiceDependencySettersAndTaskCallbackTrigger(t *testing.T) {
	svc := &Service{}
	taskCallback := &recordingTaskCallbackEnqueuer{events: make(chan db.RunEvent, 1)}

	svc.SetTaskCallbackEnqueuer(taskCallback)
	require.Equal(t, taskCallback, svc.taskCallbackSvc)

	svc.triggerTaskCallbackEvent(nil)
	event := &db.RunEvent{ID: uuid.New(), RunID: uuid.New(), EventType: "run.message.delta"}
	svc.triggerTaskCallbackEvent(event)
	select {
	case got := <-taskCallback.events:
		require.Equal(t, event.ID, got.ID)
		require.Equal(t, event.RunID, got.RunID)
	case <-time.After(time.Second):
		t.Fatal("task callback event was not enqueued")
	}
}

func TestRuntimeResponseAndNextActionHelpers(t *testing.T) {
	runID := uuid.New()
	agentID := uuid.New()
	latestAttemptID := uuid.New()
	activeAttemptID := uuid.New()
	replayOfRunID := uuid.New()
	duration := int32(123)
	errCode := "AGENT_ERROR"
	errMsg := "failed"
	cancelState := "stopping"
	cancelReason := "operator requested"
	connectionMode := "runtime"
	nextAttemptAt := time.Date(2026, 7, 12, 2, 0, 0, 0, time.UTC)
	cancelRequestedAt := nextAttemptAt.Add(-time.Minute)
	cancelAcknowledgedAt := nextAttemptAt.Add(-30 * time.Second)
	deadLetteredAt := nextAttemptAt.Add(time.Minute)
	outputJSON := []byte(`{"answer":42,"next_action":{"label":"Ship","hint":"Deploy it","href":"/deploy","method":"POST","resource_type":"task","resource_id":"t1","type":"deploy"}}`)

	success := runToResponse(&db.Run{
		ID: runID, AgentID: agentID, Status: "success", Output: outputJSON,
		CostCents: 99, DurationMs: &duration, Source: "mcp",
		RuntimeContractID: RuntimeContractID, DispatchState: "retry_wait",
		AttemptCount: 2, MaxAttempts: 3, NextAttemptAt: &nextAttemptAt,
		LatestAttemptID: &latestAttemptID, ActiveAttemptID: &activeAttemptID,
		CancelState: &cancelState, CancelRequestedAt: &cancelRequestedAt,
		CancelAcknowledgedAt: &cancelAcknowledgedAt, CancelReason: &cancelReason,
		DeadLetteredAt: &deadLetteredAt, ReplayOfRunID: &replayOfRunID,
		ConnectionModeSnapshot: &connectionMode,
	})
	require.Equal(t, "success", success.Status)
	require.Equal(t, int32(99), success.CostCents)
	require.Equal(t, float64(42), success.Output["answer"])
	require.Equal(t, "deploy", success.NextAction.Type)
	require.Equal(t, "Ship", success.NextAction.Label)
	require.Equal(t, RuntimeContractID, success.RuntimeContractID)
	require.Equal(t, "retry_wait", success.DispatchState)
	require.Equal(t, int32(2), success.AttemptCount)
	require.Equal(t, int32(3), success.MaxAttempts)
	require.Equal(t, latestAttemptID.String(), success.LatestAttemptID)
	require.Equal(t, activeAttemptID.String(), success.ActiveAttemptID)
	require.Equal(t, "stopping", success.CancelState)
	require.Equal(t, replayOfRunID.String(), success.ReplayOfRunID)
	require.Equal(t, "runtime", success.AgentConnectionMode)

	failed := runToResponse(&db.Run{ID: runID, Status: "timeout", CostCents: 99, ErrorCode: &errCode, ErrorMessage: &errMsg, DurationMs: &duration})
	require.Equal(t, int32(0), failed.CostCents)
	require.Equal(t, "检查超时并重试", failed.NextAction.Label)
	require.Equal(t, "AGENT_ERROR", failed.NextAction.AdditionalProps["error_code"])

	running := runToResponse(&db.Run{ID: runID, Status: "running", CostCents: 12})
	require.Equal(t, "wait", running.NextAction.Type)
	require.Contains(t, running.NextAction.Href, runID.String())

	delegated := nextActionForSuccess(map[string]interface{}{"next_action": "ignored"}, runID.String(), "free_delegation")
	require.Equal(t, "return_to_parent", delegated.Type)
	require.Equal(t, runID.String(), delegated.ResourceID)
	require.Equal(t, "review_output", nextActionForSuccess(nil, "", "").Type)
	suggested, ok := nextActionFromOutput(map[string]interface{}{"next_action": " Review the result "})
	require.True(t, ok)
	require.Equal(t, "agent_suggested", suggested.Type)
	require.Equal(t, "Review the result", suggested.Hint)
	emptySuggestion, ok := nextActionFromOutput(map[string]interface{}{"next_action": " "})
	require.False(t, ok)
	require.Nil(t, emptySuggestion)
	missingSuggestion, ok := nextActionFromOutput(map[string]interface{}{})
	require.False(t, ok)
	require.Nil(t, missingSuggestion)
	described, ok := nextActionFromOutput(map[string]interface{}{"next_action": map[string]interface{}{"description": "Use result", "method": 123}})
	require.True(t, ok)
	require.Equal(t, "执行 Agent 建议", described.Label)
	require.Equal(t, "Use result", described.Hint)
	require.Equal(t, "123", described.Method)
	labelOnly, ok := nextActionFromOutput(map[string]interface{}{"next_action": map[string]interface{}{
		"label":         "Open run",
		"href":          "/run/123",
		"resource_type": "run",
		"resource_id":   "123",
	}})
	require.True(t, ok)
	require.Equal(t, "Open run", labelOnly.Label)
	require.Equal(t, "Open run", labelOnly.Hint)
	require.Equal(t, "run", labelOnly.ResourceType)
	require.Equal(t, "123", labelOnly.ResourceID)
	emptyAction, ok := nextActionFromOutput(map[string]interface{}{"next_action": map[string]interface{}{"label": " "}})
	require.False(t, ok)
	require.Nil(t, emptyAction)
	require.Nil(t, nextActionFromUnsupportedOutput(t))
	resp := &RunResponse{Status: "canceled"}
	decorateNextAction(resp)
	require.Nil(t, resp.NextAction)
	decorateNextAction(nil)
	require.Equal(t, "fallback", coalesceString(" ", "fallback"))
	require.Equal(t, "value", coalesceString(" value ", "fallback"))
	require.Equal(t, "7", stringFromMap(map[string]interface{}{"count": 7}, "count"))
	require.Equal(t, "", stringPtrValue(nil))
	require.Equal(t, "value", *stringPtrOrNil(" value "))
	require.Nil(t, stringPtrOrNil(" "))

	wait := queuedRuntimeWaitingNextAction("run-1", agentID)
	require.Equal(t, "start_runtime_worker", wait.Type)
	require.Equal(t, agentID.String(), wait.AdditionalProps["agent_id"])
}

func TestRuntimeArtifactMessageAndEventHelpers(t *testing.T) {
	runID := uuid.New()
	artifactID := uuid.New()
	created := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	source := "artifact-1"
	mimeType := "application/json"
	fileURI := "s3://bucket/file.json"
	fileName := "file.json"
	fileSHA := strings.Repeat("a", 64)
	size := int64(42)

	artifact := runArtifactToResponse(db.RunArtifact{
		ID:               artifactID,
		RunID:            runID,
		ArtifactType:     "file",
		Title:            "File",
		Content:          []byte(`{"ok":true}`),
		Visibility:       "shared",
		SourceArtifactID: &source,
		MimeType:         &mimeType,
		FileUri:          &fileURI,
		FileName:         &fileName,
		FileSha256:       &fileSHA,
		FileSizeBytes:    &size,
		CreatedAt:        created,
	})
	require.Equal(t, artifactID.String(), artifact.ID)
	require.Equal(t, true, artifact.Content["ok"])
	require.Equal(t, fileURI, artifact.FileURI)
	require.Equal(t, &size, artifact.FileSizeBytes)
	require.Equal(t, map[string]interface{}{"raw": "not-json"}, runArtifactToResponse(db.RunArtifact{Content: []byte("not-json")}).Content)

	seq := int32(3)
	message := runMessageToResponse(db.RunMessage{ID: uuid.New(), RunID: runID, EventSequence: &seq, Role: "agent", Content: "hello", Payload: []byte(`{"text":"hello"}`), CreatedAt: created})
	require.Equal(t, &seq, message.EventSequence)
	require.Equal(t, "hello", message.Payload["text"])
	require.Equal(t, map[string]interface{}{"raw": "bad"}, runMessageToResponse(db.RunMessage{Payload: []byte("bad")}).Payload)

	parentID := uuid.New()
	event := runEventToResponse(db.RunEvent{ID: uuid.New(), RunID: runID, ParentRunID: &parentID, Sequence: 9, EventType: "run.completed", Payload: []byte(`{"status":"success"}`), CreatedAt: created})
	require.Equal(t, parentID.String(), event.ParentRunID)
	require.Equal(t, "success", event.Payload["status"])
	require.Empty(t, runEventToResponse(db.RunEvent{Payload: []byte("bad")}).Payload)

	require.Equal(t, "hello", messageContentFromMap(map[string]interface{}{"text": " hello "}))
	require.Equal(t, "summary", messageContentFromMap(map[string]interface{}{"text": " ", "summary": " summary "}))
	require.Equal(t, "prompt", messageContentFromMap(map[string]interface{}{"content": nil, "prompt": " prompt "}))
	require.Equal(t, `{"value":3}`, messageContentFromMap(map[string]interface{}{"value": 3}))
	require.Equal(t, "", messageContentFromMap(map[string]interface{}{"bad": func() {}}))
	require.Equal(t, "", messageContentFromMap(nil))
	require.Len(t, []rune(truncateRunMessageContent(strings.Repeat("数", maxRunMessageContentLen+1))), maxRunMessageContentLen)
	require.True(t, constantTimeEqual("secret", "secret"))
	require.False(t, constantTimeEqual("secret", "other"))
	require.False(t, constantTimeEqual("secret", "secret2"))
}

func TestConversationContextFromMappingBuildsHistory(t *testing.T) {
	ctx := context.Background()
	userID := uuid.New()
	agentID := uuid.New()
	previousRunID := uuid.New()
	currentRunID := uuid.New()
	now := time.Date(2026, 7, 5, 12, 0, 0, 0, time.UTC)
	seq := int32(2)

	dbtx := &runtimeFakeDBTX{queryRows: []pgx.Rows{
		&runtimeFakeRows{rows: []runtimeFakeRow{{
			values: []any{
				uuid.New(), previousRunID, userID, agentID,
				"conv-1", "turn-1", "conv-1", "", "",
				nil, nil, nil, "trace-1", []string{"ref-1"}, "a2a_protocol",
				now.Add(-time.Minute), now.Add(-time.Minute),
			},
		}}},
		&runtimeFakeRows{rows: []runtimeFakeRow{
			{values: []any{uuid.New(), previousRunID, (*int32)(nil), "user", "first question", []byte(`{"message":"first question"}`), now.Add(-time.Minute)}},
			{values: []any{uuid.New(), previousRunID, &seq, "agent", "first answer", []byte(`{"text":"first answer"}`), now.Add(-30 * time.Second)}},
		}},
	}}
	svc := &Service{queries: db.New(dbtx)}
	conversation := svc.conversationContextFromMapping(ctx, db.A2AContextMapping{
		ID:                uuid.New(),
		RunID:             currentRunID,
		UserID:            userID,
		AgentID:           agentID,
		ProtocolContextID: "conv-1",
		ProtocolTaskID:    "turn-2",
		RootContextID:     "conv-1",
		TraceID:           "trace-1",
		Source:            "a2a_protocol",
		CreatedAt:         now,
		UpdatedAt:         now,
	})

	require.NotNil(t, conversation)
	require.Equal(t, "conv-1", conversation.SessionKey)
	require.Equal(t, currentRunID.String(), conversation.CurrentRunID)
	require.Equal(t, "turn-2", conversation.CurrentProtocolTask)
	require.False(t, conversation.Truncated)
	require.Len(t, conversation.HistoryBeforeCurrent, 2)
	require.Equal(t, "user", conversation.HistoryBeforeCurrent[0].Role)
	require.Equal(t, "first question", conversation.HistoryBeforeCurrent[0].Content)
	require.Equal(t, "agent", conversation.HistoryBeforeCurrent[1].Role)
	require.Equal(t, "first answer", conversation.HistoryBeforeCurrent[1].Payload["text"])
}

func TestTrustedRunMetadataRejectsCallerOwnedSessionFields(t *testing.T) {
	original := map[string]interface{}{
		"tenant":       "seller-research",
		"a2a":          map[string]interface{}{"root_context_id": "spoofed"},
		"conversation": map[string]interface{}{"session_key": "spoofed", "source": "caller"},
	}

	trusted := trustedRunMetadata(original)
	require.Equal(t, "seller-research", trusted["tenant"])
	require.NotContains(t, trusted, "a2a")
	require.NotContains(t, trusted, "conversation")
	require.Contains(t, original, "a2a", "sanitizing metadata must not mutate the caller request")
	require.Contains(t, original, "conversation", "sanitizing metadata must not mutate the caller request")
}

func TestAgentA2AContextUsesTypedMessageAndTrustedCreationSource(t *testing.T) {
	runID := uuid.New()
	svc := &Service{}
	req := &RunRequest{
		Metadata: map[string]interface{}{
			"a2a": map[string]interface{}{
				"message_id": "spoofed-message",
				"protocol":   "spoofed-protocol",
				"method":     "spoofed-method",
			},
		},
		A2AContext: &RunA2AContextRequest{
			MessageID:         "message-42",
			ProtocolContextID: "context-1",
			ProtocolTaskID:    "task-1",
		},
		CreationProtocol: "a2a",
		CreationMethod:   "message.send",
	}

	ctx := svc.agentA2AContextForRequest(runID, nil, req)
	require.Equal(t, "message-42", ctx.MessageID)
	require.Equal(t, "a2a", ctx.Protocol)
	require.Equal(t, "message.send", ctx.Method)
	require.Equal(t, "context-1", ctx.ProtocolContextID)
	require.Equal(t, "task-1", ctx.ProtocolTaskID)

	mapped := agentA2AContextMap(ctx)
	require.Equal(t, "message-42", mapped["message_id"])
	require.Equal(t, "a2a", mapped["protocol"])
	require.Equal(t, "message.send", mapped["method"])
	require.NotEqual(t, "spoofed-message", mapped["message_id"])
}

func TestReplayA2AContextPreservesCoreOwnedConversation(t *testing.T) {
	parentRunID := uuid.New()
	callerAgentID := uuid.New()
	targetAgentID := uuid.New()
	mapping := db.A2AContextMapping{
		ProtocolContextID: "conversation-1", ProtocolTaskID: "task-1", RootContextID: "conversation-1",
		ParentContextID: "parent-context", ParentTaskID: "parent-task", ParentRunID: &parentRunID,
		CallerAgentID: &callerAgentID, TargetAgentID: &targetAgentID, TraceID: "trace-1",
		ReferenceTaskIDs: []string{"ref-1"}, Source: "a2a_protocol",
	}

	replayed := replayA2AContext(mapping)
	require.Equal(t, "conversation-1", replayed.ProtocolContextID)
	require.Equal(t, "conversation-1", replayed.RootContextID)
	require.Equal(t, "task-1", replayed.ProtocolTaskID)
	require.Equal(t, parentRunID.String(), replayed.ParentRunID)
	require.Equal(t, callerAgentID.String(), replayed.CallerAgentID)
	require.Equal(t, targetAgentID.String(), replayed.TargetAgentID)
	require.Equal(t, []string{"ref-1"}, replayed.ReferenceTaskIDs)
	mapping.ReferenceTaskIDs[0] = "mutated"
	require.Equal(t, []string{"ref-1"}, replayed.ReferenceTaskIDs)
}

func TestRuntimePersistenceHelperEdges(t *testing.T) {
	ctx := context.Background()
	runID := uuid.New()
	seq := int32(5)
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	messageID := uuid.New()

	dbtx := &runtimeFakeDBTX{rows: []runtimeFakeRow{{
		values: []any{messageID, runID, &seq, "agent", "trimmed", []byte(`{}`), now},
	}}}
	q := db.New(dbtx)
	require.NoError(t, createRunMessage(ctx, q, runID, &seq, "", "  trimmed  ", nil))
	require.Len(t, dbtx.queryRowArgs, 1)
	require.Equal(t, "agent", dbtx.queryRowArgs[0][2])
	require.Equal(t, "trimmed", dbtx.queryRowArgs[0][3])
	require.JSONEq(t, `{}`, string(dbtx.queryRowArgs[0][4].([]byte)))

	require.Error(t, createRunMessage(ctx, q, runID, nil, "agent", "bad", map[string]interface{}{"bad": func() {}}))
	_, err := createRunEventRecord(ctx, q, runID, nil, "run.message.delta", map[string]interface{}{"bad": func() {}})
	require.Error(t, err)

	errorDB := db.New(&runtimeFakeDBTX{rows: []runtimeFakeRow{{err: errors.New("insert failed")}}})
	require.Error(t, createRunMessage(ctx, errorDB, runID, nil, "agent", "ok", map[string]interface{}{}))

	svc := &Service{queries: db.New(&runtimeFakeDBTX{rows: []runtimeFakeRow{{err: errors.New("event insert failed")}}})}
	require.Nil(t, svc.recordRunEventBestEffort(ctx, runID, "run.completed", map[string]interface{}{"status": "success"}))
	svc.recordRunMessageBestEffort(ctx, runID, nil, "", "", map[string]interface{}{"bad": func() {}})
}

func TestRuntimeReadServiceQueryErrorEdges(t *testing.T) {
	ctx := context.Background()
	runID := uuid.New()
	userID := uuid.New()
	agentID := uuid.New()

	for name, call := range map[string]func(*Service) error{
		"get run": func(s *Service) error {
			_, err := s.GetRun(ctx, userID, runID)
			return err
		},
		"list events": func(s *Service) error {
			_, err := s.ListRunEvents(ctx, userID, runID, 0, 10)
			return err
		},
		"list artifacts": func(s *Service) error {
			_, err := s.ListRunArtifacts(ctx, userID, runID)
			return err
		},
		"list messages": func(s *Service) error {
			_, err := s.ListRunMessages(ctx, userID, runID)
			return err
		},
	} {
		t.Run(name+" get run error", func(t *testing.T) {
			svc := &Service{queries: db.New(&runtimeFakeDBTX{rows: []runtimeFakeRow{{err: errors.New("run store down")}}})}
			requireRuntimeHTTPStatus(t, call(svc), http.StatusInternalServerError)
		})
	}

	runRow := runtimeRunRowValues(runID, userID, agentID, "running")
	for name, call := range map[string]func(*Service) error{
		"events query error": func(s *Service) error {
			_, err := s.ListRunEvents(ctx, userID, runID, 0, 10)
			return err
		},
		"artifacts query error": func(s *Service) error {
			_, err := s.ListRunArtifacts(ctx, userID, runID)
			return err
		},
		"messages query error": func(s *Service) error {
			_, err := s.ListRunMessages(ctx, userID, runID)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			dbtx := &runtimeFakeDBTX{
				rows:     []runtimeFakeRow{{values: runRow}},
				queryErr: errors.New("list store down"),
			}
			svc := &Service{queries: db.New(dbtx)}
			requireRuntimeHTTPStatus(t, call(svc), http.StatusInternalServerError)
			require.Equal(t, 1, dbtx.queryCalls)
		})
	}
}

func TestRuntimeArtifactDraftHelpers(t *testing.T) {
	size := int64(123)
	fileSHA := strings.Repeat("b", 64)
	require.Error(t, createRunArtifacts(context.Background(), nil, uuid.New(), map[string]interface{}{
		"artifact": map[string]interface{}{
			"title":   "Bad artifact",
			"content": map[string]interface{}{"bad": func() {}},
		},
	}))

	draft := artifactDraftFromMap(map[string]interface{}{
		"title":           "  Report  ",
		"type":            "file",
		"visibility":      "public_example",
		"content":         map[string]interface{}{"text": "hello"},
		"file_uri":        "https://example.com/report.pdf",
		"file_name":       "report.pdf",
		"mime_type":       "application/pdf",
		"file_sha256":     fileSHA,
		"file_size_bytes": float64(size),
	}, "fallback")
	require.Equal(t, "file", draft.ArtifactType)
	require.Equal(t, "Report", draft.Title)
	require.Equal(t, "public_example", draft.Visibility)
	require.Equal(t, "https://example.com/report.pdf", draft.FileURI)
	require.Equal(t, size, *draft.FileSizeBytes)
	require.Equal(t, size, draft.Content["file_size_bytes"])

	items := runArtifactsFromOutput(map[string]interface{}{"artifacts": []interface{}{map[string]interface{}{"title": "A"}, "raw"}})
	require.Len(t, items, 2)
	require.Equal(t, "A", items[0].Title)
	require.Equal(t, map[string]interface{}{"value": "raw"}, items[1].Content)
	identified := runArtifactsFromOutput(map[string]interface{}{"artifact": map[string]interface{}{
		"artifact_id": " final-evidence ",
		"content":     map[string]interface{}{"ok": true},
	}})
	require.Equal(t, "final-evidence", identified[0].SourceArtifactID)
	require.ErrorContains(t, createRunArtifacts(context.Background(), nil, uuid.New(), map[string]interface{}{
		"artifacts": []interface{}{
			map[string]interface{}{"artifact_id": "duplicate", "content": map[string]interface{}{}},
			map[string]interface{}{"artifact_id": " duplicate ", "content": map[string]interface{}{}},
		},
	}), "duplicate result artifact_id")
	require.Equal(t, "Agent 产物", runArtifactsFromOutput(map[string]interface{}{"artifact": map[string]interface{}{"data": map[string]interface{}{"x": 1}}})[0].Title)
	require.Equal(t, "Agent 输出", runArtifactsFromOutput(map[string]interface{}{"answer": 1})[0].Title)
	require.Equal(t, map[string]interface{}{}, runArtifactsFromOutput(nil)[0].Content)
	require.True(t, runArtifactsFromOutput(map[string]interface{}{"answer": 1})[0].Fallback)
	invalidDraft := artifactDraftFromMap(map[string]interface{}{
		"title":      " ",
		"type":       "html",
		"visibility": "world",
		"data":       map[string]interface{}{"file_uri": "https://example.com/nested.csv", "name": "nested.csv"},
	}, "Fallback")
	require.Equal(t, "json", invalidDraft.ArtifactType)
	require.Equal(t, "private", invalidDraft.Visibility)
	require.Equal(t, "Fallback", invalidDraft.Title)
	require.Equal(t, "https://example.com/nested.csv", invalidDraft.FileURI)
	require.Equal(t, "nested.csv", invalidDraft.FileName)
	explicitParts := []interface{}{"part-a", map[string]interface{}{"text": "part-b"}}
	require.Equal(t, explicitParts, artifactDeltaPartsFromPayload(map[string]interface{}{"parts": explicitParts}))
	require.Equal(t, []interface{}{map[string]interface{}{"type": "data", "data": map[string]interface{}{"value": 1}}}, artifactDeltaPartsFromPayload(map[string]interface{}{"content": map[string]interface{}{"value": 1}}))
	require.Equal(t, []interface{}{map[string]interface{}{"type": "data", "data": []interface{}{"x"}}}, artifactDeltaPartsFromPayload(map[string]interface{}{"data": []interface{}{"x"}}))
	require.Equal(t, []interface{}{map[string]interface{}{"type": "text", "text": "hello"}}, artifactDeltaPartsFromPayload(map[string]interface{}{"message": "hello"}))

	delta := artifactDeltaDraftFromPayload(map[string]interface{}{
		"artifact_id": "stream-1",
		"text":        "hello",
		"append":      false,
		"last_chunk":  true,
		"file_uri":    "https://example.com/out.txt",
	})
	require.Equal(t, "stream-1", delta.SourceArtifactID)
	require.Equal(t, "file", delta.ArtifactType)
	require.False(t, delta.Append)
	require.True(t, delta.LastChunk)
	require.Equal(t, []interface{}{map[string]interface{}{"type": "text", "text": "hello"}}, delta.Parts)
	defaultDelta := artifactDeltaDraftFromPayload(map[string]interface{}{
		"id":         "raw id",
		"type":       "bad",
		"visibility": "world",
		"parts": []interface{}{map[string]interface{}{
			"file": map[string]interface{}{"url": "https://files.example/default.bin", "contentType": "application/octet-stream"},
		}},
	})
	require.Equal(t, "raw id", defaultDelta.SourceArtifactID)
	require.Equal(t, "file", defaultDelta.ArtifactType)
	require.Equal(t, "private", defaultDelta.Visibility)
	require.True(t, defaultDelta.Append)
	require.False(t, defaultDelta.LastChunk)
	require.Equal(t, "Artifact raw id", defaultDelta.Title)
	require.Equal(t, "https://files.example/default.bin", defaultDelta.FileURI)
	require.Equal(t, "application/octet-stream", defaultDelta.MimeType)

	seq := int32(7)
	partsSHA := "parts"
	payloadSHA := "payload"
	declaredSHA := "declared"
	content := mergeArtifactDeltaContent(nil, delta, db.RunArtifactChunk{
		EventSequence:  &seq,
		ChunkIndex:     2,
		Append:         false,
		LastChunk:      true,
		PartsSha256:    &partsSHA,
		PayloadSha256:  &payloadSHA,
		DeclaredSha256: &declaredSHA,
		ChecksumStatus: "verified",
	})
	require.Equal(t, "stream-1", content["artifact_id"])
	require.Equal(t, "hello", content["text"])
	require.Equal(t, "verified", content["last_checksum_status"])
	appended := mergeArtifactDeltaContent(map[string]interface{}{"parts": "previous", "chunks": []interface{}{"old"}}, runArtifactDeltaDraft{
		SourceArtifactID: "append-1",
		ArtifactType:     "data",
		Title:            "Append",
		Visibility:       "shared",
		Append:           true,
		Parts:            []interface{}{"new"},
	}, db.RunArtifactChunk{ChunkIndex: 3, ChecksumStatus: "not_provided"})
	require.Equal(t, []interface{}{"previous", "new"}, appended["parts"])
	require.Len(t, appended["chunks"], 2)

	require.Equal(t, []interface{}{"x"}, interfaceSliceFromAny("x"))
	require.Equal(t, []interface{}{"x", "y"}, interfaceSliceFromAny([]interface{}{"x", "y"}))
	require.Equal(t, []interface{}{}, interfaceSliceFromAny(nil))
	require.Equal(t, "ab", artifactTextFromParts([]interface{}{"a", map[string]interface{}{"text": "b"}}))
	declared, status := artifactChunkChecksum(map[string]interface{}{"parts_sha256": sha256Hex([]byte("parts"))}, sha256Hex([]byte("parts")))
	require.Equal(t, sha256Hex([]byte("parts")), declared)
	require.Equal(t, "verified", status)
	declared, status = artifactChunkChecksum(map[string]interface{}{"parts_sha256": strings.Repeat("d", 64)}, strings.Repeat("e", 64))
	require.Equal(t, strings.Repeat("d", 64), declared)
	require.Equal(t, "mismatch", status)
	_, status = artifactChunkChecksum(map[string]interface{}{}, "x")
	require.Equal(t, "not_provided", status)
	_, status = artifactChunkChecksum(map[string]interface{}{"parts_sha256": "bad"}, "x")
	require.Equal(t, "invalid", status)
	require.Equal(t, strings.Repeat("a", 64), normalizeSHA256(strings.ToUpper(strings.Repeat("a", 64))))
	require.Equal(t, "", normalizeSHA256(strings.Repeat("g", 64)))
	require.Equal(t, "", normalizeSHA256("bad"))
	require.Equal(t, "ab", normalizeArtifactMetadataString("abcd", 2))
	partsMeta := artifactFileMetadataFromParts([]interface{}{
		"skip",
		map[string]interface{}{
			"file": map[string]interface{}{
				"uri":      "https://files.example/export.csv",
				"mimeType": "text/csv",
				"size":     int32(64),
			},
		},
		map[string]interface{}{
			"file_name": "export.csv",
			"checksum":  strings.Repeat("c", 64),
		},
	})
	require.Equal(t, "text/csv", partsMeta.MimeType)
	require.Equal(t, "https://files.example/export.csv", partsMeta.FileURI)
	require.Equal(t, "export.csv", partsMeta.FileName)
	require.Equal(t, strings.Repeat("c", 64), partsMeta.FileSHA256)
	require.NotNil(t, partsMeta.FileSizeBytes)
	require.Equal(t, int64(64), *partsMeta.FileSizeBytes)
	require.Equal(t, artifactFileMetadata{}, artifactFileMetadataFromMap(nil))
	bytesMeta := artifactFileMetadataFromMap(map[string]interface{}{
		"bytes": map[string]interface{}{
			"url":         "https://files.example/raw.bin",
			"contentType": "application/octet-stream",
			"sizeBytes":   float32(128.9),
		},
	})
	require.Equal(t, "https://files.example/raw.bin", bytesMeta.FileURI)
	require.Equal(t, "application/octet-stream", bytesMeta.MimeType)
	require.NotNil(t, bytesMeta.FileSizeBytes)
	require.Equal(t, int64(128), *bytesMeta.FileSizeBytes)
	sizeFromInt64, ok := firstArtifactInt64(map[string]interface{}{"size": int64(9)}, "size")
	require.True(t, ok)
	require.Equal(t, int64(9), sizeFromInt64)
	sizeFromInt, ok := firstArtifactInt64(map[string]interface{}{"size": 8}, "size")
	require.True(t, ok)
	require.Equal(t, int64(8), sizeFromInt)
	sizeFromFloat64, ok := firstArtifactInt64(map[string]interface{}{"size": float64(6.9)}, "size")
	require.True(t, ok)
	require.Equal(t, int64(6), sizeFromFloat64)
	sizeFromFloat, ok := firstArtifactInt64(map[string]interface{}{"bad": -1, "size": float32(7.8)}, "bad", "size")
	require.True(t, ok)
	require.Equal(t, int64(7), sizeFromFloat)
	_, ok = firstArtifactInt64(map[string]interface{}{"size": int32(-1)}, "size")
	require.False(t, ok)
	_, ok = firstArtifactInt64(map[string]interface{}{"size": "7"}, "size")
	require.False(t, ok)
	require.Equal(t, "default", normalizeArtifactSourceID(""))
	require.Equal(t, "Agent 产物", normalizeArtifactTitle(""))
	require.Len(t, []rune(normalizeArtifactSourceID(strings.Repeat("s", 201))), 200)
	require.Len(t, []rune(normalizeArtifactTitle(strings.Repeat("t", 201))), 200)
	require.Equal(t, "fallback", coalesceArtifactString(map[string]interface{}{"x": " "}, "x", "fallback"))
	require.True(t, validArtifactType("json"))
	require.False(t, validArtifactType("html"))
	require.True(t, validArtifactVisibility("shared"))
	require.False(t, validArtifactVisibility("world"))
}

func TestRuntimeSSEAndHandlerValidation(t *testing.T) {
	rec := httptest.NewRecorder()
	err := writeSSEStreamError(rec, errors.New("boom"))
	require.NoError(t, err)
	require.Contains(t, rec.Body.String(), "run.stream.error")
	require.Error(t, writeSSEStreamError(errorResponseWriter{}, errors.New("boom")))
	require.True(t, isTerminalRunEvent("run.failed"))
	require.True(t, isTerminalRunEvent("run.canceled"))
	require.False(t, isTerminalRunEvent("run.started"))

	validUser := uuid.NewString()
	validRun := uuid.NewString()
	validAgent := uuid.NewString()
	tests := []struct {
		name      string
		call      func(*Handler, echo.Context) error
		method    string
		path      string
		body      string
		userID    string
		auth      string
		scopes    []string
		paramID   string
		query     string
		wantHTTP  int
		wantError string
	}{
		{name: "post run invalid json", call: (*Handler).PostRun, method: http.MethodPost, path: "/api/v1/run", body: `{`, userID: validUser, auth: "jwt", wantHTTP: http.StatusBadRequest, wantError: "请求体格式错误"},
		{name: "post run validation", call: (*Handler).PostRun, method: http.MethodPost, path: "/api/v1/run", body: `{}`, userID: validUser, auth: "jwt", wantHTTP: http.StatusUnprocessableEntity, wantError: "AgentID"},
		{name: "post async missing scope", call: (*Handler).PostRunAsync, method: http.MethodPost, path: "/api/v1/runs", body: `{"agent_id":"` + validAgent + `","input":{"x":1}}`, userID: validUser, auth: "user_token", scopes: []string{"runs:read"}, wantHTTP: http.StatusForbidden, wantError: "agents:run"},
		{name: "get run invalid id", call: (*Handler).GetRun, method: http.MethodGet, path: "/api/v1/runs/bad", userID: validUser, auth: "jwt", paramID: "bad", wantHTTP: http.StatusBadRequest, wantError: "id 不是合法 uuid"},
		{name: "events bad after sequence", call: (*Handler).GetRunEvents, method: http.MethodGet, path: "/api/v1/runs/" + validRun + "/events", userID: validUser, auth: "jwt", paramID: validRun, query: "after_sequence=bad", wantHTTP: http.StatusBadRequest, wantError: "after_sequence"},
		{name: "events bad limit", call: (*Handler).GetRunEvents, method: http.MethodGet, path: "/api/v1/runs/" + validRun + "/events", userID: validUser, auth: "jwt", paramID: validRun, query: "limit=bad", wantHTTP: http.StatusBadRequest, wantError: "limit"},
		{name: "artifacts invalid id", call: (*Handler).GetRunArtifacts, method: http.MethodGet, path: "/api/v1/runs/bad/artifacts", userID: validUser, auth: "jwt", paramID: "bad", wantHTTP: http.StatusBadRequest, wantError: "id 不是合法 uuid"},
		{name: "messages invalid id", call: (*Handler).GetRunMessages, method: http.MethodGet, path: "/api/v1/runs/bad/messages", userID: validUser, auth: "jwt", paramID: "bad", wantHTTP: http.StatusBadRequest, wantError: "id 不是合法 uuid"},
		{name: "stream invalid last event", call: (*Handler).StreamRunEvents, method: http.MethodGet, path: "/api/v1/runs/" + validRun + "/stream", userID: validUser, auth: "jwt", paramID: validRun, query: "after_sequence=bad", wantHTTP: http.StatusBadRequest, wantError: "Last-Event-ID"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := echo.New()
			target := tt.path
			if tt.query != "" {
				target += "?" + tt.query
			}
			c := e.NewContext(runtimeJSONRequest(tt.method, target, tt.body), httptest.NewRecorder())
			if tt.userID != "" {
				c.Set(string(httpx.CtxKeyUserID), tt.userID)
			}
			switch tt.auth {
			case "jwt":
				c.Set(string(httpx.CtxKeyAuthMethod), "jwt")
			case "user_token":
				c.Set(string(httpx.CtxKeyAuthMethod), "user_token")
				c.Set(string(httpx.CtxKeyAuthScopes), tt.scopes)
			case "runtime":
				c.Request().Header.Set(echo.HeaderAuthorization, "Bearer ol_agent_validtoken")
			}
			if tt.paramID != "" {
				c.SetParamNames("id")
				c.SetParamValues(tt.paramID)
			}
			err := tt.call(NewHandler(nil), c)
			require.Error(t, err)

			var httpErr *httpx.HTTPError
			require.True(t, errors.As(err, &httpErr), "expected *httpx.HTTPError, got %T", err)
			require.Equal(t, tt.wantHTTP, httpErr.Status)
			require.Contains(t, httpErr.Message, tt.wantError)
		})
	}
}

func TestRuntimeRoutes(t *testing.T) {
	e := echo.New()
	h := NewHandler(nil)
	require.NotNil(t, NewHandler(nil, &config.Config{APIURL: "https://api.example.com"}).cfg)
	h.RegisterProtected(e.Group("/api/v1"), func(next echo.HandlerFunc) echo.HandlerFunc { return next }, func(next echo.HandlerFunc) echo.HandlerFunc { return next })
	h.RegisterAdmin(e.Group("/api/v1"), func(next echo.HandlerFunc) echo.HandlerFunc { return next }, func(next echo.HandlerFunc) echo.HandlerFunc { return next })
	h.RegisterAgentRuntime(e.Group("/api/v1"))

	routes := map[string]bool{}
	for _, route := range e.Routes() {
		routes[route.Method+" "+route.Path] = true
	}
	for _, key := range []string{
		http.MethodPost + " /api/v1/run",
		http.MethodPost + " /api/v1/runs",
		http.MethodGet + " /api/v1/runs/:id",
		http.MethodGet + " /api/v1/runs/:id/events",
		http.MethodGet + " /api/v1/runs/:id/artifacts",
		http.MethodGet + " /api/v1/runs/:id/messages",
		http.MethodGet + " /api/v1/runs/:id/stream",
		http.MethodPost + " /api/v1/runs/:id/cancel",
		http.MethodPost + " /api/v1/runs/:id/replay",
		http.MethodGet + " /api/v1/admin/runtime/dead-letters",
		http.MethodGet + " /api/v1/admin/runtime/nodes",
		http.MethodPost + " /api/v1/admin/runtime/nodes/:id/drain",
		http.MethodPost + " /api/v1/admin/runtime/nodes/:id/activate",
		http.MethodPost + " /api/v1/admin/runtime/nodes/:id/revoke",
		http.MethodPost + " /api/v1/agent-runtime/runs/:id/result",
		http.MethodGet + " /api/v1/agent-runtime/ws",
		http.MethodPost + " /api/v1/agent-runtime/call-agent",
	} {
		require.True(t, routes[key], key)
	}
	for _, key := range []string{
		http.MethodPost + " /api/v1/runs/:id/events",
		http.MethodPost + " /api/v1/agent-runtime/heartbeat",
	} {
		require.False(t, routes[key], key)
	}
}

type errorResponseWriter struct{}

func (errorResponseWriter) Header() http.Header {
	return http.Header{}
}

func (errorResponseWriter) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
}

func (errorResponseWriter) WriteHeader(int) {}

func nextActionFromUnsupportedOutput(t *testing.T) *RunNextAction {
	t.Helper()
	action, ok := nextActionFromOutput(map[string]interface{}{"next_action": 42})
	require.False(t, ok)
	return action
}

func runtimeJSONRequest(method, target, body string) *http.Request {
	req := httptest.NewRequest(method, target, bytes.NewBufferString(body))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	return req
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func testHTTPResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func TestRuntimeJSONHelperIsValid(t *testing.T) {
	req := runtimeJSONRequest(http.MethodPost, "/", `{"ok":true}`)
	var body map[string]interface{}
	require.NoError(t, json.NewDecoder(req.Body).Decode(&body))
	require.Equal(t, true, body["ok"])
}

type fakeRunRequirementQueries struct {
	task      db.TaskQuery
	taskID    uuid.UUID
	taskErr   error
	skills    []db.Skill
	agentID   uuid.UUID
	skillsErr error

	evidence    db.RunRequirementEvidence
	runID       uuid.UUID
	evidenceErr error
}

func (q *fakeRunRequirementQueries) GetTaskQuery(_ context.Context, taskID uuid.UUID) (db.TaskQuery, error) {
	q.taskID = taskID
	return q.task, q.taskErr
}

func (q *fakeRunRequirementQueries) ListAgentSkills(_ context.Context, agentID uuid.UUID) ([]db.Skill, error) {
	q.agentID = agentID
	return q.skills, q.skillsErr
}

func (q *fakeRunRequirementQueries) GetRunRequirementEvidenceByRun(_ context.Context, runID uuid.UUID) (db.RunRequirementEvidence, error) {
	q.runID = runID
	return q.evidence, q.evidenceErr
}

type runtimeFakeDBTX struct {
	rows         []runtimeFakeRow
	queryRows    []pgx.Rows
	queryErr     error
	queryCalls   int
	queryRowArgs [][]interface{}
	queryArgs    [][]interface{}
}

func (f *runtimeFakeDBTX) Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error) {
	return pgconn.NewCommandTag("INSERT 1"), nil
}

func (f *runtimeFakeDBTX) Query(_ context.Context, _ string, args ...interface{}) (pgx.Rows, error) {
	f.queryCalls++
	f.queryArgs = append(f.queryArgs, args)
	if f.queryErr != nil {
		return nil, f.queryErr
	}
	if len(f.queryRows) > 0 {
		rows := f.queryRows[0]
		f.queryRows = f.queryRows[1:]
		return rows, nil
	}
	return nil, errors.New("unexpected query")
}

func (f *runtimeFakeDBTX) QueryRow(_ context.Context, _ string, args ...interface{}) pgx.Row {
	f.queryRowArgs = append(f.queryRowArgs, args)
	if len(f.rows) == 0 {
		return runtimeFakeRow{err: errors.New("unexpected query row")}
	}
	row := f.rows[0]
	f.rows = f.rows[1:]
	return row
}

type runtimeFakeRow struct {
	values []any
	err    error
}

type runtimeFakeRows struct {
	rows    []runtimeFakeRow
	current int
	closed  bool
	err     error
}

func (r *runtimeFakeRows) Close() {
	r.closed = true
}

func (r *runtimeFakeRows) Err() error {
	return r.err
}

func (r *runtimeFakeRows) CommandTag() pgconn.CommandTag {
	return pgconn.NewCommandTag("SELECT")
}

func (r *runtimeFakeRows) FieldDescriptions() []pgconn.FieldDescription {
	return nil
}

func (r *runtimeFakeRows) Next() bool {
	if r.current >= len(r.rows) {
		r.Close()
		return false
	}
	r.current++
	return true
}

func (r *runtimeFakeRows) Scan(dest ...any) error {
	if r.current == 0 || r.current > len(r.rows) {
		return errors.New("scan without current row")
	}
	return r.rows[r.current-1].Scan(dest...)
}

func (r *runtimeFakeRows) Values() ([]any, error) {
	if r.current == 0 || r.current > len(r.rows) {
		return nil, errors.New("values without current row")
	}
	return r.rows[r.current-1].values, nil
}

func (r *runtimeFakeRows) RawValues() [][]byte {
	return nil
}

func (r *runtimeFakeRows) Conn() *pgx.Conn {
	return nil
}

func (r runtimeFakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != len(r.values) {
		return errors.New("scan destination count mismatch")
	}
	for i, value := range r.values {
		target := reflect.ValueOf(dest[i])
		if target.Kind() != reflect.Pointer || target.IsNil() {
			return errors.New("scan destination must be pointer")
		}
		slot := target.Elem()
		if value == nil {
			slot.SetZero()
			continue
		}
		got := reflect.ValueOf(value)
		if got.Type().AssignableTo(slot.Type()) {
			slot.Set(got)
			continue
		}
		if got.Type().ConvertibleTo(slot.Type()) {
			slot.Set(got.Convert(slot.Type()))
			continue
		}
		return errors.New("scan value type mismatch")
	}
	return nil
}

func runtimeRunRowValues(runID, userID, agentID uuid.UUID, status string) []any {
	started := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	return []any{
		runID,
		userID,
		agentID,
		[]byte(`{"q":"hello"}`),
		[]byte(`{}`),
		status,
		(*string)(nil),
		(*string)(nil),
		int32(0),
		int32(0),
		int32(0),
		(*int32)(nil),
		started,
		(*time.Time)(nil),
		"api",
		RuntimeContractID,
		"terminal",
		int32(0),
		int32(1),
		(*time.Time)(nil),
		(*uuid.UUID)(nil),
		(*uuid.UUID)(nil),
		(*string)(nil),
		(*time.Time)(nil),
		(*time.Time)(nil),
		(*string)(nil),
		(*time.Time)(nil),
		(*uuid.UUID)(nil),
	}
}

type recordingTaskCallbackEnqueuer struct {
	events chan db.RunEvent
}

func (r *recordingTaskCallbackEnqueuer) EnqueueRunEvent(_ context.Context, event db.RunEvent) error {
	r.events <- event
	return nil
}

type fakeTimeoutErr struct {
	timeout bool
}

func (e fakeTimeoutErr) Error() string {
	return "timeout"
}

func (e fakeTimeoutErr) Timeout() bool {
	return e.timeout
}
