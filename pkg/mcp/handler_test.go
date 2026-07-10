package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/OpenLinker-ai/openlinker-core/pkg/task"
)

func TestPostRunAgentRejectsAPIKeyWithoutRunScope(t *testing.T) {
	e := echo.New()
	c := e.NewContext(httptest.NewRequest(http.MethodPost, "/api/v1/mcp/run_agent", nil), httptest.NewRecorder())
	c.Set(string(httpx.CtxKeyAuthMethod), "user_token")
	c.Set(string(httpx.CtxKeyAuthScopes), []string{"agents:read"})

	err := NewHandler(nil).PostRunAgent(c)
	var httpErr *httpx.HTTPError
	require.True(t, errors.As(err, &httpErr))
	require.Equal(t, http.StatusForbidden, httpErr.Status)
}

func TestPostRPCListToolsUsesMCPToolShape(t *testing.T) {
	e := echo.New()
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`)
	rec := httptest.NewRecorder()
	c := e.NewContext(httptest.NewRequest(http.MethodPost, "/api/v1/mcp", bytes.NewReader(body)), rec)
	c.Set(string(httpx.CtxKeyAuthMethod), "user_token")
	c.Set(string(httpx.CtxKeyAuthScopes), []string{"agents:read"})

	err := NewHandler(NewService(nil, nil, nil)).PostRPC(c)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Tools []struct {
				Name        string                 `json:"name"`
				InputSchema map[string]interface{} `json:"inputSchema"`
			} `json:"tools"`
		} `json:"result"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.NotEmpty(t, resp.Result.Tools)
	require.Equal(t, "search_agents", resp.Result.Tools[0].Name)
	require.Equal(t, "object", resp.Result.Tools[0].InputSchema["type"])
}

func TestMCPToolDescriptionsStayNeutralAndMachineReadable(t *testing.T) {
	tools := NewService(nil, nil, nil).Tools()
	names := make([]string, 0, len(tools))
	descriptions := make([]string, 0)

	var collectDescriptions func(interface{})
	collectDescriptions = func(value interface{}) {
		switch typed := value.(type) {
		case map[string]interface{}:
			for key, child := range typed {
				if key == "description" {
					if description, ok := child.(string); ok {
						descriptions = append(descriptions, description)
					}
				}
				collectDescriptions(child)
			}
		case []interface{}:
			for _, child := range typed {
				collectDescriptions(child)
			}
		}
	}

	for _, tool := range tools {
		names = append(names, tool.Name)
		descriptions = append(descriptions, tool.Description)
		collectDescriptions(tool.InputSchema)
		collectDescriptions(tool.OutputSchema)
	}
	require.Equal(t, []string{"search_agents", "get_agent", "run_agent", "get_run", "create_task"}, names)

	for _, description := range descriptions {
		require.NotEmpty(t, strings.TrimSpace(description))
		for _, r := range description {
			require.Falsef(t, unicode.Is(unicode.Han, r), "description must not contain Chinese copy: %q", description)
		}
		lower := strings.ToLower(description)
		for _, forbidden := range []string{"price", "balance", "creator endpoint", "used_mcp_tools", "future", "later"} {
			require.NotContains(t, lower, forbidden)
		}
	}
}

func TestPostRPCToolCallReportsMissingScopeAsToolError(t *testing.T) {
	e := echo.New()
	body := []byte(`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"run_agent","arguments":{"agent_id":"8582c7a4-0f02-4895-8570-7c7cce357e5f","input":{"text":"hi"}}}}`)
	rec := httptest.NewRecorder()
	c := e.NewContext(httptest.NewRequest(http.MethodPost, "/api/v1/mcp", bytes.NewReader(body)), rec)
	c.Set(string(httpx.CtxKeyAuthMethod), "user_token")
	c.Set(string(httpx.CtxKeyAuthScopes), []string{"agents:read"})

	err := NewHandler(nil).PostRPC(c)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			IsError bool `json:"isError"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"result"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.True(t, resp.Result.IsError)
	require.NotEmpty(t, resp.Result.Content)
	require.Contains(t, resp.Result.Content[0].Text, "agents:run")
}

func TestGetEndpointInfoDocumentsHTTPTransportAndRejectsSSE(t *testing.T) {
	e := echo.New()

	rec := httptest.NewRecorder()
	c := e.NewContext(httptest.NewRequest(http.MethodGet, "/api/v1/mcp", nil), rec)
	require.NoError(t, NewHandler(nil).GetEndpointInfo(c))
	require.Equal(t, http.StatusOK, rec.Code)

	var info map[string]interface{}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &info))
	require.Equal(t, "openlinker-mcp", info["name"])
	require.Equal(t, "streamable_http_json_response", info["transport"])
	require.Equal(t, mcpProtocolVersion, info["protocol_version"])
	require.NotEmpty(t, info["tools"])

	rec = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/mcp", nil)
	req.Header.Set("Accept", "application/json, text/event-stream")
	c = e.NewContext(req, rec)
	require.NoError(t, NewHandler(nil).GetEndpointInfo(c))
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestPostRPCErrorsForMalformedAndUnsupportedRequests(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		wantHTTP int
		wantCode int
		wantMsg  string
	}{
		{
			name:     "parse error",
			body:     `not-json`,
			wantHTTP: http.StatusBadRequest,
			wantCode: -32700,
			wantMsg:  "Parse error",
		},
		{
			name:     "batch unsupported",
			body:     `[{"jsonrpc":"2.0","id":1,"method":"initialize"}]`,
			wantHTTP: http.StatusOK,
			wantCode: -32600,
			wantMsg:  "Batch JSON-RPC requests are not supported",
		},
		{
			name:     "invalid jsonrpc version",
			body:     `{"jsonrpc":"1.0","id":1,"method":"initialize"}`,
			wantHTTP: http.StatusOK,
			wantCode: -32600,
			wantMsg:  "Invalid Request",
		},
		{
			name:     "unknown method",
			body:     `{"jsonrpc":"2.0","id":1,"method":"resources/list"}`,
			wantHTTP: http.StatusOK,
			wantCode: -32601,
			wantMsg:  "Method not found: resources/list",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c := newRPCContext(tt.body, rec)
			c.Set(string(httpx.CtxKeyAuthMethod), "user_token")

			require.NoError(t, NewHandler(nil).PostRPC(c))
			require.Equal(t, tt.wantHTTP, rec.Code)

			var resp rpcResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			require.NotNil(t, resp.Error)
			require.Equal(t, tt.wantCode, resp.Error.Code)
			require.Equal(t, tt.wantMsg, resp.Error.Message)
		})
	}
}

func TestPostRPCAcceptsNotificationsWithoutServiceWork(t *testing.T) {
	rec := httptest.NewRecorder()
	c := newRPCContext(`{"jsonrpc":"2.0","method":"tools/list"}`, rec)
	c.Set(string(httpx.CtxKeyAuthMethod), "user_token")

	require.NoError(t, NewHandler(nil).PostRPC(c))
	require.Equal(t, http.StatusAccepted, rec.Code)
	require.Empty(t, rec.Body.String())
}

func TestPostRPCInitializeRequiresAPIKeyAndReturnsCapabilities(t *testing.T) {
	rec := httptest.NewRecorder()
	c := newRPCContext(`{"jsonrpc":"2.0","id":"auth","method":"initialize"}`, rec)
	c.Set(string(httpx.CtxKeyAuthMethod), "jwt")

	require.NoError(t, NewHandler(nil).PostRPC(c))
	require.Equal(t, http.StatusForbidden, rec.Code)

	var denied rpcResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &denied))
	require.NotNil(t, denied.Error)
	require.Equal(t, -32000, denied.Error.Code)
	require.Contains(t, denied.Error.Message, "User Token")

	rec = httptest.NewRecorder()
	c = newRPCContext(`{"jsonrpc":"2.0","id":"ok","method":"initialize"}`, rec)
	c.Set(string(httpx.CtxKeyAuthMethod), "user_token")
	require.NoError(t, NewHandler(nil).PostRPC(c))
	require.Equal(t, http.StatusOK, rec.Code)

	var ok struct {
		Result struct {
			ProtocolVersion string `json:"protocolVersion"`
			ServerInfo      struct {
				Name string `json:"name"`
			} `json:"serverInfo"`
		} `json:"result"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ok))
	require.Equal(t, mcpProtocolVersion, ok.Result.ProtocolVersion)
	require.Equal(t, "openlinker", ok.Result.ServerInfo.Name)
}

func TestPostRPCToolCallValidatesParamsBeforeServiceDispatch(t *testing.T) {
	tests := []struct {
		name    string
		params  string
		wantMsg string
	}{
		{
			name:    "missing params",
			params:  ``,
			wantMsg: "tools/call requires params",
		},
		{
			name:    "invalid params",
			params:  `"bad"`,
			wantMsg: "Invalid tools/call params",
		},
		{
			name:    "missing tool name",
			params:  `{}`,
			wantMsg: "tools/call params.name is required",
		},
		{
			name:    "unknown tool name",
			params:  `{"name":"unknown"}`,
			wantMsg: "Unknown tool: unknown",
		},
		{
			name:    "invalid get_agent arguments",
			params:  `{"name":"get_agent","arguments":123}`,
			wantMsg: "Invalid arguments:",
		},
		{
			name:    "get_agent validation",
			params:  `{"name":"get_agent","arguments":{}}`,
			wantMsg: "Invalid arguments:",
		},
		{
			name:    "run_agent validation",
			params:  `{"name":"run_agent","arguments":{"agent_id":"8582c7a4-0f02-4895-8570-7c7cce357e5f"}}`,
			wantMsg: "Invalid arguments:",
		},
		{
			name:    "create_task validation",
			params:  `{"name":"create_task","arguments":{"query":"abc"}}`,
			wantMsg: "Invalid arguments:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := `{"jsonrpc":"2.0","id":9,"method":"tools/call"`
			if tt.params != "" {
				body += `,"params":` + tt.params
			}
			body += `}`

			rec := httptest.NewRecorder()
			c := newRPCContext(body, rec)
			c.Set(string(httpx.CtxKeyAuthMethod), "user_token")
			c.Set(string(httpx.CtxKeyAuthScopes), []string{"agents:read", "agents:run", "runs:read", "tasks:write"})
			c.Set(string(httpx.CtxKeyUserID), "8582c7a4-0f02-4895-8570-7c7cce357e5f")

			require.NoError(t, NewHandler(nil).PostRPC(c))
			require.Equal(t, http.StatusOK, rec.Code)

			var resp rpcResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			require.NotNil(t, resp.Error)
			require.Equal(t, -32602, resp.Error.Code)
			require.Contains(t, resp.Error.Message, tt.wantMsg)
		})
	}
}

func TestPostRPCToolCallReportsUserContextErrorsAsToolErrors(t *testing.T) {
	tests := []struct {
		name      string
		userID    string
		wantError string
	}{
		{name: "missing user id", wantError: "认证失败"},
		{name: "invalid user id", userID: "not-a-uuid", wantError: "token 无效"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := `{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"get_run","arguments":{"run_id":"8582c7a4-0f02-4895-8570-7c7cce357e5f"}}}`
			rec := httptest.NewRecorder()
			c := newRPCContext(body, rec)
			c.Set(string(httpx.CtxKeyAuthMethod), "user_token")
			c.Set(string(httpx.CtxKeyAuthScopes), []string{"runs:read"})
			if tt.userID != "" {
				c.Set(string(httpx.CtxKeyUserID), tt.userID)
			}

			require.NoError(t, NewHandler(nil).PostRPC(c))
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				Result mcpToolResult `json:"result"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			require.True(t, resp.Result.IsError)
			require.NotEmpty(t, resp.Result.Content)
			require.Contains(t, resp.Result.Content[0].Text, tt.wantError)
		})
	}
}

func TestRESTHandlersValidateAuthBodiesAndUserContextBeforeServiceDispatch(t *testing.T) {
	tests := []struct {
		name      string
		build     func(e *echo.Echo, rec *httptest.ResponseRecorder) echo.Context
		call      func(*Handler, echo.Context) error
		wantError string
		wantHTTP  int
	}{
		{
			name: "get tools missing api key",
			build: func(e *echo.Echo, rec *httptest.ResponseRecorder) echo.Context {
				return e.NewContext(httptest.NewRequest(http.MethodGet, "/api/v1/mcp/tools", nil), rec)
			},
			call:      (*Handler).GetTools,
			wantError: "User Token",
			wantHTTP:  http.StatusForbidden,
		},
		{
			name: "get agent invalid json",
			build: func(e *echo.Echo, rec *httptest.ResponseRecorder) echo.Context {
				c := e.NewContext(newJSONRequest(http.MethodPost, "/api/v1/mcp/get_agent", `{`), rec)
				setAPIKeyScopes(c, "agents:read")
				return c
			},
			call:      (*Handler).PostGetAgent,
			wantError: "请求体格式错误",
			wantHTTP:  http.StatusBadRequest,
		},
		{
			name: "get agent validation",
			build: func(e *echo.Echo, rec *httptest.ResponseRecorder) echo.Context {
				c := e.NewContext(newJSONRequest(http.MethodPost, "/api/v1/mcp/get_agent", `{}`), rec)
				setAPIKeyScopes(c, "agents:read")
				return c
			},
			call:      (*Handler).PostGetAgent,
			wantError: "Slug",
			wantHTTP:  http.StatusUnprocessableEntity,
		},
		{
			name: "run agent missing user",
			build: func(e *echo.Echo, rec *httptest.ResponseRecorder) echo.Context {
				c := e.NewContext(newJSONRequest(http.MethodPost, "/api/v1/mcp/run_agent", `{"agent_id":"8582c7a4-0f02-4895-8570-7c7cce357e5f","input":{"text":"hi"}}`), rec)
				setAPIKeyScopes(c, "agents:run")
				return c
			},
			call:      (*Handler).PostRunAgent,
			wantError: "认证失败",
			wantHTTP:  http.StatusUnauthorized,
		},
		{
			name: "get run invalid user",
			build: func(e *echo.Echo, rec *httptest.ResponseRecorder) echo.Context {
				c := e.NewContext(newJSONRequest(http.MethodPost, "/api/v1/mcp/get_run", `{"run_id":"8582c7a4-0f02-4895-8570-7c7cce357e5f"}`), rec)
				setAPIKeyScopes(c, "runs:read")
				c.Set(string(httpx.CtxKeyUserID), "bad-user")
				return c
			},
			call:      (*Handler).PostGetRun,
			wantError: "token 无效",
			wantHTTP:  http.StatusUnauthorized,
		},
		{
			name: "create task validation",
			build: func(e *echo.Echo, rec *httptest.ResponseRecorder) echo.Context {
				c := e.NewContext(newJSONRequest(http.MethodPost, "/api/v1/mcp/create_task", `{"query":"abc"}`), rec)
				setAPIKeyScopes(c, "tasks:write")
				c.Set(string(httpx.CtxKeyUserID), "8582c7a4-0f02-4895-8570-7c7cce357e5f")
				return c
			},
			call:      (*Handler).PostCreateTask,
			wantError: "Query",
			wantHTTP:  http.StatusUnprocessableEntity,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := echo.New()
			rec := httptest.NewRecorder()
			err := tt.call(NewHandler(nil), tt.build(e, rec))
			require.Error(t, err)

			var httpErr *httpx.HTTPError
			require.True(t, errors.As(err, &httpErr))
			require.Equal(t, tt.wantHTTP, httpErr.Status)
			require.Contains(t, httpErr.Message, tt.wantError)
		})
	}
}

func TestRESTHandlersDispatchToService(t *testing.T) {
	userID := uuid.MustParse("8582c7a4-0f02-4895-8570-7c7cce357e5f")
	runID := uuid.MustParse("c93dbab2-404f-4460-bcb7-0f17ece85567")
	agentID := uuid.MustParse("1a63b493-52c8-4b43-81a3-0fbd18422a7f")
	svc := newFakeMCPService()
	tests := []struct {
		name     string
		build    func(e *echo.Echo, rec *httptest.ResponseRecorder) echo.Context
		call     func(*Handler, echo.Context) error
		wantHTTP int
		assert   func(t *testing.T)
	}{
		{
			name: "search agents",
			build: func(e *echo.Echo, rec *httptest.ResponseRecorder) echo.Context {
				c := e.NewContext(newJSONRequest(http.MethodPost, "/api/v1/mcp/search_agents", `{"query":"translate","tags":["text"],"skill_ids":["content/translation"],"limit":99}`), rec)
				setAPIKeyScopes(c, "agents:read")
				return c
			},
			call:     (*Handler).PostSearchAgents,
			wantHTTP: http.StatusOK,
			assert: func(t *testing.T) {
				require.Equal(t, "translate", svc.searchReq.Query)
				require.Equal(t, []string{"content/translation"}, svc.searchReq.SkillIDs)
				require.Equal(t, int32(99), svc.searchReq.Limit)
			},
		},
		{
			name: "get agent",
			build: func(e *echo.Echo, rec *httptest.ResponseRecorder) echo.Context {
				c := e.NewContext(newJSONRequest(http.MethodPost, "/api/v1/mcp/get_agent", `{"slug":"agent-one"}`), rec)
				setAPIKeyScopes(c, "agents:read")
				return c
			},
			call:     (*Handler).PostGetAgent,
			wantHTTP: http.StatusOK,
			assert: func(t *testing.T) {
				require.Equal(t, "agent-one", svc.getReq.Slug)
			},
		},
		{
			name: "run agent",
			build: func(e *echo.Echo, rec *httptest.ResponseRecorder) echo.Context {
				c := e.NewContext(newJSONRequest(http.MethodPost, "/api/v1/mcp/run_agent", `{"agent_id":"`+agentID.String()+`","input":{"text":"hi"},"metadata":{"trace":"mcp"}}`), rec)
				setAPIKeyScopes(c, "agents:run")
				c.Set(string(httpx.CtxKeyUserID), userID.String())
				return c
			},
			call:     (*Handler).PostRunAgent,
			wantHTTP: http.StatusOK,
			assert: func(t *testing.T) {
				require.Equal(t, userID, svc.runUserID)
				require.Equal(t, agentID.String(), svc.runReq.AgentID)
				require.Equal(t, "mcp", svc.runReq.Metadata["trace"])
			},
		},
		{
			name: "get run",
			build: func(e *echo.Echo, rec *httptest.ResponseRecorder) echo.Context {
				c := e.NewContext(newJSONRequest(http.MethodPost, "/api/v1/mcp/get_run", `{"run_id":"`+runID.String()+`"}`), rec)
				setAPIKeyScopes(c, "runs:read")
				c.Set(string(httpx.CtxKeyUserID), userID.String())
				return c
			},
			call:     (*Handler).PostGetRun,
			wantHTTP: http.StatusOK,
			assert: func(t *testing.T) {
				require.Equal(t, userID, svc.getRunUserID)
				require.Equal(t, runID, svc.getRunID)
			},
		},
		{
			name: "create task",
			build: func(e *echo.Echo, rec *httptest.ResponseRecorder) echo.Context {
				c := e.NewContext(newJSONRequest(http.MethodPost, "/api/v1/mcp/create_task", `{"query":"summarize a long document","skill_ids":["summary"],"mcp_tools":["search_agents"]}`), rec)
				setAPIKeyScopes(c, "tasks:write")
				c.Set(string(httpx.CtxKeyUserID), userID.String())
				return c
			},
			call:     (*Handler).PostCreateTask,
			wantHTTP: http.StatusOK,
			assert: func(t *testing.T) {
				require.Equal(t, userID, svc.taskUserID)
				require.Equal(t, "summarize a long document", svc.taskReq.Query)
				require.Equal(t, []string{"search_agents"}, svc.taskReq.MCPTools)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := echo.New()
			rec := httptest.NewRecorder()
			require.NoError(t, tt.call(NewHandler(svc), tt.build(e, rec)))
			require.Equal(t, tt.wantHTTP, rec.Code)
			tt.assert(t)
		})
	}
}

func TestPostRPCToolCallDispatchesAllTools(t *testing.T) {
	userID := uuid.MustParse("8582c7a4-0f02-4895-8570-7c7cce357e5f")
	runID := uuid.MustParse("c93dbab2-404f-4460-bcb7-0f17ece85567")
	agentID := uuid.MustParse("1a63b493-52c8-4b43-81a3-0fbd18422a7f")
	svc := newFakeMCPService()
	tests := []struct {
		name   string
		params string
		assert func(t *testing.T)
	}{
		{
			name:   "search_agents",
			params: `{"name":"search_agents","arguments":{"query":"data","skill_ids":["data/sql-query"],"limit":2}}`,
			assert: func(t *testing.T) {
				require.Equal(t, "data", svc.searchReq.Query)
				require.Equal(t, []string{"data/sql-query"}, svc.searchReq.SkillIDs)
				require.Equal(t, int32(2), svc.searchReq.Limit)
			},
		},
		{
			name:   "get_agent",
			params: `{"name":"get_agent","arguments":{"slug":"agent-one"}}`,
			assert: func(t *testing.T) {
				require.Equal(t, "agent-one", svc.getReq.Slug)
			},
		},
		{
			name:   "run_agent",
			params: `{"name":"run_agent","arguments":{"agent_id":"` + agentID.String() + `","input":{"text":"hi"}}}`,
			assert: func(t *testing.T) {
				require.Equal(t, userID, svc.runUserID)
				require.Equal(t, agentID.String(), svc.runReq.AgentID)
			},
		},
		{
			name:   "get_run",
			params: `{"name":"get_run","arguments":{"run_id":"` + runID.String() + `"}}`,
			assert: func(t *testing.T) {
				require.Equal(t, userID, svc.getRunUserID)
				require.Equal(t, runID, svc.getRunID)
			},
		},
		{
			name:   "create_task",
			params: `{"name":"create_task","arguments":{"query":"summarize a long document"}}`,
			assert: func(t *testing.T) {
				require.Equal(t, userID, svc.taskUserID)
				require.Equal(t, "summarize a long document", svc.taskReq.Query)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c := newRPCContext(`{"jsonrpc":"2.0","id":"ok","method":"tools/call","params":`+tt.params+`}`, rec)
			c.Set(string(httpx.CtxKeyAuthMethod), "user_token")
			c.Set(string(httpx.CtxKeyAuthScopes), []string{"agents:read", "agents:run", "runs:read", "tasks:write"})
			c.Set(string(httpx.CtxKeyUserID), userID.String())

			require.NoError(t, NewHandler(svc).PostRPC(c))
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				Result mcpToolResult `json:"result"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			require.False(t, resp.Result.IsError)
			require.NotNil(t, resp.Result.StructuredContent)
			tt.assert(t)
		})
	}
}

func TestGetToolsReturnsRESTToolDescriptors(t *testing.T) {
	e := echo.New()
	rec := httptest.NewRecorder()
	c := e.NewContext(httptest.NewRequest(http.MethodGet, "/api/v1/mcp/tools", nil), rec)
	setAPIKeyScopes(c, "agents:read")

	require.NoError(t, NewHandler(nil).GetTools(c))
	require.Equal(t, http.StatusOK, rec.Code)

	var resp ToolsResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Tools, len(mcpTools))
	require.Equal(t, "input_schema", jsonFieldNameForToolDescriptorInput(t, resp.Tools[0]))
}

func TestMCPHelpersNormalizeToolSchemasArgumentsAndErrors(t *testing.T) {
	tools := toMCPTools([]ToolDescriptor{{Name: "empty"}, {Name: "custom", InputSchema: map[string]interface{}{"type": "array"}}})
	require.Len(t, tools, 2)
	require.Equal(t, map[string]interface{}{"type": "object"}, tools[0].InputSchema)
	require.Equal(t, map[string]interface{}{"type": "array"}, tools[1].InputSchema)

	var decoded map[string]interface{}
	require.NoError(t, decodeToolArguments(nil, &decoded))
	require.Empty(t, decoded)
	require.NoError(t, decodeToolArguments(json.RawMessage(`null`), &decoded))
	require.Empty(t, decoded)
	require.Error(t, decodeToolArguments(json.RawMessage(`{`), &decoded))

	require.Equal(t, []string{"run_agent"}, appendStringList(nil, "run_agent"))
	require.Equal(t, []string{"search_agents", "run_agent"}, appendStringList([]string{"search_agents"}, "run_agent"))
	require.Equal(t, []string{"run_agent"}, appendStringList([]string{"run_agent"}, "run_agent"))
	require.Equal(t, []string{"get_agent", "run_agent"}, appendStringList([]interface{}{"get_agent", 42}, "run_agent"))
	require.Equal(t, []string{"create_task", "run_agent"}, appendStringList("create_task", "run_agent"))

	ok := toolResult(map[string]string{"ok": "yes"}, nil)
	require.False(t, ok.IsError)
	require.Equal(t, map[string]string{"ok": "yes"}, ok.StructuredContent)
	require.Contains(t, ok.Content[0].Text, `"ok": "yes"`)

	marshalErr := toolResult(map[string]interface{}{"bad": func() {}}, nil)
	require.True(t, marshalErr.IsError)
	require.Contains(t, marshalErr.Content[0].Text, "unsupported type")

	httpErr := toolError(&httpx.HTTPError{
		Status:  http.StatusForbidden,
		Code:    httpx.CodeForbidden,
		Message: "denied",
		Details: map[string]string{"scope": "agents:run"},
	})
	require.True(t, httpErr.IsError)
	require.Equal(t, "denied", httpErr.Content[0].Text)
	require.NotNil(t, httpErr.StructuredContent)
}

func newRPCContext(body string, rec *httptest.ResponseRecorder) echo.Context {
	e := echo.New()
	return e.NewContext(httptest.NewRequest(http.MethodPost, "/api/v1/mcp", bytes.NewBufferString(body)), rec)
}

func newJSONRequest(method, target, body string) *http.Request {
	req := httptest.NewRequest(method, target, bytes.NewBufferString(body))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	return req
}

func setAPIKeyScopes(c echo.Context, scopes ...string) {
	c.Set(string(httpx.CtxKeyAuthMethod), "user_token")
	c.Set(string(httpx.CtxKeyAuthScopes), scopes)
}

func jsonFieldNameForToolDescriptorInput(t *testing.T, tool ToolDescriptor) string {
	t.Helper()
	raw, err := json.Marshal(tool)
	require.NoError(t, err)
	require.Contains(t, string(raw), `"input_schema"`)
	return "input_schema"
}

type fakeMCPService struct {
	searchResp *agent.MarketListResponse
	getResp    *agent.AgentDetailResponse
	runResp    *runtime.RunResponse
	taskResp   *task.RecommendResponse

	searchReq    SearchAgentsRequest
	getReq       GetAgentRequest
	runUserID    uuid.UUID
	runReq       RunAgentRequest
	getRunUserID uuid.UUID
	getRunID     uuid.UUID
	taskUserID   uuid.UUID
	taskReq      CreateTaskRequest
}

func newFakeMCPService() *fakeMCPService {
	return &fakeMCPService{
		searchResp: &agent.MarketListResponse{
			Items: []agent.MarketListItem{{ID: "agent-1", Slug: "agent-one", Name: "Agent One"}},
			Total: 1,
			Page:  1,
			Size:  1,
		},
		getResp: &agent.AgentDetailResponse{
			ID:   "agent-1",
			Slug: "agent-one",
			Name: "Agent One",
		},
		runResp: &runtime.RunResponse{
			RunID:  "run-1",
			Status: "success",
			Output: map[string]interface{}{"ok": true},
			Source: "mcp",
		},
		taskResp: &task.RecommendResponse{
			TaskID:     uuid.MustParse("9e080dc1-1d0f-4806-a570-4aa25a2e759c"),
			Visibility: "private",
			MCPTools:   []string{"search_agents"},
		},
	}
}

func (f *fakeMCPService) SearchAgents(_ context.Context, req *SearchAgentsRequest) (*agent.MarketListResponse, error) {
	f.searchReq = *req
	return f.searchResp, nil
}

func (f *fakeMCPService) GetAgent(_ context.Context, req *GetAgentRequest) (*agent.AgentDetailResponse, error) {
	f.getReq = *req
	return f.getResp, nil
}

func (f *fakeMCPService) RunAgent(_ context.Context, userID uuid.UUID, req *RunAgentRequest) (*runtime.RunResponse, error) {
	f.runUserID = userID
	f.runReq = *req
	return f.runResp, nil
}

func (f *fakeMCPService) GetRun(_ context.Context, userID, runID uuid.UUID) (*runtime.RunResponse, error) {
	f.getRunUserID = userID
	f.getRunID = runID
	return f.runResp, nil
}

func (f *fakeMCPService) CreateTask(_ context.Context, userID uuid.UUID, req *CreateTaskRequest) (*task.RecommendResponse, error) {
	f.taskUserID = userID
	f.taskReq = *req
	return f.taskResp, nil
}

func (f *fakeMCPService) Tools() []ToolDescriptor {
	return mcpTools
}
