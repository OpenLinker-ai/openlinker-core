package mcp

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"

	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

func TestPostRunAgentRejectsAPIKeyWithoutRunScope(t *testing.T) {
	e := echo.New()
	c := e.NewContext(httptest.NewRequest(http.MethodPost, "/api/v1/mcp/run_agent", nil), httptest.NewRecorder())
	c.Set(string(httpx.CtxKeyAuthMethod), "apikey")
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
	c.Set(string(httpx.CtxKeyAuthMethod), "apikey")
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

func TestPostRPCToolCallReportsMissingScopeAsToolError(t *testing.T) {
	e := echo.New()
	body := []byte(`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"run_agent","arguments":{"agent_id":"8582c7a4-0f02-4895-8570-7c7cce357e5f","input":{"text":"hi"}}}}`)
	rec := httptest.NewRecorder()
	c := e.NewContext(httptest.NewRequest(http.MethodPost, "/api/v1/mcp", bytes.NewReader(body)), rec)
	c.Set(string(httpx.CtxKeyAuthMethod), "apikey")
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
