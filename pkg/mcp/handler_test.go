package mcp

import (
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
