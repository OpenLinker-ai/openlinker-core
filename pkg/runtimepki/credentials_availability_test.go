package runtimepki

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	"github.com/OpenLinker-ai/openlinker-core/pkg/credential"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	coreruntime "github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

func TestCredentialIssueAndRenewClosedStoreReturn503(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "postgres://unused:unused@127.0.0.1:1/unused?sslmode=disable")
	require.NoError(t, err)
	pool.Close()
	tokens := coreruntime.NewService(pool, &config.Config{})
	service := NewCredentialService(pool, &Manager{}, tokens)
	e := echo.New()
	e.HTTPErrorHandler = func(err error, c echo.Context) { _ = httpx.SendError(c, err) }
	service.Register(e.Group("/api/v1"))
	token, _, err := credential.GenerateAgentToken()
	require.NoError(t, err)
	for _, path := range []string{"/api/v1/runtime-credentials", "/api/v1/runtime-credentials/renew"} {
		for _, rawToken := range []string{token, "invalid-token"} {
			req := httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}"))
			req.Header.Set(echo.HeaderAuthorization, "Bearer "+rawToken)
			req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)
			var body coreruntime.RuntimeError
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			if rawToken == token {
				require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
				require.Equal(t, coreruntime.RuntimeErrorServiceUnavailable, body.Error.Code)
				require.True(t, body.Error.Retryable)
			} else {
				require.Equal(t, http.StatusUnauthorized, rec.Code)
				require.Equal(t, coreruntime.RuntimeErrorUnauthorized, body.Error.Code)
				require.False(t, body.Error.Retryable)
			}
			require.NotContains(t, rec.Body.String(), rawToken)
		}
	}
}
