package agent_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

func TestRegistrationHandler_AcceptsBearerDesignContract(t *testing.T) {
	pool := setupTestDB(t)
	creatorID := insertCreatorUser(t, pool, "Bearer Creator")
	svc := agent.NewRegistrationService(pool)
	minted, err := svc.CreateAgentToken(context.Background(), creatorID, &agent.CreateAgentTokenRequest{
		Name: "bearer-contract",
	})
	require.NoError(t, err)

	e := echo.New()
	e.HTTPErrorHandler = func(err error, c echo.Context) { _ = httpx.SendError(c, err) }
	api := e.Group("/api/v1")
	agent.NewRegistrationHandler(svc).RegisterPublic(api)

	body, err := json.Marshal(map[string]any{
		"name":         "Design Contract Agent",
		"endpoint_url": "https://example.com/design-agent",
		"ability_tags": []string{"research"},
		"visibility":   "private",
	})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/agent-registration/agents", bytes.NewReader(body))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	req.Header.Set(echo.HeaderAuthorization, "Bearer "+minted.PlaintextToken)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	var resp agent.RegisterAgentViaTokenResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Equal(t, "private", resp.Agent.Visibility)
	require.Equal(t, int32(0), resp.Agent.PricePerCallCents)
	require.Equal(t, []string{"research"}, resp.Agent.Tags)
	require.Equal(t, "active_runtime", resp.AgentToken.Status)
	require.Empty(t, resp.AgentToken.PlaintextToken)
}

func TestRegistrationHandler_ReplaysCompletedRegistrationOverBearer(t *testing.T) {
	pool := setupTestDB(t)
	creatorID := insertCreatorUser(t, pool, "Bearer Replay Creator")
	svc := agent.NewRegistrationService(pool)
	minted, err := svc.CreateAgentToken(context.Background(), creatorID, &agent.CreateAgentTokenRequest{
		Name: "bearer-replay",
	})
	require.NoError(t, err)

	e := echo.New()
	e.HTTPErrorHandler = func(err error, c echo.Context) { _ = httpx.SendError(c, err) }
	agent.NewRegistrationHandler(svc).RegisterPublic(e.Group("/api/v1"))
	slug := freshSlug("bearer-replay")
	post := func(slug string) *httptest.ResponseRecorder {
		body, err := json.Marshal(map[string]any{
			"slug":         slug,
			"name":         "Bearer Replay Agent",
			"endpoint_url": "https://example.com/bearer-replay",
			"ability_tags": []string{"research"},
		})
		require.NoError(t, err)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/agent-registration/agents", bytes.NewReader(body))
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
		req.Header.Set(echo.HeaderAuthorization, "Bearer "+minted.PlaintextToken)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		return rec
	}

	first := post(slug)
	require.Equal(t, http.StatusCreated, first.Code, first.Body.String())
	replayed := post(slug)
	require.Equal(t, http.StatusCreated, replayed.Code, replayed.Body.String())
	require.JSONEq(t, first.Body.String(), replayed.Body.String())
	require.NotContains(t, replayed.Body.String(), "plaintext_token")

	mismatch := post(freshSlug("bearer-other"))
	require.Equal(t, http.StatusUnauthorized, mismatch.Code, mismatch.Body.String())
}
