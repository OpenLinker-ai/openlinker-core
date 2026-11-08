package agent_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

func registrationCapability() *agent.UpsertCapabilityRequest {
	return &agent.UpsertCapabilityRequest{
		InputSchema: map[string]interface{}{
			"type":       "object",
			"properties": map[string]interface{}{"query": map[string]interface{}{"type": "string"}},
			"required":   []interface{}{"query"},
		},
		OutputSchema: map[string]interface{}{
			"type":       "object",
			"properties": map[string]interface{}{"result": map[string]interface{}{"type": "string"}},
			"required":   []interface{}{"result"},
		},
		Summary: "  answers one query  ",
	}
}

func registrationExamples(n int) []agent.CreateExampleRequest {
	out := make([]agent.CreateExampleRequest, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, agent.CreateExampleRequest{
			Title:              fmt.Sprintf("example %d", i),
			InputJSON:          map[string]interface{}{"query": fmt.Sprintf("q%d", i)},
			ExpectedOutputJSON: map[string]interface{}{"result": fmt.Sprintf("r%d", i)},
		})
	}
	return out
}

func nextActionTypes(resp *agent.RegisterAgentViaTokenResponse) []string {
	if resp.NextActions == nil {
		return nil
	}
	out := make([]string, 0, len(*resp.NextActions))
	for _, action := range *resp.NextActions {
		out = append(out, action.Type)
	}
	return out
}

func mintRegistrationToken(t *testing.T, svc *agent.RegistrationService, creatorID uuid.UUID) string {
	t.Helper()
	minted, err := svc.CreateAgentToken(context.Background(), creatorID, &agent.CreateAgentTokenRequest{Name: "registration readiness"})
	require.NoError(t, err)
	return minted.PlaintextToken
}

func capabilityVersion(t *testing.T, pool *pgxpool.Pool, agentID string) (int32, bool) {
	t.Helper()
	var version int32
	err := pool.QueryRow(context.Background(),
		`SELECT version FROM agent_capabilities WHERE agent_id = $1`, uuid.MustParse(agentID)).Scan(&version)
	if err != nil {
		return 0, false
	}
	return version, true
}

func exampleTitles(t *testing.T, pool *pgxpool.Pool, agentID string) []string {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT title FROM agent_examples WHERE agent_id = $1 ORDER BY sort_order, created_at`, uuid.MustParse(agentID))
	require.NoError(t, err)
	defer rows.Close()
	var titles []string
	for rows.Next() {
		var title string
		require.NoError(t, rows.Scan(&title))
		titles = append(titles, title)
	}
	require.NoError(t, rows.Err())
	return titles
}

func TestRegisterAgentViaToken_DeclaresCapabilityAndExamplesAtomically(t *testing.T) {
	pool := setupTestDB(t)
	creatorID := insertCreatorUser(t, pool, "Declaring Creator")
	svc := agent.NewRegistrationService(pool)
	ctx := context.Background()

	req := registrationReplayRequest(mintRegistrationToken(t, svc, creatorID), freshSlug("declared"))
	req.Capability = registrationCapability()
	req.Examples = registrationExamples(3)
	resp, err := svc.RegisterAgentViaToken(ctx, req)
	require.NoError(t, err)

	assert.Equal(t, "private", resp.Agent.Visibility, "omitted visibility defaults to private")
	require.NotNil(t, resp.Agent.Readiness)
	require.NotNil(t, resp.Agent.Availability)
	assert.False(t, resp.Agent.Readiness.Callable)
	assert.Equal(t, "unknown", resp.Agent.Availability.Status)
	require.NotNil(t, resp.Onboarding)
	assert.True(t, resp.Onboarding.EndpointSet)
	assert.True(t, resp.Onboarding.CapabilitiesSet)
	assert.True(t, resp.Onboarding.ExamplesSet)
	assert.False(t, resp.Onboarding.DryRunPassed)
	assert.Equal(t, []string{
		agent.RegistrationNextActionRunHealthCheck,
		agent.RegistrationNextActionMakePublic,
	}, nextActionTypes(resp))
	for _, action := range *resp.NextActions {
		assert.Equal(t, "agent", action.ResourceType)
		assert.Equal(t, resp.Agent.ID, action.ResourceID)
		assert.Contains(t, action.Href, "/hub/agents/"+resp.Agent.ID+"/")
	}
	makePublic := (*resp.NextActions)[1]
	assert.Equal(t, "PATCH", makePublic.AdditionalProps["api_method"])
	assert.Equal(t, "/api/v1/creator/agents/"+resp.Agent.ID+"/visibility", makePublic.AdditionalProps["api_path"])

	version, ok := capabilityVersion(t, pool, resp.Agent.ID)
	require.True(t, ok)
	assert.Equal(t, int32(1), version)
	var summary string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT summary FROM agent_capabilities WHERE agent_id = $1`, uuid.MustParse(resp.Agent.ID)).Scan(&summary))
	assert.Equal(t, "answers one query", summary)
	assert.Equal(t, []string{"example 0", "example 1", "example 2"}, exampleTitles(t, pool, resp.Agent.ID),
		"examples keep request order when sort_order is omitted")

	// The declared Agent is immediately due for the availability monitor and
	// can pass a dry-run without visiting the Web UI.
	agentSvc := newTestService(t, pool)
	agentSvc.SetDryRunner(mockDryRunner{output: map[string]interface{}{"result": "pong"}})
	dry, err := agentSvc.RunDryRun(ctx, uuid.MustParse(resp.Agent.ID), creatorID)
	require.NoError(t, err)
	assert.Equal(t, "pass", dry.Result)
}

func TestRegisterAgentViaToken_RuntimeWithoutDeclarationsListsAllSteps(t *testing.T) {
	pool := setupTestDB(t)
	creatorID := insertCreatorUser(t, pool, "Runtime Creator")
	svc := agent.NewRegistrationService(pool)

	req := registrationReplayRequest(mintRegistrationToken(t, svc, creatorID), freshSlug("runtime-steps"))
	req.EndpointURL = ""
	req.ConnectionMode = agent.ConnectionModeRuntime
	resp, err := svc.RegisterAgentViaToken(context.Background(), req)
	require.NoError(t, err)

	require.NotNil(t, resp.Onboarding, "registration creates the onboarding row")
	assert.False(t, resp.Onboarding.CapabilitiesSet)
	assert.False(t, resp.Onboarding.ExamplesSet)
	assert.Equal(t, []string{
		agent.RegistrationNextActionDeclareCapability,
		agent.RegistrationNextActionAddExample,
		agent.RegistrationNextActionStartRuntimeWorker,
		agent.RegistrationNextActionMakePublic,
	}, nextActionTypes(resp))
	worker := (*resp.NextActions)[2]
	assert.False(t, worker.RequiresHuman, "the registering Agent starts its own worker")
	assert.Equal(t, "agent_runtime", worker.Source)
	assert.Equal(t, "capability", (*resp.NextActions)[0].AdditionalProps["registration_field"])
}

func TestRegisterAgentViaToken_ExplicitPublicHasNoPublishStep(t *testing.T) {
	pool := setupTestDB(t)
	creatorID := insertCreatorUser(t, pool, "Public Creator")
	svc := agent.NewRegistrationService(pool)

	req := registrationReplayRequest(mintRegistrationToken(t, svc, creatorID), freshSlug("explicit-public"))
	req.Visibility = "public"
	req.Capability = registrationCapability()
	req.Examples = registrationExamples(1)
	resp, err := svc.RegisterAgentViaToken(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, "public", resp.Agent.Visibility)
	assert.True(t, resp.Agent.Readiness.Listed)
	assert.Equal(t, []string{agent.RegistrationNextActionRunHealthCheck}, nextActionTypes(resp))
}

func TestRegisterAgentViaToken_InvalidDeclarationsCreateNothing(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(req *agent.RegisterAgentViaTokenRequest)
		status int
	}{
		{"capability missing output schema", func(req *agent.RegisterAgentViaTokenRequest) {
			req.Capability = registrationCapability()
			req.Capability.OutputSchema = nil
		}, http.StatusUnprocessableEntity},
		{"capability invalid schema", func(req *agent.RegisterAgentViaTokenRequest) {
			req.Capability = registrationCapability()
			req.Capability.InputSchema = map[string]interface{}{"type": "nonsense"}
		}, http.StatusUnprocessableEntity},
		{"example mismatches declared schema", func(req *agent.RegisterAgentViaTokenRequest) {
			req.Capability = registrationCapability()
			req.Examples = registrationExamples(2)
			req.Examples[1].InputJSON = map[string]interface{}{"query": 7}
		}, http.StatusUnprocessableEntity},
		{"example expected output mismatches", func(req *agent.RegisterAgentViaTokenRequest) {
			req.Capability = registrationCapability()
			req.Examples = registrationExamples(1)
			req.Examples[0].ExpectedOutputJSON = map[string]interface{}{"other": "x"}
		}, http.StatusUnprocessableEntity},
		{"example without title", func(req *agent.RegisterAgentViaTokenRequest) {
			req.Examples = registrationExamples(1)
			req.Examples[0].Title = "  "
		}, http.StatusUnprocessableEntity},
		{"too many examples", func(req *agent.RegisterAgentViaTokenRequest) {
			req.Examples = registrationExamples(21)
		}, http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := setupTestDB(t)
			creatorID := insertCreatorUser(t, pool, "Invalid Declaration Creator")
			svc := agent.NewRegistrationService(pool)
			minted, err := svc.CreateAgentToken(context.Background(), creatorID, &agent.CreateAgentTokenRequest{Name: "invalid"})
			require.NoError(t, err)

			req := registrationReplayRequest(minted.PlaintextToken, freshSlug("invalid-declaration"))
			tc.mutate(req)
			_, err = svc.RegisterAgentViaToken(context.Background(), req)
			assertHTTPStatus(t, err, tc.status)
			assert.Zero(t, countCreatorAgents(t, pool, creatorID))
			token := readAgentTokenRow(t, pool, uuid.MustParse(minted.ID))
			assert.Equal(t, "pending_registration", token.status, "a rejected declaration must not redeem the token")
			assert.Nil(t, token.agentID)
		})
	}
}

func TestRegisterAgentViaToken_ExamplesWithoutCapabilityAreStoredUnchecked(t *testing.T) {
	pool := setupTestDB(t)
	creatorID := insertCreatorUser(t, pool, "Examples Only Creator")
	svc := agent.NewRegistrationService(pool)

	req := registrationReplayRequest(mintRegistrationToken(t, svc, creatorID), freshSlug("examples-only"))
	req.Examples = registrationExamples(2)
	req.Examples[0].SortOrder = 5
	req.Examples[1].SortOrder = 1
	resp, err := svc.RegisterAgentViaToken(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, []string{"example 1", "example 0"}, exampleTitles(t, pool, resp.Agent.ID),
		"explicit sort_order values are kept")
	assert.Equal(t, []string{
		agent.RegistrationNextActionDeclareCapability,
		agent.RegistrationNextActionRunHealthCheck,
		agent.RegistrationNextActionMakePublic,
	}, nextActionTypes(resp))
}

// Replay never applies capability / examples a second time and reports the
// stored state, whatever the retried body says.
func TestRegisterAgentViaToken_ReplayWithDeclarationsNeverReapplies(t *testing.T) {
	pool := setupTestDB(t)
	creatorID := insertCreatorUser(t, pool, "Replay Declaring Creator")
	svc := agent.NewRegistrationService(pool)
	ctx := context.Background()
	minted, err := svc.CreateAgentToken(ctx, creatorID, &agent.CreateAgentTokenRequest{Name: "replay"})
	require.NoError(t, err)
	tokenID := uuid.MustParse(minted.ID)
	slug := freshSlug("replay-declared")

	req := registrationReplayRequest(minted.PlaintextToken, slug)
	req.Capability = registrationCapability()
	req.Examples = registrationExamples(2)
	first, err := svc.RegisterAgentViaToken(ctx, req)
	require.NoError(t, err)
	tokenBefore := readAgentTokenRow(t, pool, tokenID)

	// Identical retry: same response, nothing written twice.
	replayed, err := svc.RegisterAgentViaToken(ctx, req)
	require.NoError(t, err)
	firstJSON, err := json.Marshal(first)
	require.NoError(t, err)
	replayJSON, err := json.Marshal(replayed)
	require.NoError(t, err)
	assert.JSONEq(t, string(firstJSON), string(replayJSON))
	version, ok := capabilityVersion(t, pool, first.Agent.ID)
	require.True(t, ok)
	assert.Equal(t, int32(1), version, "replay must not upsert the capability again")
	assert.Len(t, exampleTitles(t, pool, first.Agent.ID), 2, "replay must not add examples again")

	// A retry carrying different declarations is not applied either; the
	// response shows what is stored.
	changed := registrationReplayRequest(minted.PlaintextToken, slug)
	changed.Capability = registrationCapability()
	changed.Capability.Summary = "different"
	changed.Examples = registrationExamples(5)
	replayed, err = svc.RegisterAgentViaToken(ctx, changed)
	require.NoError(t, err)
	assert.Equal(t, first.Agent.ID, replayed.Agent.ID)
	assert.Len(t, exampleTitles(t, pool, first.Agent.ID), 2)
	version, _ = capabilityVersion(t, pool, first.Agent.ID)
	assert.Equal(t, int32(1), version)
	assert.Equal(t, nextActionTypes(first), nextActionTypes(replayed))

	// A retry that omits them still reports the stored declarations.
	bare := registrationReplayRequest(minted.PlaintextToken, slug)
	replayed, err = svc.RegisterAgentViaToken(ctx, bare)
	require.NoError(t, err)
	require.NotNil(t, replayed.Onboarding)
	assert.True(t, replayed.Onboarding.CapabilitiesSet)
	assert.NotContains(t, nextActionTypes(replayed), agent.RegistrationNextActionDeclareCapability)

	assert.Equal(t, tokenBefore, readAgentTokenRow(t, pool, tokenID), "replay must not write the token row")
	assert.Equal(t, 1, countCreatorAgents(t, pool, creatorID))
}

func TestRegisterAgentViaToken_ReplayWithoutDeclarationsDoesNotApplyLateOnes(t *testing.T) {
	pool := setupTestDB(t)
	creatorID := insertCreatorUser(t, pool, "Replay Late Declaring Creator")
	svc := agent.NewRegistrationService(pool)
	ctx := context.Background()
	token := mintRegistrationToken(t, svc, creatorID)
	slug := freshSlug("replay-late")

	first, err := svc.RegisterAgentViaToken(ctx, registrationReplayRequest(token, slug))
	require.NoError(t, err)
	assert.Contains(t, nextActionTypes(first), agent.RegistrationNextActionDeclareCapability)

	late := registrationReplayRequest(token, slug)
	late.Capability = registrationCapability()
	late.Examples = registrationExamples(1)
	replayed, err := svc.RegisterAgentViaToken(ctx, late)
	require.NoError(t, err)
	_, ok := capabilityVersion(t, pool, first.Agent.ID)
	assert.False(t, ok, "a replay never declares a capability")
	assert.Empty(t, exampleTitles(t, pool, first.Agent.ID))
	require.NotNil(t, replayed.Onboarding)
	assert.False(t, replayed.Onboarding.CapabilitiesSet)
	assert.Equal(t, nextActionTypes(first), nextActionTypes(replayed),
		"next_actions still ask for the capability the replay did not apply")

	// Owner progress after registration is reflected on replay.
	agentSvc := newTestService(t, pool)
	agentID := uuid.MustParse(first.Agent.ID)
	_, err = agentSvc.UpsertCapability(ctx, agentID, creatorID, registrationCapability())
	require.NoError(t, err)
	replayed, err = svc.RegisterAgentViaToken(ctx, registrationReplayRequest(token, slug))
	require.NoError(t, err)
	assert.NotContains(t, nextActionTypes(replayed), agent.RegistrationNextActionDeclareCapability)
}

// Replay stays read-only even for Agents registered before onboarding rows
// were created at registration time.
func TestRegisterAgentViaToken_ReplayDoesNotCreateMissingOnboardingRow(t *testing.T) {
	pool := setupTestDB(t)
	creatorID := insertCreatorUser(t, pool, "Legacy Replay Creator")
	svc := agent.NewRegistrationService(pool)
	ctx := context.Background()
	token := mintRegistrationToken(t, svc, creatorID)
	slug := freshSlug("legacy-replay")

	first, err := svc.RegisterAgentViaToken(ctx, registrationReplayRequest(token, slug))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `DELETE FROM agent_onboarding_status WHERE agent_id = $1`, uuid.MustParse(first.Agent.ID))
	require.NoError(t, err)

	replayed, err := svc.RegisterAgentViaToken(ctx, registrationReplayRequest(token, slug))
	require.NoError(t, err)
	assert.Nil(t, replayed.Onboarding)
	require.NotNil(t, replayed.NextActions)
	assert.Equal(t, nextActionTypes(first), nextActionTypes(replayed))
	var rows int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM agent_onboarding_status WHERE agent_id = $1`, uuid.MustParse(first.Agent.ID)).Scan(&rows))
	assert.Zero(t, rows)
}

func postRegistration(t *testing.T, e *echo.Echo, token string, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/agent-registration/agents", bytes.NewReader(raw))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	req.Header.Set(echo.HeaderAuthorization, "Bearer "+token)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	return rec
}

func TestRegistrationHandler_VisibilityDefaultAndDeclarationsOverHTTP(t *testing.T) {
	pool := setupTestDB(t)
	creatorID := insertCreatorUser(t, pool, "HTTP Declaring Creator")
	svc := agent.NewRegistrationService(pool)
	e := echo.New()
	e.HTTPErrorHandler = func(err error, c echo.Context) { _ = httpx.SendError(c, err) }
	agent.NewRegistrationHandler(svc).RegisterPublic(e.Group("/api/v1"))

	omitted := postRegistration(t, e, mintRegistrationToken(t, svc, creatorID), map[string]any{
		"name":         "Omitted Visibility Agent",
		"endpoint_url": "https://example.com/omitted",
		"ability_tags": []string{"research"},
		"capability": map[string]any{
			"input_schema":  map[string]any{"type": "object"},
			"output_schema": map[string]any{"type": "object"},
		},
		"examples": []map[string]any{{"title": "first", "input_json": map[string]any{"q": "x"}}},
	})
	require.Equal(t, http.StatusCreated, omitted.Code, omitted.Body.String())
	var raw map[string]any
	require.NoError(t, json.Unmarshal(omitted.Body.Bytes(), &raw))
	agentObj := raw["agent"].(map[string]any)
	assert.Equal(t, "private", agentObj["visibility"], "the handler no longer forces public")
	assert.Contains(t, agentObj, "readiness")
	assert.Contains(t, raw, "onboarding")
	actions, ok := raw["next_actions"].([]any)
	require.True(t, ok, "next_actions is a JSON array: %s", omitted.Body.String())
	require.Len(t, actions, 2)
	assert.Equal(t, agent.RegistrationNextActionRunHealthCheck, actions[0].(map[string]any)["type"])

	public := postRegistration(t, e, mintRegistrationToken(t, svc, creatorID), map[string]any{
		"name":         "Explicit Public Agent",
		"endpoint_url": "https://example.com/public",
		"ability_tags": []string{"research"},
		"visibility":   "public",
	})
	require.Equal(t, http.StatusCreated, public.Code, public.Body.String())
	var publicResp agent.RegisterAgentViaTokenResponse
	require.NoError(t, json.Unmarshal(public.Body.Bytes(), &publicResp))
	assert.Equal(t, "public", publicResp.Agent.Visibility)

	// Nested declaration fields are validated with the creator endpoint tags.
	invalidToken := mintRegistrationToken(t, svc, creatorID)
	invalid := postRegistration(t, e, invalidToken, map[string]any{
		"name":         "Invalid Example Agent",
		"endpoint_url": "https://example.com/invalid",
		"ability_tags": []string{"research"},
		"examples":     []map[string]any{{"title": "", "input_json": map[string]any{"q": "x"}}},
	})
	require.Equal(t, http.StatusUnprocessableEntity, invalid.Code, invalid.Body.String())
	missingSchema := postRegistration(t, e, invalidToken, map[string]any{
		"name":         "Invalid Capability Agent",
		"endpoint_url": "https://example.com/invalid",
		"ability_tags": []string{"research"},
		"capability":   map[string]any{"input_schema": map[string]any{"type": "object"}},
	})
	require.Equal(t, http.StatusUnprocessableEntity, missingSchema.Code, missingSchema.Body.String())
	assert.Equal(t, 2, countCreatorAgents(t, pool, creatorID))
}

func TestCreateAgentExplicitVisibilityStillHonoured(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ownerID := insertCreator(t, pool)
	for _, visibility := range []string{"public", "unlisted", "private"} {
		req := validCreateReq(freshSlug("explicit-" + visibility))
		req.Visibility = visibility
		resp, err := svc.CreateAgent(context.Background(), ownerID, req)
		require.NoError(t, err)
		assert.Equal(t, visibility, resp.Visibility)
	}
}
