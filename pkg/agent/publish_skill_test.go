package agent

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"
)

func TestServeConsumeAgentSkillUsesRequestOriginWhenEnvMissing(t *testing.T) {
	t.Setenv("API_URL", "")
	t.Setenv("FRONTEND_URL", "")

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "https://staging.example/skill/consume-agent", nil)
	rec := httptest.NewRecorder()

	if err := ServeConsumeAgentSkill(e.NewContext(req, rec)); err != nil {
		t.Fatalf("ServeConsumeAgentSkill() error = %v", err)
	}
	body := rec.Body.String()
	assertContains(t, body, "https://staging.example/mcp")
	assertContains(t, body, "https://staging.example/api/v1/agents?keyword=data")
	assertNotContains(t, body, "openlinker.ai")
	assertNotContains(t, body, skillDocAPIBase)
	assertNotContains(t, body, skillDocWebBase)
}

func TestServePublishAgentSkillUsesConfiguredBaseURLs(t *testing.T) {
	t.Setenv("API_URL", "https://api.stage.example/")
	t.Setenv("FRONTEND_URL", "https://stage.example/")

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "http://internal.local/skill/publish-agent", nil)
	rec := httptest.NewRecorder()

	if err := ServePublishAgentSkill(e.NewContext(req, rec)); err != nil {
		t.Fatalf("ServePublishAgentSkill() error = %v", err)
	}
	body := rec.Body.String()
	assertContains(t, body, "https://api.stage.example/api/v1/agent-registration/agents")
	assertContains(t, body, "https://stage.example/mcp")
	assertNotContains(t, body, "openlinker.ai")
	assertNotContains(t, body, skillDocAPIBase)
	assertNotContains(t, body, skillDocWebBase)
}

func TestServePublishAgentSkillIncludesCanonicalRuntimePullOnboarding(t *testing.T) {
	t.Setenv("API_URL", "https://api.stage.example/")
	t.Setenv("FRONTEND_URL", "https://stage.example/")

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "http://internal.local/skill/publish-agent", nil)
	rec := httptest.NewRecorder()

	if err := ServePublishAgentSkill(e.NewContext(req, rec)); err != nil {
		t.Fatalf("ServePublishAgentSkill() error = %v", err)
	}
	body := rec.Body.String()

	assertContains(t, body, "GET https://api.stage.example/api/v1/skills")
	assertContains(t, body, "Map your own internal skills or tools to at most 5 existing OpenLinker skill_ids")
	assertContains(t, body, "Do not invent new skill_ids")
	assertContains(t, body, "If no run is returned, do not exit")
	assertContains(t, body, "Hard runtime contract")
	assertContains(t, body, "Treat it as a hard server limit")
	assertContains(t, body, `"result_endpoint": "/api/v1/agent-runtime/runs/RUN_ID/result"`)
	assertContains(t, body, `"result_required": true`)
	assertContains(t, body, "Every claimed run must end with POST /agent-runtime/runs/{run_id}/result")
	assertContains(t, body, "always POST /agent-runtime/runs/{run_id}/result")
	assertContains(t, body, "Re-claiming a stale run does not extend result_timeout_seconds")
	assertContains(t, body, "Keep the worker process alive under a supervisor")
	assertContains(t, body, "OPENLINKER_API_BASE")
}

func TestPublicAgentSkillDocsUseCurrentCredentialNamesWithoutRoadmapCopy(t *testing.T) {
	body := PublishAgentSkillMarkdown + "\n" + ConsumeAgentSkillMarkdown

	assertContains(t, body, "OPENLINKER_USER_TOKEN")
	assertContains(t, body, "OPENLINKER_AGENT_TOKEN")
	assertContains(t, body, "ol_user_***")
	assertContains(t, body, "ol_agent_***")

	for _, unwanted := range []string{
		"Phase 1",
		"later actions",
		"display-only",
		"price fields",
		"price_per_call_cents",
		"in this release",
	} {
		assertNotContains(t, body, unwanted)
	}
}

func assertContains(t *testing.T, body string, want string) {
	t.Helper()
	if !strings.Contains(body, want) {
		t.Fatalf("expected response to contain %q", want)
	}
}

func assertNotContains(t *testing.T, body string, unwanted string) {
	t.Helper()
	if strings.Contains(body, unwanted) {
		t.Fatalf("expected response not to contain %q", unwanted)
	}
}
