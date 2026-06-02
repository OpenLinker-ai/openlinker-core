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
