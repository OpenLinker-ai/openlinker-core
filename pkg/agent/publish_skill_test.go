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

func TestServePublishAgentSkillIncludesDiscoveredCanonicalRuntimeOnboarding(t *testing.T) {
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
	assertContains(t, body, "OPENLINKER_AGENT_NODE_TRANSPORT=auto")
	assertContains(t, body, "30-minute expiry is only the first-registration window")
	assertContains(t, body, "clears expires_at")
	assertContains(t, body, "the creator revokes it.")
	assertContains(t, body, "https://api.stage.example/.well-known/openlinker.json")
	assertContains(t, body, "OPENLINKER_URL=https://api.stage.example")
	assertContains(t, body, "openlinker.NewRuntimeWorker")
	assertContains(t, body, "openlinker.RuntimeWorkerConfig")
	assertContains(t, body, "openlinker.RuntimeHandlerFunc")
	assertContains(t, body, `NodeID:      os.Getenv("OPENLINKER_NODE_ID")`)
	assertContains(t, body, "TypeScript uses the server-only Runtime entry")
	assertContains(t, body, "Python uses the async Runtime")
	assertContains(t, body, "platformURL, nodeID, agentID")
	assertContains(t, body, "node_id=node_id")
	assertContains(t, body, "await worker.start()")
	assertContains(t, body, "await worker.run()")
	assertContains(t, body, "RuntimeContext.Emit")
	assertContains(t, body, "RuntimeContext.CallAgent")
	assertContains(t, body, "Transport accepts `auto`, `ws` or `pull`.")
	assertContains(t, body, "the SDK starts with WebSocket, falls back to `pull`")
	assertContains(t, body, "probes WebSocket recovery while `pull` continues")
	assertContains(t, body, "Runtime Session")
	assertContains(t, body, "matching ACK")
	assertContains(t, body, "Runtime WebSocket/long-poll transport")
	assertContains(t, body, `"connection_mode": "runtime"`)
	assertContains(t, body, "mTLS")
	assertContains(t, body, "Agent Node compatibility Adapter")
	assertContains(t, body, "agent_node.helper")
	assertContains(t, body, "OPENLINKER_NODE_ID=11111111-1111-4111-8111-111111111111")
	assertContains(t, body, "OPENLINKER_AGENT_ID=22222222-2222-4222-8222-222222222222")
	assertContains(t, body, "OPENLINKER_AGENT_NODE_DATA_DIR=/var/lib/openlinker-agent-node")
	assertContains(t, body, "OPENLINKER_URL")
	assertNotContains(t, body, "/api/v1/agent-runtime/heartbeat")
	assertNotContains(t, body, "https://api.stage.example/api/v1/agent-runtime")
	assertNotContains(t, body, "CONNECT ${RUNTIME_ORIGIN}")
	assertNotContains(t, body, "POST /api/v1/agent-runtime/runs/claim")
	assertNotContains(t, body, "run.assignment_ack")
	assertNotContains(t, body, "run.assignment_confirmed")
	assertNotContains(t, body, "run.event_ack")
	assertNotContains(t, body, "run.result_ack")
}

func TestConsumeAgentSkillKeepsTasksPrivate(t *testing.T) {
	body := ConsumeAgentSkillMarkdown

	assertContains(t, body, "create a private task with create_task")
	assertContains(t, body, "Do not publish that task")
	assertNotContains(t, body, "publish a task")
	assertNotContains(t, body, "Task acceptance states")
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
