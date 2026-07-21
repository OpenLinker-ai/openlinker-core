package a2a

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

// TestA2ARestRoutingDispatchesSpecPaths locks the A2A HTTP+JSON (REST) transport
// at the router level. The other handler tests invoke methods directly with
// pre-populated params, so they do not prove that the canonical A2A REST spec
// paths — derived from the google.api.http annotations in proto/a2a/v1/a2a.proto
// — actually reach the correct handler through the real Echo router. That
// dispatch relies on subtle behavior (the `/message:action` colon param, the
// `:subscribe` suffix redirect inside GetTaskHTTP, and the `/tasks/*` catch-all
// for `:cancel`); a route refactor could silently break native REST clients
// without this guard.
func TestA2ARestRoutingDispatchesSpecPaths(t *testing.T) {
	taskID := uuid.MustParse("c93dbab2-404f-4460-bcb7-0f17ece85567").String()
	configID := uuid.MustParse("2f151345-b29a-463b-90fc-7e20e27fbf20").String()
	const slug = "agent-one"
	const base = "/api/v1/a2a/agents/" + slug

	messageBody := `{"message":{"messageId":"m1","role":"user","parts":[{"kind":"text","text":"hi"}]}}`

	cases := []struct {
		name       string
		method     string
		path       string
		body       string
		wantStatus int
		wantCall   string
		streaming  bool
	}{
		{"SendMessage", http.MethodPost, base + "/message:send?version=1.0", messageBody, http.StatusOK, "message/send", false},
		{"SendStreamingMessage", http.MethodPost, base + "/message:stream?version=1.0", messageBody, http.StatusOK, "message/stream", true},
		{"ListTasks", http.MethodGet, base + "/tasks?version=1.0", "", http.StatusOK, "tasks/list", false},
		{"GetTask", http.MethodGet, base + "/tasks/" + taskID + "?version=1.0", "", http.StatusOK, "tasks/get", false},
		{"CancelTask", http.MethodPost, base + "/tasks/" + taskID + ":cancel?version=1.0", "", http.StatusOK, "tasks/cancel", false},
		{"SubscribeToTask", http.MethodGet, base + "/tasks/" + taskID + ":subscribe?version=1.0", "", http.StatusOK, "tasks/get", true},
		{"CreatePushConfig", http.MethodPost, base + "/tasks/" + taskID + "/pushNotificationConfigs", `{}`, http.StatusCreated, "push/set", false},
		{"ListPushConfigs", http.MethodGet, base + "/tasks/" + taskID + "/pushNotificationConfigs", "", http.StatusOK, "push/list", false},
		{"GetPushConfig", http.MethodGet, base + "/tasks/" + taskID + "/pushNotificationConfigs/" + configID, "", http.StatusOK, "push/get", false},
		{"DeletePushConfig", http.MethodDelete, base + "/tasks/" + taskID + "/pushNotificationConfigs/" + configID, "", http.StatusNoContent, "push/delete", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := newFakeA2AService(taskID)
			h := NewHandler(svc)
			h.SetAgentCardProvider(&fakeA2ACardProvider{})

			e := echo.New()
			group := e.Group(
				"/api/v1/a2a/agents/:slug",
				a2aRESTTestAuthInjector,
				h.resolveA2ATargetAgent,
				a2aHTTPErrorMiddleware,
			)
			h.registerProtocolRoutes(group)

			var reader *strings.Reader
			if tc.body != "" {
				reader = strings.NewReader(tc.body)
			} else {
				reader = strings.NewReader("")
			}
			req := httptest.NewRequest(tc.method, tc.path, reader)
			if tc.body != "" {
				req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
			}
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("%s %s => status %d (want %d), body=%s", tc.method, tc.path, rec.Code, tc.wantStatus, rec.Body.String())
			}
			if !svc.called(tc.wantCall) {
				t.Fatalf("%s %s did not dispatch %q; recorded calls=%v", tc.method, tc.path, tc.wantCall, svc.calls)
			}
			if tc.streaming {
				if !svc.called("events") {
					t.Fatalf("%s %s streaming path did not reach the event stream; calls=%v", tc.method, tc.path, svc.calls)
				}
				if ct := rec.Header().Get(echo.HeaderContentType); !strings.Contains(ct, "text/event-stream") {
					t.Fatalf("%s %s streaming Content-Type = %q, want text/event-stream", tc.method, tc.path, ct)
				}
			}
		})
	}
}

// a2aRESTTestAuthInjector stands in for the production auth/query middleware,
// seeding the request context with an authenticated user and the superset of
// scopes required across every A2A protocol route so that routing — not
// authorization — is the property under test.
func a2aRESTTestAuthInjector(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		c.Set(string(httpx.CtxKeyUserID), "8582c7a4-0f02-4895-8570-7c7cce357e5f")
		c.Set(string(httpx.CtxKeyAuthMethod), "user_token")
		c.Set(string(httpx.CtxKeyAuthScopes), []string{
			"agents:run", "agents:read", "runs:read", "runs:cancel",
		})
		return next(c)
	}
}
