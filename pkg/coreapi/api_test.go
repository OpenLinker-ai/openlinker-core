package coreapi

import (
	"context"
	"net/http"
	"testing"

	"github.com/gorilla/sessions"
	"github.com/labstack/echo/v4"
	"github.com/markbates/goth"
	"github.com/markbates/goth/gothic"

	"github.com/kinzhi/openlinker-core/pkg/config"
)

func TestConfigureGothSetsSessionStoreAndProviders(t *testing.T) {
	resetGothGlobals(t)

	ConfigureGoth(&config.Config{
		Env:                "production",
		JWTSecret:          "test-secret",
		APIURL:             "https://api.openlinker.test",
		GoogleClientID:     "google-id",
		GoogleClientSecret: "google-secret",
		GithubClientID:     "github-id",
		GithubClientSecret: "github-secret",
	})

	store, ok := gothic.Store.(*sessions.CookieStore)
	if !ok {
		t.Fatalf("gothic.Store = %T, want *sessions.CookieStore", gothic.Store)
	}
	if store.Options == nil {
		t.Fatalf("cookie store options should be configured")
	}
	if !store.Options.HttpOnly {
		t.Fatalf("cookie store should be HttpOnly")
	}
	if !store.Options.Secure {
		t.Fatalf("production cookie store should be Secure")
	}
	if store.Options.SameSite != http.SameSiteLaxMode {
		t.Fatalf("SameSite = %v, want %v", store.Options.SameSite, http.SameSiteLaxMode)
	}
	if store.Options.Path != "/" {
		t.Fatalf("Path = %q, want /", store.Options.Path)
	}
	if store.Options.MaxAge != 600 {
		t.Fatalf("MaxAge = %d, want 600", store.Options.MaxAge)
	}
	if provider, err := goth.GetProvider("google"); err != nil || provider.Name() != "google" {
		t.Fatalf("google provider = %v, %v", provider, err)
	}
	if provider, err := goth.GetProvider("github"); err != nil || provider.Name() != "github" {
		t.Fatalf("github provider = %v, %v", provider, err)
	}
}

func TestConfigureGothDevelopmentStoreSkipsMissingProviders(t *testing.T) {
	resetGothGlobals(t)

	ConfigureGoth(&config.Config{
		Env:       "development",
		JWTSecret: "test-secret",
		APIURL:    "http://localhost:8080",
	})

	store, ok := gothic.Store.(*sessions.CookieStore)
	if !ok {
		t.Fatalf("gothic.Store = %T, want *sessions.CookieStore", gothic.Store)
	}
	if store.Options.Secure {
		t.Fatalf("development cookie store should not be Secure")
	}
	if _, err := goth.GetProvider("google"); err == nil {
		t.Fatalf("google provider should not be registered without credentials")
	}
	if _, err := goth.GetProvider("github"); err == nil {
		t.Fatalf("github provider should not be registered without credentials")
	}
}

func TestRegisterMountsCoreRoutesAndReturnsServices(t *testing.T) {
	resetGothGlobals(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	e := echo.New()
	services := Register(ctx, e, nil, &config.Config{
		Env:                           "development",
		JWTSecret:                     "test-secret",
		JWTExpireHours:                24,
		APIURL:                        "https://api.openlinker.test",
		FrontendURL:                   "https://openlinker.test",
		RunTimeoutSeconds:             60,
		AllowLocalHTTPEndpoints:       false,
		AvailabilityMonitorEnabled:    false,
		RuntimePullRunWorkerEnabled:   false,
		WorkflowRunWorkerEnabled:      false,
		RegistryProxyRunWorkerEnabled: false,
	}, Options{
		AdminMiddleware: func(next echo.HandlerFunc) echo.HandlerFunc {
			return func(c echo.Context) error {
				return next(c)
			}
		},
	})

	if services == nil {
		t.Fatalf("Register returned nil services")
	}
	if services.Auth == nil || services.AgentMarket == nil || services.Agent == nil || services.Skill == nil ||
		services.Runtime == nil || services.Webhook == nil || services.A2A == nil || services.Workflow == nil ||
		services.Registry == nil || services.Benchmark == nil || services.Task == nil || services.MCP == nil ||
		services.Delivery == nil {
		t.Fatalf("Register returned incomplete services: %#v", services)
	}

	routes := routeSet(e)
	expected := []string{
		"GET /.well-known/openlinker.json",
		"GET /skill/publish-agent",
		"GET /skill/consume-agent",
		"GET /internal/core/v1/runs",
		"GET /internal/core/v1/dashboard",
		"GET /internal/core/v1/creator/dashboard",
		"GET /internal/core/v1/creator/agents/:id/runs",
		"POST /api/v1/auth/register",
		"GET /api/v1/me",
		"GET /api/v1/agents",
		"POST /api/v1/creator/agents",
		"GET /api/v1/admin/agents/pending",
		"POST /api/v1/agent-registration/agents",
		"POST /api/v1/agent-runtime/heartbeat",
		"GET /api/v1/agent-runtime/runs/claim",
		"POST /api/v1/agent-runtime/call-agent",
		"POST /api/v1/a2a/agents/:slug",
		"GET /api/v1/a2a/agents/:slug/.well-known/agent-card.json",
		"GET /api/v1/a2a/agents/:slug/tasks/:taskID/subscribe",
		"GET /api/v1/mcp/tools",
		"POST /api/v1/mcp/run_agent",
		"POST /api/v1/workflows/:id/run",
		"GET /api/v1/skills",
		"GET /api/v1/benchmark/status",
		"POST /api/v1/delivery-targets",
		"POST /api/v1/proxy/runs",
	}
	for _, key := range expected {
		if !routes[key] {
			t.Fatalf("route %s not registered; routes=%v", key, sortedRouteKeys(routes))
		}
	}
}

func resetGothGlobals(t *testing.T) {
	t.Helper()
	previousStore := gothic.Store
	goth.ClearProviders()
	t.Cleanup(func() {
		goth.ClearProviders()
		gothic.Store = previousStore
	})
}

func routeSet(e *echo.Echo) map[string]bool {
	routes := make(map[string]bool)
	for _, route := range e.Routes() {
		if route.Method == echo.RouteNotFound {
			continue
		}
		routes[route.Method+" "+route.Path] = true
	}
	return routes
}

func sortedRouteKeys(routes map[string]bool) []string {
	keys := make([]string, 0, len(routes))
	for key := range routes {
		keys = append(keys, key)
	}
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}
