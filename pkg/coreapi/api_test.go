package coreapi

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/sessions"
	"github.com/labstack/echo/v4"
	"github.com/markbates/goth"
	"github.com/markbates/goth/gothic"

	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	"github.com/OpenLinker-ai/openlinker-core/pkg/externalexecution"
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

func TestOAuthSessionSecretPrefersDedicatedSecret(t *testing.T) {
	if got := oauthSessionSecret(&config.Config{JWTSecret: "jwt", OAuthSessionSecret: " oauth "}); got != "oauth" {
		t.Fatalf("oauthSessionSecret = %q, want dedicated secret", got)
	}
	if got := oauthSessionSecret(&config.Config{JWTSecret: "jwt"}); got != "jwt" {
		t.Fatalf("oauthSessionSecret fallback = %q, want jwt", got)
	}
}

func TestNewAuthServiceAcceptsConfiguredOAuthCodeStorageModes(t *testing.T) {
	for _, mode := range []string{"", "legacy-jwt", "subject-only"} {
		t.Run(mode, func(t *testing.T) {
			if svc := newAuthService(nil, &config.Config{
				JWTSecret:            "test-secret",
				JWTExpireHours:       24,
				OAuthCodeStorageMode: mode,
			}); svc == nil {
				t.Fatal("newAuthService returned nil")
			}
		})
	}
}

func TestNewAuthServiceRejectsUnknownOAuthCodeStorageMode(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("newAuthService should fail closed for an unknown OAuth code storage mode")
		}
	}()
	_ = newAuthService(nil, &config.Config{
		JWTSecret:            "test-secret",
		JWTExpireHours:       24,
		OAuthCodeStorageMode: "secret-looking-invalid-mode",
	})
}

func TestConfigureGothUsesOAuthCallbackBaseURLWhenSet(t *testing.T) {
	resetGothGlobals(t)

	ConfigureGoth(&config.Config{
		JWTSecret:            "test-secret",
		APIURL:               "https://api.openlinker.test",
		OAuthCallbackBaseURL: " https://openlinker.test/ ",
		GoogleClientID:       "google-id",
		GoogleClientSecret:   "google-secret",
		GithubClientID:       "github-id",
		GithubClientSecret:   "github-secret",
	})

	googleProvider, err := goth.GetProvider("google")
	if err != nil {
		t.Fatalf("google provider: %v", err)
	}
	googleSession, err := googleProvider.BeginAuth("state")
	if err != nil {
		t.Fatalf("google BeginAuth: %v", err)
	}
	googleAuthURL, err := googleSession.GetAuthURL()
	if err != nil {
		t.Fatalf("google auth url: %v", err)
	}
	if !strings.Contains(googleAuthURL, "redirect_uri=https%3A%2F%2Fopenlinker.test%2Fapi%2Fv1%2Fauth%2Fgoogle%2Fcallback") {
		t.Fatalf("google auth url does not use callback base: %s", googleAuthURL)
	}

	githubProvider, err := goth.GetProvider("github")
	if err != nil {
		t.Fatalf("github provider: %v", err)
	}
	githubSession, err := githubProvider.BeginAuth("state")
	if err != nil {
		t.Fatalf("github BeginAuth: %v", err)
	}
	githubAuthURL, err := githubSession.GetAuthURL()
	if err != nil {
		t.Fatalf("github auth url: %v", err)
	}
	if !strings.Contains(githubAuthURL, "redirect_uri=https%3A%2F%2Fopenlinker.test%2Fapi%2Fv1%2Fauth%2Fgithub%2Fcallback") {
		t.Fatalf("github auth url does not use callback base: %s", githubAuthURL)
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
		WorkflowRunWorkerEnabled:      false,
		RegistryProxyRunWorkerEnabled: false,
	}, Options{
		AdminMiddleware: func(next echo.HandlerFunc) echo.HandlerFunc {
			return func(c echo.Context) error {
				return next(c)
			}
		},
		ExternalExecutionAuthorizer: testExternalExecutionAuthorizer(t),
	})

	if services == nil {
		t.Fatalf("Register returned nil services")
	}
	if services.Auth == nil || services.Admin == nil || services.AgentMarket == nil || services.Agent == nil || services.Skill == nil ||
		services.Runtime == nil || services.RuntimeController == nil || services.Webhook == nil || services.A2A == nil || services.Workflow == nil || services.ExternalExecution == nil ||
		services.Registry == nil || services.Benchmark == nil || services.Task == nil || services.MCP == nil ||
		services.Delivery == nil || services.UserToken == nil {
		t.Fatalf("Register returned incomplete services: %#v", services)
	}
	if services.EventWake != nil {
		t.Fatal("Register configured event wake without a PostgreSQL pool")
	}

	routes := routeSet(e)
	expected := []string{
		"GET /.well-known/openlinker.json",
		"GET /skill/publish-agent",
		"GET /skill/consume-agent",
		"GET /api/v1/runs",
		"GET /api/v1/dashboard",
		"GET /api/v1/creator/dashboard",
		"GET /api/v1/creator/agents/:id/runs",
		"GET /api/v1/me",
		"POST /api/v1/user-tokens",
		"GET /api/v1/user-tokens/:id",
		"PATCH /api/v1/user-tokens/:id",
		"DELETE /api/v1/user-tokens/:id",
		"POST /internal/user-tokens/introspect",
		"POST /internal/external-execution-targets/validate",
		"POST /internal/external-executions",
		"GET /internal/external-executions/:external_request_id",
		"GET /api/v1/agents",
		"POST /api/v1/creator/agents",
		"GET /api/v1/admin/summary",
		"GET /api/v1/admin/users",
		"POST /api/v1/admin/users",
		"PATCH /api/v1/admin/users/:id/flags",
		"GET /api/v1/admin/agents",
		"PATCH /api/v1/admin/agents/:id/moderation",
		"GET /api/v1/admin/tasks",
		"GET /api/v1/admin/agents/pending",
		"GET /api/v1/admin/runtime/dead-letters",
		"GET /api/v1/admin/runtime/nodes",
		"GET /api/v1/admin/runtime/maintenance",
		"POST /api/v1/admin/runtime/nodes/:id/drain",
		"POST /api/v1/admin/runtime/nodes/:id/revoke",
		"POST /api/v1/agent-registration/agents",
		"POST /api/v1/runs/:id/cancel",
		"POST /api/v1/a2a/agents/:slug",
		"GET /api/v1/a2a/agents/:slug/.well-known/agent-card.json",
		"GET /api/v1/a2a/agents/:slug/tasks/:taskID/subscribe",
		"GET /api/v1/mcp/tools",
		"POST /api/v1/mcp/run_agent",
		"POST /api/v1/workflows/:id/run",
		"GET /api/v1/skills",
		"POST /api/v1/skills/proposals",
		"GET /api/v1/creator/skill-proposals",
		"GET /api/v1/benchmark/status",
		"POST /api/v1/delivery-targets",
		"PATCH /api/v1/delivery-targets/:id",
		"GET /api/v1/deliveries",
		"POST /api/v1/proxy/runs",
	}
	for _, key := range expected {
		if !routes[key] {
			t.Fatalf("route %s not registered; routes=%v", key, sortedRouteKeys(routes))
		}
	}
	for _, legacy := range []string{
		"POST /internal/hosted/service-targets/validate",
		"POST /internal/hosted/service-executions",
		"GET /internal/hosted/service-executions/:external_order_id",
	} {
		if routes[legacy] {
			t.Fatalf("legacy Hosted bridge route %s must not be registered", legacy)
		}
	}
}

func TestRegisterRuntimeAttachOnlyMountsStrictReadOnlySurface(t *testing.T) {
	e := echo.New()
	services := RegisterRuntimeAttachOnly(
		context.Background(),
		e,
		nil,
		&config.Config{JWTSecret: "test-secret", JWTExpireHours: 24},
		Options{},
	)
	if services == nil || services.Auth == nil || services.Agent == nil ||
		services.Runtime == nil || services.RuntimeController == nil {
		t.Fatalf("RegisterRuntimeAttachOnly returned incomplete services: %#v", services)
	}

	routes := routeSet(e)
	for _, route := range []string{
		"POST /api/v1/auth/login",
		"GET /api/v1/creator/agents",
		"GET /api/v1/creator/agents/:id/onboarding",
		"GET /api/v1/creator/agent-tokens",
		"POST /api/v1/agent-runtime/sessions",
		"POST /api/v1/agent-runtime/sessions/:id/heartbeat",
		"POST /api/v1/agent-runtime/sessions/:id/drain",
		"POST /api/v1/agent-runtime/sessions/:id/close",
		"GET /api/v1/agent-runtime/ws",
	} {
		if !routes[route] {
			t.Fatalf("attach-only route %s not registered; routes=%v", route, sortedRouteKeys(routes))
		}
	}
	for _, forbidden := range []string{
		"POST /api/v1/auth/register",
		"POST /api/v1/auth/refresh",
		"POST /api/v1/creator/agents",
		"PATCH /api/v1/creator/agents/:id",
		"POST /api/v1/creator/agent-tokens",
		"DELETE /api/v1/creator/agent-tokens/:id",
		"POST /api/v1/agent-registration/agents",
		"POST /api/v1/agent-runtime/runs/claim",
		"POST /api/v1/agent-runtime/runs/:id/events",
		"POST /api/v1/agent-runtime/runs/:id/result",
		"POST /api/v1/agent-runtime/runs/resume",
		"POST /api/v1/agent-runtime/call-agent",
		"POST /api/v1/runs",
		"POST /api/v1/mcp/run_agent",
		"POST /api/v1/workflows/:id/run",
	} {
		if routes[forbidden] {
			t.Fatalf("attach-only mode exposed forbidden route %s", forbidden)
		}
	}
}

func testExternalExecutionAuthorizer(t *testing.T) *externalexecution.Authorizer {
	t.Helper()
	seed := bytes.Repeat([]byte{3}, ed25519.SeedSize)
	publicKey := ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)
	authorizer, err := externalexecution.NewAuthorizer(
		[]externalexecution.VerificationKey{{KeyID: "test", PublicKey: base64.RawStdEncoding.EncodeToString(publicKey)}},
		"openlinker-cloud", "openlinker-core.external-execution", "openlinker-cloud", acceptingReplayStore{},
	)
	if err != nil {
		t.Fatal(err)
	}
	return authorizer
}

type acceptingReplayStore struct{}

func (acceptingReplayStore) Consume(context.Context, string, string, time.Duration) (bool, error) {
	return true, nil
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
