package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	migratecmd "github.com/golang-migrate/migrate/v4"
	migratefile "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/migrationinit"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

func TestAllowedCORSOrigins(t *testing.T) {
	tests := []struct {
		name string
		cfg  *config.Config
		want []string
	}{
		{
			name: "development includes frontend and localhost",
			cfg:  &config.Config{Env: "development", FrontendURL: " https://app.example "},
			want: []string{"https://app.example", "http://localhost:3000"},
		},
		{
			name: "development without frontend keeps localhost",
			cfg:  &config.Config{Env: "development"},
			want: []string{"http://localhost:3000"},
		},
		{
			name: "production only includes configured frontend",
			cfg:  &config.Config{Env: "production", FrontendURL: "https://app.example"},
			want: []string{"https://app.example"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := allowedCORSOrigins(tt.cfg); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("allowedCORSOrigins() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestValidateProductionConfig(t *testing.T) {
	validJWTSecret := strings.Repeat("j", 32)
	if err := validateProductionConfig(&config.Config{Env: "development"}); err != nil {
		t.Fatalf("development config should pass: %v", err)
	}
	if err := validateProductionConfig(&config.Config{
		Env: "development", ExternalExecutionCallerServiceID: "custom-cloud",
	}); err == nil || !strings.Contains(err.Error(), "must be \"openlinker-cloud\"") {
		t.Fatalf("custom external execution caller must fail in every environment: %v", err)
	}
	if err := validateProductionConfig(&config.Config{Env: "production"}); err == nil || !strings.Contains(err.Error(), "FRONTEND_URL") {
		t.Fatalf("missing frontend error = %v", err)
	}
	if err := validateProductionConfig(&config.Config{
		Env:            "production",
		FrontendURL:    "https://app.example",
		ReleaseVersion: "20260712-test",
		ReleaseCommit:  "0123456789abcdef",
		JWTSecret:      validJWTSecret,
	}); err != nil {
		t.Fatalf("valid production config without internal integrations: %v", err)
	}
	if err := validateProductionConfig(&config.Config{
		Env:            "production",
		FrontendURL:    "https://app.example",
		ReleaseVersion: "20260712-test",
		ReleaseCommit:  "0123456789abcdef",
		JWTSecret:      validJWTSecret,
		InternalToken:  strings.Repeat("i", 32),
	}); err != nil {
		t.Fatalf("valid production config error = %v", err)
	}
	if err := validateProductionConfig(&config.Config{
		Env: "production", FrontendURL: "https://app.example",
		ReleaseVersion: "20260712-test", ReleaseCommit: "0123456789abcdef", JWTSecret: validJWTSecret,
		ExternalExecutionJWTCurrentPublicKey: "public-key", ExternalExecutionCallerServiceID: "openlinker-cloud",
	}); err == nil || !strings.Contains(err.Error(), "REDIS_URL") {
		t.Fatalf("missing external execution Redis error = %v", err)
	}
	if err := validateProductionConfig(&config.Config{
		Env: "production", FrontendURL: "https://app.example",
		ReleaseVersion: "20260712-test", ReleaseCommit: "0123456789abcdef", JWTSecret: validJWTSecret,
		RedisURL: "redis://redis:6379/0", ExternalExecutionJWTCurrentPublicKey: "public-key",
	}); err == nil || !strings.Contains(err.Error(), "caller service id") {
		t.Fatalf("missing external execution identity metadata error = %v", err)
	}
	if err := validateProductionConfig(&config.Config{
		Env: "production", FrontendURL: "https://app.example",
		ReleaseVersion: "20260712-test", ReleaseCommit: "0123456789abcdef", JWTSecret: validJWTSecret,
		RedisURL: "redis://redis:6379/0", ExternalExecutionJWTCurrentPublicKey: "public-key",
		ExternalExecutionJWTCurrentKeyID: "current", ExternalExecutionJWTIssuer: "openlinker-cloud",
		ExternalExecutionJWTAudience: "openlinker-core.external-execution", ExternalExecutionCallerServiceID: "openlinker-cloud",
	}); err != nil {
		t.Fatalf("valid external execution production config error = %v", err)
	}
	if err := validateProductionConfig(&config.Config{
		Env: "production", FrontendURL: "https://app.example",
		ReleaseVersion: "20260712-test", ReleaseCommit: "0123456789abcdef", JWTSecret: validJWTSecret,
		RedisURL: "redis://redis:6379/0", ExternalExecutionJWTCurrentPublicKey: "public-key",
		ExternalExecutionJWTCurrentKeyID: "current", ExternalExecutionJWTIssuer: "openlinker-cloud",
		ExternalExecutionJWTAudience: "openlinker-core.external-execution", ExternalExecutionCallerServiceID: "openlinker-cloud",
		ExternalExecutionJWTNextPublicKey: "next-public-key",
	}); err == nil || !strings.Contains(err.Error(), "configured together") {
		t.Fatalf("half-configured next key error = %v", err)
	}
	if err := validateProductionConfig(&config.Config{
		Env: "production", FrontendURL: "https://app.example",
		ReleaseVersion: "20260712-test", ReleaseCommit: "0123456789abcdef", JWTSecret: validJWTSecret,
		RedisURL: "redis://redis:6379/0", ExternalExecutionJWTCurrentPublicKey: "public-key",
		ExternalExecutionJWTCurrentKeyID: "current", ExternalExecutionJWTIssuer: "openlinker-cloud",
		ExternalExecutionJWTAudience: "openlinker-core.external-execution", ExternalExecutionCallerServiceID: "openlinker-cloud",
		ExternalExecutionJWTNextPublicKey: "next-public-key", ExternalExecutionJWTNextKeyID: "current",
	}); err == nil || !strings.Contains(err.Error(), "must differ") {
		t.Fatalf("duplicate next kid error = %v", err)
	}
	if err := validateProductionConfig(&config.Config{
		Env: "production", FrontendURL: "https://app.example",
		ReleaseVersion: "local", ReleaseCommit: "0123456789abcdef", JWTSecret: validJWTSecret,
	}); err == nil || !strings.Contains(err.Error(), "OPENLINKER_RELEASE_ID") {
		t.Fatalf("placeholder release error = %v", err)
	}
}

func TestValidateProductionConfigSecretLengths(t *testing.T) {
	base := func() *config.Config {
		return &config.Config{
			Env:                  "production",
			FrontendURL:          "https://app.example",
			ReleaseVersion:       "20260718-test",
			ReleaseCommit:        "0123456789abcdef",
			JWTSecret:            strings.Repeat("j", 32),
			OAuthCodeStorageMode: "legacy-jwt",
		}
	}

	t.Run("JWT 31 bytes fails", func(t *testing.T) {
		cfg := base()
		cfg.JWTSecret = strings.Repeat("j", 31)
		err := validateProductionConfig(cfg)
		if err == nil || !strings.Contains(err.Error(), "JWT_SECRET") || strings.Contains(err.Error(), cfg.JWTSecret) {
			t.Fatalf("short JWT error = %v", err)
		}
	})

	t.Run("JWT 32 bytes passes", func(t *testing.T) {
		if err := validateProductionConfig(base()); err != nil {
			t.Fatalf("32-byte JWT should pass: %v", err)
		}
	})

	t.Run("JWT uses UTF-8 bytes", func(t *testing.T) {
		cfg := base()
		cfg.JWTSecret = strings.Repeat("密", 10) // 30 UTF-8 bytes.
		if err := validateProductionConfig(cfg); err == nil || !strings.Contains(err.Error(), "JWT_SECRET") {
			t.Fatalf("30-byte UTF-8 JWT error = %v", err)
		}
		cfg.JWTSecret = strings.Repeat("密", 11) // 33 UTF-8 bytes.
		if err := validateProductionConfig(cfg); err != nil {
			t.Fatalf("33-byte UTF-8 JWT should pass: %v", err)
		}
	})

	t.Run("empty internal token remains disabled", func(t *testing.T) {
		if err := validateProductionConfig(base()); err != nil {
			t.Fatalf("empty internal token should remain valid: %v", err)
		}
	})

	t.Run("internal token uses trimmed bytes", func(t *testing.T) {
		cfg := base()
		cfg.InternalToken = "  " + strings.Repeat("i", 31) + "  "
		err := validateProductionConfig(cfg)
		if err == nil || !strings.Contains(err.Error(), "OPENLINKER_INTERNAL_TOKEN") || strings.Contains(err.Error(), cfg.InternalToken) {
			t.Fatalf("short internal token error = %v", err)
		}
		cfg.InternalToken = "  " + strings.Repeat("i", 32) + "  "
		if err := validateProductionConfig(cfg); err != nil {
			t.Fatalf("trimmed 32-byte internal token should pass: %v", err)
		}
	})

	t.Run("internal token uses UTF-8 bytes after trim", func(t *testing.T) {
		cfg := base()
		cfg.InternalToken = "  " + strings.Repeat("密", 10) + "  " // 30 trimmed bytes.
		if err := validateProductionConfig(cfg); err == nil || !strings.Contains(err.Error(), "OPENLINKER_INTERNAL_TOKEN") {
			t.Fatalf("30-byte UTF-8 internal token error = %v", err)
		}
		cfg.InternalToken = "  " + strings.Repeat("密", 11) + "  " // 33 trimmed bytes.
		if err := validateProductionConfig(cfg); err != nil {
			t.Fatalf("33-byte UTF-8 internal token should pass: %v", err)
		}
	})

	t.Run("development preserves short test secrets", func(t *testing.T) {
		cfg := &config.Config{
			Env:                  "development",
			JWTSecret:            "short",
			InternalToken:        "short",
			OAuthCodeStorageMode: "legacy-jwt",
		}
		if err := validateProductionConfig(cfg); err != nil {
			t.Fatalf("development short secrets should pass: %v", err)
		}
	})
}

func TestValidateProductionConfigRejectsUnknownOAuthCodeStorageMode(t *testing.T) {
	for _, invalid := range []string{
		"secret-looking-invalid-storage-value",
		" subject-only ",
		" legacy-jwt ",
	} {
		err := validateProductionConfig(&config.Config{
			Env:                  "development",
			OAuthCodeStorageMode: invalid,
		})
		if err == nil || !strings.Contains(err.Error(), "OAUTH_CODE_STORAGE_MODE") {
			t.Fatalf("unknown storage mode %q error = %v", invalid, err)
		}
		if strings.Contains(err.Error(), invalid) {
			t.Fatalf("unknown storage mode error echoed value: %v", err)
		}
	}
}

func TestRateLimiterConfigSkipsHealthAndDeniesWithStandardError(t *testing.T) {
	cfg := rateLimiterConfig()
	e := echo.New()

	health := e.NewContext(httptest.NewRequest(http.MethodGet, "/healthz", nil), httptest.NewRecorder())
	if !cfg.Skipper(health) {
		t.Fatal("healthz should skip rate limiting")
	}
	ready := e.NewContext(httptest.NewRequest(http.MethodGet, "/readyz", nil), httptest.NewRecorder())
	if !cfg.Skipper(ready) {
		t.Fatal("readyz should skip rate limiting")
	}
	runtimeRequest := e.NewContext(
		httptest.NewRequest(http.MethodGet, "/api/v1/agent-runtime/ws", nil),
		httptest.NewRecorder(),
	)
	if !cfg.Skipper(runtimeRequest) {
		t.Fatal("mTLS Runtime route should skip the shared HTTP IP limiter")
	}
	runtimeNearPrefix := e.NewContext(
		httptest.NewRequest(http.MethodGet, "/api/v1/agent-runtime-not-runtime", nil),
		httptest.NewRecorder(),
	)
	if cfg.Skipper(runtimeNearPrefix) {
		t.Fatal("non-Runtime near-prefix route should remain rate limited")
	}

	api := e.NewContext(httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil), httptest.NewRecorder())
	if cfg.Skipper(api) {
		t.Fatal("api route should not skip rate limiting")
	}
	err := cfg.DenyHandler(api, "127.0.0.1", nil)
	var httpErr *httpx.HTTPError
	if !errors.As(err, &httpErr) || httpErr.Status != http.StatusTooManyRequests || httpErr.Code != httpx.CodeRateLimited {
		t.Fatalf("DenyHandler error = %#v", err)
	}
}

type fakeRateLimiterStore struct{}

func (fakeRateLimiterStore) Allow(string) (bool, error) { return true, nil }

func TestRateLimiterConfigUsesInjectedStore(t *testing.T) {
	store := fakeRateLimiterStore{}
	cfg := rateLimiterConfig(store)
	if cfg.Store != store {
		t.Fatalf("rateLimiterConfig should use injected distributed store")
	}
}

func TestHTTPRateLimitConfigUsesCustomValuesAndFallbacks(t *testing.T) {
	cfg := &config.Config{
		HTTPRateLimitRate:      500,
		HTTPRateLimitBurst:     5000,
		HTTPRateLimitPeriodSec: 2,
	}
	if got := httpRateLimitRate(cfg); got != 500 {
		t.Fatalf("httpRateLimitRate = %d", got)
	}
	if got := httpRateLimitBurst(cfg); got != 5000 {
		t.Fatalf("httpRateLimitBurst = %d", got)
	}
	if got := httpRateLimitPeriod(cfg); got != 2*time.Second {
		t.Fatalf("httpRateLimitPeriod = %s", got)
	}

	empty := &config.Config{}
	if got := httpRateLimitRate(empty); got != 50 {
		t.Fatalf("fallback httpRateLimitRate = %d", got)
	}
	if got := httpRateLimitBurst(empty); got != 200 {
		t.Fatalf("fallback httpRateLimitBurst = %d", got)
	}
	if got := httpRateLimitPeriod(empty); got != time.Second {
		t.Fatalf("fallback httpRateLimitPeriod = %s", got)
	}
}

func TestNewHTTPServerSetsConnectionTimeouts(t *testing.T) {
	srv := newHTTPServer(9090)
	if srv.ReadTimeout != 15*time.Second ||
		srv.ReadHeaderTimeout != 10*time.Second ||
		srv.WriteTimeout != 120*time.Second ||
		srv.IdleTimeout != 120*time.Second {
		t.Fatalf("server timeouts = read %s header %s write %s idle %s",
			srv.ReadTimeout, srv.ReadHeaderTimeout, srv.WriteTimeout, srv.IdleTimeout)
	}
}

func TestRequestLoggerHandlesHandlerErrorBeforeLogging(t *testing.T) {
	e := echo.New()
	e.HTTPErrorHandler = func(err error, c echo.Context) {
		_ = httpx.SendError(c, err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/test", nil)
	req.Header.Set(echo.HeaderXRequestID, "rid-123")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	sentinel := errors.New("handler failed")

	err := requestLogger()(func(c echo.Context) error {
		return sentinel
	})(c)
	if err != nil {
		t.Fatalf("requestLogger returned %v, want nil", err)
	}
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusInternalServerError, rec.Body.String())
	}
}

func TestRequestLoggerSuccess(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := requestLogger()(func(c echo.Context) error {
		return c.NoContent(http.StatusNoContent)
	})(c)
	if err != nil {
		t.Fatalf("requestLogger success returned %v", err)
	}
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
}

func TestNewEchoAndHealthRoutes(t *testing.T) {
	cfg := &config.Config{Env: "development", FrontendURL: "https://app.example"}
	e := newEcho(cfg)
	if !e.HideBanner || !e.HidePort {
		t.Fatalf("newEcho should hide banner and port")
	}

	pinger := &fakePinger{}
	readiness := &fakeClusterReadiness{result: runtime.RuntimeClusterReadiness{
		Ready: true, Status: "ready", InstanceID: uuid.New(),
	}}
	registerHealthRoutes(e, cfg, pinger, readiness)

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /healthz status = %d, want %d", rec.Code, http.StatusOK)
	}
	var health map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&health); err != nil {
		t.Fatalf("decode healthz: %v", err)
	}
	if health["status"] != "ok" || health["env"] != "development" {
		t.Fatalf("health response = %#v", health)
	}

	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(http.MethodHead, "/healthz", nil))
	if rec.Code != http.StatusOK || rec.Body.Len() != 0 {
		t.Fatalf("HEAD /healthz status/body = %d/%q", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz/db", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /healthz/db status = %d, want %d", rec.Code, http.StatusOK)
	}
	if pinger.calls != 1 || !pinger.sawDeadline {
		t.Fatalf("db pinger calls/deadline = %d/%v", pinger.calls, pinger.sawDeadline)
	}

	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(http.MethodHead, "/healthz/db", nil))
	if rec.Code != http.StatusOK || rec.Body.Len() != 0 {
		t.Fatalf("HEAD /healthz/db status/body = %d/%q", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK || readiness.calls != 1 || !readiness.sawDeadline {
		t.Fatalf("GET /readyz status/calls/deadline = %d/%d/%v: %s", rec.Code, readiness.calls, readiness.sawDeadline, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(http.MethodHead, "/readyz", nil))
	if rec.Code != http.StatusOK || rec.Body.Len() != 0 || readiness.calls != 2 {
		t.Fatalf("HEAD /readyz status/body/calls = %d/%q/%d", rec.Code, rec.Body.String(), readiness.calls)
	}
}

func TestNewEchoAllowsRunIdempotencyCORSHeaders(t *testing.T) {
	cfg := &config.Config{Env: "development", FrontendURL: "https://app.example"}
	e := newEcho(cfg)
	e.POST("/api/v1/runs", func(c echo.Context) error {
		c.Response().Header().Set("Idempotency-Replayed", "true")
		c.Response().Header().Set("Preference-Applied", "wait=0")
		c.Response().Header().Set(echo.HeaderLocation, "/api/v1/runs/test")
		return c.NoContent(http.StatusCreated)
	})

	req := httptest.NewRequest(http.MethodOptions, "/api/v1/runs", nil)
	req.Header.Set(echo.HeaderOrigin, "https://app.example")
	req.Header.Set(echo.HeaderAccessControlRequestMethod, http.MethodPost)
	req.Header.Set(echo.HeaderAccessControlRequestHeaders, "authorization,content-type,idempotency-key,prefer")
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("preflight status = %d: %s", rec.Code, rec.Body.String())
	}
	allow := strings.ToLower(rec.Header().Get(echo.HeaderAccessControlAllowHeaders))
	for _, header := range []string{"authorization", "content-type", "idempotency-key", "prefer"} {
		if !strings.Contains(allow, header) {
			t.Fatalf("Access-Control-Allow-Headers = %q, missing %q", allow, header)
		}
	}
	post := httptest.NewRequest(http.MethodPost, "/api/v1/runs", nil)
	post.Header.Set(echo.HeaderOrigin, "https://app.example")
	post.Header.Set("Idempotency-Key", "cors-test")
	postRec := httptest.NewRecorder()
	e.ServeHTTP(postRec, post)
	if postRec.Code != http.StatusCreated {
		t.Fatalf("POST status = %d: %s", postRec.Code, postRec.Body.String())
	}
	expose := strings.ToLower(postRec.Header().Get(echo.HeaderAccessControlExposeHeaders))
	for _, header := range []string{"location", "idempotency-replayed", "preference-applied"} {
		if !strings.Contains(expose, header) {
			t.Fatalf("Access-Control-Expose-Headers = %q, missing %q", expose, header)
		}
	}
}

func TestHealthDBFailureUsesStandardError(t *testing.T) {
	e := newEcho(&config.Config{Env: "production"})
	registerHealthRoutes(e, &config.Config{Env: "production"}, &fakePinger{err: errors.New("db down")}, nil)

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz/db", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET /healthz/db status = %d, want %d: %s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}
	if got := rec.Body.String(); !containsAll(got, "SERVICE_UNAVAILABLE", "database unavailable") {
		t.Fatalf("GET /healthz/db body = %s", got)
	}
}

func TestReadinessFailureReturnsServiceUnavailableForGetAndHead(t *testing.T) {
	e := newEcho(&config.Config{Env: "development"})
	checker := &fakeClusterReadiness{result: runtime.RuntimeClusterReadiness{
		Status: "not_ready", Reasons: []string{"replicas_unavailable"},
	}}
	registerHealthRoutes(e, &config.Config{Env: "development"}, &fakePinger{}, checker)

	for _, method := range []string{http.MethodGet, http.MethodHead} {
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, httptest.NewRequest(method, "/readyz", nil))
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s /readyz = %d: %s", method, rec.Code, rec.Body.String())
		}
	}
}

func TestExternalExecutionReadinessRequiresReplayDependency(t *testing.T) {
	base := &fakeClusterReadiness{result: runtime.RuntimeClusterReadiness{Ready: true, Status: "ready", InstanceID: uuid.New()}}
	replay := &fakeReadinessDependency{err: errors.New("redis endpoint and credential must stay private")}
	checker := externalExecutionReadiness{base: base, replay: replay}
	e := newEcho(&config.Config{Env: "development"})
	registerHealthRoutes(e, &config.Config{Env: "development"}, &fakePinger{}, checker)

	get := httptest.NewRecorder()
	e.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if get.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET /readyz = %d: %s", get.Code, get.Body.String())
	}
	var result runtime.RuntimeClusterReadiness
	if err := json.NewDecoder(get.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.Ready || result.Status != "not_ready" || len(result.Reasons) != 1 || result.Reasons[0] != "external_execution_replay_dependency_unavailable" {
		t.Fatalf("external execution readiness = %#v", result)
	}
	if strings.Contains(get.Body.String(), "redis endpoint") || strings.Contains(get.Body.String(), "credential") {
		t.Fatalf("readiness leaked dependency error: %s", get.Body.String())
	}

	head := httptest.NewRecorder()
	e.ServeHTTP(head, httptest.NewRequest(http.MethodHead, "/readyz", nil))
	if head.Code != http.StatusServiceUnavailable || head.Body.Len() != 0 || replay.calls != 2 {
		t.Fatalf("HEAD /readyz status/body/replay calls = %d/%q/%d", head.Code, head.Body.String(), replay.calls)
	}

	replay.err = nil
	ok := checker.Readiness(context.Background())
	if !ok.Ready || ok.Status != "ready" {
		t.Fatalf("recovered readiness = %#v", ok)
	}
}

func TestApplicationReadinessSkipsRedisWhenExternalExecutionDisabled(t *testing.T) {
	base := &fakeClusterReadiness{result: runtime.RuntimeClusterReadiness{Ready: true, Status: "ready"}}
	checker := applicationReadiness(&config.Config{}, base, nil)
	result := checker.Readiness(context.Background())
	if !result.Ready || base.calls != 1 {
		t.Fatalf("disabled external execution readiness = %#v, base calls=%d", result, base.calls)
	}
}

func TestNewRedisClientDoesNotRequireReachableServer(t *testing.T) {
	client, err := newRedisClient("redis://127.0.0.1:1/0")
	if err != nil {
		t.Fatalf("newRedisClient: %v", err)
	}
	defer func() { _ = client.Close() }()
	if got := client.Options().Addr; got != "127.0.0.1:1" {
		t.Fatalf("redis addr = %q", got)
	}
	if _, err = newRedisClient("not a redis url"); err == nil {
		t.Fatal("invalid redis URL should fail")
	}
}

func TestNewHTTPServer(t *testing.T) {
	srv := newHTTPServer(18080)
	if srv.Addr != ":18080" {
		t.Fatalf("server addr = %q", srv.Addr)
	}
	if srv.ReadHeaderTimeout != 10*time.Second {
		t.Fatalf("read header timeout = %s", srv.ReadHeaderTimeout)
	}
}

func TestMigrationConfig(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantURL string
		wantSrc string
		wantErr string
	}{
		{name: "missing database", env: map[string]string{}, wantErr: "DATABASE_URL not set"},
		{name: "default source", env: map[string]string{"DATABASE_URL": "postgres://db"}, wantURL: "postgres://db", wantSrc: "./migrations"},
		{name: "custom source", env: map[string]string{"DATABASE_URL": "postgres://db", "MIGRATIONS_DIR": "/app/migrations"}, wantURL: "postgres://db", wantSrc: "/app/migrations"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotURL, gotSrc, err := migrationConfig(func(key string) string { return tt.env[key] })
			if tt.wantErr != "" {
				if err == nil || err.Error() != tt.wantErr {
					t.Fatalf("migrationConfig err = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("migrationConfig unexpected err = %v", err)
			}
			if gotURL != tt.wantURL || gotSrc != tt.wantSrc {
				t.Fatalf("migrationConfig = %q/%q, want %q/%q", gotURL, gotSrc, tt.wantURL, tt.wantSrc)
			}
		})
	}
}

func TestMigrationSourceLoadsWithoutDuplicateVersions(t *testing.T) {
	driver, err := (&migratefile.File{}).Open("file://../../migrations")
	if err != nil {
		t.Fatalf("open migration source: %v", err)
	}
	t.Cleanup(func() {
		if err := driver.Close(); err != nil {
			t.Errorf("close migration source: %v", err)
		}
	})
	if _, err := driver.First(); err != nil {
		t.Fatalf("read first migration: %v", err)
	}
}

func TestRunMigrateWithCommandBranches(t *testing.T) {
	tests := []struct {
		name         string
		args         []string
		env          map[string]string
		snapshot     migrationinit.Snapshot
		postSnapshot *migrationinit.Snapshot
		inspectErr   error
		migrator     *fakeMigrator
		newErr       error
		wantCode     int
		wantOut      string
		wantErr      string
		wantSrc      string
		wantDBURL    string
		wantInspect  int
		wantFactory  int
	}{
		{name: "missing command", wantCode: 2, wantOut: "usage: api migrate <up|check|status>"},
		{name: "missing database", args: []string{"up"}, wantCode: 1, wantErr: "DATABASE_URL not set"},
		{
			name:        "inspection failure",
			args:        []string{"up"},
			env:         map[string]string{"DATABASE_URL": "postgres://db"},
			inspectErr:  errors.New("inspection failed"),
			wantCode:    1,
			wantErr:     "migration preflight: inspection failed",
			wantInspect: 1,
		},
		{
			name:         "fresh up success",
			args:         []string{"up"},
			env:          map[string]string{"DATABASE_URL": "postgres://db", "MIGRATIONS_DIR": "/app/migrations"},
			migrator:     &fakeMigrator{},
			postSnapshot: migrationSnapshotPointer(currentCoreMigrationSnapshot()),
			wantCode:     0,
			wantOut:      "migrate up: ok",
			wantSrc:      "file:///app/migrations",
			wantDBURL:    "postgres://db",
			wantInspect:  2,
			wantFactory:  1,
		},
		{
			name:         "fresh up no change is ok",
			args:         []string{"up"},
			env:          map[string]string{"DATABASE_URL": "postgres://db"},
			migrator:     &fakeMigrator{upErr: migratecmd.ErrNoChange},
			postSnapshot: migrationSnapshotPointer(currentCoreMigrationSnapshot()),
			wantCode:     0,
			wantOut:      "migrate up: ok",
			wantSrc:      "file://./migrations",
			wantDBURL:    "postgres://db",
			wantInspect:  2,
			wantFactory:  1,
		},
		{
			name:        "fresh up failure",
			args:        []string{"up"},
			env:         map[string]string{"DATABASE_URL": "postgres://db"},
			migrator:    &fakeMigrator{upErr: errors.New("up failed")},
			wantCode:    1,
			wantErr:     "migrate up: up failed",
			wantSrc:     "file://./migrations",
			wantDBURL:   "postgres://db",
			wantInspect: 1,
			wantFactory: 1,
		},
		{
			name:         "postflight drift fails",
			args:         []string{"up"},
			env:          map[string]string{"DATABASE_URL": "postgres://db"},
			migrator:     &fakeMigrator{},
			postSnapshot: migrationSnapshotPointer(legacyCoreMigrationSnapshot(81)),
			wantCode:     1,
			wantErr:      "migrate up postflight: schema is not the exact current Core state",
			wantInspect:  2,
			wantFactory:  1,
		},
		{
			name:        "current is a preflight no-op",
			args:        []string{"up"},
			env:         map[string]string{"DATABASE_URL": "postgres://db"},
			snapshot:    currentCoreMigrationSnapshot(),
			wantCode:    0,
			wantOut:     "migrate up: ok",
			wantInspect: 1,
		},
		{
			name:        "check reports fresh without factory",
			args:        []string{"check"},
			env:         map[string]string{"DATABASE_URL": "postgres://db"},
			wantCode:    0,
			wantOut:     "migrate check: state=fresh",
			wantInspect: 1,
		},
		{
			name:        "check reports current without factory",
			args:        []string{"check"},
			env:         map[string]string{"DATABASE_URL": "postgres://db"},
			snapshot:    currentCoreMigrationSnapshot(),
			wantCode:    0,
			wantOut:     "migrate check: state=current",
			wantInspect: 1,
		},
		{
			name:        "legacy is rejected before factory",
			args:        []string{"up"},
			env:         map[string]string{"DATABASE_URL": "postgres://db"},
			snapshot:    legacyCoreMigrationSnapshot(81),
			wantCode:    1,
			wantErr:     "unsupported; rebuild an empty database",
			wantInspect: 1,
		},
		{
			name:        "factory failure",
			args:        []string{"up"},
			env:         map[string]string{"DATABASE_URL": "postgres://db"},
			newErr:      errors.New("bad migration source"),
			wantCode:    1,
			wantErr:     "migrate init: bad migration source",
			wantSrc:     "file://./migrations",
			wantDBURL:   "postgres://db",
			wantInspect: 1,
			wantFactory: 1,
		},
		{
			name:        "empty status is read only",
			args:        []string{"status"},
			env:         map[string]string{"DATABASE_URL": "postgres://db"},
			wantCode:    0,
			wantOut:     "version=0 dirty=false",
			wantInspect: 1,
		},
		{
			name:        "legacy status remains visible",
			args:        []string{"status"},
			env:         map[string]string{"DATABASE_URL": "postgres://db"},
			snapshot:    migrationinit.Snapshot{Core: migrationinit.MigrationTableState{Exists: true, Rows: 1, Version: 81, Dirty: true}},
			wantCode:    0,
			wantOut:     "version=81 dirty=true",
			wantInspect: 1,
		},
		{
			name:        "malformed status fails",
			args:        []string{"status"},
			env:         map[string]string{"DATABASE_URL": "postgres://db"},
			snapshot:    migrationinit.Snapshot{Core: migrationinit.MigrationTableState{Exists: true, Rows: 2}},
			wantCode:    1,
			wantErr:     "expected exactly one",
			wantInspect: 1,
		},
		{
			name:     "down is disabled before inspection",
			args:     []string{"down"},
			env:      map[string]string{"DATABASE_URL": "postgres://db"},
			wantCode: 1,
			wantErr:  "migrate down is disabled",
		},
		{
			name:     "unknown command does not inspect",
			args:     []string{"sideways"},
			env:      map[string]string{"DATABASE_URL": "postgres://db"},
			wantCode: 2,
			wantErr:  "unknown migrate command: sideways",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			var gotSrc, gotDBURL string
			inspectCalls := 0
			factoryCalls := 0
			fakeM := tt.migrator
			if fakeM == nil {
				fakeM = &fakeMigrator{}
			}
			code := runMigrateWith(tt.args, func(key string) string { return tt.env[key] }, func(ctx context.Context, databaseURL string) (migrationinit.Snapshot, error) {
				inspectCalls++
				if _, ok := ctx.Deadline(); !ok {
					t.Fatal("migration inspection context has no deadline")
				}
				if databaseURL != tt.env["DATABASE_URL"] {
					t.Fatalf("inspection database URL = %q", databaseURL)
				}
				if inspectCalls > 1 && tt.postSnapshot != nil {
					return *tt.postSnapshot, nil
				}
				return tt.snapshot, tt.inspectErr
			}, func(sourceURL, databaseURL string) (migrator, error) {
				factoryCalls++
				gotSrc = sourceURL
				gotDBURL = databaseURL
				if tt.newErr != nil {
					return nil, tt.newErr
				}
				return fakeM, nil
			}, &stdout, &stderr)

			if code != tt.wantCode {
				t.Fatalf("runMigrateWith code = %d, want %d", code, tt.wantCode)
			}
			if tt.wantOut != "" && !strings.Contains(stdout.String(), tt.wantOut) {
				t.Fatalf("stdout = %q, want contains %q", stdout.String(), tt.wantOut)
			}
			if tt.wantErr != "" && !strings.Contains(stderr.String(), tt.wantErr) {
				t.Fatalf("stderr = %q, want contains %q", stderr.String(), tt.wantErr)
			}
			if tt.wantSrc != "" && gotSrc != tt.wantSrc {
				t.Fatalf("sourceURL = %q, want %q", gotSrc, tt.wantSrc)
			}
			if tt.wantDBURL != "" && gotDBURL != tt.wantDBURL {
				t.Fatalf("databaseURL = %q, want %q", gotDBURL, tt.wantDBURL)
			}
			if inspectCalls != tt.wantInspect {
				t.Fatalf("inspection calls = %d, want %d", inspectCalls, tt.wantInspect)
			}
			if factoryCalls != tt.wantFactory {
				t.Fatalf("factory calls = %d, want %d", factoryCalls, tt.wantFactory)
			}
		})
	}
}

func currentCoreMigrationSnapshot() migrationinit.Snapshot {
	return migrationinit.Snapshot{
		Core:                  migrationinit.MigrationTableState{Exists: true, Rows: 1, Version: migrationinit.CoreVersion},
		NonBookkeepingObjects: 69,
		CoreShape: migrationinit.SchemaShape{
			Digest: migrationinit.CoreSchemaDigest,
			Tables: 69, Constraints: 587, Indexes: 259, Triggers: 70,
			CoreIdentities: 1, RuntimeControls: 1, RuntimeSchemas: 9,
			CurrentRuntime: 1, RuntimeWires: 5, CurrentWire: 1, PreviousWire: 1,
			BuiltInSkills: 30, BuiltInSkillCases: 15,
		},
	}
}

func legacyCoreMigrationSnapshot(version int64) migrationinit.Snapshot {
	snapshot := currentCoreMigrationSnapshot()
	snapshot.Core.Version = version
	return snapshot
}

func migrationSnapshotPointer(snapshot migrationinit.Snapshot) *migrationinit.Snapshot {
	return &snapshot
}

type fakePinger struct {
	err         error
	calls       int
	sawDeadline bool
}

type fakeClusterReadiness struct {
	result      runtime.RuntimeClusterReadiness
	calls       int
	sawDeadline bool
}

type fakeReadinessDependency struct {
	err   error
	calls int
}

func (f *fakeReadinessDependency) Ping(context.Context) error {
	f.calls++
	return f.err
}

func (f *fakeClusterReadiness) Readiness(ctx context.Context) runtime.RuntimeClusterReadiness {
	f.calls++
	_, f.sawDeadline = ctx.Deadline()
	return f.result
}

type fakeMigrator struct {
	upErr      error
	stepsErr   error
	version    uint
	dirty      bool
	versionErr error
	closed     bool
}

func (m *fakeMigrator) Up() error {
	return m.upErr
}

func (m *fakeMigrator) Steps(n int) error {
	if n != -1 {
		return errors.New("unexpected step count")
	}
	return m.stepsErr
}

func (m *fakeMigrator) Version() (uint, bool, error) {
	return m.version, m.dirty, m.versionErr
}

func (m *fakeMigrator) Close() (error, error) {
	m.closed = true
	return nil, nil
}

func (p *fakePinger) Ping(ctx context.Context) error {
	p.calls++
	_, p.sawDeadline = ctx.Deadline()
	return p.err
}

func containsAll(s string, parts ...string) bool {
	for _, part := range parts {
		if !strings.Contains(s, part) {
			return false
		}
	}
	return true
}
