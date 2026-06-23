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
	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
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

func TestRateLimiterConfigSkipsHealthAndDeniesWithStandardError(t *testing.T) {
	cfg := rateLimiterConfig()
	e := echo.New()

	health := e.NewContext(httptest.NewRequest(http.MethodGet, "/healthz", nil), httptest.NewRecorder())
	if !cfg.Skipper(health) {
		t.Fatal("healthz should skip rate limiting")
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

func TestRequestLoggerReturnsHandlerError(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/test", nil)
	req.Header.Set(echo.HeaderXRequestID, "rid-123")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	sentinel := errors.New("handler failed")

	err := requestLogger()(func(c echo.Context) error {
		return sentinel
	})(c)
	if !errors.Is(err, sentinel) {
		t.Fatalf("requestLogger returned %v, want sentinel", err)
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
	registerHealthRoutes(e, cfg, pinger)

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
}

func TestHealthDBFailureUsesStandardError(t *testing.T) {
	e := newEcho(&config.Config{Env: "production"})
	registerHealthRoutes(e, &config.Config{Env: "production"}, &fakePinger{err: errors.New("db down")})

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz/db", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET /healthz/db status = %d, want %d: %s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}
	if got := rec.Body.String(); !containsAll(got, "SERVICE_UNAVAILABLE", "database unavailable") {
		t.Fatalf("GET /healthz/db body = %s", got)
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

func TestRunMigrateWithCommandBranches(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		env       map[string]string
		migrator  *fakeMigrator
		newErr    error
		wantCode  int
		wantOut   string
		wantErr   string
		wantSrc   string
		wantDBURL string
	}{
		{name: "missing command", wantCode: 2, wantOut: "usage: api migrate <up|down|status>"},
		{name: "missing database", args: []string{"up"}, wantCode: 1, wantErr: "DATABASE_URL not set"},
		{
			name:      "init failure",
			args:      []string{"up"},
			env:       map[string]string{"DATABASE_URL": "postgres://db"},
			newErr:    errors.New("bad migration source"),
			wantCode:  1,
			wantErr:   "migrate init: bad migration source",
			wantSrc:   "file://./migrations",
			wantDBURL: "postgres://db",
		},
		{
			name:      "up success",
			args:      []string{"up"},
			env:       map[string]string{"DATABASE_URL": "postgres://db", "MIGRATIONS_DIR": "/app/migrations"},
			migrator:  &fakeMigrator{},
			wantCode:  0,
			wantOut:   "migrate up: ok",
			wantSrc:   "file:///app/migrations",
			wantDBURL: "postgres://db",
		},
		{
			name:      "up no change is ok",
			args:      []string{"up"},
			env:       map[string]string{"DATABASE_URL": "postgres://db"},
			migrator:  &fakeMigrator{upErr: migratecmd.ErrNoChange},
			wantCode:  0,
			wantOut:   "migrate up: ok",
			wantSrc:   "file://./migrations",
			wantDBURL: "postgres://db",
		},
		{
			name:      "up failure",
			args:      []string{"up"},
			env:       map[string]string{"DATABASE_URL": "postgres://db"},
			migrator:  &fakeMigrator{upErr: errors.New("up failed")},
			wantCode:  1,
			wantErr:   "migrate up: up failed",
			wantSrc:   "file://./migrations",
			wantDBURL: "postgres://db",
		},
		{
			name:      "down success",
			args:      []string{"down"},
			env:       map[string]string{"DATABASE_URL": "postgres://db"},
			migrator:  &fakeMigrator{},
			wantCode:  0,
			wantOut:   "migrate down 1 step: ok",
			wantSrc:   "file://./migrations",
			wantDBURL: "postgres://db",
		},
		{
			name:      "down failure",
			args:      []string{"down"},
			env:       map[string]string{"DATABASE_URL": "postgres://db"},
			migrator:  &fakeMigrator{stepsErr: errors.New("down failed")},
			wantCode:  1,
			wantErr:   "migrate down: down failed",
			wantSrc:   "file://./migrations",
			wantDBURL: "postgres://db",
		},
		{
			name:      "status success",
			args:      []string{"status"},
			env:       map[string]string{"DATABASE_URL": "postgres://db"},
			migrator:  &fakeMigrator{version: 42, dirty: true},
			wantCode:  0,
			wantOut:   "version=42 dirty=true",
			wantSrc:   "file://./migrations",
			wantDBURL: "postgres://db",
		},
		{
			name:      "status failure",
			args:      []string{"status"},
			env:       map[string]string{"DATABASE_URL": "postgres://db"},
			migrator:  &fakeMigrator{versionErr: errors.New("status failed")},
			wantCode:  1,
			wantErr:   "status: status failed",
			wantSrc:   "file://./migrations",
			wantDBURL: "postgres://db",
		},
		{
			name:      "unknown command",
			args:      []string{"sideways"},
			env:       map[string]string{"DATABASE_URL": "postgres://db"},
			migrator:  &fakeMigrator{},
			wantCode:  2,
			wantErr:   "unknown migrate command: sideways",
			wantSrc:   "file://./migrations",
			wantDBURL: "postgres://db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			var gotSrc, gotDBURL string
			fakeM := tt.migrator
			if fakeM == nil {
				fakeM = &fakeMigrator{}
			}
			code := runMigrateWith(tt.args, func(key string) string { return tt.env[key] }, func(sourceURL, databaseURL string) (migrator, error) {
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
		})
	}
}

type fakePinger struct {
	err         error
	calls       int
	sawDeadline bool
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
