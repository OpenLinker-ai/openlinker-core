package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/labstack/echo/v4"

	"github.com/kinzhi/openlinker-core/pkg/config"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
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
