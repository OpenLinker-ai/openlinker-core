package coreapi

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/auth"
	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
)

func TestOAuthStartAccountSelection(t *testing.T) {
	resetGothGlobals(t)
	cfg := &config.Config{
		Env:                  "production",
		JWTSecret:            "oauth-account-selection-test-secret",
		APIURL:               "https://api.openlinker.test",
		OAuthCallbackBaseURL: "https://openlinker.test",
		GoogleClientID:       "google-id",
		GoogleClientSecret:   "google-secret",
		GithubClientID:       "github-id",
		GithubClientSecret:   "github-secret",
	}
	ConfigureGoth(cfg)
	e := echo.New()
	auth.NewHandler(nil, cfg).Register(e.Group("/api/v1"))

	for _, tc := range []struct {
		provider string
		host     string
		prompt   string
	}{
		{provider: "google", host: "accounts.google.com", prompt: "select_account"},
		{provider: "github", host: "github.com", prompt: ""},
	} {
		t.Run(tc.provider, func(t *testing.T) {
			var cookies []*http.Cookie
			var previousState string
			// Repeat with the existing OAuth cookie and caller-supplied hints:
			// every Google authorization request must still carry select_account.
			for _, query := range []string{"", "?prompt=none&login_hint=default%40example.com&authuser=0"} {
				req := httptest.NewRequest(http.MethodGet, "/api/v1/auth/"+tc.provider+query, nil)
				for _, cookie := range cookies {
					req.AddCookie(cookie)
				}
				rec := httptest.NewRecorder()
				e.ServeHTTP(rec, req)
				if rec.Code != http.StatusTemporaryRedirect {
					t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
				}
				location, err := url.Parse(rec.Header().Get(echo.HeaderLocation))
				if err != nil {
					t.Fatal(err)
				}
				if location.Scheme != "https" || location.Host != tc.host {
					t.Fatalf("unexpected OAuth destination: %s", location)
				}
				params := location.Query()
				for key, want := range map[string]string{
					"prompt":        tc.prompt,
					"client_id":     tc.provider + "-id",
					"redirect_uri":  cfg.OAuthCallbackBaseURL + "/api/v1/auth/" + tc.provider + "/callback",
					"response_type": "code",
					"login_hint":    "",
					"authuser":      "",
				} {
					if got := params.Get(key); got != want {
						t.Errorf("%s = %q, want %q", key, got, want)
					}
				}
				state := params.Get("state")
				if state == "" || state == previousState {
					t.Fatal("each OAuth start must generate fresh state")
				}
				previousState = state
				cookies = rec.Result().Cookies()
				if len(cookies) == 0 {
					t.Fatal("OAuth start did not persist its session cookie")
				}
			}
		})
	}
}
