package coreapi

import (
	"net/http"
	"testing"

	"github.com/gorilla/sessions"
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

func resetGothGlobals(t *testing.T) {
	t.Helper()
	previousStore := gothic.Store
	goth.ClearProviders()
	t.Cleanup(func() {
		goth.ClearProviders()
		gothic.Store = previousStore
	})
}
