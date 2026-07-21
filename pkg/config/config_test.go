package config

import (
	"os"
	"strings"
	"testing"
)

func unsetEnv(t *testing.T, key string) {
	t.Helper()
	original, ok := os.LookupEnv(key)
	if err := os.Unsetenv(key); err != nil {
		t.Fatalf("unset %s: %v", key, err)
	}
	t.Cleanup(func() {
		if ok {
			_ = os.Setenv(key, original)
		} else {
			_ = os.Unsetenv(key)
		}
	})
}

func TestLoadAppliesRequiredEnvAndDefaults(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://dev:dev@localhost/openlinker_test")
	t.Setenv("JWT_SECRET", "test-secret")
	t.Setenv("OAUTH_SESSION_SECRET", "oauth-secret")
	t.Setenv("OPENLINKER_INTERNAL_TOKEN", "internal-secret")
	t.Setenv("EXTERNAL_EXECUTION_JWT_CURRENT_PUBLIC_KEY", "base64-public-key")
	t.Setenv("EXTERNAL_EXECUTION_JWT_CURRENT_KEY_ID", "cloud-key-2026-07")
	t.Setenv("EXTERNAL_EXECUTION_JWT_NEXT_PUBLIC_KEY", "base64-next-public-key")
	t.Setenv("EXTERNAL_EXECUTION_JWT_NEXT_KEY_ID", "cloud-key-2026-08")
	t.Setenv("EXTERNAL_EXECUTION_REQUEST_BINDING_REQUIRED", "true")
	t.Setenv("ENV", "production")
	t.Setenv("PORT", "9090")
	t.Setenv("ALLOW_LOCAL_HTTP_ENDPOINTS", "true")
	t.Setenv("OAUTH_CALLBACK_BASE_URL", "https://openlinker.test")
	t.Setenv("OAUTH_ALLOWED_FRONTEND_ORIGINS", "https://*.openlinker.test")
	t.Setenv("LLM_COMPLETE_URL", "https://cloud.internal/llm")
	t.Setenv("OPENLINKER_RELEASE_ID", "20260712-test")
	t.Setenv("OPENLINKER_GIT_SHA", "0123456789abcdef")
	t.Setenv("RUNTIME_HA_MODE", "true")
	unsetEnv(t, "OAUTH_CODE_STORAGE_MODE")
	unsetEnv(t, "RUNTIME_INVOCATION_SIGNING_SECRET")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if !cfg.IsProduction() {
		t.Fatalf("expected production env")
	}
	if cfg.Port != 9090 {
		t.Fatalf("Port = %d", cfg.Port)
	}
	if cfg.ReleaseVersion != "20260712-test" || cfg.ReleaseCommit != "0123456789abcdef" || !cfg.RuntimeHAMode {
		t.Fatalf("unexpected release/HA config: %#v", cfg)
	}
	if cfg.RedisURL != "redis://localhost:6379/0" {
		t.Fatalf("unexpected default RedisURL: %q", cfg.RedisURL)
	}
	if cfg.OAuthCallbackBaseURL != "https://openlinker.test" {
		t.Fatalf("OAuthCallbackBaseURL = %q", cfg.OAuthCallbackBaseURL)
	}
	if cfg.OAuthAllowedFrontendOrigins != "https://*.openlinker.test" {
		t.Fatalf("OAuthAllowedFrontendOrigins = %q", cfg.OAuthAllowedFrontendOrigins)
	}
	if cfg.OAuthSessionSecret != "oauth-secret" {
		t.Fatalf("OAuthSessionSecret = %q", cfg.OAuthSessionSecret)
	}
	if cfg.OAuthCodeStorageMode != "subject-only" {
		t.Fatalf("OAuthCodeStorageMode = %q, want subject-only", cfg.OAuthCodeStorageMode)
	}
	if cfg.InternalToken != "internal-secret" {
		t.Fatalf("InternalToken = %q", cfg.InternalToken)
	}
	if !cfg.ExternalExecutionEnabled() || cfg.ExternalExecutionJWTCurrentPublicKey != "base64-public-key" ||
		cfg.ExternalExecutionJWTCurrentKeyID != "cloud-key-2026-07" || cfg.ExternalExecutionJWTIssuer != "openlinker-cloud" ||
		cfg.ExternalExecutionJWTAudience != "openlinker-core.external-execution" ||
		cfg.ExternalExecutionCallerServiceID != "openlinker-cloud" ||
		!cfg.ExternalExecutionRequestBindingRequired ||
		cfg.ExternalExecutionJWTNextPublicKey != "base64-next-public-key" || cfg.ExternalExecutionJWTNextKeyID != "cloud-key-2026-08" {
		t.Fatalf("external execution config = %#v", cfg)
	}
	if cfg.LLMCompleteURL != "https://cloud.internal/llm" {
		t.Fatalf("LLMCompleteURL = %q", cfg.LLMCompleteURL)
	}
	if cfg.DBMaxConns != 20 || cfg.DBMinConns != 2 || cfg.DBMaxConnLifetimeMinutes != 30 ||
		cfg.DBMaxConnIdleTimeMinutes != 5 || cfg.DBHealthCheckPeriodSeconds != 60 {
		t.Fatalf("unexpected db pool defaults: %#v", cfg)
	}
	if !cfg.AllowLocalHTTPEndpoints {
		t.Fatalf("expected AllowLocalHTTPEndpoints from env")
	}
	if !cfg.RuntimeMTLSEnabled || cfg.RuntimeMTLSPort != 8443 || cfg.RuntimeMTLSMaxConnections != 4096 ||
		cfg.RuntimeMTLSAPIURL != "https://localhost:8443" || cfg.RuntimePKIMode != "auto" ||
		cfg.RuntimeInvocationSigningKeyID != "current" || len(cfg.RuntimeInvocationSigningSecret) != 64 {
		t.Fatalf("unexpected runtime mTLS defaults: %#v", cfg)
	}
	if cfg.HTTPRateLimitRate != 50 || cfg.HTTPRateLimitBurst != 200 || cfg.HTTPRateLimitPeriodSec != 1 {
		t.Fatalf("unexpected http rate limit defaults: %#v", cfg)
	}
}

func TestLoadAppliesSafeDevelopmentReleaseDefaults(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://dev:dev@localhost/openlinker_test")
	t.Setenv("JWT_SECRET", "test-secret")
	unsetEnv(t, "OPENLINKER_RELEASE_ID")
	unsetEnv(t, "OPENLINKER_GIT_SHA")
	unsetEnv(t, "RUNTIME_HA_MODE")
	unsetEnv(t, "OAUTH_CODE_STORAGE_MODE")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.ReleaseVersion != "local" || cfg.ReleaseCommit != "unknown" || cfg.RuntimeHAMode {
		t.Fatalf("release defaults = %q/%q HA=%v", cfg.ReleaseVersion, cfg.ReleaseCommit, cfg.RuntimeHAMode)
	}
}

func TestLoadOAuthCodeStorageMode(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://dev:dev@localhost/openlinker_test")
	t.Setenv("JWT_SECRET", "test-secret")

	for _, mode := range []string{"legacy-jwt", "subject-only"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("OAUTH_CODE_STORAGE_MODE", mode)
			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load(%s) returned error: %v", mode, err)
			}
			if cfg.OAuthCodeStorageMode != mode {
				t.Fatalf("OAuthCodeStorageMode = %q, want %q", cfg.OAuthCodeStorageMode, mode)
			}
		})
	}

	for _, invalid := range []string{
		"subject-only-secret-looking-invalid-value",
		" subject-only ",
		" legacy-jwt ",
	} {
		t.Run("invalid mode fails without echoing value", func(t *testing.T) {
			t.Setenv("OAUTH_CODE_STORAGE_MODE", invalid)
			_, err := Load()
			if err == nil {
				t.Fatalf("Load should reject OAuth code storage mode %q", invalid)
			}
			if !strings.Contains(err.Error(), "OAUTH_CODE_STORAGE_MODE") {
				t.Fatalf("error does not identify OAUTH_CODE_STORAGE_MODE: %v", err)
			}
			if strings.Contains(err.Error(), invalid) {
				t.Fatalf("error echoed the rejected value: %v", err)
			}
		})
	}
}

func TestLoadRequiresDatabaseURLAndJWTSecret(t *testing.T) {
	unsetEnv(t, "DATABASE_URL")
	unsetEnv(t, "JWT_SECRET")

	if _, err := Load(); err == nil {
		t.Fatalf("Load should fail when required env is missing")
	}
}
