package config

import (
	"os"
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
	t.Setenv("ENV", "production")
	t.Setenv("PORT", "9090")
	t.Setenv("ALLOW_LOCAL_HTTP_ENDPOINTS", "true")

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
	if cfg.RedisURL != "redis://localhost:6379/0" {
		t.Fatalf("unexpected default RedisURL: %q", cfg.RedisURL)
	}
	if !cfg.AllowLocalHTTPEndpoints {
		t.Fatalf("expected AllowLocalHTTPEndpoints from env")
	}
	if cfg.RuntimePullRunWorkerTimeoutBatchSize != 50 {
		t.Fatalf("unexpected runtime pull batch default: %d", cfg.RuntimePullRunWorkerTimeoutBatchSize)
	}
}

func TestLoadRequiresDatabaseURLAndJWTSecret(t *testing.T) {
	unsetEnv(t, "DATABASE_URL")
	unsetEnv(t, "JWT_SECRET")

	if _, err := Load(); err == nil {
		t.Fatalf("Load should fail when required env is missing")
	}
}
