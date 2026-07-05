package main

import "testing"

func TestParseBootstrapAdminConfigDefaults(t *testing.T) {
	cfg, err := parseBootstrapAdminConfig(nil, func(key string) string {
		if key == "DATABASE_URL" {
			return "postgres://dev:dev@localhost/openlinker?sslmode=disable"
		}
		return ""
	})
	if err != nil {
		t.Fatalf("parseBootstrapAdminConfig returned error: %v", err)
	}
	if cfg.Email != defaultBootstrapAdminEmail {
		t.Fatalf("Email = %q, want %q", cfg.Email, defaultBootstrapAdminEmail)
	}
	if cfg.Password != defaultBootstrapAdminPassword {
		t.Fatalf("Password = %q, want default password", cfg.Password)
	}
	if cfg.DisplayName != defaultBootstrapAdminDisplayName {
		t.Fatalf("DisplayName = %q, want %q", cfg.DisplayName, defaultBootstrapAdminDisplayName)
	}
}

func TestParseBootstrapAdminConfigOverrides(t *testing.T) {
	env := map[string]string{
		"DATABASE_URL":                            "postgres://dev:dev@localhost/openlinker?sslmode=disable",
		"OPENLINKER_BOOTSTRAP_ADMIN_EMAIL":        "Root@OpenLinker.AI",
		"OPENLINKER_BOOTSTRAP_ADMIN_PASSWORD":     "private-password",
		"OPENLINKER_BOOTSTRAP_ADMIN_DISPLAY_NAME": "Root Admin",
	}
	cfg, err := parseBootstrapAdminConfig(nil, func(key string) string {
		return env[key]
	})
	if err != nil {
		t.Fatalf("parseBootstrapAdminConfig returned error: %v", err)
	}
	if cfg.Email != "root@openlinker.ai" {
		t.Fatalf("Email = %q, want normalized email", cfg.Email)
	}
	if cfg.Password != "private-password" {
		t.Fatalf("Password = %q, want env password", cfg.Password)
	}
	if cfg.DisplayName != "Root Admin" {
		t.Fatalf("DisplayName = %q, want env display name", cfg.DisplayName)
	}
}

func TestParseBootstrapAdminConfigRejectsDisplayAddress(t *testing.T) {
	_, err := parseBootstrapAdminConfig([]string{"-email", "Root <root@openlinker.ai>"}, func(key string) string {
		if key == "DATABASE_URL" {
			return "postgres://dev:dev@localhost/openlinker?sslmode=disable"
		}
		return ""
	})
	if err == nil {
		t.Fatal("parseBootstrapAdminConfig returned nil error for display address")
	}
}
