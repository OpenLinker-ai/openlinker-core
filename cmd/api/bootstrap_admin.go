package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/mail"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"
	"golang.org/x/crypto/bcrypt"

	"github.com/OpenLinker-ai/openlinker-core/pkg/db"
)

const (
	localBootstrapAdminEmail         = "admin@openlinker.ai"
	localBootstrapAdminPassword      = "openlinker-admin"
	defaultBootstrapAdminDisplayName = "OpenLinker Admin"
	bootstrapAdminBcryptCost         = 12
)

type bootstrapAdminConfig struct {
	Environment string
	DatabaseURL string
	Email       string
	Password    string
	DisplayName string
}

func runBootstrapAdmin(args []string) {
	cfg, err := parseBootstrapAdminConfig(args, os.Getenv)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bootstrap-admin: %v\n", err)
		os.Exit(1)
	}
	if err := bootstrapAdmin(context.Background(), cfg); err != nil {
		fmt.Fprintf(os.Stderr, "bootstrap-admin: %v\n", err)
		os.Exit(1)
	}
}

func parseBootstrapAdminConfig(args []string, getenv func(string) string) (bootstrapAdminConfig, error) {
	cfg := bootstrapAdminConfig{
		Environment: strings.TrimSpace(getenv("ENV")),
		DatabaseURL: strings.TrimSpace(getenv("DATABASE_URL")),
		Email:       strings.TrimSpace(getenv("OPENLINKER_BOOTSTRAP_ADMIN_EMAIL")),
		Password:    strings.TrimSpace(getenv("OPENLINKER_BOOTSTRAP_ADMIN_PASSWORD")),
		DisplayName: envOrDefault(getenv, "OPENLINKER_BOOTSTRAP_ADMIN_DISPLAY_NAME", defaultBootstrapAdminDisplayName),
	}

	fs := flag.NewFlagSet("bootstrap-admin", flag.ContinueOnError)
	fs.StringVar(&cfg.Environment, "env", cfg.Environment, "environment; defaults to ENV")
	fs.StringVar(&cfg.DatabaseURL, "database-url", cfg.DatabaseURL, "Postgres URL; defaults to DATABASE_URL")
	fs.StringVar(&cfg.Email, "email", cfg.Email, "admin email")
	fs.StringVar(&cfg.Password, "password", cfg.Password, "admin password; prefer OPENLINKER_BOOTSTRAP_ADMIN_PASSWORD")
	fs.StringVar(&cfg.DisplayName, "display-name", cfg.DisplayName, "admin display name")
	if err := fs.Parse(args); err != nil {
		return bootstrapAdminConfig{}, err
	}

	return resolveBootstrapAdminConfig(cfg, true)
}

func resolveBootstrapAdminConfig(cfg bootstrapAdminConfig, requireDatabaseURL bool) (bootstrapAdminConfig, error) {
	cfg.Environment = strings.ToLower(strings.TrimSpace(cfg.Environment))
	if cfg.Environment == "" {
		cfg.Environment = "development"
	}
	if isLocalBootstrapEnvironment(cfg.Environment) {
		if strings.TrimSpace(cfg.Email) == "" {
			cfg.Email = localBootstrapAdminEmail
		}
		if strings.TrimSpace(cfg.Password) == "" {
			cfg.Password = localBootstrapAdminPassword
		}
	} else {
		if strings.TrimSpace(cfg.Email) == "" {
			return bootstrapAdminConfig{}, errors.New("OPENLINKER_BOOTSTRAP_ADMIN_EMAIL is required outside local/development/test")
		}
		if strings.TrimSpace(cfg.Password) == "" {
			return bootstrapAdminConfig{}, errors.New("OPENLINKER_BOOTSTRAP_ADMIN_PASSWORD is required outside local/development/test")
		}
		if strings.TrimSpace(cfg.Password) == localBootstrapAdminPassword {
			return bootstrapAdminConfig{}, errors.New("OPENLINKER_BOOTSTRAP_ADMIN_PASSWORD must not use the local development default")
		}
	}
	return normalizeBootstrapAdminConfig(cfg, requireDatabaseURL)
}

func normalizeBootstrapAdminConfig(cfg bootstrapAdminConfig, requireDatabaseURL bool) (bootstrapAdminConfig, error) {
	cfg.Environment = strings.ToLower(strings.TrimSpace(cfg.Environment))
	cfg.DatabaseURL = strings.TrimSpace(cfg.DatabaseURL)
	cfg.Email = strings.ToLower(strings.TrimSpace(cfg.Email))
	cfg.Password = strings.TrimSpace(cfg.Password)
	cfg.DisplayName = strings.TrimSpace(cfg.DisplayName)
	if cfg.DisplayName == "" {
		cfg.DisplayName = defaultBootstrapAdminDisplayName
	}
	if requireDatabaseURL && cfg.DatabaseURL == "" {
		return bootstrapAdminConfig{}, errors.New("DATABASE_URL or -database-url is required")
	}
	parsedEmail, err := mail.ParseAddress(cfg.Email)
	if err != nil {
		return bootstrapAdminConfig{}, fmt.Errorf("invalid admin email: %w", err)
	}
	if parsedEmail.Address != cfg.Email {
		return bootstrapAdminConfig{}, errors.New("admin email must be a plain email address")
	}
	domain := cfg.Email[strings.LastIndexByte(cfg.Email, '@')+1:]
	if !isLocalBootstrapEnvironment(cfg.Environment) &&
		(domain == "local" || strings.HasSuffix(domain, ".local")) {
		return bootstrapAdminConfig{}, errors.New("OPENLINKER_BOOTSTRAP_ADMIN_EMAIL must not use a .local domain outside local/development/test")
	}
	minimumPasswordLength := 12
	if isLocalBootstrapEnvironment(cfg.Environment) {
		minimumPasswordLength = 8
	}
	if len(cfg.Password) < minimumPasswordLength || len(cfg.Password) > 72 {
		return bootstrapAdminConfig{}, fmt.Errorf("admin password length must be %d-72 bytes", minimumPasswordLength)
	}
	return cfg, nil
}

func isLocalBootstrapEnvironment(environment string) bool {
	switch strings.ToLower(strings.TrimSpace(environment)) {
	case "", "local", "dev", "development", "test":
		return true
	default:
		return false
	}
}

func envOrDefault(getenv func(string) string, key, fallback string) string {
	if v := strings.TrimSpace(getenv(key)); v != "" {
		return v
	}
	return fallback
}

func bootstrapAdmin(parent context.Context, cfg bootstrapAdminConfig) error {
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()

	pool, err := db.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	hash, err := bcrypt.GenerateFromPassword([]byte(cfg.Password), bootstrapAdminBcryptCost)
	if err != nil {
		return fmt.Errorf("hash admin password: %w", err)
	}

	userID, created, err := upsertBootstrapAdmin(ctx, pool, cfg.Email, string(hash), cfg.DisplayName)
	if err != nil {
		return err
	}
	fmt.Printf("bootstrap admin ready email=%s id=%s created=%t password_updated=true\n", cfg.Email, userID, created)
	return nil
}

type bootstrapAdminDB interface {
	Begin(context.Context) (pgx.Tx, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

func autoBootstrapAdminIfNeeded(ctx context.Context, dbtx bootstrapAdminDB, environment string) error {
	cfg, err := resolveBootstrapAdminConfig(bootstrapAdminConfig{
		Environment: environment,
		Email:       strings.TrimSpace(os.Getenv("OPENLINKER_BOOTSTRAP_ADMIN_EMAIL")),
		Password:    strings.TrimSpace(os.Getenv("OPENLINKER_BOOTSTRAP_ADMIN_PASSWORD")),
		DisplayName: envOrDefault(os.Getenv, "OPENLINKER_BOOTSTRAP_ADMIN_DISPLAY_NAME", defaultBootstrapAdminDisplayName),
	}, false)
	if err != nil {
		return err
	}

	hasAdmin, err := hasBootstrapAdmin(ctx, dbtx)
	if err != nil {
		return err
	}
	if hasAdmin {
		log.Info().Msg("admin bootstrap skipped; admin user already exists")
		return nil
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(cfg.Password), bootstrapAdminBcryptCost)
	if err != nil {
		return fmt.Errorf("hash admin password: %w", err)
	}
	userID, created, err := upsertBootstrapAdmin(ctx, dbtx, cfg.Email, string(hash), cfg.DisplayName)
	if err != nil {
		return err
	}
	log.Info().
		Str("email", cfg.Email).
		Str("user_id", userID.String()).
		Bool("created", created).
		Str("environment", cfg.Environment).
		Msg("bootstrap admin ready")
	return nil
}

func hasBootstrapAdmin(ctx context.Context, dbtx bootstrapAdminDB) (bool, error) {
	var exists bool
	if err := dbtx.QueryRow(ctx, `
SELECT EXISTS (
  SELECT 1
  FROM users
  WHERE is_admin = TRUE AND deleted_at IS NULL AND disabled_at IS NULL
)
`).Scan(&exists); err != nil {
		return false, fmt.Errorf("check admin users: %w", err)
	}
	return exists, nil
}

func upsertBootstrapAdmin(ctx context.Context, dbtx bootstrapAdminDB, email, passwordHash, displayName string) (uuid.UUID, bool, error) {
	tx, err := dbtx.Begin(ctx)
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var userID uuid.UUID
	err = tx.QueryRow(ctx, `SELECT id FROM users WHERE email = $1`, email).Scan(&userID)
	switch {
	case err == nil:
		if _, err := tx.Exec(ctx, `
UPDATE users
SET password_hash = $2,
    display_name = $3,
    is_admin = TRUE,
    disabled_at = NULL,
    deleted_at = NULL,
    updated_at = NOW()
WHERE id = $1
`, userID, passwordHash, displayName); err != nil {
			return uuid.Nil, false, fmt.Errorf("promote existing admin: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return uuid.Nil, false, fmt.Errorf("commit: %w", err)
		}
		return userID, false, nil
	case errors.Is(err, pgx.ErrNoRows):
		if err := tx.QueryRow(ctx, `
INSERT INTO users (email, password_hash, display_name, is_admin)
VALUES ($1, $2, $3, TRUE)
RETURNING id
`, email, passwordHash, displayName).Scan(&userID); err != nil {
			return uuid.Nil, false, fmt.Errorf("create admin: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return uuid.Nil, false, fmt.Errorf("commit: %w", err)
		}
		return userID, true, nil
	default:
		return uuid.Nil, false, fmt.Errorf("lookup admin: %w", err)
	}
}
