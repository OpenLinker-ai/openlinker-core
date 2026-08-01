package migrationinit

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestJWTAndCallbackForwardMigrationsConvergeFromFreshAndVersion88(t *testing.T) {
	baseURL := os.Getenv("TEST_DATABASE_URL")
	if baseURL == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	for _, mode := range []string{"fresh", "version-88"} {
		t.Run(mode, func(t *testing.T) {
			databaseURL := createMigrationTestDatabase(t, baseURL)
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			if mode == "version-88" {
				conn, err := pgx.Connect(ctx, databaseURL)
				if err != nil {
					t.Fatal(err)
				}
				applyMigrationFile(t, ctx, conn, "086_current_schema_init.up.sql")
				applyMigrationFile(t, ctx, conn, "087_browser_agent_execution_profile.up.sql")
				applyMigrationFile(t, ctx, conn, "088_browser_human_control.up.sql")
				if _, err := conn.Exec(ctx, `
CREATE TABLE public.schema_migrations (version bigint NOT NULL, dirty boolean NOT NULL);
INSERT INTO public.schema_migrations VALUES (88, false);
`); err != nil {
					conn.Close(context.Background())
					t.Fatal(err)
				}
				conn.Close(context.Background())
				snapshot, err := Inspect(ctx, databaseURL)
				if err != nil {
					t.Fatal(err)
				}
				noop, err := snapshot.ValidateCoreUp()
				if err != nil || noop {
					t.Fatalf("version 88 preflight noop=%t err=%v", noop, err)
				}
			}

			migrationDir, err := filepath.Abs(filepath.Join("..", "..", "migrations"))
			if err != nil {
				t.Fatal(err)
			}
			migrator, err := migrate.New("file://"+migrationDir, databaseURL)
			if err != nil {
				t.Fatal(err)
			}
			if err := migrator.Up(); err != nil {
				_, _ = migrator.Close()
				t.Fatal(err)
			}
			if sourceErr, databaseErr := migrator.Close(); sourceErr != nil || databaseErr != nil {
				t.Fatalf("close migrator source=%v database=%v", sourceErr, databaseErr)
			}

			conn, err := pgx.Connect(ctx, databaseURL)
			if err != nil {
				t.Fatal(err)
			}
			applyMigrationFile(t, ctx, conn, "086_current_schema_init_verify.sql")
			conn.Close(context.Background())

			snapshot, err := Inspect(ctx, databaseURL)
			if err != nil {
				t.Fatal(err)
			}
			noop, err := snapshot.ValidateCoreUp()
			if err != nil || !noop {
				t.Fatalf("version 90 postflight noop=%t err=%v", noop, err)
			}
		})
	}
}

func createMigrationTestDatabase(t *testing.T, baseURL string) string {
	t.Helper()
	parsed, err := url.Parse(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	databaseName := "openlinker_migration_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	adminURL := *parsed
	adminURL.Path = "/postgres"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, adminURL.String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{databaseName}.Sanitize()); err != nil {
		admin.Close(context.Background())
		t.Fatal(err)
	}
	admin.Close(context.Background())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		cleanup, err := pgx.Connect(cleanupCtx, adminURL.String())
		if err != nil {
			t.Errorf("connect cleanup database: %v", err)
			return
		}
		defer cleanup.Close(context.Background())
		if _, err := cleanup.Exec(cleanupCtx, "DROP DATABASE "+pgx.Identifier{databaseName}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Errorf("drop migration database: %v", err)
		}
	})
	parsed.Path = "/" + databaseName
	return parsed.String()
}

func applyMigrationFile(t *testing.T, ctx context.Context, conn *pgx.Conn, name string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "migrations", name))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, string(raw)); err != nil {
		t.Fatalf("apply %s: %v", name, err)
	}
}
