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

func TestBrowserPolicyMigrationConvergesFromFreshReviewedBridgeAndVersion90(t *testing.T) {
	baseURL := os.Getenv("TEST_DATABASE_URL")
	if baseURL == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	for _, mode := range []string{"fresh", "version-86", "version-90"} {
		t.Run(mode, func(t *testing.T) {
			databaseURL := createMigrationTestDatabase(t, baseURL)
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			if mode == "version-86" || mode == "version-90" {
				conn, err := pgx.Connect(ctx, databaseURL)
				if err != nil {
					t.Fatal(err)
				}
				applyMigrationFile(t, ctx, conn, "086_current_schema_init.up.sql")
				version := int64(86)
				if mode == "version-90" {
					applyMigrationFile(t, ctx, conn, "087_browser_agent_execution_profile.up.sql")
					applyMigrationFile(t, ctx, conn, "088_browser_human_control.up.sql")
					applyMigrationFile(t, ctx, conn, "089_user_jwt_token_version.up.sql")
					applyMigrationFile(t, ctx, conn, "090_task_callback_owner_index.up.sql")
					version = 90
				}
				if _, err := conn.Exec(ctx, `CREATE TABLE public.schema_migrations (version bigint NOT NULL, dirty boolean NOT NULL)`); err != nil {
					conn.Close(context.Background())
					t.Fatal(err)
				}
				if _, err := conn.Exec(ctx, `INSERT INTO public.schema_migrations VALUES ($1, false)`, version); err != nil {
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
					t.Fatalf("version %d preflight noop=%t err=%v", version, noop, err)
				}
			}

			migrateTestDatabaseToCurrent(t, databaseURL)

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
				t.Fatalf("version 91 postflight noop=%t err=%v", noop, err)
			}
		})
	}
}

func TestCurrentSchemaVerifierRejectsInvalidCallbackOwnerIndex(t *testing.T) {
	baseURL := os.Getenv("TEST_DATABASE_URL")
	if baseURL == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	databaseURL := createMigrationTestDatabase(t, baseURL)
	migrateTestDatabaseToCurrent(t, databaseURL)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())

	if _, err := conn.Exec(ctx, `SET allow_system_table_mods = on`); err != nil {
		t.Fatal(err)
	}
	result, err := conn.Exec(ctx, `
UPDATE pg_catalog.pg_index
SET indisvalid = false
WHERE indexrelid = 'public.idx_task_callback_subscriptions_owner'::regclass
`)
	if err != nil {
		t.Fatal(err)
	}
	if result.RowsAffected() != 1 {
		t.Fatalf("invalidated index rows = %d, want 1", result.RowsAffected())
	}
	if _, err := conn.Exec(ctx, `SET allow_system_table_mods = off`); err != nil {
		t.Fatal(err)
	}

	var valid bool
	if err := conn.QueryRow(ctx, `
SELECT i.indisvalid
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
  AND c.relname = 'idx_task_callback_subscriptions_owner'
`).Scan(&valid); err != nil {
		t.Fatal(err)
	}
	if valid {
		t.Fatal("catalog fixture unexpectedly retained a valid index")
	}

	snapshot, err := Inspect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.CoreShape.Digest != CoreSchemaDigest {
		t.Fatalf("invalidity fixture changed index definition digest: got %s want %s", snapshot.CoreShape.Digest, CoreSchemaDigest)
	}
	current, validationErr := snapshot.ValidateCoreUp()
	if validationErr == nil || current {
		t.Errorf("migration preflight accepted invalid callback owner index: current=%t err=%v", current, validationErr)
	} else if !strings.Contains(validationErr.Error(), "idx_task_callback_subscriptions_owner is missing or invalid") {
		t.Errorf("migration preflight returned the wrong error: %v", validationErr)
	}

	verifySQL, err := os.ReadFile(filepath.Join("..", "..", "migrations", "086_current_schema_init_verify.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, string(verifySQL)); err == nil {
		t.Error("current-schema verifier accepted an invalid callback owner index")
	} else if !strings.Contains(err.Error(), "idx_task_callback_subscriptions_owner is missing or invalid") {
		t.Errorf("current-schema verifier returned the wrong error: %v", err)
	}
	if t.Failed() {
		return
	}

	if _, err := conn.Exec(ctx, `DROP INDEX CONCURRENTLY public.idx_task_callback_subscriptions_owner`); err != nil {
		t.Fatal(err)
	}
	applyMigrationFile(t, ctx, conn, "090_task_callback_owner_index.up.sql")
	applyMigrationFile(t, ctx, conn, "086_current_schema_init_verify.sql")
	recoveredSnapshot, err := Inspect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	current, err = recoveredSnapshot.ValidateCoreUp()
	if err != nil || !current {
		t.Fatalf("recovered migration state current=%t err=%v", current, err)
	}
}

func migrateTestDatabaseToCurrent(t *testing.T, databaseURL string) {
	t.Helper()
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
