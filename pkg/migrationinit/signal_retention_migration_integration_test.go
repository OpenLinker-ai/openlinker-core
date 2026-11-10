package migrationinit

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// 101 is index-only: rolling it back must restore the exact 100 fingerprint,
// and reapplying it must converge on the current one. The rollback starts
// from exact 101: 102 adds columns, and a column re-added after a rollback
// gets a new attribute number and therefore a different catalog fingerprint.
func TestRuntimeSignalRetentionIndexRollbackAndReapply(t *testing.T) {
	base := os.Getenv("TEST_DATABASE_URL")
	if base == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	databaseURL := createMigrationTestDatabase(t, base)
	migrateTestDatabaseToVersion(t, databaseURL, uint(CoreMCPTransportPredecessorVersion))
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	snapshot, err := Inspect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.InvalidCoreIndexes != 0 {
		t.Fatalf("fresh 101 schema has %d invalid indexes", snapshot.InvalidCoreIndexes)
	}
	if noop, err := snapshot.ValidateCoreUp(); err != nil || noop {
		t.Fatalf("101 predecessor preflight: noop=%t err=%v", noop, err)
	}

	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	applyMigrationFile(t, ctx, conn, "101_runtime_signal_outbox_retention.down.sql")
	if _, err := conn.Exec(ctx, `UPDATE schema_migrations SET version=100,dirty=false`); err != nil {
		t.Fatal(err)
	}
	conn.Close(ctx)
	snapshot, err = Inspect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.CoreShape.Digest != CoreSignalRetentionPredecessorDigest || snapshot.CoreShape.Indexes != 308 {
		t.Fatalf("rollback did not restore the exact 100 schema: %s", formatShape(snapshot.CoreShape))
	}
	if noop, err := snapshot.ValidateCoreUp(); err != nil || noop {
		t.Fatalf("100 predecessor preflight: noop=%t err=%v", noop, err)
	}
	if noop, err := snapshot.ValidateCloudUp(); err == nil || noop {
		t.Fatalf("Cloud must wait for the current Core: noop=%t err=%v", noop, err)
	}

	migrateTestDatabaseToCurrent(t, databaseURL)
	snapshot, err = Inspect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if noop, err := snapshot.ValidateCoreUp(); err != nil || !noop {
		t.Fatalf("reapply preflight: noop=%t err=%v", noop, err)
	}
}

// An interrupted CREATE INDEX CONCURRENTLY leaves an invalid index whose
// definition, and therefore the catalog digest, is unchanged. Both the
// migration inspector and the SQL verifier must still reject it, a forced
// version 100 must not accept the leftover index, and the documented recovery
// must converge.
func TestRuntimeSignalRetentionIndexRejectsInterruptedBuild(t *testing.T) {
	base := os.Getenv("TEST_DATABASE_URL")
	if base == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	databaseURL := createMigrationTestDatabase(t, base)
	migrateTestDatabaseToCurrent(t, databaseURL)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}

	invalidateRetentionIndex(t, ctx, conn)

	snapshot, err := Inspect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.CoreShape.Digest != CoreSchemaDigest || snapshot.InvalidCoreIndexes != 1 {
		t.Fatalf("invalid-index fixture: digest=%s invalid=%d", snapshot.CoreShape.Digest, snapshot.InvalidCoreIndexes)
	}
	if current, err := snapshot.ValidateCoreUp(); err == nil || current || !strings.Contains(err.Error(), "invalid indexes") {
		t.Fatalf("Core preflight accepted an invalid retention index: current=%t err=%v", current, err)
	}
	if current, err := snapshot.ValidateCloudUp(); err == nil || current || !strings.Contains(err.Error(), "invalid indexes") {
		t.Fatalf("Cloud preflight accepted an invalid retention index: current=%t err=%v", current, err)
	}
	verifySQL, err := os.ReadFile(filepath.Join("..", "..", "migrations", "086_current_schema_init_verify.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, string(verifySQL)); err == nil ||
		!strings.Contains(err.Error(), "idx_runtime_signal_outbox_published_retention is missing or invalid") {
		t.Fatalf("current-schema verifier accepted an invalid retention index: %v", err)
	}

	// Forcing the version back without dropping the leftover index must not
	// pass as a clean 100 predecessor; a replay would fail on the name. This
	// recovery runs on a separate exact-101 database: rolling 102 back and
	// reapplying it would not reproduce the measured 102 fingerprint.
	conn.Close(ctx)
	databaseURL = createMigrationTestDatabase(t, base)
	migrateTestDatabaseToVersion(t, databaseURL, uint(CoreMCPTransportPredecessorVersion))
	conn, err = pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	invalidateRetentionIndex(t, ctx, conn)
	if _, err := conn.Exec(ctx, `UPDATE schema_migrations SET version=100,dirty=false`); err != nil {
		t.Fatal(err)
	}
	snapshot, err = Inspect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if noop, err := snapshot.ValidateCoreUp(); err == nil || noop {
		t.Fatalf("forced 100 with a leftover 101 index was accepted: noop=%t err=%v", noop, err)
	}

	if _, err := conn.Exec(ctx, `DROP INDEX CONCURRENTLY public.idx_runtime_signal_outbox_published_retention`); err != nil {
		t.Fatal(err)
	}
	snapshot, err = Inspect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if noop, err := snapshot.ValidateCoreUp(); err != nil || noop {
		t.Fatalf("recovered 100 predecessor: noop=%t err=%v", noop, err)
	}
	migrateTestDatabaseToCurrent(t, databaseURL)
	applyMigrationFile(t, ctx, conn, "086_current_schema_init_verify.sql")
	snapshot, err = Inspect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if current, err := snapshot.ValidateCoreUp(); err != nil || !current {
		t.Fatalf("recovered migration state current=%t err=%v", current, err)
	}
}

// invalidateRetentionIndex reproduces what an interrupted CREATE INDEX
// CONCURRENTLY leaves behind.
func invalidateRetentionIndex(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	t.Helper()
	if _, err := conn.Exec(ctx, `SET allow_system_table_mods = on`); err != nil {
		t.Fatal(err)
	}
	result, err := conn.Exec(ctx, `
UPDATE pg_catalog.pg_index
SET indisvalid = false
WHERE indexrelid = 'public.idx_runtime_signal_outbox_published_retention'::regclass
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
}
