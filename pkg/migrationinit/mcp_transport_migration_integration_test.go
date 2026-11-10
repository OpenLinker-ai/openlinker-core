package migrationinit

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// 102 adds the Agent MCP transport columns. Existing Agents read back the
// default, the CHECKs reject unknown transports and malformed header names,
// and the rollback refuses while any Agent depends on a non-default value,
// because dropping the columns would silently change how it is called.
func TestAgentMCPTransportMigrationDefaultsChecksAndGuardedRollback(t *testing.T) {
	base := os.Getenv("TEST_DATABASE_URL")
	if base == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	databaseURL := createMigrationTestDatabase(t, base)
	migrateTestDatabaseToVersion(t, databaseURL, uint(CoreMCPTransportPredecessorVersion))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	owner, agentID := uuid.New(), uuid.New()
	if _, err := conn.Exec(ctx, `INSERT INTO users(id,email,password_hash,display_name) VALUES($1,$2,'synthetic','Synthetic')`, owner, owner.String()+"@example.invalid"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO agents(id,creator_id,slug,name,description,endpoint_url,endpoint_auth_header,connection_mode,mcp_tool_name,price_per_call_cents)
VALUES($1,$2,'mcp-transport-existing','Existing MCP','Synthetic','https://mcp.example/mcp','Bearer synthetic','mcp_server','lookup',0)`, agentID, owner); err != nil {
		t.Fatal(err)
	}
	conn.Close(ctx)

	migrateTestDatabaseToCurrent(t, databaseURL)
	snapshot, err := Inspect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if noop, err := snapshot.ValidateCoreUp(); err != nil || !noop {
		t.Fatalf("upgraded 102 preflight: noop=%t err=%v shape=%s", noop, err, formatShape(snapshot.CoreShape))
	}

	conn, err = pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { conn.Close(context.Background()) }()
	var transport string
	var headerName *string
	if err := conn.QueryRow(ctx, `SELECT mcp_transport, endpoint_auth_header_name FROM agents WHERE id=$1`, agentID).Scan(&transport, &headerName); err != nil {
		t.Fatal(err)
	}
	if transport != "legacy_json" || headerName != nil {
		t.Fatalf("existing Agent transport = %q header = %v, want legacy_json and NULL", transport, headerName)
	}
	for _, statement := range []string{
		`UPDATE agents SET mcp_transport='websocket' WHERE id=$1`,
		`UPDATE agents SET endpoint_auth_header_name='Bad Header' WHERE id=$1`,
		`UPDATE agents SET endpoint_auth_header_name='' WHERE id=$1`,
		`UPDATE agents SET endpoint_auth_header_name='X-Key:' WHERE id=$1`,
	} {
		if _, err := conn.Exec(ctx, statement, agentID); err == nil || !strings.Contains(err.Error(), "violates check constraint") {
			t.Fatalf("%s: err=%v, want CHECK violation", statement, err)
		}
	}

	rollback := func() error {
		raw, err := os.ReadFile("../../migrations/102_agent_mcp_transport.down.sql")
		if err != nil {
			t.Fatal(err)
		}
		_, err = conn.Exec(ctx, string(raw))
		if err != nil {
			// The failed script leaves the session in an aborted transaction.
			_, _ = conn.Exec(ctx, "ROLLBACK")
		}
		return err
	}
	if _, err := conn.Exec(ctx, `UPDATE agents SET mcp_transport='streamable_http' WHERE id=$1`, agentID); err != nil {
		t.Fatal(err)
	}
	if err := rollback(); err == nil || !strings.Contains(err.Error(), "non-default mcp_transport") {
		t.Fatalf("rollback with streamable_http Agent: %v", err)
	}
	if _, err := conn.Exec(ctx, `UPDATE agents SET mcp_transport='legacy_json', endpoint_auth_header_name='X-Api-Key' WHERE id=$1`, agentID); err != nil {
		t.Fatal(err)
	}
	if err := rollback(); err == nil || !strings.Contains(err.Error(), "custom endpoint_auth_header_name") {
		t.Fatalf("rollback with custom header Agent: %v", err)
	}
	if _, err := conn.Exec(ctx, `UPDATE agents SET endpoint_auth_header_name=NULL WHERE id=$1`, agentID); err != nil {
		t.Fatal(err)
	}
	if err := rollback(); err != nil {
		t.Fatalf("rollback without new data: %v", err)
	}
	if _, err := conn.Exec(ctx, `UPDATE schema_migrations SET version=101,dirty=false`); err != nil {
		t.Fatal(err)
	}
	snapshot, err = Inspect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.CoreShape.Digest != CoreMCPTransportPredecessorDigest || snapshot.CoreShape.Constraints != 728 {
		t.Fatalf("rollback did not restore the exact 101 schema: %s", formatShape(snapshot.CoreShape))
	}
	if noop, err := snapshot.ValidateCoreUp(); err != nil || noop {
		t.Fatalf("101 predecessor preflight: noop=%t err=%v", noop, err)
	}
	if noop, err := snapshot.ValidateCloudUp(); err == nil || noop {
		t.Fatalf("Cloud must wait for Core 102: noop=%t err=%v", noop, err)
	}

	// Reapplying 102 after a rollback re-adds the columns with new attribute
	// numbers, so the catalog is not the measured 102 fingerprint. The schema
	// contract holds, but the preflight fails closed: the rollback exists for
	// disposable databases and the production migrate command is forward-only.
	migrateTestDatabaseToCurrent(t, databaseURL)
	applyMigrationFile(t, ctx, conn, "086_current_schema_init_verify.sql")
	snapshot, err = Inspect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if noop, err := snapshot.ValidateCoreUp(); err == nil || noop || !strings.Contains(err.Error(), "fingerprint mismatch") {
		t.Fatalf("reapply after rollback must fail closed: noop=%t err=%v", noop, err)
	}
}
