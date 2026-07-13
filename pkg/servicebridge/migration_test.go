package servicebridge

import (
	"os"
	"strings"
	"testing"
)

func TestHostedServiceExecutionMigrationKeepsIdempotencyInvariants(t *testing.T) {
	up, err := os.ReadFile("../../migrations/068_hosted_service_execution_bridge.up.sql")
	if err != nil {
		t.Fatalf("read up migration: %v", err)
	}
	down, err := os.ReadFile("../../migrations/068_hosted_service_execution_bridge.down.sql")
	if err != nil {
		t.Fatalf("read down migration: %v", err)
	}
	for _, fragment := range []string{
		"external_order_id UUID PRIMARY KEY",
		"input_fingerprint BYTEA NOT NULL CHECK (octet_length(input_fingerprint) = 32)",
		"CHECK ((execution_kind IS NULL) = (execution_id IS NULL))",
		"CREATE UNIQUE INDEX idx_hosted_service_executions_execution",
		"REFERENCES users(id) ON DELETE RESTRICT",
	} {
		if !strings.Contains(string(up), fragment) {
			t.Fatalf("up migration missing %q", fragment)
		}
	}
	if !strings.Contains(string(down), "DROP TABLE IF EXISTS hosted_service_executions") {
		t.Fatalf("down migration does not remove bridge table")
	}
}
