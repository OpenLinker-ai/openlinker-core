package externalexecution

import (
	"os"
	"strings"
	"testing"
)

func TestExternalExecutionCancellationMigrationInvariants(t *testing.T) {
	up, err := os.ReadFile("../../migrations/077_external_execution_cancellation.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	down, err := os.ReadFile("../../migrations/077_external_execution_cancellation.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		"CREATE TABLE external_execution_keys",
		"UNIQUE (caller_service_id, external_request_id, actor_user_id)",
		"external_executions_key_actor_fkey",
		"CREATE TABLE external_execution_cancellations",
		"state IN ('requested', 'stopping', 'stopped', 'unconfirmed', 'not_applied')",
		"start_state = 'launching'",
		"start_state = 'canceled'",
		"CREATE TABLE workflow_run_cancellations",
		"CREATE TABLE workflow_step_launches",
		"state IN ('claimed', 'created', 'attached', 'invalidated')",
		"mode = 'hard_maintenance'",
	} {
		if !strings.Contains(string(up), fragment) {
			t.Fatalf("up migration missing %q", fragment)
		}
	}
	for _, fragment := range []string{
		"rollback refuses external cancellation evidence",
		"rollback refuses workflow cancellation evidence",
		"rollback refuses workflow child launch evidence",
		"rollback refuses external launch or cancellation state",
		"rollback refuses key-only external execution tombstones",
	} {
		if !strings.Contains(string(down), fragment) {
			t.Fatalf("down migration missing fail-closed guard %q", fragment)
		}
	}
}
