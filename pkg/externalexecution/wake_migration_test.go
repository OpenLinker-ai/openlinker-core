package externalexecution

import (
	"os"
	"strings"
	"testing"
)

func TestExternalExecutionWakeMigration084Boundary(t *testing.T) {
	t.Parallel()
	up, err := os.ReadFile("../../migrations/084_external_execution_wake.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	down, err := os.ReadFile("../../migrations/084_external_execution_wake.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	verify, err := os.ReadFile("../../migrations/084_external_execution_wake_verify.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		"'topic', 'external_execution.changed'",
		"pg_notify('openlinker_external_v1', payload)",
		"external_execution_cancellations_wake_insert",
		"run_cancellations_external_execution_wake_update",
		"workflow_run_cancellations_external_execution_wake_update",
	} {
		if !strings.Contains(string(up), fragment) {
			t.Fatalf("migration up missing %q", fragment)
		}
	}
	for _, forbidden := range []string{"NEW.input", "NEW.output", "NEW.token", "NEW.payload"} {
		if strings.Contains(string(up), forbidden) {
			t.Fatalf("wake migration crosses data boundary with %q", forbidden)
		}
	}
	if !strings.Contains(string(down), "DROP FUNCTION IF EXISTS emit_external_execution_wake_notification()") {
		t.Fatal("migration down must remove only the advisory wake objects")
	}
	if !strings.Contains(string(verify), "external execution wake payload boundary is invalid") {
		t.Fatal("migration verify must lock the payload boundary")
	}
}
