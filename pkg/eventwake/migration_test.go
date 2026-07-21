package eventwake

import (
	"os"
	"strings"
	"testing"
)

func TestEventWakeMigrationIsAdvisoryAndReversible(t *testing.T) {
	up := readMigration(t, "../../migrations/082_event_wake_notifications.up.sql")
	down := readMigration(t, "../../migrations/082_event_wake_notifications.down.sql")
	verify := readMigration(t, "../../migrations/082_event_wake_notifications_verify.sql")

	for _, fragment := range []string{
		"CREATE FUNCTION emit_event_wake_notification()",
		"PERFORM pg_notify(channel_name, payload)",
		"octet_length(payload) > 1024",
		"resource_id := NEW.run_id::TEXT",
		"wake_generation := NEW.sequence::BIGINT",
		"'openlinker_run_v1', 'run.changed'",
		"'openlinker_work_v1', 'work.runtime_signal.available'",
		"'openlinker_work_v1', 'work.run_effect.available'",
		"OLD.status IS DISTINCT FROM NEW.status",
		"NEW.available_at < OLD.available_at",
		"OLD.status = 'processing'",
		"NEW.lease_expires_at < OLD.lease_expires_at",
	} {
		if !strings.Contains(up, fragment) {
			t.Fatalf("up migration missing %q", fragment)
		}
	}
	for _, forbidden := range []string{
		"CREATE TABLE",
		"ALTER TABLE",
		"DROP TABLE",
		"DELETE FROM",
		"UPDATE runs SET",
		"to_jsonb(NEW)",
	} {
		if strings.Contains(up, forbidden) {
			t.Fatalf("advisory migration contains forbidden statement %q", forbidden)
		}
	}
	for _, fragment := range []string{
		"DROP TRIGGER IF EXISTS run_events_event_wake_insert ON run_events",
		"DROP TRIGGER IF EXISTS runs_event_wake_state_update ON runs",
		"DROP TRIGGER IF EXISTS runtime_signal_outbox_event_wake_due_update ON runtime_signal_outbox",
		"DROP TRIGGER IF EXISTS run_effect_outbox_event_wake_due_update ON run_effect_outbox",
		"DROP FUNCTION IF EXISTS emit_event_wake_notification()",
	} {
		if !strings.Contains(down, fragment) {
			t.Fatalf("down migration missing %q", fragment)
		}
	}
	for _, forbidden := range []string{"DROP TABLE", "DELETE FROM", "TRUNCATE"} {
		if strings.Contains(down, forbidden) {
			t.Fatalf("down migration contains forbidden statement %q", forbidden)
		}
	}
	for _, fragment := range []string{
		"trigger_count <> 7",
		"to_regprocedure('emit_event_wake_notification()')",
		"function_definition NOT LIKE '%openlinker_run_v1%'",
		"function_definition NOT LIKE '%openlinker_work_v1%'",
		"function_definition LIKE '%to_jsonb(NEW)%'",
	} {
		if !strings.Contains(verify, fragment) {
			t.Fatalf("verify migration missing %q", fragment)
		}
	}
}

func readMigration(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read migration %s: %v", path, err)
	}
	return string(raw)
}
