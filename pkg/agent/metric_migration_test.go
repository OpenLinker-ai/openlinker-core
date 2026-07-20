package agent

import (
	"os"
	"strings"
	"testing"
)

func TestAgentMetricEventRefreshMigrationIsIndexOnlyAndReversible(t *testing.T) {
	up, err := os.ReadFile("../../migrations/083_agent_metric_event_refresh.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	down, err := os.ReadFile("../../migrations/083_agent_metric_event_refresh.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	verify, err := os.ReadFile("../../migrations/083_agent_metric_event_refresh_verify.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		"CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_run_events_metric_cursor",
		"ON run_events (created_at, id)",
		"INCLUDE (run_id)",
	} {
		if !strings.Contains(string(up), fragment) {
			t.Fatalf("up migration missing %q", fragment)
		}
	}
	if strings.Contains(strings.ToUpper(string(up)), "DROP TABLE") ||
		strings.Contains(strings.ToUpper(string(down)), "DROP TABLE") {
		t.Fatal("metric cursor migration must not delete business tables")
	}
	if !strings.Contains(string(down), "DROP INDEX CONCURRENTLY IF EXISTS idx_run_events_metric_cursor") {
		t.Fatal("down migration does not remove only the metric cursor index")
	}
	if !strings.Contains(string(verify), "RunEvent metric cursor index is missing") {
		t.Fatal("verify migration does not guard the metric cursor index")
	}
}
