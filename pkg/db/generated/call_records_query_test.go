package db

import (
	"os"
	"strings"
	"testing"
)

func TestCallRecordsProjectImmutableExecutionEvidence(t *testing.T) {
	canonical, err := os.ReadFile("../queries/runs.sql")
	if err != nil {
		t.Fatalf("read canonical call-record query: %v", err)
	}
	sources := map[string]string{
		"canonical": string(canonical),
		"generated": listCallRecordsForUser,
	}
	for _, fragment := range []string{
		"r.connection_mode_snapshot",
		"attempt.id = COALESCE(r.active_attempt_id, r.latest_attempt_id)",
		"attachment.id = attempt.runtime_attachment_id",
		"attachment.runtime_session_id = attempt.runtime_session_id",
		"attempt.executor_type = 'runtime'",
		"attempt.accepted_at IS NOT NULL",
		"attachment.transport IN ('websocket', 'long_poll')",
		"attachment.transport_reason",
		"attachment.transport_changed_at",
	} {
		for label, source := range sources {
			if !strings.Contains(source, fragment) {
				t.Fatalf("%s call-record query missing immutable execution evidence fragment %q", label, fragment)
			}
		}
	}
	if strings.Contains(listCallRecordsForUser, "a.connection_mode AS agent_connection_mode") {
		t.Fatal("call-record query must use the Run connection_mode snapshot, not current Agent state")
	}
}
