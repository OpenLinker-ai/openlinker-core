package db

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestAgentEvidenceQueriesUseExpectedSQLAndArgs(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	tool := "search"
	header := "Bearer x"
	dbtx := &fakeDBTX{
		row:     fakeRow{values: []any{agentID}},
		execTag: pgconn.NewCommandTag("UPDATE 1"),
	}
	q := New(dbtx)

	locked, err := q.LockAgentEvidenceTarget(ctx, LockAgentEvidenceTargetParams{
		ID: agentID, ConnectionMode: "mcp_server", EndpointURL: "https://a.example/rpc",
		MCPToolName: &tool, CompareAuthHeader: true, EndpointAuthHeader: &header,
	})
	if err != nil || locked != agentID {
		t.Fatalf("LockAgentEvidenceTarget = %v, %v", locked, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "LockAgentEvidenceTarget")
	if !strings.Contains(dbtx.queryRowSQL, "FOR SHARE") {
		t.Fatalf("LockAgentEvidenceTarget must share-lock the Agent row: %s", dbtx.queryRowSQL)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{agentID, "mcp_server", "https://a.example/rpc", &tool, true, &header}) {
		t.Fatalf("LockAgentEvidenceTarget args = %#v", dbtx.queryRowArgs)
	}

	resets := []struct {
		name string
		call func(context.Context, uuid.UUID) (int64, error)
		must []string
	}{
		{"ResetAgentAvailabilityEvidence", q.ResetAgentAvailabilityEvidence, []string{"'unknown'", "last_checked_at = NULL", "last_successful_run_at = NULL"}},
		{"ResetAgentDryRunEvidence", q.ResetAgentDryRunEvidence, []string{"dry_run_passed = FALSE", "'pending'"}},
		{"ReopenAgentCertificationAfterTargetChange", q.ReopenAgentCertificationAfterTargetChange, []string{"certification_status = 'certified'", "certified_at = NULL"}},
		{"ResetAgentSkillScoresAfterTargetChange", q.ResetAgentSkillScoresAfterTargetChange, []string{"status = 'pending'", "verified_at = NULL"}},
	}
	for _, reset := range resets {
		affected, err := reset.call(ctx, agentID)
		if err != nil || affected != 1 {
			t.Fatalf("%s = %d, %v", reset.name, affected, err)
		}
		requireSQLName(t, dbtx.execSQL, reset.name)
		for _, fragment := range reset.must {
			if !strings.Contains(dbtx.execSQL, fragment) {
				t.Fatalf("%s SQL missing %q: %s", reset.name, fragment, dbtx.execSQL)
			}
		}
		if !reflect.DeepEqual(dbtx.execArgs, []any{agentID}) {
			t.Fatalf("%s args = %#v", reset.name, dbtx.execArgs)
		}
	}

	dbtx.execErr = errors.New("boom")
	if _, err := q.ResetAgentAvailabilityEvidence(ctx, agentID); err == nil {
		t.Fatal("ResetAgentAvailabilityEvidence must surface Exec errors")
	}
}
