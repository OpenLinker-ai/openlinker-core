package db

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestRuntimeAgentExecutionProfileQueriesKeepIdentityAndAuditEvidence(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	agentID := uuid.New()
	credentialID := uuid.New()
	sessionID := uuid.New()
	dbtx := &fakeDBTX{
		row: fakeRow{values: []any{
			agentID,
			"browser",
			now,
			credentialID,
			sessionID,
			now,
			(*time.Time)(nil),
			(*uuid.UUID)(nil),
			(*string)(nil),
			(*bool)(nil),
		}},
	}
	queries := New(dbtx)

	classified, err := queries.ClassifyRuntimeAgentBrowserExecutionProfile(
		context.Background(),
		ClassifyRuntimeAgentBrowserExecutionProfileParams{
			AgentID:          agentID,
			CredentialID:     credentialID,
			RuntimeSessionID: sessionID,
		},
	)
	if err != nil {
		t.Fatalf("classify profile: %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "ClassifyRuntimeAgentBrowserExecutionProfile")
	if classified.AgentID != agentID || classified.ExecutionProfile != "browser" {
		t.Fatalf("classified profile = %#v", classified)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{agentID, credentialID, sessionID}) {
		t.Fatalf("classification args = %#v", dbtx.queryRowArgs)
	}

	resetBy := uuid.New()
	reason := "profile store purged and active Sessions drained"
	purgeAttested := true
	resetAt := now.Add(time.Minute)
	dbtx.row = fakeRow{values: []any{
		agentID,
		"standard",
		now,
		credentialID,
		sessionID,
		resetAt,
		&resetAt,
		&resetBy,
		&reason,
		&purgeAttested,
	}}
	reset, err := queries.ResetRuntimeAgentExecutionProfile(
		context.Background(),
		ResetRuntimeAgentExecutionProfileParams{
			AgentID:              agentID,
			ResetByUserID:        resetBy,
			ResetReason:          reason,
			ProfilePurgeAttested: true,
		},
	)
	if err != nil {
		t.Fatalf("reset profile: %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "ResetRuntimeAgentExecutionProfile")
	if reset.ExecutionProfile != "standard" || reset.ResetAt == nil ||
		reset.ProfilePurgeAttested == nil || !*reset.ProfilePurgeAttested {
		t.Fatalf("reset profile = %#v", reset)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{agentID, resetBy, reason, true}) {
		t.Fatalf("reset args = %#v", dbtx.queryRowArgs)
	}
	for _, fragment := range []string{
		"session.status IN ('active', 'draining', 'offline')",
		"FROM runtime_resume_grants grant_row",
		"grant_row.revoked_at IS NULL",
		"grant_row.expires_at > clock_timestamp()",
		"SET browser_execution_profile = FALSE",
	} {
		if !strings.Contains(resetRuntimeAgentExecutionProfile, fragment) {
			t.Fatalf("reset query is missing precondition %q", fragment)
		}
	}
}
