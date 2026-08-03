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
			"restricted",
			[]string{},
			int64(1),
			now,
			uuid.New(),
		}},
	}
	queries := New(dbtx)
	locked, err := queries.LockRuntimeAgentBrowserProfileForRunCreate(
		context.Background(),
		agentID,
	)
	if err != nil || locked.AgentID != agentID || locked.InteractionPolicy != "restricted" {
		t.Fatalf("lock Browser profile = %#v, %v", locked, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "LockRuntimeAgentBrowserProfileForRunCreate")
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{agentID}) ||
		!strings.Contains(lockRuntimeAgentBrowserProfileForRunCreate, "pg_advisory_xact_lock") ||
		!strings.Contains(lockRuntimeAgentBrowserProfileForRunCreate, "FOR SHARE OF profile") {
		t.Fatalf("Run-create Browser profile lock query = %q args=%#v", lockRuntimeAgentBrowserProfileForRunCreate, dbtx.queryRowArgs)
	}

	classified, err := queries.ClassifyRuntimeAgentBrowserExecutionProfile(
		context.Background(),
		ClassifyRuntimeAgentBrowserExecutionProfileParams{
			AgentID:                agentID,
			CredentialID:           credentialID,
			FullBrowserInteraction: false,
			RuntimeSessionID:       sessionID,
		},
	)
	if err != nil {
		t.Fatalf("classify profile: %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "ClassifyRuntimeAgentBrowserExecutionProfile")
	if classified.AgentID != agentID || classified.ExecutionProfile != "browser" {
		t.Fatalf("classified profile = %#v", classified)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{agentID, credentialID, false, sessionID}) {
		t.Fatalf("classification args = %#v", dbtx.queryRowArgs)
	}
	for _, fragment := range []string{
		"runtime_agent_browser_policy_intents",
		"existing.agent_id IS NOT NULL",
		"intent.interaction_policy",
		"DELETE FROM runtime_agent_browser_policy_intents",
	} {
		if !strings.Contains(classifyRuntimeAgentBrowserExecutionProfile, fragment) {
			t.Fatalf("classification query is missing initial-policy contract %q", fragment)
		}
	}

	resetBy := uuid.New()
	reason := "profile store purged and active Sessions drained"
	purgeAttested := true
	resetAt := now.Add(time.Minute)
	changedBy := resetBy
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
		"restricted",
		[]string{},
		int64(2),
		resetAt,
		changedBy,
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

	fullOrigins := []string{"https://example.com"}
	changedAt := resetAt.Add(time.Minute)
	dbtx.row = fakeRow{values: []any{
		agentID,
		"browser",
		now,
		credentialID,
		sessionID,
		changedAt,
		(*time.Time)(nil),
		(*uuid.UUID)(nil),
		(*string)(nil),
		(*bool)(nil),
		"full",
		fullOrigins,
		int64(3),
		changedAt,
		resetBy,
	}}
	updated, err := queries.UpdateRuntimeAgentBrowserInteractionPolicyForOwner(
		context.Background(),
		UpdateRuntimeAgentBrowserInteractionPolicyForOwnerParams{
			AgentID:                agentID,
			InteractionPolicy:      "full",
			BrowserMutationOrigins: fullOrigins,
			ChangedByUserID:        resetBy,
		},
	)
	if err != nil || updated.InteractionPolicy != "full" ||
		updated.InteractionPolicyGeneration != 3 {
		t.Fatalf("update policy = %#v, %v", updated, err)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{
		agentID, "full", fullOrigins, resetBy,
	}) {
		t.Fatalf("update policy args = %#v", dbtx.queryRowArgs)
	}
	for _, fragment := range []string{
		"session.status IN ('active', 'draining', 'offline')",
		"run.status = 'running'",
		"control.state IN ('paused', 'human')",
		"interaction_policy_generation + 1",
		"agent.creator_id = $4",
		"agent.visibility = 'private'",
	} {
		if !strings.Contains(updateRuntimeAgentBrowserInteractionPolicyForOwner, fragment) {
			t.Fatalf("update policy query is missing precondition %q", fragment)
		}
	}
}

func TestInitialBrowserPolicyIntentQueriesAreOwnerBoundAndQuiescent(t *testing.T) {
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	agentID := uuid.New()
	ownerID := uuid.New()
	origins := []string{"https://example.com"}
	dbtx := &fakeDBTX{row: fakeRow{values: []any{
		agentID,
		"full",
		origins,
		now,
		ownerID,
	}}}
	queries := New(dbtx)
	intent, err := queries.StageRuntimeAgentBrowserPolicyIntentForOwner(
		context.Background(),
		StageRuntimeAgentBrowserPolicyIntentForOwnerParams{
			AgentID:                agentID,
			ConfiguredByUserID:     ownerID,
			InteractionPolicy:      "full",
			BrowserMutationOrigins: origins,
		},
	)
	if err != nil || intent.AgentID != agentID || intent.InteractionPolicy != "full" {
		t.Fatalf("stage initial policy = %#v, %v", intent, err)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{agentID, ownerID, "full", origins}) {
		t.Fatalf("stage initial policy args = %#v", dbtx.queryRowArgs)
	}
	for _, fragment := range []string{
		"pg_advisory_xact_lock",
		"agent.creator_id = $2",
		"agent.visibility = 'private'",
		"agent.connection_mode = 'runtime'",
		"session.status IN ('active', 'draining', 'offline')",
		"run.status = 'running'",
		"control.state IN ('paused', 'human')",
		"SET browser_execution_profile = TRUE",
	} {
		if !strings.Contains(stageRuntimeAgentBrowserPolicyIntentForOwner, fragment) {
			t.Fatalf("initial policy query is missing precondition %q", fragment)
		}
	}

	dbtx.row = fakeRow{values: []any{true}}
	declared, err := queries.LockRuntimeAgentBrowserDeclaration(context.Background(), agentID)
	if err != nil || !declared {
		t.Fatalf("lock Browser declaration = %t, %v", declared, err)
	}
	if !strings.Contains(lockRuntimeAgentBrowserDeclaration, "pg_advisory_xact_lock") ||
		!strings.Contains(lockRuntimeAgentBrowserDeclaration, "FOR SHARE OF agent") {
		t.Fatalf("Browser declaration query is not fenced: %s", lockRuntimeAgentBrowserDeclaration)
	}
}
