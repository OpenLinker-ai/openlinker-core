package runtime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

func TestBrowserExecutionProfileClassificationIsDurableOwnerBoundAndResettable(t *testing.T) {
	pool := setupTestDB(t)
	ctx := context.Background()
	queries := db.New(pool)

	ownerID := uuid.New()
	otherID := uuid.New()
	operatorID := uuid.New()
	agentID := uuid.New()
	credentialID := uuid.New()
	sessionID := uuid.New()
	if _, err := pool.Exec(ctx, `
INSERT INTO users (id, email, password_hash, display_name, is_creator, creator_verified)
VALUES
    ($1, $4, 'x', 'Browser owner', TRUE, TRUE),
    ($2, $5, 'x', 'Other creator', TRUE, TRUE),
    ($3, $6, 'x', 'Browser operator', TRUE, TRUE)`,
		ownerID,
		otherID,
		operatorID,
		"browser-owner-"+ownerID.String()+"@example.test",
		"browser-other-"+otherID.String()+"@example.test",
		"browser-operator-"+operatorID.String()+"@example.test",
	); err != nil {
		t.Fatalf("insert users: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO agents (
    id, creator_id, slug, name, description, endpoint_url,
    price_per_call_cents, tags, lifecycle_status, visibility,
    certification_status, connection_mode
) VALUES (
    $1, $2, $3, 'Browser Agent', 'browser profile test',
    'openlinker-runtime://browser-profile-test', 0, '{}',
    'active', 'private', 'unreviewed', 'runtime'
)`,
		agentID,
		ownerID,
		"browser-profile-"+agentID.String(),
	); err != nil {
		t.Fatalf("insert Agent: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO agent_tokens (
    id, agent_id, creator_user_id, name, prefix, token_hash,
    scopes, status, redeemed_at
) VALUES (
    $1, $2, $3, 'browser-profile-token', $4, 'test-hash',
    ARRAY['agent:pull']::text[], 'active_runtime', clock_timestamp()
)`,
		credentialID,
		agentID,
		ownerID,
		"ol_agent_"+credentialID.String()[:8],
	); err != nil {
		t.Fatalf("insert Token: %v", err)
	}

	profile, err := queries.ClassifyRuntimeAgentBrowserExecutionProfile(
		ctx,
		db.ClassifyRuntimeAgentBrowserExecutionProfileParams{
			AgentID:          agentID,
			CredentialID:     credentialID,
			RuntimeSessionID: sessionID,
		},
	)
	if err != nil {
		t.Fatalf("classify private owner Browser Agent: %v", err)
	}
	if profile.ExecutionProfile != "browser" ||
		profile.ClassifiedByCredentialID != credentialID ||
		profile.SourceRuntimeSessionID != sessionID {
		t.Fatalf("classified profile = %#v", profile)
	}

	if _, err = pool.Exec(ctx, `
UPDATE agents
SET visibility = 'public'
WHERE id = $1
  AND NOT EXISTS (
      SELECT 1
      FROM runtime_agent_execution_profiles profile
      WHERE profile.agent_id = agents.id
        AND profile.execution_profile = 'browser'
  )`, agentID); err != nil {
		t.Fatalf("guard Browser visibility: %v", err)
	}
	var visibility string
	if err = pool.QueryRow(ctx, `SELECT visibility FROM agents WHERE id = $1`, agentID).
		Scan(&visibility); err != nil {
		t.Fatalf("read guarded visibility: %v", err)
	}
	if visibility != "private" {
		t.Fatalf("Browser Agent visibility = %q, want private", visibility)
	}

	resetParams := db.ResetRuntimeAgentExecutionProfileParams{
		AgentID:              agentID,
		ResetByUserID:        operatorID,
		ResetReason:          "all active Browser Sessions drained and Profile artifacts purged",
		ProfilePurgeAttested: true,
	}
	withoutAttestation := resetParams
	withoutAttestation.ProfilePurgeAttested = false
	if _, err = queries.ResetRuntimeAgentExecutionProfile(
		ctx,
		withoutAttestation,
	); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("reset without Profile purge attestation = %v, want pgx.ErrNoRows", err)
	}

	if _, err = queries.ResetRuntimeAgentExecutionProfile(
		ctx,
		resetParams,
	); err != nil {
		t.Fatalf("audited reset: %v", err)
	}
	if _, err = pool.Exec(ctx, `UPDATE agents SET visibility = 'public' WHERE id = $1`, agentID); err != nil {
		t.Fatalf("publish reset Agent: %v", err)
	}

	publicAgentID := uuid.New()
	publicCredentialID := uuid.New()
	if _, err = pool.Exec(ctx, `
INSERT INTO agents (
    id, creator_id, slug, name, description, endpoint_url,
    price_per_call_cents, tags, lifecycle_status, visibility,
    certification_status, connection_mode
) VALUES (
    $1, $2, $3, 'Public Browser Agent', 'must reject Browser declaration',
    'openlinker-runtime://public-browser-profile-test', 0, '{}',
	    'active', 'public', 'unreviewed', 'runtime'
	)`,
		publicAgentID,
		ownerID,
		"public-browser-profile-"+publicAgentID.String(),
	); err != nil {
		t.Fatalf("insert public Browser candidate Agent: %v", err)
	}
	if _, err = pool.Exec(ctx, `
	INSERT INTO agent_tokens (
	    id, agent_id, creator_user_id, name, prefix, token_hash,
	    scopes, status, redeemed_at
	) VALUES (
	    $1, $2, $3, 'public-browser-token', $4, 'test-hash-public',
	    ARRAY['agent:pull']::text[], 'active_runtime', clock_timestamp()
	)`,
		publicCredentialID,
		publicAgentID,
		ownerID,
		"ol_agent_"+publicCredentialID.String()[:8],
	); err != nil {
		t.Fatalf("insert public Browser candidate Token: %v", err)
	}
	_, err = queries.ClassifyRuntimeAgentBrowserExecutionProfile(
		ctx,
		db.ClassifyRuntimeAgentBrowserExecutionProfileParams{
			AgentID:          publicAgentID,
			CredentialID:     publicCredentialID,
			RuntimeSessionID: uuid.New(),
		},
	)
	if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("public Browser classification error = %v, want pgx.ErrNoRows", err)
	}

	wrongOwnerCredentialID := uuid.New()
	if _, err = pool.Exec(ctx, `
	UPDATE agents SET visibility = 'private' WHERE id = $1`,
		publicAgentID,
	); err != nil {
		t.Fatalf("make Browser candidate private: %v", err)
	}
	if _, err = pool.Exec(ctx, `
	INSERT INTO agent_tokens (
	    id, agent_id, creator_user_id, name, prefix, token_hash,
	    scopes, status, redeemed_at
	) VALUES (
	    $1, $2, $3, 'wrong-owner-browser-token', $4, 'test-hash-wrong-owner',
	    ARRAY['agent:pull']::text[], 'active_runtime', clock_timestamp()
	)`,
		wrongOwnerCredentialID,
		publicAgentID,
		otherID,
		"ol_agent_"+wrongOwnerCredentialID.String()[:8],
	); err != nil {
		t.Fatalf("prepare wrong-owner credential: %v", err)
	}
	_, err = queries.ClassifyRuntimeAgentBrowserExecutionProfile(
		ctx,
		db.ClassifyRuntimeAgentBrowserExecutionProfileParams{
			AgentID:          publicAgentID,
			CredentialID:     wrongOwnerCredentialID,
			RuntimeSessionID: uuid.New(),
		},
	)
	if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("wrong-owner Browser classification error = %v, want pgx.ErrNoRows", err)
	}

	raceAgentID := uuid.New()
	raceCredentialID := uuid.New()
	if _, err = pool.Exec(ctx, `
INSERT INTO agents (
    id, creator_id, slug, name, description, endpoint_url,
    price_per_call_cents, tags, lifecycle_status, visibility,
    certification_status, connection_mode
) VALUES (
    $1, $2, $3, 'Concurrent Browser Agent', 'classification lock test',
    'openlinker-runtime://concurrent-browser-profile-test', 0, '{}',
    'active', 'private', 'unreviewed', 'runtime'
)`,
		raceAgentID,
		ownerID,
		"concurrent-browser-profile-"+raceAgentID.String(),
	); err != nil {
		t.Fatalf("insert concurrent Browser Agent: %v", err)
	}
	if _, err = pool.Exec(ctx, `
INSERT INTO agent_tokens (
    id, agent_id, creator_user_id, name, prefix, token_hash,
    scopes, status, redeemed_at
) VALUES (
    $1, $2, $3, 'concurrent-browser-token', $4, 'test-hash-concurrent',
    ARRAY['agent:pull']::text[], 'active_runtime', clock_timestamp()
)`,
		raceCredentialID,
		raceAgentID,
		ownerID,
		"ol_agent_"+raceCredentialID.String()[:8],
	); err != nil {
		t.Fatalf("insert concurrent Browser Token: %v", err)
	}
	classifyTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin classification transaction: %v", err)
	}
	if _, err = db.New(classifyTx).ClassifyRuntimeAgentBrowserExecutionProfile(
		ctx,
		db.ClassifyRuntimeAgentBrowserExecutionProfileParams{
			AgentID:          raceAgentID,
			CredentialID:     raceCredentialID,
			RuntimeSessionID: uuid.New(),
		},
	); err != nil {
		_ = classifyTx.Rollback(ctx)
		t.Fatalf("classify concurrent Browser Agent: %v", err)
	}
	visibilityDone := make(chan error, 1)
	go func() {
		visibilityDone <- queries.SetAgentVisibilityForOwner(
			context.Background(),
			db.SetAgentVisibilityForOwnerParams{
				ID:         raceAgentID,
				CreatorID:  ownerID,
				Visibility: "public",
			},
		)
	}()
	select {
	case updateErr := <-visibilityDone:
		_ = classifyTx.Rollback(ctx)
		t.Fatalf("visibility update bypassed classification lock: %v", updateErr)
	case <-time.After(100 * time.Millisecond):
	}
	if err = classifyTx.Commit(ctx); err != nil {
		t.Fatalf("commit classification: %v", err)
	}
	select {
	case updateErr := <-visibilityDone:
		var pgErr *pgconn.PgError
		if !errors.As(updateErr, &pgErr) ||
			pgErr.Code != "23514" ||
			pgErr.ConstraintName != "agents_browser_execution_profile_private" {
			t.Fatalf("guarded visibility update error = %v", updateErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("guarded visibility update did not finish")
	}
	if err = pool.QueryRow(ctx, `SELECT visibility FROM agents WHERE id = $1`, raceAgentID).
		Scan(&visibility); err != nil {
		t.Fatalf("read concurrent visibility: %v", err)
	}
	if visibility != "private" {
		t.Fatalf("concurrent Browser Agent visibility = %q, want private", visibility)
	}
}
