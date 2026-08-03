package runtime_test

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

func TestFullBrowserProfileRejectsOldRuntimeSessionWithoutDowngrade(t *testing.T) {
	pool := setupTestDB(t)
	requireReliableRuntimeSchema(t, pool)
	resetRuntimeNodeAdminTables(t, pool)
	fixture := insertRuntimeNodeAdminFixture(t, pool)
	ctx := context.Background()

	var ownerID uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `
SELECT creator_id FROM agents WHERE id = $1`, fixture.agentID).Scan(&ownerID))
	fixtureTx, err := pool.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = fixtureTx.Rollback(context.Background())
	})
	_, err = fixtureTx.Exec(ctx, `
UPDATE runtime_session_attachments
SET detached_at = clock_timestamp(), disconnect_reason = 'browser-policy-fixture'
WHERE runtime_session_id = $1 AND detached_at IS NULL`, fixture.sessionID)
	require.NoError(t, err)
	_, err = fixtureTx.Exec(ctx, `
UPDATE runtime_sessions
SET status = 'closed', inflight = 0, attached_core_instance_id = NULL,
    disconnected_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE runtime_session_id = $1`, fixture.sessionID)
	require.NoError(t, err)
	_, err = fixtureTx.Exec(ctx, `
UPDATE runtime_nodes SET inflight = 0 WHERE node_id = $1`, fixture.nodeID)
	require.NoError(t, err)
	_, err = fixtureTx.Exec(ctx, `
UPDATE agents SET visibility = 'private' WHERE id = $1`, fixture.agentID)
	require.NoError(t, err)
	require.NoError(t, fixtureTx.Commit(ctx))

	queries := db.New(pool)
	_, err = queries.ClassifyRuntimeAgentBrowserExecutionProfile(
		ctx,
		db.ClassifyRuntimeAgentBrowserExecutionProfileParams{
			AgentID:          fixture.agentID,
			CredentialID:     fixture.credentialID,
			RuntimeSessionID: fixture.sessionID,
		},
	)
	require.NoError(t, err)
	_, err = queries.UpdateRuntimeAgentBrowserInteractionPolicyForOwner(
		ctx,
		db.UpdateRuntimeAgentBrowserInteractionPolicyForOwnerParams{
			AgentID:                fixture.agentID,
			InteractionPolicy:      "full",
			BrowserMutationOrigins: []string{"https://fixture.example"},
			ChangedByUserID:        ownerID,
		},
	)
	require.NoError(t, err)

	sessions := runtime.NewRuntimeSessionService(pool, fixture.coreInstanceID)
	principal := runtimeNodeAdminPrincipal(fixture)
	request := func(workerID string, features ...string) runtime.RuntimeSessionRequest {
		value := runtimeNodeAdminSessionRequest(
			fixture,
			uuid.New(),
			workerID,
			2,
			1,
		)
		value.Features = append(value.Features, features...)
		sort.Strings(value.Features)
		value.AttachmentID = uuid.New()
		return value
	}

	oldWorker := request(
		"old-browser-worker",
		runtime.RuntimeBrowserExecutionProfileFeature,
	)
	_, err = sessions.CreateOrAttachSession(ctx, principal, oldWorker)
	require.True(
		t,
		runtime.IsRuntimeSessionError(err, runtime.RuntimeSessionErrorSessionConflict),
		"old Browser Worker error = %v, want SESSION_CONFLICT",
		err,
	)
	requireRuntimeSessionAbsent(t, pool, oldWorker.RuntimeSessionID)

	partialWorker := request(
		"partial-full-browser-worker",
		runtime.RuntimeBrowserFullInteractionFeature,
	)
	_, err = sessions.CreateOrAttachSession(ctx, principal, partialWorker)
	require.True(
		t,
		runtime.IsRuntimeSessionError(err, runtime.RuntimeSessionErrorSessionConflict),
		"partial full Browser Worker error = %v, want SESSION_CONFLICT",
		err,
	)
	requireRuntimeSessionAbsent(t, pool, partialWorker.RuntimeSessionID)

	fullWorker := request(
		"full-browser-worker",
		runtime.RuntimeBrowserExecutionProfileFeature,
		runtime.RuntimeBrowserFullInteractionFeature,
	)
	_, err = pool.Exec(ctx, `
UPDATE runtime_nodes SET features = $1 WHERE node_id = $2`,
		fullWorker.Features,
		fixture.nodeID,
	)
	require.NoError(t, err)
	state, err := sessions.CreateOrAttachSession(ctx, principal, fullWorker)
	require.NoError(t, err)
	require.Equal(t, fullWorker.RuntimeSessionID, state.Session.RuntimeSessionID)
	require.ElementsMatch(t, fullWorker.Features, state.Session.Features)
}

func TestOwnerStagedBrowserPolicyFencesRunCreationUntilFirstMatchingSession(t *testing.T) {
	pool := setupTestDB(t)
	requireReliableRuntimeSchema(t, pool)
	ctx := context.Background()
	ownerID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, ownerID, "https://runtime.invalid", 0, "approved")
	_, err := pool.Exec(ctx, `
UPDATE agents
SET visibility = 'private', connection_mode = 'runtime',
    endpoint_url = 'openlinker-runtime://queued'
WHERE id = $1`, agentID)
	require.NoError(t, err)

	_, err = db.New(pool).StageRuntimeAgentBrowserPolicyIntentForOwner(
		ctx,
		db.StageRuntimeAgentBrowserPolicyIntentForOwnerParams{
			AgentID:                agentID,
			ConfiguredByUserID:     ownerID,
			InteractionPolicy:      "full",
			BrowserMutationOrigins: []string{"https://fixture.example"},
		},
	)
	require.NoError(t, err)

	service := newTestService(t, pool)
	_, err = service.StartRun(ctx, ownerID, &runtime.RunRequest{
		AgentID:        agentID.String(),
		Input:          map[string]any{"task": "must wait for Browser authority"},
		IdempotencyKey: "staged-browser-policy-run-fence",
	}, "api")
	var httpErr *httpx.HTTPError
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, 409, httpErr.Status)
	require.Contains(t, httpErr.Message, "Browser 执行策略尚未就绪")

	var runCount int
	require.NoError(t, pool.QueryRow(ctx, `
SELECT COUNT(*)::int FROM runs WHERE agent_id = $1`, agentID).Scan(&runCount))
	require.Zero(t, runCount)
}

func TestFirstFullBrowserSessionConsumesOwnerStagedPolicy(t *testing.T) {
	pool := setupTestDB(t)
	requireReliableRuntimeSchema(t, pool)
	resetRuntimeNodeAdminTables(t, pool)
	fixture := insertRuntimeNodeAdminFixture(t, pool)
	ctx := context.Background()

	var ownerID uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `
SELECT creator_id FROM agents WHERE id = $1`, fixture.agentID).Scan(&ownerID))
	fixtureTx, err := pool.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = fixtureTx.Rollback(context.Background())
	})
	_, err = fixtureTx.Exec(ctx, `
UPDATE runtime_session_attachments
SET detached_at = clock_timestamp(), disconnect_reason = 'initial-browser-policy-fixture'
WHERE runtime_session_id = $1 AND detached_at IS NULL`, fixture.sessionID)
	require.NoError(t, err)
	_, err = fixtureTx.Exec(ctx, `
UPDATE runtime_sessions
SET status = 'closed', inflight = 0, attached_core_instance_id = NULL,
    disconnected_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE runtime_session_id = $1`, fixture.sessionID)
	require.NoError(t, err)
	_, err = fixtureTx.Exec(ctx, `
UPDATE runtime_nodes SET inflight = 0 WHERE node_id = $1`, fixture.nodeID)
	require.NoError(t, err)
	_, err = fixtureTx.Exec(ctx, `
UPDATE agents SET visibility = 'private' WHERE id = $1`, fixture.agentID)
	require.NoError(t, err)
	require.NoError(t, fixtureTx.Commit(ctx))

	queries := db.New(pool)
	intent, err := queries.StageRuntimeAgentBrowserPolicyIntentForOwner(
		ctx,
		db.StageRuntimeAgentBrowserPolicyIntentForOwnerParams{
			AgentID:                fixture.agentID,
			ConfiguredByUserID:     ownerID,
			InteractionPolicy:      "full",
			BrowserMutationOrigins: []string{"https://fixture.example"},
		},
	)
	require.NoError(t, err)
	require.Equal(t, "full", intent.InteractionPolicy)

	sessions := runtime.NewRuntimeSessionService(pool, fixture.coreInstanceID)
	principal := runtimeNodeAdminPrincipal(fixture)
	request := func(workerID string, features ...string) runtime.RuntimeSessionRequest {
		value := runtimeNodeAdminSessionRequest(
			fixture,
			uuid.New(),
			workerID,
			2,
			1,
		)
		value.Features = append(value.Features, features...)
		sort.Strings(value.Features)
		value.AttachmentID = uuid.New()
		return value
	}

	ordinary := request("ordinary-worker-after-browser-declaration")
	_, err = sessions.CreateOrAttachSession(ctx, principal, ordinary)
	require.True(t, runtime.IsRuntimeSessionError(err, runtime.RuntimeSessionErrorSessionConflict))
	requireRuntimeSessionAbsent(t, pool, ordinary.RuntimeSessionID)

	restricted := request(
		"restricted-worker-after-full-declaration",
		runtime.RuntimeBrowserExecutionProfileFeature,
	)
	_, err = sessions.CreateOrAttachSession(ctx, principal, restricted)
	require.True(t, runtime.IsRuntimeSessionError(err, runtime.RuntimeSessionErrorSessionConflict))
	requireRuntimeSessionAbsent(t, pool, restricted.RuntimeSessionID)

	full := request(
		"first-full-browser-worker",
		runtime.RuntimeBrowserExecutionProfileFeature,
		runtime.RuntimeBrowserFullInteractionFeature,
	)
	_, err = pool.Exec(ctx, `
UPDATE runtime_nodes SET features = $1 WHERE node_id = $2`,
		full.Features,
		fixture.nodeID,
	)
	require.NoError(t, err)
	state, err := sessions.CreateOrAttachSession(ctx, principal, full)
	require.NoError(t, err)
	require.Equal(t, full.RuntimeSessionID, state.Session.RuntimeSessionID)

	profile, err := queries.GetRuntimeAgentExecutionProfile(ctx, fixture.agentID)
	require.NoError(t, err)
	require.Equal(t, "full", profile.InteractionPolicy)
	require.Equal(t, []string{"https://fixture.example"}, profile.BrowserMutationOrigins)
	require.Equal(t, int64(1), profile.InteractionPolicyGeneration)
	var intentCount int
	require.NoError(t, pool.QueryRow(ctx, `
SELECT count(*) FROM runtime_agent_browser_policy_intents WHERE agent_id = $1`,
		fixture.agentID,
	).Scan(&intentCount))
	require.Zero(t, intentCount)
}

func requireRuntimeSessionAbsent(
	t *testing.T,
	pool *pgxpool.Pool,
	runtimeSessionID uuid.UUID,
) {
	t.Helper()
	var count int
	require.NoError(t, pool.QueryRow(context.Background(), `
SELECT count(*) FROM runtime_sessions WHERE runtime_session_id = $1`,
		runtimeSessionID,
	).Scan(&count))
	require.Zero(t, count)
}

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

	policyParams := db.UpdateRuntimeAgentBrowserInteractionPolicyForOwnerParams{
		AgentID:                agentID,
		InteractionPolicy:      "full",
		BrowserMutationOrigins: []string{"https://fixture.example"},
		ChangedByUserID:        ownerID,
	}
	nonOwnerParams := policyParams
	nonOwnerParams.ChangedByUserID = otherID
	if _, err = queries.UpdateRuntimeAgentBrowserInteractionPolicyForOwner(
		ctx,
		nonOwnerParams,
	); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("non-owner policy update = %v, want pgx.ErrNoRows", err)
	}
	runningRunID := uuid.New()
	activeRunTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin active Browser Run fixture: %v", err)
	}
	if _, err = activeRunTx.Exec(ctx, `
INSERT INTO runs (
    id, user_id, agent_id, input, request_metadata,
    cost_cents, platform_fee_cents, creator_revenue_cents,
    runtime_contract_id, idempotency_key_hash, idempotency_fingerprint,
    connection_mode_snapshot, endpoint_idempotency_snapshot,
    max_offer_count, max_attempts, dispatch_deadline_at, run_deadline_at
) VALUES (
    $1, $2, $3, '{}'::jsonb, '{}'::jsonb, 0, 0, 0,
    'openlinker.runtime.v2', decode(repeat('31', 32), 'hex'),
    decode(repeat('32', 32), 'hex'), 'runtime', FALSE, 20, 3,
    clock_timestamp() + INTERVAL '5 minutes',
    clock_timestamp() + INTERVAL '10 minutes'
)`,
		runningRunID, ownerID, agentID,
	); err != nil {
		_ = activeRunTx.Rollback(ctx)
		t.Fatalf("insert active Browser Run: %v", err)
	}
	if _, err = db.New(activeRunTx).UpdateRuntimeAgentBrowserInteractionPolicyForOwner(
		ctx,
		policyParams,
	); !errors.Is(err, pgx.ErrNoRows) {
		_ = activeRunTx.Rollback(ctx)
		t.Fatalf("policy update with active Run = %v, want pgx.ErrNoRows", err)
	}
	if err = activeRunTx.Rollback(ctx); err != nil {
		t.Fatalf("rollback active Browser Run fixture: %v", err)
	}
	updatedPolicy, err := queries.UpdateRuntimeAgentBrowserInteractionPolicyForOwner(
		ctx,
		policyParams,
	)
	if err != nil {
		t.Fatalf("owner policy update: %v", err)
	}
	if updatedPolicy.InteractionPolicy != "full" ||
		updatedPolicy.InteractionPolicyGeneration != 2 ||
		len(updatedPolicy.BrowserMutationOrigins) != 1 ||
		updatedPolicy.BrowserMutationOrigins[0] != "https://fixture.example" ||
		updatedPolicy.InteractionPolicyChangedByUserID != ownerID {
		t.Fatalf("updated policy = %#v", updatedPolicy)
	}
	if _, err = queries.UpdateRuntimeAgentBrowserInteractionPolicyForOwner(
		ctx,
		policyParams,
	); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("identical policy update = %v, want pgx.ErrNoRows", err)
	}
	lockTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin Browser Run-create lock transaction: %v", err)
	}
	if _, err = db.New(lockTx).LockRuntimeAgentBrowserProfileForRunCreate(
		ctx,
		agentID,
	); err != nil {
		_ = lockTx.Rollback(ctx)
		t.Fatalf("lock Browser authority for Run creation: %v", err)
	}
	mutationTx, err := pool.Begin(ctx)
	if err != nil {
		_ = lockTx.Rollback(ctx)
		t.Fatalf("begin concurrent policy mutation: %v", err)
	}
	if _, err = mutationTx.Exec(ctx, `SET LOCAL lock_timeout = '100ms'`); err != nil {
		_ = mutationTx.Rollback(ctx)
		_ = lockTx.Rollback(ctx)
		t.Fatalf("set policy mutation lock timeout: %v", err)
	}
	restrictedParams := policyParams
	restrictedParams.InteractionPolicy = "restricted"
	restrictedParams.BrowserMutationOrigins = []string{}
	_, err = db.New(mutationTx).UpdateRuntimeAgentBrowserInteractionPolicyForOwner(
		ctx,
		restrictedParams,
	)
	var lockErr *pgconn.PgError
	if !errors.As(err, &lockErr) || lockErr.Code != "55P03" {
		_ = mutationTx.Rollback(ctx)
		_ = lockTx.Rollback(ctx)
		t.Fatalf("concurrent policy mutation error = %v, want lock timeout", err)
	}
	_ = mutationTx.Rollback(ctx)
	if err = lockTx.Commit(ctx); err != nil {
		t.Fatalf("commit Browser Run-create lock transaction: %v", err)
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
