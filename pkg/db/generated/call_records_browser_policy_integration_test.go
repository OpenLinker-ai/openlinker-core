package db

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestListCallRecordsReadsBrowserPolicyEvidenceFromRunAuthority(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	ownerID := uuid.New()
	agentID := uuid.New()
	runID := uuid.New()
	if _, err = tx.Exec(ctx, `
INSERT INTO users (id, email, password_hash, display_name, is_creator, creator_verified)
VALUES ($1, $2, 'hash', 'Browser Owner', TRUE, TRUE)
`, ownerID, ownerID.String()+"@browser-policy.test"); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `
INSERT INTO agents (
    id, creator_id, slug, name, description, endpoint_url,
    price_per_call_cents, visibility, connection_mode
) VALUES ($1, $2, $3, 'Browser Agent', 'fixture', 'openlinker-runtime://browser-policy-test', 0, 'private', 'runtime')
`, agentID, ownerID, "browser-policy-"+agentID.String()[:8]); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `
INSERT INTO runs (
    id, user_id, agent_id, input, request_metadata, status, cost_cents,
    platform_fee_cents, creator_revenue_cents, runtime_contract_id,
    dispatch_state, connection_mode_snapshot, endpoint_idempotency_snapshot,
    idempotency_key_hash, idempotency_fingerprint,
    dispatch_deadline_at, run_deadline_at
) VALUES (
    $1, $2, $3, '{}'::jsonb,
    jsonb_build_object('_openlinker_runtime_authority', jsonb_build_object(
        'execution_profile', 'browser',
        'browser_interaction_policy', 'full',
        'browser_interaction_policy_generation', 7,
        'browser_mutation_origins', jsonb_build_array('https://github.com'),
        'browser_mutation_origins_sha256',
        '2c829a3140408db7e59b0159777c8a00f7496b92bb154365d1fcfabd590b2c98'
    )),
    'running', 0, 0, 0, 'openlinker.runtime.v2', 'pending', 'runtime', FALSE,
    decode(repeat('11', 32), 'hex'), decode(repeat('22', 32), 'hex'),
    clock_timestamp() + INTERVAL '5 minutes',
    clock_timestamp() + INTERVAL '10 minutes'
)
`, runID, ownerID, agentID); err != nil {
		t.Fatal(err)
	}

	rows, err := New(tx).ListCallRecordsForUser(ctx, ListCallRecordsForUserParams{
		UserID: ownerID,
		View:   "made",
		Sort:   "started_desc",
		Limit:  20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("call record rows = %d", len(rows))
	}
	row := rows[0]
	if row.BrowserInteractionPolicy != "full" ||
		row.BrowserInteractionPolicyGeneration != 7 ||
		string(row.BrowserMutationOrigins) != `["https://github.com"]` ||
		row.BrowserMutationOriginsSHA256 !=
			"2c829a3140408db7e59b0159777c8a00f7496b92bb154365d1fcfabd590b2c98" {
		t.Fatalf("Browser policy evidence = %#v", row)
	}
}
