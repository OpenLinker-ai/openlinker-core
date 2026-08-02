package db

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestTaskCallbackOwnerQueryUsesCoveringPartialIndexWithoutSort(t *testing.T) {
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

	targetOwner := uuid.New()
	otherOwner := uuid.New()
	agentID := uuid.New()
	runID := uuid.New()
	for _, owner := range []uuid.UUID{targetOwner, otherOwner} {
		if _, err := tx.Exec(ctx, `
INSERT INTO users (id, email, password_hash, display_name)
VALUES ($1, $2, 'hash', 'Callback Owner')
`, owner, owner.String()+"@callback-index.test"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO agents (
    id, creator_id, slug, name, description, endpoint_url,
    price_per_call_cents, visibility
) VALUES ($1, $2, 'callback-index-agent', 'Callback Index', 'fixture',
          'https://example.test/agent', 0, 'private')
`, agentID, targetOwner); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO runs (
    id, user_id, agent_id, input, status, cost_cents,
    platform_fee_cents, creator_revenue_cents, runtime_contract_id,
    dispatch_state, connection_mode_snapshot, endpoint_idempotency_snapshot,
    idempotency_key_hash, idempotency_fingerprint,
    dispatch_deadline_at, run_deadline_at
) VALUES ($1, $2, $3, '{}'::jsonb, 'running', 0, 0, 0,
          'openlinker.runtime.v2', 'pending', 'direct_http', TRUE,
          decode(repeat('11', 32), 'hex'), decode(repeat('22', 32), 'hex'),
          clock_timestamp() + INTERVAL '5 minutes',
          clock_timestamp() + INTERVAL '10 minutes')
`, runID, targetOwner, agentID); err != nil {
		t.Fatal(err)
	}

	if _, err := tx.Exec(ctx, `
INSERT INTO task_callback_subscriptions (
    run_id, owner_user_id, target_url, secret, status, created_at, updated_at
)
SELECT $1, $2, 'https://example.test/callback', 'secret',
       CASE WHEN value % 11 = 0 THEN 'paused' ELSE 'active' END,
       TIMESTAMPTZ '2026-08-01 00:00:00+00' - value * INTERVAL '1 second',
       TIMESTAMPTZ '2026-08-01 00:00:00+00' - (value / 3) * INTERVAL '1 second'
FROM generate_series(1, 30000) AS value
`, runID, otherOwner); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO task_callback_subscriptions (
    run_id, owner_user_id, target_url, secret, status, created_at, updated_at
)
SELECT $1, $2, 'https://example.test/callback', 'secret',
       CASE WHEN value % 5 = 0 THEN 'deleted' ELSE 'active' END,
       TIMESTAMPTZ '2026-08-01 00:00:00+00' - value * INTERVAL '1 second',
       TIMESTAMPTZ '2026-08-01 00:00:00+00' - (value / 3) * INTERVAL '1 second'
FROM generate_series(1, 30) AS value
`, runID, targetOwner); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `ANALYZE task_callback_subscriptions`); err != nil {
		t.Fatal(err)
	}

	query := listTaskCallbackSubscriptionsByOwner
	if newline := strings.IndexByte(query, '\n'); newline >= 0 {
		query = query[newline+1:]
	}
	var rawPlan []byte
	if err := tx.QueryRow(
		ctx,
		"EXPLAIN (ANALYZE, COSTS OFF, FORMAT JSON) "+query,
		targetOwner,
		"",
		int32(20),
	).Scan(&rawPlan); err != nil {
		t.Fatal(err)
	}
	var plan []map[string]any
	if err := json.Unmarshal(rawPlan, &plan); err != nil {
		t.Fatal(err)
	}
	if len(plan) != 1 {
		t.Fatalf("EXPLAIN roots = %d, want 1", len(plan))
	}
	encoded := string(rawPlan)
	if !strings.Contains(encoded, `"Index Name": "idx_task_callback_subscriptions_owner"`) {
		t.Fatalf("owner index not selected: %s", encoded)
	}
	if strings.Contains(encoded, `"Node Type": "Sort"`) {
		t.Fatalf("owner query retained a separate Sort: %s", encoded)
	}

	rows, err := New(tx).ListTaskCallbackSubscriptionsByOwner(ctx, ListTaskCallbackSubscriptionsByOwnerParams{
		OwnerUserID: targetOwner,
		Limit:       20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 20 {
		t.Fatalf("rows = %d, want 20", len(rows))
	}
	for index := 1; index < len(rows); index++ {
		previous := rows[index-1]
		current := rows[index]
		if current.UpdatedAt.After(previous.UpdatedAt) ||
			(current.UpdatedAt.Equal(previous.UpdatedAt) && current.CreatedAt.After(previous.CreatedAt)) {
			t.Fatalf("rows are not in updated_at/created_at descending order")
		}
	}
}
