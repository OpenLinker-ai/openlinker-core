package db

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type retentionExecDBTX struct {
	signalQueryDBTX
	execSQL  string
	execArgs []any
	tag      pgconn.CommandTag
}

func (f *retentionExecDBTX) Exec(_ context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	f.execSQL = sql
	f.execArgs = append([]any(nil), args...)
	return f.tag, nil
}

func TestDeletePublishedRuntimeSignalsTouchesOnlyExpiredPublishedRows(t *testing.T) {
	dbtx := &retentionExecDBTX{tag: pgconn.NewCommandTag("DELETE 7")}
	deleted, err := New(dbtx).DeletePublishedRuntimeSignals(context.Background(), DeletePublishedRuntimeSignalsParams{
		RetentionMs: 604800000, Limit: 1000,
	})
	if err != nil || deleted != 7 {
		t.Fatalf("DeletePublishedRuntimeSignals = %d, %v", deleted, err)
	}
	requireSignalQueryName(t, dbtx.execSQL, "DeletePublishedRuntimeSignals")
	if !reflect.DeepEqual(dbtx.execArgs, []any{int64(604800000), int32(1000)}) {
		t.Fatalf("DeletePublishedRuntimeSignals args = %#v", dbtx.execArgs)
	}
	for _, guard := range []string{
		"WHERE status = 'published'",
		"published_at < statement_timestamp() - ($1::bigint * INTERVAL '1 millisecond')",
		"ORDER BY published_at, id",
		"LIMIT $2",
		"FOR UPDATE SKIP LOCKED",
		"AND s.status = 'published'",
	} {
		if !strings.Contains(dbtx.execSQL, guard) {
			t.Fatalf("DeletePublishedRuntimeSignals missing guard %q:\n%s", guard, dbtx.execSQL)
		}
	}
	for _, forbidden := range []string{"'pending'", "'processing'", "created_at", "clock_timestamp()"} {
		if strings.Contains(dbtx.execSQL, forbidden) {
			t.Fatalf("DeletePublishedRuntimeSignals must not select by %s:\n%s", forbidden, dbtx.execSQL)
		}
	}
}

func TestDeletePublishedRuntimeSignalsUsesRetentionIndexWithoutSort(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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

	// Expired rows left by other tests would be deleted first; the
	// transaction is rolled back, so this changes nothing outside it.
	if _, err := tx.Exec(ctx, `DELETE FROM runtime_signal_outbox
WHERE status = 'published' AND published_at < clock_timestamp() - INTERVAL '7 days'`); err != nil {
		t.Fatal(err)
	}
	userID, agentID := uuid.New(), uuid.New()
	if _, err := tx.Exec(ctx, `
INSERT INTO users (id, email, password_hash, display_name)
VALUES ($1, $2, 'hash', 'Signal Retention Index')
`, userID, userID.String()+"@signal-retention.test"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO agents (
    id, creator_id, slug, name, description, endpoint_url,
    price_per_call_cents, connection_mode
) VALUES ($1, $2, $3, 'Signal Retention Index', 'fixture',
          'openlinker-runtime://node', 0, 'runtime')
`, agentID, userID, "signal-retention-index-"+agentID.String()); err != nil {
		t.Fatal(err)
	}
	// Mostly recent published rows, a tail of expired ones and a pending
	// backlog: the shape of a table that has never been cleaned.
	if _, err := tx.Exec(ctx, `
INSERT INTO runtime_signal_outbox (
    event_type, agent_id, payload, created_at, available_at, status, published_at, attempt_count
)
SELECT 'run.available', $1, '{}'::jsonb,
       clock_timestamp() - value * INTERVAL '1 minute',
       clock_timestamp() - value * INTERVAL '1 minute',
       'published',
       clock_timestamp() - value * INTERVAL '1 minute' + INTERVAL '1 second',
       1
FROM generate_series(1, 20000) AS value
`, agentID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO runtime_signal_outbox (event_type, agent_id, payload, created_at, available_at)
SELECT 'run.available', $1, '{}'::jsonb,
       clock_timestamp() - INTERVAL '30 days', clock_timestamp() - INTERVAL '30 days'
FROM generate_series(1, 2000)
`, agentID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `ANALYZE runtime_signal_outbox`); err != nil {
		t.Fatal(err)
	}

	query := deletePublishedRuntimeSignals
	if newline := strings.IndexByte(query, '\n'); newline >= 0 {
		query = query[newline+1:]
	}
	var rawPlan []byte
	if err := tx.QueryRow(ctx, "EXPLAIN (COSTS OFF, FORMAT JSON) "+query, int64(604800000), int32(1000)).Scan(&rawPlan); err != nil {
		t.Fatal(err)
	}
	encoded := string(rawPlan)
	if !strings.Contains(encoded, `"Index Name": "idx_runtime_signal_outbox_published_retention"`) {
		t.Fatalf("retention index not selected: %s", encoded)
	}
	if strings.Contains(encoded, `"Node Type": "Sort"`) {
		t.Fatalf("retention query retained a separate Sort: %s", encoded)
	}
	requireRetentionIndexRangeScan(t, rawPlan, false)

	var pendingBefore int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM runtime_signal_outbox WHERE agent_id = $1 AND status = 'pending'`, agentID).Scan(&pendingBefore); err != nil {
		t.Fatal(err)
	}
	var cutoff time.Time
	if err := tx.QueryRow(ctx, `
SELECT published_at FROM runtime_signal_outbox
WHERE agent_id = $1 AND status = 'published'
ORDER BY published_at, id OFFSET 999 LIMIT 1`, agentID).Scan(&cutoff); err != nil {
		t.Fatal(err)
	}
	deleted, err := New(tx).DeletePublishedRuntimeSignals(ctx, DeletePublishedRuntimeSignalsParams{RetentionMs: 604800000, Limit: 1000})
	if err != nil || deleted != 1000 {
		t.Fatalf("DeletePublishedRuntimeSignals = %d, %v", deleted, err)
	}
	var pendingAfter int
	var oldestDeleted bool
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM runtime_signal_outbox WHERE agent_id = $1 AND status = 'pending'`, agentID).Scan(&pendingAfter); err != nil {
		t.Fatal(err)
	}
	if pendingAfter != pendingBefore || pendingBefore != 2000 {
		t.Fatalf("pending rows before=%d after=%d, want 2000 kept", pendingBefore, pendingAfter)
	}
	// Oldest first: the batch removed exactly the 1000 oldest published rows.
	if err := tx.QueryRow(ctx, `
SELECT NOT EXISTS (
    SELECT 1 FROM runtime_signal_outbox
    WHERE agent_id = $1 AND status = 'published' AND published_at <= $2
)`, agentID, cutoff).Scan(&oldestDeleted); err != nil {
		t.Fatal(err)
	}
	if !oldestDeleted {
		t.Fatal("retention did not delete the oldest published rows first")
	}
}

func TestRuntimeSignalRetentionIndexReadyTracksMigration101Index(t *testing.T) {
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
	ready, err := New(pool).RuntimeSignalRetentionIndexReady(ctx)
	if err != nil || !ready {
		t.Fatalf("migrated schema retention index ready = %t, %v", ready, err)
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `DROP INDEX public.idx_runtime_signal_outbox_published_retention`); err != nil {
		t.Fatal(err)
	}
	ready, err = New(tx).RuntimeSignalRetentionIndexReady(ctx)
	if err != nil || ready {
		t.Fatalf("dropped retention index ready = %t, %v", ready, err)
	}
}

// A drained table, where every published row is still inside the window, is
// the steady state. The cutoff must bound the index range so a pass reads no
// retained row; a volatile cutoff would only filter and walk them all.
func TestDeletePublishedRuntimeSignalsRangeScansDrainedTable(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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
	if _, err := tx.Exec(ctx, `DELETE FROM runtime_signal_outbox
WHERE status = 'published' AND published_at < clock_timestamp() - INTERVAL '7 days'`); err != nil {
		t.Fatal(err)
	}
	userID, agentID := uuid.New(), uuid.New()
	if _, err := tx.Exec(ctx, `
INSERT INTO users (id, email, password_hash, display_name)
VALUES ($1, $2, 'hash', 'Signal Retention Drained')
`, userID, userID.String()+"@signal-retention.test"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO agents (
    id, creator_id, slug, name, description, endpoint_url,
    price_per_call_cents, connection_mode
) VALUES ($1, $2, $3, 'Signal Retention Drained', 'fixture',
          'openlinker-runtime://node', 0, 'runtime')
`, agentID, userID, "signal-retention-drained-"+agentID.String()); err != nil {
		t.Fatal(err)
	}
	// 30,000 retained published rows spread over the last ~5 days.
	if _, err := tx.Exec(ctx, `
INSERT INTO runtime_signal_outbox (
    event_type, agent_id, payload, created_at, available_at, status, published_at, attempt_count
)
SELECT 'run.available', $1, '{}'::jsonb,
       clock_timestamp() - value * INTERVAL '14 seconds',
       clock_timestamp() - value * INTERVAL '14 seconds',
       'published',
       clock_timestamp() - value * INTERVAL '14 seconds' + INTERVAL '1 second',
       1
FROM generate_series(1, 30000) AS value
`, agentID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `ANALYZE runtime_signal_outbox`); err != nil {
		t.Fatal(err)
	}

	query := deletePublishedRuntimeSignals
	if newline := strings.IndexByte(query, '\n'); newline >= 0 {
		query = query[newline+1:]
	}
	var rawPlan []byte
	if err := tx.QueryRow(ctx, "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, FORMAT JSON) "+query,
		int64(604800000), int32(1000)).Scan(&rawPlan); err != nil {
		t.Fatal(err)
	}
	requireRetentionIndexRangeScan(t, rawPlan, true)
}

// requireRetentionIndexRangeScan requires the retention index with the
// published_at cutoff as an Index Cond and no filter on published_at. With
// analyzed set, it also requires that the scan neither returned nor
// discarded a row.
func requireRetentionIndexRangeScan(t *testing.T, rawPlan []byte, analyzed bool) {
	t.Helper()
	var plans []struct {
		Plan map[string]any `json:"Plan"`
	}
	if err := json.Unmarshal(rawPlan, &plans); err != nil || len(plans) != 1 {
		t.Fatalf("decode plan: %v: %s", err, rawPlan)
	}
	var nodes []map[string]any
	var walk func(map[string]any)
	walk = func(node map[string]any) {
		nodes = append(nodes, node)
		children, _ := node["Plans"].([]any)
		for _, child := range children {
			if next, ok := child.(map[string]any); ok {
				walk(next)
			}
		}
	}
	walk(plans[0].Plan)
	var scan map[string]any
	for _, node := range nodes {
		if node["Index Name"] == "idx_runtime_signal_outbox_published_retention" {
			scan = node
		}
		if filter, _ := node["Filter"].(string); strings.Contains(filter, "published_at") {
			t.Fatalf("published_at cutoff is a filter, not an index bound: %s", rawPlan)
		}
		if removed, ok := node["Rows Removed by Filter"].(float64); ok && removed > 0 {
			t.Fatalf("retention pass discarded %v rows by filter: %s", removed, rawPlan)
		}
	}
	if scan == nil {
		t.Fatalf("retention index not selected: %s", rawPlan)
	}
	if cond, _ := scan["Index Cond"].(string); !strings.Contains(cond, "published_at <") {
		t.Fatalf("published_at cutoff is not an Index Cond: %s", rawPlan)
	}
	if analyzed {
		if rows, _ := scan["Actual Rows"].(float64); rows != 0 {
			t.Fatalf("drained retention scan returned %v rows: %s", rows, rawPlan)
		}
	}
}
