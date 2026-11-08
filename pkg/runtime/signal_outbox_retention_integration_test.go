package runtime

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

type signalRetentionFixture struct {
	pool    *pgxpool.Pool
	agentID uuid.UUID
}

func newSignalRetentionFixture(t *testing.T, ctx context.Context) *signalRetentionFixture {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	pool, err := pgxpool.New(ctx, databaseURL)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	// Eligible rows from other tests would make the batch accounting below
	// ambiguous; removing them is exactly what retention would do.
	_, err = pool.Exec(ctx, `DELETE FROM runtime_signal_outbox
		WHERE status = 'published' AND published_at < clock_timestamp() - INTERVAL '7 days'`)
	require.NoError(t, err)

	userID, agentID := uuid.New(), uuid.New()
	_, err = pool.Exec(ctx, `
		INSERT INTO users (id, email, password_hash, display_name, is_creator)
		VALUES ($1, $2, 'hash', 'Signal Retention Test', TRUE)`,
		userID, userID.String()+"@example.test")
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO agents (
			id, creator_id, slug, name, description, endpoint_url,
			price_per_call_cents, connection_mode
		) VALUES ($1, $2, $3, 'Signal Retention Agent', 'Signal retention fixture',
			'openlinker-runtime://node', 0, 'runtime')`,
		agentID, userID, "signal-retention-"+agentID.String())
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM runtime_signal_outbox WHERE agent_id = $1`, agentID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM agents WHERE id = $1`, agentID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM users WHERE id = $1`, userID)
	})
	return &signalRetentionFixture{pool: pool, agentID: agentID}
}

// insert writes one outbox row in an explicit state. age is how long ago the
// row was created; publishedAge is how long ago it was published (published
// rows only).
func (f *signalRetentionFixture) insert(t *testing.T, ctx context.Context, status string, age, publishedAge time.Duration) uuid.UUID {
	t.Helper()
	id := uuid.New()
	var leaseOwner *uuid.UUID
	var leaseExpiresAgo, publishedAgo *int64
	switch status {
	case "processing":
		owner := uuid.New()
		leaseOwner = &owner
		// An expired lease is the oldest possible in-flight state; it still
		// belongs to the publisher that recovers it, never to retention.
		expired := age.Milliseconds()
		leaseExpiresAgo = &expired
	case "published":
		ms := publishedAge.Milliseconds()
		publishedAgo = &ms
	}
	_, err := f.pool.Exec(ctx, `
		INSERT INTO runtime_signal_outbox (
			id, event_type, agent_id, payload, created_at, available_at, status,
			lease_owner, lease_expires_at, published_at, attempt_count, last_error
		) VALUES (
			$1, 'run.available', $2, '{}'::jsonb,
			clock_timestamp() - ($3::bigint * INTERVAL '1 millisecond'),
			clock_timestamp() - ($3::bigint * INTERVAL '1 millisecond'),
			$4, $5,
			clock_timestamp() - ($6::bigint * INTERVAL '1 millisecond'),
			clock_timestamp() - ($7::bigint * INTERVAL '1 millisecond'),
			CASE WHEN $4 = 'pending' THEN 3 ELSE 1 END,
			CASE WHEN $4 = 'pending' THEN 'SIGNAL_PUBLISH_FAILED' END
		)`,
		id, f.agentID, age.Milliseconds(), status, leaseOwner, leaseExpiresAgo, publishedAgo)
	require.NoError(t, err)
	return id
}

func (f *signalRetentionFixture) remaining(t *testing.T, ctx context.Context, ids ...uuid.UUID) map[uuid.UUID]string {
	t.Helper()
	rows, err := f.pool.Query(ctx, `SELECT id, status FROM runtime_signal_outbox WHERE id = ANY($1::uuid[])`, ids)
	require.NoError(t, err)
	defer rows.Close()
	out := map[uuid.UUID]string{}
	for rows.Next() {
		var id uuid.UUID
		var status string
		require.NoError(t, rows.Scan(&id, &status))
		out[id] = status
	}
	require.NoError(t, rows.Err())
	return out
}

func TestRuntimeSignalOutboxRetentionDeletesOnlyExpiredPublishedRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	f := newSignalRetentionFixture(t, ctx)
	day := 24 * time.Hour

	// Five expired published rows, oldest first: 12d, 11d, 10d, 9d, 8d.
	var expired []uuid.UUID
	for age := 12; age >= 8; age-- {
		expired = append(expired, f.insert(t, ctx, "published", time.Duration(age+1)*day, time.Duration(age)*day))
	}
	kept := map[string]uuid.UUID{
		// Published inside the window, although created long ago.
		"recent published": f.insert(t, ctx, "published", 30*day, 6*day),
		// Published just inside the default window.
		"window edge": f.insert(t, ctx, "published", 8*day, 7*day-time.Minute),
		// Undelivered and retrying for a month: never eligible.
		"old pending": f.insert(t, ctx, "pending", 30*day, 0),
		// Leased (lease long expired, recoverable by a publisher): never eligible.
		"old processing": f.insert(t, ctx, "processing", 30*day, 0),
	}
	all := append([]uuid.UUID(nil), expired...)
	for _, id := range kept {
		all = append(all, id)
	}

	store := db.New(f.pool)
	// One bounded batch deletes exactly the two oldest expired rows.
	result, err := PruneRuntimeSignalOutbox(ctx, store, RuntimeSignalOutboxRetentionConfig{BatchSize: 2, MaxBatches: 1})
	require.NoError(t, err)
	require.Equal(t, RuntimeSignalOutboxRetentionResult{Batches: 1, Deleted: 2, Backlog: true}, result)
	left := f.remaining(t, ctx, all...)
	require.NotContains(t, left, expired[0])
	require.NotContains(t, left, expired[1])
	for _, id := range expired[2:] {
		require.Equal(t, "published", left[id])
	}

	// The next pass drains the rest in batches of two and reports no backlog.
	result, err = PruneRuntimeSignalOutbox(ctx, store, RuntimeSignalOutboxRetentionConfig{BatchSize: 2, MaxBatches: 10})
	require.NoError(t, err)
	require.Equal(t, RuntimeSignalOutboxRetentionResult{Batches: 2, Deleted: 3}, result)

	left = f.remaining(t, ctx, all...)
	for _, id := range expired {
		require.NotContains(t, left, id)
	}
	require.Len(t, left, len(kept))
	for name, id := range kept {
		want := "published"
		switch name {
		case "old pending":
			want = "pending"
		case "old processing":
			want = "processing"
		}
		require.Equal(t, want, left[id], name)
	}

	// Nothing eligible remains: a further pass is a single empty batch.
	result, err = PruneRuntimeSignalOutbox(ctx, store, RuntimeSignalOutboxRetentionConfig{})
	require.NoError(t, err)
	require.Equal(t, RuntimeSignalOutboxRetentionResult{Batches: 1}, result)

	// A longer configured window keeps rows the default would delete.
	tenDays := f.insert(t, ctx, "published", 11*day, 10*day)
	result, err = PruneRuntimeSignalOutbox(ctx, store, RuntimeSignalOutboxRetentionConfig{Retention: 14 * day})
	require.NoError(t, err)
	require.Zero(t, result.Deleted)
	require.Contains(t, f.remaining(t, ctx, tenDays), tenDays)
}

func TestRuntimeSignalOutboxRetentionSkipsLockedRowsAcrossReplicas(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	f := newSignalRetentionFixture(t, ctx)
	day := 24 * time.Hour

	locked := f.insert(t, ctx, "published", 21*day, 20*day)
	var others []uuid.UUID
	for range 40 {
		others = append(others, f.insert(t, ctx, "published", 10*day, 9*day))
	}

	// Another replica holds a lock on the oldest eligible row. Retention must
	// neither wait for it nor delete it.
	tx, err := f.pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.Background()) }()
	_, err = tx.Exec(ctx, `SELECT id FROM runtime_signal_outbox WHERE id = $1 FOR UPDATE`, locked)
	require.NoError(t, err)

	// Two replicas prune concurrently in small batches; their batches must be
	// disjoint and together delete every unlocked eligible row exactly once.
	store := db.New(f.pool)
	var wg sync.WaitGroup
	results := make([]RuntimeSignalOutboxRetentionResult, 2)
	errs := make([]error, 2)
	passCtx, passCancel := context.WithTimeout(ctx, 10*time.Second)
	defer passCancel()
	for replica := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[replica], errs[replica] = PruneRuntimeSignalOutbox(passCtx, store, RuntimeSignalOutboxRetentionConfig{
				BatchSize: 3, MaxBatches: 50,
			})
		}()
	}
	wg.Wait()
	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	require.Equal(t, int64(len(others)), results[0].Deleted+results[1].Deleted)
	require.Empty(t, f.remaining(t, ctx, others...))
	require.Equal(t, map[uuid.UUID]string{locked: "published"}, f.remaining(t, ctx, locked))

	require.NoError(t, tx.Rollback(ctx))
	result, err := PruneRuntimeSignalOutbox(ctx, store, RuntimeSignalOutboxRetentionConfig{})
	require.NoError(t, err)
	require.Equal(t, int64(1), result.Deleted)
	require.Empty(t, f.remaining(t, ctx, locked))
}

// A signal that the publisher claims and publishes is retained until its
// published_at, not its created_at, leaves the window.
func TestRuntimeSignalOutboxRetentionUsesPublicationTime(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	f := newSignalRetentionFixture(t, ctx)
	queries := db.New(f.pool)

	created, err := queries.CreateRuntimeSignal(ctx, db.CreateRuntimeSignalParams{
		EventType: "run.available", AgentID: f.agentID, Payload: []byte(`{}`),
	})
	require.NoError(t, err)
	// The row waited 30 days before the publisher reached it.
	_, err = f.pool.Exec(ctx, `UPDATE runtime_signal_outbox
		SET created_at = clock_timestamp() - INTERVAL '30 days',
		    available_at = clock_timestamp() - INTERVAL '30 days'
		WHERE id = $1`, created.ID)
	require.NoError(t, err)
	bus := NewLocalSignalBus(uuid.New())
	defer func() { _ = bus.Close() }()
	worker := NewRuntimeSignalOutboxWorker(queries, bus)
	published := false
	for range 10 {
		if _, err := worker.ProcessOnce(ctx, RuntimeSignalOutboxWorkerConfig{BatchSize: 256}); err != nil {
			t.Fatal(err)
		}
		row, err := queries.GetRuntimeSignalByID(ctx, created.ID)
		require.NoError(t, err)
		if row.Status == "published" {
			published = true
			break
		}
	}
	require.True(t, published)

	result, err := PruneRuntimeSignalOutbox(ctx, queries, RuntimeSignalOutboxRetentionConfig{})
	require.NoError(t, err)
	require.Zero(t, result.Deleted)
	require.Equal(t, map[uuid.UUID]string{created.ID: "published"}, f.remaining(t, ctx, created.ID))
}
