package registryprojection

import (
	"context"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func migratedPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	base := os.Getenv("TEST_DATABASE_URL")
	if base == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, base)
	require.NoError(t, err)
	name := "core_release_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = admin.Exec(ctx, "CREATE DATABASE "+name)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP DATABASE "+name+" WITH (FORCE)")
		admin.Close()
	})
	u, err := url.Parse(base)
	require.NoError(t, err)
	u.Path = "/" + name
	dir, _ := filepath.Abs(filepath.Join("..", "..", "migrations"))
	m, err := migrate.New("file://"+dir, u.String())
	require.NoError(t, err)
	require.NoError(t, m.Up())
	_, _ = m.Close()
	pool, err := pgxpool.New(ctx, u.String())
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	_, err = pool.Exec(ctx, `UPDATE registry_projection_settings SET mode = 'registry', registry_url = 'https://registry.example', consumer = 'core'`)
	require.NoError(t, err)
	return pool
}

func TestReleaseRules(t *testing.T) {
	pool := migratedPool(t)
	ctx := context.Background()
	p := &Projector{pool: pool}
	p.registry.URL, p.registry.Consumer = "https://registry.example", "core"
	mark := func(id uuid.UUID) {
		_, err := pool.Exec(ctx, `SELECT registry_projection_mark('skill', $1, 1::smallint)`, id)
		require.NoError(t, err)
	}
	row := func(id uuid.UUID) (suspended bool, delayed bool, lastError string) {
		require.NoError(t, pool.QueryRow(ctx, `SELECT suspended, COALESCE(next_attempt_at > clock_timestamp(), false), last_error
			FROM registry_projection_dirty WHERE subject_id = $1`, id).Scan(&suspended, &delayed, &lastError))
		return
	}

	t.Run("a stale failure does not delay a newer mark", func(t *testing.T) {
		id := uuid.New()
		mark(id)
		c, err := p.claim(ctx)
		require.NoError(t, err)
		require.Equal(t, id, c.Subject)
		mark(id)
		p.release(ctx, c, errors.New("timeout"))
		suspended, delayed, lastError := row(id)
		require.False(t, suspended)
		require.False(t, delayed)
		require.Empty(t, lastError)
		_, err = pool.Exec(ctx, `DELETE FROM registry_projection_dirty`)
		require.NoError(t, err)
	})

	t.Run("a permanent failure suspends the subject until a newer mark", func(t *testing.T) {
		id := uuid.New()
		mark(id)
		c, err := p.claim(ctx)
		require.NoError(t, err)
		p.release(ctx, c, &remoteError{Status: 409, Code: "name_conflict"})
		suspended, _, lastError := row(id)
		require.True(t, suspended)
		require.Contains(t, lastError, "name_conflict")
		next, err := p.claim(ctx)
		require.NoError(t, err)
		require.Nil(t, next)
		require.ErrorContains(t, p.Drain(ctx, time.Second), "failed permanently")
		mark(id)
		suspended, _, _ = row(id)
		require.False(t, suspended)
		_, err = pool.Exec(ctx, `DELETE FROM registry_projection_dirty`)
		require.NoError(t, err)
	})

	t.Run("a claim for another destination is refused", func(t *testing.T) {
		id := uuid.New()
		mark(id)
		other := &Projector{pool: pool}
		other.registry.URL, other.registry.Consumer = "https://other.example", "core"
		c, err := other.claim(ctx)
		require.NoError(t, err)
		require.Nil(t, c)
		c, err = p.claim(ctx)
		require.NoError(t, err)
		_, err = pool.Exec(ctx, `UPDATE registry_projection_settings SET registry_url = 'https://other.example'`)
		require.NoError(t, err)
		ok, err := p.registerAttempt(ctx, c, "p-x", uuid.New())
		require.NoError(t, err)
		require.False(t, ok)
		require.NoError(t, p.ack(ctx, c))
		var left int
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM registry_projection_dirty WHERE subject_id = $1`, id).Scan(&left))
		require.Equal(t, 1, left)
	})

	t.Run("a permanent refusal keeps the attempt", func(t *testing.T) {
		_, err := pool.Exec(ctx, `DELETE FROM registry_projection_dirty`)
		require.NoError(t, err)
		_, err = pool.Exec(ctx, `UPDATE registry_projection_settings SET registry_url = 'https://registry.example'`)
		require.NoError(t, err)
		id := uuid.New()
		mark(id)
		c, err := p.claim(ctx)
		require.NoError(t, err)
		ok, err := p.registerAttempt(ctx, c, "p-x", uuid.New())
		require.NoError(t, err)
		require.True(t, ok)
		p.release(ctx, c, &remoteError{Status: 409, Code: "namespace_conflict"})
		suspended, _, _ := row(id)
		require.True(t, suspended)
		l, err := loadLink(ctx, pool, KindSkill, id, "https://registry.example", "core")
		require.NoError(t, err)
		require.True(t, l.Attempted)
		_, err = pool.Exec(ctx, `DELETE FROM registry_projection_dirty`)
		require.NoError(t, err)
	})

	t.Run("rollback refuses when the destination or its generation changed meanwhile", func(t *testing.T) {
		_, err := pool.Exec(ctx, `UPDATE registry_projection_settings SET mode = 'registry', registry_url = 'https://registry.example', changed_at = clock_timestamp()`)
		require.NoError(t, err)
		s, err := loadSettings(ctx, pool)
		require.NoError(t, err)
		lock := func(q *Projector, generation time.Time, modes ...string) error {
			return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error { return q.lockDestination(ctx, tx, generation, modes...) })
		}
		require.ErrorIs(t, lock(p, s.ChangedAt, ModeDraining), ErrNotConfigured)
		other := &Projector{pool: pool}
		other.registry.URL, other.registry.Consumer = "https://other.example", "core"
		require.ErrorIs(t, lock(other, s.ChangedAt, ModeRegistry), ErrNotConfigured)
		require.NoError(t, lock(p, s.ChangedAt, ModeRegistry))
		_, err = pool.Exec(ctx, `UPDATE registry_projection_settings SET changed_at = clock_timestamp()`)
		require.NoError(t, err)
		require.ErrorIs(t, lock(p, s.ChangedAt, ModeRegistry), ErrNotConfigured)
	})

	t.Run("projection history is kept per destination URL and consumer", func(t *testing.T) {
		id, ownerA, ownerB := uuid.New(), uuid.New(), uuid.New()
		_, err := pool.Exec(ctx, `INSERT INTO registry_projection_links (kind, subject_id, registry_url, consumer, registry_entry, owner_id, attempted)
			VALUES ('skill', $1, 'https://a.example', 'core', 'p-x', $2, true), ('skill', $1, 'https://a.example', 'other', 'p-x', $3, true)`, id, ownerA, ownerB)
		require.NoError(t, err)
		a, err := loadLink(ctx, pool, KindSkill, id, "https://a.example", "core")
		require.NoError(t, err)
		b, err := loadLink(ctx, pool, KindSkill, id, "https://a.example", "other")
		require.NoError(t, err)
		require.True(t, a.Found && a.Attempted && b.Found && b.Attempted)
		require.Equal(t, ownerA, a.Owner)
		require.Equal(t, ownerB, b.Owner)
	})

	t.Run("making a public package unlisted is restrictive", func(t *testing.T) {
		_, err := pool.Exec(ctx, `DELETE FROM registry_projection_dirty`)
		require.NoError(t, err)
		_, err = pool.Exec(ctx, `UPDATE registry_projection_settings SET mode = 'local'`)
		require.NoError(t, err)
		owner, pkg := uuid.New(), uuid.New()
		_, err = pool.Exec(ctx, `INSERT INTO users(id,email,password_hash,display_name) VALUES($1,$2,'x','Owner')`, owner, owner.String()+"@example.invalid")
		require.NoError(t, err)
		_, err = pool.Exec(ctx, `INSERT INTO skill_packages(id,owner_user_id,name,description,visibility) VALUES($1,$2,'p','d','public')`, pkg, owner)
		require.NoError(t, err)
		_, err = pool.Exec(ctx, `UPDATE registry_projection_settings SET mode = 'registry'`)
		require.NoError(t, err)
		_, err = pool.Exec(ctx, `UPDATE skill_packages SET visibility = 'unlisted' WHERE id = $1`, pkg)
		require.NoError(t, err)
		var priority int
		require.NoError(t, pool.QueryRow(ctx, `SELECT priority FROM registry_projection_dirty WHERE subject_id = $1`, pkg).Scan(&priority))
		require.Zero(t, priority)
	})

	t.Run("drain keeps a scheduled backoff", func(t *testing.T) {
		_, err := pool.Exec(ctx, `DELETE FROM registry_projection_dirty`)
		require.NoError(t, err)
		_, err = pool.Exec(ctx, `UPDATE registry_projection_settings SET registry_url = 'https://registry.example'`)
		require.NoError(t, err)
		id := uuid.New()
		mark(id)
		_, err = pool.Exec(ctx, `UPDATE registry_projection_dirty SET next_attempt_at = clock_timestamp() + interval '1 minute' WHERE subject_id = $1`, id)
		require.NoError(t, err)
		require.Error(t, p.Drain(ctx, 500*time.Millisecond))
		var delayed bool
		require.NoError(t, pool.QueryRow(ctx, `SELECT next_attempt_at > clock_timestamp() + interval '30 seconds' FROM registry_projection_dirty WHERE subject_id = $1`, id).Scan(&delayed))
		require.True(t, delayed)
	})

	t.Run("a hidden state refused as not ours is done", func(t *testing.T) {
		require.True(t, hiddenDone(&remoteError{Status: 409, Code: "name_conflict"}))
		require.True(t, hiddenDone(&remoteError{Status: 409, Code: "namespace_conflict"}))
		require.True(t, hiddenDone(&remoteError{Status: 409, Code: "managed_by_core"}))
		require.False(t, hiddenDone(&remoteError{Status: 403, Code: "projection_not_allowed"}))
		require.False(t, hiddenDone(errors.New("timeout")))
	})

	t.Run("reconciliation leaves suspended subjects alone", func(t *testing.T) {
		for _, q := range []string{`DELETE FROM registry_projection_dirty`, `DELETE FROM registry_projection_links`,
			`UPDATE registry_projection_settings SET mode = 'registry', registry_url = 'https://registry.example', consumer = 'core'`} {
			_, err := pool.Exec(ctx, q)
			require.NoError(t, err)
		}
		id := uuid.New()
		mark(id)
		_, err := pool.Exec(ctx, `UPDATE registry_projection_dirty SET suspended = true, last_error = 'registry answered 409 name_conflict' WHERE subject_id = $1`, id)
		require.NoError(t, err)
		_, err = pool.Exec(ctx, `INSERT INTO registry_projection_links (kind, subject_id, registry_url, consumer, registry_entry, owner_id, attempted)
			VALUES ('skill', $1, 'https://registry.example', 'core', 'p-x', $2, true)`, id, uuid.New())
		require.NoError(t, err)
		rep, err := p.Reconcile(ctx)
		require.NoError(t, err)
		require.Zero(t, rep.Marked)
		suspended, _, _ := row(id)
		require.True(t, suspended)
	})
}
