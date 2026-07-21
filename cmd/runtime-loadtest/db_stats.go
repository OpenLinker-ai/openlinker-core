package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var runtimeDBCounterTables = []string{
	"agent_metric_snapshots",
	"agent_tokens",
	"run_effect_outbox",
	"run_events",
	"runs",
	"runtime_nodes",
	"runtime_session_attachments",
	"runtime_sessions",
	"runtime_signal_outbox",
}

type dbCounterReader struct {
	pool    *pgxpool.Pool
	initErr error
}

type dbCounterSnapshot struct {
	CollectedAt  time.Time
	StatsResetAt time.Time
	XactCommit   int64
	XactRollback int64
	Tables       map[string]dbTableCounterSnapshot
}

type dbTableCounterSnapshot struct {
	Present    bool
	SeqScan    int64
	IdxScan    int64
	RowsRead   int64
	RowsFetch  int64
	Inserts    int64
	Updates    int64
	Deletes    int64
	HotUpdates int64
}

type dbCounterDelta struct {
	DurationMS               float64               `json:"duration_ms"`
	StatsResetDetected       bool                  `json:"stats_reset_detected"`
	RawXactCommit            int64                 `json:"raw_xact_commit"`
	ObserverXactCommit       int64                 `json:"observer_xact_commit"`
	AdjustedXactCommit       int64                 `json:"adjusted_xact_commit"`
	AdjustedXactCommitPerSec float64               `json:"adjusted_xact_commit_per_second"`
	XactRollback             int64                 `json:"xact_rollback"`
	XactRollbackPerSec       float64               `json:"xact_rollback_per_second"`
	MissingTables            []string              `json:"missing_tables,omitempty"`
	Tables                   []dbTableCounterDelta `json:"tables"`
}

type dbTableCounterDelta struct {
	Table       string  `json:"table"`
	SeqScan     int64   `json:"seq_scan"`
	SeqScanRate float64 `json:"seq_scan_per_second"`
	IdxScan     int64   `json:"idx_scan"`
	IdxScanRate float64 `json:"idx_scan_per_second"`
	RowsRead    int64   `json:"rows_read"`
	RowsFetch   int64   `json:"rows_fetch"`
	Inserts     int64   `json:"inserts"`
	Updates     int64   `json:"updates"`
	Deletes     int64   `json:"deletes"`
	HotUpdates  int64   `json:"hot_updates"`
}

func newDBCounterReader(parent context.Context, databaseURL string) *dbCounterReader {
	reader := &dbCounterReader{}
	if strings.TrimSpace(databaseURL) == "" {
		reader.initErr = errors.New("database URL is empty")
		return reader
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		reader.initErr = fmt.Errorf("connect: %w", err)
		return reader
	}
	reader.pool = pool
	return reader
}

func (r *dbCounterReader) Close() {
	if r != nil && r.pool != nil {
		r.pool.Close()
	}
}

func (r *dbCounterReader) Snapshot(parent context.Context) (dbCounterSnapshot, error) {
	if r == nil {
		return dbCounterSnapshot{}, errors.New("database counter reader is not configured")
	}
	if r.initErr != nil {
		return dbCounterSnapshot{}, r.initErr
	}
	if r.pool == nil {
		return dbCounterSnapshot{}, errors.New("database counter reader has no connection pool")
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return dbCounterSnapshot{}, fmt.Errorf("begin database counter snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	snapshot := dbCounterSnapshot{Tables: make(map[string]dbTableCounterSnapshot, len(runtimeDBCounterTables))}
	for _, table := range runtimeDBCounterTables {
		snapshot.Tables[table] = dbTableCounterSnapshot{}
	}
	err = tx.QueryRow(ctx, `
SELECT clock_timestamp(),
       COALESCE(stats_reset, 'epoch'::timestamptz),
       xact_commit,
       xact_rollback
FROM pg_stat_database
WHERE datname = current_database()`).Scan(
		&snapshot.CollectedAt,
		&snapshot.StatsResetAt,
		&snapshot.XactCommit,
		&snapshot.XactRollback,
	)
	if err != nil {
		return dbCounterSnapshot{}, fmt.Errorf("read database transaction counters: %w", err)
	}
	rows, err := tx.Query(ctx, `
SELECT relname,
       seq_scan,
       idx_scan,
       seq_tup_read,
       idx_tup_fetch,
       n_tup_ins,
       n_tup_upd,
       n_tup_del,
       n_tup_hot_upd
FROM pg_stat_user_tables
WHERE relname = ANY($1::text[])
ORDER BY relname`, runtimeDBCounterTables)
	if err != nil {
		return dbCounterSnapshot{}, fmt.Errorf("read database table counters: %w", err)
	}
	for rows.Next() {
		var table string
		counter := dbTableCounterSnapshot{Present: true}
		if err = rows.Scan(
			&table,
			&counter.SeqScan,
			&counter.IdxScan,
			&counter.RowsRead,
			&counter.RowsFetch,
			&counter.Inserts,
			&counter.Updates,
			&counter.Deletes,
			&counter.HotUpdates,
		); err != nil {
			rows.Close()
			return dbCounterSnapshot{}, fmt.Errorf("scan database table counters: %w", err)
		}
		snapshot.Tables[table] = counter
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return dbCounterSnapshot{}, fmt.Errorf("iterate database table counters: %w", err)
	}
	rows.Close()
	if err = tx.Commit(ctx); err != nil {
		return dbCounterSnapshot{}, fmt.Errorf("commit database counter snapshot: %w", err)
	}
	return snapshot, nil
}

func calculateDBCounterDelta(start, end dbCounterSnapshot) (dbCounterDelta, error) {
	duration := end.CollectedAt.Sub(start.CollectedAt)
	if duration <= 0 {
		return dbCounterDelta{}, errors.New("database counter snapshot duration must be positive")
	}
	delta := dbCounterDelta{
		DurationMS:         round(ms(duration)),
		StatsResetDetected: end.StatsResetAt.After(start.StatsResetAt),
		RawXactCommit:      end.XactCommit - start.XactCommit,
		XactRollback:       end.XactRollback - start.XactRollback,
		Tables:             make([]dbTableCounterDelta, 0, len(runtimeDBCounterTables)),
	}
	// The starting snapshot commits after reading pg_stat_database, so exactly
	// one observer transaction is visible to the ending snapshot.
	delta.ObserverXactCommit = 1
	delta.AdjustedXactCommit = delta.RawXactCommit - delta.ObserverXactCommit
	if delta.AdjustedXactCommit < 0 {
		delta.StatsResetDetected = true
		delta.AdjustedXactCommit = 0
	}
	if delta.XactRollback < 0 {
		delta.StatsResetDetected = true
		delta.XactRollback = 0
	}
	seconds := duration.Seconds()
	delta.AdjustedXactCommitPerSec = round(float64(delta.AdjustedXactCommit) / seconds)
	delta.XactRollbackPerSec = round(float64(delta.XactRollback) / seconds)

	tableNames := append([]string(nil), runtimeDBCounterTables...)
	sort.Strings(tableNames)
	for _, table := range tableNames {
		startCounter := start.Tables[table]
		endCounter := end.Tables[table]
		if !startCounter.Present || !endCounter.Present {
			delta.MissingTables = append(delta.MissingTables, table)
			continue
		}
		tableDelta := dbTableCounterDelta{
			Table:      table,
			SeqScan:    endCounter.SeqScan - startCounter.SeqScan,
			IdxScan:    endCounter.IdxScan - startCounter.IdxScan,
			RowsRead:   endCounter.RowsRead - startCounter.RowsRead,
			RowsFetch:  endCounter.RowsFetch - startCounter.RowsFetch,
			Inserts:    endCounter.Inserts - startCounter.Inserts,
			Updates:    endCounter.Updates - startCounter.Updates,
			Deletes:    endCounter.Deletes - startCounter.Deletes,
			HotUpdates: endCounter.HotUpdates - startCounter.HotUpdates,
		}
		if dbTableCounterWentBackwards(tableDelta) {
			delta.StatsResetDetected = true
		}
		tableDelta.SeqScanRate = round(float64(max(tableDelta.SeqScan, 0)) / seconds)
		tableDelta.IdxScanRate = round(float64(max(tableDelta.IdxScan, 0)) / seconds)
		delta.Tables = append(delta.Tables, tableDelta)
	}
	if delta.StatsResetDetected {
		return delta, errors.New("PostgreSQL statistics reset or counter rollback detected during sample")
	}
	return delta, nil
}

func dbTableCounterWentBackwards(delta dbTableCounterDelta) bool {
	return delta.SeqScan < 0 ||
		delta.IdxScan < 0 ||
		delta.RowsRead < 0 ||
		delta.RowsFetch < 0 ||
		delta.Inserts < 0 ||
		delta.Updates < 0 ||
		delta.Deletes < 0 ||
		delta.HotUpdates < 0
}
