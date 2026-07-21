package main

import (
	"testing"
	"time"
)

func TestCalculateDBCounterDeltaSeparatesObserverTransaction(t *testing.T) {
	start := dbCounterSnapshot{
		CollectedAt:  time.Unix(100, 0),
		StatsResetAt: time.Unix(1, 0),
		XactCommit:   100,
		XactRollback: 4,
		Tables: map[string]dbTableCounterSnapshot{
			"runtime_sessions": {
				Present: true, SeqScan: 10, IdxScan: 20, RowsRead: 30, RowsFetch: 40,
				Inserts: 2, Updates: 3, Deletes: 1, HotUpdates: 2,
			},
		},
	}
	end := dbCounterSnapshot{
		CollectedAt:  time.Unix(110, 0),
		StatsResetAt: time.Unix(1, 0),
		XactCommit:   121,
		XactRollback: 6,
		Tables: map[string]dbTableCounterSnapshot{
			"runtime_sessions": {
				Present: true, SeqScan: 15, IdxScan: 30, RowsRead: 45, RowsFetch: 60,
				Inserts: 3, Updates: 10, Deletes: 2, HotUpdates: 5,
			},
		},
	}

	delta, err := calculateDBCounterDelta(start, end)
	if err != nil {
		t.Fatalf("calculateDBCounterDelta error = %v", err)
	}
	if delta.RawXactCommit != 21 || delta.ObserverXactCommit != 1 || delta.AdjustedXactCommit != 20 {
		t.Fatalf("transaction delta = %#v", delta)
	}
	if delta.AdjustedXactCommitPerSec != 2 || delta.XactRollbackPerSec != 0.2 {
		t.Fatalf("transaction rates = commit %v rollback %v", delta.AdjustedXactCommitPerSec, delta.XactRollbackPerSec)
	}
	var runtimeSessions *dbTableCounterDelta
	for index := range delta.Tables {
		if delta.Tables[index].Table == "runtime_sessions" {
			runtimeSessions = &delta.Tables[index]
			break
		}
	}
	if runtimeSessions == nil {
		t.Fatal("runtime_sessions delta is missing")
	}
	if runtimeSessions.SeqScan != 5 || runtimeSessions.IdxScan != 10 ||
		runtimeSessions.Updates != 7 || runtimeSessions.HotUpdates != 3 {
		t.Fatalf("runtime_sessions delta = %#v", *runtimeSessions)
	}
}

func TestCalculateDBCounterDeltaRejectsStatisticsReset(t *testing.T) {
	start := dbCounterSnapshot{
		CollectedAt:  time.Unix(100, 0),
		StatsResetAt: time.Unix(1, 0),
		XactCommit:   100,
		Tables: map[string]dbTableCounterSnapshot{
			"runtime_sessions": {Present: true, IdxScan: 20},
		},
	}
	end := dbCounterSnapshot{
		CollectedAt:  time.Unix(110, 0),
		StatsResetAt: time.Unix(105, 0),
		XactCommit:   2,
		Tables: map[string]dbTableCounterSnapshot{
			"runtime_sessions": {Present: true, IdxScan: 1},
		},
	}

	delta, err := calculateDBCounterDelta(start, end)
	if err == nil {
		t.Fatal("statistics reset was accepted")
	}
	if !delta.StatsResetDetected {
		t.Fatalf("statistics reset flag = false, delta = %#v", delta)
	}
}

func TestCalculateDBCounterDeltaRejectsNonPositiveDuration(t *testing.T) {
	now := time.Now()
	_, err := calculateDBCounterDelta(
		dbCounterSnapshot{CollectedAt: now},
		dbCounterSnapshot{CollectedAt: now},
	)
	if err == nil {
		t.Fatal("zero duration was accepted")
	}
}

func TestCalculateDBCounterDeltaReportsMissingTrackedTables(t *testing.T) {
	start := dbCounterSnapshot{
		CollectedAt:  time.Unix(100, 0),
		StatsResetAt: time.Unix(1, 0),
		XactCommit:   10,
		Tables:       map[string]dbTableCounterSnapshot{},
	}
	end := dbCounterSnapshot{
		CollectedAt:  time.Unix(110, 0),
		StatsResetAt: time.Unix(1, 0),
		XactCommit:   11,
		Tables:       map[string]dbTableCounterSnapshot{},
	}
	delta, err := calculateDBCounterDelta(start, end)
	if err != nil {
		t.Fatalf("calculateDBCounterDelta error = %v", err)
	}
	if len(delta.MissingTables) != len(runtimeDBCounterTables) {
		t.Fatalf("missing tables = %v, want %d", delta.MissingTables, len(runtimeDBCounterTables))
	}
}

func TestEnforceDBIdleCommitRate(t *testing.T) {
	cfg := config{DBStrictIdleCommitRate: 2}
	stage := connectionCapacityStage{DBHold: &dbCounterDelta{
		AdjustedXactCommitPerSec: 1.99,
		Tables:                   []dbTableCounterDelta{},
	}}
	if err := enforceDBIdleCommitRate(cfg, stage); err != nil {
		t.Fatalf("rate below limit failed: %v", err)
	}
	stage.DBHold.AdjustedXactCommitPerSec = 2.01
	if err := enforceDBIdleCommitRate(cfg, stage); err == nil {
		t.Fatal("rate above limit was accepted")
	}
	stage.DBHold.AdjustedXactCommitPerSec = 1
	stage.DBError = "permission denied"
	if err := enforceDBIdleCommitRate(cfg, stage); err == nil {
		t.Fatal("database counter error was accepted")
	}
}

func TestEnforceDBIdleCommitRateHonorsMinimumObservationDuration(t *testing.T) {
	cfg := config{DBStrictIdleCommitRate: 2, DBStrictIdleMinDuration: 10 * time.Minute}
	stage := connectionCapacityStage{DBHold: &dbCounterDelta{
		DurationMS: 120_000, AdjustedXactCommitPerSec: 2.02, Tables: []dbTableCounterDelta{},
	}}
	if err := enforceDBIdleCommitRate(cfg, stage); err != nil {
		t.Fatalf("short diagnostic hold was treated as the sustained gate: %v", err)
	}
	stage.DBHold.DurationMS = 600_000
	if err := enforceDBIdleCommitRate(cfg, stage); err == nil {
		t.Fatal("eligible sustained hold above the limit was accepted")
	}
}
