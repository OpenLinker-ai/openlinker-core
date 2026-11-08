package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

const (
	// DefaultRuntimeSignalOutboxRetention keeps published signals long enough
	// to diagnose a delivery incident. Nothing reads a published row back: the
	// bus consumers return to the business tables to dedupe, and every pending
	// or processing row stays until it is published.
	DefaultRuntimeSignalOutboxRetention = 7 * 24 * time.Hour
	// minimumRuntimeSignalOutboxRetention guards against a caller passing a
	// window short enough to discard the evidence of a live incident.
	minimumRuntimeSignalOutboxRetention    = time.Hour
	defaultRuntimeSignalRetentionInterval  = 15 * time.Minute
	defaultRuntimeSignalRetentionBatchSize = int32(1000)
	maxRuntimeSignalRetentionBatchSize     = int32(5000)
	defaultRuntimeSignalRetentionBatches   = 20
	maxRuntimeSignalRetentionBatches       = 100
	defaultRuntimeSignalRetentionCatchUp   = 5 * time.Second
)

type runtimeSignalOutboxRetentionStore interface {
	DeletePublishedRuntimeSignals(context.Context, db.DeletePublishedRuntimeSignalsParams) (int64, error)
}

// RuntimeSignalOutboxRetentionConfig bounds one retention pass. Zero values
// select the defaults; Retention below one hour is raised to the default.
type RuntimeSignalOutboxRetentionConfig struct {
	Retention    time.Duration
	Interval     time.Duration
	BatchSize    int32
	MaxBatches   int
	CatchUpDelay time.Duration
	Observer     WorkerObserver
}

type RuntimeSignalOutboxRetentionResult struct {
	Batches int
	Deleted int64
	// Backlog reports that the pass stopped at MaxBatches with a full last
	// batch, so more eligible rows probably remain.
	Backlog bool
}

func normalizeRuntimeSignalOutboxRetentionConfig(cfg RuntimeSignalOutboxRetentionConfig) RuntimeSignalOutboxRetentionConfig {
	if cfg.Retention < minimumRuntimeSignalOutboxRetention {
		cfg.Retention = DefaultRuntimeSignalOutboxRetention
	}
	if cfg.Interval <= 0 {
		cfg.Interval = defaultRuntimeSignalRetentionInterval
	}
	if cfg.BatchSize <= 0 || cfg.BatchSize > maxRuntimeSignalRetentionBatchSize {
		cfg.BatchSize = defaultRuntimeSignalRetentionBatchSize
	}
	if cfg.MaxBatches <= 0 || cfg.MaxBatches > maxRuntimeSignalRetentionBatches {
		cfg.MaxBatches = defaultRuntimeSignalRetentionBatches
	}
	if cfg.CatchUpDelay <= 0 || cfg.CatchUpDelay > cfg.Interval {
		cfg.CatchUpDelay = min(defaultRuntimeSignalRetentionCatchUp, cfg.Interval)
	}
	return cfg
}

// PruneRuntimeSignalOutbox deletes published signals older than the retention
// window in bounded batches, oldest first. Each batch is its own statement, so
// no transaction or row lock outlives one batch. Undelivered (pending) and
// leased (processing) rows are never eligible.
func PruneRuntimeSignalOutbox(
	ctx context.Context,
	store runtimeSignalOutboxRetentionStore,
	cfg RuntimeSignalOutboxRetentionConfig,
) (RuntimeSignalOutboxRetentionResult, error) {
	var result RuntimeSignalOutboxRetentionResult
	if store == nil {
		return result, errors.New("runtime signal outbox retention is not configured")
	}
	cfg = normalizeRuntimeSignalOutboxRetentionConfig(cfg)
	params := db.DeletePublishedRuntimeSignalsParams{
		RetentionMs: cfg.Retention.Milliseconds(),
		Limit:       cfg.BatchSize,
	}
	for batch := 0; batch < cfg.MaxBatches; batch++ {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		deleted, err := store.DeletePublishedRuntimeSignals(ctx, params)
		result.Batches++
		if err != nil {
			return result, fmt.Errorf("delete published runtime signals: %w", err)
		}
		result.Deleted += deleted
		if deleted < int64(cfg.BatchSize) {
			return result, nil
		}
	}
	result.Backlog = true
	return result, nil
}

// StartRuntimeSignalOutboxRetentionWorker runs PruneRuntimeSignalOutbox at
// startup and then every Interval. A pass that leaves a backlog is followed by
// another after CatchUpDelay instead, so a large historical table drains
// without any single pass holding the database for long. Every Core replica
// may run it; SKIP LOCKED keeps their batches disjoint.
func StartRuntimeSignalOutboxRetentionWorker(
	ctx context.Context,
	store runtimeSignalOutboxRetentionStore,
	cfg RuntimeSignalOutboxRetentionConfig,
) {
	if ctx == nil || store == nil {
		return
	}
	cfg = normalizeRuntimeSignalOutboxRetentionConfig(cfg)
	timer := time.NewTimer(0)
	defer timer.Stop()
	reason := "startup"
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		observeWorker(cfg.Observer, "runtime.signal_outbox.retention", reason, int(cfg.BatchSize))
		result, err := PruneRuntimeSignalOutbox(ctx, store, cfg)
		if err != nil && ctx.Err() == nil {
			log.Error().Err(err).Int64("deleted", result.Deleted).Msg("runtime signal outbox retention pass failed")
		} else if result.Deleted > 0 {
			log.Info().Int64("deleted", result.Deleted).Bool("backlog", result.Backlog).
				Msg("runtime signal outbox retention pruned published signals")
		}
		delay, next := cfg.Interval, "ticker"
		if err == nil && result.Backlog {
			delay, next = cfg.CatchUpDelay, "catch_up"
		}
		reason = next
		timer.Reset(delay)
	}
}
