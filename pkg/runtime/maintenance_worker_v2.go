package runtime

import (
	"context"
	"errors"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	defaultRuntimeV2MaintenanceInterval    = time.Second
	defaultRuntimeV2MaintenanceBatchSize   = 128
	defaultRuntimeV2MaintenanceCatchUpRuns = 4
)

type runtimeV2DeadlineReconcileWorker interface {
	ReconcileBatch(context.Context, int) (RuntimeReconcileBatchResult, error)
}

type runtimeV2CancellationReapWorker interface {
	ReapExpiredCancellations(context.Context, int) (int, error)
}

type runtimeSessionReapWorker interface {
	ReapStaleSessions(context.Context, int) (int, error)
}

// RuntimeMaintenanceWorkerConfig bounds every tick so a large stale queue
// cannot monopolize a Core process. A full batch triggers a small, bounded
// catch-up loop; the next tick continues any remaining work.
type RuntimeMaintenanceWorkerConfig struct {
	Interval              time.Duration
	ReconcileBatchSize    int
	CancellationBatchSize int
	SessionBatchSize      int
	MaxCatchUpBatches     int
}

type RuntimeMaintenanceResult struct {
	ReconcileBatches    int
	CancellationBatches int
	SessionBatches      int
	Reconciled          int
	Requeued            int
	TimedOut            int
	DeadLettered        int
	CancellationsReaped int
	SessionsReaped      int
}

func normalizeRuntimeMaintenanceWorkerConfig(cfg RuntimeMaintenanceWorkerConfig) RuntimeMaintenanceWorkerConfig {
	if cfg.Interval <= 0 {
		cfg.Interval = defaultRuntimeV2MaintenanceInterval
	}
	if cfg.ReconcileBatchSize <= 0 || cfg.ReconcileBatchSize > maxRuntimeV2ReconcileBatch {
		cfg.ReconcileBatchSize = defaultRuntimeV2MaintenanceBatchSize
	}
	if cfg.CancellationBatchSize <= 0 || cfg.CancellationBatchSize > maxRuntimeCancellationReapBatch {
		cfg.CancellationBatchSize = defaultRuntimeV2MaintenanceBatchSize
	}
	if cfg.SessionBatchSize <= 0 || cfg.SessionBatchSize > maxRuntimeSessionReapBatch {
		cfg.SessionBatchSize = defaultRuntimeV2MaintenanceBatchSize
	}
	if cfg.MaxCatchUpBatches <= 0 || cfg.MaxCatchUpBatches > 32 {
		cfg.MaxCatchUpBatches = defaultRuntimeV2MaintenanceCatchUpRuns
	}
	return cfg
}

// RunRuntimeMaintenanceOnce executes lease/deadline reconciliation and
// cancellation deadline recovery independently. An error in one path does not
// suppress the other path; errors are joined after both bounded passes finish.
func RunRuntimeMaintenanceOnce(
	ctx context.Context,
	reconciler runtimeV2DeadlineReconcileWorker,
	cancellations runtimeV2CancellationReapWorker,
	sessions runtimeSessionReapWorker,
	cfg RuntimeMaintenanceWorkerConfig,
) (RuntimeMaintenanceResult, error) {
	cfg = normalizeRuntimeMaintenanceWorkerConfig(cfg)
	var result RuntimeMaintenanceResult
	var errs []error

	if reconciler == nil {
		errs = append(errs, ErrRuntimeReconcilerNotConfigured)
	} else {
		for batch := 0; batch < cfg.MaxCatchUpBatches; batch++ {
			if err := ctx.Err(); err != nil {
				errs = append(errs, err)
				break
			}
			batchResult, err := reconciler.ReconcileBatch(ctx, cfg.ReconcileBatchSize)
			result.ReconcileBatches++
			result.Reconciled += batchResult.Reconciled
			result.Requeued += batchResult.Requeued
			result.TimedOut += batchResult.TimedOut
			result.DeadLettered += batchResult.DeadLettered
			if err != nil {
				errs = append(errs, err)
				break
			}
			if batchResult.Scanned < cfg.ReconcileBatchSize {
				break
			}
		}
	}

	if cancellations == nil {
		errs = append(errs, errRuntimeCancellationNotReady)
	} else {
		for batch := 0; batch < cfg.MaxCatchUpBatches; batch++ {
			if err := ctx.Err(); err != nil {
				errs = append(errs, err)
				break
			}
			reaped, err := cancellations.ReapExpiredCancellations(ctx, cfg.CancellationBatchSize)
			result.CancellationBatches++
			result.CancellationsReaped += reaped
			if err != nil {
				errs = append(errs, err)
				break
			}
			if reaped < cfg.CancellationBatchSize {
				break
			}
		}
	}

	if sessions == nil {
		errs = append(errs, ErrRuntimeSessionReaperNotConfigured)
	} else {
		for batch := 0; batch < cfg.MaxCatchUpBatches; batch++ {
			if err := ctx.Err(); err != nil {
				errs = append(errs, err)
				break
			}
			reaped, err := sessions.ReapStaleSessions(ctx, cfg.SessionBatchSize)
			result.SessionBatches++
			result.SessionsReaped += reaped
			if err != nil {
				errs = append(errs, err)
				break
			}
			if reaped < cfg.SessionBatchSize {
				break
			}
		}
	}

	return result, errors.Join(errs...)
}

// StartRuntimeMaintenanceWorker runs an immediate pass and then continues
// until shutdown. PostgreSQL remains the truth source; failures are logged and
// retried on the next tick instead of terminating the API process.
func StartRuntimeMaintenanceWorker(
	ctx context.Context,
	reconciler runtimeV2DeadlineReconcileWorker,
	cancellations runtimeV2CancellationReapWorker,
	sessions runtimeSessionReapWorker,
	cfg RuntimeMaintenanceWorkerConfig,
) {
	cfg = normalizeRuntimeMaintenanceWorkerConfig(cfg)
	run := func() {
		result, err := RunRuntimeMaintenanceOnce(ctx, reconciler, cancellations, sessions, cfg)
		if err != nil && ctx.Err() == nil {
			log.Error().Err(err).Msg("Runtime maintenance pass failed")
			return
		}
		if result.Reconciled > 0 || result.CancellationsReaped > 0 || result.SessionsReaped > 0 {
			log.Info().
				Int("reconciled", result.Reconciled).
				Int("requeued", result.Requeued).
				Int("timed_out", result.TimedOut).
				Int("dead_lettered", result.DeadLettered).
				Int("cancellations_reaped", result.CancellationsReaped).
				Int("sessions_reaped", result.SessionsReaped).
				Msg("Runtime maintenance pass committed")
		}
	}

	run()
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run()
		}
	}
}
