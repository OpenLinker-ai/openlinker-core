package runtime

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/OpenLinker-ai/openlinker-core/pkg/eventwake"
)

const (
	defaultRuntimeMaintenanceInterval    = time.Second
	defaultRuntimeMaintenanceBatchSize   = 128
	defaultRuntimeMaintenanceCatchUpRuns = 4
	runtimeMaintenanceWakeTopic          = "run.changed"
	runtimeMaintenanceEventCoalesce      = time.Second
	runtimeMaintenanceReconcileInterval  = time.Minute
)

type runtimeDeadlineReconcileWorker interface {
	ReconcileBatch(context.Context, int) (RuntimeReconcileBatchResult, error)
}

type runtimeCancellationReapWorker interface {
	ReapExpiredCancellations(context.Context, int) (int, error)
}

type runtimeSessionReapWorker interface {
	ReapStaleSessions(context.Context, int) (int, error)
}

type runtimeDeadlineScheduleWorker interface {
	nextReconcileDue(context.Context) (*time.Time, time.Time, error)
}

type runtimeCancellationScheduleWorker interface {
	nextReapDue(context.Context) (*time.Time, time.Time, error)
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
	Observer              WorkerObserver
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
		cfg.Interval = defaultRuntimeMaintenanceInterval
	}
	if cfg.ReconcileBatchSize <= 0 || cfg.ReconcileBatchSize > maxRuntimeReconcileBatch {
		cfg.ReconcileBatchSize = defaultRuntimeMaintenanceBatchSize
	}
	if cfg.CancellationBatchSize <= 0 || cfg.CancellationBatchSize > maxRuntimeCancellationReapBatch {
		cfg.CancellationBatchSize = defaultRuntimeMaintenanceBatchSize
	}
	if cfg.SessionBatchSize <= 0 || cfg.SessionBatchSize > maxRuntimeSessionReapBatch {
		cfg.SessionBatchSize = defaultRuntimeMaintenanceBatchSize
	}
	if cfg.MaxCatchUpBatches <= 0 || cfg.MaxCatchUpBatches > 32 {
		cfg.MaxCatchUpBatches = defaultRuntimeMaintenanceCatchUpRuns
	}
	return cfg
}

// RunRuntimeMaintenanceOnce executes lease/deadline reconciliation and
// cancellation deadline recovery independently. An error in one path does not
// suppress the other path; errors are joined after both bounded passes finish.
func RunRuntimeMaintenanceOnce(
	ctx context.Context,
	reconciler runtimeDeadlineReconcileWorker,
	cancellations runtimeCancellationReapWorker,
	sessions runtimeSessionReapWorker,
	cfg RuntimeMaintenanceWorkerConfig,
) (RuntimeMaintenanceResult, error) {
	cfg = normalizeRuntimeMaintenanceWorkerConfig(cfg)
	var result RuntimeMaintenanceResult
	var errs []error
	if err := runRuntimeReconcileBatches(ctx, reconciler, cfg, &result); err != nil {
		errs = append(errs, err)
	}
	if err := runRuntimeCancellationBatches(ctx, cancellations, cfg, &result); err != nil {
		errs = append(errs, err)
	}
	if err := runRuntimeSessionBatches(ctx, sessions, cfg, &result); err != nil {
		errs = append(errs, err)
	}

	return result, errors.Join(errs...)
}

func runRuntimeReconcileBatches(
	ctx context.Context,
	reconciler runtimeDeadlineReconcileWorker,
	cfg RuntimeMaintenanceWorkerConfig,
	result *RuntimeMaintenanceResult,
) error {
	if reconciler == nil {
		return ErrRuntimeReconcilerNotConfigured
	}
	for batch := 0; batch < cfg.MaxCatchUpBatches; batch++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		batchResult, err := reconciler.ReconcileBatch(ctx, cfg.ReconcileBatchSize)
		result.ReconcileBatches++
		result.Reconciled += batchResult.Reconciled
		result.Requeued += batchResult.Requeued
		result.TimedOut += batchResult.TimedOut
		result.DeadLettered += batchResult.DeadLettered
		if err != nil {
			return err
		}
		if batchResult.Scanned < cfg.ReconcileBatchSize {
			return nil
		}
	}
	return nil
}

func runRuntimeCancellationBatches(
	ctx context.Context,
	cancellations runtimeCancellationReapWorker,
	cfg RuntimeMaintenanceWorkerConfig,
	result *RuntimeMaintenanceResult,
) error {
	if cancellations == nil {
		return errRuntimeCancellationNotReady
	}
	for batch := 0; batch < cfg.MaxCatchUpBatches; batch++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		reaped, err := cancellations.ReapExpiredCancellations(ctx, cfg.CancellationBatchSize)
		result.CancellationBatches++
		result.CancellationsReaped += reaped
		if err != nil {
			return err
		}
		if reaped < cfg.CancellationBatchSize {
			return nil
		}
	}
	return nil
}

func runRuntimeSessionBatches(
	ctx context.Context,
	sessions runtimeSessionReapWorker,
	cfg RuntimeMaintenanceWorkerConfig,
	result *RuntimeMaintenanceResult,
) error {
	if sessions == nil {
		return ErrRuntimeSessionReaperNotConfigured
	}
	for batch := 0; batch < cfg.MaxCatchUpBatches; batch++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		reaped, err := sessions.ReapStaleSessions(ctx, cfg.SessionBatchSize)
		result.SessionBatches++
		result.SessionsReaped += reaped
		if err != nil {
			return err
		}
		if reaped < cfg.SessionBatchSize {
			return nil
		}
	}
	return nil
}

// StartRuntimeMaintenanceWorker runs an immediate pass and then continues
// until shutdown. PostgreSQL remains the truth source; failures are logged and
// retried on the next tick instead of terminating the API process.
func StartRuntimeMaintenanceWorker(
	ctx context.Context,
	reconciler runtimeDeadlineReconcileWorker,
	cancellations runtimeCancellationReapWorker,
	sessions runtimeSessionReapWorker,
	cfg RuntimeMaintenanceWorkerConfig,
) {
	cfg = normalizeRuntimeMaintenanceWorkerConfig(cfg)
	run := func(reason string) {
		observeWorker(cfg.Observer, "runtime.maintenance.scan", reason, cfg.ReconcileBatchSize)
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

	run("startup")
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run("ticker")
		}
	}
}

// StartRuntimeMaintenanceWorkerWithWake keeps the legacy worker available for
// tests and degraded deployments, while replacing healthy PostgreSQL deadline
// and cancellation polling with transactional Run wakes, exact due timers and
// one low-frequency reconciliation. Session expiry retains its existing
// cadence because healthy Redis lease checks do not touch PostgreSQL and its
// externally visible offline convergence must not be extended.
func StartRuntimeMaintenanceWorkerWithWake(
	ctx context.Context,
	reconciler runtimeDeadlineReconcileWorker,
	cancellations runtimeCancellationReapWorker,
	sessions runtimeSessionReapWorker,
	cfg RuntimeMaintenanceWorkerConfig,
	source eventwake.TopicSource,
) {
	cfg = normalizeRuntimeMaintenanceWorkerConfig(cfg)
	deadlineSchedule, deadlineScheduleOK := reconciler.(runtimeDeadlineScheduleWorker)
	cancellationSchedule, cancellationScheduleOK := cancellations.(runtimeCancellationScheduleWorker)
	if source == nil || !deadlineScheduleOK || !cancellationScheduleOK {
		StartRuntimeMaintenanceWorker(ctx, reconciler, cancellations, sessions, cfg)
		return
	}

	var passMu sync.Mutex
	firstDatabasePassDone := make(chan struct{})
	var firstDatabasePass sync.Once
	runDatabasePass := func(
		runCtx context.Context,
		reason string,
	) (eventwake.SchedulerResult, error) {
		if reason == "event" && !waitWorkerFallbackInterval(runCtx, runtimeMaintenanceEventCoalesce) {
			return eventwake.SchedulerResult{}, nil
		}
		passMu.Lock()
		defer passMu.Unlock()
		defer firstDatabasePass.Do(func() { close(firstDatabasePassDone) })

		observeWorker(cfg.Observer, "runtime.maintenance.database_scan", reason, cfg.ReconcileBatchSize)
		forceReconcile := reason == "degraded_poll" || reason == "reconcile"
		if !forceReconcile {
			schedule, err := nextRuntimeMaintenanceSchedule(runCtx, deadlineSchedule, cancellationSchedule)
			if err != nil || !schedule.HasNext || schedule.NextDelay > 0 {
				return schedule, err
			}
		}
		var result RuntimeMaintenanceResult
		err := errors.Join(
			runRuntimeReconcileBatches(runCtx, reconciler, cfg, &result),
			runRuntimeCancellationBatches(runCtx, cancellations, cfg, &result),
		)
		logRuntimeMaintenanceResult(result)
		if err != nil {
			return eventwake.SchedulerResult{}, err
		}
		return nextRuntimeMaintenanceSchedule(runCtx, deadlineSchedule, cancellationSchedule)
	}

	go startRuntimeSessionMaintenanceWorker(
		ctx, sessions, cfg, firstDatabasePassDone, &passMu,
	)

	for ctx.Err() == nil {
		if !eventWakeSourceHealthy(source) {
			if _, err := runDatabasePass(ctx, "degraded_poll"); err != nil && ctx.Err() == nil {
				log.Error().Err(err).Msg("Runtime maintenance degraded pass failed")
			}
			if !waitWorkerFallbackInterval(ctx, cfg.Interval) {
				return
			}
			continue
		}
		subscription, err := source.SubscribeTopic(runtimeMaintenanceWakeTopic)
		if err != nil {
			if _, passErr := runDatabasePass(ctx, "degraded_poll"); passErr != nil && ctx.Err() == nil {
				log.Error().Err(passErr).Msg("Runtime maintenance degraded pass failed")
			}
			if !waitWorkerFallbackInterval(ctx, cfg.Interval) {
				return
			}
			continue
		}
		if !eventWakeSourceHealthy(source) {
			subscription.Close()
			continue
		}
		err = eventwake.RunScheduler(ctx, subscription, eventwake.SchedulerConfig{
			ReconcileInterval: runtimeMaintenanceReconcileInterval,
			ErrorRetry:        cfg.Interval,
			MinimumDelay:      cfg.Interval,
			HealthCheck:       cfg.Interval,
			Healthy:           func() bool { return eventWakeSourceHealthy(source) },
		}, runDatabasePass)
		subscription.Close()
		if ctx.Err() != nil {
			return
		}
		if err != nil && !errors.Is(err, eventwake.ErrWakeSourceDegraded) {
			log.Error().Err(err).Msg("Runtime maintenance event scheduler stopped; polling fallback enabled")
		}
	}
}

func startRuntimeSessionMaintenanceWorker(
	ctx context.Context,
	sessions runtimeSessionReapWorker,
	cfg RuntimeMaintenanceWorkerConfig,
	firstDatabasePassDone <-chan struct{},
	passMu *sync.Mutex,
) {
	select {
	case <-ctx.Done():
		return
	case <-firstDatabasePassDone:
	}
	run := func(reason string) {
		passMu.Lock()
		defer passMu.Unlock()
		observeWorker(cfg.Observer, "runtime.maintenance.session_scan", reason, cfg.SessionBatchSize)
		var result RuntimeMaintenanceResult
		if err := runRuntimeSessionBatches(ctx, sessions, cfg, &result); err != nil && ctx.Err() == nil {
			log.Error().Err(err).Msg("Runtime Session maintenance pass failed")
			return
		}
		logRuntimeMaintenanceResult(result)
	}
	run("startup")
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run("ticker")
		}
	}
}

func nextRuntimeMaintenanceSchedule(
	ctx context.Context,
	deadlines runtimeDeadlineScheduleWorker,
	cancellations runtimeCancellationScheduleWorker,
) (eventwake.SchedulerResult, error) {
	deadlineDue, deadlineNow, err := deadlines.nextReconcileDue(ctx)
	if err != nil {
		return eventwake.SchedulerResult{}, err
	}
	cancellationDue, cancellationNow, err := cancellations.nextReapDue(ctx)
	if err != nil {
		return eventwake.SchedulerResult{}, err
	}
	return earlierRuntimeMaintenanceSchedule(
		databaseDueSchedule(deadlineDue, deadlineNow),
		databaseDueSchedule(cancellationDue, cancellationNow),
	), nil
}

func earlierRuntimeMaintenanceSchedule(
	first eventwake.SchedulerResult,
	second eventwake.SchedulerResult,
) eventwake.SchedulerResult {
	if !first.HasNext {
		return second
	}
	if !second.HasNext || first.NextDelay <= second.NextDelay {
		return first
	}
	return second
}

func logRuntimeMaintenanceResult(result RuntimeMaintenanceResult) {
	if result.Reconciled == 0 && result.CancellationsReaped == 0 && result.SessionsReaped == 0 {
		return
	}
	log.Info().
		Int("reconciled", result.Reconciled).
		Int("requeued", result.Requeued).
		Int("timed_out", result.TimedOut).
		Int("dead_lettered", result.DeadLettered).
		Int("cancellations_reaped", result.CancellationsReaped).
		Int("sessions_reaped", result.SessionsReaped).
		Msg("Runtime maintenance pass committed")
}
