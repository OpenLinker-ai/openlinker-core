package externalexecution

import (
	"context"
	"errors"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/OpenLinker-ai/openlinker-core/pkg/eventwake"
)

const (
	defaultCancellationMaintenanceInterval = time.Second
	externalCancellationWakeTopic          = "external_execution.changed"
	externalCancellationReconcileInterval  = time.Minute
)

func StartCancellationMaintenanceWorker(ctx context.Context, svc *Service, interval time.Duration, batch int) {
	if svc == nil {
		return
	}
	if interval <= 0 {
		interval = defaultCancellationMaintenanceInterval
	}
	if batch < 1 || batch > 1000 {
		batch = 100
	}
	run := func() {
		if _, err := svc.ReconcilePendingCancellations(ctx, batch); err != nil && ctx.Err() == nil {
			log.Warn().Err(err).Msg("external execution cancellation reconciliation failed")
		}
	}
	run()
	ticker := time.NewTicker(interval)
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

// StartCancellationMaintenanceWorkerWithWake replaces the healthy idle poll
// with transactional Core external-execution hints. Unresolved cancellation
// evidence retains the original interval, and listener degradation immediately
// restores the legacy poll. PostgreSQL remains the only cancellation fact.
func StartCancellationMaintenanceWorkerWithWake(
	ctx context.Context,
	svc *Service,
	interval time.Duration,
	batch int,
	source eventwake.TopicSource,
) {
	if svc == nil {
		return
	}
	if interval <= 0 {
		interval = defaultCancellationMaintenanceInterval
	}
	if batch < 1 || batch > 1000 {
		batch = 100
	}
	if source == nil {
		StartCancellationMaintenanceWorker(ctx, svc, interval, batch)
		return
	}
	run := func(runCtx context.Context, reason string) (eventwake.SchedulerResult, error) {
		if reason == "event" && !waitExternalCancellationWorker(runCtx, interval) {
			return eventwake.SchedulerResult{}, nil
		}
		result, err := svc.reconcilePendingCancellations(runCtx, batch)
		if err != nil {
			return eventwake.SchedulerResult{}, err
		}
		if result.Pending {
			return eventwake.SchedulerResult{HasNext: true, NextDelay: interval}, nil
		}
		return eventwake.SchedulerResult{}, nil
	}
	for ctx.Err() == nil {
		if !externalCancellationWakeHealthy(source) {
			if _, err := run(ctx, "degraded_poll"); err != nil && ctx.Err() == nil {
				log.Warn().Err(err).Msg("external execution cancellation degraded reconciliation failed")
			}
			if !waitExternalCancellationWorker(ctx, interval) {
				return
			}
			continue
		}
		subscription, err := source.SubscribeTopic(externalCancellationWakeTopic)
		if err != nil {
			if _, passErr := run(ctx, "degraded_poll"); passErr != nil && ctx.Err() == nil {
				log.Warn().Err(passErr).Msg("external execution cancellation degraded reconciliation failed")
			}
			if !waitExternalCancellationWorker(ctx, interval) {
				return
			}
			continue
		}
		if !externalCancellationWakeHealthy(source) {
			subscription.Close()
			continue
		}
		err = eventwake.RunScheduler(ctx, subscription, eventwake.SchedulerConfig{
			ReconcileInterval: externalCancellationReconcileInterval,
			ErrorRetry:        interval,
			MinimumDelay:      interval,
			HealthCheck:       interval,
			Healthy:           func() bool { return externalCancellationWakeHealthy(source) },
		}, run)
		subscription.Close()
		if ctx.Err() != nil {
			return
		}
		if err != nil && !errors.Is(err, eventwake.ErrWakeSourceDegraded) {
			log.Warn().Err(err).Msg("external execution cancellation event scheduler stopped; polling fallback enabled")
		}
	}
}

func externalCancellationWakeHealthy(source eventwake.TopicSource) bool {
	return source != nil && source.Health().Connected
}

func waitExternalCancellationWorker(ctx context.Context, interval time.Duration) bool {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
