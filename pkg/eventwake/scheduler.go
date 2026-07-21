package eventwake

import (
	"context"
	"errors"
	"time"
)

const (
	defaultSchedulerReconcileInterval = 60 * time.Second
	defaultSchedulerErrorRetry        = time.Second
	defaultSchedulerMinimumDelay      = 10 * time.Millisecond
	defaultSchedulerHealthCheck       = time.Second
)

var ErrWakeSourceDegraded = errors.New("event wake source is degraded")

type SchedulerConfig struct {
	ReconcileInterval time.Duration
	ErrorRetry        time.Duration
	MinimumDelay      time.Duration
	HealthCheck       time.Duration
	Healthy           func() bool
}

type SchedulerResult struct {
	NextDelay time.Duration
	HasNext   bool
}

type SchedulerRun func(context.Context, string) (SchedulerResult, error)

// RunScheduler replaces fixed-frequency claim attempts with three bounded
// wake sources: a transactional topic notification, the worker's earliest
// durable due time, and a low-frequency reconciliation timer. The callback
// remains solely responsible for the authoritative PostgreSQL claim/CAS.
func RunScheduler(
	ctx context.Context,
	subscription *Subscription,
	config SchedulerConfig,
	run SchedulerRun,
) error {
	if subscription == nil || run == nil {
		return errors.New("event wake scheduler is not configured")
	}
	config = normalizeSchedulerConfig(config)
	reason := "startup"
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		result, runErr := run(ctx, reason)
		wait, timeoutReason := schedulerWait(config, result, runErr)
		waitErr := waitForSchedulerWake(ctx, subscription, config, wait)
		if waitErr == nil {
			reason = "event"
			continue
		}
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(waitErr, context.DeadlineExceeded) {
			reason = timeoutReason
			continue
		}
		if errors.Is(waitErr, ErrWakeSourceDegraded) {
			return waitErr
		}
		return waitErr
	}
}

func normalizeSchedulerConfig(config SchedulerConfig) SchedulerConfig {
	if config.ReconcileInterval <= 0 {
		config.ReconcileInterval = defaultSchedulerReconcileInterval
	}
	if config.ErrorRetry <= 0 {
		config.ErrorRetry = defaultSchedulerErrorRetry
	}
	if config.MinimumDelay <= 0 {
		config.MinimumDelay = defaultSchedulerMinimumDelay
	}
	if config.HealthCheck <= 0 {
		config.HealthCheck = defaultSchedulerHealthCheck
	}
	return config
}

func waitForSchedulerWake(
	ctx context.Context,
	subscription *Subscription,
	config SchedulerConfig,
	wait time.Duration,
) error {
	deadline := time.Now().Add(wait)
	for {
		if config.Healthy != nil && !config.Healthy() {
			return ErrWakeSourceDegraded
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return context.DeadlineExceeded
		}
		checkAfter := remaining
		if config.Healthy != nil && config.HealthCheck < checkAfter {
			checkAfter = config.HealthCheck
		}
		waitCtx, cancel := context.WithTimeout(ctx, checkAfter)
		_, err := subscription.Wait(waitCtx)
		cancel()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		// A health timeout is not a scheduler run. Continue waiting against
		// the original due/reconciliation deadline without querying storage.
	}
}

func schedulerWait(
	config SchedulerConfig,
	result SchedulerResult,
	runErr error,
) (time.Duration, string) {
	if runErr != nil {
		return config.ErrorRetry, "error_retry"
	}
	wait := config.ReconcileInterval
	reason := "reconcile"
	if result.HasNext && result.NextDelay < wait {
		wait = result.NextDelay
		reason = "due"
	}
	if wait < config.MinimumDelay {
		wait = config.MinimumDelay
	}
	return wait, reason
}
