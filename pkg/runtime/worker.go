package runtime

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	defaultRuntimePullRunWorkerInterval = 30 * time.Second
	defaultRuntimePullDispatchTimeout   = 2 * time.Minute
	defaultRuntimePullResultTimeout     = 15 * time.Minute
	defaultRuntimePullTimeoutBatchSize  = 50
)

type RuntimePullRunWorkerConfig struct {
	Interval        time.Duration
	DispatchTimeout time.Duration
	ResultTimeout   time.Duration
	BatchSize       int32
}

// StartRuntimePullRunWorker closes abandoned runtime_pull runs so a crashed or
// misconfigured local worker cannot leave user-visible calls stuck forever.
func StartRuntimePullRunWorker(ctx context.Context, svc *Service, cfg RuntimePullRunWorkerConfig) {
	if svc == nil {
		return
	}
	if cfg.Interval <= 0 {
		cfg.Interval = defaultRuntimePullRunWorkerInterval
	}
	if cfg.DispatchTimeout <= 0 {
		cfg.DispatchTimeout = defaultRuntimePullDispatchTimeout
	}
	if cfg.ResultTimeout <= 0 {
		cfg.ResultTimeout = defaultRuntimePullResultTimeout
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultRuntimePullTimeoutBatchSize
	}
	log.Info().
		Dur("interval", cfg.Interval).
		Dur("dispatch_timeout", cfg.DispatchTimeout).
		Dur("result_timeout", cfg.ResultTimeout).
		Int32("batch_size", cfg.BatchSize).
		Msg("runtime: runtime_pull run worker started")
	defer log.Info().Msg("runtime: runtime_pull run worker stopped")

	runRuntimePullRunWorkerTick(ctx, svc, cfg)
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runRuntimePullRunWorkerTick(ctx, svc, cfg)
		}
	}
}

func runRuntimePullRunWorkerTick(ctx context.Context, svc *Service, cfg RuntimePullRunWorkerConfig) {
	timedOut, err := svc.TimeoutStaleRuntimePullRuns(ctx, RuntimePullRunTimeoutConfig{
		DispatchTimeout: cfg.DispatchTimeout,
		ResultTimeout:   cfg.ResultTimeout,
		BatchSize:       cfg.BatchSize,
	})
	if err != nil {
		log.Warn().Err(err).Msg("runtime: runtime_pull stale run timeout scan failed")
		return
	}
	if timedOut > 0 {
		log.Info().Int32("timed_out", timedOut).Msg("runtime: timed out stale runtime_pull runs")
	}
}
