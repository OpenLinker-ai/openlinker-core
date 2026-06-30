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

	defaultEndpointRunWorkerInterval = 30 * time.Second
	defaultEndpointRunTimeout        = 3 * time.Minute
	defaultEndpointRunTimeoutBuffer  = 30 * time.Second
	defaultEndpointRunBatchSize      = 50
)

type RuntimePullRunWorkerConfig struct {
	Interval        time.Duration
	DispatchTimeout time.Duration
	ResultTimeout   time.Duration
	BatchSize       int32
}

type EndpointRunWorkerConfig struct {
	Interval   time.Duration
	StaleAfter time.Duration
	RunTimeout time.Duration
	BatchSize  int32
}

// StartRuntimePullRunWorker closes abandoned queued runtime runs so a crashed
// or misconfigured local worker cannot leave user-visible calls stuck forever.
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
		Msg("runtime: queued runtime run worker started")
	defer log.Info().Msg("runtime: queued runtime run worker stopped")

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

// StartEndpointRunWorker closes abandoned direct_http / mcp_server runs so an
// API process crash after run creation cannot leave user-visible calls stuck.
func StartEndpointRunWorker(ctx context.Context, svc *Service, cfg EndpointRunWorkerConfig) {
	if svc == nil {
		return
	}
	if cfg.Interval <= 0 {
		cfg.Interval = defaultEndpointRunWorkerInterval
	}
	if cfg.RunTimeout <= 0 {
		cfg.RunTimeout = defaultEndpointRunTimeout - defaultEndpointRunTimeoutBuffer
	}
	if cfg.StaleAfter <= 0 {
		cfg.StaleAfter = defaultEndpointRunTimeout
	}
	minStaleAfter := cfg.RunTimeout + defaultEndpointRunTimeoutBuffer
	if cfg.StaleAfter < minStaleAfter {
		cfg.StaleAfter = minStaleAfter
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultEndpointRunBatchSize
	}
	log.Info().
		Dur("interval", cfg.Interval).
		Dur("stale_after", cfg.StaleAfter).
		Int32("batch_size", cfg.BatchSize).
		Msg("runtime: endpoint run worker started")
	defer log.Info().Msg("runtime: endpoint run worker stopped")

	runEndpointRunWorkerTick(ctx, svc, cfg)
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runEndpointRunWorkerTick(ctx, svc, cfg)
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
		log.Warn().Err(err).Msg("runtime: queued runtime stale run timeout scan failed")
		return
	}
	if timedOut > 0 {
		log.Info().Int32("timed_out", timedOut).Msg("runtime: timed out stale queued runtime runs")
	}
}

func runEndpointRunWorkerTick(ctx context.Context, svc *Service, cfg EndpointRunWorkerConfig) {
	timedOut, err := svc.TimeoutStaleEndpointRuns(ctx, EndpointRunTimeoutConfig{
		StaleAfter: cfg.StaleAfter,
		BatchSize:  cfg.BatchSize,
	})
	if err != nil {
		log.Warn().Err(err).Msg("runtime: endpoint stale run timeout scan failed")
		return
	}
	if timedOut > 0 {
		log.Info().Int32("timed_out", timedOut).Msg("runtime: timed out stale endpoint runs")
	}
}
