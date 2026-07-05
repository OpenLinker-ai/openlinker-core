package registry

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	defaultProxyRunTimeout        = 15 * time.Minute
	defaultProxyRunWorkerInterval = 30 * time.Second
)

type ProxyRunWorkerConfig struct {
	Interval time.Duration
	Timeout  time.Duration
}

// StartProxyRunWorker marks pending / claimed proxy runs as timeout when a
// Registry Node never returns a terminal result.
func StartProxyRunWorker(ctx context.Context, svc *Service, cfg ProxyRunWorkerConfig) {
	if cfg.Interval <= 0 {
		cfg.Interval = defaultProxyRunWorkerInterval
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultProxyRunTimeout
	}
	log.Info().Dur("interval", cfg.Interval).Dur("timeout", cfg.Timeout).Msg("registry: proxy run worker started")
	defer log.Info().Msg("registry: proxy run worker stopped")

	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			total, err := svc.ExpireStaleProxyRuns(ctx, cfg.Timeout)
			if err != nil {
				log.Warn().Err(err).Msg("registry: proxy run timeout scan failed")
				continue
			}
			if total > 0 {
				log.Info().Int32("expired", total).Msg("registry: expired stale proxy runs")
			}
		}
	}
}
