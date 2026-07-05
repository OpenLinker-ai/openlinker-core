package workflow

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	defaultRunWorkerInterval = 10 * time.Second
	defaultRunStaleAfter     = 30 * time.Minute
	defaultRunClaimBurst     = 5
)

type RunWorkerConfig struct {
	Interval   time.Duration
	StaleAfter time.Duration
	ClaimBurst int
}

func StartRunWorker(ctx context.Context, svc *Service, cfg RunWorkerConfig) {
	if svc == nil {
		return
	}
	if cfg.Interval <= 0 {
		cfg.Interval = defaultRunWorkerInterval
	}
	if cfg.StaleAfter <= 0 {
		cfg.StaleAfter = defaultRunStaleAfter
	}
	if cfg.ClaimBurst <= 0 {
		cfg.ClaimBurst = defaultRunClaimBurst
	}
	log.Info().Dur("interval", cfg.Interval).Dur("stale_after", cfg.StaleAfter).Int("claim_burst", cfg.ClaimBurst).Msg("workflow: run worker started")
	defer log.Info().Msg("workflow: run worker stopped")

	runWorkflowWorkerTick(ctx, svc, cfg)
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runWorkflowWorkerTick(ctx, svc, cfg)
		}
	}
}

func runWorkflowWorkerTick(ctx context.Context, svc *Service, cfg RunWorkerConfig) {
	requeued, err := svc.RequeueStaleWorkflowRuns(ctx, cfg.StaleAfter)
	if err != nil {
		log.Warn().Err(err).Msg("workflow: stale run recovery failed")
	}
	if requeued > 0 {
		log.Info().Int64("requeued", requeued).Msg("workflow: recovered stale workflow runs")
	}
	for i := 0; i < cfg.ClaimBurst; i++ {
		claimed, err := svc.ClaimAndRunPendingWorkflow(ctx)
		if err != nil {
			log.Warn().Err(err).Msg("workflow: claim pending run failed")
			return
		}
		if !claimed {
			return
		}
	}
}
