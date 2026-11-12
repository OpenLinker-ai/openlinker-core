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
	defaultRunMaxConcurrent  = 8

	workflowCancellationScanBatch = 20
)

type RunWorkerConfig struct {
	Interval   time.Duration
	StaleAfter time.Duration
	ClaimBurst int
	// MaxConcurrentRuns bounds how many claimed runs execute at once in this
	// process. The tick only claims; execution happens in goroutines.
	MaxConcurrentRuns int
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
	if cfg.MaxConcurrentRuns <= 0 {
		cfg.MaxConcurrentRuns = defaultRunMaxConcurrent
	}
	slots := make(chan struct{}, cfg.MaxConcurrentRuns)
	log.Info().Dur("interval", cfg.Interval).Dur("stale_after", cfg.StaleAfter).Int("claim_burst", cfg.ClaimBurst).Msg("workflow: run worker started")
	defer log.Info().Msg("workflow: run worker stopped")

	runWorkflowWorkerTick(ctx, svc, cfg, slots)
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runWorkflowWorkerTick(ctx, svc, cfg, slots)
		}
	}
}

func runWorkflowWorkerTick(ctx context.Context, svc *Service, cfg RunWorkerConfig, slots chan struct{}) {
	// Cancellations, stops and deadlines run regardless of admission and of
	// any executor: a drain and a failed or paused run depend on them.
	if attempted, err := svc.ReconcileWorkflowCancellations(ctx, workflowCancellationScanBatch); err != nil {
		log.Warn().Err(err).Msg("workflow: cancellation reconcile scan failed")
	} else if attempted > 0 {
		log.Info().Int("attempted", attempted).Msg("workflow: reconciled pending workflow cancellations")
	}
	if settled, err := svc.SettleExpiredWorkflowAttempts(ctx, workflowDeadlineScanBatch); err != nil {
		log.Warn().Err(err).Msg("workflow: attempt deadline scan failed")
	} else if settled > 0 {
		log.Info().Int("settled", settled).Msg("workflow: settled attempts past their deadline")
	}
	if handled, err := svc.ReconcileWorkflowStepStops(ctx, workflowStopScanBatch); err != nil {
		log.Warn().Err(err).Msg("workflow: step stop reconcile failed")
	} else if handled > 0 {
		log.Info().Int("handled", handled).Msg("workflow: reconciled workflow step stops")
	}
	requeued, err := svc.RequeueStaleWorkflowRuns(ctx, cfg.StaleAfter)
	if err != nil {
		log.Warn().Err(err).Msg("workflow: stale run recovery failed")
	}
	if requeued > 0 {
		log.Info().Int64("requeued", requeued).Msg("workflow: recovered stale workflow runs")
	}
	for i := 0; i < cfg.ClaimBurst; i++ {
		select {
		case slots <- struct{}{}:
		default:
			return // every execution slot is busy
		}
		execute, claimed, err := svc.ClaimPendingWorkflow(ctx)
		if err != nil || !claimed {
			<-slots
			if err != nil {
				log.Warn().Err(err).Msg("workflow: claim pending run failed")
			}
			return
		}
		go func() {
			defer func() { <-slots }()
			execute(ctx)
		}()
	}
}
