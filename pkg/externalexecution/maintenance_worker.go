package externalexecution

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
)

const defaultCancellationMaintenanceInterval = time.Second

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
