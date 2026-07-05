package delivery

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
)

const workerInterval = 30 * time.Second

// StartWorker 后台重试 worker（main.go 调，应在 goroutine 中）。
//
// 每 workerInterval 扫一次 run_deliveries：status='pending' AND next_retry_at <= NOW()
// 第 1 次投递在 enqueue → attemptInBackground 已立即触发；这里只负责重试调度。
func StartWorker(ctx context.Context, svc *Service) {
	log.Info().Dur("interval", workerInterval).Msg("delivery: worker started")
	defer log.Info().Msg("delivery: worker stopped")

	ticker := time.NewTicker(workerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			svc.processPending(ctx)
		}
	}
}
