package webhook

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
)

// workerInterval worker 扫表间隔（30s 足够，重试 1min/5min/30min 粒度宽）。
const workerInterval = 30 * time.Second

// StartWorker 启动调用方 task callback 后台重试 worker（main.go 调，应该在 goroutine 中）。
//
// 行为：
//   - 每 workerInterval 扫一次 task_callback_deliveries：status='pending' AND next_retry_at <= NOW()
//   - 逐条调 AttemptTaskCallbackDelivery（顺序处理，避免对单一调用方 endpoint 并发打压）
//   - ctx.Done() → 优雅退出
func StartWorker(ctx context.Context, svc *Service) {
	log.Info().Dur("interval", workerInterval).Msg("webhook: worker started")
	defer log.Info().Msg("webhook: worker stopped")

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
