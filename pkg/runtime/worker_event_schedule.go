package runtime

import (
	"context"
	"time"

	"github.com/OpenLinker-ai/openlinker-core/pkg/eventwake"
)

func databaseDueSchedule(nextDue *time.Time, databaseNow time.Time) eventwake.SchedulerResult {
	if nextDue == nil {
		return eventwake.SchedulerResult{}
	}
	delay := nextDue.Sub(databaseNow)
	if delay < 0 {
		delay = 0
	}
	return eventwake.SchedulerResult{NextDelay: delay, HasNext: true}
}

func waitWorkerFallbackInterval(ctx context.Context, interval time.Duration) bool {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func eventWakeSourceHealthy(source eventwake.TopicSource) bool {
	return source != nil && source.Health().Connected
}
