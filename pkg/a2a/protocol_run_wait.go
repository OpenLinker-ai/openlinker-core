package a2a

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

const (
	protocolRunHealthRecheckInterval = 2 * time.Second
	protocolRunDegradedPollInterval  = time.Second
)

type protocolRunLoader func(context.Context, uuid.UUID, uuid.UUID) (*runtime.RunResponse, error)

func isTerminalProtocolRunStatus(status string) bool {
	switch status {
	case "success", "failed", "timeout", "canceled":
		return true
	default:
		return false
	}
}

func (s *Service) waitForProtocolRun(ctx context.Context, userID uuid.UUID, initial *runtime.RunResponse) (*runtime.RunResponse, error) {
	if initial == nil {
		return nil, errors.New("A2A Runtime 返回空响应")
	}
	runID, err := uuid.Parse(initial.RunID)
	if err != nil || runID == uuid.Nil {
		return nil, fmt.Errorf("A2A Runtime runID 无效")
	}
	return waitForProtocolRun(ctx, userID, runID, initial, s.runUpdates, s.runtime.GetRun,
		protocolRunHealthRecheckInterval, protocolRunDegradedPollInterval)
}

// waitForProtocolRun treats wake notifications only as hints. It subscribes
// before the first durable re-read to close the missed-wake race, and degrades
// to bounded-rate PostgreSQL polling whenever the wake path is unavailable.
func waitForProtocolRun(
	ctx context.Context,
	userID, runID uuid.UUID,
	initial *runtime.RunResponse,
	updates runtime.RunUpdateSource,
	load protocolRunLoader,
	healthRecheck, degradedPoll time.Duration,
) (*runtime.RunResponse, error) {
	if load == nil {
		return nil, errors.New("A2A Runtime Run loader 未配置")
	}
	if healthRecheck <= 0 {
		healthRecheck = protocolRunHealthRecheckInterval
	}
	if degradedPoll <= 0 {
		degradedPoll = protocolRunDegradedPollInterval
	}

	var subscription runtime.RunUpdateSubscription
	if updates != nil && updates.Healthy() {
		subscription, _ = updates.SubscribeRun(runID)
		if subscription != nil {
			defer subscription.Close()
		}
	}

	current := initial
	for {
		if current == nil || current != initial || subscription != nil {
			var err error
			current, err = load(ctx, userID, runID)
			if err != nil {
				return nil, err
			}
		}
		if current != nil && isTerminalProtocolRunStatus(current.Status) {
			return current, nil
		}

		if subscription == nil || updates == nil || !updates.Healthy() {
			timer := time.NewTimer(degradedPoll)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
				current = nil
				continue
			}
		}

		waitCtx, cancel := context.WithTimeout(ctx, healthRecheck)
		waitErr := subscription.Wait(waitCtx)
		cancel()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if waitErr != nil && !errors.Is(waitErr, context.DeadlineExceeded) {
			subscription.Close()
			subscription = nil
		}
		current = nil
	}
}
