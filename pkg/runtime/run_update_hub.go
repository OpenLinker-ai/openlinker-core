package runtime

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/OpenLinker-ai/openlinker-core/pkg/eventwake"
)

const runChangedWakeTopic = "run.changed"

type RunUpdateSubscription interface {
	Wait(context.Context) error
	Close()
}

type RunUpdateSource interface {
	SubscribeRun(uuid.UUID) (RunUpdateSubscription, error)
	Healthy() bool
}

type runUpdateInfrastructure interface {
	Subscribe(string, string) (*eventwake.Subscription, error)
	Health() eventwake.ListenerHealth
}

// RunUpdateHub adapts the generic event-wake infrastructure to Run IDs. It
// carries no Run fields or event payloads; every waiter must re-read
// PostgreSQL after a wake.
type RunUpdateHub struct {
	infrastructure runUpdateInfrastructure
}

func NewRunUpdateHub(infrastructure *eventwake.Infrastructure) *RunUpdateHub {
	if infrastructure == nil {
		return nil
	}
	return &RunUpdateHub{infrastructure: infrastructure}
}

func (h *RunUpdateHub) SubscribeRun(runID uuid.UUID) (RunUpdateSubscription, error) {
	if h == nil || h.infrastructure == nil {
		return nil, errors.New("Run update hub is not configured")
	}
	if runID == uuid.Nil {
		return nil, errors.New("Run update subscription requires a Run ID")
	}
	subscription, err := h.infrastructure.Subscribe(runChangedWakeTopic, runID.String())
	if err != nil {
		return nil, err
	}
	return runUpdateSubscription{subscription: subscription}, nil
}

func (h *RunUpdateHub) Healthy() bool {
	return h != nil && h.infrastructure != nil && h.infrastructure.Health().Connected
}

type runUpdateSubscription struct {
	subscription *eventwake.Subscription
}

func (s runUpdateSubscription) Wait(ctx context.Context) error {
	if s.subscription == nil {
		return errors.New("Run update subscription is not configured")
	}
	_, err := s.subscription.Wait(ctx)
	return err
}

func (s runUpdateSubscription) Close() {
	if s.subscription != nil {
		s.subscription.Close()
	}
}

var _ RunUpdateSource = (*RunUpdateHub)(nil)
