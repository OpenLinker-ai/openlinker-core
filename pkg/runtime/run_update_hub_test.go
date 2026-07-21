package runtime

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/eventwake"
)

func TestRunUpdateHubUsesTypedRunTopicAndHealth(t *testing.T) {
	infrastructure := &runUpdateInfrastructureFake{health: eventwake.ListenerHealth{Connected: true}}
	hub := &RunUpdateHub{infrastructure: infrastructure}
	runID := uuid.New()
	subscription, err := hub.SubscribeRun(runID)
	require.NoError(t, err)
	require.True(t, hub.Healthy())
	require.Equal(t, runChangedWakeTopic, infrastructure.topic)
	require.Equal(t, runID.String(), infrastructure.resourceID)

	infrastructure.hub.Publish(runID.String())
	require.NoError(t, subscription.Wait(context.Background()))
	subscription.Close()
}

func TestRunUpdateHubRejectsMissingIdentityAndDegradedHealth(t *testing.T) {
	infrastructure := &runUpdateInfrastructureFake{}
	hub := &RunUpdateHub{infrastructure: infrastructure}
	_, err := hub.SubscribeRun(uuid.Nil)
	require.Error(t, err)
	require.False(t, hub.Healthy())
	require.False(t, (*RunUpdateHub)(nil).Healthy())
}

type runUpdateInfrastructureFake struct {
	hub        *eventwake.Hub
	health     eventwake.ListenerHealth
	topic      string
	resourceID string
}

func (f *runUpdateInfrastructureFake) Subscribe(topic, resourceID string) (*eventwake.Subscription, error) {
	if f.hub == nil {
		f.hub = eventwake.NewHub()
	}
	f.topic = topic
	f.resourceID = resourceID
	return f.hub.Subscribe(resourceID), nil
}

func (f *runUpdateInfrastructureFake) Health() eventwake.ListenerHealth {
	return f.health
}
