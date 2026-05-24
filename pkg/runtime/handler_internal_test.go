package runtime

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteSSEHeartbeat(t *testing.T) {
	rec := httptest.NewRecorder()

	require.NoError(t, writeSSEHeartbeat(rec))

	assert.Equal(t, ": heartbeat\n\n", rec.Body.String())
}

func TestWriteSSEEventsStopsOnTerminalEvent(t *testing.T) {
	rec := httptest.NewRecorder()
	events := []RunEventResponse{
		{
			Sequence:  1,
			EventType: "run.started",
			Payload:   map[string]interface{}{"status": "running"},
			CreatedAt: time.Now(),
		},
		{
			Sequence:  2,
			EventType: "run.completed",
			Payload:   map[string]interface{}{"status": "success"},
			CreatedAt: time.Now(),
		},
	}

	terminal, nextSequence, err := writeSSEEvents(rec, events, 0)

	require.NoError(t, err)
	assert.True(t, terminal)
	assert.Equal(t, int32(2), nextSequence)
	assert.Contains(t, rec.Body.String(), "id: 1\nevent: run.started")
	assert.Contains(t, rec.Body.String(), "id: 2\nevent: run.completed")
}
