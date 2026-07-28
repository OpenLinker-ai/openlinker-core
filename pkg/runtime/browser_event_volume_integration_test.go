package runtime_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

// TestBrowserProjectedEventVolumeIsIndependentOfActionCount is the real
// PostgreSQL half of the Browser O(1) durability contract. The CLI half lives
// in TestRunCodexCommandKeepsBrowserActionVolumeOutOfProjectedEvents and proves
// that one and 500 Browser MCP actions produce the same projected event list.
// This test proves that persisting that list through Core creates the same
// bounded durable rows for both action volumes.
func TestBrowserProjectedEventVolumeIsIndependentOfActionCount(t *testing.T) {
	pool := setupTestDB(t)
	requireReliableRuntimeSchema(t, pool)

	type durableCounts struct {
		runEvents      int
		browserEvents  int
		runMessages    int
		runArtifacts   int
		artifactChunks int
		actionPayloads int
	}
	counts := make(map[int]durableCounts, 2)
	for _, actionCount := range []int{1, 500} {
		t.Run(fmt.Sprintf("%d-actions", actionCount), func(t *testing.T) {
			fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
			service := newTestService(t, pool)
			for index, event := range projectedBrowserRunEvents(actionCount) {
				ack, err := service.AppendRuntimeEvent(
					context.Background(),
					fixture.principal,
					fixture.identity,
					runtime.RuntimeEventRequest{
						ClientEventID:  uuid.New(),
						ClientEventSeq: int64(index + 1),
						EventType:      event.eventType,
						Payload:        event.payload,
					},
				)
				require.NoError(t, err)
				require.True(t, ack.Inserted)
			}

			var observed durableCounts
			require.NoError(t, pool.QueryRow(context.Background(), `
				SELECT
					(SELECT COUNT(*)::int FROM run_events WHERE run_id = $1),
					(SELECT COUNT(*)::int FROM run_events
					 WHERE run_id = $1 AND event_type = 'run.browser.lifecycle'),
					(SELECT COUNT(*)::int FROM run_messages WHERE run_id = $1),
					(SELECT COUNT(*)::int FROM run_artifacts WHERE run_id = $1),
					(SELECT COUNT(*)::int FROM run_artifact_chunks WHERE run_id = $1),
					(SELECT COUNT(*)::int FROM run_events
					 WHERE run_id = $1
					   AND (payload ? 'action'
					        OR payload ? 'action_count'
					        OR payload ? 'frame'
					        OR payload ? 'screenshot'
					        OR payload ? 'url'))`,
				fixture.identity.RunID,
			).Scan(
				&observed.runEvents,
				&observed.browserEvents,
				&observed.runMessages,
				&observed.runArtifacts,
				&observed.artifactChunks,
				&observed.actionPayloads,
			))
			require.LessOrEqual(t, observed.browserEvents, 8)
			require.Zero(t, observed.runMessages)
			require.Zero(t, observed.runArtifacts)
			require.Zero(t, observed.artifactChunks)
			require.Zero(t, observed.actionPayloads)
			counts[actionCount] = observed
		})
	}
	require.Equal(t, counts[1], counts[500])
}

type projectedBrowserRunEvent struct {
	eventType string
	payload   map[string]any
}

func projectedBrowserRunEvents(actionCount int) []projectedBrowserRunEvent {
	if actionCount < 1 {
		panic("Browser action count must be positive")
	}
	// Browser action/frame progress is intentionally absent. Only coarse
	// lifecycle plus non-Browser Provider progress crosses the Runtime event
	// boundary, independent of the number of browser_session invocations.
	return []projectedBrowserRunEvent{
		{
			eventType: "run.browser.lifecycle",
			payload: map[string]any{
				"phase":             "preparing",
				"execution_profile": "browser",
				"runtime":           "isolated",
			},
		},
		{
			eventType: "run.browser.lifecycle",
			payload: map[string]any{
				"phase":             "ready",
				"execution_profile": "browser",
				"runtime":           "isolated",
			},
		},
		{
			eventType: "run.status.changed",
			payload: map[string]any{
				"status":    "provider_tool_started",
				"provider":  "codex",
				"phase":     "started",
				"tool_kind": "command",
			},
		},
		{
			eventType: "run.status.changed",
			payload: map[string]any{
				"status":    "provider_tool_completed",
				"provider":  "codex",
				"phase":     "completed",
				"tool_kind": "command",
			},
		},
		{
			eventType: "run.status.changed",
			payload: map[string]any{
				"status":    "provider_tool_completed",
				"provider":  "codex",
				"phase":     "completed",
				"tool_kind": "mcp_tool",
			},
		},
		{
			eventType: "run.browser.lifecycle",
			payload: map[string]any{
				"phase":             "closed",
				"status":            "success",
				"execution_profile": "browser",
				"runtime":           "isolated",
			},
		},
	}
}
