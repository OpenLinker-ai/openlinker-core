package runtime_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

type browserViewerCommandCapture struct {
	mu       sync.Mutex
	commands []runtime.BrowserViewerCommandPayload
}

func (capture *browserViewerCommandCapture) SendBrowserViewerCommand(
	_ uuid.UUID,
	command runtime.BrowserViewerCommandPayload,
) error {
	capture.mu.Lock()
	capture.commands = append(capture.commands, command)
	capture.mu.Unlock()
	return nil
}

func TestBrowserHumanControlPersistsOnlyLifecycleAndAggregateAudit(t *testing.T) {
	pool := setupTestDB(t)
	requireReliableRuntimeSchema(t, pool)
	fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
	service := newTestService(t, pool)
	control := service.BrowserHumanControl()
	capture := &browserViewerCommandCapture{}
	control.BindCommandSender(capture)

	browserSessionID := uuid.New()
	attachmentID := uuid.New()
	ack, err := service.AppendRuntimeEvent(
		context.Background(),
		fixture.principal,
		fixture.identity,
		runtime.RuntimeEventRequest{
			ClientEventID:  uuid.New(),
			ClientEventSeq: 1,
			EventType:      "run.browser.lifecycle",
			Payload: map[string]any{
				"phase":              "paused",
				"reason":             "user_action_required",
				"execution_profile":  "browser",
				"runtime":            "isolated",
				"browser_session_id": browserSessionID.String(),
				"session_epoch":      1,
				"attachment_id":      attachmentID.String(),
				"control_epoch":      2,
				"controller":         "none",
			},
		},
	)
	require.NoError(t, err)
	require.True(t, ack.Inserted)

	var ownerID uuid.UUID
	require.NoError(t, pool.QueryRow(
		context.Background(),
		`SELECT user_id FROM runs WHERE id = $1`,
		fixture.identity.RunID,
	).Scan(&ownerID))

	paused, err := control.State(
		context.Background(),
		ownerID,
		fixture.identity.RunID,
	)
	require.NoError(t, err)
	require.Equal(t, "paused", paused.State)
	require.Equal(t, "none", paused.Controller)
	require.Equal(t, uint64(2), paused.ControlEpoch)
	require.WithinDuration(
		t,
		time.Now().Add(10*time.Minute),
		paused.PauseExpiresAt,
		10*time.Second,
	)

	_, err = control.Claim(
		context.Background(),
		uuid.New(),
		fixture.identity.RunID,
	)
	require.Error(t, err, "a non-Owner claim must not resolve the control row")

	human, err := control.Claim(
		context.Background(),
		ownerID,
		fixture.identity.RunID,
	)
	require.NoError(t, err)
	require.Equal(t, "human", human.State)
	require.Equal(t, uint64(3), human.ControlEpoch)
	_, err = control.Claim(
		context.Background(),
		ownerID,
		fixture.identity.RunID,
	)
	require.Error(t, err, "a second concurrent claim must be rejected")

	var durableBefore int
	require.NoError(t, pool.QueryRow(context.Background(), `
SELECT (SELECT count(*) FROM run_events WHERE run_id = $1)
     + (SELECT count(*) FROM browser_run_controls WHERE run_id = $1)
     + (SELECT count(*) FROM browser_human_control_audits WHERE run_id = $1)
`, fixture.identity.RunID).Scan(&durableBefore))

	attemptIdentity := runtime.AttemptIdentity{
		RunID:            fixture.identity.RunID,
		AttemptID:        fixture.identity.AttemptID,
		LeaseID:          fixture.identity.LeaseID,
		FencingToken:     fixture.identity.FencingToken,
		NodeID:           *fixture.identity.NodeID,
		AgentID:          fixture.identity.AgentID,
		WorkerID:         *fixture.identity.WorkerID,
		RuntimeSessionID: *fixture.identity.RuntimeSessionID,
	}
	for sequence := 1; sequence <= 25; sequence++ {
		x, y := sequence, sequence
		require.NoError(t, control.Input(
			context.Background(),
			ownerID,
			fixture.identity.RunID,
			runtime.BrowserViewerInputPayload{
				Kind:          "pointer",
				PointerAction: "move",
				X:             &x,
				Y:             &y,
			},
		))
		require.NoError(t, control.PublishFrame(
			runtime.BrowserViewerFramePayload{
				AttemptIdentity:  attemptIdentity,
				BrowserSessionID: browserSessionID,
				SessionEpoch:     1,
				AttachmentID:     attachmentID,
				ControlEpoch:     human.ControlEpoch,
				FrameSeq:         uint64(sequence),
				MIMEType:         "image/jpeg",
				Data:             []byte{0xff, 0xd8, 0xff, 0xd9},
				Width:            1280,
				Height:           720,
			},
		))
	}

	var durableAfter int
	require.NoError(t, pool.QueryRow(context.Background(), `
SELECT (SELECT count(*) FROM run_events WHERE run_id = $1)
     + (SELECT count(*) FROM browser_run_controls WHERE run_id = $1)
     + (SELECT count(*) FROM browser_human_control_audits WHERE run_id = $1)
`, fixture.identity.RunID).Scan(&durableAfter))
	require.Equal(t, durableBefore, durableAfter)

	released, err := control.Release(
		context.Background(),
		ownerID,
		fixture.identity.RunID,
	)
	require.NoError(t, err)
	require.Equal(t, "released", released.State)
	require.Equal(t, uint64(4), released.ControlEpoch)

	var inputCount, frameCount, durationMS int64
	var auditBrowserSessionID, auditAttachmentID uuid.UUID
	var auditSessionEpoch int64
	var controller, pauseReason string
	require.NoError(t, pool.QueryRow(context.Background(), `
SELECT browser_session_id, session_epoch, attachment_id, controller,
       pause_reason, duration_ms, input_count, frame_count
FROM browser_human_control_audits
WHERE run_id = $1
`, fixture.identity.RunID).Scan(
		&auditBrowserSessionID,
		&auditSessionEpoch,
		&auditAttachmentID,
		&controller,
		&pauseReason,
		&durationMS,
		&inputCount,
		&frameCount,
	))
	require.Equal(t, browserSessionID, auditBrowserSessionID)
	require.Equal(t, int64(1), auditSessionEpoch)
	require.Equal(t, attachmentID, auditAttachmentID)
	require.Equal(t, "human", controller)
	require.Equal(t, "user_action_required", pauseReason)
	require.GreaterOrEqual(t, durationMS, int64(0))
	require.Equal(t, int64(25), inputCount)
	require.Equal(t, int64(25), frameCount)

	resumed, err := control.Resume(
		context.Background(),
		ownerID,
		fixture.identity.RunID,
	)
	require.NoError(t, err)
	require.Equal(t, "resumed", resumed.State)
	require.Equal(t, uint64(5), resumed.ControlEpoch)

	capture.mu.Lock()
	defer capture.mu.Unlock()
	require.Len(t, capture.commands, 28)
	require.Equal(t, runtime.BrowserViewerActionClaim, capture.commands[0].Action)
	require.Equal(t, runtime.BrowserViewerActionRelease, capture.commands[26].Action)
	require.Equal(t, runtime.BrowserViewerActionResume, capture.commands[27].Action)
}
