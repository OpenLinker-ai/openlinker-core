package runtime_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

// observerCommandCapture records the commands Core pushes and answers a start
// with the lifecycle event the Worker would send. Start blocks on that
// handshake, so a capture that stays silent is what a stalled Worker looks like.
type observerCommandCapture struct {
	mu          sync.Mutex
	commands    []runtime.BrowserObserverCommandPayload
	observation *runtime.BrowserObservation
	reply       runtime.BrowserObserverEventKind
	errorCode   string
	sendErr     error
}

func (capture *observerCommandCapture) SendBrowserObserverCommand(
	_ uuid.UUID,
	command runtime.BrowserObserverCommandPayload,
) error {
	capture.mu.Lock()
	capture.commands = append(capture.commands, command)
	reply := capture.reply
	errorCode := capture.errorCode
	sendErr := capture.sendErr
	observation := capture.observation
	capture.mu.Unlock()
	if sendErr != nil {
		return sendErr
	}
	if command.Action != runtime.BrowserObserverStart || reply == "" {
		return nil
	}
	go func() {
		_, _ = observation.HandleEvent(
			context.Background(),
			runtime.BrowserObserverEventPayload{
				AttemptIdentity: command.AttemptIdentity,
				CommandID:       command.CommandID,
				LeaseID:         command.LeaseID,
				EventSeq:        1,
				Kind:            reply,
				ErrorCode:       errorCode,
			},
		)
	}()
	return nil
}

func (capture *observerCommandCapture) actions() []runtime.BrowserObserverAction {
	capture.mu.Lock()
	defer capture.mu.Unlock()
	actions := make([]runtime.BrowserObserverAction, 0, len(capture.commands))
	for _, command := range capture.commands {
		actions = append(actions, command.Action)
	}
	return actions
}

func observationDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

// appendBrowserLifecycle replays deterministically: the same sequence produces
// the same client event id, which is what makes a second call a replay rather
// than a new event.
func appendBrowserLifecycle(
	t *testing.T,
	service *runtime.Service,
	fixture eventStoreFixture,
	sequence int64,
	payload map[string]any,
) runtime.RuntimeEventAck {
	t.Helper()
	ack, err := service.AppendRuntimeEvent(
		context.Background(),
		fixture.principal,
		fixture.identity,
		runtime.RuntimeEventRequest{
			ClientEventID: uuid.NewSHA1(
				uuid.Nil,
				fmt.Appendf(nil, "%s/%d", fixture.identity.AttemptID, sequence),
			),
			ClientEventSeq: sequence,
			EventType:      "run.browser.lifecycle",
			Payload:        payload,
		},
	)
	require.NoError(t, err)
	return ack
}

func browserReadyPayload(epoch int64, session, attachment string) map[string]any {
	return map[string]any{
		"phase":                     "ready",
		"execution_profile":         "browser",
		"runtime":                   "isolated",
		"browser_session_sha256":    observationDigest(session),
		"browser_session_epoch":     epoch,
		"browser_attachment_sha256": observationDigest(attachment),
	}
}

func observationFixture(t *testing.T) (
	*pgxpool.Pool,
	*runtime.Service,
	eventStoreFixture,
	*observerCommandCapture,
	uuid.UUID,
) {
	t.Helper()
	pool := setupTestDB(t)
	requireReliableRuntimeSchema(t, pool)
	// Declared at Session creation: the feature list is immutable by trigger,
	// which is also why observability is read from the live Session row on every
	// request rather than cached.
	fixture := insertEventStoreExecutingAttempt(
		t, pool, 5*time.Minute, runtime.BrowserObservationFeature,
	)

	service := newTestService(t, pool)
	observation := service.BrowserObservation()
	capture := &observerCommandCapture{
		observation: observation,
		reply:       runtime.BrowserObserverStarted,
	}
	observation.BindCommandSender(capture)

	var ownerID uuid.UUID
	require.NoError(t, pool.QueryRow(
		context.Background(),
		`SELECT user_id FROM runs WHERE id = $1`,
		fixture.identity.RunID,
	).Scan(&ownerID))
	return pool, service, fixture, capture, ownerID
}

// The ready lifecycle event is the only thing that makes a Run observable, and
// the identity it projects is hashed. A raw-UUID read here is what silently kept
// the projection empty before.
func TestBrowserObservationProjectsHashedIdentityFromReady(t *testing.T) {
	_, service, fixture, _, ownerID := observationFixture(t)
	observation := service.BrowserObservation()

	_, err := observation.ResolveIdentity(
		context.Background(), fixture.identity.RunID, ownerID, false,
	)
	require.Error(t, err, "a Run with no ready event must not be observable")

	appendBrowserLifecycle(t, service, fixture, 1, browserReadyPayload(3, "session-a", "attachment-a"))

	identity, err := observation.ResolveIdentity(
		context.Background(), fixture.identity.RunID, ownerID, false,
	)
	require.NoError(t, err)
	require.Equal(t, fixture.identity.AttemptID, identity.AttemptID)
	require.Equal(t, int64(3), identity.SessionEpoch)
	require.Equal(t, observationDigest("session-a"), identity.BrowserSessionSHA256)
	require.Equal(t, observationDigest("attachment-a"), identity.AttachmentSHA256)
	require.Equal(t, *fixture.identity.RuntimeSessionID, identity.RuntimeSessionID)

	_, err = observation.ResolveIdentity(
		context.Background(), fixture.identity.RunID, uuid.New(), false,
	)
	require.ErrorIs(t, err, runtime.ErrObservationForbidden)
}

// A projection that failed after its event was appended is only ever retried as
// a replay. If replays were skipped the Run would stay unobservable forever.
func TestBrowserObservationProjectionSurvivesReplay(t *testing.T) {
	pool, service, fixture, _, ownerID := observationFixture(t)
	observation := service.BrowserObservation()

	ack := appendBrowserLifecycle(t, service, fixture, 1, browserReadyPayload(3, "session-a", "attachment-a"))
	require.True(t, ack.Inserted)

	// Stands in for a projection that was lost after the event was durable.
	_, err := pool.Exec(
		context.Background(),
		`DELETE FROM browser_observable_attempts WHERE run_id = $1`,
		fixture.identity.RunID,
	)
	require.NoError(t, err)

	replay := appendBrowserLifecycle(t, service, fixture, 1, browserReadyPayload(3, "session-a", "attachment-a"))
	require.False(t, replay.Inserted, "the replay must not append a second event")

	identity, err := observation.ResolveIdentity(
		context.Background(), fixture.identity.RunID, ownerID, false,
	)
	require.NoError(t, err, "the replay must restore the projection")
	require.Equal(t, observationDigest("session-a"), identity.BrowserSessionSHA256)
}

// The full round trip: start blocks until the Worker confirms, a frame becomes
// exactly one acked frame the viewer can read, and stop closes the audit once.
func TestBrowserObservationStartFrameStopRoundTrip(t *testing.T) {
	pool, service, fixture, capture, ownerID := observationFixture(t)
	observation := service.BrowserObservation()
	appendBrowserLifecycle(t, service, fixture, 1, browserReadyPayload(3, "session-a", "attachment-a"))

	identity, err := observation.ResolveIdentity(
		context.Background(), fixture.identity.RunID, ownerID, false,
	)
	require.NoError(t, err)

	state, err := observation.Start(
		context.Background(), fixture.identity.RunID, ownerID, false, "", identity,
	)
	require.NoError(t, err)
	require.True(t, state.Active)

	_, err = observation.Start(
		context.Background(), fixture.identity.RunID, ownerID, false, "", identity,
	)
	require.ErrorIs(t, err, runtime.ErrObservationAlreadyActive)

	captured := time.Now().UTC()
	commandID := observationStartCommandID(t, pool, state.LeaseID)
	ack, err := observation.HandleEvent(context.Background(), runtime.BrowserObserverEventPayload{
		AttemptIdentity: identity,
		CommandID:       commandID,
		LeaseID:         state.LeaseID,
		EventSeq:        2,
		Kind:            runtime.BrowserObserverFrame,
		CapturedAt:      &captured,
		Frame: &runtime.BrowserObserverFramePayload{
			MIMEType: "image/jpeg",
			Data:     []byte{0xff, 0xd8, 0xff, 0xd9},
			Width:    1280,
			Height:   720,
		},
	})
	require.NoError(t, err)
	require.Equal(t, state.LeaseID, ack.LeaseID)
	require.Equal(t, int64(2), ack.EventSeq)
	require.Equal(t, identity, ack.AttemptIdentity)

	frame, err := observation.WaitFrame(context.Background(), fixture.identity.RunID, 0)
	require.NoError(t, err)
	require.NotNil(t, frame)
	require.Equal(t, int64(2), frame.FrameSeq)
	require.Equal(t, "image/jpeg", frame.MIMEType)

	// A frame from a superseded lease belongs to an observation that has ended.
	_, err = observation.HandleEvent(context.Background(), runtime.BrowserObserverEventPayload{
		AttemptIdentity: identity,
		CommandID:       commandID,
		LeaseID:         uuid.New(),
		EventSeq:        3,
		Kind:            runtime.BrowserObserverFrame,
		CapturedAt:      &captured,
		Frame: &runtime.BrowserObserverFramePayload{
			MIMEType: "image/jpeg",
			Data:     []byte{0xff, 0xd8, 0xff, 0xd9},
			Width:    1280,
			Height:   720,
		},
	})
	require.Error(t, err)

	// And a frame naming the right lease but a superseded command is refused:
	// the lease alone cannot tell the two apart.
	_, err = observation.HandleEvent(context.Background(), runtime.BrowserObserverEventPayload{
		AttemptIdentity: identity,
		CommandID:       uuid.New(),
		LeaseID:         state.LeaseID,
		EventSeq:        4,
		Kind:            runtime.BrowserObserverFrame,
		CapturedAt:      &captured,
		Frame: &runtime.BrowserObserverFramePayload{
			MIMEType: "image/jpeg",
			Data:     []byte{0xff, 0xd8, 0xff, 0xd9},
			Width:    1280,
			Height:   720,
		},
	})
	require.Error(t, err)

	require.NoError(t, observation.Stop(
		context.Background(), fixture.identity.RunID, "viewer_stopped",
	))
	require.Contains(t, capture.actions(), runtime.BrowserObserverStop)

	var status, endReason string
	var frameCount int64
	var complete bool
	require.NoError(t, pool.QueryRow(context.Background(), `
SELECT status, end_reason, frame_count, frame_count_complete
FROM browser_observation_audits WHERE lease_id = $1
`, state.LeaseID).Scan(&status, &endReason, &frameCount, &complete))
	require.Equal(t, "closed", status)
	require.Equal(t, "viewer_stopped", endReason)
	require.Equal(t, int64(1), frameCount)
	require.True(t, complete)

	// Stop is called from every teardown path, so a second one must be a no-op.
	require.NoError(t, observation.Stop(
		context.Background(), fixture.identity.RunID, "viewer_stopped",
	))
}

// A Worker that refuses the start must not leave an active audit behind: the
// unique index would then block the Run from ever being observed again.
func TestBrowserObservationUnconfirmedStartClosesItsAudit(t *testing.T) {
	pool, service, fixture, capture, ownerID := observationFixture(t)
	observation := service.BrowserObservation()
	capture.reply = runtime.BrowserObserverError
	capture.errorCode = "browser_unavailable"
	appendBrowserLifecycle(t, service, fixture, 1, browserReadyPayload(3, "session-a", "attachment-a"))

	identity, err := observation.ResolveIdentity(
		context.Background(), fixture.identity.RunID, ownerID, false,
	)
	require.NoError(t, err)

	_, err = observation.Start(
		context.Background(), fixture.identity.RunID, ownerID, false, "", identity,
	)
	require.ErrorIs(t, err, runtime.ErrObservationNotConfirmed)

	var active int
	require.NoError(t, pool.QueryRow(context.Background(), `
SELECT count(*) FROM browser_observation_audits
WHERE run_id = $1 AND status = 'active'
`, fixture.identity.RunID).Scan(&active))
	require.Zero(t, active, "an unconfirmed start must not leave an active audit")

	// And the Run stays observable, which is the point of closing it.
	capture.reply = runtime.BrowserObserverStarted
	capture.errorCode = ""
	_, err = observation.Start(
		context.Background(), fixture.identity.RunID, ownerID, false, "", identity,
	)
	require.NoError(t, err)
}

// The Browser lifecycle close path must close the audit from the Run and Attempt
// alone. Closing through the projection cannot work, because the same path
// deletes it.
func TestBrowserObservationClosedLifecycleClosesTheAudit(t *testing.T) {
	pool, service, fixture, _, ownerID := observationFixture(t)
	observation := service.BrowserObservation()
	appendBrowserLifecycle(t, service, fixture, 1, browserReadyPayload(3, "session-a", "attachment-a"))

	identity, err := observation.ResolveIdentity(
		context.Background(), fixture.identity.RunID, ownerID, false,
	)
	require.NoError(t, err)
	state, err := observation.Start(
		context.Background(), fixture.identity.RunID, ownerID, false, "", identity,
	)
	require.NoError(t, err)

	appendBrowserLifecycle(t, service, fixture, 2, map[string]any{
		"phase":             "closed",
		"execution_profile": "browser",
		"runtime":           "isolated",
	})

	require.Eventually(t, func() bool {
		var status string
		if err := pool.QueryRow(context.Background(), `
SELECT status FROM browser_observation_audits WHERE lease_id = $1
`, state.LeaseID).Scan(&status); err != nil {
			return false
		}
		return status == "closed"
	}, 5*time.Second, 50*time.Millisecond, "the lifecycle close must close the audit")

	var projections int
	require.NoError(t, pool.QueryRow(context.Background(), `
SELECT count(*) FROM browser_observable_attempts WHERE run_id = $1
`, fixture.identity.RunID).Scan(&projections))
	require.Zero(t, projections)
}

// An expired lease has to release this instance's memory as well as the audit
// row. A SQL-only reconcile leaves the frame buffer open: its poller is never
// woken and its quota slot is never returned.
func TestBrowserObservationReconcileReleasesInProcessState(t *testing.T) {
	pool, service, fixture, _, ownerID := observationFixture(t)
	observation := service.BrowserObservation()
	appendBrowserLifecycle(t, service, fixture, 1, browserReadyPayload(3, "session-a", "attachment-a"))

	identity, err := observation.ResolveIdentity(
		context.Background(), fixture.identity.RunID, ownerID, false,
	)
	require.NoError(t, err)
	state, err := observation.Start(
		context.Background(), fixture.identity.RunID, ownerID, false, "", identity,
	)
	require.NoError(t, err)

	// Expire the lease the way time would.
	_, err = pool.Exec(context.Background(), `
UPDATE browser_observation_audits
SET lease_expires_at = clock_timestamp() - interval '1 minute'
WHERE lease_id = $1
`, state.LeaseID)
	require.NoError(t, err)

	waited := make(chan error, 1)
	go func() {
		_, frameErr := observation.WaitFrame(context.Background(), fixture.identity.RunID, 0)
		waited <- frameErr
	}()

	require.NoError(t, observation.ReconcileExpired(context.Background()))

	select {
	case frameErr := <-waited:
		require.ErrorIs(t, frameErr, runtime.ErrObservationInactive)
	case <-time.After(5 * time.Second):
		t.Fatal("the reconcile left a poller waiting on a closed observation")
	}

	// And the Run is observable again rather than holding a slot forever.
	_, err = observation.Start(
		context.Background(), fixture.identity.RunID, ownerID, false, "", identity,
	)
	require.NoError(t, err)
}

// An observation belongs to the Core instance holding the Worker socket. Another
// instance must refuse instead of reporting a live observation whose frames only
// exist in a different process.
func TestBrowserObservationFailsClosedOnAnotherInstance(t *testing.T) {
	pool, service, fixture, _, ownerID := observationFixture(t)
	observation := service.BrowserObservation()
	appendBrowserLifecycle(t, service, fixture, 1, browserReadyPayload(3, "session-a", "attachment-a"))

	identity, err := observation.ResolveIdentity(
		context.Background(), fixture.identity.RunID, ownerID, false,
	)
	require.NoError(t, err)
	state, err := observation.Start(
		context.Background(), fixture.identity.RunID, ownerID, false, "", identity,
	)
	require.NoError(t, err)

	live, err := observation.State(context.Background(), fixture.identity.RunID)
	require.NoError(t, err)
	require.True(t, live.Active)

	// Stands in for the audit having been opened by a different Core process.
	_, err = pool.Exec(context.Background(), `
UPDATE browser_observation_audits SET core_instance_id = $2 WHERE lease_id = $1
`, state.LeaseID, uuid.New())
	require.NoError(t, err)

	_, err = observation.State(context.Background(), fixture.identity.RunID)
	require.ErrorIs(t, err, runtime.ErrObservationChannelUnavailable)
	require.ErrorIs(
		t,
		observation.Stop(context.Background(), fixture.identity.RunID, "viewer_stopped"),
		runtime.ErrObservationChannelUnavailable,
	)
}

// A poll for an observation held by another Core instance must say so. Reporting
// it as ended would send the viewer back to state, which would start the same
// unservable observation again.
func TestBrowserObservationFramePathFailsClosedOnAnotherInstance(t *testing.T) {
	pool, service, fixture, _, ownerID := observationFixture(t)
	observation := service.BrowserObservation()
	appendBrowserLifecycle(t, service, fixture, 1, browserReadyPayload(3, "session-a", "attachment-a"))

	identity, err := observation.ResolveIdentity(
		context.Background(), fixture.identity.RunID, ownerID, false,
	)
	require.NoError(t, err)
	state, err := observation.Start(
		context.Background(), fixture.identity.RunID, ownerID, false, "", identity,
	)
	require.NoError(t, err)

	// The audit stays active and moves to another instance, and the local buffer
	// goes away the way a restart would take it.
	_, err = pool.Exec(context.Background(), `
UPDATE browser_observation_audits SET core_instance_id = $2 WHERE lease_id = $1
`, state.LeaseID, uuid.New())
	require.NoError(t, err)
	service.BrowserObservation().DropLocalFramesForTest(fixture.identity.RunID)

	_, err = observation.WaitFrame(context.Background(), fixture.identity.RunID, 0)
	require.ErrorIs(t, err, runtime.ErrObservationChannelUnavailable)

	// With no active audit at all the same absence means the observation ended.
	_, err = pool.Exec(context.Background(), `
UPDATE browser_observation_audits
SET status = 'closed', ended_at = clock_timestamp(), end_reason = 'test'
WHERE lease_id = $1
`, state.LeaseID)
	require.NoError(t, err)
	_, err = observation.WaitFrame(context.Background(), fixture.identity.RunID, 0)
	require.ErrorIs(t, err, runtime.ErrObservationInactive)
}

// The reconciler is the one path that knows the exact total, and it runs after
// the audit is already closed. Writing the count through the active-only update
// dropped it there.
func TestBrowserObservationReconcileWritesTheFinalCount(t *testing.T) {
	pool, service, fixture, _, ownerID := observationFixture(t)
	observation := service.BrowserObservation()
	appendBrowserLifecycle(t, service, fixture, 1, browserReadyPayload(3, "session-a", "attachment-a"))

	identity, err := observation.ResolveIdentity(
		context.Background(), fixture.identity.RunID, ownerID, false,
	)
	require.NoError(t, err)
	state, err := observation.Start(
		context.Background(), fixture.identity.RunID, ownerID, false, "", identity,
	)
	require.NoError(t, err)

	captured := time.Now().UTC()
	for sequence := int64(2); sequence <= 4; sequence++ {
		_, err = observation.HandleEvent(context.Background(), runtime.BrowserObserverEventPayload{
			AttemptIdentity: identity,
			CommandID:       observationStartCommandID(t, pool, state.LeaseID),
			LeaseID:         state.LeaseID,
			EventSeq:        sequence,
			Kind:            runtime.BrowserObserverFrame,
			CapturedAt:      &captured,
			Frame: &runtime.BrowserObserverFramePayload{
				MIMEType: "image/jpeg",
				Data:     []byte{0xff, 0xd8, 0xff, 0xd9},
				Width:    1280,
				Height:   720,
			},
		})
		require.NoError(t, err)
	}

	_, err = pool.Exec(context.Background(), `
UPDATE browser_observation_audits
SET lease_expires_at = clock_timestamp() - interval '1 minute'
WHERE lease_id = $1
`, state.LeaseID)
	require.NoError(t, err)
	require.NoError(t, observation.ReconcileExpired(context.Background()))

	var count int64
	var complete bool
	var status string
	require.NoError(t, pool.QueryRow(context.Background(), `
SELECT frame_count, frame_count_complete, status
FROM browser_observation_audits WHERE lease_id = $1
`, state.LeaseID).Scan(&count, &complete, &status))
	require.Equal(t, "closed", status)
	require.Equal(t, int64(3), count, "the reconciled audit must carry the frames it served")
	require.True(t, complete)
}

func observationStartCommandID(t *testing.T, pool *pgxpool.Pool, leaseID uuid.UUID) uuid.UUID {
	t.Helper()
	var commandID uuid.UUID
	require.NoError(t, pool.QueryRow(
		context.Background(),
		`SELECT command_id FROM browser_observation_audits WHERE lease_id = $1`,
		leaseID,
	).Scan(&commandID))
	return commandID
}
