package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrObservationChannelUnavailable is returned when this Core process does not
// hold the Worker's WebSocket. Frames and wakeups live in process memory, so an
// observation started here could never deliver anything. Failing closed with a
// distinguishable error keeps that from presenting as "connected but no frames
// ever arrive", which is the shape a multi-instance deployment would otherwise
// produce.
var ErrObservationChannelUnavailable = errors.New("browser observation channel is unavailable on this Core instance")

// ErrObservationAlreadyActive is returned when a Run is already being observed.
// The Runtime lease is singular, so a second observation cannot exist.
var ErrObservationAlreadyActive = errors.New("browser observation is already active for this Run")

const (
	observationDefaultFrameIntervalMS = 500
	observationDefaultTTL             = 10 * time.Minute
	observationMaxTTL                 = 30 * time.Minute
	// Frame counts are persisted on this cadence so a crashed Core still leaves a
	// lower bound behind rather than a zero.
	observationCountFlushInterval = 5 * time.Second
)

type BrowserObservationState struct {
	RunID              uuid.UUID  `json:"run_id"`
	Active             bool       `json:"active"`
	LeaseID            uuid.UUID  `json:"lease_id,omitempty"`
	LeaseExpiresAt     *time.Time `json:"lease_expires_at,omitempty"`
	FrameCount         int64      `json:"frame_count"`
	FrameCountComplete bool       `json:"frame_count_complete"`
}

type BrowserObservation struct {
	pool     *pgxpool.Pool
	now      func() time.Time
	sender   browserObserverCommandSender
	instance uuid.UUID
	frames   *observationFrameBuffer
}

type browserObserverCommandSender interface {
	SendBrowserObserverCommand(uuid.UUID, BrowserObserverCommandPayload) error
}

func NewBrowserObservation(
	pool *pgxpool.Pool,
	now func() time.Time,
	instance uuid.UUID,
) *BrowserObservation {
	if now == nil {
		now = time.Now
	}
	return &BrowserObservation{
		pool:     pool,
		now:      now,
		instance: instance,
		frames:   newObservationFrameBuffer(),
	}
}

func (observation *BrowserObservation) BindCommandSender(
	sender browserObserverCommandSender,
) {
	if observation == nil {
		return
	}
	observation.sender = sender
}

// Start opens an observation. The channel check happens before the audit record
// is written: creating an active record for an observation that can never
// deliver a frame would leave a dangling row that reconciliation later has to
// explain away.
func (observation *BrowserObservation) Start(
	ctx context.Context,
	runID uuid.UUID,
	observerUserID uuid.UUID,
	isAdmin bool,
	reason string,
	identity BrowserObserverIdentity,
) (BrowserObservationState, error) {
	if observation == nil || observation.pool == nil || observation.sender == nil {
		return BrowserObservationState{}, ErrObservationChannelUnavailable
	}
	if isAdmin && reason == "" {
		return BrowserObservationState{}, errors.New("cross-user browser observation requires a reason")
	}
	now := observation.now().UTC()
	leaseID := uuid.New()
	expiresAt := now.Add(observationDefaultTTL)
	command := BrowserObserverCommandPayload{
		AttemptIdentity: identity,
		CommandID:       uuid.New(),
		Action:          BrowserObserverStart,
		LeaseID:         leaseID,
		LeaseExpiresAt:  expiresAt,
		DeadlineAt:      now.Add(observationMaxTTL),
		FrameIntervalMS: observationDefaultFrameIntervalMS,
	}
	if err := command.Validate(); err != nil {
		return BrowserObservationState{}, err
	}

	var auditID uuid.UUID
	err := observation.pool.QueryRow(ctx, `
INSERT INTO browser_observation_audits (
    run_id, attempt_id, observer_user_id, observer_is_admin, reason,
    session_epoch, attachment_sha256, lease_id, lease_expires_at,
    status, started_at, frame_count, frame_count_complete
) VALUES ($1,$2,$3,$4,NULLIF($5,''),$6,$7,$8,$9,'active',$10,0,false)
RETURNING id
`,
		runID, identity.AttemptID, observerUserID, isAdmin, reason,
		identity.SessionEpoch, identity.AttachmentSHA256, leaseID, expiresAt, now,
	).Scan(&auditID)
	if err != nil {
		// The partial unique index makes a second active row impossible, so a
		// conflict here means someone is already observing this Run.
		return BrowserObservationState{}, ErrObservationAlreadyActive
	}

	observation.frames.open(runID, leaseID)
	if err := observation.sender.SendBrowserObserverCommand(
		identity.RuntimeSessionID,
		command,
	); err != nil {
		observation.frames.close(runID)
		observation.close(ctx, leaseID, "channel_unavailable")
		return BrowserObservationState{}, ErrObservationChannelUnavailable
	}
	return BrowserObservationState{
		RunID:          runID,
		Active:         true,
		LeaseID:        leaseID,
		LeaseExpiresAt: &expiresAt,
	}, nil
}

// Stop ends an observation and closes its audit record. It is safe to call for
// an observation that has already ended, because every teardown path -- explicit
// stop, Run terminal, disconnect, TTL -- has to be able to call it.
func (observation *BrowserObservation) Stop(
	ctx context.Context,
	runID uuid.UUID,
	endReason string,
) error {
	if observation == nil || observation.pool == nil {
		return nil
	}
	var leaseID uuid.UUID
	var sessionID uuid.UUID
	var attemptID uuid.UUID
	var epoch int64
	var sessionDigest string
	var attachmentDigest string
	// Joined on the Attempt as well as the Run. Joining on run_id alone could
	// pair an old audit with a Runtime Session that has since been replaced, and
	// the stop would be addressed to the wrong Session.
	err := observation.pool.QueryRow(ctx, `
SELECT a.lease_id, a.attempt_id, a.session_epoch,
       a.attachment_sha256, c.browser_session_sha256, c.runtime_session_id
FROM browser_observation_audits a
JOIN browser_observable_attempts c
  ON c.run_id = a.run_id AND c.attempt_id = a.attempt_id
WHERE a.run_id = $1 AND a.status = 'active'
`, runID).Scan(&leaseID, &attemptID, &epoch, &attachmentDigest, &sessionDigest, &sessionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if observation.sender != nil && sessionID != uuid.Nil {
		_ = observation.sender.SendBrowserObserverCommand(sessionID, BrowserObserverCommandPayload{
			AttemptIdentity: BrowserObserverIdentity{
				RunID:                runID,
				AttemptID:            attemptID,
				SessionEpoch:         epoch,
				BrowserSessionSHA256: sessionDigest,
				AttachmentSHA256:     attachmentDigest,
				RuntimeSessionID:     sessionID,
			},
			CommandID: uuid.New(),
			Action:    BrowserObserverStop,
			LeaseID:   leaseID,
		})
	}
	count := observation.frames.close(runID)
	_ = observation.RecordFrames(ctx, leaseID, count)
	return observation.close(ctx, leaseID, endReason)
}

func (observation *BrowserObservation) close(
	ctx context.Context,
	leaseID uuid.UUID,
	endReason string,
) error {
	// A normal close knows the exact total, so the count stops being a lower
	// bound here and only here.
	_, err := observation.pool.Exec(ctx, `
UPDATE browser_observation_audits
SET status = 'closed',
    ended_at = $2,
    end_reason = $3,
    frame_count = GREATEST(frame_count, $4),
    frame_count_complete = true,
    updated_at = clock_timestamp()
WHERE lease_id = $1 AND status = 'active'
`, leaseID, observation.now().UTC(), endReason, observation.finalFrameCount(leaseID))
	return err
}

func (observation *BrowserObservation) finalFrameCount(leaseID uuid.UUID) int64 {
	observation.frames.mu.Lock()
	defer observation.frames.mu.Unlock()
	for _, live := range observation.frames.live {
		if live.leaseID == leaseID {
			return live.count
		}
	}
	return 0
}

// RecordFrames persists a lower bound for the frame count while an observation
// is running. It is deliberately not called per frame: the point is that a Core
// which exits mid-observation leaves a bound behind, not that the running count
// is exact.
func (observation *BrowserObservation) RecordFrames(
	ctx context.Context,
	leaseID uuid.UUID,
	count int64,
) error {
	if observation == nil || observation.pool == nil {
		return nil
	}
	_, err := observation.pool.Exec(ctx, `
UPDATE browser_observation_audits
SET frame_count = GREATEST(frame_count, $2), updated_at = clock_timestamp()
WHERE lease_id = $1 AND status = 'active'
`, leaseID, count)
	return err
}

// ReconcileExpired closes observations whose lease has passed while they were
// still marked active. A Core that exits mid-observation cannot run its own
// teardown, so without this the record would stay active forever and an auditor
// could not tell "still watching" from "Core crashed".
func (observation *BrowserObservation) ReconcileExpired(ctx context.Context) error {
	if observation == nil || observation.pool == nil {
		return nil
	}
	_, err := observation.pool.Exec(ctx, `
UPDATE browser_observation_audits
SET status = 'closed',
    ended_at = COALESCE(ended_at, lease_expires_at),
    end_reason = 'lease_expired_reconciled',
    updated_at = clock_timestamp()
WHERE status = 'active' AND lease_expires_at <= $1
`, observation.now().UTC())
	return err
}

// RunGC reconciles on startup and then periodically, so a crash that happened
// while this process was down is repaired as soon as one comes back.
func (observation *BrowserObservation) RunGC(ctx context.Context) {
	if observation == nil || observation.pool == nil {
		return
	}
	_ = observation.ReconcileExpired(ctx)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = observation.ReconcileExpired(ctx)
		}
	}
}

// ErrObservationUnsupported is returned when the Runtime holding this Run never
// declared the observation feature. Reporting it distinctly keeps an old Runtime
// from looking like a broken one.
var ErrObservationUnsupported = errors.New("browser observation is unsupported by this Runtime")

// ErrObservationForbidden is returned when the caller may not observe this Run.
var ErrObservationForbidden = errors.New("browser observation is not permitted for this caller")

// ResolveIdentity finds the Attempt to observe and authorizes the caller.
//
// An owner may only observe their own Run. An admin may observe any Run, but
// arrives through a separate route with its own permission and a recorded
// reason, so the two never collapse into one check.
func (observation *BrowserObservation) ResolveIdentity(
	ctx context.Context,
	runID uuid.UUID,
	callerUserID uuid.UUID,
	isAdmin bool,
) (BrowserObserverIdentity, error) {
	if observation == nil || observation.pool == nil {
		return BrowserObserverIdentity{}, ErrObservationChannelUnavailable
	}
	var ownerID uuid.UUID
	var identity BrowserObserverIdentity
	var features []string
	err := observation.pool.QueryRow(ctx, `
SELECT r.user_id,
       c.attempt_id,
       c.session_epoch,
       c.browser_session_sha256,
       c.browser_attachment_sha256,
       c.runtime_session_id,
       COALESCE(s.features, ARRAY[]::text[])
FROM runs r
JOIN browser_observable_attempts c ON c.run_id = r.id
LEFT JOIN runtime_sessions s ON s.id = c.runtime_session_id
WHERE r.id = $1
`, runID).Scan(
		&ownerID,
		&identity.AttemptID,
		&identity.SessionEpoch,
		&identity.BrowserSessionSHA256,
		&identity.AttachmentSHA256,
		&identity.RuntimeSessionID,
		&features,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return BrowserObserverIdentity{}, ErrObservationForbidden
	}
	if err != nil {
		return BrowserObserverIdentity{}, err
	}
	if !isAdmin && ownerID != callerUserID {
		return BrowserObserverIdentity{}, ErrObservationForbidden
	}
	if !observationFeatureDeclared(features) {
		return BrowserObserverIdentity{}, ErrObservationUnsupported
	}
	identity.RunID = runID
	return identity, nil
}

// observationFeatureDeclared checks the current Session's declaration. A Runtime
// that reconnects without the feature must stop being observable immediately,
// so this is read per request rather than cached.
func observationFeatureDeclared(features []string) bool {
	for _, feature := range features {
		if feature == BrowserObservationFeature {
			return true
		}
	}
	return false
}

// BrowserObservationFeature is the optional Runtime capability that gates this
// whole surface.
const BrowserObservationFeature = "browser_authenticated_observation.v1"

// State reports the live observation for a Run, if any.
func (observation *BrowserObservation) State(
	ctx context.Context,
	runID uuid.UUID,
) (BrowserObservationState, error) {
	if observation == nil || observation.pool == nil {
		return BrowserObservationState{}, ErrObservationChannelUnavailable
	}
	state := BrowserObservationState{RunID: runID}
	var expiresAt time.Time
	err := observation.pool.QueryRow(ctx, `
SELECT lease_id, lease_expires_at, frame_count, frame_count_complete
FROM browser_observation_audits
WHERE run_id = $1 AND status = 'active'
`, runID).Scan(&state.LeaseID, &expiresAt, &state.FrameCount, &state.FrameCountComplete)
	if errors.Is(err, pgx.ErrNoRows) {
		return state, nil
	}
	if err != nil {
		return BrowserObservationState{}, err
	}
	state.Active = true
	state.LeaseExpiresAt = &expiresAt
	return state, nil
}

// HandleEvent consumes one Worker event. It returns the ack the Worker is
// waiting on: the window is a single unacknowledged event, so failing to ack
// stops the stream rather than dropping one frame.
func (observation *BrowserObservation) HandleEvent(
	ctx context.Context,
	event BrowserObserverEventPayload,
) (BrowserObserverEventAckPayload, error) {
	if observation == nil {
		return BrowserObserverEventAckPayload{}, ErrObservationChannelUnavailable
	}
	if err := event.Validate(); err != nil {
		return BrowserObserverEventAckPayload{}, err
	}
	runID := event.AttemptIdentity.RunID
	switch event.Kind {
	case BrowserObserverFrame:
		if err := observation.frames.publish(runID, event.LeaseID, BrowserObservationFrame{
			FrameSeq:   event.EventSeq,
			CapturedAt: event.CapturedAt.UTC(),
			MIMEType:   event.Frame.MIMEType,
			Data:       event.Frame.Data,
			Width:      event.Frame.Width,
			Height:     event.Frame.Height,
		}); err != nil {
			return BrowserObserverEventAckPayload{}, err
		}
		// Persist a lower bound on an interval, not per frame: the point is that
		// a Core which exits mid-observation leaves a bound behind.
		if event.EventSeq%observationCountFlushFrames == 0 {
			_ = observation.RecordFrames(ctx, event.LeaseID, observation.frames.frameCount(runID))
		}
	case BrowserObserverStopped:
		observation.closeLeaseAsync(event.LeaseID, event.AttemptIdentity, "worker_stopped")
	case BrowserObserverError:
		observation.closeLeaseAsync(
			event.LeaseID,
			event.AttemptIdentity,
			boundedObservationEndReason(event.ErrorCode),
		)
	}
	return BrowserObserverEventAckPayload{
		AttemptIdentity: event.AttemptIdentity,
		LeaseID:         event.LeaseID,
		EventSeq:        event.EventSeq,
	}, nil
}

// closeLeaseAsync ends one specific lease off the caller's goroutine.
//
// Naming the lease matters: a stopped event from lease A can arrive after the
// user has already started lease B on the same Run, and closing "whatever this
// Run is observing now" would tear down B. The Attempt is checked as well, so a
// late event from a previous Attempt cannot close the current one either.
//
// It runs off the caller's goroutine because HandleEvent executes while the
// Runtime WebSocket holds its lifecycle lock, and Stop takes that same lock
// through the sender. Go's RWMutex is not reentrant, so closing inline would
// deadlock the connection and the ack would never be written. The Worker has
// already stopped in these cases, so the confirmation's ordering costs nothing.
func (observation *BrowserObservation) closeLeaseAsync(
	leaseID uuid.UUID,
	identity BrowserObserverIdentity,
	endReason string,
) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = observation.closeLease(ctx, leaseID, identity, endReason)
	}()
}

// closeRunAsync ends whatever lease a Run currently has. Only lifecycle
// teardown uses it, where the Attempt itself is gone and no successor can exist.
func (observation *BrowserObservation) closeRunAsync(runID uuid.UUID, endReason string) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = observation.Stop(ctx, runID, endReason)
	}()
}

// closeLease closes exactly the named lease, refusing to touch a successor.
func (observation *BrowserObservation) closeLease(
	ctx context.Context,
	leaseID uuid.UUID,
	identity BrowserObserverIdentity,
	endReason string,
) error {
	if observation == nil || observation.pool == nil {
		return nil
	}
	var runID uuid.UUID
	err := observation.pool.QueryRow(ctx, `
SELECT run_id FROM browser_observation_audits
WHERE lease_id = $1 AND attempt_id = $2 AND status = 'active'
`, leaseID, identity.AttemptID).Scan(&runID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	count := observation.frames.closeLease(runID, leaseID)
	_ = observation.RecordFrames(ctx, leaseID, count)
	return observation.close(ctx, leaseID, endReason)
}

// WaitFrame serves one long poll.// WaitFrame serves one long poll. A nil frame with no error means the poll timed
// out with nothing new, which is a normal empty response rather than a failure.
func (observation *BrowserObservation) WaitFrame(
	ctx context.Context,
	runID uuid.UUID,
	after int64,
) (*BrowserObservationFrame, error) {
	if observation == nil {
		return nil, ErrObservationChannelUnavailable
	}
	return observation.frames.wait(ctx, runID, after)
}

// observationCountFlushFrames turns the flush interval into a frame count at
// the default interval, so the flush stays periodic without a second timer.
const observationCountFlushFrames = int64(observationCountFlushInterval / (observationDefaultFrameIntervalMS * time.Millisecond))

func boundedObservationEndReason(code string) string {
	if code == "" {
		return "worker_error"
	}
	if len(code) > 100 {
		return code[:100]
	}
	return code
}

// AuthorizeOwner rejects a caller who does not own the Run. Every observation
// endpoint calls it, including the read-only ones: state reveals that a Run is
// being watched and the frame endpoint returns the page itself, so neither may
// rely on start having been authorized earlier.
func (observation *BrowserObservation) AuthorizeOwner(
	ctx context.Context,
	runID uuid.UUID,
	callerUserID uuid.UUID,
) error {
	if observation == nil || observation.pool == nil {
		return ErrObservationChannelUnavailable
	}
	var ownerID uuid.UUID
	err := observation.pool.QueryRow(
		ctx,
		`SELECT user_id FROM runs WHERE id = $1`,
		runID,
	).Scan(&ownerID)
	if errors.Is(err, pgx.ErrNoRows) {
		// A missing Run and someone else's Run are indistinguishable to the
		// caller on purpose: otherwise this endpoint enumerates Run IDs.
		return ErrObservationForbidden
	}
	if err != nil {
		return err
	}
	if ownerID != callerUserID {
		return ErrObservationForbidden
	}
	return nil
}

// ProjectFromEvent records the live Browser identity of a running Run.
//
// Observation cannot read browser_run_controls: that row is written only when a
// challenge pauses the Run for takeover, so a normally executing Run has none --
// and "watch while the Agent keeps working" is the entire point of this surface.
// The ready lifecycle event carries the same identity fields the pause event
// does, so the observable identity is projected from there instead.
func (observation *BrowserObservation) ProjectFromEvent(
	ctx context.Context,
	identity RuntimeAttemptIdentity,
	payload map[string]any,
) error {
	if observation == nil || observation.pool == nil {
		return nil
	}
	phase, _ := payload["phase"].(string)
	switch phase {
	case "ready":
	case "closed", "failed":
		// The Attempt is gone. Drop the projection so a later start cannot
		// resolve a stale identity, and end any observation off this goroutine:
		// this runs under the WebSocket lifecycle read lock and Stop takes it
		// again through the sender, which Go's RWMutex does not allow.
		if _, err := observation.pool.Exec(
			ctx,
			`DELETE FROM browser_observable_attempts WHERE run_id = $1`,
			identity.RunID,
		); err != nil {
			return err
		}
		observation.closeRunAsync(identity.RunID, "run_browser_closed")
		return nil
	default:
		return nil
	}
	if identity.RuntimeSessionID == nil {
		return nil
	}
	// The ready event publishes hashed identity, never the raw UUIDs. Reading
	// the raw names here is what made this projection silently never write.
	sessionDigest, err := browserPayloadSHA256(payload, "browser_session_sha256")
	if err != nil {
		return err
	}
	attachmentDigest, err := browserPayloadSHA256(payload, "browser_attachment_sha256")
	if err != nil {
		return err
	}
	sessionEpoch, err := browserPayloadUint64(payload, "browser_session_epoch")
	if err != nil {
		return err
	}
	_, err = observation.pool.Exec(ctx, `
INSERT INTO browser_observable_attempts (
    run_id, attempt_id, runtime_session_id, browser_session_sha256,
    session_epoch, browser_attachment_sha256, updated_at
) VALUES ($1,$2,$3,$4,$5,$6,clock_timestamp())
ON CONFLICT (run_id) DO UPDATE SET
    attempt_id = EXCLUDED.attempt_id,
    runtime_session_id = EXCLUDED.runtime_session_id,
    browser_session_sha256 = EXCLUDED.browser_session_sha256,
    session_epoch = EXCLUDED.session_epoch,
    browser_attachment_sha256 = EXCLUDED.browser_attachment_sha256,
    updated_at = clock_timestamp()
`, identity.RunID, identity.AttemptID, *identity.RuntimeSessionID,
		sessionDigest, sessionEpoch, attachmentDigest)
	return err
}

func browserPayloadSHA256(payload map[string]any, key string) (string, error) {
	value, _ := payload[key].(string)
	if !validSHA256Hex(value) {
		return "", fmt.Errorf("browser lifecycle %s is invalid", key)
	}
	return value, nil
}
