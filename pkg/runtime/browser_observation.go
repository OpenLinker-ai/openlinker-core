package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
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
	// How long start waits for the Worker to confirm. The command is a one-way
	// push with no reply channel, so the confirmation is the started event and
	// the wait is what turns "accepted" into "actually observing".
	observationStartHandshakeTimeout = 15 * time.Second
	// Fallback ceiling when the deployment does not configure one.
	observationDefaultQuota = 32
	// How long an expired audit owned by another instance is left alone before
	// this one closes it. Long enough that a live instance reconciles its own
	// first, short enough that a crashed instance does not block the Run.
	observationForeignReconcileGrace = 2 * time.Minute
)

// ErrObservationInactive is returned when a Run has no live observation to read
// from. It is separate from a failure so a viewer polling an observation that
// just ended gets a plain answer instead of an internal error.
var ErrObservationInactive = errors.New("browser observation is not active for this Run")

// ErrObservationBusy is returned when this instance is already at its
// concurrent-observation ceiling.
var ErrObservationBusy = errors.New("browser observation capacity is exhausted on this Core instance")

// ErrObservationNotConfirmed is returned when the Worker never confirmed a
// start. The lease is torn down before it surfaces, so the Run is left
// observable rather than pinned by an observation that never began.
var ErrObservationNotConfirmed = errors.New("browser observation was not confirmed by the Worker")

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
	// Concurrent observations this instance will hold. A deployment decision,
	// because each observation pins an in-process frame buffer and a Runtime
	// lease on the Worker.
	quota int

	// Pending start handshakes, keyed by lease. A start blocks on its channel
	// until the Worker's first lifecycle event for that exact lease arrives.
	handshakeMu sync.Mutex
	handshakes  map[uuid.UUID]chan string
}

type browserObserverCommandSender interface {
	SendBrowserObserverCommand(uuid.UUID, BrowserObserverCommandPayload) error
}

func NewBrowserObservation(
	pool *pgxpool.Pool,
	now func() time.Time,
	instance uuid.UUID,
	quota int,
) *BrowserObservation {
	if now == nil {
		now = time.Now
	}
	if quota < 1 {
		quota = observationDefaultQuota
	}
	return &BrowserObservation{
		pool:       pool,
		now:        now,
		instance:   instance,
		quota:      quota,
		frames:     newObservationFrameBuffer(quota),
		handshakes: make(map[uuid.UUID]chan string),
	}
}

// BindInstance records which Core process owns observations started here. It is
// separate from construction because the process identity is only known once
// cluster membership is configured.
func (observation *BrowserObservation) BindInstance(instance uuid.UUID) {
	if observation == nil || instance == uuid.Nil {
		return
	}
	observation.instance = instance
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
	// Frames and wakeups live in this process's memory, so an observation is only
	// serviceable by the instance that owns it. Recording the instance lets a
	// later request on another one refuse instead of polling a buffer that will
	// never fill.
	if observation.instance == uuid.Nil {
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
    command_id, core_instance_id, status, started_at, frame_count,
    frame_count_complete
) VALUES ($1,$2,$3,$4,NULLIF($5,''),$6,$7,$8,$9,$12,$11,'active',$10,0,false)
RETURNING id
`,
		runID, identity.AttemptID, observerUserID, isAdmin, reason,
		identity.SessionEpoch, identity.AttachmentSHA256, leaseID, expiresAt, now,
		observation.instance, command.CommandID,
	).Scan(&auditID)
	if err != nil {
		// The partial unique index makes a second active row impossible, so a
		// conflict here means someone is already observing this Run.
		return BrowserObservationState{}, ErrObservationAlreadyActive
	}

	// The audit row is claimed first, so this is the only start that can be
	// admitting this Run, and the ceiling is then applied in the same step that
	// takes the slot. Checking capacity separately would let concurrent starts
	// on different Runs all pass the check and then all open.
	if !observation.frames.open(runID, leaseID, command.CommandID, identity) {
		_ = observation.close(ctx, leaseID, "instance_saturated")
		return BrowserObservationState{}, ErrObservationBusy
	}

	// Registered before the command is sent, because the started event can come
	// back before this goroutine reaches the wait.
	confirmed := observation.registerHandshake(leaseID)
	defer observation.resolveHandshake(leaseID, "")
	if err := observation.sender.SendBrowserObserverCommand(
		identity.RuntimeSessionID,
		command,
	); err != nil {
		observation.frames.close(runID)
		_ = observation.close(ctx, leaseID, "channel_unavailable")
		return BrowserObservationState{}, ErrObservationChannelUnavailable
	}
	if failure := observation.awaitStart(ctx, confirmed); failure != nil {
		// The Worker may still be holding the lease -- a timeout cannot tell a
		// slow Worker from a dead one -- so the remote stop is sent regardless
		// and only the local teardown is authoritative.
		observation.frames.closeLease(runID, leaseID)
		observation.stopRemote(identity, leaseID)
		_ = observation.close(ctx, leaseID, "start_not_confirmed")
		return BrowserObservationState{}, failure
	}
	return BrowserObservationState{
		RunID:          runID,
		Active:         true,
		LeaseID:        leaseID,
		LeaseExpiresAt: &expiresAt,
	}, nil
}

// CloseSessionObservations ends every observation bound to a Runtime Session.
// A disconnected Worker cannot answer a stop command and will drop its lease on
// its own TTL, so the audit has to be closed from this side or it stays active
// and its unique index blocks the Run from ever being observed again.
func (observation *BrowserObservation) CloseSessionObservations(
	ctx context.Context,
	runtimeSessionID uuid.UUID,
	endReason string,
) error {
	if observation == nil || observation.pool == nil || runtimeSessionID == uuid.Nil {
		return nil
	}
	rows, err := observation.pool.Query(ctx, `
SELECT a.run_id, a.lease_id
FROM browser_observation_audits a
JOIN browser_observable_attempts c
  ON c.run_id = a.run_id AND c.attempt_id = a.attempt_id
WHERE c.runtime_session_id = $1 AND a.status = 'active'
`, runtimeSessionID)
	if err != nil {
		return err
	}
	type endedObservation struct {
		runID   uuid.UUID
		leaseID uuid.UUID
	}
	var ended []endedObservation
	for rows.Next() {
		var row endedObservation
		if scanErr := rows.Scan(&row.runID, &row.leaseID); scanErr != nil {
			rows.Close()
			return scanErr
		}
		ended = append(ended, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, row := range ended {
		observation.resolveHandshake(row.leaseID, endReason)
		count := observation.frames.closeLease(row.runID, row.leaseID)
		_ = observation.RecordFrames(ctx, row.leaseID, count)
		if closeErr := observation.close(ctx, row.leaseID, endReason); closeErr != nil {
			err = closeErr
		}
	}
	return err
}

// awaitStart blocks until the Worker reports started, reports a failure, or the
// handshake times out. A start that returns without this wait would report
// success for an observation the Worker may have refused outright.
func (observation *BrowserObservation) awaitStart(
	ctx context.Context,
	confirmed <-chan string,
) error {
	timeout := time.NewTimer(observationStartHandshakeTimeout)
	defer timeout.Stop()
	select {
	case outcome := <-confirmed:
		if outcome == "" {
			return nil
		}
		return fmt.Errorf("%w: %s", ErrObservationNotConfirmed, outcome)
	case <-timeout.C:
		return ErrObservationNotConfirmed
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (observation *BrowserObservation) registerHandshake(leaseID uuid.UUID) <-chan string {
	confirmed := make(chan string, 1)
	observation.handshakeMu.Lock()
	defer observation.handshakeMu.Unlock()
	observation.handshakes[leaseID] = confirmed
	return confirmed
}

// resolveHandshake settles a pending start. It is also how the waiter
// deregisters itself, so a lifecycle event arriving after the wait gave up does
// not accumulate a channel nobody reads.
func (observation *BrowserObservation) resolveHandshake(leaseID uuid.UUID, failure string) {
	observation.handshakeMu.Lock()
	confirmed := observation.handshakes[leaseID]
	delete(observation.handshakes, leaseID)
	observation.handshakeMu.Unlock()
	if confirmed == nil {
		return
	}
	select {
	case confirmed <- failure:
	default:
	}
}

// stopRemote tells the Worker to drop a lease without touching any local state.
// Used where the audit is being closed by the caller and the only thing left is
// to release the Runtime side.
func (observation *BrowserObservation) stopRemote(
	identity BrowserObserverIdentity,
	leaseID uuid.UUID,
) {
	if observation.sender == nil || identity.RuntimeSessionID == uuid.Nil {
		return
	}
	_ = observation.sender.SendBrowserObserverCommand(
		identity.RuntimeSessionID,
		BrowserObserverCommandPayload{
			AttemptIdentity: identity,
			CommandID:       uuid.New(),
			Action:          BrowserObserverStop,
			LeaseID:         leaseID,
		},
	)
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
	var owner uuid.UUID
	err := observation.pool.QueryRow(ctx, `
SELECT a.lease_id, a.attempt_id, a.session_epoch, a.core_instance_id,
       a.attachment_sha256, c.browser_session_sha256, c.runtime_session_id
FROM browser_observation_audits a
JOIN browser_observable_attempts c
  ON c.run_id = a.run_id AND c.attempt_id = a.attempt_id
WHERE a.run_id = $1 AND a.status = 'active'
`, runID).Scan(
		&leaseID, &attemptID, &epoch, &owner,
		&attachmentDigest, &sessionDigest, &sessionID,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	// Only the owning instance can finish the teardown: the frame buffer and its
	// waiters live there. Closing the audit from elsewhere would leave that
	// buffer open, its quota slot held, and its viewers polling a dead lease.
	if owner != observation.instance {
		return ErrObservationChannelUnavailable
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
	// Only this instance's observations are reconciled here, and the rows it
	// closes are returned so their in-process state can be released too. A pure
	// SQL close would leave the frame buffer open forever: its waiters would
	// never be woken and its quota slot never returned.
	rows, err := observation.pool.Query(ctx, `
UPDATE browser_observation_audits
SET status = 'closed',
    ended_at = COALESCE(ended_at, lease_expires_at),
    end_reason = 'lease_expired_reconciled',
    updated_at = clock_timestamp()
WHERE status = 'active' AND lease_expires_at <= $1
  AND core_instance_id = $2
RETURNING run_id, lease_id
`, observation.now().UTC(), observation.instance)
	if err != nil {
		return err
	}
	type expiredObservation struct {
		runID   uuid.UUID
		leaseID uuid.UUID
	}
	var expired []expiredObservation
	for rows.Next() {
		var row expiredObservation
		if scanErr := rows.Scan(&row.runID, &row.leaseID); scanErr != nil {
			rows.Close()
			return scanErr
		}
		expired = append(expired, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, row := range expired {
		observation.resolveHandshake(row.leaseID, "lease_expired")
		count := observation.frames.closeLease(row.runID, row.leaseID)
		// Not RecordFrames: that only writes to an active audit, and the update
		// above has already closed this one, so the count would be dropped on
		// exactly the path that knows the final total.
		_ = observation.recordFinalFrames(ctx, row.leaseID, count)
	}
	return nil
}

// recordFinalFrames writes the total onto an audit this instance has just
// closed. The count is exact -- it is what this process actually buffered -- so
// it also settles frame_count_complete.
func (observation *BrowserObservation) recordFinalFrames(
	ctx context.Context,
	leaseID uuid.UUID,
	count int64,
) error {
	_, err := observation.pool.Exec(ctx, `
UPDATE browser_observation_audits
SET frame_count = GREATEST(frame_count, $2),
    frame_count_complete = true,
    updated_at = clock_timestamp()
WHERE lease_id = $1 AND status = 'closed'
`, leaseID, count)
	return err
}

// ReconcileForeignExpired closes expired audits left behind by an instance that
// is no longer running. It is separate from ReconcileExpired because there is no
// in-process state to release, and because an audit must not be closed out from
// under an instance that is still serving it.
func (observation *BrowserObservation) ReconcileForeignExpired(ctx context.Context) error {
	if observation == nil || observation.pool == nil {
		return nil
	}
	// The grace period is what distinguishes a dead instance from a live one
	// that has not run its own reconcile tick yet.
	_, err := observation.pool.Exec(ctx, `
UPDATE browser_observation_audits
SET status = 'closed',
    ended_at = COALESCE(ended_at, lease_expires_at),
    end_reason = 'lease_expired_reconciled',
    updated_at = clock_timestamp()
WHERE status = 'active'
  AND lease_expires_at <= $1
  AND core_instance_id <> $2
`, observation.now().UTC().Add(-observationForeignReconcileGrace), observation.instance)
	return err
}

// RunGC reconciles on startup and then periodically, so a crash that happened
// while this process was down is repaired as soon as one comes back.
func (observation *BrowserObservation) RunGC(ctx context.Context) {
	if observation == nil || observation.pool == nil {
		return
	}
	observation.reconcile(ctx)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			observation.reconcile(ctx)
		}
	}
}

func (observation *BrowserObservation) reconcile(ctx context.Context) {
	_ = observation.ReconcileExpired(ctx)
	_ = observation.ReconcileForeignExpired(ctx)
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
	var runStatus string
	var dispatchState string
	var activeAttemptID *uuid.UUID
	var identity BrowserObserverIdentity
	var features []string
	err := observation.pool.QueryRow(ctx, `
SELECT r.user_id,
       r.status,
       r.dispatch_state,
       r.active_attempt_id,
       c.attempt_id,
       c.session_epoch,
       c.browser_session_sha256,
       c.browser_attachment_sha256,
       c.runtime_session_id,
       COALESCE(s.features, ARRAY[]::text[])
FROM runs r
JOIN browser_observable_attempts c ON c.run_id = r.id
LEFT JOIN runtime_sessions s ON s.runtime_session_id = c.runtime_session_id
WHERE r.id = $1
`, runID).Scan(
		&ownerID,
		&runStatus,
		&dispatchState,
		&activeAttemptID,
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
	// A projection can outlive the Attempt it describes if a teardown was
	// missed, so the Run itself has to still be running and still be on the
	// Attempt the projection names. Without this a stale row would authorize an
	// observation of an Attempt the Runtime has already left.
	// executing, not offered: an Attempt that has only been offered has no
	// Worker running a Browser yet, so there is nothing to observe.
	if runStatus != "running" || dispatchState != "executing" {
		return BrowserObserverIdentity{}, ErrObservationForbidden
	}
	if activeAttemptID == nil || *activeAttemptID != identity.AttemptID {
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
	var owner uuid.UUID
	err := observation.pool.QueryRow(ctx, `
SELECT lease_id, lease_expires_at, frame_count, frame_count_complete,
       core_instance_id
FROM browser_observation_audits
WHERE run_id = $1 AND status = 'active'
`, runID).Scan(
		&state.LeaseID, &expiresAt, &state.FrameCount, &state.FrameCountComplete,
		&owner,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return state, nil
	}
	if err != nil {
		return BrowserObservationState{}, err
	}
	// Reporting an observation this instance cannot serve would send the viewer
	// on to poll a frame buffer that only exists in another process, which
	// presents as a live observation that never produces a frame.
	if owner != observation.instance {
		return BrowserObservationState{}, ErrObservationChannelUnavailable
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
	// Every kind is correlated, not only frames. A stopped or error event from a
	// superseded command names a lease that may still look current, and acting
	// on it would close the observation that replaced it.
	if !observation.frames.owns(
		runID,
		event.LeaseID,
		event.CommandID,
		event.AttemptIdentity,
	) {
		return BrowserObserverEventAckPayload{}, runtimeValidationError(
			"browser observer event does not name a live observation",
			nil,
		)
	}
	switch event.Kind {
	case BrowserObserverStarted:
		observation.resolveHandshake(event.LeaseID, "")
	case BrowserObserverFrame:
		if err := observation.frames.publish(
			runID,
			event.LeaseID,
			event.CommandID,
			event.AttemptIdentity,
			BrowserObservationFrame{
				FrameSeq:   event.EventSeq,
				CapturedAt: event.CapturedAt.UTC(),
				MIMEType:   event.Frame.MIMEType,
				Data:       event.Frame.Data,
				Width:      event.Frame.Width,
				Height:     event.Frame.Height,
			},
		); err != nil {
			return BrowserObserverEventAckPayload{}, err
		}
		// Persist a lower bound on an interval, not per frame: the point is that
		// a Core which exits mid-observation leaves a bound behind.
		if event.EventSeq%observationCountFlushFrames == 0 {
			_ = observation.RecordFrames(ctx, event.LeaseID, observation.frames.frameCount(runID))
		}
	case BrowserObserverStopped:
		observation.resolveHandshake(event.LeaseID, "stopped")
		observation.closeLeaseAsync(event.LeaseID, event.AttemptIdentity, "worker_stopped")
	case BrowserObserverError:
		observation.resolveHandshake(
			event.LeaseID,
			boundedObservationEndReason(event.ErrorCode),
		)
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

// closeAttemptAsync ends the observation of one Attempt and forgets it.
//
// It resolves the lease from the audit itself rather than from the projection,
// so teardown does not depend on a row it is about to delete. The remote stop is
// best effort: the Attempt has already gone, and the audit must close whether or
// not the Worker is still reachable.
func (observation *BrowserObservation) closeAttemptAsync(
	runID uuid.UUID,
	attemptID uuid.UUID,
	endReason string,
) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = observation.closeAttempt(ctx, runID, attemptID, endReason)
	}()
}

func (observation *BrowserObservation) closeAttempt(
	ctx context.Context,
	runID uuid.UUID,
	attemptID uuid.UUID,
	endReason string,
) error {
	if observation == nil || observation.pool == nil {
		return nil
	}
	var leaseID uuid.UUID
	err := observation.pool.QueryRow(ctx, `
SELECT lease_id FROM browser_observation_audits
WHERE run_id = $1 AND attempt_id = $2 AND status = 'active'
`, runID, attemptID).Scan(&leaseID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// Nothing was being observed; still forget the Attempt below.
	case err != nil:
		return err
	default:
		count := observation.frames.closeLease(runID, leaseID)
		_ = observation.RecordFrames(ctx, leaseID, count)
		if closeErr := observation.close(ctx, leaseID, endReason); closeErr != nil {
			return closeErr
		}
	}
	_, err = observation.pool.Exec(
		ctx,
		`DELETE FROM browser_observable_attempts WHERE run_id = $1 AND attempt_id = $2`,
		runID, attemptID,
	)
	return err
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

// WaitFrame serves one long poll. A nil frame with no error means the poll timed
// out with nothing new, which is a normal empty response rather than a failure.
func (observation *BrowserObservation) WaitFrame(
	ctx context.Context,
	runID uuid.UUID,
	after int64,
) (*BrowserObservationFrame, error) {
	if observation == nil {
		return nil, ErrObservationChannelUnavailable
	}
	frame, err := observation.frames.wait(ctx, runID, after)
	if !errors.Is(err, ErrObservationInactive) {
		return frame, err
	}
	// No local buffer can mean two different things, and they need different
	// answers: the observation ended, or it belongs to another Core instance and
	// this one can never serve it. Only this path touches the database, so a
	// normal poll stays in memory.
	return nil, observation.absentFrameReason(ctx, runID)
}

func (observation *BrowserObservation) absentFrameReason(
	ctx context.Context,
	runID uuid.UUID,
) error {
	if observation.pool == nil {
		return ErrObservationInactive
	}
	var owner uuid.UUID
	err := observation.pool.QueryRow(ctx, `
SELECT core_instance_id FROM browser_observation_audits
WHERE run_id = $1 AND status = 'active'
`, runID).Scan(&owner)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return ErrObservationInactive
	case err != nil:
		return err
	case owner != observation.instance:
		return ErrObservationChannelUnavailable
	}
	// Active, owned here, and yet no buffer: the observation was torn down
	// locally and the audit has not caught up. Ended, from the viewer's side.
	return ErrObservationInactive
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
	eventSeq int64,
) error {
	if observation == nil || observation.pool == nil {
		return nil
	}
	phase, _ := payload["phase"].(string)
	switch phase {
	case "ready":
	case "closed", "failed":
		// The Attempt is gone, so close the audit and drop the projection.
		//
		// The audit is closed from the lifecycle identity alone, never through a
		// path that reads the projection: deleting the projection first and then
		// closing through it leaves the audit active forever, and its unique
		// index then blocks the Run from ever being observed again. Closing runs
		// off this goroutine because the lifecycle handler holds the WebSocket
		// lifecycle read lock, which the sender takes again.
		observation.closeAttemptAsync(
			identity.RunID,
			identity.AttemptID,
			"run_browser_closed",
		)
		return nil
	default:
		return nil
	}
	if identity.RuntimeSessionID == nil {
		return nil
	}
	// A Runtime that does not support observation still reports ready, and its
	// ready event carries no observation identity. Absence is that case and is
	// skipped; a present-but-malformed identity is a contract violation and is
	// reported below.
	if !browserPayloadHasObservationIdentity(payload) {
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
	// Runs on replays too, not only on first insert. A projection that failed
	// after the event was appended would otherwise be skipped on every retry and
	// lost for good, leaving the Run permanently unobservable.
	//
	// Replay safety comes from two fences instead. The Run must still be on the
	// Attempt this event names, so a replay belonging to a finished Attempt
	// cannot overwrite the Attempt now running; and within one Attempt the event
	// sequence must not go backwards.
	_, err = observation.pool.Exec(ctx, `
INSERT INTO browser_observable_attempts (
    run_id, attempt_id, runtime_session_id, browser_session_sha256,
    session_epoch, browser_attachment_sha256, event_seq, updated_at
)
SELECT $1,$2,$3,$4,$5,$6,$7,clock_timestamp()
FROM runs r
WHERE r.id = $1 AND r.active_attempt_id = $2
ON CONFLICT (run_id) DO UPDATE SET
    attempt_id = EXCLUDED.attempt_id,
    runtime_session_id = EXCLUDED.runtime_session_id,
    browser_session_sha256 = EXCLUDED.browser_session_sha256,
    session_epoch = EXCLUDED.session_epoch,
    browser_attachment_sha256 = EXCLUDED.browser_attachment_sha256,
    event_seq = EXCLUDED.event_seq,
    updated_at = clock_timestamp()
WHERE browser_observable_attempts.attempt_id <> EXCLUDED.attempt_id
   OR browser_observable_attempts.event_seq <= EXCLUDED.event_seq
`, identity.RunID, identity.AttemptID, *identity.RuntimeSessionID,
		sessionDigest, sessionEpoch, attachmentDigest, eventSeq)
	return err
}

// browserPayloadHasObservationIdentity reports whether the event claims an
// observable identity at all. It checks only for presence: the fields are
// validated where they are read.
func browserPayloadHasObservationIdentity(payload map[string]any) bool {
	for _, key := range []string{
		"browser_session_sha256",
		"browser_session_epoch",
		"browser_attachment_sha256",
	} {
		if _, present := payload[key]; present {
			return true
		}
	}
	return false
}

func browserPayloadSHA256(payload map[string]any, key string) (string, error) {
	value, _ := payload[key].(string)
	if !validSHA256Hex(value) {
		return "", fmt.Errorf("browser lifecycle %s is invalid", key)
	}
	return value, nil
}
