package runtime

import (
	"context"
	"errors"
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
	return &BrowserObservation{pool: pool, now: now, instance: instance}
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
    session_epoch, attachment_id, lease_id, lease_expires_at,
    status, started_at, frame_count, frame_count_complete
) VALUES ($1,$2,$3,$4,NULLIF($5,''),$6,$7,$8,$9,'active',$10,0,false)
RETURNING id
`,
		runID, identity.AttemptID, observerUserID, isAdmin, reason,
		identity.SessionEpoch, identity.AttachmentID, leaseID, expiresAt, now,
	).Scan(&auditID)
	if err != nil {
		// The partial unique index makes a second active row impossible, so a
		// conflict here means someone is already observing this Run.
		return BrowserObservationState{}, ErrObservationAlreadyActive
	}

	if err := observation.sender.SendBrowserObserverCommand(
		identity.RuntimeSessionID,
		command,
	); err != nil {
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
	var attachmentID uuid.UUID
	err := observation.pool.QueryRow(ctx, `
SELECT lease_id, attempt_id, session_epoch, attachment_id
FROM browser_observation_audits
WHERE run_id = $1 AND status = 'active'
`, runID).Scan(&leaseID, &attemptID, &epoch, &attachmentID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if observation.sender != nil && sessionID != uuid.Nil {
		_ = observation.sender.SendBrowserObserverCommand(sessionID, BrowserObserverCommandPayload{
			AttemptIdentity: BrowserObserverIdentity{
				RunID:        runID,
				AttemptID:    attemptID,
				SessionEpoch: epoch,
				AttachmentID: attachmentID,
			},
			CommandID: uuid.New(),
			Action:    BrowserObserverStop,
			LeaseID:   leaseID,
		})
	}
	return observation.close(ctx, leaseID, endReason)
}

func (observation *BrowserObservation) close(
	ctx context.Context,
	leaseID uuid.UUID,
	endReason string,
) error {
	_, err := observation.pool.Exec(ctx, `
UPDATE browser_observation_audits
SET status = 'closed',
    ended_at = $2,
    end_reason = $3,
    frame_count_complete = true,
    updated_at = clock_timestamp()
WHERE lease_id = $1 AND status = 'active'
`, leaseID, observation.now().UTC(), endReason)
	return err
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
