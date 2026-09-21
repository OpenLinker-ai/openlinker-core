package runtime

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	// One frame in flight per Run. The Worker window is a single unacknowledged
	// event, so a deeper buffer here could only hold frames the Worker is not
	// allowed to have sent yet.
	observationFrameBytesLimit = 1 << 20
	observationWaitTimeout     = 30 * time.Second
	// Each waiter is one authorized HTTP long poll over the same in-memory
	// latest frame. Bounding them prevents one Run from pinning unbounded
	// goroutines and response copies while still allowing several read-only
	// entry points to observe it together.
	observationMaxFrameWaitersPerRun = 8
)

// BrowserObservationFrame is the only shape the browser ever receives. It
// deliberately carries no Runtime, Node, Attachment or Session identity: those
// are internal invariants between the Worker and Core, and a viewer has no use
// for them.
type BrowserObservationFrame struct {
	FrameSeq   int64     `json:"frame_seq"`
	CapturedAt time.Time `json:"captured_at"`
	MIMEType   string    `json:"mime_type"`
	Data       []byte    `json:"data"`
	Width      int       `json:"width"`
	Height     int       `json:"height"`
}

type observationLiveFrame struct {
	leaseID uuid.UUID
	// What this observation was opened with. An event is only accepted when it
	// names all of it: a Worker that moved to another Attempt, or that is still
	// answering a superseded command, must not have its frames attributed here.
	commandID uuid.UUID
	identity  BrowserObserverIdentity
	frame     *BrowserObservationFrame
	notify    chan struct{}
	count     int64
	// When a viewer last asked for a frame. A viewer that is still watching polls
	// at least once per long-poll timeout, so a gap much larger than that means
	// nobody is on the other end any more.
	lastPolledAt time.Time
	// The highest event sequence accepted from the Worker. The Worker numbers
	// every event it sends from one counter, so this is what makes a replayed or
	// reordered event -- of any kind, not only a frame -- detectable.
	lastEventSeq int64
	// Number of independent frame long polls currently blocked on this live
	// lease. The latest frame and notification are shared; no per-viewer frame
	// history is retained.
	waiters int
}

// retiredObservation is what an observation this process opened leaves behind
// when it ends. It exists only so a terminal event that arrives after the fact
// can be recognised as ours and settled, instead of being answered by a rule
// that would settle any terminal event at all.
type retiredObservation struct {
	commandID uuid.UUID
	identity  BrowserObserverIdentity
	// The last sequence this observation consumed while it was live, advanced
	// again by each settle. Without it a terminal event can be replayed for as
	// long as the tombstone survives.
	lastEventSeq int64
	retiredAt    time.Time
}

const (
	// How many ended observations are remembered. A terminal event follows its
	// stop by one round trip, so this only has to outlive that; it is a bound,
	// not a history.
	observationRetiredLimit = 256
	// And for no longer than this. The count alone is not a bound: on a quiet
	// Core a tombstone could survive for days and keep answering for a lease
	// that ended long ago.
	observationRetiredTTL = 2 * time.Minute
)

// retainedFinalFrame is the frame a round ended on, kept so a viewer can read it
// after the Run reaches its terminal state and the live poll has stopped.
//
// It is written the moment the Worker delivers the capture it marks as final --
// not when the observation closes. The Worker makes that capture while the
// attachment is still open, waits for Core's acknowledgement, and only then
// closes the attachment and reports the Run's result. So by the time the Run is
// terminal the retention already exists, whatever order the observation's own
// ending arrives in: an error from the Engine being torn down, the viewer's stop,
// the browser's closed event. Nothing here depends on how the observation ends.
//
// It stays in this process's memory with its own expiry: a bounded handoff, not
// a stored history, and nothing here survives a restart or reaches another
// instance.
type retainedFinalFrame struct {
	// The Attempt whose round this frame ended. A retry re-runs the whole round,
	// so a read answers only with the retention of the Run's latest Attempt.
	attemptID  uuid.UUID
	frame      BrowserObservationFrame
	retainedAt time.Time
}

const (
	// How long a final frame stays readable. Long enough that a viewer reading
	// the Run it just watched finish gets the picture, short enough that page
	// content is not held for a session somebody reopens much later.
	observationFinalFrameTTL = 10 * time.Minute
	// How many Runs keep a final frame, and the total bytes they may hold between
	// them. Each frame is already capped at observationFrameBytesLimit, so both
	// bounds are needed: the count keeps the map small and the byte ceiling is
	// what actually bounds the memory.
	observationFinalFrameLimit      = 32
	observationFinalFrameBytesLimit = 32 << 20
)

type observationFrameBuffer struct {
	mu    sync.Mutex
	quota int
	// Injected so the tombstone window can be tested without waiting it out.
	now          func() time.Time
	live         map[uuid.UUID]*observationLiveFrame
	retired      map[uuid.UUID]retiredObservation
	retiredOrder []uuid.UUID
	// Final frames keyed by Run, because that is what a viewer asks with.
	// Insertion order is kept separately so the oldest is the one evicted when
	// either bound is reached.
	finalFrames map[uuid.UUID]retainedFinalFrame
	finalOrder  []uuid.UUID
	finalBytes  int
}

func newObservationFrameBuffer(quota int) *observationFrameBuffer {
	return &observationFrameBuffer{
		quota:       quota,
		now:         time.Now,
		live:        make(map[uuid.UUID]*observationLiveFrame),
		retired:     make(map[uuid.UUID]retiredObservation),
		finalFrames: make(map[uuid.UUID]retainedFinalFrame),
	}
}

// open reserves the slot and admits the observation in one step. Checking the
// quota separately and opening afterwards lets concurrent starts all pass the
// check and then all open, which is how a ceiling of N admits N+k.
//
// Replacing an existing entry for the same Run does not consume a new slot: the
// database unique index is the authority on a second concurrent observation, and
// refusing here would strand a Run whose buffer outlived its audit.
func (buffer *observationFrameBuffer) open(
	runID, leaseID, commandID uuid.UUID,
	identity BrowserObserverIdentity,
) bool {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	if _, replacing := buffer.live[runID]; !replacing && len(buffer.live) >= buffer.quota {
		return false
	}
	if previous := buffer.live[runID]; previous != nil {
		buffer.retire(previous)
		close(previous.notify)
	}
	buffer.live[runID] = &observationLiveFrame{
		leaseID:   leaseID,
		commandID: commandID,
		identity:  identity,
		// Counted from the start, so an observation nobody ever polls is reaped
		// on the same schedule as one whose viewer went away.
		lastPolledAt: buffer.now(),
		notify:       make(chan struct{}),
	}
	return true
}

// close wakes every waiter so a viewer polling a finished observation returns
// promptly instead of holding its request open until the timeout.
func (buffer *observationFrameBuffer) close(runID uuid.UUID) int64 {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.closeLocked(runID)
}

// closeAbandoned closes the observations no viewer has polled within the grace
// period, and returns them. Core cannot be told that a browser went away -- a
// crashed tab sends nothing -- so absence of polling is the only evidence there
// is.
//
// Selecting and closing happen under one lock. Between a separate select and
// close a viewer can poll, and the observation it is actively reading would be
// torn down anyway on evidence that was true a moment ago and is not any more.
func (buffer *observationFrameBuffer) closeAbandoned(
	grace time.Duration,
) []observationClosure {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	var closed []observationClosure
	cutoff := buffer.now().Add(-grace)
	for runID, live := range buffer.live {
		if !live.lastPolledAt.Before(cutoff) {
			continue
		}
		closed = append(closed, observationClosure{
			observationLease: observationLease{runID: runID, leaseID: live.leaseID},
			frames:           buffer.closeLocked(runID),
		})
	}
	return closed
}

// observationClosure is a lease that has just been closed, with the frame total
// it served. The two travel together because the count is only knowable at the
// moment the buffer is dropped.
type observationClosure struct {
	observationLease
	frames int64
}

// observationLease names one observation by the two identifiers every teardown
// path needs.
type observationLease struct {
	runID   uuid.UUID
	leaseID uuid.UUID
}

// closeLocked is close without taking the lock. Callers hold it.
func (buffer *observationFrameBuffer) closeLocked(runID uuid.UUID) int64 {
	live := buffer.live[runID]
	if live == nil {
		return 0
	}
	delete(buffer.live, runID)
	buffer.retire(live)
	close(live.notify)
	return live.count
}

// retire remembers an ended observation. Callers hold the lock.
func (buffer *observationFrameBuffer) retire(live *observationLiveFrame) {
	if _, known := buffer.retired[live.leaseID]; known {
		return
	}
	buffer.retired[live.leaseID] = retiredObservation{
		commandID:    live.commandID,
		identity:     live.identity,
		lastEventSeq: live.lastEventSeq,
		retiredAt:    buffer.now(),
	}
	buffer.retiredOrder = append(buffer.retiredOrder, live.leaseID)
	if len(buffer.retiredOrder) > observationRetiredLimit {
		delete(buffer.retired, buffer.retiredOrder[0])
		buffer.retiredOrder = buffer.retiredOrder[1:]
	}
}

// settleRetired reports whether a terminal event may be acknowledged without
// acting: the lease must be one this process opened and has since ended, named
// by the same command and identity, still inside the window a terminal event
// can legitimately arrive in, and carrying a sequence that has not been settled
// before. The sequence is consumed here, so the same event cannot settle twice.
func (buffer *observationFrameBuffer) settleRetired(
	leaseID, commandID uuid.UUID,
	identity BrowserObserverIdentity,
	eventSeq int64,
) bool {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	retired, known := buffer.retired[leaseID]
	if !known || retired.commandID != commandID || retired.identity != identity {
		return false
	}
	if buffer.now().Sub(retired.retiredAt) > observationRetiredTTL {
		buffer.forgetRetired(leaseID)
		return false
	}
	if eventSeq <= retired.lastEventSeq {
		return false
	}
	retired.lastEventSeq = eventSeq
	buffer.retired[leaseID] = retired
	return true
}

// forgetRetired drops one tombstone. Callers hold the lock.
func (buffer *observationFrameBuffer) forgetRetired(leaseID uuid.UUID) {
	delete(buffer.retired, leaseID)
	for index, retired := range buffer.retiredOrder {
		if retired == leaseID {
			buffer.retiredOrder = append(
				buffer.retiredOrder[:index],
				buffer.retiredOrder[index+1:]...,
			)
			return
		}
	}
}

// admit decides whether an event may be acted on, and consumes its sequence in
// the same step. An event has to name the observation this Run is actually
// running -- the same lease, the same start command, the same Attempt identity
// -- and it has to advance the Worker's event sequence.
//
// Every kind goes through here, not only frames. A lifecycle event from a
// superseded command would otherwise close the observation that replaced it, and
// a replayed one would be acted on twice.
func (buffer *observationFrameBuffer) admit(
	runID, leaseID, commandID uuid.UUID,
	identity BrowserObserverIdentity,
	eventSeq int64,
) bool {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	live := buffer.live[runID]
	if live == nil || live.leaseID != leaseID || live.commandID != commandID ||
		live.identity != identity {
		return false
	}
	if eventSeq <= live.lastEventSeq {
		return false
	}
	live.lastEventSeq = eventSeq
	return true
}

// publish accepts a frame only for the lease that currently owns the Run and
// only if it advances the sequence. A frame from a superseded lease belongs to
// an observation that has already ended.
func (buffer *observationFrameBuffer) publish(
	runID uuid.UUID,
	leaseID uuid.UUID,
	commandID uuid.UUID,
	identity BrowserObserverIdentity,
	frame BrowserObservationFrame,
	roundFinal bool,
) error {
	if frame.MIMEType != "image/jpeg" || len(frame.Data) == 0 ||
		len(frame.Data) > observationFrameBytesLimit ||
		frame.Width < 1 || frame.Height < 1 || frame.FrameSeq < 1 {
		return errors.New("browser observation frame is invalid")
	}
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	live := buffer.live[runID]
	if live == nil || live.leaseID != leaseID {
		return errors.New("browser observation lease is stale")
	}
	// The lease alone is not enough. A Worker still answering a superseded
	// command, or one that has moved to another Attempt, would otherwise have
	// its frames accepted under a lease that is nominally still current.
	if live.commandID != commandID || live.identity != identity {
		return errors.New("browser observation frame does not name its command")
	}
	if live.frame != nil && frame.FrameSeq <= live.frame.FrameSeq {
		return errors.New("browser observation frame sequence regressed")
	}
	copied := frame
	copied.Data = append([]byte(nil), frame.Data...)
	live.frame = &copied
	live.count++
	// Retained here, on receipt, under the same lock and with the same identity
	// checks the live frame just passed. Deferring it to the close is what made
	// the final frame depend on the order the observation's ending arrived in.
	if roundFinal {
		buffer.retainFinalLocked(runID, identity.AttemptID, copied)
	}
	close(live.notify)
	live.notify = make(chan struct{})
	return nil
}

// retainFinalLocked keeps a round's final frame, replacing whatever the Run held.
// Callers hold the lock.
func (buffer *observationFrameBuffer) retainFinalLocked(
	runID, attemptID uuid.UUID,
	frame BrowserObservationFrame,
) {
	buffer.expireFinalFramesLocked()
	buffer.dropFinalLocked(runID)
	retained := retainedFinalFrame{
		attemptID:  attemptID,
		frame:      frame,
		retainedAt: buffer.now(),
	}
	retained.frame.Data = append([]byte(nil), frame.Data...)
	buffer.finalFrames[runID] = retained
	buffer.finalOrder = append(buffer.finalOrder, runID)
	buffer.finalBytes += len(retained.frame.Data)
	// Evicting the oldest keeps the most recently finished Runs readable, which
	// are the ones a viewer is looking at.
	for len(buffer.finalOrder) > 0 &&
		(len(buffer.finalOrder) > observationFinalFrameLimit ||
			buffer.finalBytes > observationFinalFrameBytesLimit) {
		buffer.dropFinalLocked(buffer.finalOrder[0])
	}
}

// expireFinalFramesLocked drops every retention past its window. Callers hold
// the lock.
func (buffer *observationFrameBuffer) expireFinalFramesLocked() {
	cutoff := buffer.now().Add(-observationFinalFrameTTL)
	for _, runID := range append([]uuid.UUID(nil), buffer.finalOrder...) {
		retained, known := buffer.finalFrames[runID]
		if !known || retained.retainedAt.After(cutoff) {
			continue
		}
		buffer.dropFinalLocked(runID)
	}
}

// dropFinalLocked forgets one retention and its bytes. Callers hold the lock.
func (buffer *observationFrameBuffer) dropFinalLocked(runID uuid.UUID) {
	retained, known := buffer.finalFrames[runID]
	if !known {
		return
	}
	buffer.finalBytes -= len(retained.frame.Data)
	if buffer.finalBytes < 0 {
		buffer.finalBytes = 0
	}
	delete(buffer.finalFrames, runID)
	for index, candidate := range buffer.finalOrder {
		if candidate == runID {
			buffer.finalOrder = append(
				buffer.finalOrder[:index],
				buffer.finalOrder[index+1:]...,
			)
			break
		}
	}
}

// observationFinalFrameRead is one retention as a reader sees it.
type observationFinalFrameRead struct {
	frame         *BrowserObservationFrame
	attemptID     uuid.UUID
	retainedUntil time.Time
}

// finalFrame returns the Run's retention, if any, with a copy of its bytes.
func (buffer *observationFrameBuffer) finalFrame(
	runID uuid.UUID,
) observationFinalFrameRead {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	buffer.expireFinalFramesLocked()
	retained, known := buffer.finalFrames[runID]
	if !known {
		return observationFinalFrameRead{}
	}
	copied := retained.frame
	copied.Data = append([]byte(nil), retained.frame.Data...)
	return observationFinalFrameRead{
		frame:         &copied,
		attemptID:     retained.attemptID,
		retainedUntil: retained.retainedAt.Add(observationFinalFrameTTL),
	}
}

// expireFinalFrames is the periodic sweep. Retention is also checked on every
// read, so this only stops an idle Core from holding a frame nobody asks for.
func (buffer *observationFrameBuffer) expireFinalFrames() {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	buffer.expireFinalFramesLocked()
}

func (buffer *observationFrameBuffer) frameCount(runID uuid.UUID) int64 {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	if live := buffer.live[runID]; live != nil {
		return live.count
	}
	return 0
}

// wait blocks until a frame newer than after arrives, the observation ends, or
// the poll times out. A nil frame with no error means "nothing new yet", which
// the caller turns into an empty long-poll response rather than an error.
func (buffer *observationFrameBuffer) wait(
	ctx context.Context,
	runID uuid.UUID,
	after int64,
) (*BrowserObservationFrame, error) {
	deadline := time.NewTimer(observationWaitTimeout)
	defer deadline.Stop()
	buffer.mu.Lock()
	live := buffer.live[runID]
	if live == nil {
		buffer.mu.Unlock()
		return nil, ErrObservationInactive
	}
	if live.frame != nil && live.frame.FrameSeq > after {
		live.lastPolledAt = buffer.now()
		copied := *live.frame
		copied.Data = append([]byte(nil), live.frame.Data...)
		buffer.mu.Unlock()
		return &copied, nil
	}
	if live.waiters >= observationMaxFrameWaitersPerRun {
		buffer.mu.Unlock()
		return nil, ErrObservationViewerCapacity
	}
	admitted := live
	admitted.waiters++
	admitted.lastPolledAt = buffer.now()
	buffer.mu.Unlock()
	defer buffer.releaseFrameWaiter(admitted)
	for {
		buffer.mu.Lock()
		live = buffer.live[runID]
		if live == nil || live != admitted {
			// The observation ended while this poll was waiting. Reported as an
			// ended observation, not a failure. Pointer identity also prevents a
			// waiter from crossing into a replacement lease for the same Run.
			buffer.mu.Unlock()
			return nil, ErrObservationInactive
		}
		if live.frame != nil && live.frame.FrameSeq > after {
			copied := *live.frame
			copied.Data = append([]byte(nil), live.frame.Data...)
			buffer.mu.Unlock()
			return &copied, nil
		}
		notify := live.notify
		buffer.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline.C:
			return nil, nil
		case <-notify:
		}
	}
}

func (buffer *observationFrameBuffer) releaseFrameWaiter(live *observationLiveFrame) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	if live.waiters > 0 {
		live.waiters--
	}
}

// closeLease closes a Run's buffer only when the named lease still owns it, so a
// late teardown cannot drop the buffer a successor observation is filling.
func (buffer *observationFrameBuffer) closeLease(runID, leaseID uuid.UUID) int64 {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	// Checked and closed under one lock. Releasing between the two lets a
	// successor observation open for this Run in the gap and be closed by a
	// teardown that was only ever entitled to close its predecessor -- which is
	// the exact bug this lease check exists to prevent.
	if live := buffer.live[runID]; live == nil || live.leaseID != leaseID {
		return 0
	}
	return buffer.closeLocked(runID)
}

// count reports how many observations this instance is holding open, which is
// what the concurrency quota is measured against.
func (buffer *observationFrameBuffer) count() int {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return len(buffer.live)
}
