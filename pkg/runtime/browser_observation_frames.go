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
	// Identifies the one downstream viewer allowed to poll. A newer poll takes
	// over from an older one rather than being refused, because a reloaded tab
	// leaves its previous long poll hanging for the full timeout and refusing
	// would lock the viewer out for that whole window.
	waiter int64
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

type observationFrameBuffer struct {
	mu    sync.Mutex
	quota int
	// Injected so the tombstone window can be tested without waiting it out.
	now          func() time.Time
	live         map[uuid.UUID]*observationLiveFrame
	retired      map[uuid.UUID]retiredObservation
	retiredOrder []uuid.UUID
}

func newObservationFrameBuffer(quota int) *observationFrameBuffer {
	return &observationFrameBuffer{
		quota:   quota,
		now:     time.Now,
		live:    make(map[uuid.UUID]*observationLiveFrame),
		retired: make(map[uuid.UUID]retiredObservation),
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
	close(live.notify)
	live.notify = make(chan struct{})
	return nil
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
	live.waiter++
	live.lastPolledAt = buffer.now()
	waiter := live.waiter
	// Wakes the poll being superseded so it returns now instead of holding its
	// request open until the timeout it can no longer be served by.
	close(live.notify)
	live.notify = make(chan struct{})
	buffer.mu.Unlock()
	for {
		buffer.mu.Lock()
		live = buffer.live[runID]
		if live == nil {
			// The observation ended while this poll was waiting. Reported as an
			// ended observation, not a failure: the viewer's next step is to
			// re-read state, not to retry the frame.
			buffer.mu.Unlock()
			return nil, ErrObservationInactive
		}
		if live.waiter != waiter {
			// A newer poll took over. Returning empty rather than erroring lets
			// the superseded request finish as an ordinary idle long poll.
			buffer.mu.Unlock()
			return nil, nil
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
