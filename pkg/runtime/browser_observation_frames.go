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
	frame   *BrowserObservationFrame
	notify  chan struct{}
	count   int64
}

type observationFrameBuffer struct {
	mu   sync.Mutex
	live map[uuid.UUID]*observationLiveFrame
}

func newObservationFrameBuffer() *observationFrameBuffer {
	return &observationFrameBuffer{live: make(map[uuid.UUID]*observationLiveFrame)}
}

func (buffer *observationFrameBuffer) open(runID, leaseID uuid.UUID) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	buffer.live[runID] = &observationLiveFrame{
		leaseID: leaseID,
		notify:  make(chan struct{}),
	}
}

// close wakes every waiter so a viewer polling a finished observation returns
// promptly instead of holding its request open until the timeout.
func (buffer *observationFrameBuffer) close(runID uuid.UUID) int64 {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	live := buffer.live[runID]
	if live == nil {
		return 0
	}
	delete(buffer.live, runID)
	close(live.notify)
	return live.count
}

// publish accepts a frame only for the lease that currently owns the Run and
// only if it advances the sequence. A frame from a superseded lease belongs to
// an observation that has already ended.
func (buffer *observationFrameBuffer) publish(
	runID uuid.UUID,
	leaseID uuid.UUID,
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
	for {
		buffer.mu.Lock()
		live := buffer.live[runID]
		if live == nil {
			buffer.mu.Unlock()
			return nil, errors.New("browser observation is not active")
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
	live := buffer.live[runID]
	if live == nil || live.leaseID != leaseID {
		buffer.mu.Unlock()
		return 0
	}
	buffer.mu.Unlock()
	return buffer.close(runID)
}
