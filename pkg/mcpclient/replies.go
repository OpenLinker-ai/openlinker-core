package mcpclient

import (
	"context"
	"sync/atomic"
	"time"
)

const (
	// maxQueuedServerRequests bounds server requests waiting for a reply.
	maxQueuedServerRequests = 16
	// maxServerRequestReplies bounds replies sent in one session.
	maxServerRequestReplies = 64
	// serverRequestReplyTimeout bounds one reply POST.
	serverRequestReplyTimeout = 5 * time.Second
)

// replyWorker answers requests the server sent on an event stream (ping,
// sampling, ...) off the stream's read loop. A server may keep writing to
// the stream, for example a large result, before it acknowledges a reply
// POST; replying inline would leave both sides waiting. One worker answers
// from a bounded queue, each reply has its own deadline, and a session sends
// at most maxServerRequestReplies replies. Requests beyond the queue or the
// budget are dropped.
type replyWorker struct {
	queue chan incoming
	send  func(context.Context, message) error
	// budget is shared by every worker of one session.
	budget  *atomic.Int32
	done    chan struct{}
	closing chan struct{}
}

func newReplyWorker(send func(context.Context, message) error, budget *atomic.Int32) *replyWorker {
	return &replyWorker{
		queue:   make(chan incoming, maxQueuedServerRequests),
		send:    send,
		budget:  budget,
		closing: make(chan struct{}),
	}
}

// enqueue never blocks the read loop.
func (w *replyWorker) enqueue(m incoming) {
	select {
	case <-w.closing:
		return
	default:
	}
	select {
	case w.queue <- m:
	default:
	}
}

// start answers queued requests until ctx ends. Requests queued before
// start wait for it.
func (w *replyWorker) start(ctx context.Context) {
	w.done = make(chan struct{})
	go func() {
		defer close(w.done)
		for {
			select {
			case <-ctx.Done():
				return
			case m := <-w.queue:
				w.reply(ctx, m)
			case <-w.closing:
				for {
					select {
					case m := <-w.queue:
						w.reply(ctx, m)
					default:
						return
					}
				}
			}
		}
	}()
}

func (w *replyWorker) reply(ctx context.Context, m incoming) {
	if ctx.Err() != nil || w.budget.Add(1) > maxServerRequestReplies {
		return
	}
	replyCtx, cancel := context.WithTimeout(ctx, serverRequestReplyTimeout)
	_ = w.send(replyCtx, replyToServerRequest(m))
	cancel()
}

// finish stops intake: the worker answers what is already queued and then
// exits. The caller bounds that with wait and the start context.
func (w *replyWorker) finish() {
	select {
	case <-w.closing:
	default:
		close(w.closing)
	}
}

// wait returns when the started worker has stopped or wait ends; the caller
// stops the worker by ending the context passed to start.
func (w *replyWorker) wait(ctx context.Context) {
	if w.done == nil {
		return
	}
	select {
	case <-w.done:
	case <-ctx.Done():
	}
}
