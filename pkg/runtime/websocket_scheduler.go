package runtime

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
)

type runtimeWSInboundWork struct {
	attemptID uuid.UUID
	envelope  RuntimeEnvelope
}

type runtimeWSProcessLimiter struct {
	permits chan struct{}
}

func newRuntimeWSProcessLimiter(limit int) *runtimeWSProcessLimiter {
	if limit < 1 {
		limit = 1
	}
	return &runtimeWSProcessLimiter{permits: make(chan struct{}, limit)}
}

func (l *runtimeWSProcessLimiter) acquire(ctx context.Context, onWait func()) (func(), error) {
	if l == nil {
		return func() {}, nil
	}
	select {
	case l.permits <- struct{}{}:
		var once sync.Once
		return func() { once.Do(func() { <-l.permits }) }, nil
	default:
		if onWait != nil {
			onWait()
		}
	}
	select {
	case l.permits <- struct{}{}:
		var once sync.Once
		return func() { once.Do(func() { <-l.permits }) }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type runtimeWSInboundSchedulerConfig struct {
	LaneCount      int
	QueueDepth     int
	ProcessLimiter *runtimeWSProcessLimiter
	Handle         func(runtimeWSInboundWork)
	HandlePanic    func()
	Observer       WorkerObserver
}

type runtimeWSInboundScheduler struct {
	ctx            context.Context
	lanes          []chan runtimeWSInboundWork
	processLimiter *runtimeWSProcessLimiter
	handle         func(runtimeWSInboundWork)
	handlePanic    func()
	observer       WorkerObserver
	active         atomic.Int64
	pending        sync.WaitGroup
	workers        sync.WaitGroup
	stopOnce       sync.Once
	stopped        chan struct{}
}

func newRuntimeWSInboundScheduler(ctx context.Context, cfg runtimeWSInboundSchedulerConfig) *runtimeWSInboundScheduler {
	if ctx == nil {
		ctx = context.Background()
	}
	if cfg.LaneCount < 1 {
		cfg.LaneCount = 1
	}
	if cfg.QueueDepth < 1 {
		cfg.QueueDepth = 1
	}
	if cfg.ProcessLimiter == nil {
		cfg.ProcessLimiter = newRuntimeWSProcessLimiter(cfg.LaneCount)
	}
	if cfg.Handle == nil {
		cfg.Handle = func(runtimeWSInboundWork) {}
	}
	scheduler := &runtimeWSInboundScheduler{
		ctx:            ctx,
		lanes:          make([]chan runtimeWSInboundWork, cfg.LaneCount),
		processLimiter: cfg.ProcessLimiter,
		handle:         cfg.Handle,
		handlePanic:    cfg.HandlePanic,
		observer:       cfg.Observer,
		stopped:        make(chan struct{}),
	}
	for index := range scheduler.lanes {
		lane := make(chan runtimeWSInboundWork, cfg.QueueDepth)
		scheduler.lanes[index] = lane
		scheduler.workers.Add(1)
		go scheduler.runLane(lane)
	}
	return scheduler
}

func (s *runtimeWSInboundScheduler) enqueue(work runtimeWSInboundWork) error {
	if s == nil || len(s.lanes) == 0 {
		return errors.New("Runtime WebSocket inbound scheduler is unavailable")
	}
	lane := s.lanes[runtimeWSLaneIndex(work.attemptID, len(s.lanes))]
	s.pending.Add(1)
	messageType := string(work.envelope.Type)
	observeWorker(s.observer, "runtime.websocket.inbound_enqueue", messageType, 1)
	select {
	case lane <- work:
		return nil
	default:
		observeWorker(s.observer, "runtime.websocket.inbound_backpressure", messageType, 1)
	}
	select {
	case lane <- work:
		return nil
	case <-s.ctx.Done():
		s.pending.Done()
		return s.ctx.Err()
	}
}

func (s *runtimeWSInboundScheduler) barrier(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	done := make(chan struct{})
	go func() {
		s.pending.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *runtimeWSInboundScheduler) stop() {
	s.stopWithin(context.Background())
}

func (s *runtimeWSInboundScheduler) stopWithin(ctx context.Context) bool {
	if s == nil {
		return true
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.stopOnce.Do(func() {
		for _, lane := range s.lanes {
			close(lane)
		}
		go func() {
			s.workers.Wait()
			close(s.stopped)
		}()
	})
	select {
	case <-s.stopped:
		return true
	case <-ctx.Done():
		return false
	}
}

func (s *runtimeWSInboundScheduler) runLane(lane <-chan runtimeWSInboundWork) {
	defer s.workers.Done()
	for work := range lane {
		if s.ctx.Err() == nil {
			s.process(work)
		}
		s.pending.Done()
	}
}

func (s *runtimeWSInboundScheduler) process(work runtimeWSInboundWork) {
	messageType := string(work.envelope.Type)
	observeWorker(s.observer, "runtime.websocket.inbound_queue_wait", messageType, 1)
	release, err := s.processLimiter.acquire(s.ctx, func() {
		observeWorker(s.observer, "runtime.websocket.process_backpressure", messageType, 1)
	})
	if err != nil {
		return
	}
	active := s.active.Add(1)
	observeWorker(s.observer, "runtime.websocket.inbound_active", messageType, int(active))
	defer func() {
		s.active.Add(-1)
		release()
		observeWorker(s.observer, "runtime.websocket.inbound_complete", messageType, 1)
		if recovered := recover(); recovered != nil {
			observeWorker(s.observer, "runtime.websocket.inbound_outcome", "panic", 1)
			if s.handlePanic != nil {
				s.handlePanic()
			}
		}
	}()
	s.handle(work)
}

func runtimeWSLaneIndex(attemptID uuid.UUID, lanes int) int {
	if lanes <= 1 {
		return 0
	}
	return int(binary.BigEndian.Uint64(attemptID[8:]) % uint64(lanes))
}

func runtimeWSAttemptID(envelope RuntimeEnvelope) (uuid.UUID, bool, error) {
	var identity AttemptIdentity
	switch envelope.Type {
	case RuntimeMessageAssignmentAck:
		payload, err := DecodeRuntimeMessagePayload[RunAssignmentAckPayload](envelope, envelope.Type)
		if err != nil {
			return uuid.Nil, true, err
		}
		identity = payload.AttemptIdentity
	case RuntimeMessageAssignmentReject:
		payload, err := DecodeRuntimeMessagePayload[RunAssignmentRejectPayload](envelope, envelope.Type)
		if err != nil {
			return uuid.Nil, true, err
		}
		identity = payload.AttemptIdentity
	case RuntimeMessageLeaseRenew:
		payload, err := DecodeRuntimeMessagePayload[RunLeaseRenewPayload](envelope, envelope.Type)
		if err != nil {
			return uuid.Nil, true, err
		}
		identity = payload.AttemptIdentity
	case RuntimeMessageRunEvent:
		payload, err := DecodeRuntimeMessagePayload[RunEventPayload](envelope, envelope.Type)
		if err != nil {
			return uuid.Nil, true, err
		}
		identity = payload.AttemptIdentity
	case RuntimeMessageRunResult:
		payload, err := DecodeRuntimeMessagePayload[RunResultPayload](envelope, envelope.Type)
		if err != nil {
			return uuid.Nil, true, err
		}
		identity = payload.AttemptIdentity
	case RuntimeMessageRunCancelAck:
		payload, err := DecodeRuntimeMessagePayload[RunCancelAckPayload](envelope, envelope.Type)
		if err != nil {
			return uuid.Nil, true, err
		}
		identity = payload.AttemptIdentity
	default:
		return uuid.Nil, false, nil
	}
	return identity.AttemptID, true, nil
}
