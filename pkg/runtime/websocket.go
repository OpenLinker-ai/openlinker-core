package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

const (
	runtimeWSWriteWait           = 10 * time.Second
	runtimeWSPongWait            = 75 * time.Second
	runtimeWSPingInterval        = 30 * time.Second
	runtimeWSPolicyCheckInterval = 500 * time.Millisecond
	runtimeWSHeartbeatInterval   = RuntimeHeartbeatInterval
	runtimeWSCleanupTimeout      = 5 * time.Second
	runtimeWSWriteQueue          = 32
)

var runtimeWSUpgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin: func(request *http.Request) bool {
		return request != nil && strings.TrimSpace(request.Header.Get("Origin")) == ""
	},
}

// RuntimeResumeAPI is optional while the HTTP/WS recovery surface is
// being wired. A missing implementation produces a correlated runtime.error;
// it never acknowledges recovery that Core did not durably authorize.
type RuntimeResumeAPI interface {
	Resume(context.Context, RuntimeSessionPrincipal, RuntimeResumePayload) (RuntimeResumeResponse, error)
}

type runtimeWSManagedConnection interface {
	shutdown()
}

// runtimeWSRegistry owns WebSocket admission and draining. An admitted request
// is counted before Upgrade so Shutdown cannot miss a socket that is racing
// between hijack and registration.
type runtimeWSRegistry struct {
	mu          sync.Mutex
	stopping    bool
	inFlight    int
	connections map[runtimeWSManagedConnection]struct{}
	drained     chan struct{}
	drainedOnce sync.Once
}

func newRuntimeWSRegistry() *runtimeWSRegistry {
	return &runtimeWSRegistry{
		connections: make(map[runtimeWSManagedConnection]struct{}),
		drained:     make(chan struct{}),
	}
}

func (r *runtimeWSRegistry) admit() bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopping {
		return false
	}
	r.inFlight++
	return true
}

// register returns false and closes the socket when Shutdown won the race
// after admission. It remains in-flight until the HTTP handler calls finish.
func (r *runtimeWSRegistry) register(connection runtimeWSManagedConnection) bool {
	if r == nil || connection == nil {
		return false
	}
	r.mu.Lock()
	r.connections[connection] = struct{}{}
	accepting := !r.stopping
	r.mu.Unlock()
	if !accepting {
		connection.shutdown()
	}
	return accepting
}

func (r *runtimeWSRegistry) finish(connection runtimeWSManagedConnection) {
	if r == nil {
		return
	}
	r.mu.Lock()
	if connection != nil {
		delete(r.connections, connection)
	}
	if r.inFlight > 0 {
		r.inFlight--
	}
	if r.stopping && r.inFlight == 0 {
		r.drainedOnce.Do(func() { close(r.drained) })
	}
	r.mu.Unlock()
}

func (r *runtimeWSRegistry) shutdown(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	r.mu.Lock()
	r.stopping = true
	connections := make([]runtimeWSManagedConnection, 0, len(r.connections))
	for connection := range r.connections {
		connections = append(connections, connection)
	}
	if r.inFlight == 0 {
		r.drainedOnce.Do(func() { close(r.drained) })
	}
	drained := r.drained
	r.mu.Unlock()

	for _, connection := range connections {
		connection.shutdown()
	}

	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *runtimeWSRegistry) sendBrowserViewerCommand(
	runtimeSessionID uuid.UUID,
	payload BrowserViewerCommandPayload,
) error {
	if r == nil || runtimeSessionID == uuid.Nil {
		return errors.New("Runtime websocket registry is unavailable")
	}
	r.mu.Lock()
	connections := make([]runtimeWSBrowserViewerConnection, 0, 1)
	for connection := range r.connections {
		viewer, ok := connection.(runtimeWSBrowserViewerConnection)
		if ok && viewer.browserViewerRuntimeSessionID() == runtimeSessionID {
			connections = append(connections, viewer)
		}
	}
	r.mu.Unlock()
	if len(connections) != 1 {
		return errors.New("Runtime websocket Viewer connection is unavailable")
	}
	return connections[0].sendBrowserViewerCommand(payload)
}

// sendBrowserObserverCommand mirrors the Viewer path. It is deliberately not
// merged with it: observation and takeover are separate capabilities, and a
// shared send path would make it easy for one to acquire the other's reach.
func (r *runtimeWSRegistry) sendBrowserObserverCommand(
	runtimeSessionID uuid.UUID,
	payload BrowserObserverCommandPayload,
) error {
	if r == nil || runtimeSessionID == uuid.Nil {
		return ErrObservationChannelUnavailable
	}
	r.mu.Lock()
	connections := make([]runtimeWSBrowserObserverConnection, 0, 1)
	for connection := range r.connections {
		observer, ok := connection.(runtimeWSBrowserObserverConnection)
		if ok && observer.browserObserverRuntimeSessionID() == runtimeSessionID {
			connections = append(connections, observer)
		}
	}
	r.mu.Unlock()
	// Zero means the Worker is attached to another Core instance; more than one
	// would mean two connections claim the same Session. Both are unusable, and
	// both must fail closed rather than pick one.
	if len(connections) != 1 {
		return ErrObservationChannelUnavailable
	}
	return connections[0].sendBrowserObserverCommand(payload)
}

type runtimeWSBrowserObserverConnection interface {
	browserObserverRuntimeSessionID() uuid.UUID
	sendBrowserObserverCommand(BrowserObserverCommandPayload) error
}

type runtimeWSBrowserViewerConnection interface {
	browserViewerRuntimeSessionID() uuid.UUID
	sendBrowserViewerCommand(BrowserViewerCommandPayload) error
}

func (h *RuntimeHTTPController) SendBrowserViewerCommand(
	runtimeSessionID uuid.UUID,
	payload BrowserViewerCommandPayload,
) error {
	if h == nil || h.webSockets == nil {
		return errors.New("Runtime websocket Viewer is unavailable")
	}
	if err := ValidateRuntimePayload(payload); err != nil {
		return err
	}
	if payload.AttemptIdentity.RuntimeSessionID != runtimeSessionID {
		return errors.New("Runtime websocket Viewer Session identity mismatch")
	}
	return h.webSockets.sendBrowserViewerCommand(runtimeSessionID, payload)
}

// SendBrowserObserverCommand routes an observation command to the Worker held by
// this process. Frames and wakeups live in process memory, so a command that
// cannot be delivered locally must fail closed here rather than appear to start
// an observation no frame will ever reach.
func (h *RuntimeHTTPController) SendBrowserObserverCommand(
	runtimeSessionID uuid.UUID,
	payload BrowserObserverCommandPayload,
) error {
	if h == nil || h.webSockets == nil {
		return ErrObservationChannelUnavailable
	}
	if err := ValidateRuntimePayload(payload); err != nil {
		return err
	}
	if payload.AttemptIdentity.RuntimeSessionID != runtimeSessionID {
		return errors.New("Runtime websocket observer Session identity mismatch")
	}
	return h.webSockets.sendBrowserObserverCommand(runtimeSessionID, payload)
}

// Shutdown rejects new Runtime HTTP and WebSocket authentication, interrupts
// every hijacked connection, and waits for its durable cleanup. The owning HTTP
// servers must then drain admitted HTTP requests before detaching Sessions.
func (h *RuntimeHTTPController) Shutdown(ctx context.Context) error {
	if h == nil {
		return nil
	}
	h.stopping.Store(true)
	if h.webSockets == nil {
		return nil
	}
	return h.webSockets.shutdown(ctx)
}

// WebSocket authenticates both Agent Token and Node certificate before the
// HTTP connection is upgraded. No unauthenticated peer can consume a socket or
// create/attach a durable Session.
func (h *RuntimeHTTPController) WebSocket(c echo.Context) error {
	authenticated, transportErr := h.authenticate(c)
	if transportErr != nil {
		return writeRuntimeError(c, transportErr)
	}
	if !h.allowRuntimeHTTP(authenticated) {
		return writeRuntimeError(c, runtimeRateLimitedError())
	}
	reportedReason, reasonErr := runtimeFallbackReasonFromRequest(c.Request())
	if reasonErr != nil {
		return writeRuntimeError(c, reasonErr)
	}
	if admissionErr := h.runtimeTransportAdmission(RuntimeTransportWebSocket, false); admissionErr != nil {
		return writeRuntimeError(c, admissionErr)
	}
	if h == nil || h.dependencies.Sessions == nil || (!h.dependencies.AttachOnly &&
		(h.dependencies.Leases == nil || h.dependencies.EventProjector == nil ||
			h.dependencies.Finalizer == nil || h.dependencies.Cancellations == nil)) {
		return writeRuntimeError(c, runtimeUnavailableError())
	}
	if !websocket.IsWebSocketUpgrade(c.Request()) {
		return writeRuntimeError(c, NewRuntimeTransportError(
			RuntimeErrorBadRequest, runtimeErrorDefaultMessage(RuntimeErrorBadRequest),
		))
	}
	if runtimeWSUpgrader.CheckOrigin != nil && !runtimeWSUpgrader.CheckOrigin(c.Request()) {
		return writeRuntimeError(c, NewRuntimeTransportError(
			RuntimeErrorForbidden, runtimeErrorDefaultMessage(RuntimeErrorForbidden),
		))
	}
	releaseAdmission := func() {}
	if h.dependencies.AdmissionLimiter != nil {
		var allowed bool
		releaseAdmission, allowed = h.dependencies.AdmissionLimiter.AcquireWebSocket(
			runtimeAdmissionIdentityFromPrincipal(authenticated),
		)
		if !allowed {
			return writeRuntimeError(c, runtimeRateLimitedError())
		}
		if releaseAdmission == nil {
			releaseAdmission = func() {}
		}
	}
	defer releaseAdmission()
	if h.webSockets == nil || !h.webSockets.admit() {
		return writeRuntimeError(c, runtimeUnavailableError())
	}
	var tracked runtimeWSManagedConnection
	defer func() { h.webSockets.finish(tracked) }()

	socket, err := runtimeWSUpgrader.Upgrade(c.Response().Writer, c.Request(), nil)
	if err != nil {
		// The upgrader has already written the handshake failure.
		return nil
	}
	connection := newRuntimeWSConnection(c.Request().Context(), h, socket, authenticated, reportedReason)
	tracked = connection
	if !h.webSockets.register(connection) {
		return nil
	}
	connection.run()
	return nil
}

type runtimeWSConnection struct {
	controller      *RuntimeHTTPController
	socket          *websocket.Conn
	authenticated   AuthenticatedRuntimePrincipal
	ctx             context.Context
	cancel          context.CancelFunc
	writes          chan runtimeWSWriteRequest
	writerDone      chan struct{}
	maintenanceDone chan struct{}
	cleanupOnce     sync.Once
	revocationWake  <-chan struct{}
	fatalCloseOnce  sync.Once
	inbound         *runtimeWSInboundScheduler

	sessionRequest     RuntimeSessionRequest
	sessionState       RuntimeSessionState
	sessionPrincipal   RuntimeSessionPrincipal
	connectionIdentity RuntimeConnectionIdentity
	attachmentID       uuid.UUID
	attached           bool
	leaseRegistered    bool
	maintenanceStarted bool
	connectionID       string
	reportedReason     RuntimeTransportReason
	// tokenUseRecordedAt is owned by the maintenance goroutine. Connecting
	// authenticated the token, which already recorded its use.
	tokenUseRecordedAt time.Time

	lifecycleMu   sync.RWMutex
	dispatchMu    sync.Mutex
	dispatchState sync.Mutex
	controlMu     sync.Mutex
	correlationMu sync.Mutex
	assignments   map[uuid.UUID]runtimeWSAssignmentCorrelation
	cancellations map[uuid.UUID]runtimeWSCancellationCorrelation

	dispatchPending bool
	dispatchCredits int64
	dispatchLimit   int64
	attachProbe     bool // guarded by dispatchState; next claim is the attachment probe
	probeDemandOnly bool // guarded by dispatchState; demand exists only for the probe
	// dispatchSignalled (guarded by dispatchState) records that a real signal
	// fed the next drain pass; background re-attempts alone never close.
	dispatchSignalled bool
	claimRetrying     bool // guarded by dispatchMu; throttles retry logging
	controlPending    atomic.Bool
	dispatchContinue  chan struct{}
	controlContinue   chan struct{}
}

type runtimeWSWriteRequest struct {
	message     any
	controlType int
	controlData []byte
	result      chan error
}

type runtimeWSAssignmentCorrelation struct {
	envelope RuntimeEnvelope
	payload  RunAssignedPayload
}

type runtimeWSCancellationCorrelation struct {
	envelope RuntimeEnvelope
	payload  RunCancelPayload
}

func newRuntimeWSConnection(
	parent context.Context,
	controller *RuntimeHTTPController,
	socket *websocket.Conn,
	authenticated AuthenticatedRuntimePrincipal,
	reportedReason RuntimeTransportReason,
) *runtimeWSConnection {
	ctx, cancel := context.WithCancel(parent)
	return &runtimeWSConnection{
		controller:         controller,
		socket:             socket,
		authenticated:      authenticated,
		ctx:                ctx,
		cancel:             cancel,
		writes:             make(chan runtimeWSWriteRequest, runtimeWSWriteQueue),
		writerDone:         make(chan struct{}),
		maintenanceDone:    make(chan struct{}),
		dispatchContinue:   make(chan struct{}, 1),
		controlContinue:    make(chan struct{}, 1),
		assignments:        make(map[uuid.UUID]runtimeWSAssignmentCorrelation),
		cancellations:      make(map[uuid.UUID]runtimeWSCancellationCorrelation),
		dispatchLimit:      1,
		connectionID:       "ws:" + uuid.NewString(),
		reportedReason:     reportedReason,
		tokenUseRecordedAt: time.Now(),
	}
}

func (c *runtimeWSConnection) shutdown() {
	if c == nil {
		return
	}
	if c.cancel != nil {
		c.cancel()
	}
	if c.socket != nil {
		_ = c.socket.Close()
	}
}

func (c *runtimeWSConnection) run() {
	go c.writeLoop()
	defer c.cleanup()

	c.socket.SetPingHandler(func(data string) error {
		return c.writeControl(websocket.PongMessage, []byte(data))
	})
	c.socket.SetCloseHandler(func(int, string) error { return nil })
	if err := c.socket.SetReadDeadline(time.Now().Add(RuntimeHelloTimeoutSeconds * time.Second)); err != nil {
		c.closeForError(runtimeUnavailableError())
		return
	}

	helloEnvelope, err := c.readEnvelope()
	if err != nil {
		c.closeForError(mapRuntimeHTTPError(err))
		return
	}
	if helloEnvelope.Type != RuntimeMessageHello || helloEnvelope.ReplyToMessageID != nil {
		c.replyErrorAndMaybeClose(helloEnvelope, runtimeTransportValidationError(), true)
		return
	}
	hello, err := DecodeRuntimeMessagePayload[RuntimeHelloPayload](helloEnvelope, RuntimeMessageHello)
	if err != nil {
		c.replyErrorAndMaybeClose(helloEnvelope, err, true)
		return
	}
	// Re-check after Upgrade so a policy change racing the HTTP admission can
	// never attach a newly forbidden transport.
	if admissionErr := c.controller.runtimeTransportAdmission(RuntimeTransportWebSocket, false); admissionErr != nil {
		c.replyErrorAndMaybeClose(helloEnvelope, admissionErr, true)
		return
	}

	c.sessionRequest = runtimeSessionRequestFromHello(hello)
	c.sessionRequest.Transport = RuntimeTransportWebSocket
	c.sessionRequest.ReportedTransportReason = c.reportedReason
	c.sessionRequest.TransportPolicy = c.controller.currentRuntimeTransportPolicy()
	state, err := c.controller.dependencies.Sessions.CreateOrAttachSession(
		c.ctx, c.authenticated, c.sessionRequest,
	)
	if err != nil {
		c.replyErrorAndMaybeClose(helloEnvelope, err, true)
		return
	}
	if state.Attachment == nil || state.Attachment.ID == uuid.Nil {
		c.replyErrorAndMaybeClose(helloEnvelope, newRuntimeSessionError(RuntimeSessionErrorNotAttached, nil), true)
		return
	}
	c.attached = true
	c.sessionState = state
	c.attachmentID = state.Attachment.ID
	c.sessionRequest.AttachmentID = c.attachmentID
	c.controller.refreshPresence(c.ctx, state, c.connectionID)
	principal, err := c.controller.dependencies.Sessions.ResolveSessionPrincipal(
		c.ctx, c.authenticated, hello.RuntimeSessionID,
	)
	if err == nil {
		err = validateRuntimeResolvedSession(c.authenticated, principal)
	}
	if err == nil && (principal.AttachmentID != c.attachmentID ||
		principal.RuntimeContractDigest != state.Session.RuntimeContractDigest) {
		err = newRuntimeSessionError(RuntimeSessionErrorSessionConflict, nil)
	}
	if err != nil {
		c.replyErrorAndMaybeClose(helloEnvelope, err, true)
		return
	}
	c.sessionPrincipal = principal
	c.connectionIdentity = RuntimeConnectionIdentity{
		RuntimeSessionID: principal.RuntimeSessionID,
		SessionEpoch:     principal.SessionEpoch,
		AttachmentID:     principal.AttachmentID,
	}
	if c.controller.dependencies.WakeHub != nil {
		c.revocationWake = c.controller.dependencies.WakeHub.RegisterConnection(
			c.connectionIdentity,
			principal.CredentialID,
		)
		select {
		case <-c.revocationWake:
			c.closeForError(runtimeUnauthorizedError(errors.New("runtime credential revoked")))
			return
		default:
		}
	}
	if c.controller.dependencies.SessionLeases != nil {
		record, leaseErr := runtimeSessionLeaseRecordFromState(state, c.connectionID)
		if leaseErr == nil {
			leaseErr = c.controller.dependencies.SessionLeases.Register(record)
		}
		if leaseErr != nil {
			log.Warn().Err(leaseErr).Msg("Runtime websocket Session lease registration failed; using database heartbeat")
		} else {
			c.leaseRegistered = true
			if leaseErr = c.controller.dependencies.SessionLeases.RefreshConnection(c.ctx, c.connectionID); leaseErr != nil {
				log.Warn().Err(leaseErr).Msg("Runtime websocket initial Session lease refresh failed; using database heartbeat")
			}
		}
	}

	ready, err := runtimeReadyFromSessionState(state)
	if err != nil {
		c.replyErrorAndMaybeClose(helloEnvelope, err, true)
		return
	}
	if err = sendRuntimeWSReply(c, helloEnvelope, RuntimeMessageReady, ready); err != nil {
		c.closeForError(mapRuntimeHTTPError(err))
		return
	}
	laneCount := int(state.Session.Capacity)
	if laneCount < 1 {
		laneCount = 1
	}
	if laneCount > c.controller.webSocketConfig.ConnectionMaxInflight {
		laneCount = c.controller.webSocketConfig.ConnectionMaxInflight
	}
	c.setDispatchLimit(int64(state.Session.Capacity))
	c.inbound = newRuntimeWSInboundScheduler(c.ctx, runtimeWSInboundSchedulerConfig{
		LaneCount:      laneCount,
		QueueDepth:     c.controller.webSocketConfig.LaneQueueDepth,
		ProcessLimiter: c.controller.webSocketProcesses,
		Handle:         c.handleScheduledEnvelope,
		HandlePanic:    c.handleScheduledPanic,
		Observer:       c.controller.dependencies.Observer,
	})

	if err = c.socket.SetReadDeadline(time.Now().Add(runtimeWSPongWait)); err != nil {
		return
	}
	c.socket.SetPongHandler(func(string) error {
		return c.socket.SetReadDeadline(time.Now().Add(runtimeWSPongWait))
	})
	c.maintenanceStarted = true
	go func() {
		defer close(c.maintenanceDone)
		c.maintenanceLoop()
	}()

	for {
		envelope, readErr := c.readEnvelope()
		if readErr != nil {
			if c.ctx.Err() == nil {
				var closeErr *websocket.CloseError
				if !errors.As(readErr, &closeErr) {
					c.closeForError(mapRuntimeHTTPError(readErr))
				}
			}
			return
		}
		if c.controller.dependencies.AdmissionLimiter != nil &&
			!c.controller.dependencies.AdmissionLimiter.AllowWebSocketMessage(
				runtimeAdmissionIdentityFromPrincipal(c.authenticated),
			) {
			if c.replyErrorAndMaybeClose(envelope, runtimeRateLimitedError(), false) {
				return
			}
			continue
		}
		attemptID, scheduled, identityErr := runtimeWSAttemptID(envelope)
		if identityErr != nil {
			observeWorker(c.controller.dependencies.Observer, "runtime.websocket.inbound_validation", "rejected", 1)
			if c.replyErrorAndMaybeClose(envelope, identityErr, false) {
				return
			}
			continue
		}
		if scheduled {
			observeWorker(c.controller.dependencies.Observer, "runtime.websocket.inbound_validation", "accepted", 1)
			if err = c.inbound.enqueue(runtimeWSInboundWork{attemptID: attemptID, envelope: envelope}); err != nil {
				return
			}
			continue
		}
		if err = c.inbound.barrier(c.ctx); err != nil {
			return
		}
		c.lifecycleMu.Lock()
		closeConnection := c.handleEnvelope(envelope)
		c.lifecycleMu.Unlock()
		if closeConnection {
			return
		}
	}
}

func (c *runtimeWSConnection) handleScheduledEnvelope(work runtimeWSInboundWork) {
	c.lifecycleMu.RLock()
	closeConnection := c.handleEnvelope(work.envelope)
	c.lifecycleMu.RUnlock()
	if closeConnection {
		c.shutdown()
	}
}

func (c *runtimeWSConnection) handleScheduledPanic() {
	log.Error().Msg("Runtime websocket inbound handler panicked")
	c.closeForError(runtimeWSOutboundError(errors.New("Runtime websocket inbound handler panicked")))
}

func (c *runtimeWSConnection) readEnvelope() (RuntimeEnvelope, error) {
	messageType, reader, err := c.socket.NextReader()
	if err != nil {
		return RuntimeEnvelope{}, err
	}
	if messageType != websocket.TextMessage {
		return RuntimeEnvelope{}, runtimeTransportValidationError()
	}
	frame, err := io.ReadAll(io.LimitReader(reader, MaxRuntimeMessageBytes+1))
	if err != nil {
		return RuntimeEnvelope{}, newRuntimeTransportError(
			RuntimeErrorBadRequest, runtimeErrorDefaultMessage(RuntimeErrorBadRequest), err,
		)
	}
	if int64(len(frame)) > MaxRuntimeMessageBytes {
		return RuntimeEnvelope{}, runtimeMessageTooLargeError()
	}
	return ParseRuntimeEnvelope(frame)
}

func (c *runtimeWSConnection) handleEnvelope(envelope RuntimeEnvelope) bool {
	if admissionErr := c.controller.runtimeTransportAdmission(RuntimeTransportWebSocket, true); admissionErr != nil {
		return c.replyErrorAndMaybeClose(envelope, admissionErr, true)
	}
	if envelope.Type == RuntimeMessageHello {
		c.replyErrorAndMaybeClose(envelope, NewRuntimeTransportError(
			RuntimeErrorSessionConflict, runtimeErrorDefaultMessage(RuntimeErrorSessionConflict),
		), true)
		return true
	}
	if err := c.refreshSession(); err != nil {
		return c.replyErrorAndMaybeClose(envelope, err, true)
	}
	if c.controller.dependencies.AttachOnly && envelope.Type != RuntimeMessageDrain {
		return c.replyErrorAndMaybeClose(envelope, runtimeUnavailableError(), false)
	}

	var err error
	switch envelope.Type {
	case RuntimeMessageAssignmentAck:
		err = c.handleAssignmentAck(envelope)
	case RuntimeMessageAssignmentReject:
		err = c.handleAssignmentReject(envelope)
	case RuntimeMessageLeaseRenew:
		err = c.handleLeaseRenew(envelope)
	case RuntimeMessageRunEvent:
		err = c.handleRunEvent(envelope)
	case RuntimeMessageRunResult:
		err = c.handleRunResult(envelope)
	case RuntimeMessageRunCancelAck:
		err = c.handleRunCancelAck(envelope)
	case RuntimeMessageResume:
		err = c.handleResume(envelope)
	case RuntimeMessageDrain:
		err = c.handleDrain(envelope)
	case RuntimeMessageBrowserViewerFrame:
		err = c.handleBrowserViewerFrame(envelope)
	case RuntimeMessageBrowserObserverEvent:
		err = c.handleBrowserObserverEvent(envelope)
	default:
		err = runtimeTransportValidationError()
	}
	if err == nil {
		c.scheduleMaintenanceContinuation(envelope.Type)
		return false
	}
	return c.replyErrorAndMaybeClose(envelope, err, false)
}

// handleBrowserObserverEvent consumes one Worker event and returns its ack. The
// Worker window is a single unacknowledged event, so failing to ack here stops
// the observation rather than dropping one frame.
func (c *runtimeWSConnection) handleBrowserObserverEvent(
	envelope RuntimeEnvelope,
) error {
	if c.controller.dependencies.BrowserObservation == nil {
		return runtimeUnavailableError()
	}
	payload, err := DecodeRuntimeMessagePayload[BrowserObserverEventPayload](
		envelope,
		RuntimeMessageBrowserObserverEvent,
	)
	if err != nil {
		return err
	}
	// The Session on the wire must be the one this connection authenticated as,
	// or a Worker could report frames under another Runtime's identity.
	if payload.AttemptIdentity.RuntimeSessionID !=
		c.sessionPrincipal.RuntimeSessionID {
		return runtimeTransportValidationError()
	}
	ack, err := c.controller.dependencies.BrowserObservation.HandleEvent(
		c.ctx,
		payload,
	)
	if err != nil {
		return runtimeTransportValidationError()
	}
	return sendRuntimeWSReply(
		c,
		envelope,
		RuntimeMessageBrowserObserverEventAck,
		ack,
	)
}

func (c *runtimeWSConnection) handleBrowserViewerFrame(
	envelope RuntimeEnvelope,
) error {
	if c.controller.dependencies.BrowserControl == nil {
		return runtimeUnavailableError()
	}
	payload, err := DecodeRuntimeMessagePayload[BrowserViewerFramePayload](
		envelope,
		RuntimeMessageBrowserViewerFrame,
	)
	if err != nil {
		return err
	}
	if payload.AttemptIdentity.RuntimeSessionID !=
		c.sessionPrincipal.RuntimeSessionID {
		return runtimeTransportValidationError()
	}
	if err := c.controller.dependencies.BrowserControl.PublishFrame(payload); err != nil {
		return runtimeTransportValidationError()
	}
	return sendRuntimeWSReply(
		c,
		envelope,
		RuntimeMessageBrowserViewerFrameAck,
		BrowserViewerFrameAckPayload{
			AttemptIdentity: payload.AttemptIdentity,
			ControlEpoch:    payload.ControlEpoch,
			FrameSeq:        payload.FrameSeq,
		},
	)
}

func (c *runtimeWSConnection) browserViewerRuntimeSessionID() uuid.UUID {
	c.lifecycleMu.RLock()
	defer c.lifecycleMu.RUnlock()
	if !c.attached {
		return uuid.Nil
	}
	return c.sessionPrincipal.RuntimeSessionID
}

func (c *runtimeWSConnection) sendBrowserViewerCommand(
	payload BrowserViewerCommandPayload,
) error {
	c.lifecycleMu.RLock()
	defer c.lifecycleMu.RUnlock()
	if !c.attached ||
		c.sessionPrincipal.RuntimeSessionID !=
			payload.AttemptIdentity.RuntimeSessionID {
		return errors.New("Runtime websocket Viewer Session is unavailable")
	}
	message, _, err := newRuntimeWSTypedMessage(
		RuntimeMessageBrowserViewerCommand,
		nil,
		payload,
	)
	if err != nil {
		return err
	}
	return c.writeMessage(message)
}

func (c *runtimeWSConnection) browserObserverRuntimeSessionID() uuid.UUID {
	c.lifecycleMu.RLock()
	defer c.lifecycleMu.RUnlock()
	if !c.attached {
		return uuid.Nil
	}
	return c.sessionPrincipal.RuntimeSessionID
}

func (c *runtimeWSConnection) sendBrowserObserverCommand(
	payload BrowserObserverCommandPayload,
) error {
	c.lifecycleMu.RLock()
	defer c.lifecycleMu.RUnlock()
	if !c.attached ||
		c.sessionPrincipal.RuntimeSessionID !=
			payload.AttemptIdentity.RuntimeSessionID {
		return ErrObservationChannelUnavailable
	}
	message, _, err := newRuntimeWSTypedMessage(
		RuntimeMessageBrowserObserverCommand,
		nil,
		payload,
	)
	if err != nil {
		return err
	}
	return c.writeMessage(message)
}

func (c *runtimeWSConnection) scheduleMaintenanceContinuation(messageType RuntimeMessageType) {
	switch messageType {
	case RuntimeMessageAssignmentAck:
		c.requestDispatch(1, false, "assignment_ack")
	case RuntimeMessageRunCancelAck:
		if c.controlPending.Load() {
			select {
			case c.controlContinue <- struct{}{}:
			default:
			}
		}
	case RuntimeMessageResume:
		if c.controlPending.Load() {
			select {
			case c.controlContinue <- struct{}{}:
			default:
			}
		}
	}
}

func (c *runtimeWSConnection) refreshSession() error {
	if admissionErr := c.controller.runtimeTransportAdmission(RuntimeTransportWebSocket, true); admissionErr != nil {
		return admissionErr
	}
	observeWorker(c.controller.dependencies.Observer, "runtime.websocket.session_principal_query", "maintenance", 1)
	principal, err := c.controller.dependencies.Sessions.ResolveSessionPrincipal(
		c.ctx, c.authenticated, c.sessionPrincipal.RuntimeSessionID,
	)
	if err != nil {
		return err
	}
	if err = validateRuntimeResolvedSession(c.authenticated, principal); err != nil {
		return err
	}
	if principal.RuntimeSessionID != c.sessionPrincipal.RuntimeSessionID ||
		principal.WorkerID != c.sessionPrincipal.WorkerID ||
		principal.SessionEpoch != c.sessionPrincipal.SessionEpoch ||
		principal.CoreInstanceID != c.sessionPrincipal.CoreInstanceID ||
		principal.AttachmentID != c.attachmentID {
		return newRuntimeSessionError(RuntimeSessionErrorSessionConflict, nil)
	}
	return nil
}

func (c *runtimeWSConnection) handleAssignmentAck(envelope RuntimeEnvelope) error {
	payload, err := DecodeRuntimeMessagePayload[RunAssignmentAckPayload](envelope, RuntimeMessageAssignmentAck)
	if err != nil {
		return err
	}
	correlation, err := c.assignmentCorrelation(envelope, payload.AttemptIdentity)
	if err != nil {
		return err
	}
	if err = ValidateRuntimeReplyCorrelation(correlation.envelope, envelope); err != nil {
		return err
	}
	confirmed, err := c.controller.dependencies.Leases.AckAssignment(c.ctx, c.sessionPrincipal, payload)
	if err != nil {
		return err
	}
	return sendRuntimeWSReply(c, envelope, RuntimeMessageAssignmentConfirmed, confirmed)
}

func (c *runtimeWSConnection) handleAssignmentReject(envelope RuntimeEnvelope) error {
	payload, err := DecodeRuntimeMessagePayload[RunAssignmentRejectPayload](envelope, RuntimeMessageAssignmentReject)
	if err != nil {
		return err
	}
	correlation, err := c.assignmentCorrelation(envelope, payload.AttemptIdentity)
	if err != nil {
		return err
	}
	if err = ValidateRuntimeReplyCorrelation(correlation.envelope, envelope); err != nil {
		return err
	}
	rejected, err := c.controller.dependencies.Leases.RejectAssignment(c.ctx, c.sessionPrincipal, payload)
	if err != nil {
		return err
	}
	return sendRuntimeWSReply(c, envelope, RuntimeMessageAssignmentRejected, rejected)
}

func (c *runtimeWSConnection) handleLeaseRenew(envelope RuntimeEnvelope) error {
	if envelope.ReplyToMessageID != nil {
		return runtimeTransportValidationError()
	}
	payload, err := DecodeRuntimeMessagePayload[RunLeaseRenewPayload](envelope, RuntimeMessageLeaseRenew)
	if err != nil {
		return err
	}
	renewed, err := c.controller.dependencies.Leases.RenewLease(c.ctx, c.sessionPrincipal, payload)
	if err != nil {
		return err
	}
	return sendRuntimeWSReply(c, envelope, RuntimeMessageLeaseRenewed, renewed)
}

func (c *runtimeWSConnection) handleRunEvent(envelope RuntimeEnvelope) error {
	if envelope.ReplyToMessageID != nil {
		return runtimeTransportValidationError()
	}
	payload, err := DecodeRuntimeMessagePayload[RunEventPayload](envelope, RuntimeMessageRunEvent)
	if err != nil {
		return err
	}
	ack, err := c.controller.dependencies.EventProjector.AppendRuntimeEvent(
		c.ctx,
		c.sessionPrincipal.EventPrincipal(),
		payload.AttemptIdentity.RuntimeIdentity(),
		payload.StoreRequest(),
	)
	if err != nil {
		return err
	}
	response := RunEventAckPayload{
		ClientEventID:  ack.ClientEventID,
		ClientEventSeq: ack.ClientEventSeq,
		Sequence:       int64(ack.Sequence),
		Replayed:       ack.Replayed,
	}
	return sendRuntimeWSReply(c, envelope, RuntimeMessageRunEventAck, response)
}

func (c *runtimeWSConnection) handleRunResult(envelope RuntimeEnvelope) error {
	if envelope.ReplyToMessageID != nil {
		return runtimeTransportValidationError()
	}
	payload, err := DecodeRuntimeMessagePayload[RunResultPayload](envelope, RuntimeMessageRunResult)
	if err != nil {
		return err
	}
	request, err := payload.FinalizerRequest()
	if err != nil {
		return err
	}
	ack, err := c.controller.dependencies.Finalizer.Finalize(
		c.ctx, c.sessionPrincipal.EventPrincipal(), request,
	)
	if err != nil {
		return err
	}
	response := RunResultAckPayload{
		ResultID:       ack.ResultID,
		Classification: ack.Classification,
		RunStatus:      RuntimeRunStatus(ack.RunStatus),
		DispatchState:  RuntimeDispatchState(ack.DispatchState),
		Replayed:       ack.Replayed,
		NextAttemptAt:  ack.NextAttemptAt,
	}
	return sendRuntimeWSReply(c, envelope, RuntimeMessageRunResultAck, response)
}

func (c *runtimeWSConnection) handleRunCancelAck(envelope RuntimeEnvelope) error {
	payload, err := DecodeRuntimeMessagePayload[RunCancelAckPayload](envelope, RuntimeMessageRunCancelAck)
	if err != nil {
		return err
	}
	correlation, err := c.cancellationCorrelation(envelope, payload)
	if err != nil {
		return err
	}
	if err = ValidateRuntimeReplyCorrelation(correlation.envelope, envelope); err != nil {
		return err
	}
	_, err = c.controller.dependencies.Cancellations.AckCancel(c.ctx, c.sessionPrincipal, payload)
	if err == nil && runtimeCancellationAckFinalState(payload.CancelState) {
		c.removeCancellationMessage(*envelope.ReplyToMessageID)
	}
	return err
}

func (c *runtimeWSConnection) handleResume(envelope RuntimeEnvelope) error {
	if envelope.ReplyToMessageID != nil {
		return runtimeTransportValidationError()
	}
	payload, err := DecodeRuntimeMessagePayload[RuntimeResumePayload](envelope, RuntimeMessageResume)
	if err != nil {
		return err
	}
	// The WS reply contract emits one run.resume.accepted message per Attempt.
	// An empty batch would have no correlated reply and leave the caller waiting.
	if len(payload.Attempts) == 0 {
		return runtimeTransportValidationError()
	}
	if c.controller.dependencies.Resume == nil {
		return runtimeUnavailableError()
	}
	response, err := c.controller.dependencies.Resume.Resume(c.ctx, c.sessionPrincipal, payload)
	if err != nil {
		return err
	}
	if len(response.Decisions) != len(payload.Attempts) {
		return runtimeWSOutboundError(errors.New("runtime resume response count does not match request"))
	}
	for _, decision := range response.Decisions {
		if err = sendRuntimeWSReply(c, envelope, RuntimeMessageResumeAccepted, decision); err != nil {
			return err
		}
	}
	// Only a Session that actually resumes durable Attempts can own a pending
	// cancellation command. Fresh idle Sessions stay database-silent and rely
	// on the typed run.cancel wake for future work.
	c.controlPending.Store(true)
	return nil
}

func (c *runtimeWSConnection) handleDrain(envelope RuntimeEnvelope) error {
	if envelope.ReplyToMessageID != nil {
		return runtimeTransportValidationError()
	}
	payload, err := DecodeRuntimeMessagePayload[RuntimeDrainPayload](envelope, RuntimeMessageDrain)
	if err != nil {
		return err
	}
	receipt, err := c.controller.dependencies.Sessions.DrainSession(
		c.ctx, c.authenticated, RuntimeSessionDrainRequest{
			RuntimeSessionID: c.sessionPrincipal.RuntimeSessionID,
			AttachmentID:     c.attachmentID,
			Payload:          payload,
		},
	)
	if err != nil {
		return err
	}
	c.sessionPrincipal.Status = "draining"
	return sendRuntimeWSReply(c, envelope, RuntimeMessageDrain, receipt)
}

func (c *runtimeWSConnection) maintenanceLoop() {
	heartbeatInterval := runtimeWSHeartbeatInterval
	if c.controller.webSocketHeartbeat > 0 {
		heartbeatInterval = c.controller.webSocketHeartbeat
	}
	heartbeatTicker := time.NewTicker(heartbeatInterval)
	defer heartbeatTicker.Stop()
	if c.controller.dependencies.AttachOnly {
		for {
			select {
			case <-c.ctx.Done():
				return
			case <-heartbeatTicker.C:
				if !c.heartbeatMaintenanceSession("attach_only") {
					return
				}
			}
		}
	}

	var policyChecks <-chan time.Time
	var policyTicker *time.Ticker
	if c.controller.dependencies.TransportPolicy != nil {
		policyTicker = time.NewTicker(runtimeWSPolicyCheckInterval)
		policyChecks = policyTicker.C
		defer policyTicker.Stop()
	}

	wakeHub := c.controller.dependencies.WakeHub
	agentID := c.sessionPrincipal.AgentID
	dispatchClass := runtimeDispatchEligibilityClass(c.sessionPrincipal.Features)
	var dispatchToken, controlWake, nodeDispatchWake <-chan struct{}
	// The Agent token is shared with sibling Sessions and Pull waiters. A
	// signalled claim that proves this Session cannot take new work hands the
	// token on; any such claim parks (dispatchWake=nil) until the Session's own
	// capacity can change: a Node capacity wake, a pass that makes progress,
	// or the heartbeat tick. Parked Sessions never consume tokens, so
	// ineligible siblings cannot ping-pong.
	var dispatchWake <-chan struct{}
	setDispatchListening := func(listening bool) {
		if listening == (dispatchWake != nil) || dispatchToken == nil {
			return
		}
		if listening {
			dispatchWake = dispatchToken
		} else {
			dispatchWake = nil
		}
		wakeHub.setDispatchListening(agentID, dispatchClass, listening)
	}
	if wakeHub != nil {
		dispatchToken, _ = wakeHub.registerWebSocketDispatch(agentID, dispatchClass)
		dispatchWake = dispatchToken
		defer func() {
			wakeHub.unregisterWebSocketDispatch(agentID, dispatchClass, dispatchWake != nil)
		}()
		controlWake = wakeHub.WaitControl(c.sessionPrincipal.AgentID)
		nodeDispatchWake = wakeHub.WaitNodeDispatch(c.sessionPrincipal.NodeID)
	}
	// Work that predates attachment (Core restart, added Worker) has no signal
	// left to announce it. Probe once, like a Pull request's entry claim: the
	// probe coalesces with any hint already pending for this Session (credits
	// are capped), so attaching costs at most one bounded claim and idle
	// Sessions still never poll. It is Session-local rather than a shared Agent
	// token, so it can never make a sibling claim through a cluster fence, and
	// it tolerates failure (see claimPendingAndSend). The process-level
	// reconciliation pass remains the durable backstop.
	c.requestAttachProbe()
	// Only a pass fed by a real signal may hand a token on: a background
	// re-attempt's token would reach a sibling as if it were a real signal.
	// A background claim checks for a free slot before locking any candidate,
	// so parking silently cannot hide a Run from a sibling.
	parkDispatch := func(handOff bool) {
		setDispatchListening(false)
		if handOff {
			wakeHub.passDispatchClass(agentID, dispatchClass)
		}
	}
	var claimRetry runtimeWSClaimRetry
	defer claimRetry.stop()
	var claimRetryWake <-chan time.Time
	resetClaimRetry := func() {
		claimRetry.reset()
		claimRetryWake = nil
	}
	var retry runtimeCancellationRetry
	defer retry.stop()
	var controlRetry <-chan time.Time
	var contentionCount uint64
	var lastContentionLog time.Time
	logContention := func(state string) {
		if contentionCount == 0 {
			return
		}
		log.Info().Str("category", "runtime.websocket.command_retry").
			Str("reason", "contention").Str("state", state).
			Str("runtime_session_id", c.sessionPrincipal.RuntimeSessionID.String()).
			Uint64("retry_count", contentionCount).
			Msg("Runtime websocket cancellation contention")
	}
	defer func() { logContention("connection_closed") }()
	sendControl := func() {
		retry.stop()
		if c.commandPendingAndSend() {
			controlRetry = retry.wait()
			contentionCount++
			observeWorker(c.controller.dependencies.Observer, "runtime.websocket.command_retry", "contention", 1)
			// Production does not install the test observer. Report the first
			// retry, then at most once per five seconds while contention persists.
			if contentionCount == 1 || time.Since(lastContentionLog) >= 5*time.Second {
				logContention("retrying")
				lastContentionLog = time.Now()
			}
		} else {
			logContention("retry_stopped")
			contentionCount = 0
			retry.reset()
			controlRetry = nil
		}
	}
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.revocationWake:
			c.closeForError(runtimeUnauthorizedError(errors.New("runtime credential revoked")))
			return
		case <-policyChecks:
			observeWorker(c.controller.dependencies.Observer, "runtime.websocket.policy_check", "ticker", 1)
			// Policy providers are in-memory. Preserve fast policy cutover without
			// reintroducing the former per-connection PostgreSQL poll.
			if admissionErr := c.controller.runtimeTransportAdmission(RuntimeTransportWebSocket, true); admissionErr != nil {
				c.closeForError(admissionErr)
				return
			}
		case <-dispatchWake:
			c.requestDispatch(1, true, "run_available")
		case <-controlWake:
			if c.controller.dependencies.WakeHub != nil {
				controlWake = c.controller.dependencies.WakeHub.WaitControl(c.sessionPrincipal.AgentID)
			}
			c.controlPending.Store(true)
			sendControl()
		case <-c.dispatchContinue:
			if claimRetryWake != nil {
				// A transient claim failure is backing off. Hints arriving
				// meanwhile were already recorded as capped credits by
				// requestDispatch; the retry timer serves them in one pass, so
				// signal frequency cannot shorten the backoff.
				break
			}
			outcome, signalled := c.drainDispatchDemand()
			switch outcome {
			case runtimeDispatchNoDemand:
			case runtimeDispatchRetry:
				claimRetryWake = claimRetry.wait()
			case runtimeDispatchIneligible:
				resetClaimRetry()
				parkDispatch(signalled)
			case runtimeDispatchClaimDeferred:
				// Unchanged: another error (possibly closing this socket) is
				// never handed to siblings, so a fenced claim cannot cascade.
				resetClaimRetry()
			default:
				resetClaimRetry()
				setDispatchListening(true)
			}
		case <-claimRetryWake:
			claimRetryWake = nil
			c.requestBackgroundDispatch(1, false, "claim_retry")
		case <-c.controlContinue:
			sendControl()
		case <-controlRetry:
			sendControl()
		case <-nodeDispatchWake:
			if c.controller.dependencies.WakeHub != nil {
				nodeDispatchWake = c.controller.dependencies.WakeHub.WaitNodeDispatch(c.sessionPrincipal.NodeID)
			}
			setDispatchListening(true)
			c.requestDispatch(1, false, "node_capacity")
		case <-heartbeatTicker.C:
			// Bounded safety net for a parked Session whose capacity changed
			// without a delivered Node capacity hint: resume listening and
			// retry its parked demand once. The credit only continues existing
			// demand (never establishes it), and a pending retry backoff still
			// holds the claim until its timer.
			if dispatchToken != nil && dispatchWake == nil {
				setDispatchListening(true)
				c.requestBackgroundDispatch(1, false, "heartbeat")
			}
			if !c.heartbeatMaintenanceSession("ticker") {
				return
			}
		}
	}
}

func (c *runtimeWSConnection) heartbeatMaintenanceSession(reason string) bool {
	c.recordTokenUse()
	if c.leaseRegistered && c.controller.dependencies.SessionLeases != nil &&
		c.controller.dependencies.SessionLeases.HealthyFor(c.connectionID) {
		observeWorker(c.controller.dependencies.Observer, "runtime.websocket.session_heartbeat", "redis_lease", 1)
		return true
	}
	if !c.refreshMaintenanceSession() {
		return false
	}
	observeWorker(c.controller.dependencies.Observer, "runtime.websocket.session_heartbeat", reason, 1)
	state, err := c.controller.dependencies.Sessions.HeartbeatSession(
		c.ctx, c.authenticated, c.sessionRequest,
	)
	if err != nil {
		c.closeForError(mapRuntimeHTTPError(err))
		return false
	}
	c.sessionState = state
	if c.leaseRegistered && c.controller.dependencies.SessionLeases != nil {
		record, recordErr := runtimeSessionLeaseRecordFromState(state, c.connectionID)
		if recordErr == nil && c.controller.dependencies.SessionLeases.UpdatePresence(c.connectionID, record.Presence) {
			if refreshErr := c.controller.dependencies.SessionLeases.RefreshConnection(c.ctx, c.connectionID); refreshErr == nil {
				return true
			}
		}
	}
	c.controller.refreshPresence(c.ctx, state, c.connectionID)
	return true
}

// recordTokenUse keeps the Agent Token's last use current for a long-lived
// WebSocket, which presents the token only when connecting. It runs on every
// heartbeat path, including healthy Redis leases that skip PostgreSQL, but
// writes at most once per agentTokenTouchInterval.
func (c *runtimeWSConnection) recordTokenUse() {
	recorder := c.controller.dependencies.TokenUsage
	if recorder == nil || time.Since(c.tokenUseRecordedAt) < agentTokenTouchInterval {
		return
	}
	c.tokenUseRecordedAt = time.Now()
	recorder.RecordRuntimeTokenUse(c.ctx, c.authenticated.CredentialID)
}

func (c *runtimeWSConnection) refreshMaintenanceSession() bool {
	if err := c.refreshSession(); err != nil {
		c.closeForError(mapRuntimeHTTPError(err))
		return false
	}
	return true
}

// commandPendingAndSend reports whether a contended queue needs a timed retry.
func (c *runtimeWSConnection) commandPendingAndSend() bool {
	if !c.controlPending.Load() {
		return false
	}
	c.lifecycleMu.RLock()
	defer c.lifecycleMu.RUnlock()
	c.controlMu.Lock()
	defer c.controlMu.Unlock()
	if !c.controlPending.Load() {
		return false
	}
	command, _, err := c.controller.dependencies.Cancellations.NextCommand(c.ctx, c.sessionPrincipal)
	if errors.Is(err, errRuntimeCancellationContended) {
		return true
	}
	if err != nil {
		mapped := mapRuntimeHTTPError(err)
		if _, fatal := RuntimeWebSocketCloseCode(mapped.Body.Code); fatal {
			c.closeForError(mapped)
		}
		return false
	}
	if command == nil {
		c.controlPending.Store(false)
		return false
	}
	decoded, err := DecodePendingCommand(*command)
	if err != nil {
		c.closeForError(runtimeWSOutboundError(err))
		return false
	}
	if decoded.Type != RuntimeMessageRunCancel || decoded.Cancel == nil {
		c.closeForError(runtimeWSOutboundError(errors.New("unexpected Runtime websocket command type")))
		return false
	}
	if c.cancellationAlreadySent(*decoded.Cancel) {
		return false
	}
	message, envelope, err := newRuntimeWSTypedMessage(RuntimeMessageRunCancel, nil, *decoded.Cancel)
	if err != nil {
		c.closeForError(runtimeWSOutboundError(err))
		return false
	}
	c.recordCancellation(envelope, *decoded.Cancel)
	if err = c.writeMessage(message); err != nil {
		c.removeCancellationMessage(envelope.MessageID)
		c.cancel()
	}
	return false
}

type runtimeDispatchClaimOutcome uint8

const (
	runtimeDispatchClaimDeferred runtimeDispatchClaimOutcome = iota
	runtimeDispatchClaimed
	runtimeDispatchEmpty
	// runtimeDispatchIneligible means this Session cannot take new work until
	// its own capacity or outstanding offer changes: the Session/Node slot was
	// unavailable, or Core returned the Session's unacknowledged offer again.
	runtimeDispatchIneligible
	// runtimeDispatchRetry means a transient database failure; demand is kept
	// and the claim is retried with bounded backoff instead of closing.
	runtimeDispatchRetry
	// runtimeDispatchNoDemand means a coalesced continuation found no credit.
	runtimeDispatchNoDemand
)

const (
	runtimeWSClaimRetryMin = 100 * time.Millisecond
	runtimeWSClaimRetryMax = 5 * time.Second
)

// runtimeWSClaimRetry is owned by the maintenance loop; no callback outlives it.
type runtimeWSClaimRetry struct {
	timer *time.Timer
	delay time.Duration
}

func (r *runtimeWSClaimRetry) wait() <-chan time.Time {
	if r.delay < runtimeWSClaimRetryMin {
		r.delay = runtimeWSClaimRetryMin
	} else if r.delay < runtimeWSClaimRetryMax {
		r.delay = min(2*r.delay, runtimeWSClaimRetryMax)
	}
	if r.timer == nil {
		r.timer = time.NewTimer(r.delay)
	} else {
		r.timer.Reset(r.delay)
	}
	return r.timer.C
}

func (r *runtimeWSClaimRetry) stop() {
	if r.timer != nil {
		r.timer.Stop()
	}
}

func (r *runtimeWSClaimRetry) reset() {
	r.stop()
	r.delay = 0
}

// runtimeTransientClaimError reports database failures that say nothing about
// the Session, credential or protocol: the claim transaction rolled back and
// can simply be retried. Everything else keeps its existing close mapping.
func runtimeTransientClaimError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	switch pgErr.Code {
	case "40001", // serialization_failure
		"40P01", // deadlock_detected
		"55P03", // lock_not_available
		"57014": // query_canceled (statement or lock timeout)
		return true
	default:
		return false
	}
}

func (c *runtimeWSConnection) setDispatchLimit(limit int64) {
	if limit < 1 {
		limit = 1
	}
	c.dispatchState.Lock()
	c.dispatchLimit = limit
	if c.dispatchCredits > limit {
		c.dispatchCredits = limit
	}
	c.dispatchState.Unlock()
}

// requestDispatch records demand from a real signal: an Agent dispatch hint,
// a Node capacity hint or an assignment ACK continuation.
func (c *runtimeWSConnection) requestDispatch(credits int64, establishDemand bool, reason string) {
	c.addDispatchDemand(credits, establishDemand, true, reason)
}

// requestBackgroundDispatch re-attempts demand without a new signal (attach
// probe, heartbeat resume, claim retry). A pass fed only by such credits never
// closes the connection on a claim error: before these re-attempts existed no
// claim would have run then, and a session may be finishing accepted work.
func (c *runtimeWSConnection) requestBackgroundDispatch(credits int64, establishDemand bool, reason string) {
	c.addDispatchDemand(credits, establishDemand, false, reason)
}

func (c *runtimeWSConnection) addDispatchDemand(credits int64, establishDemand, signalled bool, reason string) {
	if credits < 1 {
		return
	}
	c.dispatchState.Lock()
	if establishDemand {
		c.dispatchPending = true
		c.probeDemandOnly = false
	}
	if signalled && c.dispatchPending {
		c.dispatchSignalled = true
	}
	if c.dispatchPending {
		limit := c.dispatchLimit
		if limit < 1 {
			limit = 1
		}
		if credits > limit-c.dispatchCredits {
			c.dispatchCredits = limit
		} else {
			c.dispatchCredits += credits
		}
	}
	hasDemand := c.dispatchPending && c.dispatchCredits > 0
	creditCount := c.dispatchCredits
	c.dispatchState.Unlock()
	var observer WorkerObserver
	if c.controller != nil {
		observer = c.controller.dependencies.Observer
	}
	observeWorker(observer, "runtime.websocket.dispatch_credit", reason, int(creditCount))
	if hasDemand {
		select {
		case c.dispatchContinue <- struct{}{}:
		default:
		}
	}
}

// requestAttachProbe establishes one unit of demand for the claim made right
// after attachment and marks that claim as the failure-tolerant probe. It runs
// on the maintenance goroutine before its loop, so no claim can intervene.
func (c *runtimeWSConnection) requestAttachProbe() {
	c.requestBackgroundDispatch(1, true, "attach")
	c.dispatchState.Lock()
	c.attachProbe = true
	// Any later hint that establishes demand clears this (requestDispatch).
	c.probeDemandOnly = true
	c.dispatchState.Unlock()
}

func (c *runtimeWSConnection) takeAttachProbe() bool {
	c.dispatchState.Lock()
	defer c.dispatchState.Unlock()
	probe := c.attachProbe
	c.attachProbe = false
	return probe
}

// endAttachProbe settles the probe claim. A failed probe drops the demand it
// created, including credits that only continued it (a Node capacity hint),
// so it is neither retried nor resumed later. Demand that a real hint
// established meanwhile is kept with a credit, so a normal claim serves it.
func (c *runtimeWSConnection) endAttachProbe(failed bool) {
	c.dispatchState.Lock()
	defer c.dispatchState.Unlock()
	if failed {
		if c.probeDemandOnly {
			c.dispatchPending = false
			c.dispatchCredits = 0
		} else if c.dispatchPending && c.dispatchCredits < 1 {
			c.dispatchCredits = 1
		}
	}
	c.probeDemandOnly = false
}

func (c *runtimeWSConnection) takeDispatchCredit() bool {
	c.dispatchState.Lock()
	defer c.dispatchState.Unlock()
	if !c.dispatchPending || c.dispatchCredits < 1 {
		return false
	}
	c.dispatchCredits--
	return true
}

func (c *runtimeWSConnection) clearDispatchDemandIfIdle() bool {
	c.dispatchState.Lock()
	defer c.dispatchState.Unlock()
	if c.dispatchCredits > 0 {
		return false
	}
	c.dispatchPending = false
	return true
}

// drainDispatchDemand returns the outcome that ended the pass, or
// runtimeDispatchClaimed when the pass used every credit on new assignments,
// and whether a real signal fed the pass.
func (c *runtimeWSConnection) drainDispatchDemand() (runtimeDispatchClaimOutcome, bool) {
	signalled := false
	outcome := runtimeDispatchNoDemand
	for c.takeDispatchCredit() {
		// A real signal recorded before this claim (including one that
		// arrived mid-pass) gives it the signalled close handling.
		if c.takeDispatchSignalled() {
			signalled = true
		}
		outcome = c.claimPendingAndSend(signalled)
		switch outcome {
		case runtimeDispatchClaimed:
			continue
		case runtimeDispatchEmpty:
			if c.clearDispatchDemandIfIdle() {
				return outcome, signalled
			}
		case runtimeDispatchRetry:
			if signalled {
				// The retry of a signalled claim keeps its error handling.
				c.markDispatchSignalled()
			}
			return outcome, signalled
		default:
			return outcome, signalled
		}
	}
	return outcome, signalled
}

func (c *runtimeWSConnection) takeDispatchSignalled() bool {
	c.dispatchState.Lock()
	defer c.dispatchState.Unlock()
	signalled := c.dispatchSignalled
	c.dispatchSignalled = false
	return signalled
}

func (c *runtimeWSConnection) markDispatchSignalled() {
	c.dispatchState.Lock()
	c.dispatchSignalled = true
	c.dispatchState.Unlock()
}

func (c *runtimeWSConnection) claimPendingAndSend(signalled bool) (outcome runtimeDispatchClaimOutcome) {
	c.lifecycleMu.RLock()
	defer c.lifecycleMu.RUnlock()
	c.dispatchMu.Lock()
	defer c.dispatchMu.Unlock()
	defer func() { c.claimRetrying = outcome == runtimeDispatchRetry }()
	probe := c.takeAttachProbe()
	claimCtx := c.ctx
	if !signalled {
		// Background claims stop before locking a candidate when this Session
		// has no free slot (see ClaimOffer), so they never hide a Run from a
		// sibling that holds the real hint.
		claimCtx = withRuntimeBackgroundClaim(c.ctx)
	}
	assignment, err := c.controller.dependencies.Leases.ClaimOffer(claimCtx, c.sessionPrincipal)
	if probe {
		c.endAttachProbe(err != nil)
		if err != nil {
			// The attachment probe is opportunistic and never acts on its own
			// error: hard_maintenance fences claims but still admits
			// reattachment so accepted work can finish, so the probe must not
			// turn that fence into a disconnect, a retry loop or a parked
			// Session. Report it as empty; endAttachProbe kept only the demand
			// a real hint established, which the next credit serves normally.
			log.Debug().Err(err).
				Str("runtime_session_id", c.sessionPrincipal.RuntimeSessionID.String()).
				Msg("Runtime websocket attachment probe found no claimable work")
			return runtimeDispatchEmpty
		}
	}
	if err != nil {
		if IsRuntimeLeaseError(err, RuntimeLeaseErrorNodeAtCapacity) {
			return runtimeDispatchIneligible
		}
		if c.ctx.Err() == nil && runtimeTransientClaimError(err) {
			if !c.claimRetrying {
				log.Warn().Err(err).
					Str("runtime_session_id", c.sessionPrincipal.RuntimeSessionID.String()).
					Msg("Runtime websocket claim hit a transient database error; retrying with backoff")
			}
			return runtimeDispatchRetry
		}
		mapped := mapRuntimeHTTPError(err)
		if _, fatal := RuntimeWebSocketCloseCode(mapped.Body.Code); fatal {
			if !signalled {
				// Only a background re-attempt asked for this claim (e.g. a
				// heartbeat resume during hard_maintenance). Keep the demand
				// and the connection; a real signal claims with the existing
				// close handling.
				log.Debug().Err(err).
					Str("runtime_session_id", c.sessionPrincipal.RuntimeSessionID.String()).
					Msg("Runtime websocket background claim deferred")
				return runtimeDispatchClaimDeferred
			}
			c.closeForError(mapped)
		}
		return runtimeDispatchClaimDeferred
	}
	if assignment == nil {
		return runtimeDispatchEmpty
	}
	if c.assignmentAlreadySent(assignment.AttemptIdentity) {
		// Core returns an outstanding unacknowledged offer before considering
		// new work, so this Session cannot claim again until it is resolved.
		return runtimeDispatchIneligible
	}
	message, envelope, err := newRuntimeWSTypedMessage(RuntimeMessageRunAssigned, nil, *assignment)
	if err != nil {
		c.closeForError(runtimeWSOutboundError(err))
		return runtimeDispatchClaimDeferred
	}
	c.recordAssignment(envelope, *assignment)
	if err = c.writeMessage(message); err != nil {
		c.removeAssignmentMessage(envelope.MessageID)
		c.cancel()
		return runtimeDispatchClaimDeferred
	}
	return runtimeDispatchClaimed
}

func (c *runtimeWSConnection) assignmentCorrelation(
	envelope RuntimeEnvelope,
	identity AttemptIdentity,
) (runtimeWSAssignmentCorrelation, error) {
	if envelope.ReplyToMessageID == nil {
		return runtimeWSAssignmentCorrelation{}, runtimeTransportValidationError()
	}
	c.correlationMu.Lock()
	correlation, ok := c.assignments[*envelope.ReplyToMessageID]
	c.correlationMu.Unlock()
	if !ok || correlation.payload.AttemptIdentity != identity {
		return runtimeWSAssignmentCorrelation{}, runtimeTransportValidationError()
	}
	return correlation, nil
}

func (c *runtimeWSConnection) assignmentAlreadySent(identity AttemptIdentity) bool {
	c.correlationMu.Lock()
	defer c.correlationMu.Unlock()
	for _, correlation := range c.assignments {
		if correlation.payload.AttemptIdentity == identity {
			return true
		}
	}
	return false
}

func (c *runtimeWSConnection) recordAssignment(envelope RuntimeEnvelope, payload RunAssignedPayload) {
	c.correlationMu.Lock()
	// Correlation evidence is transport-only, but retaining a bounded history
	// lets an ACK/reject replay recover its persisted response after a lost WS
	// frame. A newly claimed offer proves older entries are no longer the sole
	// unaccepted offer, so bounded eviction cannot mutate durable lease state.
	if len(c.assignments) >= 256 {
		for messageID := range c.assignments {
			delete(c.assignments, messageID)
			break
		}
	}
	c.assignments[envelope.MessageID] = runtimeWSAssignmentCorrelation{envelope: envelope, payload: payload}
	c.correlationMu.Unlock()
}

func (c *runtimeWSConnection) removeAssignmentMessage(messageID uuid.UUID) {
	c.correlationMu.Lock()
	delete(c.assignments, messageID)
	c.correlationMu.Unlock()
}

func (c *runtimeWSConnection) cancellationCorrelation(
	envelope RuntimeEnvelope,
	payload RunCancelAckPayload,
) (runtimeWSCancellationCorrelation, error) {
	if envelope.ReplyToMessageID == nil {
		return runtimeWSCancellationCorrelation{}, runtimeTransportValidationError()
	}
	c.correlationMu.Lock()
	correlation, ok := c.cancellations[*envelope.ReplyToMessageID]
	c.correlationMu.Unlock()
	if !ok || correlation.payload.CancellationID != payload.CancellationID ||
		correlation.payload.AttemptIdentity != payload.AttemptIdentity {
		return runtimeWSCancellationCorrelation{}, runtimeTransportValidationError()
	}
	return correlation, nil
}

func (c *runtimeWSConnection) cancellationAlreadySent(payload RunCancelPayload) bool {
	c.correlationMu.Lock()
	defer c.correlationMu.Unlock()
	for _, correlation := range c.cancellations {
		if correlation.payload.CancellationID == payload.CancellationID &&
			correlation.payload.AttemptIdentity == payload.AttemptIdentity {
			return true
		}
	}
	return false
}

func (c *runtimeWSConnection) recordCancellation(envelope RuntimeEnvelope, payload RunCancelPayload) {
	c.correlationMu.Lock()
	if len(c.cancellations) >= RuntimeMaximumNodeCapacity {
		for messageID := range c.cancellations {
			delete(c.cancellations, messageID)
			break
		}
	}
	c.cancellations[envelope.MessageID] = runtimeWSCancellationCorrelation{
		envelope: envelope,
		payload:  payload,
	}
	c.correlationMu.Unlock()
}

func (c *runtimeWSConnection) removeCancellationMessage(messageID uuid.UUID) {
	c.correlationMu.Lock()
	delete(c.cancellations, messageID)
	c.correlationMu.Unlock()
}

func sendRuntimeWSReply[P any](
	c *runtimeWSConnection,
	request RuntimeEnvelope,
	messageType RuntimeMessageType,
	payload P,
) error {
	message, envelope, err := newRuntimeWSTypedMessage(messageType, &request.MessageID, payload)
	if err != nil {
		return runtimeWSOutboundError(err)
	}
	if err = ValidateRuntimeReplyCorrelation(request, envelope); err != nil {
		return runtimeWSOutboundError(err)
	}
	return c.writeMessage(message)
}

func newRuntimeWSTypedMessage[P any](
	messageType RuntimeMessageType,
	replyTo *uuid.UUID,
	payload P,
) (RuntimeTypedEnvelope[P], RuntimeEnvelope, error) {
	message, err := NewRuntimeTypedMessage(messageType, replyTo, payload)
	if err != nil {
		return RuntimeTypedEnvelope[P]{}, RuntimeEnvelope{}, err
	}
	rawPayload, err := json.Marshal(message.Payload)
	if err != nil {
		return RuntimeTypedEnvelope[P]{}, RuntimeEnvelope{}, err
	}
	envelope := RuntimeEnvelope{RuntimeEnvelopeFields: message.RuntimeEnvelopeFields, Payload: rawPayload}
	if err = ValidateRuntimeEnvelope(envelope); err != nil {
		return RuntimeTypedEnvelope[P]{}, RuntimeEnvelope{}, err
	}
	return message, envelope, nil
}

func runtimeWSOutboundError(cause error) *RuntimeTransportError {
	return newRuntimeTransportError(RuntimeErrorInternal, runtimeErrorDefaultMessage(RuntimeErrorInternal), cause)
}

func (c *runtimeWSConnection) replyErrorAndMaybeClose(
	request RuntimeEnvelope,
	cause error,
	forceClose bool,
) bool {
	mapped := mapRuntimeHTTPError(cause)
	if runtimeMessageExpectsReply(request.Type) {
		if err := sendRuntimeWSReply(c, request, RuntimeMessageError, mapped.Body); err != nil {
			c.cancel()
			return true
		}
	}
	closeCode, fatal := RuntimeWebSocketCloseCode(mapped.Body.Code)
	closeReason := string(mapped.Body.Code)
	if signal, policySignal := runtimePolicySignal(mapped); policySignal {
		closeCode, closeReason, fatal = websocket.ClosePolicyViolation, signal, true
	}
	if forceClose && !fatal {
		closeCode, fatal = RuntimeWSCloseProtocolError, true
	}
	if fatal {
		c.closeTransport(closeCode, closeReason)
		return true
	}
	return false
}

func (c *runtimeWSConnection) closeForError(mapped *RuntimeTransportError) {
	if mapped == nil {
		mapped = NewRuntimeTransportError(RuntimeErrorInternal, runtimeErrorDefaultMessage(RuntimeErrorInternal))
	}
	closeCode, fatal := RuntimeWebSocketCloseCode(mapped.Body.Code)
	closeReason := string(mapped.Body.Code)
	if signal, policySignal := runtimePolicySignal(mapped); policySignal {
		closeCode, closeReason, fatal = websocket.ClosePolicyViolation, signal, true
	}
	if !fatal {
		closeCode = RuntimeWSCloseInternalError
	}
	c.closeTransport(closeCode, closeReason)
}

func (c *runtimeWSConnection) closeTransport(code int, reason string) {
	c.fatalCloseOnce.Do(func() {
		_ = c.writeClose(code, reason)
		c.cancel()
	})
}

func (c *runtimeWSConnection) writeMessage(message any) error {
	return c.enqueueWrite(runtimeWSWriteRequest{message: message, result: make(chan error, 1)})
}

func (c *runtimeWSConnection) writeControl(messageType int, data []byte) error {
	return c.enqueueWrite(runtimeWSWriteRequest{
		controlType: messageType,
		controlData: append([]byte(nil), data...),
		result:      make(chan error, 1),
	})
}

func (c *runtimeWSConnection) writeClose(code int, reason string) error {
	return c.writeControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, reason))
}

func (c *runtimeWSConnection) enqueueWrite(request runtimeWSWriteRequest) error {
	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	case <-c.writerDone:
		return errors.New("Runtime websocket writer stopped")
	case c.writes <- request:
	}
	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	case <-c.writerDone:
		select {
		case err := <-request.result:
			return err
		default:
			return errors.New("Runtime websocket writer stopped")
		}
	case err := <-request.result:
		return err
	}
}

func (c *runtimeWSConnection) writeLoop() {
	ticker := time.NewTicker(runtimeWSPingInterval)
	defer ticker.Stop()
	defer close(c.writerDone)
	defer c.socket.Close()
	for {
		select {
		case <-c.ctx.Done():
			return
		case request := <-c.writes:
			_ = c.socket.SetWriteDeadline(time.Now().Add(runtimeWSWriteWait))
			var err error
			switch {
			case request.message != nil:
				err = c.socket.WriteJSON(request.message)
			case request.controlType != 0:
				err = c.socket.WriteControl(
					request.controlType, request.controlData, time.Now().Add(runtimeWSWriteWait),
				)
			default:
				err = errors.New("empty Runtime websocket write")
			}
			request.result <- err
			if err != nil || request.controlType == websocket.CloseMessage {
				return
			}
		case <-ticker.C:
			_ = c.socket.SetWriteDeadline(time.Now().Add(runtimeWSWriteWait))
			if err := c.socket.WriteControl(
				websocket.PingMessage, nil, time.Now().Add(runtimeWSWriteWait),
			); err != nil {
				c.cancel()
				return
			}
		}
	}
}

func (c *runtimeWSConnection) cleanup() {
	c.cleanupOnce.Do(func() {
		// Stop claim/heartbeat work before changing durable Session state.
		c.cancel()
		if c.controller.dependencies.WakeHub != nil {
			c.controller.dependencies.WakeHub.UnregisterConnection(c.connectionIdentity)
		}
		if c.inbound != nil {
			stopCtx, stopCancel := context.WithTimeout(context.Background(), runtimeWSCleanupTimeout)
			if !c.inbound.stopWithin(stopCtx) {
				log.Warn().Msg("Runtime websocket inbound handlers did not stop before cleanup")
			}
			stopCancel()
		}
		if c.maintenanceStarted {
			select {
			case <-c.maintenanceDone:
			case <-time.After(runtimeWSCleanupTimeout):
				log.Warn().Msg("Runtime websocket maintenance did not stop before cleanup")
			}
		}
		// Ends observations before the Session state changes. The Worker is gone
		// and cannot answer a stop, so its lease will lapse on its own TTL while
		// the audit here would stay active forever.
		if c.controller.dependencies.BrowserObservation != nil &&
			c.sessionPrincipal.RuntimeSessionID != uuid.Nil {
			observationCtx, observationCancel := context.WithTimeout(
				context.Background(), runtimeWSCleanupTimeout,
			)
			if observationErr := c.controller.dependencies.BrowserObservation.CloseSessionObservations(
				observationCtx, c.sessionPrincipal.RuntimeSessionID, "session_disconnected",
			); observationErr != nil {
				log.Warn().Err(observationErr).Msg("Runtime websocket close browser observations")
			}
			observationCancel()
		}
		if c.attached {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), runtimeWSCleanupTimeout)
			detached := false
			state, closeErr := c.controller.dependencies.Sessions.CloseSession(
				closeCtx,
				c.authenticated,
				RuntimeSessionCloseRequest{
					RuntimeSessionIdentity: c.sessionRequest.RuntimeSessionIdentity,
					Status:                 "offline",
					Reason:                 "websocket disconnected",
					AttachmentID:           c.attachmentID,
				},
			)
			if closeErr != nil {
				if !IsRuntimeSessionError(closeErr, RuntimeSessionErrorNotAttached) {
					log.Warn().Err(closeErr).Msg("Runtime websocket close Session")
				}
			} else {
				detached = !state.Replayed
			}
			if c.leaseRegistered && c.controller.dependencies.SessionLeases != nil {
				removed, leaseErr := c.controller.dependencies.SessionLeases.Unregister(
					closeCtx, c.connectionID, c.attachmentID,
				)
				if leaseErr != nil {
					log.Warn().Err(leaseErr).Msg("Runtime websocket remove Session lease")
				} else if !removed {
					c.controller.removePresence(closeCtx, c.sessionState, c.connectionID)
				}
			} else {
				c.controller.removePresence(closeCtx, c.sessionState, c.connectionID)
			}
			closeCancel()
			// This call is deliberately after durable detach/offline evidence.
			// Lease implementations must use the exact offline cleanup path: only
			// an unaccepted offer may be released; executing Attempts are untouched.
			if detached && c.sessionPrincipal.RuntimeSessionID != uuid.Nil &&
				c.controller.dependencies.Leases != nil {
				releaseCtx, releaseCancel := context.WithTimeout(context.Background(), runtimeWSCleanupTimeout)
				if releaseErr := c.controller.dependencies.Leases.ReleaseUnackedOffer(
					releaseCtx, c.sessionPrincipal, "SESSION_DISCONNECTED",
				); releaseErr != nil {
					log.Warn().Err(releaseErr).Msg("Runtime websocket release unacked offer")
				}
				releaseCancel()
			}
		}
		_ = c.socket.Close()
		select {
		case <-c.writerDone:
		case <-time.After(runtimeWSWriteWait):
		}
	})
}
