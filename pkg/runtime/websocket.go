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

// Shutdown rejects new Runtime WebSockets, interrupts every hijacked
// connection, and waits until each handler has completed durable cleanup.
func (h *RuntimeHTTPController) Shutdown(ctx context.Context) error {
	if h == nil || h.webSockets == nil {
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

	lifecycleMu   sync.RWMutex
	dispatchMu    sync.Mutex
	dispatchState sync.Mutex
	controlMu     sync.Mutex
	correlationMu sync.Mutex
	assignments   map[uuid.UUID]runtimeWSAssignmentCorrelation
	cancellations map[uuid.UUID]runtimeWSCancellationCorrelation

	dispatchPending  bool
	dispatchCredits  int64
	dispatchLimit    int64
	controlPending   atomic.Bool
	dispatchContinue chan struct{}
	controlContinue  chan struct{}
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
		controller:       controller,
		socket:           socket,
		authenticated:    authenticated,
		ctx:              ctx,
		cancel:           cancel,
		writes:           make(chan runtimeWSWriteRequest, runtimeWSWriteQueue),
		writerDone:       make(chan struct{}),
		maintenanceDone:  make(chan struct{}),
		dispatchContinue: make(chan struct{}, 1),
		controlContinue:  make(chan struct{}, 1),
		assignments:      make(map[uuid.UUID]runtimeWSAssignmentCorrelation),
		cancellations:    make(map[uuid.UUID]runtimeWSCancellationCorrelation),
		dispatchLimit:    1,
		connectionID:     "ws:" + uuid.NewString(),
		reportedReason:   reportedReason,
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
	default:
		err = runtimeTransportValidationError()
	}
	if err == nil {
		c.scheduleMaintenanceContinuation(envelope.Type)
		return false
	}
	return c.replyErrorAndMaybeClose(envelope, err, false)
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
	heartbeatTicker := time.NewTicker(runtimeWSHeartbeatInterval)
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

	var dispatchWake, controlWake, nodeDispatchWake <-chan struct{}
	if c.controller.dependencies.WakeHub != nil {
		dispatchWake, _ = c.controller.dependencies.WakeHub.RegisterWebSocketDispatch(
			c.sessionPrincipal.AgentID,
		)
		defer c.controller.dependencies.WakeHub.UnregisterWebSocketDispatch(c.sessionPrincipal.AgentID)
		controlWake = c.controller.dependencies.WakeHub.WaitControl(c.sessionPrincipal.AgentID)
		nodeDispatchWake = c.controller.dependencies.WakeHub.WaitNodeDispatch(c.sessionPrincipal.NodeID)
	}
	// Ready completes attachment without a per-Session database probe. New work
	// arrives through a typed dispatch token; work that predates attachment is
	// recovered by the bounded process-level PostgreSQL reconciliation pass.
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
			c.commandPendingAndSend()
		case <-c.dispatchContinue:
			c.drainDispatchDemand()
		case <-c.controlContinue:
			c.commandPendingAndSend()
		case <-nodeDispatchWake:
			if c.controller.dependencies.WakeHub != nil {
				nodeDispatchWake = c.controller.dependencies.WakeHub.WaitNodeDispatch(c.sessionPrincipal.NodeID)
			}
			c.requestDispatch(1, false, "node_capacity")
		case <-heartbeatTicker.C:
			if !c.heartbeatMaintenanceSession("ticker") {
				return
			}
		}
	}
}

func (c *runtimeWSConnection) heartbeatMaintenanceSession(reason string) bool {
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

func (c *runtimeWSConnection) refreshMaintenanceSession() bool {
	if err := c.refreshSession(); err != nil {
		c.closeForError(mapRuntimeHTTPError(err))
		return false
	}
	return true
}

func (c *runtimeWSConnection) commandPendingAndSend() {
	if !c.controlPending.Load() {
		return
	}
	c.lifecycleMu.RLock()
	defer c.lifecycleMu.RUnlock()
	c.controlMu.Lock()
	defer c.controlMu.Unlock()
	if !c.controlPending.Load() {
		return
	}
	command, _, err := c.controller.dependencies.Cancellations.NextCommand(c.ctx, c.sessionPrincipal)
	if err != nil {
		mapped := mapRuntimeHTTPError(err)
		if _, fatal := RuntimeWebSocketCloseCode(mapped.Body.Code); fatal {
			c.closeForError(mapped)
		}
		return
	}
	if command == nil {
		c.controlPending.Store(false)
		return
	}
	decoded, err := DecodePendingCommand(*command)
	if err != nil {
		c.closeForError(runtimeWSOutboundError(err))
		return
	}
	if decoded.Type != RuntimeMessageRunCancel || decoded.Cancel == nil {
		c.closeForError(runtimeWSOutboundError(errors.New("unexpected Runtime websocket command type")))
		return
	}
	if c.cancellationAlreadySent(*decoded.Cancel) {
		return
	}
	message, envelope, err := newRuntimeWSTypedMessage(RuntimeMessageRunCancel, nil, *decoded.Cancel)
	if err != nil {
		c.closeForError(runtimeWSOutboundError(err))
		return
	}
	c.recordCancellation(envelope, *decoded.Cancel)
	if err = c.writeMessage(message); err != nil {
		c.removeCancellationMessage(envelope.MessageID)
		c.cancel()
	}
}

type runtimeDispatchClaimOutcome uint8

const (
	runtimeDispatchClaimDeferred runtimeDispatchClaimOutcome = iota
	runtimeDispatchClaimed
	runtimeDispatchEmpty
)

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

func (c *runtimeWSConnection) requestDispatch(credits int64, establishDemand bool, reason string) {
	if credits < 1 {
		return
	}
	c.dispatchState.Lock()
	if establishDemand {
		c.dispatchPending = true
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

func (c *runtimeWSConnection) drainDispatchDemand() {
	for c.takeDispatchCredit() {
		switch c.claimPendingAndSend() {
		case runtimeDispatchClaimed:
			continue
		case runtimeDispatchEmpty:
			if c.clearDispatchDemandIfIdle() {
				return
			}
		case runtimeDispatchClaimDeferred:
			return
		}
	}
}

func (c *runtimeWSConnection) claimPendingAndSend() runtimeDispatchClaimOutcome {
	c.lifecycleMu.RLock()
	defer c.lifecycleMu.RUnlock()
	c.dispatchMu.Lock()
	defer c.dispatchMu.Unlock()
	assignment, err := c.controller.dependencies.Leases.ClaimOffer(c.ctx, c.sessionPrincipal)
	if err != nil {
		mapped := mapRuntimeHTTPError(err)
		if _, fatal := RuntimeWebSocketCloseCode(mapped.Body.Code); fatal {
			c.closeForError(mapped)
		}
		return runtimeDispatchClaimDeferred
	}
	if assignment == nil {
		return runtimeDispatchEmpty
	}
	if c.assignmentAlreadySent(assignment.AttemptIdentity) {
		return runtimeDispatchClaimed
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
