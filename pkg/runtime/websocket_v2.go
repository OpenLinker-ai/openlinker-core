package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

const (
	runtimeV2WSWriteWait         = 10 * time.Second
	runtimeV2WSPongWait          = 75 * time.Second
	runtimeV2WSPingInterval      = 30 * time.Second
	runtimeV2WSClaimInterval     = 500 * time.Millisecond
	runtimeV2WSHeartbeatInterval = 20 * time.Second
	runtimeV2WSCleanupTimeout    = 5 * time.Second
	runtimeV2WSWriteQueue        = 32
)

var runtimeV2WSUpgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin: func(request *http.Request) bool {
		return request != nil && strings.TrimSpace(request.Header.Get("Origin")) == ""
	},
}

// RuntimeV2ResumeService is optional while the HTTP/WS recovery surface is
// being wired. A missing implementation produces a correlated runtime.error;
// it never acknowledges recovery that Core did not durably authorize.
type RuntimeV2ResumeService interface {
	Resume(context.Context, RuntimeSessionPrincipal, RuntimeResumePayload) (RuntimeResumeResponse, error)
}

// WebSocket authenticates both Agent Token and Node certificate before the
// HTTP connection is upgraded. No unauthenticated peer can consume a socket or
// create/attach a durable Session.
func (h *RuntimeV2HTTPController) WebSocket(c echo.Context) error {
	authenticated, transportErr := h.authenticate(c)
	if transportErr != nil {
		return writeRuntimeV2Error(c, transportErr)
	}
	if h == nil || h.dependencies.Sessions == nil || h.dependencies.Leases == nil ||
		h.dependencies.EventStore == nil || h.dependencies.Finalizer == nil {
		return writeRuntimeV2Error(c, runtimeV2UnavailableError())
	}
	if !websocket.IsWebSocketUpgrade(c.Request()) {
		return writeRuntimeV2Error(c, NewRuntimeTransportError(
			RuntimeErrorBadRequest, runtimeErrorDefaultMessage(RuntimeErrorBadRequest),
		))
	}
	if runtimeV2WSUpgrader.CheckOrigin != nil && !runtimeV2WSUpgrader.CheckOrigin(c.Request()) {
		return writeRuntimeV2Error(c, NewRuntimeTransportError(
			RuntimeErrorForbidden, runtimeErrorDefaultMessage(RuntimeErrorForbidden),
		))
	}

	socket, err := runtimeV2WSUpgrader.Upgrade(c.Response().Writer, c.Request(), nil)
	if err != nil {
		// The upgrader has already written the handshake failure.
		return nil
	}
	connection := newRuntimeV2WSConnection(c.Request().Context(), h, socket, authenticated)
	connection.run()
	return nil
}

type runtimeV2WSConnection struct {
	controller      *RuntimeV2HTTPController
	socket          *websocket.Conn
	authenticated   AuthenticatedRuntimePrincipal
	ctx             context.Context
	cancel          context.CancelFunc
	writes          chan runtimeV2WSWriteRequest
	writerDone      chan struct{}
	maintenanceDone chan struct{}
	cleanupOnce     sync.Once

	sessionRequest     RuntimeSessionRequest
	sessionPrincipal   RuntimeSessionPrincipal
	attached           bool
	maintenanceStarted bool

	operationMu   sync.Mutex
	correlationMu sync.Mutex
	assignments   map[uuid.UUID]runtimeV2WSAssignmentCorrelation
}

type runtimeV2WSWriteRequest struct {
	message     any
	controlType int
	controlData []byte
	result      chan error
}

type runtimeV2WSAssignmentCorrelation struct {
	envelope RuntimeEnvelope
	payload  RunAssignedPayload
}

func newRuntimeV2WSConnection(
	parent context.Context,
	controller *RuntimeV2HTTPController,
	socket *websocket.Conn,
	authenticated AuthenticatedRuntimePrincipal,
) *runtimeV2WSConnection {
	ctx, cancel := context.WithCancel(parent)
	return &runtimeV2WSConnection{
		controller:      controller,
		socket:          socket,
		authenticated:   authenticated,
		ctx:             ctx,
		cancel:          cancel,
		writes:          make(chan runtimeV2WSWriteRequest, runtimeV2WSWriteQueue),
		writerDone:      make(chan struct{}),
		maintenanceDone: make(chan struct{}),
		assignments:     make(map[uuid.UUID]runtimeV2WSAssignmentCorrelation),
	}
}

func (c *runtimeV2WSConnection) run() {
	go c.writeLoop()
	defer c.cleanup()

	c.socket.SetPingHandler(func(data string) error {
		return c.writeControl(websocket.PongMessage, []byte(data))
	})
	c.socket.SetCloseHandler(func(int, string) error { return nil })
	if err := c.socket.SetReadDeadline(time.Now().Add(RuntimeHelloTimeoutSeconds * time.Second)); err != nil {
		c.closeForError(runtimeV2UnavailableError())
		return
	}

	helloEnvelope, err := c.readEnvelope()
	if err != nil {
		c.closeForError(mapRuntimeV2HTTPError(err))
		return
	}
	if helloEnvelope.Type != RuntimeMessageHello || helloEnvelope.ReplyToMessageID != nil {
		c.replyErrorAndMaybeClose(helloEnvelope, runtimeV2ValidationError(), true)
		return
	}
	hello, err := DecodeRuntimeMessagePayload[RuntimeHelloPayload](helloEnvelope, RuntimeMessageHello)
	if err != nil {
		c.replyErrorAndMaybeClose(helloEnvelope, err, true)
		return
	}

	c.sessionRequest = runtimeSessionRequestFromHello(hello)
	state, err := c.controller.dependencies.Sessions.CreateOrAttachSession(
		c.ctx, c.authenticated, c.sessionRequest,
	)
	if err != nil {
		c.replyErrorAndMaybeClose(helloEnvelope, err, true)
		return
	}
	c.attached = true
	principal, err := c.controller.dependencies.Sessions.ResolveSessionPrincipal(
		c.ctx, c.authenticated, hello.RuntimeSessionID,
	)
	if err == nil {
		err = validateRuntimeV2ResolvedSession(c.authenticated, principal)
	}
	if err != nil {
		c.replyErrorAndMaybeClose(helloEnvelope, err, true)
		return
	}
	c.sessionPrincipal = principal

	ready, err := runtimeReadyFromSessionState(state)
	if err != nil {
		c.replyErrorAndMaybeClose(helloEnvelope, err, true)
		return
	}
	if err = sendRuntimeV2WSReply(c, helloEnvelope, RuntimeMessageReady, ready); err != nil {
		c.closeForError(mapRuntimeV2HTTPError(err))
		return
	}

	if err = c.socket.SetReadDeadline(time.Now().Add(runtimeV2WSPongWait)); err != nil {
		return
	}
	c.socket.SetPongHandler(func(string) error {
		return c.socket.SetReadDeadline(time.Now().Add(runtimeV2WSPongWait))
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
					c.closeForError(mapRuntimeV2HTTPError(readErr))
				}
			}
			return
		}
		if c.handleEnvelope(envelope) {
			return
		}
	}
}

func (c *runtimeV2WSConnection) readEnvelope() (RuntimeEnvelope, error) {
	messageType, reader, err := c.socket.NextReader()
	if err != nil {
		return RuntimeEnvelope{}, err
	}
	if messageType != websocket.TextMessage {
		return RuntimeEnvelope{}, runtimeV2ValidationError()
	}
	frame, err := io.ReadAll(io.LimitReader(reader, MaxRuntimeV2MessageBytes+1))
	if err != nil {
		return RuntimeEnvelope{}, newRuntimeTransportError(
			RuntimeErrorBadRequest, runtimeErrorDefaultMessage(RuntimeErrorBadRequest), err,
		)
	}
	if int64(len(frame)) > MaxRuntimeV2MessageBytes {
		return RuntimeEnvelope{}, runtimeMessageTooLargeError()
	}
	return ParseRuntimeEnvelope(frame)
}

func (c *runtimeV2WSConnection) handleEnvelope(envelope RuntimeEnvelope) bool {
	if envelope.Type == RuntimeMessageHello {
		c.replyErrorAndMaybeClose(envelope, NewRuntimeTransportError(
			RuntimeErrorSessionConflict, runtimeErrorDefaultMessage(RuntimeErrorSessionConflict),
		), true)
		return true
	}
	if err := c.refreshSession(); err != nil {
		return c.replyErrorAndMaybeClose(envelope, err, true)
	}

	c.operationMu.Lock()
	defer c.operationMu.Unlock()
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
	case RuntimeMessageResume:
		err = c.handleResume(envelope)
	default:
		err = runtimeV2ValidationError()
	}
	if err == nil {
		return false
	}
	return c.replyErrorAndMaybeClose(envelope, err, false)
}

func (c *runtimeV2WSConnection) refreshSession() error {
	principal, err := c.controller.dependencies.Sessions.ResolveSessionPrincipal(
		c.ctx, c.authenticated, c.sessionPrincipal.RuntimeSessionID,
	)
	if err != nil {
		return err
	}
	if err = validateRuntimeV2ResolvedSession(c.authenticated, principal); err != nil {
		return err
	}
	if principal.RuntimeSessionID != c.sessionPrincipal.RuntimeSessionID ||
		principal.WorkerID != c.sessionPrincipal.WorkerID ||
		principal.SessionEpoch != c.sessionPrincipal.SessionEpoch ||
		principal.CoreInstanceID != c.sessionPrincipal.CoreInstanceID {
		return newRuntimeSessionError(RuntimeSessionErrorSessionConflict, nil)
	}
	return nil
}

func (c *runtimeV2WSConnection) handleAssignmentAck(envelope RuntimeEnvelope) error {
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
	return sendRuntimeV2WSReply(c, envelope, RuntimeMessageAssignmentConfirmed, confirmed)
}

func (c *runtimeV2WSConnection) handleAssignmentReject(envelope RuntimeEnvelope) error {
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
	return sendRuntimeV2WSReply(c, envelope, RuntimeMessageAssignmentRejected, rejected)
}

func (c *runtimeV2WSConnection) handleLeaseRenew(envelope RuntimeEnvelope) error {
	if envelope.ReplyToMessageID != nil {
		return runtimeV2ValidationError()
	}
	payload, err := DecodeRuntimeMessagePayload[RunLeaseRenewPayload](envelope, RuntimeMessageLeaseRenew)
	if err != nil {
		return err
	}
	renewed, err := c.controller.dependencies.Leases.RenewLease(c.ctx, c.sessionPrincipal, payload)
	if err != nil {
		return err
	}
	return sendRuntimeV2WSReply(c, envelope, RuntimeMessageLeaseRenewed, renewed)
}

func (c *runtimeV2WSConnection) handleRunEvent(envelope RuntimeEnvelope) error {
	if envelope.ReplyToMessageID != nil {
		return runtimeV2ValidationError()
	}
	payload, err := DecodeRuntimeMessagePayload[RunEventPayload](envelope, RuntimeMessageRunEvent)
	if err != nil {
		return err
	}
	ack, err := c.controller.dependencies.EventStore.Append(
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
	return sendRuntimeV2WSReply(c, envelope, RuntimeMessageRunEventAck, response)
}

func (c *runtimeV2WSConnection) handleRunResult(envelope RuntimeEnvelope) error {
	if envelope.ReplyToMessageID != nil {
		return runtimeV2ValidationError()
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
	return sendRuntimeV2WSReply(c, envelope, RuntimeMessageRunResultAck, response)
}

func (c *runtimeV2WSConnection) handleResume(envelope RuntimeEnvelope) error {
	if envelope.ReplyToMessageID != nil {
		return runtimeV2ValidationError()
	}
	payload, err := DecodeRuntimeMessagePayload[RuntimeResumePayload](envelope, RuntimeMessageResume)
	if err != nil {
		return err
	}
	// The WS reply contract emits one run.resume.accepted message per Attempt.
	// An empty batch would have no correlated reply and leave the caller waiting.
	if len(payload.Attempts) == 0 {
		return runtimeV2ValidationError()
	}
	if c.controller.dependencies.Resume == nil {
		return runtimeV2UnavailableError()
	}
	response, err := c.controller.dependencies.Resume.Resume(c.ctx, c.sessionPrincipal, payload)
	if err != nil {
		return err
	}
	if len(response.Decisions) != len(payload.Attempts) {
		return runtimeV2WSOutboundError(errors.New("runtime resume response count does not match request"))
	}
	for _, decision := range response.Decisions {
		if err = sendRuntimeV2WSReply(c, envelope, RuntimeMessageResumeAccepted, decision); err != nil {
			return err
		}
	}
	return nil
}

func (c *runtimeV2WSConnection) maintenanceLoop() {
	claimTicker := time.NewTicker(runtimeV2WSClaimInterval)
	heartbeatTicker := time.NewTicker(runtimeV2WSHeartbeatInterval)
	defer claimTicker.Stop()
	defer heartbeatTicker.Stop()

	c.claimAndSend()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-claimTicker.C:
			c.claimAndSend()
		case <-heartbeatTicker.C:
			if _, err := c.controller.dependencies.Sessions.HeartbeatSession(
				c.ctx, c.authenticated, c.sessionRequest,
			); err != nil {
				c.closeForError(mapRuntimeV2HTTPError(err))
				return
			}
		}
	}
}

func (c *runtimeV2WSConnection) claimAndSend() {
	c.operationMu.Lock()
	defer c.operationMu.Unlock()
	assignment, err := c.controller.dependencies.Leases.ClaimOffer(c.ctx, c.sessionPrincipal)
	if err != nil {
		mapped := mapRuntimeV2HTTPError(err)
		if _, fatal := RuntimeWebSocketCloseCode(mapped.Body.Code); fatal {
			c.closeForError(mapped)
		}
		return
	}
	if assignment == nil || c.assignmentAlreadySent(assignment.AttemptIdentity) {
		return
	}
	message, envelope, err := newRuntimeV2WSTypedMessage(RuntimeMessageRunAssigned, nil, *assignment)
	if err != nil {
		c.closeForError(runtimeV2WSOutboundError(err))
		return
	}
	c.recordAssignment(envelope, *assignment)
	if err = c.writeMessage(message); err != nil {
		c.removeAssignmentMessage(envelope.MessageID)
		c.cancel()
	}
}

func (c *runtimeV2WSConnection) assignmentCorrelation(
	envelope RuntimeEnvelope,
	identity AttemptIdentity,
) (runtimeV2WSAssignmentCorrelation, error) {
	if envelope.ReplyToMessageID == nil {
		return runtimeV2WSAssignmentCorrelation{}, runtimeV2ValidationError()
	}
	c.correlationMu.Lock()
	correlation, ok := c.assignments[*envelope.ReplyToMessageID]
	c.correlationMu.Unlock()
	if !ok || correlation.payload.AttemptIdentity != identity {
		return runtimeV2WSAssignmentCorrelation{}, runtimeV2ValidationError()
	}
	return correlation, nil
}

func (c *runtimeV2WSConnection) assignmentAlreadySent(identity AttemptIdentity) bool {
	c.correlationMu.Lock()
	defer c.correlationMu.Unlock()
	for _, correlation := range c.assignments {
		if correlation.payload.AttemptIdentity == identity {
			return true
		}
	}
	return false
}

func (c *runtimeV2WSConnection) recordAssignment(envelope RuntimeEnvelope, payload RunAssignedPayload) {
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
	c.assignments[envelope.MessageID] = runtimeV2WSAssignmentCorrelation{envelope: envelope, payload: payload}
	c.correlationMu.Unlock()
}

func (c *runtimeV2WSConnection) removeAssignmentMessage(messageID uuid.UUID) {
	c.correlationMu.Lock()
	delete(c.assignments, messageID)
	c.correlationMu.Unlock()
}

func sendRuntimeV2WSReply[P any](
	c *runtimeV2WSConnection,
	request RuntimeEnvelope,
	messageType RuntimeMessageType,
	payload P,
) error {
	message, envelope, err := newRuntimeV2WSTypedMessage(messageType, &request.MessageID, payload)
	if err != nil {
		return runtimeV2WSOutboundError(err)
	}
	if err = ValidateRuntimeReplyCorrelation(request, envelope); err != nil {
		return runtimeV2WSOutboundError(err)
	}
	return c.writeMessage(message)
}

func newRuntimeV2WSTypedMessage[P any](
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

func runtimeV2WSOutboundError(cause error) *RuntimeTransportError {
	return newRuntimeTransportError(RuntimeErrorInternal, runtimeErrorDefaultMessage(RuntimeErrorInternal), cause)
}

func (c *runtimeV2WSConnection) replyErrorAndMaybeClose(
	request RuntimeEnvelope,
	cause error,
	forceClose bool,
) bool {
	mapped := mapRuntimeV2HTTPError(cause)
	if runtimeMessageExpectsReply(request.Type) {
		if err := sendRuntimeV2WSReply(c, request, RuntimeMessageError, mapped.Body); err != nil {
			c.cancel()
			return true
		}
	}
	closeCode, fatal := RuntimeWebSocketCloseCode(mapped.Body.Code)
	if forceClose && !fatal {
		closeCode, fatal = RuntimeWSCloseProtocolError, true
	}
	if fatal {
		_ = c.writeClose(closeCode, string(mapped.Body.Code))
		c.cancel()
		return true
	}
	return false
}

func (c *runtimeV2WSConnection) closeForError(mapped *RuntimeTransportError) {
	if mapped == nil {
		mapped = NewRuntimeTransportError(RuntimeErrorInternal, runtimeErrorDefaultMessage(RuntimeErrorInternal))
	}
	closeCode, fatal := RuntimeWebSocketCloseCode(mapped.Body.Code)
	if !fatal {
		closeCode = RuntimeWSCloseInternalError
	}
	_ = c.writeClose(closeCode, string(mapped.Body.Code))
	c.cancel()
}

func (c *runtimeV2WSConnection) writeMessage(message any) error {
	return c.enqueueWrite(runtimeV2WSWriteRequest{message: message, result: make(chan error, 1)})
}

func (c *runtimeV2WSConnection) writeControl(messageType int, data []byte) error {
	return c.enqueueWrite(runtimeV2WSWriteRequest{
		controlType: messageType,
		controlData: append([]byte(nil), data...),
		result:      make(chan error, 1),
	})
}

func (c *runtimeV2WSConnection) writeClose(code int, reason string) error {
	return c.writeControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, reason))
}

func (c *runtimeV2WSConnection) enqueueWrite(request runtimeV2WSWriteRequest) error {
	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	case <-c.writerDone:
		return errors.New("runtime v2 websocket writer stopped")
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
			return errors.New("runtime v2 websocket writer stopped")
		}
	case err := <-request.result:
		return err
	}
}

func (c *runtimeV2WSConnection) writeLoop() {
	ticker := time.NewTicker(runtimeV2WSPingInterval)
	defer ticker.Stop()
	defer close(c.writerDone)
	defer c.socket.Close()
	for {
		select {
		case <-c.ctx.Done():
			return
		case request := <-c.writes:
			_ = c.socket.SetWriteDeadline(time.Now().Add(runtimeV2WSWriteWait))
			var err error
			switch {
			case request.message != nil:
				err = c.socket.WriteJSON(request.message)
			case request.controlType != 0:
				err = c.socket.WriteControl(
					request.controlType, request.controlData, time.Now().Add(runtimeV2WSWriteWait),
				)
			default:
				err = errors.New("empty runtime v2 websocket write")
			}
			request.result <- err
			if err != nil || request.controlType == websocket.CloseMessage {
				return
			}
		case <-ticker.C:
			_ = c.socket.SetWriteDeadline(time.Now().Add(runtimeV2WSWriteWait))
			if err := c.socket.WriteControl(
				websocket.PingMessage, nil, time.Now().Add(runtimeV2WSWriteWait),
			); err != nil {
				c.cancel()
				return
			}
		}
	}
}

func (c *runtimeV2WSConnection) cleanup() {
	c.cleanupOnce.Do(func() {
		// Stop claim/heartbeat work before changing durable Session state.
		c.cancel()
		if c.maintenanceStarted {
			select {
			case <-c.maintenanceDone:
			case <-time.After(runtimeV2WSCleanupTimeout):
				log.Warn().Msg("runtime v2 websocket maintenance did not stop before cleanup")
			}
		}
		if c.attached {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), runtimeV2WSCleanupTimeout)
			_, closeErr := c.controller.dependencies.Sessions.CloseSession(
				closeCtx,
				c.authenticated,
				RuntimeSessionCloseRequest{
					RuntimeSessionIdentity: c.sessionRequest.RuntimeSessionIdentity,
					Status:                 "offline",
					Reason:                 "websocket disconnected",
				},
			)
			closeCancel()
			if closeErr != nil {
				log.Warn().Err(closeErr).Msg("runtime v2 websocket close Session")
			}
			// This call is deliberately after durable detach/offline evidence.
			// Lease implementations must use the exact offline cleanup path: only
			// an unaccepted offer may be released; executing Attempts are untouched.
			if c.sessionPrincipal.RuntimeSessionID != uuid.Nil {
				releaseCtx, releaseCancel := context.WithTimeout(context.Background(), runtimeV2WSCleanupTimeout)
				if releaseErr := c.controller.dependencies.Leases.ReleaseUnackedOffer(
					releaseCtx, c.sessionPrincipal, "SESSION_DISCONNECTED",
				); releaseErr != nil {
					log.Warn().Err(releaseErr).Msg("runtime v2 websocket release unacked offer")
				}
				releaseCancel()
			}
		}
		_ = c.socket.Close()
		select {
		case <-c.writerDone:
		case <-time.After(runtimeV2WSWriteWait):
		}
	})
}
