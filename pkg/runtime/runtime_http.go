package runtime

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

const (
	runtimeTokenScope                       = "agent:pull"
	RuntimeAttachmentIDHeader               = "OpenLinker-Runtime-Attachment"
	RuntimeNodeIDHeader                     = "OpenLinker-Runtime-Node"
	RuntimeFallbackReasonHeader             = "OpenLinker-Runtime-Fallback-Reason"
	runtimeAuthenticatedPrincipalContextKey = "openlinker.runtime.authenticated-principal"
)

// RuntimeTokenValidator authenticates the Agent half of a runtime principal.
// Implementations must check revocation, expiry, and the requested scope.
type RuntimeTokenValidator interface {
	ValidateRuntimeToken(context.Context, string, ...string) (db.AgentRuntimeToken, error)
}

// RuntimeDeviceAuthenticator authenticates the independently enrolled Node
// device. An implementation must use the verified TLS peer certificate, never
// forwarded certificate or identity headers.
type RuntimeDeviceAuthenticator interface {
	AuthenticateHTTP(context.Context, *http.Request) (RuntimeDeviceIdentity, error)
}

// RuntimePrincipalBinder enforces the durable one-to-one relationship between
// an Agent Token Credential and a Runtime Node public key. Token-only transport
// resolves the same Node through this binding instead of trusting request data.
type RuntimePrincipalBinder interface {
	VerifyRuntimePrincipalBinding(context.Context, uuid.UUID, RuntimeDeviceIdentity) error
	ResolveRuntimeDeviceIdentity(context.Context, uuid.UUID) (RuntimeDeviceIdentity, error)
	ResolveTokenOnlyRuntimeDeviceIdentity(context.Context, uuid.UUID, uuid.UUID) (RuntimeDeviceIdentity, error)
}

type RuntimeSessionAPI interface {
	CreateOrAttachSession(context.Context, AuthenticatedRuntimePrincipal, RuntimeSessionRequest) (RuntimeSessionState, error)
	HeartbeatSession(context.Context, AuthenticatedRuntimePrincipal, RuntimeSessionHeartbeatRequest) (RuntimeSessionState, error)
	DrainSession(context.Context, AuthenticatedRuntimePrincipal, RuntimeSessionDrainRequest) (RuntimeDrainPayload, error)
	CloseSession(context.Context, AuthenticatedRuntimePrincipal, RuntimeSessionCloseRequest) (RuntimeSessionState, error)
	ResolveSessionPrincipal(context.Context, AuthenticatedRuntimePrincipal, uuid.UUID) (RuntimeSessionPrincipal, error)
	// ResolveWorkerSessionPrincipal returns the currently acting Session. The
	// source Session ID in an Attempt remains immutable across resume and must
	// not be mistaken for the authenticated uploader.
	ResolveWorkerSessionPrincipal(context.Context, AuthenticatedRuntimePrincipal, string) (RuntimeSessionPrincipal, error)
}

type RuntimeLeaseAPI interface {
	ClaimOffer(context.Context, RuntimeSessionPrincipal) (*RunAssignedPayload, error)
	AckAssignment(context.Context, RuntimeSessionPrincipal, RunAssignmentAckPayload) (RunAssignmentConfirmedPayload, error)
	RejectAssignment(context.Context, RuntimeSessionPrincipal, RunAssignmentRejectPayload) (RunAssignmentRejectedPayload, error)
	RenewLease(context.Context, RuntimeSessionPrincipal, RunLeaseRenewPayload) (RunLeaseRenewedPayload, error)
	ReleaseUnackedOffer(context.Context, RuntimeSessionPrincipal, ...string) error
}

// RuntimeEventProjector is the only execution-event entrypoint exposed to
// transport adapters. Persistence and message/artifact/callback projections
// must stay behind this boundary so WebSocket and Pull cannot diverge.
type RuntimeEventProjector interface {
	AppendRuntimeEvent(context.Context, RuntimeEventPrincipal, RuntimeAttemptIdentity, RuntimeEventRequest) (RuntimeEventAck, error)
}

type RuntimeResultFinalizer interface {
	Finalize(context.Context, RuntimeResultPrincipal, RuntimeResultRequest) (RuntimeResultAck, error)
}

type RuntimeDelegationAPI interface {
	CallAgent(context.Context, RuntimeDelegationAuthorization) (RunSummary, error)
}

type runtimeInvocationDeviceResolver interface {
	ResolveInvocationDevice(context.Context, string) (RuntimeDeviceIdentity, error)
}

type RuntimeCancellationAPI interface {
	NextCommand(context.Context, RuntimeSessionPrincipal) (*PendingCommand, time.Time, error)
	PollCommands(context.Context, RuntimeSessionPrincipal) (RuntimeCommandsResponse, error)
	AckCancel(context.Context, RuntimeSessionPrincipal, RunCancelAckPayload) (RunCancellationState, error)
}

// RuntimeHTTPDependencies are deliberately narrow so the HTTP adapter has
// no database access and cannot reconstruct trusted principals from JSON.
type RuntimeHTTPDependencies struct {
	TokenValidator       RuntimeTokenValidator
	DeviceAuthenticator  RuntimeDeviceAuthenticator
	PrincipalBinder      RuntimePrincipalBinder
	TokenOnlyTransport   bool
	TransportPolicy      RuntimeTransportPolicyProvider
	Sessions             RuntimeSessionAPI
	Leases               RuntimeLeaseAPI
	EventProjector       RuntimeEventProjector
	Finalizer            RuntimeResultFinalizer
	Resume               RuntimeResumeAPI
	Delegation           RuntimeDelegationAPI
	Cancellations        RuntimeCancellationAPI
	WakeHub              *RuntimeWakeHub
	Presence             RuntimePresenceStore
	SessionLeases        *RuntimeSessionLeaseManager
	AdmissionLimiter     RuntimeAdmissionLimiter
	Observer             WorkerObserver
	CoreInstanceID       uuid.UUID
	WebSocketConcurrency RuntimeWebSocketConcurrencyConfig
	// AttachOnly is a release-cutover safety mode. It permits authenticated
	// Session lifecycle traffic, but never claims Runs or accepts execution
	// events/results before the normal Core producer boundary is crossed.
	AttachOnly bool
}

// RuntimeHTTPController is the strict HTTP transport adapter for the durable
// Runtime state machine.
type RuntimeHTTPController struct {
	dependencies       RuntimeHTTPDependencies
	webSockets         *runtimeWSRegistry
	webSocketConfig    RuntimeWebSocketConcurrencyConfig
	webSocketProcesses *runtimeWSProcessLimiter
}

const (
	defaultRuntimeWSConnectionMaxInflight = 8
	defaultRuntimeWSProcessMaxInflight    = 16
	defaultRuntimeWSLaneQueueDepth        = 16
)

// RuntimeWebSocketConcurrencyConfig bounds independent Attempt work without
// changing message ordering or protocol behavior.
type RuntimeWebSocketConcurrencyConfig struct {
	ConnectionMaxInflight int
	ProcessMaxInflight    int
	LaneQueueDepth        int
}

func normalizeRuntimeWebSocketConcurrencyConfig(cfg RuntimeWebSocketConcurrencyConfig) RuntimeWebSocketConcurrencyConfig {
	if cfg.ConnectionMaxInflight < 1 {
		cfg.ConnectionMaxInflight = defaultRuntimeWSConnectionMaxInflight
	}
	if cfg.ProcessMaxInflight < 1 {
		cfg.ProcessMaxInflight = defaultRuntimeWSProcessMaxInflight
	}
	if cfg.LaneQueueDepth < 1 {
		cfg.LaneQueueDepth = defaultRuntimeWSLaneQueueDepth
	}
	return cfg
}

// runtimePreviousReadyPayload is the strict pre-attachment-generation Ready
// shape. It is selected only from the committed Session's database digest;
// request data can never opt a current Session out of its attachment fence.
type runtimePreviousReadyPayload struct {
	CoreInstanceID  string    `json:"core_instance_id" runtime:"required"`
	Features        []string  `json:"features" runtime:"required"`
	OfferTTLSeconds int64     `json:"offer_ttl_seconds" runtime:"required"`
	LeaseTTLSeconds int64     `json:"lease_ttl_seconds" runtime:"required"`
	DatabaseTime    time.Time `json:"database_time" runtime:"required"`
}

func NewRuntimeHTTPController(dependencies RuntimeHTTPDependencies) *RuntimeHTTPController {
	webSocketConfig := normalizeRuntimeWebSocketConcurrencyConfig(dependencies.WebSocketConcurrency)
	return &RuntimeHTTPController{
		dependencies:       dependencies,
		webSockets:         newRuntimeWSRegistry(),
		webSocketConfig:    webSocketConfig,
		webSocketProcesses: newRuntimeWSProcessLimiter(webSocketConfig.ProcessMaxInflight),
	}
}

func newRuntimeHTTPControllerForService(service runtimeService) *RuntimeHTTPController {
	dependencies := RuntimeHTTPDependencies{}
	if validator, ok := any(service).(RuntimeTokenValidator); ok {
		dependencies.TokenValidator = validator
	}
	return NewRuntimeHTTPController(dependencies)
}

// SetRuntimeDependencies completes explicit production wiring. When the token
// validator is omitted, Handler's runtime service supplies authentication.
func (h *Handler) SetRuntimeDependencies(dependencies RuntimeHTTPDependencies) {
	if h == nil {
		return
	}
	if dependencies.TokenValidator == nil {
		if validator, ok := any(h.svc).(RuntimeTokenValidator); ok {
			dependencies.TokenValidator = validator
		}
	}
	h.runtime = NewRuntimeHTTPController(dependencies)
}

// RuntimeController exposes the transport lifecycle owner so the process can
// drain hijacked WebSocket connections before shutting down its HTTP servers.
func (h *Handler) RuntimeController() *RuntimeHTTPController {
	if h == nil {
		return nil
	}
	return h.runtime
}

func (h *RuntimeHTTPController) Register(api *echo.Group) {
	if api == nil {
		return
	}
	if h == nil {
		h = NewRuntimeHTTPController(RuntimeHTTPDependencies{})
	}
	api.POST("/agent-runtime/sessions", h.runtimeTransportEndpoint(
		RuntimeTransportLongPoll, false, h.CreateSession,
	))
	api.POST("/agent-runtime/sessions/:id/heartbeat", h.runtimeTransportEndpoint(
		RuntimeTransportLongPoll, true, h.HeartbeatSession,
	))
	api.POST("/agent-runtime/sessions/:id/drain", h.runtimeTransportEndpoint(
		RuntimeTransportLongPoll, true, h.DrainSession,
	))
	api.POST("/agent-runtime/sessions/:id/close", h.runtimeTransportEndpoint(
		RuntimeTransportLongPoll, true, h.CloseSession,
	))
	api.POST("/agent-runtime/runs/claim", h.runtimeTransportEndpoint(
		RuntimeTransportLongPoll, true, h.ClaimRun,
	))
	api.POST("/agent-runtime/runs/:id/assignment-ack", h.runtimeTransportEndpoint(
		RuntimeTransportLongPoll, true, h.AckAssignment,
	))
	api.POST("/agent-runtime/runs/:id/assignment-reject", h.runtimeTransportEndpoint(
		RuntimeTransportLongPoll, true, h.RejectAssignment,
	))
	api.POST("/agent-runtime/runs/:id/lease-renew", h.runtimeTransportEndpoint(
		RuntimeTransportLongPoll, true, h.RenewLease,
	))
	api.POST("/agent-runtime/runs/:id/events", h.runtimeTransportEndpoint(
		RuntimeTransportLongPoll, true, h.AppendEvent,
	))
	api.POST("/agent-runtime/runs/:id/result", h.runtimeTransportEndpoint(
		RuntimeTransportLongPoll, true, h.FinalizeResult,
	))
	api.POST("/agent-runtime/runs/resume", h.runtimeTransportEndpoint(
		RuntimeTransportLongPoll, true, h.ResumeRuns,
	))
	api.POST("/agent-runtime/runs/:id/cancel-ack", h.runtimeTransportEndpoint(
		RuntimeTransportLongPoll, true, h.AckCancel,
	))
	api.GET("/agent-runtime/commands", h.runtimeTransportEndpoint(
		RuntimeTransportLongPoll, true, h.PollCommands,
	))
	// call-agent is an assignment-scoped auxiliary HTTP operation used by both
	// connection modes; it is not a long-poll attachment endpoint.
	api.POST("/agent-runtime/call-agent", h.CallAgent)
	api.GET("/agent-runtime/ws", h.WebSocket)
}

// RegisterAttachOnly mounts only the Runtime Session lifecycle required to
// prove SDK connectivity during a release cutover. Execution, command, resume,
// result, event, and delegation routes deliberately remain absent.
func (h *RuntimeHTTPController) RegisterAttachOnly(api *echo.Group) {
	if api == nil {
		return
	}
	if h == nil {
		h = NewRuntimeHTTPController(RuntimeHTTPDependencies{AttachOnly: true})
	}
	h.dependencies.AttachOnly = true
	api.POST("/agent-runtime/sessions", h.runtimeTransportEndpoint(
		RuntimeTransportLongPoll, false, h.CreateSession,
	))
	api.POST("/agent-runtime/sessions/:id/heartbeat", h.runtimeTransportEndpoint(
		RuntimeTransportLongPoll, true, h.HeartbeatSession,
	))
	api.POST("/agent-runtime/sessions/:id/drain", h.runtimeTransportEndpoint(
		RuntimeTransportLongPoll, true, h.DrainSession,
	))
	api.POST("/agent-runtime/sessions/:id/close", h.runtimeTransportEndpoint(
		RuntimeTransportLongPoll, true, h.CloseSession,
	))
	api.GET("/agent-runtime/ws", h.WebSocket)
}

func (h *RuntimeHTTPController) runtimeTransportEndpoint(
	transport RuntimeTransport,
	established bool,
	next echo.HandlerFunc,
) echo.HandlerFunc {
	return func(c echo.Context) error {
		principal, authenticationErr := h.authenticate(c)
		if authenticationErr != nil {
			return writeRuntimeError(c, authenticationErr)
		}
		if !h.allowRuntimeHTTP(principal) {
			return writeRuntimeError(c, runtimeRateLimitedError())
		}
		if admissionErr := h.runtimeTransportAdmission(transport, established); admissionErr != nil {
			return writeRuntimeError(c, admissionErr)
		}
		return next(c)
	}
}

func (h *RuntimeHTTPController) allowRuntimeHTTP(principal AuthenticatedRuntimePrincipal) bool {
	if h == nil || h.dependencies.AdmissionLimiter == nil {
		return true
	}
	return h.dependencies.AdmissionLimiter.AllowHTTP(runtimeAdmissionIdentityFromPrincipal(principal))
}

func (h *RuntimeHTTPController) runtimeTransportAdmission(
	transport RuntimeTransport,
	established bool,
) *RuntimeTransportError {
	policy := h.currentRuntimeTransportPolicy()
	if runtimeTransportAllowed(policy, transport) {
		return nil
	}
	if established {
		return runtimePolicyChangedError()
	}
	return runtimeTransportForbiddenError()
}

func (h *RuntimeHTTPController) currentRuntimeTransportPolicy() RuntimeTransportPolicy {
	policy := CurrentRuntimeTransportPolicy()
	if h != nil && h.dependencies.TransportPolicy != nil {
		policy = h.dependencies.TransportPolicy()
	}
	return effectiveRuntimeTransportPolicy(policy)
}

func (h *RuntimeHTTPController) CreateSession(c echo.Context) error {
	principal, transportErr := h.authenticate(c)
	if transportErr != nil {
		return writeRuntimeError(c, transportErr)
	}
	if h.dependencies.Sessions == nil {
		return writeRuntimeError(c, runtimeUnavailableError())
	}
	reportedReason, reasonErr := runtimeFallbackReasonFromRequest(c.Request())
	if reasonErr != nil {
		return writeRuntimeError(c, reasonErr)
	}
	hello, err := DecodeRuntimeBody[RuntimeHelloPayload](c.Request().Body)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	request := runtimeSessionRequestFromHello(hello)
	request.Transport = RuntimeTransportLongPoll
	request.ReportedTransportReason = reportedReason
	request.TransportPolicy = h.currentRuntimeTransportPolicy()
	if !runtimeTransportAllowed(request.TransportPolicy, request.Transport) {
		return writeRuntimeError(c, runtimeTransportForbiddenError())
	}
	state, err := h.dependencies.Sessions.CreateOrAttachSession(c.Request().Context(), principal, request)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	h.refreshPresence(c.Request().Context(), state, "pull:"+state.Session.RuntimeSessionID.String())
	ready, err := runtimeReadyFromSessionState(state)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	return writeRuntimePayload(c, http.StatusOK, ready)
}

func (h *RuntimeHTTPController) HeartbeatSession(c echo.Context) error {
	principal, transportErr := h.authenticate(c)
	if transportErr != nil {
		return writeRuntimeError(c, transportErr)
	}
	if h.dependencies.Sessions == nil {
		return writeRuntimeError(c, runtimeUnavailableError())
	}
	sessionID, err := parseRuntimePathUUID(c.Param("id"))
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	hello, err := DecodeRuntimeBody[RuntimeHelloPayload](c.Request().Body)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	if hello.RuntimeSessionID != sessionID {
		return writeRuntimeError(c, runtimeTransportValidationError())
	}
	attachmentID, err := h.runtimeAttachmentIDForSessionRequest(c, principal, sessionID)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	request := runtimeSessionRequestFromHello(hello)
	request.AttachmentID = attachmentID
	request.Transport = RuntimeTransportLongPoll
	state, err := h.dependencies.Sessions.HeartbeatSession(
		c.Request().Context(), principal, request,
	)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	h.refreshPresence(c.Request().Context(), state, "pull:"+state.Session.RuntimeSessionID.String())
	ready, err := runtimeReadyFromSessionState(state)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	return writeRuntimePayload(c, http.StatusOK, ready)
}

func (h *RuntimeHTTPController) CloseSession(c echo.Context) error {
	principal, transportErr := h.authenticate(c)
	if transportErr != nil {
		return writeRuntimeError(c, transportErr)
	}
	if h.dependencies.Sessions == nil {
		return writeRuntimeError(c, runtimeUnavailableError())
	}
	sessionID, err := parseRuntimePathUUID(c.Param("id"))
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	request, err := decodeRuntimeSessionClose(c.Request(), principal)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	if request.RuntimeSessionID != sessionID {
		return writeRuntimeError(c, runtimeTransportValidationError())
	}
	request.AttachmentID, err = h.runtimeAttachmentIDForSessionRequest(c, principal, sessionID)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	state, err := h.dependencies.Sessions.CloseSession(c.Request().Context(), principal, request)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	h.removePresence(c.Request().Context(), state, "pull:"+state.Session.RuntimeSessionID.String())
	return c.NoContent(http.StatusNoContent)
}

// DrainSession is the Pull half of the server-authoritative drain handshake.
// The payload cannot establish identity or capacity: Core authenticates the
// path Session and attachment, atomically commits draining/capacity=0, then
// returns only the database receipt.
func (h *RuntimeHTTPController) DrainSession(c echo.Context) error {
	principal, transportErr := h.authenticate(c)
	if transportErr != nil {
		return writeRuntimeError(c, transportErr)
	}
	if h.dependencies.Sessions == nil {
		return writeRuntimeError(c, runtimeUnavailableError())
	}
	sessionID, err := parseRuntimePathUUID(c.Param("id"))
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	payload, err := DecodeRuntimeBody[RuntimeDrainPayload](c.Request().Body)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	attachmentID, err := h.runtimeAttachmentIDForSessionRequest(c, principal, sessionID)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	receipt, err := h.dependencies.Sessions.DrainSession(
		c.Request().Context(), principal, RuntimeSessionDrainRequest{
			RuntimeSessionID: sessionID,
			AttachmentID:     attachmentID,
			Payload:          payload,
		},
	)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	return writeRuntimePayload(c, http.StatusOK, receipt)
}

func (h *RuntimeHTTPController) ClaimRun(c echo.Context) error {
	authenticated, transportErr := h.authenticate(c)
	if transportErr != nil {
		return writeRuntimeError(c, transportErr)
	}
	if h.dependencies.Sessions == nil || h.dependencies.Leases == nil {
		return writeRuntimeError(c, runtimeUnavailableError())
	}
	wait, err := parseRuntimeWait(c.QueryParam("wait"))
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	request, err := DecodeRuntimeBody[RuntimeClaimRequest](c.Request().Body)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	if !validRuntimeCapacityReport(request.Capacity, request.Inflight) {
		return writeRuntimeError(c, runtimeTransportValidationError())
	}
	principal, err := h.resolveSession(c, authenticated, request.RuntimeSessionID)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	assignment, err := h.claimWithWait(c.Request().Context(), principal, wait)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	if assignment == nil {
		return c.NoContent(http.StatusNoContent)
	}
	return writeRuntimePayload(c, http.StatusOK, *assignment)
}

func (h *RuntimeHTTPController) AckAssignment(c echo.Context) error {
	authenticated, transportErr := h.authenticate(c)
	if transportErr != nil {
		return writeRuntimeError(c, transportErr)
	}
	if h.dependencies.Sessions == nil || h.dependencies.Leases == nil {
		return writeRuntimeError(c, runtimeUnavailableError())
	}
	runID, err := parseRuntimePathUUID(c.Param("id"))
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	request, err := DecodeRuntimeBody[RunAssignmentAckPayload](c.Request().Body)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	if request.AttemptIdentity.RunID != runID {
		return writeRuntimeError(c, runtimeTransportValidationError())
	}
	principal, err := h.resolveSession(c, authenticated, request.AttemptIdentity.RuntimeSessionID)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	response, err := h.dependencies.Leases.AckAssignment(c.Request().Context(), principal, request)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	return writeRuntimePayload(c, http.StatusOK, response)
}

func (h *RuntimeHTTPController) RejectAssignment(c echo.Context) error {
	authenticated, transportErr := h.authenticate(c)
	if transportErr != nil {
		return writeRuntimeError(c, transportErr)
	}
	if h.dependencies.Sessions == nil || h.dependencies.Leases == nil {
		return writeRuntimeError(c, runtimeUnavailableError())
	}
	runID, err := parseRuntimePathUUID(c.Param("id"))
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	request, err := DecodeRuntimeBody[RunAssignmentRejectPayload](c.Request().Body)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	if request.AttemptIdentity.RunID != runID {
		return writeRuntimeError(c, runtimeTransportValidationError())
	}
	principal, err := h.resolveSession(c, authenticated, request.AttemptIdentity.RuntimeSessionID)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	response, err := h.dependencies.Leases.RejectAssignment(c.Request().Context(), principal, request)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	return writeRuntimePayload(c, http.StatusOK, response)
}

func (h *RuntimeHTTPController) RenewLease(c echo.Context) error {
	authenticated, transportErr := h.authenticate(c)
	if transportErr != nil {
		return writeRuntimeError(c, transportErr)
	}
	if h.dependencies.Sessions == nil || h.dependencies.Leases == nil {
		return writeRuntimeError(c, runtimeUnavailableError())
	}
	runID, err := parseRuntimePathUUID(c.Param("id"))
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	request, err := DecodeRuntimeBody[RunLeaseRenewPayload](c.Request().Body)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	if request.AttemptIdentity.RunID != runID {
		return writeRuntimeError(c, runtimeTransportValidationError())
	}
	principal, err := h.resolveSession(c, authenticated, request.AttemptIdentity.RuntimeSessionID)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	response, err := h.dependencies.Leases.RenewLease(c.Request().Context(), principal, request)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	return writeRuntimePayload(c, http.StatusOK, response)
}

func (h *RuntimeHTTPController) AppendEvent(c echo.Context) error {
	authenticated, transportErr := h.authenticate(c)
	if transportErr != nil {
		return writeRuntimeError(c, transportErr)
	}
	if h.dependencies.Sessions == nil || h.dependencies.EventProjector == nil {
		return writeRuntimeError(c, runtimeUnavailableError())
	}
	runID, err := parseRuntimePathUUID(c.Param("id"))
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	request, err := DecodeRuntimeBody[RunEventPayload](c.Request().Body)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	if request.AttemptIdentity.RunID != runID {
		return writeRuntimeError(c, runtimeTransportValidationError())
	}
	principal, err := h.resolveEventResultSession(c, authenticated, request.AttemptIdentity.WorkerID)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	ack, err := h.dependencies.EventProjector.AppendRuntimeEvent(
		c.Request().Context(), principal.EventPrincipal(), request.AttemptIdentity.RuntimeIdentity(), request.StoreRequest(),
	)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	response := RunEventAckPayload{
		ClientEventID:  ack.ClientEventID,
		ClientEventSeq: ack.ClientEventSeq,
		Sequence:       int64(ack.Sequence),
		Replayed:       ack.Replayed,
	}
	return writeRuntimePayload(c, http.StatusOK, response)
}

func (h *RuntimeHTTPController) FinalizeResult(c echo.Context) error {
	authenticated, transportErr := h.authenticate(c)
	if transportErr != nil {
		return writeRuntimeError(c, transportErr)
	}
	if h.dependencies.Sessions == nil || h.dependencies.Finalizer == nil {
		return writeRuntimeError(c, runtimeUnavailableError())
	}
	runID, err := parseRuntimePathUUID(c.Param("id"))
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	payload, err := DecodeRuntimeBody[RunResultPayload](c.Request().Body)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	if payload.AttemptIdentity.RunID != runID {
		return writeRuntimeError(c, runtimeTransportValidationError())
	}
	principal, err := h.resolveEventResultSession(c, authenticated, payload.AttemptIdentity.WorkerID)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	request, err := payload.FinalizerRequest()
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	ack, err := h.dependencies.Finalizer.Finalize(c.Request().Context(), principal.EventPrincipal(), request)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	response := RunResultAckPayload{
		ResultID:       ack.ResultID,
		Classification: ack.Classification,
		RunStatus:      RuntimeRunStatus(ack.RunStatus),
		DispatchState:  RuntimeDispatchState(ack.DispatchState),
		Replayed:       ack.Replayed,
		NextAttemptAt:  ack.NextAttemptAt,
	}
	return writeRuntimePayload(c, http.StatusOK, response)
}

// ResumeRuns authorizes recovery against the currently authenticated target
// Session. Attempt identities in the payload continue to name their immutable
// source Sessions; the body therefore cannot be resolved through an Attempt's
// source runtime_session_id.
func (h *RuntimeHTTPController) ResumeRuns(c echo.Context) error {
	authenticated, transportErr := h.authenticate(c)
	if transportErr != nil {
		return writeRuntimeError(c, transportErr)
	}
	if h.dependencies.Sessions == nil || h.dependencies.Resume == nil {
		return writeRuntimeError(c, runtimeUnavailableError())
	}
	payload, err := DecodeRuntimeBody[RuntimeResumePayload](c.Request().Body)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	if payload.AgentID != authenticated.AgentID || payload.NodeID != authenticated.Device.NodeID {
		return writeRuntimeError(c, newRuntimeTransportError(
			RuntimeErrorPermissionDenied,
			runtimeErrorDefaultMessage(RuntimeErrorPermissionDenied),
			nil,
		))
	}
	principal, err := h.resolveSession(c, authenticated, payload.RuntimeSessionID)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	if principal.WorkerID != payload.WorkerID {
		return writeRuntimeError(c, newRuntimeTransportError(
			RuntimeErrorLeaseIdentityMismatch,
			runtimeErrorDefaultMessage(RuntimeErrorLeaseIdentityMismatch),
			nil,
		))
	}
	response, err := h.dependencies.Resume.Resume(c.Request().Context(), principal, payload)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	return writeRuntimePayload(c, http.StatusOK, response)
}

// PollCommands long-polls commands for one explicit Session. A token and
// device may legitimately own several worker Sessions, so resolving a command
// poll from authentication alone would deliver work to an arbitrary process.
func (h *RuntimeHTTPController) PollCommands(c echo.Context) error {
	authenticated, transportErr := h.authenticate(c)
	if transportErr != nil {
		return writeRuntimeError(c, transportErr)
	}
	if h.dependencies.Sessions == nil || h.dependencies.Cancellations == nil {
		return writeRuntimeError(c, runtimeUnavailableError())
	}
	sessionID, wait, err := parseRuntimeCommandsQuery(c.Request())
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	principal, err := h.resolveSession(c, authenticated, sessionID)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	response, err := h.pollCommandsWithWait(c.Request().Context(), principal, wait)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	return writeRuntimePayload(c, http.StatusOK, response)
}

func (h *RuntimeHTTPController) AckCancel(c echo.Context) error {
	authenticated, transportErr := h.authenticate(c)
	if transportErr != nil {
		return writeRuntimeError(c, transportErr)
	}
	if h.dependencies.Sessions == nil || h.dependencies.Cancellations == nil {
		return writeRuntimeError(c, runtimeUnavailableError())
	}
	runID, err := parseRuntimePathUUID(c.Param("id"))
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	payload, err := DecodeRuntimeBody[RunCancelAckPayload](c.Request().Body)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	if payload.AttemptIdentity.RunID != runID {
		return writeRuntimeError(c, runtimeTransportValidationError())
	}
	principal, err := h.resolveSession(c, authenticated, payload.AttemptIdentity.RuntimeSessionID)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	state, err := h.dependencies.Cancellations.AckCancel(c.Request().Context(), principal, payload)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	return writeRuntimePayload(c, http.StatusOK, state)
}

// CallAgent authenticates the Node and both assignment-scoped signed
// capabilities before the exact request bytes are decoded as business input.
// The invocation capability, not a long-lived Agent Token, is the Bearer
// credential for this endpoint. Token-only transport resolves the same durable
// Node/key binding from that signed capability.
func (h *RuntimeHTTPController) CallAgent(c echo.Context) error {
	if h == nil || h.dependencies.Delegation == nil ||
		(!h.dependencies.TokenOnlyTransport && h.dependencies.DeviceAuthenticator == nil) {
		return writeRuntimeError(c, runtimeUnavailableError())
	}
	invocationToken, err := runtimeBearerToken(c.Request().Header.Get(echo.HeaderAuthorization))
	if err != nil {
		return writeRuntimeError(c, runtimeUnauthorizedError(err))
	}
	var device RuntimeDeviceIdentity
	if h.dependencies.TokenOnlyTransport {
		resolver, ok := h.dependencies.Delegation.(runtimeInvocationDeviceResolver)
		if !ok {
			return writeRuntimeError(c, runtimeUnavailableError())
		}
		device, err = resolver.ResolveInvocationDevice(c.Request().Context(), invocationToken)
	} else {
		device, err = h.dependencies.DeviceAuthenticator.AuthenticateHTTP(
			c.Request().Context(), c.Request(),
		)
	}
	if err != nil {
		return writeRuntimeError(c, runtimeUnauthorizedError(err))
	}
	if h.dependencies.AdmissionLimiter != nil &&
		!h.dependencies.AdmissionLimiter.AllowHTTP(runtimeAdmissionIdentityFromDevice(device)) {
		return writeRuntimeError(c, runtimeRateLimitedError())
	}
	if c.Request().URL.RawQuery != "" {
		return writeRuntimeError(c, runtimeTransportValidationError())
	}
	rawBody, err := readRuntimeJSON(c.Request().Body)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	authorization := RuntimeDelegationAuthorization{
		Device:            device,
		InvocationContext: c.Request().Header.Get("OpenLinker-Invocation-Context"),
		InvocationToken:   invocationToken,
		InvocationProof:   c.Request().Header.Get("OpenLinker-Invocation-Proof"),
		IdempotencyKey:    c.Request().Header.Get("Idempotency-Key"),
		ProofRequest:      RuntimeInvocationProofRequestFromHTTP(c.Request(), rawBody),
	}
	summary, err := h.dependencies.Delegation.CallAgent(c.Request().Context(), authorization)
	if err != nil {
		return writeRuntimeError(c, mapRuntimeHTTPError(err))
	}
	status := http.StatusAccepted
	if summary.Status != RuntimeRunRunning {
		status = http.StatusOK
	}
	return writeRuntimePayload(c, status, summary)
}

func (h *RuntimeHTTPController) authenticate(c echo.Context) (AuthenticatedRuntimePrincipal, *RuntimeTransportError) {
	if h == nil || h.dependencies.TokenValidator == nil ||
		(!h.dependencies.TokenOnlyTransport && h.dependencies.DeviceAuthenticator == nil) {
		return AuthenticatedRuntimePrincipal{}, runtimeUnavailableError()
	}
	if cached, ok := c.Get(runtimeAuthenticatedPrincipalContextKey).(AuthenticatedRuntimePrincipal); ok {
		if err := validateAuthenticatedRuntimePrincipal(cached); err == nil {
			return cached, nil
		}
	}
	rawToken, err := runtimeBearerToken(c.Request().Header.Get(echo.HeaderAuthorization))
	if err != nil {
		return AuthenticatedRuntimePrincipal{}, runtimeUnauthorizedError(err)
	}
	token, err := h.dependencies.TokenValidator.ValidateRuntimeToken(c.Request().Context(), rawToken, runtimeTokenScope)
	if err != nil {
		return AuthenticatedRuntimePrincipal{}, runtimeUnauthorizedError(err)
	}
	var device RuntimeDeviceIdentity
	if h.dependencies.TokenOnlyTransport {
		if h.dependencies.PrincipalBinder == nil {
			return AuthenticatedRuntimePrincipal{}, runtimeUnauthorizedError(nil)
		}
		nodeID, nodeErr := runtimeNodeIDFromRequest(c.Request())
		if nodeErr != nil {
			return AuthenticatedRuntimePrincipal{}, runtimeUnauthorizedError(nodeErr)
		}
		device, err = h.dependencies.PrincipalBinder.ResolveTokenOnlyRuntimeDeviceIdentity(
			c.Request().Context(), token.ID, nodeID,
		)
	} else {
		device, err = h.dependencies.DeviceAuthenticator.AuthenticateHTTP(c.Request().Context(), c.Request())
	}
	if err != nil {
		return AuthenticatedRuntimePrincipal{}, runtimeUnauthorizedError(err)
	}
	if !h.dependencies.TokenOnlyTransport && h.dependencies.PrincipalBinder != nil {
		if err = h.dependencies.PrincipalBinder.VerifyRuntimePrincipalBinding(c.Request().Context(), token.ID, device); err != nil {
			return AuthenticatedRuntimePrincipal{}, runtimeUnauthorizedError(err)
		}
	}
	principal := AuthenticatedRuntimePrincipal{AgentID: token.AgentID, CredentialID: token.ID, Device: device}
	if err = validateAuthenticatedRuntimePrincipal(principal); err != nil {
		return AuthenticatedRuntimePrincipal{}, runtimeUnauthorizedError(err)
	}
	c.Set(runtimeAuthenticatedPrincipalContextKey, principal)
	return principal, nil
}

func runtimeNodeIDFromRequest(request *http.Request) (uuid.UUID, error) {
	if request == nil {
		return uuid.Nil, errors.New("runtime Node selector is missing")
	}
	values := request.Header.Values(RuntimeNodeIDHeader)
	if len(values) != 1 || values[0] == "" || strings.TrimSpace(values[0]) != values[0] {
		return uuid.Nil, errors.New("runtime Node selector is invalid")
	}
	nodeID, err := uuid.Parse(values[0])
	if err != nil || nodeID == uuid.Nil || nodeID.String() != values[0] {
		return uuid.Nil, errors.New("runtime Node selector is invalid")
	}
	return nodeID, nil
}

func (h *RuntimeHTTPController) resolveSession(
	c echo.Context,
	authenticated AuthenticatedRuntimePrincipal,
	sessionID uuid.UUID,
) (RuntimeSessionPrincipal, error) {
	observeWorker(h.dependencies.Observer, "runtime.http.session_principal_query", "session", 1)
	principal, err := h.dependencies.Sessions.ResolveSessionPrincipal(c.Request().Context(), authenticated, sessionID)
	if err != nil {
		return RuntimeSessionPrincipal{}, err
	}
	if err = validateRuntimeResolvedSession(authenticated, principal); err != nil {
		return RuntimeSessionPrincipal{}, err
	}
	if _, err = runtimeAttachmentIDForResolvedPrincipal(c.Request(), principal); err != nil {
		return RuntimeSessionPrincipal{}, err
	}
	return principal, nil
}

func (h *RuntimeHTTPController) resolveEventResultSession(
	c echo.Context,
	authenticated AuthenticatedRuntimePrincipal,
	workerID string,
) (RuntimeSessionPrincipal, error) {
	observeWorker(h.dependencies.Observer, "runtime.http.session_principal_query", "worker", 1)
	principal, err := h.dependencies.Sessions.ResolveWorkerSessionPrincipal(
		c.Request().Context(), authenticated, workerID,
	)
	if err != nil {
		return RuntimeSessionPrincipal{}, err
	}
	// Event/Result authorization combines this resolved acting Session with the
	// immutable source Attempt and durable resume-grant checks downstream.
	if err = validateRuntimeResolvedSession(authenticated, principal); err != nil {
		return RuntimeSessionPrincipal{}, err
	}
	if _, err = runtimeAttachmentIDForResolvedPrincipal(c.Request(), principal); err != nil {
		return RuntimeSessionPrincipal{}, err
	}
	return principal, nil
}

func (h *RuntimeHTTPController) claimWithWait(
	ctx context.Context,
	principal RuntimeSessionPrincipal,
	wait time.Duration,
) (*RunAssignedPayload, error) {
	// Query once on request entry, then let the durable signal outbox/WakeHub
	// drive normal delivery. The next long-poll request is the database fallback
	// when a signal is missed; polling inside this request only amplifies idle DB
	// traffic by the number of connected workers.
	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	reason := "entry"
	for {
		var dispatchWake, nodeDispatchWake <-chan struct{}
		if h.dependencies.WakeHub != nil {
			dispatchWake = h.dependencies.WakeHub.WaitDispatch(principal.AgentID)
			nodeDispatchWake = h.dependencies.WakeHub.WaitNodeDispatch(principal.NodeID)
		}
		observeWorker(h.dependencies.Observer, "runtime.http.run_claim_query", reason, 1)
		assignment, err := h.dependencies.Leases.ClaimOffer(ctx, principal)
		if err != nil || assignment != nil || wait == 0 {
			return assignment, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline.C:
			return nil, nil
		case <-dispatchWake:
		case <-nodeDispatchWake:
		}
		reason = "wake"
	}
}

func (h *RuntimeHTTPController) pollCommandsWithWait(
	ctx context.Context,
	principal RuntimeSessionPrincipal,
	wait time.Duration,
) (RuntimeCommandsResponse, error) {
	// Cancellation signals use the same wake path. A request deadline remains
	// the bounded fallback without a 200ms PostgreSQL loop.
	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	reason := "entry"
	for {
		var wake <-chan struct{}
		if h.dependencies.WakeHub != nil {
			wake = h.dependencies.WakeHub.WaitControl(principal.AgentID)
		}
		observeWorker(h.dependencies.Observer, "runtime.http.command_query", reason, 1)
		response, err := h.dependencies.Cancellations.PollCommands(ctx, principal)
		if err != nil || len(response.Commands) > 0 || wait == 0 {
			return response, err
		}
		select {
		case <-ctx.Done():
			return RuntimeCommandsResponse{}, ctx.Err()
		case <-deadline.C:
			return response, nil
		case <-wake:
		}
		reason = "wake"
	}
}

func runtimeSessionRequestFromHello(hello RuntimeHelloPayload) RuntimeSessionRequest {
	return RuntimeSessionRequest{
		RuntimeSessionIdentity: RuntimeSessionIdentity{
			RuntimeSessionID: hello.RuntimeSessionID,
			NodeID:           hello.NodeID,
			AgentID:          hello.AgentID,
			WorkerID:         hello.WorkerID,
			SessionEpoch:     hello.SessionEpoch,
		},
		NodeVersion:           hello.NodeVersion,
		ProtocolVersion:       RuntimeProtocolVersion,
		RuntimeContractID:     RuntimeContractID,
		RuntimeContractDigest: hello.ContractDigest,
		Features:              append([]string(nil), hello.Features...),
		Capacity:              int32(hello.Capacity),
	}
}

func runtimeFallbackReasonFromRequest(request *http.Request) (RuntimeTransportReason, *RuntimeTransportError) {
	if request == nil {
		return "", runtimeTransportValidationError()
	}
	values := request.Header.Values(RuntimeFallbackReasonHeader)
	if len(values) == 0 {
		return "", nil
	}
	if len(values) != 1 || values[0] == "" || values[0] != strings.TrimSpace(values[0]) {
		return "", runtimeTransportValidationError()
	}
	reason := RuntimeTransportReason(values[0])
	if !reason.IsValid() {
		return "", runtimeTransportValidationError()
	}
	return reason, nil
}

func runtimeReadyFromSessionState(state RuntimeSessionState) (any, error) {
	if state.Session.AttachedCoreInstanceID == nil || *state.Session.AttachedCoreInstanceID == uuid.Nil ||
		state.Attachment == nil || state.Attachment.ID == uuid.Nil || state.DatabaseTime.IsZero() {
		return nil, errors.New("invalid committed runtime session state")
	}
	if !runtimeWireContractSupported(state.Session.RuntimeContractDigest) {
		return nil, errors.New("unsupported committed runtime wire contract")
	}
	readyFeatures := runtimeRequiredFeaturesForDigest(state.Session.RuntimeContractDigest)
	if runtimeWireContractAllowsMissingAttachment(state.Session.RuntimeContractDigest) {
		ready := runtimePreviousReadyPayload{
			CoreInstanceID:  state.Session.AttachedCoreInstanceID.String(),
			Features:        readyFeatures,
			OfferTTLSeconds: RuntimeOfferTTLSeconds,
			LeaseTTLSeconds: RuntimeLeaseTTLSeconds,
			DatabaseTime:    state.DatabaseTime,
		}
		if err := ValidateRuntimePayload(ready); err != nil {
			return nil, err
		}
		return ready, nil
	}
	ready := RuntimeReadyPayload{
		CoreInstanceID:  state.Session.AttachedCoreInstanceID.String(),
		AttachmentID:    state.Attachment.ID,
		Features:        readyFeatures,
		OfferTTLSeconds: RuntimeOfferTTLSeconds,
		LeaseTTLSeconds: RuntimeLeaseTTLSeconds,
		DatabaseTime:    state.DatabaseTime,
	}
	if err := ValidateRuntimePayload(ready); err != nil {
		return nil, err
	}
	return ready, nil
}

type runtimeSessionClosePayload struct {
	NodeID           uuid.UUID `json:"node_id" runtime:"required"`
	AgentID          uuid.UUID `json:"agent_id" runtime:"required"`
	WorkerID         string    `json:"worker_id" runtime:"required"`
	RuntimeSessionID uuid.UUID `json:"runtime_session_id" runtime:"required"`
	SessionEpoch     int64     `json:"session_epoch" runtime:"required"`
	Status           string    `json:"status" runtime:"required"`
	Reason           string    `json:"reason" runtime:"required"`
}

func decodeRuntimeSessionClose(req *http.Request, principal AuthenticatedRuntimePrincipal) (RuntimeSessionCloseRequest, error) {
	if req == nil {
		return RuntimeSessionCloseRequest{}, runtimeTransportValidationError()
	}
	raw, err := readRuntimeJSON(req.Body)
	if err != nil {
		return RuntimeSessionCloseRequest{}, err
	}
	var payload runtimeSessionClosePayload
	if err = decodeRuntimeJSON(raw, &payload); err != nil {
		return RuntimeSessionCloseRequest{}, err
	}
	request := RuntimeSessionCloseRequest{
		RuntimeSessionIdentity: RuntimeSessionIdentity{
			RuntimeSessionID: payload.RuntimeSessionID,
			NodeID:           payload.NodeID,
			AgentID:          payload.AgentID,
			WorkerID:         payload.WorkerID,
			SessionEpoch:     payload.SessionEpoch,
		},
		Status: payload.Status,
		Reason: payload.Reason,
	}
	if err = validateRuntimeSessionIdentity(principal, request.RuntimeSessionIdentity); err != nil {
		return RuntimeSessionCloseRequest{}, err
	}
	if (request.Status != "offline" && request.Status != "closed") ||
		!validRuntimeIdentityText(request.Reason, 1, maxRuntimeDisconnectReasonRunes) {
		return RuntimeSessionCloseRequest{}, runtimeTransportValidationError()
	}
	return request, nil
}

func (h *RuntimeHTTPController) runtimeAttachmentIDForSessionRequest(
	c echo.Context,
	authenticated AuthenticatedRuntimePrincipal,
	sessionID uuid.UUID,
) (uuid.UUID, error) {
	attachmentID, present, err := runtimeOptionalAttachmentIDFromRequest(c.Request())
	if err != nil || present {
		return attachmentID, err
	}
	principal, err := h.dependencies.Sessions.ResolveSessionPrincipal(
		c.Request().Context(), authenticated, sessionID,
	)
	if err != nil {
		return uuid.Nil, err
	}
	if err = validateRuntimeResolvedSession(authenticated, principal); err != nil {
		return uuid.Nil, err
	}
	if !runtimeWireContractAllowsMissingAttachment(principal.RuntimeContractDigest) {
		return uuid.Nil, runtimeTransportValidationError()
	}
	return principal.AttachmentID, nil
}

func runtimeAttachmentIDForResolvedPrincipal(
	req *http.Request,
	principal RuntimeSessionPrincipal,
) (uuid.UUID, error) {
	attachmentID, present, err := runtimeOptionalAttachmentIDFromRequest(req)
	if err != nil {
		return uuid.Nil, err
	}
	if !present {
		if runtimeWireContractAllowsMissingAttachment(principal.RuntimeContractDigest) {
			return principal.AttachmentID, nil
		}
		return uuid.Nil, runtimeTransportValidationError()
	}
	if attachmentID != principal.AttachmentID {
		return uuid.Nil, newRuntimeSessionError(RuntimeSessionErrorNotAttached, nil)
	}
	return attachmentID, nil
}

func validateRuntimeResolvedSession(authenticated AuthenticatedRuntimePrincipal, principal RuntimeSessionPrincipal) error {
	if principal.RuntimeSessionID == uuid.Nil || principal.NodeID != authenticated.Device.NodeID ||
		principal.AgentID != authenticated.AgentID || principal.CredentialID != authenticated.CredentialID ||
		principal.WorkerID == "" || principal.SessionEpoch < 1 || principal.CoreInstanceID == uuid.Nil || principal.AttachmentID == uuid.Nil ||
		!runtimeWireContractSupported(principal.RuntimeContractDigest) ||
		(principal.Status != "active" && principal.Status != "draining") || principal.DatabaseTime.IsZero() ||
		!constantTimeStringEqual(principal.DeviceCertificateSerial, authenticated.Device.CertificateSerial) ||
		!constantTimeStringEqual(principal.DevicePublicKeyThumbprintSHA256, authenticated.Device.PublicKeyThumbprintSHA256) {
		return newRuntimeSessionError(RuntimeSessionErrorAuthenticationFailed, nil)
	}
	return nil
}

func runtimeAttachmentIDFromRequest(req *http.Request) (uuid.UUID, error) {
	attachmentID, present, err := runtimeOptionalAttachmentIDFromRequest(req)
	if err != nil || !present {
		return uuid.Nil, runtimeTransportValidationError()
	}
	return attachmentID, nil
}

func runtimeOptionalAttachmentIDFromRequest(req *http.Request) (uuid.UUID, bool, error) {
	if req == nil {
		return uuid.Nil, false, runtimeTransportValidationError()
	}
	values := req.Header.Values(RuntimeAttachmentIDHeader)
	if len(values) == 0 {
		return uuid.Nil, false, nil
	}
	if len(values) != 1 {
		return uuid.Nil, true, runtimeTransportValidationError()
	}
	raw := values[0]
	if raw == "" || strings.TrimSpace(raw) != raw {
		return uuid.Nil, true, runtimeTransportValidationError()
	}
	attachmentID, err := uuid.Parse(raw)
	if err != nil || attachmentID == uuid.Nil || attachmentID.String() != raw {
		return uuid.Nil, true, runtimeTransportValidationError()
	}
	return attachmentID, true, nil
}

func parseRuntimePathUUID(raw string) (uuid.UUID, error) {
	if raw == "" || strings.TrimSpace(raw) != raw {
		return uuid.Nil, runtimeTransportValidationError()
	}
	parsed, err := uuid.Parse(raw)
	if err != nil || parsed == uuid.Nil || parsed.String() != raw {
		return uuid.Nil, runtimeTransportValidationError()
	}
	return parsed, nil
}

func parseRuntimeWait(raw string) (time.Duration, error) {
	if raw == "" {
		return 0, nil
	}
	if strings.TrimSpace(raw) != raw {
		return 0, runtimeTransportValidationError()
	}
	for _, char := range raw {
		if char < '0' || char > '9' {
			return 0, runtimeTransportValidationError()
		}
	}
	seconds, err := strconv.ParseUint(raw, 10, 8)
	if err != nil || seconds > RuntimeMaxPullWaitSeconds {
		return 0, runtimeTransportValidationError()
	}
	return time.Duration(seconds) * time.Second, nil
}

func parseRuntimeCommandsQuery(request *http.Request) (uuid.UUID, time.Duration, error) {
	if request == nil || request.URL == nil {
		return uuid.Nil, 0, runtimeTransportValidationError()
	}
	query := request.URL.Query()
	for key, values := range query {
		if (key != "runtime_session_id" && key != "wait") || len(values) != 1 {
			return uuid.Nil, 0, runtimeTransportValidationError()
		}
	}
	sessionValues, ok := query["runtime_session_id"]
	if !ok || len(sessionValues) != 1 {
		return uuid.Nil, 0, runtimeTransportValidationError()
	}
	sessionID, err := parseRuntimePathUUID(sessionValues[0])
	if err != nil {
		return uuid.Nil, 0, err
	}
	waitValues, ok := query["wait"]
	if !ok {
		return sessionID, 0, nil
	}
	wait, err := parseRuntimeWait(waitValues[0])
	if err != nil {
		return uuid.Nil, 0, err
	}
	return sessionID, wait, nil
}

func runtimeTransportValidationError() *RuntimeTransportError {
	return NewRuntimeTransportError(RuntimeErrorValidationFailed, runtimeErrorDefaultMessage(RuntimeErrorValidationFailed))
}

func runtimeUnauthorizedError(cause error) *RuntimeTransportError {
	return newRuntimeTransportError(RuntimeErrorUnauthorized, runtimeErrorDefaultMessage(RuntimeErrorUnauthorized), cause)
}

func runtimeTransportForbiddenError() *RuntimeTransportError {
	return NewRuntimeTransportError(RuntimeErrorForbidden, RuntimeTransportForbiddenSignal)
}

func runtimePolicyChangedError() *RuntimeTransportError {
	return NewRuntimeTransportError(RuntimeErrorForbidden, RuntimePolicyChangedSignal)
}

func runtimePolicySignal(err *RuntimeTransportError) (string, bool) {
	if err == nil || err.Body.Code != RuntimeErrorForbidden {
		return "", false
	}
	switch err.Body.Message {
	case RuntimeTransportForbiddenSignal, RuntimePolicyChangedSignal:
		return err.Body.Message, true
	default:
		return "", false
	}
}

func runtimeUnavailableError() *RuntimeTransportError {
	err := NewRuntimeTransportError(RuntimeErrorServiceUnavailable, runtimeErrorDefaultMessage(RuntimeErrorServiceUnavailable))
	err.Body.Retryable = true
	return err
}

func runtimeRateLimitedError() *RuntimeTransportError {
	err := NewRuntimeTransportError(RuntimeErrorRateLimited, runtimeErrorDefaultMessage(RuntimeErrorRateLimited))
	err.Body.Retryable = true
	return err
}

func mapRuntimeHTTPError(err error) *RuntimeTransportError {
	if err == nil {
		return nil
	}
	var transportErr *RuntimeTransportError
	if errors.As(err, &transportErr) {
		return transportErr
	}
	var httpErr *httpx.HTTPError
	if errors.As(err, &httpErr) {
		code := RuntimeErrorCode(httpErr.Code)
		if !validRuntimeErrorCode(code) {
			switch httpErr.Status {
			case http.StatusBadRequest:
				code = RuntimeErrorBadRequest
			case http.StatusUnauthorized:
				code = RuntimeErrorUnauthorized
			case http.StatusForbidden:
				code = RuntimeErrorPermissionDenied
			case http.StatusNotFound:
				code = RuntimeErrorNotFound
			case http.StatusConflict:
				code = RuntimeErrorConflict
			case http.StatusUnprocessableEntity:
				code = RuntimeErrorValidationFailed
			case http.StatusTooManyRequests:
				code = RuntimeErrorRateLimited
			case http.StatusServiceUnavailable:
				code = RuntimeErrorServiceUnavailable
			default:
				code = RuntimeErrorInternal
			}
		}
		mapped := newRuntimeTransportError(code, runtimeErrorDefaultMessage(code), err)
		mapped.Body.Retryable = code == RuntimeErrorRateLimited || code == RuntimeErrorServiceUnavailable
		return mapped
	}
	var sessionErr *RuntimeSessionError
	if errors.As(err, &sessionErr) {
		switch sessionErr.Code {
		case RuntimeSessionErrorAuthenticationFailed, RuntimeSessionErrorPrincipalInactive:
			return runtimeUnauthorizedError(err)
		case RuntimeSessionErrorAgentMismatch, RuntimeSessionErrorDeviceMismatch:
			return newRuntimeTransportError(RuntimeErrorPermissionDenied, runtimeErrorDefaultMessage(RuntimeErrorPermissionDenied), err)
		case RuntimeSessionErrorProtocolUnsupported, RuntimeSessionErrorContractMismatch:
			return newRuntimeTransportError(RuntimeErrorClientUpgradeRequired, runtimeErrorDefaultMessage(RuntimeErrorClientUpgradeRequired), err)
		case RuntimeSessionErrorRequiredFeatureMissing:
			return newRuntimeTransportError(RuntimeErrorRequiredFeatureMissing, runtimeErrorDefaultMessage(RuntimeErrorRequiredFeatureMissing), err)
		case RuntimeSessionErrorSessionConflict, RuntimeSessionErrorNotAttached:
			return newRuntimeTransportError(RuntimeErrorSessionConflict, runtimeErrorDefaultMessage(RuntimeErrorSessionConflict), err)
		case RuntimeSessionErrorValidationFailed:
			return newRuntimeTransportError(RuntimeErrorValidationFailed, runtimeErrorDefaultMessage(RuntimeErrorValidationFailed), err)
		}
	}
	var leaseErr *RuntimeLeaseError
	if errors.As(err, &leaseErr) {
		var code RuntimeErrorCode
		switch leaseErr.Code {
		case RuntimeLeaseErrorValidationFailed:
			code = RuntimeErrorValidationFailed
		case RuntimeLeaseErrorIdentityMismatch:
			code = RuntimeErrorLeaseIdentityMismatch
		case RuntimeLeaseErrorStaleLease:
			code = RuntimeErrorStaleLease
		case RuntimeLeaseErrorLeaseExpired:
			code = RuntimeErrorLeaseExpired
		case RuntimeLeaseErrorNodeAtCapacity:
			code = RuntimeErrorNodeAtCapacity
		case RuntimeLeaseErrorRunTerminal:
			code = RuntimeErrorRunAlreadyTerminal
		case RuntimeLeaseErrorCancelRequested:
			code = RuntimeErrorRunCancelRequested
		default:
			code = RuntimeErrorInternal
		}
		return newRuntimeTransportError(code, runtimeErrorDefaultMessage(code), err)
	}
	if errors.Is(err, ErrRuntimeCancellationInvalid) {
		return newRuntimeTransportError(RuntimeErrorValidationFailed, runtimeErrorDefaultMessage(RuntimeErrorValidationFailed), err)
	}
	if errors.Is(err, ErrRuntimeCancellationNotFound) {
		return newRuntimeTransportError(RuntimeErrorNotFound, runtimeErrorDefaultMessage(RuntimeErrorNotFound), err)
	}
	if errors.Is(err, ErrRuntimeCancellationRunEnded) {
		return newRuntimeTransportError(RuntimeErrorRunAlreadyTerminal, runtimeErrorDefaultMessage(RuntimeErrorRunAlreadyTerminal), err)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		mapped := newRuntimeTransportError(RuntimeErrorServiceUnavailable, runtimeErrorDefaultMessage(RuntimeErrorServiceUnavailable), err)
		mapped.Body.Retryable = true
		return mapped
	}
	return MapRuntimeTransportError(err)
}

func writeRuntimePayload(c echo.Context, status int, payload any) error {
	if err := ValidateRuntimePayload(payload); err != nil {
		return writeRuntimeError(c, newRuntimeTransportError(RuntimeErrorInternal, runtimeErrorDefaultMessage(RuntimeErrorInternal), err))
	}
	return c.JSON(status, payload)
}

func writeRuntimeError(c echo.Context, err *RuntimeTransportError) error {
	if err == nil {
		err = NewRuntimeTransportError(RuntimeErrorInternal, runtimeErrorDefaultMessage(RuntimeErrorInternal))
	}
	return c.JSON(RuntimeHTTPStatus(err.Body.Code), err.Envelope())
}
