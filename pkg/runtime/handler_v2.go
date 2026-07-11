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
)

const runtimeV2TokenScope = "agent:pull"

// RuntimeV2TokenValidator authenticates the Agent half of a runtime principal.
// Implementations must check revocation, expiry, and the requested scope.
type RuntimeV2TokenValidator interface {
	ValidateRuntimeToken(context.Context, string, ...string) (db.AgentRuntimeToken, error)
}

// RuntimeV2DeviceAuthenticator authenticates the independently enrolled Node
// device. An implementation must use the verified TLS peer certificate, never
// forwarded certificate or identity headers.
type RuntimeV2DeviceAuthenticator interface {
	AuthenticateHTTP(context.Context, *http.Request) (RuntimeDeviceIdentity, error)
}

type RuntimeV2SessionService interface {
	CreateOrAttachSession(context.Context, AuthenticatedRuntimePrincipal, RuntimeSessionRequest) (RuntimeSessionState, error)
	HeartbeatSession(context.Context, AuthenticatedRuntimePrincipal, RuntimeSessionHeartbeatRequest) (RuntimeSessionState, error)
	CloseSession(context.Context, AuthenticatedRuntimePrincipal, RuntimeSessionCloseRequest) (RuntimeSessionState, error)
	ResolveSessionPrincipal(context.Context, AuthenticatedRuntimePrincipal, uuid.UUID) (RuntimeSessionPrincipal, error)
	// ResolveWorkerSessionPrincipal returns the currently acting Session. The
	// source Session ID in an Attempt remains immutable across resume and must
	// not be mistaken for the authenticated uploader.
	ResolveWorkerSessionPrincipal(context.Context, AuthenticatedRuntimePrincipal, string) (RuntimeSessionPrincipal, error)
}

type RuntimeV2LeaseService interface {
	ClaimOffer(context.Context, RuntimeSessionPrincipal) (*RunAssignedPayload, error)
	AckAssignment(context.Context, RuntimeSessionPrincipal, RunAssignmentAckPayload) (RunAssignmentConfirmedPayload, error)
	RejectAssignment(context.Context, RuntimeSessionPrincipal, RunAssignmentRejectPayload) (RunAssignmentRejectedPayload, error)
	RenewLease(context.Context, RuntimeSessionPrincipal, RunLeaseRenewPayload) (RunLeaseRenewedPayload, error)
	ReleaseUnackedOffer(context.Context, RuntimeSessionPrincipal, ...string) error
}

type RuntimeV2EventStore interface {
	Append(context.Context, RuntimeEventPrincipal, RuntimeAttemptIdentity, RuntimeEventRequest) (RuntimeEventAck, error)
}

type RuntimeV2ResultFinalizer interface {
	Finalize(context.Context, RuntimeResultPrincipal, RuntimeResultRequest) (RuntimeResultAck, error)
}

// RuntimeV2HTTPDependencies are deliberately narrow so the HTTP adapter has
// no database access and cannot reconstruct trusted principals from JSON.
type RuntimeV2HTTPDependencies struct {
	TokenValidator      RuntimeV2TokenValidator
	DeviceAuthenticator RuntimeV2DeviceAuthenticator
	Sessions            RuntimeV2SessionService
	Leases              RuntimeV2LeaseService
	EventStore          RuntimeV2EventStore
	Finalizer           RuntimeV2ResultFinalizer
	Resume              RuntimeV2ResumeService
}

// RuntimeV2HTTPController is the strict HTTP transport adapter for the durable
// runtime v2 state machine.
type RuntimeV2HTTPController struct {
	dependencies RuntimeV2HTTPDependencies
}

func NewRuntimeV2HTTPController(dependencies RuntimeV2HTTPDependencies) *RuntimeV2HTTPController {
	return &RuntimeV2HTTPController{dependencies: dependencies}
}

func newRuntimeV2HTTPControllerForService(service runtimeService) *RuntimeV2HTTPController {
	dependencies := RuntimeV2HTTPDependencies{}
	if validator, ok := any(service).(RuntimeV2TokenValidator); ok {
		dependencies.TokenValidator = validator
	}
	return NewRuntimeV2HTTPController(dependencies)
}

// SetRuntimeV2Dependencies completes explicit production wiring. The legacy
// runtime service remains a token-validator fallback for source compatibility.
func (h *Handler) SetRuntimeV2Dependencies(dependencies RuntimeV2HTTPDependencies) {
	if h == nil {
		return
	}
	if dependencies.TokenValidator == nil {
		if validator, ok := any(h.svc).(RuntimeV2TokenValidator); ok {
			dependencies.TokenValidator = validator
		}
	}
	h.runtimeV2 = NewRuntimeV2HTTPController(dependencies)
}

func (h *RuntimeV2HTTPController) Register(api *echo.Group) {
	if api == nil {
		return
	}
	if h == nil {
		h = NewRuntimeV2HTTPController(RuntimeV2HTTPDependencies{})
	}
	api.POST("/agent-runtime/v2/sessions", h.CreateSession)
	api.POST("/agent-runtime/v2/sessions/:id/heartbeat", h.HeartbeatSession)
	api.POST("/agent-runtime/v2/sessions/:id/close", h.CloseSession)
	api.POST("/agent-runtime/v2/runs/claim", h.ClaimRun)
	api.POST("/agent-runtime/v2/runs/:id/assignment-ack", h.AckAssignment)
	api.POST("/agent-runtime/v2/runs/:id/assignment-reject", h.RejectAssignment)
	api.POST("/agent-runtime/v2/runs/:id/lease-renew", h.RenewLease)
	api.POST("/agent-runtime/v2/runs/:id/events", h.AppendEvent)
	api.POST("/agent-runtime/v2/runs/:id/result", h.FinalizeResult)
	api.GET("/agent-runtime/v2/ws", h.WebSocket)
}

func (h *RuntimeV2HTTPController) CreateSession(c echo.Context) error {
	principal, transportErr := h.authenticate(c)
	if transportErr != nil {
		return writeRuntimeV2Error(c, transportErr)
	}
	if h.dependencies.Sessions == nil {
		return writeRuntimeV2Error(c, runtimeV2UnavailableError())
	}
	hello, err := DecodeRuntimeBody[RuntimeHelloPayload](c.Request().Body)
	if err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	request := runtimeSessionRequestFromHello(hello)
	state, err := h.dependencies.Sessions.CreateOrAttachSession(c.Request().Context(), principal, request)
	if err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	ready, err := runtimeReadyFromSessionState(state)
	if err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	return writeRuntimeV2Payload(c, http.StatusOK, ready)
}

func (h *RuntimeV2HTTPController) HeartbeatSession(c echo.Context) error {
	principal, transportErr := h.authenticate(c)
	if transportErr != nil {
		return writeRuntimeV2Error(c, transportErr)
	}
	if h.dependencies.Sessions == nil {
		return writeRuntimeV2Error(c, runtimeV2UnavailableError())
	}
	sessionID, err := parseRuntimeV2PathUUID(c.Param("id"))
	if err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	hello, err := DecodeRuntimeBody[RuntimeHelloPayload](c.Request().Body)
	if err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	if hello.RuntimeSessionID != sessionID {
		return writeRuntimeV2Error(c, runtimeV2ValidationError())
	}
	state, err := h.dependencies.Sessions.HeartbeatSession(
		c.Request().Context(), principal, runtimeSessionRequestFromHello(hello),
	)
	if err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	ready, err := runtimeReadyFromSessionState(state)
	if err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	return writeRuntimeV2Payload(c, http.StatusOK, ready)
}

func (h *RuntimeV2HTTPController) CloseSession(c echo.Context) error {
	principal, transportErr := h.authenticate(c)
	if transportErr != nil {
		return writeRuntimeV2Error(c, transportErr)
	}
	if h.dependencies.Sessions == nil {
		return writeRuntimeV2Error(c, runtimeV2UnavailableError())
	}
	sessionID, err := parseRuntimeV2PathUUID(c.Param("id"))
	if err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	request, err := decodeRuntimeSessionClose(c.Request(), principal)
	if err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	if request.RuntimeSessionID != sessionID {
		return writeRuntimeV2Error(c, runtimeV2ValidationError())
	}
	if _, err = h.dependencies.Sessions.CloseSession(c.Request().Context(), principal, request); err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	return c.NoContent(http.StatusNoContent)
}

func (h *RuntimeV2HTTPController) ClaimRun(c echo.Context) error {
	authenticated, transportErr := h.authenticate(c)
	if transportErr != nil {
		return writeRuntimeV2Error(c, transportErr)
	}
	if h.dependencies.Sessions == nil || h.dependencies.Leases == nil {
		return writeRuntimeV2Error(c, runtimeV2UnavailableError())
	}
	wait, err := parseRuntimeV2Wait(c.QueryParam("wait"))
	if err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	request, err := DecodeRuntimeBody[RuntimeClaimRequest](c.Request().Body)
	if err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	if !validRuntimeCapacityReport(request.Capacity, request.Inflight) {
		return writeRuntimeV2Error(c, runtimeV2ValidationError())
	}
	principal, err := h.resolveSession(c, authenticated, request.RuntimeSessionID)
	if err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	assignment, err := h.claimWithWait(c.Request().Context(), principal, wait)
	if err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	if assignment == nil {
		return c.NoContent(http.StatusNoContent)
	}
	return writeRuntimeV2Payload(c, http.StatusOK, *assignment)
}

func (h *RuntimeV2HTTPController) AckAssignment(c echo.Context) error {
	authenticated, transportErr := h.authenticate(c)
	if transportErr != nil {
		return writeRuntimeV2Error(c, transportErr)
	}
	if h.dependencies.Sessions == nil || h.dependencies.Leases == nil {
		return writeRuntimeV2Error(c, runtimeV2UnavailableError())
	}
	runID, err := parseRuntimeV2PathUUID(c.Param("id"))
	if err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	request, err := DecodeRuntimeBody[RunAssignmentAckPayload](c.Request().Body)
	if err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	if request.AttemptIdentity.RunID != runID {
		return writeRuntimeV2Error(c, runtimeV2ValidationError())
	}
	principal, err := h.resolveSession(c, authenticated, request.AttemptIdentity.RuntimeSessionID)
	if err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	response, err := h.dependencies.Leases.AckAssignment(c.Request().Context(), principal, request)
	if err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	return writeRuntimeV2Payload(c, http.StatusOK, response)
}

func (h *RuntimeV2HTTPController) RejectAssignment(c echo.Context) error {
	authenticated, transportErr := h.authenticate(c)
	if transportErr != nil {
		return writeRuntimeV2Error(c, transportErr)
	}
	if h.dependencies.Sessions == nil || h.dependencies.Leases == nil {
		return writeRuntimeV2Error(c, runtimeV2UnavailableError())
	}
	runID, err := parseRuntimeV2PathUUID(c.Param("id"))
	if err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	request, err := DecodeRuntimeBody[RunAssignmentRejectPayload](c.Request().Body)
	if err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	if request.AttemptIdentity.RunID != runID {
		return writeRuntimeV2Error(c, runtimeV2ValidationError())
	}
	principal, err := h.resolveSession(c, authenticated, request.AttemptIdentity.RuntimeSessionID)
	if err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	response, err := h.dependencies.Leases.RejectAssignment(c.Request().Context(), principal, request)
	if err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	return writeRuntimeV2Payload(c, http.StatusOK, response)
}

func (h *RuntimeV2HTTPController) RenewLease(c echo.Context) error {
	authenticated, transportErr := h.authenticate(c)
	if transportErr != nil {
		return writeRuntimeV2Error(c, transportErr)
	}
	if h.dependencies.Sessions == nil || h.dependencies.Leases == nil {
		return writeRuntimeV2Error(c, runtimeV2UnavailableError())
	}
	runID, err := parseRuntimeV2PathUUID(c.Param("id"))
	if err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	request, err := DecodeRuntimeBody[RunLeaseRenewPayload](c.Request().Body)
	if err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	if request.AttemptIdentity.RunID != runID {
		return writeRuntimeV2Error(c, runtimeV2ValidationError())
	}
	principal, err := h.resolveSession(c, authenticated, request.AttemptIdentity.RuntimeSessionID)
	if err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	response, err := h.dependencies.Leases.RenewLease(c.Request().Context(), principal, request)
	if err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	return writeRuntimeV2Payload(c, http.StatusOK, response)
}

func (h *RuntimeV2HTTPController) AppendEvent(c echo.Context) error {
	authenticated, transportErr := h.authenticate(c)
	if transportErr != nil {
		return writeRuntimeV2Error(c, transportErr)
	}
	if h.dependencies.Sessions == nil || h.dependencies.EventStore == nil {
		return writeRuntimeV2Error(c, runtimeV2UnavailableError())
	}
	runID, err := parseRuntimeV2PathUUID(c.Param("id"))
	if err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	request, err := DecodeRuntimeBody[RunEventPayload](c.Request().Body)
	if err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	if request.AttemptIdentity.RunID != runID {
		return writeRuntimeV2Error(c, runtimeV2ValidationError())
	}
	principal, err := h.resolveEventResultSession(c, authenticated, request.AttemptIdentity.WorkerID)
	if err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	ack, err := h.dependencies.EventStore.Append(
		c.Request().Context(), principal.EventPrincipal(), request.AttemptIdentity.RuntimeIdentity(), request.StoreRequest(),
	)
	if err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	response := RunEventAckPayload{
		ClientEventID:  ack.ClientEventID,
		ClientEventSeq: ack.ClientEventSeq,
		Sequence:       int64(ack.Sequence),
		Replayed:       ack.Replayed,
	}
	return writeRuntimeV2Payload(c, http.StatusOK, response)
}

func (h *RuntimeV2HTTPController) FinalizeResult(c echo.Context) error {
	authenticated, transportErr := h.authenticate(c)
	if transportErr != nil {
		return writeRuntimeV2Error(c, transportErr)
	}
	if h.dependencies.Sessions == nil || h.dependencies.Finalizer == nil {
		return writeRuntimeV2Error(c, runtimeV2UnavailableError())
	}
	runID, err := parseRuntimeV2PathUUID(c.Param("id"))
	if err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	payload, err := DecodeRuntimeBody[RunResultPayload](c.Request().Body)
	if err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	if payload.AttemptIdentity.RunID != runID {
		return writeRuntimeV2Error(c, runtimeV2ValidationError())
	}
	principal, err := h.resolveEventResultSession(c, authenticated, payload.AttemptIdentity.WorkerID)
	if err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	request, err := payload.FinalizerRequest()
	if err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	ack, err := h.dependencies.Finalizer.Finalize(c.Request().Context(), principal.EventPrincipal(), request)
	if err != nil {
		return writeRuntimeV2Error(c, mapRuntimeV2HTTPError(err))
	}
	response := RunResultAckPayload{
		ResultID:       ack.ResultID,
		Classification: ack.Classification,
		RunStatus:      RuntimeRunStatus(ack.RunStatus),
		DispatchState:  RuntimeDispatchState(ack.DispatchState),
		Replayed:       ack.Replayed,
		NextAttemptAt:  ack.NextAttemptAt,
	}
	return writeRuntimeV2Payload(c, http.StatusOK, response)
}

func (h *RuntimeV2HTTPController) authenticate(c echo.Context) (AuthenticatedRuntimePrincipal, *RuntimeTransportError) {
	if h == nil || h.dependencies.TokenValidator == nil || h.dependencies.DeviceAuthenticator == nil {
		return AuthenticatedRuntimePrincipal{}, runtimeV2UnavailableError()
	}
	rawToken, err := runtimeBearerToken(c.Request().Header.Get(echo.HeaderAuthorization))
	if err != nil {
		return AuthenticatedRuntimePrincipal{}, runtimeV2UnauthorizedError(err)
	}
	token, err := h.dependencies.TokenValidator.ValidateRuntimeToken(c.Request().Context(), rawToken, runtimeV2TokenScope)
	if err != nil {
		return AuthenticatedRuntimePrincipal{}, runtimeV2UnauthorizedError(err)
	}
	device, err := h.dependencies.DeviceAuthenticator.AuthenticateHTTP(c.Request().Context(), c.Request())
	if err != nil {
		return AuthenticatedRuntimePrincipal{}, runtimeV2UnauthorizedError(err)
	}
	principal := AuthenticatedRuntimePrincipal{AgentID: token.AgentID, CredentialID: token.ID, Device: device}
	if err = validateAuthenticatedRuntimePrincipal(principal); err != nil {
		return AuthenticatedRuntimePrincipal{}, runtimeV2UnauthorizedError(err)
	}
	return principal, nil
}

func (h *RuntimeV2HTTPController) resolveSession(
	c echo.Context,
	authenticated AuthenticatedRuntimePrincipal,
	sessionID uuid.UUID,
) (RuntimeSessionPrincipal, error) {
	principal, err := h.dependencies.Sessions.ResolveSessionPrincipal(c.Request().Context(), authenticated, sessionID)
	if err != nil {
		return RuntimeSessionPrincipal{}, err
	}
	if err = validateRuntimeV2ResolvedSession(authenticated, principal); err != nil {
		return RuntimeSessionPrincipal{}, err
	}
	return principal, nil
}

func (h *RuntimeV2HTTPController) resolveEventResultSession(
	c echo.Context,
	authenticated AuthenticatedRuntimePrincipal,
	workerID string,
) (RuntimeSessionPrincipal, error) {
	principal, err := h.dependencies.Sessions.ResolveWorkerSessionPrincipal(
		c.Request().Context(), authenticated, workerID,
	)
	if err != nil {
		return RuntimeSessionPrincipal{}, err
	}
	// Event/Result authorization combines this resolved acting Session with the
	// immutable source Attempt and durable resume-grant checks downstream.
	if err = validateRuntimeV2ResolvedSession(authenticated, principal); err != nil {
		return RuntimeSessionPrincipal{}, err
	}
	return principal, nil
}

func (h *RuntimeV2HTTPController) claimWithWait(
	ctx context.Context,
	principal RuntimeSessionPrincipal,
	wait time.Duration,
) (*RunAssignedPayload, error) {
	if wait == 0 {
		return h.dependencies.Leases.ClaimOffer(ctx, principal)
	}
	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	poll := time.NewTicker(200 * time.Millisecond)
	defer poll.Stop()
	for {
		assignment, err := h.dependencies.Leases.ClaimOffer(ctx, principal)
		if err != nil || assignment != nil {
			return assignment, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline.C:
			return nil, nil
		case <-poll.C:
		}
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

func runtimeReadyFromSessionState(state RuntimeSessionState) (RuntimeReadyPayload, error) {
	if state.Session.AttachedCoreInstanceID == nil || *state.Session.AttachedCoreInstanceID == uuid.Nil || state.DatabaseTime.IsZero() {
		return RuntimeReadyPayload{}, errors.New("invalid committed runtime session state")
	}
	ready := RuntimeReadyPayload{
		CoreInstanceID:  state.Session.AttachedCoreInstanceID.String(),
		Features:        RuntimeRequiredFeatures(),
		OfferTTLSeconds: RuntimeOfferTTLSeconds,
		LeaseTTLSeconds: RuntimeLeaseTTLSeconds,
		DatabaseTime:    state.DatabaseTime,
	}
	if err := ValidateRuntimePayload(ready); err != nil {
		return RuntimeReadyPayload{}, err
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
		return RuntimeSessionCloseRequest{}, runtimeV2ValidationError()
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
		return RuntimeSessionCloseRequest{}, runtimeV2ValidationError()
	}
	return request, nil
}

func validateRuntimeV2ResolvedSession(authenticated AuthenticatedRuntimePrincipal, principal RuntimeSessionPrincipal) error {
	if principal.RuntimeSessionID == uuid.Nil || principal.NodeID != authenticated.Device.NodeID ||
		principal.AgentID != authenticated.AgentID || principal.CredentialID != authenticated.CredentialID ||
		principal.WorkerID == "" || principal.SessionEpoch < 1 || principal.CoreInstanceID == uuid.Nil ||
		(principal.Status != "active" && principal.Status != "draining") || principal.DatabaseTime.IsZero() ||
		!constantTimeStringEqual(principal.DeviceCertificateSerial, authenticated.Device.CertificateSerial) ||
		!constantTimeStringEqual(principal.DevicePublicKeyThumbprintSHA256, authenticated.Device.PublicKeyThumbprintSHA256) {
		return newRuntimeSessionError(RuntimeSessionErrorAuthenticationFailed, nil)
	}
	return nil
}

func parseRuntimeV2PathUUID(raw string) (uuid.UUID, error) {
	if raw == "" || strings.TrimSpace(raw) != raw {
		return uuid.Nil, runtimeV2ValidationError()
	}
	parsed, err := uuid.Parse(raw)
	if err != nil || parsed == uuid.Nil || parsed.String() != raw {
		return uuid.Nil, runtimeV2ValidationError()
	}
	return parsed, nil
}

func parseRuntimeV2Wait(raw string) (time.Duration, error) {
	if raw == "" {
		return 0, nil
	}
	if strings.TrimSpace(raw) != raw {
		return 0, runtimeV2ValidationError()
	}
	for _, char := range raw {
		if char < '0' || char > '9' {
			return 0, runtimeV2ValidationError()
		}
	}
	seconds, err := strconv.ParseUint(raw, 10, 8)
	if err != nil || seconds > RuntimeMaxPullWaitSeconds {
		return 0, runtimeV2ValidationError()
	}
	return time.Duration(seconds) * time.Second, nil
}

func runtimeV2ValidationError() *RuntimeTransportError {
	return NewRuntimeTransportError(RuntimeErrorValidationFailed, runtimeErrorDefaultMessage(RuntimeErrorValidationFailed))
}

func runtimeV2UnauthorizedError(cause error) *RuntimeTransportError {
	return newRuntimeTransportError(RuntimeErrorUnauthorized, runtimeErrorDefaultMessage(RuntimeErrorUnauthorized), cause)
}

func runtimeV2UnavailableError() *RuntimeTransportError {
	err := NewRuntimeTransportError(RuntimeErrorServiceUnavailable, runtimeErrorDefaultMessage(RuntimeErrorServiceUnavailable))
	err.Body.Retryable = true
	return err
}

func mapRuntimeV2HTTPError(err error) *RuntimeTransportError {
	if err == nil {
		return nil
	}
	var transportErr *RuntimeTransportError
	if errors.As(err, &transportErr) {
		return transportErr
	}
	var sessionErr *RuntimeSessionError
	if errors.As(err, &sessionErr) {
		switch sessionErr.Code {
		case RuntimeSessionErrorAuthenticationFailed, RuntimeSessionErrorPrincipalInactive:
			return runtimeV2UnauthorizedError(err)
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
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		mapped := newRuntimeTransportError(RuntimeErrorServiceUnavailable, runtimeErrorDefaultMessage(RuntimeErrorServiceUnavailable), err)
		mapped.Body.Retryable = true
		return mapped
	}
	return MapRuntimeTransportError(err)
}

func writeRuntimeV2Payload(c echo.Context, status int, payload any) error {
	if err := ValidateRuntimePayload(payload); err != nil {
		return writeRuntimeV2Error(c, newRuntimeTransportError(RuntimeErrorInternal, runtimeErrorDefaultMessage(RuntimeErrorInternal), err))
	}
	return c.JSON(status, payload)
}

func writeRuntimeV2Error(c echo.Context, err *RuntimeTransportError) error {
	if err == nil {
		err = NewRuntimeTransportError(RuntimeErrorInternal, runtimeErrorDefaultMessage(RuntimeErrorInternal))
	}
	return c.JSON(RuntimeHTTPStatus(err.Body.Code), err.Envelope())
}
